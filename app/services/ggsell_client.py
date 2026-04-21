from __future__ import annotations

import json
import re
from dataclasses import dataclass
from urllib.parse import quote

import httpx

from app.core.config import Settings
from app.services.http_client import RetryHttpClient
from app.services.proxy import ProxyPool

GGSELL_BASE_URL = "https://ggsel.net"
GGSELL_API_BASE_URL = "https://api.ggsel.com"

REVIEW_ROW_RE = re.compile(
    r'\{"type_response":"(?P<type>[^"]*)","text_response":"(?P<text>(?:\\.|[^"\\])*)",'
    r'"date_response":"(?P<date>[^"]*)","comment":(?P<comment>null|"(?:\\.|[^"\\])*"),'
    r'"date_comment":(?P<date_comment>null|"(?:\\.|[^"\\])*")\}'
)
_NUMBER_RE = re.compile(r"[^\d.,\-]+")


@dataclass
class GgSellCategoryType:
    type_slug: str
    type_name: str
    category_url: str | None = None
    icon_alias: str | None = None


@dataclass
class GgSellCategory:
    category_slug: str
    category_name: str
    category_url: str
    type_slug: str | None = None
    type_name: str | None = None
    parent_slug: str | None = None
    parent_name: str | None = None
    digi_catalog: int | None = None
    offers_count: int | None = None


@dataclass
class GgSellOffer:
    section_slug: str
    section_id: str | None
    offer_id: str
    offer_url: str
    title: str
    seller_id: str | None
    seller_name: str
    seller_url: str | None
    price: float
    currency: str
    sold_count: int | None
    sold_text: str | None
    reviews_count: int | None = None
    auto_delivery: bool | None = None
    is_online: bool | None = None


@dataclass
class GgSellReview:
    seller_id: str | None
    type_response: str | None
    text_response: str
    date_response: str | None


class GgSellClient:
    _CURRENCY_TO_GGSELL = {
        "RUB": "wmr",
        "USD": "wmz",
        "EUR": "wme",
        "KZT": "wmt",
        "BRL": "brl",
    }
    _GGSELL_TO_CURRENCY = {
        "wmr": "RUB",
        "wmz": "USD",
        "wme": "EUR",
        "wmt": "KZT",
        "brl": "BRL",
    }

    def __init__(
        self,
        *,
        settings: Settings,
        datacenter_proxies: str | None = None,
        residential_proxies: str | None = None,
        mobile_proxies: str | None = None,
        allow_direct_fallback: bool = True,
    ) -> None:
        self.settings = settings
        self.proxy_pool = ProxyPool(
            datacenter_proxies=datacenter_proxies or settings.datacenter_proxies,
            residential_proxies=residential_proxies or settings.residential_proxies,
            mobile_proxies=mobile_proxies or settings.mobile_proxies,
            allow_direct_fallback=allow_direct_fallback,
        )
        self._http = RetryHttpClient(
            settings=settings,
            proxy_pool=self.proxy_pool,
            default_headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
                ),
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.7,en;q=0.6",
                "Origin": GGSELL_BASE_URL,
                "Referer": f"{GGSELL_BASE_URL}/",
            },
        )

    @staticmethod
    def _clean(value: object) -> str:
        if value is None:
            return ""
        return " ".join(str(value).replace("\xa0", " ").split())

    @classmethod
    def _to_int(cls, value: object) -> int | None:
        if value is None:
            return None
        text = cls._clean(value)
        if not text:
            return None
        clean = _NUMBER_RE.sub("", text).replace(",", ".")
        if not clean:
            return None
        try:
            return int(float(clean))
        except ValueError:
            return None

    @classmethod
    def _to_float(cls, value: object) -> float | None:
        if value is None:
            return None
        text = cls._clean(value)
        if not text:
            return None
        clean = _NUMBER_RE.sub("", text).replace(",", ".")
        if not clean:
            return None
        try:
            return float(clean)
        except ValueError:
            return None

    @staticmethod
    def _normalize_slug(value: object) -> str:
        return str(value or "").strip().strip("/").lower()

    @classmethod
    def to_ggsell_currency(cls, currency: str | None) -> str:
        normalized = (currency or "RUB").strip().upper()
        return cls._CURRENCY_TO_GGSELL.get(normalized, "wmr")

    @classmethod
    def from_ggsell_currency(cls, currency: str | None) -> str:
        normalized = (currency or "wmr").strip().lower()
        return cls._GGSELL_TO_CURRENCY.get(normalized, "RUB")

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        params: dict | None = None,
        json_payload: dict | None = None,
    ) -> dict:
        response = self._http.request(
            method=method,
            url=f"{GGSELL_API_BASE_URL}{path}",
            params=params,
            json=json_payload,
            headers={"Content-Type": "application/json"},
        )
        payload = response.json()
        if isinstance(payload, dict):
            return payload
        raise RuntimeError("Некорректный JSON-ответ GGSell API")

    def fetch_category_types(self, *, lang: str = "ru") -> list[GgSellCategoryType]:
        payload = self._request_json("GET", "/main/category-types", params={"lang": lang})
        rows = payload.get("data") if isinstance(payload.get("data"), list) else []
        result: list[GgSellCategoryType] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            type_slug = self._normalize_slug(row.get("url"))
            type_name = self._clean(row.get("name"))
            if not type_slug or not type_name:
                continue
            result.append(
                GgSellCategoryType(
                    type_slug=type_slug,
                    type_name=type_name,
                    category_url=self._normalize_slug(row.get("category_url")) or None,
                    icon_alias=self._clean(row.get("icon_alias")) or None,
                )
            )
        return result

    def fetch_compilation(self, *, category_type_slug: str, lang: str = "ru") -> list[dict]:
        payload = self._request_json(
            "GET",
            "/main/category-types/compilation",
            params={"category_type_url": category_type_slug, "lang": lang},
        )
        rows = payload.get("data")
        if isinstance(rows, list):
            return [row for row in rows if isinstance(row, dict)]
        return []

    def fetch_categories_catalog(self, *, lang: str = "ru") -> tuple[list[GgSellCategoryType], list[GgSellCategory]]:
        types = self.fetch_category_types(lang=lang)
        categories: dict[str, GgSellCategory] = {}
        for category_type in types:
            rows = self.fetch_compilation(category_type_slug=category_type.type_slug, lang=lang)
            for row in rows:
                row_slug = self._normalize_slug(row.get("url"))
                row_title = self._clean(row.get("title")) or row_slug
                if row_slug:
                    categories[row_slug] = GgSellCategory(
                        category_slug=row_slug,
                        category_name=row_title,
                        category_url=f"{GGSELL_BASE_URL}/catalog/{row_slug}",
                        type_slug=category_type.type_slug,
                        type_name=category_type.type_name,
                    )
                for item in row.get("items") or []:
                    if not isinstance(item, dict):
                        continue
                    slug = self._normalize_slug(item.get("url"))
                    name = self._clean(item.get("name")) or slug
                    if not slug or not name:
                        continue
                    categories[slug] = GgSellCategory(
                        category_slug=slug,
                        category_name=name,
                        category_url=f"{GGSELL_BASE_URL}/catalog/{slug}",
                        type_slug=category_type.type_slug,
                        type_name=category_type.type_name,
                        parent_slug=row_slug or None,
                        parent_name=row_title if row_slug else None,
                    )
        rows = sorted(categories.values(), key=lambda item: (item.type_name or "", item.category_name.lower()))
        return types, rows

    def search_categories(self, *, search_term: str, lang: str = "ru", limit: int = 10) -> list[GgSellCategory]:
        term = self._clean(search_term)
        if len(term) < 2:
            return []
        payload = self._request_json(
            "POST",
            "/elastic/goods/query-categories",
            json_payload={
                "search_term": term,
                "lang": lang,
                "limit": max(1, min(limit, 50)),
                "is_russian_ip": True,
            },
        )
        rows = payload.get("data") if isinstance(payload.get("data"), list) else []
        result: list[GgSellCategory] = []
        seen: set[str] = set()
        for row in rows:
            if not isinstance(row, dict):
                continue
            slug = self._normalize_slug(row.get("url"))
            if not slug or slug in seen:
                continue
            seen.add(slug)
            result.append(
                GgSellCategory(
                    category_slug=slug,
                    category_name=self._clean(row.get("name")) or slug,
                    category_url=f"{GGSELL_BASE_URL}/catalog/{slug}",
                )
            )
        return result

    def fetch_category_details(self, *, category_slug: str, lang: str = "ru") -> GgSellCategory | None:
        slug = self._normalize_slug(category_slug)
        if not slug:
            return None
        try:
            payload = self._request_json("GET", f"/categories/{quote(slug)}", params={"lang": lang})
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                return None
            raise
        data = payload.get("data") if isinstance(payload.get("data"), dict) else None
        if not data:
            return None
        title = self._clean(data.get("title")) or self._clean(data.get("name")) or slug
        parent = data.get("parent") if isinstance(data.get("parent"), dict) else None
        parent_slug = self._normalize_slug(parent.get("url")) if parent else None
        parent_name = self._clean(parent.get("title")) if parent else ""
        offers_count = self._to_int(data.get("cnt_goods"))
        digi_catalog = self._to_int(data.get("digi_catalog"))
        return GgSellCategory(
            category_slug=slug,
            category_name=title,
            category_url=f"{GGSELL_BASE_URL}/catalog/{slug}",
            parent_slug=parent_slug or None,
            parent_name=parent_name or None,
            digi_catalog=digi_catalog,
            offers_count=offers_count,
        )

    def fetch_category_offers(
        self,
        *,
        digi_catalog: int,
        requested_currency: str = "RUB",
        lang: str = "ru",
        page: int = 1,
        limit: int = 40,
        query: str = "",
    ) -> tuple[int, list[dict]]:
        payload = self._request_json(
            "POST",
            "/elastic/goods/categories",
            json_payload={
                "digi_catalog": int(digi_catalog),
                "limit": max(1, min(limit, 200)),
                "content_type_ids": [],
                "search_after": [],
                "with_filters": True,
                "is_preorders": False,
                "ab_test_without_emoji": True,
                "sort": "sortByRec",
                "query_string": self._clean(query),
                "with_forbidden": False,
                "min_price": "",
                "max_price": "",
                "currency": self.to_ggsell_currency(requested_currency),
                "lang": lang,
                "platforms": [],
                "page": max(1, int(page)),
                "is_russian_ip": True,
            },
        )
        data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
        total = self._to_int(data.get("total")) or 0
        items = data.get("items") if isinstance(data.get("items"), list) else []
        return total, [item for item in items if isinstance(item, dict)]

    def parse_offer_item(self, item: dict, *, section_slug: str, requested_currency: str = "RUB") -> GgSellOffer | None:
        offer_id = self._to_int(item.get("id_goods"))
        offer_slug = self._normalize_slug(item.get("url"))
        seller_name = self._clean(item.get("seller_name"))
        if offer_id is None or not offer_slug or not seller_name:
            return None
        seller_id = self._to_int(item.get("id_seller"))
        currency_code = self.to_ggsell_currency(requested_currency)
        price_raw = item.get(f"price_{currency_code}") or item.get("price_wmr") or item.get("price_wmz")
        price = self._to_float(price_raw)
        if price is None:
            return None
        sold_count = self._to_int(item.get("cnt_sell"))
        sold_text = f"Продано {sold_count}" if sold_count is not None else None
        reviews_count = self._to_int(item.get("cnt_digi_responses"))
        section_id = self._to_int(item.get("id_section"))
        return GgSellOffer(
            section_slug=section_slug,
            section_id=(str(section_id) if section_id is not None else None),
            offer_id=str(offer_id),
            offer_url=f"{GGSELL_BASE_URL}/catalog/product/{offer_slug}",
            title=self._clean(item.get("name")) or offer_slug,
            seller_id=(str(seller_id) if seller_id is not None else None),
            seller_name=seller_name,
            seller_url=(f"{GGSELL_BASE_URL}/sellers/{seller_id}" if seller_id is not None else None),
            price=float(price),
            currency=self.from_ggsell_currency(currency_code),
            sold_count=sold_count,
            sold_text=sold_text,
            reviews_count=reviews_count,
            auto_delivery=bool(item.get("autoselling")) if item.get("autoselling") is not None else None,
            is_online=None,
        )

    def fetch_seller_reviews(
        self,
        *,
        seller_id: str,
        lang: str = "ru",
        page: int = 1,
        limit: int = 20,
    ) -> tuple[int, list[dict]]:
        payload = self._request_json(
            "GET",
            f"/sellers/{quote(str(seller_id).strip())}/reviews",
            params={
                "page": max(1, int(page)),
                "limit": max(1, min(limit, 100)),
                "lang": lang,
            },
        )
        total = self._to_int(payload.get("total")) or 0
        rows = payload.get("data") if isinstance(payload.get("data"), list) else []
        return total, [row for row in rows if isinstance(row, dict)]

    @staticmethod
    def _decode_json_string(value: str) -> str:
        try:
            return json.loads(f"\"{value}\"")
        except Exception:
            return value.replace("\\n", "\n").replace("\\t", "\t").replace('\\"', '"')

    def fetch_product_reviews_from_html(self, *, offer_url: str, seller_id: str | None = None) -> list[GgSellReview]:
        response = self._http.request(
            method="GET",
            url=offer_url,
            headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
        )
        html = response.text or ""
        reviews: list[GgSellReview] = []
        seen: set[tuple[str, str, str]] = set()
        for match in REVIEW_ROW_RE.finditer(html):
            raw_text = match.group("text")
            text_value = self._decode_json_string(raw_text).strip()
            type_value = self._clean(match.group("type")).lower() or None
            date_value = self._clean(match.group("date")) or None
            signature = (type_value or "", text_value, date_value or "")
            if signature in seen:
                continue
            seen.add(signature)
            reviews.append(
                GgSellReview(
                    seller_id=seller_id,
                    type_response=type_value,
                    text_response=text_value,
                    date_response=date_value,
                )
            )
        return reviews
