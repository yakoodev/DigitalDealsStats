from __future__ import annotations

import json
import random
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import urljoin

from selectolax.parser import HTMLParser

from app.core.config import Settings
from app.services.http_client import RetryHttpClient
from app.services.proxy import ProxyPool

PLATI_BASE_URL = "https://plati.market"

MONEY_RE = re.compile(
    r"(?P<amount>\d[\d\s]*(?:[.,]\d+)?)\s*(?P<currency>₽|RUB|USD|EUR|\$|€|UAH|грн)?",
    flags=re.IGNORECASE,
)
SELLER_PATH_RE = re.compile(r"/seller/([^/]+)/(\d+)/?", flags=re.IGNORECASE)
ITEM_PATH_RE = re.compile(r"/itm/[^/]+/(\d+)", flags=re.IGNORECASE)
GAME_PATH_RE = re.compile(r"/games/([^/]+)/(\d+)/?", flags=re.IGNORECASE)
DATE_RE = re.compile(r"(\d{1,2})\.(\d{1,2})\.(\d{4})")
REVIEW_COUNT_RE = re.compile(r"Отзывы\s*([0-9][0-9\s\xa0]*)", flags=re.IGNORECASE)
WMID_RE = re.compile(r"sellerWmid:\s*'(\d+)'", flags=re.IGNORECASE)
SOLD_RE = re.compile(
    r"(?:Продано|Sold)\s*([0-9][0-9\s\xa0]*(?:[.,]\d+)?)\s*(млн|тыс|m|k)?\s*(\+?)",
    flags=re.IGNORECASE,
)
SOLD_LESS_THAN_RE = re.compile(
    r"(?:Продано|Sold)\s*(?:менее|less\s+than)\s*([0-9][0-9\s\xa0]*)",
    flags=re.IGNORECASE,
)
CAT_PATH_RE = re.compile(r"/cat/([^/?#]+)/(\d+)/?", flags=re.IGNORECASE)


@dataclass
class PlatiCategorySection:
    section_id: int
    section_slug: str
    section_url: str
    section_name: str
    full_name: str
    counter_total: int | None = None
    group_id: int | None = None


@dataclass
class PlatiCategoryGroup:
    group_id: int
    group_slug: str
    group_url: str
    group_name: str
    sections: list[PlatiCategorySection]


@dataclass
class PlatiCatalogNode:
    section_id: int
    section_slug: str
    title: str
    cnt: int | None
    path: list[str]
    url: str
    children: list["PlatiCatalogNode"]


@dataclass
class PlatiOfferCard:
    section_id: int
    section_slug: str | None
    offer_id: str
    offer_url: str
    title: str
    seller_name: str
    sold_text: str | None
    price: float
    currency: str
    reviews_count: int | None = None
    seller_id: str | None = None
    seller_url: str | None = None
    seller_wmid: str | None = None
    is_online: bool | None = None
    auto_delivery: bool | None = None
    sold_count: int | None = None
    sold_is_lower_bound: bool = False


@dataclass
class PlatiReview:
    seller_id: str
    detail: str
    text: str
    rating: int | None
    date_bucket: str | None
    offer_id: str | None
    offer_url: str | None
    created_at: str | None


@dataclass
class PlatiGame:
    game_id: int
    game_slug: str
    game_url: str
    game_name: str


@dataclass
class PlatiGameCategory:
    category_id: int
    category_name: str
    offers_count: int | None = None


class PlatiMarketClient:
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
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.7,en;q=0.6",
            },
        )

    @staticmethod
    def _clean(value: str | None) -> str:
        if not value:
            return ""
        return " ".join(str(value).replace("\xa0", " ").split())

    @staticmethod
    def _to_int(value: object) -> int | None:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        text = text.replace("\xa0", "").replace(" ", "")
        if not text.lstrip("-").isdigit():
            return None
        try:
            return int(text)
        except ValueError:
            return None

    @staticmethod
    def _to_float(value: object) -> float | None:
        if value is None:
            return None
        text = str(value).strip().replace("\xa0", "").replace(" ", "").replace(",", ".")
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None

    @staticmethod
    def _normalize_slug(value: object) -> str:
        return str(value or "").strip().strip("/").lower()

    @staticmethod
    def _absolute(url: str) -> str:
        return urljoin(PLATI_BASE_URL, url)

    @staticmethod
    def _normalize_currency(raw: str | None, fallback: str = "RUB") -> str:
        value = (raw or "").strip().upper()
        if value in {"₽", "RUR", "RUB"}:
            return "RUB"
        if value in {"USD", "$"}:
            return "USD"
        if value in {"EUR", "€"}:
            return "EUR"
        if value in {"UAH", "ГРН"}:
            return "UAH"
        return fallback.upper()

    @classmethod
    def parse_sold_text(cls, value: str | None) -> tuple[int | None, bool]:
        text = cls._clean(value)
        if not text:
            return None, False
        less_than_match = SOLD_LESS_THAN_RE.search(text)
        if less_than_match:
            upper_limit = cls._to_int(less_than_match.group(1))
            if upper_limit is None:
                return None, False
            return max(upper_limit - 1, 0), False
        match = SOLD_RE.search(text)
        if not match:
            return None, False
        amount_raw = cls._clean(match.group(1)).replace(" ", "").replace(",", ".")
        if not amount_raw:
            return None, False
        try:
            amount = float(amount_raw)
        except ValueError:
            return None, False
        suffix = (match.group(2) or "").strip().lower()
        multiplier = 1
        if suffix in {"млн", "m"}:
            multiplier = 1_000_000
        elif suffix in {"тыс", "k"}:
            multiplier = 1_000
        normalized_amount = max(0, int(round(amount * multiplier)))
        is_lower_bound = bool(match.group(3))
        return normalized_amount, is_lower_bound

    @staticmethod
    def _split_prefixed_response(payload: str, parts: int) -> tuple[list[str], str]:
        data = payload or ""
        parsed: list[str] = []
        start = 0
        for _ in range(parts):
            sep = data.find("|", start)
            if sep < 0:
                return [], data
            parsed.append(data[start:sep])
            start = sep + 1
        return parsed, data[start:]

    def _request(
        self,
        method: str,
        url: str,
        *,
        params: dict | None = None,
        headers: dict[str, str] | None = None,
    ) -> str:
        response = self._http.request(
            method=method,
            url=url,
            params=params,
            headers=headers,
        )
        return response.text

    def fetch_categories_catalog(self) -> list[PlatiCategoryGroup]:
        tree = self.fetch_catalog_tree()
        if not tree:
            return []

        groups: list[PlatiCategoryGroup] = []
        for node in tree:
            flat_nodes = [node, *self.flatten_catalog_children(node)]
            sections: list[PlatiCategorySection] = []
            seen: set[int] = set()
            for entry in flat_nodes:
                if entry.section_id in seen:
                    continue
                seen.add(entry.section_id)
                section_name = entry.title
                full_name = " > ".join(entry.path) if entry.path else section_name
                sections.append(
                    PlatiCategorySection(
                        section_id=entry.section_id,
                        section_slug=entry.section_slug,
                        section_url=entry.url,
                        section_name=section_name,
                        full_name=full_name,
                        counter_total=entry.cnt,
                        group_id=node.section_id,
                    )
                )
            groups.append(
                PlatiCategoryGroup(
                    group_id=node.section_id,
                    group_slug=node.section_slug,
                    group_url=node.url,
                    group_name=node.title,
                    sections=sorted(sections, key=lambda item: item.full_name.lower()),
                )
            )
        groups.sort(key=lambda item: item.group_name.lower())
        return groups

    @staticmethod
    def flatten_catalog_children(node: PlatiCatalogNode) -> list[PlatiCatalogNode]:
        rows: list[PlatiCatalogNode] = []
        for child in node.children:
            rows.append(child)
            rows.extend(PlatiMarketClient.flatten_catalog_children(child))
        return rows

    def fetch_catalog_tree(self) -> list[PlatiCatalogNode]:
        body = self._request("GET", f"{PLATI_BASE_URL}/api/catalog_menu_json.asp")
        payload = json.loads(body)
        if not isinstance(payload, dict):
            return []

        def parse_node(node: dict, path: list[str]) -> PlatiCatalogNode | None:
            if not isinstance(node, dict):
                return None
            node_id = self._to_int(node.get("ID_R"))
            title = self._clean(node.get("Title"))
            slug = self._normalize_slug(node.get("UrlPreparedName"))
            cnt = self._to_int(node.get("Cnt"))
            if node_id is None or not title:
                return None
            if not slug:
                url_raw = self._clean(node.get("Link")) or self._clean(node.get("Url"))
                if url_raw:
                    match = CAT_PATH_RE.search(url_raw)
                    if match:
                        slug = self._normalize_slug(match.group(1))
            if not slug:
                slug = str(node_id)
            next_path = [*path, title]
            children_rows: list[PlatiCatalogNode] = []
            children = node.get("Children")
            if isinstance(children, dict):
                for raw in children.values():
                    child = parse_node(raw, next_path)
                    if child is not None:
                        children_rows.append(child)
            return PlatiCatalogNode(
                section_id=node_id,
                section_slug=slug,
                title=title,
                cnt=cnt,
                path=next_path,
                url=f"{PLATI_BASE_URL}/cat/{slug}/{node_id}/",
                children=sorted(children_rows, key=lambda item: item.title.lower()),
            )

        rows: list[PlatiCatalogNode] = []
        for item in payload.values():
            parsed = parse_node(item, [])
            if parsed is not None:
                rows.append(parsed)
        rows.sort(key=lambda item: item.title.lower())
        return rows

    def parse_games_block(self, html: str) -> list[PlatiGame]:
        parser = HTMLParser(html)
        games: list[PlatiGame] = []
        seen: set[int] = set()
        for anchor in parser.css("a[href^='/games/']"):
            href = self._clean(anchor.attributes.get("href"))
            if not href:
                continue
            match = GAME_PATH_RE.search(href)
            if not match:
                continue
            game_slug = self._normalize_slug(match.group(1))
            game_id = self._to_int(match.group(2))
            if game_id is None or not game_slug or game_id in seen:
                continue
            game_name = self._clean(anchor.attributes.get("title")) or self._clean(anchor.text())
            if not game_name:
                continue
            seen.add(game_id)
            games.append(
                PlatiGame(
                    game_id=game_id,
                    game_slug=game_slug,
                    game_url=f"{PLATI_BASE_URL}/games/{game_slug}/{game_id}/",
                    game_name=game_name,
                )
            )
        return games

    def fetch_games_page(
        self,
        *,
        page: int,
        rows: int = 200,
        id_cg: int = 0,
        letter: str = "",
        preorders: int = 0,
        sort: str = "",
        lang: str = "ru",
    ) -> tuple[int | None, list[PlatiGame]]:
        params: dict[str, object] = {
            "id_cg": id_cg,
            "l": letter,
            "preorders": preorders,
            "sort": sort,
            "page": page,
            "rows": rows,
            "lang": lang,
        }
        body = self._request("GET", f"{PLATI_BASE_URL}/asp/block_games2.asp", params=params)
        parts, html = self._split_prefixed_response(body, 1)
        total_count = self._to_int(parts[0]) if parts else None
        games = self.parse_games_block(html)
        return total_count, games

    def fetch_games_catalog(
        self,
        *,
        rows: int = 200,
        max_pages: int | None = None,
        lang: str = "ru",
    ) -> list[PlatiGame]:
        seen: set[int] = set()
        games: list[PlatiGame] = []
        total_count: int | None = None
        # Если источник возвращает total_count, читаем до него без жесткого лимита.
        # Иначе используем мягкий ceiling по страницам, чтобы не уйти в бесконечный цикл.
        hard_limit_pages = max_pages if isinstance(max_pages, int) and max_pages > 0 else 200
        stagnant_pages = 0
        for page in range(1, hard_limit_pages + 1):
            page_total, page_items = self.fetch_games_page(page=page, rows=rows, lang=lang)
            if total_count is None:
                total_count = page_total
            if not page_items:
                break
            added = 0
            for item in page_items:
                if item.game_id in seen:
                    continue
                seen.add(item.game_id)
                games.append(item)
                added += 1
            if added == 0:
                stagnant_pages += 1
                if stagnant_pages >= 2:
                    break
            else:
                stagnant_pages = 0
            if total_count is not None and len(games) >= total_count:
                break
        games.sort(key=lambda item: item.game_name.lower())
        return games

    def fetch_game_categories(
        self,
        *,
        game_id: int,
        game_slug: str | None = None,
        lang: str = "ru",
    ) -> list[PlatiGameCategory]:
        normalized_slug = self._normalize_slug(game_slug)
        if normalized_slug:
            url = f"{PLATI_BASE_URL}/games/{normalized_slug}/{game_id}/"
        else:
            # Fallback: URL без slug обычно редиректит/отдаёт каноническую страницу.
            url = f"{PLATI_BASE_URL}/games/{game_id}/"
        html = self._request("GET", url, params={"lang": lang})
        parser = HTMLParser(html)
        categories: list[PlatiGameCategory] = []
        seen: set[int] = set()
        for row in parser.css("#cat_selector .radio"):
            input_node = row.css_first("input.id_c_radios")
            if input_node is None:
                continue
            category_id = self._to_int(input_node.attributes.get("value"))
            if category_id is None or category_id in seen:
                continue
            category_name = self._clean(input_node.attributes.get("data-name"))
            if not category_name:
                label_node = row.css_first("label.radio__label")
                category_name = self._clean(label_node.text() if label_node else "")
            if not category_name:
                continue
            count_node = row.css_first("label.radio__label span.footnote-regular")
            offers_count = self._to_int(count_node.text() if count_node else None)
            categories.append(
                PlatiGameCategory(
                    category_id=category_id,
                    category_name=category_name,
                    offers_count=offers_count,
                )
            )
            seen.add(category_id)
        categories.sort(key=lambda item: (item.category_id != 0, item.category_name.lower()))
        return categories

    def fetch_game_offers_page(
        self,
        *,
        game_id: int,
        category_id: int = 0,
        currency: str,
        page: int,
        rows: int = 24,
        sort: str = "",
        min_price: float | None = None,
        max_price: float | None = None,
        lang: str = "ru",
    ) -> tuple[int | None, float | None, float | None, list[PlatiOfferCard]]:
        params: dict[str, object] = {
            "id_cb": game_id,
            "id_c": category_id,
            "sort": sort,
            "page": page,
            "rows": rows,
            "curr": currency.lower(),
            "lang": lang,
        }
        if min_price is not None:
            params["price_min"] = min_price
        if max_price is not None:
            params["price_max"] = max_price

        body = self._request("GET", f"{PLATI_BASE_URL}/asp/block_goods_category_2.asp", params=params)
        parts, html = self._split_prefixed_response(body, 3)
        total_count = self._to_int(parts[0]) if len(parts) >= 1 else None
        min_value = self._to_float(parts[1]) if len(parts) >= 2 else None
        max_value = self._to_float(parts[2]) if len(parts) >= 3 else None
        offers = self.parse_offers_block(
            html=html,
            section_id=game_id,
            section_slug=f"game/{game_id}",
            default_currency=currency,
        )
        return total_count, min_value, max_value, offers

    def fetch_section_stats(
        self,
        *,
        section_id: int,
        currency: str,
        min_price: float | None = None,
        max_price: float | None = None,
    ) -> tuple[int | None, float | None, float | None]:
        params: dict[str, object] = {
            "id_r": section_id,
            "curr": currency,
            "GetStats": 1,
            "rnd": random.random(),
        }
        if min_price is not None:
            params["minPrice"] = min_price
        if max_price is not None:
            params["maxPrice"] = max_price

        body = self._request("GET", f"{PLATI_BASE_URL}/asp/block_goods_r.asp", params=params)
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            return None, None, None
        if not isinstance(data, dict):
            return None, None, None
        total_count = self._to_int(data.get("TotalCount"))
        min_value = self._to_float(data.get("MinPrice"))
        max_value = self._to_float(data.get("MaxPrice"))
        return total_count, min_value, max_value

    def fetch_section_offers_page(
        self,
        *,
        section_id: int,
        section_slug: str | None,
        currency: str,
        page: int,
        rows: int = 10,
        sort: str = "popular",
        min_price: float | None = None,
        max_price: float | None = None,
    ) -> list[PlatiOfferCard]:
        params: dict[str, object] = {
            "id_r": section_id,
            "sort": sort,
            "page": page,
            "rows": rows,
            "curr": currency,
            "rnd": random.random(),
        }
        if min_price is not None:
            params["minprice"] = min_price
        if max_price is not None:
            params["maxprice"] = max_price

        html = self._request("GET", f"{PLATI_BASE_URL}/asp/block_goods_r.asp", params=params)
        return self.parse_offers_block(
            html=html,
            section_id=section_id,
            section_slug=section_slug,
            default_currency=currency,
        )

    def parse_offers_block(
        self,
        *,
        html: str,
        section_id: int,
        section_slug: str | None,
        default_currency: str,
    ) -> list[PlatiOfferCard]:
        parser = HTMLParser(html)
        cards: list[PlatiOfferCard] = []
        seen: set[str] = set()
        for anchor in parser.css("a[product_id]"):
            offer_id = self._clean(anchor.attributes.get("product_id"))
            if not offer_id or offer_id in seen:
                continue
            seen.add(offer_id)

            href = self._clean(anchor.attributes.get("href"))
            if not href:
                continue
            offer_url = self._absolute(href)

            title_node = (
                anchor.css_first("[name='title'] span")
                or anchor.css_first("[name='title']")
                or anchor.css_first("p.custom-link span.footnote-medium")
                or anchor.css_first("p.custom-link span")
                or anchor.css_first("span.footnote-medium")
            )
            title = self._clean(title_node.text() if title_node else anchor.attributes.get("title"))
            if not title:
                title = self._clean(anchor.text())
            if not title:
                continue

            price_node = (
                anchor.css_first("span[name='price']")
                or anchor.css_first("span.title-bold")
                or anchor.css_first("span.title-semibold")
            )
            price_text = self._clean(price_node.text() if price_node else "")
            price_match = MONEY_RE.search(price_text)
            if not price_match:
                price_match = MONEY_RE.search(self._clean(anchor.text()))
            if not price_match:
                continue
            price = self._to_float(price_match.group("amount"))
            if price is None:
                continue
            currency = self._normalize_currency(price_match.group("currency"), fallback=default_currency)

            seller_node = (
                anchor.css_first(".card-image-wrapper .caption-semibold")
                or anchor.css_first("p .caption-semibold")
            )
            seller_name = self._clean(seller_node.text() if seller_node else "")
            sold_node = (
                anchor.css_first("span[name='sold']")
                or anchor.css_first("span.footnote-regular.color-text-secondary")
            )
            sold_text = self._clean(sold_node.text() if sold_node else "")
            sold_count, sold_is_lower_bound = self.parse_sold_text(sold_text)

            cards.append(
                PlatiOfferCard(
                    section_id=section_id,
                    section_slug=section_slug,
                    offer_id=offer_id,
                    offer_url=offer_url,
                    title=title,
                    seller_name=seller_name,
                    sold_text=sold_text or None,
                    sold_count=sold_count,
                    sold_is_lower_bound=sold_is_lower_bound,
                    price=float(price),
                    currency=currency,
                )
            )
        return cards

    def fetch_offer_details(self, offer_url: str) -> dict[str, str | int | bool | None]:
        html = self._request("GET", offer_url)
        parser = HTMLParser(html)

        title_node = parser.css_first("h1")
        title = self._clean(title_node.text() if title_node else "")

        seller_node = parser.css_first("a[href^='/seller/']")
        seller_url: str | None = None
        seller_name: str | None = None
        seller_id: str | None = None
        if seller_node is not None:
            href = self._clean(seller_node.attributes.get("href"))
            if href:
                seller_url = self._absolute(href)
                match = SELLER_PATH_RE.search(href)
                if match:
                    seller_name = self._clean(match.group(1))
                    seller_id = self._clean(match.group(2))
            if not seller_name:
                seller_name = self._clean(seller_node.text())

        review_match = REVIEW_COUNT_RE.search(html)
        reviews_count = self._to_int(review_match.group(1)) if review_match else None

        wmid_match = WMID_RE.search(html)
        seller_wmid = self._clean(wmid_match.group(1)) if wmid_match else None

        sold_text = ""
        sold_node = parser.css_first("span.footnote-regular.color-text-secondary")
        if sold_node is not None:
            sold_text = self._clean(sold_node.text())
        if not sold_text:
            sold_match = SOLD_RE.search(html) or SOLD_LESS_THAN_RE.search(html)
            if sold_match is not None:
                sold_text = self._clean(sold_match.group(0))
        sold_count, sold_is_lower_bound = self.parse_sold_text(sold_text)

        return {
            "title": title or None,
            "seller_id": seller_id or None,
            "seller_name": seller_name or None,
            "seller_url": seller_url or None,
            "reviews_count": reviews_count,
            "seller_wmid": seller_wmid or None,
            "sold_text": sold_text or None,
            "sold_count": sold_count,
            "sold_is_lower_bound": sold_is_lower_bound,
        }

    def fetch_seller_reviews(
        self,
        *,
        seller_id: str,
        lang: str = "ru-RU",
        max_pages: int = 3,
        rows: int = 5,
        mode: int = 0,
        order: int = 1,
    ) -> list[PlatiReview]:
        reviews: list[PlatiReview] = []
        for page in range(1, max_pages + 1):
            params = {
                "id_d": 0,
                "id_s": seller_id,
                "mode": mode,
                "page": page,
                "rows": rows,
                "cat": "digi",
                "ord": order,
                "lang": lang,
            }
            body = self._request("GET", f"{PLATI_BASE_URL}/asp/block_responses2.asp", params=params)
            payload = body.strip()
            if not payload:
                break

            prefixed = re.match(r"^\s*(\d+)\|(.*)$", payload, flags=re.S)
            if prefixed:
                payload = prefixed.group(2)

            page_reviews = self.parse_reviews_block(payload, default_seller_id=seller_id)
            if not page_reviews:
                break
            reviews.extend(page_reviews)
            if len(page_reviews) < rows:
                break
        return self._dedupe_reviews(reviews)

    def parse_reviews_block(self, html: str, *, default_seller_id: str) -> list[PlatiReview]:
        parser = HTMLParser(f"<ul>{html}</ul>")
        rows: list[PlatiReview] = []
        for item in parser.css("li"):
            raw_html = item.html or ""
            positive = "#thumb-up" in raw_html
            negative = "#thumb-down" in raw_html
            rating = 5 if positive else (1 if negative else None)

            date_node = item.css_first("span.footnote-regular")
            date_bucket = self._clean(date_node.text() if date_node else "")

            link_node = item.css_first("a.custom-link--primary")
            detail_node = link_node.css_first("span") if link_node else None
            detail = self._clean(detail_node.text() if detail_node else (link_node.text() if link_node else ""))

            offer_url: str | None = None
            offer_id: str | None = None
            if link_node is not None:
                href = self._clean(link_node.attributes.get("href"))
                if href:
                    offer_url = self._absolute(href)
                    offer_match = ITEM_PATH_RE.search(href)
                    if offer_match:
                        offer_id = self._clean(offer_match.group(1))

            text_node = item.css_first("div.body-regular")
            text = self._clean(text_node.text() if text_node else "")

            if not detail and not text:
                continue
            rows.append(
                PlatiReview(
                    seller_id=default_seller_id,
                    detail=detail,
                    text=text,
                    rating=rating,
                    date_bucket=date_bucket or None,
                    offer_id=offer_id,
                    offer_url=offer_url,
                    created_at=self._extract_iso_date(date_bucket),
                )
            )
        return rows

    @staticmethod
    def _extract_iso_date(value: str | None) -> str | None:
        if not value:
            return None
        match = DATE_RE.search(value)
        if not match:
            return None
        day, month, year = match.groups()
        try:
            parsed = datetime(int(year), int(month), int(day), tzinfo=UTC)
        except ValueError:
            return None
        return parsed.isoformat()

    @staticmethod
    def _dedupe_reviews(reviews: list[PlatiReview]) -> list[PlatiReview]:
        seen: set[tuple[str, str, str, str]] = set()
        result: list[PlatiReview] = []
        for row in reviews:
            key = (
                row.seller_id,
                row.offer_id or "",
                row.detail,
                row.text,
            )
            if key in seen:
                continue
            seen.add(key)
            result.append(row)
        return result
