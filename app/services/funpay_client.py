from __future__ import annotations

import json
import re
from collections.abc import Callable
from dataclasses import dataclass
from urllib.parse import parse_qs, urljoin, urlparse

import httpx
from selectolax.parser import HTMLParser

from app.core.config import Settings
from app.services.http_client import RetryHttpClient
from app.services.proxy import ProxyPool

FUNPAY_BASE_URL = "https://funpay.com"
LOTS_URL_RE = re.compile(r"/en/lots/\d+/")
OFFER_ID_RE = re.compile(r"/lots/offer\?id=(\d+)")
USER_ID_RE = re.compile(r"/users/(\d+)/?")
RATING_CLASS_RE = re.compile(r"rating(\d)")
INT_RE = re.compile(r"\d+")
CURRENCY_SYMBOL_RE = re.compile(r"[₽$€]")
CURRENCY_CODE_RE = re.compile(r"\b(RUB|USD|EUR)\b", re.IGNORECASE)


@dataclass
class OfferData:
    section_id: int | None
    offer_id: int
    seller_id: int | None
    seller_name: str
    description: str
    price: float
    currency: str
    reviews_count: int | None
    seller_age: str | None
    is_online: bool | None
    auto_delivery: bool | None


@dataclass
class SectionParseResult:
    section_url: str
    section_id: int | None
    counter_total: int | None
    loaded_count: int
    offers: list[OfferData]


@dataclass
class ReviewData:
    seller_id: int
    detail: str
    text: str
    rating: int | None
    date_bucket: str | None


@dataclass
class CategorySection:
    section_id: int
    section_url: str
    section_name: str
    full_name: str


@dataclass
class GameCategory:
    game_section_id: int
    game_url: str
    game_name: str
    sections: list[CategorySection]


class FunPayClient:
    def __init__(
        self,
        settings: Settings,
        datacenter_proxies: str | None = None,
        residential_proxies: str | None = None,
        mobile_proxies: str | None = None,
        allow_direct_fallback: bool = True,
    ) -> None:
        self.settings = settings
        self.proxy_pool = ProxyPool(
            datacenter_proxies=(
                settings.datacenter_proxies if datacenter_proxies is None else datacenter_proxies
            ),
            residential_proxies=(
                settings.residential_proxies if residential_proxies is None else residential_proxies
            ),
            mobile_proxies=(settings.mobile_proxies if mobile_proxies is None else mobile_proxies),
            allow_direct_fallback=allow_direct_fallback,
        )
        self._cookies = httpx.Cookies()
        self._http = RetryHttpClient(
            settings=settings,
            proxy_pool=self.proxy_pool,
            default_headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/123.0.0.0 Safari/537.36"
                )
            },
        )

    def _request(self, method: str, url: str, data: dict[str, str] | None = None) -> httpx.Response:
        response = self._http.request(
            method=method,
            url=url,
            data=data,
            cookies=self._cookies,
        )
        self._cookies.update(response.cookies)
        return response

    @staticmethod
    def _parse_section_id_from_url(url: str) -> int | None:
        match = re.search(r"/lots/(\d+)/?$", url)
        if not match:
            return None
        return int(match.group(1))

    @staticmethod
    def _to_int(raw: str | None) -> int | None:
        if not raw:
            return None
        match = INT_RE.search(raw.replace(" ", ""))
        if not match:
            return None
        return int(match.group(0))

    @staticmethod
    def _to_float(raw: str | None) -> float | None:
        if not raw:
            return None
        normalized = raw.replace(" ", "").replace(",", ".")
        match = re.search(r"\d+(?:\.\d+)?", normalized)
        if not match:
            return None
        return float(match.group(0))

    @staticmethod
    def _extract_currency(raw_text: str, unit_text: str | None = None) -> str:
        if unit_text:
            return unit_text.strip()
        symbol_match = CURRENCY_SYMBOL_RE.search(raw_text or "")
        if symbol_match:
            return symbol_match.group(0)
        code_match = CURRENCY_CODE_RE.search(raw_text or "")
        if code_match:
            return code_match.group(1).upper()
        return ""

    @staticmethod
    def _extract_offer_id(href: str) -> int | None:
        parsed = urlparse(href)
        query_id = parse_qs(parsed.query).get("id")
        if query_id and query_id[0].isdigit():
            return int(query_id[0])
        match = OFFER_ID_RE.search(href)
        if not match:
            return None
        return int(match.group(1))

    @staticmethod
    def _extract_user_id(raw: str | None) -> int | None:
        if not raw:
            return None
        match = USER_ID_RE.search(raw)
        if not match:
            return None
        return int(match.group(1))

    def get_all_sections(self) -> list[str]:
        url = f"{FUNPAY_BASE_URL}/{self.settings.funpay_locale}/"
        response = self._request("GET", url)
        html = response.text

        found = set(LOTS_URL_RE.findall(html))
        absolute = sorted(urljoin(FUNPAY_BASE_URL, item) for item in found)
        return absolute

    @staticmethod
    def section_url(section_id: int, locale: str = "en") -> str:
        if locale == "ru":
            return f"{FUNPAY_BASE_URL}/lots/{section_id}/"
        return f"{FUNPAY_BASE_URL}/{locale}/lots/{section_id}/"

    def get_categories_catalog(self) -> list[GameCategory]:
        url = f"{FUNPAY_BASE_URL}/{self.settings.funpay_locale}/"
        response = self._request("GET", url)
        tree = HTMLParser(response.text)
        games: list[GameCategory] = []
        seen_game_ids: set[int] = set()
        generic_names = {
            "accounts",
            "items",
            "services",
            "other",
            "keys",
            "top up",
            "subscription",
            "coaching",
            "offline activation",
            "game pass",
            "twitch drops",
            "guides",
        }

        for item in tree.css(".promo-game-item"):
            game_anchor = item.css_first(".game-title a[href*='/lots/']")
            if not game_anchor:
                continue
            game_href = game_anchor.attributes.get("href", "")
            game_url = urljoin(FUNPAY_BASE_URL, game_href)
            game_id = self._parse_section_id_from_url(game_url)
            game_name = game_anchor.text(strip=True)
            if not game_id or not game_name or game_id in seen_game_ids:
                continue
            seen_game_ids.add(game_id)

            sections: dict[int, CategorySection] = {}
            for anchor in item.css("a[href*='/lots/']"):
                href = anchor.attributes.get("href", "")
                section_url = urljoin(FUNPAY_BASE_URL, href)
                section_id = self._parse_section_id_from_url(section_url)
                if not section_id:
                    continue
                section_name = anchor.text(strip=True)
                if not section_name:
                    continue

                normalized_name = section_name.lower()
                full_name = (
                    game_name
                    if normalized_name == game_name.lower()
                    else f"{game_name} > {section_name}"
                )
                current = sections.get(section_id)
                if current is None:
                    sections[section_id] = CategorySection(
                        section_id=section_id,
                        section_url=section_url,
                        section_name=section_name,
                        full_name=full_name,
                    )
                    continue

                current_is_generic = current.section_name.lower() in generic_names
                new_is_generic = normalized_name in generic_names
                if current_is_generic and not new_is_generic:
                    sections[section_id] = CategorySection(
                        section_id=section_id,
                        section_url=section_url,
                        section_name=section_name,
                        full_name=full_name,
                    )

            if game_id not in sections:
                sections[game_id] = CategorySection(
                    section_id=game_id,
                    section_url=game_url,
                    section_name=game_name,
                    full_name=game_name,
                )

            games.append(
                GameCategory(
                    game_section_id=game_id,
                    game_url=game_url,
                    game_name=game_name,
                    sections=sorted(sections.values(), key=lambda section: section.full_name.lower()),
                )
            )

        games.sort(key=lambda game: game.game_name.lower())
        return games

    def get_game_section_urls(self, game_section_id: int) -> list[str]:
        for game in self.get_categories_catalog():
            if game.game_section_id == game_section_id:
                return [section.section_url for section in game.sections]
        return []

    def search_sections(self, query: str) -> list[str]:
        url = f"{FUNPAY_BASE_URL}/{self.settings.funpay_locale}/games/promoFilter"
        response = self._request("POST", url, data={"query": query})
        payload_html = ""
        try:
            payload = response.json()
            if isinstance(payload, dict):
                payload_html = payload.get("html", "")
        except (ValueError, json.JSONDecodeError):
            payload_html = response.text

        found = set(LOTS_URL_RE.findall(payload_html))
        absolute = sorted(urljoin(FUNPAY_BASE_URL, item) for item in found)
        return absolute

    def parse_section(self, section_url: str) -> SectionParseResult:
        response = self._request("GET", section_url)
        tree = HTMLParser(response.text)

        active_counter = tree.css_first(".counter-item.active .counter-value")
        counter_total = self._to_int(active_counter.text(strip=True) if active_counter else None)

        section_id = self._parse_section_id_from_url(section_url)
        offers: list[OfferData] = []

        for row in tree.css("a.tc-item"):
            href = row.attributes.get("href", "")
            offer_id = self._extract_offer_id(href)
            if offer_id is None:
                continue

            seller_name_node = row.css_first(".media-user-name")
            seller_name = seller_name_node.text(strip=True) if seller_name_node else ""
            if not seller_name:
                continue

            description_node = row.css_first(".tc-desc-text")
            description = description_node.text(strip=True) if description_node else ""

            price_node = row.css_first(".tc-price")
            currency_node = row.css_first(".tc-price .unit")
            currency_text = currency_node.text(strip=True) if currency_node else ""
            price_text_full = price_node.text(separator=" ", strip=True) if price_node else ""
            currency = self._extract_currency(price_text_full, currency_text)
            price_text = price_text_full
            if currency:
                price_text = price_text.replace(currency, " ")
            price = self._to_float(price_text)
            if price is None:
                # fallback на data-s только если не удалось разобрать видимую цену
                price = self._to_float(price_node.attributes.get("data-s") if price_node else None)
            price = price or 0.0

            reviews_node = row.css_first(".rating-mini-count")
            reviews_count = self._to_int(reviews_node.text(strip=True) if reviews_node else None)

            seller_age_node = row.css_first(".media-user-info")
            seller_age = seller_age_node.text(strip=True) if seller_age_node else None

            user_link = row.css_first(".avatar-photo")
            user_href = user_link.attributes.get("data-href") if user_link else None
            if not user_href:
                profile_anchor = row.css_first(".avatar-photo a")
                user_href = profile_anchor.attributes.get("href") if profile_anchor else None
            seller_id = self._extract_user_id(user_href)

            media_user = row.css_first(".media-user")
            class_names = media_user.attributes.get("class", "") if media_user else ""
            is_online = " online " in f" {class_names} "

            auto_delivery = row.css_first(".auto-dlv-icon") is not None

            offers.append(
                OfferData(
                    section_id=section_id,
                    offer_id=offer_id,
                    seller_id=seller_id,
                    seller_name=seller_name,
                    description=description,
                    price=price,
                    currency=currency,
                    reviews_count=reviews_count,
                    seller_age=seller_age,
                    is_online=is_online,
                    auto_delivery=auto_delivery,
                )
            )

        deduplicated_offers = list({offer.offer_id: offer for offer in offers}.values())
        return SectionParseResult(
            section_url=section_url,
            section_id=section_id,
            counter_total=counter_total,
            loaded_count=len(deduplicated_offers),
            offers=deduplicated_offers,
        )

    @staticmethod
    def _extract_reviews_from_html(seller_id: int, html: str) -> list[ReviewData]:
        tree = HTMLParser(html)
        parsed_reviews: list[ReviewData] = []

        for item in tree.css(".review-item"):
            detail_node = item.css_first(".review-item-detail")
            detail = detail_node.text(strip=True) if detail_node else ""
            if not detail:
                continue

            text_node = item.css_first(".review-item-text")
            text = text_node.text(separator=" ", strip=True) if text_node else ""

            date_node = item.css_first(".review-item-date")
            date_bucket = date_node.text(strip=True) if date_node else None

            rating = None
            rating_node = item.css_first(".review-item-rating .rating > div")
            if rating_node:
                class_name = rating_node.attributes.get("class", "")
                match = RATING_CLASS_RE.search(class_name)
                if match:
                    rating = int(match.group(1))

            parsed_reviews.append(
                ReviewData(
                    seller_id=seller_id,
                    detail=detail,
                    text=text,
                    rating=rating,
                    date_bucket=date_bucket,
                )
            )
        return parsed_reviews

    @staticmethod
    def _extract_continue_token(html: str) -> str | None:
        tree = HTMLParser(html)
        node = tree.css_first('form.dyn-table-form input[name="continue"]')
        if not node:
            return None
        value = node.attributes.get("value")
        return value or None

    @staticmethod
    def _locale_prefix(locale: str) -> str:
        return "" if locale == "ru" else f"/{locale}"

    def get_seller_reviews(
        self,
        seller_id: int,
        max_pages: int | None = None,
        locale_override: str | None = None,
        page_callback: Callable[[int], None] | None = None,
    ) -> list[ReviewData]:
        locale = self.settings.funpay_locale if locale_override is None else locale_override
        locale_prefix = self._locale_prefix(locale)
        initial_url = f"{FUNPAY_BASE_URL}{locale_prefix}/users/{seller_id}/"
        initial_response = self._request("GET", initial_url)
        if page_callback:
            page_callback(1)

        all_reviews = self._extract_reviews_from_html(seller_id, initial_response.text)
        continue_token = self._extract_continue_token(initial_response.text)
        seen_tokens: set[str] = set()
        pages_limit = self.settings.review_max_pages_per_seller if max_pages is None else max_pages

        for page_index in range(2, pages_limit + 2):
            if not continue_token or continue_token in seen_tokens:
                break
            seen_tokens.add(continue_token)

            next_page = self._request(
                "POST",
                f"{FUNPAY_BASE_URL}{locale_prefix}/users/reviews",
                data={
                    "user_id": str(seller_id),
                    "continue": continue_token,
                    "filter": "",
                },
            )
            page_html = next_page.text
            page_reviews = self._extract_reviews_from_html(seller_id, page_html)
            if not page_reviews:
                break
            all_reviews.extend(page_reviews)
            if page_callback:
                page_callback(page_index)
            continue_token = self._extract_continue_token(page_html)

        return all_reviews
