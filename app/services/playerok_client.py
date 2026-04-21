from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import urljoin, urlparse

import httpx
from selectolax.parser import HTMLParser

from app.core.config import Settings
from app.services.http_client import RetryHttpClient
from app.services.proxy import ProxyPool
from app.services.text_utils import is_text_relevant, normalize_text, query_tokens

PLAYEROK_BASE_URL = "https://playerok.com"
PLAYEROK_GRAPHQL_URL = f"{PLAYEROK_BASE_URL}/graphql"
PLAYEROK_GRAPHQL_PAGE_LIMIT = 20
PLAYEROK_MIN_DELAY_SECONDS = 0.06
PLAYEROK_JITTER_MIN = 0.04
PLAYEROK_JITTER_MAX = 0.12

PRODUCT_PATH_RE = re.compile(r"^/products/([a-z0-9-]+)", re.IGNORECASE)
PROFILE_PRODUCTS_RE = re.compile(r"^/profile/([^/]+)/products/?$", re.IGNORECASE)
SECTION_PATH_RE = re.compile(r"^/([a-z0-9-]+)/([a-z0-9-]+)$", re.IGNORECASE)

MONEY_RE = re.compile(
    r"(\d[\d\s]*(?:[.,]\d+)?)\s*(₽|\$|€|RUB|USD|EUR|руб(?:\.|лей|ля)?)",
    re.IGNORECASE,
)
REVIEW_COUNT_RE = re.compile(r"(\d[\d\s]*)\s*(?:отзыв|review)", re.IGNORECASE)
TOTAL_OFFERS_RE = re.compile(r"(?:Всего|Total)\s+(\d[\d\s]*)\s+(?:товар|product)", re.IGNORECASE)
SAFE_ENDPOINT_RE = re.compile(r"(/api/[a-zA-Z0-9_/?=&\-]+)", re.IGNORECASE)

ITEMS_QUERY = """
query items($filter: ItemFilter, $pagination: Pagination) {
  items(filter: $filter, pagination: $pagination) {
    totalCount
    pageInfo {
      hasNextPage
      endCursor
    }
    edges {
      cursor
      node {
        id
        slug
        name
        price
        rawPrice
        approvalDate
        createdAt
        user {
          id
          username
          isOnline
          rating
          testimonialCounter
        }
        game {
          id
          slug
          name
        }
        category {
          id
          slug
          name
        }
      }
    }
  }
}
"""

TOP_ITEMS_QUERY = """
query topItems($pagination: Pagination, $filter: TopItemFilter) {
  topItems(pagination: $pagination, filter: $filter) {
    totalCount
    pageInfo {
      hasNextPage
      endCursor
    }
    edges {
      cursor
      node {
        id
        slug
        name
        price
        rawPrice
        approvalDate
        createdAt
        user {
          id
          username
          isOnline
          rating
          testimonialCounter
        }
        game {
          id
          slug
          name
        }
        category {
          id
          slug
          name
        }
      }
    }
  }
}
"""

GAMES_QUERY = """
query games($pagination: Pagination, $filter: GameFilter) {
  games(pagination: $pagination, filter: $filter) {
    totalCount
    pageInfo {
      hasNextPage
      endCursor
    }
    edges {
      cursor
      node {
        id
        slug
        name
      }
    }
  }
}
"""

GAME_CATEGORIES_QUERY = """
query gameCategories($slug: String) {
  game(slug: $slug) {
    id
    slug
    name
    categories {
      id
      slug
      name
    }
  }
}
"""

TESTIMONIALS_QUERY = """
query testimonials($pagination: Pagination, $filter: TestimonialFilter!) {
  testimonials(pagination: $pagination, filter: $filter) {
    totalCount
    pageInfo {
      hasNextPage
      endCursor
    }
    edges {
      cursor
      node {
        id
        status
        text
        rating
        createdAt
        deal {
          id
          item {
            id
            slug
            name
            price
            rawPrice
            sellerType
            game {
              id
              slug
              name
            }
            category {
              id
              slug
              name
            }
          }
        }
      }
    }
  }
}
"""


@dataclass
class PlayerOkCategorySection:
    section_id: str | None
    game_id: str | None
    game_slug: str
    game_name: str
    category_slug: str
    section_slug: str
    section_url: str
    section_name: str
    full_name: str


@dataclass
class PlayerOkGameCategory:
    game_id: str | None
    game_slug: str
    game_url: str
    game_name: str
    sections: list[PlayerOkCategorySection]


@dataclass
class PlayerOkOfferPreview:
    offer_id: str
    offer_url: str
    description: str


@dataclass
class PlayerOkSectionParseResult:
    section_url: str
    section_slug: str
    section_name: str
    counter_total: int | None
    loaded_count: int
    offers: list[PlayerOkOfferPreview]
    safe_endpoints: list[str]


@dataclass
class PlayerOkReviewData:
    seller_slug: str
    seller_uuid: str | None
    detail: str
    text: str
    rating: int | None
    date_bucket: str | None
    game_id: str | None
    category_id: str | None
    amount: float | None
    created_at: str | None


@dataclass
class PlayerOkOfferData:
    game_id: str | None
    game_slug: str | None
    game_name: str | None
    category_id: str | None
    category_slug: str | None
    section_slug: str | None
    section_name: str | None
    offer_id: str
    offer_url: str
    seller_uuid: str | None
    seller_slug: str | None
    seller_name: str
    seller_url: str | None
    description: str
    price: float
    currency: str
    reviews_count: int | None
    is_online: bool | None
    auto_delivery: bool | None
    reviews: list[PlayerOkReviewData]


@dataclass
class PlayerOkSectionOffersResult:
    section_url: str
    section_slug: str
    section_name: str
    counter_total: int | None
    loaded_count: int
    offers: list[PlayerOkOfferData]


class PlayerOkClient:
    def __init__(
        self,
        settings: Settings,
        *,
        datacenter_proxies: str | None = None,
        residential_proxies: str | None = None,
        mobile_proxies: str | None = None,
        advanced_headers: dict[str, str] | None = None,
        advanced_cookies: dict[str, str] | None = None,
        use_html_degrade: bool = True,
        allow_direct_fallback: bool = True,
    ) -> None:
        self.settings = settings
        self.use_html_degrade = use_html_degrade
        self.proxy_pool = ProxyPool(
            datacenter_proxies=settings.datacenter_proxies if datacenter_proxies is None else datacenter_proxies,
            residential_proxies=(
                settings.residential_proxies if residential_proxies is None else residential_proxies
            ),
            mobile_proxies=settings.mobile_proxies if mobile_proxies is None else mobile_proxies,
            allow_direct_fallback=allow_direct_fallback,
        )

        default_headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            )
        }
        if advanced_headers:
            for key, value in advanced_headers.items():
                k = str(key or "").strip()
                v = str(value or "").strip()
                if k and v:
                    default_headers[k] = v

        self._cookies = httpx.Cookies()
        if advanced_cookies:
            for key, value in advanced_cookies.items():
                k = str(key or "").strip()
                v = str(value or "").strip()
                if k and v:
                    self._cookies.set(k, v, domain="playerok.com", path="/")

        self._http = RetryHttpClient(
            settings=settings,
            proxy_pool=self.proxy_pool,
            min_delay_seconds=PLAYEROK_MIN_DELAY_SECONDS,
            jitter_min=PLAYEROK_JITTER_MIN,
            jitter_max=PLAYEROK_JITTER_MAX,
            default_headers=default_headers,
        )

    @staticmethod
    def _normalize_slug(value: str) -> str:
        return normalize_text(value.strip().strip("/")).replace(" ", "-")

    @staticmethod
    def _to_abs_url(value: str) -> str:
        return urljoin(PLAYEROK_BASE_URL, value)

    @staticmethod
    def _offer_id_from_url(offer_url: str) -> str | None:
        parsed = urlparse(offer_url)
        match = PRODUCT_PATH_RE.match(parsed.path)
        if not match:
            return None
        return match.group(1)

    @staticmethod
    def _extract_int(value: str) -> int | None:
        compact = re.sub(r"\D+", "", value or "")
        if not compact:
            return None
        try:
            return int(compact)
        except ValueError:
            return None

    @staticmethod
    def _extract_money(value: str) -> tuple[float, str] | None:
        match = MONEY_RE.search(value.replace("\xa0", " "))
        if not match:
            return None
        raw_amount = match.group(1).replace(" ", "").replace(",", ".")
        try:
            amount = float(raw_amount)
        except ValueError:
            return None
        currency_raw = match.group(2).upper()
        if "RUB" in currency_raw or "РУБ" in currency_raw or "₽" in currency_raw:
            currency = "RUB"
        elif "USD" in currency_raw or "$" in currency_raw:
            currency = "USD"
        elif "EUR" in currency_raw or "€" in currency_raw:
            currency = "EUR"
        else:
            currency = currency_raw
        return amount, currency

    @staticmethod
    def _clean_lines(html: str) -> list[str]:
        tree = HTMLParser(html)
        text = tree.text(separator="\n")
        lines = [line.strip() for line in text.splitlines()]
        return [line for line in lines if line]

    @staticmethod
    def _discover_safe_endpoints(html: str) -> list[str]:
        found = {
            endpoint
            for endpoint in SAFE_ENDPOINT_RE.findall(html)
            if "/graphql" not in endpoint.lower()
        }
        return sorted(found)

    def _request(
        self,
        method: str,
        url: str,
        *,
        json_payload: dict | None = None,
        headers: dict[str, str] | None = None,
    ) -> str:
        response = self._http.request(
            method=method,
            url=url,
            json=json_payload,
            headers=headers,
            cookies=self._cookies,
        )
        self._cookies.update(response.cookies)
        return response.text

    def _graphql(
        self,
        *,
        operation_name: str,
        query: str,
        variables: dict,
        gql_path: str,
    ) -> dict:
        path = gql_path if gql_path.startswith("/") else f"/{gql_path}"
        headers = {
            "Origin": PLAYEROK_BASE_URL,
            "Referer": f"{PLAYEROK_BASE_URL}{path}",
            "x-gql-op": operation_name,
            "x-gql-path": path,
            "x-timezone-offset": str(-int(datetime.now().astimezone().utcoffset().total_seconds() // 60)),
            "Apollo-Require-Preflight": "true",
            "x-apollo-operation-name": operation_name,
            "Content-Type": "application/json",
        }
        payload = {
            "operationName": operation_name,
            "query": query,
            "variables": variables,
        }
        raw_text = self._request("POST", PLAYEROK_GRAPHQL_URL, json_payload=payload, headers=headers)
        try:
            body = json.loads(raw_text)
        except Exception as exc:
            raise RuntimeError("PlayerOK GraphQL returned non-JSON response") from exc

        errors = body.get("errors")
        if isinstance(errors, list) and errors:
            first = errors[0] if isinstance(errors[0], dict) else {"message": str(errors[0])}
            message = str(first.get("message") or "unknown graphql error")
            raise RuntimeError(f"PlayerOK GraphQL error ({operation_name}): {message}")

        data = body.get("data")
        if not isinstance(data, dict):
            raise RuntimeError(f"PlayerOK GraphQL empty data ({operation_name})")
        return data

    @staticmethod
    def _offer_from_graphql_node(node: dict, section: PlayerOkCategorySection | None = None) -> PlayerOkOfferData:
        offer_id = str(node.get("id") or "").strip()
        slug = str(node.get("slug") or "").strip()
        if not offer_id and slug:
            offer_id = slug
        offer_url = f"{PLAYEROK_BASE_URL}/products/{slug}" if slug else ""

        game = node.get("game") if isinstance(node.get("game"), dict) else {}
        category = node.get("category") if isinstance(node.get("category"), dict) else {}
        user = node.get("user") if isinstance(node.get("user"), dict) else {}

        game_id = str(game.get("id") or "").strip() or None
        game_slug = str(game.get("slug") or "").strip() or None
        game_name = str(game.get("name") or "").strip() or None

        category_id = str(category.get("id") or "").strip() or None
        category_slug = str(category.get("slug") or "").strip() or None
        category_name = str(category.get("name") or "").strip() or None

        seller_uuid = str(user.get("id") or "").strip() or None
        seller_slug = str(user.get("username") or "").strip() or None
        seller_name = seller_slug or "unknown"
        seller_url = f"{PLAYEROK_BASE_URL}/profile/{seller_slug}/products" if seller_slug else None

        description = str(node.get("name") or slug or offer_id).strip()
        raw_price = node.get("price")
        price = 0.0
        if isinstance(raw_price, (int, float)):
            price = float(raw_price)
        elif raw_price is not None:
            try:
                price = float(str(raw_price).replace(",", "."))
            except ValueError:
                price = 0.0

        section_slug = section.section_slug if section else None
        section_name = section.full_name if section else None
        if game_slug and category_slug:
            section_slug = f"{game_slug}/{category_slug}"
        if game_name and category_name:
            section_name = f"{game_name} > {category_name}"

        text_blob = normalize_text(description)
        auto_delivery = (
            "автовыдача" in text_blob
            or "auto delivery" in text_blob
            or "auto-delivery" in text_blob
        )

        reviews_count = None
        testimonial_counter = user.get("testimonialCounter")
        if isinstance(testimonial_counter, int):
            reviews_count = testimonial_counter

        return PlayerOkOfferData(
            game_id=game_id,
            game_slug=game_slug,
            game_name=game_name,
            category_id=category_id,
            category_slug=category_slug,
            section_slug=section_slug,
            section_name=section_name,
            offer_id=offer_id,
            offer_url=offer_url,
            seller_uuid=seller_uuid,
            seller_slug=seller_slug,
            seller_name=seller_name,
            seller_url=seller_url,
            description=description,
            price=price,
            currency="RUB",
            reviews_count=reviews_count,
            is_online=(bool(user.get("isOnline")) if "isOnline" in user else None),
            auto_delivery=auto_delivery,
            reviews=[],
        )

    @staticmethod
    def _to_date_bucket_from_iso(value: str | None) -> str | None:
        if not value:
            return None
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
        except Exception:
            return value
        now = datetime.now(UTC)
        if dt.year == now.year and dt.month == now.month:
            return "this month"
        return "month ago"

    def fetch_games_catalog(self, max_games: int | None = None) -> list[PlayerOkGameCategory]:
        games: list[PlayerOkGameCategory] = []
        seen: set[str] = set()
        after: str | None = None
        while True:
            variables = {
                "pagination": {
                    "first": PLAYEROK_GRAPHQL_PAGE_LIMIT,
                    "after": after,
                },
                "filter": {},
            }
            data = self._graphql(
                operation_name="games",
                query=GAMES_QUERY,
                variables=variables,
                gql_path="/games",
            )
            block = data.get("games") if isinstance(data.get("games"), dict) else {}
            edges = block.get("edges") if isinstance(block.get("edges"), list) else []
            for edge in edges:
                node = edge.get("node") if isinstance(edge, dict) and isinstance(edge.get("node"), dict) else {}
                game_id = str(node.get("id") or "").strip() or None
                slug = str(node.get("slug") or "").strip()
                name = str(node.get("name") or slug).strip()
                if not slug or slug in seen:
                    continue
                seen.add(slug)
                games.append(
                    PlayerOkGameCategory(
                        game_id=game_id,
                        game_slug=slug,
                        game_url=f"{PLAYEROK_BASE_URL}/{slug}",
                        game_name=name,
                        sections=[],
                    )
                )
                if max_games is not None and len(games) >= max_games:
                    return games

            page_info = block.get("pageInfo") if isinstance(block.get("pageInfo"), dict) else {}
            has_next = bool(page_info.get("hasNextPage"))
            if not has_next:
                break
            after = str(page_info.get("endCursor") or "").strip() or None
            if not after:
                break
        return games

    def fetch_game_categories(self, game_slug: str) -> PlayerOkGameCategory | None:
        normalized_slug = self._normalize_slug(game_slug)
        data = self._graphql(
            operation_name="gameCategories",
            query=GAME_CATEGORIES_QUERY,
            variables={"slug": normalized_slug},
            gql_path=f"/{normalized_slug}",
        )
        game = data.get("game") if isinstance(data.get("game"), dict) else None
        if not game:
            return None

        game_id = str(game.get("id") or "").strip() or None
        slug = str(game.get("slug") or normalized_slug).strip()
        name = str(game.get("name") or slug).strip()
        categories = game.get("categories") if isinstance(game.get("categories"), list) else []

        sections: list[PlayerOkCategorySection] = []
        seen_sections: set[str] = set()
        for category in categories:
            if not isinstance(category, dict):
                continue
            category_slug = str(category.get("slug") or "").strip()
            if not category_slug:
                continue
            section_slug = f"{slug}/{category_slug}"
            if section_slug in seen_sections:
                continue
            seen_sections.add(section_slug)
            section_name = str(category.get("name") or category_slug).strip()
            sections.append(
                PlayerOkCategorySection(
                    section_id=(str(category.get("id") or "").strip() or None),
                    game_id=game_id,
                    game_slug=slug,
                    game_name=name,
                    category_slug=category_slug,
                    section_slug=section_slug,
                    section_url=f"{PLAYEROK_BASE_URL}/{section_slug}",
                    section_name=section_name,
                    full_name=f"{name} > {section_name}",
                )
            )

        sections.sort(key=lambda item: item.full_name.lower())
        return PlayerOkGameCategory(
            game_id=game_id,
            game_slug=slug,
            game_url=f"{PLAYEROK_BASE_URL}/{slug}",
            game_name=name,
            sections=sections,
        )

    def get_categories_catalog(
        self,
        max_games: int | None = None,
        game_slug: str | None = None,
    ) -> list[PlayerOkGameCategory]:
        if game_slug:
            game = self.fetch_game_categories(game_slug)
            return [game] if game is not None else []
        return self.fetch_games_catalog(max_games=max_games)

    def fetch_items_for_section(
        self,
        *,
        section: PlayerOkCategorySection,
        query: str,
        max_items: int,
    ) -> PlayerOkSectionOffersResult:
        if not section.game_id or not section.section_id:
            raise RuntimeError("PlayerOK section has no game/category id for GraphQL")

        filtered_tokens = query_tokens(query)
        offers: list[PlayerOkOfferData] = []
        seen_offer_ids: set[str] = set()
        total_count: int | None = None
        after: str | None = None
        scanned_count = 0

        while scanned_count < max_items:
            page_size = min(PLAYEROK_GRAPHQL_PAGE_LIMIT, max_items - scanned_count)
            variables = {
                "pagination": {
                    "first": page_size,
                    "after": after,
                },
                "filter": {
                    "gameId": section.game_id,
                    "gameCategoryId": section.section_id,
                    "status": "APPROVED",
                },
            }
            data = self._graphql(
                operation_name="items",
                query=ITEMS_QUERY,
                variables=variables,
                gql_path=f"/{section.section_slug}",
            )
            block = data.get("items") if isinstance(data.get("items"), dict) else {}
            if total_count is None:
                count_raw = block.get("totalCount")
                if isinstance(count_raw, int):
                    total_count = count_raw

            edges = block.get("edges") if isinstance(block.get("edges"), list) else []
            if not edges:
                break
            for edge in edges:
                node = edge.get("node") if isinstance(edge, dict) and isinstance(edge.get("node"), dict) else {}
                offer = self._offer_from_graphql_node(node, section)
                if not offer.offer_id or offer.offer_id in seen_offer_ids:
                    continue
                seen_offer_ids.add(offer.offer_id)
                scanned_count += 1
                if filtered_tokens and not is_text_relevant(offer.description, filtered_tokens):
                    continue
                offers.append(offer)
                if scanned_count >= max_items:
                    break

            page_info = block.get("pageInfo") if isinstance(block.get("pageInfo"), dict) else {}
            has_next = bool(page_info.get("hasNextPage"))
            if not has_next:
                break
            after = str(page_info.get("endCursor") or "").strip() or None
            if not after:
                break

        return PlayerOkSectionOffersResult(
            section_url=section.section_url,
            section_slug=section.section_slug,
            section_name=section.full_name,
            counter_total=total_count,
            loaded_count=scanned_count,
            offers=offers,
        )

    def fetch_top_items(
        self,
        *,
        query: str,
        max_items: int,
    ) -> PlayerOkSectionOffersResult:
        filtered_tokens = query_tokens(query)
        offers: list[PlayerOkOfferData] = []
        seen_offer_ids: set[str] = set()
        total_count: int | None = None
        after: str | None = None
        scanned_count = 0

        while scanned_count < max_items:
            page_size = min(PLAYEROK_GRAPHQL_PAGE_LIMIT, max_items - scanned_count)
            variables = {
                "pagination": {
                    "first": page_size,
                    "after": after,
                },
                "filter": {},
            }
            data = self._graphql(
                operation_name="topItems",
                query=TOP_ITEMS_QUERY,
                variables=variables,
                gql_path="/",
            )
            block = data.get("topItems") if isinstance(data.get("topItems"), dict) else {}
            if total_count is None:
                count_raw = block.get("totalCount")
                if isinstance(count_raw, int):
                    total_count = count_raw

            edges = block.get("edges") if isinstance(block.get("edges"), list) else []
            if not edges:
                break
            for edge in edges:
                node = edge.get("node") if isinstance(edge, dict) and isinstance(edge.get("node"), dict) else {}
                offer = self._offer_from_graphql_node(node, None)
                if not offer.offer_id or offer.offer_id in seen_offer_ids:
                    continue
                seen_offer_ids.add(offer.offer_id)
                scanned_count += 1
                if filtered_tokens and not is_text_relevant(offer.description, filtered_tokens):
                    continue
                offers.append(offer)
                if scanned_count >= max_items:
                    break

            page_info = block.get("pageInfo") if isinstance(block.get("pageInfo"), dict) else {}
            has_next = bool(page_info.get("hasNextPage"))
            if not has_next:
                break
            after = str(page_info.get("endCursor") or "").strip() or None
            if not after:
                break

        return PlayerOkSectionOffersResult(
            section_url=f"{PLAYEROK_BASE_URL}/",
            section_slug="global/top-items",
            section_name="Global top items",
            counter_total=total_count,
            loaded_count=scanned_count,
            offers=offers,
        )

    def fetch_testimonials(
        self,
        *,
        seller_uuid: str,
        seller_slug: str,
        game_id: str,
        category_id: str,
        max_pages: int,
        per_page: int = 20,
    ) -> tuple[list[PlayerOkReviewData], int | None]:
        reviews: list[PlayerOkReviewData] = []
        total_count: int | None = None
        after: str | None = None

        for _ in range(max_pages):
            variables = {
                "pagination": {
                    "first": max(1, min(per_page, PLAYEROK_GRAPHQL_PAGE_LIMIT)),
                    "after": after,
                },
                "filter": {
                    "userId": seller_uuid,
                    "gameId": game_id,
                    "gameCategoryId": category_id,
                    "status": "APPROVED",
                },
            }
            data = self._graphql(
                operation_name="testimonials",
                query=TESTIMONIALS_QUERY,
                variables=variables,
                gql_path=f"/profile/{seller_slug}/products",
            )
            block = data.get("testimonials") if isinstance(data.get("testimonials"), dict) else {}
            if total_count is None:
                count_raw = block.get("totalCount")
                if isinstance(count_raw, int):
                    total_count = count_raw
            edges = block.get("edges") if isinstance(block.get("edges"), list) else []
            if not edges:
                break
            for edge in edges:
                node = edge.get("node") if isinstance(edge, dict) and isinstance(edge.get("node"), dict) else {}
                rating_raw = node.get("rating")
                rating = int(rating_raw) if isinstance(rating_raw, (int, float)) else None
                text = str(node.get("text") or "").strip()
                created_at = str(node.get("createdAt") or "").strip() or None
                deal = node.get("deal") if isinstance(node.get("deal"), dict) else {}
                item = deal.get("item") if isinstance(deal.get("item"), dict) else {}

                item_name = str(item.get("name") or "").strip()
                item_price_raw = item.get("price")
                item_price = None
                if isinstance(item_price_raw, (int, float)):
                    item_price = float(item_price_raw)
                elif item_price_raw is not None:
                    try:
                        item_price = float(str(item_price_raw).replace(",", "."))
                    except ValueError:
                        item_price = None

                detail = item_name
                if item_price is not None:
                    detail = f"{item_name}, {item_price:g} ₽"

                review_game = item.get("game") if isinstance(item.get("game"), dict) else {}
                review_category = item.get("category") if isinstance(item.get("category"), dict) else {}

                reviews.append(
                    PlayerOkReviewData(
                        seller_slug=seller_slug,
                        seller_uuid=seller_uuid,
                        detail=detail,
                        text=text,
                        rating=rating,
                        date_bucket=self._to_date_bucket_from_iso(created_at),
                        game_id=(str(review_game.get("id") or "").strip() or None),
                        category_id=(str(review_category.get("id") or "").strip() or None),
                        amount=item_price,
                        created_at=created_at,
                    )
                )

            page_info = block.get("pageInfo") if isinstance(block.get("pageInfo"), dict) else {}
            has_next = bool(page_info.get("hasNextPage"))
            if not has_next:
                break
            after = str(page_info.get("endCursor") or "").strip() or None
            if not after:
                break

        return reviews, total_count

    def parse_section(self, section_url: str, *, max_offers: int | None = 300) -> PlayerOkSectionParseResult:
        html = self._request("GET", section_url)
        tree = HTMLParser(html)
        lines = self._clean_lines(html)

        counter_total: int | None = None
        for line in lines:
            total_match = TOTAL_OFFERS_RE.search(line.replace("\xa0", " "))
            if total_match:
                counter_total = self._extract_int(total_match.group(1))
                if counter_total is not None:
                    break

        parsed = urlparse(section_url)
        section_match = SECTION_PATH_RE.match(parsed.path)
        section_slug = (
            f"{section_match.group(1).lower()}/{section_match.group(2).lower()}"
            if section_match
            else normalize_text(parsed.path.strip("/")).replace(" ", "-")
        )
        title_node = tree.css_first("h1")
        section_name = title_node.text(strip=True) if title_node else section_slug

        offers: list[PlayerOkOfferPreview] = []
        seen_ids: set[str] = set()
        for anchor in tree.css("a[href]"):
            href = (anchor.attributes.get("href") or "").strip()
            if not href.startswith("/products/"):
                continue
            offer_url = self._to_abs_url(href)
            offer_id = self._offer_id_from_url(offer_url)
            if not offer_id or offer_id in seen_ids:
                continue
            description = anchor.text(strip=True)
            if not description:
                continue
            seen_ids.add(offer_id)
            offers.append(
                PlayerOkOfferPreview(
                    offer_id=offer_id,
                    offer_url=offer_url,
                    description=description,
                )
            )
            if max_offers is not None and len(offers) >= max_offers:
                break

        return PlayerOkSectionParseResult(
            section_url=section_url,
            section_slug=section_slug,
            section_name=section_name,
            counter_total=counter_total,
            loaded_count=len(offers),
            offers=offers,
            safe_endpoints=self._discover_safe_endpoints(html),
        )

    @staticmethod
    def _extract_profile_info(tree: HTMLParser) -> tuple[str | None, str | None, str]:
        for anchor in tree.css("a[href]"):
            href = (anchor.attributes.get("href") or "").strip()
            match = PROFILE_PRODUCTS_RE.match(href)
            if not match:
                continue
            seller_slug = match.group(1)
            seller_url = urljoin(PLAYEROK_BASE_URL, href)
            seller_name_raw = anchor.text(strip=True)
            seller_name = REVIEW_COUNT_RE.sub("", seller_name_raw).strip()
            seller_name = re.sub(r"\b\d+(?:\.\d+)?\b", "", seller_name).strip()
            if not seller_name:
                seller_name = seller_slug
            return seller_slug, seller_url, seller_name
        return None, None, "unknown"

    @staticmethod
    def _extract_section_from_offer(tree: HTMLParser) -> tuple[str | None, str | None]:
        for anchor in tree.css("a[href]"):
            href = (anchor.attributes.get("href") or "").strip()
            match = SECTION_PATH_RE.match(href)
            if not match:
                continue
            game_slug = match.group(1).lower()
            section_slug = match.group(2).lower()
            full_slug = f"{game_slug}/{section_slug}"
            section_name = anchor.text(strip=True) or full_slug
            return full_slug, section_name
        return None, None

    @staticmethod
    def _to_date_bucket(value: str) -> str | None:
        normalized = normalize_text(value)
        if not normalized:
            return None
        recent_markers = (
            "this month",
            "в этом месяце",
            "сегодня",
            "вчера",
            "minutes ago",
            "minute ago",
            "hours ago",
            "hour ago",
            "дня назад",
            "дней назад",
            "неделю назад",
            "недели назад",
        )
        if any(marker in normalized for marker in recent_markers):
            return "this month"
        if "month ago" in normalized or "месяц назад" in normalized:
            return "month ago"
        return value.strip() or None

    @staticmethod
    def _extract_reviews(lines: list[str], seller_slug: str) -> list[PlayerOkReviewData]:
        start_idx = -1
        for idx, line in enumerate(lines):
            normalized = normalize_text(line)
            if normalized.startswith("отзывы о") or normalized.startswith("reviews about"):
                start_idx = idx
                break
        if start_idx < 0:
            return []

        end_idx = len(lines)
        stop_markers = (
            "все отзывы оставлены после покупки",
            "другие товары продавца",
            "похожие товары",
            "similar offers",
        )
        for idx in range(start_idx + 1, len(lines)):
            normalized = normalize_text(lines[idx])
            if any(marker in normalized for marker in stop_markers):
                end_idx = idx
                break

        reviews: list[PlayerOkReviewData] = []
        last_date: str | None = None
        for idx in range(start_idx + 1, end_idx):
            line = lines[idx]
            normalized = normalize_text(line)
            if not normalized:
                continue
            if any(
                token in normalized
                for token in ("сегодня", "вчера", "this month", "в этом месяце", "month ago")
            ):
                last_date = line
                continue
            money = MONEY_RE.search(line.replace("\xa0", " "))
            if not money:
                continue
            if len(normalized.split()) < 2:
                continue
            feedback = ""
            if idx + 1 < end_idx:
                next_line = lines[idx + 1]
                next_normalized = normalize_text(next_line)
                if next_normalized and not MONEY_RE.search(next_line) and "отзыв" not in next_normalized:
                    feedback = next_line
            amount = None
            extracted = PlayerOkClient._extract_money(line)
            if extracted:
                amount = extracted[0]
            reviews.append(
                PlayerOkReviewData(
                    seller_slug=seller_slug,
                    seller_uuid=None,
                    detail=line,
                    text=feedback,
                    rating=None,
                    date_bucket=PlayerOkClient._to_date_bucket(last_date or ""),
                    game_id=None,
                    category_id=None,
                    amount=amount,
                    created_at=None,
                )
            )
        return reviews

    def parse_offer(self, offer_url: str) -> PlayerOkOfferData:
        html = self._request("GET", offer_url)
        tree = HTMLParser(html)
        lines = self._clean_lines(html)

        offer_id = self._offer_id_from_url(offer_url) or normalize_text(offer_url).replace(" ", "-")
        title_node = tree.css_first("h1")
        description = title_node.text(strip=True) if title_node else offer_id

        price = 0.0
        currency = "RUB"
        for line in lines[:160]:
            parsed_money = self._extract_money(line)
            if not parsed_money:
                continue
            price, currency = parsed_money
            break

        reviews_count: int | None = None
        for line in lines[:220]:
            match = REVIEW_COUNT_RE.search(line)
            if not match:
                continue
            value = self._extract_int(match.group(1))
            if value is None:
                continue
            reviews_count = max(reviews_count or 0, value)

        seller_slug, seller_url, seller_name = self._extract_profile_info(tree)
        section_slug, section_name = self._extract_section_from_offer(tree)

        blob = normalize_text("\n".join(lines[:260]))
        auto_delivery = ("автовыдача" in blob) or ("auto delivery" in blob) or ("auto-delivery" in blob)
        reviews = self._extract_reviews(lines, seller_slug or "unknown")

        game_slug = None
        category_slug = None
        if section_slug and "/" in section_slug:
            game_slug, category_slug = section_slug.split("/", 1)

        return PlayerOkOfferData(
            game_id=None,
            game_slug=game_slug,
            game_name=None,
            category_id=None,
            category_slug=category_slug,
            section_slug=section_slug,
            section_name=section_name,
            offer_id=offer_id,
            offer_url=offer_url,
            seller_uuid=None,
            seller_slug=seller_slug,
            seller_name=seller_name,
            seller_url=seller_url,
            description=description,
            price=price,
            currency=currency,
            reviews_count=reviews_count,
            is_online=None,
            auto_delivery=auto_delivery,
            reviews=reviews,
        )
