from __future__ import annotations

import hashlib
import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.models import AnalysisCache
from app.schemas.analyze import AnalyzeOptionsDTO
from app.schemas.v2 import (
    CommonFiltersDTO,
    CoverageV2DTO,
    DemandStatsV2DTO,
    MarketplaceCoreDTO,
    MarketplaceOffersResponseDTO,
    MarketplaceRunResultDTO,
    MarketplaceSlug,
    MarketplaceSummaryDTO,
    NormalizedOfferDTO,
    NormalizedReviewDTO,
    NormalizedSellerDTO,
    OffersStatsV2DTO,
    PlayerOkCategoryGameDTO,
    PlayerOkCategorySectionDTO,
)
from app.services.i18n import tr
from app.services.marketplaces.base import MarketplaceProvider
from app.services.network_settings import NetworkSettingsService
from app.services.playerok_client import (
    PlayerOkCategorySection,
    PlayerOkClient,
    PlayerOkGameCategory,
    PlayerOkOfferData,
    PlayerOkReviewData,
)
from app.services.proxy_utils import normalize_proxy_list
from app.services.text_utils import (
    is_text_relevant,
    meaningful_query_tokens,
    normalize_text,
    query_tokens,
    tokenize,
)

AMOUNT_RE = re.compile(
    r"(\d+(?:[.,]\d+)?)\s*(?:₽|руб(?:\.|лей|ля)?|rub|usd|\$|eur|€)?",
    flags=re.IGNORECASE,
)


@dataclass
class _EffectiveOptions:
    profile: str
    include_reviews: bool
    include_demand_index: bool
    include_fallback_scan: bool
    section_limit: int
    seller_limit: int
    review_pages_per_seller: int
    history_points_limit: int
    mode_label: str
    options_hash: str


@dataclass
class _ReviewDiagnostics:
    sellers_analyzed: int = 0
    reviews_scanned: int = 0
    failed_sellers: int = 0
    no_amount: int = 0
    no_game_match: int = 0
    no_price_match: int = 0


@dataclass
class _SellerDemandStat:
    seller_slug: str
    seller_name: str
    estimated_purchases_total: int
    estimated_purchases_30d: int
    reviews_scanned: int


class PlayerOkProvider(MarketplaceProvider):
    slug = MarketplaceSlug.playerok
    label = "PlayerOK"
    CACHE_MODE = "playerok_v2"
    _categories_cache: list[PlayerOkCategoryGameDTO] | None = None
    _categories_cache_expires_at: datetime | None = None

    REVIEW_TOKEN_STOPWORDS = {
        "playerok",
        "маркетплейс",
        "быстро",
        "надежно",
        "надежный",
        "гарантия",
        "моментально",
        "аккаунт",
        "аренда",
        "день",
        "дней",
        "часа",
        "часов",
        "лучший",
        "best",
        "rent",
        "hour",
        "hours",
        "days",
    }

    def __init__(self, db: Session, settings: Settings) -> None:
        self.db = db
        self.settings = settings
        self.network_settings = NetworkSettingsService(db=db, settings=settings)

    @staticmethod
    def _to_marketplace_filters(raw_filters: dict | None) -> dict:
        if not isinstance(raw_filters, dict):
            return {}
        return raw_filters

    @staticmethod
    def _normalize_slug(value: str) -> str:
        slug = value.strip().strip("/")
        return normalize_text(slug).replace(" ", "-")

    @staticmethod
    def _normalize_section_slug(value: str) -> str:
        slug = value.strip().strip("/")
        if "/" in slug:
            parts = [normalize_text(part).replace(" ", "-") for part in slug.split("/") if part.strip()]
            return "/".join(parts)
        return normalize_text(slug).replace(" ", "-")

    @staticmethod
    def _normalize_str_map(raw: object) -> dict[str, str]:
        if not isinstance(raw, dict):
            return {}
        normalized: dict[str, str] = {}
        for key, value in raw.items():
            k = str(key or "").strip()
            v = str(value or "").strip()
            if k and v:
                normalized[k] = v
        return normalized

    @staticmethod
    def _hash_map(data: dict[str, str]) -> str:
        if not data:
            return "none"
        payload = json.dumps(data, sort_keys=True, ensure_ascii=True)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    @staticmethod
    def _mask_map(data: dict[str, str]) -> dict[str, str]:
        if not data:
            return {}
        return {key: "***" for key in sorted(data)}

    def _resolve_proxy_pool(self, filters: dict, common_filters: CommonFiltersDTO):
        resolved = self.network_settings.resolve(
            common_filters=common_filters,
            marketplace_filters=filters,
        )
        allow_direct = bool(common_filters.allow_direct_fallback)
        self.network_settings.ensure_proxy_policy(resolved, allow_direct_fallback=allow_direct)
        return resolved, allow_direct

    def _build_client(
        self,
        filters: dict,
        common_filters: CommonFiltersDTO,
        *,
        advanced_headers: dict[str, str],
        advanced_cookies: dict[str, str],
        use_html_degrade: bool,
    ) -> PlayerOkClient:
        resolved, allow_direct = self._resolve_proxy_pool(filters, common_filters)
        return PlayerOkClient(
            settings=self.settings,
            datacenter_proxies=normalize_proxy_list(resolved.datacenter),
            residential_proxies=normalize_proxy_list(resolved.residential),
            mobile_proxies=normalize_proxy_list(resolved.mobile),
            advanced_headers=advanced_headers,
            advanced_cookies=advanced_cookies,
            use_html_degrade=use_html_degrade,
            allow_direct_fallback=allow_direct,
        )

    def _resolve_options(
        self,
        raw_options: dict | AnalyzeOptionsDTO | None,
        *,
        category_game_slug: str | None,
        category_slugs: list[str],
        html_degrade_enabled: bool,
    ) -> _EffectiveOptions:
        if isinstance(raw_options, AnalyzeOptionsDTO):
            raw = raw_options
        else:
            raw = AnalyzeOptionsDTO.model_validate(raw_options or {})

        default_seller_limit = 3
        profile = raw.profile.value
        if profile == "safe":
            include_reviews = False
            include_demand_index = False
            include_fallback_scan = False
            section_limit = min(self.settings.quick_sections_limit, 40)
            seller_limit = default_seller_limit
            review_pages_per_seller = 1
            history_points_limit = 30
        elif profile == "deep":
            include_reviews = True
            include_demand_index = True
            include_fallback_scan = True
            section_limit = max(self.settings.fallback_sections_limit, self.settings.quick_sections_limit)
            seller_limit = default_seller_limit
            review_pages_per_seller = max(self.settings.review_max_pages_per_seller, 8)
            history_points_limit = 120
        else:
            include_reviews = False
            include_demand_index = False
            include_fallback_scan = True
            section_limit = self.settings.quick_sections_limit
            seller_limit = default_seller_limit
            review_pages_per_seller = self.settings.review_max_pages_per_seller
            history_points_limit = 60

        if raw.include_reviews is not None:
            include_reviews = raw.include_reviews
        if raw.include_demand_index is not None:
            include_demand_index = raw.include_demand_index
        if raw.include_fallback_scan is not None:
            include_fallback_scan = raw.include_fallback_scan
        if raw.section_limit is not None:
            section_limit = raw.section_limit
        if raw.seller_limit is not None:
            seller_limit = raw.seller_limit
        if raw.review_pages_per_seller is not None:
            review_pages_per_seller = raw.review_pages_per_seller
        if raw.history_points_limit is not None:
            history_points_limit = raw.history_points_limit

        include_fallback_scan = include_fallback_scan and html_degrade_enabled
        seller_limit = max(1, min(20, seller_limit))
        if include_demand_index:
            include_reviews = True
        if not include_reviews:
            include_demand_index = False
        mode_label = "demand" if include_reviews else "search"

        payload = {
            "profile": profile,
            "include_reviews": include_reviews,
            "include_demand_index": include_demand_index,
            "include_fallback_scan": include_fallback_scan,
            "section_limit": section_limit,
            "seller_limit": seller_limit,
            "review_pages_per_seller": review_pages_per_seller,
            "history_points_limit": history_points_limit,
            "mode_label": mode_label,
            "category_game_slug": category_game_slug,
            "category_slugs": sorted(set(category_slugs)),
            "html_degrade_enabled": html_degrade_enabled,
        }
        options_hash = hashlib.sha256(
            json.dumps(payload, sort_keys=True, ensure_ascii=True).encode("utf-8")
        ).hexdigest()
        return _EffectiveOptions(
            profile=profile,
            include_reviews=include_reviews,
            include_demand_index=include_demand_index,
            include_fallback_scan=include_fallback_scan,
            section_limit=section_limit,
            seller_limit=seller_limit,
            review_pages_per_seller=review_pages_per_seller,
            history_points_limit=history_points_limit,
            mode_label=mode_label,
            options_hash=options_hash,
        )

    @staticmethod
    def _percentile(values: list[float], percentile: float) -> float | None:
        clean = [float(item) for item in values if item is not None]
        if not clean:
            return None
        ordered = sorted(clean)
        if len(ordered) == 1:
            return ordered[0]
        rank = (len(ordered) - 1) * percentile
        lower_idx = int(rank)
        upper_idx = min(lower_idx + 1, len(ordered) - 1)
        fraction = rank - lower_idx
        return round(ordered[lower_idx] + (ordered[upper_idx] - ordered[lower_idx]) * fraction, 6)

    @classmethod
    def _offers_stats(cls, offers: list[PlayerOkOfferData]) -> OffersStatsV2DTO:
        prices = [float(item.price) for item in offers if item.price is not None]
        unique_sellers = {item.seller_slug or item.seller_uuid for item in offers if item.seller_slug or item.seller_uuid}
        online_known = [item for item in offers if item.is_online is not None]
        auto_known = [item for item in offers if item.auto_delivery is not None]
        return OffersStatsV2DTO(
            matched_offers=len(offers),
            unique_sellers=len(unique_sellers),
            min_price=min(prices) if prices else None,
            avg_price=(round(sum(prices) / len(prices), 6) if prices else None),
            p50_price=cls._percentile(prices, 0.50),
            p90_price=cls._percentile(prices, 0.90),
            max_price=max(prices) if prices else None,
            online_share=(
                round(sum(1 for item in online_known if item.is_online) / len(online_known), 4)
                if online_known
                else None
            ),
            auto_delivery_share=(
                round(sum(1 for item in auto_known if item.auto_delivery) / len(auto_known), 4)
                if auto_known
                else None
            ),
        )

    @staticmethod
    def _normalize_currency(raw: str | None) -> str | None:
        if not raw:
            return None
        normalized = raw.strip().upper()
        if normalized in {"RUB", "₽", "РУБ"}:
            return "RUB"
        if normalized in {"USD", "$"}:
            return "USD"
        if normalized in {"EUR", "€"}:
            return "EUR"
        return normalized

    def _filter_offers_by_currency(
        self,
        offers: list[PlayerOkOfferData],
        requested_currency: str,
    ) -> tuple[list[PlayerOkOfferData], int, bool]:
        filtered: list[PlayerOkOfferData] = []
        excluded = 0
        for offer in offers:
            offer_currency = self._normalize_currency(offer.currency)
            if offer_currency is None:
                offer.currency = requested_currency
                filtered.append(offer)
                continue
            if offer_currency == requested_currency:
                filtered.append(offer)
                continue
            excluded += 1
        if filtered:
            return filtered, excluded, False
        if offers and excluded > 0:
            return offers, excluded, True
        return filtered, excluded, False

    @staticmethod
    def _extract_amounts_from_text(value: str) -> list[float]:
        if not value:
            return []
        matches = AMOUNT_RE.findall(value.replace("\xa0", " ").replace(",", "."))
        result: list[float] = []
        for amount_raw in matches:
            try:
                result.append(float(amount_raw))
            except ValueError:
                continue
        return result

    @staticmethod
    def _is_amount_close(amount: float, prices: list[float]) -> bool:
        for price in prices:
            if price <= 0:
                continue
            if abs(amount - price) <= max(2.0, price * 0.40):
                return True
        return False

    @classmethod
    def _derive_review_tokens_from_offers(cls, offers: list[PlayerOkOfferData]) -> list[str]:
        token_counter: Counter[str] = Counter()
        for offer in offers:
            for token in set(tokenize(offer.description)):
                if len(token) < 3:
                    continue
                if token in cls.REVIEW_TOKEN_STOPWORDS:
                    continue
                if token.isdigit():
                    continue
                token_counter[token] += 1
        return [token for token, _ in token_counter.most_common(8)]

    @staticmethod
    def _is_this_month_bucket(value: str | None) -> bool:
        normalized = normalize_text(value or "")
        if not normalized:
            return False
        return normalized in {"this month", "в этом месяце"} or "вчера" in normalized or "сегодня" in normalized

    def _is_review_match(
        self,
        review: PlayerOkReviewData,
        *,
        seller_offers: list[PlayerOkOfferData],
        category_tokens: list[str],
    ) -> tuple[bool, str | None]:
        offer_game_ids = {item.game_id for item in seller_offers if item.game_id}
        offer_category_ids = {item.category_id for item in seller_offers if item.category_id}

        game_match = False
        if review.game_id and review.game_id in offer_game_ids:
            if review.category_id is None or not offer_category_ids or review.category_id in offer_category_ids:
                game_match = True

        detail_tokens = set(tokenize(review.detail))
        if not game_match and detail_tokens:
            seller_tokens = set(self._derive_review_tokens_from_offers(seller_offers))
            game_tokens = sorted(seller_tokens | set(category_tokens))
            if any(token in detail_tokens for token in game_tokens):
                game_match = True

        if not game_match:
            return False, "no_game_match"

        amounts = []
        if review.amount is not None:
            amounts.append(float(review.amount))
        amounts.extend(self._extract_amounts_from_text(review.detail))
        if not amounts:
            return False, "no_amount"

        prices = [float(item.price) for item in seller_offers if item.price and item.price > 0]
        if not prices:
            return False, "no_price_match"
        for amount in amounts:
            if self._is_amount_close(amount, prices):
                return True, None
        return False, "no_price_match"

    @staticmethod
    def _review_signature(review: PlayerOkReviewData) -> tuple[str, str, str]:
        return (
            normalize_text(review.detail),
            normalize_text(review.text),
            normalize_text(review.created_at or review.date_bucket or ""),
        )

    @classmethod
    def _dedupe_reviews(cls, reviews: list[PlayerOkReviewData]) -> list[PlayerOkReviewData]:
        unique: list[PlayerOkReviewData] = []
        seen: set[tuple[str, str, str]] = set()
        for review in reviews:
            signature = cls._review_signature(review)
            if signature in seen:
                continue
            seen.add(signature)
            unique.append(review)
        return unique

    @classmethod
    def _compute_demand_stats(
        cls,
        relevant_reviews: list[PlayerOkReviewData],
        *,
        include_index: bool,
        sellers_analyzed: int,
        reviews_scanned: int,
    ) -> DemandStatsV2DTO:
        if not relevant_reviews:
            return DemandStatsV2DTO(
                relevant_reviews=0,
                positive_share=0.0,
                volume_30d=0,
                demand_index=(0.0 if include_index else None),
                unique_sellers_with_relevant_reviews=0,
                estimated_purchases_total=0,
                estimated_purchases_30d=0,
                sellers_analyzed=sellers_analyzed,
                reviews_scanned=reviews_scanned,
            )
        rated_reviews = [item for item in relevant_reviews if item.rating is not None]
        positives = [item for item in rated_reviews if (item.rating or 0) >= 4]
        positive_share = len(positives) / len(rated_reviews) if rated_reviews else 0.0
        volume_30d = sum(1 for item in relevant_reviews if cls._is_this_month_bucket(item.date_bucket))
        unique_sellers = len({item.seller_slug for item in relevant_reviews if item.seller_slug})
        demand_index: float | None = None
        if include_index:
            demand_index = round((min(volume_30d, 100) / 100 * 60) + (positive_share * 40), 4)
        return DemandStatsV2DTO(
            relevant_reviews=len(relevant_reviews),
            positive_share=round(positive_share, 4),
            volume_30d=volume_30d,
            demand_index=demand_index,
            unique_sellers_with_relevant_reviews=unique_sellers,
            estimated_purchases_total=len(relevant_reviews),
            estimated_purchases_30d=volume_30d,
            sellers_analyzed=sellers_analyzed,
            reviews_scanned=reviews_scanned,
        )

    @staticmethod
    def _coverage_status(counter_total: int | None, loaded_count: int) -> str:
        if counter_total is not None and counter_total > loaded_count:
            return "lower_bound"
        return "full"

    @staticmethod
    def _compute_competition_metrics(offers: list[PlayerOkOfferData]) -> dict[str, float | None]:
        if not offers:
            return {"hhi": None, "top3_share": None, "price_spread": None}
        seller_counts: Counter[str] = Counter(
            item.seller_slug or item.seller_uuid or f"n:{item.seller_name}" for item in offers
        )
        total = len(offers)
        shares = [count / total for count in seller_counts.values() if count > 0]
        hhi = sum((share * 100) ** 2 for share in shares)
        top3_share = sum(sorted(seller_counts.values(), reverse=True)[:3]) / total
        prices = [float(item.price) for item in offers if item.price is not None]
        p10 = PlayerOkProvider._percentile(prices, 0.10)
        p90 = PlayerOkProvider._percentile(prices, 0.90)
        p50 = PlayerOkProvider._percentile(prices, 0.50)
        spread = None
        if p10 is not None and p90 is not None and p50 is not None and p50 > 0:
            spread = round((p90 - p10) / p50, 6)
        return {"hhi": round(hhi, 6), "top3_share": round(top3_share, 6), "price_spread": spread}

    @staticmethod
    def _cache_key(
        *,
        query: str,
        currency: str,
        ui_locale: str,
        options_hash: str,
        category_game_slug: str | None,
        category_slugs: list[str],
        use_html_degrade: bool,
        advanced_headers_hash: str,
        advanced_cookies_hash: str,
    ) -> str:
        payload = {
            "v": "playerok-v2",
            "query": normalize_text(query),
            "currency": currency,
            "ui_locale": ui_locale,
            "options_hash": options_hash,
            "category_game_slug": category_game_slug,
            "category_slugs": sorted(set(category_slugs)),
            "use_html_degrade": use_html_degrade,
            "advanced_headers_hash": advanced_headers_hash,
            "advanced_cookies_hash": advanced_cookies_hash,
        }
        return hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=True).encode("utf-8")).hexdigest()

    def _load_cache(self, cache_key: str) -> MarketplaceRunResultDTO | None:
        now = datetime.now(UTC)
        cached = self.db.scalar(
            select(AnalysisCache).where(
                AnalysisCache.cache_key_hash == cache_key,
                AnalysisCache.mode == self.CACHE_MODE,
                AnalysisCache.valid_until > now,
            )
        )
        if not cached:
            return None
        payload = cached.response_json if isinstance(cached.response_json, dict) else None
        if payload is None:
            return None
        try:
            result = MarketplaceRunResultDTO.model_validate(payload)
        except Exception:
            return None
        result.summary.cache_hit = True
        return result

    def _persist_cache(
        self,
        *,
        cache_key: str,
        query: str,
        currency: str,
        generated_at: datetime,
        valid_until: datetime,
        result: MarketplaceRunResultDTO,
    ) -> None:
        existing = self.db.scalar(
            select(AnalysisCache).where(
                AnalysisCache.cache_key_hash == cache_key,
                AnalysisCache.mode == self.CACHE_MODE,
            )
        )
        payload = result.model_dump(mode="json")
        if existing is not None:
            existing.query = query
            existing.currency = currency
            existing.mode = self.CACHE_MODE
            existing.generated_at = generated_at
            existing.valid_until = valid_until
            existing.response_json = payload
            self.db.add(existing)
            self.db.commit()
            return
        self.db.add(
            AnalysisCache(
                cache_key_hash=cache_key,
                query=query,
                mode=self.CACHE_MODE,
                currency=currency,
                generated_at=generated_at,
                valid_until=valid_until,
                response_json=payload,
            )
        )
        self.db.commit()

    @staticmethod
    def _synthetic_section(section_slug: str) -> PlayerOkCategorySection:
        normalized = section_slug.strip().strip("/")
        if "/" in normalized:
            game_slug, category_slug = normalized.split("/", 1)
            game_name = game_slug
            full_name = f"{game_name} > {category_slug}"
            section_url = f"https://playerok.com/{normalized}"
        else:
            game_slug = "categories"
            category_slug = normalized
            game_name = "categories"
            full_name = f"categories > {category_slug}"
            section_url = f"https://playerok.com/categories/{category_slug}"
            normalized = f"categories/{category_slug}"
        return PlayerOkCategorySection(
            section_id=None,
            game_id=None,
            game_slug=game_slug,
            game_name=game_name,
            category_slug=category_slug,
            section_slug=normalized,
            section_url=section_url,
            section_name=category_slug,
            full_name=full_name,
        )

    def _resolve_scope(
        self,
        *,
        client: PlayerOkClient,
        category_game_slug: str | None,
        category_slugs: list[str],
        use_game_scope: bool,
        query: str,
        section_limit: int,
    ) -> tuple[list[PlayerOkCategorySection], dict[str, str], dict[str, str]]:
        game_name_by_slug: dict[str, str] = {}
        game_by_slug: dict[str, PlayerOkGameCategory] = {}

        required_game_slugs: set[str] = set()
        if category_game_slug:
            required_game_slugs.add(category_game_slug)
        for slug in category_slugs:
            if "/" in slug:
                required_game_slugs.add(slug.split("/", 1)[0])
        if use_game_scope and category_game_slug:
            required_game_slugs.add(category_game_slug)

        section_by_slug: dict[str, PlayerOkCategorySection] = {}
        for game_slug in sorted(required_game_slugs):
            try:
                loaded = client.get_categories_catalog(game_slug=game_slug)
            except Exception:
                continue
            if not loaded:
                continue
            game = loaded[0]
            game_by_slug[game.game_slug] = game
            game_name_by_slug[game.game_slug] = game.game_name
            for section in game.sections:
                section_by_slug[section.section_slug] = section

        selected_map: dict[str, PlayerOkCategorySection] = {}
        if category_game_slug and use_game_scope:
            game = game_by_slug.get(category_game_slug)
            if game is not None:
                for section in game.sections:
                    selected_map[section.section_slug] = section

        for raw_slug in category_slugs:
            slug = self._normalize_section_slug(raw_slug)
            if not slug:
                continue
            if "/" in slug:
                selected_map[slug] = section_by_slug.get(slug) or self._synthetic_section(slug)
                continue

            if category_game_slug:
                full_slug = f"{category_game_slug}/{slug}"
                selected_map[full_slug] = section_by_slug.get(full_slug) or self._synthetic_section(full_slug)
                continue

            matched = [item for key, item in section_by_slug.items() if key.endswith(f"/{slug}")]
            if matched:
                for item in matched:
                    selected_map[item.section_slug] = item
            else:
                selected_map[f"categories/{slug}"] = self._synthetic_section(f"categories/{slug}")

        query_value = query.strip()
        if not selected_map and query_value and category_game_slug:
            game = game_by_slug.get(category_game_slug)
            if game is not None:
                qtokens = query_tokens(query_value)
                for section in game.sections:
                    if is_text_relevant(section.full_name, qtokens):
                        selected_map[section.section_slug] = section

        selected = list(selected_map.values())
        selected.sort(key=lambda item: item.full_name.lower())
        selected = selected[: max(1, section_limit)]
        section_name_map = {section.section_slug: section.full_name for section in section_by_slug.values()}
        return selected, game_name_by_slug, section_name_map

    def analyze(self, common_filters: CommonFiltersDTO, marketplace_filters: dict | None) -> MarketplaceRunResultDTO:
        filters = self._to_marketplace_filters(marketplace_filters)
        category_game_slug_raw = filters.get("category_game_slug")
        category_game_slug = (
            self._normalize_slug(category_game_slug_raw)
            if isinstance(category_game_slug_raw, str) and category_game_slug_raw.strip()
            else None
        )
        category_slugs_raw = filters.get("category_slugs") if isinstance(filters.get("category_slugs"), list) else []
        category_slugs = [
            self._normalize_section_slug(item)
            for item in category_slugs_raw
            if isinstance(item, str) and item.strip()
        ]
        use_game_scope = bool(filters.get("use_game_scope", True))
        use_html_degrade = bool(filters.get("use_html_degrade", True))
        advanced_headers = self._normalize_str_map(filters.get("advanced_headers"))
        advanced_cookies = self._normalize_str_map(filters.get("advanced_cookies"))

        options = self._resolve_options(
            filters.get("options"),
            category_game_slug=category_game_slug,
            category_slugs=category_slugs,
            html_degrade_enabled=use_html_degrade,
        )
        cache_key = self._cache_key(
            query=common_filters.query,
            currency=common_filters.currency.value,
            ui_locale=common_filters.ui_locale.value,
            options_hash=options.options_hash,
            category_game_slug=category_game_slug,
            category_slugs=category_slugs,
            use_html_degrade=use_html_degrade,
            advanced_headers_hash=self._hash_map(advanced_headers),
            advanced_cookies_hash=self._hash_map(advanced_cookies),
        )
        if not common_filters.force_refresh:
            cached = self._load_cache(cache_key)
            if cached is not None:
                return cached

        client = self._build_client(
            filters,
            common_filters,
            advanced_headers=advanced_headers,
            advanced_cookies=advanced_cookies,
            use_html_degrade=use_html_degrade,
        )

        generated_at = datetime.now(UTC)
        valid_until = generated_at + timedelta(hours=self.settings.cache_ttl_hours)
        warnings: list[str] = []
        section_rows: list[dict] = []
        query_value = common_filters.query.strip()

        selected_sections, game_name_by_slug, section_name_map = self._resolve_scope(
            client=client,
            category_game_slug=category_game_slug,
            category_slugs=category_slugs,
            use_game_scope=use_game_scope,
            query=query_value,
            section_limit=options.section_limit,
        )

        offers: list[PlayerOkOfferData] = []
        source_stats = {"graphql": 0, "html_degrade": 0, "graphql_failed": 0}
        max_total_offers = max(120, options.section_limit * 40)
        max_per_section = max(40, min(300, (max_total_offers // max(1, len(selected_sections))) + 20))

        for section in selected_sections:
            graph_error: str | None = None
            section_source = "graphql"
            section_offers: list[PlayerOkOfferData] = []
            section_total: int | None = None
            section_loaded_count = 0
            try:
                graph_result = client.fetch_items_for_section(
                    section=section,
                    query=query_value,
                    max_items=max_per_section,
                )
                section_offers = graph_result.offers
                section_total = graph_result.counter_total
                section_loaded_count = graph_result.loaded_count
                source_stats["graphql"] += 1
            except Exception as exc:
                graph_error = str(exc)
                source_stats["graphql_failed"] += 1

            if graph_error and options.include_fallback_scan:
                try:
                    parsed = client.parse_section(section.section_url, max_offers=max_per_section)
                    section_loaded_count = parsed.loaded_count
                    loaded_offers: list[PlayerOkOfferData] = []
                    for preview in parsed.offers:
                        try:
                            offer = client.parse_offer(preview.offer_url)
                        except Exception:
                            continue
                        if offer.section_slug is None:
                            offer.section_slug = section.section_slug
                        if offer.section_name is None:
                            offer.section_name = section.full_name
                        if query_value and not is_text_relevant(offer.description, query_tokens(query_value)):
                            continue
                        loaded_offers.append(offer)
                    section_offers = loaded_offers
                    section_total = parsed.counter_total
                    section_source = "html_degrade"
                    source_stats["html_degrade"] += 1
                except Exception:
                    section_offers = []
                    section_loaded_count = 0

            if graph_error and section_loaded_count == 0:
                warnings.append(
                    tr(
                        common_filters.ui_locale.value,
                        "warning.playerok.graphql_section_failed",
                        section=section.full_name,
                    )
                )
                continue

            coverage_status = self._coverage_status(section_total, section_loaded_count)
            section_rows.append(
                {
                    "section_url": section.section_url,
                    "section_id": section.section_slug,
                    "section_name": section.full_name,
                    "counter_total": section_total,
                    "loaded_count": section_loaded_count,
                    "coverage_status": coverage_status,
                    "source": section_source,
                }
            )
            offers.extend(section_offers)

        if not selected_sections and query_value:
            try:
                top_items = client.fetch_top_items(query=query_value, max_items=max_total_offers)
                section_rows.append(
                    {
                        "section_url": top_items.section_url,
                        "section_id": top_items.section_slug,
                        "section_name": top_items.section_name,
                        "counter_total": top_items.counter_total,
                        "loaded_count": top_items.loaded_count,
                        "coverage_status": self._coverage_status(top_items.counter_total, top_items.loaded_count),
                        "source": "graphql",
                    }
                )
                offers.extend(top_items.offers)
                source_stats["graphql"] += 1
            except Exception:
                warnings.append(tr(common_filters.ui_locale.value, "warning.playerok.graphql_top_items_failed"))

        deduped: dict[str, PlayerOkOfferData] = {}
        for item in offers:
            key = item.offer_id or item.offer_url
            if not key:
                continue
            deduped[key] = item
        offers = list(deduped.values())

        offers, excluded_currency, relaxed_currency = self._filter_offers_by_currency(
            offers,
            common_filters.currency.value,
        )
        if excluded_currency > 0 and relaxed_currency:
            warnings.append(
                tr(
                    common_filters.ui_locale.value,
                    "warning.currency.relaxed",
                    currency=common_filters.currency.value,
                    count=excluded_currency,
                )
            )
        elif excluded_currency > 0:
            warnings.append(
                tr(
                    common_filters.ui_locale.value,
                    "warning.currency.filtered",
                    currency=common_filters.currency.value,
                    count=excluded_currency,
                )
            )

        stats = self._offers_stats(offers)
        lower_bound_sections = sum(1 for row in section_rows if row["coverage_status"] == "lower_bound")
        coverage_note = tr(common_filters.ui_locale.value, "warning.coverage.lower_bound") if lower_bound_sections else None
        if coverage_note:
            warnings.append(coverage_note)
        if source_stats["html_degrade"] > 0:
            warnings.append(
                tr(
                    common_filters.ui_locale.value,
                    "warning.playerok.html_degrade_used",
                    count=source_stats["html_degrade"],
                )
            )
        if source_stats["graphql_failed"] > 0:
            warnings.append(
                tr(
                    common_filters.ui_locale.value,
                    "warning.playerok.graphql_failed_count",
                    count=source_stats["graphql_failed"],
                )
            )
        if not offers:
            warnings.append(
                tr(
                    common_filters.ui_locale.value,
                    "warning.offers.none_query" if query_value else "warning.offers.none_scope",
                )
            )

        seller_offers: dict[str, list[PlayerOkOfferData]] = defaultdict(list)
        seller_meta: dict[str, dict[str, str | None]] = {}
        for item in offers:
            seller_key = item.seller_uuid or item.seller_slug or f"unknown:{item.seller_name}"
            seller_offers[seller_key].append(item)
            if seller_key not in seller_meta:
                seller_meta[seller_key] = {
                    "seller_uuid": item.seller_uuid,
                    "seller_slug": item.seller_slug,
                    "seller_name": item.seller_name,
                }

        sellers_agg: list[NormalizedSellerDTO] = []
        for seller_key, items in seller_offers.items():
            prices = [float(item.price) for item in items if item.price is not None]
            online_known = [item for item in items if item.is_online is not None]
            auto_known = [item for item in items if item.auto_delivery is not None]
            meta = seller_meta.get(seller_key, {})
            seller_name = str(meta.get("seller_name") or items[0].seller_name)
            seller_id = str(meta.get("seller_slug") or meta.get("seller_uuid") or seller_key)
            sellers_agg.append(
                NormalizedSellerDTO(
                    marketplace=MarketplaceSlug.playerok,
                    seller_id=seller_id,
                    seller_name=seller_name,
                    offers_count=len(items),
                    min_price=min(prices) if prices else None,
                    p50_price=self._percentile(prices, 0.50),
                    max_price=max(prices) if prices else None,
                    online_share=(
                        round(sum(1 for item in online_known if item.is_online) / len(online_known), 4)
                        if online_known
                        else None
                    ),
                    auto_delivery_share=(
                        round(sum(1 for item in auto_known if item.auto_delivery) / len(auto_known), 4)
                        if auto_known
                        else None
                    ),
                )
            )
        sellers_agg.sort(key=lambda item: (-item.offers_count, item.seller_name.lower()))

        category_tokens = set()
        if category_game_slug and category_game_slug in game_name_by_slug:
            category_tokens.update(tokenize(game_name_by_slug[category_game_slug]))
        for slug in category_slugs:
            label = section_name_map.get(slug, slug)
            category_tokens.update(tokenize(label))
        category_tokens = set(meaningful_query_tokens(category_tokens))

        demand: DemandStatsV2DTO | None = None
        relevant_reviews: list[PlayerOkReviewData] = []
        diagnostics = _ReviewDiagnostics()
        top_demand_sellers: list[_SellerDemandStat] = []

        if options.include_reviews:
            ranked_sellers = sorted(
                seller_offers.keys(),
                key=lambda key: (-len(seller_offers[key]), str(seller_meta.get(key, {}).get("seller_name") or key).lower()),
            )
            for seller_key in ranked_sellers[: options.seller_limit]:
                seller_rows = seller_offers[seller_key]
                meta = seller_meta.get(seller_key, {})
                seller_uuid = str(meta.get("seller_uuid") or "").strip() or None
                seller_slug = str(meta.get("seller_slug") or "").strip() or None
                seller_name = str(meta.get("seller_name") or seller_key)

                diagnostics.sellers_analyzed += 1
                seller_reviews_rows: list[PlayerOkReviewData] = []

                if seller_uuid:
                    combos_counter: Counter[tuple[str, str]] = Counter()
                    for offer in seller_rows:
                        if offer.game_id and offer.category_id:
                            combos_counter[(offer.game_id, offer.category_id)] += 1
                    combos = [item for item, _ in combos_counter.most_common(5)]
                    if combos:
                        for game_id, category_id in combos:
                            try:
                                fetched, _ = client.fetch_testimonials(
                                    seller_uuid=seller_uuid,
                                    seller_slug=seller_slug or seller_name,
                                    game_id=game_id,
                                    category_id=category_id,
                                    max_pages=options.review_pages_per_seller,
                                )
                                seller_reviews_rows.extend(fetched)
                            except Exception:
                                diagnostics.failed_sellers += 1
                                break
                    else:
                        diagnostics.failed_sellers += 1
                elif options.include_fallback_scan:
                    for offer in seller_rows[: max(1, min(8, options.review_pages_per_seller * 2))]:
                        if not offer.offer_url:
                            continue
                        try:
                            parsed_offer = client.parse_offer(offer.offer_url)
                        except Exception:
                            continue
                        seller_reviews_rows.extend(parsed_offer.reviews)
                else:
                    diagnostics.failed_sellers += 1

                seller_reviews_rows = self._dedupe_reviews(seller_reviews_rows)
                diagnostics.reviews_scanned += len(seller_reviews_rows)
                matched_for_seller: list[PlayerOkReviewData] = []
                for review in seller_reviews_rows:
                    is_match, reason = self._is_review_match(
                        review,
                        seller_offers=seller_rows,
                        category_tokens=sorted(category_tokens),
                    )
                    if is_match:
                        matched_for_seller.append(review)
                        relevant_reviews.append(review)
                        continue
                    if reason == "no_amount":
                        diagnostics.no_amount += 1
                    elif reason == "no_game_match":
                        diagnostics.no_game_match += 1
                    elif reason == "no_price_match":
                        diagnostics.no_price_match += 1
                top_demand_sellers.append(
                    _SellerDemandStat(
                        seller_slug=seller_slug or seller_name,
                        seller_name=seller_name,
                        estimated_purchases_total=len(matched_for_seller),
                        estimated_purchases_30d=sum(
                            1 for item in matched_for_seller if self._is_this_month_bucket(item.date_bucket)
                        ),
                        reviews_scanned=len(seller_reviews_rows),
                    )
                )

            demand = self._compute_demand_stats(
                relevant_reviews,
                include_index=options.include_demand_index,
                sellers_analyzed=diagnostics.sellers_analyzed,
                reviews_scanned=diagnostics.reviews_scanned,
            )
            if diagnostics.failed_sellers > 0:
                warnings.append(
                    tr(
                        common_filters.ui_locale.value,
                        "warning.reviews.failed_sellers",
                        count=diagnostics.failed_sellers,
                    )
                )
            if diagnostics.no_amount > 0:
                warnings.append(tr(common_filters.ui_locale.value, "warning.reviews.no_amount"))
            if diagnostics.no_game_match > 0:
                warnings.append(tr(common_filters.ui_locale.value, "warning.reviews.no_game"))
            if diagnostics.no_price_match > 0:
                warnings.append(tr(common_filters.ui_locale.value, "warning.reviews.no_price"))
            if demand.relevant_reviews == 0:
                warnings.append(tr(common_filters.ui_locale.value, "warning.reviews.none_relevant"))

        top_demand_sellers.sort(
            key=lambda item: (-item.estimated_purchases_30d, -item.estimated_purchases_total, -item.reviews_scanned)
        )
        competition = self._compute_competition_metrics(offers)

        core_offers = [
            NormalizedOfferDTO(
                marketplace=MarketplaceSlug.playerok,
                offer_id=item.offer_id,
                offer_url=item.offer_url,
                section_id=item.section_slug,
                seller_id=item.seller_slug or item.seller_uuid,
                seller_name=item.seller_name,
                seller_url=item.seller_url,
                description=item.description,
                price=float(item.price),
                currency=item.currency,
                reviews_count=item.reviews_count,
                is_online=item.is_online,
                auto_delivery=item.auto_delivery,
            )
            for item in offers
        ]
        core_reviews = [
            NormalizedReviewDTO(
                marketplace=MarketplaceSlug.playerok,
                seller_id=item.seller_slug or item.seller_uuid or "unknown",
                detail=item.detail,
                text=item.text,
                rating=item.rating,
                date_bucket=item.date_bucket,
                is_relevant=True,
            )
            for item in relevant_reviews
        ]

        summary = MarketplaceSummaryDTO(
            marketplace=MarketplaceSlug.playerok,
            label=self.label,
            status="done",
            request_id=str(uuid4()),
            generated_at=generated_at,
            valid_until=valid_until,
            cache_hit=False,
            ui_locale=common_filters.ui_locale,
            content_locale_requested="auto",
            content_locale_applied=("ru" if common_filters.currency.value == "RUB" else "en"),
            category_game_slug=category_game_slug,
            category_slugs=sorted(set(category_slugs)),
            offers_stats=stats,
            coverage=CoverageV2DTO(
                sections_scanned=len(section_rows),
                sections_lower_bound=lower_bound_sections,
                coverage_note=coverage_note,
            ),
            demand=demand,
            warnings=warnings,
        )

        top_offers_rows = sorted(offers, key=lambda item: float(item.price))[:100]
        top_sellers_rows = sellers_agg[:100]
        result = MarketplaceRunResultDTO(
            summary=summary,
            core=MarketplaceCoreDTO(
                offers=core_offers,
                sellers=top_sellers_rows,
                reviews=core_reviews,
            ),
            raw={
                "provider_request_id": summary.request_id,
                "legacy_result": {
                    "offers_stats": stats.model_dump(mode="json"),
                    "coverage": summary.coverage.model_dump(mode="json"),
                    "demand": demand.model_dump(mode="json") if demand else None,
                    "warnings": warnings,
                    "charts": {"history_points": []},
                    "tables": {
                        "top_offers": [
                            {
                                "offer_id": item.offer_id,
                                "offer_url": item.offer_url,
                                "seller_id": item.seller_slug or item.seller_uuid,
                                "seller_name": item.seller_name,
                                "description": item.description,
                                "price": float(item.price),
                                "currency": item.currency,
                                "reviews_count": item.reviews_count,
                                "is_online": item.is_online,
                                "auto_delivery": item.auto_delivery,
                                "seller_url": item.seller_url,
                            }
                            for item in top_offers_rows
                        ],
                        "top_sellers": [item.model_dump(mode="json") for item in top_sellers_rows],
                        "top_demand_sellers": [
                            {
                                "seller_id": item.seller_slug,
                                "seller_name": item.seller_name,
                                "estimated_purchases_total": item.estimated_purchases_total,
                                "estimated_purchases_30d": item.estimated_purchases_30d,
                                "reviews_scanned": item.reviews_scanned,
                            }
                            for item in top_demand_sellers
                        ],
                        "sections": section_rows,
                    },
                    "competition": competition,
                    "collection_sources": source_stats,
                    "advanced_headers": self._mask_map(advanced_headers),
                    "advanced_cookies": self._mask_map(advanced_cookies),
                    "use_html_degrade": use_html_degrade,
                },
            },
        )
        self._persist_cache(
            cache_key=cache_key,
            query=common_filters.query,
            currency=common_filters.currency.value,
            generated_at=generated_at,
            valid_until=valid_until,
            result=result,
        )
        return result

    @staticmethod
    def _filter_offers(
        offers: list[NormalizedOfferDTO],
        *,
        price_min: float | None = None,
        price_max: float | None = None,
        min_reviews: int | None = None,
        online_only: bool = False,
        auto_delivery_only: bool = False,
        seller_query: str | None = None,
    ) -> list[NormalizedOfferDTO]:
        query = (seller_query or "").strip().lower()
        result: list[NormalizedOfferDTO] = []
        for item in offers:
            if price_min is not None and item.price < price_min:
                continue
            if price_max is not None and item.price > price_max:
                continue
            if min_reviews is not None and (item.reviews_count or 0) < min_reviews:
                continue
            if online_only and item.is_online is not True:
                continue
            if auto_delivery_only and item.auto_delivery is not True:
                continue
            if query:
                seller_id = item.seller_id or ""
                if query not in item.seller_name.lower() and query not in seller_id.lower():
                    continue
            result.append(item)
        return result

    def list_offers(
        self,
        run_result: MarketplaceRunResultDTO,
        *,
        limit: int,
        offset: int,
        price_min: float | None = None,
        price_max: float | None = None,
        min_reviews: int | None = None,
        online_only: bool = False,
        auto_delivery_only: bool = False,
        seller_query: str | None = None,
    ) -> MarketplaceOffersResponseDTO:
        filtered = self._filter_offers(
            run_result.core.offers,
            price_min=price_min,
            price_max=price_max,
            min_reviews=min_reviews,
            online_only=online_only,
            auto_delivery_only=auto_delivery_only,
            seller_query=seller_query,
        )
        safe_limit = max(1, limit)
        safe_offset = max(0, offset)
        items = filtered[safe_offset : safe_offset + safe_limit]
        return MarketplaceOffersResponseDTO(
            run_id=run_result.summary.request_id,
            marketplace=MarketplaceSlug.playerok,
            total=len(filtered),
            limit=safe_limit,
            offset=safe_offset,
            items=items,
        )

    def categories(
        self,
        game_slug: str | None = None,
        *,
        common_filters: CommonFiltersDTO | None = None,
        force_refresh: bool = False,
        with_source: bool = False,
    ) -> list[PlayerOkCategoryGameDTO] | tuple[list[PlayerOkCategoryGameDTO], str]:
        filters = common_filters or CommonFiltersDTO()
        now = datetime.now(UTC)
        cache_items = type(self)._categories_cache
        cache_expires_at = type(self)._categories_cache_expires_at
        if (
            game_slug is None
            and not force_refresh
            and cache_items is not None
            and cache_expires_at is not None
            and cache_expires_at > now
        ):
            cached = [
                PlayerOkCategoryGameDTO.model_validate(item.model_dump(mode="json"))
                for item in cache_items
            ]
            if with_source:
                return cached, "cache"
            return cached

        resolved, allow_direct = self._resolve_proxy_pool({}, filters)
        client = PlayerOkClient(
            settings=self.settings,
            datacenter_proxies=normalize_proxy_list(resolved.datacenter),
            residential_proxies=normalize_proxy_list(resolved.residential),
            mobile_proxies=normalize_proxy_list(resolved.mobile),
            allow_direct_fallback=allow_direct,
        )
        games = client.get_categories_catalog(game_slug=game_slug)
        result = [
            PlayerOkCategoryGameDTO(
                game_id=game.game_id,
                game_slug=game.game_slug,
                game_url=game.game_url,
                game_name=game.game_name,
                sections_loaded=bool(game_slug),
                sections=[
                    PlayerOkCategorySectionDTO(
                        section_id=section.section_id,
                        section_slug=section.section_slug,
                        section_url=section.section_url,
                        section_name=section.section_name,
                        full_name=section.full_name,
                    )
                    for section in game.sections
                ],
            )
            for game in games
        ]
        if game_slug is None:
            type(self)._categories_cache = [
                PlayerOkCategoryGameDTO.model_validate(item.model_dump(mode="json"))
                for item in result
            ]
            type(self)._categories_cache_expires_at = now + timedelta(hours=self.settings.cache_ttl_hours)
        if with_source:
            return result, "network"
        return result
