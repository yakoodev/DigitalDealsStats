from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.models import AnalysisCache, AnalysisRequest
from app.schemas.analyze import AnalyzeOptionsDTO
from app.schemas.v2 import (
    CommonFiltersDTO,
    CoverageV2DTO,
    DemandStatsV2DTO,
    GgSellCategoryDTO,
    GgSellCategoryTypeDTO,
    MarketplaceCoreDTO,
    MarketplaceOffersResponseDTO,
    MarketplaceRunResultDTO,
    MarketplaceSlug,
    MarketplaceSummaryDTO,
    NormalizedOfferDTO,
    NormalizedReviewDTO,
    NormalizedSellerDTO,
    OffersStatsV2DTO,
)
from app.services.ggsell_client import GgSellCategory, GgSellClient, GgSellOffer, GgSellReview
from app.services.i18n import tr
from app.services.marketplaces.base import MarketplaceProvider
from app.services.network_settings import NetworkSettingsService
from app.services.proxy_utils import normalize_proxy_list
from app.services.text_utils import is_text_relevant, normalize_text, query_tokens


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


@dataclass
class _SellerDemandStat:
    seller_id: str | None
    seller_name: str
    seller_url: str | None
    estimated_purchases_total: int
    estimated_purchases_30d: int
    reviews_scanned: int


class GgSellProvider(MarketplaceProvider):
    slug = MarketplaceSlug.ggsell
    label = "GGSell"
    CACHE_MODE = "ggsell_v2"
    DEFAULT_SECTION_LIMIT = 60

    _categories_cache_types: list[GgSellCategoryTypeDTO] | None = None
    _categories_cache_items: list[GgSellCategoryDTO] | None = None
    _categories_cache_expires_at: datetime | None = None

    def __init__(self, db: Session, settings: Settings) -> None:
        self.db = db
        self.settings = settings
        self.network_settings = NetworkSettingsService(db=db, settings=settings)

    @staticmethod
    def _to_marketplace_filters(raw_filters: dict | None) -> dict:
        return raw_filters if isinstance(raw_filters, dict) else {}

    @staticmethod
    def _normalize_slug(value: object) -> str:
        return str(value or "").strip().strip("/").lower()

    def _resolve_proxy_pool(self, filters: dict, common_filters: CommonFiltersDTO):
        resolved = self.network_settings.resolve(
            common_filters=common_filters,
            marketplace_filters=filters,
        )
        allow_direct = bool(common_filters.allow_direct_fallback)
        self.network_settings.ensure_proxy_policy(resolved, allow_direct_fallback=allow_direct)
        return resolved, allow_direct

    def _build_client(self, filters: dict, common_filters: CommonFiltersDTO) -> GgSellClient:
        resolved, allow_direct = self._resolve_proxy_pool(filters, common_filters)
        return GgSellClient(
            settings=self.settings,
            datacenter_proxies=normalize_proxy_list(resolved.datacenter),
            residential_proxies=normalize_proxy_list(resolved.residential),
            mobile_proxies=normalize_proxy_list(resolved.mobile),
            allow_direct_fallback=allow_direct,
        )

    def _resolve_options(self, raw_options: dict | AnalyzeOptionsDTO | None) -> _EffectiveOptions:
        if isinstance(raw_options, AnalyzeOptionsDTO):
            options = raw_options
        else:
            options = AnalyzeOptionsDTO.model_validate(raw_options or {})

        profile = options.profile.value
        if profile == "safe":
            include_reviews = False
            include_demand_index = False
            include_fallback_scan = False
            section_limit = min(self.settings.quick_sections_limit, 30)
            seller_limit = 3
            review_pages_per_seller = 1
            history_points_limit = 30
        elif profile == "deep":
            include_reviews = True
            include_demand_index = True
            include_fallback_scan = True
            section_limit = max(self.settings.fallback_sections_limit, 120)
            seller_limit = 6
            review_pages_per_seller = 6
            history_points_limit = 120
        else:
            include_reviews = False
            include_demand_index = False
            include_fallback_scan = True
            section_limit = self.settings.quick_sections_limit
            seller_limit = 3
            review_pages_per_seller = self.settings.review_max_pages_per_seller
            history_points_limit = 60

        if options.include_reviews is not None:
            include_reviews = options.include_reviews
        if options.include_demand_index is not None:
            include_demand_index = options.include_demand_index
        if options.include_fallback_scan is not None:
            include_fallback_scan = options.include_fallback_scan
        if options.section_limit is not None:
            section_limit = options.section_limit
        if options.seller_limit is not None:
            seller_limit = options.seller_limit
        if options.review_pages_per_seller is not None:
            review_pages_per_seller = options.review_pages_per_seller
        if options.history_points_limit is not None:
            history_points_limit = options.history_points_limit

        section_limit = max(1, min(500, section_limit or self.DEFAULT_SECTION_LIMIT))
        seller_limit = max(1, min(20, seller_limit))
        review_pages_per_seller = max(1, min(20, review_pages_per_seller))
        history_points_limit = max(5, min(365, history_points_limit))

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
    def _cache_key(
        *,
        query: str,
        currency: str,
        ui_locale: str,
        options_hash: str,
        category_type_slug: str | None,
        category_slugs: list[str],
        use_type_scope: bool,
    ) -> str:
        payload = {
            "v": "ggsell-v2",
            "query": normalize_text(query),
            "currency": currency,
            "ui_locale": ui_locale,
            "options_hash": options_hash,
            "category_type_slug": category_type_slug,
            "category_slugs": sorted(set(category_slugs)),
            "use_type_scope": use_type_scope,
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
    def _coverage_status(counter_total: int | None, loaded_count: int) -> str:
        if counter_total is None:
            return "full"
        return "lower_bound" if loaded_count < max(counter_total, 0) else "full"

    @staticmethod
    def _is_recent_30d(review: GgSellReview) -> bool:
        if not review.date_response:
            return False
        try:
            created = datetime.fromisoformat(review.date_response.replace("Z", "+00:00"))
            return created >= datetime.now(UTC) - timedelta(days=30)
        except Exception:
            return False

    @classmethod
    def _percentile(cls, values: list[float], percentile: float) -> float | None:
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
    def _offers_stats(cls, offers: list[GgSellOffer]) -> OffersStatsV2DTO:
        prices = [float(item.price) for item in offers if item.price is not None]
        unique_sellers = {
            (item.seller_id or "") + "|" + (item.seller_name or "")
            for item in offers
            if item.seller_id or item.seller_name
        }
        auto_known = [item for item in offers if item.auto_delivery is not None]
        return OffersStatsV2DTO(
            matched_offers=len(offers),
            unique_sellers=len(unique_sellers),
            min_price=min(prices) if prices else None,
            avg_price=(round(sum(prices) / len(prices), 6) if prices else None),
            p50_price=cls._percentile(prices, 0.50),
            p90_price=cls._percentile(prices, 0.90),
            max_price=max(prices) if prices else None,
            online_share=None,
            auto_delivery_share=(
                round(sum(1 for item in auto_known if item.auto_delivery) / len(auto_known), 4)
                if auto_known
                else None
            ),
        )

    @classmethod
    def _compute_demand(
        cls,
        reviews: list[GgSellReview],
        *,
        include_index: bool,
        sellers_analyzed: int,
        reviews_scanned: int,
        purchases_from_sold_total: int,
        purchases_total_is_lower_bound: bool,
    ) -> DemandStatsV2DTO:
        positive = [item for item in reviews if (item.type_response or "") == "good"]
        positive_share = len(positive) / len(reviews) if reviews else 0.0
        volume_30d = sum(1 for item in reviews if cls._is_recent_30d(item))
        unique_sellers = len({item.seller_id for item in reviews if item.seller_id})
        demand_index: float | None = None
        if include_index:
            demand_index = round((min(volume_30d, 100) / 100 * 60) + (positive_share * 40), 4)
        return DemandStatsV2DTO(
            relevant_reviews=len(reviews),
            positive_share=round(positive_share, 4),
            volume_30d=volume_30d,
            demand_index=demand_index,
            unique_sellers_with_relevant_reviews=unique_sellers,
            estimated_purchases_total=purchases_from_sold_total,
            estimated_purchases_30d=volume_30d,
            sellers_analyzed=sellers_analyzed,
            reviews_scanned=reviews_scanned,
            purchases_from_sold_total=purchases_from_sold_total,
            purchases_from_reviews_total=len(reviews),
            purchases_from_reviews_30d=volume_30d,
            purchases_total_is_lower_bound=purchases_total_is_lower_bound,
        )

    @staticmethod
    def _history_points(
        db: Session,
        *,
        query: str,
        currency: str,
        limit: int,
    ) -> list[dict]:
        rows = db.scalars(
            select(AnalysisRequest)
            .where(
                AnalysisRequest.mode == "global_v2",
                AnalysisRequest.status == "done",
                AnalysisRequest.query == query,
                AnalysisRequest.currency == currency,
            )
            .order_by(desc(AnalysisRequest.updated_at))
            .limit(max(20, min(limit * 2, 240)))
        ).all()
        points: list[dict] = []
        for row in reversed(rows):
            payload = row.result_json if isinstance(row.result_json, dict) else {}
            summaries = payload.get("marketplace_summaries")
            if not isinstance(summaries, dict):
                continue
            summary = summaries.get("ggsell")
            if not isinstance(summary, dict):
                continue
            generated_at = summary.get("generated_at")
            offers_stats = summary.get("offers_stats") if isinstance(summary.get("offers_stats"), dict) else {}
            demand = summary.get("demand") if isinstance(summary.get("demand"), dict) else {}
            if not isinstance(generated_at, str):
                continue
            points.append(
                {
                    "generated_at": generated_at,
                    "matched_offers": int(offers_stats.get("matched_offers", 0)),
                    "unique_sellers": int(offers_stats.get("unique_sellers", 0)),
                    "p50_price": offers_stats.get("p50_price"),
                    "demand_index": demand.get("demand_index") if demand else None,
                }
            )
        return points[-limit:]

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
        filtered: list[NormalizedOfferDTO] = []
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
            filtered.append(item)
        return filtered

    def analyze(self, common_filters: CommonFiltersDTO, marketplace_filters: dict | None) -> MarketplaceRunResultDTO:
        filters = self._to_marketplace_filters(marketplace_filters)
        category_type_slug = self._normalize_slug(filters.get("category_type_slug"))
        category_slugs = [
            self._normalize_slug(item)
            for item in (filters.get("category_slugs") if isinstance(filters.get("category_slugs"), list) else [])
        ]
        category_slugs = [item for item in category_slugs if item]
        use_type_scope = bool(filters.get("use_type_scope", True))

        options = self._resolve_options(filters.get("options"))
        cache_key = self._cache_key(
            query=common_filters.query,
            currency=common_filters.currency.value,
            ui_locale=common_filters.ui_locale.value,
            options_hash=options.options_hash,
            category_type_slug=category_type_slug or None,
            category_slugs=category_slugs,
            use_type_scope=use_type_scope,
        )
        if not common_filters.force_refresh:
            cached = self._load_cache(cache_key)
            if cached is not None:
                return cached

        client = self._build_client(filters, common_filters)
        generated_at = datetime.now(UTC)
        valid_until = generated_at + timedelta(hours=self.settings.cache_ttl_hours)
        query_value = common_filters.query.strip()
        qtokens = query_tokens(query_value)
        warnings: list[str] = []

        lang = "en" if common_filters.ui_locale.value == "en" else "ru"
        _, catalog_categories = client.fetch_categories_catalog(lang=lang)
        category_by_slug = {item.category_slug: item for item in catalog_categories}

        selected_slugs: list[str] = []
        seen_slugs: set[str] = set()

        def add_slug(value: str) -> None:
            slug = self._normalize_slug(value)
            if not slug or slug in seen_slugs:
                return
            seen_slugs.add(slug)
            selected_slugs.append(slug)

        for slug in category_slugs:
            add_slug(slug)

        if use_type_scope and category_type_slug:
            rows = [item.category_slug for item in catalog_categories if item.type_slug == category_type_slug]
            if not rows:
                warnings.append(tr(common_filters.ui_locale.value, "warning.ggsell.type_not_found"))
            for slug in sorted(rows):
                add_slug(slug)

        if not selected_slugs and query_value:
            suggestions = client.search_categories(
                search_term=query_value,
                lang=lang,
                limit=max(5, min(options.section_limit * 2, 80)),
            )
            for row in suggestions:
                add_slug(row.category_slug)

        if not selected_slugs and query_value and options.include_fallback_scan:
            ranked = sorted(
                catalog_categories,
                key=lambda item: (-(item.offers_count or 0), item.category_name.lower()),
            )
            for row in ranked:
                if qtokens and not is_text_relevant(f"{row.category_name} {row.category_slug}", qtokens):
                    continue
                add_slug(row.category_slug)
                if len(selected_slugs) >= options.section_limit:
                    break

        if len(selected_slugs) > options.section_limit:
            selected_slugs = selected_slugs[: options.section_limit]

        resolved_categories: list[GgSellCategory] = []
        missing_categories = 0
        for slug in selected_slugs:
            try:
                details = client.fetch_category_details(category_slug=slug, lang=lang)
            except Exception:
                warnings.append(
                    tr(
                        common_filters.ui_locale.value,
                        "warning.ggsell.category_failed",
                        section=slug,
                    )
                )
                missing_categories += 1
                continue
            if details is None:
                missing_categories += 1
                continue
            catalog_item = category_by_slug.get(slug)
            if catalog_item is not None:
                details.type_slug = details.type_slug or catalog_item.type_slug
                details.type_name = details.type_name or catalog_item.type_name
                details.parent_slug = details.parent_slug or catalog_item.parent_slug
                details.parent_name = details.parent_name or catalog_item.parent_name
            resolved_categories.append(details)

        if missing_categories > 0:
            warnings.append(
                tr(common_filters.ui_locale.value, "warning.ggsell.categories_not_found", count=missing_categories)
            )

        offers_map: dict[str, GgSellOffer] = {}
        section_rows: list[dict] = []
        failed_categories = 0

        max_total_offers = max(160, options.section_limit * 60)
        max_offers_per_section = max(30, min(600, int(max_total_offers / max(1, len(resolved_categories))) + 20))
        for category in resolved_categories:
            if category.digi_catalog is None:
                continue
            loaded_raw_count = 0
            total_count: int | None = None
            max_pages = max(1, min(100, (max_offers_per_section + 39) // 40))
            try:
                for page in range(1, max_pages + 1):
                    page_total, page_items = client.fetch_category_offers(
                        digi_catalog=category.digi_catalog,
                        requested_currency=common_filters.currency.value,
                        lang=lang,
                        page=page,
                        limit=40,
                        query=query_value,
                    )
                    if total_count is None:
                        total_count = page_total
                    if not page_items:
                        break
                    loaded_raw_count += len(page_items)
                    for raw_item in page_items:
                        offer = client.parse_offer_item(
                            raw_item,
                            section_slug=category.category_slug,
                            requested_currency=common_filters.currency.value,
                        )
                        if offer is None:
                            continue
                        if qtokens and not is_text_relevant(offer.title, qtokens):
                            continue
                        offers_map[offer.offer_id] = offer
                    if loaded_raw_count >= max_offers_per_section:
                        break
                    if total_count is not None and loaded_raw_count >= total_count:
                        break
            except Exception:
                failed_categories += 1
                warnings.append(
                    tr(
                        common_filters.ui_locale.value,
                        "warning.ggsell.category_failed",
                        section=category.category_name,
                    )
                )
                continue

            section_rows.append(
                {
                    "section_url": category.category_url,
                    "section_id": str(category.digi_catalog),
                    "section_name": category.category_name,
                    "counter_total": total_count,
                    "loaded_count": loaded_raw_count,
                    "coverage_status": self._coverage_status(total_count, loaded_raw_count),
                }
            )

        offers = list(offers_map.values())
        stats = self._offers_stats(offers)
        lower_bound_sections = sum(1 for row in section_rows if row["coverage_status"] == "lower_bound")
        coverage_note = tr(common_filters.ui_locale.value, "warning.coverage.lower_bound") if lower_bound_sections else None
        if coverage_note:
            warnings.append(coverage_note)
        if failed_categories > 0:
            warnings.append(
                tr(common_filters.ui_locale.value, "warning.ggsell.categories_failed_count", count=failed_categories)
            )
        if not offers:
            warnings.append(
                tr(
                    common_filters.ui_locale.value,
                    "warning.offers.none_query" if query_value else "warning.offers.none_scope",
                )
            )

        seller_offers: dict[str, list[GgSellOffer]] = defaultdict(list)
        for offer in offers:
            key = offer.seller_id or f"name:{offer.seller_name}"
            seller_offers[key].append(offer)

        sellers_agg: list[NormalizedSellerDTO] = []
        seller_url_by_key: dict[str, str | None] = {}
        for seller_key, rows in seller_offers.items():
            prices = [float(item.price) for item in rows if item.price is not None]
            sellers_agg.append(
                NormalizedSellerDTO(
                    marketplace=MarketplaceSlug.ggsell,
                    seller_id=rows[0].seller_id,
                    seller_name=rows[0].seller_name,
                    offers_count=len(rows),
                    min_price=min(prices) if prices else None,
                    p50_price=self._percentile(prices, 0.50),
                    max_price=max(prices) if prices else None,
                    online_share=None,
                    auto_delivery_share=(
                        round(sum(1 for item in rows if item.auto_delivery is True) / len(rows), 4)
                        if rows
                        else None
                    ),
                )
            )
            seller_url_by_key[seller_key] = rows[0].seller_url
        sellers_agg.sort(key=lambda item: (-item.offers_count, item.seller_name.lower()))

        purchases_from_sold_total = sum(max(0, int(item.sold_count or 0)) for item in offers)
        purchases_total_is_lower_bound = False

        diagnostics = _ReviewDiagnostics()
        relevant_reviews: list[GgSellReview] = []
        top_demand_sellers: list[_SellerDemandStat] = []

        if options.include_reviews:
            ranked_sellers = sorted(
                seller_offers.keys(),
                key=lambda key: (-len(seller_offers[key]), seller_offers[key][0].seller_name.lower()),
            )
            for seller_key in ranked_sellers[: options.seller_limit]:
                rows = seller_offers[seller_key]
                seller = rows[0]
                diagnostics.sellers_analyzed += 1
                if not seller.offer_url:
                    diagnostics.failed_sellers += 1
                    continue
                seller_reviews: list[GgSellReview] = []
                sample_offers = sorted(
                    rows,
                    key=lambda item: (-(item.sold_count or 0), float(item.price)),
                )[: max(1, min(options.review_pages_per_seller, 8))]
                for offer in sample_offers:
                    try:
                        page_reviews = client.fetch_product_reviews_from_html(
                            offer_url=offer.offer_url,
                            seller_id=seller.seller_id,
                        )
                    except Exception:
                        continue
                    diagnostics.reviews_scanned += len(page_reviews)
                    seller_reviews.extend(page_reviews)
                    relevant_reviews.extend(page_reviews)
                if not seller_reviews:
                    diagnostics.failed_sellers += 1
                top_demand_sellers.append(
                    _SellerDemandStat(
                        seller_id=seller.seller_id,
                        seller_name=seller.seller_name,
                        seller_url=seller.seller_url,
                        estimated_purchases_total=sum(max(0, int(item.sold_count or 0)) for item in rows),
                        estimated_purchases_30d=sum(1 for item in seller_reviews if self._is_recent_30d(item)),
                        reviews_scanned=len(seller_reviews),
                    )
                )
            if diagnostics.failed_sellers > 0:
                warnings.append(
                    tr(
                        common_filters.ui_locale.value,
                        "warning.reviews.failed_sellers",
                        count=diagnostics.failed_sellers,
                    )
                )
            if not relevant_reviews:
                warnings.append(tr(common_filters.ui_locale.value, "warning.reviews.none_relevant"))

        demand = self._compute_demand(
            relevant_reviews,
            include_index=options.include_demand_index,
            sellers_analyzed=diagnostics.sellers_analyzed,
            reviews_scanned=diagnostics.reviews_scanned,
            purchases_from_sold_total=purchases_from_sold_total,
            purchases_total_is_lower_bound=purchases_total_is_lower_bound,
        )

        top_demand_sellers.sort(
            key=lambda item: (-item.estimated_purchases_30d, -item.estimated_purchases_total, -item.reviews_scanned)
        )

        core_offers = [
            NormalizedOfferDTO(
                marketplace=MarketplaceSlug.ggsell,
                offer_id=item.offer_id,
                offer_url=item.offer_url,
                section_id=item.section_id,
                seller_id=item.seller_id,
                seller_name=item.seller_name,
                seller_url=item.seller_url,
                description=item.title,
                price=float(item.price),
                currency=item.currency,
                reviews_count=item.reviews_count,
                is_online=item.is_online,
                auto_delivery=item.auto_delivery,
                sold_count=item.sold_count,
                sold_text=item.sold_text,
                sold_is_lower_bound=False,
            )
            for item in offers
        ]
        core_reviews = [
            NormalizedReviewDTO(
                marketplace=MarketplaceSlug.ggsell,
                seller_id=item.seller_id or "",
                detail=item.text_response,
                text=item.text_response,
                rating=(5 if (item.type_response or "") == "good" else (1 if (item.type_response or "") == "bad" else None)),
                date_bucket=item.date_response,
                is_relevant=True,
            )
            for item in relevant_reviews
        ]

        summary = MarketplaceSummaryDTO(
            marketplace=MarketplaceSlug.ggsell,
            label=self.label,
            status="done",
            request_id=str(uuid4()),
            generated_at=generated_at,
            valid_until=valid_until,
            cache_hit=False,
            ui_locale=common_filters.ui_locale,
            content_locale_requested="auto",
            content_locale_applied=("ru" if common_filters.ui_locale.value == "ru" else "en"),
            category_game_slug=category_type_slug or None,
            category_slugs=selected_slugs,
            ggsell_type_slug=category_type_slug or None,
            ggsell_category_slugs=selected_slugs,
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
        top_sellers_rows_raw = []
        for row in top_sellers_rows:
            seller_key = row.seller_id or f"name:{row.seller_name}"
            top_sellers_rows_raw.append(
                {
                    **row.model_dump(mode="json"),
                    "seller_url": seller_url_by_key.get(seller_key),
                }
            )

        history_points = self._history_points(
            self.db,
            query=common_filters.query,
            currency=common_filters.currency.value,
            limit=options.history_points_limit,
        )

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
                    "charts": {"history_points": history_points},
                    "tables": {
                        "top_offers": [
                            {
                                "offer_id": item.offer_id,
                                "offer_url": item.offer_url,
                                "seller_id": item.seller_id,
                                "seller_name": item.seller_name,
                                "description": item.title,
                                "price": float(item.price),
                                "currency": item.currency,
                                "reviews_count": item.reviews_count,
                                "is_online": item.is_online,
                                "auto_delivery": item.auto_delivery,
                                "sold_count": item.sold_count,
                                "sold_text": item.sold_text,
                                "sold_is_lower_bound": False,
                                "seller_url": item.seller_url,
                            }
                            for item in top_offers_rows
                        ],
                        "top_sellers": top_sellers_rows_raw,
                        "top_demand_sellers": [
                            {
                                "seller_id": item.seller_id,
                                "seller_name": item.seller_name,
                                "seller_url": item.seller_url,
                                "estimated_purchases_total": item.estimated_purchases_total,
                                "estimated_purchases_30d": item.estimated_purchases_30d,
                                "reviews_scanned": item.reviews_scanned,
                            }
                            for item in top_demand_sellers
                        ],
                        "sections": section_rows,
                    },
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
            marketplace=MarketplaceSlug.ggsell,
            total=len(filtered),
            limit=safe_limit,
            offset=safe_offset,
            items=items,
        )

    def categories(
        self,
        *,
        common_filters: CommonFiltersDTO | None = None,
        force_refresh: bool = False,
        with_source: bool = False,
    ) -> tuple[list[GgSellCategoryTypeDTO], list[GgSellCategoryDTO]] | tuple[
        tuple[list[GgSellCategoryTypeDTO], list[GgSellCategoryDTO]],
        str,
    ]:
        filters = common_filters or CommonFiltersDTO()
        now = datetime.now(UTC)
        cache_types = type(self)._categories_cache_types
        cache_items = type(self)._categories_cache_items
        cache_expires_at = type(self)._categories_cache_expires_at
        if (
            not force_refresh
            and cache_types is not None
            and cache_items is not None
            and cache_expires_at is not None
            and cache_expires_at > now
        ):
            types = [GgSellCategoryTypeDTO.model_validate(item.model_dump(mode="json")) for item in cache_types]
            items = [GgSellCategoryDTO.model_validate(item.model_dump(mode="json")) for item in cache_items]
            payload = (types, items)
            if with_source:
                return payload, "cache"
            return payload

        client = self._build_client({}, filters)
        lang = "en" if filters.ui_locale.value == "en" else "ru"
        types_raw, categories_raw = client.fetch_categories_catalog(lang=lang)
        types = [
            GgSellCategoryTypeDTO(
                type_slug=item.type_slug,
                type_name=item.type_name,
                category_url=item.category_url,
                icon_alias=item.icon_alias,
            )
            for item in types_raw
        ]
        categories = [
            GgSellCategoryDTO(
                category_slug=item.category_slug,
                category_name=item.category_name,
                category_url=item.category_url,
                type_slug=item.type_slug,
                type_name=item.type_name,
                parent_slug=item.parent_slug,
                parent_name=item.parent_name,
                digi_catalog=item.digi_catalog,
                offers_count=item.offers_count,
            )
            for item in categories_raw
        ]
        type(self)._categories_cache_types = [
            GgSellCategoryTypeDTO.model_validate(item.model_dump(mode="json"))
            for item in types
        ]
        type(self)._categories_cache_items = [
            GgSellCategoryDTO.model_validate(item.model_dump(mode="json"))
            for item in categories
        ]
        type(self)._categories_cache_expires_at = now + timedelta(hours=self.settings.cache_ttl_hours)
        payload = (types, categories)
        if with_source:
            return payload, "network"
        return payload
