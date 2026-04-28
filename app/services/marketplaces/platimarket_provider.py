from __future__ import annotations

import hashlib
import json
from collections import defaultdict
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
    PlatiCatalogTreeNodeDTO,
    PlatiCategoryGroupDTO,
    PlatiGameCategoryDTO,
    PlatiGameDTO,
    PlatiCategorySectionDTO,
)
from app.services.i18n import tr
from app.services.marketplaces.base import MarketplaceProvider
from app.services.marketplaces.platimarket_analytics import (
    compute_competition_metrics as compute_plati_competition_metrics,
    compute_demand as compute_plati_demand,
    coverage_status as plati_coverage_status,
    filter_offers_by_currency as filter_plati_offers_by_currency,
    history_points as plati_history_points,
    is_recent_30d as is_plati_recent_30d,
    is_review_link_match as is_plati_review_link_match,
    is_this_month_bucket as is_plati_this_month_bucket,
    normalize_currency as normalize_plati_currency,
    offers_stats as plati_offers_stats,
    percentile as plati_percentile,
    sold_parsing_diagnostics as plati_sold_parsing_diagnostics,
)
from app.services.marketplaces.platimarket_scope import (
    build_scope_from_filters as build_plati_scope_from_filters,
)
from app.services.network_settings import NetworkSettingsService
from app.services.platimarket_client import (
    PlatiCatalogNode,
    PlatiCategorySection,
    PlatiGame,
    PlatiGameCategory,
    PlatiMarketClient,
    PlatiOfferCard,
    PlatiReview,
)
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
    enrich_limit: int
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


class PlatiMarketProvider(MarketplaceProvider):
    slug = MarketplaceSlug.platimarket
    label = "Plati.Market"
    CACHE_MODE = "platimarket_v2"
    DEFAULT_SECTION_LIMIT = 80
    _games_cache: list[PlatiGameDTO] | None = None
    _games_cache_expires_at: datetime | None = None
    _catalog_tree_cache: list[PlatiCatalogTreeNodeDTO] | None = None
    _catalog_tree_cache_expires_at: datetime | None = None

    def __init__(self, db: Session, settings: Settings) -> None:
        self.db = db
        self.settings = settings
        self.network_settings = NetworkSettingsService(db=db, settings=settings)

    @staticmethod
    def _to_marketplace_filters(raw_filters: dict | None) -> dict:
        if not isinstance(raw_filters, dict):
            return {}
        return raw_filters

    def _resolve_proxy_pool(self, filters: dict, common_filters: CommonFiltersDTO):
        resolved = self.network_settings.resolve(
            common_filters=common_filters,
            marketplace_filters=filters,
        )
        allow_direct = bool(common_filters.allow_direct_fallback)
        self.network_settings.ensure_proxy_policy(resolved, allow_direct_fallback=allow_direct)
        return resolved, allow_direct

    def _build_client(self, filters: dict, common_filters: CommonFiltersDTO) -> PlatiMarketClient:
        resolved, allow_direct = self._resolve_proxy_pool(filters, common_filters)
        return PlatiMarketClient(
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
            section_limit = min(self.settings.quick_sections_limit, 40)
            seller_limit = 3
            review_pages_per_seller = 1
            history_points_limit = 30
            enrich_limit = 40
        elif profile == "deep":
            include_reviews = True
            include_demand_index = True
            include_fallback_scan = True
            section_limit = max(self.settings.fallback_sections_limit, self.settings.quick_sections_limit)
            seller_limit = 5
            review_pages_per_seller = max(self.settings.review_max_pages_per_seller, 6)
            history_points_limit = 120
            enrich_limit = 260
        else:
            include_reviews = False
            include_demand_index = False
            include_fallback_scan = True
            section_limit = self.settings.quick_sections_limit
            seller_limit = 3
            review_pages_per_seller = self.settings.review_max_pages_per_seller
            history_points_limit = 60
            enrich_limit = 120

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

        seller_limit = max(1, min(20, seller_limit))
        section_limit = max(1, min(500, section_limit or self.DEFAULT_SECTION_LIMIT))
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
            "enrich_limit": enrich_limit,
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
            enrich_limit=enrich_limit,
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
        category_game_id: int | None,
        category_game_slug: str | None,
        game_category_ids: list[int],
        use_game_scope: bool,
        category_group_id: int | None,
        category_ids: list[int],
        use_group_scope: bool,
    ) -> str:
        payload = {
            "v": "platimarket-v2",
            "query": normalize_text(query),
            "currency": currency,
            "ui_locale": ui_locale,
            "options_hash": options_hash,
            "category_game_id": category_game_id,
            "category_game_slug": category_game_slug,
            "game_category_ids": sorted(set(game_category_ids)),
            "use_game_scope": use_game_scope,
            "category_group_id": category_group_id,
            "category_ids": sorted(set(category_ids)),
            "use_group_scope": use_group_scope,
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
    def _percentile(values: list[float], percentile: float) -> float | None:
        return plati_percentile(values, percentile)

    @classmethod
    def _offers_stats(cls, offers: list[PlatiOfferCard]) -> OffersStatsV2DTO:
        return plati_offers_stats(offers)

    @staticmethod
    def _coverage_status(counter_total: int | None, loaded_count: int) -> str:
        return plati_coverage_status(counter_total, loaded_count)

    @staticmethod
    def _normalize_currency(raw: str | None) -> str | None:
        return normalize_plati_currency(raw)

    def _filter_offers_by_currency(
        self,
        offers: list[PlatiOfferCard],
        requested_currency: str,
    ) -> tuple[list[PlatiOfferCard], int, bool]:
        return filter_plati_offers_by_currency(offers, requested_currency)

    @staticmethod
    def _compute_competition_metrics(offers: list[PlatiOfferCard]) -> dict[str, float | None]:
        return compute_plati_competition_metrics(offers)

    @staticmethod
    def _sold_parsing_diagnostics(offers: list[PlatiOfferCard]) -> dict[str, object]:
        return plati_sold_parsing_diagnostics(offers)

    @staticmethod
    def _is_review_link_match(
        review: PlatiReview,
        *,
        current_offer_ids: set[str],
        seller_offer_ids: set[str],
    ) -> bool:
        return is_plati_review_link_match(
            review,
            current_offer_ids=current_offer_ids,
            seller_offer_ids=seller_offer_ids,
        )

    @staticmethod
    def _is_this_month_bucket(value: str | None) -> bool:
        return is_plati_this_month_bucket(value)

    @staticmethod
    def _is_recent_30d(review: PlatiReview) -> bool:
        return is_plati_recent_30d(review)

    @classmethod
    def _compute_demand(
        cls,
        relevant_reviews: list[PlatiReview],
        *,
        include_index: bool,
        sellers_analyzed: int,
        reviews_scanned: int,
        purchases_from_sold_total: int,
        purchases_total_is_lower_bound: bool,
    ) -> DemandStatsV2DTO:
        return compute_plati_demand(
            relevant_reviews,
            include_index=include_index,
            sellers_analyzed=sellers_analyzed,
            reviews_scanned=reviews_scanned,
            purchases_from_sold_total=purchases_from_sold_total,
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
        return plati_history_points(db, query=query, currency=currency, limit=limit)

    @staticmethod
    def _build_scope_from_filters(
        *,
        catalog_tree: list[PlatiCatalogNode],
        category_group_id: int | None,
        category_ids: list[int],
        use_group_scope: bool,
        section_limit: int,
        query_value: str,
    ) -> tuple[list[PlatiCategorySection], list[str]]:
        return build_plati_scope_from_filters(
            catalog_tree=catalog_tree,
            category_group_id=category_group_id,
            category_ids=category_ids,
            use_group_scope=use_group_scope,
            section_limit=section_limit,
            query_value=query_value,
        )

    def analyze(self, common_filters: CommonFiltersDTO, marketplace_filters: dict | None) -> MarketplaceRunResultDTO:
        filters = self._to_marketplace_filters(marketplace_filters)
        category_game_id = filters.get("category_game_id")
        if isinstance(category_game_id, str) and category_game_id.isdigit():
            category_game_id = int(category_game_id)
        if not isinstance(category_game_id, int):
            category_game_id = None
        category_game_slug_raw = filters.get("category_game_slug")
        category_game_slug = (
            str(category_game_slug_raw).strip().strip("/").lower()
            if isinstance(category_game_slug_raw, str) and category_game_slug_raw.strip()
            else None
        )
        category_game_name = (
            str(filters.get("category_game_name")).strip()
            if isinstance(filters.get("category_game_name"), str) and str(filters.get("category_game_name")).strip()
            else None
        )
        game_category_ids_raw = filters.get("game_category_ids")
        game_category_ids = sorted(
            {
                int(item)
                for item in (game_category_ids_raw if isinstance(game_category_ids_raw, list) else [])
                if str(item).isdigit() and int(item) >= 0
            }
        )
        use_game_scope = bool(filters.get("use_game_scope", True))
        category_group_id = filters.get("category_group_id")
        if isinstance(category_group_id, str) and category_group_id.isdigit():
            category_group_id = int(category_group_id)
        if not isinstance(category_group_id, int):
            category_group_id = None

        category_ids_raw = filters.get("category_ids")
        category_ids = [
            int(item)
            for item in (category_ids_raw if isinstance(category_ids_raw, list) else [])
            if str(item).isdigit()
        ]
        use_group_scope = bool(filters.get("use_group_scope", True))

        options = self._resolve_options(filters.get("options"))
        cache_key = self._cache_key(
            query=common_filters.query,
            currency=common_filters.currency.value,
            ui_locale=common_filters.ui_locale.value,
            options_hash=options.options_hash,
            category_game_id=category_game_id,
            category_game_slug=category_game_slug,
            game_category_ids=game_category_ids,
            use_game_scope=use_game_scope,
            category_group_id=category_group_id,
            category_ids=category_ids,
            use_group_scope=use_group_scope,
        )
        if not common_filters.force_refresh:
            cached = self._load_cache(cache_key)
            if cached is not None:
                return cached

        client = self._build_client(filters, common_filters)
        needs_game_catalog_lookup = (
            (category_game_id is None and category_game_slug is not None)
            or (category_game_id is not None and not category_game_slug)
            or (category_game_id is not None and not category_game_name)
        )
        if needs_game_catalog_lookup:
            try:
                for game in client.fetch_games_catalog(lang=("en" if common_filters.ui_locale.value == "en" else "ru")):
                    slug_match = category_game_slug is not None and game.game_slug == category_game_slug
                    id_match = category_game_id is not None and game.game_id == category_game_id
                    if not slug_match and not id_match:
                        continue
                    if category_game_id is None:
                        category_game_id = game.game_id
                    if not category_game_slug:
                        category_game_slug = game.game_slug
                    if not category_game_name:
                        category_game_name = game.game_name
                    break
            except Exception:
                pass
        generated_at = datetime.now(UTC)
        valid_until = generated_at + timedelta(hours=self.settings.cache_ttl_hours)

        query_value = common_filters.query.strip()
        qtokens = query_tokens(query_value)
        warnings: list[str] = []
        section_rows: list[dict] = []

        catalog_tree = client.fetch_catalog_tree()
        scope_sections, scope_warnings = self._build_scope_from_filters(
            catalog_tree=catalog_tree,
            category_group_id=category_group_id,
            category_ids=category_ids,
            use_group_scope=use_group_scope,
            section_limit=options.section_limit,
            query_value=query_value,
        )
        for item in scope_warnings:
            if item == "group_not_found":
                warnings.append(tr(common_filters.ui_locale.value, "warning.plati.group_not_found"))
            elif item == "sections_not_found":
                warnings.append(tr(common_filters.ui_locale.value, "warning.plati.sections_not_found"))
        if use_game_scope and category_game_id is None and (category_game_slug or category_game_name):
            warnings.append(tr(common_filters.ui_locale.value, "warning.plati.game_not_found"))

        offers: list[PlatiOfferCard] = []
        failed_sections = 0
        failed_games = 0
        max_total_offers = max(120, options.section_limit * 60)
        max_offers_per_section = max(30, min(400, int(max_total_offers / max(1, len(scope_sections))) + 20))
        max_offers_per_game = max(48, min(600, max_total_offers))

        if use_game_scope and category_game_id is not None:
            game_offers: dict[str, PlatiOfferCard] = {}
            game_slug = category_game_slug or str(category_game_id)
            game_name = category_game_name or game_slug
            game_url = f"https://plati.market/games/{game_slug}/{category_game_id}/"
            game_categories: list[PlatiGameCategory] = []
            game_category_by_id: dict[int, PlatiGameCategory] = {}
            try:
                game_categories = client.fetch_game_categories(
                    game_id=category_game_id,
                    game_slug=category_game_slug,
                    lang=("en" if common_filters.ui_locale.value == "en" else "ru"),
                )
                game_category_by_id = {item.category_id: item for item in game_categories}
            except Exception:
                # Категории игры используются как дополнительный фильтр.
                # Если не загрузились, основной сбор по игре продолжаем.
                game_categories = []
                game_category_by_id = {}

            if game_category_ids:
                missing_game_categories = [item for item in game_category_ids if item not in game_category_by_id]
                if missing_game_categories:
                    warnings.append(
                        tr(
                            common_filters.ui_locale.value,
                            "warning.plati.game_categories_not_found",
                        )
                    )

            scan_game_category_ids = game_category_ids if game_category_ids else [0]
            for game_category_id in scan_game_category_ids:
                loaded_raw_count = 0
                game_total_count: int | None = None
                max_pages = max(1, min(100, (max_offers_per_game + 23) // 24))
                try:
                    for page in range(1, max_pages + 1):
                        total_count, _, _, page_items = client.fetch_game_offers_page(
                            game_id=category_game_id,
                            category_id=game_category_id,
                            currency=common_filters.currency.value,
                            page=page,
                            rows=24,
                            sort="",
                            lang=("en" if common_filters.ui_locale.value == "en" else "ru"),
                        )
                        if game_total_count is None:
                            game_total_count = total_count
                        if not page_items:
                            break
                        loaded_raw_count += len(page_items)
                        for offer in page_items:
                            offer.section_id = category_game_id
                            offer.section_slug = f"game/{category_game_id}/cat/{game_category_id}"
                            if qtokens and not is_text_relevant(offer.title, qtokens):
                                continue
                            game_offers[offer.offer_id] = offer
                        if loaded_raw_count >= max_offers_per_game:
                            break
                        if game_total_count is not None and loaded_raw_count >= game_total_count:
                            break
                except Exception:
                    failed_games += 1
                    warnings.append(
                        tr(
                            common_filters.ui_locale.value,
                            "warning.plati.game_failed",
                            game=game_name,
                        )
                    )
                    continue

                category_meta = game_category_by_id.get(game_category_id)
                if category_meta is not None:
                    category_name = category_meta.category_name
                elif game_category_id == 0:
                    category_name = "All offers" if common_filters.ui_locale.value == "en" else "Все предложения"
                else:
                    category_name = (
                        f"Game category #{game_category_id}"
                        if common_filters.ui_locale.value == "en"
                        else f"Категория игры #{game_category_id}"
                    )
                category_url = (
                    game_url if game_category_id == 0 else f"{game_url}?id_c={game_category_id}"
                )
                section_rows.append(
                    {
                        "section_url": category_url,
                        "section_id": f"game:{category_game_id}:cat:{game_category_id}",
                        "section_name": (
                            f"{'Game' if common_filters.ui_locale.value == 'en' else 'Игра'}: {game_name} / "
                            f"{category_name}"
                        ),
                        "counter_total": game_total_count,
                        "loaded_count": loaded_raw_count,
                        "coverage_status": self._coverage_status(game_total_count, loaded_raw_count),
                    }
                )

            offers.extend(game_offers.values())

        for section in scope_sections:
            try:
                total_count, _, _ = client.fetch_section_stats(
                    section_id=section.section_id,
                    currency=common_filters.currency.value,
                )
            except Exception:
                total_count = None
            loaded_raw_count = 0
            section_offers: dict[str, PlatiOfferCard] = {}
            max_pages = max(1, min(100, (max_offers_per_section + 9) // 10))
            if total_count is not None:
                max_pages = min(max_pages, max(1, (total_count + 9) // 10))

            try:
                for page in range(1, max_pages + 1):
                    page_items = client.fetch_section_offers_page(
                        section_id=section.section_id,
                        section_slug=section.section_slug,
                        currency=common_filters.currency.value,
                        page=page,
                        rows=10,
                    )
                    if not page_items:
                        break
                    loaded_raw_count += len(page_items)
                    for offer in page_items:
                        offer.section_id = section.section_id
                        offer.section_slug = section.section_slug
                        if qtokens and not is_text_relevant(offer.title, qtokens):
                            continue
                        section_offers[offer.offer_id] = offer
                    if loaded_raw_count >= max_offers_per_section:
                        break
            except Exception:
                failed_sections += 1
                warnings.append(
                    tr(
                        common_filters.ui_locale.value,
                        "warning.plati.section_failed",
                        section=section.full_name,
                    )
                )
                continue

            coverage_status = self._coverage_status(total_count, loaded_raw_count)
            section_rows.append(
                {
                    "section_url": section.section_url,
                    "section_id": str(section.section_id),
                    "section_name": section.full_name,
                    "counter_total": total_count,
                    "loaded_count": loaded_raw_count,
                    "coverage_status": coverage_status,
                }
            )
            offers.extend(section_offers.values())

        deduped = {item.offer_id: item for item in offers}
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
        sold_parsing_diagnostics = self._sold_parsing_diagnostics(offers)

        enrich_limit = max(1, min(options.enrich_limit, len(offers)))
        for offer in offers[:enrich_limit]:
            try:
                details = client.fetch_offer_details(offer.offer_url)
            except Exception:
                continue
            title = details.get("title")
            if isinstance(title, str) and title.strip():
                offer.title = title.strip()
            seller_id = details.get("seller_id")
            if isinstance(seller_id, str) and seller_id.strip():
                offer.seller_id = seller_id.strip()
            seller_name = details.get("seller_name")
            if isinstance(seller_name, str) and seller_name.strip():
                offer.seller_name = seller_name.strip()
            seller_url = details.get("seller_url")
            if isinstance(seller_url, str) and seller_url.strip():
                offer.seller_url = seller_url.strip()
            reviews_count = details.get("reviews_count")
            if isinstance(reviews_count, int):
                offer.reviews_count = reviews_count
            seller_wmid = details.get("seller_wmid")
            if isinstance(seller_wmid, str) and seller_wmid.strip():
                offer.seller_wmid = seller_wmid.strip()
            sold_text = details.get("sold_text")
            if isinstance(sold_text, str) and sold_text.strip():
                offer.sold_text = sold_text.strip()
            sold_count = details.get("sold_count")
            if isinstance(sold_count, int) and sold_count >= 0:
                offer.sold_count = sold_count
            sold_is_lower_bound = details.get("sold_is_lower_bound")
            if isinstance(sold_is_lower_bound, bool):
                offer.sold_is_lower_bound = sold_is_lower_bound

        stats = self._offers_stats(offers)
        lower_bound_sections = sum(1 for row in section_rows if row["coverage_status"] == "lower_bound")
        coverage_note = tr(common_filters.ui_locale.value, "warning.coverage.lower_bound") if lower_bound_sections else None
        if coverage_note:
            warnings.append(coverage_note)
        if failed_sections > 0:
            warnings.append(
                tr(common_filters.ui_locale.value, "warning.plati.sections_failed_count", count=failed_sections)
            )
        if failed_games > 0:
            warnings.append(
                tr(common_filters.ui_locale.value, "warning.plati.games_failed_count", count=failed_games)
            )
        if not offers:
            warnings.append(
                tr(
                    common_filters.ui_locale.value,
                    "warning.offers.none_query" if query_value else "warning.offers.none_scope",
                )
            )

        seller_offers: dict[str, list[PlatiOfferCard]] = defaultdict(list)
        for offer in offers:
            seller_key = offer.seller_id or f"name:{offer.seller_name}"
            seller_offers[seller_key].append(offer)

        sellers_agg: list[NormalizedSellerDTO] = []
        seller_url_by_key: dict[str, str | None] = {}
        for seller_key, rows in seller_offers.items():
            prices = [float(item.price) for item in rows if item.price is not None]
            seller_id = rows[0].seller_id
            seller_name = rows[0].seller_name
            seller_url_by_key[seller_key] = rows[0].seller_url
            sellers_agg.append(
                NormalizedSellerDTO(
                    marketplace=MarketplaceSlug.platimarket,
                    seller_id=seller_id,
                    seller_name=seller_name,
                    offers_count=len(rows),
                    min_price=min(prices) if prices else None,
                    p50_price=self._percentile(prices, 0.50),
                    max_price=max(prices) if prices else None,
                    online_share=None,
                    auto_delivery_share=None,
                )
            )
        sellers_agg.sort(key=lambda item: (-item.offers_count, item.seller_name.lower()))
        purchases_from_sold_total = sum(max(0, int(item.sold_count or 0)) for item in offers)
        purchases_total_is_lower_bound = any(
            bool(item.sold_is_lower_bound) for item in offers if item.sold_count is not None
        )

        demand: DemandStatsV2DTO | None = None
        diagnostics = _ReviewDiagnostics()
        relevant_reviews: list[PlatiReview] = []
        top_demand_sellers: list[_SellerDemandStat] = []

        if options.include_reviews:
            current_offer_ids = {item.offer_id for item in offers}
            ranked_sellers = sorted(
                seller_offers.keys(),
                key=lambda key: (-len(seller_offers[key]), str(seller_offers[key][0].seller_name).lower()),
            )
            for seller_key in ranked_sellers[: options.seller_limit]:
                rows = seller_offers[seller_key]
                seller = rows[0]
                diagnostics.sellers_analyzed += 1
                if not seller.seller_id:
                    diagnostics.failed_sellers += 1
                    continue
                try:
                    fetched = client.fetch_seller_reviews(
                        seller_id=seller.seller_id,
                        max_pages=options.review_pages_per_seller,
                    )
                except Exception:
                    diagnostics.failed_sellers += 1
                    continue

                diagnostics.reviews_scanned += len(fetched)
                seller_offer_ids = {item.offer_id for item in rows}
                seller_sold_total = sum(max(0, int(item.sold_count or 0)) for item in rows)
                matched: list[PlatiReview] = []
                for review in fetched:
                    if not self._is_review_link_match(
                        review,
                        current_offer_ids=current_offer_ids,
                        seller_offer_ids=seller_offer_ids,
                    ):
                        continue
                    matched.append(review)
                    relevant_reviews.append(review)

                top_demand_sellers.append(
                    _SellerDemandStat(
                        seller_id=seller.seller_id,
                        seller_name=seller.seller_name,
                        seller_url=seller.seller_url,
                        estimated_purchases_total=seller_sold_total,
                        estimated_purchases_30d=sum(1 for item in matched if self._is_recent_30d(item)),
                        reviews_scanned=len(fetched),
                    )
                )

            demand = self._compute_demand(
                relevant_reviews,
                include_index=options.include_demand_index,
                sellers_analyzed=diagnostics.sellers_analyzed,
                reviews_scanned=diagnostics.reviews_scanned,
                purchases_from_sold_total=purchases_from_sold_total,
                purchases_total_is_lower_bound=purchases_total_is_lower_bound,
            )
            if diagnostics.failed_sellers > 0:
                warnings.append(
                    tr(
                        common_filters.ui_locale.value,
                        "warning.reviews.failed_sellers",
                        count=diagnostics.failed_sellers,
                    )
                )
            if demand.relevant_reviews == 0:
                warnings.append(tr(common_filters.ui_locale.value, "warning.reviews.none_relevant"))
        else:
            demand = self._compute_demand(
                [],
                include_index=False,
                sellers_analyzed=0,
                reviews_scanned=0,
                purchases_from_sold_total=purchases_from_sold_total,
                purchases_total_is_lower_bound=purchases_total_is_lower_bound,
            )

        top_demand_sellers.sort(
            key=lambda item: (-item.estimated_purchases_30d, -item.estimated_purchases_total, -item.reviews_scanned)
        )
        competition = self._compute_competition_metrics(offers)
        history_points = self._history_points(
            self.db,
            query=common_filters.query,
            currency=common_filters.currency.value,
            limit=options.history_points_limit,
        )

        core_offers = [
            NormalizedOfferDTO(
                marketplace=MarketplaceSlug.platimarket,
                offer_id=item.offer_id,
                offer_url=item.offer_url,
                section_id=(str(item.section_id) if item.section_id is not None else None),
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
                sold_is_lower_bound=item.sold_is_lower_bound,
            )
            for item in offers
        ]
        core_reviews = [
            NormalizedReviewDTO(
                marketplace=MarketplaceSlug.platimarket,
                seller_id=item.seller_id,
                detail=item.detail,
                text=item.text,
                rating=item.rating,
                date_bucket=item.date_bucket,
                is_relevant=True,
            )
            for item in relevant_reviews
        ]

        summary = MarketplaceSummaryDTO(
            marketplace=MarketplaceSlug.platimarket,
            label=self.label,
            status="done",
            request_id=str(uuid4()),
            generated_at=generated_at,
            valid_until=valid_until,
            cache_hit=False,
            ui_locale=common_filters.ui_locale,
            content_locale_requested="auto",
            content_locale_applied=("ru" if common_filters.currency.value == "RUB" else "en"),
            platimarket_game_id=category_game_id,
            platimarket_game_slug=category_game_slug,
            platimarket_game_name=category_game_name,
            platimarket_game_category_ids=game_category_ids,
            platimarket_group_id=category_group_id,
            platimarket_category_ids=sorted(set(category_ids)),
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
                                "sold_is_lower_bound": item.sold_is_lower_bound,
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
                    "competition": competition,
                    "diagnostics": {
                        "sold_parsing": sold_parsing_diagnostics,
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
            marketplace=MarketplaceSlug.platimarket,
            total=len(filtered),
            limit=safe_limit,
            offset=safe_offset,
            items=items,
        )

    @classmethod
    def _clone_tree_nodes(cls, rows: list[PlatiCatalogTreeNodeDTO]) -> list[PlatiCatalogTreeNodeDTO]:
        return [PlatiCatalogTreeNodeDTO.model_validate(item.model_dump(mode="json")) for item in rows]

    @classmethod
    def _tree_from_client_node(cls, node: PlatiCatalogNode) -> PlatiCatalogTreeNodeDTO:
        return PlatiCatalogTreeNodeDTO(
            section_id=node.section_id,
            section_slug=node.section_slug,
            title=node.title,
            cnt=node.cnt,
            path=list(node.path),
            url=node.url,
            children=[cls._tree_from_client_node(child) for child in node.children],
        )

    @staticmethod
    def _flatten_tree_nodes(node: PlatiCatalogTreeNodeDTO) -> list[PlatiCatalogTreeNodeDTO]:
        rows = [node]
        for child in node.children:
            rows.extend(PlatiMarketProvider._flatten_tree_nodes(child))
        return rows

    def catalog_tree(
        self,
        *,
        common_filters: CommonFiltersDTO | None = None,
        force_refresh: bool = False,
        with_source: bool = False,
    ) -> list[PlatiCatalogTreeNodeDTO] | tuple[list[PlatiCatalogTreeNodeDTO], str]:
        filters = common_filters or CommonFiltersDTO()
        now = datetime.now(UTC)
        cache_items = type(self)._catalog_tree_cache
        cache_expires_at = type(self)._catalog_tree_cache_expires_at
        if (
            not force_refresh
            and cache_items is not None
            and cache_expires_at is not None
            and cache_expires_at > now
        ):
            cached = self._clone_tree_nodes(cache_items)
            if with_source:
                return cached, "cache"
            return cached

        client = self._build_client({}, filters)
        rows = [self._tree_from_client_node(item) for item in client.fetch_catalog_tree()]
        type(self)._catalog_tree_cache = self._clone_tree_nodes(rows)
        type(self)._catalog_tree_cache_expires_at = now + timedelta(hours=self.settings.cache_ttl_hours)
        if with_source:
            return rows, "network"
        return rows

    def categories(
        self,
        *,
        common_filters: CommonFiltersDTO | None = None,
        force_refresh: bool = False,
        with_source: bool = False,
    ) -> list[PlatiCategoryGroupDTO] | tuple[list[PlatiCategoryGroupDTO], str]:
        tree_payload = self.catalog_tree(
            common_filters=common_filters,
            force_refresh=force_refresh,
            with_source=True,
        )
        tree, source = tree_payload if isinstance(tree_payload, tuple) else (tree_payload, "network")
        groups: list[PlatiCategoryGroupDTO] = []
        for root in tree:
            flat = self._flatten_tree_nodes(root)
            sections: list[PlatiCategorySectionDTO] = []
            seen: set[int] = set()
            for node in flat:
                if node.section_id in seen:
                    continue
                seen.add(node.section_id)
                full_name = " > ".join(node.path) if node.path else node.title
                sections.append(
                    PlatiCategorySectionDTO(
                        section_id=node.section_id,
                        section_slug=node.section_slug,
                        section_url=node.url,
                        section_name=node.title,
                        full_name=full_name,
                        counter_total=node.cnt,
                        group_id=root.section_id,
                    )
                )
            groups.append(
                PlatiCategoryGroupDTO(
                    group_id=root.section_id,
                    group_slug=root.section_slug,
                    group_url=root.url,
                    group_name=root.title,
                    sections=sorted(sections, key=lambda item: item.full_name.lower()),
                )
            )
        result = sorted(groups, key=lambda item: item.group_name.lower())
        if with_source:
            return result, source
        return result

    def games(
        self,
        *,
        common_filters: CommonFiltersDTO | None = None,
        force_refresh: bool = False,
        with_source: bool = False,
    ) -> list[PlatiGameDTO] | tuple[list[PlatiGameDTO], str]:
        filters = common_filters or CommonFiltersDTO()
        now = datetime.now(UTC)
        cache_items = type(self)._games_cache
        cache_expires_at = type(self)._games_cache_expires_at
        if (
            not force_refresh
            and cache_items is not None
            and cache_expires_at is not None
            and cache_expires_at > now
        ):
            cached = [
                PlatiGameDTO.model_validate(item.model_dump(mode="json"))
                for item in cache_items
            ]
            if with_source:
                return cached, "cache"
            return cached

        client = self._build_client({}, filters)
        games_raw: list[PlatiGame] = client.fetch_games_catalog(lang="ru")
        result = [
            PlatiGameDTO(
                game_id=item.game_id,
                game_slug=item.game_slug,
                game_url=item.game_url,
                game_name=item.game_name,
            )
            for item in games_raw
        ]
        type(self)._games_cache = [
            PlatiGameDTO.model_validate(item.model_dump(mode="json"))
            for item in result
        ]
        type(self)._games_cache_expires_at = now + timedelta(hours=self.settings.cache_ttl_hours)
        if with_source:
            return result, "network"
        return result

    def game_categories(
        self,
        *,
        game_id: int | None = None,
        game_slug: str | None = None,
        ui_locale: str = "ru",
        common_filters: CommonFiltersDTO | None = None,
        force_refresh: bool = False,
    ) -> tuple[int | None, str | None, list[PlatiGameCategoryDTO]]:
        filters = common_filters or CommonFiltersDTO(ui_locale=ui_locale)
        resolved_game_id = game_id if isinstance(game_id, int) and game_id > 0 else None
        resolved_game_slug = (
            str(game_slug).strip().strip("/").lower()
            if isinstance(game_slug, str) and str(game_slug).strip()
            else None
        )

        if resolved_game_id is None or resolved_game_slug is None:
            try:
                games = self.games(common_filters=filters, force_refresh=force_refresh)
            except Exception:
                games = []
            if resolved_game_id is not None and resolved_game_slug is None:
                matched = next((item for item in games if item.game_id == resolved_game_id), None)
                if matched is not None:
                    resolved_game_slug = matched.game_slug
            if resolved_game_slug is not None and resolved_game_id is None:
                matched = next((item for item in games if item.game_slug == resolved_game_slug), None)
                if matched is not None:
                    resolved_game_id = matched.game_id

        if resolved_game_id is None:
            return None, resolved_game_slug, []

        client = self._build_client({}, filters)
        rows = client.fetch_game_categories(
            game_id=resolved_game_id,
            game_slug=resolved_game_slug,
            lang=("en" if ui_locale == "en" else "ru"),
        )
        return (
            resolved_game_id,
            resolved_game_slug,
            [
                PlatiGameCategoryDTO(
                    category_id=item.category_id,
                    category_name=item.category_name,
                    offers_count=item.offers_count,
                )
                for item in rows
            ],
        )
