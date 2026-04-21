from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.models import AnalysisHistory, ReviewSnapshot
from app.schemas.analyze import AnalyzeRequestDTO, CategoryGameDTO, CategorySectionDTO
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
)
from app.services.analyzer import AnalyzerService
from app.services.funpay_client import FunPayClient
from app.services.marketplaces.base import MarketplaceProvider
from app.services.network_settings import NetworkSettingsService
from app.services.proxy_utils import normalize_proxy_list
from app.services.text_utils import normalize_text


class FunPayProvider(MarketplaceProvider):
    slug = MarketplaceSlug.funpay
    label = "FunPay"

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

    def _build_client(self, filters: dict, common_filters: CommonFiltersDTO) -> FunPayClient:
        resolved, allow_direct = self._resolve_proxy_pool(filters, common_filters)
        return FunPayClient(
            settings=self.settings,
            datacenter_proxies=normalize_proxy_list(resolved.datacenter),
            residential_proxies=normalize_proxy_list(resolved.residential),
            mobile_proxies=normalize_proxy_list(resolved.mobile),
            allow_direct_fallback=allow_direct,
        )

    def _build_legacy_request(
        self,
        common_filters: CommonFiltersDTO,
        filters: dict,
        *,
        datacenter_proxies: list[str],
        residential_proxies: list[str],
        mobile_proxies: list[str],
    ) -> AnalyzeRequestDTO:
        return AnalyzeRequestDTO(
            query=common_filters.query,
            force_refresh=common_filters.force_refresh,
            currency=common_filters.currency,
            ui_locale=common_filters.ui_locale,
            content_locale=filters.get("content_locale", "auto"),
            category_game_id=filters.get("category_game_id"),
            category_id=filters.get("category_id"),
            category_ids=filters.get("category_ids") or [],
            options=filters.get("options", {}),
            datacenter_proxies=datacenter_proxies,
            residential_proxies=residential_proxies,
            mobile_proxies=mobile_proxies,
        )

    @staticmethod
    def _map_offers_stats(payload: object) -> OffersStatsV2DTO:
        data = payload if isinstance(payload, dict) else {}
        return OffersStatsV2DTO(
            matched_offers=int(data.get("matched_offers", 0)),
            unique_sellers=int(data.get("unique_sellers", 0)),
            min_price=data.get("min_price"),
            avg_price=data.get("avg_price"),
            p50_price=data.get("p50_price"),
            p90_price=data.get("p90_price"),
            max_price=data.get("max_price"),
            online_share=data.get("online_share"),
            auto_delivery_share=data.get("auto_delivery_share"),
        )

    @staticmethod
    def _map_coverage(payload: object) -> CoverageV2DTO:
        data = payload if isinstance(payload, dict) else {}
        return CoverageV2DTO(
            sections_scanned=int(data.get("sections_scanned", 0)),
            sections_lower_bound=int(data.get("sections_lower_bound", 0)),
            coverage_note=data.get("coverage_note"),
        )

    @staticmethod
    def _map_demand(payload: object) -> DemandStatsV2DTO | None:
        if not isinstance(payload, dict):
            return None
        return DemandStatsV2DTO(
            relevant_reviews=int(payload.get("relevant_reviews", 0)),
            positive_share=float(payload.get("positive_share", 0.0)),
            volume_30d=int(payload.get("volume_30d", 0)),
            demand_index=payload.get("demand_index"),
            unique_sellers_with_relevant_reviews=int(payload.get("unique_sellers_with_relevant_reviews", 0)),
            estimated_purchases_total=int(payload.get("estimated_purchases_total", 0)),
            estimated_purchases_30d=int(payload.get("estimated_purchases_30d", 0)),
            sellers_analyzed=int(payload.get("sellers_analyzed", 0)),
            reviews_scanned=int(payload.get("reviews_scanned", 0)),
        )

    def _build_reviews(self, provider_request_id: str, limit: int = 400) -> list[NormalizedReviewDTO]:
        rows = self.db.scalars(
            select(ReviewSnapshot)
            .where(ReviewSnapshot.request_id == provider_request_id)
            .order_by(ReviewSnapshot.id.asc())
            .limit(limit)
        ).all()
        return [
            NormalizedReviewDTO(
                marketplace=MarketplaceSlug.funpay,
                seller_id=str(row.seller_id),
                detail=row.detail,
                text=row.text,
                rating=row.rating,
                date_bucket=row.date_bucket,
                is_relevant=bool(row.is_relevant),
            )
            for row in rows
        ]

    def _resolve_cache_source_request_id(
        self,
        *,
        query: str,
        currency: str,
        options_hash: str | None,
        generated_at: datetime | None,
    ) -> str | None:
        if not options_hash:
            return None
        rows = self.db.scalars(
            select(AnalysisHistory)
            .where(
                AnalysisHistory.query_normalized == normalize_text(query),
                AnalysisHistory.currency == currency,
                AnalysisHistory.options_hash == options_hash,
            )
            .order_by(desc(AnalysisHistory.generated_at))
            .limit(30)
        ).all()
        if not rows:
            return None
        if generated_at is None:
            return rows[0].request_id

        # generated_at приходит из кэша и может отличаться форматированием по timezone;
        # принимаем ближайшее совпадение в пределах одной секунды.
        for row in rows:
            try:
                delta = abs((row.generated_at - generated_at).total_seconds())
            except Exception:  # noqa: BLE001
                continue
            if delta <= 1.0:
                return row.request_id
        return rows[0].request_id

    def analyze(self, common_filters: CommonFiltersDTO, marketplace_filters: dict | None) -> MarketplaceRunResultDTO:
        filters = self._to_marketplace_filters(marketplace_filters)
        client = self._build_client(filters, common_filters)
        analyzer = AnalyzerService(db=self.db, client=client, settings=self.settings)
        resolved, _ = self._resolve_proxy_pool(filters, common_filters)
        legacy_request = self._build_legacy_request(
            common_filters,
            filters,
            datacenter_proxies=resolved.datacenter,
            residential_proxies=resolved.residential,
            mobile_proxies=resolved.mobile,
        )
        legacy_envelope = analyzer.analyze(legacy_request)
        if legacy_envelope.result is None:
            raise RuntimeError("FunPay анализ завершился без результата")

        result_raw = legacy_envelope.result.model_dump(mode="json")
        meta_raw = result_raw.get("meta", {})
        tables_raw = result_raw.get("tables", {})
        sellers_raw = tables_raw.get("top_sellers", []) if isinstance(tables_raw, dict) else []
        offers_slice = analyzer.list_request_offers(
            request_id=legacy_envelope.request_id,
            limit=5000,
            offset=0,
        )
        meta_generated_at_raw = meta_raw.get("generated_at")
        meta_generated_at: datetime | None = None
        if isinstance(meta_generated_at_raw, str):
            try:
                meta_generated_at = datetime.fromisoformat(meta_generated_at_raw.replace("Z", "+00:00"))
            except ValueError:
                meta_generated_at = None
        options_hash = None
        if isinstance(meta_raw.get("effective_options"), dict):
            options_hash = meta_raw.get("effective_options", {}).get("options_hash")
        source_request_id = legacy_envelope.request_id
        if legacy_envelope.cache_hit and offers_slice.total == 0:
            inferred_source_id = self._resolve_cache_source_request_id(
                query=common_filters.query,
                currency=common_filters.currency.value,
                options_hash=str(options_hash) if options_hash else None,
                generated_at=meta_generated_at,
            )
            if inferred_source_id:
                fallback_slice = analyzer.list_request_offers(
                    request_id=inferred_source_id,
                    limit=5000,
                    offset=0,
                )
                if fallback_slice.total > 0:
                    offers_slice = fallback_slice
                    source_request_id = inferred_source_id
        core_offers = [
            NormalizedOfferDTO(
                marketplace=MarketplaceSlug.funpay,
                offer_id=str(item.offer_id),
                offer_url=item.offer_url,
                section_id=(str(item.section_id) if item.section_id is not None else None),
                seller_id=(str(item.seller_id) if item.seller_id is not None else None),
                seller_name=item.seller_name,
                seller_url=(
                    f"https://funpay.com/users/{item.seller_id}/"
                    if item.seller_id is not None
                    else None
                ),
                description=item.description,
                price=item.price,
                currency=item.currency,
                reviews_count=item.reviews_count,
                is_online=item.is_online,
                auto_delivery=item.auto_delivery,
            )
            for item in offers_slice.items
        ]
        core_sellers: list[NormalizedSellerDTO] = []
        if isinstance(sellers_raw, list):
            for row in sellers_raw:
                if not isinstance(row, dict):
                    continue
                core_sellers.append(
                    NormalizedSellerDTO(
                        marketplace=MarketplaceSlug.funpay,
                        seller_id=(
                            str(row.get("seller_id"))
                            if row.get("seller_id") is not None
                            else None
                        ),
                        seller_name=str(row.get("seller_name", "")),
                        offers_count=int(row.get("offers_count", 0)),
                        min_price=row.get("min_price"),
                        p50_price=row.get("p50_price"),
                        max_price=row.get("max_price"),
                        online_share=row.get("online_share"),
                        auto_delivery_share=row.get("auto_delivery_share"),
                    )
                )

        core_reviews = self._build_reviews(source_request_id)
        summary = MarketplaceSummaryDTO(
            marketplace=MarketplaceSlug.funpay,
            label=self.label,
            status=legacy_envelope.status,
            request_id=legacy_envelope.request_id,
            generated_at=datetime.fromisoformat(str(meta_raw.get("generated_at")).replace("Z", "+00:00"))
            if isinstance(meta_raw.get("generated_at"), str)
            else datetime.now(UTC),
            valid_until=datetime.fromisoformat(str(meta_raw.get("valid_until")).replace("Z", "+00:00"))
            if isinstance(meta_raw.get("valid_until"), str)
            else datetime.now(UTC),
            cache_hit=legacy_envelope.cache_hit,
            ui_locale=meta_raw.get("ui_locale", "ru"),
            content_locale_requested=meta_raw.get("content_locale_requested"),
            content_locale_applied=meta_raw.get("content_locale_applied"),
            category_game_id=meta_raw.get("category_game_id"),
            category_id=meta_raw.get("category_id"),
            category_ids=meta_raw.get("category_ids") or [],
            offers_stats=self._map_offers_stats(result_raw.get("offers_stats")),
            coverage=self._map_coverage(result_raw.get("coverage")),
            demand=self._map_demand(result_raw.get("demand")),
            warnings=[str(item) for item in result_raw.get("warnings", []) if isinstance(item, str)],
        )
        return MarketplaceRunResultDTO(
            summary=summary,
            core=MarketplaceCoreDTO(
                offers=core_offers,
                sellers=core_sellers,
                reviews=core_reviews,
            ),
            raw={
                "provider_request_id": legacy_envelope.request_id,
                "source_request_id": source_request_id,
                "legacy_result": result_raw,
            },
        )

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
        normalized_query = seller_query.strip().lower() if seller_query else None
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
            if normalized_query:
                seller_id_text = "" if item.seller_id is None else str(item.seller_id)
                if normalized_query not in item.seller_name.lower() and normalized_query not in seller_id_text:
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
        sliced = filtered[safe_offset : safe_offset + safe_limit]
        return MarketplaceOffersResponseDTO(
            run_id=run_result.summary.request_id,
            marketplace=MarketplaceSlug.funpay,
            total=len(filtered),
            limit=safe_limit,
            offset=safe_offset,
            items=sliced,
        )

    def categories(self, *, common_filters: CommonFiltersDTO | None = None) -> list[CategoryGameDTO]:
        filters = common_filters or CommonFiltersDTO()
        resolved, allow_direct = self._resolve_proxy_pool({}, filters)
        client = FunPayClient(
            settings=self.settings,
            datacenter_proxies=normalize_proxy_list(resolved.datacenter),
            residential_proxies=normalize_proxy_list(resolved.residential),
            mobile_proxies=normalize_proxy_list(resolved.mobile),
            allow_direct_fallback=allow_direct,
        )
        games = client.get_categories_catalog()
        return [
            CategoryGameDTO(
                game_section_id=game.game_section_id,
                game_url=game.game_url,
                game_name=game.game_name,
                sections=[
                    CategorySectionDTO(
                        section_id=section.section_id,
                        section_url=section.section_url,
                        section_name=section.section_name,
                        full_name=section.full_name,
                    )
                    for section in game.sections
                ],
            )
            for game in games
        ]
