from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.models import ReviewSnapshot
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


def _normalize_single_proxy(value: str) -> str:
    raw = value.strip()
    if not raw:
        return raw

    if raw.startswith(("http://", "https://", "socks5://", "socks5h://")):
        return raw

    if "@" in raw:
        left, right = raw.split("@", 1)
        left = left.strip()
        right = right.strip()
        left_parts = left.rsplit(":", 1)
        right_parts = right.split(":", 1)
        if len(left_parts) == 2 and left_parts[1].isdigit() and len(right_parts) == 2:
            user, password = right_parts
            host, port = left_parts
            if user and password and host and port:
                return f"http://{user}:{password}@{host}:{port}"
        return f"http://{left}@{right}"

    if ":" in raw:
        return f"http://{raw}"

    return raw


def _normalize_proxy_list(raw_values: list[str] | None) -> str | None:
    if raw_values is None:
        return None
    values = [_normalize_single_proxy(item) for item in raw_values if item and item.strip()]
    return ",".join(values)


class FunPayProvider(MarketplaceProvider):
    slug = MarketplaceSlug.funpay
    label = "FunPay"

    def __init__(self, db: Session, settings: Settings) -> None:
        self.db = db
        self.settings = settings

    @staticmethod
    def _to_marketplace_filters(raw_filters: dict | None) -> dict:
        if not isinstance(raw_filters, dict):
            return {}
        return raw_filters

    def _build_client(self, filters: dict) -> FunPayClient:
        return FunPayClient(
            settings=self.settings,
            datacenter_proxies=_normalize_proxy_list(filters.get("datacenter_proxies")),
            residential_proxies=_normalize_proxy_list(filters.get("residential_proxies")),
            mobile_proxies=_normalize_proxy_list(filters.get("mobile_proxies")),
        )

    def _build_legacy_request(self, common_filters: CommonFiltersDTO, filters: dict) -> AnalyzeRequestDTO:
        return AnalyzeRequestDTO(
            query=common_filters.query,
            force_refresh=common_filters.force_refresh,
            currency=common_filters.currency,
            content_locale=filters.get("content_locale", "auto"),
            category_game_id=filters.get("category_game_id"),
            category_id=filters.get("category_id"),
            options=filters.get("options", {}),
            datacenter_proxies=filters.get("datacenter_proxies"),
            residential_proxies=filters.get("residential_proxies"),
            mobile_proxies=filters.get("mobile_proxies"),
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
                seller_id=row.seller_id,
                detail=row.detail,
                text=row.text,
                rating=row.rating,
                date_bucket=row.date_bucket,
                is_relevant=bool(row.is_relevant),
            )
            for row in rows
        ]

    def analyze(self, common_filters: CommonFiltersDTO, marketplace_filters: dict | None) -> MarketplaceRunResultDTO:
        filters = self._to_marketplace_filters(marketplace_filters)
        client = self._build_client(filters)
        analyzer = AnalyzerService(db=self.db, client=client, settings=self.settings)
        legacy_request = self._build_legacy_request(common_filters, filters)
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
        core_offers = [
            NormalizedOfferDTO(
                marketplace=MarketplaceSlug.funpay,
                offer_id=item.offer_id,
                offer_url=item.offer_url,
                section_id=item.section_id,
                seller_id=item.seller_id,
                seller_name=item.seller_name,
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
                        seller_id=row.get("seller_id"),
                        seller_name=str(row.get("seller_name", "")),
                        offers_count=int(row.get("offers_count", 0)),
                        min_price=row.get("min_price"),
                        p50_price=row.get("p50_price"),
                        max_price=row.get("max_price"),
                        online_share=row.get("online_share"),
                        auto_delivery_share=row.get("auto_delivery_share"),
                    )
                )

        core_reviews = self._build_reviews(legacy_envelope.request_id)
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
            content_locale_requested=meta_raw.get("content_locale_requested"),
            content_locale_applied=meta_raw.get("content_locale_applied"),
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

    def categories(self) -> list[CategoryGameDTO]:
        client = FunPayClient(settings=self.settings)
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
