from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field

from app.schemas.analyze import AnalyzeOptionsDTO, ContentLocale, Currency


class V2ExecutionMode(str, Enum):
    auto = "auto"
    sync = "sync"
    async_mode = "async"


class MarketplaceSlug(str, Enum):
    funpay = "funpay"
    playerok = "playerok"
    ggsell = "ggsell"
    platimarket = "platimarket"


class CommonFiltersDTO(BaseModel):
    query: str = Field(default="", min_length=0, max_length=512)
    currency: Currency = Currency.rub
    force_refresh: bool = False
    execution: V2ExecutionMode = V2ExecutionMode.auto


class FunPayFiltersDTO(BaseModel):
    content_locale: ContentLocale = ContentLocale.auto
    category_game_id: int | None = Field(default=None, ge=1)
    category_id: int | None = Field(default=None, ge=1)
    options: AnalyzeOptionsDTO = Field(default_factory=AnalyzeOptionsDTO)
    datacenter_proxies: list[str] | None = None
    residential_proxies: list[str] | None = None
    mobile_proxies: list[str] | None = None


class MarketplaceFiltersDTO(BaseModel):
    funpay: FunPayFiltersDTO | None = None


class AnalyzeV2RequestDTO(BaseModel):
    marketplaces: list[MarketplaceSlug] = Field(default_factory=lambda: [MarketplaceSlug.funpay], min_length=1)
    common_filters: CommonFiltersDTO = Field(default_factory=CommonFiltersDTO)
    marketplace_filters: MarketplaceFiltersDTO = Field(default_factory=MarketplaceFiltersDTO)


class ProgressLogV2DTO(BaseModel):
    ts: datetime
    stage: str
    message: str


class ProgressV2DTO(BaseModel):
    percent: float = 0.0
    stage: str | None = None
    message: str | None = None
    logs: list[ProgressLogV2DTO] = Field(default_factory=list)


class NormalizedOfferDTO(BaseModel):
    marketplace: MarketplaceSlug
    offer_id: int
    offer_url: str
    section_id: int | None = None
    seller_id: int | None = None
    seller_name: str
    description: str
    price: float
    currency: str
    reviews_count: int | None = None
    is_online: bool | None = None
    auto_delivery: bool | None = None


class NormalizedSellerDTO(BaseModel):
    marketplace: MarketplaceSlug
    seller_id: int | None = None
    seller_name: str
    offers_count: int
    min_price: float | None = None
    p50_price: float | None = None
    max_price: float | None = None
    online_share: float | None = None
    auto_delivery_share: float | None = None


class NormalizedReviewDTO(BaseModel):
    marketplace: MarketplaceSlug
    seller_id: int
    detail: str
    text: str
    rating: int | None = None
    date_bucket: str | None = None
    is_relevant: bool


class OffersStatsV2DTO(BaseModel):
    matched_offers: int
    unique_sellers: int
    min_price: float | None = None
    avg_price: float | None = None
    p50_price: float | None = None
    p90_price: float | None = None
    max_price: float | None = None
    online_share: float | None = None
    auto_delivery_share: float | None = None


class DemandStatsV2DTO(BaseModel):
    relevant_reviews: int
    positive_share: float
    volume_30d: int
    demand_index: float | None = None
    unique_sellers_with_relevant_reviews: int


class CoverageV2DTO(BaseModel):
    sections_scanned: int
    sections_lower_bound: int
    coverage_note: str | None = None


class MarketplaceSummaryDTO(BaseModel):
    marketplace: MarketplaceSlug
    label: str
    status: str
    request_id: str
    generated_at: datetime
    valid_until: datetime
    cache_hit: bool = False
    content_locale_requested: str | None = None
    content_locale_applied: str | None = None
    offers_stats: OffersStatsV2DTO
    coverage: CoverageV2DTO
    demand: DemandStatsV2DTO | None = None
    warnings: list[str] = Field(default_factory=list)


class MarketplaceCoreDTO(BaseModel):
    offers: list[NormalizedOfferDTO] = Field(default_factory=list)
    sellers: list[NormalizedSellerDTO] = Field(default_factory=list)
    reviews: list[NormalizedReviewDTO] = Field(default_factory=list)


class MarketplaceRunResultDTO(BaseModel):
    summary: MarketplaceSummaryDTO
    core: MarketplaceCoreDTO
    raw: dict = Field(default_factory=dict)


class MarketplaceComparisonDTO(BaseModel):
    marketplace: MarketplaceSlug
    label: str
    matched_offers: int
    unique_sellers: int
    p50_price: float | None = None
    demand_index: float | None = None


class OverviewAggregatesDTO(BaseModel):
    avg_matched_offers: float | None = None
    avg_unique_sellers: float | None = None
    avg_p50_price: float | None = None


class OverviewV2DTO(BaseModel):
    generated_at: datetime
    marketplaces: list[MarketplaceSlug]
    pooled_offers_stats: OffersStatsV2DTO
    comparison: list[MarketplaceComparisonDTO] = Field(default_factory=list)
    aggregates: OverviewAggregatesDTO


class AnalyzeV2EnvelopeDTO(BaseModel):
    run_id: str
    status: str
    error: str | None = None
    progress: ProgressV2DTO | None = None
    overview: OverviewV2DTO | None = None
    marketplaces: dict[str, MarketplaceSummaryDTO] = Field(default_factory=dict)


class MarketplaceOffersResponseDTO(BaseModel):
    run_id: str
    marketplace: MarketplaceSlug
    total: int
    limit: int
    offset: int
    items: list[NormalizedOfferDTO]


class MarketplaceCatalogItemDTO(BaseModel):
    slug: MarketplaceSlug
    label: str
    enabled: bool
    reason: str | None = None
    route_path: str


class MarketplacesCatalogResponseDTO(BaseModel):
    generated_at: datetime
    items: list[MarketplaceCatalogItemDTO]


class HistoryMarketplaceItemDTO(BaseModel):
    marketplace: MarketplaceSlug
    label: str
    matched_offers: int
    unique_sellers: int
    p50_price: float | None = None
    demand_index: float | None = None
    warnings_count: int = 0


class HistoryRunItemDTO(BaseModel):
    run_id: str
    query: str
    currency: Currency
    generated_at: datetime
    marketplaces: list[MarketplaceSlug]
    pooled_matched_offers: int = 0
    pooled_unique_sellers: int = 0
    pooled_p50_price: float | None = None
    marketplace_items: list[HistoryMarketplaceItemDTO] = Field(default_factory=list)


class HistoryV2ResponseDTO(BaseModel):
    generated_at: datetime
    items: list[HistoryRunItemDTO]
