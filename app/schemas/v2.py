from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field

from app.schemas.analyze import AnalyzeOptionsDTO, ContentLocale, Currency, UiLocale


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
    ui_locale: UiLocale = UiLocale.ru
    force_refresh: bool = False
    allow_direct_fallback: bool = False
    execution: V2ExecutionMode = V2ExecutionMode.auto
    datacenter_proxies: list[str] | None = None
    residential_proxies: list[str] | None = None
    mobile_proxies: list[str] | None = None


class FunPayFiltersDTO(BaseModel):
    content_locale: ContentLocale = ContentLocale.auto
    category_game_id: int | None = Field(default=None, ge=1)
    category_id: int | None = Field(default=None, ge=1)
    category_ids: list[int] = Field(default_factory=list)
    options: AnalyzeOptionsDTO = Field(default_factory=AnalyzeOptionsDTO)
    datacenter_proxies: list[str] | None = None
    residential_proxies: list[str] | None = None
    mobile_proxies: list[str] | None = None


class PlayerOkFiltersDTO(BaseModel):
    category_game_slug: str | None = None
    category_slugs: list[str] = Field(default_factory=list)
    use_game_scope: bool = True
    use_html_degrade: bool = True
    advanced_headers: dict[str, str] | None = None
    advanced_cookies: dict[str, str] | None = None
    options: AnalyzeOptionsDTO = Field(default_factory=AnalyzeOptionsDTO)
    datacenter_proxies: list[str] | None = None
    residential_proxies: list[str] | None = None
    mobile_proxies: list[str] | None = None


class PlatiMarketFiltersDTO(BaseModel):
    category_game_id: int | None = Field(default=None, ge=1)
    category_game_slug: str | None = None
    category_game_name: str | None = None
    game_category_ids: list[int] = Field(default_factory=list)
    category_group_id: int | None = Field(default=None, ge=1)
    category_ids: list[int] = Field(default_factory=list)
    use_game_scope: bool = True
    use_group_scope: bool = True
    options: AnalyzeOptionsDTO = Field(default_factory=AnalyzeOptionsDTO)
    datacenter_proxies: list[str] | None = None
    residential_proxies: list[str] | None = None
    mobile_proxies: list[str] | None = None


class GgSellFiltersDTO(BaseModel):
    category_type_slug: str | None = None
    category_slugs: list[str] = Field(default_factory=list)
    use_type_scope: bool = True
    options: AnalyzeOptionsDTO = Field(default_factory=AnalyzeOptionsDTO)
    datacenter_proxies: list[str] | None = None
    residential_proxies: list[str] | None = None
    mobile_proxies: list[str] | None = None


class MarketplaceFiltersDTO(BaseModel):
    funpay: FunPayFiltersDTO | None = None
    playerok: PlayerOkFiltersDTO | None = None
    platimarket: PlatiMarketFiltersDTO | None = None
    ggsell: GgSellFiltersDTO | None = None


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
    offer_id: str
    offer_url: str
    section_id: str | None = None
    seller_id: str | None = None
    seller_name: str
    seller_url: str | None = None
    description: str
    price: float
    currency: str
    reviews_count: int | None = None
    is_online: bool | None = None
    auto_delivery: bool | None = None
    sold_count: int | None = None
    sold_text: str | None = None
    sold_is_lower_bound: bool | None = None


class NormalizedSellerDTO(BaseModel):
    marketplace: MarketplaceSlug
    seller_id: str | None = None
    seller_name: str
    offers_count: int
    min_price: float | None = None
    p50_price: float | None = None
    max_price: float | None = None
    online_share: float | None = None
    auto_delivery_share: float | None = None


class NormalizedReviewDTO(BaseModel):
    marketplace: MarketplaceSlug
    seller_id: str
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
    estimated_purchases_total: int = 0
    estimated_purchases_30d: int = 0
    sellers_analyzed: int = 0
    reviews_scanned: int = 0
    purchases_from_sold_total: int = 0
    purchases_from_reviews_total: int = 0
    purchases_from_reviews_30d: int = 0
    purchases_total_is_lower_bound: bool = False


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
    ui_locale: UiLocale = UiLocale.ru
    content_locale_requested: str | None = None
    content_locale_applied: str | None = None
    category_game_id: int | None = None
    category_id: int | None = None
    category_ids: list[int] = Field(default_factory=list)
    category_game_slug: str | None = None
    category_slugs: list[str] = Field(default_factory=list)
    ggsell_type_slug: str | None = None
    ggsell_category_slugs: list[str] = Field(default_factory=list)
    platimarket_game_id: int | None = None
    platimarket_game_slug: str | None = None
    platimarket_game_name: str | None = None
    platimarket_game_category_ids: list[int] = Field(default_factory=list)
    platimarket_group_id: int | None = None
    platimarket_category_ids: list[int] = Field(default_factory=list)
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
    capabilities: list[str] = Field(default_factory=list)
    data_source: str | None = None
    demand_mode: str | None = None


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
    ui_locale: UiLocale = UiLocale.ru
    generated_at: datetime
    marketplaces: list[MarketplaceSlug]
    category_game_id: int | None = None
    category_id: int | None = None
    category_ids: list[int] = Field(default_factory=list)
    category_game_slug: str | None = None
    category_slugs: list[str] = Field(default_factory=list)
    ggsell_type_slug: str | None = None
    ggsell_category_slugs: list[str] = Field(default_factory=list)
    platimarket_game_id: int | None = None
    platimarket_game_slug: str | None = None
    platimarket_game_name: str | None = None
    platimarket_game_category_ids: list[int] = Field(default_factory=list)
    platimarket_group_id: int | None = None
    platimarket_category_ids: list[int] = Field(default_factory=list)
    pooled_matched_offers: int = 0
    pooled_unique_sellers: int = 0
    pooled_p50_price: float | None = None
    marketplace_items: list[HistoryMarketplaceItemDTO] = Field(default_factory=list)


class HistoryV2ResponseDTO(BaseModel):
    generated_at: datetime
    items: list[HistoryRunItemDTO]


class PlayerOkCategorySectionDTO(BaseModel):
    section_id: str | None = None
    section_slug: str
    section_url: str
    section_name: str
    full_name: str


class PlayerOkCategoryGameDTO(BaseModel):
    game_id: str | None = None
    game_slug: str
    game_url: str
    game_name: str
    sections_loaded: bool = False
    sections: list[PlayerOkCategorySectionDTO]


class PlayerOkCategoriesResponseDTO(BaseModel):
    generated_at: datetime
    source: str = "network"
    games: list[PlayerOkCategoryGameDTO]


class GgSellCategoryTypeDTO(BaseModel):
    type_slug: str
    type_name: str
    category_url: str | None = None
    icon_alias: str | None = None


class GgSellCategoryDTO(BaseModel):
    category_slug: str
    category_name: str
    category_url: str
    type_slug: str | None = None
    type_name: str | None = None
    parent_slug: str | None = None
    parent_name: str | None = None
    digi_catalog: int | None = None
    offers_count: int | None = None


class GgSellCategoriesResponseDTO(BaseModel):
    generated_at: datetime
    source: str = "network"
    types: list[GgSellCategoryTypeDTO] = Field(default_factory=list)
    categories: list[GgSellCategoryDTO] = Field(default_factory=list)


class PlatiCategorySectionDTO(BaseModel):
    section_id: int
    section_slug: str
    section_url: str
    section_name: str
    full_name: str
    counter_total: int | None = None
    group_id: int | None = None


class PlatiCategoryGroupDTO(BaseModel):
    group_id: int
    group_slug: str
    group_url: str
    group_name: str
    sections: list[PlatiCategorySectionDTO]


class PlatiCategoriesResponseDTO(BaseModel):
    generated_at: datetime
    source: str = "network"
    groups: list[PlatiCategoryGroupDTO]


class PlatiGameDTO(BaseModel):
    game_id: int
    game_slug: str
    game_url: str
    game_name: str


class PlatiGameCategoryDTO(BaseModel):
    category_id: int
    category_name: str
    offers_count: int | None = None


class PlatiGamesResponseDTO(BaseModel):
    generated_at: datetime
    source: str = "network"
    games: list[PlatiGameDTO]


class PlatiGameCategoriesResponseDTO(BaseModel):
    generated_at: datetime
    source: str = "network"
    game_id: int | None = None
    game_slug: str | None = None
    categories: list[PlatiGameCategoryDTO]


class PlatiCatalogTreeNodeDTO(BaseModel):
    section_id: int
    section_slug: str
    title: str
    cnt: int | None = None
    path: list[str] = Field(default_factory=list)
    url: str
    children: list["PlatiCatalogTreeNodeDTO"] = Field(default_factory=list)


class PlatiCatalogTreeResponseDTO(BaseModel):
    generated_at: datetime
    source: str = "network"
    nodes: list[PlatiCatalogTreeNodeDTO] = Field(default_factory=list)


class NetworkSettingsDTO(BaseModel):
    datacenter_proxies: list[str] = Field(default_factory=list)
    residential_proxies: list[str] = Field(default_factory=list)
    mobile_proxies: list[str] = Field(default_factory=list)
    updated_at: datetime | None = None


PlatiCatalogTreeNodeDTO.model_rebuild()
