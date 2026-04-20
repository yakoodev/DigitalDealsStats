from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field


class Currency(str, Enum):
    rub = "RUB"
    usd = "USD"
    eur = "EUR"


class ExecutionMode(str, Enum):
    auto = "auto"
    sync = "sync"
    async_mode = "async"


class AnalyzeProfile(str, Enum):
    safe = "safe"
    balanced = "balanced"
    deep = "deep"


class ContentLocale(str, Enum):
    auto = "auto"
    ru = "ru"
    en = "en"


class AnalyzeOptionsDTO(BaseModel):
    profile: AnalyzeProfile = AnalyzeProfile.balanced
    include_reviews: bool | None = None
    include_demand_index: bool | None = None
    include_fallback_scan: bool | None = None
    section_limit: int | None = Field(default=None, ge=1, le=500)
    seller_limit: int | None = Field(default=None, ge=1, le=500)
    review_pages_per_seller: int | None = Field(default=None, ge=1, le=20)
    history_points_limit: int | None = Field(default=None, ge=5, le=365)


class EffectiveAnalyzeOptionsDTO(BaseModel):
    profile: AnalyzeProfile
    include_reviews: bool
    include_demand_index: bool
    include_fallback_scan: bool
    section_limit: int
    seller_limit: int
    review_pages_per_seller: int
    history_points_limit: int
    mode_label: str
    options_hash: str


class AnalyzeRequestDTO(BaseModel):
    query: str = Field(min_length=0, max_length=512)
    force_refresh: bool = False
    currency: Currency = Currency.rub
    content_locale: ContentLocale = ContentLocale.auto
    execution: ExecutionMode = ExecutionMode.auto
    category_game_id: int | None = Field(default=None, ge=1)
    category_id: int | None = Field(default=None, ge=1)
    options: AnalyzeOptionsDTO = Field(default_factory=AnalyzeOptionsDTO)
    datacenter_proxies: list[str] | None = None
    residential_proxies: list[str] | None = None
    mobile_proxies: list[str] | None = None


class OffersStatsDTO(BaseModel):
    matched_offers: int
    unique_sellers: int
    min_price: float | None
    avg_price: float | None
    p50_price: float | None
    p90_price: float | None
    max_price: float | None
    online_share: float | None
    auto_delivery_share: float | None


class CoverageDTO(BaseModel):
    sections_scanned: int
    sections_lower_bound: int
    coverage_note: str | None = None


class DemandStatsDTO(BaseModel):
    relevant_reviews: int
    positive_share: float
    volume_30d: int
    demand_index: float | None
    unique_sellers_with_relevant_reviews: int


class PriceHistogramBinDTO(BaseModel):
    label: str
    from_price: float
    to_price: float
    count: int


class HistoryPointDTO(BaseModel):
    generated_at: datetime
    matched_offers: int
    unique_sellers: int
    p50_price: float | None
    demand_index: float | None


class DeltaDTO(BaseModel):
    matched_offers_delta: int | None = None
    unique_sellers_delta: int | None = None
    p50_price_delta: float | None = None
    demand_index_delta: float | None = None


class ChartsDTO(BaseModel):
    price_histogram: list[PriceHistogramBinDTO]
    history_points: list[HistoryPointDTO]
    delta_vs_previous: DeltaDTO | None


class TopOfferDTO(BaseModel):
    offer_id: int
    offer_url: str
    seller_id: int | None
    seller_name: str
    description: str
    price: float
    currency: str
    reviews_count: int | None
    is_online: bool | None
    auto_delivery: bool | None


class TopSellerDTO(BaseModel):
    seller_id: int | None
    seller_name: str
    offers_count: int
    min_price: float | None
    p50_price: float | None
    max_price: float | None
    online_share: float | None
    auto_delivery_share: float | None


class SectionRowDTO(BaseModel):
    section_url: str
    section_id: int | None
    counter_total: int | None
    loaded_count: int
    coverage_status: str


class TablesDTO(BaseModel):
    top_offers: list[TopOfferDTO]
    top_sellers: list[TopSellerDTO]
    sections: list[SectionRowDTO]


class AnalyzeMetaDTO(BaseModel):
    query: str
    currency: Currency
    content_locale_requested: ContentLocale = ContentLocale.auto
    content_locale_applied: str = "en"
    category_game_id: int | None = None
    category_id: int | None = None
    generated_at: datetime
    valid_until: datetime
    effective_options: EffectiveAnalyzeOptionsDTO


class AnalyzeResultDTO(BaseModel):
    meta: AnalyzeMetaDTO
    offers_stats: OffersStatsDTO
    coverage: CoverageDTO
    demand: DemandStatsDTO | None = None
    charts: ChartsDTO
    tables: TablesDTO
    warnings: list[str] = Field(default_factory=list)


class ProgressLogDTO(BaseModel):
    ts: datetime
    stage: str
    message: str


class AnalyzeProgressDTO(BaseModel):
    percent: float = 0.0
    stage: str | None = None
    message: str | None = None
    logs: list[ProgressLogDTO] = Field(default_factory=list)


class AnalyzeEnvelopeDTO(BaseModel):
    request_id: str
    status: str
    cache_hit: bool = False
    result: AnalyzeResultDTO | None = None
    error: str | None = None
    progress: AnalyzeProgressDTO | None = None


class CategorySectionDTO(BaseModel):
    section_id: int
    section_url: str
    section_name: str
    full_name: str


class CategoryGameDTO(BaseModel):
    game_section_id: int
    game_url: str
    game_name: str
    sections: list[CategorySectionDTO]


class CategoriesResponseDTO(BaseModel):
    generated_at: datetime
    games: list[CategoryGameDTO]


class HistoryItemDTO(BaseModel):
    request_id: str
    query: str
    currency: str
    generated_at: datetime
    category_game_id: int | None = None
    category_id: int | None = None
    matched_offers: int = 0
    unique_sellers: int = 0
    p50_price: float | None = None
    demand_index: float | None = None
    warnings_count: int = 0


class HistoryResponseDTO(BaseModel):
    generated_at: datetime
    items: list[HistoryItemDTO]


class OfferSnapshotDTO(BaseModel):
    offer_id: int
    offer_url: str
    section_id: int | None
    seller_id: int | None
    seller_name: str
    description: str
    price: float
    currency: str
    reviews_count: int | None
    seller_age: str | None
    is_online: bool | None
    auto_delivery: bool | None


class OffersSliceResponseDTO(BaseModel):
    request_id: str
    total: int
    limit: int
    offset: int
    items: list[OfferSnapshotDTO]
