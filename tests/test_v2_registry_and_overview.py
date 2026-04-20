from datetime import UTC, datetime, timedelta

from app.schemas.v2 import (
    AnalyzeV2RequestDTO,
    CoverageV2DTO,
    DemandStatsV2DTO,
    MarketplaceCoreDTO,
    MarketplaceRunResultDTO,
    MarketplaceSlug,
    MarketplaceSummaryDTO,
    NormalizedOfferDTO,
    OffersStatsV2DTO,
)
from app.services.global_analyzer import GlobalAnalyzerService
from app.services.marketplaces.registry import MarketplaceRegistry


def test_registry_contains_disabled_marketplaces() -> None:
    items = MarketplaceRegistry.catalog_dto()
    slugs = {item.slug.value: item for item in items}
    assert "funpay" in slugs and slugs["funpay"].enabled is True
    assert "playerok" in slugs and slugs["playerok"].enabled is True
    assert "ggsell" in slugs and slugs["ggsell"].enabled is False
    assert "platimarket" in slugs and slugs["platimarket"].enabled is False


def _mk_result(prices: list[float], matched: int, sellers: int, p50: float | None) -> MarketplaceRunResultDTO:
    now = datetime.now(UTC)
    return MarketplaceRunResultDTO(
        summary=MarketplaceSummaryDTO(
            marketplace=MarketplaceSlug.funpay,
            label="FunPay",
            status="done",
            request_id="provider-req-1",
            generated_at=now,
            valid_until=now + timedelta(hours=24),
            cache_hit=False,
            offers_stats=OffersStatsV2DTO(
                matched_offers=matched,
                unique_sellers=sellers,
                min_price=min(prices) if prices else None,
                avg_price=(sum(prices) / len(prices)) if prices else None,
                p50_price=p50,
                p90_price=max(prices) if prices else None,
                max_price=max(prices) if prices else None,
                online_share=None,
                auto_delivery_share=None,
            ),
            coverage=CoverageV2DTO(sections_scanned=1, sections_lower_bound=0, coverage_note=None),
            demand=DemandStatsV2DTO(
                relevant_reviews=0,
                positive_share=0.0,
                volume_30d=0,
                demand_index=None,
                unique_sellers_with_relevant_reviews=0,
            ),
            warnings=[],
        ),
        core=MarketplaceCoreDTO(
            offers=[
                NormalizedOfferDTO(
                    marketplace=MarketplaceSlug.funpay,
                    offer_id=str(idx + 1),
                    offer_url=f"https://funpay.com/lots/offer?id={idx + 1}",
                    section_id="1",
                    seller_id=str(100 + idx),
                    seller_name=f"seller_{idx}",
                    description="offer",
                    price=price,
                    currency="RUB",
                    reviews_count=10,
                    is_online=True,
                    auto_delivery=False,
                )
                for idx, price in enumerate(prices)
            ],
            sellers=[],
            reviews=[],
        ),
        raw={},
    )


def test_compute_overview_builds_pooled_and_aggregates() -> None:
    result = _mk_result(prices=[10.0, 20.0, 30.0], matched=3, sellers=3, p50=20.0)
    overview = GlobalAnalyzerService._compute_overview(
        selected_marketplaces=[MarketplaceSlug.funpay],
        marketplace_results={"funpay": result},
    )
    assert overview.pooled_offers_stats.matched_offers == 3
    assert overview.pooled_offers_stats.unique_sellers == 3
    assert overview.pooled_offers_stats.p50_price == 20.0
    assert overview.aggregates.avg_matched_offers == 3.0
    assert overview.aggregates.avg_unique_sellers == 3.0
    assert overview.aggregates.avg_p50_price == 20.0


def test_is_heavy_request_for_deep_funpay_profile() -> None:
    payload = AnalyzeV2RequestDTO.model_validate(
        {
            "marketplaces": ["funpay"],
            "common_filters": {
                "query": "zomboid",
                "currency": "RUB",
                "execution": "auto",
            },
            "marketplace_filters": {
                "funpay": {
                    "options": {
                        "profile": "deep",
                    }
                }
            },
        }
    )
    assert GlobalAnalyzerService.is_heavy_request(payload) is True
    payload.marketplace_filters.funpay.options.profile = "safe"
    payload.marketplace_filters.funpay.options.include_reviews = False
    payload.marketplace_filters.funpay.options.include_demand_index = False
    assert GlobalAnalyzerService.is_heavy_request(payload) is False


def test_is_heavy_request_for_playerok_reviews() -> None:
    payload = AnalyzeV2RequestDTO.model_validate(
        {
            "marketplaces": ["playerok"],
            "common_filters": {
                "query": "robux",
                "currency": "RUB",
                "execution": "auto",
            },
            "marketplace_filters": {
                "playerok": {
                    "category_game_slug": "roblox",
                    "options": {
                        "profile": "safe",
                        "include_reviews": True,
                    },
                }
            },
        }
    )
    assert GlobalAnalyzerService.is_heavy_request(payload) is True
