from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.orm import Session

from app.core.config import Settings
from app.schemas.v2 import MarketplaceCatalogItemDTO, MarketplaceSlug
from app.services.marketplaces.funpay_provider import FunPayProvider
from app.services.marketplaces.ggsell_provider import GgSellProvider
from app.services.marketplaces.playerok_provider import PlayerOkProvider
from app.services.marketplaces.platimarket_provider import PlatiMarketProvider


@dataclass(frozen=True)
class MarketplaceInfo:
    slug: MarketplaceSlug
    label: str
    enabled: bool
    reason: str | None = None
    capabilities: tuple[str, ...] = ()
    data_source: str | None = None
    demand_mode: str | None = None


class MarketplaceRegistry:
    CATALOG: tuple[MarketplaceInfo, ...] = (
        MarketplaceInfo(
            slug=MarketplaceSlug.funpay,
            label="FunPay",
            enabled=True,
            capabilities=("offers", "coverage", "reviews", "demand_index", "history"),
            data_source="public_html",
            demand_mode="review_match_game_price",
        ),
        MarketplaceInfo(
            slug=MarketplaceSlug.playerok,
            label="PlayerOK",
            enabled=True,
            capabilities=("offers", "coverage", "reviews", "demand_index", "history", "graphql_first"),
            data_source="graphql+html_degrade",
            demand_mode="review_match_game_price",
        ),
        MarketplaceInfo(
            slug=MarketplaceSlug.ggsell,
            label="GGSell",
            enabled=True,
            capabilities=("offers", "coverage", "reviews", "demand_index", "history"),
            data_source="public_api+html_reviews",
            demand_mode="sold_total+reviews_30d",
        ),
        MarketplaceInfo(
            slug=MarketplaceSlug.platimarket,
            label="Plati.Market",
            enabled=True,
            capabilities=("offers", "coverage", "reviews", "demand_index", "history", "sold_count"),
            data_source="public_http_api+html",
            demand_mode="sold_total+reviews_30d",
        ),
    )

    def __init__(self, db: Session, settings: Settings) -> None:
        self.db = db
        self.settings = settings

    @classmethod
    def list_marketplaces(cls) -> list[MarketplaceInfo]:
        return list(cls.CATALOG)

    @classmethod
    def marketplace_info(cls, slug: MarketplaceSlug) -> MarketplaceInfo | None:
        for item in cls.CATALOG:
            if item.slug == slug:
                return item
        return None

    def provider_for(self, slug: MarketplaceSlug):
        info = self.marketplace_info(slug)
        if info is None:
            raise ValueError("Неизвестная площадка")
        if not info.enabled:
            reason = info.reason or "Площадка пока недоступна"
            raise ValueError(f"marketplace_not_available:{slug.value}:{reason}")
        if slug == MarketplaceSlug.funpay:
            return FunPayProvider(db=self.db, settings=self.settings)
        if slug == MarketplaceSlug.playerok:
            return PlayerOkProvider(db=self.db, settings=self.settings)
        if slug == MarketplaceSlug.ggsell:
            return GgSellProvider(db=self.db, settings=self.settings)
        if slug == MarketplaceSlug.platimarket:
            return PlatiMarketProvider(db=self.db, settings=self.settings)
        raise ValueError(f"marketplace_not_available:{slug.value}:Провайдер не реализован")

    @classmethod
    def catalog_dto(cls) -> list[MarketplaceCatalogItemDTO]:
        return [
            MarketplaceCatalogItemDTO(
                slug=item.slug,
                label=item.label,
                enabled=item.enabled,
                reason=item.reason,
                route_path=f"/analysis/{item.slug.value}",
                capabilities=list(item.capabilities),
                data_source=item.data_source,
                demand_mode=item.demand_mode,
            )
            for item in cls.CATALOG
        ]
