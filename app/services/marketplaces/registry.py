from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.orm import Session

from app.core.config import Settings
from app.schemas.v2 import MarketplaceCatalogItemDTO, MarketplaceSlug
from app.services.marketplaces.funpay_provider import FunPayProvider
from app.services.marketplaces.playerok_provider import PlayerOkProvider
from app.services.marketplaces.platimarket_provider import PlatiMarketProvider


@dataclass(frozen=True)
class MarketplaceInfo:
    slug: MarketplaceSlug
    label: str
    enabled: bool
    reason: str | None = None


class MarketplaceRegistry:
    CATALOG: tuple[MarketplaceInfo, ...] = (
        MarketplaceInfo(slug=MarketplaceSlug.funpay, label="FunPay", enabled=True),
        MarketplaceInfo(slug=MarketplaceSlug.playerok, label="PlayerOK", enabled=True),
        MarketplaceInfo(
            slug=MarketplaceSlug.ggsell,
            label="GGSell",
            enabled=False,
            reason="Скоро: провайдер еще не реализован",
        ),
        MarketplaceInfo(
            slug=MarketplaceSlug.platimarket,
            label="Plati.Market",
            enabled=True,
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
            )
            for item in cls.CATALOG
        ]
