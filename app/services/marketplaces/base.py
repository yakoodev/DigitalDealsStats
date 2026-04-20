from __future__ import annotations

from abc import ABC, abstractmethod

from app.schemas.v2 import (
    CommonFiltersDTO,
    MarketplaceOffersResponseDTO,
    MarketplaceRunResultDTO,
    MarketplaceSlug,
)


class MarketplaceProvider(ABC):
    slug: MarketplaceSlug
    label: str

    @abstractmethod
    def analyze(self, common_filters: CommonFiltersDTO, marketplace_filters: dict | None) -> MarketplaceRunResultDTO:
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError
