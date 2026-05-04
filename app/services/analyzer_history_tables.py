from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any, Callable

from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from app.models import AnalysisHistory
from app.schemas.analyze import (
    DeltaDTO,
    DemandStatsDTO,
    HistoryPointDTO,
    OffersStatsDTO,
    PriceHistogramBinDTO,
    SectionRowDTO,
    TopDemandSellerDTO,
    TopOfferDTO,
    TopSellerDTO,
)
from app.services.funpay_client import OfferData
from app.services.text_utils import normalize_text


def build_histogram(prices: list[float], bins_count: int = 8) -> list[PriceHistogramBinDTO]:
    if not prices:
        return []
    min_price = min(prices)
    max_price = max(prices)
    if min_price == max_price:
        return [
            PriceHistogramBinDTO(
                label=f"{min_price:.2f}",
                from_price=min_price,
                to_price=max_price,
                count=len(prices),
            )
        ]

    step = (max_price - min_price) / bins_count
    buckets = [0 for _ in range(bins_count)]
    for price in prices:
        if step == 0:
            idx = 0
        else:
            idx = int((price - min_price) / step)
        idx = min(max(idx, 0), bins_count - 1)
        buckets[idx] += 1

    histogram: list[PriceHistogramBinDTO] = []
    for idx, count in enumerate(buckets):
        left = min_price + step * idx
        right = max_price if idx == bins_count - 1 else min_price + step * (idx + 1)
        histogram.append(
            PriceHistogramBinDTO(
                label=f"{left:.2f} - {right:.2f}",
                from_price=round(left, 6),
                to_price=round(right, 6),
                count=count,
            )
        )
    return histogram


def point_from_result_dict(data: dict) -> HistoryPointDTO | None:
    try:
        offers = data.get("offers_stats") or {}
        demand = data.get("demand") or {}
        generated = data.get("meta", {}).get("generated_at") or data.get("generated_at")
        if not generated:
            return None
        return HistoryPointDTO(
            generated_at=datetime.fromisoformat(generated.replace("Z", "+00:00")),
            matched_offers=int(offers.get("matched_offers", 0)),
            unique_sellers=int(offers.get("unique_sellers", 0)),
            p50_price=offers.get("p50_price"),
            demand_index=demand.get("demand_index"),
        )
    except Exception:  # noqa: BLE001
        return None


def load_history_rows(
    db: Session,
    *,
    query: str,
    currency: str,
    options_hash: str,
    limit: int,
) -> list[AnalysisHistory]:
    query_norm = normalize_text(query)
    stmt = (
        select(AnalysisHistory)
        .where(
            AnalysisHistory.query_normalized == query_norm,
            AnalysisHistory.currency == currency,
            AnalysisHistory.options_hash == options_hash,
        )
        .order_by(desc(AnalysisHistory.generated_at))
        .limit(limit)
    )
    rows = db.scalars(stmt).all()
    rows.reverse()
    return rows


def build_history_points(
    db: Session,
    *,
    query: str,
    currency: str,
    options_hash: str,
    limit: int,
    current_generated_at: datetime,
    current_offers_stats: OffersStatsDTO,
    current_demand: DemandStatsDTO | None,
) -> tuple[list[HistoryPointDTO], DeltaDTO | None]:
    rows = load_history_rows(
        db,
        query=query,
        currency=currency,
        options_hash=options_hash,
        limit=max(1, limit),
    )
    previous_point: HistoryPointDTO | None = None

    points: list[HistoryPointDTO] = []
    for row in rows:
        point = point_from_result_dict(row.result_json)
        if point is not None:
            points.append(point)
            previous_point = point

    current_point = HistoryPointDTO(
        generated_at=current_generated_at,
        matched_offers=current_offers_stats.matched_offers,
        unique_sellers=current_offers_stats.unique_sellers,
        p50_price=current_offers_stats.p50_price,
        demand_index=current_demand.demand_index if current_demand else None,
    )
    points.append(current_point)
    points = points[-limit:]

    if previous_point is None:
        return points, None

    return points, DeltaDTO(
        matched_offers_delta=current_point.matched_offers - previous_point.matched_offers,
        unique_sellers_delta=current_point.unique_sellers - previous_point.unique_sellers,
        p50_price_delta=(
            round(current_point.p50_price - previous_point.p50_price, 6)
            if current_point.p50_price is not None and previous_point.p50_price is not None
            else None
        ),
        demand_index_delta=(
            round(current_point.demand_index - previous_point.demand_index, 6)
            if current_point.demand_index is not None and previous_point.demand_index is not None
            else None
        ),
    )


def build_top_offers_table(offers: list[OfferData], limit: int = 20) -> list[TopOfferDTO]:
    top = sorted(offers, key=lambda item: item.price)[:limit]
    return [
        TopOfferDTO(
            offer_id=offer.offer_id,
            offer_url=f"https://funpay.com/lots/offer?id={offer.offer_id}",
            seller_id=offer.seller_id,
            seller_name=offer.seller_name,
            description=offer.description,
            price=round(offer.price, 6),
            currency=offer.currency,
            reviews_count=offer.reviews_count,
            is_online=offer.is_online,
            auto_delivery=offer.auto_delivery,
        )
        for offer in top
    ]


def build_top_sellers_table(
    offers: list[OfferData],
    *,
    limit: int = 20,
    percentile_fn: Callable[[list[float], float], float | None],
) -> list[TopSellerDTO]:
    grouped: dict[tuple[int | None, str], list[OfferData]] = defaultdict(list)
    for offer in offers:
        grouped[(offer.seller_id, offer.seller_name)].append(offer)

    rows: list[TopSellerDTO] = []
    for (seller_id, seller_name), seller_offers in grouped.items():
        prices = [item.price for item in seller_offers if item.price is not None]
        online = [item for item in seller_offers if item.is_online is not None]
        auto = [item for item in seller_offers if item.auto_delivery is not None]
        rows.append(
            TopSellerDTO(
                seller_id=seller_id,
                seller_name=seller_name,
                offers_count=len(seller_offers),
                min_price=min(prices) if prices else None,
                p50_price=percentile_fn(prices, 0.5),
                max_price=max(prices) if prices else None,
                online_share=(
                    round(sum(1 for item in online if item.is_online) / len(online), 4)
                    if online
                    else None
                ),
                auto_delivery_share=(
                    round(sum(1 for item in auto if item.auto_delivery) / len(auto), 4)
                    if auto
                    else None
                ),
            )
        )

    rows.sort(key=lambda row: (-row.offers_count, row.min_price or float("inf")))
    return rows[:limit]


def build_top_demand_sellers_table(
    seller_stats: list[Any],
    *,
    limit: int = 20,
) -> list[TopDemandSellerDTO]:
    top = sorted(
        seller_stats,
        key=lambda item: (-item.estimated_purchases_30d, -item.estimated_purchases_total, -item.reviews_scanned),
    )[:limit]
    return [
        TopDemandSellerDTO(
            seller_id=item.seller_id,
            seller_name=item.seller_name,
            estimated_purchases_total=item.estimated_purchases_total,
            estimated_purchases_30d=item.estimated_purchases_30d,
            reviews_scanned=item.reviews_scanned,
        )
        for item in top
    ]


def build_sections_table(
    coverage_rows: list[Any],
    *,
    ui_locale: str = "ru",
) -> list[SectionRowDTO]:
    sorted_rows = sorted(
        coverage_rows,
        key=lambda row: (0 if row.coverage_status == "lower_bound" else 1, row.section_url),
    )
    section_fallback_prefix = "Section" if ui_locale == "en" else "Раздел"
    return [
        SectionRowDTO(
            section_url=row.section_url,
            section_id=row.section_id,
            section_name=row.section_name
            or (f"{section_fallback_prefix} #{row.section_id}" if row.section_id is not None else None),
            counter_total=row.counter_total,
            loaded_count=row.loaded_count,
            coverage_status=row.coverage_status,
        )
        for row in sorted_rows
    ]
