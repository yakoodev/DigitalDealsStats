from __future__ import annotations

import re
from collections import Counter

from app.schemas.analyze import DemandStatsDTO
from app.services.funpay_client import OfferData, ReviewData


def extract_amounts_from_text(value: str) -> list[float]:
    if not value:
        return []
    normalized = value.replace("\xa0", " ").replace(",", ".")
    matches = re.findall(
        r"(\d+(?:\.\d+)?)\s*(?:₽|руб(?:\.|ля|лей)?|rub|usd|\$|eur|€)?",
        normalized,
        flags=re.IGNORECASE,
    )
    amounts: list[float] = []
    for raw in matches:
        try:
            amounts.append(float(raw))
        except ValueError:
            continue
    return amounts


def is_amount_close_to_seller_prices(amount: float, seller_prices: list[float]) -> bool:
    for price in seller_prices:
        if price <= 0:
            continue
        if abs(amount - price) <= max(2.0, price * 0.40):
            return True
    return False


def is_this_month_bucket(value: str | None) -> bool:
    normalized = (value or "").strip().lower()
    return normalized in {"this month", "в этом месяце"}


def percentile(values: list[float], percentile_value: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * percentile_value
    lower_idx = int(rank)
    upper_idx = min(lower_idx + 1, len(ordered) - 1)
    fraction = rank - lower_idx
    return round(ordered[lower_idx] + (ordered[upper_idx] - ordered[lower_idx]) * fraction, 6)


def coverage_status(counter_total: int | None, loaded_count: int) -> str:
    if counter_total is not None and loaded_count == 4000 and counter_total > loaded_count:
        return "lower_bound"
    return "full"


def compute_dumping_threshold(prices: list[float]) -> float | None:
    clean_prices = [float(value) for value in prices if value is not None]
    if not clean_prices:
        return None
    q1 = percentile(clean_prices, 0.25)
    q3 = percentile(clean_prices, 0.75)
    if q1 is None or q3 is None:
        return None
    iqr = round(max(0.0, q3 - q1), 6)
    if iqr > 0:
        return round(q1 - 1.5 * iqr, 6)
    p10 = percentile(clean_prices, 0.10)
    if p10 is None:
        return None
    return round(p10 * 0.8, 6)


def select_dumping_offers(offers: list[OfferData], threshold: float | None) -> list[OfferData]:
    if threshold is None:
        return []
    return [offer for offer in offers if offer.price is not None and float(offer.price) < threshold]


def compute_competition_metrics(offers: list[OfferData]) -> dict[str, float | None]:
    if not offers:
        return {
            "hhi": None,
            "top3_share": None,
            "price_spread": None,
        }

    seller_counts = Counter((offer.seller_id, offer.seller_name) for offer in offers)
    total_offers = len(offers)
    shares = [count / total_offers for count in seller_counts.values() if count > 0]
    hhi = sum((share * 100) ** 2 for share in shares)
    top3_share = sum(sorted(seller_counts.values(), reverse=True)[:3]) / total_offers

    prices = [offer.price for offer in offers if offer.price is not None]
    p10 = percentile(prices, 0.10)
    p90 = percentile(prices, 0.90)
    p50 = percentile(prices, 0.50)
    price_spread = None
    if p10 is not None and p90 is not None and p50 is not None and p50 > 0:
        price_spread = round((p90 - p10) / p50, 6)

    return {
        "hhi": round(hhi, 6),
        "top3_share": round(top3_share, 6),
        "price_spread": price_spread,
    }


def compute_demand_stats(
    relevant_reviews: list[ReviewData],
    include_index: bool,
    sellers_analyzed: int = 0,
    reviews_scanned: int = 0,
) -> DemandStatsDTO:
    if not relevant_reviews:
        return DemandStatsDTO(
            relevant_reviews=0,
            positive_share=0.0,
            volume_30d=0,
            demand_index=0.0 if include_index else None,
            unique_sellers_with_relevant_reviews=0,
            estimated_purchases_total=0,
            estimated_purchases_30d=0,
            sellers_analyzed=sellers_analyzed,
            reviews_scanned=reviews_scanned,
        )

    rated_reviews = [review for review in relevant_reviews if review.rating is not None]
    positives = [review for review in rated_reviews if (review.rating or 0) >= 4]
    positive_share = (len(positives) / len(rated_reviews)) if rated_reviews else 0.0

    volume_30d = sum(1 for review in relevant_reviews if is_this_month_bucket(review.date_bucket))
    unique_sellers = len({review.seller_id for review in relevant_reviews})

    demand_index: float | None = None
    if include_index:
        volume_component = min(volume_30d, 100) / 100 * 60
        quality_component = positive_share * 40
        demand_index = round(volume_component + quality_component, 4)

    return DemandStatsDTO(
        relevant_reviews=len(relevant_reviews),
        positive_share=round(positive_share, 4),
        volume_30d=volume_30d,
        demand_index=demand_index,
        unique_sellers_with_relevant_reviews=unique_sellers,
        estimated_purchases_total=len(relevant_reviews),
        estimated_purchases_30d=volume_30d,
        sellers_analyzed=sellers_analyzed,
        reviews_scanned=reviews_scanned,
    )
