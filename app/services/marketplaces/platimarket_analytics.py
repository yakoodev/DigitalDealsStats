from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime, timedelta

from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from app.models import AnalysisRequest
from app.schemas.v2 import DemandStatsV2DTO, OffersStatsV2DTO
from app.services.platimarket_client import PlatiOfferCard, PlatiReview
from app.services.text_utils import normalize_text


def percentile(values: list[float], ratio: float) -> float | None:
    clean = [float(item) for item in values if item is not None]
    if not clean:
        return None
    ordered = sorted(clean)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * ratio
    lower_idx = int(rank)
    upper_idx = min(lower_idx + 1, len(ordered) - 1)
    fraction = rank - lower_idx
    return round(ordered[lower_idx] + (ordered[upper_idx] - ordered[lower_idx]) * fraction, 6)


def offers_stats(offers: list[PlatiOfferCard]) -> OffersStatsV2DTO:
    prices = [float(item.price) for item in offers if item.price is not None]
    unique_sellers = {
        (item.seller_id or "") + "|" + (item.seller_name or "")
        for item in offers
        if item.seller_id or item.seller_name
    }
    online_known = [item for item in offers if item.is_online is not None]
    auto_known = [item for item in offers if item.auto_delivery is not None]
    return OffersStatsV2DTO(
        matched_offers=len(offers),
        unique_sellers=len(unique_sellers),
        min_price=min(prices) if prices else None,
        avg_price=(round(sum(prices) / len(prices), 6) if prices else None),
        p50_price=percentile(prices, 0.50),
        p90_price=percentile(prices, 0.90),
        max_price=max(prices) if prices else None,
        online_share=(
            round(sum(1 for item in online_known if item.is_online) / len(online_known), 4)
            if online_known
            else None
        ),
        auto_delivery_share=(
            round(sum(1 for item in auto_known if item.auto_delivery) / len(auto_known), 4)
            if auto_known
            else None
        ),
    )


def coverage_status(counter_total: int | None, loaded_count: int) -> str:
    if counter_total is not None and counter_total > loaded_count:
        return "lower_bound"
    return "full"


def normalize_currency(raw: str | None) -> str | None:
    if not raw:
        return None
    normalized = raw.strip().upper()
    if normalized in {"RUB", "RUR", "₽"}:
        return "RUB"
    if normalized in {"USD", "$"}:
        return "USD"
    if normalized in {"EUR", "€"}:
        return "EUR"
    if normalized in {"UAH", "ГРН"}:
        return "UAH"
    return normalized


def filter_offers_by_currency(
    offers: list[PlatiOfferCard],
    requested_currency: str,
) -> tuple[list[PlatiOfferCard], int, bool]:
    filtered: list[PlatiOfferCard] = []
    excluded = 0
    for offer in offers:
        offer_currency = normalize_currency(offer.currency)
        if offer_currency is None:
            offer.currency = requested_currency
            filtered.append(offer)
            continue
        if offer_currency == requested_currency:
            filtered.append(offer)
            continue
        excluded += 1
    if filtered:
        return filtered, excluded, False
    if offers and excluded > 0:
        return offers, excluded, True
    return filtered, excluded, False


def compute_competition_metrics(offers: list[PlatiOfferCard]) -> dict[str, float | None]:
    if not offers:
        return {"hhi": None, "top3_share": None, "price_spread": None}
    seller_counts: Counter[str] = Counter(
        f"{item.seller_id or ''}|{item.seller_name}" for item in offers
    )
    total = len(offers)
    shares = [count / total for count in seller_counts.values() if count > 0]
    hhi = sum((share * 100) ** 2 for share in shares)
    top3_share = sum(sorted(seller_counts.values(), reverse=True)[:3]) / total
    prices = [float(item.price) for item in offers if item.price is not None]
    p10 = percentile(prices, 0.10)
    p90 = percentile(prices, 0.90)
    p50 = percentile(prices, 0.50)
    spread = None
    if p10 is not None and p90 is not None and p50 is not None and p50 > 0:
        spread = round((p90 - p10) / p50, 6)
    return {
        "hhi": round(hhi, 6),
        "top3_share": round(top3_share, 6),
        "price_spread": spread,
    }


def sold_parsing_diagnostics(offers: list[PlatiOfferCard]) -> dict[str, object]:
    rows_with_sold_text = [item for item in offers if (item.sold_text or "").strip()]
    parsed_rows = [item for item in rows_with_sold_text if item.sold_count is not None]
    unparsed_rows = [item for item in rows_with_sold_text if item.sold_count is None]
    unparsed_counter: Counter[str] = Counter()
    for item in unparsed_rows:
        sold_text = (item.sold_text or "").strip()
        if sold_text:
            unparsed_counter[sold_text] += 1
    return {
        "rows_with_sold_text": len(rows_with_sold_text),
        "rows_parsed": len(parsed_rows),
        "rows_unparsed": len(unparsed_rows),
        "rows_with_plus_lower_bound": sum(1 for item in parsed_rows if item.sold_is_lower_bound),
        "rows_with_less_than_pattern": sum(
            1
            for item in parsed_rows
            if "менее" in (item.sold_text or "").lower() or "less than" in (item.sold_text or "").lower()
        ),
        "unparsed_examples": [
            {"sold_text": sold_text, "count": count}
            for sold_text, count in unparsed_counter.most_common(10)
        ],
    }


def is_review_link_match(
    review: PlatiReview,
    *,
    current_offer_ids: set[str],
    seller_offer_ids: set[str],
) -> bool:
    if not review.offer_id:
        return False
    if review.offer_id not in current_offer_ids:
        return False
    if seller_offer_ids and review.offer_id not in seller_offer_ids:
        return False
    return True


def is_this_month_bucket(value: str | None) -> bool:
    normalized = normalize_text(value or "")
    if not normalized:
        return False
    return normalized in {"this month", "в этом месяце"} or "сегодня" in normalized or "вчера" in normalized


def is_recent_30d(review: PlatiReview) -> bool:
    if review.created_at:
        try:
            created = datetime.fromisoformat(review.created_at.replace("Z", "+00:00"))
            return created >= datetime.now(UTC) - timedelta(days=30)
        except Exception:
            pass
    return is_this_month_bucket(review.date_bucket)


def compute_demand(
    relevant_reviews: list[PlatiReview],
    *,
    include_index: bool,
    sellers_analyzed: int,
    reviews_scanned: int,
    purchases_from_sold_total: int,
    purchases_total_is_lower_bound: bool,
) -> DemandStatsV2DTO:
    rated_reviews = [item for item in relevant_reviews if item.rating is not None]
    positive_reviews = [item for item in rated_reviews if (item.rating or 0) >= 4]
    positive_share = len(positive_reviews) / len(rated_reviews) if rated_reviews else 0.0
    volume_30d = sum(1 for item in relevant_reviews if is_recent_30d(item))
    unique_sellers = len({item.seller_id for item in relevant_reviews if item.seller_id})

    demand_index: float | None = None
    if include_index:
        demand_index = round((min(volume_30d, 100) / 100 * 60) + (positive_share * 40), 4)

    return DemandStatsV2DTO(
        relevant_reviews=len(relevant_reviews),
        positive_share=round(positive_share, 4),
        volume_30d=volume_30d,
        demand_index=demand_index,
        unique_sellers_with_relevant_reviews=unique_sellers,
        estimated_purchases_total=purchases_from_sold_total,
        estimated_purchases_30d=volume_30d,
        sellers_analyzed=sellers_analyzed,
        reviews_scanned=reviews_scanned,
        purchases_from_sold_total=purchases_from_sold_total,
        purchases_from_reviews_total=len(relevant_reviews),
        purchases_from_reviews_30d=volume_30d,
        purchases_total_is_lower_bound=purchases_total_is_lower_bound,
    )


def history_points(
    db: Session,
    *,
    query: str,
    currency: str,
    limit: int,
) -> list[dict]:
    rows = db.scalars(
        select(AnalysisRequest)
        .where(
            AnalysisRequest.mode == "global_v2",
            AnalysisRequest.status == "done",
            AnalysisRequest.query == query,
            AnalysisRequest.currency == currency,
        )
        .order_by(desc(AnalysisRequest.updated_at))
        .limit(max(20, min(limit * 2, 240)))
    ).all()
    points: list[dict] = []
    for row in reversed(rows):
        payload = row.result_json if isinstance(row.result_json, dict) else {}
        summaries = payload.get("marketplace_summaries")
        if not isinstance(summaries, dict):
            continue
        summary = summaries.get("platimarket")
        if not isinstance(summary, dict):
            continue
        generated_at = summary.get("generated_at")
        offers_stats_raw = summary.get("offers_stats") if isinstance(summary.get("offers_stats"), dict) else {}
        demand = summary.get("demand") if isinstance(summary.get("demand"), dict) else {}
        if not isinstance(generated_at, str):
            continue
        points.append(
            {
                "generated_at": generated_at,
                "matched_offers": int(offers_stats_raw.get("matched_offers", 0)),
                "unique_sellers": int(offers_stats_raw.get("unique_sellers", 0)),
                "p50_price": offers_stats_raw.get("p50_price"),
                "demand_index": demand.get("demand_index") if demand else None,
            }
        )
    if len(points) > limit:
        points = points[-limit:]
    return points
