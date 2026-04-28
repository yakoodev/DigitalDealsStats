from __future__ import annotations

import re
from collections import Counter

from app.schemas.v2 import DemandStatsV2DTO, OffersStatsV2DTO
from app.services.playerok_client import PlayerOkOfferData, PlayerOkReviewData
from app.services.text_utils import normalize_text, tokenize

AMOUNT_RE = re.compile(
    r"(\d+(?:[.,]\d+)?)\s*(?:₽|руб(?:\.|лей|ля)?|rub|usd|\$|eur|€)?",
    flags=re.IGNORECASE,
)


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


def offers_stats(offers: list[PlayerOkOfferData]) -> OffersStatsV2DTO:
    prices = [float(item.price) for item in offers if item.price is not None]
    unique_sellers = {item.seller_slug or item.seller_uuid for item in offers if item.seller_slug or item.seller_uuid}
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


def normalize_currency(raw: str | None) -> str | None:
    if not raw:
        return None
    normalized = raw.strip().upper()
    if normalized in {"RUB", "₽", "РУБ"}:
        return "RUB"
    if normalized in {"USD", "$"}:
        return "USD"
    if normalized in {"EUR", "€"}:
        return "EUR"
    return normalized


def filter_offers_by_currency(
    offers: list[PlayerOkOfferData],
    requested_currency: str,
) -> tuple[list[PlayerOkOfferData], int, bool]:
    filtered: list[PlayerOkOfferData] = []
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


def extract_amounts_from_text(value: str) -> list[float]:
    if not value:
        return []
    matches = AMOUNT_RE.findall(value.replace("\xa0", " ").replace(",", "."))
    result: list[float] = []
    for amount_raw in matches:
        try:
            result.append(float(amount_raw))
        except ValueError:
            continue
    return result


def is_amount_close(amount: float, prices: list[float]) -> bool:
    for price in prices:
        if price <= 0:
            continue
        if abs(amount - price) <= max(2.0, price * 0.40):
            return True
    return False


def derive_review_tokens_from_offers(
    offers: list[PlayerOkOfferData],
    *,
    token_stopwords: set[str],
) -> list[str]:
    token_counter: Counter[str] = Counter()
    for offer in offers:
        for token in set(tokenize(offer.description)):
            if len(token) < 3:
                continue
            if token in token_stopwords:
                continue
            if token.isdigit():
                continue
            token_counter[token] += 1
    return [token for token, _ in token_counter.most_common(8)]


def is_this_month_bucket(value: str | None) -> bool:
    normalized = normalize_text(value or "")
    if not normalized:
        return False
    return normalized in {"this month", "в этом месяце"} or "вчера" in normalized or "сегодня" in normalized


def is_review_match(
    review: PlayerOkReviewData,
    *,
    seller_offers: list[PlayerOkOfferData],
    category_tokens: list[str],
    token_stopwords: set[str],
) -> tuple[bool, str | None]:
    offer_game_ids = {item.game_id for item in seller_offers if item.game_id}
    offer_category_ids = {item.category_id for item in seller_offers if item.category_id}

    game_match = False
    if review.game_id and review.game_id in offer_game_ids:
        if review.category_id is None or not offer_category_ids or review.category_id in offer_category_ids:
            game_match = True

    detail_tokens = set(tokenize(review.detail))
    if not game_match and detail_tokens:
        seller_tokens = set(
            derive_review_tokens_from_offers(
                seller_offers,
                token_stopwords=token_stopwords,
            )
        )
        game_tokens = sorted(seller_tokens | set(category_tokens))
        if any(token in detail_tokens for token in game_tokens):
            game_match = True

    if not game_match:
        return False, "no_game_match"

    amounts = []
    if review.amount is not None:
        amounts.append(float(review.amount))
    amounts.extend(extract_amounts_from_text(review.detail))
    if not amounts:
        return False, "no_amount"

    prices = [float(item.price) for item in seller_offers if item.price and item.price > 0]
    if not prices:
        return False, "no_price_match"
    for amount in amounts:
        if is_amount_close(amount, prices):
            return True, None
    return False, "no_price_match"


def review_signature(review: PlayerOkReviewData) -> tuple[str, str, str]:
    return (
        normalize_text(review.detail),
        normalize_text(review.text),
        normalize_text(review.created_at or review.date_bucket or ""),
    )


def dedupe_reviews(reviews: list[PlayerOkReviewData]) -> list[PlayerOkReviewData]:
    unique: list[PlayerOkReviewData] = []
    seen: set[tuple[str, str, str]] = set()
    for review in reviews:
        signature = review_signature(review)
        if signature in seen:
            continue
        seen.add(signature)
        unique.append(review)
    return unique


def compute_demand_stats(
    relevant_reviews: list[PlayerOkReviewData],
    *,
    include_index: bool,
    sellers_analyzed: int,
    reviews_scanned: int,
) -> DemandStatsV2DTO:
    if not relevant_reviews:
        return DemandStatsV2DTO(
            relevant_reviews=0,
            positive_share=0.0,
            volume_30d=0,
            demand_index=(0.0 if include_index else None),
            unique_sellers_with_relevant_reviews=0,
            estimated_purchases_total=0,
            estimated_purchases_30d=0,
            sellers_analyzed=sellers_analyzed,
            reviews_scanned=reviews_scanned,
        )
    rated_reviews = [item for item in relevant_reviews if item.rating is not None]
    positives = [item for item in rated_reviews if (item.rating or 0) >= 4]
    positive_share = len(positives) / len(rated_reviews) if rated_reviews else 0.0
    volume_30d = sum(1 for item in relevant_reviews if is_this_month_bucket(item.date_bucket))
    unique_sellers = len({item.seller_slug for item in relevant_reviews if item.seller_slug})
    demand_index: float | None = None
    if include_index:
        demand_index = round((min(volume_30d, 100) / 100 * 60) + (positive_share * 40), 4)
    return DemandStatsV2DTO(
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


def coverage_status(counter_total: int | None, loaded_count: int) -> str:
    if counter_total is not None and counter_total > loaded_count:
        return "lower_bound"
    return "full"


def compute_competition_metrics(offers: list[PlayerOkOfferData]) -> dict[str, float | None]:
    if not offers:
        return {"hhi": None, "top3_share": None, "price_spread": None}
    seller_counts: Counter[str] = Counter(
        item.seller_slug or item.seller_uuid or f"n:{item.seller_name}" for item in offers
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
    return {"hhi": round(hhi, 6), "top3_share": round(top3_share, 6), "price_spread": spread}
