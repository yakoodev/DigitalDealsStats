from app.core.config import Settings
from app.services.analyzer import AnalyzerService
from app.services.funpay_client import OfferData, ReviewData


def _offer(
    offer_id: int,
    seller_id: int,
    description: str,
    price: float,
) -> OfferData:
    return OfferData(
        section_id=2893,
        offer_id=offer_id,
        seller_id=seller_id,
        seller_name=f"seller_{seller_id}",
        description=description,
        price=price,
        currency="₽",
        reviews_count=10,
        seller_age=None,
        is_online=True,
        auto_delivery=True,
    )


def _review(
    seller_id: int,
    detail: str,
    rating: int = 5,
    date_bucket: str = "This month",
) -> ReviewData:
    return ReviewData(
        seller_id=seller_id,
        detail=detail,
        text="ok",
        rating=rating,
        date_bucket=date_bucket,
    )


def test_demand_stats_empty() -> None:
    stats = AnalyzerService.compute_demand_stats([], include_index=True, sellers_analyzed=3, reviews_scanned=12)
    assert stats.relevant_reviews == 0
    assert stats.demand_index == 0.0
    assert stats.sellers_analyzed == 3
    assert stats.reviews_scanned == 12


def test_demand_stats_values() -> None:
    reviews = [
        _review(1, "Project Zomboid, 10 ₽", rating=5, date_bucket="This month"),
        _review(1, "Project Zomboid, 10 ₽", rating=4, date_bucket="This month"),
        _review(2, "Project Zomboid, 35 ₽", rating=2, date_bucket="Month ago"),
    ]
    stats = AnalyzerService.compute_demand_stats(
        reviews,
        include_index=True,
        sellers_analyzed=2,
        reviews_scanned=40,
    )
    assert stats.relevant_reviews == 3
    assert stats.unique_sellers_with_relevant_reviews == 2
    assert stats.volume_30d == 2
    assert stats.estimated_purchases_total == 3
    assert stats.estimated_purchases_30d == 2
    assert stats.positive_share > 0.6
    assert stats.sellers_analyzed == 2
    assert stats.reviews_scanned == 40


def test_extract_amounts_from_text() -> None:
    amounts = AnalyzerService._extract_amounts_from_text("Project Zomboid, 10 ₽ / 10.5 RUB")
    assert 10.0 in amounts
    assert 10.5 in amounts


def test_strict_review_match_requires_game_and_amount() -> None:
    service = AnalyzerService(db=None, client=None, settings=Settings())  # type: ignore[arg-type]
    seller_offers = [
        _offer(1, 1, "Project Zomboid аренда 24 часа", 10.0),
        _offer(2, 1, "Project Zomboid аренда 7 дней", 35.0),
    ]
    ok, reason = service._is_review_matching_purchase(
        review=_review(1, "Project Zomboid, 10 ₽"),
        seller_offers=seller_offers,
        category_tokens=["project", "zomboid"],
    )
    assert ok is True
    assert reason is None

    bad_amount, reason_amount = service._is_review_matching_purchase(
        review=_review(1, "Project Zomboid, 100 ₽"),
        seller_offers=seller_offers,
        category_tokens=["project", "zomboid"],
    )
    assert bad_amount is False
    assert reason_amount == "no_price_match"

    bad_game, reason_game = service._is_review_matching_purchase(
        review=_review(1, "Steam, 10 ₽"),
        seller_offers=seller_offers,
        category_tokens=["project", "zomboid"],
    )
    assert bad_game is False
    assert reason_game == "no_game_match"

    no_amount, reason_no_amount = service._is_review_matching_purchase(
        review=_review(1, "Project Zomboid"),
        seller_offers=seller_offers,
        category_tokens=["project", "zomboid"],
    )
    assert no_amount is False
    assert reason_no_amount == "no_amount"


def test_filter_offers_by_requested_currency_keeps_only_matching() -> None:
    service = AnalyzerService(db=None, client=None, settings=Settings())  # type: ignore[arg-type]
    offers = [
        _offer(1, 1, "rub", 10.0),
        OfferData(
            section_id=1,
            offer_id=2,
            seller_id=2,
            seller_name="eur_seller",
            description="eur",
            price=1.0,
            currency="€",
            reviews_count=1,
            seller_age=None,
            is_online=True,
            auto_delivery=True,
        ),
        OfferData(
            section_id=1,
            offer_id=3,
            seller_id=3,
            seller_name="unknown_seller",
            description="unknown",
            price=20.0,
            currency="",
            reviews_count=1,
            seller_age=None,
            is_online=True,
            auto_delivery=True,
        ),
    ]

    filtered, excluded, relaxed = service._filter_offers_by_currency(offers, "RUB")
    assert excluded == 1
    assert relaxed is False
    assert len(filtered) == 2
    assert {offer.offer_id for offer in filtered} == {1, 3}


def test_filter_offers_by_currency_relaxes_when_all_mismatch() -> None:
    service = AnalyzerService(db=None, client=None, settings=Settings())  # type: ignore[arg-type]
    offers = [
        OfferData(
            section_id=1,
            offer_id=10,
            seller_id=10,
            seller_name="eur_1",
            description="eur one",
            price=1.0,
            currency="€",
            reviews_count=1,
            seller_age=None,
            is_online=True,
            auto_delivery=False,
        ),
        OfferData(
            section_id=1,
            offer_id=11,
            seller_id=11,
            seller_name="eur_2",
            description="eur two",
            price=2.0,
            currency="EUR",
            reviews_count=2,
            seller_age=None,
            is_online=False,
            auto_delivery=False,
        ),
    ]

    filtered, excluded, relaxed = service._filter_offers_by_currency(offers, "RUB")
    assert excluded == 2
    assert relaxed is True
    assert len(filtered) == 2


def test_preferred_content_locale_prioritizes_requested_currency() -> None:
    service = AnalyzerService(db=None, client=None, settings=Settings())  # type: ignore[arg-type]
    assert service._preferred_content_locale("pragmata", "RUB") == "ru"
    assert service._preferred_content_locale("аренда", "EUR") == "en"
