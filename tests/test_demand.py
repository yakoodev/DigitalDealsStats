from app.core.config import Settings
from app.services.analyzer import AnalyzerService
from app.services.funpay_client import OfferData, ReviewData


def test_demand_stats_empty() -> None:
    stats = AnalyzerService.compute_demand_stats([], include_index=True)
    assert stats.relevant_reviews == 0
    assert stats.demand_index == 0.0


def test_demand_stats_values() -> None:
    reviews = [
        ReviewData(
            seller_id=1,
            detail="Jump Space, 0.6 €",
            text="ok",
            rating=5,
            date_bucket="This month",
        ),
        ReviewData(
            seller_id=1,
            detail="Jump Space, 0.6 €",
            text="ok",
            rating=4,
            date_bucket="This month",
        ),
        ReviewData(
            seller_id=2,
            detail="Jump Space, 0.6 €",
            text="bad",
            rating=2,
            date_bucket="Month ago",
        ),
    ]
    stats = AnalyzerService.compute_demand_stats(reviews, include_index=True)
    assert stats.relevant_reviews == 3
    assert stats.unique_sellers_with_relevant_reviews == 2
    assert stats.volume_30d == 2
    assert stats.positive_share > 0.6


def test_demand_stats_without_index() -> None:
    reviews = [
        ReviewData(
            seller_id=1,
            detail="Jump Space, 0.6 €",
            text="ok",
            rating=5,
            date_bucket="This month",
        ),
    ]
    stats = AnalyzerService.compute_demand_stats(reviews, include_index=False)
    assert stats.relevant_reviews == 1
    assert stats.demand_index is None


def test_derive_review_tokens_skips_noise_words() -> None:
    service = AnalyzerService(db=None, client=None, settings=Settings())  # type: ignore[arg-type]
    offers = [
        OfferData(
            section_id=2893,
            offer_id=1,
            seller_id=1,
            seller_name="seller",
            description="Project Zomboid Standard Edition Steam Deluxe PS5 аренда",
            price=10.0,
            currency="₽",
            reviews_count=10,
            seller_age=None,
            is_online=True,
            auto_delivery=True,
        ),
        OfferData(
            section_id=2893,
            offer_id=2,
            seller_id=2,
            seller_name="seller2",
            description="Project Zomboid Standard Edition Steam Deluxe PS5 аренда",
            price=11.0,
            currency="₽",
            reviews_count=11,
            seller_age=None,
            is_online=True,
            auto_delivery=True,
        ),
        OfferData(
            section_id=2893,
            offer_id=3,
            seller_id=3,
            seller_name="seller3",
            description="Project Zomboid Standard Edition Steam Deluxe PS5 аренда",
            price=12.0,
            currency="₽",
            reviews_count=12,
            seller_age=None,
            is_online=True,
            auto_delivery=True,
        ),
    ]
    tokens = service._derive_review_tokens_from_offers(offers)
    assert "project" in tokens
    assert "zomboid" in tokens
    assert "standard" not in tokens
    assert "edition" not in tokens
    assert "steam" not in tokens
    assert "deluxe" not in tokens
    assert "ps5" not in tokens


def test_fallback_review_relevance_uses_detail_tokens() -> None:
    review_ok = ReviewData(
        seller_id=1,
        detail="Project Zomboid, 100 ₽",
        text="Быстро",
        rating=5,
        date_bucket="This month",
    )
    review_bad = ReviewData(
        seller_id=1,
        detail="Steam, 100 ₽",
        text="Топ",
        rating=5,
        date_bucket="This month",
    )
    assert AnalyzerService._is_fallback_review_relevant(review_ok, ["project", "zomboid"])
    assert not AnalyzerService._is_fallback_review_relevant(review_bad, ["project", "zomboid"])


def test_fallback_review_relevance_accepts_single_overlap_for_derived_tokens() -> None:
    review = ReviewData(
        seller_id=1,
        detail="Pragmata, 20 ₽",
        text="быстро",
        rating=5,
        date_bucket="This month",
    )
    assert AnalyzerService._is_fallback_review_relevant(review, ["pragmata", "deluxe", "ps5"])


def test_review_relevance_tokens_uses_fallback_for_duration_query() -> None:
    service = AnalyzerService(db=None, client=None, settings=Settings())  # type: ignore[arg-type]
    offers = [
        OfferData(
            section_id=2893,
            offer_id=1,
            seller_id=1,
            seller_name="seller",
            description="Project Zomboid Standard Edition аренда",
            price=10.0,
            currency="₽",
            reviews_count=10,
            seller_age=None,
            is_online=True,
            auto_delivery=True,
        ),
        OfferData(
            section_id=2893,
            offer_id=2,
            seller_id=2,
            seller_name="seller2",
            description="Project Zomboid Standard Edition аренда",
            price=11.0,
            currency="₽",
            reviews_count=11,
            seller_age=None,
            is_online=True,
            auto_delivery=True,
        ),
        OfferData(
            section_id=2893,
            offer_id=3,
            seller_id=3,
            seller_name="seller3",
            description="Project Zomboid Standard Edition аренда",
            price=12.0,
            currency="₽",
            reviews_count=12,
            seller_age=None,
            is_online=True,
            auto_delivery=True,
        ),
    ]
    token_list, used_fallback = service._review_relevance_tokens(
        query="24 часа",
        offers=offers,
        category_game_id=None,
        category_id=2893,
    )
    assert used_fallback is True
    assert "project" in token_list
    assert "zomboid" in token_list


def test_top_seller_review_relevance_requires_game_and_amount_match() -> None:
    service = AnalyzerService(db=None, client=None, settings=Settings())  # type: ignore[arg-type]
    seller_offers = [
        OfferData(
            section_id=2893,
            offer_id=1,
            seller_id=1,
            seller_name="seller",
            description="Project Zomboid аренда 24 часа",
            price=10.0,
            currency="₽",
            reviews_count=10,
            seller_age=None,
            is_online=True,
            auto_delivery=True,
        ),
        OfferData(
            section_id=2893,
            offer_id=2,
            seller_id=1,
            seller_name="seller",
            description="Project Zomboid аренда 7 дней",
            price=35.0,
            currency="₽",
            reviews_count=10,
            seller_age=None,
            is_online=True,
            auto_delivery=True,
        ),
    ]
    review_ok = ReviewData(
        seller_id=1,
        detail="Project Zomboid, 10 ₽",
        text="ok",
        rating=5,
        date_bucket="This month",
    )
    review_bad_amount = ReviewData(
        seller_id=1,
        detail="Project Zomboid, 100 ₽",
        text="ok",
        rating=5,
        date_bucket="This month",
    )
    review_bad_game = ReviewData(
        seller_id=1,
        detail="Steam, 10 ₽",
        text="ok",
        rating=5,
        date_bucket="This month",
    )

    assert service._is_top_seller_review_relevant(review_ok, ["project", "zomboid"], seller_offers)
    assert not service._is_top_seller_review_relevant(review_bad_amount, ["project", "zomboid"], seller_offers)
    assert not service._is_top_seller_review_relevant(review_bad_game, ["project", "zomboid"], seller_offers)


def test_filter_offers_by_requested_currency_keeps_only_matching() -> None:
    service = AnalyzerService(db=None, client=None, settings=Settings())  # type: ignore[arg-type]
    offers = [
        OfferData(
            section_id=1,
            offer_id=1,
            seller_id=1,
            seller_name="rub_seller",
            description="rub",
            price=10.0,
            currency="₽",
            reviews_count=1,
            seller_age=None,
            is_online=True,
            auto_delivery=True,
        ),
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
    unknown = next(offer for offer in filtered if offer.offer_id == 3)
    assert unknown.currency == "RUB"


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
