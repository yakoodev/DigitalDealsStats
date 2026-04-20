from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.db.base import Base
from app.models import AnalysisRequest, OfferSnapshot
from app.services.analyzer import AnalyzerService
from app.services.funpay_client import OfferData


def _make_offer(
    offer_id: int,
    seller_id: int,
    seller_name: str,
    price: float,
) -> OfferData:
    return OfferData(
        section_id=2893,
        offer_id=offer_id,
        seller_id=seller_id,
        seller_name=seller_name,
        description=f"{seller_name} offer {offer_id}",
        price=price,
        currency="RUB",
        reviews_count=10,
        seller_age=None,
        is_online=True,
        auto_delivery=False,
    )


def _service_with_db() -> tuple[Session, AnalyzerService]:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session = Session(engine)
    service = AnalyzerService(db=session, client=None, settings=Settings())  # type: ignore[arg-type]
    return session, service


def test_dumping_threshold_iqr_and_fallback() -> None:
    service = AnalyzerService(db=None, client=None, settings=Settings())  # type: ignore[arg-type]
    assert service.compute_dumping_threshold([100.0, 100.0, 100.0]) == 80.0
    assert service.compute_dumping_threshold([10.0, 11.0, 12.0, 13.0, 100.0]) == 8.0


def test_competition_metrics_values() -> None:
    service = AnalyzerService(db=None, client=None, settings=Settings())  # type: ignore[arg-type]
    offers = [
        _make_offer(1, 10, "alpha", 10.0),
        _make_offer(2, 10, "alpha", 20.0),
        _make_offer(3, 10, "alpha", 30.0),
        _make_offer(4, 20, "beta", 40.0),
        _make_offer(5, 30, "gamma", 50.0),
    ]
    metrics = service.compute_competition_metrics(offers)
    assert metrics["hhi"] is not None
    assert abs(metrics["hhi"] - 4400.0) < 0.001
    assert metrics["top3_share"] == 1.0
    assert metrics["price_spread"] is not None
    assert metrics["price_spread"] > 1.0


def test_list_request_offers_filters_and_total() -> None:
    session, service = _service_with_db()
    try:
        request_id = "00000000-0000-0000-0000-000000000001"
        session.add(
            AnalysisRequest(
                id=request_id,
                status="done",
                query="zomboid",
                mode="search",
                currency="RUB",
                force_refresh=False,
                result_json={"cache_hit": False, "result": None},
            )
        )
        session.add_all(
            [
                OfferSnapshot(
                    request_id=request_id,
                    section_id=2893,
                    offer_id=1,
                    seller_id=100,
                    seller_name="alpha_shop",
                    description="offer one",
                    price=12.0,
                    currency="RUB",
                    reviews_count=25,
                    seller_age=None,
                    is_online=True,
                    auto_delivery=True,
                ),
                OfferSnapshot(
                    request_id=request_id,
                    section_id=2893,
                    offer_id=2,
                    seller_id=200,
                    seller_name="beta_store",
                    description="offer two",
                    price=18.0,
                    currency="RUB",
                    reviews_count=2,
                    seller_age=None,
                    is_online=False,
                    auto_delivery=False,
                ),
                OfferSnapshot(
                    request_id=request_id,
                    section_id=2893,
                    offer_id=3,
                    seller_id=101,
                    seller_name="alpha_market",
                    description="offer three",
                    price=30.0,
                    currency="RUB",
                    reviews_count=40,
                    seller_age=None,
                    is_online=True,
                    auto_delivery=False,
                ),
            ]
        )
        session.commit()

        response = service.list_request_offers(
            request_id=request_id,
            limit=50,
            offset=0,
            price_min=10.0,
            price_max=25.0,
            min_reviews=10,
            online_only=True,
            auto_delivery_only=True,
            seller_query="alpha",
        )

        assert response.total == 1
        assert len(response.items) == 1
        assert response.items[0].offer_id == 1
        assert response.items[0].offer_url.endswith("id=1")
    finally:
        session.close()


def test_list_request_offers_raises_for_unknown_request() -> None:
    session, service = _service_with_db()
    try:
        try:
            service.list_request_offers(request_id="unknown-request")
        except ValueError as exc:
            assert "Запрос не найден" in str(exc)
        else:
            raise AssertionError("Ожидалось исключение ValueError")
    finally:
        session.close()
