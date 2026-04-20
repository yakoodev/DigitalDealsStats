from app.core.config import Settings
from app.schemas.analyze import AnalyzeOptionsDTO, AnalyzeRequestDTO
from app.services.analyzer import AnalyzerService
from app.services.funpay_client import OfferData, SectionParseResult


class _FakeClient:
    def section_url(self, section_id: int, locale: str = "en") -> str:  # noqa: ARG002
        return f"https://funpay.com/lots/{section_id}/"

    def get_game_section_urls(self, game_section_id: int) -> list[str]:  # noqa: ARG002
        return []

    def search_sections(self, query: str) -> list[str]:  # noqa: ARG002
        return []

    def get_all_sections(self) -> list[str]:
        return []

    def parse_section(self, section_url: str) -> SectionParseResult:
        return SectionParseResult(
            section_url=section_url,
            section_id=2893,
            counter_total=2,
            loaded_count=2,
            offers=[
                OfferData(
                    section_id=2893,
                    offer_id=1,
                    seller_id=10,
                    seller_name="seller_1",
                    description="Project Zomboid аренда на 24 часа",
                    price=8.0,
                    currency="₽",
                    reviews_count=100,
                    seller_age=None,
                    is_online=True,
                    auto_delivery=True,
                ),
                OfferData(
                    section_id=2893,
                    offer_id=2,
                    seller_id=11,
                    seller_name="seller_2",
                    description="Project Zomboid аренда на 7 дней",
                    price=35.0,
                    currency="₽",
                    reviews_count=50,
                    seller_age=None,
                    is_online=False,
                    auto_delivery=False,
                ),
            ],
        )


def test_request_schema_accepts_empty_query() -> None:
    dto = AnalyzeRequestDTO(query="")
    assert dto.query == ""


def test_collect_offers_with_empty_query_returns_all_offers_in_selected_section() -> None:
    service = AnalyzerService(db=None, client=_FakeClient(), settings=Settings())  # type: ignore[arg-type]
    options = service.resolve_options(AnalyzeOptionsDTO(), category_id=2893)
    offers, coverage_rows, weak_filtered = service._collect_offers(
        query="",
        options=options,
        content_locale="ru",
        category_id=2893,
    )
    assert len(offers) == 2
    assert weak_filtered == 0
    assert len(coverage_rows) == 1


def test_collect_offers_with_category_ids_union_works_for_empty_query() -> None:
    service = AnalyzerService(db=None, client=_FakeClient(), settings=Settings())  # type: ignore[arg-type]
    options = service.resolve_options(AnalyzeOptionsDTO(), category_id=2893, category_ids=[2893])
    offers, coverage_rows, weak_filtered = service._collect_offers(
        query="",
        options=options,
        content_locale="ru",
        category_ids=[2893],
    )
    assert len(offers) == 2
    assert weak_filtered == 0
    assert len(coverage_rows) == 1
