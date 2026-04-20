from app.core.config import Settings
from app.schemas.analyze import AnalyzeOptionsDTO, AnalyzeProfile
from app.services.analyzer import AnalyzerService


def _service() -> AnalyzerService:
    return AnalyzerService(db=None, client=None, settings=Settings())  # type: ignore[arg-type]


def test_balanced_profile_defaults() -> None:
    service = _service()
    options = service.resolve_options(AnalyzeOptionsDTO(profile=AnalyzeProfile.balanced))
    assert options.include_reviews is False
    assert options.include_demand_index is False
    assert options.include_fallback_scan is True


def test_demand_forces_reviews() -> None:
    service = _service()
    options = service.resolve_options(
        AnalyzeOptionsDTO(
            profile=AnalyzeProfile.safe,
            include_reviews=False,
            include_demand_index=True,
        )
    )
    assert options.include_demand_index is True
    assert options.include_reviews is True


def test_heavy_request_when_reviews_enabled() -> None:
    service = _service()
    options = service.resolve_options(AnalyzeOptionsDTO(include_reviews=True))
    assert service.is_heavy_request(options) is True
