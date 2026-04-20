from app.services.analyzer import AnalyzerService


def test_coverage_lower_bound() -> None:
    assert AnalyzerService.coverage_status(counter_total=6020, loaded_count=4000) == "lower_bound"


def test_coverage_full() -> None:
    assert AnalyzerService.coverage_status(counter_total=3999, loaded_count=3999) == "full"
    assert AnalyzerService.coverage_status(counter_total=None, loaded_count=4000) == "full"
