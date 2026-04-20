from app.services.text_utils import (
    is_text_relevant,
    meaningful_query_tokens,
    normalize_text,
    query_tokens,
    relevance_score,
)


def test_normalize_text() -> None:
    assert normalize_text("  Jump   Space!!!  ") == "jump space"


def test_query_tokens_unique() -> None:
    assert query_tokens("jump jump space") == ["jump", "space"]


def test_relevance_score() -> None:
    score = relevance_score("jump space rental", ["jump", "space"])
    assert score == 1.0


def test_is_text_relevant() -> None:
    assert is_text_relevant("Jump Space Steam account", ["jump", "space"])
    assert not is_text_relevant("Roblox cheap account", ["jump", "space"])


def test_generic_only_overlap_filtered() -> None:
    query = query_tokens("peak аренда")
    assert meaningful_query_tokens(query) == ["peak"]
    assert not is_text_relevant("Diablo 3 Reaper Souls аренда на 7 дней", query)
    assert is_text_relevant("Peak account аренда 7 дней", query)
