import re
import unicodedata
from collections.abc import Iterable

NON_WORD_RE = re.compile(r"[^a-zA-Zа-яА-Я0-9]+")
GENERIC_MARKET_TOKENS = {
    "аренда",
    "rent",
    "rental",
    "акк",
    "аккаунт",
    "account",
    "accounts",
    "ключ",
    "keys",
    "key",
    "услуга",
    "услуги",
    "services",
    "service",
    "доставка",
    "delivery",
    "продажа",
    "sale",
    "скидка",
    "cheap",
}


def normalize_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value).lower()
    normalized = NON_WORD_RE.sub(" ", normalized)
    return " ".join(normalized.split())


def tokenize(value: str) -> list[str]:
    if not value:
        return []
    return normalize_text(value).split()


def query_tokens(query: str) -> list[str]:
    tokens = [token for token in tokenize(query) if len(token) >= 2]
    # preserve order while removing duplicates
    return list(dict.fromkeys(tokens))


def meaningful_query_tokens(query_token_list: Iterable[str]) -> list[str]:
    return [token for token in query_token_list if token not in GENERIC_MARKET_TOKENS]


def relevance_score(text: str, query_token_list: Iterable[str]) -> float:
    token_set = set(tokenize(text))
    qset = set(query_token_list)
    if not qset:
        return 0.0
    overlap = len(token_set & qset)
    return overlap / len(qset)


def is_text_relevant(text: str, query_token_list: Iterable[str]) -> bool:
    qlist = list(query_token_list)
    if not qlist:
        return False

    token_set = set(tokenize(text))
    if not token_set:
        return False

    meaningful = meaningful_query_tokens(qlist)
    if meaningful:
        if not any(token in token_set for token in meaningful):
            return False

    score = relevance_score(text, qlist)
    if len(qlist) == 1:
        return score >= 1.0
    if len(meaningful) >= 2:
        return score >= 0.66
    return score >= 0.5
