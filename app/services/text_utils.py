import re
import unicodedata
from collections.abc import Iterable

NON_WORD_RE = re.compile(r"[^a-zA-Zа-яА-Я0-9]+")
CYRILLIC_CHAR_RE = re.compile(r"[а-яА-ЯёЁ]")
MOJIBAKE_MARKER_RE = re.compile(r"[ÐÑРС]")
CP1252_REVERSE_MAP = {
    0x20AC: 0x80,
    0x201A: 0x82,
    0x0192: 0x83,
    0x201E: 0x84,
    0x2026: 0x85,
    0x2020: 0x86,
    0x2021: 0x87,
    0x02C6: 0x88,
    0x2030: 0x89,
    0x0160: 0x8A,
    0x2039: 0x8B,
    0x0152: 0x8C,
    0x017D: 0x8E,
    0x2018: 0x91,
    0x2019: 0x92,
    0x201C: 0x93,
    0x201D: 0x94,
    0x2022: 0x95,
    0x2013: 0x96,
    0x2014: 0x97,
    0x02DC: 0x98,
    0x2122: 0x99,
    0x0161: 0x9A,
    0x203A: 0x9B,
    0x0153: 0x9C,
    0x017E: 0x9E,
    0x0178: 0x9F,
}
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


def repair_mojibake_cyrillic(value: str) -> str:
    text = value or ""
    if not text:
        return text
    if CYRILLIC_CHAR_RE.search(text) and "?" not in text:
        return text
    if not MOJIBAKE_MARKER_RE.search(text):
        return text

    candidates = [text]
    for source_encoding in ("latin1", "cp1252"):
        try:
            repaired = text.encode(source_encoding).decode("utf-8")
        except Exception:  # noqa: BLE001
            continue
        candidates.append(repaired)
    try:
        buffer = bytearray()
        for ch in text:
            code = ord(ch)
            if code <= 0xFF:
                buffer.append(code)
                continue
            mapped = CP1252_REVERSE_MAP.get(code)
            if mapped is None:
                buffer = bytearray()
                break
            buffer.append(mapped)
        if buffer:
            candidates.append(bytes(buffer).decode("utf-8"))
    except Exception:  # noqa: BLE001
        pass

    def _score(item: str) -> int:
        cyr = len(CYRILLIC_CHAR_RE.findall(item))
        markers = len(MOJIBAKE_MARKER_RE.findall(item))
        questions = item.count("?")
        return (cyr * 3) - (markers * 2) - (questions * 2)

    best = max(candidates, key=_score)
    if _score(best) >= _score(text) + 2:
        return best
    return text
