from __future__ import annotations

import hashlib
import json
import re
from collections import Counter, defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from urllib.parse import urlsplit, urlunsplit

from sqlalchemy import delete, desc, func, or_, select, text
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.models import (
    AnalysisCache,
    AnalysisHistory,
    AnalysisRequest,
    OfferSnapshot,
    ReviewSnapshot,
    SectionCoverage,
)
from app.schemas.analyze import (
    AnalyzeEnvelopeDTO,
    AnalyzeMetaDTO,
    AnalyzeOptionsDTO,
    AnalyzeProgressDTO,
    AnalyzeProfile,
    AnalyzeRequestDTO,
    AnalyzeResultDTO,
    ChartsDTO,
    ContentLocale,
    CoverageDTO,
    DeltaDTO,
    DemandStatsDTO,
    EffectiveAnalyzeOptionsDTO,
    HistoryPointDTO,
    HistoryItemDTO,
    HistoryResponseDTO,
    OfferSnapshotDTO,
    OffersStatsDTO,
    OffersSliceResponseDTO,
    PriceHistogramBinDTO,
    ProgressLogDTO,
    SectionRowDTO,
    TablesDTO,
    TopOfferDTO,
    TopDemandSellerDTO,
    TopSellerDTO,
)
from app.services.funpay_client import FunPayClient, OfferData, ReviewData
from app.services.i18n import tr
from app.services.text_utils import (
    is_text_relevant,
    meaningful_query_tokens,
    normalize_text,
    query_tokens,
    tokenize,
)


@dataclass
class CoverageRow:
    section_url: str
    section_id: int | None
    section_name: str | None
    counter_total: int | None
    loaded_count: int
    coverage_status: str


@dataclass
class ReviewMatchDiagnostics:
    sellers_targeted: int = 0
    sellers_analyzed: int = 0
    failed_sellers: int = 0
    reviews_scanned: int = 0
    no_game_match: int = 0
    no_amount: int = 0
    no_price_match: int = 0


@dataclass
class SellerDemandStat:
    seller_id: int | None
    seller_name: str
    reviews_scanned: int
    estimated_purchases_total: int
    estimated_purchases_30d: int


ProgressCallback = Callable[[str, float | None, str], None]


class AnalyzerService:
    REVIEW_TOKEN_STOPWORDS = {
        "edition",
        "standard",
        "steam",
        "deluxe",
        "ultimate",
        "premium",
        "account",
        "accounts",
        "pc",
        "ps5",
        "ps4",
        "xbox",
        "xboxone",
        "xboxseries",
        "автовыдача",
        "авто",
        "выдача",
        "online",
        "онлайн",
        "отзыв",
        "гарантия",
        "свободно",
        "час",
        "часа",
        "часов",
        "день",
        "дня",
        "дней",
        "минут",
        "бот",
        "ботом",
        "аккаунт",
        "аккаунта",
        "акка",
    }

    def __init__(self, db: Session, client: FunPayClient, settings: Settings) -> None:
        self.db = db
        self.client = client
        self.settings = settings

    @staticmethod
    def _utc_iso() -> str:
        return datetime.now(UTC).isoformat().replace("+00:00", "Z")

    def _read_progress_payload(self, row: AnalysisRequest) -> dict:
        raw_payload = row.result_json if isinstance(row.result_json, dict) else {}
        payload = json.loads(json.dumps(raw_payload))
        progress = payload.get("progress")
        if not isinstance(progress, dict):
            progress = {"percent": 0.0, "stage": None, "message": None, "logs": []}
            payload["progress"] = progress
        logs = progress.get("logs")
        if not isinstance(logs, list):
            progress["logs"] = []
        return payload

    def _set_progress(
        self,
        row: AnalysisRequest,
        *,
        percent: float | None = None,
        stage: str | None = None,
        message: str | None = None,
        append_log: bool = False,
        commit: bool = True,
    ) -> None:
        payload = self._read_progress_payload(row)
        progress = payload["progress"]
        if percent is not None:
            progress["percent"] = max(0.0, min(100.0, round(float(percent), 2)))
        if stage is not None:
            progress["stage"] = stage
        if message is not None:
            progress["message"] = message
        if append_log and message:
            logs = progress.get("logs", [])
            logs.append(
                {
                    "ts": self._utc_iso(),
                    "stage": stage or progress.get("stage") or "info",
                    "message": message,
                }
            )
            progress["logs"] = logs[-200:]
        payload["progress"] = progress
        row.result_json = payload
        row.updated_at = datetime.now(UTC)
        self.db.add(row)
        if commit:
            self.db.commit()

    def _build_progress_dto(self, row: AnalysisRequest) -> AnalyzeProgressDTO | None:
        payload = row.result_json if isinstance(row.result_json, dict) else {}
        progress_raw = payload.get("progress")
        if not isinstance(progress_raw, dict):
            return None
        logs_raw = progress_raw.get("logs")
        logs: list[ProgressLogDTO] = []
        if isinstance(logs_raw, list):
            for item in logs_raw[-200:]:
                if not isinstance(item, dict):
                    continue
                ts_raw = item.get("ts")
                if not isinstance(ts_raw, str):
                    continue
                try:
                    ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
                except Exception:  # noqa: BLE001
                    continue
                message = item.get("message")
                if not isinstance(message, str) or not message.strip():
                    continue
                stage = item.get("stage")
                logs.append(
                    ProgressLogDTO(
                        ts=ts,
                        stage=stage if isinstance(stage, str) and stage.strip() else "info",
                        message=message,
                    )
                )
        percent = progress_raw.get("percent")
        try:
            percent_value = float(percent) if percent is not None else 0.0
        except (TypeError, ValueError):
            percent_value = 0.0
        stage_value = progress_raw.get("stage")
        message_value = progress_raw.get("message")
        return AnalyzeProgressDTO(
            percent=max(0.0, min(100.0, percent_value)),
            stage=stage_value if isinstance(stage_value, str) else None,
            message=message_value if isinstance(message_value, str) else None,
            logs=logs,
        )

    def resolve_options(
        self,
        raw: AnalyzeOptionsDTO,
        category_game_id: int | None = None,
        category_id: int | None = None,
        category_ids: list[int] | None = None,
    ) -> EffectiveAnalyzeOptionsDTO:
        default_seller_limit = 3
        if raw.profile == AnalyzeProfile.safe:
            include_reviews = False
            include_demand_index = False
            include_fallback_scan = False
            section_limit = min(self.settings.quick_sections_limit, 40)
            seller_limit = default_seller_limit
            review_pages_per_seller = 1
            history_points_limit = 30
        elif raw.profile == AnalyzeProfile.deep:
            include_reviews = True
            include_demand_index = True
            include_fallback_scan = True
            section_limit = max(self.settings.fallback_sections_limit, self.settings.quick_sections_limit)
            seller_limit = default_seller_limit
            review_pages_per_seller = max(self.settings.review_max_pages_per_seller, 8)
            history_points_limit = 120
        else:
            include_reviews = False
            include_demand_index = False
            include_fallback_scan = True
            section_limit = self.settings.quick_sections_limit
            seller_limit = default_seller_limit
            review_pages_per_seller = self.settings.review_max_pages_per_seller
            history_points_limit = 60

        if raw.include_reviews is not None:
            include_reviews = raw.include_reviews
        if raw.include_demand_index is not None:
            include_demand_index = raw.include_demand_index
        if raw.include_fallback_scan is not None:
            include_fallback_scan = raw.include_fallback_scan
        if raw.section_limit is not None:
            section_limit = raw.section_limit
        if raw.seller_limit is not None:
            seller_limit = raw.seller_limit
        if raw.review_pages_per_seller is not None:
            review_pages_per_seller = raw.review_pages_per_seller
        if raw.history_points_limit is not None:
            history_points_limit = raw.history_points_limit

        seller_limit = max(1, min(20, seller_limit))

        if include_demand_index:
            include_reviews = True
        if not include_reviews:
            include_demand_index = False

        mode_label = "demand" if include_reviews else "search"
        options_payload = {
            "profile": raw.profile.value,
            "include_reviews": include_reviews,
            "include_demand_index": include_demand_index,
            "include_fallback_scan": include_fallback_scan,
            "section_limit": section_limit,
            "seller_limit": seller_limit,
            "review_pages_per_seller": review_pages_per_seller,
            "history_points_limit": history_points_limit,
            "mode_label": mode_label,
            "category_game_id": category_game_id,
            "category_id": category_id,
            "category_ids": sorted(set(category_ids or [])),
        }
        options_hash = hashlib.sha256(
            json.dumps(options_payload, sort_keys=True, ensure_ascii=True).encode("utf-8")
        ).hexdigest()
        return EffectiveAnalyzeOptionsDTO(
            profile=raw.profile,
            include_reviews=include_reviews,
            include_demand_index=include_demand_index,
            include_fallback_scan=include_fallback_scan,
            section_limit=section_limit,
            seller_limit=seller_limit,
            review_pages_per_seller=review_pages_per_seller,
            history_points_limit=history_points_limit,
            mode_label=mode_label,
            options_hash=options_hash,
        )

    def is_heavy_request(self, options: EffectiveAnalyzeOptionsDTO) -> bool:
        if options.include_reviews:
            return True
        if options.section_limit > self.settings.quick_sections_limit:
            return True
        if options.seller_limit > self.settings.demand_max_sellers:
            return True
        return False

    @staticmethod
    def _cache_key(
        query: str,
        currency: str,
        options_hash: str,
        content_locale_key: str,
        ui_locale_key: str,
    ) -> str:
        normalized = normalize_text(query)
        raw = f"v8|{currency}|{options_hash}|{content_locale_key}|{ui_locale_key}|{normalized}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    @staticmethod
    def _normalize_offer_currency(raw_value: str | None) -> str | None:
        if not raw_value:
            return None
        raw = raw_value.strip().upper()
        mapping = {
            "₽": "RUB",
            "RUB": "RUB",
            "RUR": "RUB",
            "$": "USD",
            "USD": "USD",
            "€": "EUR",
            "EUR": "EUR",
        }
        return mapping.get(raw)

    def _filter_offers_by_currency(
        self,
        offers: list[OfferData],
        requested_currency: str,
    ) -> tuple[list[OfferData], int, bool]:
        filtered: list[OfferData] = []
        excluded = 0
        for offer in offers:
            offer_currency = self._normalize_offer_currency(offer.currency)
            if offer_currency is None:
                # Если валюта не распознана, не теряем оффер и считаем его в валюте запроса.
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
            # Если отфильтровали всё (например en-локаль и EUR-цены),
            # возвращаем исходную выборку, чтобы не терять результаты полностью.
            return offers, excluded, True
        return filtered, excluded, False

    @staticmethod
    def _query_has_cyrillic(query: str) -> bool:
        return bool(re.search(r"[а-яА-ЯёЁ]", query))

    @staticmethod
    def _rewrite_url_locale(url: str, locale: str) -> str:
        parts = urlsplit(url)
        path = parts.path
        if path.startswith("/en/"):
            path = path[3:]
        elif path.startswith("/ru/"):
            path = path[3:]

        if locale == "en":
            if not path.startswith("/"):
                path = "/" + path
            path = "/en" + path
        else:
            if not path.startswith("/"):
                path = "/" + path

        return urlunsplit((parts.scheme, parts.netloc, path, parts.query, parts.fragment))

    def _preferred_content_locale(self, query: str, preferred_currency: str | None = None) -> str:
        if preferred_currency == "RUB":
            return "ru"
        if preferred_currency in {"USD", "EUR"}:
            return "en"
        if self._query_has_cyrillic(query):
            return "ru"
        return self.settings.funpay_locale

    def _resolve_content_locale(
        self,
        query: str,
        requested_locale: ContentLocale,
        preferred_currency: str | None = None,
    ) -> str:
        if requested_locale == ContentLocale.auto:
            return self._preferred_content_locale(query, preferred_currency=preferred_currency)
        return requested_locale.value

    def _derive_review_tokens_from_offers(self, offers: list[OfferData]) -> list[str]:
        if not offers:
            return []
        token_counter: Counter[str] = Counter()
        for offer in offers:
            tokens = set(tokenize(offer.description))
            for token in meaningful_query_tokens(token for token in tokens if len(token) >= 3):
                if token in self.REVIEW_TOKEN_STOPWORDS:
                    continue
                if not re.search(r"[a-zа-яё]", token):
                    continue
                if re.fullmatch(r"\d+", token):
                    continue
                token_counter[token] += 1
        picked: list[str] = []
        for token, _ in token_counter.most_common(20):
            picked.append(token)
            if len(picked) >= 8:
                break
        return picked

    @staticmethod
    def _extract_amounts_from_text(value: str) -> list[float]:
        if not value:
            return []
        normalized = value.replace("\xa0", " ").replace(",", ".")
        matches = re.findall(
            r"(\d+(?:\.\d+)?)\s*(?:₽|руб(?:\.|ля|лей)?|rub|usd|\$|eur|€)?",
            normalized,
            flags=re.IGNORECASE,
        )
        amounts: list[float] = []
        for raw in matches:
            try:
                amounts.append(float(raw))
            except ValueError:
                continue
        return amounts

    @staticmethod
    def _is_amount_close_to_seller_prices(amount: float, seller_prices: list[float]) -> bool:
        for price in seller_prices:
            if price <= 0:
                continue
            if abs(amount - price) <= max(2.0, price * 0.40):
                return True
        return False

    @staticmethod
    def _is_this_month_bucket(value: str | None) -> bool:
        normalized = (value or "").strip().lower()
        return normalized in {"this month", "в этом месяце"}

    @staticmethod
    def _normalized_category_ids(
        category_id: int | None,
        category_ids: list[int] | None,
    ) -> list[int]:
        seen: set[int] = set()
        result: list[int] = []
        if category_id is not None and category_id > 0:
            seen.add(category_id)
            result.append(category_id)
        for item in category_ids or []:
            value = int(item)
            if value <= 0 or value in seen:
                continue
            seen.add(value)
            result.append(value)
        return result

    def _category_tokens(
        self,
        category_game_id: int | None,
        category_ids: list[int],
    ) -> tuple[list[str], dict[int, str]]:
        if category_game_id is None and not category_ids:
            return [], {}
        try:
            games = self.client.get_categories_catalog()
        except Exception:  # noqa: BLE001
            return [], {}

        tokens: set[str] = set()
        section_name_map: dict[int, str] = {}

        def add_tokens_from_text(text_value: str) -> None:
            for token in meaningful_query_tokens(tokenize(text_value)):
                if len(token) < 3:
                    continue
                if token in self.REVIEW_TOKEN_STOPWORDS:
                    continue
                if not re.search(r"[a-zа-яё]", token):
                    continue
                tokens.add(token)

        for game in games:
            section_name_map[game.game_section_id] = game.game_name
            if category_game_id is not None and game.game_section_id == category_game_id:
                add_tokens_from_text(game.game_name)

            for section in game.sections:
                section_name_map[section.section_id] = section.full_name
                if category_game_id is not None and game.game_section_id == category_game_id:
                    add_tokens_from_text(section.full_name)
                if section.section_id in category_ids:
                    add_tokens_from_text(section.full_name)

        return sorted(tokens), section_name_map

    def _is_review_matching_purchase(
        self,
        review: ReviewData,
        seller_offers: list[OfferData],
        category_tokens: list[str],
    ) -> tuple[bool, str | None]:
        detail_tokens = set(tokenize(review.detail))
        if not detail_tokens:
            return False, "no_game_match"

        seller_tokens = set(self._derive_review_tokens_from_offers(seller_offers))
        all_game_tokens = sorted(seller_tokens | set(category_tokens))
        if not all_game_tokens:
            return False, "no_game_match"
        if not any(token in detail_tokens for token in all_game_tokens):
            return False, "no_game_match"

        amounts = self._extract_amounts_from_text(review.detail)
        if not amounts:
            return False, "no_amount"

        seller_prices = [float(offer.price) for offer in seller_offers if offer.price is not None and offer.price > 0]
        if not seller_prices:
            return False, "no_price_match"
        for amount in amounts:
            if self._is_amount_close_to_seller_prices(amount, seller_prices):
                return True, None
        return False, "no_price_match"

    def has_valid_cache(self, request_dto: AnalyzeRequestDTO, options: EffectiveAnalyzeOptionsDTO) -> bool:
        cache_key = self._cache_key(
            request_dto.query,
            request_dto.currency.value,
            options.options_hash,
            request_dto.content_locale.value,
            request_dto.ui_locale.value,
        )
        now = datetime.now(UTC)
        cached = self.db.scalar(
            select(AnalysisCache).where(
                AnalysisCache.cache_key_hash == cache_key,
                AnalysisCache.valid_until > now,
            )
        )
        return cached is not None

    @staticmethod
    def _percentile(values: list[float], percentile: float) -> float | None:
        if not values:
            return None
        ordered = sorted(values)
        if len(ordered) == 1:
            return ordered[0]
        rank = (len(ordered) - 1) * percentile
        lower_idx = int(rank)
        upper_idx = min(lower_idx + 1, len(ordered) - 1)
        fraction = rank - lower_idx
        return round(ordered[lower_idx] + (ordered[upper_idx] - ordered[lower_idx]) * fraction, 6)

    @staticmethod
    def coverage_status(counter_total: int | None, loaded_count: int) -> str:
        if counter_total is not None and loaded_count == 4000 and counter_total > loaded_count:
            return "lower_bound"
        return "full"

    @classmethod
    def compute_dumping_threshold(cls, prices: list[float]) -> float | None:
        clean_prices = [float(value) for value in prices if value is not None]
        if not clean_prices:
            return None
        q1 = cls._percentile(clean_prices, 0.25)
        q3 = cls._percentile(clean_prices, 0.75)
        if q1 is None or q3 is None:
            return None
        iqr = round(max(0.0, q3 - q1), 6)
        if iqr > 0:
            return round(q1 - 1.5 * iqr, 6)
        p10 = cls._percentile(clean_prices, 0.10)
        if p10 is None:
            return None
        return round(p10 * 0.8, 6)

    @staticmethod
    def select_dumping_offers(offers: list[OfferData], threshold: float | None) -> list[OfferData]:
        if threshold is None:
            return []
        return [offer for offer in offers if offer.price is not None and float(offer.price) < threshold]

    @classmethod
    def compute_competition_metrics(cls, offers: list[OfferData]) -> dict[str, float | None]:
        if not offers:
            return {
                "hhi": None,
                "top3_share": None,
                "price_spread": None,
            }

        seller_counts = Counter((offer.seller_id, offer.seller_name) for offer in offers)
        total_offers = len(offers)
        shares = [count / total_offers for count in seller_counts.values() if count > 0]
        hhi = sum((share * 100) ** 2 for share in shares)
        top3_share = sum(sorted(seller_counts.values(), reverse=True)[:3]) / total_offers

        prices = [offer.price for offer in offers if offer.price is not None]
        p10 = cls._percentile(prices, 0.10)
        p90 = cls._percentile(prices, 0.90)
        p50 = cls._percentile(prices, 0.50)
        price_spread = None
        if p10 is not None and p90 is not None and p50 is not None and p50 > 0:
            price_spread = round((p90 - p10) / p50, 6)

        return {
            "hhi": round(hhi, 6),
            "top3_share": round(top3_share, 6),
            "price_spread": price_spread,
        }

    @staticmethod
    def compute_demand_stats(
        relevant_reviews: list[ReviewData],
        include_index: bool,
        sellers_analyzed: int = 0,
        reviews_scanned: int = 0,
    ) -> DemandStatsDTO:
        if not relevant_reviews:
            return DemandStatsDTO(
                relevant_reviews=0,
                positive_share=0.0,
                volume_30d=0,
                demand_index=0.0 if include_index else None,
                unique_sellers_with_relevant_reviews=0,
                estimated_purchases_total=0,
                estimated_purchases_30d=0,
                sellers_analyzed=sellers_analyzed,
                reviews_scanned=reviews_scanned,
            )

        rated_reviews = [review for review in relevant_reviews if review.rating is not None]
        positives = [review for review in rated_reviews if (review.rating or 0) >= 4]
        positive_share = (len(positives) / len(rated_reviews)) if rated_reviews else 0.0

        volume_30d = sum(
            1
            for review in relevant_reviews
            if AnalyzerService._is_this_month_bucket(review.date_bucket)
        )
        unique_sellers = len({review.seller_id for review in relevant_reviews})

        demand_index: float | None = None
        if include_index:
            volume_component = min(volume_30d, 100) / 100 * 60
            quality_component = positive_share * 40
            demand_index = round(volume_component + quality_component, 4)

        return DemandStatsDTO(
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

    def _get_cached_result(self, cache_key: str) -> AnalyzeResultDTO | None:
        now = datetime.now(UTC)
        cached = self.db.scalar(
            select(AnalysisCache).where(
                AnalysisCache.cache_key_hash == cache_key,
                AnalysisCache.valid_until > now,
            )
        )
        if not cached:
            return None
        try:
            return AnalyzeResultDTO.model_validate(cached.response_json)
        except Exception:  # noqa: BLE001
            return None

    def _persist_cache(
        self,
        cache_key: str,
        request: AnalyzeRequestDTO,
        mode_label: str,
        generated_at: datetime,
        valid_until: datetime,
        result: AnalyzeResultDTO,
    ) -> None:
        existing = self.db.scalar(select(AnalysisCache).where(AnalysisCache.cache_key_hash == cache_key))
        payload = result.model_dump(mode="json")
        if existing:
            existing.query = request.query
            existing.mode = mode_label
            existing.currency = request.currency.value
            existing.generated_at = generated_at
            existing.valid_until = valid_until
            existing.response_json = payload
            return
        self.db.add(
            AnalysisCache(
                cache_key_hash=cache_key,
                query=request.query,
                mode=mode_label,
                currency=request.currency.value,
                generated_at=generated_at,
                valid_until=valid_until,
                response_json=payload,
            )
        )

    def _persist_request_done(
        self,
        request_row: AnalysisRequest,
        envelope: AnalyzeEnvelopeDTO,
        ui_locale: str = "ru",
    ) -> None:
        payload = self._read_progress_payload(request_row)
        progress = payload["progress"]
        logs = progress.get("logs", [])
        logs.append(
            {
                "ts": self._utc_iso(),
                "stage": "done",
                "message": tr(ui_locale, "progress.local.done.log"),
            }
        )
        progress["percent"] = 100.0
        progress["stage"] = "done"
        progress["message"] = tr(ui_locale, "progress.local.done")
        progress["logs"] = logs[-200:]
        payload["progress"] = progress
        payload["cache_hit"] = envelope.cache_hit
        payload["result"] = envelope.result.model_dump(mode="json") if envelope.result else None
        request_row.status = "done"
        request_row.result_json = payload
        request_row.error_text = None
        request_row.updated_at = datetime.now(UTC)

    def _persist_request_error(self, request_row: AnalysisRequest, error_text: str, ui_locale: str = "ru") -> None:
        payload = self._read_progress_payload(request_row)
        progress = payload["progress"]
        logs = progress.get("logs", [])
        logs.append(
            {
                "ts": self._utc_iso(),
                "stage": "failed",
                "message": f"{tr(ui_locale, 'error.prefix')}: {error_text}",
            }
        )
        progress["stage"] = "failed"
        progress["message"] = tr(ui_locale, "progress.local.failed")
        progress["logs"] = logs[-200:]
        payload["progress"] = progress
        request_row.status = "failed"
        request_row.error_text = error_text
        request_row.result_json = payload
        request_row.updated_at = datetime.now(UTC)

    def _prepare_request_row(
        self,
        request_dto: AnalyzeRequestDTO,
        options: EffectiveAnalyzeOptionsDTO,
        request_id: str | None = None,
        status: str = "running",
    ) -> AnalysisRequest:
        row: AnalysisRequest | None = None
        if request_id:
            row = self.db.scalar(select(AnalysisRequest).where(AnalysisRequest.id == request_id))

        if row is None:
            row = AnalysisRequest(
                id=request_id,
                query=request_dto.query,
                mode=options.mode_label,
                currency=request_dto.currency.value,
                force_refresh=request_dto.force_refresh,
                status=status,
                result_json={
                    "cache_hit": False,
                    "result": None,
                    "progress": {
                        "percent": 0.0,
                        "stage": status,
                        "message": None,
                        "logs": [],
                    },
                },
            )
        else:
            row.query = request_dto.query
            row.mode = options.mode_label
            row.currency = request_dto.currency.value
            row.force_refresh = request_dto.force_refresh
            row.status = status
            row.error_text = None
            raw_payload = row.result_json if isinstance(row.result_json, dict) else {}
            payload = json.loads(json.dumps(raw_payload))
            payload["cache_hit"] = False
            payload["result"] = None
            progress = payload.get("progress")
            if not isinstance(progress, dict):
                progress = {"percent": 0.0, "stage": status, "message": None, "logs": []}
            progress["stage"] = status
            payload["progress"] = progress
            row.result_json = payload
            row.updated_at = datetime.now(UTC)

        self.db.add(row)
        self.db.flush()
        return row

    def create_queued_request(self, request_dto: AnalyzeRequestDTO) -> tuple[str, EffectiveAnalyzeOptionsDTO]:
        options = self.resolve_options(
            request_dto.options,
            category_game_id=request_dto.category_game_id,
            category_id=request_dto.category_id,
            category_ids=request_dto.category_ids,
        )
        row = self._prepare_request_row(
            request_dto=request_dto,
            options=options,
            status="queued",
        )
        self._set_progress(
            row,
            percent=1,
            stage="queued",
            message=tr(request_dto.ui_locale.value, "progress.local.queued"),
            append_log=True,
            commit=False,
        )
        self.db.commit()
        return row.id, options

    def _cleanup_request_snapshots(self, request_id: str) -> None:
        self.db.execute(delete(SectionCoverage).where(SectionCoverage.request_id == request_id))
        self.db.execute(delete(OfferSnapshot).where(OfferSnapshot.request_id == request_id))
        self.db.execute(delete(ReviewSnapshot).where(ReviewSnapshot.request_id == request_id))

    def _persist_section_coverage(self, request_id: str, coverage_rows: list[CoverageRow]) -> None:
        for row in coverage_rows:
            self.db.add(
                SectionCoverage(
                    request_id=request_id,
                    section_url=row.section_url,
                    section_id=row.section_id,
                    counter_total=row.counter_total,
                    loaded_count=row.loaded_count,
                    coverage_status=row.coverage_status,
                )
            )

    def _persist_offers(self, request_id: str, offers: list[OfferData], currency: str) -> None:
        for offer in offers[:5000]:
            self.db.add(
                OfferSnapshot(
                    request_id=request_id,
                    section_id=offer.section_id,
                    offer_id=offer.offer_id,
                    seller_id=offer.seller_id,
                    seller_name=offer.seller_name,
                    description=offer.description,
                    price=offer.price,
                    currency=offer.currency or currency,
                    reviews_count=offer.reviews_count,
                    seller_age=offer.seller_age,
                    is_online=offer.is_online,
                    auto_delivery=offer.auto_delivery,
                )
            )

    @staticmethod
    def _review_key(review: ReviewData) -> tuple[int, str, str, str | None]:
        return (
            int(review.seller_id),
            normalize_text(review.detail),
            normalize_text(review.text),
            (review.date_bucket or "").strip().lower() or None,
        )

    def _persist_reviews(
        self,
        request_id: str,
        reviews: list[ReviewData],
        relevant_keys: set[tuple[int, str, str, str | None]],
    ) -> None:
        for review in reviews[:10000]:
            self.db.add(
                ReviewSnapshot(
                    request_id=request_id,
                    seller_id=review.seller_id,
                    detail=review.detail,
                    text=review.text,
                    rating=review.rating,
                    date_bucket=review.date_bucket,
                    is_relevant=self._review_key(review) in relevant_keys,
                )
            )

    def _persist_history(
        self,
        request_id: str,
        query: str,
        currency: str,
        options_hash: str,
        generated_at: datetime,
        result: AnalyzeResultDTO,
    ) -> None:
        self.db.add(
            AnalysisHistory(
                request_id=request_id,
                query=query,
                query_normalized=normalize_text(query),
                currency=currency,
                options_hash=options_hash,
                generated_at=generated_at,
                result_json=result.model_dump(mode="json"),
            )
        )

    def _collect_offers(
        self,
        query: str,
        options: EffectiveAnalyzeOptionsDTO,
        content_locale: str,
        category_game_id: int | None = None,
        category_id: int | None = None,
        category_ids: list[int] | None = None,
        section_name_map: dict[int, str] | None = None,
        ui_locale: str = "ru",
        progress: ProgressCallback | None = None,
    ) -> tuple[list[OfferData], list[CoverageRow], int]:
        token_list = query_tokens(query)
        meaningful_tokens = meaningful_query_tokens(token_list)
        has_query_filter = bool(token_list)
        weak_filtered_offers = 0
        selected_category_ids = self._normalized_category_ids(category_id, category_ids)
        category_scope_applied = bool(selected_category_ids) or category_game_id is not None

        quick_sections: list[str] = []
        if category_game_id is not None:
            quick_sections.extend(
                self._rewrite_url_locale(url, content_locale)
                for url in self.client.get_game_section_urls(category_game_id)
            )
        for section_id in selected_category_ids:
            quick_sections.append(self.client.section_url(section_id, locale=content_locale))
        if not category_scope_applied:
            quick_sections = [
                self._rewrite_url_locale(url, content_locale)
                for url in self.client.search_sections(query)
            ]

        dedup_sections: list[str] = []
        seen_sections: set[str] = set()
        for url in quick_sections:
            if url in seen_sections:
                continue
            seen_sections.add(url)
            dedup_sections.append(url)
        quick_sections = dedup_sections

        scanned: set[str] = set()
        matched_offers: dict[int, OfferData] = {}
        coverage_rows: list[CoverageRow] = []
        locale_hint = "RU" if content_locale == "ru" else "EN"
        if progress:
            progress(
                tr(ui_locale, "progress.local.sections.prepared", count=len(quick_sections), locale_hint=locale_hint),
                14,
                "sections",
            )

        def scan_sections(section_urls: list[str], max_to_scan: int) -> None:
            nonlocal weak_filtered_offers
            total_target = min(len(section_urls), max_to_scan)
            processed = 0
            for section_url in section_urls:
                if len(scanned) >= max_to_scan:
                    break
                if section_url in scanned:
                    continue
                scanned.add(section_url)
                processed += 1
                if progress:
                    progress(
                        tr(
                            ui_locale,
                            "progress.local.section.scan",
                            processed=processed,
                            total=max(total_target, 1),
                            section_url=section_url,
                        ),
                        15 + min(20.0, processed * 0.6),
                        "section",
                    )
                parsed = self.client.parse_section(section_url)
                status = self.coverage_status(parsed.counter_total, parsed.loaded_count)
                section_name = None
                if parsed.section_id is not None and section_name_map:
                    section_name = section_name_map.get(parsed.section_id)
                coverage_rows.append(
                    CoverageRow(
                        section_url=parsed.section_url,
                        section_id=parsed.section_id,
                        section_name=section_name,
                        counter_total=parsed.counter_total,
                        loaded_count=parsed.loaded_count,
                        coverage_status=status,
                    )
                )
                if progress:
                    progress(
                        tr(
                            ui_locale,
                            "progress.local.section.done",
                            loaded_count=parsed.loaded_count,
                            counter_total=parsed.counter_total if parsed.counter_total is not None else "n/a",
                        ),
                        17 + min(23.0, processed * 0.7),
                        "section",
                    )
                for offer in parsed.offers:
                    if (not has_query_filter) or is_text_relevant(offer.description, token_list):
                        matched_offers[offer.offer_id] = offer
                    elif has_query_filter:
                        offer_tokens = set(tokenize(offer.description))
                        has_any_query_token = any(token in offer_tokens for token in token_list)
                        has_meaningful_token = any(token in offer_tokens for token in meaningful_tokens)
                        if has_any_query_token and not has_meaningful_token and meaningful_tokens:
                            weak_filtered_offers += 1

        if category_scope_applied:
            scan_limit = len(quick_sections) if quick_sections else 1
            scan_sections(quick_sections, scan_limit)
            if progress:
                progress(
                    tr(ui_locale, "progress.local.sections.category_done", count=len(scanned)),
                    40,
                    "sections",
                )
            return list(matched_offers.values()), coverage_rows, weak_filtered_offers

        scan_sections(quick_sections, options.section_limit)

        need_fallback = (not quick_sections) or (
            len(matched_offers) < self.settings.low_coverage_min_matched_offers
        )
        if options.include_fallback_scan and need_fallback and len(scanned) < options.section_limit:
            if progress:
                progress(
                    tr(ui_locale, "progress.local.sections.fallback_start"),
                    35,
                    "sections",
                )
            all_sections = [
                self._rewrite_url_locale(url, content_locale)
                for url in self.client.get_all_sections()
            ]
            scan_sections(all_sections, options.section_limit)
            if progress:
                progress(
                    tr(ui_locale, "progress.local.sections.fallback_done", count=len(scanned)),
                    45,
                    "sections",
                )

        return list(matched_offers.values()), coverage_rows, weak_filtered_offers

    def _collect_demand_reviews(
        self,
        offers: list[OfferData],
        options: EffectiveAnalyzeOptionsDTO,
        category_game_id: int | None = None,
        category_id: int | None = None,
        category_ids: list[int] | None = None,
        category_tokens: list[str] | None = None,
        locale: str | None = None,
        ui_locale: str = "ru",
        progress: ProgressCallback | None = None,
    ) -> tuple[list[ReviewData], list[ReviewData], ReviewMatchDiagnostics, list[SellerDemandStat]]:
        seller_freq = Counter(offer.seller_id for offer in offers if offer.seller_id is not None)
        seller_ids = [seller_id for seller_id, _ in seller_freq.most_common(options.seller_limit)]
        seller_offer_map: dict[int, list[OfferData]] = defaultdict(list)
        seller_name_map: dict[int, str] = {}
        for offer in offers:
            if offer.seller_id is not None:
                seller_offer_map[offer.seller_id].append(offer)
                seller_name_map.setdefault(offer.seller_id, offer.seller_name)
        if category_tokens is None:
            category_tokens, _ = self._category_tokens(
                category_game_id=category_game_id,
                category_ids=self._normalized_category_ids(category_id, category_ids),
            )
        diagnostics = ReviewMatchDiagnostics(sellers_targeted=len(seller_ids))
        seller_stats: list[SellerDemandStat] = []
        if progress:
            progress(
                tr(ui_locale, "progress.local.reviews.sellers_prepared", count=len(seller_ids)),
                55,
                "reviews",
            )

        all_reviews: list[ReviewData] = []
        relevant_reviews: list[ReviewData] = []
        total_sellers = len(seller_ids)
        for idx, seller_id in enumerate(seller_ids, start=1):
            review_pages_limit = options.review_pages_per_seller
            # Для первых продавцов углубляем выборку отзывов, чтобы в строгом режиме
            # (игра + цена) чаще находить валидные покупки по текущей категории.
            if idx <= min(5, total_sellers):
                review_pages_limit = max(review_pages_limit, 5)
            if progress:
                progress(
                    tr(
                        ui_locale,
                        "progress.local.reviews.seller.scan",
                        idx=idx,
                        total=max(total_sellers, 1),
                        seller_id=seller_id,
                    ),
                    56 + min(20.0, idx * 0.45),
                    "seller",
                )
            try:

                def on_review_page(page_number: int) -> None:
                    if progress:
                        progress(
                            tr(
                                ui_locale,
                                "progress.local.reviews.page",
                                page=page_number,
                                seller_id=seller_id,
                            ),
                            57 + min(20.0, idx * 0.45),
                            "review-page",
                        )

                seller_reviews = self.client.get_seller_reviews(
                    seller_id=seller_id,
                    max_pages=review_pages_limit,
                    locale_override=locale,
                    page_callback=on_review_page,
                )
            except Exception:  # noqa: BLE001
                diagnostics.failed_sellers += 1
                if progress:
                    progress(
                        tr(ui_locale, "progress.local.reviews.seller.failed", seller_id=seller_id),
                        57 + min(21.0, idx * 0.45),
                        "seller",
                    )
                continue
            diagnostics.sellers_analyzed += 1
            diagnostics.reviews_scanned += len(seller_reviews)
            all_reviews.extend(seller_reviews)
            if progress:
                progress(
                    tr(
                        ui_locale,
                        "progress.local.reviews.seller.done",
                        seller_id=seller_id,
                        count=len(seller_reviews),
                    ),
                    58 + min(22.0, idx * 0.45),
                    "review",
                )
            seller_offers = seller_offer_map.get(seller_id, [])
            seller_relevant: list[ReviewData] = []
            for review in seller_reviews:
                is_match, reason = self._is_review_matching_purchase(
                    review=review,
                    seller_offers=seller_offers,
                    category_tokens=category_tokens or [],
                )
                if is_match:
                    seller_relevant.append(review)
                    continue
                if reason == "no_game_match":
                    diagnostics.no_game_match += 1
                elif reason == "no_amount":
                    diagnostics.no_amount += 1
                elif reason == "no_price_match":
                    diagnostics.no_price_match += 1
            relevant_reviews.extend(seller_relevant)
            seller_stats.append(
                SellerDemandStat(
                    seller_id=seller_id,
                    seller_name=seller_name_map.get(seller_id, f"seller_{seller_id}"),
                    reviews_scanned=len(seller_reviews),
                    estimated_purchases_total=len(seller_relevant),
                    estimated_purchases_30d=sum(
                        1 for review in seller_relevant if self._is_this_month_bucket(review.date_bucket)
                    ),
                )
            )
            if progress:
                progress(
                    tr(
                        ui_locale,
                        "progress.local.reviews.relevant_after_seller",
                        seller_id=seller_id,
                        count=len(relevant_reviews),
                    ),
                    59 + min(24.0, idx * 0.5),
                    "review",
                )
        seller_stats.sort(
            key=lambda item: (-item.estimated_purchases_30d, -item.estimated_purchases_total, -item.reviews_scanned)
        )
        return all_reviews, relevant_reviews, diagnostics, seller_stats

    @staticmethod
    def _build_histogram(prices: list[float], bins_count: int = 8) -> list[PriceHistogramBinDTO]:
        if not prices:
            return []
        min_price = min(prices)
        max_price = max(prices)
        if min_price == max_price:
            return [
                PriceHistogramBinDTO(
                    label=f"{min_price:.2f}",
                    from_price=min_price,
                    to_price=max_price,
                    count=len(prices),
                )
            ]

        step = (max_price - min_price) / bins_count
        buckets = [0 for _ in range(bins_count)]
        for price in prices:
            if step == 0:
                idx = 0
            else:
                idx = int((price - min_price) / step)
            idx = min(max(idx, 0), bins_count - 1)
            buckets[idx] += 1

        histogram: list[PriceHistogramBinDTO] = []
        for idx, count in enumerate(buckets):
            left = min_price + step * idx
            right = max_price if idx == bins_count - 1 else min_price + step * (idx + 1)
            histogram.append(
                PriceHistogramBinDTO(
                    label=f"{left:.2f} - {right:.2f}",
                    from_price=round(left, 6),
                    to_price=round(right, 6),
                    count=count,
                )
            )
        return histogram

    @staticmethod
    def _point_from_result_dict(data: dict) -> HistoryPointDTO | None:
        try:
            offers = data.get("offers_stats") or {}
            demand = data.get("demand") or {}
            generated = data.get("meta", {}).get("generated_at") or data.get("generated_at")
            if not generated:
                return None
            return HistoryPointDTO(
                generated_at=datetime.fromisoformat(generated.replace("Z", "+00:00")),
                matched_offers=int(offers.get("matched_offers", 0)),
                unique_sellers=int(offers.get("unique_sellers", 0)),
                p50_price=offers.get("p50_price"),
                demand_index=demand.get("demand_index"),
            )
        except Exception:  # noqa: BLE001
            return None

    def _load_history_rows(
        self,
        query: str,
        currency: str,
        options_hash: str,
        limit: int,
    ) -> list[AnalysisHistory]:
        query_norm = normalize_text(query)
        stmt = (
            select(AnalysisHistory)
            .where(
                AnalysisHistory.query_normalized == query_norm,
                AnalysisHistory.currency == currency,
                AnalysisHistory.options_hash == options_hash,
            )
            .order_by(desc(AnalysisHistory.generated_at))
            .limit(limit)
        )
        rows = self.db.scalars(stmt).all()
        rows.reverse()
        return rows

    def _build_history_points(
        self,
        query: str,
        currency: str,
        options_hash: str,
        limit: int,
        current_generated_at: datetime,
        current_offers_stats: OffersStatsDTO,
        current_demand: DemandStatsDTO | None,
    ) -> tuple[list[HistoryPointDTO], DeltaDTO | None]:
        rows = self._load_history_rows(query, currency, options_hash, max(1, limit))
        previous_point: HistoryPointDTO | None = None

        points: list[HistoryPointDTO] = []
        for row in rows:
            point = self._point_from_result_dict(row.result_json)
            if point is not None:
                points.append(point)
                previous_point = point

        current_point = HistoryPointDTO(
            generated_at=current_generated_at,
            matched_offers=current_offers_stats.matched_offers,
            unique_sellers=current_offers_stats.unique_sellers,
            p50_price=current_offers_stats.p50_price,
            demand_index=current_demand.demand_index if current_demand else None,
        )
        points.append(current_point)
        points = points[-limit:]

        if previous_point is None:
            return points, None

        return points, DeltaDTO(
            matched_offers_delta=current_point.matched_offers - previous_point.matched_offers,
            unique_sellers_delta=current_point.unique_sellers - previous_point.unique_sellers,
            p50_price_delta=(
                round(current_point.p50_price - previous_point.p50_price, 6)
                if current_point.p50_price is not None and previous_point.p50_price is not None
                else None
            ),
            demand_index_delta=(
                round(current_point.demand_index - previous_point.demand_index, 6)
                if current_point.demand_index is not None and previous_point.demand_index is not None
                else None
            ),
        )

    def _build_top_offers_table(self, offers: list[OfferData], limit: int = 20) -> list[TopOfferDTO]:
        top = sorted(offers, key=lambda item: item.price)[:limit]
        return [
            TopOfferDTO(
                offer_id=offer.offer_id,
                offer_url=f"https://funpay.com/lots/offer?id={offer.offer_id}",
                seller_id=offer.seller_id,
                seller_name=offer.seller_name,
                description=offer.description,
                price=round(offer.price, 6),
                currency=offer.currency,
                reviews_count=offer.reviews_count,
                is_online=offer.is_online,
                auto_delivery=offer.auto_delivery,
            )
            for offer in top
        ]

    def _build_top_sellers_table(self, offers: list[OfferData], limit: int = 20) -> list[TopSellerDTO]:
        grouped: dict[tuple[int | None, str], list[OfferData]] = defaultdict(list)
        for offer in offers:
            grouped[(offer.seller_id, offer.seller_name)].append(offer)

        rows: list[TopSellerDTO] = []
        for (seller_id, seller_name), seller_offers in grouped.items():
            prices = [item.price for item in seller_offers if item.price is not None]
            online = [item for item in seller_offers if item.is_online is not None]
            auto = [item for item in seller_offers if item.auto_delivery is not None]
            rows.append(
                TopSellerDTO(
                    seller_id=seller_id,
                    seller_name=seller_name,
                    offers_count=len(seller_offers),
                    min_price=min(prices) if prices else None,
                    p50_price=self._percentile(prices, 0.5),
                    max_price=max(prices) if prices else None,
                    online_share=(
                        round(sum(1 for item in online if item.is_online) / len(online), 4)
                        if online
                        else None
                    ),
                    auto_delivery_share=(
                        round(sum(1 for item in auto if item.auto_delivery) / len(auto), 4)
                        if auto
                        else None
                    ),
                )
            )

        rows.sort(key=lambda row: (-row.offers_count, row.min_price or float("inf")))
        return rows[:limit]

    @staticmethod
    def _build_top_demand_sellers_table(
        seller_stats: list[SellerDemandStat],
        limit: int = 20,
    ) -> list[TopDemandSellerDTO]:
        top = sorted(
            seller_stats,
            key=lambda item: (-item.estimated_purchases_30d, -item.estimated_purchases_total, -item.reviews_scanned),
        )[:limit]
        return [
            TopDemandSellerDTO(
                seller_id=item.seller_id,
                seller_name=item.seller_name,
                estimated_purchases_total=item.estimated_purchases_total,
                estimated_purchases_30d=item.estimated_purchases_30d,
                reviews_scanned=item.reviews_scanned,
            )
            for item in top
        ]

    def _build_sections_table(self, coverage_rows: list[CoverageRow], ui_locale: str = "ru") -> list[SectionRowDTO]:
        sorted_rows = sorted(
            coverage_rows,
            key=lambda row: (0 if row.coverage_status == "lower_bound" else 1, row.section_url),
        )
        section_fallback_prefix = "Section" if ui_locale == "en" else "Раздел"
        return [
            SectionRowDTO(
                section_url=row.section_url,
                section_id=row.section_id,
                section_name=row.section_name or (
                    f"{section_fallback_prefix} #{row.section_id}" if row.section_id is not None else None
                ),
                counter_total=row.counter_total,
                loaded_count=row.loaded_count,
                coverage_status=row.coverage_status,
            )
            for row in sorted_rows
        ]

    def analyze(
        self,
        request_dto: AnalyzeRequestDTO,
        request_id: str | None = None,
    ) -> AnalyzeEnvelopeDTO:
        now = datetime.now(UTC)
        valid_until = now + timedelta(hours=self.settings.cache_ttl_hours)
        ui_locale = request_dto.ui_locale.value
        selected_category_ids = self._normalized_category_ids(request_dto.category_id, request_dto.category_ids)
        request_dto.category_ids = selected_category_ids
        options = self.resolve_options(
            request_dto.options,
            category_game_id=request_dto.category_game_id,
            category_id=request_dto.category_id,
            category_ids=selected_category_ids,
        )
        cache_key = self._cache_key(
            request_dto.query,
            request_dto.currency.value,
            options.options_hash,
            request_dto.content_locale.value,
            ui_locale,
        )
        request_row = self._prepare_request_row(
            request_dto=request_dto,
            options=options,
            request_id=request_id,
            status="running",
        )
        request_key = request_row.id

        def emit_progress(message: str, percent: float | None = None, stage: str = "info") -> None:
            self._set_progress(
                request_row,
                percent=percent,
                stage=stage,
                message=message,
                append_log=True,
                commit=True,
            )

        try:
            emit_progress(tr(ui_locale, "progress.local.start"), 2, "start")
            if not request_dto.force_refresh:
                emit_progress(tr(ui_locale, "progress.local.cache.check"), 4, "cache")
                cached_result = self._get_cached_result(cache_key)
                if cached_result:
                    emit_progress(tr(ui_locale, "progress.local.cache.hit"), 100, "cache")
                    envelope = AnalyzeEnvelopeDTO(
                        request_id=request_key,
                        status="done",
                        cache_hit=True,
                        result=cached_result,
                        error=None,
                        progress=self._build_progress_dto(request_row),
                    )
                    self._persist_request_done(request_row, envelope, ui_locale=ui_locale)
                    self.db.commit()
                    envelope.progress = self._build_progress_dto(request_row)
                    return envelope

            content_locale = self._resolve_content_locale(
                request_dto.query,
                requested_locale=request_dto.content_locale,
                preferred_currency=request_dto.currency.value,
            )
            category_tokens, section_name_map = self._category_tokens(
                category_game_id=request_dto.category_game_id,
                category_ids=selected_category_ids,
            )
            emit_progress(
                tr(
                    ui_locale,
                    "progress.local.collect_offers",
                    content_locale=content_locale.upper(),
                    currency=request_dto.currency.value,
                ),
                10,
                "sections",
            )
            offers, coverage_rows, weak_filtered_offers = self._collect_offers(
                request_dto.query,
                options,
                content_locale=content_locale,
                category_game_id=request_dto.category_game_id,
                category_id=request_dto.category_id,
                category_ids=selected_category_ids,
                section_name_map=section_name_map,
                ui_locale=ui_locale,
                progress=emit_progress,
            )
            offers, excluded_by_currency, relaxed_currency_filter = self._filter_offers_by_currency(
                offers,
                request_dto.currency.value,
            )
            emit_progress(
                tr(ui_locale, "progress.local.offers.done", count=len(offers)),
                50,
                "sections",
            )
            prices = [offer.price for offer in offers if offer.price is not None]
            seller_ids = {offer.seller_id for offer in offers if offer.seller_id is not None}
            online_known = [offer for offer in offers if offer.is_online is not None]
            auto_known = [offer for offer in offers if offer.auto_delivery is not None]

            lower_bound_sections = sum(1 for row in coverage_rows if row.coverage_status == "lower_bound")
            coverage_note = (
                tr(ui_locale, "warning.coverage.lower_bound")
                if lower_bound_sections > 0
                else None
            )
            warnings: list[str] = []
            if coverage_note:
                warnings.append(coverage_note)
            if not offers:
                if request_dto.query.strip():
                    warnings.append(tr(ui_locale, "warning.offers.none_query"))
                else:
                    warnings.append(tr(ui_locale, "warning.offers.none_scope"))
            if weak_filtered_offers > 0:
                warnings.append(
                    tr(ui_locale, "warning.offers.weak_filtered", count=weak_filtered_offers)
                )
            if excluded_by_currency > 0:
                if relaxed_currency_filter:
                    warnings.append(
                        tr(
                            ui_locale,
                            "warning.currency.relaxed",
                            currency=request_dto.currency.value,
                            count=excluded_by_currency,
                        )
                    )
                else:
                    warnings.append(
                        tr(
                            ui_locale,
                            "warning.currency.filtered",
                            currency=request_dto.currency.value,
                            count=excluded_by_currency,
                        )
                    )
            if request_dto.category_game_id is not None and not selected_category_ids and not coverage_rows:
                warnings.append(tr(ui_locale, "warning.category.game_not_found"))
            if request_dto.category_id is not None and not coverage_rows:
                warnings.append(tr(ui_locale, "warning.category.section_not_found"))
            if selected_category_ids and not coverage_rows and request_dto.category_id is None:
                warnings.append(tr(ui_locale, "warning.category.sections_not_found"))

            offers_stats = OffersStatsDTO(
                matched_offers=len(offers),
                unique_sellers=len(seller_ids),
                min_price=min(prices) if prices else None,
                avg_price=round(sum(prices) / len(prices), 6) if prices else None,
                p50_price=self._percentile(prices, 0.50),
                p90_price=self._percentile(prices, 0.90),
                max_price=max(prices) if prices else None,
                online_share=(
                    round(sum(1 for offer in online_known if offer.is_online) / len(online_known), 4)
                    if online_known
                    else None
                ),
                auto_delivery_share=(
                    round(sum(1 for offer in auto_known if offer.auto_delivery) / len(auto_known), 4)
                    if auto_known
                    else None
                ),
            )
            coverage = CoverageDTO(
                sections_scanned=len(coverage_rows),
                sections_lower_bound=lower_bound_sections,
                coverage_note=coverage_note,
            )

            demand_stats: DemandStatsDTO | None = None
            all_reviews: list[ReviewData] = []
            relevant_reviews: list[ReviewData] = []
            demand_diagnostics = ReviewMatchDiagnostics()
            top_demand_sellers: list[SellerDemandStat] = []
            if options.include_reviews:
                emit_progress(tr(ui_locale, "progress.local.reviews.start"), 55, "reviews")
                all_reviews, relevant_reviews, demand_diagnostics, top_demand_sellers = self._collect_demand_reviews(
                    offers=offers,
                    options=options,
                    category_game_id=request_dto.category_game_id,
                    category_id=request_dto.category_id,
                    category_ids=selected_category_ids,
                    category_tokens=category_tokens,
                    locale=content_locale,
                    ui_locale=ui_locale,
                    progress=emit_progress,
                )
                demand_stats = self.compute_demand_stats(
                    relevant_reviews=relevant_reviews,
                    include_index=options.include_demand_index,
                    sellers_analyzed=demand_diagnostics.sellers_analyzed,
                    reviews_scanned=demand_diagnostics.reviews_scanned,
                )
                emit_progress(
                    tr(
                        ui_locale,
                        "progress.local.reviews.done",
                        count=demand_stats.estimated_purchases_total,
                    ),
                    80,
                    "reviews",
                )
                if demand_diagnostics.failed_sellers > 0:
                    warnings.append(
                        tr(
                            ui_locale,
                            "warning.reviews.failed_sellers",
                            count=demand_diagnostics.failed_sellers,
                        )
                    )
                if demand_diagnostics.no_amount > 0:
                    warnings.append(tr(ui_locale, "warning.reviews.no_amount"))
                if demand_diagnostics.no_game_match > 0:
                    warnings.append(tr(ui_locale, "warning.reviews.no_game"))
                if demand_diagnostics.no_price_match > 0:
                    warnings.append(tr(ui_locale, "warning.reviews.no_price"))
                if demand_stats.relevant_reviews == 0:
                    warnings.append(tr(ui_locale, "warning.reviews.none_relevant"))
            else:
                emit_progress(tr(ui_locale, "progress.local.reviews.skipped"), 78, "reviews")

            emit_progress(tr(ui_locale, "progress.local.aggregate"), 86, "aggregate")
            history_points, delta_vs_previous = self._build_history_points(
                query=request_dto.query,
                currency=request_dto.currency.value,
                options_hash=options.options_hash,
                limit=options.history_points_limit,
                current_generated_at=now,
                current_offers_stats=offers_stats,
                current_demand=demand_stats,
            )

            result = AnalyzeResultDTO(
                meta=AnalyzeMetaDTO(
                    query=request_dto.query,
                    currency=request_dto.currency,
                    ui_locale=request_dto.ui_locale,
                    content_locale_requested=request_dto.content_locale,
                    content_locale_applied=content_locale,
                    category_game_id=request_dto.category_game_id,
                    category_id=request_dto.category_id,
                    category_ids=selected_category_ids,
                    generated_at=now,
                    valid_until=valid_until,
                    effective_options=options,
                ),
                offers_stats=offers_stats,
                coverage=coverage,
                demand=demand_stats,
                charts=ChartsDTO(
                    price_histogram=self._build_histogram(prices),
                    history_points=history_points,
                    delta_vs_previous=delta_vs_previous,
                ),
                tables=TablesDTO(
                    top_offers=self._build_top_offers_table(offers),
                    top_sellers=self._build_top_sellers_table(offers),
                    top_demand_sellers=self._build_top_demand_sellers_table(top_demand_sellers),
                    sections=self._build_sections_table(coverage_rows, ui_locale=ui_locale),
                ),
                warnings=warnings,
            )

            emit_progress(tr(ui_locale, "progress.local.persist"), 92, "persist")
            self._cleanup_request_snapshots(request_key)
            self._persist_cache(
                cache_key=cache_key,
                request=request_dto,
                mode_label=options.mode_label,
                generated_at=now,
                valid_until=valid_until,
                result=result,
            )
            self._persist_section_coverage(request_key, coverage_rows)
            self._persist_offers(request_key, offers, request_dto.currency.value)
            if options.include_reviews:
                relevant_keys = {self._review_key(review) for review in relevant_reviews}
                self._persist_reviews(request_key, all_reviews, relevant_keys)
            self._persist_history(
                request_id=request_key,
                query=request_dto.query,
                currency=request_dto.currency.value,
                options_hash=options.options_hash,
                generated_at=now,
                result=result,
            )

            envelope = AnalyzeEnvelopeDTO(
                request_id=request_key,
                status="done",
                cache_hit=False,
                result=result,
                error=None,
                progress=self._build_progress_dto(request_row),
            )
            self._persist_request_done(request_row, envelope, ui_locale=ui_locale)
            self.db.commit()
            envelope.progress = self._build_progress_dto(request_row)
            return envelope
        except Exception as exc:  # noqa: BLE001
            self.db.rollback()
            failed_row = self.db.scalar(select(AnalysisRequest).where(AnalysisRequest.id == request_key))
            if failed_row is not None:
                self._persist_request_error(failed_row, str(exc), ui_locale=ui_locale)
                self.db.commit()
            raise

    def get_request_status(self, request_id: str) -> AnalyzeEnvelopeDTO:
        row = self.db.scalar(select(AnalysisRequest).where(AnalysisRequest.id == request_id))
        if not row:
            raise ValueError("Запрос не найден")

        cache_hit = False
        result: AnalyzeResultDTO | None = None
        progress = self._build_progress_dto(row)
        if row.result_json:
            payload = row.result_json
            if isinstance(payload, dict) and "result" in payload:
                cache_hit = bool(payload.get("cache_hit", False))
                if payload.get("result") is not None:
                    try:
                        result = AnalyzeResultDTO.model_validate(payload.get("result"))
                    except Exception:  # noqa: BLE001
                        result = None

        return AnalyzeEnvelopeDTO(
            request_id=row.id,
            status=row.status,
            cache_hit=cache_hit,
            result=result,
            error=row.error_text,
            progress=progress,
        )

    @staticmethod
    def _snapshot_to_offer_dto(snapshot: OfferSnapshot) -> OfferSnapshotDTO:
        return OfferSnapshotDTO(
            offer_id=snapshot.offer_id,
            offer_url=f"https://funpay.com/lots/offer?id={snapshot.offer_id}",
            section_id=snapshot.section_id,
            seller_id=snapshot.seller_id,
            seller_name=snapshot.seller_name,
            description=snapshot.description,
            price=round(float(snapshot.price), 6),
            currency=snapshot.currency,
            reviews_count=snapshot.reviews_count,
            seller_age=snapshot.seller_age,
            is_online=snapshot.is_online,
            auto_delivery=snapshot.auto_delivery,
        )

    def list_request_offers(
        self,
        request_id: str,
        *,
        limit: int = 500,
        offset: int = 0,
        price_min: float | None = None,
        price_max: float | None = None,
        min_reviews: int | None = None,
        online_only: bool = False,
        auto_delivery_only: bool = False,
        seller_query: str | None = None,
    ) -> OffersSliceResponseDTO:
        request_exists = self.db.scalar(
            select(AnalysisRequest.id).where(AnalysisRequest.id == request_id)
        )
        if request_exists is None:
            raise ValueError("Запрос не найден")

        filters = [OfferSnapshot.request_id == request_id]
        if price_min is not None:
            filters.append(OfferSnapshot.price >= price_min)
        if price_max is not None:
            filters.append(OfferSnapshot.price <= price_max)
        if min_reviews is not None:
            filters.append(func.coalesce(OfferSnapshot.reviews_count, 0) >= min_reviews)
        if online_only:
            filters.append(OfferSnapshot.is_online.is_(True))
        if auto_delivery_only:
            filters.append(OfferSnapshot.auto_delivery.is_(True))
        if seller_query is not None and seller_query.strip():
            normalized = seller_query.strip().lower()
            text_filter = func.lower(OfferSnapshot.seller_name).like(f"%{normalized}%")
            if normalized.isdigit():
                filters.append(or_(text_filter, OfferSnapshot.seller_id == int(normalized)))
            else:
                filters.append(text_filter)

        total = int(
            self.db.scalar(
                select(func.count()).select_from(OfferSnapshot).where(*filters)
            )
            or 0
        )
        rows = self.db.scalars(
            select(OfferSnapshot)
            .where(*filters)
            .order_by(OfferSnapshot.price.asc(), OfferSnapshot.offer_id.asc())
            .offset(max(0, offset))
            .limit(max(1, limit))
        ).all()

        return OffersSliceResponseDTO(
            request_id=request_id,
            total=total,
            limit=max(1, limit),
            offset=max(0, offset),
            items=[self._snapshot_to_offer_dto(item) for item in rows],
        )

    @staticmethod
    def _history_item_from_row(row: AnalysisHistory) -> HistoryItemDTO | None:
        try:
            payload = row.result_json if isinstance(row.result_json, dict) else {}
            meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
            offers = payload.get("offers_stats") if isinstance(payload.get("offers_stats"), dict) else {}
            demand = payload.get("demand") if isinstance(payload.get("demand"), dict) else {}
            warnings = payload.get("warnings")
            warnings_count = len(warnings) if isinstance(warnings, list) else 0
            return HistoryItemDTO(
                request_id=row.request_id,
                query=row.query,
                currency=row.currency,
                ui_locale=meta.get("ui_locale", "ru"),
                generated_at=row.generated_at,
                category_game_id=meta.get("category_game_id"),
                category_id=meta.get("category_id"),
                category_ids=meta.get("category_ids") or [],
                matched_offers=int(offers.get("matched_offers", 0)),
                unique_sellers=int(offers.get("unique_sellers", 0)),
                p50_price=offers.get("p50_price"),
                demand_index=demand.get("demand_index"),
                warnings_count=warnings_count,
            )
        except Exception:  # noqa: BLE001
            return None

    def list_completed_history(self, limit: int = 100) -> HistoryResponseDTO:
        safe_limit = max(1, min(limit, 500))
        rows = self.db.scalars(
            select(AnalysisHistory).order_by(desc(AnalysisHistory.generated_at)).limit(safe_limit)
        ).all()
        items: list[HistoryItemDTO] = []
        for row in rows:
            parsed = self._history_item_from_row(row)
            if parsed is not None:
                items.append(parsed)
        return HistoryResponseDTO(
            generated_at=datetime.now(UTC),
            items=items,
        )

    def ensure_pg_trgm(self) -> None:
        # Не падаем на SQLite и на БД без прав на CREATE EXTENSION.
        try:
            self.db.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
            self.db.commit()
        except Exception:  # noqa: BLE001
            self.db.rollback()
