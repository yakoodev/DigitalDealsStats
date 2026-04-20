from __future__ import annotations

import json
from collections import defaultdict
from datetime import UTC, datetime

from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.models import AnalysisRequest
from app.schemas.v2 import (
    AnalyzeV2EnvelopeDTO,
    AnalyzeV2RequestDTO,
    CommonFiltersDTO,
    HistoryMarketplaceItemDTO,
    HistoryRunItemDTO,
    HistoryV2ResponseDTO,
    MarketplaceOffersResponseDTO,
    MarketplaceRunResultDTO,
    MarketplaceSlug,
    MarketplaceSummaryDTO,
    MarketplacesCatalogResponseDTO,
    OffersStatsV2DTO,
    OverviewAggregatesDTO,
    OverviewV2DTO,
    ProgressLogV2DTO,
    ProgressV2DTO,
)
from app.services.i18n import tr
from app.services.marketplaces.registry import MarketplaceRegistry
from app.services.text_utils import repair_mojibake_cyrillic


class GlobalAnalyzerService:
    RUN_MODE = "global_v2"

    def __init__(self, db: Session, settings: Settings) -> None:
        self.db = db
        self.settings = settings
        self.registry = MarketplaceRegistry(db=db, settings=settings)

    @staticmethod
    def _utc_now() -> datetime:
        return datetime.now(UTC)

    @classmethod
    def _utc_iso(cls) -> str:
        return cls._utc_now().isoformat().replace("+00:00", "Z")

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

    @classmethod
    def _offers_stats_from_normalized(cls, offers: list[dict]) -> OffersStatsV2DTO:
        prices = [float(item["price"]) for item in offers if item.get("price") is not None]
        seller_ids = {
            item.get("seller_id")
            for item in offers
            if item.get("seller_id") is not None
        }
        online_known = [item for item in offers if item.get("is_online") is not None]
        auto_known = [item for item in offers if item.get("auto_delivery") is not None]
        return OffersStatsV2DTO(
            matched_offers=len(offers),
            unique_sellers=len(seller_ids),
            min_price=min(prices) if prices else None,
            avg_price=round(sum(prices) / len(prices), 6) if prices else None,
            p50_price=cls._percentile(prices, 0.50),
            p90_price=cls._percentile(prices, 0.90),
            max_price=max(prices) if prices else None,
            online_share=(
                round(sum(1 for item in online_known if item.get("is_online")) / len(online_known), 4)
                if online_known
                else None
            ),
            auto_delivery_share=(
                round(sum(1 for item in auto_known if item.get("auto_delivery")) / len(auto_known), 4)
                if auto_known
                else None
            ),
        )

    @classmethod
    def _offers_stats_from_summaries(
        cls,
        summaries: list[MarketplaceSummaryDTO],
    ) -> OffersStatsV2DTO:
        if not summaries:
            return OffersStatsV2DTO(
                matched_offers=0,
                unique_sellers=0,
                min_price=None,
                avg_price=None,
                p50_price=None,
                p90_price=None,
                max_price=None,
                online_share=None,
                auto_delivery_share=None,
            )
        matched = [item.offers_stats.matched_offers for item in summaries]
        unique_sellers = [item.offers_stats.unique_sellers for item in summaries]
        min_prices = [item.offers_stats.min_price for item in summaries if item.offers_stats.min_price is not None]
        max_prices = [item.offers_stats.max_price for item in summaries if item.offers_stats.max_price is not None]
        p50_values = [item.offers_stats.p50_price for item in summaries if item.offers_stats.p50_price is not None]
        p90_values = [item.offers_stats.p90_price for item in summaries if item.offers_stats.p90_price is not None]

        weighted_price_sum = 0.0
        weighted_price_count = 0
        for item in summaries:
            if item.offers_stats.avg_price is None:
                continue
            offers_count = max(0, int(item.offers_stats.matched_offers))
            weighted_price_sum += float(item.offers_stats.avg_price) * offers_count
            weighted_price_count += offers_count

        online_values = [item.offers_stats.online_share for item in summaries if item.offers_stats.online_share is not None]
        auto_values = [
            item.offers_stats.auto_delivery_share
            for item in summaries
            if item.offers_stats.auto_delivery_share is not None
        ]

        return OffersStatsV2DTO(
            matched_offers=sum(matched),
            unique_sellers=sum(unique_sellers),
            min_price=min(min_prices) if min_prices else None,
            avg_price=(round(weighted_price_sum / weighted_price_count, 6) if weighted_price_count > 0 else None),
            p50_price=(round(sum(p50_values) / len(p50_values), 6) if p50_values else None),
            p90_price=(round(sum(p90_values) / len(p90_values), 6) if p90_values else None),
            max_price=max(max_prices) if max_prices else None,
            online_share=(round(sum(online_values) / len(online_values), 4) if online_values else None),
            auto_delivery_share=(round(sum(auto_values) / len(auto_values), 4) if auto_values else None),
        )

    def _read_payload(self, row: AnalysisRequest) -> dict:
        raw_payload = row.result_json if isinstance(row.result_json, dict) else {}
        payload = json.loads(json.dumps(raw_payload))
        if "progress" not in payload or not isinstance(payload.get("progress"), dict):
            payload["progress"] = {
                "percent": 0.0,
                "stage": row.status,
                "message": None,
                "logs": [],
            }
        logs = payload["progress"].get("logs")
        if not isinstance(logs, list):
            payload["progress"]["logs"] = []
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
        payload = self._read_payload(row)
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
        row.updated_at = self._utc_now()
        self.db.add(row)
        if commit:
            self.db.commit()

    def _build_progress_dto(self, row: AnalysisRequest) -> ProgressV2DTO | None:
        payload = row.result_json if isinstance(row.result_json, dict) else {}
        progress_raw = payload.get("progress")
        if not isinstance(progress_raw, dict):
            return None
        logs_raw = progress_raw.get("logs")
        logs: list[ProgressLogV2DTO] = []
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
                    ProgressLogV2DTO(
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
        return ProgressV2DTO(
            percent=max(0.0, min(100.0, percent_value)),
            stage=stage_value if isinstance(stage_value, str) else None,
            message=message_value if isinstance(message_value, str) else None,
            logs=logs,
        )

    def _prepare_run_row(
        self,
        payload: AnalyzeV2RequestDTO,
        run_id: str | None = None,
        status: str = "running",
    ) -> AnalysisRequest:
        row: AnalysisRequest | None = None
        if run_id:
            row = self.db.scalar(select(AnalysisRequest).where(AnalysisRequest.id == run_id))

        query = payload.common_filters.query
        currency = payload.common_filters.currency.value
        marketplace_filters_payload = payload.marketplace_filters.model_dump(mode="json")
        self._mask_sensitive_marketplace_filters(marketplace_filters_payload)
        request_filters_payload = {
            "marketplaces": [item.value for item in payload.marketplaces],
            "common_filters": payload.common_filters.model_dump(mode="json"),
            "marketplace_filters": marketplace_filters_payload,
        }
        if row is None:
            row = AnalysisRequest(
                id=run_id,
                query=query,
                mode=self.RUN_MODE,
                currency=currency,
                force_refresh=payload.common_filters.force_refresh,
                status=status,
                result_json={
                    "progress": {
                        "percent": 0.0,
                        "stage": status,
                        "message": None,
                        "logs": [],
                    },
                    "request_filters": request_filters_payload,
                },
            )
        else:
            row.query = query
            row.mode = self.RUN_MODE
            row.currency = currency
            row.force_refresh = payload.common_filters.force_refresh
            row.status = status
            row.error_text = None
            payload_json = self._read_payload(row)
            payload_json["overview"] = None
            payload_json["marketplace_summaries"] = {}
            payload_json["marketplace_results"] = {}
            payload_json["request_filters"] = request_filters_payload
            progress = payload_json.get("progress", {})
            progress["stage"] = status
            payload_json["progress"] = progress
            row.result_json = payload_json
            row.updated_at = self._utc_now()
        self.db.add(row)
        self.db.flush()
        return row

    @staticmethod
    def _mask_sensitive_map(raw: object) -> object:
        if not isinstance(raw, dict):
            return raw
        masked: dict[str, object] = {}
        for key, value in raw.items():
            if isinstance(value, str) and value.strip():
                masked[str(key)] = "***"
            else:
                masked[str(key)] = value
        return masked

    @classmethod
    def _mask_sensitive_marketplace_filters(cls, payload: dict) -> None:
        if not isinstance(payload, dict):
            return
        playerok = payload.get("playerok")
        if not isinstance(playerok, dict):
            return
        if "advanced_headers" in playerok:
            playerok["advanced_headers"] = cls._mask_sensitive_map(playerok.get("advanced_headers"))
        if "advanced_cookies" in playerok:
            playerok["advanced_cookies"] = cls._mask_sensitive_map(playerok.get("advanced_cookies"))

    def create_queued_run(self, payload: AnalyzeV2RequestDTO) -> str:
        row = self._prepare_run_row(payload=payload, status="queued")
        ui_locale = payload.common_filters.ui_locale.value
        self._set_progress(
            row,
            percent=1,
            stage="queued",
            message=tr(ui_locale, "progress.global.queued"),
            append_log=True,
            commit=False,
        )
        self.db.commit()
        return row.id

    def validate_marketplaces(self, marketplaces: list[MarketplaceSlug]) -> None:
        for slug in marketplaces:
            self.registry.provider_for(slug)

    @staticmethod
    def is_heavy_request(payload: AnalyzeV2RequestDTO) -> bool:
        if len(payload.marketplaces) > 1:
            return True
        for slug in payload.marketplaces:
            raw_filters = getattr(payload.marketplace_filters, slug.value, None)
            if raw_filters is None:
                continue
            options = getattr(raw_filters, "options", None)
            if options is None:
                continue
            profile_value = options.profile.value if hasattr(options.profile, "value") else str(options.profile)
            if profile_value == "deep":
                return True
            if options.include_reviews is True or options.include_demand_index is True:
                return True
        return False

    @classmethod
    def _compute_overview(
        cls,
        selected_marketplaces: list[MarketplaceSlug],
        marketplace_results: dict[str, MarketplaceRunResultDTO],
    ) -> OverviewV2DTO:
        pooled_offers: list[dict] = []
        pooled_summaries: list[MarketplaceSummaryDTO] = []
        comparison = []
        matched_values: list[int] = []
        unique_seller_values: list[int] = []
        p50_values: list[float] = []
        use_summary_pooling = False

        for slug in selected_marketplaces:
            key = slug.value
            result = marketplace_results.get(key)
            if result is None:
                continue
            summary = result.summary
            pooled_summaries.append(summary)
            core_offers_count = len(result.core.offers)
            summary_offers_count = int(summary.offers_stats.matched_offers)
            if core_offers_count > 0:
                pooled_offers.extend([item.model_dump(mode="json") for item in result.core.offers])
            else:
                use_summary_pooling = True
            # Если core-срез неполный (например cache-hit без snapshot'ов или limit),
            # сводные pooled-метрики считаем по summary, чтобы избежать занижения.
            if summary_offers_count > core_offers_count:
                use_summary_pooling = True
            matched_values.append(summary.offers_stats.matched_offers)
            unique_seller_values.append(summary.offers_stats.unique_sellers)
            if summary.offers_stats.p50_price is not None:
                p50_values.append(summary.offers_stats.p50_price)
            comparison.append(
                {
                    "marketplace": slug,
                    "label": summary.label,
                    "matched_offers": summary.offers_stats.matched_offers,
                    "unique_sellers": summary.offers_stats.unique_sellers,
                    "p50_price": summary.offers_stats.p50_price,
                    "demand_index": summary.demand.demand_index if summary.demand else None,
                }
            )

        aggregates = OverviewAggregatesDTO(
            avg_matched_offers=(round(sum(matched_values) / len(matched_values), 4) if matched_values else None),
            avg_unique_sellers=(
                round(sum(unique_seller_values) / len(unique_seller_values), 4)
                if unique_seller_values
                else None
            ),
            avg_p50_price=(round(sum(p50_values) / len(p50_values), 6) if p50_values else None),
        )
        return OverviewV2DTO(
            generated_at=cls._utc_now(),
            marketplaces=selected_marketplaces,
            pooled_offers_stats=(
                cls._offers_stats_from_summaries(pooled_summaries)
                if use_summary_pooling or not pooled_offers
                else cls._offers_stats_from_normalized(pooled_offers)
            ),
            comparison=comparison,
            aggregates=aggregates,
        )

    @staticmethod
    def _summaries_map(
        marketplace_results: dict[str, MarketplaceRunResultDTO],
    ) -> dict[str, MarketplaceSummaryDTO]:
        return {slug: result.summary for slug, result in marketplace_results.items()}

    @staticmethod
    def _serialize_marketplace_results(
        marketplace_results: dict[str, MarketplaceRunResultDTO],
    ) -> dict[str, dict]:
        return {slug: result.model_dump(mode="json") for slug, result in marketplace_results.items()}

    def analyze(self, payload: AnalyzeV2RequestDTO, run_id: str | None = None) -> AnalyzeV2EnvelopeDTO:
        run_row = self._prepare_run_row(payload=payload, run_id=run_id, status="running")
        ui_locale = payload.common_filters.ui_locale.value

        def emit_progress(message: str, percent: float | None = None, stage: str = "info") -> None:
            self._set_progress(
                run_row,
                percent=percent,
                stage=stage,
                message=message,
                append_log=True,
                commit=True,
            )

        try:
            emit_progress(tr(ui_locale, "progress.global.validate"), 3, "validate")
            self.validate_marketplaces(payload.marketplaces)
            selected_marketplaces = [item for item in payload.marketplaces]
            emit_progress(
                tr(
                    ui_locale,
                    "progress.global.start",
                    marketplaces=", ".join(item.value for item in selected_marketplaces),
                ),
                8,
                "start",
            )
            results: dict[str, MarketplaceRunResultDTO] = {}
            filter_payload = payload.marketplace_filters.model_dump(mode="python")
            total = max(1, len(selected_marketplaces))
            for idx, slug in enumerate(selected_marketplaces, start=1):
                emit_progress(
                    tr(ui_locale, "progress.global.marketplace.start", idx=idx, total=total, slug=slug.value),
                    10 + min(75.0, idx * (70.0 / total)),
                    f"marketplace:{slug.value}",
                )
                provider = self.registry.provider_for(slug)
                marketplace_filters = filter_payload.get(slug.value)
                results[slug.value] = provider.analyze(payload.common_filters, marketplace_filters)
                emit_progress(
                    tr(ui_locale, "progress.global.marketplace.done", slug=slug.value),
                    12 + min(77.0, idx * (72.0 / total)),
                    f"marketplace:{slug.value}",
                )

            overview = self._compute_overview(selected_marketplaces, results)
            summaries = self._summaries_map(results)
            envelope = AnalyzeV2EnvelopeDTO(
                run_id=run_row.id,
                status="done",
                error=None,
                progress=self._build_progress_dto(run_row),
                overview=overview,
                marketplaces=summaries,
            )

            payload_json = self._read_payload(run_row)
            payload_json["envelope"] = envelope.model_dump(mode="json")
            payload_json["overview"] = overview.model_dump(mode="json")
            payload_json["marketplace_summaries"] = {
                key: value.model_dump(mode="json") for key, value in summaries.items()
            }
            payload_json["marketplace_results"] = self._serialize_marketplace_results(results)
            progress_payload = payload_json.get("progress", {})
            logs = progress_payload.get("logs", [])
            if isinstance(logs, list):
                logs.append(
                    {
                        "ts": self._utc_iso(),
                        "stage": "done",
                        "message": tr(ui_locale, "progress.global.done"),
                    }
                )
                progress_payload["logs"] = logs[-200:]
            progress_payload["percent"] = 100.0
            progress_payload["stage"] = "done"
            progress_payload["message"] = tr(ui_locale, "progress.global.done")
            payload_json["progress"] = progress_payload
            run_row.result_json = payload_json
            run_row.status = "done"
            run_row.error_text = None
            run_row.updated_at = self._utc_now()
            self.db.add(run_row)
            self.db.commit()
            envelope.progress = self._build_progress_dto(run_row)
            return envelope
        except Exception as exc:  # noqa: BLE001
            self.db.rollback()
            failed_row = self.db.scalar(select(AnalysisRequest).where(AnalysisRequest.id == run_row.id))
            if failed_row is not None:
                payload_json = self._read_payload(failed_row)
                progress = payload_json.get("progress", {})
                logs = progress.get("logs", [])
                logs.append(
                    {
                        "ts": self._utc_iso(),
                        "stage": "failed",
                        "message": f"{tr(ui_locale, 'error.prefix')}: {exc}",
                    }
                )
                progress["logs"] = logs[-200:]
                progress["stage"] = "failed"
                progress["message"] = tr(ui_locale, "progress.global.failed")
                payload_json["progress"] = progress
                failed_row.status = "failed"
                failed_row.error_text = str(exc)
                failed_row.result_json = payload_json
                failed_row.updated_at = self._utc_now()
                self.db.add(failed_row)
                self.db.commit()
            raise

    def _load_run_row(self, run_id: str) -> AnalysisRequest:
        row = self.db.scalar(
            select(AnalysisRequest).where(
                AnalysisRequest.id == run_id,
                AnalysisRequest.mode == self.RUN_MODE,
            )
        )
        if row is None:
            raise ValueError("Запуск не найден")
        return row

    @staticmethod
    def _parse_marketplace_slug(value: str) -> MarketplaceSlug:
        try:
            return MarketplaceSlug(value)
        except Exception as exc:  # noqa: BLE001
            raise ValueError("Неизвестная площадка") from exc

    def get_status(self, run_id: str) -> AnalyzeV2EnvelopeDTO:
        row = self._load_run_row(run_id)
        payload = row.result_json if isinstance(row.result_json, dict) else {}
        progress = self._build_progress_dto(row)
        if isinstance(payload.get("envelope"), dict):
            envelope = AnalyzeV2EnvelopeDTO.model_validate(payload["envelope"])
            envelope.progress = progress
            envelope.status = row.status
            envelope.error = row.error_text
            return envelope
        return AnalyzeV2EnvelopeDTO(
            run_id=row.id,
            status=row.status,
            error=row.error_text,
            progress=progress,
            overview=None,
            marketplaces={},
        )

    def get_overview(self, run_id: str) -> OverviewV2DTO:
        row = self._load_run_row(run_id)
        payload = row.result_json if isinstance(row.result_json, dict) else {}
        overview_raw = payload.get("overview")
        if not isinstance(overview_raw, dict):
            raise ValueError("Общий результат еще не готов")
        return OverviewV2DTO.model_validate(overview_raw)

    def get_marketplace_result(self, run_id: str, marketplace: str) -> MarketplaceRunResultDTO:
        row = self._load_run_row(run_id)
        slug = self._parse_marketplace_slug(marketplace)
        payload = row.result_json if isinstance(row.result_json, dict) else {}
        raw = payload.get("marketplace_results")
        if not isinstance(raw, dict):
            raise ValueError("Результаты по площадкам еще не готовы")
        item = raw.get(slug.value)
        if not isinstance(item, dict):
            raise ValueError("Результат по площадке не найден")
        return MarketplaceRunResultDTO.model_validate(item)

    @staticmethod
    def _generic_filter_offers(
        offers: list,
        *,
        limit: int,
        offset: int,
        price_min: float | None = None,
        price_max: float | None = None,
        min_reviews: int | None = None,
        online_only: bool = False,
        auto_delivery_only: bool = False,
        seller_query: str | None = None,
    ) -> tuple[list, int]:
        query = seller_query.strip().lower() if seller_query else None
        filtered = []
        for item in offers:
            if price_min is not None and item.price < price_min:
                continue
            if price_max is not None and item.price > price_max:
                continue
            if min_reviews is not None and (item.reviews_count or 0) < min_reviews:
                continue
            if online_only and item.is_online is not True:
                continue
            if auto_delivery_only and item.auto_delivery is not True:
                continue
            if query:
                sid = "" if item.seller_id is None else str(item.seller_id)
                if query not in item.seller_name.lower() and query not in sid:
                    continue
            filtered.append(item)
        total = len(filtered)
        safe_limit = max(1, limit)
        safe_offset = max(0, offset)
        return filtered[safe_offset : safe_offset + safe_limit], total

    def get_marketplace_offers(
        self,
        run_id: str,
        marketplace: str,
        *,
        limit: int = 500,
        offset: int = 0,
        price_min: float | None = None,
        price_max: float | None = None,
        min_reviews: int | None = None,
        online_only: bool = False,
        auto_delivery_only: bool = False,
        seller_query: str | None = None,
    ) -> MarketplaceOffersResponseDTO:
        result = self.get_marketplace_result(run_id, marketplace)
        slug = result.summary.marketplace
        try:
            provider = self.registry.provider_for(slug)
            response = provider.list_offers(
                result,
                limit=limit,
                offset=offset,
                price_min=price_min,
                price_max=price_max,
                min_reviews=min_reviews,
                online_only=online_only,
                auto_delivery_only=auto_delivery_only,
                seller_query=seller_query,
            )
            response.run_id = run_id
            return response
        except Exception:
            items, total = self._generic_filter_offers(
                result.core.offers,
                limit=limit,
                offset=offset,
                price_min=price_min,
                price_max=price_max,
                min_reviews=min_reviews,
                online_only=online_only,
                auto_delivery_only=auto_delivery_only,
                seller_query=seller_query,
            )
            return MarketplaceOffersResponseDTO(
                run_id=run_id,
                marketplace=slug,
                total=total,
                limit=max(1, limit),
                offset=max(0, offset),
                items=items,
            )

    def list_marketplaces(self) -> MarketplacesCatalogResponseDTO:
        return MarketplacesCatalogResponseDTO(
            generated_at=self._utc_now(),
            items=self.registry.catalog_dto(),
        )

    def list_history(self, limit: int = 100) -> HistoryV2ResponseDTO:
        safe_limit = max(1, min(limit, 500))
        rows = self.db.scalars(
            select(AnalysisRequest)
            .where(AnalysisRequest.mode == self.RUN_MODE, AnalysisRequest.status == "done")
            .order_by(desc(AnalysisRequest.updated_at))
            .limit(safe_limit)
        ).all()
        items: list[HistoryRunItemDTO] = []
        for row in rows:
            payload = row.result_json if isinstance(row.result_json, dict) else {}
            overview_raw = payload.get("overview")
            summaries_raw = payload.get("marketplace_summaries")
            if not isinstance(overview_raw, dict) or not isinstance(summaries_raw, dict):
                continue
            request_filters = payload.get("request_filters")
            common_filters = (
                request_filters.get("common_filters")
                if isinstance(request_filters, dict)
                else None
            )
            marketplace_filters = (
                request_filters.get("marketplace_filters")
                if isinstance(request_filters, dict)
                else None
            )
            funpay_filters = (
                marketplace_filters.get("funpay")
                if isinstance(marketplace_filters, dict)
                else None
            )
            playerok_filters = (
                marketplace_filters.get("playerok")
                if isinstance(marketplace_filters, dict)
                else None
            )
            try:
                overview = OverviewV2DTO.model_validate(overview_raw)
            except Exception:  # noqa: BLE001
                continue
            marketplace_items: list[HistoryMarketplaceItemDTO] = []
            for key, value in summaries_raw.items():
                if not isinstance(value, dict):
                    continue
                try:
                    summary = MarketplaceSummaryDTO.model_validate(value)
                except Exception:  # noqa: BLE001
                    continue
                marketplace_items.append(
                    HistoryMarketplaceItemDTO(
                        marketplace=summary.marketplace,
                        label=summary.label,
                        matched_offers=summary.offers_stats.matched_offers,
                        unique_sellers=summary.offers_stats.unique_sellers,
                        p50_price=summary.offers_stats.p50_price,
                        demand_index=summary.demand.demand_index if summary.demand else None,
                        warnings_count=len(summary.warnings),
                    )
                )
            items.append(
                HistoryRunItemDTO(
                    run_id=row.id,
                    query=repair_mojibake_cyrillic(row.query),
                    currency=row.currency,
                    ui_locale=(common_filters.get("ui_locale", "ru") if isinstance(common_filters, dict) else "ru"),
                    generated_at=overview.generated_at,
                    marketplaces=overview.marketplaces,
                    category_game_id=(
                        funpay_filters.get("category_game_id")
                        if isinstance(funpay_filters, dict)
                        else None
                    ),
                    category_id=(
                        funpay_filters.get("category_id")
                        if isinstance(funpay_filters, dict)
                        else None
                    ),
                    category_ids=(
                        [
                            int(item)
                            for item in (funpay_filters.get("category_ids") or [])
                            if str(item).isdigit()
                        ]
                        if isinstance(funpay_filters, dict)
                        else []
                    ),
                    category_game_slug=(
                        playerok_filters.get("category_game_slug")
                        if isinstance(playerok_filters, dict)
                        else None
                    ),
                    category_slugs=(
                        [
                            str(item).strip()
                            for item in (playerok_filters.get("category_slugs") or [])
                            if str(item).strip()
                        ]
                        if isinstance(playerok_filters, dict)
                        else []
                    ),
                    pooled_matched_offers=overview.pooled_offers_stats.matched_offers,
                    pooled_unique_sellers=overview.pooled_offers_stats.unique_sellers,
                    pooled_p50_price=overview.pooled_offers_stats.p50_price,
                    marketplace_items=marketplace_items,
                )
            )
        return HistoryV2ResponseDTO(generated_at=self._utc_now(), items=items)

    def funpay_categories(self):
        provider = self.registry.provider_for(MarketplaceSlug.funpay)
        return provider.categories()

    def playerok_categories(self, game_slug: str | None = None):
        provider = self.registry.provider_for(MarketplaceSlug.playerok)
        return provider.categories(game_slug=game_slug)
