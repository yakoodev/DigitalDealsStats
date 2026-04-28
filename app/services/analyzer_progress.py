from __future__ import annotations

import json
from datetime import UTC, datetime

from sqlalchemy.orm import Session

from app.models import AnalysisRequest
from app.schemas.analyze import AnalyzeProgressDTO, ProgressLogDTO


def utc_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def read_progress_payload(row: AnalysisRequest) -> dict:
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


def set_progress(
    db: Session,
    row: AnalysisRequest,
    *,
    percent: float | None = None,
    stage: str | None = None,
    message: str | None = None,
    append_log: bool = False,
    commit: bool = True,
) -> None:
    payload = read_progress_payload(row)
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
                "ts": utc_iso(),
                "stage": stage or progress.get("stage") or "info",
                "message": message,
            }
        )
        progress["logs"] = logs[-200:]
    payload["progress"] = progress
    row.result_json = payload
    row.updated_at = datetime.now(UTC)
    db.add(row)
    if commit:
        db.commit()


def build_progress_dto(row: AnalysisRequest) -> AnalyzeProgressDTO | None:
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
