from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.models import AnalysisCache


class CatalogCacheService:
    MODE = "catalog_v2"
    CURRENCY = "SYS"

    def __init__(self, db: Session, settings: Settings) -> None:
        self.db = db
        self.settings = settings

    @staticmethod
    def _utc_now() -> datetime:
        return datetime.now(UTC)

    @classmethod
    def _key_hash(cls, key: str) -> str:
        payload = f"catalog:{key}".encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def get(self, key: str, *, force_refresh: bool = False) -> dict | list | None:
        if force_refresh:
            return None
        now = self._utc_now()
        cache_hash = self._key_hash(key)
        cached = self.db.scalar(
            select(AnalysisCache).where(
                AnalysisCache.cache_key_hash == cache_hash,
                AnalysisCache.mode == self.MODE,
                AnalysisCache.valid_until > now,
            )
        )
        if not cached:
            return None
        payload = cached.response_json
        if isinstance(payload, (dict, list)):
            return json.loads(json.dumps(payload))
        return None

    def set(
        self,
        key: str,
        payload: dict | list,
        *,
        ttl_hours: int | None = None,
    ) -> None:
        ttl = ttl_hours if ttl_hours is not None else self.settings.cache_ttl_hours
        generated_at = self._utc_now()
        valid_until = generated_at + timedelta(hours=max(1, int(ttl)))
        cache_hash = self._key_hash(key)
        query = f"catalog:{key}"[:512]

        existing = self.db.scalar(
            select(AnalysisCache).where(
                AnalysisCache.cache_key_hash == cache_hash,
                AnalysisCache.mode == self.MODE,
            )
        )
        serialized = json.loads(json.dumps(payload))
        if existing is not None:
            existing.query = query
            existing.currency = self.CURRENCY
            existing.generated_at = generated_at
            existing.valid_until = valid_until
            existing.response_json = serialized
            self.db.add(existing)
            self.db.commit()
            return

        self.db.add(
            AnalysisCache(
                cache_key_hash=cache_hash,
                query=query,
                mode=self.MODE,
                currency=self.CURRENCY,
                generated_at=generated_at,
                valid_until=valid_until,
                response_json=serialized,
            )
        )
        self.db.commit()
