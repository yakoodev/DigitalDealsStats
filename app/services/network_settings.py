from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.models import RuntimeNetworkSettings
from app.schemas.v2 import CommonFiltersDTO, NetworkSettingsDTO
from app.services.proxy import ProxyRequiredError
from app.services.proxy_utils import normalize_single_proxy


def _parse_csv_list(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [item.strip() for item in str(raw).split(",") if str(item).strip()]


def _normalize_list(values: list[str] | None) -> list[str]:
    if not values:
        return []
    result: list[str] = []
    seen: set[str] = set()
    for raw in values:
        value = normalize_single_proxy(str(raw or "").strip())
        if not value:
            continue
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(value)
    return result


@dataclass
class ResolvedProxyPool:
    datacenter: list[str]
    residential: list[str]
    mobile: list[str]
    source: dict[str, str]

    @property
    def has_any(self) -> bool:
        return bool(self.datacenter or self.residential or self.mobile)


class NetworkSettingsService:
    SINGLETON_ID = 1

    def __init__(self, db: Session | None, settings: Settings) -> None:
        self.db = db
        self.settings = settings

    def _ensure_table(self) -> None:
        if self.db is None:
            return
        bind = self.db.get_bind()
        RuntimeNetworkSettings.__table__.create(bind=bind, checkfirst=True)

    def _env_settings(self) -> NetworkSettingsDTO:
        return NetworkSettingsDTO(
            datacenter_proxies=_normalize_list(_parse_csv_list(self.settings.datacenter_proxies)),
            residential_proxies=_normalize_list(_parse_csv_list(self.settings.residential_proxies)),
            mobile_proxies=_normalize_list(_parse_csv_list(self.settings.mobile_proxies)),
            updated_at=None,
        )

    def get(self) -> NetworkSettingsDTO:
        if self.db is None:
            return self._env_settings()
        self._ensure_table()
        row = self.db.scalar(
            select(RuntimeNetworkSettings).where(RuntimeNetworkSettings.id == self.SINGLETON_ID)
        )
        if row is None:
            env = self._env_settings()
            row = RuntimeNetworkSettings(
                id=self.SINGLETON_ID,
                datacenter_proxies=env.datacenter_proxies,
                residential_proxies=env.residential_proxies,
                mobile_proxies=env.mobile_proxies,
            )
            self.db.add(row)
            self.db.commit()
            self.db.refresh(row)
        return NetworkSettingsDTO(
            datacenter_proxies=_normalize_list(list(row.datacenter_proxies or [])),
            residential_proxies=_normalize_list(list(row.residential_proxies or [])),
            mobile_proxies=_normalize_list(list(row.mobile_proxies or [])),
            updated_at=row.updated_at,
        )

    def update(self, payload: NetworkSettingsDTO) -> NetworkSettingsDTO:
        if self.db is None:
            now = datetime.now(UTC)
            return NetworkSettingsDTO(
                datacenter_proxies=_normalize_list(payload.datacenter_proxies),
                residential_proxies=_normalize_list(payload.residential_proxies),
                mobile_proxies=_normalize_list(payload.mobile_proxies),
                updated_at=now,
            )

        self._ensure_table()
        row = self.db.scalar(
            select(RuntimeNetworkSettings).where(RuntimeNetworkSettings.id == self.SINGLETON_ID)
        )
        if row is None:
            row = RuntimeNetworkSettings(id=self.SINGLETON_ID)

        row.datacenter_proxies = _normalize_list(payload.datacenter_proxies)
        row.residential_proxies = _normalize_list(payload.residential_proxies)
        row.mobile_proxies = _normalize_list(payload.mobile_proxies)
        self.db.add(row)
        self.db.commit()
        self.db.refresh(row)
        return NetworkSettingsDTO(
            datacenter_proxies=list(row.datacenter_proxies or []),
            residential_proxies=list(row.residential_proxies or []),
            mobile_proxies=list(row.mobile_proxies or []),
            updated_at=row.updated_at,
        )

    def resolve(
        self,
        *,
        common_filters: CommonFiltersDTO | None,
        marketplace_filters: dict | None,
    ) -> ResolvedProxyPool:
        runtime_settings = self.get()
        env_settings = self._env_settings()
        mkt = marketplace_filters if isinstance(marketplace_filters, dict) else {}
        common = common_filters or CommonFiltersDTO()

        def _pick(key: str) -> tuple[list[str], str]:
            runtime_values = _normalize_list(getattr(runtime_settings, key, []) or [])
            if runtime_values:
                return runtime_values, "runtime"

            market_values = _normalize_list(mkt.get(key) if isinstance(mkt.get(key), list) else [])
            if market_values:
                return market_values, "marketplace_override"

            common_values = _normalize_list(getattr(common, key, None) if isinstance(getattr(common, key, None), list) else [])
            if common_values:
                return common_values, "common_override"

            env_values = _normalize_list(getattr(env_settings, key, []) or [])
            if env_values:
                return env_values, "env"
            return [], "none"

        dc, dc_source = _pick("datacenter_proxies")
        res, res_source = _pick("residential_proxies")
        mob, mob_source = _pick("mobile_proxies")
        return ResolvedProxyPool(
            datacenter=dc,
            residential=res,
            mobile=mob,
            source={
                "datacenter": dc_source,
                "residential": res_source,
                "mobile": mob_source,
            },
        )

    @staticmethod
    def ensure_proxy_policy(resolved: ResolvedProxyPool, *, allow_direct_fallback: bool) -> None:
        if resolved.has_any or allow_direct_fallback:
            return
        raise ProxyRequiredError("proxy_required")


def mask_proxy_values(values: list[str] | None) -> list[str]:
    if not values:
        return []
    return ["***" for _ in values]


def mask_proxy_payload(payload: dict) -> None:
    if not isinstance(payload, dict):
        return
    for key in ("datacenter_proxies", "residential_proxies", "mobile_proxies"):
        value = payload.get(key)
        if isinstance(value, list):
            payload[key] = mask_proxy_values([str(item) for item in value if str(item).strip()])
