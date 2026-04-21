from dataclasses import dataclass


def _parse_proxy_list(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


@dataclass
class ProxySelection:
    tier: str
    proxy_url: str | None


class ProxyRequiredError(RuntimeError):
    pass


class ProxyPool:
    def __init__(
        self,
        datacenter_proxies: str = "",
        residential_proxies: str = "",
        mobile_proxies: str = "",
        *,
        allow_direct_fallback: bool = True,
    ) -> None:
        self.datacenter = _parse_proxy_list(datacenter_proxies)
        self.residential = _parse_proxy_list(residential_proxies)
        self.mobile = _parse_proxy_list(mobile_proxies)
        self.allow_direct_fallback = allow_direct_fallback

        self._idx_dc = 0
        self._idx_res = 0
        self._idx_mob = 0

    def has_any_proxy(self) -> bool:
        return bool(self.datacenter or self.residential or self.mobile)

    def _pick_round_robin(self, items: list[str], idx_attr: str) -> str | None:
        if not items:
            return None
        idx = getattr(self, idx_attr)
        value = items[idx % len(items)]
        setattr(self, idx_attr, idx + 1)
        return value

    def choose(self, attempt: int, last_status: int | None) -> ProxySelection:
        # Базовая политика:
        # 1) первые 2 попытки — DC;
        # 2) при 429 / поздних попытках — residential;
        # 3) при финальных попытках — mobile.
        if attempt >= 4:
            proxy = self._pick_round_robin(self.mobile, "_idx_mob")
            if proxy:
                return ProxySelection(tier="mobile", proxy_url=proxy)

        if last_status == 429 or attempt >= 2:
            proxy = self._pick_round_robin(self.residential, "_idx_res")
            if proxy:
                return ProxySelection(tier="residential", proxy_url=proxy)

        proxy = self._pick_round_robin(self.datacenter, "_idx_dc")
        if proxy:
            return ProxySelection(tier="datacenter", proxy_url=proxy)

        # fallback без прокси, если пул пуст.
        if not self.allow_direct_fallback:
            raise ProxyRequiredError("proxy_required")
        return ProxySelection(tier="none", proxy_url=None)
