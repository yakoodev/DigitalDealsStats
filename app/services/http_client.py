from __future__ import annotations

import random
import time
from collections.abc import Mapping
from typing import Any

import httpx

from app.core.config import Settings
from app.services.proxy import ProxyPool


class RetryHttpClient:
    def __init__(
        self,
        settings: Settings,
        *,
        proxy_pool: ProxyPool,
        timeout: float | None = None,
        max_retries: int | None = None,
        base_backoff_seconds: float | None = None,
        min_delay_seconds: float | None = None,
        jitter_min: float | None = None,
        jitter_max: float | None = None,
        default_headers: Mapping[str, str] | None = None,
    ) -> None:
        self.settings = settings
        self.proxy_pool = proxy_pool
        self.timeout = settings.funpay_request_timeout if timeout is None else timeout
        self.max_retries = settings.funpay_max_retries if max_retries is None else max_retries
        self.base_backoff_seconds = (
            settings.funpay_base_backoff_seconds
            if base_backoff_seconds is None
            else base_backoff_seconds
        )
        self.min_delay_seconds = (
            settings.funpay_min_delay_seconds if min_delay_seconds is None else min_delay_seconds
        )
        self.jitter_min = settings.funpay_jitter_min if jitter_min is None else jitter_min
        self.jitter_max = settings.funpay_jitter_max if jitter_max is None else jitter_max
        self.default_headers = dict(default_headers or {})
        self._last_request_ts: float = 0.0

    def _sleep_rate_limit(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_request_ts
        if elapsed < self.min_delay_seconds:
            time.sleep(self.min_delay_seconds - elapsed)
        jitter = random.uniform(self.jitter_min, self.jitter_max)
        time.sleep(jitter)
        self._last_request_ts = time.monotonic()

    def request(
        self,
        method: str,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        data: Mapping[str, Any] | None = None,
        json: Any = None,
        cookies: httpx.Cookies | None = None,
        headers: Mapping[str, str] | None = None,
        follow_redirects: bool = True,
    ) -> httpx.Response:
        last_status: int | None = None
        last_error: Exception | None = None
        merged_headers: dict[str, str] = dict(self.default_headers)
        if headers:
            merged_headers.update(headers)

        for attempt in range(self.max_retries):
            selection = self.proxy_pool.choose(attempt=attempt, last_status=last_status)
            self._sleep_rate_limit()

            client_kwargs: dict[str, Any] = {
                "timeout": self.timeout,
                "follow_redirects": follow_redirects,
            }
            if merged_headers:
                client_kwargs["headers"] = merged_headers
            if selection.proxy_url:
                client_kwargs["proxy"] = selection.proxy_url

            try:
                with httpx.Client(**client_kwargs) as client:
                    response = client.request(
                        method=method,
                        url=url,
                        params=params,
                        data=data,
                        json=json,
                        cookies=cookies,
                    )
                if response.status_code == 429:
                    last_status = 429
                    backoff = self.base_backoff_seconds * (2**attempt)
                    time.sleep(backoff)
                    continue
                response.raise_for_status()
                return response
            except httpx.HTTPStatusError as exc:
                last_error = exc
                last_status = exc.response.status_code
                if last_status in {429, 500, 502, 503, 504}:
                    backoff = self.base_backoff_seconds * (2**attempt)
                    time.sleep(backoff)
                    continue
                raise
            except httpx.RequestError as exc:
                last_error = exc
                backoff = self.base_backoff_seconds * (2**attempt)
                time.sleep(backoff)

        if last_error:
            raise last_error
        if last_status is not None:
            raise RuntimeError(
                f"Не удалось выполнить HTTP-запрос: повторяющийся статус {last_status} "
                f"после {self.max_retries} попыток"
            )
        raise RuntimeError(
            f"Не удалось выполнить HTTP-запрос: сетевой сбой после {self.max_retries} попыток"
        )
