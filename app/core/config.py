from functools import lru_cache
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "MarketStat"
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    log_level: str = "INFO"

    database_url: str = "sqlite:///./funpaystat.db"
    redis_url: str = "redis://localhost:6379/0"
    rq_queue_name: str = "funpaystat"
    rq_job_timeout_seconds: int = 600

    cache_ttl_hours: int = 24
    funpay_locale: Literal["en", "ru"] = "en"
    funpay_request_timeout: float = 20.0
    funpay_max_retries: int = 5
    funpay_base_backoff_seconds: float = 0.8
    funpay_min_delay_seconds: float = 0.25
    funpay_jitter_min: float = 0.3
    funpay_jitter_max: float = 0.9

    low_coverage_min_matched_offers: int = 25
    quick_sections_limit: int = 80
    fallback_sections_limit: int = 120

    demand_max_sellers: int = 40
    review_max_pages_per_seller: int = 4

    datacenter_proxies: str = ""
    residential_proxies: str = ""
    mobile_proxies: str = ""


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
