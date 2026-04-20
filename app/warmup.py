import os

from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.db.session import SessionLocal
from app.schemas.analyze import AnalyzeRequestDTO, Currency
from app.services.analyzer import AnalyzerService
from app.services.funpay_client import FunPayClient


def _queries_from_env() -> list[str]:
    raw = os.getenv("WARMUP_QUERIES", "")
    queries = [item.strip() for item in raw.split(",") if item.strip()]
    return list(dict.fromkeys(queries))


def run_warmup() -> None:
    settings = get_settings()
    queries = _queries_from_env()
    if not queries:
        return

    db: Session = SessionLocal()
    try:
        service = AnalyzerService(db=db, client=FunPayClient(settings), settings=settings)
        for query in queries:
            payload = AnalyzeRequestDTO(
                query=query,
                force_refresh=False,
                currency=Currency.rub,
            )
            service.analyze(payload)
    finally:
        db.close()


if __name__ == "__main__":
    run_warmup()
