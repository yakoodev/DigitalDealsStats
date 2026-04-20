from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.db.session import SessionLocal
from app.schemas.analyze import AnalyzeRequestDTO
from app.schemas.v2 import AnalyzeV2RequestDTO
from app.services.analyzer import AnalyzerService
from app.services.funpay_client import FunPayClient
from app.services.global_analyzer import GlobalAnalyzerService


def run_analysis_job(
    payload: dict,
    request_id: str,
    datacenter_proxies: str | None = None,
    residential_proxies: str | None = None,
    mobile_proxies: str | None = None,
) -> dict:
    settings = get_settings()
    db: Session = SessionLocal()
    try:
        dto = AnalyzeRequestDTO.model_validate(payload)
        analyzer = AnalyzerService(
            db=db,
            client=FunPayClient(
                settings,
                datacenter_proxies=datacenter_proxies,
                residential_proxies=residential_proxies,
                mobile_proxies=mobile_proxies,
            ),
            settings=settings,
        )
        envelope = analyzer.analyze(dto, request_id=request_id)
        return envelope.model_dump(mode="json")
    finally:
        db.close()


def run_global_analysis_job(payload: dict, run_id: str) -> dict:
    settings = get_settings()
    db: Session = SessionLocal()
    try:
        dto = AnalyzeV2RequestDTO.model_validate(payload)
        service = GlobalAnalyzerService(db=db, settings=settings)
        envelope = service.analyze(dto, run_id=run_id)
        return envelope.model_dump(mode="json")
    finally:
        db.close()
