from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from redis import Redis
from rq import Queue
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.db.session import get_db
from app.jobs import run_global_analysis_job
from app.schemas.analyze import CategoriesResponseDTO
from app.schemas.v2 import (
    AnalyzeV2EnvelopeDTO,
    AnalyzeV2RequestDTO,
    HistoryV2ResponseDTO,
    MarketplaceOffersResponseDTO,
    MarketplaceRunResultDTO,
    MarketplacesCatalogResponseDTO,
    OverviewV2DTO,
    V2ExecutionMode,
)
from app.services.global_analyzer import GlobalAnalyzerService
from app.services.i18n import tr

router = APIRouter(prefix="/v2", tags=["v2"])


def _make_service(db: Session) -> GlobalAnalyzerService:
    settings = get_settings()
    return GlobalAnalyzerService(db=db, settings=settings)


def _normalize_marketplace_error(exc: Exception) -> HTTPException:
    message = str(exc)
    if message.startswith("marketplace_not_available:"):
        parts = message.split(":", 2)
        slug = parts[1] if len(parts) > 1 else "unknown"
        reason = parts[2] if len(parts) > 2 else "Площадка недоступна"
        return HTTPException(
            status_code=400,
            detail={
                "code": "marketplace_not_available",
                "marketplace": slug,
                "message": reason,
            },
        )
    return HTTPException(status_code=500, detail=f"Ошибка анализа: {exc}")


@router.post("/analyze", response_model=AnalyzeV2EnvelopeDTO)
def analyze_v2(
    payload: AnalyzeV2RequestDTO,
    db: Session = Depends(get_db),
) -> AnalyzeV2EnvelopeDTO:
    settings = get_settings()
    payload.common_filters.query = payload.common_filters.query.strip()
    ui_locale = payload.common_filters.ui_locale.value
    if payload.common_filters.query == "":
        needs_category = any(item.value == "funpay" for item in payload.marketplaces)
        funpay_filters = payload.marketplace_filters.funpay
        if needs_category and (
            funpay_filters is None
            or (
                funpay_filters.category_game_id is None
                and funpay_filters.category_id is None
                and len(funpay_filters.category_ids) == 0
            )
        ):
            raise HTTPException(
                status_code=400,
                detail=tr(ui_locale, "validation.empty_query_requires_scope"),
            )

    service = _make_service(db)
    try:
        # Валидацию площадок делаем до любых DB-операций, чтобы возвращать
        # понятную 400-ошибку даже в облегчённых test-окружениях.
        service.validate_marketplaces(payload.marketplaces)
        should_enqueue = False
        if payload.common_filters.execution == V2ExecutionMode.async_mode:
            should_enqueue = True
        elif payload.common_filters.execution == V2ExecutionMode.sync:
            should_enqueue = False
        else:
            should_enqueue = service.is_heavy_request(payload)

        if should_enqueue:
            run_id = service.create_queued_run(payload)
            try:
                redis_conn = Redis.from_url(settings.redis_url)
                queue = Queue(settings.rq_queue_name, connection=redis_conn)
                queue.enqueue(
                    run_global_analysis_job,
                    payload.model_dump(mode="json"),
                    run_id,
                    job_timeout=settings.rq_job_timeout_seconds,
                )
                return service.get_status(run_id)
            except Exception:
                return service.analyze(payload, run_id=run_id)

        return service.analyze(payload)
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise _normalize_marketplace_error(exc) from exc


@router.get("/analyze/{run_id}", response_model=AnalyzeV2EnvelopeDTO)
def analyze_v2_status(run_id: str, db: Session = Depends(get_db)) -> AnalyzeV2EnvelopeDTO:
    service = _make_service(db)
    try:
        return service.get_status(run_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Ошибка получения статуса: {exc}") from exc


@router.get("/analyze/{run_id}/overview", response_model=OverviewV2DTO)
def analyze_v2_overview(run_id: str, db: Session = Depends(get_db)) -> OverviewV2DTO:
    service = _make_service(db)
    try:
        return service.get_overview(run_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Ошибка получения overview: {exc}") from exc


@router.get("/analyze/{run_id}/marketplaces/{marketplace}", response_model=MarketplaceRunResultDTO)
def analyze_v2_marketplace(
    run_id: str,
    marketplace: str,
    db: Session = Depends(get_db),
) -> MarketplaceRunResultDTO:
    service = _make_service(db)
    try:
        return service.get_marketplace_result(run_id, marketplace)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Ошибка получения данных площадки: {exc}") from exc


@router.get(
    "/analyze/{run_id}/marketplaces/{marketplace}/offers",
    response_model=MarketplaceOffersResponseDTO,
)
def analyze_v2_marketplace_offers(
    run_id: str,
    marketplace: str,
    limit: int = Query(default=500, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
    price_min: float | None = Query(default=None, ge=0),
    price_max: float | None = Query(default=None, ge=0),
    min_reviews: int | None = Query(default=None, ge=0),
    online_only: bool = False,
    auto_delivery_only: bool = False,
    seller_query: str | None = Query(default=None, min_length=1, max_length=256),
    db: Session = Depends(get_db),
) -> MarketplaceOffersResponseDTO:
    service = _make_service(db)
    try:
        return service.get_marketplace_offers(
            run_id=run_id,
            marketplace=marketplace,
            limit=limit,
            offset=offset,
            price_min=price_min,
            price_max=price_max,
            min_reviews=min_reviews,
            online_only=online_only,
            auto_delivery_only=auto_delivery_only,
            seller_query=seller_query,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Ошибка получения офферов площадки: {exc}") from exc


@router.get("/history", response_model=HistoryV2ResponseDTO)
def history_v2(limit: int = 100, db: Session = Depends(get_db)) -> HistoryV2ResponseDTO:
    service = _make_service(db)
    try:
        return service.list_history(limit=limit)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Ошибка загрузки истории: {exc}") from exc


@router.get("/marketplaces", response_model=MarketplacesCatalogResponseDTO)
def marketplaces_catalog(db: Session = Depends(get_db)) -> MarketplacesCatalogResponseDTO:
    service = _make_service(db)
    try:
        return service.list_marketplaces()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Ошибка загрузки площадок: {exc}") from exc


@router.get("/marketplaces/funpay/categories", response_model=CategoriesResponseDTO)
def funpay_categories_v2(db: Session = Depends(get_db)) -> CategoriesResponseDTO:
    service = _make_service(db)
    try:
        games = service.funpay_categories()
        return CategoriesResponseDTO(
            generated_at=datetime.now(UTC),
            games=games,
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Ошибка загрузки категорий FunPay: {exc}") from exc
