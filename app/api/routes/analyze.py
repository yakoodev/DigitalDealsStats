from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from redis import Redis
from rq import Queue
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.db.session import get_db
from app.jobs import run_analysis_job
from app.schemas.analyze import (
    AnalyzeEnvelopeDTO,
    AnalyzeRequestDTO,
    CategoriesResponseDTO,
    CategoryGameDTO,
    CategorySectionDTO,
    ExecutionMode,
    HistoryResponseDTO,
    OffersSliceResponseDTO,
)
from app.services.analyzer import AnalyzerService
from app.services.funpay_client import FunPayClient, GameCategory

router = APIRouter(prefix="/v1", tags=["analyze"])


def _normalize_single_proxy(value: str) -> str:
    raw = value.strip()
    if not raw:
        return raw

    # Поддерживаем как корректный URL, так и сокращенные варианты.
    # 1) user:pass@host:port -> добавляем http://
    # 2) host:port@user:pass -> переставляем местами + http://
    # 3) host:port -> добавляем http://
    if raw.startswith(("http://", "https://", "socks5://", "socks5h://")):
        return raw

    if "@" in raw:
        left, right = raw.split("@", 1)
        left = left.strip()
        right = right.strip()

        # host:port@user:pass -> user:pass@host:port
        left_parts = left.rsplit(":", 1)
        right_parts = right.split(":", 1)
        if len(left_parts) == 2 and left_parts[1].isdigit() and len(right_parts) == 2:
            user, password = right_parts
            host, port = left_parts
            if user and password and host and port:
                return f"http://{user}:{password}@{host}:{port}"

        # user:pass@host:port -> просто добавляем схему
        return f"http://{left}@{right}"

    # host:port
    if ":" in raw:
        return f"http://{raw}"

    return raw


def _normalize_proxy_list(raw_values: list[str] | None) -> str | None:
    if raw_values is None:
        return None
    values = [_normalize_single_proxy(item) for item in raw_values if item and item.strip()]
    return ",".join(values)


def _build_funpay_client(payload: AnalyzeRequestDTO) -> FunPayClient:
    settings = get_settings()
    return FunPayClient(
        settings=settings,
        datacenter_proxies=_normalize_proxy_list(payload.datacenter_proxies),
        residential_proxies=_normalize_proxy_list(payload.residential_proxies),
        mobile_proxies=_normalize_proxy_list(payload.mobile_proxies),
    )


def _proxy_overrides(payload: AnalyzeRequestDTO) -> tuple[str | None, str | None, str | None]:
    return (
        _normalize_proxy_list(payload.datacenter_proxies),
        _normalize_proxy_list(payload.residential_proxies),
        _normalize_proxy_list(payload.mobile_proxies),
    )


def _game_to_dto(game: GameCategory) -> CategoryGameDTO:
    return CategoryGameDTO(
        game_section_id=game.game_section_id,
        game_url=game.game_url,
        game_name=game.game_name,
        sections=[
            CategorySectionDTO(
                section_id=section.section_id,
                section_url=section.section_url,
                section_name=section.section_name,
                full_name=section.full_name,
            )
            for section in game.sections
        ],
    )


@router.post("/analyze", response_model=AnalyzeEnvelopeDTO)
def analyze(
    payload: AnalyzeRequestDTO,
    db: Session = Depends(get_db),
) -> AnalyzeEnvelopeDTO:
    settings = get_settings()
    payload.query = payload.query.strip()
    if not payload.query and payload.category_id is None and payload.category_game_id is None:
        raise HTTPException(
            status_code=400,
            detail="Пустой запрос разрешен только при выбранной игре или разделе категории.",
        )
    options_probe_client = _build_funpay_client(payload)
    service = AnalyzerService(db=db, client=options_probe_client, settings=settings)

    try:
        options = service.resolve_options(
            payload.options,
            category_game_id=payload.category_game_id,
            category_id=payload.category_id,
        )
        has_valid_cache = (not payload.force_refresh) and service.has_valid_cache(payload, options)

        should_enqueue = False
        if payload.execution == ExecutionMode.async_mode:
            should_enqueue = True
        elif payload.execution == ExecutionMode.sync:
            should_enqueue = False
        else:
            should_enqueue = (not has_valid_cache) and service.is_heavy_request(options)

        if should_enqueue:
            request_id, _ = service.create_queued_request(payload)
            dc, res, mob = _proxy_overrides(payload)
            try:
                redis_conn = Redis.from_url(settings.redis_url)
                queue = Queue(settings.rq_queue_name, connection=redis_conn)
                queue.enqueue(
                    run_analysis_job,
                    payload.model_dump(mode="json"),
                    request_id,
                    dc,
                    res,
                    mob,
                    job_timeout=settings.rq_job_timeout_seconds,
                )
                return service.get_request_status(request_id)
            except Exception:
                return service.analyze(payload, request_id=request_id)

        return service.analyze(payload)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Ошибка анализа: {exc}") from exc


@router.get("/categories", response_model=CategoriesResponseDTO)
def categories() -> CategoriesResponseDTO:
    settings = get_settings()
    client = FunPayClient(settings=settings)
    try:
        games = client.get_categories_catalog()
        return CategoriesResponseDTO(
            generated_at=datetime.now(UTC),
            games=[_game_to_dto(game) for game in games],
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Ошибка загрузки категорий: {exc}") from exc


@router.get("/history", response_model=HistoryResponseDTO)
def history(
    limit: int = 100,
    db: Session = Depends(get_db),
) -> HistoryResponseDTO:
    settings = get_settings()
    client = FunPayClient(settings=settings)
    service = AnalyzerService(db=db, client=client, settings=settings)
    try:
        return service.list_completed_history(limit=limit)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Ошибка загрузки истории: {exc}") from exc


@router.get("/analyze/{request_id}", response_model=AnalyzeEnvelopeDTO)
def analyze_status(
    request_id: str,
    db: Session = Depends(get_db),
) -> AnalyzeEnvelopeDTO:
    settings = get_settings()
    client = FunPayClient(settings=settings)
    service = AnalyzerService(db=db, client=client, settings=settings)
    try:
        return service.get_request_status(request_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Ошибка получения статуса: {exc}") from exc


@router.get("/analyze/{request_id}/offers", response_model=OffersSliceResponseDTO)
def analyze_offers(
    request_id: str,
    limit: int = Query(default=500, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
    price_min: float | None = Query(default=None, ge=0),
    price_max: float | None = Query(default=None, ge=0),
    min_reviews: int | None = Query(default=None, ge=0),
    online_only: bool = False,
    auto_delivery_only: bool = False,
    seller_query: str | None = Query(default=None, min_length=1, max_length=256),
    db: Session = Depends(get_db),
) -> OffersSliceResponseDTO:
    settings = get_settings()
    client = FunPayClient(settings=settings)
    service = AnalyzerService(db=db, client=client, settings=settings)
    try:
        return service.list_request_offers(
            request_id=request_id,
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
        raise HTTPException(status_code=500, detail=f"Ошибка загрузки среза офферов: {exc}") from exc
