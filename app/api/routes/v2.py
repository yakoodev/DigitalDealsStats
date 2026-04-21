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
    NetworkSettingsDTO,
    OverviewV2DTO,
    PlatiCatalogTreeResponseDTO,
    PlatiCategoriesResponseDTO,
    PlatiGameCategoriesResponseDTO,
    PlatiGamesResponseDTO,
    PlayerOkCategoriesResponseDTO,
    V2ExecutionMode,
)
from app.services.global_analyzer import GlobalAnalyzerService
from app.services.i18n import tr
from app.services.proxy import ProxyRequiredError
from app.services.text_utils import repair_mojibake_cyrillic

router = APIRouter(prefix="/v2", tags=["v2"])


def _make_service(db: Session) -> GlobalAnalyzerService:
    settings = get_settings()
    return GlobalAnalyzerService(db=db, settings=settings)


def _normalize_marketplace_error(exc: Exception) -> HTTPException:
    message = str(exc)
    if isinstance(exc, ProxyRequiredError) or message.startswith("proxy_required"):
        return HTTPException(
            status_code=400,
            detail={
                "code": "proxy_required",
                "message": "Прокси не настроены. Добавьте прокси в настройках сети или подтвердите запуск без прокси.",
            },
        )
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
    payload.common_filters.query = repair_mojibake_cyrillic(payload.common_filters.query).strip()
    ui_locale = payload.common_filters.ui_locale.value
    if payload.common_filters.query == "":
        needs_funpay_scope = any(item.value == "funpay" for item in payload.marketplaces)
        needs_playerok_scope = any(item.value == "playerok" for item in payload.marketplaces)
        needs_plati_scope = any(item.value == "platimarket" for item in payload.marketplaces)
        funpay_filters = payload.marketplace_filters.funpay
        playerok_filters = payload.marketplace_filters.playerok
        plati_filters = payload.marketplace_filters.platimarket
        if needs_funpay_scope and (
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
        if needs_plati_scope and (
            plati_filters is None
            or (
                plati_filters.category_game_id is None
                and not (plati_filters.category_game_slug or "").strip()
                and
                plati_filters.category_group_id is None
                and len(plati_filters.category_ids) == 0
            )
        ):
            raise HTTPException(
                status_code=400,
                detail=tr(ui_locale, "validation.empty_query_requires_scope"),
            )
        if needs_playerok_scope and (
            playerok_filters is None
            or (
                not playerok_filters.category_game_slug
                and len(playerok_filters.category_slugs) == 0
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
def funpay_categories_v2(
    allow_direct_fallback: bool = Query(default=False),
    db: Session = Depends(get_db),
) -> CategoriesResponseDTO:
    service = _make_service(db)
    try:
        games = service.funpay_categories(allow_direct_fallback=allow_direct_fallback)
        return CategoriesResponseDTO(
            generated_at=datetime.now(UTC),
            games=games,
        )
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        normalized = _normalize_marketplace_error(exc)
        if normalized.status_code != 500:
            raise normalized from exc
        raise HTTPException(status_code=500, detail=f"Ошибка загрузки категорий FunPay: {exc}") from exc


@router.get("/marketplaces/playerok/categories", response_model=PlayerOkCategoriesResponseDTO)
def playerok_categories_v2(
    game_slug: str | None = Query(default=None, min_length=1),
    allow_direct_fallback: bool = Query(default=False),
    force_refresh: bool = Query(default=False),
    db: Session = Depends(get_db),
) -> PlayerOkCategoriesResponseDTO:
    service = _make_service(db)
    try:
        games_payload = service.playerok_categories(
            game_slug=game_slug,
            allow_direct_fallback=allow_direct_fallback,
            force_refresh=force_refresh,
        )
        games, source = games_payload if isinstance(games_payload, tuple) else (games_payload, "network")
        return PlayerOkCategoriesResponseDTO(
            generated_at=datetime.now(UTC),
            source=source,
            games=games,
        )
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        normalized = _normalize_marketplace_error(exc)
        if normalized.status_code != 500:
            raise normalized from exc
        raise HTTPException(status_code=500, detail=f"Ошибка загрузки категорий PlayerOK: {exc}") from exc


@router.get("/marketplaces/platimarket/categories", response_model=PlatiCategoriesResponseDTO)
def platimarket_categories_v2(
    allow_direct_fallback: bool = Query(default=False),
    force_refresh: bool = Query(default=False),
    db: Session = Depends(get_db),
) -> PlatiCategoriesResponseDTO:
    service = _make_service(db)
    try:
        groups_payload = service.platimarket_categories(
            allow_direct_fallback=allow_direct_fallback,
            force_refresh=force_refresh,
        )
        groups, source = groups_payload if isinstance(groups_payload, tuple) else (groups_payload, "network")
        return PlatiCategoriesResponseDTO(
            generated_at=datetime.now(UTC),
            source=source,
            groups=groups,
        )
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        normalized = _normalize_marketplace_error(exc)
        if normalized.status_code != 500:
            raise normalized from exc
        raise HTTPException(status_code=500, detail=f"Ошибка загрузки категорий Plati.Market: {exc}") from exc


@router.get("/marketplaces/platimarket/catalog-tree", response_model=PlatiCatalogTreeResponseDTO)
def platimarket_catalog_tree_v2(
    allow_direct_fallback: bool = Query(default=False),
    force_refresh: bool = Query(default=False),
    db: Session = Depends(get_db),
) -> PlatiCatalogTreeResponseDTO:
    service = _make_service(db)
    try:
        nodes_payload = service.platimarket_catalog_tree(
            allow_direct_fallback=allow_direct_fallback,
            force_refresh=force_refresh,
        )
        nodes, source = nodes_payload if isinstance(nodes_payload, tuple) else (nodes_payload, "network")
        return PlatiCatalogTreeResponseDTO(
            generated_at=datetime.now(UTC),
            source=source,
            nodes=nodes,
        )
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        normalized = _normalize_marketplace_error(exc)
        if normalized.status_code != 500:
            raise normalized from exc
        raise HTTPException(status_code=500, detail=f"Ошибка загрузки дерева каталога Plati.Market: {exc}") from exc


@router.get("/marketplaces/platimarket/games", response_model=PlatiGamesResponseDTO)
def platimarket_games_v2(
    allow_direct_fallback: bool = Query(default=False),
    force_refresh: bool = Query(default=False),
    db: Session = Depends(get_db),
) -> PlatiGamesResponseDTO:
    service = _make_service(db)
    try:
        games_payload = service.platimarket_games(
            allow_direct_fallback=allow_direct_fallback,
            force_refresh=force_refresh,
        )
        games, source = games_payload if isinstance(games_payload, tuple) else (games_payload, "network")
        return PlatiGamesResponseDTO(
            generated_at=datetime.now(UTC),
            source=source,
            games=games,
        )
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        normalized = _normalize_marketplace_error(exc)
        if normalized.status_code != 500:
            raise normalized from exc
        raise HTTPException(status_code=500, detail=f"Ошибка загрузки игр Plati.Market: {exc}") from exc


@router.get("/marketplaces/platimarket/game-categories", response_model=PlatiGameCategoriesResponseDTO)
def platimarket_game_categories_v2(
    game_id: int | None = Query(default=None, ge=1),
    game_slug: str | None = Query(default=None, min_length=1),
    ui_locale: str = Query(default="ru"),
    allow_direct_fallback: bool = Query(default=False),
    force_refresh: bool = Query(default=False),
    db: Session = Depends(get_db),
) -> PlatiGameCategoriesResponseDTO:
    service = _make_service(db)
    try:
        resolved_game_id, resolved_game_slug, categories = service.platimarket_game_categories(
            game_id=game_id,
            game_slug=game_slug,
            ui_locale=ui_locale,
            allow_direct_fallback=allow_direct_fallback,
            force_refresh=force_refresh,
        )
        return PlatiGameCategoriesResponseDTO(
            generated_at=datetime.now(UTC),
            source="network",
            game_id=resolved_game_id,
            game_slug=resolved_game_slug,
            categories=categories,
        )
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        normalized = _normalize_marketplace_error(exc)
        if normalized.status_code != 500:
            raise normalized from exc
        raise HTTPException(
            status_code=500,
            detail=f"Ошибка загрузки категорий игры Plati.Market: {exc}",
        ) from exc


@router.get("/settings/network", response_model=NetworkSettingsDTO)
def get_network_settings_v2(db: Session = Depends(get_db)) -> NetworkSettingsDTO:
    service = _make_service(db)
    try:
        return service.get_network_settings()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Ошибка чтения сетевых настроек: {exc}") from exc


@router.put("/settings/network", response_model=NetworkSettingsDTO)
def put_network_settings_v2(payload: NetworkSettingsDTO, db: Session = Depends(get_db)) -> NetworkSettingsDTO:
    service = _make_service(db)
    try:
        return service.update_network_settings(payload)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Ошибка сохранения сетевых настроек: {exc}") from exc
