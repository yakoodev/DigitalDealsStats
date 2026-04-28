from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from app.api.routes import v2_router, web_router
from app.core.config import get_settings
from app.db.base import Base
from app.db.session import SessionLocal, engine
from app.models import entities as _entities  # noqa: F401
from app.services.analyzer import AnalyzerService
from app.services.funpay_client import FunPayClient


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    Base.metadata.create_all(bind=engine)

    db = SessionLocal()
    try:
        service = AnalyzerService(
            db=db,
            client=FunPayClient(settings=settings),
            settings=settings,
        )
        service.ensure_pg_trgm()
    finally:
        db.close()

    yield


settings = get_settings()
OPENAPI_TAGS = [
    {
        "name": "Анализ",
        "description": (
            "Запуск мульти-площадочного анализа и постановка задач в очередь."
        ),
    },
    {
        "name": "Результаты",
        "description": "Статусы запусков, overview, детальные результаты и срез офферов.",
    },
    {
        "name": "Каталоги",
        "description": "Каталоги площадок: игры, категории, деревья разделов.",
    },
    {
        "name": "Настройки",
        "description": "Runtime-настройки сети (пулы прокси).",
    },
]
app = FastAPI(
    title=settings.app_name,
    version="0.1.0",
    description=(
        "MarketStat v2: мульти-площадочная аналитика по цифровым товарам. "
        "Поддерживает FunPay, PlayerOK, GGSell и Plati.Market."
    ),
    lifespan=lifespan,
    openapi_tags=OPENAPI_TAGS,
)
WEB_ROOT = Path(__file__).resolve().parent / "web"
STATIC_ROOT = WEB_ROOT / "static"
if STATIC_ROOT.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_ROOT)), name="static")
app.include_router(web_router)
app.include_router(v2_router)


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/swagger", include_in_schema=False)
def swagger_alias() -> HTMLResponse:
    return get_swagger_ui_html(
        openapi_url=app.openapi_url or "/openapi.json",
        title=f"{app.title} - Swagger UI",
    )
