from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.routes import v2_router, web_router
from app.core.config import get_settings
from app.db.base import Base
from app.db.session import SessionLocal, engine
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
app = FastAPI(
    title=settings.app_name,
    version="0.1.0",
    lifespan=lifespan,
)
app.include_router(web_router)
app.include_router(v2_router)


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}
