from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import FileResponse

router = APIRouter(tags=["web"])

WEB_ROOT = Path(__file__).resolve().parents[2] / "web"
INDEX_PATH = WEB_ROOT / "index.html"


@router.get("/", include_in_schema=False)
def web_index() -> FileResponse:
    return FileResponse(INDEX_PATH)


@router.get("/analysis/{marketplace}", include_in_schema=False)
def web_marketplace_analysis(marketplace: str) -> FileResponse:  # noqa: ARG001
    return FileResponse(INDEX_PATH)
