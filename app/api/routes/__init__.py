from app.api.routes.ddcrm_internal import router as ddcrm_internal_router
from app.api.routes.web import router as web_router
from app.api.routes.v2 import router as v2_router

__all__ = ["v2_router", "web_router", "ddcrm_internal_router"]
