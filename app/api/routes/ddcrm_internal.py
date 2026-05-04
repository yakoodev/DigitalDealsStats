import hashlib
import logging
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.db.session import get_db
from app.models.entities import DdcrmProjectToken

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/internal/v1/ddcrm", tags=["ddcrm-internal"])


class ProjectTokenUpsertRequest(BaseModel):
    project_id: str = Field(alias="projectId")
    token: str = Field(min_length=16, max_length=512)
    scopes: list[str] = Field(default_factory=lambda: ["read", "jobs"])


class ProjectTokenRevokeRequest(BaseModel):
    project_id: str = Field(alias="projectId")


class IntegrationInvokeRequest(BaseModel):
    project_id: str = Field(alias="projectId")
    payload: dict[str, Any] | None = None


def _require_service_token(x_service_token: str | None) -> None:
    settings = get_settings()
    if not settings.ddcrm_service_auth_enabled:
        return

    accepted_tokens = {
        value.strip()
        for value in settings.ddcrm_service_auth_tokens.split(",")
        if value.strip()
    }

    if not accepted_tokens:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="ddcrm_service_auth_tokens is empty",
        )

    if not x_service_token or x_service_token.strip() not in accepted_tokens:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid service token",
        )


def _validate_project_uuid(project_id: str) -> str:
    try:
        value = str(UUID(project_id))
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="projectId must be UUID",
        ) from exc
    return value


def _token_hash(token: str) -> str:
    settings = get_settings()
    payload = f"{settings.ddcrm_project_token_signing_salt}:{token}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _split_scopes(scopes_csv: str) -> set[str]:
    return {
        scope.strip().lower()
        for scope in scopes_csv.split(",")
        if scope.strip()
    }


@router.post("/project-tokens/upsert")
def upsert_project_token(
    request: ProjectTokenUpsertRequest,
    db: Session = Depends(get_db),
    x_service_token: str | None = Header(default=None, alias="X-Service-Token"),
) -> dict[str, Any]:
    _require_service_token(x_service_token)
    project_id = _validate_project_uuid(request.project_id)
    normalized_scopes = sorted({
        scope.strip().lower()
        for scope in request.scopes
        if scope and scope.strip().lower() in {"read", "jobs"}
    })

    if not normalized_scopes:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="scopes must include read/jobs")

    entity = db.get(DdcrmProjectToken, project_id)
    if entity is None:
        entity = DdcrmProjectToken(
            project_id=project_id,
            token_hash_sha256=_token_hash(request.token),
            scopes_csv=",".join(normalized_scopes),
            status="active",
            updated_at=datetime.now(UTC),
        )
        db.add(entity)
    else:
        entity.token_hash_sha256 = _token_hash(request.token)
        entity.scopes_csv = ",".join(normalized_scopes)
        entity.status = "active"
        entity.updated_at = datetime.now(UTC)

    db.commit()
    logger.info("ddcrm token upsert project_id=%s scopes=%s", project_id, normalized_scopes)
    return {"status": "completed", "projectId": project_id}


@router.post("/project-tokens/revoke")
def revoke_project_token(
    request: ProjectTokenRevokeRequest,
    db: Session = Depends(get_db),
    x_service_token: str | None = Header(default=None, alias="X-Service-Token"),
) -> dict[str, Any]:
    _require_service_token(x_service_token)
    project_id = _validate_project_uuid(request.project_id)
    entity = db.get(DdcrmProjectToken, project_id)
    if entity is not None:
        entity.status = "revoked"
        entity.updated_at = datetime.now(UTC)
        db.commit()
    logger.info("ddcrm token revoke project_id=%s", project_id)
    return {"status": "completed", "projectId": project_id}


@router.post("/integration/{scope}")
def invoke_integration_scope(
    scope: str,
    request: IntegrationInvokeRequest,
    db: Session = Depends(get_db),
    x_service_token: str | None = Header(default=None, alias="X-Service-Token"),
    x_project_token: str | None = Header(default=None, alias="X-Project-Service-Token"),
) -> dict[str, Any]:
    _require_service_token(x_service_token)
    normalized_scope = scope.strip().lower()
    if normalized_scope not in {"read", "jobs"}:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="scope must be read/jobs")

    project_id = _validate_project_uuid(request.project_id)
    entity = db.get(DdcrmProjectToken, project_id)
    if entity is None or entity.status != "active":
        logger.warning("ddcrm denied project token missing/revoked project_id=%s scope=%s", project_id, normalized_scope)
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="project token is not active")

    if not x_project_token or _token_hash(x_project_token) != entity.token_hash_sha256:
        logger.warning("ddcrm denied project token mismatch project_id=%s scope=%s", project_id, normalized_scope)
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="project token mismatch")

    allowed_scopes = _split_scopes(entity.scopes_csv)
    if normalized_scope not in allowed_scopes:
        logger.warning(
            "ddcrm denied by scope project_id=%s scope=%s allowed=%s",
            project_id,
            normalized_scope,
            sorted(allowed_scopes),
        )
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="scope denied")

    payload = request.payload or {}
    if normalized_scope == "read":
        result = {
            "service": "FunPayStat",
            "projectId": project_id,
            "scope": normalized_scope,
            "health": "ok",
            "marketplaces": ["funpay", "playerok", "ggsell", "platimarket"],
        }
    else:
        result = {
            "service": "FunPayStat",
            "projectId": project_id,
            "scope": normalized_scope,
            "accepted": True,
            "echo": payload,
        }

    return {"result": result}
