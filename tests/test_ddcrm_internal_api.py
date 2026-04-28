import os
import uuid

from fastapi.testclient import TestClient

from app.core.config import get_settings
from app.main import app


def _prepare_env() -> None:
    os.environ["DDCRM_SERVICE_AUTH_ENABLED"] = "true"
    os.environ["DDCRM_SERVICE_AUTH_TOKENS"] = "svc-token-a"
    os.environ["DDCRM_PROJECT_TOKEN_SIGNING_SALT"] = "test-salt"
    get_settings.cache_clear()


def test_ddcrm_token_upsert_requires_service_token() -> None:
    _prepare_env()
    client = TestClient(app)

    response = client.post(
        "/internal/v1/ddcrm/project-tokens/upsert",
        json={
            "projectId": str(uuid.uuid4()),
            "token": "token-value-123456",
            "scopes": ["read", "jobs"],
        },
    )

    assert response.status_code == 401


def test_ddcrm_project_token_lifecycle_and_scope_guard() -> None:
    _prepare_env()
    client = TestClient(app)
    project_id = str(uuid.uuid4())
    project_token = "ddcrm-project-token-123456"

    upsert = client.post(
        "/internal/v1/ddcrm/project-tokens/upsert",
        headers={"X-Service-Token": "svc-token-a"},
        json={
            "projectId": project_id,
            "token": project_token,
            "scopes": ["read"],
        },
    )
    assert upsert.status_code == 200

    read_response = client.post(
        "/internal/v1/ddcrm/integration/read",
        headers={
            "X-Service-Token": "svc-token-a",
            "X-Project-Service-Token": project_token,
        },
        json={"projectId": project_id, "payload": {"foo": "bar"}},
    )
    assert read_response.status_code == 200
    assert read_response.json()["result"]["scope"] == "read"

    denied_jobs = client.post(
        "/internal/v1/ddcrm/integration/jobs",
        headers={
            "X-Service-Token": "svc-token-a",
            "X-Project-Service-Token": project_token,
        },
        json={"projectId": project_id, "payload": {"jobType": "analyze"}},
    )
    assert denied_jobs.status_code == 403

    revoke = client.post(
        "/internal/v1/ddcrm/project-tokens/revoke",
        headers={"X-Service-Token": "svc-token-a"},
        json={"projectId": project_id},
    )
    assert revoke.status_code == 200

    denied_after_revoke = client.post(
        "/internal/v1/ddcrm/integration/read",
        headers={
            "X-Service-Token": "svc-token-a",
            "X-Project-Service-Token": project_token,
        },
        json={"projectId": project_id, "payload": {}},
    )
    assert denied_after_revoke.status_code == 403
