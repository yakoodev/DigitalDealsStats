from fastapi.testclient import TestClient

from app.main import app


def test_v2_marketplaces_endpoint_available() -> None:
    client = TestClient(app)
    response = client.get("/v2/marketplaces")
    assert response.status_code == 200
    payload = response.json()
    slugs = {item["slug"]: item for item in payload["items"]}
    assert slugs["funpay"]["enabled"] is True
    assert slugs["playerok"]["enabled"] is False


def test_v2_analyze_rejects_unavailable_marketplace() -> None:
    client = TestClient(app)
    response = client.post(
        "/v2/analyze",
        json={
            "marketplaces": ["playerok"],
            "common_filters": {
                "query": "test",
                "currency": "RUB",
                "execution": "sync",
            },
            "marketplace_filters": {},
        },
    )
    assert response.status_code == 400
    detail = response.json().get("detail", {})
    assert detail.get("code") == "marketplace_not_available"
    assert detail.get("marketplace") == "playerok"
