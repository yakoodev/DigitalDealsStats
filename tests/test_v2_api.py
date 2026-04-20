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


def test_v2_empty_query_accepts_category_ids_scope() -> None:
    client = TestClient(app)
    response = client.post(
        "/v2/analyze",
        json={
            "marketplaces": ["funpay"],
            "common_filters": {
                "query": "",
                "currency": "RUB",
                "ui_locale": "ru",
                "execution": "sync",
            },
            "marketplace_filters": {
                "funpay": {
                    "category_ids": [2893],
                    "options": {
                        "include_reviews": False,
                    },
                }
            },
        },
    )
    assert response.status_code != 400
