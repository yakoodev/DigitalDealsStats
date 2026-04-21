from fastapi.testclient import TestClient

from app.main import app


def test_v2_marketplaces_endpoint_available() -> None:
    client = TestClient(app)
    response = client.get("/v2/marketplaces")
    assert response.status_code == 200
    payload = response.json()
    slugs = {item["slug"]: item for item in payload["items"]}
    assert slugs["funpay"]["enabled"] is True
    assert slugs["playerok"]["enabled"] is True
    assert slugs["ggsell"]["enabled"] is True
    assert slugs["platimarket"]["enabled"] is True


def test_v2_empty_query_requires_scope_for_ggsell() -> None:
    client = TestClient(app)
    response = client.post(
        "/v2/analyze",
        json={
            "marketplaces": ["ggsell"],
            "common_filters": {
                "query": "",
                "currency": "RUB",
                "ui_locale": "ru",
                "allow_direct_fallback": True,
                "execution": "sync",
            },
            "marketplace_filters": {"ggsell": {}},
        },
    )
    assert response.status_code == 400


def test_v2_empty_query_requires_scope_for_playerok() -> None:
    client = TestClient(app)
    response = client.post(
        "/v2/analyze",
        json={
            "marketplaces": ["playerok"],
            "common_filters": {
                "query": "",
                "currency": "RUB",
                "ui_locale": "ru",
                "allow_direct_fallback": True,
                "execution": "sync",
            },
            "marketplace_filters": {"playerok": {}},
        },
    )
    assert response.status_code == 400


def test_v2_empty_query_requires_scope_for_platimarket() -> None:
    client = TestClient(app)
    response = client.post(
        "/v2/analyze",
        json={
            "marketplaces": ["platimarket"],
            "common_filters": {
                "query": "",
                "currency": "RUB",
                "ui_locale": "ru",
                "allow_direct_fallback": True,
                "execution": "sync",
            },
            "marketplace_filters": {"platimarket": {}},
        },
    )
    assert response.status_code == 400


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
                "allow_direct_fallback": True,
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


def test_v2_empty_query_accepts_plati_game_slug_scope() -> None:
    client = TestClient(app)
    response = client.post(
        "/v2/analyze",
        json={
            "marketplaces": ["platimarket"],
            "common_filters": {
                "query": "",
                "currency": "RUB",
                "ui_locale": "ru",
                "allow_direct_fallback": True,
                "execution": "sync",
            },
            "marketplace_filters": {
                "platimarket": {
                    "category_game_slug": "pragmata",
                    "use_game_scope": True,
                    "use_group_scope": False,
                    "options": {
                        "profile": "safe",
                        "include_reviews": False,
                        "section_limit": 1,
                    },
                }
            },
        },
    )
    assert response.status_code != 400


def test_v2_network_settings_crud() -> None:
    client = TestClient(app)
    response = client.put(
        "/v2/settings/network",
        json={
            "datacenter_proxies": ["45.88.208.237:1508@user:pass"],
            "residential_proxies": [],
            "mobile_proxies": [],
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert isinstance(payload.get("datacenter_proxies"), list)
    assert payload["datacenter_proxies"]

    loaded = client.get("/v2/settings/network")
    assert loaded.status_code == 200
    loaded_payload = loaded.json()
    assert loaded_payload.get("datacenter_proxies") == payload.get("datacenter_proxies")


def test_v2_catalog_returns_proxy_required_without_pool() -> None:
    client = TestClient(app)
    client.put(
        "/v2/settings/network",
        json={
            "datacenter_proxies": [],
            "residential_proxies": [],
            "mobile_proxies": [],
        },
    )
    response = client.get("/v2/marketplaces/platimarket/catalog-tree")
    assert response.status_code == 400
    detail = response.json().get("detail", {})
    assert detail.get("code") == "proxy_required"


def test_v2_ggsell_categories_returns_proxy_required_without_pool() -> None:
    client = TestClient(app)
    client.put(
        "/v2/settings/network",
        json={
            "datacenter_proxies": [],
            "residential_proxies": [],
            "mobile_proxies": [],
        },
    )
    response = client.get("/v2/marketplaces/ggsell/categories")
    assert response.status_code == 400
    detail = response.json().get("detail", {})
    assert detail.get("code") == "proxy_required"
