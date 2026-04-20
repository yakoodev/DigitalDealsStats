from app.core.config import Settings
from app.services.marketplaces.playerok_provider import PlayerOkProvider
from app.services.playerok_client import (
    PlayerOkCategorySection,
    PlayerOkClient,
    PlayerOkOfferData,
    PlayerOkReviewData,
)


SECTION_HTML = """
<html><body>
  <h1>Project Zomboid > Аренда</h1>
  <div>Всего 5490 товаров</div>
  <a href="/products/a0c411915b2f-offer-one">Project Zomboid аренда 24 часа</a>
  <a href="/products/a0c411915b2f-offer-one">дубликат</a>
  <a href="/products/bb33aa-offer-two">Project Zomboid аренда 7 дней</a>
</body></html>
"""

OFFER_HTML = """
<html><body>
  <a href="/project-zomboid/rent">Project Zomboid Аренда</a>
  <h1>Project Zomboid аренда 24 часа</h1>
  <div>137 ₽</div>
  <a href="/profile/KabanSh0p/products">KabanSh0p 5.0 120 отзывов</a>
  <div>Отзывы о KabanSh0p</div>
  <div>В этом месяце</div>
  <div>Project Zomboid, 137 ₽</div>
  <div>быстро</div>
  <div>Все отзывы оставлены после покупки товаров</div>
</body></html>
"""


class _FakePlayerOkGraphqlClient(PlayerOkClient):
    def __init__(self) -> None:
        self.settings = Settings()
        self.use_html_degrade = True
        self.proxy_pool = None  # type: ignore[assignment]
        self._cookies = None  # type: ignore[assignment]
        self._http = None  # type: ignore[assignment]

    def _graphql(  # type: ignore[override]
        self,
        *,
        operation_name: str,
        query: str,  # noqa: ARG002
        variables: dict,
        gql_path: str,  # noqa: ARG002
    ) -> dict:
        if operation_name == "games":
            after = (variables.get("pagination") or {}).get("after")
            if after is None:
                return {
                    "games": {
                        "totalCount": 4,
                        "pageInfo": {"hasNextPage": True, "endCursor": "c1"},
                        "edges": [
                            {"node": {"id": "g1", "slug": "project-zomboid", "name": "Project Zomboid"}},
                            {"node": {"id": "g2", "slug": "roblox", "name": "Roblox"}},
                        ],
                    }
                }
            return {
                "games": {
                    "totalCount": 4,
                    "pageInfo": {"hasNextPage": False, "endCursor": "c2"},
                    "edges": [
                        {"node": {"id": "g3", "slug": "steam", "name": "Steam"}},
                        {"node": {"id": "g4", "slug": "telegram", "name": "Telegram"}},
                    ],
                }
            }
        if operation_name == "gameCategories":
            assert variables.get("slug") == "project-zomboid"
            return {
                "game": {
                    "id": "g1",
                    "slug": "project-zomboid",
                    "name": "Project Zomboid",
                    "categories": [
                        {"id": "c1", "slug": "accounts", "name": "Аккаунты"},
                        {"id": "c2", "slug": "rent", "name": "Аренда"},
                    ],
                }
            }
        if operation_name == "items":
            return {
                "items": {
                    "totalCount": 3,
                    "pageInfo": {"hasNextPage": False, "endCursor": "i1"},
                    "edges": [
                        {
                            "node": {
                                "id": "o1",
                                "slug": "o1-zomboid-rent-24h",
                                "name": "Project Zomboid аренда 24 часа",
                                "price": 137,
                                "rawPrice": 200,
                                "user": {
                                    "id": "u1",
                                    "username": "seller_1",
                                    "isOnline": True,
                                    "rating": 5,
                                    "testimonialCounter": 25,
                                },
                                "game": {"id": "g1", "slug": "project-zomboid", "name": "Project Zomboid"},
                                "category": {"id": "c2", "slug": "rent", "name": "Аренда"},
                            }
                        },
                        {
                            "node": {
                                "id": "o2",
                                "slug": "o2-zomboid-leveling",
                                "name": "Project Zomboid прокачка",
                                "price": 50,
                                "rawPrice": 80,
                                "user": {
                                    "id": "u2",
                                    "username": "seller_2",
                                    "isOnline": False,
                                    "rating": 5,
                                    "testimonialCounter": 10,
                                },
                                "game": {"id": "g1", "slug": "project-zomboid", "name": "Project Zomboid"},
                                "category": {"id": "c2", "slug": "rent", "name": "Аренда"},
                            }
                        },
                    ],
                }
            }
        if operation_name == "testimonials":
            return {
                "testimonials": {
                    "totalCount": 1,
                    "pageInfo": {"hasNextPage": False, "endCursor": "t1"},
                    "edges": [
                        {
                            "node": {
                                "id": "r1",
                                "status": "APPROVED",
                                "text": "быстро",
                                "rating": 5,
                                "createdAt": "2026-04-20T11:46:15.554Z",
                                "deal": {
                                    "id": "d1",
                                    "item": {
                                        "id": "o1",
                                        "slug": "o1-zomboid-rent-24h",
                                        "name": "Project Zomboid аренда 24 часа",
                                        "price": 137,
                                        "rawPrice": 200,
                                        "sellerType": "USER",
                                        "game": {"id": "g1", "slug": "project-zomboid", "name": "Project Zomboid"},
                                        "category": {"id": "c2", "slug": "rent", "name": "Аренда"},
                                    },
                                },
                            }
                        }
                    ],
                }
            }
        raise AssertionError(f"Unexpected operation: {operation_name}")


class _FakePlayerOkHtmlClient(PlayerOkClient):
    def __init__(self) -> None:
        self.settings = Settings()
        self.use_html_degrade = True
        self.proxy_pool = None  # type: ignore[assignment]
        self._cookies = None  # type: ignore[assignment]
        self._http = None  # type: ignore[assignment]
        self._responses = {
            "https://playerok.com/project-zomboid/rent": SECTION_HTML,
            "https://playerok.com/products/a0c411915b2f-offer-one": OFFER_HTML,
        }

    def _request(  # type: ignore[override]
        self,
        method: str,  # noqa: ARG002
        url: str,
        *,
        json_payload: dict | None = None,  # noqa: ARG002
        headers: dict[str, str] | None = None,  # noqa: ARG002
    ) -> str:
        return self._responses[url]


def test_fetch_games_catalog_uses_cursor_pagination() -> None:
    client = _FakePlayerOkGraphqlClient()
    games = client.fetch_games_catalog()
    slugs = [item.game_slug for item in games]
    assert slugs == ["project-zomboid", "roblox", "steam", "telegram"]


def test_fetch_game_categories_contains_rent_section() -> None:
    client = _FakePlayerOkGraphqlClient()
    game = client.fetch_game_categories("project-zomboid")
    assert game is not None
    sections = {item.section_slug: item for item in game.sections}
    assert "project-zomboid/rent" in sections
    assert sections["project-zomboid/rent"].section_id == "c2"


def test_fetch_items_for_section_applies_local_keyword_filter() -> None:
    client = _FakePlayerOkGraphqlClient()
    section = PlayerOkCategorySection(
        section_id="c2",
        game_id="g1",
        game_slug="project-zomboid",
        game_name="Project Zomboid",
        category_slug="rent",
        section_slug="project-zomboid/rent",
        section_url="https://playerok.com/project-zomboid/rent",
        section_name="Аренда",
        full_name="Project Zomboid > Аренда",
    )
    result = client.fetch_items_for_section(section=section, query="24 часа", max_items=50)
    assert result.counter_total == 3
    assert result.loaded_count == 2
    assert len(result.offers) == 1
    assert result.offers[0].description == "Project Zomboid аренда 24 часа"


def test_fetch_testimonials_maps_item_game_category_and_amount() -> None:
    client = _FakePlayerOkGraphqlClient()
    reviews, total = client.fetch_testimonials(
        seller_uuid="u1",
        seller_slug="seller_1",
        game_id="g1",
        category_id="c2",
        max_pages=1,
    )
    assert total == 1
    assert len(reviews) == 1
    review = reviews[0]
    assert review.game_id == "g1"
    assert review.category_id == "c2"
    assert review.amount == 137.0
    assert review.detail.startswith("Project Zomboid аренда 24 часа")


def test_parse_section_and_offer_html_degrade() -> None:
    client = _FakePlayerOkHtmlClient()
    section = client.parse_section("https://playerok.com/project-zomboid/rent")
    assert section.counter_total == 5490
    assert section.loaded_count == 2
    offer = client.parse_offer("https://playerok.com/products/a0c411915b2f-offer-one")
    assert offer.seller_slug == "KabanSh0p"
    assert offer.price == 137.0
    assert offer.section_slug == "project-zomboid/rent"
    assert len(offer.reviews) == 1


def _mk_offer(
    *,
    offer_id: str,
    price: float,
    game_id: str = "g1",
    category_id: str = "c2",
) -> PlayerOkOfferData:
    return PlayerOkOfferData(
        game_id=game_id,
        game_slug="project-zomboid",
        game_name="Project Zomboid",
        category_id=category_id,
        category_slug="rent",
        section_slug="project-zomboid/rent",
        section_name="Project Zomboid > Аренда",
        offer_id=offer_id,
        offer_url=f"https://playerok.com/products/{offer_id}",
        seller_uuid="u1",
        seller_slug="seller_1",
        seller_name="seller_1",
        seller_url="https://playerok.com/profile/seller_1/products",
        description="Project Zomboid аренда 24 часа",
        price=price,
        currency="RUB",
        reviews_count=10,
        is_online=True,
        auto_delivery=False,
        reviews=[],
    )


def test_playerok_provider_review_match_requires_game_and_price() -> None:
    provider = PlayerOkProvider(db=None, settings=Settings())  # type: ignore[arg-type]
    offers = [_mk_offer(offer_id="o1", price=137.0)]

    match_review = PlayerOkReviewData(
        seller_slug="seller_1",
        seller_uuid="u1",
        detail="Project Zomboid аренда 24 часа, 137 ₽",
        text="быстро",
        rating=5,
        date_bucket="this month",
        game_id="g1",
        category_id="c2",
        amount=137.0,
        created_at="2026-04-20T11:46:15.554Z",
    )
    matched, reason = provider._is_review_match(  # noqa: SLF001
        match_review,
        seller_offers=offers,
        category_tokens=["project", "zomboid", "rent"],
    )
    assert matched is True
    assert reason is None

    no_game_review = PlayerOkReviewData(
        seller_slug="seller_1",
        seller_uuid="u1",
        detail="Other game, 137 ₽",
        text="ok",
        rating=5,
        date_bucket="this month",
        game_id="g999",
        category_id="c2",
        amount=137.0,
        created_at=None,
    )
    matched, reason = provider._is_review_match(  # noqa: SLF001
        no_game_review,
        seller_offers=offers,
        category_tokens=["project", "zomboid", "rent"],
    )
    assert matched is False
    assert reason == "no_game_match"

    no_amount_review = PlayerOkReviewData(
        seller_slug="seller_1",
        seller_uuid="u1",
        detail="Project Zomboid аренда",
        text="ok",
        rating=5,
        date_bucket="this month",
        game_id="g1",
        category_id="c2",
        amount=None,
        created_at=None,
    )
    matched, reason = provider._is_review_match(  # noqa: SLF001
        no_amount_review,
        seller_offers=offers,
        category_tokens=["project", "zomboid", "rent"],
    )
    assert matched is False
    assert reason == "no_amount"

    wrong_price_review = PlayerOkReviewData(
        seller_slug="seller_1",
        seller_uuid="u1",
        detail="Project Zomboid аренда, 999 ₽",
        text="ok",
        rating=5,
        date_bucket="this month",
        game_id="g1",
        category_id="c2",
        amount=999.0,
        created_at=None,
    )
    matched, reason = provider._is_review_match(  # noqa: SLF001
        wrong_price_review,
        seller_offers=offers,
        category_tokens=["project", "zomboid", "rent"],
    )
    assert matched is False
    assert reason == "no_price_match"


def test_playerok_provider_dedupes_reviews() -> None:
    review = PlayerOkReviewData(
        seller_slug="seller-1",
        seller_uuid="u1",
        detail="В этом месяце Project Zomboid, 20 ₽",
        text="быстро",
        rating=5,
        date_bucket="this month",
        game_id="g1",
        category_id="c2",
        amount=20.0,
        created_at="2026-04-20T10:00:00Z",
    )
    deduped = PlayerOkProvider._dedupe_reviews([review, review])  # noqa: SLF001
    assert len(deduped) == 1
