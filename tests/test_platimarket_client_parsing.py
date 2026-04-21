import json

from app.core.config import Settings
from app.services.marketplaces.platimarket_provider import PlatiMarketProvider
from app.services.platimarket_client import PLATI_BASE_URL, PlatiMarketClient


CATEGORIES_JSON = {
    "1": {
        "ID_R": 101,
        "Title": "Игры",
        "UrlPreparedName": "games",
        "Children": {
            "1011": {
                "ID_R": 2893,
                "Title": "Project Zomboid",
                "UrlPreparedName": "project-zomboid",
                "Cnt": 210,
                "Children": {
                    "10111": {
                        "ID_R": 4001,
                        "Title": "Аренда",
                        "UrlPreparedName": "rent",
                        "Cnt": 48,
                        "Children": {},
                    }
                },
            }
        },
    }
}

OFFERS_HTML = """
<a product_id="1001" href="/itm/project-zomboid-rent/1001">
  <span name="title"><span>Project Zomboid аренда 24 часа</span></span>
  <span name="price">149 ₽</span>
  <div class="card-image-wrapper"><span class="caption-semibold">SellerOne</span></div>
  <span name="sold">Продано 22</span>
</a>
<a product_id="1002" href="/itm/project-zomboid-rent-7d/1002">
  <span name="title"><span>Project Zomboid аренда 7 дней</span></span>
  <span name="price">399 ₽</span>
  <div class="card-image-wrapper"><span class="caption-semibold">SellerTwo</span></div>
  <span name="sold">Продано 10</span>
</a>
"""

GAME_STYLE_OFFER_HTML = """
<a class="card card-hover border-0 m-auto" href="/itm/pragmata-standard/5589827" title="PRAGMATA Standard" product_id="5589827">
  <div class="card-image-wrapper position-relative round-12 overflow-hidden mb-2 w-100">
    <p class='d-inline-flex align-items-center justify-content-center gutter-column-3 w-100 mt-1 mb-1 ps-2 pe-2'>
      <span class='caption-semibold color-text-secondary text-truncate'>KEYBD</span>
    </p>
  </div>
  <p class="d-inline-flex align-items-end gutter-column-6 padding-left-2 mt-1 mb-1 text-nowrap w-100">
    <span class="title-bold color-text-title">3 949 ₽</span>
  </p>
  <div class="d-flex flex-wrap gutter-row-2 pb-1 pb-1 mb-2 w-100">
    <p class="custom-link custom-link--primary color-text-primary w-100 min-height-40 m-0 align-items-start padding-left-2 round-4">
      <span class="d-block footnote-medium text-break">PRAGMATA · Standard · STEAM RU</span>
    </p>
  </div>
</a>
"""

REVIEWS_HTML = """
<li>
  <span class="footnote-regular">20.04.2026</span>
  <a class="custom-link--primary" href="/itm/project-zomboid-rent/1001">
    <span>Project Zomboid, 149 ₽</span>
  </a>
  <div class="body-regular">быстро</div>
  <svg><use href="#thumb-up"></use></svg>
</li>
<li>
  <span class="footnote-regular">18.03.2026</span>
  <a class="custom-link--primary" href="/itm/project-zomboid-rent-7d/1002">
    <span>Project Zomboid, 399 ₽</span>
  </a>
  <div class="body-regular">норм</div>
</li>
"""

GAMES_HTML = """
<li class="title-list__item" id_cb="1848">
  <a class="card" href="/games/pragmata/1848/" title="Pragmata"></a>
</li>
<li class="title-list__item" id_cb="378">
  <a class="card" href="/games/minecraft/378/" title="Minecraft"></a>
</li>
"""

GAME_PAGE_HTML = """
<div id="filters">
  <div id="cat_selector">
    <div class="radio form-check w-100">
      <input class="radio__input form-check-input id_c_radios" type="radio" name="ID_C" value="0" id="cat-0" data-name="Все категории" />
      <label class="radio__label form-check-label w-100 text-wrap" for="cat-0">
        Все предложения
        <span class="footnote-regular color-text-tertiary">223</span>
      </label>
    </div>
    <div class="radio form-check w-100">
      <input class="radio__input form-check-input id_c_radios" type="radio" name="ID_C" value="13139" id="cat-13139" data-name="Steam Аккаунты" />
      <label class="radio__label form-check-label w-100 text-wrap" for="cat-13139">
        Steam Аккаунты
        <span class="footnote-regular color-text-tertiary">48</span>
      </label>
    </div>
  </div>
</div>
"""

OFFER_DETAILS_HTML = """
<html>
  <body>
    <h1>PRAGMATA DELUXE EDITION</h1>
    <span class="footnote-regular color-text-secondary">Продано 100+</span>
    <a href="/seller/superseller/777/">superseller</a>
    <script>var sellerWmid = '123456789012';</script>
    Отзывы 25
  </body>
</html>
"""


class _FakePlatiClient(PlatiMarketClient):
    def __init__(self) -> None:
        self._responses = {
            f"{PLATI_BASE_URL}/api/catalog_menu_json.asp": json.dumps(CATEGORIES_JSON, ensure_ascii=False),
            f"{PLATI_BASE_URL}/asp/block_goods_r.asp?id_r=4001&curr=RUB&GetStats=1&rnd=0.1": json.dumps(
                {"TotalCount": 48, "MinPrice": 149, "MaxPrice": 399}
            ),
        }

    def _request(  # type: ignore[override]
        self,
        method: str,  # noqa: ARG002
        url: str,
        *,
        params: dict | None = None,
        headers: dict[str, str] | None = None,  # noqa: ARG002
    ) -> str:
        if url.endswith("/api/catalog_menu_json.asp"):
            return self._responses[f"{PLATI_BASE_URL}/api/catalog_menu_json.asp"]
        if url.endswith("/asp/block_goods_r.asp") and params and params.get("GetStats") == 1:
            return json.dumps({"TotalCount": 48, "MinPrice": 149, "MaxPrice": 399})
        if url.endswith("/asp/block_games2.asp"):
            return f"2|{GAMES_HTML}"
        if url.endswith("/asp/block_goods_category_2.asp"):
            return f"224|299|7999|{OFFERS_HTML}"
        if url.endswith("/games/pragmata/1848/"):
            return GAME_PAGE_HTML
        if "/itm/" in url:
            return OFFER_DETAILS_HTML
        raise AssertionError(f"Unexpected request: {url} params={params}")


def test_fetch_categories_catalog_parses_group_and_sections() -> None:
    client = _FakePlatiClient()
    groups = client.fetch_categories_catalog()
    assert len(groups) >= 1
    by_id = {item.group_id: item for item in groups}
    assert 101 in by_id
    top_group = by_id[101]
    assert top_group.group_slug == "games"
    top_section_ids = {item.section_id for item in top_group.sections}
    assert 101 in top_section_ids
    assert 2893 in top_section_ids
    assert 4001 in top_section_ids


def test_fetch_catalog_tree_keeps_hierarchy() -> None:
    client = _FakePlatiClient()
    tree = client.fetch_catalog_tree()
    assert len(tree) == 1
    root = tree[0]
    assert root.section_id == 101
    assert root.title == "Игры"
    assert root.children
    zomboid = root.children[0]
    assert zomboid.section_id == 2893
    assert zomboid.children
    assert zomboid.children[0].section_id == 4001


def test_fetch_section_stats_parses_getstats_payload() -> None:
    client = _FakePlatiClient()
    total, min_price, max_price = client.fetch_section_stats(section_id=4001, currency="RUB")
    assert total == 48
    assert min_price == 149.0
    assert max_price == 399.0


def test_parse_offers_block_extracts_cards() -> None:
    client = _FakePlatiClient()
    cards = client.parse_offers_block(
        html=OFFERS_HTML,
        section_id=4001,
        section_slug="project-zomboid/rent",
        default_currency="RUB",
    )
    assert len(cards) == 2
    assert cards[0].offer_id == "1001"
    assert cards[0].seller_name == "SellerOne"
    assert cards[0].price == 149.0
    assert cards[0].sold_count == 22
    assert cards[1].offer_id == "1002"
    assert cards[1].currency == "RUB"


def test_parse_sold_text_supports_lower_bound_plus() -> None:
    count, is_lower_bound = PlatiMarketClient.parse_sold_text("Продано 100+")
    assert count == 100
    assert is_lower_bound is True


def test_parse_sold_text_supports_million_and_thousand_suffixes() -> None:
    count_million, lower_million = PlatiMarketClient.parse_sold_text("Продано 2.63 млн")
    assert count_million == 2_630_000
    assert lower_million is False

    count_thousand, lower_thousand = PlatiMarketClient.parse_sold_text("Продано 1,2 тыс")
    assert count_thousand == 1_200
    assert lower_thousand is False


def test_parse_sold_text_supports_less_than_bucket() -> None:
    count, is_lower_bound = PlatiMarketClient.parse_sold_text("Продано менее 10")
    assert count == 9
    assert is_lower_bound is False


def test_parse_reviews_block_extracts_offer_links_for_link_based_demand() -> None:
    client = _FakePlatiClient()
    reviews = client.parse_reviews_block(REVIEWS_HTML, default_seller_id="777")
    assert len(reviews) == 2
    assert reviews[0].offer_id == "1001"
    assert reviews[0].seller_id == "777"
    assert reviews[0].rating == 5
    assert reviews[1].offer_id == "1002"


def test_fetch_games_catalog_parses_game_cards() -> None:
    client = _FakePlatiClient()
    games = client.fetch_games_catalog(rows=50, max_pages=2)
    assert len(games) == 2
    by_slug = {item.game_slug: item for item in games}
    assert "pragmata" in by_slug
    assert by_slug["pragmata"].game_id == 1848


def test_fetch_game_offers_page_parses_prefixed_payload() -> None:
    client = _FakePlatiClient()
    total, min_price, max_price, offers = client.fetch_game_offers_page(
        game_id=1848,
        currency="RUB",
        page=1,
        rows=24,
    )
    assert total == 224
    assert min_price == 299.0
    assert max_price == 7999.0
    assert len(offers) == 2
    assert offers[0].offer_id == "1001"


def test_fetch_game_categories_parses_cat_selector() -> None:
    client = _FakePlatiClient()
    categories = client.fetch_game_categories(game_id=1848, game_slug="pragmata")
    assert len(categories) == 2
    assert categories[0].category_id == 0
    assert categories[0].category_name == "Все категории"
    assert categories[1].category_id == 13139
    assert categories[1].offers_count == 48


def test_parse_offers_block_extracts_game_style_cards() -> None:
    client = _FakePlatiClient()
    cards = client.parse_offers_block(
        html=GAME_STYLE_OFFER_HTML,
        section_id=1848,
        section_slug="game/1848",
        default_currency="RUB",
    )
    assert len(cards) == 1
    assert cards[0].offer_id == "5589827"
    assert cards[0].seller_name == "KEYBD"
    assert cards[0].price == 3949.0
    assert "PRAGMATA" in cards[0].title


def test_fetch_offer_details_parses_sold_from_item_page() -> None:
    client = _FakePlatiClient()
    details = client.fetch_offer_details(f"{PLATI_BASE_URL}/itm/pragmata-deluxe-edition/5832746")
    assert details["seller_id"] == "777"
    assert details["sold_text"] == "Продано 100+"
    assert details["sold_count"] == 100
    assert details["sold_is_lower_bound"] is True


def test_provider_link_based_review_match() -> None:
    provider = PlatiMarketProvider(db=None, settings=Settings())  # type: ignore[arg-type]
    reviews = _FakePlatiClient().parse_reviews_block(REVIEWS_HTML, default_seller_id="777")
    assert provider._is_review_link_match(  # noqa: SLF001
        reviews[0],
        current_offer_ids={"1001", "1002"},
        seller_offer_ids={"1001"},
    )
    assert provider._is_review_link_match(  # noqa: SLF001
        reviews[1],
        current_offer_ids={"1001", "1002"},
        seller_offer_ids={"1001"},
    ) is False
