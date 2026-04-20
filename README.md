# MarketStat v2 (Multi-Marketplace Framework)

Каркас мульти-площадочной аналитики:

- общий запуск через `POST /v2/analyze`;
- провайдеры площадок (`marketplace providers`);
- единый формат `Core + raw` для сравнения между площадками;
- отдельные страницы UI: общий overview и детализация по площадке.

Сейчас реализованы `funpay` и `playerok`; `ggsell` и `platimarket` остаются в каталоге как disabled.

## Что уже работает

- `MarketplaceProvider` контракт + `MarketplaceRegistry`.
- `FunPayProvider`, который использует существующий парсер и возвращает:
  - `summary`,
  - `core` (normalized offers/sellers/reviews),
  - `raw` (полный legacy payload FunPay).
- `PlayerOkProvider`:
  - GraphQL-first сбор (`/graphql`) через обычный HTTP,
  - HTML degrade как fallback (без browser emulation),
  - lower-bound диагностика покрытия,
  - анализ спроса по отзывам с матчингом `игра + цена`,
  - кэш результатов на 24 часа.
- `GlobalAnalyzerService`:
  - запуск анализа по выбранным площадкам,
  - расчет pooled overview + comparison + averages,
  - хранение запусков и истории.
- Новый API `/v2/*`:
  - `POST /v2/analyze`
  - `GET /v2/analyze/{run_id}`
  - `GET /v2/analyze/{run_id}/overview`
  - `GET /v2/analyze/{run_id}/marketplaces/{marketplace}`
  - `GET /v2/analyze/{run_id}/marketplaces/{marketplace}/offers`
  - `GET /v2/history`
  - `GET /v2/marketplaces`
  - `GET /v2/marketplaces/funpay/categories`
  - `GET /v2/marketplaces/playerok/categories`
- UI:
  - `/` — overview общего анализа,
  - `/analysis/funpay` — детальная страница FunPay,
  - `/analysis/playerok` — детальная страница PlayerOK,
  - общий выбор площадок + общий пул прокси в форме запуска,
  - история запусков с подпунктами по площадкам.

## Быстрый старт

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -e .
copy .env.example .env
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

Проверка:

```bash
curl http://localhost:8000/healthz
```

UI:

```text
http://localhost:8000/
```

## Docker

```bash
docker compose up --build -d
```

Проверка:

```bash
curl http://localhost:8000/healthz
curl http://localhost:8000/v2/marketplaces
```

## Пример запроса

```json
{
  "marketplaces": ["funpay", "playerok"],
  "common_filters": {
    "query": "project zomboid аренда",
    "currency": "RUB",
    "force_refresh": false,
    "execution": "auto",
    "datacenter_proxies": [],
    "residential_proxies": [],
    "mobile_proxies": []
  },
  "marketplace_filters": {
    "funpay": {
      "content_locale": "auto",
      "category_game_id": null,
      "category_id": null,
      "options": {
        "profile": "balanced",
        "include_reviews": false,
        "include_demand_index": false,
        "include_fallback_scan": true,
        "section_limit": 80,
        "seller_limit": 40,
        "review_pages_per_seller": 4,
        "history_points_limit": 60
      }
    },
    "playerok": {
      "category_game_slug": "project-zomboid",
      "category_slugs": ["project-zomboid/rent"],
      "use_game_scope": true,
      "use_html_degrade": true,
      "advanced_headers": {},
      "advanced_cookies": {},
      "options": {
        "profile": "balanced",
        "include_reviews": true,
        "include_demand_index": true,
        "include_fallback_scan": true,
        "section_limit": 80,
        "seller_limit": 3,
        "review_pages_per_seller": 3,
        "history_points_limit": 60
      }
    }
  }
}
```

## Тесты

```bash
pip install -e .[dev]
pytest
```
