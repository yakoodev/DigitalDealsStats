# MarketStat v2 (Multi-Marketplace Framework)

Каркас мульти-площадочной аналитики:

- общий запуск через `POST /v2/analyze`;
- провайдеры площадок (`marketplace providers`);
- единый формат `Core + raw` для сравнения между площадками;
- отдельные страницы UI: общий overview и детализация по площадке.

Сейчас реализован только `funpay`, но API/UI уже готовы для добавления `playerok`, `ggsell`, `platimarket`.

## Что уже работает

- `MarketplaceProvider` контракт + `MarketplaceRegistry`.
- `FunPayProvider`, который использует существующий парсер и возвращает:
  - `summary`,
  - `core` (normalized offers/sellers/reviews),
  - `raw` (полный legacy payload FunPay).
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
- UI:
  - `/` — overview общего анализа,
  - `/analysis/funpay` — детальная страница FunPay,
  - список площадок с disabled статусом для еще не реализованных провайдеров,
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
  "marketplaces": ["funpay"],
  "common_filters": {
    "query": "project zomboid аренда",
    "currency": "RUB",
    "force_refresh": false,
    "execution": "auto"
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
    }
  }
}
```

## Тесты

```bash
pip install -e .[dev]
pytest
```
