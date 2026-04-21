# MarketStat v2

Мульти-площадочный аналитический сервис для цифровых товаров.

Поддерживаемые провайдеры:
- `funpay`
- `playerok`
- `ggsell`
- `platimarket`

API строится вокруг единого запуска анализа (`/v2/analyze`) и нормализованной модели данных (`Core + raw`) для сравнения площадок.

## Swagger / OpenAPI

- Swagger UI (основной): [http://localhost:8000/docs](http://localhost:8000/docs)
- Swagger UI (алиас): [http://localhost:8000/swagger](http://localhost:8000/swagger)
- OpenAPI JSON: [http://localhost:8000/openapi.json](http://localhost:8000/openapi.json)

## Быстрый старт (локально)

```bash
py -3.11 --version
py -3.11 -m venv .venv
.venv\Scripts\activate
python --version
pip install -e .
copy .env.example .env
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

Ожидается `Python 3.11+` (см. `requires-python` в `pyproject.toml`).

Проверка:

```bash
curl http://localhost:8000/healthz
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

## Основной flow

1. `POST /v2/analyze` — запуск анализа.
2. `GET /v2/analyze/{run_id}` — статус.
3. `GET /v2/analyze/{run_id}/overview` — pooled сводка.
4. `GET /v2/analyze/{run_id}/marketplaces/{marketplace}` — детали площадки.
5. `GET /v2/analyze/{run_id}/marketplaces/{marketplace}/offers` — полный срез офферов с фильтрами.

## Модульный паспорт площадок

`GET /v2/marketplaces` теперь возвращает не только `enabled/route`, но и модульные поля:
- `capabilities` — поддерживаемые возможности модуля,
- `data_source` — основной источник данных,
- `demand_mode` — режим расчета спроса.

Это используется и в UI (блок выбора площадок), и в документации как единая карта возможностей модулей.

## Минимальный пример запроса

```json
{
  "marketplaces": ["funpay", "ggsell"],
  "common_filters": {
    "query": "pragmata аренда",
    "currency": "RUB",
    "ui_locale": "ru",
    "force_refresh": false,
    "allow_direct_fallback": false,
    "execution": "auto"
  },
  "marketplace_filters": {
    "funpay": {
      "content_locale": "auto",
      "category_ids": [2893],
      "options": {
        "profile": "balanced",
        "include_reviews": true,
        "include_demand_index": true
      }
    },
    "ggsell": {
      "category_type_slug": "games",
      "category_slugs": ["pragmata"],
      "use_type_scope": false,
      "options": {
        "profile": "safe",
        "include_reviews": false,
        "include_demand_index": false,
        "section_limit": 10
      }
    }
  }
}
```

## Важные замечания

- По умолчанию действует strict policy по прокси: если прокси-пулы пустые, сервис может вернуть `proxy_required`.
- `allow_direct_fallback=true` разрешает запуск без прокси в текущем запросе.
- Для пустого `query` нужно задать scope площадки (`category_ids`, `category_slugs`, `category_game_slug` и т.д.).
- Для крупных категорий используется lower-bound диагностика покрытия.

## Документация

- API v2: [docs/API_V2.md](docs/API_V2.md)
- Архитектура и провайдеры: [docs/IMPLEMENTATION.md](docs/IMPLEMENTATION.md)
- Паспорта модулей: [docs/MODULES.md](docs/MODULES.md)

## Тесты

```bash
pip install -e .[dev]
python -c "import sys; assert sys.version_info >= (3, 11), 'Python 3.11+ required'"
pytest
```
