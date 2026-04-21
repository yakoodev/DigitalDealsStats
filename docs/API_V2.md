# API v2

## Base URL

`http://localhost:8000/v2`

## Swagger

- `/docs`
- `/swagger`
- `/openapi.json`

## 1) Запуск анализа

### `POST /analyze`

Запускает мульти-площадочный анализ.

Ключевые поля:
- `marketplaces`: массив площадок (`funpay`, `playerok`, `ggsell`, `platimarket`)
- `common_filters`: общий query/currency/ui_locale/execution/proxy policy
- `marketplace_filters`: площадко-специфичные фильтры

Ответ:
- `run_id`
- `status` (`queued` / `running` / `done` / `failed`)
- `overview`
- `marketplaces` (summary по каждой площадке)
- `progress` (проценты и логи этапов)

## 2) Статус и результаты

### `GET /analyze/{run_id}`

Текущий статус запуска и последний снимок результата.

### `GET /analyze/{run_id}/overview`

Pooled overview по всем площадкам запуска:
- pooled offers stats,
- comparison block,
- averages.

### `GET /analyze/{run_id}/marketplaces/{marketplace}`

Полный результат конкретной площадки:
- `summary`,
- `core` (normalized offers/sellers/reviews),
- `raw` (диагностика и таблицы площадки).

### `GET /analyze/{run_id}/marketplaces/{marketplace}/offers`

Срез офферов площадки с фильтрами:
- `limit`, `offset`
- `price_min`, `price_max`
- `min_reviews`
- `online_only`, `auto_delivery_only`
- `seller_query`

## 3) История

### `GET /history?limit=100`

История завершенных запусков:
- pooled метрики,
- список площадок в запуске,
- scope фильтров.

## 4) Каталоги площадок

### `GET /marketplaces`

Каталог площадок и их доступность.

### FunPay

- `GET /marketplaces/funpay/categories`

### PlayerOK

- `GET /marketplaces/playerok/categories`
  - `game_slug` (optional)
  - `force_refresh` (optional)

### GGSell

- `GET /marketplaces/ggsell/categories`
  - `type_slug` (optional)
  - `search` (optional)
  - `force_refresh` (optional)

### Plati.Market

- `GET /marketplaces/platimarket/categories`
- `GET /marketplaces/platimarket/catalog-tree`
- `GET /marketplaces/platimarket/games`
- `GET /marketplaces/platimarket/game-categories`
  - `game_id` или `game_slug`

## 5) Сетевые настройки

### `GET /settings/network`

Читает runtime-пулы прокси из БД.

### `PUT /settings/network`

Сохраняет runtime-пулы прокси.

Payload:

```json
{
  "datacenter_proxies": ["45.88.208.237:1508@user:pass"],
  "residential_proxies": [],
  "mobile_proxies": []
}
```

## Ошибки (типовые)

- `proxy_required` — strict policy, прокси не заданы.
- `marketplace_not_available` — площадка недоступна в реестре.
- `validation.empty_query_requires_scope` — пустой query без scope.
