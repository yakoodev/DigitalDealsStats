# v2 Implementation Notes

## Core idea

Продукт переведен на провайдерную архитектуру:

- `MarketplaceProvider` — единый контракт площадки;
- `FunPayProvider` и `PlayerOkProvider` — реализованные провайдеры;
- `GlobalAnalyzerService` — оркестратор мульти-площадочного запуска.

## Data shape

Результат площадки состоит из:

- `summary` — метрики и диагностика площадки;
- `core` — normalized entities (offers/sellers/reviews) для межплощадочного сравнения;
- `raw` — площадко-специфичный payload (legacy FunPay JSON).

Общий результат запуска:

- pooled метрики по объединенным офферам;
- comparison по каждой площадке;
- агрегаты (средние по площадкам).

## API

Роуты v2:

- `POST /v2/analyze`
- `GET /v2/analyze/{run_id}`
- `GET /v2/analyze/{run_id}/overview`
- `GET /v2/analyze/{run_id}/marketplaces/{marketplace}`
- `GET /v2/analyze/{run_id}/marketplaces/{marketplace}/offers`
- `GET /v2/history`
- `GET /v2/marketplaces`
- `GET /v2/marketplaces/funpay/categories`
- `GET /v2/marketplaces/playerok/categories`

Для не реализованных площадок возвращается явная ошибка:

- `detail.code = marketplace_not_available`

## UI routing

- `/` — общий анализ (overview);
- `/analysis/{marketplace}` — детальная страница площадки (сейчас `funpay` и `playerok`).

## Current scope

- Реализованы провайдеры `funpay` и `playerok`.
- `ggsell`, `platimarket` отображаются в UI/API каталоге как disabled.
