# Архитектура v2

## Цель

Сервис агрегирует предложения с разных площадок в единый формат, чтобы:
- считать общие KPI рынка,
- сравнивать площадки между собой,
- детально анализировать каждую площадку в отдельности.

## Основные сущности

- `MarketplaceProvider` — контракт провайдера площадки.
- `MarketplaceRegistry` — реестр провайдеров и их доступности.
- `GlobalAnalyzerService` — оркестратор мульти-площадочного запуска.
- `MarketplaceRunResultDTO` — результат провайдера (`summary`, `core`, `raw`).

## Контракт провайдера

Каждый провайдер реализует:
- `analyze(common_filters, marketplace_filters) -> MarketplaceRunResultDTO`
- `list_offers(run_result, ...) -> MarketplaceOffersResponseDTO`

Дополнительно провайдер может предоставлять каталожные методы (игры, категории, дерево и т.д.), которые используются UI и API.

## Нормализованная модель (`Core + raw`)

### Core

Унифицированные поля для кросс-площадочного анализа:
- офферы (`NormalizedOfferDTO`),
- продавцы (`NormalizedSellerDTO`),
- отзывы (`NormalizedReviewDTO`).

### Raw

Площадко-специфичные данные:
- диагностические таблицы,
- внутренние warnings,
- расширенные графики,
- служебные метаданные парсинга.

## Текущие провайдеры

- `FunPayProvider`
- `PlayerOkProvider`
- `GgSellProvider`
- `PlatiMarketProvider`

Все провайдеры подключены в `MarketplaceRegistry` как `enabled=true`.

В `MarketplaceRegistry` для каждого модуля также задан "паспорт":
- `capabilities`
- `data_source`
- `demand_mode`

Эти поля отдаются наружу через `GET /v2/marketplaces` и используются UI.

## Кэширование

- Кэш результата провайдера: таблица `AnalysisCache`.
- Ключ кэша включает:
  - query,
  - валюту,
  - locale,
  - scope,
  - effective options.
- TTL по умолчанию: 24 часа.

## Прокси и сетевая политика

- Runtime-пулы прокси хранятся в БД (`RuntimeNetworkSettings`).
- Разрешение источника:
  1. runtime settings (БД),
  2. marketplace/common override из запроса,
  3. env.
- По умолчанию strict policy: если прокси нет, возвращается `proxy_required`.
- Для разового запуска можно передать `allow_direct_fallback=true`.

## Очереди и выполнение

- `POST /v2/analyze` поддерживает режимы:
  - `sync`,
  - `async`,
  - `auto`.
- В `auto` оркестратор определяет heavy/safe запуск по options и количеству площадок.
- Async задачи уходят в Redis/RQ.

## История и наблюдаемость

- Запуски сохраняются в `AnalysisRequest` (`mode=global_v2`).
- В истории доступны:
  - pooled-метрики,
  - метрики по каждой площадке,
  - сохраненные фильтры запуска.
- Прогресс ведется по этапам с логами.

## Swagger/OpenAPI

- `/docs` — Swagger UI
- `/swagger` — алиас Swagger UI
- `/openapi.json` — OpenAPI схема

Swagger сгруппирован по тегам:
- `Анализ`
- `Результаты`
- `Каталоги`
- `Настройки`
