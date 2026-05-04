# Паспорта модулей

Единая карта текущих модулей (`marketplace providers`) в `v2`.

## FunPay

- `slug`: `funpay`
- `data_source`: `public_html`
- `demand_mode`: `review_match_game_price`
- `capabilities`:
  - `offers`
  - `coverage`
  - `reviews`
  - `demand_index`
  - `history`

## PlayerOK

- `slug`: `playerok`
- `data_source`: `graphql+html_degrade`
- `demand_mode`: `review_match_game_price`
- `capabilities`:
  - `offers`
  - `coverage`
  - `reviews`
  - `demand_index`
  - `history`
  - `graphql_first`

## GGSell

- `slug`: `ggsell`
- `data_source`: `public_api+html_reviews`
- `demand_mode`: `sold_total+reviews_30d`
- `capabilities`:
  - `offers`
  - `coverage`
  - `reviews`
  - `demand_index`
  - `history`

Примечание:
- если часть slug категорий возвращает `404`, анализ не падает; категория пропускается с warning.

## Plati.Market

- `slug`: `platimarket`
- `data_source`: `public_http_api+html`
- `demand_mode`: `sold_total+reviews_30d`
- `capabilities`:
  - `offers`
  - `coverage`
  - `reviews`
  - `demand_index`
  - `history`
  - `sold_count`

## Где это видно

- API: `GET /v2/marketplaces`
- UI: блок выбора площадок на странице запуска анализа
