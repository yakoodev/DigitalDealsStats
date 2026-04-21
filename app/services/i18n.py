from __future__ import annotations

from typing import Any

SUPPORTED_UI_LOCALES = {"ru", "en"}

MESSAGES: dict[str, dict[str, str]] = {
    "ru": {
        "validation.empty_query_requires_scope": (
            "Для пустого запроса укажите область сканирования: "
            "для FunPay выберите category_game_id/category_id/category_ids, "
            "для PlayerOK — category_game_slug/category_slugs, "
            "для GGSell — category_type_slug/category_slugs, "
            "для Plati.Market — category_game_id/category_group_id/category_ids."
        ),
        "progress.global.validate": "Проверяю доступность выбранных площадок.",
        "progress.global.start": "Запускаю анализ площадок: {marketplaces}.",
        "progress.global.marketplace.start": "[{idx}/{total}] Анализ площадки {slug}.",
        "progress.global.marketplace.done": "Площадка {slug}: готово.",
        "progress.global.queued": "Задача поставлена в очередь.",
        "progress.global.failed": "Анализ завершился с ошибкой.",
        "progress.global.done": "Анализ завершён.",
        "progress.local.start": "Запуск анализа.",
        "progress.local.cache.check": "Проверяю кэш запроса.",
        "progress.local.cache.hit": "Найден валидный кэш. Возвращаю сохранённый результат.",
        "progress.local.collect_offers": (
            "Собираю офферы в локали {content_locale} (валюта запроса: {currency})."
        ),
        "progress.local.sections.prepared": "Сформирован список разделов: {count} ({locale_hint})",
        "progress.local.section.scan": "Проверяю раздел {processed}/{total}: {section_url}",
        "progress.local.section.done": (
            "Раздел проверен: найдено {loaded_count} лотов (counter={counter_total})"
        ),
        "progress.local.sections.category_done": "Разделы в выбранной области обработаны: {count}",
        "progress.local.sections.fallback_start": "Запускаю резервный обход разделов.",
        "progress.local.sections.fallback_done": "Резервный обход завершён. Просканировано разделов: {count}",
        "progress.local.offers.done": "Сбор офферов завершён: {count}.",
        "progress.local.reviews.start": "Начинаю проверку продавцов и их отзывов.",
        "progress.local.reviews.sellers_prepared": "Подготовлен список продавцов для отзывов: {count}",
        "progress.local.reviews.seller.scan": "Проверяю продавца {idx}/{total}: #{seller_id}",
        "progress.local.reviews.page": "Проверил страницу отзывов #{page} продавца #{seller_id}",
        "progress.local.reviews.seller.failed": "Не удалось получить отзывы продавца #{seller_id}",
        "progress.local.reviews.seller.done": "Отзывы продавца #{seller_id}: {count}",
        "progress.local.reviews.relevant_after_seller": (
            "Подтвержденные покупки после продавца #{seller_id}: {count}"
        ),
        "progress.local.reviews.done": "Отзывы обработаны. Подтвержденных покупок: {count}.",
        "progress.local.reviews.skipped": "Режим без анализа отзывов: этап пропущен.",
        "progress.local.aggregate": "Формирую метрики, таблицы и графики.",
        "progress.local.persist": "Сохраняю результат в БД и историю запусков.",
        "progress.local.queued": "Задача поставлена в очередь.",
        "progress.local.done": "Анализ завершён.",
        "progress.local.done.log": "Анализ завершён успешно.",
        "progress.local.failed": "Анализ завершился с ошибкой.",
        "error.prefix": "Ошибка",
        "warning.coverage.lower_bound": (
            "Часть разделов имеет ограничение выгрузки (~4000 лотов), метрики являются нижней оценкой."
        ),
        "warning.offers.none_query": "По текущему запросу не найдено релевантных офферов.",
        "warning.offers.none_scope": "В выбранной области сканирования офферы не найдены.",
        "warning.offers.weak_filtered": (
            "Отсечено {count} офферов со слабыми общими совпадениями "
            "(например только по слову 'аренда')."
        ),
        "warning.currency.relaxed": (
            "Офферов в валюте {currency} не найдено; показаны исходные офферы в валюте площадки. "
            "Потенциально смешанные валюты: {count} офферов."
        ),
        "warning.currency.filtered": (
            "Отфильтровано {count} офферов в другой валюте (выбрана {currency}; конвертация не выполняется)."
        ),
        "warning.category.game_not_found": "Выбрана игра, но разделы не найдены в каталоге категорий.",
        "warning.category.section_not_found": "Выбранный раздел категории не найден или недоступен.",
        "warning.category.sections_not_found": "Выбранные разделы категории не найдены или недоступны.",
        "warning.reviews.failed_sellers": (
            "Не удалось собрать отзывы у {count} продавцов (временные лимиты/прокси-сбои). "
            "Метрика спроса может быть занижена."
        ),
        "warning.reviews.none_relevant": "Отзывы собраны, но подтвержденные покупки не найдены.",
        "warning.reviews.no_amount": "Во многих отзывах нет суммы оплаты, поэтому они не засчитаны.",
        "warning.reviews.no_game": "Часть отзывов не совпала по игре/разделу с текущей выборкой.",
        "warning.reviews.no_price": "Часть отзывов не совпала по сумме с ценами офферов продавцов.",
        "warning.playerok.safe_endpoints": "Найдены безопасные внутренние endpoint'ы: {endpoints}",
        "warning.playerok.graphql_section_failed": (
            "Не удалось получить офферы по GraphQL для раздела '{section}'."
        ),
        "warning.playerok.graphql_top_items_failed": (
            "Не удалось получить глобальную выдачу PlayerOK по GraphQL."
        ),
        "warning.playerok.html_degrade_used": (
            "Для {count} разделов применен HTML degrade из-за ошибки GraphQL."
        ),
        "warning.playerok.graphql_failed_count": (
            "GraphQL дал ошибки в {count} местах; проверьте прокси/заголовки."
        ),
        "warning.ggsell.type_not_found": "Выбранный тип каталога GGSell не найден.",
        "warning.ggsell.categories_not_found": "Не удалось загрузить данные для {count} выбранных категорий GGSell.",
        "warning.ggsell.category_failed": "Не удалось обработать категорию GGSell: {section}.",
        "warning.ggsell.categories_failed_count": "Сбор завершился с ошибками по категориям GGSell: {count}.",
        "warning.plati.group_not_found": "Выбранная группа Plati.Market не найдена в каталоге.",
        "warning.plati.game_not_found": "Выбранная игра Plati.Market не найдена в каталоге.",
        "warning.plati.game_categories_not_found": "Часть выбранных категорий выбранной игры не найдена.",
        "warning.plati.sections_not_found": "Часть выбранных разделов Plati.Market не найдена в каталоге.",
        "warning.plati.section_failed": "Не удалось обработать раздел Plati.Market: {section}.",
        "warning.plati.sections_failed_count": "Сбор завершился с ошибками по разделам Plati.Market: {count}.",
        "warning.plati.game_failed": "Не удалось обработать игру Plati.Market: {game}.",
        "warning.plati.games_failed_count": "Сбор завершился с ошибками по играм Plati.Market: {count}.",
    },
    "en": {
        "validation.empty_query_requires_scope": (
            "For an empty query you must provide scan scope: "
            "FunPay requires category_game_id/category_id/category_ids, "
            "PlayerOK requires category_game_slug/category_slugs, "
            "GGSell requires category_type_slug/category_slugs, "
            "Plati.Market requires category_game_id/category_group_id/category_ids."
        ),
        "progress.global.validate": "Checking selected marketplace availability.",
        "progress.global.start": "Starting marketplace analysis: {marketplaces}.",
        "progress.global.marketplace.start": "[{idx}/{total}] Analyzing {slug}.",
        "progress.global.marketplace.done": "{slug}: done.",
        "progress.global.queued": "Job has been queued.",
        "progress.global.failed": "Analysis failed.",
        "progress.global.done": "Analysis completed.",
        "progress.local.start": "Starting analysis.",
        "progress.local.cache.check": "Checking request cache.",
        "progress.local.cache.hit": "Valid cache found. Returning stored result.",
        "progress.local.collect_offers": (
            "Collecting offers in {content_locale} locale (requested currency: {currency})."
        ),
        "progress.local.sections.prepared": "Prepared section list: {count} ({locale_hint})",
        "progress.local.section.scan": "Scanning section {processed}/{total}: {section_url}",
        "progress.local.section.done": "Section parsed: {loaded_count} offers (counter={counter_total})",
        "progress.local.sections.category_done": "Selected scope parsed: {count} sections",
        "progress.local.sections.fallback_start": "Running fallback section scan.",
        "progress.local.sections.fallback_done": "Fallback scan finished. Sections parsed: {count}",
        "progress.local.offers.done": "Offer collection finished: {count}.",
        "progress.local.reviews.start": "Starting seller reviews analysis.",
        "progress.local.reviews.sellers_prepared": "Prepared sellers for reviews: {count}",
        "progress.local.reviews.seller.scan": "Checking seller {idx}/{total}: #{seller_id}",
        "progress.local.reviews.page": "Checked review page #{page} for seller #{seller_id}",
        "progress.local.reviews.seller.failed": "Failed to fetch reviews for seller #{seller_id}",
        "progress.local.reviews.seller.done": "Seller #{seller_id} reviews: {count}",
        "progress.local.reviews.relevant_after_seller": (
            "Confirmed purchases after seller #{seller_id}: {count}"
        ),
        "progress.local.reviews.done": "Reviews processed. Confirmed purchases: {count}.",
        "progress.local.reviews.skipped": "Reviews stage skipped.",
        "progress.local.aggregate": "Building metrics, tables and charts.",
        "progress.local.persist": "Saving result to DB and history.",
        "progress.local.queued": "Job has been queued.",
        "progress.local.done": "Analysis completed.",
        "progress.local.done.log": "Analysis completed successfully.",
        "progress.local.failed": "Analysis failed.",
        "error.prefix": "Error",
        "warning.coverage.lower_bound": (
            "Some sections are capped (~4000 offers loaded), metrics should be treated as lower-bound."
        ),
        "warning.offers.none_query": "No relevant offers found for the current query.",
        "warning.offers.none_scope": "No offers found in the selected scan scope.",
        "warning.offers.weak_filtered": (
            "{count} offers were filtered as weak generic matches (for example only by 'rent')."
        ),
        "warning.currency.relaxed": (
            "No offers in {currency}; showing original marketplace currency offers. "
            "Potential mixed-currency offers: {count}."
        ),
        "warning.currency.filtered": (
            "{count} offers in other currencies were filtered out (requested {currency}; no conversion applied)."
        ),
        "warning.category.game_not_found": "Selected game has no sections in category catalog.",
        "warning.category.section_not_found": "Selected category section is missing or unavailable.",
        "warning.category.sections_not_found": "Selected category sections are missing or unavailable.",
        "warning.reviews.failed_sellers": (
            "Failed to fetch reviews for {count} sellers (temporary limits/proxy failures). "
            "Demand metrics may be underestimated."
        ),
        "warning.reviews.none_relevant": "Reviews were fetched, but no confirmed purchases were found.",
        "warning.reviews.no_amount": "Many reviews have no payment amount and were excluded.",
        "warning.reviews.no_game": "Some reviews do not match the game/category in current offer scope.",
        "warning.reviews.no_price": "Some reviews do not match seller offer prices.",
        "warning.playerok.safe_endpoints": "Discovered safe internal endpoints: {endpoints}",
        "warning.playerok.graphql_section_failed": (
            "Failed to fetch offers via GraphQL for section '{section}'."
        ),
        "warning.playerok.graphql_top_items_failed": (
            "Failed to fetch global PlayerOK feed via GraphQL."
        ),
        "warning.playerok.html_degrade_used": (
            "HTML degrade was used for {count} sections because GraphQL failed."
        ),
        "warning.playerok.graphql_failed_count": (
            "GraphQL failed in {count} places; check proxies/headers."
        ),
        "warning.ggsell.type_not_found": "Selected GGSell category type was not found.",
        "warning.ggsell.categories_not_found": "Failed to resolve {count} selected GGSell categories.",
        "warning.ggsell.category_failed": "Failed to process GGSell category: {section}.",
        "warning.ggsell.categories_failed_count": "GGSell category parsing failed for {count} categories.",
        "warning.plati.group_not_found": "Selected Plati.Market group was not found in catalog.",
        "warning.plati.game_not_found": "Selected Plati.Market game was not found in catalog.",
        "warning.plati.game_categories_not_found": "Some selected categories for the chosen game were not found.",
        "warning.plati.sections_not_found": "Some selected Plati.Market sections were not found in catalog.",
        "warning.plati.section_failed": "Failed to process Plati.Market section: {section}.",
        "warning.plati.sections_failed_count": "Plati.Market section parsing failed for {count} sections.",
        "warning.plati.game_failed": "Failed to process Plati.Market game: {game}.",
        "warning.plati.games_failed_count": "Plati.Market game parsing failed for {count} games.",
    },
}


def normalize_ui_locale(locale: str | None) -> str:
    if not locale:
        return "ru"
    normalized = str(locale).strip().lower()
    if normalized in SUPPORTED_UI_LOCALES:
        return normalized
    return "ru"


def tr(locale: str | None, key: str, **kwargs: Any) -> str:
    active = normalize_ui_locale(locale)
    template = MESSAGES.get(active, {}).get(key) or MESSAGES["ru"].get(key) or key
    try:
        return template.format(**kwargs)
    except Exception:
        return template
