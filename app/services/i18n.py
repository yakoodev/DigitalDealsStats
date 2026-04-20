from __future__ import annotations

from typing import Any

SUPPORTED_UI_LOCALES = {"ru", "en"}

MESSAGES: dict[str, dict[str, str]] = {
    "ru": {
        "validation.empty_query_requires_scope": (
            "Для пустого запроса по FunPay выберите category_game_id, category_id "
            "или непустой category_ids в marketplace_filters.funpay."
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
    },
    "en": {
        "validation.empty_query_requires_scope": (
            "For empty FunPay queries provide category_game_id, category_id "
            "or non-empty category_ids in marketplace_filters.funpay."
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
