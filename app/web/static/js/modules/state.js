/**
 * @typedef {Object} OffersLoadState
 * @property {number} loaded
 * @property {number|null} total
 * @property {boolean} partial
 * @property {boolean} maxReached
 * @property {number} maxItems
 * @property {number} pageSize
 */

/**
 * @typedef {Object} MarketplaceCatalogState
 * @property {"network"|"local_cache"|"local_cache_stale"|"default"} source
 * @property {number} count
 * @property {boolean} stale
 */

/**
 * @typedef {Object} UiLocaleState
 * @property {"ru"|"en"} locale
 * @property {string} localeTag
 */

/**
 * @param {Partial<OffersLoadState>=} raw
 * @returns {OffersLoadState}
 */
export function createOffersLoadState(raw = {}) {
  const loadedRaw = Number(raw.loaded);
  const totalRaw = Number(raw.total);
  const maxItemsRaw = Number(raw.maxItems);
  const pageSizeRaw = Number(raw.pageSize);
  const loaded = Number.isFinite(loadedRaw) && loadedRaw >= 0 ? Math.floor(loadedRaw) : 0;
  const total = Number.isFinite(totalRaw) && totalRaw >= 0 ? Math.floor(totalRaw) : null;
  const maxItems = Number.isFinite(maxItemsRaw) && maxItemsRaw > 0 ? Math.floor(maxItemsRaw) : 50000;
  const pageSize = Number.isFinite(pageSizeRaw) && pageSizeRaw > 0 ? Math.floor(pageSizeRaw) : 1000;
  const partial = Boolean(raw.partial) || (total !== null && loaded < total);
  const maxReached = Boolean(raw.maxReached) || loaded >= maxItems;
  return {
    loaded,
    total,
    partial,
    maxReached,
    maxItems,
    pageSize,
  };
}

/**
 * @param {Partial<MarketplaceCatalogState>=} raw
 * @returns {MarketplaceCatalogState}
 */
export function createMarketplaceCatalogState(raw = {}) {
  const source = raw.source || "default";
  const countRaw = Number(raw.count);
  const count = Number.isFinite(countRaw) && countRaw >= 0 ? Math.floor(countRaw) : 0;
  return {
    source,
    count,
    stale: source === "local_cache_stale",
  };
}

/**
 * @param {Partial<UiLocaleState>=} raw
 * @returns {UiLocaleState}
 */
export function createUiLocaleState(raw = {}) {
  const locale = raw.locale === "en" ? "en" : "ru";
  return {
    locale,
    localeTag: locale === "en" ? "en-US" : "ru-RU",
  };
}
