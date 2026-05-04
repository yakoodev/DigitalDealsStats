import { createOffersLoadState } from "./state.js";

/**
 * @typedef {Object} FetchAllMarketplaceOffersOptions
 * @property {string} runId
 * @property {string} marketplace
 * @property {(url: string) => Promise<any>} fetchJson
 * @property {number=} pageSize
 * @property {number=} maxItems
 * @property {(state: import("./state.js").OffersLoadState) => void=} onProgress
 */

/**
 * @param {FetchAllMarketplaceOffersOptions} options
 * @returns {Promise<{items: any[], loadState: import("./state.js").OffersLoadState}>}
 */
export async function fetchAllMarketplaceOffers(options) {
  const pageSize = Number.isFinite(Number(options.pageSize)) && Number(options.pageSize) > 0
    ? Math.floor(Number(options.pageSize))
    : 1000;
  const maxItems = Number.isFinite(Number(options.maxItems)) && Number(options.maxItems) > 0
    ? Math.floor(Number(options.maxItems))
    : 50000;
  const items = [];
  let offset = 0;
  let total = null;
  let maxReached = false;

  while (true) {
    const remaining = maxItems - items.length;
    if (remaining <= 0) {
      maxReached = true;
      break;
    }
    const limit = Math.max(1, Math.min(pageSize, remaining));
    const response = await options.fetchJson(
      `/v2/analyze/${encodeURIComponent(options.runId)}/marketplaces/${encodeURIComponent(options.marketplace)}/offers?limit=${limit}&offset=${offset}`
    );
    if (total === null) {
      const totalRaw = Number(response?.total);
      if (Number.isFinite(totalRaw) && totalRaw >= 0) {
        total = Math.floor(totalRaw);
      }
    }
    const batch = Array.isArray(response?.items) ? response.items : [];
    if (!batch.length) break;
    items.push(...batch);
    offset += batch.length;

    const progressState = createOffersLoadState({
      loaded: items.length,
      total: total !== null ? total : null,
      partial: total !== null ? items.length < total : false,
      maxReached,
      maxItems,
      pageSize,
    });
    if (typeof options.onProgress === "function") {
      options.onProgress(progressState);
    }

    if (total !== null && items.length >= total) break;
    if (batch.length < limit) break;
  }

  const resolvedTotal = total !== null ? total : items.length;
  const loadState = createOffersLoadState({
    loaded: items.length,
    total: resolvedTotal,
    partial: maxReached || (resolvedTotal > items.length),
    maxReached,
    maxItems,
    pageSize,
  });
  return { items, loadState };
}

/**
 * @param {object} args
 * @param {any[]|null|undefined} args.networkItems
 * @param {any[]|null|undefined} args.cachedItems
 * @param {any[]|null|undefined} args.defaultItems
 * @returns {{items: any[], source: "network"|"local_cache"|"default"}}
 */
export function resolveCatalogFallback({ networkItems, cachedItems, defaultItems }) {
  if (Array.isArray(networkItems) && networkItems.length > 0) {
    return { items: networkItems, source: "network" };
  }
  if (Array.isArray(cachedItems) && cachedItems.length > 0) {
    return { items: cachedItems, source: "local_cache" };
  }
  return {
    items: Array.isArray(defaultItems) ? defaultItems : [],
    source: "default",
  };
}
