import { fetchAllMarketplaceOffers } from "./api.js";
import { createOffersLoadState } from "./state.js";

/**
 * @typedef {Object} LoadMarketplaceOffersArgs
 * @property {string} runId
 * @property {string} marketplace
 * @property {(url: string) => Promise<any>} fetchJson
 * @property {number=} pageSize
 * @property {number=} maxItems
 */

/**
 * @param {LoadMarketplaceOffersArgs} args
 * @returns {Promise<{items: any[], loadState: import("./state.js").OffersLoadState}>}
 */
export async function loadMarketplaceOffers(args) {
  const response = await fetchAllMarketplaceOffers({
    runId: args.runId,
    marketplace: args.marketplace,
    fetchJson: args.fetchJson,
    pageSize: args.pageSize,
    maxItems: args.maxItems,
  });
  return {
    items: Array.isArray(response.items) ? response.items : [],
    loadState: createOffersLoadState(response.loadState),
  };
}
