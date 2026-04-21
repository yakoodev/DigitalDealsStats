import { createOffersLoadState } from "./state.js";

/**
 * @param {object} args
 * @param {import("./state.js").OffersLoadState|Partial<import("./state.js").OffersLoadState>} args.loadState
 * @param {"ru"|"en"} args.locale
 * @param {(value: number) => string} args.formatNum
 * @returns {string}
 */
export function formatOffersLoadSummary({ loadState, locale, formatNum }) {
  const state = createOffersLoadState(loadState);
  const loaded = formatNum(state.loaded);
  if (state.total === null) {
    return locale === "en"
      ? `Loaded ${loaded}, total is unknown`
      : `Загружено ${loaded}, общее число неизвестно`;
  }
  const total = formatNum(state.total);
  if (state.partial) {
    return locale === "en"
      ? `Loaded ${loaded} of ${total} (partial slice)`
      : `Загружено ${loaded} из ${total} (частичный срез)`;
  }
  return locale === "en"
    ? `Loaded ${loaded} of ${total}`
    : `Загружено ${loaded} из ${total}`;
}
