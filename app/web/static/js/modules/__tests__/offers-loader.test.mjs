import test from "node:test";
import assert from "node:assert/strict";

import { fetchAllMarketplaceOffers } from "../api.js";

test("fetchAllMarketplaceOffers агрегирует страницы без пересечений", async () => {
  const dataset = [
    { offer_id: "1" },
    { offer_id: "2" },
    { offer_id: "3" },
    { offer_id: "4" },
    { offer_id: "5" },
    { offer_id: "6" },
  ];
  const fetchJson = async (url) => {
    const parsed = new URL(`http://localhost${url}`);
    const limit = Number(parsed.searchParams.get("limit"));
    const offset = Number(parsed.searchParams.get("offset"));
    return {
      total: dataset.length,
      items: dataset.slice(offset, offset + limit),
    };
  };

  const { items, loadState } = await fetchAllMarketplaceOffers({
    runId: "run",
    marketplace: "funpay",
    fetchJson,
    pageSize: 2,
    maxItems: 10,
  });

  assert.deepEqual(items.map((item) => item.offer_id), ["1", "2", "3", "4", "5", "6"]);
  assert.equal(loadState.loaded, 6);
  assert.equal(loadState.total, 6);
  assert.equal(loadState.partial, false);
});

test("fetchAllMarketplaceOffers помечает частичную выборку при защитном лимите", async () => {
  const fetchJson = async (url) => {
    const parsed = new URL(`http://localhost${url}`);
    const limit = Number(parsed.searchParams.get("limit"));
    const offset = Number(parsed.searchParams.get("offset"));
    const data = Array.from({ length: 100 }, (_, index) => ({ offer_id: String(index + 1) }));
    return {
      total: data.length,
      items: data.slice(offset, offset + limit),
    };
  };

  const { loadState } = await fetchAllMarketplaceOffers({
    runId: "run",
    marketplace: "funpay",
    fetchJson,
    pageSize: 30,
    maxItems: 50,
  });

  assert.equal(loadState.loaded, 50);
  assert.equal(loadState.total, 100);
  assert.equal(loadState.partial, true);
});
