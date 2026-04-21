import test from "node:test";
import assert from "node:assert/strict";

import { resolveCatalogFallback } from "../api.js";

test("resolveCatalogFallback использует сеть как источник истины", () => {
  const result = resolveCatalogFallback({
    networkItems: [{ slug: "funpay" }],
    cachedItems: [{ slug: "cached" }],
    defaultItems: [{ slug: "default" }],
  });
  assert.equal(result.source, "network");
  assert.deepEqual(result.items, [{ slug: "funpay" }]);
});

test("resolveCatalogFallback переключается на кэш, если сеть недоступна", () => {
  const result = resolveCatalogFallback({
    networkItems: [],
    cachedItems: [{ slug: "playerok" }],
    defaultItems: [{ slug: "default" }],
  });
  assert.equal(result.source, "local_cache");
  assert.deepEqual(result.items, [{ slug: "playerok" }]);
});

test("resolveCatalogFallback использует дефолт, если других источников нет", () => {
  const result = resolveCatalogFallback({
    networkItems: null,
    cachedItems: null,
    defaultItems: [{ slug: "default" }],
  });
  assert.equal(result.source, "default");
  assert.deepEqual(result.items, [{ slug: "default" }]);
});
