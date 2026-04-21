    const $ = (id) => document.getElementById(id);
    const statusBanner = $("statusBanner");
    const runIdInput = $("runIdInput");
    const lastUpdateChip = $("lastUpdateChip");
    const tooltip = $("tooltip");
    const progressFill = $("progressFill");
    const progressStage = $("progressStage");
    const progressPercent = $("progressPercent");
    const progressMessage = $("progressMessage");
    const progressTable = $("progressTable");
    const overviewPage = $("overviewPage");
    const marketplacePage = $("marketplacePage");
    const historyHost = $("historyHost");
    const workspaceResults = $("workspaceResults");
    const workspaceTrace = $("workspaceTrace");
    const workspaceHistory = $("workspaceHistory");
    const workspaceGuide = $("workspaceGuide");

    let pollTimer = null;
    let selectedRunId = null;
    let marketplacesCatalog = [];
    let categoriesCatalog = [];
    let playerokCategoriesCatalog = [];
    let ggsellTypesCatalog = [];
    let ggsellCategoriesCatalog = [];
    let platiCatalogTree = [];
    let platiGamesCatalog = [];
    let platiGameCategoriesCatalog = [];
    let funpayCatalogLoaded = false;
    let playerokCatalogLoaded = false;
    let ggsellCatalogLoaded = false;
    let platiCatalogTreeLoaded = false;
    let platiGamesLoaded = false;
    let allowDirectFallbackSession = sessionStorage.getItem("marketstat_allow_direct_fallback") === "1";
    let currentMarketplaceResult = null;
    let fullOffers = [];
    let filteredOffers = [];
    let offersLoadState = null;
    let marketAnalyticsOffers = [];
    let sellerFocusPoint = null;
    let selectedSectionIds = new Set();
    let selectedPlayerOkSectionSlugs = new Set();
    let selectedGgSellCategorySlugs = new Set();
    let selectedPlatiCategoryIds = new Set();
    let selectedPlatiGameCategoryIds = new Set();
    let platiTreeExpandedIds = new Set();
    let platiScopeMode = "catalog";
    let offersPage = 0;
    let historyLoadedOnce = false;
    let activeWorkspaceTab = localStorage.getItem("marketstat_v2_workspace_tab") || "results";
    let activeConfigMarketplace = localStorage.getItem("marketstat_v2_active_config_marketplace") || "funpay";
    let isCatalogLoading = false;

    const CATALOG_CACHE_TTL_MS = 24 * 60 * 60 * 1000;
    const OFFERS_FETCH_PAGE_SIZE = 1000;
    const OFFERS_FETCH_MAX_ITEMS = 50000;
    const LOCAL_CACHE_PREFIX = "marketstat_v2_cache:";
    const DEFAULT_MARKETPLACES = [
      {
        slug: "funpay",
        label: "FunPay",
        enabled: true,
        reason: null,
        capabilities: [],
        data_source: null,
        demand_mode: null,
      },
      {
        slug: "playerok",
        label: "PlayerOK",
        enabled: true,
        reason: null,
        capabilities: [],
        data_source: null,
        demand_mode: null,
      },
      {
        slug: "ggsell",
        label: "GGSell",
        enabled: true,
        reason: null,
        capabilities: [],
        data_source: null,
        demand_mode: null,
      },
      {
        slug: "platimarket",
        label: "Plati.Market",
        enabled: true,
        reason: null,
        capabilities: [],
        data_source: null,
        demand_mode: null,
      },
    ];
    marketplacesCatalog = [...DEFAULT_MARKETPLACES];

    const routePath = window.location.pathname;
    const isMarketplaceRoute = routePath.startsWith("/analysis/");
    const activeMarketplace = isMarketplaceRoute ? routePath.split("/")[2] : null;
    const isFunPayRoute = isMarketplaceRoute && activeMarketplace === "funpay";
    const isPlayerOkRoute = isMarketplaceRoute && activeMarketplace === "playerok";
    const isGgSellRoute = isMarketplaceRoute && activeMarketplace === "ggsell";
    const isPlatiRoute = isMarketplaceRoute && activeMarketplace === "platimarket";

    const chartState = {
      price: { bins: [], bars: [], hoverIndex: -1, canvas: $("marketPriceCanvas") },
      history: { points: [], coords: [], hoverIndex: -1, canvas: $("marketHistoryCanvas") },
      sellers: { points: [], canvas: $("sellerCompetitionCanvas"), hoverIndex: -1 },
      coverage: { bars: [], canvas: $("coverageCanvas"), hoverIndex: -1 },
    };

    const I18N = {
      ru: {
        statusWaiting: "Ожидание запуска",
        stagePrefix: "Этап",
        noData: "Нет данных",
        noHistory: "Недостаточно истории",
        runDone: "Анализ завершен.",
        runStart: "Запускаю анализ...",
        selectMarketplace: "Выберите хотя бы одну площадку.",
        runIdRequired: "Укажите run_id",
        copyRunIdRequired: "Сначала укажите run_id.",
        copiedRunId: "run_id скопирован в буфер обмена.",
        csvNoData: "Нет данных для экспорта CSV. Сначала примените фильтры к срезу офферов.",
        csvDone: "CSV экспортирован: {count} строк.",
        errorPrefix: "Ошибка",
        unknown: "неизвестно",
        yes: "да",
        no: "нет",
        openOverview: "Открыть overview",
        openMarketplace: "Открыть площадку",
        emptyQueryLabel: "пустой запрос",
        historyEmpty: "История пока пустая.",
        historyNoFilter: "По выбранным фильтрам записи не найдены.",
        historyLoaded: "Показано запусков: {shown} из {total}",
        offersLoadedFull: "Загружено {loaded} из {total}",
        offersLoadedPartial: "Загружено {loaded} из {total} (частичный срез)",
        offersPartialNotice: "Показаны не все офферы: {loaded} из {total}. Уточните фильтры или повторите загрузку.",
        offersLoadedUnknownTotal: "Загружено {loaded}, общее число неизвестно",
        csvPartialWarning: "CSV создан по частичному срезу: {loaded} из {total}.",
      },
      en: {
        statusWaiting: "Waiting for run",
        stagePrefix: "Stage",
        noData: "No data",
        noHistory: "Not enough history",
        runDone: "Analysis completed.",
        runStart: "Starting analysis...",
        selectMarketplace: "Select at least one marketplace.",
        runIdRequired: "Provide run_id",
        copyRunIdRequired: "Provide run_id first.",
        copiedRunId: "run_id copied to clipboard.",
        csvNoData: "No data for CSV export. Apply offers filters first.",
        csvDone: "CSV exported: {count} rows.",
        errorPrefix: "Error",
        unknown: "unknown",
        yes: "yes",
        no: "no",
        openOverview: "Open overview",
        openMarketplace: "Open marketplace",
        emptyQueryLabel: "empty query",
        historyEmpty: "History is empty.",
        historyNoFilter: "No records for selected filters.",
        historyLoaded: "Runs shown: {shown} of {total}",
        offersLoadedFull: "Loaded {loaded} of {total}",
        offersLoadedPartial: "Loaded {loaded} of {total} (partial slice)",
        offersPartialNotice: "Only part of offers is loaded: {loaded} of {total}. Narrow filters or reload.",
        offersLoadedUnknownTotal: "Loaded {loaded}, total is unknown",
        csvPartialWarning: "CSV was built from partial slice: {loaded} of {total}.",
      },
    };

    let currentUiLocale = $("uiLocale").value || "ru";
    const savedPlatiScopeMode = localStorage.getItem("marketstat_v2_plati_scope_mode");
    if (savedPlatiScopeMode === "catalog" || savedPlatiScopeMode === "game") {
      platiScopeMode = savedPlatiScopeMode;
      const scopeSelect = $("pmScopeMode");
      if (scopeSelect) scopeSelect.value = savedPlatiScopeMode;
    }

    function t(key, vars = {}) {
      const dict = I18N[currentUiLocale] || I18N.ru;
      let template = dict[key] || I18N.ru[key] || key;
      for (const [name, value] of Object.entries(vars)) {
        template = template.replaceAll(`{${name}}`, String(value));
      }
      return template;
    }

    function localeTag() {
      return currentUiLocale === "en" ? "en-US" : "ru-RU";
    }

    function createOffersLoadState(raw = null) {
      const loadedRaw = Number(raw?.loaded);
      const totalRaw = Number(raw?.total);
      const loaded = Number.isFinite(loadedRaw) && loadedRaw >= 0 ? Math.floor(loadedRaw) : 0;
      const hasTotal = Number.isFinite(totalRaw) && totalRaw >= 0;
      const total = hasTotal ? Math.floor(totalRaw) : null;
      const maxItemsRaw = Number(raw?.maxItems);
      const pageSizeRaw = Number(raw?.pageSize);
      const maxItems = Number.isFinite(maxItemsRaw) && maxItemsRaw > 0
        ? Math.floor(maxItemsRaw)
        : OFFERS_FETCH_MAX_ITEMS;
      const pageSize = Number.isFinite(pageSizeRaw) && pageSizeRaw > 0
        ? Math.floor(pageSizeRaw)
        : OFFERS_FETCH_PAGE_SIZE;
      const partial = Boolean(raw?.partial) || (total !== null && loaded < total);
      const maxReached = Boolean(raw?.maxReached) || loaded >= maxItems;
      return {
        loaded,
        total,
        partial,
        maxReached,
        maxItems,
        pageSize,
      };
    }

    function getOffersLoadState() {
      if (!offersLoadState) {
        offersLoadState = createOffersLoadState({
          loaded: Array.isArray(fullOffers) ? fullOffers.length : 0,
          total: Array.isArray(fullOffers) ? fullOffers.length : 0,
        });
      }
      return offersLoadState;
    }

    function setOffersLoadState(raw = null) {
      offersLoadState = createOffersLoadState(raw);
      return offersLoadState;
    }

    function offersLoadSummaryText() {
      const state = getOffersLoadState();
      const moduleFormatter = window.MarketStatModules?.render?.formatOffersLoadSummary;
      if (typeof moduleFormatter === "function") {
        return moduleFormatter({
          loadState: state,
          locale: currentUiLocale === "en" ? "en" : "ru",
          formatNum,
        });
      }
      const loaded = formatNum(state.loaded);
      if (state.total === null) {
        return t("offersLoadedUnknownTotal", { loaded });
      }
      const total = formatNum(state.total);
      if (state.partial) {
        return t("offersLoadedPartial", { loaded, total });
      }
      return t("offersLoadedFull", { loaded, total });
    }

    function safeUrl(url) {
      if (!url) return null;
      try {
        const parsed = new URL(url, window.location.origin);
        if (parsed.protocol === "http:" || parsed.protocol === "https:") return parsed.toString();
      } catch (_) {
        return null;
      }
      return null;
    }

    function sellerProfileUrl(marketplace, sellerId, sellerUrl = null) {
      if (sellerUrl) return safeUrl(sellerUrl);
      const sid = String(sellerId || "").trim();
      if (!sid) return null;
      if (marketplace === "playerok") {
        return safeUrl(`https://playerok.com/profile/${encodeURIComponent(sid)}/products`);
      }
      if (marketplace === "ggsell") {
        return safeUrl(`https://ggsel.net/sellers/${encodeURIComponent(sid)}`);
      }
      if (marketplace === "platimarket") {
        return safeUrl(`https://plati.market/seller/${encodeURIComponent(sid)}/`);
      }
      return safeUrl(`https://funpay.com/users/${encodeURIComponent(sid)}/`);
    }

    function escapeHtml(value) {
      return String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll("\"", "&quot;")
        .replaceAll("'", "&#39;");
    }

    function formatNum(value) {
      if (value === null || value === undefined) return "—";
      const number = Number(value);
      if (!Number.isFinite(number)) return "—";
      return number.toLocaleString(localeTag());
    }

    function currencyMark(raw) {
      const value = String(raw ?? "").toUpperCase();
      if (value === "RUB" || raw === "₽") return "₽";
      if (value === "USD" || raw === "$") return "$";
      if (value === "EUR" || raw === "€") return "€";
      return raw ?? "";
    }

    function formatMoney(value, currency = "RUB") {
      if (value === null || value === undefined) return "—";
      const number = Number(value);
      if (!Number.isFinite(number)) return "—";
      const mark = currencyMark(currency);
      const base = number.toLocaleString(localeTag(), { maximumFractionDigits: 2, minimumFractionDigits: 0 });
      return mark ? `${base} ${mark}` : base;
    }

    function formatPct(value) {
      if (value === null || value === undefined) return "—";
      const number = Number(value);
      if (!Number.isFinite(number)) return "—";
      return `${(number * 100).toFixed(1)}%`;
    }

    function repairCyrillicMojibake(value) {
      const input = String(value ?? "");
      if (!input) return input;
      if (!/[ÐÑРС]/.test(input)) return input;
      try {
        const bytes = Uint8Array.from(Array.from(input, (ch) => ch.charCodeAt(0) & 0xff));
        const decoded = new TextDecoder("utf-8", { fatal: false }).decode(bytes);
        if (/[А-Яа-яЁё]/.test(decoded)) return decoded;
      } catch (_) {
        return input;
      }
      return input;
    }

    function formatHistoryQuery(value) {
      const repaired = repairCyrillicMojibake(value);
      const trimmed = String(repaired || "").trim();
      if (!trimmed) return t("emptyQueryLabel");
      const questionMarks = (trimmed.match(/\?/g) || []).length;
      if (questionMarks >= 3 && !/[А-Яа-яЁё]/.test(trimmed)) {
        const prefix = currentUiLocale === "en" ? "[Encoding error]" : "[Ошибка кодировки]";
        return `${prefix} ${trimmed}`;
      }
      return repaired;
    }

    function localCacheRead(key, maxAgeMs = CATALOG_CACHE_TTL_MS) {
      try {
        const raw = localStorage.getItem(`${LOCAL_CACHE_PREFIX}${key}`);
        if (!raw) return null;
        const parsed = JSON.parse(raw);
        const savedAt = Number(parsed?.saved_at || 0);
        if (!Number.isFinite(savedAt) || savedAt <= 0) return null;
        if (Date.now() - savedAt > maxAgeMs) return null;
        return parsed?.data ?? null;
      } catch (_) {
        return null;
      }
    }

    function localCacheWrite(key, data) {
      try {
        const payload = {
          saved_at: Date.now(),
          data,
        };
        localStorage.setItem(`${LOCAL_CACHE_PREFIX}${key}`, JSON.stringify(payload));
      } catch (_) {
        // localStorage quota may be exceeded on some devices
      }
    }

    function localCacheDrop(key) {
      try {
        localStorage.removeItem(`${LOCAL_CACHE_PREFIX}${key}`);
      } catch (_) {
        // ignore cache drop failures
      }
    }

    function renderCatalogLoadStatus(text, mode = "") {
      const node = $("catalogLoadStatus");
      if (!node) return;
      node.textContent = text;
      node.className = `chip-inline ${mode}`.trim();
    }

    function setWorkspaceTab(tab, { save = true, autoLoad = true } = {}) {
      const resolved = (tab === "history" || tab === "guide" || tab === "trace") ? tab : "results";
      activeWorkspaceTab = resolved;
      if (save) localStorage.setItem("marketstat_v2_workspace_tab", resolved);
      workspaceResults.classList.toggle("hidden", resolved !== "results");
      workspaceTrace.classList.toggle("hidden", resolved !== "trace");
      workspaceHistory.classList.toggle("hidden", resolved !== "history");
      workspaceGuide.classList.toggle("hidden", resolved !== "guide");
      $("workspaceResultsBtn").classList.toggle("active", resolved === "results");
      $("workspaceTraceBtn").classList.toggle("active", resolved === "trace");
      $("workspaceHistoryBtn").classList.toggle("active", resolved === "history");
      $("workspaceGuideBtn").classList.toggle("active", resolved === "guide");
      if (resolved === "history" && autoLoad && !historyLoadedOnce) {
        renderHistory()
          .then(() => {
            historyLoadedOnce = true;
          })
          .catch(() => null);
      }
    }

    function readInt(id) {
      const raw = $(id).value.trim();
      if (!raw) return null;
      const number = Number(raw);
      return Number.isFinite(number) ? Math.floor(number) : null;
    }

    function readFloat(id) {
      const raw = $(id).value.trim();
      if (!raw) return null;
      const number = Number(raw);
      return Number.isFinite(number) ? number : null;
    }

    function syncSectionSelectionFromDom() {
      const checklist = $("fpSectionChecklist");
      if (!checklist) return;
      const boxes = Array.from(checklist.querySelectorAll("input[type='checkbox'][data-section-id]"));
      for (const box of boxes) {
        const id = Number(box.getAttribute("data-section-id"));
        if (!Number.isFinite(id) || id <= 0) continue;
        if (box.checked) selectedSectionIds.add(Math.floor(id));
        else selectedSectionIds.delete(Math.floor(id));
      }
    }

    function readSelectedSectionIds() {
      syncSectionSelectionFromDom();
      return [...selectedSectionIds].sort((a, b) => a - b);
    }

    function updateSectionSelectedCount() {
      const node = $("fpSectionSelectedCount");
      if (!node) return;
      const count = selectedSectionIds.size;
      node.textContent = currentUiLocale === "en" ? `Selected: ${count}` : `Выбрано: ${count}`;
    }

    function syncPlayerOkSectionSelectionFromDom() {
      const checklist = $("pkSectionChecklist");
      if (!checklist) return;
      const boxes = Array.from(checklist.querySelectorAll("input[type='checkbox'][data-section-slug]"));
      for (const box of boxes) {
        const slug = String(box.getAttribute("data-section-slug") || "").trim();
        if (!slug) continue;
        if (box.checked) selectedPlayerOkSectionSlugs.add(slug);
        else selectedPlayerOkSectionSlugs.delete(slug);
      }
    }

    function readSelectedPlayerOkSectionSlugs() {
      syncPlayerOkSectionSelectionFromDom();
      return [...selectedPlayerOkSectionSlugs].sort((left, right) => left.localeCompare(right));
    }

    function updatePlayerOkSectionSelectedCount() {
      const node = $("pkSectionSelectedCount");
      if (!node) return;
      const count = selectedPlayerOkSectionSlugs.size;
      node.textContent = currentUiLocale === "en" ? `Selected: ${count}` : `Выбрано: ${count}`;
    }

    function syncGgSellCategorySelectionFromDom() {
      const checklist = $("gsCategoryChecklist");
      if (!checklist) return;
      const boxes = Array.from(checklist.querySelectorAll("input[type='checkbox'][data-category-slug]"));
      for (const box of boxes) {
        const slug = String(box.getAttribute("data-category-slug") || "").trim();
        if (!slug) continue;
        if (box.checked) selectedGgSellCategorySlugs.add(slug);
        else selectedGgSellCategorySlugs.delete(slug);
      }
    }

    function readSelectedGgSellCategorySlugs() {
      syncGgSellCategorySelectionFromDom();
      return [...selectedGgSellCategorySlugs].sort((left, right) => left.localeCompare(right));
    }

    function updateGgSellCategorySelectedCount() {
      const node = $("gsCategorySelectedCount");
      if (!node) return;
      const count = selectedGgSellCategorySlugs.size;
      node.textContent = currentUiLocale === "en" ? `Selected: ${count}` : `Выбрано: ${count}`;
    }

    function syncPlatiSectionSelectionFromDom() {
      const checklist = $("pmSectionChecklist");
      if (!checklist) return;
      const boxes = Array.from(checklist.querySelectorAll("input[type='checkbox'][data-section-id]"));
      for (const box of boxes) {
        const id = Number(box.getAttribute("data-section-id"));
        if (!Number.isFinite(id) || id <= 0) continue;
        if (box.checked) selectedPlatiCategoryIds.add(Math.floor(id));
        else selectedPlatiCategoryIds.delete(Math.floor(id));
      }
    }

    function togglePlatiCategoryCascade(sectionId, checked) {
      const id = Number(sectionId);
      if (!Number.isFinite(id) || id <= 0) return;
      const { childrenById } = getPlatiTreeIndex();
      const subtree = collectPlatiDescendants(Math.floor(id), childrenById, new Set());
      for (const nodeId of subtree) {
        if (checked) selectedPlatiCategoryIds.add(nodeId);
        else selectedPlatiCategoryIds.delete(nodeId);
      }
    }

    function readSelectedPlatiCategoryIds() {
      syncPlatiSectionSelectionFromDom();
      return [...selectedPlatiCategoryIds].sort((a, b) => a - b);
    }

    function updatePlatiSectionSelectedCount() {
      const node = $("pmSectionSelectedCount");
      if (!node) return;
      const count = selectedPlatiCategoryIds.size;
      node.textContent = currentUiLocale === "en" ? `Selected: ${count}` : `Выбрано: ${count}`;
    }

    function syncPlatiGameCategorySelectionFromDom() {
      const checklist = $("pmGameCategoryChecklist");
      if (!checklist) return;
      const boxes = Array.from(checklist.querySelectorAll("input[type='checkbox'][data-game-category-id]"));
      for (const box of boxes) {
        const id = Number(box.getAttribute("data-game-category-id"));
        if (!Number.isFinite(id) || id < 0) continue;
        if (box.checked) selectedPlatiGameCategoryIds.add(Math.floor(id));
        else selectedPlatiGameCategoryIds.delete(Math.floor(id));
      }
    }

    function readSelectedPlatiGameCategoryIds() {
      syncPlatiGameCategorySelectionFromDom();
      return [...selectedPlatiGameCategoryIds]
        .filter((id) => Number.isFinite(id) && id >= 0)
        .sort((a, b) => a - b);
    }

    function updatePlatiGameCategorySelectedCount() {
      const node = $("pmGameCategorySelectedCount");
      if (!node) return;
      const count = selectedPlatiGameCategoryIds.size;
      node.textContent = currentUiLocale === "en" ? `Selected: ${count}` : `Выбрано: ${count}`;
    }

    function parseLines(id) {
      return $(id).value
        .split(/\r?\n/)
        .map((line) => line.trim())
        .filter((line) => line.length > 0);
    }

    function parseLooseObject(id, separator = ":") {
      const raw = String($(id).value || "").trim();
      if (!raw) return {};
      if (raw.startsWith("{")) {
        try {
          const parsed = JSON.parse(raw);
          if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
            const cleaned = {};
            for (const [key, value] of Object.entries(parsed)) {
              const k = String(key || "").trim();
              const v = String(value ?? "").trim();
              if (k && v) cleaned[k] = v;
            }
            return cleaned;
          }
        } catch (_) {
          return {};
        }
        return {};
      }
      const result = {};
      for (const line of raw.split(/\r?\n/)) {
        const value = String(line || "").trim();
        if (!value) continue;
        const idx = value.indexOf(separator);
        if (idx <= 0) continue;
        const key = value.slice(0, idx).trim();
        const val = value.slice(idx + 1).trim();
        if (key && val) result[key] = val;
      }
      return result;
    }

    function parsePlatiGameOverride(rawValue) {
      const raw = String(rawValue || "").trim();
      if (!raw) return { gameId: null, gameSlug: null };
      const cleaned = raw.replace(/^https?:\/\/(www\.)?plati\.market/i, "").trim();
      const fromPath = cleaned.match(/\/?games\/([^/?#]+)(?:\/(\d+))?/i);
      if (fromPath) {
        return {
          gameId: fromPath[2] ? Number(fromPath[2]) : null,
          gameSlug: fromPath[1] ? String(fromPath[1]).trim().toLowerCase() : null,
        };
      }
      if (/^\d+$/.test(cleaned)) {
        return { gameId: Number(cleaned), gameSlug: null };
      }
      const slugAndId = cleaned.match(/^([^/\s?#]+)\/(\d+)$/i);
      if (slugAndId) {
        return {
          gameId: Number(slugAndId[2]),
          gameSlug: String(slugAndId[1]).trim().toLowerCase(),
        };
      }
      return {
        gameId: null,
        gameSlug: String(cleaned).replace(/^\/+|\/+$/g, "").toLowerCase() || null,
      };
    }

    function getPlatiScopeMode() {
      const raw = String($("pmScopeMode")?.value || "catalog").trim().toLowerCase();
      return raw === "game" ? "game" : "catalog";
    }

    async function setPlatiScopeMode(mode, { ensureData = true } = {}) {
      platiScopeMode = mode === "game" ? "game" : "catalog";
      localStorage.setItem("marketstat_v2_plati_scope_mode", platiScopeMode);
      const modeSelect = $("pmScopeMode");
      if (modeSelect && modeSelect.value !== platiScopeMode) {
        modeSelect.value = platiScopeMode;
      }
      $("pmCatalogScopeBlock").classList.toggle("hidden", platiScopeMode !== "catalog");
      $("pmGameScopeBlock").classList.toggle("hidden", platiScopeMode !== "game");
      const scopeHint = $("pmScopeHint");
      if (scopeHint) {
        if (platiScopeMode === "catalog") {
          scopeHint.textContent = currentUiLocale === "en"
            ? "Step 2: select catalog sections (ID_R)."
            : "Шаг 2: выберите разделы каталога (ID_R).";
        } else {
          scopeHint.textContent = currentUiLocale === "en"
            ? "Step 2: select game and game categories (ID_C)."
            : "Шаг 2: выберите игру и категории игры (ID_C).";
        }
      }
      if (platiScopeMode === "game" && ensureData) {
        if (!platiGamesLoaded) {
          await loadPlatiGames();
        }
        await loadPlatiGameCategories();
      }
    }

    function setPlayerOkTab(tab) {
      const isSettings = tab === "settings";
      $("pkMainTab").classList.toggle("hidden", isSettings);
      $("pkSettingsTab").classList.toggle("hidden", !isSettings);
      $("pkTabMainBtn").classList.toggle("active", !isSettings);
      $("pkTabSettingsBtn").classList.toggle("active", isSettings);
    }

    function syncPlayerOkAdvancedState() {
      const enabled = Boolean($("pkAdvancedMode").checked);
      $("pkAdvancedHeaders").disabled = !enabled;
      $("pkAdvancedCookies").disabled = !enabled;
      if (!enabled) {
        $("pkAdvancedHeaders").value = "";
        $("pkAdvancedCookies").value = "";
      }
    }

    function applyUiLocale() {
      currentUiLocale = $("uiLocale").value || "ru";
      localStorage.setItem("marketstat_v2_ui_locale", currentUiLocale);
      document.documentElement.lang = currentUiLocale;
      document.title = currentUiLocale === "en" ? "MarketStat v2" : "MarketStat v2";
      const subtitle = document.querySelector(".subtitle");
      if (subtitle) {
        subtitle.textContent = currentUiLocale === "en"
          ? "Multi-marketplace analytics: overview + marketplace details."
          : "Мульти-площадочная аналитика: общий обзор + детализация по площадкам.";
      }
      const navHead = $("navHead");
      const step2Head = $("step2Head");
      const step3Head = $("step3Head");
      const step4Head = $("step4Head");
      const workspaceHead = $("workspaceHead");
      const statusPanelHead = $("statusPanelHead");
      const offersExplorerHead = $("offersExplorerHead");
      const runBtn = $("runBtn");
      const refreshBtn = $("refreshBtn");
      const topSwaggerLink = $("topSwaggerLink");
      if (navHead) navHead.textContent = currentUiLocale === "en" ? "Navigation" : "Навигация";
      if (step2Head) step2Head.textContent = currentUiLocale === "en" ? "Step 2. Marketplaces and catalogs" : "Шаг 2. Площадки и каталоги";
      if (step3Head) step3Head.textContent = currentUiLocale === "en" ? "Step 3. Scope and marketplace filters" : "Шаг 3. Область и фильтры площадок";
      if (step4Head) step4Head.textContent = currentUiLocale === "en" ? "Step 4. Advanced options" : "Шаг 4. Дополнительно";
      if (workspaceHead) workspaceHead.textContent = currentUiLocale === "en" ? "Workspace" : "Рабочая зона";
      if (statusPanelHead) statusPanelHead.textContent = currentUiLocale === "en" ? "Run status" : "Статус запуска";
      if (offersExplorerHead) offersExplorerHead.textContent = currentUiLocale === "en" ? "Offers slice" : "Срез офферов площадки";
      if (refreshBtn) refreshBtn.textContent = currentUiLocale === "en" ? "Refresh status" : "Обновить статус";
      if (topSwaggerLink) topSwaggerLink.textContent = "Swagger";
      if (!isMarketplaceRoute && runBtn) {
        runBtn.textContent = currentUiLocale === "en" ? "Start analysis" : "Запустить общий анализ";
      }
      const navOverview = $("navOverview");
      const navFunPay = $("navFunPay");
      const navPlayerOk = $("navPlayerOk");
      const navGgSell = $("navGgSell");
      const navPlati = $("navPlati");
      if (navOverview) navOverview.textContent = currentUiLocale === "en" ? "Overview" : "Общий анализ";
      if (navFunPay) navFunPay.textContent = "FunPay";
      if (navPlayerOk) navPlayerOk.textContent = "PlayerOK";
      if (navGgSell) navGgSell.textContent = "GGSell";
      if (navPlati) navPlati.textContent = "Plati.Market";
      const labels = [
        ["label[for='fpSellerLimit']", currentUiLocale === "en" ? "Top sellers for reviews analysis" : "Топ-продавцов для анализа отзывов"],
        ["label[for='pkSellerLimit']", currentUiLocale === "en" ? "Top sellers for reviews analysis" : "Топ-продавцов для анализа отзывов"],
        ["label[for='gsSellerLimit']", currentUiLocale === "en" ? "Top sellers for reviews analysis" : "Топ-продавцов для анализа отзывов"],
        ["label[for='pmSellerLimit']", currentUiLocale === "en" ? "Top sellers for reviews analysis" : "Топ-продавцов для анализа отзывов"],
        ["label[for='uiLocale']", currentUiLocale === "en" ? "UI locale" : "Язык интерфейса"],
        ["label[for='fpSectionSearch']", currentUiLocale === "en" ? "Category sections (checkboxes)" : "Разделы категории (чекбоксы)"],
        ["label[for='pkSectionSearch']", currentUiLocale === "en" ? "PlayerOK sections (checkboxes)" : "Разделы PlayerOK (чекбоксы)"],
        ["label[for='gsType']", currentUiLocale === "en" ? "Catalog type" : "Тип каталога"],
        ["label[for='gsUseTypeScope']", currentUiLocale === "en" ? "Type scope" : "Область типа"],
        ["label[for='gsCategorySearch']", currentUiLocale === "en" ? "GGSell categories (checkboxes)" : "Категории GGSell (чекбоксы)"],
        ["label[for='gsCategoriesCustom']", currentUiLocale === "en" ? "Category slugs (manual input)" : "Категории (slug, ручной ввод)"],
        ["label[for='gsReviewPages']", currentUiLocale === "en" ? "Offers per seller for reviews" : "Товаров на продавца для отзывов"],
        ["label[for='pmScopeMode']", currentUiLocale === "en" ? "Scope source" : "Источник области анализа"],
        ["label[for='pmSectionSearch']", currentUiLocale === "en" ? "Plati catalog tree (ID_R)" : "Дерево каталога Plati (ID_R)"],
        ["label[for='pmGame']", currentUiLocale === "en" ? "Plati.Market game" : "Игра Plati.Market"],
        ["label[for='pmGameCustom']", currentUiLocale === "en" ? "Game (slug / URL / ID, manual)" : "Игра (slug / URL / ID, вручную)"],
        ["label[for='pmGameCategorySearch']", currentUiLocale === "en" ? "Selected game categories (ID_C)" : "Категории выбранной игры (ID_C)"],
        ["label[for='pkGameCustom']", currentUiLocale === "en" ? "Game slug (manual input)" : "Игра (slug, ручной ввод)"],
        ["label[for='pkSectionsCustom']", currentUiLocale === "en" ? "Section slugs (manual input)" : "Разделы (slug, ручной ввод)"],
        ["label[for='pkAdvancedHeaders']", currentUiLocale === "en" ? "Extra headers (JSON or `Header: Value` lines)" : "Доп. заголовки (JSON или `Header: Value` по строкам)"],
        ["label[for='pkAdvancedCookies']", currentUiLocale === "en" ? "Cookie override (JSON or `name=value` lines)" : "Cookie override (JSON или `name=value` по строкам)"],
      ];
      for (const [selector, text] of labels) {
        const node = document.querySelector(selector);
        if (node) node.textContent = text;
      }
      const pkMainBtn = $("pkTabMainBtn");
      const pkSettingsBtn = $("pkTabSettingsBtn");
      if (pkMainBtn) pkMainBtn.textContent = currentUiLocale === "en" ? "Filters" : "Фильтры";
      if (pkSettingsBtn) pkSettingsBtn.textContent = currentUiLocale === "en" ? "Settings" : "Настройки";
      const pmGameHint = $("pmGameHint");
      if (pmGameHint) {
        pmGameHint.textContent = currentUiLocale === "en"
          ? "Use manual slug/URL/ID only if game is missing in dropdown."
          : "Используйте ручной slug/URL/ID только если игра не находится в списке.";
      }
      const pmScopeModeSelect = $("pmScopeMode");
      if (pmScopeModeSelect) {
        const catalogOption = pmScopeModeSelect.querySelector("option[value='catalog']");
        const gameOption = pmScopeModeSelect.querySelector("option[value='game']");
        if (catalogOption) {
          catalogOption.textContent = currentUiLocale === "en" ? "Catalog (ID_R)" : "Каталог (ID_R)";
        }
        if (gameOption) {
          gameOption.textContent = currentUiLocale === "en" ? "Game (ID_C)" : "Игра (ID_C)";
        }
      }
      const pmGameAdvancedSummary = document.querySelector("#pmGameAdvanced > summary");
      if (pmGameAdvancedSummary) {
        pmGameAdvancedSummary.textContent = currentUiLocale === "en"
          ? "Advanced: game slug / URL / ID"
          : "Advanced: slug / URL / ID игры";
      }
      const pmScopeHint = $("pmScopeHint");
      if (pmScopeHint) {
        pmScopeHint.textContent = currentUiLocale === "en"
          ? "Step 1: choose scope source."
          : "Шаг 1: выберите источник области анализа.";
      }
      const pmSectionSelectVisible = $("pmSectionSelectVisible");
      const pmSectionClearVisible = $("pmSectionClearVisible");
      const gsCategorySelectVisible = $("gsCategorySelectVisible");
      const gsCategoryClearVisible = $("gsCategoryClearVisible");
      if (gsCategorySelectVisible) {
        gsCategorySelectVisible.textContent = currentUiLocale === "en" ? "Select visible" : "Отметить видимые";
      }
      if (gsCategoryClearVisible) {
        gsCategoryClearVisible.textContent = currentUiLocale === "en" ? "Clear visible" : "Снять видимые";
      }
      if (pmSectionSelectVisible) {
        pmSectionSelectVisible.textContent = currentUiLocale === "en" ? "Select visible" : "Отметить видимые";
      }
      if (pmSectionClearVisible) {
        pmSectionClearVisible.textContent = currentUiLocale === "en" ? "Clear visible" : "Снять видимые";
      }
      const pmGameCategorySelectVisible = $("pmGameCategorySelectVisible");
      const pmGameCategoryClearVisible = $("pmGameCategoryClearVisible");
      if (pmGameCategorySelectVisible) {
        pmGameCategorySelectVisible.textContent = currentUiLocale === "en" ? "Select visible" : "Отметить видимые";
      }
      if (pmGameCategoryClearVisible) {
        pmGameCategoryClearVisible.textContent = currentUiLocale === "en" ? "Clear visible" : "Снять видимые";
      }
      const saveNetworkBtn = $("saveNetworkSettingsBtn");
      const reloadNetworkBtn = $("reloadNetworkSettingsBtn");
      const loadCatalogsBtn = $("loadCatalogsBtn");
      const refreshCatalogsBtn = $("refreshCatalogsBtn");
      if (saveNetworkBtn) {
        saveNetworkBtn.textContent = currentUiLocale === "en" ? "Save network settings" : "Сохранить сетевые настройки";
      }
      if (reloadNetworkBtn) {
        reloadNetworkBtn.textContent = currentUiLocale === "en" ? "Reload from DB" : "Перезагрузить из БД";
      }
      if (loadCatalogsBtn) {
        loadCatalogsBtn.textContent = currentUiLocale === "en"
          ? "Load catalogs for selected marketplaces"
          : "Загрузить каталоги выбранных площадок";
      }
      if (refreshCatalogsBtn) {
        refreshCatalogsBtn.textContent = currentUiLocale === "en"
          ? "Refresh catalogs (force)"
          : "Обновить каталоги (force)";
      }
      const wizardPrevBtn = $("marketplacePrevBtn");
      const wizardNextBtn = $("marketplaceNextBtn");
      if (wizardPrevBtn) wizardPrevBtn.textContent = currentUiLocale === "en" ? "Previous marketplace" : "Предыдущая площадка";
      if (wizardNextBtn) wizardNextBtn.textContent = currentUiLocale === "en" ? "Next marketplace" : "Следующая площадка";
      const resultsTab = $("workspaceResultsBtn");
      const traceTab = $("workspaceTraceBtn");
      const historyTab = $("workspaceHistoryBtn");
      const guideTab = $("workspaceGuideBtn");
      if (resultsTab) resultsTab.textContent = currentUiLocale === "en" ? "Analytics" : "Аналитика";
      if (traceTab) traceTab.textContent = currentUiLocale === "en" ? "Parsing Log" : "Лог парсинга";
      if (historyTab) historyTab.textContent = currentUiLocale === "en" ? "History" : "История";
      if (guideTab) guideTab.textContent = currentUiLocale === "en" ? "Guide" : "Гайд";
      const modalText = $("proxyFallbackModalText");
      if (modalText) {
        modalText.textContent = currentUiLocale === "en"
          ? "Continue without proxies for this session?"
          : "Продолжить без прокси в этой сессии?";
      }
      const modalContinue = $("proxyFallbackContinueBtn");
      const modalCancel = $("proxyFallbackCancelBtn");
      if (modalContinue) modalContinue.textContent = currentUiLocale === "en" ? "Continue" : "Продолжить";
      if (modalCancel) modalCancel.textContent = currentUiLocale === "en" ? "Cancel" : "Отмена";
      updateSectionSelectedCount();
      updatePlayerOkSectionSelectedCount();
      updateGgSellCategorySelectedCount();
      updatePlatiSectionSelectedCount();
      updatePlatiGameCategorySelectedCount();
      setPlatiScopeMode(getPlatiScopeMode(), { ensureData: false });
      if (!selectedRunId) {
        lastUpdateChip.textContent = t("statusWaiting");
        statusBanner.textContent = t("statusWaiting");
      }
      if (!isCatalogLoading) {
        renderCatalogLoadStatus(
          currentUiLocale === "en" ? "Catalogs are not loaded yet" : "Каталоги пока не загружены"
        );
      }
      syncMarketplaceFilterUx();
    }

    function setStatus(text, mode = "") {
      statusBanner.textContent = text;
      statusBanner.className = `status ${mode}`.trim();
    }

    class ApiError extends Error {
      constructor(message, { code = null, status = 0, detail = null } = {}) {
        super(message);
        this.name = "ApiError";
        this.code = code;
        this.status = status;
        this.detail = detail;
      }
    }

    let proxyFallbackResolver = null;
    let proxyFallbackPromptPromise = null;

    function askDirectFallback() {
      return new Promise((resolve) => {
        proxyFallbackResolver = resolve;
        $("proxyFallbackModal").classList.remove("hidden");
      });
    }

    function closeProxyFallbackModal(allow) {
      $("proxyFallbackModal").classList.add("hidden");
      if (proxyFallbackResolver) {
        const resolver = proxyFallbackResolver;
        proxyFallbackResolver = null;
        resolver(Boolean(allow));
      }
    }

    function persistDirectFallbackFlag(enabled) {
      allowDirectFallbackSession = Boolean(enabled);
      if (allowDirectFallbackSession) {
        sessionStorage.setItem("marketstat_allow_direct_fallback", "1");
      } else {
        sessionStorage.removeItem("marketstat_allow_direct_fallback");
      }
    }

    function withDirectFallbackQuery(url) {
      const prepared = new URL(url, window.location.origin);
      if (allowDirectFallbackSession) {
        prepared.searchParams.set("allow_direct_fallback", "true");
      }
      if (!prepared.searchParams.has("force_refresh")) {
        prepared.searchParams.set("force_refresh", "false");
      }
      return `${prepared.pathname}${prepared.search}`;
    }

    async function runWithProxyFallback(task) {
      try {
        return await task();
      } catch (err) {
        if (!(err instanceof ApiError) || err.code !== "proxy_required") {
          throw err;
        }
        if (allowDirectFallbackSession) {
          throw err;
        }
        if (!proxyFallbackPromptPromise) {
          proxyFallbackPromptPromise = askDirectFallback().finally(() => {
            proxyFallbackPromptPromise = null;
          });
        }
        const approved = await proxyFallbackPromptPromise;
        if (!approved) {
          throw err;
        }
        persistDirectFallbackFlag(true);
        return await task();
      }
    }

    async function fetchJson(url, options = null) {
      const response = await fetch(url, options || undefined);
      let data = null;
      try {
        data = await response.json();
      } catch (_) {
        data = null;
      }
      if (!response.ok) {
        const detail = data?.detail;
        if (typeof detail === "string") {
          throw new ApiError(detail, { status: response.status, detail });
        }
        if (detail && typeof detail === "object") {
          const message = String(detail.message || detail.code || JSON.stringify(detail));
          throw new ApiError(message, {
            code: detail.code || null,
            status: response.status,
            detail,
          });
        }
        throw new ApiError(String(data ? JSON.stringify(data) : response.statusText), {
          status: response.status,
          detail: data,
        });
      }
      return data;
    }

    function updateNavLinks() {
      const runPart = selectedRunId ? `?run_id=${encodeURIComponent(selectedRunId)}` : "";
      $("navOverview").href = `/${runPart}`;
      $("navFunPay").href = `/analysis/funpay${runPart}`;
      $("navPlayerOk").href = `/analysis/playerok${runPart}`;
      $("navGgSell").href = `/analysis/ggsell${runPart}`;
      $("navPlati").href = `/analysis/platimarket${runPart}`;
      $("navOverview").classList.toggle("active", !isMarketplaceRoute);
      $("navFunPay").classList.toggle("active", activeMarketplace === "funpay");
      $("navPlayerOk").classList.toggle("active", activeMarketplace === "playerok");
      $("navGgSell").classList.toggle("active", activeMarketplace === "ggsell");
      $("navPlati").classList.toggle("active", activeMarketplace === "platimarket");

      const runId = runIdInput.value.trim() || selectedRunId || "";
      const overviewLink = $("openRunOverviewLink");
      const funpayLink = $("openRunMarketplaceLink");
      const targetMarketplace = activeMarketplace || "funpay";
      if (!runId) {
        overviewLink.href = "#";
        overviewLink.classList.add("disabled");
        funpayLink.href = "#";
        funpayLink.classList.add("disabled");
      } else {
        overviewLink.href = `/?run_id=${encodeURIComponent(runId)}`;
        overviewLink.classList.remove("disabled");
        funpayLink.href = `/analysis/${encodeURIComponent(targetMarketplace)}?run_id=${encodeURIComponent(runId)}`;
        funpayLink.classList.remove("disabled");
      }
    }

    function marketplaceLabel(slug) {
      const item = (marketplacesCatalog || []).find((m) => m.slug === slug);
      return item?.label || slug;
    }

    function selectedMarketplaceSlugsForConfig() {
      if (isMarketplaceRoute) {
        if (isFunPayRoute || isPlayerOkRoute || isGgSellRoute || isPlatiRoute) {
          return [activeMarketplace];
        }
        return [];
      }
      if (typeof selectMarketplaceValues !== "function") return [];
      const selected = selectMarketplaceValues();
      return selected.filter((slug) => slug === "funpay" || slug === "playerok" || slug === "ggsell" || slug === "platimarket");
    }

    function filterPanelByMarketplace(slug) {
      if (slug === "funpay") return $("fpFiltersPanel");
      if (slug === "playerok") return $("pkFiltersPanel");
      if (slug === "ggsell") return $("gsFiltersPanel");
      if (slug === "platimarket") return $("pmFiltersPanel");
      return null;
    }

    function filterContainerByMarketplace(slug) {
      if (slug === "funpay") return $("funpayFiltersContainer");
      if (slug === "playerok") return $("playerokFiltersContainer");
      if (slug === "ggsell") return $("ggsellFiltersContainer");
      if (slug === "platimarket") return $("platimarketFiltersContainer");
      return null;
    }

    function setActiveConfigMarketplace(slug, { remember = true } = {}) {
      const selected = selectedMarketplaceSlugsForConfig();
      const next = selected.includes(slug) ? slug : (selected[0] || "funpay");
      activeConfigMarketplace = next;
      if (remember) {
        localStorage.setItem("marketstat_v2_active_config_marketplace", activeConfigMarketplace);
      }
      const wizardMode = !isMarketplaceRoute && selected.length > 1;
      for (const code of ["funpay", "playerok", "ggsell", "platimarket"]) {
        const panel = filterPanelByMarketplace(code);
        const container = filterContainerByMarketplace(code);
        if (!panel) continue;
        const isSelected = selected.includes(code);
        const isActive = isSelected && code === activeConfigMarketplace;
        const shouldShow = isSelected && (!wizardMode || isActive);
        panel.classList.toggle("hidden", !shouldShow);
        panel.open = Boolean(isActive);
        if (container) {
          container.classList.toggle("hidden", !shouldShow);
        }
      }
      const prevBtn = $("marketplacePrevBtn");
      const nextBtn = $("marketplaceNextBtn");
      if (prevBtn && nextBtn) {
        const idx = selected.indexOf(activeConfigMarketplace);
        prevBtn.disabled = idx <= 0;
        nextBtn.disabled = idx < 0 || idx >= selected.length - 1;
      }
      const hint = $("marketplaceWizardHint");
      if (hint) {
        const idx = selected.indexOf(activeConfigMarketplace);
        if (selected.length === 0) {
          hint.textContent = currentUiLocale === "en"
            ? "Select marketplaces to configure filters."
            : "Выберите площадки, чтобы настроить фильтры.";
        } else {
          hint.textContent = currentUiLocale === "en"
            ? `Step ${idx + 1}/${selected.length}: configure ${marketplaceLabel(activeConfigMarketplace)}`
            : `Шаг ${idx + 1}/${selected.length}: настройте ${marketplaceLabel(activeConfigMarketplace)}`;
        }
      }
    }

    function renderMarketplaceConfigTabs() {
      const host = $("marketplaceConfigTabs");
      if (!host) return;
      const selected = selectedMarketplaceSlugsForConfig();
      if (!selected.length) {
        host.innerHTML = `<div class="hint">${escapeHtml(currentUiLocale === "en" ? "No marketplaces selected." : "Площадки не выбраны.")}</div>`;
        return;
      }
      host.innerHTML = selected.map((slug) => {
        const isActive = slug === activeConfigMarketplace;
        return `<button type="button" class="subtab-btn marketplace-tab-btn ${isActive ? "active" : ""}" data-marketplace-tab="${escapeHtml(slug)}">${escapeHtml(marketplaceLabel(slug))}</button>`;
      }).join("");
      for (const button of host.querySelectorAll("button[data-marketplace-tab]")) {
        button.addEventListener("click", () => {
          const target = String(button.getAttribute("data-marketplace-tab") || "").trim();
          if (!target) return;
          setActiveConfigMarketplace(target);
          renderMarketplaceConfigTabs();
        });
      }
    }

    function syncMarketplaceFilterUx() {
      const wizard = $("marketplaceWizard");
      const selected = selectedMarketplaceSlugsForConfig();
      if (wizard) {
        wizard.classList.toggle("hidden", selected.length <= 1);
      }
      if (!selected.includes(activeConfigMarketplace)) {
        activeConfigMarketplace = selected[0] || "funpay";
      }
      renderMarketplaceConfigTabs();
      setActiveConfigMarketplace(activeConfigMarketplace, { remember: false });
    }

    function cycleMarketplaceConfig(step) {
      const selected = selectedMarketplaceSlugsForConfig();
      if (!selected.length) return;
      const index = selected.indexOf(activeConfigMarketplace);
      const start = index >= 0 ? index : 0;
      const nextIndex = Math.max(0, Math.min(selected.length - 1, start + step));
      const next = selected[nextIndex];
      setActiveConfigMarketplace(next);
      renderMarketplaceConfigTabs();
    }

    function routeVisibility() {
      overviewPage.classList.toggle("hidden", isMarketplaceRoute);
      marketplacePage.classList.toggle("hidden", !isMarketplaceRoute);
    }

    function applyFunPayPreset(preset) {
      if (preset === "safe_fast") {
        $("fpProfile").value = "safe";
        $("fpIncludeReviews").checked = false;
        $("fpIncludeDemand").checked = false;
        $("fpIncludeFallback").checked = false;
        $("fpSectionLimit").value = "40";
        $("fpSellerLimit").value = "3";
        $("fpReviewPages").value = "1";
        return;
      }
      if (preset === "balanced_market") {
        $("fpProfile").value = "balanced";
        $("fpIncludeReviews").checked = true;
        $("fpIncludeDemand").checked = false;
        $("fpIncludeFallback").checked = true;
        $("fpSectionLimit").value = "120";
        $("fpSellerLimit").value = "3";
        $("fpReviewPages").value = "2";
        return;
      }
      if (preset === "deep_demand") {
        $("fpProfile").value = "deep";
        $("fpIncludeReviews").checked = true;
        $("fpIncludeDemand").checked = true;
        $("fpIncludeFallback").checked = true;
        $("fpSectionLimit").value = "200";
        $("fpSellerLimit").value = "8";
        $("fpReviewPages").value = "4";
      }
    }

    function applyFormMode() {
      const formHead = $("formHead");
      const formModeNotice = $("formModeNotice");
      const marketBlock = $("marketplaceSelectBlock");
      const singleHint = $("singleMarketplaceHint");
      const fpPresetBlock = $("fpQuickPresetBlock");
      const runBtn = $("runBtn");
      const marketplaceWizard = $("marketplaceWizard");
      const funpayPanel = $("fpFiltersPanel");
      const playerokPanel = $("pkFiltersPanel");
      const ggsellPanel = $("gsFiltersPanel");
      const platiPanel = $("pmFiltersPanel");
      const commonOptions = $("commonOptionsBlock");
      const singleHintValue = singleHint.querySelector(".value");

      runBtn.disabled = false;
      funpayPanel.classList.remove("hidden");
      playerokPanel.classList.add("hidden");
      ggsellPanel.classList.add("hidden");
      platiPanel.classList.add("hidden");
      commonOptions.classList.remove("hidden");
      if (marketplaceWizard) marketplaceWizard.classList.add("hidden");

      if (isFunPayRoute) {
        formHead.textContent = "Запуск анализа FunPay";
        formModeNotice.className = "status ok";
        formModeNotice.textContent = "Режим страницы площадки: запускается только FunPay, без выбора других площадок.";
        formModeNotice.classList.remove("hidden");
        marketBlock.classList.add("hidden");
        singleHint.classList.remove("hidden");
        if (singleHintValue) singleHintValue.textContent = "Только FunPay (страница площадки)";
        fpPresetBlock.classList.remove("hidden");
        funpayPanel.classList.remove("hidden");
        playerokPanel.classList.add("hidden");
        ggsellPanel.classList.add("hidden");
        platiPanel.classList.add("hidden");
        funpayPanel.open = true;
        runBtn.textContent = "Запустить анализ FunPay";
        return;
      }

      if (isPlayerOkRoute) {
        formHead.textContent = "Запуск анализа PlayerOK";
        formModeNotice.className = "status ok";
        formModeNotice.textContent = "Режим страницы площадки: запускается только PlayerOK, без выбора других площадок.";
        formModeNotice.classList.remove("hidden");
        marketBlock.classList.add("hidden");
        singleHint.classList.remove("hidden");
        if (singleHintValue) singleHintValue.textContent = "Только PlayerOK (страница площадки)";
        fpPresetBlock.classList.add("hidden");
        funpayPanel.classList.add("hidden");
        playerokPanel.classList.remove("hidden");
        ggsellPanel.classList.add("hidden");
        platiPanel.classList.add("hidden");
        playerokPanel.open = true;
        runBtn.textContent = "Запустить анализ PlayerOK";
        return;
      }

      if (isGgSellRoute) {
        formHead.textContent = "Запуск анализа GGSell";
        formModeNotice.className = "status ok";
        formModeNotice.textContent = "Режим страницы площадки: запускается только GGSell, без выбора других площадок.";
        formModeNotice.classList.remove("hidden");
        marketBlock.classList.add("hidden");
        singleHint.classList.remove("hidden");
        if (singleHintValue) singleHintValue.textContent = "Только GGSell (страница площадки)";
        fpPresetBlock.classList.add("hidden");
        funpayPanel.classList.add("hidden");
        playerokPanel.classList.add("hidden");
        ggsellPanel.classList.remove("hidden");
        platiPanel.classList.add("hidden");
        ggsellPanel.open = true;
        runBtn.textContent = "Запустить анализ GGSell";
        return;
      }

      if (isPlatiRoute) {
        formHead.textContent = "Запуск анализа Plati.Market";
        formModeNotice.className = "status ok";
        formModeNotice.textContent = "Режим страницы площадки: запускается только Plati.Market, без выбора других площадок.";
        formModeNotice.classList.remove("hidden");
        marketBlock.classList.add("hidden");
        singleHint.classList.remove("hidden");
        if (singleHintValue) singleHintValue.textContent = "Только Plati.Market (страница площадки)";
        fpPresetBlock.classList.add("hidden");
        funpayPanel.classList.add("hidden");
        playerokPanel.classList.add("hidden");
        ggsellPanel.classList.add("hidden");
        platiPanel.classList.remove("hidden");
        platiPanel.open = true;
        runBtn.textContent = "Запустить анализ Plati.Market";
        return;
      }

      if (isMarketplaceRoute && !isFunPayRoute && !isPlayerOkRoute && !isGgSellRoute && !isPlatiRoute) {
        formHead.textContent = "Запуск анализа площадки";
        formModeNotice.className = "status err";
        formModeNotice.textContent = `Площадка "${activeMarketplace}" пока не реализована. Запуск недоступен.`;
        formModeNotice.classList.remove("hidden");
        marketBlock.classList.add("hidden");
        singleHint.classList.add("hidden");
        fpPresetBlock.classList.add("hidden");
        funpayPanel.classList.add("hidden");
        playerokPanel.classList.add("hidden");
        ggsellPanel.classList.add("hidden");
        platiPanel.classList.add("hidden");
        commonOptions.classList.add("hidden");
        runBtn.textContent = "Запуск недоступен";
        runBtn.disabled = true;
        return;
      }

      formHead.textContent = "Фильтры общего запуска";
      formModeNotice.className = "status warn";
      formModeNotice.textContent = "Мульти-площадочный режим: выберите площадки для общего анализа. Для детального запуска используйте страницу нужной площадки.";
      formModeNotice.classList.remove("hidden");
      marketBlock.classList.remove("hidden");
      singleHint.classList.add("hidden");
      fpPresetBlock.classList.add("hidden");
      runBtn.textContent = "Запустить общий анализ";
      syncMarketplaceFilterUx();
    }

