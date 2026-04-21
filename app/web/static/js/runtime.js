    function updateRunId(runId) {
      if (!runId) return;
      selectedRunId = runId;
      runIdInput.value = runId;
      localStorage.setItem("marketstat_v2_last_run", runId);
      updateNavLinks();
    }

    function renderCategoryScope(item) {
      const platiSectionById = new Map();
      const platiGroupById = new Map();
      const platiGameById = new Map();
      const platiGameBySlug = new Map();
      const platiGameCategoryById = new Map();
      for (const section of getPlatiCatalogSectionsFlat()) {
        const sectionId = Number(section.section_id);
        if (Number.isFinite(sectionId)) {
          platiSectionById.set(sectionId, section);
        }
      }
      for (const root of platiCatalogTree || []) {
        const groupId = Number(root.section_id);
        if (Number.isFinite(groupId)) {
          platiGroupById.set(groupId, root);
        }
      }
      for (const game of platiGamesCatalog || []) {
        const gameId = Number(game.game_id);
        if (Number.isFinite(gameId)) {
          platiGameById.set(gameId, game);
        }
        const slug = String(game.game_slug || "").trim();
        if (slug) {
          platiGameBySlug.set(slug, game);
        }
      }
      for (const category of platiGameCategoriesCatalog || []) {
        const categoryId = Number(category.category_id);
        if (Number.isFinite(categoryId) && categoryId >= 0) {
          platiGameCategoryById.set(categoryId, category);
        }
      }
      const platiCategoryIds = Array.isArray(item.platimarket_category_ids) ? item.platimarket_category_ids : [];
      const platiGameCategoryIds = Array.isArray(item.platimarket_game_category_ids) ? item.platimarket_game_category_ids : [];
      const platiGroupId = item.platimarket_group_id;
      const platiGameId = Number(item.platimarket_game_id);
      const platiGameSlug = String(item.platimarket_game_slug || "").trim();
      const platiGameName = String(item.platimarket_game_name || "").trim();
      if (platiCategoryIds.length || platiGameCategoryIds.length || platiGroupId || platiGameId || platiGameSlug) {
        const links = [];
        const gameUrlBase = Number.isFinite(platiGameId) && platiGameId > 0
          ? `https://plati.market/games/${escapeHtml(platiGameSlug || String(platiGameId))}/${escapeHtml(platiGameId)}/`
          : (platiGameSlug ? `https://plati.market/games/${escapeHtml(platiGameSlug)}/` : null);
        if (Number.isFinite(platiGameId) && platiGameId > 0) {
          const game = platiGameById.get(platiGameId);
          if (game) {
            links.push(`<a href="${escapeHtml(game.game_url)}" target="_blank" rel="noopener noreferrer">${escapeHtml(game.game_name)}</a>`);
          } else {
            const slug = platiGameSlug || String(platiGameId);
            const label = platiGameName || `${currentUiLocale === "en" ? "Game" : "Игра"} #${platiGameId}`;
            links.push(`<a href="https://plati.market/games/${escapeHtml(slug)}/${escapeHtml(platiGameId)}/" target="_blank" rel="noopener noreferrer">${escapeHtml(label)}</a>`);
          }
        } else if (platiGameSlug) {
          const game = platiGameBySlug.get(platiGameSlug);
          if (game) {
            links.push(`<a href="${escapeHtml(game.game_url)}" target="_blank" rel="noopener noreferrer">${escapeHtml(game.game_name)}</a>`);
          } else {
            const label = platiGameName || `${currentUiLocale === "en" ? "Game" : "Игра"} ${platiGameSlug}`;
            links.push(`<a href="https://plati.market/games/${escapeHtml(platiGameSlug)}/" target="_blank" rel="noopener noreferrer">${escapeHtml(label)}</a>`);
          }
        }
        for (const rawGameCategoryId of platiGameCategoryIds) {
          const id = Number(rawGameCategoryId);
          if (!Number.isFinite(id) || id < 0) continue;
          const category = platiGameCategoryById.get(id);
          const label = category?.category_name
            ? category.category_name
            : (id === 0
              ? (currentUiLocale === "en" ? "All offers" : "Все предложения")
              : `${currentUiLocale === "en" ? "Game category" : "Категория игры"} #${id}`);
          const href = gameUrlBase ? `${gameUrlBase}?id_c=${id}` : "#";
          links.push(`<a href="${href}" target="_blank" rel="noopener noreferrer">${escapeHtml(label)}</a>`);
        }
        for (const rawId of platiCategoryIds) {
          const id = Number(rawId);
          if (!Number.isFinite(id)) continue;
          const section = platiSectionById.get(id);
          if (section) {
            links.push(`<a href="${escapeHtml(section.url || section.section_url || `https://plati.market/cat/${id}/`)}" target="_blank" rel="noopener noreferrer">${escapeHtml(section.full_name || section.section_name || `Раздел #${id}`)}</a>`);
          } else {
            links.push(`<a href="https://plati.market/cat/${id}/" target="_blank" rel="noopener noreferrer">${currentUiLocale === "en" ? "Section" : "Раздел"} #${id}</a>`);
          }
        }
        if (!links.length && platiGroupId) {
          const group = platiGroupById.get(Number(platiGroupId));
          if (group) {
            links.push(`<a href="${escapeHtml(group.url || group.group_url || `https://plati.market/cat/${platiGroupId}/`)}" target="_blank" rel="noopener noreferrer">${escapeHtml(group.title || group.group_name || `Раздел #${platiGroupId}`)}</a>`);
          } else {
            links.push(`<a href="https://plati.market/cat/${escapeHtml(platiGroupId)}/" target="_blank" rel="noopener noreferrer">${currentUiLocale === "en" ? "Group" : "Группа"} #${escapeHtml(platiGroupId)}</a>`);
          }
        }
        return links.length ? links.join(", ") : "—";
      }

      const playerokSectionBySlug = new Map();
      const playerokGameBySlug = new Map();
      for (const game of playerokCategoriesCatalog || []) {
        playerokGameBySlug.set(String(game.game_slug), game);
        for (const section of game.sections || []) {
          playerokSectionBySlug.set(String(section.section_slug), section);
        }
      }
      const playerokCategorySlugs = Array.isArray(item.category_slugs) ? item.category_slugs : [];
      if (playerokCategorySlugs.length || item.category_game_slug) {
        const links = [];
        for (const rawSlug of playerokCategorySlugs) {
          const slug = String(rawSlug || "").trim();
          if (!slug) continue;
          const section = playerokSectionBySlug.get(slug);
          if (section) {
            links.push(`<a href="${escapeHtml(section.section_url)}" target="_blank" rel="noopener noreferrer">${escapeHtml(section.full_name)}</a>`);
            continue;
          }
          const fallbackUrl = slug.startsWith("categories/")
            ? `https://playerok.com/${slug}`
            : `https://playerok.com/${slug}`;
          links.push(`<a href="${escapeHtml(fallbackUrl)}" target="_blank" rel="noopener noreferrer">${escapeHtml(slug)}</a>`);
        }
        if (!links.length && item.category_game_slug) {
          const game = playerokGameBySlug.get(String(item.category_game_slug));
          if (game) {
            links.push(`<a href="${escapeHtml(game.game_url)}" target="_blank" rel="noopener noreferrer">${escapeHtml(game.game_name)}</a>`);
          } else {
            links.push(`<a href="https://playerok.com/${escapeHtml(item.category_game_slug)}" target="_blank" rel="noopener noreferrer">${currentUiLocale === "en" ? "Game" : "Игра"} ${escapeHtml(item.category_game_slug)}</a>`);
          }
        }
        return links.length ? links.join(", ") : "—";
      }

      const ggsellCategoryBySlug = new Map();
      for (const category of ggsellCategoriesCatalog || []) {
        const slug = String(category.category_slug || "").trim();
        if (!slug) continue;
        ggsellCategoryBySlug.set(slug, category);
      }
      const ggsellTypeBySlug = new Map();
      for (const type of ggsellTypesCatalog || []) {
        const slug = String(type.type_slug || "").trim();
        if (!slug) continue;
        ggsellTypeBySlug.set(slug, type);
      }
      const ggsellCategorySlugs = Array.isArray(item.ggsell_category_slugs) ? item.ggsell_category_slugs : [];
      const ggsellTypeSlug = String(item.ggsell_type_slug || "").trim();
      if (ggsellCategorySlugs.length || ggsellTypeSlug) {
        const links = [];
        for (const rawSlug of ggsellCategorySlugs) {
          const slug = String(rawSlug || "").trim();
          if (!slug) continue;
          const category = ggsellCategoryBySlug.get(slug);
          if (category) {
            const href = safeUrl(category.category_url) || safeUrl(`https://ggsel.net/catalog/${slug}`);
            const text = escapeHtml(category.category_name || slug);
            links.push(href
              ? `<a href="${escapeHtml(href)}" target="_blank" rel="noopener noreferrer">${text}</a>`
              : text);
          } else {
            links.push(`<a href="https://ggsel.net/catalog/${escapeHtml(slug)}" target="_blank" rel="noopener noreferrer">${escapeHtml(slug)}</a>`);
          }
        }
        if (!links.length && ggsellTypeSlug) {
          const type = ggsellTypeBySlug.get(ggsellTypeSlug);
          if (type) {
            const typeUrl = String(type.category_url || "").trim() || ggsellTypeSlug;
            const href = safeUrl(`https://ggsel.net/catalog/${typeUrl}`);
            const text = escapeHtml(type.type_name || ggsellTypeSlug);
            links.push(href
              ? `<a href="${escapeHtml(href)}" target="_blank" rel="noopener noreferrer">${text}</a>`
              : text);
          } else {
            links.push(`<a href="https://ggsel.net/catalog/${escapeHtml(ggsellTypeSlug)}" target="_blank" rel="noopener noreferrer">${escapeHtml(ggsellTypeSlug)}</a>`);
          }
        }
        return links.length ? links.join(", ") : "—";
      }

      const sectionById = new Map();
      const gameById = new Map();
      for (const game of categoriesCatalog || []) {
        gameById.set(Number(game.game_section_id), game);
        for (const section of game.sections || []) {
          sectionById.set(Number(section.section_id), section);
        }
      }
      const links = [];
      const ids = Array.isArray(item.category_ids) ? item.category_ids : [];
      for (const rawId of ids) {
        const id = Number(rawId);
        if (!Number.isFinite(id)) continue;
        const section = sectionById.get(id);
        if (section) {
          links.push(`<a href="${escapeHtml(section.section_url)}" target="_blank" rel="noopener noreferrer">${escapeHtml(section.full_name)}</a>`);
        } else {
          links.push(`<a href="https://funpay.com/lots/${id}/" target="_blank" rel="noopener noreferrer">${currentUiLocale === "en" ? "Section" : "Раздел"} #${id}</a>`);
        }
      }
      if (!links.length && item.category_id) {
        const id = Number(item.category_id);
        const section = sectionById.get(id);
        if (section) {
          links.push(`<a href="${escapeHtml(section.section_url)}" target="_blank" rel="noopener noreferrer">${escapeHtml(section.full_name)}</a>`);
        } else {
          links.push(`<a href="https://funpay.com/lots/${id}/" target="_blank" rel="noopener noreferrer">${currentUiLocale === "en" ? "Section" : "Раздел"} #${id}</a>`);
        }
      }
      if (!links.length && item.category_game_id) {
        const game = gameById.get(Number(item.category_game_id));
        if (game) {
          links.push(`<a href="${escapeHtml(game.game_url)}" target="_blank" rel="noopener noreferrer">${escapeHtml(game.game_name)}</a>`);
        } else {
          links.push(`<a href="https://funpay.com/lots/${escapeHtml(item.category_game_id)}" target="_blank" rel="noopener noreferrer">${currentUiLocale === "en" ? "Game" : "Игра"} #${escapeHtml(item.category_game_id)}</a>`);
        }
      }
      return links.length ? links.join(", ") : "—";
    }

    async function renderHistory() {
      try {
        const data = await fetchJson("/v2/history?limit=120");
        const items = data.items || [];
        const queryFilter = $("historyQueryFilter").value.trim().toLowerCase();
        const marketplaceFilter = $("historyMarketplaceFilter").value;
        const filteredItems = items.filter((item) => {
          const query = String(formatHistoryQuery(item.query) || "").toLowerCase();
          if (queryFilter && !query.includes(queryFilter)) return false;
          if (marketplaceFilter) {
            const marketplaces = Array.isArray(item.marketplaces) ? item.marketplaces : [];
            if (!marketplaces.includes(marketplaceFilter)) return false;
          }
          return true;
        });

        $("historyCount").textContent = t("historyLoaded", { shown: filteredItems.length, total: items.length });
        if (!filteredItems.length) {
          historyHost.innerHTML = items.length
            ? `<div class='muted'>${escapeHtml(t("historyNoFilter"))}</div>`
            : `<div class='muted'>${escapeHtml(t("historyEmpty"))}</div>`;
          return;
        }
        historyHost.innerHTML = filteredItems.map((item) => {
          const runId = escapeHtml(item.run_id);
          const marketplacesText = (item.marketplaces || [])
            .map((slug) => marketplaceLabel(slug))
            .join(", ");
          const categoryScopeHtml = renderCategoryScope(item);
          const summary = `
            <div class="summary-grid">
              <span><b>${escapeHtml(formatHistoryQuery(item.query))}</b><br/><span class="muted">${new Date(item.generated_at).toLocaleString(localeTag())}</span></span>
              <span>${currentUiLocale === "en" ? "Marketplaces" : "Площадки"}: <b>${escapeHtml(marketplacesText || "—")}</b></span>
              <span>${currentUiLocale === "en" ? "Offers" : "Офферы"}: <b>${formatNum(item.pooled_matched_offers)}</b></span>
              <span>${currentUiLocale === "en" ? "Sellers" : "Продавцы"}: <b>${formatNum(item.pooled_unique_sellers)}</b></span>
              <span>P50: <b>${formatMoney(item.pooled_p50_price, item.currency)}</b></span>
            </div>
          `;
          const sub = (item.marketplace_items || []).map((subItem) => `
            <div class="subitem">
              <div><b>${escapeHtml(subItem.label || marketplaceLabel(subItem.marketplace))}</b> (${escapeHtml(subItem.marketplace)})</div>
              <div>${currentUiLocale === "en" ? "Offers" : "Офферы"}: <b>${formatNum(subItem.matched_offers)}</b>, ${currentUiLocale === "en" ? "sellers" : "продавцы"}: <b>${formatNum(subItem.unique_sellers)}</b>, P50: <b>${formatMoney(subItem.p50_price, item.currency)}</b></div>
              <div>Demand: <b>${subItem.demand_index === null || subItem.demand_index === undefined ? "—" : Number(subItem.demand_index).toFixed(2)}</b>, ${currentUiLocale === "en" ? "warnings" : "предупреждений"}: <b>${formatNum(subItem.warnings_count)}</b></div>
              <div class="actions" style="margin-top:6px;">
                <a class="btn-secondary" style="text-decoration:none; display:inline-block; padding:6px 10px;" href="/analysis/${escapeHtml(subItem.marketplace)}?run_id=${runId}">${escapeHtml(t("openMarketplace"))}</a>
              </div>
            </div>
          `).join("");
          return `
            <details class="history-item">
              <summary>${summary}</summary>
              <div style="padding:8px 10px;">
                <div class="hint">run_id: <span class="mono">${runId}</span></div>
                <div class="hint">${currentUiLocale === "en" ? "Category" : "Категория"}: ${categoryScopeHtml}</div>
                <div class="actions" style="margin-top:0;">
                  <a class="btn-secondary" style="text-decoration:none; display:inline-block; padding:6px 10px;" href="/?run_id=${runId}">${escapeHtml(t("openOverview"))}</a>
                </div>
                ${sub}
              </div>
            </details>
          `;
        }).join("");
      } catch (err) {
        historyHost.innerHTML = `<div class="status err">${escapeHtml(t("errorPrefix"))}: ${escapeHtml(String(err))}</div>`;
      }
    }

    async function loadOverview(runId) {
      const overview = await fetchJson(`/v2/analyze/${encodeURIComponent(runId)}/overview`);
      renderOverview(overview);
    }

    async function loadMarketplaceOffersPaginated(runId, marketplace) {
      const moduleLoader = window.MarketStatModules?.actions?.loadMarketplaceOffers;
      if (typeof moduleLoader === "function") {
        try {
          return await moduleLoader({
            runId,
            marketplace,
            fetchJson,
            pageSize: OFFERS_FETCH_PAGE_SIZE,
            maxItems: OFFERS_FETCH_MAX_ITEMS,
            createOffersLoadState,
          });
        } catch (_) {
          // fallback to local loader below
        }
      }

      const items = [];
      let offset = 0;
      let total = null;
      let maxReached = false;

      while (true) {
        const remaining = OFFERS_FETCH_MAX_ITEMS - items.length;
        if (remaining <= 0) {
          maxReached = true;
          break;
        }
        const limit = Math.max(1, Math.min(OFFERS_FETCH_PAGE_SIZE, remaining));
        const response = await fetchJson(
          `/v2/analyze/${encodeURIComponent(runId)}/marketplaces/${encodeURIComponent(marketplace)}/offers?limit=${limit}&offset=${offset}`
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
        if (total !== null && items.length >= total) break;
        if (batch.length < limit) break;
      }

      const resolvedTotal = total !== null ? total : items.length;
      const loadState = createOffersLoadState({
        loaded: items.length,
        total: resolvedTotal,
        partial: maxReached || (resolvedTotal > items.length),
        maxReached,
        maxItems: OFFERS_FETCH_MAX_ITEMS,
        pageSize: OFFERS_FETCH_PAGE_SIZE,
      });
      return { items, loadState };
    }

    async function loadMarketplacePage(runId, marketplace) {
      const result = await fetchJson(`/v2/analyze/${encodeURIComponent(runId)}/marketplaces/${encodeURIComponent(marketplace)}`);
      const offersPayload = await loadMarketplaceOffersPaginated(runId, marketplace);
      fullOffers = Array.isArray(offersPayload?.items) ? offersPayload.items : [];
      const nextLoadState = setOffersLoadState(offersPayload?.loadState || null);
      offersPage = 0;
      renderMarketplaceDetail(result);
      renderOffersTable();
      if (nextLoadState.partial && nextLoadState.total !== null) {
        setStatus(
          t("offersPartialNotice", {
            loaded: formatNum(nextLoadState.loaded),
            total: formatNum(nextLoadState.total),
          }),
          "warn"
        );
      }
    }

    async function loadRunStatus(runId, refreshMode = false) {
      const envelope = await fetchJson(`/v2/analyze/${encodeURIComponent(runId)}`);
      updateRunId(runId);
      const localeFromRun = envelope?.marketplaces?.funpay?.ui_locale
        || Object.values(envelope?.marketplaces || {})?.[0]?.ui_locale;
      if (localeFromRun && (localeFromRun === "ru" || localeFromRun === "en") && $("uiLocale").value !== localeFromRun) {
        $("uiLocale").value = localeFromRun;
        applyUiLocale();
      }
      renderProgress(envelope);
      if (envelope.status === "done") {
        setStatus(t("runDone"), "ok");
        clearInterval(pollTimer);
        if (!isMarketplaceRoute) {
          await loadOverview(runId);
        } else if (activeMarketplace) {
          await loadMarketplacePage(runId, activeMarketplace);
        }
        if (envelope.overview?.generated_at) {
          lastUpdateChip.textContent = `${currentUiLocale === "en" ? "Updated" : "Обновлено"}: ${new Date(envelope.overview.generated_at).toLocaleString(localeTag())}`;
        } else {
          lastUpdateChip.textContent = `${currentUiLocale === "en" ? "Updated" : "Обновлено"}: ${new Date().toLocaleString(localeTag())}`;
        }
        if (!refreshMode && (activeWorkspaceTab === "history" || historyLoadedOnce)) {
          await renderHistory();
          historyLoadedOnce = true;
        }
      } else if (envelope.status === "failed") {
        clearInterval(pollTimer);
        setStatus(`${t("errorPrefix")}: ${envelope.error || t("unknown")}`, "err");
      } else {
        const p = Number(envelope?.progress?.percent);
        setStatus(`${currentUiLocale === "en" ? "Status" : "Статус"}: ${envelope.status}${Number.isFinite(p) ? ` (${p.toFixed(0)}%)` : ""}`, "warn");
      }
    }

    function pollRun(runId) {
      clearInterval(pollTimer);
      let retries = 0;
      pollTimer = setInterval(async () => {
        retries += 1;
        try {
          await loadRunStatus(runId, true);
          if (retries > 180) {
            clearInterval(pollTimer);
            setStatus(currentUiLocale === "en" ? "Polling stopped: timeout limit exceeded." : "Опрос остановлен: превышен лимит ожидания.", "warn");
          }
        } catch (err) {
          clearInterval(pollTimer);
          setStatus(String(err), "err");
        }
      }, 2000);
    }

    async function runAnalyze() {
      try {
        setWorkspaceTab("results");
        fullOffers = [];
        filteredOffers = [];
        setOffersLoadState({ loaded: 0, total: 0, partial: false });
        let payload = buildPayload();
        if (!payload.marketplaces.length) {
          setStatus(t("selectMarketplace"), "err");
          return;
        }
        setStatus(t("runStart"), "warn");
        const submitAnalyze = async () => {
          payload = buildPayload();
          return await fetchJson("/v2/analyze", {
            method: "POST",
            headers: {
              "Content-Type": "application/json; charset=utf-8",
              "Accept": "application/json",
            },
            body: JSON.stringify(payload),
          });
        };
        const data = await runWithProxyFallback(submitAnalyze);
        updateRunId(data.run_id);
        renderProgress(data);
        if (data.status === "queued" || data.status === "running") {
          pollRun(data.run_id);
        } else if (data.status === "done") {
          await loadRunStatus(data.run_id);
        } else if (data.status === "failed") {
          setStatus(`${t("errorPrefix")}: ${data.error || t("unknown")}`, "err");
          return;
        }

        if (data.status === "done") {
          if (payload.marketplaces.length === 1) {
            const target = payload.marketplaces[0];
            window.location.href = `/analysis/${encodeURIComponent(target)}?run_id=${encodeURIComponent(data.run_id)}`;
            return;
          }
          window.history.replaceState({}, "", `/?run_id=${encodeURIComponent(data.run_id)}`);
        }
      } catch (err) {
        const message = err instanceof ApiError ? err.message : String(err);
        setStatus(message, "err");
      }
    }

    async function manualRefresh() {
      const runId = runIdInput.value.trim();
      if (!runId) {
        setStatus(t("runIdRequired"), "err");
        return;
      }
      try {
        setWorkspaceTab("results");
        await loadRunStatus(runId);
      } catch (err) {
        setStatus(String(err), "err");
      }
    }

    function attachEvents() {
      $("runBtn").addEventListener("click", runAnalyze);
      $("refreshBtn").addEventListener("click", manualRefresh);
      $("historyRefreshBtn").addEventListener("click", renderHistory);
      $("loadCatalogsBtn").addEventListener("click", async () => {
        try {
          await loadSelectedCatalogs({ force: false });
        } catch (_) {
          // status is already shown in catalog chip
        }
      });
      $("refreshCatalogsBtn").addEventListener("click", async () => {
        try {
          await loadSelectedCatalogs({ force: true });
        } catch (_) {
          // status is already shown in catalog chip
        }
      });
      $("workspaceResultsBtn").addEventListener("click", () => setWorkspaceTab("results"));
      $("workspaceTraceBtn").addEventListener("click", () => setWorkspaceTab("trace"));
      $("workspaceHistoryBtn").addEventListener("click", () => setWorkspaceTab("history"));
      $("workspaceGuideBtn").addEventListener("click", () => setWorkspaceTab("guide"));
      $("marketplacePrevBtn").addEventListener("click", () => cycleMarketplaceConfig(-1));
      $("marketplaceNextBtn").addEventListener("click", () => cycleMarketplaceConfig(1));
      $("historyQueryFilter").addEventListener("input", renderHistory);
      $("historyMarketplaceFilter").addEventListener("change", renderHistory);
      $("marketplaceChecks").addEventListener("change", syncOverviewMarketplaceFilterBlocks);
      runIdInput.addEventListener("input", updateNavLinks);
      $("fpQuickPreset").addEventListener("change", (event) => {
        const preset = String(event.target.value || "custom");
        if (preset !== "custom") {
          applyFunPayPreset(preset);
          setStatus(currentUiLocale === "en" ? `FunPay preset applied: ${preset}` : `Применен пресет FunPay: ${preset}`, "ok");
        }
      });
      $("uiLocale").addEventListener("change", async () => {
        applyUiLocale();
        if (selectedRunId) {
          try {
            await loadRunStatus(selectedRunId, true);
          } catch (_) {
            await renderHistory();
          }
        } else {
          await renderHistory();
        }
      });
      $("copyRunIdBtn").addEventListener("click", async () => {
        const runId = runIdInput.value.trim() || selectedRunId || "";
        if (!runId) {
          setStatus(t("copyRunIdRequired"), "warn");
          return;
        }
        try {
          await navigator.clipboard.writeText(runId);
          setStatus(t("copiedRunId"), "ok");
        } catch (_) {
          runIdInput.select();
          document.execCommand("copy");
          setStatus(t("copiedRunId"), "ok");
        }
      });
      $("fpGame").addEventListener("change", () => {
        selectedSectionIds = new Set();
        updateSectionOptions();
      });
      $("fpSectionSearch").addEventListener("input", updateSectionOptions);
      $("fpSectionChecklist").addEventListener("change", () => {
        syncSectionSelectionFromDom();
        updateSectionSelectedCount();
      });
      $("fpSectionSelectVisible").addEventListener("click", () => {
        const nodes = Array.from($("fpSectionChecklist").querySelectorAll("input[type='checkbox'][data-section-id]"));
        for (const node of nodes) {
          const id = Number(node.getAttribute("data-section-id"));
          if (!Number.isFinite(id) || id <= 0) continue;
          node.checked = true;
          selectedSectionIds.add(Math.floor(id));
        }
        updateSectionSelectedCount();
      });
      $("fpSectionClearVisible").addEventListener("click", () => {
        const nodes = Array.from($("fpSectionChecklist").querySelectorAll("input[type='checkbox'][data-section-id]"));
        for (const node of nodes) {
          const id = Number(node.getAttribute("data-section-id"));
          if (!Number.isFinite(id) || id <= 0) continue;
          node.checked = false;
          selectedSectionIds.delete(Math.floor(id));
        }
        updateSectionSelectedCount();
      });
      $("fpIncludeDemand").addEventListener("change", (event) => {
        if (event.target.checked) $("fpIncludeReviews").checked = true;
      });
      $("pkGame").addEventListener("change", async () => {
        const gameSlug = String($("pkGame").value || "").trim();
        if (gameSlug) {
          try {
            await ensurePlayerOkGameSections(gameSlug);
          } catch (_) {
            // keep current catalog snapshot if details loading fails
          }
        }
        selectedPlayerOkSectionSlugs = new Set();
        updatePlayerOkSectionOptions();
      });
      $("pkSectionSearch").addEventListener("input", updatePlayerOkSectionOptions);
      $("pkSectionChecklist").addEventListener("change", () => {
        syncPlayerOkSectionSelectionFromDom();
        updatePlayerOkSectionSelectedCount();
      });
      $("pkSectionSelectVisible").addEventListener("click", () => {
        const nodes = Array.from($("pkSectionChecklist").querySelectorAll("input[type='checkbox'][data-section-slug]"));
        for (const node of nodes) {
          const slug = String(node.getAttribute("data-section-slug") || "").trim();
          if (!slug) continue;
          node.checked = true;
          selectedPlayerOkSectionSlugs.add(slug);
        }
        updatePlayerOkSectionSelectedCount();
      });
      $("pkSectionClearVisible").addEventListener("click", () => {
        const nodes = Array.from($("pkSectionChecklist").querySelectorAll("input[type='checkbox'][data-section-slug]"));
        for (const node of nodes) {
          const slug = String(node.getAttribute("data-section-slug") || "").trim();
          if (!slug) continue;
          node.checked = false;
          selectedPlayerOkSectionSlugs.delete(slug);
        }
        updatePlayerOkSectionSelectedCount();
      });
      $("pkIncludeDemand").addEventListener("change", (event) => {
        if (event.target.checked) $("pkIncludeReviews").checked = true;
      });
      $("gsType").addEventListener("change", updateGgSellCategoryOptions);
      $("gsCategorySearch").addEventListener("input", updateGgSellCategoryOptions);
      $("gsCategoryChecklist").addEventListener("change", () => {
        syncGgSellCategorySelectionFromDom();
        updateGgSellCategorySelectedCount();
      });
      $("gsCategorySelectVisible").addEventListener("click", () => {
        const nodes = Array.from($("gsCategoryChecklist").querySelectorAll("input[type='checkbox'][data-category-slug]"));
        for (const node of nodes) {
          const slug = String(node.getAttribute("data-category-slug") || "").trim();
          if (!slug) continue;
          node.checked = true;
          selectedGgSellCategorySlugs.add(slug);
        }
        updateGgSellCategorySelectedCount();
      });
      $("gsCategoryClearVisible").addEventListener("click", () => {
        const nodes = Array.from($("gsCategoryChecklist").querySelectorAll("input[type='checkbox'][data-category-slug]"));
        for (const node of nodes) {
          const slug = String(node.getAttribute("data-category-slug") || "").trim();
          if (!slug) continue;
          node.checked = false;
          selectedGgSellCategorySlugs.delete(slug);
        }
        updateGgSellCategorySelectedCount();
      });
      $("gsIncludeDemand").addEventListener("change", (event) => {
        if (event.target.checked) $("gsIncludeReviews").checked = true;
      });
      $("pmScopeMode").addEventListener("change", async () => {
        try {
          await setPlatiScopeMode(getPlatiScopeMode(), { ensureData: true });
        } catch (_) {
          // keep already loaded subset if lazy loading fails
        }
      });
      $("pmGame").addEventListener("change", async () => {
        selectedPlatiGameCategoryIds = new Set();
        try {
          await loadPlatiGameCategories();
        } catch (_) {
          platiGameCategoriesCatalog = [];
          updatePlatiGameCategoryOptions();
        }
      });
      $("pmGameCustom").addEventListener("change", async () => {
        selectedPlatiGameCategoryIds = new Set();
        try {
          await loadPlatiGameCategories();
        } catch (_) {
          platiGameCategoriesCatalog = [];
          updatePlatiGameCategoryOptions();
        }
      });
      $("pmGameCategorySearch").addEventListener("input", updatePlatiGameCategoryOptions);
      $("pmGameCategoryChecklist").addEventListener("change", () => {
        syncPlatiGameCategorySelectionFromDom();
        updatePlatiGameCategorySelectedCount();
      });
      $("pmGameCategorySelectVisible").addEventListener("click", () => {
        const nodes = Array.from($("pmGameCategoryChecklist").querySelectorAll("input[type='checkbox'][data-game-category-id]"));
        for (const node of nodes) {
          const id = Number(node.getAttribute("data-game-category-id"));
          if (!Number.isFinite(id) || id < 0) continue;
          node.checked = true;
          selectedPlatiGameCategoryIds.add(Math.floor(id));
        }
        updatePlatiGameCategorySelectedCount();
      });
      $("pmGameCategoryClearVisible").addEventListener("click", () => {
        const nodes = Array.from($("pmGameCategoryChecklist").querySelectorAll("input[type='checkbox'][data-game-category-id]"));
        for (const node of nodes) {
          const id = Number(node.getAttribute("data-game-category-id"));
          if (!Number.isFinite(id) || id < 0) continue;
          node.checked = false;
          selectedPlatiGameCategoryIds.delete(Math.floor(id));
        }
        updatePlatiGameCategorySelectedCount();
      });
      $("pmSectionSearch").addEventListener("input", updatePlatiSectionOptions);
      $("pmSectionChecklist").addEventListener("change", (event) => {
        const target = event.target;
        if (!(target instanceof HTMLInputElement)) return;
        if (!target.matches("input[type='checkbox'][data-section-id]")) return;
        const id = Number(target.getAttribute("data-section-id"));
        if (!Number.isFinite(id) || id <= 0) return;
        togglePlatiCategoryCascade(id, target.checked);
        updatePlatiSectionOptions();
      });
      $("pmSectionChecklist").addEventListener("toggle", (event) => {
        const target = event.target;
        if (!(target instanceof HTMLDetailsElement)) return;
        if (!target.matches("details[data-tree-id]")) return;
        const id = Number(target.getAttribute("data-tree-id"));
        if (!Number.isFinite(id) || id <= 0) return;
        if (target.open) platiTreeExpandedIds.add(Math.floor(id));
        else platiTreeExpandedIds.delete(Math.floor(id));
      }, true);
      $("pmSectionSelectVisible").addEventListener("click", () => {
        const nodes = Array.from($("pmSectionChecklist").querySelectorAll("input[type='checkbox'][data-section-id]"));
        for (const node of nodes) {
          const id = Number(node.getAttribute("data-section-id"));
          if (!Number.isFinite(id) || id <= 0) continue;
          node.checked = true;
          togglePlatiCategoryCascade(Math.floor(id), true);
        }
        updatePlatiSectionOptions();
      });
      $("pmSectionClearVisible").addEventListener("click", () => {
        const nodes = Array.from($("pmSectionChecklist").querySelectorAll("input[type='checkbox'][data-section-id]"));
        for (const node of nodes) {
          const id = Number(node.getAttribute("data-section-id"));
          if (!Number.isFinite(id) || id <= 0) continue;
          node.checked = false;
          togglePlatiCategoryCascade(Math.floor(id), false);
        }
        updatePlatiSectionOptions();
      });
      $("pmIncludeDemand").addEventListener("change", (event) => {
        if (event.target.checked) $("pmIncludeReviews").checked = true;
      });
      $("saveNetworkSettingsBtn").addEventListener("click", async () => {
        try {
          await saveNetworkSettings();
        } catch (err) {
          renderNetworkSettingsStatus(
            `${currentUiLocale === "en" ? "Save failed" : "Ошибка сохранения"}: ${String(err)}`,
            "err"
          );
        }
      });
      $("reloadNetworkSettingsBtn").addEventListener("click", async () => {
        try {
          await loadNetworkSettings();
        } catch (err) {
          renderNetworkSettingsStatus(
            `${currentUiLocale === "en" ? "Load failed" : "Ошибка загрузки"}: ${String(err)}`,
            "err"
          );
        }
      });
      $("proxyFallbackContinueBtn").addEventListener("click", () => closeProxyFallbackModal(true));
      $("proxyFallbackCancelBtn").addEventListener("click", () => closeProxyFallbackModal(false));
      $("proxyFallbackModal").addEventListener("click", (event) => {
        if (event.target === $("proxyFallbackModal")) {
          closeProxyFallbackModal(false);
        }
      });
      $("pkTabMainBtn").addEventListener("click", () => setPlayerOkTab("main"));
      $("pkTabSettingsBtn").addEventListener("click", () => setPlayerOkTab("settings"));
      $("pkAdvancedMode").addEventListener("change", syncPlayerOkAdvancedState);
      $("sellerFocusApplyFilterBtn").addEventListener("click", () => {
        if (!sellerFocusPoint) return;
        const queryValue = sellerFocusPoint.seller_id !== null && sellerFocusPoint.seller_id !== undefined
          ? String(sellerFocusPoint.seller_id)
          : String(sellerFocusPoint.seller_name || "").trim();
        if (!queryValue) return;
        $("offersSellerQuery").value = queryValue;
        offersPage = 0;
        renderOffersTable();
        setStatus(
          currentUiLocale === "en"
            ? `Seller filter applied: ${sellerFocusPoint.seller_name}`
            : `Фильтр продавца применен: ${sellerFocusPoint.seller_name}`,
          "ok"
        );
      });
      $("sellerFocusClearBtn").addEventListener("click", () => {
        sellerFocusPoint = null;
        renderSellerFocusTable();
      });
      $("applyOfferFiltersBtn").addEventListener("click", () => {
        offersPage = 0;
        renderOffersTable();
      });
      $("offersSort").addEventListener("change", () => {
        offersPage = 0;
        renderOffersTable();
      });
      $("offersPageSize").addEventListener("change", () => {
        offersPage = 0;
        renderOffersTable();
      });
      $("resetOfferFiltersBtn").addEventListener("click", () => {
        $("offersPriceMin").value = "";
        $("offersPriceMax").value = "";
        $("offersMinReviews").value = "";
        $("offersSellerQuery").value = "";
        $("offersOnlineOnly").checked = false;
        $("offersAutoOnly").checked = false;
        $("offersSort").value = "price_asc";
        $("offersPageSize").value = "40";
        offersPage = 0;
        renderOffersTable();
      });
      $("exportOffersCsvBtn").addEventListener("click", exportFilteredOffersCsv);
      $("offersPrevBtn").addEventListener("click", () => {
        if (offersPage > 0) {
          offersPage -= 1;
          renderOffersTable();
        }
      });
      $("offersNextBtn").addEventListener("click", () => {
        const pageSizeRaw = Number($("offersPageSize").value);
        const pageSize = Number.isFinite(pageSizeRaw) && pageSizeRaw > 0 ? Math.floor(pageSizeRaw) : 40;
        const filteredCount = (filteredOffers || []).length;
        const maxPage = Math.max(0, Math.ceil(filteredCount / pageSize) - 1);
        if (offersPage < maxPage) {
          offersPage += 1;
          renderOffersTable();
        }
      });
      bindCharts();
    }

    async function bootstrap() {
      const savedLocale = localStorage.getItem("marketstat_v2_ui_locale");
      if (savedLocale === "ru" || savedLocale === "en") {
        $("uiLocale").value = savedLocale;
      }
      applyUiLocale();
      routeVisibility();
      applyFormMode();
      setPlayerOkTab("main");
      syncPlayerOkAdvancedState();
      attachEvents();
      let marketplaceCatalogSource = "default";
      try {
        marketplaceCatalogSource = await loadMarketplaces({ force: false, allowNetwork: true });
      } catch (_) {
        marketplaceCatalogSource = await loadMarketplaces({ force: false, allowNetwork: false });
      }
      setWorkspaceTab(activeWorkspaceTab, { save: false, autoLoad: false });
      try {
        const urlRunId = new URLSearchParams(window.location.search).get("run_id");
        const savedRunId = localStorage.getItem("marketstat_v2_last_run");
        selectedRunId = urlRunId || savedRunId || null;
        if (selectedRunId) runIdInput.value = selectedRunId;
        updateNavLinks();

        renderNetworkSettingsStatus(
          currentUiLocale === "en"
            ? "Network settings are not loaded yet. Click 'Reload from DB' to fetch."
            : "Сетевые настройки ещё не загружены. Нажмите «Перезагрузить из БД»."
        );
        renderCatalogLoadStatus(
          marketplaceCatalogSource === "network"
            ? (currentUiLocale === "en" ? "Catalogs are loaded on demand" : "Каталоги загружаются по запросу")
            : (
              marketplaceCatalogSource === "local_cache" || marketplaceCatalogSource === "local_cache_stale"
                ? (currentUiLocale === "en" ? "Catalogs are loaded from browser cache" : "Каталоги загружены из кэша браузера")
                : (currentUiLocale === "en" ? "Catalog fallback is active (local defaults)" : "Каталог площадок загружен из локального fallback")
            ),
          marketplaceCatalogSource === "network"
            ? "warn"
            : (marketplaceCatalogSource === "default" ? "warn" : "ok")
        );

        if (selectedRunId) {
          await loadRunStatus(selectedRunId);
        } else {
          setStatus(currentUiLocale === "en" ? "Ready for a new run." : "Готово к запуску нового анализа.", "ok");
        }

        if (isPlatiRoute) {
          setPlatiScopeMode(getPlatiScopeMode(), { ensureData: false }).catch(() => null);
        }
      } catch (err) {
        setStatus(err instanceof ApiError ? err.message : String(err), "err");
      }
    }

    bootstrap();
