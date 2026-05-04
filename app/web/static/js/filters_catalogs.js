    function selectMarketplaceValues() {
      if (isFunPayRoute) {
        return ["funpay"];
      }
      if (isPlayerOkRoute) {
        return ["playerok"];
      }
      if (isGgSellRoute) {
        return ["ggsell"];
      }
      if (isPlatiRoute) {
        return ["platimarket"];
      }
      const checked = Array.from(document.querySelectorAll("input[name='marketplace']:checked"))
        .map((item) => item.value);
      return checked;
    }

    function buildPayload() {
      const marketplaces = selectMarketplaceValues();
      const uiLocale = $("uiLocale").value || "ru";
      const payload = {
        marketplaces,
        common_filters: {
          query: $("query").value.trim(),
          currency: $("currency").value,
          ui_locale: uiLocale,
          force_refresh: $("forceRefresh").checked,
          allow_direct_fallback: allowDirectFallbackSession,
          execution: $("execution").value,
        },
        marketplace_filters: {},
      };
      if (marketplaces.includes("funpay")) {
        const selectedSections = readSelectedSectionIds();
        const selectedGameId = readInt("fpGame");
        const useGameScope = $("fpUseGameScope").checked;
        const gameScopeId = useGameScope
          ? selectedGameId
          : (selectedSections.length > 0 ? null : selectedGameId);
        payload.marketplace_filters.funpay = {
          content_locale: $("fpContentLocale").value,
          category_game_id: gameScopeId,
          category_id: selectedSections.length ? selectedSections[0] : null,
          category_ids: selectedSections,
          options: {
            profile: $("fpProfile").value,
            include_reviews: $("fpIncludeReviews").checked,
            include_demand_index: $("fpIncludeDemand").checked,
            include_fallback_scan: $("fpIncludeFallback").checked,
            section_limit: readInt("fpSectionLimit"),
            seller_limit: readInt("fpSellerLimit"),
            review_pages_per_seller: readInt("fpReviewPages"),
            history_points_limit: readInt("fpHistoryPoints"),
          },
        };
      }
      if (marketplaces.includes("playerok")) {
        const selectedSections = readSelectedPlayerOkSectionSlugs();
        const manualSections = parseLines("pkSectionsCustom")
          .map((item) => String(item || "").trim().replace(/^\/+|\/+$/g, ""))
          .filter((item) => item.length > 0);
        const selectedSectionsSet = new Set([...selectedSections, ...manualSections]);
        const manualGameSlug = String($("pkGameCustom").value || "").trim().replace(/^\/+|\/+$/g, "");
        const selectedGameSlug = manualGameSlug || String($("pkGame").value || "").trim() || null;
        const advancedMode = Boolean($("pkAdvancedMode").checked);
        const advancedHeaders = advancedMode ? parseLooseObject("pkAdvancedHeaders", ":") : {};
        const advancedCookies = advancedMode ? parseLooseObject("pkAdvancedCookies", "=") : {};
        payload.marketplace_filters.playerok = {
          category_game_slug: selectedGameSlug,
          category_slugs: [...selectedSectionsSet].sort((left, right) => left.localeCompare(right)),
          use_game_scope: $("pkUseGameScope").value !== "false",
          use_html_degrade: $("pkUseHtmlDegrade").checked,
          options: {
            profile: $("pkProfile").value,
            include_reviews: $("pkIncludeReviews").checked,
            include_demand_index: $("pkIncludeDemand").checked,
            include_fallback_scan: $("pkIncludeFallback").checked,
            section_limit: readInt("pkSectionLimit"),
            seller_limit: readInt("pkSellerLimit"),
            review_pages_per_seller: readInt("pkReviewPages"),
            history_points_limit: readInt("pkHistoryPoints"),
          },
        };
        if (Object.keys(advancedHeaders).length > 0) {
          payload.marketplace_filters.playerok.advanced_headers = advancedHeaders;
        }
        if (Object.keys(advancedCookies).length > 0) {
          payload.marketplace_filters.playerok.advanced_cookies = advancedCookies;
        }
      }
      if (marketplaces.includes("ggsell")) {
        const selectedSlugs = readSelectedGgSellCategorySlugs();
        const manualSlugs = parseLines("gsCategoriesCustom")
          .map((item) => String(item || "").trim().replace(/^\/+|\/+$/g, ""))
          .filter((item) => item.length > 0);
        const categorySlugs = [...new Set([...selectedSlugs, ...manualSlugs])]
          .sort((left, right) => left.localeCompare(right));
        const typeSlug = String($("gsType").value || "").trim() || null;
        payload.marketplace_filters.ggsell = {
          category_type_slug: typeSlug,
          category_slugs: categorySlugs,
          use_type_scope: $("gsUseTypeScope").value !== "false",
          options: {
            profile: $("gsProfile").value,
            include_reviews: $("gsIncludeReviews").checked,
            include_demand_index: $("gsIncludeDemand").checked,
            include_fallback_scan: $("gsIncludeFallback").checked,
            section_limit: readInt("gsSectionLimit"),
            seller_limit: readInt("gsSellerLimit"),
            review_pages_per_seller: readInt("gsReviewPages"),
            history_points_limit: readInt("gsHistoryPoints"),
          },
        };
      }
      if (marketplaces.includes("platimarket")) {
        const scopeMode = getPlatiScopeMode();
        const requestedSectionLimit = readInt("pmSectionLimit");
        const optionsPayload = {
          profile: $("pmProfile").value,
          include_reviews: $("pmIncludeReviews").checked,
          include_demand_index: $("pmIncludeDemand").checked,
          include_fallback_scan: $("pmIncludeFallback").checked,
          section_limit: requestedSectionLimit,
          seller_limit: readInt("pmSellerLimit"),
          review_pages_per_seller: readInt("pmReviewPages"),
          history_points_limit: readInt("pmHistoryPoints"),
        };
        if (scopeMode === "catalog") {
          const selectedSections = readSelectedPlatiCategoryIds();
          const effectiveSectionLimit = (
            requestedSectionLimit !== null
              ? requestedSectionLimit
              : (selectedSections.length > 0 ? selectedSections.length : null)
          );
          payload.marketplace_filters.platimarket = {
            category_game_id: null,
            category_game_slug: null,
            category_game_name: null,
            game_category_ids: [],
            category_group_id: null,
            category_ids: selectedSections,
            use_game_scope: false,
            use_group_scope: false,
            options: {
              ...optionsPayload,
              section_limit: effectiveSectionLimit,
            },
          };
        } else {
          const selectedGameCategoryIds = readSelectedPlatiGameCategoryIds();
          const selectedGameIdFromSelect = readInt("pmGame");
          const manualGame = parsePlatiGameOverride($("pmGameCustom").value);
          const selectedGame = Array.isArray(platiGamesCatalog)
            ? platiGamesCatalog.find((item) => Number(item.game_id) === selectedGameIdFromSelect)
            : null;
          let selectedGameId = selectedGameIdFromSelect;
          let selectedGameSlug = selectedGame ? String(selectedGame.game_slug || "") : null;
          let selectedGameName = selectedGame ? String(selectedGame.game_name || "") : null;
          if (manualGame.gameId !== null || manualGame.gameSlug !== null) {
            if (manualGame.gameId !== null) {
              selectedGameId = manualGame.gameId;
            }
            if (manualGame.gameSlug) {
              selectedGameSlug = manualGame.gameSlug;
            }
            if (Array.isArray(platiGamesCatalog) && platiGamesCatalog.length > 0) {
              const byId = selectedGameId !== null
                ? platiGamesCatalog.find((item) => Number(item.game_id) === selectedGameId)
                : null;
              const bySlug = selectedGameSlug
                ? platiGamesCatalog.find((item) => String(item.game_slug || "").toLowerCase() === selectedGameSlug)
                : null;
              const matched = byId || bySlug || null;
              if (matched) {
                if (selectedGameId === null) selectedGameId = Number(matched.game_id);
                if (!selectedGameSlug) selectedGameSlug = String(matched.game_slug || "") || null;
                selectedGameName = String(matched.game_name || "") || selectedGameName;
              }
            }
          }
          payload.marketplace_filters.platimarket = {
            category_game_id: selectedGameId,
            category_game_slug: selectedGameSlug,
            category_game_name: selectedGameName,
            game_category_ids: selectedGameCategoryIds,
            category_group_id: null,
            category_ids: [],
            use_game_scope: true,
            use_group_scope: false,
            options: optionsPayload,
          };
        }
      }
      return payload;
    }

    function renderMarketplaceChecks() {
      const host = $("marketplaceChecks");
      if (!marketplacesCatalog.length) {
        host.innerHTML = "<div class='muted'>Площадки пока не загружены.</div>";
        return;
      }
      const previousSelected = new Set(
        Array.from(document.querySelectorAll("input[name='marketplace']:checked"))
          .map((item) => String(item.value || ""))
          .filter((value) => value.length > 0)
      );
      if (previousSelected.size === 0 && !isMarketplaceRoute) {
        previousSelected.add("funpay");
      }
      host.innerHTML = marketplacesCatalog.map((item) => {
        const disabled = !item.enabled;
        const checked = previousSelected.has(item.slug) ? "checked" : "";
        const badge = item.enabled
          ? '<span class="badge enabled">enabled</span>'
          : '<span class="badge disabled">disabled</span>';
        const capabilities = Array.isArray(item.capabilities) && item.capabilities.length
          ? `<div class="hint">caps: ${escapeHtml(item.capabilities.join(", "))}</div>`
          : "";
        const source = item.data_source
          ? `<div class="hint">source: ${escapeHtml(item.data_source)}</div>`
          : "";
        const demandMode = item.demand_mode
          ? `<div class="hint">demand: ${escapeHtml(item.demand_mode)}</div>`
          : "";
        const reason = item.reason ? `<div class="hint">${escapeHtml(item.reason)}</div>` : "";
        return `
          <label class="check">
            <input type="checkbox" name="marketplace" value="${escapeHtml(item.slug)}" ${checked} ${disabled ? "disabled" : ""} />
            <div>
              <div><b>${escapeHtml(item.label)}</b> <span class="muted">(${escapeHtml(item.slug)})</span> ${badge}</div>
              ${capabilities}
              ${source}
              ${demandMode}
              ${reason}
            </div>
          </label>
        `;
      }).join("");
      syncOverviewMarketplaceFilterBlocks();
    }

    function renderNetworkSettingsStatus(text, mode = "") {
      const node = $("networkSettingsStatus");
      if (!node) return;
      node.textContent = text;
      node.className = `hint ${mode}`.trim();
    }

    async function loadNetworkSettings() {
      const data = await fetchJson("/v2/settings/network");
      $("commonDcProxy").value = (data.datacenter_proxies || []).join("\n");
      $("commonResProxy").value = (data.residential_proxies || []).join("\n");
      $("commonMobProxy").value = (data.mobile_proxies || []).join("\n");
      const updated = data.updated_at ? new Date(data.updated_at).toLocaleString(localeTag()) : "—";
      renderNetworkSettingsStatus(
        currentUiLocale === "en"
          ? `Network settings loaded. Updated: ${updated}.`
          : `Сетевые настройки загружены. Обновлено: ${updated}.`,
        "ok"
      );
    }

    async function saveNetworkSettings() {
      const payload = {
        datacenter_proxies: parseLines("commonDcProxy"),
        residential_proxies: parseLines("commonResProxy"),
        mobile_proxies: parseLines("commonMobProxy"),
      };
      const data = await fetchJson("/v2/settings/network", {
        method: "PUT",
        headers: {
          "Content-Type": "application/json; charset=utf-8",
          "Accept": "application/json",
        },
        body: JSON.stringify(payload),
      });
      const updated = data.updated_at ? new Date(data.updated_at).toLocaleString(localeTag()) : "—";
      renderNetworkSettingsStatus(
        currentUiLocale === "en"
          ? `Network settings saved. Updated: ${updated}.`
          : `Сетевые настройки сохранены. Обновлено: ${updated}.`,
        "ok"
      );
    }

    async function ensureMarketplaceCatalogLoaded(slug, { force = false } = {}) {
      if (slug === "funpay" && (force || !funpayCatalogLoaded)) {
        await loadFunPayCategories({ force });
        return;
      }
      if (slug === "playerok" && (force || !playerokCatalogLoaded)) {
        await loadPlayerOkCategories({ force });
        return;
      }
      if (slug === "ggsell" && (force || !ggsellCatalogLoaded)) {
        await loadGgSellCategories({ force });
        return;
      }
      if (slug === "platimarket") {
        if (force || !platiCatalogTreeLoaded) {
          await loadPlatiCategories({ force });
        }
        if (getPlatiScopeMode() === "game" && (force || !platiGamesLoaded)) {
          await loadPlatiGames({ force });
        }
      }
    }

    async function loadSelectedCatalogs({ force = false } = {}) {
      if (isCatalogLoading) return;
      isCatalogLoading = true;
      renderCatalogLoadStatus(
        currentUiLocale === "en" ? "Loading catalogs..." : "Загрузка каталогов...",
        "warn"
      );
      try {
        await loadMarketplaces({ force });
        const selected = new Set(selectMarketplaceValues());
        if (selected.size === 0) {
          renderCatalogLoadStatus(
            currentUiLocale === "en" ? "No marketplaces selected" : "Площадки не выбраны",
            "warn"
          );
          return;
        }
        const tasks = [...selected].map((slug) => ensureMarketplaceCatalogLoaded(slug, { force }));
        await Promise.all(tasks);
        renderCatalogLoadStatus(
          currentUiLocale === "en" ? `Catalogs loaded for: ${[...selected].join(", ")}` : `Каталоги загружены: ${[...selected].join(", ")}`,
          "ok"
        );
      } catch (err) {
        renderCatalogLoadStatus(
          `${currentUiLocale === "en" ? "Catalog loading failed" : "Ошибка загрузки каталогов"}: ${String(err)}`,
          "err"
        );
        throw err;
      } finally {
        isCatalogLoading = false;
      }
    }

    function syncOverviewMarketplaceFilterBlocks() {
      if (typeof syncMarketplaceFilterUx === "function") {
        syncMarketplaceFilterUx();
      }
    }

    function syncHistoryMarketplaceFilter() {
      const select = $("historyMarketplaceFilter");
      const current = select.value;
      const options = ['<option value="">Все площадки</option>'];
      for (const item of marketplacesCatalog) {
        options.push(`<option value="${escapeHtml(item.slug)}">${escapeHtml(item.label)} (${escapeHtml(item.slug)})</option>`);
      }
      select.innerHTML = options.join("");
      if ([...select.options].some((opt) => opt.value === current)) {
        select.value = current;
      }
    }

    function updateSectionOptions() {
      syncSectionSelectionFromDom();
      const gameId = readInt("fpGame");
      const searchValue = String($("fpSectionSearch").value || "").trim().toLowerCase();
      const options = [];
      const seen = new Set();
      if (gameId !== null) {
        const game = categoriesCatalog.find((item) => Number(item.game_section_id) === gameId);
        if (game) {
          for (const section of game.sections || []) {
            const sectionId = Number(section.section_id);
            if (!Number.isFinite(sectionId) || sectionId <= 0 || seen.has(sectionId)) continue;
            seen.add(sectionId);
            options.push({
              section_id: Math.floor(sectionId),
              full_name: section.full_name,
              game_name: game.game_name,
            });
          }
        }
      } else {
        for (const game of categoriesCatalog) {
          for (const section of game.sections || []) {
            const sectionId = Number(section.section_id);
            if (!Number.isFinite(sectionId) || sectionId <= 0 || seen.has(sectionId)) continue;
            seen.add(sectionId);
            options.push({
              section_id: Math.floor(sectionId),
              full_name: section.full_name,
              game_name: game.game_name,
            });
          }
        }
      }

      const filtered = searchValue
        ? options.filter((item) => `${item.full_name} ${item.game_name}`.toLowerCase().includes(searchValue))
        : options;
      const checklist = $("fpSectionChecklist");
      if (!filtered.length) {
        checklist.innerHTML = `<div class="section-empty">${currentUiLocale === "en" ? "No sections found by current filter." : "По текущему фильтру разделы не найдены."}</div>`;
        updateSectionSelectedCount();
        return;
      }
      checklist.innerHTML = filtered.map((item) => {
        const checked = selectedSectionIds.has(item.section_id) ? "checked" : "";
        return `
          <label class="check section-item">
            <input type="checkbox" data-section-id="${item.section_id}" ${checked} />
            <div>
              <div><b>${escapeHtml(item.full_name)}</b></div>
              <div class="hint">${escapeHtml(item.game_name)}</div>
            </div>
          </label>
        `;
      }).join("");
      updateSectionSelectedCount();
    }

    function updatePlayerOkSectionOptions() {
      syncPlayerOkSectionSelectionFromDom();
      const gameSlug = String($("pkGame").value || "").trim();
      const searchValue = String($("pkSectionSearch").value || "").trim().toLowerCase();
      const options = [];
      const seen = new Set();
      if (gameSlug) {
        const game = playerokCategoriesCatalog.find((item) => String(item.game_slug) === gameSlug);
        if (game) {
          for (const section of game.sections || []) {
            const slug = String(section.section_slug || "").trim();
            if (!slug || seen.has(slug)) continue;
            seen.add(slug);
            options.push({
              section_slug: slug,
              full_name: section.full_name || slug,
              game_name: game.game_name || gameSlug,
            });
          }
        }
      } else {
        for (const game of playerokCategoriesCatalog) {
          for (const section of game.sections || []) {
            const slug = String(section.section_slug || "").trim();
            if (!slug || seen.has(slug)) continue;
            seen.add(slug);
            options.push({
              section_slug: slug,
              full_name: section.full_name || slug,
              game_name: game.game_name || game.game_slug,
            });
          }
        }
      }

      const filtered = searchValue
        ? options.filter((item) => `${item.full_name} ${item.game_name}`.toLowerCase().includes(searchValue))
        : options;
      const checklist = $("pkSectionChecklist");
      if (!filtered.length) {
        checklist.innerHTML = `<div class="section-empty">${currentUiLocale === "en" ? "No sections found by current filter." : "По текущему фильтру разделы не найдены."}</div>`;
        updatePlayerOkSectionSelectedCount();
        return;
      }
      checklist.innerHTML = filtered.map((item) => {
        const checked = selectedPlayerOkSectionSlugs.has(item.section_slug) ? "checked" : "";
        return `
          <label class="check section-item">
            <input type="checkbox" data-section-slug="${escapeHtml(item.section_slug)}" ${checked} />
            <div>
              <div><b>${escapeHtml(item.full_name)}</b></div>
              <div class="hint">${escapeHtml(item.game_name)}</div>
            </div>
          </label>
        `;
      }).join("");
      updatePlayerOkSectionSelectedCount();
    }

    function renderGgSellTypesSelect() {
      const select = $("gsType");
      if (!select) return;
      const current = String(select.value || "").trim();
      const options = [
        `<option value="">${currentUiLocale === "en" ? "Any type" : "Любой тип"}</option>`,
      ];
      const sorted = [...(ggsellTypesCatalog || [])].sort((left, right) =>
        String(left.type_name || left.type_slug || "").localeCompare(String(right.type_name || right.type_slug || ""), localeTag())
      );
      for (const item of sorted) {
        const slug = String(item.type_slug || "").trim();
        if (!slug) continue;
        const name = String(item.type_name || slug).trim();
        options.push(`<option value="${escapeHtml(slug)}">${escapeHtml(name)}</option>`);
      }
      select.innerHTML = options.join("");
      const hasCurrent = sorted.some((item) => String(item.type_slug || "").trim() === current);
      select.value = hasCurrent ? current : "";
    }

    function updateGgSellCategoryOptions() {
      syncGgSellCategorySelectionFromDom();
      const typeSlug = String($("gsType").value || "").trim();
      const searchValue = String($("gsCategorySearch").value || "").trim().toLowerCase();
      let options = Array.isArray(ggsellCategoriesCatalog) ? [...ggsellCategoriesCatalog] : [];
      if (typeSlug) {
        options = options.filter((item) => String(item.type_slug || "").trim() === typeSlug);
      }
      if (searchValue) {
        options = options.filter((item) => {
          const haystack = [
            item.category_name,
            item.category_slug,
            item.type_name,
            item.parent_name,
          ].map((part) => String(part || "").toLowerCase()).join(" ");
          return haystack.includes(searchValue);
        });
      }
      options.sort((left, right) => {
        const lType = String(left.type_name || left.type_slug || "");
        const rType = String(right.type_name || right.type_slug || "");
        const typeCmp = lType.localeCompare(rType, localeTag());
        if (typeCmp !== 0) return typeCmp;
        return String(left.category_name || left.category_slug || "")
          .localeCompare(String(right.category_name || right.category_slug || ""), localeTag());
      });
      const checklist = $("gsCategoryChecklist");
      if (!options.length) {
        checklist.innerHTML = `<div class="section-empty">${currentUiLocale === "en" ? "No categories found by current filter." : "По текущему фильтру категории не найдены."}</div>`;
        updateGgSellCategorySelectedCount();
        return;
      }
      checklist.innerHTML = options.map((item) => {
        const slug = String(item.category_slug || "").trim();
        if (!slug) return "";
        const checked = selectedGgSellCategorySlugs.has(slug) ? "checked" : "";
        const typeName = String(item.type_name || item.type_slug || "—");
        const parentName = String(item.parent_name || "").trim();
        const offers = item.offers_count === null || item.offers_count === undefined ? "—" : formatNum(item.offers_count);
        const hints = [
          parentName ? `${currentUiLocale === "en" ? "Parent" : "Родитель"}: ${parentName}` : null,
          `${currentUiLocale === "en" ? "Type" : "Тип"}: ${typeName}`,
          `${currentUiLocale === "en" ? "offers" : "офферов"}: ${offers}`,
        ].filter(Boolean).join(" · ");
        return `
          <label class="check section-item">
            <input type="checkbox" data-category-slug="${escapeHtml(slug)}" ${checked} />
            <div>
              <div><b>${escapeHtml(item.category_name || slug)}</b></div>
              <div class="hint">${escapeHtml(hints)}</div>
            </div>
          </label>
        `;
      }).join("");
      updateGgSellCategorySelectedCount();
    }

    function getPlatiCatalogSectionsFlat() {
      const rows = [];
      const walk = (node) => {
        if (!node || typeof node !== "object") return;
        const id = Number(node.section_id);
        if (Number.isFinite(id) && id > 0) {
          const path = Array.isArray(node.path) ? node.path.filter((part) => String(part || "").trim()) : [];
          const title = String(node.title || path[path.length - 1] || `ID_R #${id}`).trim();
          rows.push({
            section_id: Math.floor(id),
            section_slug: String(node.section_slug || "").trim(),
            section_name: title,
            full_name: path.length ? path.join(" > ") : title,
            counter_total: (node.cnt === null || node.cnt === undefined) ? null : Number(node.cnt),
            url: String(node.url || "").trim(),
            children: Array.isArray(node.children) ? node.children : [],
          });
        }
        for (const child of Array.isArray(node.children) ? node.children : []) {
          walk(child);
        }
      };
      for (const root of platiCatalogTree || []) {
        walk(root);
      }
      rows.sort((left, right) => `${left.full_name}`.localeCompare(`${right.full_name}`));
      return rows;
    }

    function getPlatiTreeIndex() {
      const byId = new Map();
      const childrenById = new Map();
      const walk = (node) => {
        if (!node || typeof node !== "object") return;
        const id = Number(node.section_id);
        if (!Number.isFinite(id) || id <= 0) return;
        byId.set(id, node);
        const children = (Array.isArray(node.children) ? node.children : [])
          .map((child) => Number(child.section_id))
          .filter((childId) => Number.isFinite(childId) && childId > 0);
        childrenById.set(id, children);
        for (const child of Array.isArray(node.children) ? node.children : []) {
          walk(child);
        }
      };
      for (const root of platiCatalogTree || []) {
        walk(root);
      }
      return { byId, childrenById };
    }

    function collectPlatiDescendants(sectionId, childrenById, acc = new Set()) {
      if (acc.has(sectionId)) return acc;
      acc.add(sectionId);
      const children = childrenById.get(sectionId) || [];
      for (const childId of children) {
        collectPlatiDescendants(childId, childrenById, acc);
      }
      return acc;
    }

    function matchesPlatiTreeNode(node, query) {
      if (!query) return true;
      const sectionId = Number(node.section_id);
      const haystack = [
        node.title,
        node.section_slug,
        (Array.isArray(node.path) ? node.path.join(" ") : ""),
        Number.isFinite(sectionId) ? String(sectionId) : "",
      ].join(" ").toLowerCase();
      return haystack.includes(query);
    }

    function renderPlatiTreeNode(node, searchValue) {
      const sectionId = Number(node.section_id);
      if (!Number.isFinite(sectionId) || sectionId <= 0) return { html: "", visible: false };
      const children = Array.isArray(node.children) ? node.children : [];
      const renderedChildren = children.map((child) => renderPlatiTreeNode(child, searchValue));
      const visibleChildren = renderedChildren.filter((item) => item.visible);
      const selfVisible = matchesPlatiTreeNode(node, searchValue);
      const visible = !searchValue || selfVisible || visibleChildren.length > 0;
      if (!visible) return { html: "", visible: false };
      const checked = selectedPlatiCategoryIds.has(sectionId) ? "checked" : "";
      const title = String(node.title || `ID_R #${sectionId}`);
      const path = Array.isArray(node.path) ? node.path.filter((part) => String(part || "").trim()) : [];
      const fullName = path.length ? path.join(" > ") : title;
      const hasCount = Number.isFinite(Number(node.cnt)) && Number(node.cnt) > 0;
      const hasChildren = children.length > 0;
      const shouldOpen = searchValue
        ? true
        : (platiTreeExpandedIds.has(sectionId) || path.length <= 1);
      const childHtml = visibleChildren.map((item) => item.html).join("");
      const detailsHtml = hasChildren
        ? `<details data-tree-id="${sectionId}" ${shouldOpen ? "open" : ""}>
            <summary>${currentUiLocale === "en" ? "Subsections" : "Подразделы"}: ${visibleChildren.length}</summary>
            <div class="tree-children">${childHtml}</div>
          </details>`
        : "";
      return {
        visible: true,
        html: `
          <div class="tree-node">
            <label class="tree-row">
              <input type="checkbox" data-section-id="${sectionId}" ${checked} />
              <div>
                <div><b>${escapeHtml(fullName)}</b></div>
                <div class="tree-meta">ID_R: ${sectionId}${hasCount ? ` · ${currentUiLocale === "en" ? "offers" : "офферов"}: ${formatNum(node.cnt)}` : ""}</div>
              </div>
            </label>
            ${detailsHtml}
          </div>
        `,
      };
    }

    function updatePlatiSectionOptions() {
      syncPlatiSectionSelectionFromDom();
      const searchValue = String($("pmSectionSearch").value || "").trim().toLowerCase();
      const checklist = $("pmSectionChecklist");
      const rendered = (platiCatalogTree || []).map((root) => renderPlatiTreeNode(root, searchValue));
      const visible = rendered.filter((item) => item.visible);
      if (!visible.length) {
        checklist.innerHTML = `<div class="section-empty">${currentUiLocale === "en" ? "No sections found by current filter." : "По текущему фильтру разделы не найдены."}</div>`;
        updatePlatiSectionSelectedCount();
        return;
      }
      checklist.innerHTML = visible.map((item) => item.html).join("");
      updatePlatiSectionSelectedCount();
    }

    function updatePlatiGameCategoryOptions() {
      syncPlatiGameCategorySelectionFromDom();
      const searchValue = String($("pmGameCategorySearch").value || "").trim().toLowerCase();
      const options = Array.isArray(platiGameCategoriesCatalog) ? platiGameCategoriesCatalog : [];
      const filtered = searchValue
        ? options.filter((item) => String(item.category_name || "").toLowerCase().includes(searchValue))
        : options;
      const checklist = $("pmGameCategoryChecklist");
      if (!filtered.length) {
        checklist.innerHTML = `<div class="section-empty">${currentUiLocale === "en" ? "No game categories loaded." : "Категории игры не загружены."}</div>`;
        updatePlatiGameCategorySelectedCount();
        return;
      }
      checklist.innerHTML = filtered.map((item) => {
        const id = Number(item.category_id);
        if (!Number.isFinite(id) || id < 0) return "";
        const checked = selectedPlatiGameCategoryIds.has(id) ? "checked" : "";
        const offers = item.offers_count === null || item.offers_count === undefined
          ? "—"
          : formatNum(item.offers_count);
        return `
          <label class="check section-item">
            <input type="checkbox" data-game-category-id="${id}" ${checked} />
            <div>
              <div><b>${escapeHtml(item.category_name || `ID_C #${id}`)}</b></div>
              <div class="hint">ID_C: ${id} · ${currentUiLocale === "en" ? "offers" : "офферов"}: ${offers}</div>
            </div>
          </label>
        `;
      }).join("");
      updatePlatiGameCategorySelectedCount();
    }

    function sourceLabel(source) {
      if (source === "cache") return currentUiLocale === "en" ? "API cache" : "кэш API";
      if (source === "local_cache") return currentUiLocale === "en" ? "browser cache" : "кэш браузера";
      if (source === "local_cache_stale") return currentUiLocale === "en" ? "stale browser cache" : "устаревший кэш браузера";
      if (source === "network") return currentUiLocale === "en" ? "network" : "сеть";
      return source || (currentUiLocale === "en" ? "unknown" : "неизвестно");
    }

    async function loadMarketplaces({ force = false, allowNetwork = true } = {}) {
      const cacheKey = "marketplaces";
      if (!force) {
        const cached = localCacheRead(cacheKey);
        if (cached && Array.isArray(cached.items) && cached.items.length > 0) {
          marketplacesCatalog = cached.items;
          renderMarketplaceChecks();
          syncHistoryMarketplaceFilter();
          return "local_cache";
        }
      } else {
        localCacheDrop(cacheKey);
      }

      if (!allowNetwork) {
        marketplacesCatalog = [...DEFAULT_MARKETPLACES];
        renderMarketplaceChecks();
        syncHistoryMarketplaceFilter();
        return "default";
      }
      try {
        const data = await fetchJson("/v2/marketplaces");
        marketplacesCatalog = Array.isArray(data.items) && data.items.length
          ? data.items
          : [...DEFAULT_MARKETPLACES];
        localCacheWrite(cacheKey, { items: marketplacesCatalog });
        renderMarketplaceChecks();
        syncHistoryMarketplaceFilter();
        return "network";
      } catch (_) {
        if (!force) {
          const stale = localCacheRead(cacheKey, Number.MAX_SAFE_INTEGER);
          if (stale && Array.isArray(stale.items) && stale.items.length > 0) {
            marketplacesCatalog = stale.items;
            renderMarketplaceChecks();
            syncHistoryMarketplaceFilter();
            return "local_cache_stale";
          }
        }
        marketplacesCatalog = [...DEFAULT_MARKETPLACES];
        renderMarketplaceChecks();
        syncHistoryMarketplaceFilter();
        return "default";
      }
    }

    async function loadFunPayCategories({ force = false } = {}) {
      const cacheKey = "funpay_categories";
      let source = "network";
      if (!force) {
        const cached = localCacheRead(cacheKey);
        if (cached && Array.isArray(cached.games)) {
          categoriesCatalog = cached.games;
          source = "local_cache";
        }
      } else {
        localCacheDrop(cacheKey);
      }
      if (source !== "local_cache") {
        const data = await runWithProxyFallback(() =>
          fetchJson(withDirectFallbackQuery(`/v2/marketplaces/funpay/categories?force_refresh=${force ? "true" : "false"}`))
        );
        categoriesCatalog = data.games || [];
        localCacheWrite(cacheKey, { games: categoriesCatalog });
      }
      selectedSectionIds = new Set();
      const options = ['<option value="">Любая игра</option>'];
      for (const game of categoriesCatalog) {
        options.push(`<option value="${game.game_section_id}">${escapeHtml(game.game_name)}</option>`);
      }
      $("fpGame").innerHTML = options.join("");
      updateSectionOptions();
      funpayCatalogLoaded = true;
      return source;
    }

    function normalizePlayerOkGames(rawGames, assumeLoaded = false) {
      const normalized = [];
      const seenGames = new Set();
      for (const game of rawGames) {
        const gameIdRaw = game?.game_id;
        const gameId = gameIdRaw === null || gameIdRaw === undefined ? null : String(gameIdRaw).trim() || null;
        const gameSlug = String(game?.game_slug || "").trim().replace(/^\/+|\/+$/g, "");
        const gameName = String(game?.game_name || "").trim();
        if (!gameSlug || !gameName || seenGames.has(gameSlug)) continue;
        seenGames.add(gameSlug);
        const sections = [];
        const seenSections = new Set();
        for (const section of Array.isArray(game?.sections) ? game.sections : []) {
          const sectionSlug = String(section?.section_slug || "").trim().replace(/^\/+|\/+$/g, "");
          if (!sectionSlug || seenSections.has(sectionSlug)) continue;
          seenSections.add(sectionSlug);
          const sectionIdRaw = section?.section_id;
          const sectionId = sectionIdRaw === null || sectionIdRaw === undefined ? null : String(sectionIdRaw).trim() || null;
          sections.push({
            section_id: sectionId,
            section_slug: sectionSlug,
            section_url: section?.section_url || `https://playerok.com/${sectionSlug}`,
            section_name: section?.section_name || sectionSlug,
            full_name: section?.full_name || `${gameName} > ${section?.section_name || sectionSlug}`,
          });
        }
        normalized.push({
          game_id: gameId,
          game_slug: gameSlug,
          game_name: gameName,
          game_url: game?.game_url || `https://playerok.com/${gameSlug}`,
          sections,
          sections_loaded: Boolean(assumeLoaded || game?.sections_loaded === true),
        });
      }
      return normalized;
    }

    function mergePlayerOkGames(games) {
      const bySlug = new Map((playerokCategoriesCatalog || []).map((item) => [String(item.game_slug), item]));
      for (const game of games || []) {
        const slug = String(game.game_slug || "");
        if (!slug) continue;
        const existing = bySlug.get(slug);
        if (!existing) {
          bySlug.set(slug, game);
          continue;
        }
        const existingSections = Array.isArray(existing.sections) ? existing.sections : [];
        const incomingSections = Array.isArray(game.sections) ? game.sections : [];
        const shouldReplaceSections = incomingSections.length > existingSections.length
          || (game.sections_loaded && !existing.sections_loaded);
        bySlug.set(slug, {
          ...existing,
          ...game,
          sections: shouldReplaceSections ? incomingSections : existingSections,
          sections_loaded: Boolean(existing.sections_loaded || game.sections_loaded),
        });
      }
      playerokCategoriesCatalog = [...bySlug.values()].sort((left, right) =>
        String(left.game_name || left.game_slug).localeCompare(String(right.game_name || right.game_slug))
      );
    }

    function renderPlayerOkGamesSelect() {
      selectedPlayerOkSectionSlugs = new Set();
      const options = ['<option value="">Любая игра</option>'];
      for (const game of playerokCategoriesCatalog) {
        options.push(`<option value="${escapeHtml(game.game_slug)}">${escapeHtml(game.game_name)}</option>`);
      }
      $("pkGame").innerHTML = options.join("");
      updatePlayerOkSectionOptions();
    }

    async function ensurePlayerOkGameSections(gameSlug) {
      const slug = String(gameSlug || "").trim().replace(/^\/+|\/+$/g, "");
      if (!slug) return;
      const current = playerokCategoriesCatalog.find((item) => String(item.game_slug) === slug);
      if (current && current.sections_loaded && Array.isArray(current.sections) && current.sections.length > 0) {
        return;
      }
      const data = await runWithProxyFallback(() =>
        fetchJson(withDirectFallbackQuery(`/v2/marketplaces/playerok/categories?game_slug=${encodeURIComponent(slug)}`))
      );
      const loadedGames = normalizePlayerOkGames(Array.isArray(data.games) ? data.games : [], true);
      if (!loadedGames.length) return;
      mergePlayerOkGames(loadedGames);
    }

    async function loadPlayerOkCategories({ force = false } = {}) {
      const cacheKey = "playerok_categories";
      let source = "network";
      let payload = null;
      if (!force) {
        const cached = localCacheRead(cacheKey);
        if (cached && Array.isArray(cached.games)) {
          payload = cached;
          source = "local_cache";
        }
      } else {
        localCacheDrop(cacheKey);
      }
      if (!payload) {
        payload = await runWithProxyFallback(() =>
          fetchJson(withDirectFallbackQuery(`/v2/marketplaces/playerok/categories?force_refresh=${force ? "true" : "false"}`))
        );
        localCacheWrite(cacheKey, { games: payload.games || [] });
      }
      const normalized = normalizePlayerOkGames(Array.isArray(payload.games) ? payload.games : [], false);
      playerokCategoriesCatalog = [];
      mergePlayerOkGames(normalized);
      renderPlayerOkGamesSelect();
      $("pkCategoriesSource").textContent = currentUiLocale === "en"
        ? `Catalog source: ${sourceLabel(payload.source || source)}`
        : `Источник каталога: ${sourceLabel(payload.source || source)}`;
      playerokCatalogLoaded = true;
      return source;
    }

    async function loadGgSellCategories({ force = false } = {}) {
      const cacheKey = "ggsell_categories";
      let source = "network";
      let payload = null;
      if (!force) {
        const cached = localCacheRead(cacheKey);
        if (
          cached
          && Array.isArray(cached.types)
          && Array.isArray(cached.categories)
        ) {
          payload = cached;
          source = "local_cache";
        }
      } else {
        localCacheDrop(cacheKey);
      }
      if (!payload) {
        payload = await runWithProxyFallback(() =>
          fetchJson(withDirectFallbackQuery(`/v2/marketplaces/ggsell/categories?force_refresh=${force ? "true" : "false"}`))
        );
        localCacheWrite(cacheKey, { types: payload.types || [], categories: payload.categories || [] });
      }
      ggsellTypesCatalog = Array.isArray(payload.types) ? payload.types : [];
      ggsellCategoriesCatalog = Array.isArray(payload.categories) ? payload.categories : [];
      const validSlugs = new Set(
        ggsellCategoriesCatalog
          .map((item) => String(item.category_slug || "").trim())
          .filter((item) => item.length > 0)
      );
      selectedGgSellCategorySlugs = new Set(
        [...selectedGgSellCategorySlugs].filter((slug) => validSlugs.has(slug))
      );
      renderGgSellTypesSelect();
      updateGgSellCategoryOptions();
      $("gsCategoriesSource").textContent = currentUiLocale === "en"
        ? `Catalog source: ${sourceLabel(payload.source || source)}`
        : `Источник каталога: ${sourceLabel(payload.source || source)}`;
      ggsellCatalogLoaded = true;
      return source;
    }

    async function loadPlatiCategories({ force = false } = {}) {
      const cacheKey = "plati_catalog_tree";
      let source = "network";
      let payload = null;
      if (!force) {
        const cached = localCacheRead(cacheKey);
        if (cached && Array.isArray(cached.nodes)) {
          payload = cached;
          source = "local_cache";
        }
      } else {
        localCacheDrop(cacheKey);
      }
      if (!payload) {
        payload = await runWithProxyFallback(() =>
          fetchJson(withDirectFallbackQuery(`/v2/marketplaces/platimarket/catalog-tree?force_refresh=${force ? "true" : "false"}`))
        );
        localCacheWrite(cacheKey, { nodes: payload.nodes || [] });
      }
      platiCatalogTree = Array.isArray(payload.nodes) ? payload.nodes : [];
      const validIds = new Set(getPlatiCatalogSectionsFlat().map((item) => Number(item.section_id)));
      selectedPlatiCategoryIds = new Set([...selectedPlatiCategoryIds].filter((id) => validIds.has(id)));
      updatePlatiSectionOptions();
      $("pmCatalogTreeSource").textContent = currentUiLocale === "en"
        ? `Catalog source: ${sourceLabel(payload.source || source)}`
        : `Источник каталога: ${sourceLabel(payload.source || source)}`;
      platiCatalogTreeLoaded = true;
      return source;
    }

    async function loadPlatiGames({ force = false } = {}) {
      const cacheKey = "plati_games";
      let source = "network";
      let payload = null;
      if (!force) {
        const cached = localCacheRead(cacheKey);
        if (cached && Array.isArray(cached.games)) {
          payload = cached;
          source = "local_cache";
        }
      } else {
        localCacheDrop(cacheKey);
      }
      if (!payload) {
        payload = await runWithProxyFallback(() =>
          fetchJson(withDirectFallbackQuery(`/v2/marketplaces/platimarket/games?force_refresh=${force ? "true" : "false"}`))
        );
        localCacheWrite(cacheKey, { games: payload.games || [] });
      }
      platiGamesCatalog = Array.isArray(payload.games) ? payload.games : [];
      const options = [
        `<option value="">${currentUiLocale === "en" ? "Any game" : "Любая игра"}</option>`,
      ];
      for (const game of platiGamesCatalog) {
        options.push(`<option value="${escapeHtml(game.game_id)}">${escapeHtml(game.game_name)}</option>`);
      }
      $("pmGame").innerHTML = options.join("");
      $("pmGamesSource").textContent = currentUiLocale === "en"
        ? `Games source: ${sourceLabel(payload.source || source)}`
        : `Источник списка игр: ${sourceLabel(payload.source || source)}`;
      platiGamesLoaded = true;
      return source;
    }

    async function loadPlatiGameCategories({ force = false } = {}) {
      if (getPlatiScopeMode() !== "game") {
        platiGameCategoriesCatalog = [];
        selectedPlatiGameCategoryIds = new Set();
        updatePlatiGameCategoryOptions();
        $("pmGameCategoriesSource").textContent = currentUiLocale === "en"
          ? "Game categories source: —"
          : "Источник категорий игры: —";
        return;
      }
      const selectedGameIdFromSelect = readInt("pmGame");
      const manualGame = parsePlatiGameOverride($("pmGameCustom").value);
      const selectedGameId = manualGame.gameId !== null ? manualGame.gameId : selectedGameIdFromSelect;
      let selectedGameSlug = manualGame.gameSlug;
      if (!selectedGameSlug && selectedGameId !== null && Array.isArray(platiGamesCatalog)) {
        const matched = platiGamesCatalog.find((item) => Number(item.game_id) === selectedGameId);
        if (matched) selectedGameSlug = String(matched.game_slug || "").trim() || null;
      }
      if (selectedGameId === null && !selectedGameSlug) {
        platiGameCategoriesCatalog = [];
        selectedPlatiGameCategoryIds = new Set();
        updatePlatiGameCategoryOptions();
        $("pmGameCategoriesSource").textContent = currentUiLocale === "en"
          ? "Game categories source: —"
          : "Источник категорий игры: —";
        return;
      }

      const query = new URLSearchParams();
      if (selectedGameId !== null) query.set("game_id", String(selectedGameId));
      if (selectedGameSlug) query.set("game_slug", selectedGameSlug);
      query.set("ui_locale", currentUiLocale);
      query.set("force_refresh", force ? "true" : "false");
      const cacheKey = `plati_game_categories:${selectedGameId || ""}:${selectedGameSlug || ""}:${currentUiLocale}`;
      let source = "network";
      let data = null;
      if (!force) {
        const cached = localCacheRead(cacheKey);
        if (cached && Array.isArray(cached.categories)) {
          data = cached;
          source = "local_cache";
        }
      } else {
        localCacheDrop(cacheKey);
      }
      if (!data) {
        data = await runWithProxyFallback(() =>
          fetchJson(withDirectFallbackQuery(`/v2/marketplaces/platimarket/game-categories?${query.toString()}`))
        );
        localCacheWrite(cacheKey, {
          game_id: data.game_id,
          game_slug: data.game_slug,
          categories: data.categories || [],
        });
      }
      platiGameCategoriesCatalog = Array.isArray(data.categories) ? data.categories : [];
      const validIds = new Set(platiGameCategoriesCatalog.map((item) => Number(item.category_id)).filter((id) => Number.isFinite(id) && id >= 0));
      selectedPlatiGameCategoryIds = new Set(
        [...selectedPlatiGameCategoryIds].filter((id) => validIds.has(id))
      );
      updatePlatiGameCategoryOptions();
      $("pmGameCategoriesSource").textContent = currentUiLocale === "en"
        ? `Game categories source: ${sourceLabel(data.source || source)}`
        : `Источник категорий игры: ${sourceLabel(data.source || source)}`;
      return source;
    }

