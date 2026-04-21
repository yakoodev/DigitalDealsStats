    function renderProgress(envelope) {
      const progress = envelope?.progress || {};
      const status = envelope?.status || "pending";
      const percentRaw = Number(progress.percent);
      const percent = Number.isFinite(percentRaw) ? Math.max(0, Math.min(100, percentRaw)) : (status === "done" ? 100 : 0);
      progressFill.style.width = `${percent}%`;
      progressPercent.textContent = `${percent.toFixed(0)}%`;
      progressStage.textContent = `${t("stagePrefix")}: ${progress.stage || status}`;
      progressMessage.textContent = progress.message || t("noData");
      const logs = Array.isArray(progress.logs) ? progress.logs : [];
      renderTable(
        progressTable,
        [
          { label: "Время", render: (r) => new Date(r.ts).toLocaleTimeString(localeTag()) },
          { label: "Этап", render: (r) => r.stage || "info" },
          { label: "Событие", render: (r) => r.message || "—" },
        ],
        logs
      );
      renderProgressTrace(envelope);
      renderOverviewMarketplaceBlocks(envelope);
    }

    function resolveStageMarketplace(stage) {
      const value = String(stage || "").trim();
      if (!value.startsWith("marketplace:")) return null;
      const parts = value.split(":");
      return parts.length >= 2 ? String(parts[1] || "").trim() : null;
    }

    function renderProgressTrace(envelope) {
      const host = $("traceLogHost");
      const summaryNode = $("traceSummary");
      if (!host || !summaryNode) return;
      const progress = envelope?.progress || {};
      const logs = Array.isArray(progress.logs) ? progress.logs : [];
      if (!logs.length) {
        summaryNode.textContent = currentUiLocale === "en"
          ? "No parsing logs yet. Start an analysis to see per-marketplace timeline."
          : "Логи парсинга пока пустые. Запустите анализ, чтобы увидеть таймлайн по площадкам.";
        host.innerHTML = "";
        return;
      }

      const grouped = new Map();
      for (const log of logs) {
        const stage = String(log.stage || "").trim() || "info";
        const marketplace = resolveStageMarketplace(stage);
        const key = marketplace || "global";
        if (!grouped.has(key)) grouped.set(key, []);
        grouped.get(key).push(log);
      }
      const groupKeys = [...grouped.keys()].sort((left, right) => {
        if (left === "global") return -1;
        if (right === "global") return 1;
        return marketplaceLabel(left).localeCompare(marketplaceLabel(right), localeTag());
      });
      summaryNode.textContent = currentUiLocale === "en"
        ? `Log records: ${logs.length}. Groups: ${groupKeys.length}.`
        : `Записей лога: ${logs.length}. Групп: ${groupKeys.length}.`;

      host.innerHTML = groupKeys.map((key) => {
        const rows = grouped.get(key) || [];
        const title = key === "global"
          ? (currentUiLocale === "en" ? "Global pipeline" : "Глобальный пайплайн")
          : marketplaceLabel(key);
        const latest = rows[rows.length - 1];
        const stageText = latest?.stage || "info";
        const messageText = latest?.message || "—";
        const table = `
          <div class="table-wrap" style="margin-top:8px;">
            <table>
              <thead>
                <tr>
                  <th>${currentUiLocale === "en" ? "Time" : "Время"}</th>
                  <th>${currentUiLocale === "en" ? "Stage" : "Этап"}</th>
                  <th>${currentUiLocale === "en" ? "Message" : "Событие"}</th>
                </tr>
              </thead>
              <tbody>
                ${rows.map((row) => `
                  <tr>
                    <td>${escapeHtml(new Date(row.ts).toLocaleTimeString(localeTag()))}</td>
                    <td>${escapeHtml(row.stage || "info")}</td>
                    <td>${escapeHtml(row.message || "—")}</td>
                  </tr>
                `).join("")}
              </tbody>
            </table>
          </div>
        `;
        return `
          <details class="history-item" ${key === "global" ? "open" : ""}>
            <summary>
              <div class="summary-grid" style="grid-template-columns:1.4fr 1fr 2fr;">
                <span><b>${escapeHtml(title)}</b></span>
                <span>${currentUiLocale === "en" ? "Entries" : "Записей"}: <b>${formatNum(rows.length)}</b></span>
                <span>${escapeHtml(stageText)}: ${escapeHtml(messageText)}</span>
              </div>
            </summary>
            <div style="padding:8px 10px;">
              ${table}
            </div>
          </details>
        `;
      }).join("");
    }

    function renderOverviewMarketplaceBlocks(envelope) {
      const host = $("overviewMarketplaceBlocks");
      if (!host) return;
      const summaries = envelope?.marketplaces || {};
      const logs = Array.isArray(envelope?.progress?.logs) ? envelope.progress.logs : [];
      const overviewList = Array.isArray(envelope?.overview?.marketplaces)
        ? envelope.overview.marketplaces.map((item) => String(item))
        : [];
      const fallbackSelected = (!isMarketplaceRoute && typeof selectMarketplaceValues === "function")
        ? selectMarketplaceValues()
        : (activeMarketplace ? [activeMarketplace] : []);
      const keys = [...new Set([
        ...overviewList,
        ...Object.keys(summaries || {}),
        ...fallbackSelected,
      ])].filter((item) => item);
      if (!keys.length) {
        host.innerHTML = `<div class="muted">${escapeHtml(currentUiLocale === "en" ? "No marketplace data for this run yet." : "По этому запуску пока нет данных площадок.")}</div>`;
        return;
      }

      const latestByMarketplace = new Map();
      for (const row of logs) {
        const slug = resolveStageMarketplace(row.stage);
        if (!slug) continue;
        latestByMarketplace.set(slug, row);
      }

      host.innerHTML = keys.map((slug) => {
        const summary = summaries?.[slug] || null;
        const latest = latestByMarketplace.get(slug) || null;
        const ready = Boolean(summary && summary.request_id);
        const statusText = ready
          ? (currentUiLocale === "en" ? "Ready" : "Готово")
          : latest
            ? (currentUiLocale === "en" ? "In progress" : "В работе")
            : (currentUiLocale === "en" ? "Queued" : "Ожидание");
        const statusClass = ready ? "ok" : (latest ? "warn" : "");
        const generated = ready && summary?.generated_at
          ? new Date(summary.generated_at).toLocaleString(localeTag())
          : "—";
        const stats = summary?.offers_stats || {};
        const openUrl = selectedRunId
          ? `/analysis/${encodeURIComponent(slug)}?run_id=${encodeURIComponent(selectedRunId)}`
          : `/analysis/${encodeURIComponent(slug)}`;
        return `
          <details class="history-item" ${ready ? "" : "open"}>
            <summary>
              <div class="summary-grid" style="grid-template-columns:1.2fr 0.8fr 1fr 1fr 1fr;">
                <span><b>${escapeHtml(marketplaceLabel(slug))}</b></span>
                <span><span class="chip-inline ${statusClass}">${escapeHtml(statusText)}</span></span>
                <span>${currentUiLocale === "en" ? "Offers" : "Офферы"}: <b>${formatNum(stats.matched_offers)}</b></span>
                <span>${currentUiLocale === "en" ? "Sellers" : "Продавцы"}: <b>${formatNum(stats.unique_sellers)}</b></span>
                <span>P50: <b>${formatMoney(stats.p50_price, $("currency").value)}</b></span>
              </div>
            </summary>
            <div style="padding:8px 10px;">
              <div class="hint">${currentUiLocale === "en" ? "Generated" : "Сгенерировано"}: ${escapeHtml(generated)}</div>
              <div class="hint">${currentUiLocale === "en" ? "Latest event" : "Последнее событие"}: ${escapeHtml(latest?.message || "—")}</div>
              <div class="actions compact">
                <a class="link-btn" href="${escapeHtml(openUrl)}">${escapeHtml(currentUiLocale === "en" ? "Open detailed page" : "Открыть детальную страницу")}</a>
              </div>
            </div>
          </details>
        `;
      }).join("");
    }

    function renderTable(host, columns, rows) {
      const renderHead = () => columns.map((c) => `<th>${c.labelHtml ? c.labelHtml : escapeHtml(c.label)}</th>`).join("");
      if (!rows || rows.length === 0) {
        host.innerHTML = `<thead><tr>${renderHead()}</tr></thead><tbody><tr><td colspan="${columns.length}">${escapeHtml(t("noData"))}</td></tr></tbody>`;
        return;
      }
      const head = `<thead><tr>${renderHead()}</tr></thead>`;
      const body = `<tbody>${rows.map((row) => `<tr>${columns.map((c) => {
        const rendered = c.render(row);
        const content = c.isHtml ? String(rendered ?? "—") : escapeHtml(rendered ?? "—");
        return `<td class="${c.className || ""}">${content}</td>`;
      }).join("")}</tr>`).join("")}</tbody>`;
      host.innerHTML = head + body;
    }

    function labelWithTip(label, tipText) {
      return `<span class="with-tip" title="${escapeHtml(tipText)}">${escapeHtml(label)}</span>`;
    }

    function renderOverview(overview) {
      const pooled = overview?.pooled_offers_stats || {};
      const cards = [
        [currentUiLocale === "en" ? "Offers (pooled)" : "Офферов (pooled)", formatNum(pooled.matched_offers)],
        [currentUiLocale === "en" ? "Unique sellers" : "Уникальных продавцов", formatNum(pooled.unique_sellers)],
        [currentUiLocale === "en" ? "Min price" : "Мин. цена", formatMoney(pooled.min_price, $("currency").value)],
        [currentUiLocale === "en" ? "Average price" : "Средняя цена", formatMoney(pooled.avg_price, $("currency").value)],
        ["P50", formatMoney(pooled.p50_price, $("currency").value)],
        ["P90", formatMoney(pooled.p90_price, $("currency").value)],
        [currentUiLocale === "en" ? "Max price" : "Макс. цена", formatMoney(pooled.max_price, $("currency").value)],
        [currentUiLocale === "en" ? "Online share" : "Доля онлайн", formatPct(pooled.online_share)],
      ];
      $("overviewKpi").innerHTML = cards.map(([n, v]) =>
        `<div class="card"><div class="name">${escapeHtml(n)}</div><div class="value">${v}</div></div>`
      ).join("");

      renderTable(
        $("comparisonTable"),
        [
          { label: currentUiLocale === "en" ? "Marketplace" : "Площадка", render: (r) => r.label || r.marketplace },
          { label: currentUiLocale === "en" ? "Offers" : "Офферы", render: (r) => formatNum(r.matched_offers) },
          { label: currentUiLocale === "en" ? "Sellers" : "Продавцы", render: (r) => formatNum(r.unique_sellers) },
          { label: "P50", render: (r) => formatMoney(r.p50_price, $("currency").value) },
          { label: "Demand", render: (r) => (r.demand_index === null || r.demand_index === undefined ? "—" : Number(r.demand_index).toFixed(2)) },
        ],
        overview?.comparison || []
      );
      const aggr = overview?.aggregates || {};
      $("aggregatesBlock").innerHTML = `
        ${currentUiLocale === "en" ? "Marketplace averages" : "Средние по площадкам"}:
        matched_offers=<b>${formatNum(aggr.avg_matched_offers)}</b>,
        unique_sellers=<b>${formatNum(aggr.avg_unique_sellers)}</b>,
        p50=<b>${formatMoney(aggr.avg_p50_price, $("currency").value)}</b>
      `;
    }

    function histogramBins(values, binsCount = 10) {
      const clean = values.filter((v) => Number.isFinite(v));
      if (!clean.length) return [];
      const min = Math.min(...clean);
      const max = Math.max(...clean);
      if (min === max) {
        return [{ label: `${min.toFixed(2)}`, from: min, to: max, count: clean.length }];
      }
      const step = (max - min) / binsCount;
      const buckets = Array.from({ length: binsCount }, () => 0);
      for (const price of clean) {
        let idx = Math.floor((price - min) / step);
        idx = Math.max(0, Math.min(binsCount - 1, idx));
        buckets[idx] += 1;
      }
      return buckets.map((count, i) => {
        const left = min + i * step;
        const right = i === binsCount - 1 ? max : min + (i + 1) * step;
        return {
          label: `${left.toFixed(2)} - ${right.toFixed(2)}`,
          from: left,
          to: right,
          count,
        };
      });
    }

    function drawPriceHistogram(bins, hoverIndex = -1) {
      const canvas = chartState.price.canvas;
      const ctx = canvas.getContext("2d");
      const w = canvas.width;
      const h = canvas.height;
      const margin = 30;
      const chartW = w - margin * 2;
      const chartH = h - margin * 2;
      ctx.clearRect(0, 0, w, h);
      ctx.fillStyle = "#f8fbff";
      ctx.fillRect(0, 0, w, h);

      if (!bins.length) {
        ctx.fillStyle = "#667ca1";
        ctx.font = "13px sans-serif";
        ctx.fillText(t("noData"), 12, 24);
        chartState.price.bars = [];
        return;
      }

      const maxCount = Math.max(...bins.map((b) => b.count), 1);
      const bw = chartW / bins.length;
      const bars = [];
      bins.forEach((bin, idx) => {
        const x = margin + idx * bw + 3;
        const height = (bin.count / maxCount) * (chartH - 8);
        const y = margin + chartH - height;
        const width = bw - 6;
        ctx.fillStyle = idx === hoverIndex ? "#ff8d2f" : "#0b78f5";
        ctx.fillRect(x, y, width, height);
        bars.push({ x, y, width, height, index: idx, bin });
      });
      ctx.strokeStyle = "#9db2d5";
      ctx.beginPath();
      ctx.moveTo(margin, margin);
      ctx.lineTo(margin, margin + chartH);
      ctx.lineTo(margin + chartW, margin + chartH);
      ctx.stroke();
      chartState.price.bins = bins;
      chartState.price.bars = bars;
      chartState.price.hoverIndex = hoverIndex;
    }

    function drawHistory(points, hoverIndex = -1) {
      const canvas = chartState.history.canvas;
      const ctx = canvas.getContext("2d");
      const w = canvas.width;
      const h = canvas.height;
      const margin = 30;
      const chartW = w - margin * 2;
      const chartH = h - margin * 2;
      ctx.clearRect(0, 0, w, h);
      ctx.fillStyle = "#f8fbff";
      ctx.fillRect(0, 0, w, h);

      if (!points || points.length < 2) {
        ctx.fillStyle = "#667ca1";
        ctx.font = "13px sans-serif";
        ctx.fillText(t("noHistory"), 12, 24);
        chartState.history.coords = [];
        return;
      }

      const values = points.map((p) => Number(p.matched_offers || 0));
      const min = Math.min(...values);
      const max = Math.max(...values);
      const range = max - min || 1;
      const coords = points.map((point, idx) => {
        const x = margin + (idx / (points.length - 1)) * chartW;
        const y = margin + chartH - ((Number(point.matched_offers || 0) - min) / range) * chartH;
        return { x, y, point, index: idx };
      });

      ctx.strokeStyle = "#9db2d5";
      ctx.beginPath();
      ctx.moveTo(margin, margin);
      ctx.lineTo(margin, margin + chartH);
      ctx.lineTo(margin + chartW, margin + chartH);
      ctx.stroke();

      ctx.strokeStyle = "#0b78f5";
      ctx.lineWidth = 2;
      ctx.beginPath();
      coords.forEach((item, idx) => {
        if (idx === 0) ctx.moveTo(item.x, item.y); else ctx.lineTo(item.x, item.y);
      });
      ctx.stroke();

      coords.forEach((item) => {
        const hovered = item.index === hoverIndex;
        ctx.fillStyle = hovered ? "#ff8d2f" : "#0b78f5";
        ctx.beginPath();
        ctx.arc(item.x, item.y, hovered ? 5 : 3.5, 0, Math.PI * 2);
        ctx.fill();
      });

      chartState.history.points = points;
      chartState.history.coords = coords;
      chartState.history.hoverIndex = hoverIndex;
    }

    function drawSellerCompetition(points, hoverIndex = -1) {
      const canvas = chartState.sellers.canvas;
      const ctx = canvas.getContext("2d");
      const w = canvas.width;
      const h = canvas.height;
      const margin = 34;
      const chartW = w - margin * 2;
      const chartH = h - margin * 2;
      ctx.clearRect(0, 0, w, h);
      ctx.fillStyle = "#f8fbff";
      ctx.fillRect(0, 0, w, h);

      if (!points || !points.length) {
        ctx.fillStyle = "#667ca1";
        ctx.font = "13px sans-serif";
        ctx.fillText(t("noData"), 12, 24);
        chartState.sellers.points = [];
        return;
      }

      const xValues = points.map((item) => Number(item.median_price || 0));
      const yValues = points.map((item) => Number(item.offers_count || 0));
      const minX = Math.min(...xValues);
      const maxX = Math.max(...xValues);
      const minY = 0;
      const maxY = Math.max(...yValues, 1);
      const rangeX = maxX - minX || 1;
      const rangeY = maxY - minY || 1;

      ctx.strokeStyle = "#9db2d5";
      ctx.beginPath();
      ctx.moveTo(margin, margin);
      ctx.lineTo(margin, margin + chartH);
      ctx.lineTo(margin + chartW, margin + chartH);
      ctx.stroke();

      const coords = points.map((point, idx) => {
        const px = margin + ((Number(point.median_price || 0) - minX) / rangeX) * chartW;
        const py = margin + chartH - ((Number(point.offers_count || 0) - minY) / rangeY) * chartH;
        const hovered = idx === hoverIndex;
        ctx.fillStyle = hovered ? "#ff8d2f" : "#0b78f5";
        ctx.beginPath();
        ctx.arc(px, py, hovered ? 6 : 4.5, 0, Math.PI * 2);
        ctx.fill();
        return { x: px, y: py, index: idx, point };
      });
      chartState.sellers.points = coords;
      chartState.sellers.hoverIndex = hoverIndex;
    }

    function nearestSellerPoint(x, y, maxDistance = 18) {
      const points = chartState.sellers.points || [];
      let nearest = null;
      let best = Number.POSITIVE_INFINITY;
      for (const point of points) {
        const d = Math.hypot(point.x - x, point.y - y);
        if (d < best) {
          best = d;
          nearest = point;
        }
      }
      if (!nearest || best > maxDistance) return null;
      return nearest;
    }

    function sellerKeyFromPoint(point) {
      if (point && point.seller_id !== null && point.seller_id !== undefined) {
        return `id:${point.seller_id}`;
      }
      return `name:${String(point?.seller_name || "").trim().toLowerCase()}`;
    }

    function sellerKeyFromOffer(offer) {
      if (offer && offer.seller_id !== null && offer.seller_id !== undefined) {
        return `id:${offer.seller_id}`;
      }
      return `name:${String(offer?.seller_name || "").trim().toLowerCase()}`;
    }

    function offersForSellerPoint(point) {
      if (!point) return [];
      const key = sellerKeyFromPoint(point);
      return (marketAnalyticsOffers || []).filter((offer) => sellerKeyFromOffer(offer) === key);
    }

    function renderSellerFocusTable() {
      const metaNode = $("sellerFocusMeta");
      const applyBtn = $("sellerFocusApplyFilterBtn");
      const clearBtn = $("sellerFocusClearBtn");
      const tableNode = $("sellerFocusTable");

      if (!sellerFocusPoint) {
        metaNode.textContent = currentUiLocale === "en"
          ? "Click a point in seller competition chart to open offers for that seller."
          : "Нажмите на точку в графике \"Конкуренция продавцов\", чтобы открыть офферы этого продавца.";
        applyBtn.disabled = true;
        clearBtn.disabled = true;
        renderTable(
          tableNode,
          [
            { label: "Оффер", render: () => "—" },
            { label: "Цена", render: () => "—" },
            { label: "Отзывы", render: () => "—" },
          ],
          []
        );
        return;
      }

      const sellerOffers = offersForSellerPoint(sellerFocusPoint).sort(
        (left, right) => Number(left.price || 0) - Number(right.price || 0)
      );
      const medianText = sellerFocusPoint.median_price === null || sellerFocusPoint.median_price === undefined
        ? "—"
        : formatMoney(sellerFocusPoint.median_price, $("currency").value);
      metaNode.innerHTML = `
        Продавец: <b>${escapeHtml(sellerFocusPoint.seller_name || "unknown")}</b>
        (${escapeHtml(sellerFocusPoint.seller_id ?? "?")}),
        офферов в графике: <b>${formatNum(sellerFocusPoint.offers_count)}</b>,
        медианная цена: <b>${medianText}</b>,
        найдено офферов для доп.анализа: <b>${formatNum(sellerOffers.length)}</b>
      `;
      applyBtn.disabled = false;
      clearBtn.disabled = false;
      renderTable(
        tableNode,
        [
          {
            label: "Оффер",
            isHtml: true,
            render: (row) => {
              const href = safeUrl(row.offer_url);
              const text = escapeHtml(`#${row.offer_id}`);
              return href ? `<a href="${escapeHtml(href)}" target="_blank" rel="noopener noreferrer">${text}</a>` : text;
            },
          },
          { label: "Описание", render: (row) => row.description || "—" },
          { label: "Цена", render: (row) => formatMoney(row.price, row.currency || $("currency").value) },
          { label: "Продано", render: (row) => row.sold_text || formatNum(row.sold_count) || "—" },
          { label: "Отзывы", render: (row) => formatNum(row.reviews_count) },
          { label: "Онлайн", render: (row) => (row.is_online === true ? t("yes") : (row.is_online === false ? t("no") : "—")) },
          { label: "Авто", render: (row) => (row.auto_delivery === true ? t("yes") : (row.auto_delivery === false ? t("no") : "—")) },
        ],
        sellerOffers
      );
    }

    function drawCoverageBars(rows, hoverIndex = -1) {
      const canvas = chartState.coverage.canvas;
      const ctx = canvas.getContext("2d");
      const w = canvas.width;
      const h = canvas.height;
      const margin = 28;
      const chartW = w - margin * 2;
      const chartH = h - margin * 2;
      ctx.clearRect(0, 0, w, h);
      ctx.fillStyle = "#f8fbff";
      ctx.fillRect(0, 0, w, h);

      if (!rows || !rows.length) {
        ctx.fillStyle = "#667ca1";
        ctx.font = "13px sans-serif";
        ctx.fillText(t("noData"), 12, 24);
        chartState.coverage.bars = [];
        return;
      }

      const sortedRows = [...rows]
        .sort((a, b) => Number(b.counter_total || b.loaded_count || 0) - Number(a.counter_total || a.loaded_count || 0))
        .slice(0, 18);
      const maxTotal = Math.max(...sortedRows.map((item) => Number(item.counter_total || item.loaded_count || 0)), 1);
      const barWidth = chartW / sortedRows.length;
      const bars = [];

      ctx.strokeStyle = "#9db2d5";
      ctx.beginPath();
      ctx.moveTo(margin, margin);
      ctx.lineTo(margin, margin + chartH);
      ctx.lineTo(margin + chartW, margin + chartH);
      ctx.stroke();

      sortedRows.forEach((row, index) => {
        const total = Number(row.counter_total || row.loaded_count || 0);
        const loaded = Number(row.loaded_count || 0);
        const x = margin + index * barWidth + 2;
        const width = Math.max(2, barWidth - 4);
        const totalHeight = (total / maxTotal) * chartH;
        const loadedHeight = (loaded / maxTotal) * chartH;
        const yBottom = margin + chartH;
        const totalTop = yBottom - totalHeight;
        const loadedTop = yBottom - loadedHeight;
        const hovered = index === hoverIndex;

        ctx.fillStyle = hovered ? "#cad9f6" : "#deebff";
        ctx.fillRect(x, totalTop, width, totalHeight);
        ctx.fillStyle = row.coverage_status === "lower_bound" ? "#ff8d2f" : "#0b78f5";
        ctx.fillRect(x, loadedTop, width, loadedHeight);
        bars.push({ x, y: totalTop, width, height: totalHeight, index, row });
      });
      chartState.coverage.bars = bars;
      chartState.coverage.hoverIndex = hoverIndex;
    }

    function tooltipShow(x, y, text) {
      tooltip.style.display = "block";
      tooltip.style.left = `${x + 12}px`;
      tooltip.style.top = `${y + 12}px`;
      tooltip.textContent = text;
    }

    function tooltipHide() {
      tooltip.style.display = "none";
    }

    function bindCharts() {
      const priceCanvas = chartState.price.canvas;
      priceCanvas.addEventListener("mousemove", (event) => {
        const rect = priceCanvas.getBoundingClientRect();
        const x = (event.clientX - rect.left) * (priceCanvas.width / rect.width);
        const y = (event.clientY - rect.top) * (priceCanvas.height / rect.height);
        const bar = (chartState.price.bars || []).find((item) =>
          x >= item.x && x <= item.x + item.width && y >= item.y && y <= item.y + item.height
        );
        if (!bar) {
          if (chartState.price.hoverIndex !== -1) drawPriceHistogram(chartState.price.bins, -1);
          tooltipHide();
          return;
        }
        if (bar.index !== chartState.price.hoverIndex) {
          drawPriceHistogram(chartState.price.bins, bar.index);
        }
        tooltipShow(event.clientX, event.clientY, `${bar.bin.label}\nОфферов: ${bar.bin.count}`);
      });
      priceCanvas.addEventListener("mouseleave", () => {
        drawPriceHistogram(chartState.price.bins || [], -1);
        tooltipHide();
      });

      const historyCanvas = chartState.history.canvas;
      historyCanvas.addEventListener("mousemove", (event) => {
        const rect = historyCanvas.getBoundingClientRect();
        const x = (event.clientX - rect.left) * (historyCanvas.width / rect.width);
        const coords = chartState.history.coords || [];
        if (!coords.length) {
          tooltipHide();
          return;
        }
        let nearest = null;
        let best = Number.POSITIVE_INFINITY;
        for (const point of coords) {
          const d = Math.abs(point.x - x);
          if (d < best) {
            best = d;
            nearest = point;
          }
        }
        if (!nearest || best > 28) {
          if (chartState.history.hoverIndex !== -1) drawHistory(chartState.history.points, -1);
          tooltipHide();
          return;
        }
        if (nearest.index !== chartState.history.hoverIndex) {
          drawHistory(chartState.history.points, nearest.index);
        }
        tooltipShow(
          event.clientX,
          event.clientY,
          `Дата: ${new Date(nearest.point.generated_at).toLocaleString(localeTag())}\nОфферы: ${nearest.point.matched_offers}\nP50: ${nearest.point.p50_price ?? "—"}\nDemand: ${nearest.point.demand_index ?? "—"}`
        );
      });
      historyCanvas.addEventListener("mouseleave", () => {
        drawHistory(chartState.history.points || [], -1);
        tooltipHide();
      });

      const sellersCanvas = chartState.sellers.canvas;
      sellersCanvas.addEventListener("mousemove", (event) => {
        const rect = sellersCanvas.getBoundingClientRect();
        const x = (event.clientX - rect.left) * (sellersCanvas.width / rect.width);
        const y = (event.clientY - rect.top) * (sellersCanvas.height / rect.height);
        const points = chartState.sellers.points || [];
        const nearest = nearestSellerPoint(x, y, 18);
        if (!nearest) {
          sellersCanvas.style.cursor = "default";
          if (chartState.sellers.hoverIndex !== -1) drawSellerCompetition(
            points.map((item) => item.point),
            -1
          );
          tooltipHide();
          return;
        }
        sellersCanvas.style.cursor = "pointer";
        if (nearest.index !== chartState.sellers.hoverIndex) {
          drawSellerCompetition(points.map((item) => item.point), nearest.index);
        }
        tooltipShow(
          event.clientX,
          event.clientY,
          `${nearest.point.seller_name}\nОфферов: ${nearest.point.offers_count}\nМедианная цена: ${formatMoney(nearest.point.median_price, $("currency").value)}`
        );
      });
      sellersCanvas.addEventListener("mouseleave", () => {
        sellersCanvas.style.cursor = "default";
        drawSellerCompetition((chartState.sellers.points || []).map((item) => item.point), -1);
        tooltipHide();
      });
      sellersCanvas.addEventListener("click", (event) => {
        const rect = sellersCanvas.getBoundingClientRect();
        const x = (event.clientX - rect.left) * (sellersCanvas.width / rect.width);
        const y = (event.clientY - rect.top) * (sellersCanvas.height / rect.height);
        const nearest = nearestSellerPoint(x, y, 18);
        if (!nearest) return;
        sellerFocusPoint = { ...nearest.point };
        renderSellerFocusTable();
      });

      const coverageCanvas = chartState.coverage.canvas;
      coverageCanvas.addEventListener("mousemove", (event) => {
        const rect = coverageCanvas.getBoundingClientRect();
        const x = (event.clientX - rect.left) * (coverageCanvas.width / rect.width);
        const y = (event.clientY - rect.top) * (coverageCanvas.height / rect.height);
        const bars = chartState.coverage.bars || [];
        const bar = bars.find((item) => x >= item.x && x <= item.x + item.width && y >= item.y && y <= item.y + item.height);
        if (!bar) {
          if (chartState.coverage.hoverIndex !== -1) {
            drawCoverageBars(bars.map((item) => item.row), -1);
          }
          tooltipHide();
          return;
        }
        if (bar.index !== chartState.coverage.hoverIndex) {
          drawCoverageBars(bars.map((item) => item.row), bar.index);
        }
        tooltipShow(
          event.clientX,
          event.clientY,
          `${bar.row.section_name || bar.row.section_url}\nloaded: ${bar.row.loaded_count}\ncounter: ${bar.row.counter_total ?? "—"}`
        );
      });
      coverageCanvas.addEventListener("mouseleave", () => {
        drawCoverageBars((chartState.coverage.bars || []).map((item) => item.row), -1);
        tooltipHide();
      });
    }

    function percentile(values, p) {
      const clean = values.filter((v) => Number.isFinite(v)).sort((a, b) => a - b);
      if (!clean.length) return null;
      if (clean.length === 1) return clean[0];
      const rank = (clean.length - 1) * p;
      const lo = Math.floor(rank);
      const hi = Math.min(clean.length - 1, lo + 1);
      const frac = rank - lo;
      return clean[lo] + (clean[hi] - clean[lo]) * frac;
    }

    function computeCompetitionAnalytics(offers) {
      const grouped = new Map();
      for (const item of offers || []) {
        const sellerId = item.seller_id ?? `name:${item.seller_name}`;
        if (!grouped.has(sellerId)) {
          grouped.set(sellerId, {
            seller_id: item.seller_id ?? null,
            seller_name: item.seller_name || "unknown",
            offers_count: 0,
            prices: [],
          });
        }
        const row = grouped.get(sellerId);
        row.offers_count += 1;
        const price = Number(item.price);
        if (Number.isFinite(price)) row.prices.push(price);
      }

      const sellers = [...grouped.values()].map((row) => ({
        ...row,
        median_price: percentile(row.prices, 0.5) ?? null,
      }));
      const totalOffers = offers.length || 0;
      const shares = sellers.map((row) => row.offers_count / Math.max(totalOffers, 1));
      const hhi = shares.length ? shares.reduce((sum, share) => sum + (share * 100) ** 2, 0) : null;
      const top3Share = shares.length
        ? sellers
            .map((row) => row.offers_count)
            .sort((a, b) => b - a)
            .slice(0, 3)
            .reduce((sum, count) => sum + count, 0) / Math.max(totalOffers, 1)
        : null;
      const prices = offers.map((item) => Number(item.price)).filter((v) => Number.isFinite(v));
      const p10 = percentile(prices, 0.1);
      const p50 = percentile(prices, 0.5);
      const p90 = percentile(prices, 0.9);
      const q1 = percentile(prices, 0.25);
      const q3 = percentile(prices, 0.75);
      const iqr = (q1 !== null && q3 !== null) ? Math.max(0, q3 - q1) : null;
      let dumpingThreshold = null;
      if (q1 !== null && iqr !== null) {
        dumpingThreshold = iqr > 0 ? (q1 - 1.5 * iqr) : ((p10 ?? q1) * 0.8);
      }
      const dumpingCandidates = dumpingThreshold === null
        ? []
        : offers.filter((item) => Number(item.price) < dumpingThreshold);
      const priceSpread = (p10 !== null && p90 !== null && p50 !== null && p50 > 0)
        ? (p90 - p10) / p50
        : null;
      sellers.sort((a, b) => b.offers_count - a.offers_count || (a.median_price ?? 0) - (b.median_price ?? 0));
      return {
        sellers,
        hhi,
        top3Share,
        priceSpread,
        q1,
        q3,
        iqr,
        dumpingThreshold,
        dumpingCandidates,
      };
    }

    function renderMarketAnalytics(offersForAnalytics, sections, marketplace = "funpay", demand = null) {
      marketAnalyticsOffers = Array.isArray(offersForAnalytics) ? offersForAnalytics : [];
      const prices = (offersForAnalytics || []).map((item) => Number(item.price)).filter((v) => Number.isFinite(v));
      drawPriceHistogram(histogramBins(prices), -1);
      const competition = computeCompetitionAnalytics(offersForAnalytics || []);
      const totalOffers = Math.max(1, Number((offersForAnalytics || []).length));
      const lowerBoundCount = Array.isArray(sections)
        ? sections.filter((item) => String(item.coverage_status || "") === "lower_bound").length
        : 0;
      const lowerBoundShare = sections && sections.length ? lowerBoundCount / sections.length : 0;
      const dumpingShare = competition.dumpingCandidates.length / totalOffers;
      const demand30d = Number(
        demand?.estimated_purchases_30d
        ?? demand?.purchases_from_reviews_30d
        ?? demand?.volume_30d
        ?? 0
      );
      const liquidityRatio = demand30d > 0 ? demand30d / totalOffers : 0;
      const hhiValue = Number(competition.hhi);
      const spreadValue = Number(competition.priceSpread);
      const concentrationScore = Number.isFinite(hhiValue)
        ? Math.max(0, Math.min(100, 100 - (hhiValue - 1200) / 35))
        : 50;
      const spreadScore = Number.isFinite(spreadValue)
        ? Math.max(0, Math.min(100, 100 - spreadValue * 60))
        : 50;
      const coverageScore = Math.max(0, Math.min(100, 100 - lowerBoundShare * 100));
      const demandScore = Math.max(0, Math.min(100, liquidityRatio * 160));
      const marketScore = Math.round(
        concentrationScore * 0.3
        + spreadScore * 0.25
        + coverageScore * 0.2
        + demandScore * 0.25
      );
      const competitionCards = [
        ["HHI", competition.hhi === null ? "—" : Number(competition.hhi).toFixed(1)],
        ["Top-3 Share", competition.top3Share === null ? "—" : formatPct(competition.top3Share)],
        ["Price Spread", competition.priceSpread === null ? "—" : Number(competition.priceSpread).toFixed(3)],
        ["IQR", competition.iqr === null ? "—" : Number(competition.iqr).toFixed(3)],
        [currentUiLocale === "en" ? "Market score" : "Оценка рынка", `${formatNum(marketScore)} / 100`],
      ];
      $("competitionKpi").innerHTML = competitionCards.map(([n, v]) =>
        `<div class="card"><div class="name">${escapeHtml(n)}</div><div class="value">${v}</div></div>`
      ).join("");
      const signalRows = [
        {
          signal: currentUiLocale === "en" ? "Concentration" : "Концентрация продавцов",
          value: competition.hhi === null ? "—" : Number(competition.hhi).toFixed(1),
          interpretation:
            competition.hhi === null
              ? "—"
              : (competition.hhi > 2500
                  ? (currentUiLocale === "en" ? "High concentration" : "Высокая концентрация")
                  : (competition.hhi > 1500
                      ? (currentUiLocale === "en" ? "Moderate concentration" : "Умеренная концентрация")
                      : (currentUiLocale === "en" ? "Competitive market" : "Конкурентный рынок"))),
        },
        {
          signal: currentUiLocale === "en" ? "Dumping risk" : "Риск демпинга",
          value: formatPct(dumpingShare),
          interpretation:
            dumpingShare > 0.15
              ? (currentUiLocale === "en" ? "High, many below-threshold offers" : "Высокий: много офферов ниже порога")
              : (dumpingShare > 0.05
                  ? (currentUiLocale === "en" ? "Medium" : "Средний")
                  : (currentUiLocale === "en" ? "Low" : "Низкий")),
        },
        {
          signal: currentUiLocale === "en" ? "Liquidity (30d)" : "Ликвидность (30д)",
          value: demand30d > 0 ? `${formatNum(demand30d)} / ${formatNum(totalOffers)}` : "—",
          interpretation:
            liquidityRatio > 1
              ? (currentUiLocale === "en" ? "Strong demand" : "Сильный спрос")
              : (liquidityRatio > 0.4
                  ? (currentUiLocale === "en" ? "Stable demand" : "Стабильный спрос")
                  : (currentUiLocale === "en" ? "Weak demand signal" : "Слабый сигнал спроса")),
        },
        {
          signal: currentUiLocale === "en" ? "Coverage quality" : "Качество покрытия",
          value: `${formatNum(lowerBoundCount)} / ${formatNum((sections || []).length || 0)}`,
          interpretation:
            lowerBoundShare > 0.3
              ? (currentUiLocale === "en" ? "Many lower-bound sections" : "Много lower-bound разделов")
              : (currentUiLocale === "en" ? "Coverage acceptable" : "Покрытие приемлемое"),
        },
      ];
      renderTable(
        $("marketSignalsTable"),
        [
          { label: currentUiLocale === "en" ? "Signal" : "Сигнал", render: (r) => r.signal },
          { label: currentUiLocale === "en" ? "Value" : "Значение", render: (r) => r.value },
          { label: currentUiLocale === "en" ? "Interpretation" : "Интерпретация", render: (r) => r.interpretation },
        ],
        signalRows
      );
      renderTable(
        $("dumpingTable"),
        [
          {
            label: "Оффер",
            isHtml: true,
            render: (r) => {
              const href = safeUrl(r.offer_url);
              const text = escapeHtml(`#${r.offer_id}`);
              return href ? `<a href="${escapeHtml(href)}" target="_blank" rel="noopener noreferrer">${text}</a>` : text;
            },
          },
          {
            label: "Продавец",
            isHtml: true,
            render: (r) => {
              const href = sellerProfileUrl(marketplace, r.seller_id, r.seller_url);
              const text = `${escapeHtml(r.seller_name)} (${escapeHtml(r.seller_id ?? "?")})`;
              return href ? `<a href="${escapeHtml(href)}" target="_blank" rel="noopener noreferrer">${text}</a>` : text;
            },
          },
          { label: "Цена", render: (r) => formatMoney(r.price, r.currency || $("currency").value) },
          { label: "Порог", render: () => competition.dumpingThreshold === null ? "—" : formatMoney(competition.dumpingThreshold, $("currency").value) },
        ],
        competition.dumpingCandidates
      );
      drawSellerCompetition(competition.sellers, -1);
      drawCoverageBars(sections || [], -1);
      if (sellerFocusPoint) {
        const currentKey = sellerKeyFromPoint(sellerFocusPoint);
        const refreshed = (competition.sellers || []).find((item) => sellerKeyFromPoint(item) === currentKey);
        sellerFocusPoint = refreshed ? { ...refreshed } : null;
      }
      renderSellerFocusTable();
    }

    function renderMarketplaceDetail(result) {
      currentMarketplaceResult = result;
      sellerFocusPoint = null;
      const summary = result?.summary || null;
      if (!summary) {
        $("marketplaceMeta").textContent = t("noData");
        renderSellerFocusTable();
        return;
      }

      $("marketplaceMeta").innerHTML = `
        <div>Площадка: <b>${escapeHtml(summary.label)}</b></div>
        <div>Provider request_id: <b class="mono">${escapeHtml(summary.request_id)}</b></div>
        <div>Сгенерировано: <b>${new Date(summary.generated_at).toLocaleString(localeTag())}</b></div>
        <div>Валидно до: <b>${new Date(summary.valid_until).toLocaleString(localeTag())}</b></div>
        <div>Локаль контента: <b>${escapeHtml(summary.content_locale_requested || "auto")}</b> → <b>${escapeHtml(summary.content_locale_applied || "—")}</b></div>
        <div>Локаль UI: <b>${escapeHtml(summary.ui_locale || currentUiLocale)}</b></div>
        <div>${currentUiLocale === "en" ? "Offers slice" : "Срез офферов"}: <b>${escapeHtml(offersLoadSummaryText())}</b></div>
        <div>Lower-bound разделов: <b>${formatNum(summary.coverage?.sections_lower_bound || 0)}</b> / ${formatNum(summary.coverage?.sections_scanned || 0)}</div>
      `;
      const stats = summary.offers_stats || {};
      const cards = [
        ["Офферы", formatNum(stats.matched_offers)],
        ["Продавцы", formatNum(stats.unique_sellers)],
        ["Мин", formatMoney(stats.min_price, $("currency").value)],
        ["Средняя", formatMoney(stats.avg_price, $("currency").value)],
        ["P50", formatMoney(stats.p50_price, $("currency").value)],
        ["P90", formatMoney(stats.p90_price, $("currency").value)],
        ["Макс", formatMoney(stats.max_price, $("currency").value)],
        ["Онлайн", formatPct(stats.online_share)],
      ];
      $("marketplaceKpi").innerHTML = cards.map(([n, v]) =>
        `<div class="card"><div class="name">${escapeHtml(n)}</div><div class="value">${v}</div></div>`
      ).join("");

      const warnings = summary.warnings || [];
      if (!warnings.length) {
        $("marketplaceWarnings").innerHTML = "";
      } else {
        $("marketplaceWarnings").innerHTML =
          `<div class="card"><div class="name">Предупреждения</div><ul class="warn-list">${warnings.map((w) => `<li>${escapeHtml(w)}</li>`).join("")}</ul></div>`;
      }

      const raw = result?.raw?.legacy_result || {};
      const tables = raw?.tables || {};
      const topOffers = tables?.top_offers || [];
      const topSellers = tables?.top_sellers || [];
      const topDemandSellers = tables?.top_demand_sellers || [];
      const sections = tables?.sections || [];
      const demand = summary.demand || null;
      const soldTotal = demand ? (demand.purchases_from_sold_total ?? demand.estimated_purchases_total) : null;
      const reviews30d = demand ? (demand.purchases_from_reviews_30d ?? demand.estimated_purchases_30d) : null;
      const reviewsTotal = demand ? (demand.purchases_from_reviews_total ?? demand.relevant_reviews) : null;
      const totalLowerBound = Boolean(demand?.purchases_total_is_lower_bound);
      const demandCards = [
        [
          totalLowerBound ? "Покупки (всего, sold lower-bound)" : "Покупки (всего, sold)",
          demand ? formatNum(soldTotal) : "—",
        ],
        ["Покупки (30д, отзывы)", demand ? formatNum(reviews30d) : "—"],
        ["Покупки (отзывы, всего)", demand ? formatNum(reviewsTotal) : "—"],
        ["Проверено продавцов", demand ? formatNum(demand.sellers_analyzed) : "—"],
        ["Сканировано отзывов", demand ? formatNum(demand.reviews_scanned) : "—"],
      ];
      $("marketDemandKpi").innerHTML = demandCards.map(([n, v]) =>
        `<div class="card"><div class="name">${escapeHtml(n)}</div><div class="value">${v}</div></div>`
      ).join("");
      renderTable(
        $("topDemandSellersTable"),
        [
          {
            label: "Продавец",
            isHtml: true,
            render: (r) => {
              const href = sellerProfileUrl(summary.marketplace, r.seller_id, r.seller_url);
              const text = `${escapeHtml(r.seller_name)} (${escapeHtml(r.seller_id ?? "?")})`;
              return href ? `<a href="${escapeHtml(href)}" target="_blank" rel="noopener noreferrer">${text}</a>` : text;
            },
          },
          { label: "Покупки 30д", render: (r) => formatNum(r.estimated_purchases_30d) },
          { label: "Покупки всего (sold)", render: (r) => formatNum(r.estimated_purchases_total) },
          { label: "Отзывы", render: (r) => formatNum(r.reviews_scanned) },
        ],
        topDemandSellers
      );
      renderTable(
        $("topOffersTable"),
        [
          {
            label: "Товар",
            isHtml: true,
            render: (r) => {
              const href = safeUrl(r.offer_url);
              const title = escapeHtml(String(r.description || r.title || `#${r.offer_id || "?"}`));
              const offerId = escapeHtml(`#${r.offer_id ?? "?"}`);
              if (!href) {
                return `${title}<div class="mono" style="opacity:.65; margin-top:2px;">${offerId}</div>`;
              }
              return `
                <a href="${escapeHtml(href)}" target="_blank" rel="noopener noreferrer">${title}</a>
                <div class="mono" style="opacity:.65; margin-top:2px;">${offerId}</div>
              `;
            },
          },
          {
            label: "Продавец",
            isHtml: true,
            render: (r) => {
              const href = sellerProfileUrl(summary.marketplace, r.seller_id, r.seller_url);
              const text = `${escapeHtml(r.seller_name)} (${escapeHtml(r.seller_id ?? "?")})`;
              return href ? `<a href="${escapeHtml(href)}" target="_blank" rel="noopener noreferrer">${text}</a>` : text;
            },
          },
          { label: "Цена", render: (r) => formatMoney(r.price, r.currency || $("currency").value) },
          { label: "Продано", render: (r) => r.sold_text || formatNum(r.sold_count) || "—" },
          { label: "Отзывы", render: (r) => formatNum(r.reviews_count) },
        ],
        topOffers
      );
      renderTable(
        $("topSellersTable"),
        [
          {
            label: "Продавец",
            isHtml: true,
            render: (r) => {
              const href = sellerProfileUrl(summary.marketplace, r.seller_id, r.seller_url);
              const text = `${escapeHtml(r.seller_name)} (${escapeHtml(r.seller_id ?? "?")})`;
              return href ? `<a href="${escapeHtml(href)}" target="_blank" rel="noopener noreferrer">${text}</a>` : text;
            },
          },
          { label: "Офферов", render: (r) => formatNum(r.offers_count) },
          { label: "P50", render: (r) => formatMoney(r.p50_price, $("currency").value) },
        ],
        topSellers
      );
      renderTable(
        $("sectionsTable"),
        [
          {
            label: "Раздел",
            isHtml: true,
            render: (r) => {
              const href = safeUrl(r.section_url);
              const text = escapeHtml(r.section_name || `Раздел #${r.section_id ?? "?"}`);
              return href ? `<a href="${escapeHtml(href)}" target="_blank" rel="noopener noreferrer">${text}</a>` : text;
            },
          },
          { label: "ID", render: (r) => r.section_id ?? "—", className: "mono" },
          { label: "counter", labelHtml: labelWithTip("counter", "Сколько лотов в разделе по счетчику FunPay."), render: (r) => formatNum(r.counter_total) },
          { label: "loaded", labelHtml: labelWithTip("loaded", "Сколько лотов реально загружено в HTML."), render: (r) => formatNum(r.loaded_count) },
          { label: "coverage", labelHtml: labelWithTip("coverage", "full = полная выгрузка, lower_bound = нижняя оценка."), render: (r) => r.coverage_status },
        ],
        sections
      );

      const historyPoints = raw?.charts?.history_points || [];
      drawHistory(historyPoints, -1);
      renderMarketAnalytics(fullOffers, sections, summary.marketplace || activeMarketplace || "funpay", demand);
    }

    function renderOffersTable() {
      const min = readFloat("offersPriceMin");
      const max = readFloat("offersPriceMax");
      const minReviews = readInt("offersMinReviews");
      const sellerQuery = $("offersSellerQuery").value.trim().toLowerCase();
      const onlineOnly = $("offersOnlineOnly").checked;
      const autoOnly = $("offersAutoOnly").checked;
      const sortMode = $("offersSort").value;
      const pageSizeRaw = Number($("offersPageSize").value);
      const pageSize = Number.isFinite(pageSizeRaw) && pageSizeRaw > 0 ? Math.floor(pageSizeRaw) : 40;
      const filtered = (fullOffers || []).filter((item) => {
        const price = Number(item.price);
        if (min !== null && price < min) return false;
        if (max !== null && price > max) return false;
        const reviews = item.reviews_count === null || item.reviews_count === undefined ? 0 : Number(item.reviews_count);
        if (minReviews !== null && reviews < minReviews) return false;
        if (onlineOnly && item.is_online !== true) return false;
        if (autoOnly && item.auto_delivery !== true) return false;
        if (sellerQuery) {
          const name = String(item.seller_name || "").toLowerCase();
          const sid = item.seller_id === null || item.seller_id === undefined ? "" : String(item.seller_id);
          if (!name.includes(sellerQuery) && !sid.includes(sellerQuery)) return false;
        }
        return true;
      });

      const sorted = [...filtered].sort((left, right) => {
        const lPrice = Number(left.price);
        const rPrice = Number(right.price);
        const lReviews = Number(left.reviews_count || 0);
        const rReviews = Number(right.reviews_count || 0);
        const lSeller = String(left.seller_name || "").toLowerCase();
        const rSeller = String(right.seller_name || "").toLowerCase();
        const lOffer = Number(left.offer_id || 0);
        const rOffer = Number(right.offer_id || 0);

        if (sortMode === "price_desc") return rPrice - lPrice;
        if (sortMode === "reviews_desc") return rReviews - lReviews;
        if (sortMode === "seller_asc") return lSeller.localeCompare(rSeller, localeTag());
        if (sortMode === "offer_desc") return rOffer - lOffer;
        return lPrice - rPrice;
      });

      filteredOffers = sorted;
      const sections = currentMarketplaceResult?.raw?.legacy_result?.tables?.sections || [];
      const marketplace = currentMarketplaceResult?.summary?.marketplace || activeMarketplace || "funpay";
      const demand = currentMarketplaceResult?.summary?.demand || null;
      renderMarketAnalytics(filteredOffers, sections, marketplace, demand);
      const pages = Math.max(1, Math.ceil(sorted.length / pageSize));
      if (offersPage > pages - 1) offersPage = pages - 1;
      const offset = offersPage * pageSize;
      const rows = sorted.slice(offset, offset + pageSize);
      const loadSummary = offersLoadSummaryText();
      $("offersPageMeta").textContent =
        `${currentUiLocale === "en" ? "Shown" : "Показано"} ${rows.length} ${currentUiLocale === "en" ? "of" : "из"} ${sorted.length} (${currentUiLocale === "en" ? "page" : "страница"} ${offersPage + 1}/${pages}). ${loadSummary}`;
      $("offersPrevBtn").disabled = offersPage <= 0;
      $("offersNextBtn").disabled = offersPage >= pages - 1;
      renderTable(
        $("offersTable"),
        [
          {
            label: "Оффер",
            className: "mono",
            isHtml: true,
            render: (r) => {
              const href = safeUrl(r.offer_url);
              const text = escapeHtml(`#${r.offer_id}`);
              return href ? `<a href="${escapeHtml(href)}" target="_blank" rel="noopener noreferrer">${text}</a>` : text;
            },
          },
          {
            label: "Продавец",
            isHtml: true,
            render: (r) => {
              const marketplace = currentMarketplaceResult?.summary?.marketplace || activeMarketplace || "funpay";
              const href = sellerProfileUrl(marketplace, r.seller_id, r.seller_url);
              const text = `${escapeHtml(r.seller_name)} (${escapeHtml(r.seller_id ?? "?")})`;
              return href ? `<a href="${escapeHtml(href)}" target="_blank" rel="noopener noreferrer">${text}</a>` : text;
            },
          },
          { label: "Описание", render: (r) => r.description || "—" },
          { label: "Цена", render: (r) => formatMoney(r.price, r.currency || $("currency").value) },
          { label: "Продано", render: (r) => r.sold_text || formatNum(r.sold_count) || "—" },
          { label: "Отзывы", render: (r) => formatNum(r.reviews_count) },
          { label: "Онлайн", render: (r) => (r.is_online === true ? t("yes") : (r.is_online === false ? t("no") : "—")) },
          { label: "Авто", render: (r) => (r.auto_delivery === true ? t("yes") : (r.auto_delivery === false ? t("no") : "—")) },
        ],
        rows
      );
    }

    function csvCell(value) {
      const raw = value === null || value === undefined ? "" : String(value);
      return `"${raw.replaceAll("\"", "\"\"")}"`;
    }

    function exportFilteredOffersCsv() {
      const rows = Array.isArray(filteredOffers) ? filteredOffers : [];
      if (!rows.length) {
        setStatus(t("csvNoData"), "warn");
        return;
      }
      const header = [
        "offer_id",
        "offer_url",
        "seller_id",
        "seller_name",
        "description",
        "price",
        "currency",
        "sold_count",
        "sold_text",
        "sold_is_lower_bound",
        "reviews_count",
        "is_online",
        "auto_delivery",
      ];
      const lines = [header.map(csvCell).join(",")];
      for (const item of rows) {
        lines.push([
          item.offer_id,
          item.offer_url,
          item.seller_id,
          item.seller_name,
          item.description,
          item.price,
          item.currency,
          item.sold_count,
          item.sold_text,
          item.sold_is_lower_bound,
          item.reviews_count,
          item.is_online,
          item.auto_delivery,
        ].map(csvCell).join(","));
      }
      const blob = new Blob(["\uFEFF" + lines.join("\n")], { type: "text/csv;charset=utf-8;" });
      const link = document.createElement("a");
      const runId = selectedRunId || "run";
      link.href = URL.createObjectURL(blob);
      link.download = `offers_${runId}.csv`;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      URL.revokeObjectURL(link.href);
      const loadState = getOffersLoadState();
      if (loadState.partial && loadState.total !== null) {
        setStatus(
          `${t("csvDone", { count: rows.length })} ${t("csvPartialWarning", {
            loaded: formatNum(loadState.loaded),
            total: formatNum(loadState.total),
          })}`,
          "warn"
        );
      } else {
        setStatus(t("csvDone", { count: rows.length }), "ok");
      }
    }

