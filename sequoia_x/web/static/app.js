const state = {
  strategies: [],
  selectedStrategy: null,
  result: null,
  pollTimer: null,
  strategyPollTimer: null,
  stockSearchTimer: null,
  stocks: [],
  selectedSymbol: null,
  selectedResultSymbol: null,
  resultPeriod: "day",
  resultSeries: [],
  resultSeriesStock: null,
  resultWindowSize: 120,
  stockFilterOptions: { industries: [], concepts: [], markets: [] },
};

const numberFormatter = new Intl.NumberFormat("zh-CN");
const chartHoverData = new Map();
let chartTooltipEl = null;
const PERIOD_LABELS = {
  day: "日K",
  week: "周K",
  month: "月K",
  quarter: "季K",
  year: "年K",
};
const METRIC_LABELS = {
  window_high: "区间最高价",
  window_low: "区间最低价",
  amplitude_pct: "区间振幅",
  distance_to_high_pct: "距区间高点",
  lookback_days: "横盘交易日",
  max_amplitude_pct: "最大区间振幅",
  min_distance_pct: "距高点下限",
  max_distance_pct: "距高点上限",
  near_high_pct: "距区间高点不超过",
};

function byId(id) {
  return document.getElementById(id);
}

document.addEventListener("DOMContentLoaded", () => {
  bindTabs();
  bindActions();
  loadInitialData();
});

function bindTabs() {
  document.querySelectorAll(".tab").forEach((button) => {
    button.addEventListener("click", () => switchView(button.dataset.view));
  });
}

function bindActions() {
  byId("refreshDataBtn").addEventListener("click", loadDataSummary);
  byId("syncBtn").addEventListener("click", () => startDataJob("/api/data/sync"));
  byId("metadataSyncBtn").addEventListener("click", () => startDataJob("/api/data/metadata"));
  byId("backfillBtn").addEventListener("click", startBackfillJob);
  byId("reloadStocksBtn").addEventListener("click", () => loadStocks());
  byId("stockSearch").addEventListener("input", () => {
    clearTimeout(state.stockSearchTimer);
    state.stockSearchTimer = setTimeout(() => loadStocks(), 240);
  });
  byId("strategySelect").addEventListener("change", (event) => {
    selectStrategy(event.target.value);
  });
  byId("runStrategyBtn").addEventListener("click", runSelectedStrategy);
  byId("resultsView").addEventListener("click", (event) => {
    const periodButton = event.target.closest("[data-result-period]");
    if (periodButton) {
      selectResultPeriod(periodButton.dataset.resultPeriod);
    }
  });
  byId("resultsView").addEventListener("input", (event) => {
    if (event.target.id === "resultRange") {
      renderResultChartWindow();
    }
  });
}

async function loadInitialData() {
  byId("referenceDate").value = new Date().toISOString().slice(0, 10);
  await Promise.all([loadDataSummary(), loadStrategies(), loadStocks(), loadStockFilterOptions()]);
}

function startBackfillJob() {
  const startDate = byId("backfillStartDate").value;
  if (!startDate) {
    renderJobError("请先选择历史 K 线起始日期");
    return;
  }
  startDataJob("/api/data/backfill", {
    start_date: startDate,
    full_refresh: byId("fullRefreshCheckbox").checked,
    source: byId("backfillSource").value,
  });
}

function switchView(viewId) {
  document.querySelectorAll(".tab").forEach((button) => {
    button.classList.toggle("active", button.dataset.view === viewId);
  });
  document.querySelectorAll(".view").forEach((view) => {
    view.classList.toggle("active", view.id === viewId);
  });
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  const text = await response.text();
  const data = text ? JSON.parse(text) : null;
  if (!response.ok) {
    const detail = data && data.detail ? data.detail : response.statusText;
    throw new Error(detail);
  }
  return data;
}

async function loadDataSummary() {
  try {
    const summary = await api("/api/data/summary");
    renderSummary(summary);
  } catch (error) {
    byId("dataStatus").textContent = `数据状态读取失败：${error.message}`;
    byId("dataDot").classList.remove("ok");
  }
}

function renderSummary(summary) {
  byId("dataDot").classList.toggle("ok", summary.has_data);
  byId("dataStatus").textContent = summary.has_data ? "本地数据可用" : "本地暂无数据";
  byId("latestDate").textContent = summary.latest_date || "--";

  const items = [
    ["股票数", formatNumber(summary.symbol_count)],
    ["行情行数", formatNumber(summary.row_count)],
    ["最早日期", summary.earliest_date || "--"],
    ["最新日期", summary.latest_date || "--"],
    ["数据库", summary.db_path || "--"],
  ];
  byId("summaryGrid").innerHTML = items
    .map(
      ([label, value]) => `
        <div class="metric">
          <div class="metric-label">${escapeHtml(label)}</div>
          <div class="metric-value">${escapeHtml(value)}</div>
        </div>
      `,
    )
    .join("");
}

async function startDataJob(path, payload = null) {
  setDataButtonsDisabled(true);
  clearTimeout(state.pollTimer);
  try {
    const options = { method: "POST" };
    if (payload !== null) {
      options.body = JSON.stringify(payload);
    }
    const job = await api(path, options);
    renderJob(job);
    pollJob(job.job_id);
  } catch (error) {
    renderJobError(error.message);
    setDataButtonsDisabled(false);
  }
}

async function pollJob(jobId) {
  try {
    const job = await api(`/api/jobs/${encodeURIComponent(jobId)}`);
    renderJob(job);
    if (job.status === "queued" || job.status === "running") {
      state.pollTimer = setTimeout(() => pollJob(jobId), 1600);
      return;
    }
    setDataButtonsDisabled(false);
    await loadDataSummary();
    await loadStocks();
    await loadStockFilterOptions();
  } catch (error) {
    renderJobError(error.message);
    setDataButtonsDisabled(false);
  }
}

async function loadStocks() {
  const query = byId("stockSearch").value.trim();
  const path = query
    ? `/api/stocks?limit=80&query=${encodeURIComponent(query)}`
    : "/api/stocks?limit=80";
  try {
    state.stocks = await api(path);
    renderStockRows(state.stocks);
    if (state.stocks.length === 0) {
      state.selectedSymbol = null;
      renderEmptyChart("暂无本地股票数据");
      return;
    }
    const selectedStillVisible = state.stocks.some((stock) => stock.symbol === state.selectedSymbol);
    const nextSymbol = selectedStillVisible ? state.selectedSymbol : state.stocks[0].symbol;
    selectStock(nextSymbol);
  } catch (error) {
    byId("stockRows").innerHTML = `
      <tr>
        <td colspan="5" class="muted">股票读取失败：${escapeHtml(error.message)}</td>
      </tr>
    `;
    renderEmptyChart("股票读取失败");
  }
}

function renderStockRows(stocks) {
  const body = byId("stockRows");
  if (stocks.length === 0) {
    body.innerHTML = `
      <tr>
        <td colspan="5" class="muted">暂无本地股票数据</td>
      </tr>
    `;
    return;
  }
  body.innerHTML = stocks
    .map(
      (stock) => `
        <tr data-symbol="${escapeHtml(stock.symbol)}" class="${stock.symbol === state.selectedSymbol ? "selected" : ""}">
          <td>${escapeHtml(stock.name || stock.symbol)}</td>
          <td>${escapeHtml(stock.symbol)}</td>
          <td>${escapeHtml(stock.latest_date || "--")}</td>
          <td>${escapeHtml(formatMaybeNumber(stock.close))}</td>
          <td>${escapeHtml(formatNumber(stock.row_count || 0))}</td>
        </tr>
      `,
    )
    .join("");
  body.querySelectorAll("tr[data-symbol]").forEach((row) => {
    row.addEventListener("click", () => selectStock(row.dataset.symbol));
  });
}

async function selectStock(symbol) {
  if (!symbol) {
    return;
  }
  state.selectedSymbol = symbol;
  renderStockRows(state.stocks);
  const stock = state.stocks.find((item) => item.symbol === symbol);
  byId("chartTitle").textContent = `${stock?.name || symbol} ${symbol}`;
  byId("chartMeta").textContent = "K 线加载中";

  try {
    const payload = await api(`/api/stocks/${encodeURIComponent(symbol)}/ohlcv?period=day&limit=160`);
    renderKlineChart(payload.rows || [], stock, {
      canvasId: "stockChart",
      titleId: "chartTitle",
      metaId: "chartMeta",
    });
  } catch (error) {
    renderEmptyChart(error.message);
  }
}

function renderEmptyChart(message, target = {}) {
  const titleId = target.titleId || "chartTitle";
  const metaId = target.metaId || "chartMeta";
  const canvasId = target.canvasId || "stockChart";
  byId(titleId).textContent = "K 线";
  byId(metaId).textContent = message;
  const canvas = byId(canvasId);
  const ctx = setupCanvas(canvas);
  setKlineHoverData(canvas, [], null);
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  ctx.fillStyle = "#65717d";
  ctx.font = "14px sans-serif";
  ctx.fillText(message, 24, 42);
}

function renderKlineChart(rows, stock, target = {}) {
  const canvasId = target.canvasId || "stockChart";
  const titleId = target.titleId || "chartTitle";
  const metaId = target.metaId || "chartMeta";
  const canvas = byId(canvasId);
  const ctx = setupCanvas(canvas);
  ctx.clearRect(0, 0, canvas.width, canvas.height);

  if (!rows.length) {
    renderEmptyChart("暂无 K 线数据", target);
    return;
  }

  const padding = { left: 52, right: 18, top: 20, bottom: 54 };
  const volumeHeight = 72;
  const chartHeight = canvas.height - padding.top - padding.bottom - volumeHeight - 18;
  const chartWidth = canvas.width - padding.left - padding.right;
  const priceValues = rows.flatMap((row) => [Number(row.high), Number(row.low)]).filter(Number.isFinite);
  const maxPrice = Math.max(...priceValues);
  const minPrice = Math.min(...priceValues);
  const priceRange = maxPrice - minPrice || 1;
  const maxVolume = Math.max(...rows.map((row) => Number(row.volume)).filter(Number.isFinite), 1);
  const candleStep = chartWidth / rows.length;
  const candleWidth = Math.max(3, Math.min(12, candleStep * 0.58));
  const priceY = (value) => padding.top + (maxPrice - value) / priceRange * chartHeight;
  const volumeTop = padding.top + chartHeight + 18;

  drawGrid(ctx, padding, chartWidth, chartHeight, minPrice, maxPrice);

  rows.forEach((row, index) => {
    const open = Number(row.open);
    const high = Number(row.high);
    const low = Number(row.low);
    const close = Number(row.close);
    const volume = Number(row.volume);
    if (![open, high, low, close].every(Number.isFinite)) {
      return;
    }
    const x = padding.left + index * candleStep + candleStep / 2;
    const up = close >= open;
    ctx.strokeStyle = up ? "#b3261e" : "#126a5e";
    ctx.fillStyle = up ? "#b3261e" : "#126a5e";
    ctx.beginPath();
    ctx.moveTo(x, priceY(high));
    ctx.lineTo(x, priceY(low));
    ctx.stroke();

    const bodyTop = priceY(Math.max(open, close));
    const bodyHeight = Math.max(1, Math.abs(priceY(open) - priceY(close)));
    ctx.fillRect(x - candleWidth / 2, bodyTop, candleWidth, bodyHeight);

    if (Number.isFinite(volume)) {
      const barHeight = volume / maxVolume * volumeHeight;
      ctx.globalAlpha = 0.32;
      ctx.fillRect(x - candleWidth / 2, volumeTop + volumeHeight - barHeight, candleWidth, barHeight);
      ctx.globalAlpha = 1;
    }
  });

  ctx.fillStyle = "#65717d";
  ctx.font = "12px sans-serif";
  ctx.fillText(rows[0].date, padding.left, canvas.height - 18);
  ctx.textAlign = "right";
  ctx.fillText(rows[rows.length - 1].date, canvas.width - padding.right, canvas.height - 18);
  ctx.textAlign = "left";

  const latest = rows[rows.length - 1];
  byId(metaId).textContent = `${latest.date}  开 ${formatMaybeNumber(latest.open)}  高 ${formatMaybeNumber(latest.high)}  低 ${formatMaybeNumber(latest.low)}  收 ${formatMaybeNumber(latest.close)}  量 ${formatNumber(latest.volume)}`;
  if (stock) {
    byId(titleId).textContent = `${stock.name || stock.symbol} ${stock.symbol}`;
  }
  setKlineHoverData(canvas, rows, {
    padding,
    chartWidth,
    candleStep,
  });
}

function drawGrid(ctx, padding, chartWidth, chartHeight, minPrice, maxPrice) {
  ctx.strokeStyle = "#e4e8ec";
  ctx.fillStyle = "#65717d";
  ctx.font = "12px sans-serif";
  ctx.lineWidth = 1;
  for (let i = 0; i <= 4; i += 1) {
    const y = padding.top + chartHeight / 4 * i;
    const price = maxPrice - (maxPrice - minPrice) / 4 * i;
    ctx.beginPath();
    ctx.moveTo(padding.left, y);
    ctx.lineTo(padding.left + chartWidth, y);
    ctx.stroke();
    ctx.fillText(price.toFixed(2), 8, y + 4);
  }
}

function setKlineHoverData(canvas, rows, layout) {
  chartHoverData.set(canvas.id, { rows, layout });
  if (canvas.dataset.klineHoverBound === "1") {
    return;
  }
  canvas.dataset.klineHoverBound = "1";
  canvas.addEventListener("mousemove", handleKlineHover);
  canvas.addEventListener("mouseleave", hideKlineTooltip);
}

function handleKlineHover(event) {
  const canvas = event.currentTarget;
  const hover = chartHoverData.get(canvas.id);
  if (!hover?.rows?.length || !hover.layout) {
    hideKlineTooltip();
    return;
  }

  const { rows, layout } = hover;
  const rect = canvas.getBoundingClientRect();
  const x = (event.clientX - rect.left) * (canvas.width / rect.width);
  const minX = layout.padding.left;
  const maxX = layout.padding.left + layout.chartWidth;
  if (x < minX || x > maxX) {
    hideKlineTooltip();
    return;
  }

  const index = Math.round((x - layout.padding.left - layout.candleStep / 2) / layout.candleStep);
  if (index < 0 || index >= rows.length) {
    hideKlineTooltip();
    return;
  }

  const centerX = layout.padding.left + index * layout.candleStep + layout.candleStep / 2;
  if (Math.abs(x - centerX) > Math.max(8, layout.candleStep / 2)) {
    hideKlineTooltip();
    return;
  }

  showKlineTooltip(event, rows[index], rows[index - 1]);
}

function showKlineTooltip(event, row, previousRow) {
  const tooltip = getKlineTooltip();
  const open = Number(row.open);
  const high = Number(row.high);
  const low = Number(row.low);
  const close = Number(row.close);
  const previousClose = Number(previousRow?.close);
  const change = Number.isFinite(close) && Number.isFinite(previousClose) ? close - previousClose : null;
  const changePct = change !== null && previousClose !== 0 ? change / previousClose * 100 : null;
  const changeClass = change === null ? "return-flat" : change >= 0 ? "return-positive" : "return-negative";
  tooltip.innerHTML = `
    <div class="chart-tooltip-title">时间　${escapeHtml(row.date || "--")}</div>
    <div class="chart-tooltip-row"><span>开盘</span><strong>${escapeHtml(formatMaybeNumber(open))}</strong></div>
    <div class="chart-tooltip-row"><span>收盘</span><strong>${escapeHtml(formatMaybeNumber(close))}</strong></div>
    <div class="chart-tooltip-row"><span>最高</span><strong>${escapeHtml(formatMaybeNumber(high))}</strong></div>
    <div class="chart-tooltip-row"><span>最低</span><strong>${escapeHtml(formatMaybeNumber(low))}</strong></div>
    <div class="chart-tooltip-row"><span>涨跌额</span><strong class="${changeClass}">${escapeHtml(formatSignedNumber(change))}</strong></div>
    <div class="chart-tooltip-row"><span>涨跌幅</span><strong class="${changeClass}">${escapeHtml(formatSignedPercent(changePct))}</strong></div>
    <div class="chart-tooltip-row"><span>成交量</span><strong>${escapeHtml(formatNumber(row.volume))}</strong></div>
    <div class="chart-tooltip-row"><span>成交额</span><strong>${escapeHtml(formatMaybeNumber(row.turnover))}</strong></div>
  `;
  tooltip.style.display = "block";

  const margin = 14;
  const width = tooltip.offsetWidth;
  const height = tooltip.offsetHeight;
  let left = event.clientX + margin;
  let top = event.clientY + margin;
  if (left + width > window.innerWidth - margin) {
    left = event.clientX - width - margin;
  }
  if (top + height > window.innerHeight - margin) {
    top = event.clientY - height - margin;
  }
  tooltip.style.left = `${Math.max(margin, left)}px`;
  tooltip.style.top = `${Math.max(margin, top)}px`;
}

function getKlineTooltip() {
  if (!chartTooltipEl) {
    chartTooltipEl = document.createElement("div");
    chartTooltipEl.className = "chart-tooltip";
    document.body.appendChild(chartTooltipEl);
  }
  return chartTooltipEl;
}

function hideKlineTooltip() {
  if (chartTooltipEl) {
    chartTooltipEl.style.display = "none";
  }
}

function setupCanvas(canvas) {
  const rect = canvas.getBoundingClientRect();
  const width = Math.max(320, Math.floor(rect.width));
  const height = Math.max(260, Math.floor(rect.height));
  if (canvas.width !== width || canvas.height !== height) {
    canvas.width = width;
    canvas.height = height;
  }
  return canvas.getContext("2d");
}

function renderJob(job) {
  const progress = job.progress || {};
  const total = Number(progress.total || 0);
  const processed = Number(progress.processed || 0);
  const progressPct = total > 0 ? Math.min(100, Math.round(processed / total * 100)) : null;
  const fullJob = [
    ["kind", jobKindLabel(job.kind)],
    ["status", jobStatusLabel(job.status)],
    ["message", job.message || "任务已创建"],
    ["progress", progressPct === null ? "--" : `${formatNumber(processed)} / ${formatNumber(total)}（${progressPct}%）`],
    ["current_symbol", progress.current_symbol || "--"],
    ["current_action", progress.current_action || "--"],
    ["current_start_date", progress.current_start_date || progress.start_date || "--"],
    ["success", formatNumber(progress.success || 0)],
    ["skipped", formatNumber(progress.skipped || 0)],
    ["failed", formatNumber(progress.failed || 0)],
    ["rows_written", formatNumber(progress.rows_written || 0)],
    ["mode", progress.full_refresh ? "强制覆盖重拉" : "续跑补齐"],
    ["started_at", job.started_at || "--"],
    ["finished_at", job.finished_at || "--"],
    ["result", formatJobResult(job.result)],
    ["error", job.error || "--"],
  ];
  byId("jobPanel").classList.remove("muted");
  byId("jobPanel").innerHTML = `
    ${
      progressPct === null
        ? ""
        : `<div class="job-progress">
            <div class="job-progress-track">
              <div class="job-progress-bar" style="width: ${progressPct}%"></div>
            </div>
          </div>`
    }
    ${fullJob
    .map(
      ([key, value]) => `
        <div>
          <div class="metric-label">${escapeHtml(jobLabel(key))}</div>
          <div>${escapeHtml(value)}</div>
        </div>
      `,
    )
    .join("")}
  `;
}

function renderJobError(message) {
  byId("jobPanel").classList.add("muted");
  byId("jobPanel").textContent = `任务失败：${message}`;
}

function setDataButtonsDisabled(disabled) {
  byId("syncBtn").disabled = disabled;
  byId("metadataSyncBtn").disabled = disabled;
  byId("backfillBtn").disabled = disabled;
  byId("backfillStartDate").disabled = disabled;
  byId("backfillSource").disabled = disabled;
  byId("fullRefreshCheckbox").disabled = disabled;
}

async function loadStrategies() {
  try {
    state.strategies = await api("/api/strategies");
    const select = byId("strategySelect");
    select.innerHTML = state.strategies
      .map((strategy) => `<option value="${escapeHtml(strategy.key)}">${escapeHtml(strategy.name)}</option>`)
      .join("");
    if (state.strategies.length > 0) {
      selectStrategy(state.strategies[0].key);
    }
  } catch (error) {
    showStrategyMessage(`策略读取失败：${error.message}`, "error");
  }
}

async function loadStockFilterOptions() {
  try {
    state.stockFilterOptions = await api("/api/stock-filters");
    renderStockFilters();
  } catch (error) {
    state.stockFilterOptions = { industries: [], concepts: [], markets: [] };
    renderStockFilters();
  }
}

function renderStockFilters() {
  renderOptionSelect("industryFilter", state.stockFilterOptions.industries || [], "code", "name");
  renderOptionSelect("conceptFilter", state.stockFilterOptions.concepts || [], "code", "name");
  renderOptionSelect("marketFilter", state.stockFilterOptions.markets || [], "value", "label");
}

function renderOptionSelect(id, options, valueKey, labelKey) {
  const select = byId(id);
  const selected = new Set(Array.from(select.selectedOptions || []).map((option) => option.value));
  select.innerHTML = options
    .map((option) => {
      const value = String(option[valueKey] || "");
      const selectedText = selected.has(value) ? "selected" : "";
      return `<option value="${escapeHtml(value)}" ${selectedText}>${escapeHtml(option[labelKey] || value)}</option>`;
    })
    .join("");
}

function selectStrategy(key) {
  state.selectedStrategy = state.strategies.find((strategy) => strategy.key === key) || null;
  if (!state.selectedStrategy) {
    byId("strategyDescription").textContent = "";
    byId("parameterForm").innerHTML = "";
    return;
  }
  byId("strategyDescription").textContent = state.selectedStrategy.description || "";
  renderParameterForm(state.selectedStrategy.parameters || []);
  showStrategyMessage("", "");
}

function renderParameterForm(parameters) {
  const form = byId("parameterForm");
  if (parameters.length === 0) {
    form.innerHTML = `<div class="parameter-field muted">此策略暂无可调参数</div>`;
    return;
  }
  form.innerHTML = parameters
    .map((parameter) => renderParameterField(parameter))
    .join("");
}

function renderParameterField(parameter) {
  const inputId = `param-${parameter.key}`;
  if (parameter.type === "boolean") {
    return `
      <label class="parameter-field" for="${escapeHtml(inputId)}">
        <span class="field-label">${escapeHtml(parameter.label)}</span>
        <input id="${escapeHtml(inputId)}" data-param="${escapeHtml(parameter.key)}" data-type="boolean" type="checkbox" ${parameter.default ? "checked" : ""} />
      </label>
    `;
  }

  if (parameter.type === "choice") {
    const options = (parameter.options || [])
      .map(
        (option) => `
          <option value="${escapeHtml(String(option.value))}" ${option.value === parameter.default ? "selected" : ""}>
            ${escapeHtml(option.label)}
          </option>
        `,
      )
      .join("");
    return `
      <label class="parameter-field" for="${escapeHtml(inputId)}">
        <span class="field-label">${escapeHtml(parameter.label)}</span>
        <select id="${escapeHtml(inputId)}" data-param="${escapeHtml(parameter.key)}" data-type="choice">${options}</select>
      </label>
    `;
  }

  const step = parameter.step ?? (parameter.type === "integer" ? 1 : 0.1);
  const type = parameter.type === "integer" ? "integer" : "number";
  return `
    <label class="parameter-field" for="${escapeHtml(inputId)}">
      <span class="field-label">${escapeHtml(parameter.label)}</span>
      <span class="input-with-unit">
        <input
          id="${escapeHtml(inputId)}"
          data-param="${escapeHtml(parameter.key)}"
          data-type="${type}"
          type="number"
          value="${escapeHtml(String(parameter.default))}"
          min="${escapeHtml(String(parameter.min ?? ""))}"
          max="${escapeHtml(String(parameter.max ?? ""))}"
          step="${escapeHtml(String(step))}"
        />
        <span class="unit">${escapeHtml(parameter.unit || "")}</span>
      </span>
    </label>
  `;
}

async function runSelectedStrategy() {
  if (!state.selectedStrategy) {
    showStrategyMessage("请选择策略", "error");
    return;
  }

  byId("runStrategyBtn").disabled = true;
  clearTimeout(state.strategyPollTimer);
  showStrategyMessage("", "");
  renderStrategyProgress({ status: "queued", message: "策略已排队", progress: {} });

  try {
    const parameters = collectParameters();
    const backtestDays = collectBacktestDays();
    const filters = collectStockFilters();
    const referenceDate = byId("referenceDate").value || null;
    const body = { parameters, backtest_days: backtestDays, filters };
    if (referenceDate) {
      body.reference_date = referenceDate;
    }
    const job = await api(`/api/strategies/${encodeURIComponent(state.selectedStrategy.key)}/run-job`, {
      method: "POST",
      body: JSON.stringify(body),
    });
    renderStrategyProgress(job);
    pollStrategyJob(job.job_id);
  } catch (error) {
    showStrategyMessage(error.message, "error");
    renderStrategyProgress(null);
    byId("runStrategyBtn").disabled = false;
  }
}

async function pollStrategyJob(jobId) {
  try {
    const job = await api(`/api/jobs/${encodeURIComponent(jobId)}`);
    renderStrategyProgress(job);
    if (job.status === "queued" || job.status === "running") {
      state.strategyPollTimer = setTimeout(() => pollStrategyJob(jobId), 900);
      return;
    }

    byId("runStrategyBtn").disabled = false;
    if (job.status === "succeeded") {
      state.result = job.result;
      renderResults(job.result);
      showStrategyMessage(`运行完成：${job.result.total} 只`, "ok");
      switchView("resultsView");
      return;
    }
    showStrategyMessage(job.error || "策略运行失败", "error");
  } catch (error) {
    showStrategyMessage(error.message, "error");
    byId("runStrategyBtn").disabled = false;
  }
}

function renderStrategyProgress(job) {
  const panel = byId("strategyProgress");
  if (!job) {
    panel.className = "strategy-progress-panel muted";
    panel.textContent = "暂无运行中的策略";
    return;
  }
  const progress = job.progress || {};
  const total = Number(progress.total || 0);
  const processed = Number(progress.processed || 0);
  const percent = total > 0 ? Math.min(100, Math.round(processed / total * 100)) : 0;
  const statusText = jobStatusLabel(job.status);
  const matched = Number(progress.matched || 0);
  panel.className = "strategy-progress-panel";
  panel.innerHTML = `
    <div class="strategy-progress-line">
      <strong>${escapeHtml(progress.strategy_name || state.selectedStrategy?.name || "策略")}</strong>
      <span>${escapeHtml(statusText)}</span>
      <span>${total > 0 ? `${formatNumber(processed)} / ${formatNumber(total)}（${percent}%）` : "--"}</span>
      <span>命中 ${formatNumber(matched)} 只</span>
      <span>${escapeHtml(progress.current_symbol || "--")}</span>
    </div>
    <div class="job-progress-track">
      <div class="job-progress-bar" style="width: ${percent}%"></div>
    </div>
    <div class="muted">${escapeHtml(job.message || progress.current_action || "")}</div>
  `;
}

function collectParameters() {
  const parameters = {};
  byId("parameterForm").querySelectorAll("[data-param]").forEach((input) => {
    const key = input.dataset.param;
    const type = input.dataset.type;
    if (type === "boolean") {
      parameters[key] = input.checked;
    } else if (type === "integer") {
      parameters[key] = Number.parseInt(input.value, 10);
    } else if (type === "number") {
      parameters[key] = Number.parseFloat(input.value);
    } else {
      parameters[key] = input.value;
    }
  });
  return parameters;
}

function collectBacktestDays() {
  const text = byId("backtestDays").value.trim();
  const parts = text ? text.split(/[,\s，、]+/).filter(Boolean) : ["1", "3", "5"];
  const days = [];
  parts.forEach((part) => {
    const day = Number.parseInt(part, 10);
    if (!Number.isInteger(day) || day <= 0 || day > 120) {
      throw new Error("回测交易日必须是 1 到 120 之间的整数");
    }
    if (!days.includes(day)) {
      days.push(day);
    }
  });
  if (days.length === 0) {
    throw new Error("请至少填写一个回测交易日");
  }
  if (days.length > 8) {
    throw new Error("回测交易日最多填写 8 个");
  }
  return days;
}

function collectStockFilters() {
  return {
    industry_board_codes: selectedValues("industryFilter"),
    concept_board_codes: selectedValues("conceptFilter"),
    markets: selectedValues("marketFilter"),
    min_listed_trade_days: readNonNegativeInteger("minListedDays"),
    min_avg_turnover_20: readNonNegativeNumber("minAvgTurnover20"),
    exclude_risks: Array.from(document.querySelectorAll("[data-risk-filter]:checked"))
      .map((input) => input.dataset.riskFilter),
  };
}

function selectedValues(id) {
  return Array.from(byId(id).selectedOptions).map((option) => option.value).filter(Boolean);
}

function readNonNegativeInteger(id) {
  const value = byId(id).value.trim();
  if (!value) {
    return 0;
  }
  const numeric = Number.parseInt(value, 10);
  if (!Number.isInteger(numeric) || numeric < 0) {
    throw new Error("上市满交易日必须是不小于 0 的整数");
  }
  return numeric;
}

function readNonNegativeNumber(id) {
  const value = byId(id).value.trim();
  if (!value) {
    return 0;
  }
  const numeric = Number.parseFloat(value);
  if (!Number.isFinite(numeric) || numeric < 0) {
    throw new Error("20日均成交额必须是不小于 0 的数字");
  }
  return numeric;
}

function renderResults(result) {
  byId("resultCount").textContent = `${formatNumber(result.total)} 只`;
  const strategy = state.strategies.find((item) => item.key === result.strategy_key);
  const parameterLabels = Object.fromEntries((strategy?.parameters || []).map((parameter) => [parameter.key, parameter.label]));
  const parameterText = Object.entries(result.parameters || {})
    .map(([key, value]) => `${parameterLabels[key] || metricLabel(key)}=${formatParameterValue(key, value)}`)
    .join("，");
  const referenceText = result.reference_date ? `参考日期=${result.reference_date}` : "";
  const filterText = result.filter_summary
    ? `筛选后股票池=${formatNumber(result.filter_summary.eligible_symbols)}只`
    : "";
  byId("resultMeta").textContent = [result.strategy_name, referenceText, parameterText, filterText]
    .filter(Boolean)
    .join("；");
  renderBacktestSummary(result.backtest);

  const rows = result.rows || [];
  renderResultHeader(result);
  if (rows.length === 0) {
    byId("resultRows").innerHTML = `
      <tr>
        <td colspan="${resultColumnCount(result)}" class="muted">没有符合条件的股票</td>
      </tr>
    `;
    resetResultDetail();
    return;
  }

  if (!rows.some((row) => row.symbol === state.selectedResultSymbol)) {
    state.selectedResultSymbol = rows[0].symbol;
  }

  renderResultRows();
  selectResultStock(state.selectedResultSymbol, { keepPeriod: true, skipRender: true });
}

function renderBacktestSummary(backtest) {
  const panel = byId("resultBacktestSummary");
  const summary = backtest?.summary || [];
  if (!summary.length) {
    panel.className = "backtest-summary muted";
    panel.innerHTML = "";
    return;
  }
  panel.className = "backtest-summary";
  panel.innerHTML = summary
    .map((item) => `
      <div class="backtest-chip">
        <strong>+${escapeHtml(item.days)}日</strong>
        <span>${escapeHtml(formatNumber(item.evaluated))}只股票</span>
        <span>平均涨跌幅:${escapeHtml(formatSignedPercent(item.average_pct))}；</span>
        <span>涨幅大于1%，共${escapeHtml(formatNumber(item.up_gt_1))}(${escapeHtml(formatPercent(item.up_gt_1_ratio))})</span>
        <span>跌幅大于-1%，共${escapeHtml(formatNumber(item.down_gt_1))}(${escapeHtml(formatPercent(item.down_gt_1_ratio))})</span>
        <span>-1%~1%之间，共 ${escapeHtml(formatNumber(item.flat_between_1))}(${escapeHtml(formatPercent(item.flat_between_1_ratio))}</span>
        <span>涨幅超过10%，共${escapeHtml(formatNumber(item.up_gt_10))}(${escapeHtml(formatPercent(item.up_gt_10_ratio))})</span>
        <span>跌幅超过10%，共${escapeHtml(formatNumber(item.down_gt_10))}(${escapeHtml(formatPercent(item.down_gt_10_ratio))})</span>
      </div>
    `)
    .join("");
}

function renderResultHeader(result) {
  const headers = result.strategy_key === "sideways_consolidation"
    ? ["名称", "代码", "最新日期", "收盘价", "区间振幅", "距区间高点"]
    : ["名称", "代码", "最新日期", "收盘价", "策略", "指标"];
  const backtestHeaders = backtestHorizons(result).map((day) => `+${day}日涨跌`);
  byId("resultHeadRow").innerHTML = [...headers, ...backtestHeaders]
    .map((header) => `<th>${escapeHtml(header)}</th>`)
    .join("");
}

function resultColumnCount(result) {
  return 6 + backtestHorizons(result).length;
}

function backtestHorizons(result) {
  return result?.backtest?.horizons || [];
}

function renderResultRows() {
  const result = state.result;
  const rows = result?.rows || [];
  if (!result || rows.length === 0) {
    return;
  }
  const columnCount = resultColumnCount(result);
  byId("resultRows").innerHTML = rows
    .map((row) => renderResultRow(row, result, columnCount))
    .join("");
  byId("resultRows").querySelectorAll("tr[data-result-symbol]").forEach((row) => {
    row.addEventListener("click", () => selectResultStock(row.dataset.resultSymbol));
  });
}

function renderResultRow(row, result, columnCount) {
  const selected = row.symbol === state.selectedResultSymbol;
  const baseCells = result.strategy_key === "sideways_consolidation"
    ? renderSidewaysResultCells(row)
    : renderDefaultResultCells(row, result.strategy_name);
  const cells = `${baseCells}${renderBacktestReturnCells(row, result)}`;
  const detail = selected
    ? `<tr class="result-detail-row"><td colspan="${columnCount}">${resultDetailPanelHtml()}</td></tr>`
    : "";
  return `
    <tr data-result-symbol="${escapeHtml(row.symbol || "")}" class="result-main-row ${selected ? "selected" : ""}">
      ${cells}
    </tr>
    ${detail}
  `;
}

function renderDefaultResultCells(row, strategyName) {
  return `
    <td>
      <strong>${escapeHtml(row.name || row.stock?.name || row.symbol || "--")}</strong>
      <div class="muted">${escapeHtml(row.code || row.stock?.code || "")}</div>
    </td>
    <td>${escapeHtml(row.symbol || "--")}</td>
    <td>${escapeHtml(row.latest_date || "--")}</td>
    <td>${escapeHtml(formatMaybeNumber(row.close))}</td>
    <td>${escapeHtml(strategyName)}</td>
    <td>${renderMetrics(row.metrics || {})}</td>
  `;
}

function renderSidewaysResultCells(row) {
  const metrics = row.metrics || {};
  return `
    <td>
      <strong>${escapeHtml(row.name || row.stock?.name || row.symbol || "--")}</strong>
      <div class="muted">${escapeHtml(row.code || row.stock?.code || "")}</div>
    </td>
    <td>${escapeHtml(row.symbol || "--")}</td>
    <td>${escapeHtml(row.latest_date || "--")}</td>
    <td>${escapeHtml(formatMaybeNumber(row.close))}</td>
    <td>${escapeHtml(formatMetricValue("amplitude_pct", metrics.amplitude_pct))}</td>
    <td>${escapeHtml(formatMetricValue("distance_to_high_pct", metrics.distance_to_high_pct))}</td>
  `;
}

function renderBacktestReturnCells(row, result) {
  const returns = row.backtest_returns || {};
  return backtestHorizons(result)
    .map((day) => {
      const value = returns[String(day)];
      return `<td class="${backtestReturnClass(value)}">${escapeHtml(formatSignedPercent(value))}</td>`;
    })
    .join("");
}

function backtestReturnClass(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric === 0) {
    return "return-flat";
  }
  return numeric > 0 ? "return-positive" : "return-negative";
}

function resultDetailPanelHtml() {
  return `
    <div id="resultDetailPanel" class="panel result-detail-panel compact-result-detail">
      <div class="result-chart-shell">
        <div class="result-chart-toolbar">
          <div id="resultKlineTitle" class="sr-only">K 线</div>
          <div id="resultKlineMeta" class="sr-only">--</div>
          <div></div>
          ${periodButtonsHtml()}
        </div>
        <canvas id="resultStockChart" width="900" height="420"></canvas>
        <div class="range-row compact-range-row">
          <input id="resultRange" type="range" min="0" max="0" value="0" />
          <span id="resultRangeLabel">--</span>
        </div>
      </div>

      <div id="resultMetricGrid" class="detail-stat-grid compact-detail-grid"></div>
    </div>
  `;
}

function periodButtonsHtml() {
  return `
    <div class="period-tabs" aria-label="K 线周期">
      <button class="period-tab ${state.resultPeriod === "day" ? "active" : ""}" data-result-period="day">日K</button>
      <button class="period-tab ${state.resultPeriod === "week" ? "active" : ""}" data-result-period="week">周K</button>
      <button class="period-tab ${state.resultPeriod === "month" ? "active" : ""}" data-result-period="month">月K</button>
      <button class="period-tab ${state.resultPeriod === "quarter" ? "active" : ""}" data-result-period="quarter">季K</button>
      <button class="period-tab ${state.resultPeriod === "year" ? "active" : ""}" data-result-period="year">年K</button>
    </div>
  `;
}

function renderMetrics(metrics) {
  const entries = Object.entries(metrics);
  if (entries.length === 0) {
    return '<span class="muted">--</span>';
  }
  return `
    <div class="metric-list">
      ${entries
        .map(([key, value]) => `<span class="metric-chip">${escapeHtml(metricLabel(key))}: ${escapeHtml(formatMetricValue(key, value))}</span>`)
        .join("")}
    </div>
  `;
}

function resetResultDetail() {
  state.selectedResultSymbol = null;
  state.resultSeries = [];
  state.resultSeriesStock = null;
  byId("resultDetailHome").innerHTML = "";
}

function selectResultStock(symbol, options = {}) {
  if (!state.result || !symbol) {
    resetResultDetail();
    return;
  }
  state.selectedResultSymbol = symbol;
  if (!options.keepPeriod) {
    state.resultPeriod = "day";
  }
  if (!options.skipRender) {
    renderResultRows();
  }
  loadResultStockDetail(symbol);
}

function selectResultPeriod(period) {
  state.resultPeriod = period;
  document.querySelectorAll("[data-result-period]").forEach((button) => {
    button.classList.toggle("active", button.dataset.resultPeriod === period);
  });
  if (state.selectedResultSymbol) {
    loadResultStockDetail(state.selectedResultSymbol);
  }
}

async function loadResultStockDetail(symbol) {
  const selectedRow = (state.result?.rows || []).find((row) => row.symbol === symbol);
  if (!selectedRow) {
    resetResultDetail();
    return;
  }

  document.querySelectorAll("[data-result-period]").forEach((button) => {
    button.classList.toggle("active", button.dataset.resultPeriod === state.resultPeriod);
  });

  renderResultMetricGrid(selectedRow);

  try {
    const payload = await api(
      `/api/stocks/${encodeURIComponent(symbol)}/ohlcv?period=${encodeURIComponent(state.resultPeriod)}&limit=10000`,
    );
    state.resultSeries = payload.rows || [];
    state.resultSeriesStock = payload.stock || selectedRow.stock || selectedRow;
    renderResultMetricGrid(selectedRow, state.resultSeriesStock);
    setupResultRange();
    renderResultChartWindow();
  } catch (error) {
    renderEmptyChart(error.message, {
      canvasId: "resultStockChart",
      titleId: "resultKlineTitle",
      metaId: "resultKlineMeta",
    });
  }
}

function renderResultMetricGrid(row, resolvedStock = null) {
  const stock = resolvedStock || row.stock || {};
  const metrics = row.metrics || {};
  const cards = [
    ["最新日期", row.latest_date || stock.latest_date || "--"],
    ["收盘价", formatMaybeNumber(row.close ?? stock.close)],
    ["开盘价", formatMaybeNumber(stock.open)],
    ["最高价", formatMaybeNumber(stock.high)],
    ["最低价", formatMaybeNumber(stock.low)],
    ["成交量", formatNumber(stock.volume)],
    ["成交额", formatMaybeNumber(stock.turnover)],
    ["区间最高价(复权后)", formatMaybeNumber(metrics.window_high)],
    ["区间最低价(复权后)", formatMaybeNumber(metrics.window_low)],
    ["区间振幅", formatMetricValue("amplitude_pct", metrics.amplitude_pct)],
    ["距区间高点", formatMetricValue("distance_to_high_pct", metrics.distance_to_high_pct)],
  ];
  byId("resultMetricGrid").innerHTML = cards
    .map(
      ([label, value]) => `
        <div class="detail-stat">
          <div class="metric-label">${escapeHtml(label)}</div>
          <div class="detail-stat-value">${escapeHtml(value)}</div>
        </div>
      `,
    )
    .join("");
}

function setupResultRange() {
  const range = byId("resultRange");
  const total = state.resultSeries.length;
  state.resultWindowSize = periodWindowSize(state.resultPeriod);
  const maxStart = Math.max(0, total - state.resultWindowSize);
  range.min = 0;
  range.max = maxStart;
  range.step = 1;
  range.value = maxStart;
  range.disabled = total <= state.resultWindowSize;
}

function renderResultChartWindow() {
  const rows = state.resultSeries || [];
  const range = byId("resultRange");
  if (!rows.length) {
    byId("resultRangeLabel").textContent = "--";
    renderEmptyChart("暂无 K 线数据", {
      canvasId: "resultStockChart",
      titleId: "resultKlineTitle",
      metaId: "resultKlineMeta",
    });
    return;
  }

  const start = Number.parseInt(range.value || "0", 10);
  const end = Math.min(rows.length, start + state.resultWindowSize);
  const windowRows = rows.slice(start, end);
  byId("resultRangeLabel").textContent =
    `${PERIOD_LABELS[state.resultPeriod]}：${windowRows[0].date} 至 ${windowRows[windowRows.length - 1].date}，共 ${formatNumber(rows.length)} 条`;
  renderKlineChart(windowRows, state.resultSeriesStock, {
    canvasId: "resultStockChart",
    titleId: "resultKlineTitle",
    metaId: "resultKlineMeta",
  });
}

function periodWindowSize(period) {
  return {
    day: 140,
    week: 120,
    month: 120,
    quarter: 100,
    year: 80,
  }[period] || 120;
}

function metricLabel(key) {
  return METRIC_LABELS[key] || key;
}

function formatMetricValue(key, value) {
  if (value === null || value === undefined || value === "") {
    return "--";
  }
  if (key.endsWith("_pct")) {
    return `${formatMaybeNumber(value)}%`;
  }
  return formatMaybeNumber(value);
}

function formatParameterValue(key, value) {
  if (key.endsWith("_pct")) {
    return `${formatMaybeNumber(value)}%`;
  }
  return formatMaybeNumber(value);
}

function showStrategyMessage(text, kind) {
  const message = byId("strategyMessage");
  message.textContent = text;
  message.className = `message ${kind || ""}`;
}

function jobLabel(key) {
  const labels = {
    kind: "任务类型",
    status: "运行状态",
    message: "当前消息",
    progress: "处理进度",
    current_symbol: "当前股票",
    current_action: "当前动作",
    current_start_date: "本次起点",
    success: "成功股票",
    skipped: "跳过股票",
    failed: "失败股票",
    rows_written: "写入行数",
    mode: "更新模式",
    started_at: "开始时间",
    finished_at: "结束时间",
    result: "最终结果",
    error: "错误",
  };
  return labels[key] || key;
}

function jobKindLabel(kind) {
  return {
    backfill: "历史 K 线更新",
    sync: "每日增量更新",
    metadata: "股票画像同步",
  }[kind] || kind || "--";
}

function jobStatusLabel(status) {
  return {
    queued: "排队中",
    running: "运行中",
    succeeded: "已完成",
    failed: "失败",
  }[status] || status || "--";
}

function formatJobResult(result) {
  if (!result) {
    return "--";
  }
  const labels = {
    symbol_count: "股票总数",
    success: "成功",
    skipped: "跳过",
    failed: "失败",
    rows_written: "写入行数",
    row_count: "写入行数",
    local_symbols: "本地股票",
    basic_records: "基础资料",
    boards: "板块数",
    members: "成分关系",
    start_date: "起始日期",
    end_date: "结束日期",
    full_refresh: "强制重拉",
  };
  return Object.entries(result)
    .map(([key, value]) => `${labels[key] || key}: ${typeof value === "number" ? formatNumber(value) : value}`)
    .join("，");
}

function formatNumber(value) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numberFormatter.format(numeric) : "--";
}

function formatMaybeNumber(value) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numberFormatter.format(numeric) : String(value ?? "--");
}

function formatPercent(value) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? `${numeric.toFixed(2)}%` : "--";
}

function formatSignedPercent(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "--";
  }
  return `${numeric > 0 ? "+" : ""}${numeric.toFixed(2)}%`;
}

function formatSignedNumber(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "--";
  }
  return `${numeric > 0 ? "+" : ""}${numeric.toFixed(3)}`;
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}
