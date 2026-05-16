const state = {
  strategies: [],
  selectedStrategy: null,
  result: null,
  pollTimer: null,
};

const numberFormatter = new Intl.NumberFormat("zh-CN");

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
  byId("backfillBtn").addEventListener("click", startBackfillJob);
  byId("strategySelect").addEventListener("change", (event) => {
    selectStrategy(event.target.value);
  });
  byId("runStrategyBtn").addEventListener("click", runSelectedStrategy);
}

async function loadInitialData() {
  await Promise.all([loadDataSummary(), loadStrategies()]);
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
  } catch (error) {
    renderJobError(error.message);
    setDataButtonsDisabled(false);
  }
}

function renderJob(job) {
  const fullJob = {
    kind: job.kind || "--",
    status: job.status || "--",
    message: job.message || "任务已创建",
    started_at: job.started_at || "--",
    finished_at: job.finished_at || "--",
    result: job.result ? JSON.stringify(job.result) : "--",
    error: job.error || "--",
  };
  byId("jobPanel").classList.remove("muted");
  byId("jobPanel").innerHTML = Object.entries(fullJob)
    .map(
      ([key, value]) => `
        <div>
          <div class="metric-label">${escapeHtml(jobLabel(key))}</div>
          <div>${escapeHtml(value)}</div>
        </div>
      `,
    )
    .join("");
}

function renderJobError(message) {
  byId("jobPanel").classList.add("muted");
  byId("jobPanel").textContent = `任务失败：${message}`;
}

function setDataButtonsDisabled(disabled) {
  byId("syncBtn").disabled = disabled;
  byId("backfillBtn").disabled = disabled;
  byId("backfillStartDate").disabled = disabled;
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
  showStrategyMessage("策略运行中", "");

  try {
    const parameters = collectParameters();
    const result = await api(`/api/strategies/${encodeURIComponent(state.selectedStrategy.key)}/run`, {
      method: "POST",
      body: JSON.stringify({ parameters }),
    });
    state.result = result;
    renderResults(result);
    showStrategyMessage(`运行完成：${result.total} 只`, "ok");
    switchView("resultsView");
  } catch (error) {
    showStrategyMessage(error.message, "error");
  } finally {
    byId("runStrategyBtn").disabled = false;
  }
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

function renderResults(result) {
  byId("resultCount").textContent = `${formatNumber(result.total)} 只`;
  const parameterText = Object.entries(result.parameters || {})
    .map(([key, value]) => `${key}=${value}`)
    .join("，");
  byId("resultMeta").textContent = parameterText
    ? `${result.strategy_name}；${parameterText}`
    : result.strategy_name;

  const rows = result.rows || [];
  if (rows.length === 0) {
    byId("resultRows").innerHTML = `
      <tr>
        <td colspan="5" class="muted">没有符合条件的股票</td>
      </tr>
    `;
    return;
  }

  byId("resultRows").innerHTML = rows
    .map(
      (row) => `
        <tr>
          <td>${escapeHtml(row.symbol || "--")}</td>
          <td>${escapeHtml(row.latest_date || "--")}</td>
          <td>${escapeHtml(formatMaybeNumber(row.close))}</td>
          <td>${escapeHtml(result.strategy_name)}</td>
          <td>${renderMetrics(row.metrics || {})}</td>
        </tr>
      `,
    )
    .join("");
}

function renderMetrics(metrics) {
  const entries = Object.entries(metrics);
  if (entries.length === 0) {
    return '<span class="muted">--</span>';
  }
  return `
    <div class="metric-list">
      ${entries
        .map(([key, value]) => `<span class="metric-chip">${escapeHtml(key)}: ${escapeHtml(formatMaybeNumber(value))}</span>`)
        .join("")}
    </div>
  `;
}

function showStrategyMessage(text, kind) {
  const message = byId("strategyMessage");
  message.textContent = text;
  message.className = `message ${kind || ""}`;
}

function jobLabel(key) {
  const labels = {
    kind: "类型",
    status: "状态",
    message: "消息",
    started_at: "开始时间",
    finished_at: "结束时间",
    result: "结果",
    error: "错误",
  };
  return labels[key] || key;
}

function formatNumber(value) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numberFormatter.format(numeric) : "--";
}

function formatMaybeNumber(value) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numberFormatter.format(numeric) : String(value ?? "--");
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}
