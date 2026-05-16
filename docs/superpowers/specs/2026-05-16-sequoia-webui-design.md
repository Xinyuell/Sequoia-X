# Sequoia-X Local WebUI Design

## Goal

Add a local WebUI to Sequoia-X so a user can visually manage market data backfill, inspect strategy stock-picking results, and run configurable strategies from a browser. The design keeps the existing daily close workflow intact while adding a local interactive workflow for research and manual screening.

## Scope

The first version targets a single local user on one machine. It does not require authentication, multi-user permissions, cloud deployment, or external job infrastructure.

Included:

- A local browser UI for data status, market data backfill, incremental sync, strategy selection, parameter editing, and results display.
- A backend API that reuses the existing SQLite database, `DataEngine`, and strategy implementations.
- A strategy registry that makes strategies discoverable by the WebUI and supports parameter schemas.
- A parameterized sideways consolidation strategy with editable `N`, `M%`, and `A%` inputs.
- Tests for parameter validation, strategy execution, registry discovery, and API behavior without real baostock or Feishu calls.

Excluded from the first version:

- User accounts, authentication, and remote deployment hardening.
- Automatic Feishu push from the WebUI run button. The existing CLI push flow remains responsible for scheduled notifications.
- A full React or Vue build pipeline.
- Persistent job queues such as Celery or Redis.

## Current Project Context

The project is currently a small Python package with:

- `main.py` as the CLI entry point for daily mode and historical backfill mode.
- `sequoia_x.data.engine.DataEngine` for SQLite initialization, baostock historical backfill, daily sync, and local OHLCV reads.
- `sequoia_x.strategy.BaseStrategy` and concrete strategy classes whose `run()` method returns `list[str]`.
- `sequoia_x.notify.FeishuNotifier` for scheduled push notifications.
- Tests based on `pytest` and `hypothesis`.

The existing strategy list is manually assembled in `main.py`, and strategies do not currently expose metadata or editable parameters to a UI. Some current baseline tests call baostock indirectly or leave SQLite files locked on Windows, so the new implementation should keep new unit tests isolated from real network and long-running data sources.

## Recommended Architecture

Use FastAPI for the local backend and serve a static HTML/CSS/JavaScript WebUI from the same process.

New entry point:

```bash
python -m sequoia_x.web
```

Proposed modules:

- `sequoia_x/web/__main__.py`: starts the local server with uvicorn.
- `sequoia_x/web/app.py`: creates the FastAPI app, mounts static files, and wires API routers.
- `sequoia_x/web/api.py`: API route handlers for data status, jobs, strategies, and results.
- `sequoia_x/web/jobs.py`: in-memory background job manager for backfill and sync.
- `sequoia_x/web/static/index.html`: local WebUI shell.
- `sequoia_x/web/static/styles.css`: tool-style layout and visual states.
- `sequoia_x/web/static/app.js`: API calls, form rendering, task polling, and result rendering.
- `sequoia_x/strategy/registry.py`: discoverable strategy registry and shared strategy metadata.
- `sequoia_x/strategy/result.py`: result data structures for WebUI-friendly detail rows.
- `sequoia_x/strategy/sideways_consolidation.py`: configurable sideways consolidation strategy.

`main.py` should continue to use the same strategies as before. It may be refactored to consume the registry only if that reduces duplication without changing CLI behavior.

## Data Flow

Data status:

1. WebUI loads.
2. Browser calls `GET /api/data/summary`.
3. Backend reads SQLite through lightweight `DataEngine` helpers.
4. UI displays local symbol count, row count, earliest date, latest date, database path, and whether data exists.

Historical backfill:

1. User clicks the backfill button.
2. Browser calls `POST /api/data/backfill`.
3. Backend creates a background job and starts a thread.
4. The job calls `DataEngine.get_all_symbols()` and `DataEngine.backfill(symbols)`.
5. Browser polls `GET /api/jobs/{job_id}` for status.
6. UI shows running, success, or failure states.

Incremental sync:

1. User clicks the sync button.
2. Browser calls `POST /api/data/sync`.
3. Backend creates a background job and calls `DataEngine.sync_today_bulk()`.
4. UI polls job status and refreshes data summary after completion.

Strategy execution:

1. Browser calls `GET /api/strategies`.
2. Backend returns registry metadata and parameter schemas.
3. UI renders a parameter form for the selected strategy.
4. Browser calls `POST /api/strategies/{key}/run` with parameter values.
5. Backend validates parameters, runs the strategy, and returns result rows.
6. UI renders the latest result table and basic count/status feedback.

## API Design

`GET /`

Returns the static WebUI page.

`GET /api/data/summary`

Returns:

```json
{
  "db_path": "data/sequoia_v2.db",
  "symbol_count": 5200,
  "row_count": 1250000,
  "earliest_date": "2024-01-02",
  "latest_date": "2026-05-15",
  "has_data": true
}
```

`POST /api/data/backfill`

Starts a historical backfill job.

Returns:

```json
{
  "job_id": "backfill-20260516-153000",
  "status": "queued"
}
```

`POST /api/data/sync`

Starts an incremental sync job.

Returns the same job envelope.

`GET /api/jobs/{job_id}`

Returns:

```json
{
  "job_id": "sync-20260516-153000",
  "kind": "sync",
  "status": "running",
  "message": "Syncing latest market data",
  "started_at": "2026-05-16T15:30:00",
  "finished_at": null,
  "result": null,
  "error": null
}
```

Allowed job statuses: `queued`, `running`, `succeeded`, `failed`.

`GET /api/strategies`

Returns:

```json
[
  {
    "key": "sideways_consolidation",
    "name": "横盘振荡",
    "description": "筛选近期横盘整理且接近区间高点的股票",
    "parameters": [
      {
        "key": "lookback_days",
        "label": "横盘交易日",
        "type": "integer",
        "default": 20,
        "min": 5,
        "max": 120,
        "step": 1,
        "unit": "日"
      },
      {
        "key": "max_amplitude_pct",
        "label": "最大区间振幅",
        "type": "number",
        "default": 12,
        "min": 1,
        "max": 80,
        "step": 0.5,
        "unit": "%"
      },
      {
        "key": "near_high_pct",
        "label": "距区间高点不超过",
        "type": "number",
        "default": 3,
        "min": 0,
        "max": 30,
        "step": 0.5,
        "unit": "%"
      }
    ]
  }
]
```

`POST /api/strategies/{key}/run`

Request:

```json
{
  "parameters": {
    "lookback_days": 20,
    "max_amplitude_pct": 12,
    "near_high_pct": 3
  }
}
```

Response:

```json
{
  "strategy_key": "sideways_consolidation",
  "strategy_name": "横盘振荡",
  "parameters": {
    "lookback_days": 20,
    "max_amplitude_pct": 12,
    "near_high_pct": 3
  },
  "total": 2,
  "rows": [
    {
      "symbol": "000001",
      "latest_date": "2026-05-15",
      "close": 10.5,
      "metrics": {
        "window_high": 10.8,
        "window_low": 9.7,
        "amplitude_pct": 11.34,
        "distance_to_high_pct": 2.78
      }
    }
  ]
}
```

## Strategy Framework

The registry should describe strategies independently from the CLI. A WebUI strategy can expose:

- `key`: stable machine-readable identifier.
- `name`: human-readable Chinese name.
- `description`: concise explanation shown in the UI.
- `parameters`: schema used by both API validation and form rendering.
- `factory`: callable that creates the strategy instance.

Parameter schema supports:

- `integer`, `number`, `boolean`, and `choice`.
- `default`, `min`, `max`, `step`, `unit`, and `options`.
- Server-side validation for missing values, type conversion, and range limits.

Existing strategies can be registered with empty parameter lists first. Later, strategies such as RPS breakout can expose `rps_period` and `rps_threshold` without changing the WebUI rendering code.

Strategies should continue to support the existing `run() -> list[str]` contract where practical. For WebUI detail rows, parameterized strategies may additionally expose a `run_with_details()` method or return a shared `StrategyRunResult` wrapper through an adapter. The CLI should keep using the simple symbol list to preserve notification behavior.

## Sideways Consolidation Strategy

The first configurable strategy is `SidewaysConsolidationStrategy`.

Default parameters:

- `lookback_days`: `20`
- `max_amplitude_pct`: `12.0`
- `near_high_pct`: `3.0`

For each local symbol:

1. Read OHLCV rows ordered by date.
2. Skip symbols with fewer than `lookback_days` rows.
3. Select the latest `lookback_days` rows.
4. Compute `window_high = max(high)`.
5. Compute `window_low = min(low)`.
6. Compute `amplitude_pct = (window_high - window_low) / window_low * 100`.
7. Compute `distance_to_high_pct = (window_high - latest_close) / window_high * 100`.
8. Select the symbol when `amplitude_pct <= max_amplitude_pct` and `distance_to_high_pct <= near_high_pct`.
9. Return result rows sorted by `distance_to_high_pct` ascending, then `amplitude_pct` ascending.

Invalid or unsafe data handling:

- Skip symbols with empty data.
- Skip windows with null high, low, or close values.
- Skip windows where `window_low <= 0` or `window_high <= 0`.

## WebUI Design

The UI should feel like a compact local operations console, not a landing page.

Layout:

- A top bar with project name, local database status, and latest data date.
- A left navigation rail or tabs for `数据`, `策略`, and `结果`.
- Main content area with dense, readable controls.

Data view:

- Summary metrics: symbol count, row count, date range, database path.
- Buttons: `历史回填`, `增量同步`, `刷新状态`.
- Job status panel showing current job kind, status, message, start time, finish time, and error if any.

Strategy view:

- Strategy selector.
- Description and parameter form generated from schema.
- Run button and loading state.
- Clear validation messages for invalid input.

Results view:

- Table with symbol, latest date, close, strategy, and metrics.
- Empty state when no strategy has been run.
- Count summary and selected parameter echo so the user can see what produced the result.

Visual constraints:

- No hero page.
- No decorative marketing sections.
- Controls should be stable in width and height so loading states do not shift the layout.
- Text must fit on desktop and mobile widths.

## Error Handling

API errors should return JSON with a clear message:

```json
{
  "detail": "lookback_days must be between 5 and 120"
}
```

Expected errors:

- Missing local database or empty data.
- Invalid strategy key.
- Invalid parameter type or range.
- Backfill or sync already running.
- baostock failures during background jobs.

The UI should show recoverable errors inline and keep the page usable.

## Testing Strategy

Add tests that avoid real external services:

- Strategy schema validation accepts defaults and rejects out-of-range values.
- Strategy registry lists expected built-in strategies.
- Sideways consolidation selects stocks that match the N/M/A rules.
- Sideways consolidation skips insufficient or invalid OHLCV data.
- API `GET /api/strategies` returns strategy metadata.
- API `POST /api/strategies/{key}/run` validates parameters and returns result rows.
- API data summary returns counts from a temporary SQLite database.
- Job manager records success and failure states for injected callables.

Keep tests deterministic by using temporary SQLite files, fake `DataEngine` instances, and monkeypatching network-facing calls. Existing baseline test issues around Windows file locks and real baostock access should be treated as separate cleanup work unless they block the new WebUI tests.

## Dependencies

Add runtime dependencies:

- `fastapi`
- `uvicorn`

No Node, npm, React, or Vue dependencies are required for the first version.

## Acceptance Criteria

- Running `python -m sequoia_x.web` starts a local WebUI.
- The WebUI shows local market data coverage from SQLite.
- A user can start historical backfill from the browser.
- A user can start incremental sync from the browser.
- A user can view available strategies from the browser.
- A user can edit strategy parameters from generated controls.
- A user can run the sideways consolidation strategy with custom `N`, `M%`, and `A%`.
- A user can see selected stocks and key metrics in a results table.
- Existing `python main.py` behavior remains available.
- New tests cover strategy registry, parameter validation, sideways strategy behavior, API behavior, and job state transitions without real network calls.
