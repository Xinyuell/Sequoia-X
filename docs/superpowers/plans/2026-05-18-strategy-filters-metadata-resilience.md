# Strategy Filters Metadata Resilience Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace native multi-select filters with checkbox multi-select groups and make metadata sync preserve cached board data during upstream failures.

**Architecture:** Keep the backend filter payload unchanged and only change how the WebUI renders and collects selected values. Harden `DataEngine.sync_stock_metadata()` by treating AkShare board failures as partial fetch failures and by avoiding destructive table clears when no usable replacement data was fetched.

**Tech Stack:** FastAPI, SQLite, plain HTML/CSS/JavaScript, pytest.

---

### Task 1: Backend Metadata Resilience

**Files:**
- Modify: `sequoia_x/data/engine.py`
- Test: `tests/test_data_engine.py`

- [ ] **Step 1: Write the failing test**

Add a test that seeds cached board rows, mocks `_fetch_akshare_boards()` to return empty data for the board type, runs `sync_stock_metadata()`, and asserts the cached board/member rows are still present.

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_data_engine.py::test_sync_stock_metadata_preserves_cached_boards_when_upstream_empty -q`

Expected: FAIL because `_write_stock_boards()` currently deletes members for a board type even when no replacement data was fetched.

- [ ] **Step 3: Implement minimal backend change**

Change `sync_stock_metadata()` and `_write_stock_boards()` so empty board/member fetches are treated as cache-preserving no-ops. Keep normal writes destructive for successful replacement fetches.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_data_engine.py::test_sync_stock_metadata_preserves_cached_boards_when_upstream_empty -q`

Expected: PASS.

### Task 2: Checkbox Filter Groups

**Files:**
- Modify: `sequoia_x/web/static/index.html`
- Modify: `sequoia_x/web/static/app.js`
- Modify: `sequoia_x/web/static/styles.css`

- [ ] **Step 1: Write frontend-facing collection test**

Add lightweight JavaScript-free coverage by keeping the backend API tests unchanged and relying on browser verification for rendered control behavior.

- [ ] **Step 2: Replace native selects**

Replace `industryFilter`, `conceptFilter`, and `marketFilter` `<select multiple>` elements with empty checkbox group containers that have the same IDs.

- [ ] **Step 3: Render option groups**

Add `renderCheckboxGroup()` and selection helpers in `app.js`. Each group renders all/none buttons and checkbox rows. Initial render selects all options for industry, concept, and market groups.

- [ ] **Step 4: Collect checked values**

Update `selectedValues(id)` so it reads checked checkbox inputs inside the group container.

- [ ] **Step 5: Add help text**

Add concise descriptions below listed-days and 20-day turnover fields.

### Task 3: Verification

**Files:**
- No new files.

- [ ] **Step 1: Run focused tests**

Run: `pytest tests/test_data_engine.py tests/test_web_api.py -q`

Expected: all selected tests pass.

- [ ] **Step 2: Start WebUI**

Run the local WebUI, open it in the in-app browser, switch to the strategy page, and verify that the three stock-pool filters render as checkbox groups with all/none controls.

- [ ] **Step 3: Check generated payload**

Use the page state to confirm checked options are collected into arrays matching the existing backend field names.
