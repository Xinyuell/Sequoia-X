# Selection Report Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a lightweight visual analysis report above the final stock selection table.

**Architecture:** Extend the existing strategy run response by enriching the current `backtest` object instead of adding a new endpoint. Render the report with native HTML/CSS inside the existing results view, keeping the stock detail table and chart workflow intact.

**Tech Stack:** FastAPI, SQLite, pytest, plain JavaScript, native CSS.

---

### Task 1: Backend Report Fields

**Files:**
- Modify: `sequoia_x/web/api.py`
- Test: `tests/test_web_api.py`

- [ ] Write a failing API test that expects `backtest.overview`, expanded `backtest.summary`, `backtest.distribution`, and row-level `backtest_valid` / `backtest_invalid_reason`.
- [ ] Run the focused test and confirm it fails on missing fields.
- [ ] Extend `_attach_backtest_returns()` and `_summarize_backtest()` to compute the report fields.
- [ ] Re-run the focused test and confirm it passes.

### Task 2: Results Page Report UI

**Files:**
- Modify: `sequoia_x/web/static/index.html`
- Modify: `sequoia_x/web/static/app.js`
- Modify: `sequoia_x/web/static/styles.css`

- [ ] Replace the current simple backtest chips with a report container above the results table.
- [ ] Render title/meta, sample cards, horizon stat cards, distribution table, and note panel from `result.backtest`.
- [ ] Add row-level backtest status column.
- [ ] Keep existing red/green return coloring and result detail interaction.

### Task 3: Verification

**Files:**
- No new source files.

- [ ] Run `uv run --extra dev pytest tests/test_web_api.py tests/test_data_engine.py -q`.
- [ ] Run `node --check sequoia_x/web/static/app.js`.
- [ ] Open the WebUI and visually verify the report layout.
