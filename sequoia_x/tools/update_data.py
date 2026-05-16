"""Local market data update helper for batch files and scheduled tasks."""

from __future__ import annotations

import argparse
import os
from datetime import datetime
from typing import Any

from dotenv import load_dotenv

from sequoia_x.core.config import Settings
from sequoia_x.data.engine import DataEngine


def main() -> None:
    parser = argparse.ArgumentParser(description="Update Sequoia-X local market data")
    parser.add_argument(
        "--mode",
        choices=["daily", "backfill"],
        default="daily",
        help="daily updates existing local data; backfill fetches historical data for all symbols",
    )
    parser.add_argument("--start-date", default=None, help="Historical backfill start date, YYYY-MM-DD")
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Re-fetch data from start date instead of only filling missing days",
    )
    parser.add_argument(
        "--source",
        choices=["auto", "tushare", "baostock"],
        default="auto",
        help="Data source: auto (baostock primary, tushare fallback), tushare, or baostock",
    )
    args = parser.parse_args()

    load_dotenv()
    settings = _load_settings()
    engine = DataEngine(settings)

    print(f"[{_now()}] Database: {engine.db_path}", flush=True)
    stock_basic = engine.sync_stock_basic()
    print(f"[{_now()}] Stock names synced: {len(stock_basic)}", flush=True)

    if args.mode == "backfill":
        symbols = [record["symbol"] for record in stock_basic] or engine.get_all_symbols()
        result = engine.backfill(
            symbols,
            start_date=args.start_date or settings.start_date,
            full_refresh=args.full_refresh,
            progress_callback=_make_progress_printer("Historical backfill"),
            source=args.source,
        )
        _print_result("Historical backfill", result)
        return

    symbols = [record["symbol"] for record in stock_basic] or engine.get_all_symbols()
    local_symbols = engine.get_local_symbols()
    if not local_symbols:
        print(
            f"[{_now()}] No local K-line data found; starting initial backfill "
            f"from {args.start_date or settings.start_date}",
            flush=True,
        )
        result = engine.backfill(
            symbols,
            start_date=args.start_date or settings.start_date,
            progress_callback=_make_progress_printer("Initial backfill"),
            source=args.source,
        )
        _print_result("Initial backfill", result)
        return

    missing_symbols = sorted(set(symbols) - set(local_symbols))
    if missing_symbols:
        print(
            f"[{_now()}] Local K-line coverage is incomplete: "
            f"{len(local_symbols)}/{len(symbols)} symbols. Continuing historical backfill.",
            flush=True,
        )
        result = engine.backfill(
            symbols,
            start_date=args.start_date or settings.start_date,
            progress_callback=_make_progress_printer("Resume backfill"),
            source=args.source,
        )
        _print_result("Resume backfill", result)
        return

    row_count = engine.sync_today_bulk()
    print(f"[{_now()}] Daily incremental update rows: {row_count}", flush=True)


def _load_settings() -> Settings:
    if os.environ.get("FEISHU_WEBHOOK_URL"):
        return Settings()
    return Settings(feishu_webhook_url="dummy")


def _print_result(label: str, result: dict[str, Any] | None) -> None:
    print(f"[{_now()}] {label} result: {result}", flush=True)


def _make_progress_printer(label: str):
    last_reported = {"processed": -1}

    def progress(**values: Any) -> None:
        total = int(values.get("total") or 0)
        processed = int(values.get("processed") or 0)
        if total <= 0:
            return
        if processed < total and processed - last_reported["processed"] < 100:
            return
        last_reported["processed"] = processed
        print(
            f"[{_now()}] {label}: {processed}/{total}, "
            f"success={values.get('success', 0)}, skipped={values.get('skipped', 0)}, "
            f"failed={values.get('failed', 0)}, rows={values.get('rows_written', 0)}, "
            f"current={values.get('current_symbol', '--')}",
            flush=True,
        )

    return progress


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


if __name__ == "__main__":
    main()
