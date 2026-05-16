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
        )
        _print_result("Historical backfill", result)
        return

    local_symbols = engine.get_local_symbols()
    if not local_symbols:
        symbols = [record["symbol"] for record in stock_basic] or engine.get_all_symbols()
        print(
            f"[{_now()}] No local K-line data found; starting initial backfill "
            f"from {args.start_date or settings.start_date}",
            flush=True,
        )
        result = engine.backfill(symbols, start_date=args.start_date or settings.start_date)
        _print_result("Initial backfill", result)
        return

    row_count = engine.sync_today_bulk()
    print(f"[{_now()}] Daily incremental update rows: {row_count}", flush=True)


def _load_settings() -> Settings:
    if os.environ.get("FEISHU_WEBHOOK_URL"):
        return Settings()
    return Settings(feishu_webhook_url="dummy")


def _print_result(label: str, result: dict[str, Any] | None) -> None:
    print(f"[{_now()}] {label} result: {result}", flush=True)


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


if __name__ == "__main__":
    main()

