import sqlite3
from datetime import date, timedelta

import pandas as pd

from sequoia_x.core.config import Settings
from sequoia_x.data.engine import DataEngine
from sequoia_x.strategy.sideways_consolidation import SidewaysConsolidationStrategy


def make_engine(tmp_path) -> tuple[DataEngine, Settings]:
    settings = Settings(
        db_path=str(tmp_path / "test.db"),
        start_date="2024-01-01",
        feishu_webhook_url="https://example.com/hook",
    )
    return DataEngine(settings), settings


def insert_rows(engine: DataEngine, symbol: str, rows: list[dict]) -> None:
    df = pd.DataFrame(rows)
    df["symbol"] = symbol
    with sqlite3.connect(engine.db_path) as conn:
        df.to_sql("stock_daily", conn, if_exists="append", index=False, method="multi")


def make_rows(days: int, high: float, low: float, latest_close: float) -> list[dict]:
    start = date(2026, 1, 1)
    rows = []
    for offset in range(days):
        rows.append(
            {
                "date": str(start + timedelta(days=offset)),
                "open": 10.0,
                "high": high,
                "low": low,
                "close": latest_close if offset == days - 1 else (high + low) / 2,
                "volume": 1000.0,
                "turnover": 10000.0,
            }
        )
    return rows


def test_sideways_consolidation_selects_matching_stock(tmp_path) -> None:
    engine, settings = make_engine(tmp_path)
    insert_rows(engine, "000001", make_rows(days=20, high=11.0, low=10.0, latest_close=10.9))
    insert_rows(engine, "000002", make_rows(days=20, high=13.0, low=10.0, latest_close=12.9))
    insert_rows(engine, "000003", make_rows(days=20, high=11.0, low=10.0, latest_close=10.0))
    strategy = SidewaysConsolidationStrategy(
        engine=engine,
        settings=settings,
        lookback_days=20,
        max_amplitude_pct=12.0,
        min_distance_pct=0.0,
        max_distance_pct=3.0,
    )

    rows = strategy.run_with_details()

    assert [row.symbol for row in rows] == ["000001"]
    assert rows[0].metrics["window_high"] == 11.0
    assert rows[0].metrics["window_low"] == 10.0


def test_sideways_consolidation_skips_insufficient_and_invalid_data(tmp_path) -> None:
    engine, settings = make_engine(tmp_path)
    insert_rows(engine, "000001", make_rows(days=4, high=11.0, low=10.0, latest_close=10.9))
    invalid_rows = make_rows(days=20, high=11.0, low=10.0, latest_close=10.9)
    invalid_rows[-1]["low"] = None
    insert_rows(engine, "000002", invalid_rows)
    zero_low_rows = make_rows(days=20, high=11.0, low=0.0, latest_close=10.9)
    insert_rows(engine, "000003", zero_low_rows)
    strategy = SidewaysConsolidationStrategy(
        engine=engine,
        settings=settings,
        lookback_days=20,
        max_amplitude_pct=12.0,
        min_distance_pct=0.0,
        max_distance_pct=3.0,
    )

    assert strategy.run_with_details() == []
