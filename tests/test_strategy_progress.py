import sqlite3

import pandas as pd

from sequoia_x.core.config import Settings
from sequoia_x.data.engine import DataEngine
from sequoia_x.strategy.sideways_consolidation import SidewaysConsolidationStrategy


def test_sideways_strategy_reports_progress(tmp_path) -> None:
    settings = Settings(
        db_path=str(tmp_path / "strategy.db"),
        start_date="2024-01-01",
        feishu_webhook_url="https://example.com/hook",
    )
    engine = DataEngine(settings)
    rows = []
    for symbol in ["000001", "000002"]:
        for day in range(1, 8):
            rows.append(
                {
                    "symbol": symbol,
                    "date": f"2026-01-{day:02d}",
                    "open": 10.0,
                    "high": 10.5,
                    "low": 10.0,
                    "close": 10.4,
                    "volume": 1000,
                    "turnover": 10000,
                }
            )
    with sqlite3.connect(engine.db_path) as conn:
        pd.DataFrame(rows).to_sql("stock_daily", conn, if_exists="append", index=False)

    events = []
    strategy = SidewaysConsolidationStrategy(
        engine=engine,
        settings=settings,
        lookback_days=5,
        max_amplitude_pct=10,
        min_distance_pct=0,
        max_distance_pct=5,
    )

    result = strategy.run_with_details(progress_callback=lambda **values: events.append(values))

    assert len(result) == 2
    assert events[-1]["processed"] == 2
    assert events[-1]["total"] == 2
