import sqlite3

import pandas as pd

from sequoia_x.core.config import Settings
from sequoia_x.data.engine import DataEngine


def test_sync_today_skips_weekend_when_latest_local_data_is_current(tmp_path, monkeypatch) -> None:
    settings = Settings(
        db_path=str(tmp_path / "market.db"),
        start_date="2024-01-01",
        feishu_webhook_url="https://example.com/hook",
    )
    engine = DataEngine(settings)
    rows = pd.DataFrame(
        [
            {
                "symbol": "000001",
                "date": "2026-05-15",
                "open": 1,
                "high": 1,
                "low": 1,
                "close": 1,
                "volume": 100,
                "turnover": 1000,
            }
        ]
    )
    with sqlite3.connect(engine.db_path) as conn:
        rows.to_sql("stock_daily", conn, if_exists="append", index=False)

    called = {"baostock": False, "tushare": False}
    monkeypatch.setattr(
        engine,
        "_ts_fetch_daily_all",
        lambda trade_date: called.__setitem__("tushare", True),
    )
    monkeypatch.setattr(
        engine,
        "_bs_sync_today_bulk",
        lambda today_str: called.__setitem__("baostock", True),
    )

    result = engine.sync_today_bulk(today_str="2026-05-16")

    assert result == 0
    assert called == {"baostock": False, "tushare": False}
