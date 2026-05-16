import sqlite3
from datetime import date, timedelta

import pandas as pd
from fastapi.testclient import TestClient

from sequoia_x.core.config import Settings
from sequoia_x.data.engine import DataEngine
from sequoia_x.web.app import create_app
from sequoia_x.web.jobs import InMemoryJobManager


def make_app(tmp_path) -> TestClient:
    settings = Settings(
        db_path=str(tmp_path / "test.db"),
        start_date="2024-01-01",
        feishu_webhook_url="https://example.com/hook",
    )
    engine = DataEngine(settings)
    insert_rows(engine, "000001", make_rows(days=20, high=11.0, low=10.0, latest_close=10.9))
    with sqlite3.connect(engine.db_path) as conn:
        conn.execute(
            """
            INSERT INTO stock_basic(symbol, code, name, status, stock_type, updated_at)
            VALUES ('000001', 'sz.000001', '平安银行', '1', '1', '2026-05-16T00:00:00')
            """
        )
        conn.commit()
    app = create_app(
        settings=settings,
        engine=engine,
        jobs=InMemoryJobManager(run_async=False),
    )
    return TestClient(app)


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


def test_data_summary_returns_sqlite_counts(tmp_path) -> None:
    client = make_app(tmp_path)

    response = client.get("/api/data/summary")

    assert response.status_code == 200
    payload = response.json()
    assert payload["symbol_count"] == 1
    assert payload["row_count"] == 20
    assert payload["earliest_date"] == "2026-01-01"
    assert payload["latest_date"] == "2026-01-20"
    assert payload["has_data"] is True


def test_api_lists_strategy_metadata(tmp_path) -> None:
    client = make_app(tmp_path)

    response = client.get("/api/strategies")

    assert response.status_code == 200
    strategies = response.json()
    sideways = next(item for item in strategies if item["key"] == "sideways_consolidation")
    assert sideways["parameters"][0]["key"] == "lookback_days"


def test_api_runs_sideways_strategy_with_parameters(tmp_path) -> None:
    client = make_app(tmp_path)

    response = client.post(
        "/api/strategies/sideways_consolidation/run",
        json={
            "parameters": {
                "lookback_days": 20,
                "max_amplitude_pct": 12,
                "min_distance_pct": 0,
                "max_distance_pct": 3,
            }
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["rows"][0]["symbol"] == "000001"
    assert payload["rows"][0]["name"] == "平安银行"
    assert payload["rows"][0]["stock"]["row_count"] == 20
    assert payload["rows"][0]["metrics"]["distance_to_high_pct"] < 3


def test_api_rejects_invalid_strategy_parameters(tmp_path) -> None:
    client = make_app(tmp_path)

    response = client.post(
        "/api/strategies/sideways_consolidation/run",
        json={"parameters": {"lookback_days": 2}},
    )

    assert response.status_code == 422
    assert "lookback_days" in response.json()["detail"]


def test_backfill_api_passes_start_date_and_full_refresh(tmp_path) -> None:
    settings = Settings(
        db_path=str(tmp_path / "fake.db"),
        start_date="2024-01-01",
        feishu_webhook_url="https://example.com/hook",
    )

    class FakeEngine:
        db_path = settings.db_path

        def __init__(self) -> None:
            self.calls = []

        def get_all_symbols(self) -> list[str]:
            return ["000001", "600000"]

        def backfill(self, symbols, start_date=None, full_refresh=False):
            self.calls.append((symbols, start_date, full_refresh))
            return {
                "symbol_count": len(symbols),
                "success": 2,
                "skipped": 0,
                "failed": 0,
                "rows_written": 12,
                "start_date": start_date,
                "end_date": "2026-05-16",
                "full_refresh": full_refresh,
            }

    engine = FakeEngine()
    app = create_app(
        settings=settings,
        engine=engine,
        jobs=InMemoryJobManager(run_async=False),
    )
    client = TestClient(app)

    response = client.post(
        "/api/data/backfill",
        json={"start_date": "1990-01-01", "full_refresh": True},
    )

    assert response.status_code == 200
    job = client.get(f"/api/jobs/{response.json()['job_id']}").json()
    assert job["status"] == "succeeded"
    assert job["result"]["rows_written"] == 12
    assert engine.calls == [(["000001", "600000"], "1990-01-01", True)]


def test_api_lists_local_stocks_with_names(tmp_path) -> None:
    settings = Settings(
        db_path=str(tmp_path / "stocks.db"),
        start_date="2024-01-01",
        feishu_webhook_url="https://example.com/hook",
    )
    engine = DataEngine(settings)
    insert_rows(engine, "000001", make_rows(days=5, high=11.0, low=10.0, latest_close=10.8))
    with sqlite3.connect(engine.db_path) as conn:
        conn.execute(
            """
            INSERT INTO stock_basic(symbol, code, name, status, stock_type, updated_at)
            VALUES ('000001', 'sz.000001', '平安银行', '1', '1', '2026-05-16T00:00:00')
            """
        )
        conn.commit()
    app = create_app(settings=settings, engine=engine, jobs=InMemoryJobManager(run_async=False))
    client = TestClient(app)

    response = client.get("/api/stocks?query=平安")

    assert response.status_code == 200
    payload = response.json()
    assert payload[0]["symbol"] == "000001"
    assert payload[0]["name"] == "平安银行"
    assert payload[0]["row_count"] == 5
    assert payload[0]["latest_date"] == "2026-01-05"


def test_api_returns_ohlcv_tail(tmp_path) -> None:
    client = make_app(tmp_path)

    response = client.get("/api/stocks/000001/ohlcv?limit=3")

    assert response.status_code == 200
    payload = response.json()
    assert payload["symbol"] == "000001"
    assert [row["date"] for row in payload["rows"]] == [
        "2026-01-18",
        "2026-01-19",
        "2026-01-20",
    ]
    assert payload["period"] == "day"
    assert payload["stock"]["name"] == "平安银行"


def test_api_returns_weekly_ohlcv(tmp_path) -> None:
    client = make_app(tmp_path)

    response = client.get("/api/stocks/000001/ohlcv?period=week&limit=2")

    assert response.status_code == 200
    payload = response.json()
    assert payload["period"] == "week"
    assert len(payload["rows"]) == 2
    assert payload["rows"][-1]["date"] == "2026-01-20"
    assert payload["rows"][-1]["high"] == 11.0
    assert payload["rows"][-1]["low"] == 10.0
