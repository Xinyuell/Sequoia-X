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


def test_static_assets_are_not_cached(tmp_path) -> None:
    client = make_app(tmp_path)

    index_response = client.get("/")
    app_response = client.get("/static/app.js")

    assert "no-store" in index_response.headers["cache-control"]
    assert "no-store" in app_response.headers["cache-control"]


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


def test_api_runs_strategy_as_progress_job(tmp_path) -> None:
    client = make_app(tmp_path)

    response = client.post(
        "/api/strategies/sideways_consolidation/run-job",
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
    job = client.get(f"/api/jobs/{response.json()['job_id']}").json()
    assert job["status"] == "succeeded"
    assert job["result"]["total"] == 1
    assert job["progress"]["strategy_key"] == "sideways_consolidation"
    assert job["progress"]["matched"] == 1


def test_api_applies_reference_date_to_non_sideways_strategy(tmp_path) -> None:
    settings = Settings(
        db_path=str(tmp_path / "reference.db"),
        start_date="2024-01-01",
        feishu_webhook_url="https://example.com/hook",
    )
    engine = DataEngine(settings)
    rows = []
    start = date(2026, 1, 1)
    for offset in range(22):
        close = 10.0
        volume = 1000.0
        if offset == 19:
            close = 9.0
        elif offset == 20:
            close = 20.0
            volume = 3000.0
        elif offset == 21:
            close = 5.0
        rows.append(
            {
                "date": str(start + timedelta(days=offset)),
                "open": close,
                "high": close,
                "low": close,
                "close": close,
                "volume": volume,
                "turnover": volume * close,
            }
        )
    insert_rows(engine, "000001", rows)
    app = create_app(settings=settings, engine=engine, jobs=InMemoryJobManager(run_async=False))
    client = TestClient(app)

    historical = client.post(
        "/api/strategies/ma_volume/run",
        json={"parameters": {}, "reference_date": "2026-01-21", "backtest_days": [1]},
    )
    latest = client.post(
        "/api/strategies/ma_volume/run",
        json={"parameters": {}, "backtest_days": [1]},
    )

    assert historical.status_code == 200
    assert historical.json()["rows"][0]["symbol"] == "000001"
    assert historical.json()["rows"][0]["latest_date"] == "2026-01-21"
    assert latest.status_code == 200
    assert latest.json()["total"] == 0


def test_api_adds_backtest_returns_and_summary(tmp_path) -> None:
    settings = Settings(
        db_path=str(tmp_path / "backtest.db"),
        start_date="2024-01-01",
        feishu_webhook_url="https://example.com/hook",
    )
    engine = DataEngine(settings)
    start = date(2026, 1, 1)
    closes = [10.9] * 20 + [11.2, 10.9, 10.75, 12.1, 9.7]
    rows = []
    for offset, close in enumerate(closes):
        rows.append(
            {
                "date": str(start + timedelta(days=offset)),
                "open": close,
                "high": 11.0 if offset < 20 else close,
                "low": 10.0 if offset < 20 else close,
                "close": close,
                "volume": 1000.0,
                "turnover": 10000.0,
            }
        )
    insert_rows(engine, "000001", rows)
    insert_rows(engine, "000002", rows[:20])
    app = create_app(settings=settings, engine=engine, jobs=InMemoryJobManager(run_async=False))
    client = TestClient(app)

    response = client.post(
        "/api/strategies/sideways_consolidation/run",
        json={
            "reference_date": "2026-01-20",
            "backtest_days": [1, 3, 5],
            "parameters": {
                "lookback_days": 20,
                "max_amplitude_pct": 12,
                "min_distance_pct": 0,
                "max_distance_pct": 3,
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["backtest"]["horizons"] == [1, 3, 5]
    assert payload["backtest"]["overview"] == {
        "total": 2,
        "valid": 1,
        "invalid": 1,
    }
    rows_by_symbol = {row["symbol"]: row for row in payload["rows"]}
    assert rows_by_symbol["000001"]["backtest_valid"] is True
    assert rows_by_symbol["000001"]["backtest_invalid_reason"] is None
    assert rows_by_symbol["000002"]["backtest_valid"] is False
    assert rows_by_symbol["000002"]["backtest_invalid_reason"] == "insufficient_future_data"

    returns = rows_by_symbol["000001"]["backtest_returns"]
    assert returns["1"] > 1
    assert returns["3"] < -1
    assert returns["5"] < -10
    summary_by_day = {item["days"]: item for item in payload["backtest"]["summary"]}
    assert summary_by_day[1]["sample_count"] == 1
    assert summary_by_day[1]["win_count"] == 1
    assert summary_by_day[1]["win_rate"] == 100
    assert summary_by_day[1]["gt_1_count"] == 1
    assert summary_by_day[3]["lt_minus_1_count"] == 1
    assert summary_by_day[5]["lt_minus_10_count"] == 1
    distribution_by_bucket = {
        item["bucket"]: item
        for item in payload["backtest"]["distribution"]
    }
    assert distribution_by_bucket["1% ~ 5%"]["counts"]["1"] == 1
    assert distribution_by_bucket["-5% ~ -1%"]["counts"]["3"] == 1
    assert distribution_by_bucket["<= -10%"]["counts"]["5"] == 1


def test_api_lists_stock_filter_options(tmp_path) -> None:
    client = make_app(tmp_path)
    engine = client.app.state.engine
    with sqlite3.connect(engine.db_path) as conn:
        conn.execute("UPDATE stock_basic SET market = 'SZ', list_date = '1991-04-03'")
        conn.execute(
            """
            INSERT INTO stock_boards(board_code, board_name, board_type, source, fetched_at)
            VALUES ('IND001', '银行', 'industry', 'test', '2026-05-17T00:00:00')
            """
        )
        conn.executemany(
            """
            INSERT INTO stock_boards(board_code, board_name, board_type, source, fetched_at)
            VALUES (?, ?, 'industry', 'test', '2026-05-17T00:00:00')
            """,
            [
                ("IND_SUB_1", "IT服务Ⅱ"),
                ("IND_SUB_2", "其他化学制品"),
            ],
        )
        conn.execute(
            """
            INSERT INTO stock_boards(board_code, board_name, board_type, source, fetched_at)
            VALUES ('CON001', '大金融', 'concept', 'test', '2026-05-17T00:00:00')
            """
        )
        conn.execute(
            """
            INSERT INTO stock_board_members(symbol, board_code, board_type, board_name, source, fetched_at)
            VALUES ('000001', 'IND001', 'industry', '银行', 'test', '2026-05-17T00:00:00')
            """
        )
        conn.execute(
            """
            INSERT INTO stock_board_members(symbol, board_code, board_type, board_name, source, fetched_at)
            VALUES ('000001', 'CON001', 'concept', '大金融', 'test', '2026-05-17T00:00:00')
            """
        )
        conn.commit()

    response = client.get("/api/stock-filters")

    assert response.status_code == 200
    payload = response.json()
    assert payload["industries"] == [
        {"code": "IND_SUB_1", "name": "IT服务Ⅱ"},
        {"code": "IND_SUB_2", "name": "其他化学制品"},
        {"code": "IND001", "name": "银行"},
    ]
    assert payload["concepts"] == [{"code": "CON001", "name": "大金融"}]
    assert {"value": "SZ", "label": "深圳"} in payload["markets"]


def test_api_applies_strategy_stock_filters(tmp_path) -> None:
    settings = Settings(
        db_path=str(tmp_path / "filters.db"),
        start_date="2024-01-01",
        feishu_webhook_url="https://example.com/hook",
    )
    engine = DataEngine(settings)
    insert_rows(engine, "000001", make_rows(days=25, high=11.0, low=10.0, latest_close=10.9))
    insert_rows(engine, "000002", make_rows(days=25, high=11.0, low=10.0, latest_close=10.9))
    insert_rows(engine, "000003", make_rows(days=25, high=11.0, low=10.0, latest_close=10.9))
    insert_rows(engine, "000004", make_rows(days=25, high=11.0, low=10.0, latest_close=10.9))
    insert_rows(engine, "000005", make_rows(days=10, high=11.0, low=10.0, latest_close=10.9))
    with sqlite3.connect(engine.db_path) as conn:
        conn.executemany(
            """
            INSERT INTO stock_basic(
                symbol, code, name, status, stock_type, market, list_date, updated_at
            )
            VALUES (?, ?, ?, ?, '1', ?, ?, '2026-05-17T00:00:00')
            """,
            [
                ("000001", "sz.000001", "平安银行", "1", "SZ", "1991-04-03"),
                ("000002", "sh.000002", "上海样本", "1", "SH", "1991-04-03"),
                ("000003", "sz.000003", "*ST 风险", "1", "SZ", "1991-04-03"),
                ("000004", "sz.000004", "低流动性", "1", "SZ", "1991-04-03"),
                ("000005", "sz.000005", "新股样本", "1", "SZ", "2026-01-10"),
            ],
        )
        conn.executemany(
            """
            INSERT INTO stock_boards(board_code, board_name, board_type, source, fetched_at)
            VALUES (?, ?, ?, 'test', '2026-05-17T00:00:00')
            """,
            [
                ("IND001", "银行", "industry"),
                ("IND002", "地产", "industry"),
                ("CON001", "大金融", "concept"),
                ("CON002", "低价股", "concept"),
            ],
        )
        conn.executemany(
            """
            INSERT INTO stock_board_members(symbol, board_code, board_type, board_name, source, fetched_at)
            VALUES (?, ?, ?, ?, 'test', '2026-05-17T00:00:00')
            """,
            [
                ("000001", "IND001", "industry", "银行"),
                ("000002", "IND001", "industry", "银行"),
                ("000003", "IND001", "industry", "银行"),
                ("000004", "IND001", "industry", "银行"),
                ("000005", "IND001", "industry", "银行"),
                ("000001", "CON001", "concept", "大金融"),
                ("000002", "CON001", "concept", "大金融"),
                ("000003", "CON001", "concept", "大金融"),
                ("000004", "CON001", "concept", "大金融"),
                ("000005", "CON001", "concept", "大金融"),
            ],
        )
        conn.execute("UPDATE stock_daily SET turnover = 200000000 WHERE symbol <> '000004'")
        conn.execute("UPDATE stock_daily SET turnover = 50000000 WHERE symbol = '000004'")
        conn.commit()
    app = create_app(settings=settings, engine=engine, jobs=InMemoryJobManager(run_async=False))
    client = TestClient(app)

    response = client.post(
        "/api/strategies/sideways_consolidation/run",
        json={
            "filters": {
                "industry_board_codes": ["IND001"],
                "concept_board_codes": ["CON001"],
                "markets": ["SZ"],
                "min_listed_trade_days": 20,
                "min_avg_turnover_20": 10000,
                "exclude_risks": ["st"],
            },
            "parameters": {
                "lookback_days": 5,
                "max_amplitude_pct": 12,
                "min_distance_pct": 0,
                "max_distance_pct": 3,
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["filter_summary"]["eligible_symbols"] == 1
    assert [row["symbol"] for row in payload["rows"]] == ["000001"]


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

        def backfill(
            self,
            symbols,
            start_date=None,
            full_refresh=False,
            progress_callback=None,
            source="auto",
        ):
            self.calls.append((symbols, start_date, full_refresh, progress_callback is not None, source))
            if progress_callback is not None:
                progress_callback(
                    message="fake progress",
                    total=len(symbols),
                    processed=1,
                    success=1,
                    skipped=0,
                    failed=0,
                    rows_written=6,
                    current_symbol="000001",
                    current_action="写入完成",
                )
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
        json={"start_date": "1990-01-01", "full_refresh": True, "source": "tushare"},
    )

    assert response.status_code == 200
    job = client.get(f"/api/jobs/{response.json()['job_id']}").json()
    assert job["status"] == "succeeded"
    assert job["result"]["rows_written"] == 12
    assert job["progress"]["processed"] == 1
    assert job["progress"]["current_symbol"] == "000001"
    assert engine.calls == [(["000001", "600000"], "1990-01-01", True, True, "tushare")]


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
