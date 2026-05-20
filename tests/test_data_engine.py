"""数据引擎属性测试。"""

import sqlite3
import tempfile
from datetime import date
from pathlib import Path

import pandas as pd
from hypothesis import given, settings as h_settings
from hypothesis import strategies as st

from sequoia_x.core.config import Settings
from sequoia_x.data.engine import DataEngine, _normalize_stock_symbol


def make_engine_in(tmp_dir: str) -> tuple[DataEngine, Settings]:
    """创建使用临时数据库的 DataEngine 实例。"""
    settings = Settings(
        db_path=str(Path(tmp_dir) / "test.db"),
        start_date="2024-01-01",
        feishu_webhook_url="https://example.com/hook",
    )
    engine = DataEngine(settings)
    return engine, settings


# Property 4: (symbol, date) 唯一约束防止重复写入
@given(
    symbol=st.text(min_size=6, max_size=6, alphabet="0123456789"),
    trade_date=st.dates(min_value=date(2024, 1, 1), max_value=date(2025, 12, 31)),
)
@h_settings(max_examples=50, deadline=None)
def test_unique_symbol_date_constraint(symbol: str, trade_date: date) -> None:
    """相同 (symbol, date) 插入两次，数据库中该组合记录数应保持为 1。"""
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
        engine, _ = make_engine_in(tmp_dir)
        row = {
            "symbol": symbol, "date": str(trade_date),
            "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5,
            "volume": 1000.0, "turnover": 10500.0,
        }
        df = pd.DataFrame([row])
        with sqlite3.connect(engine.db_path) as conn:
            df.to_sql("stock_daily", conn, if_exists="append", index=False, method="multi")
            try:
                df.to_sql("stock_daily", conn, if_exists="append", index=False, method="multi")
            except sqlite3.IntegrityError:
                pass
            count = conn.execute(
                "SELECT COUNT(*) FROM stock_daily WHERE symbol=? AND date=?",
                (symbol, str(trade_date)),
            ).fetchone()[0]
        assert count == 1


def test_data_engine_initializes_board_tables_and_basic_columns(tmp_path) -> None:
    engine = DataEngine(
        Settings(
            db_path=str(tmp_path / "metadata.db"),
            start_date="2024-01-01",
            feishu_webhook_url="https://example.com/hook",
        )
    )

    with sqlite3.connect(engine.db_path) as conn:
        columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info(stock_basic)").fetchall()
        }
        board_table = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='stock_boards'"
        ).fetchone()
        member_table = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='stock_board_members'"
        ).fetchone()

    assert {"market", "list_date", "out_date", "industry_board_name", "concept_board_names_json"} <= columns
    assert board_table is not None
    assert member_table is not None


def test_sync_stock_metadata_preserves_cached_boards_when_upstream_empty(
    tmp_path,
    monkeypatch,
) -> None:
    engine = DataEngine(
        Settings(
            db_path=str(tmp_path / "metadata-cache.db"),
            start_date="2024-01-01",
            feishu_webhook_url="https://example.com/hook",
        )
    )
    with sqlite3.connect(engine.db_path) as conn:
        conn.execute(
            """
            INSERT INTO stock_daily(symbol, date, open, high, low, close, volume, turnover)
            VALUES ('000001', '2026-01-01', 10, 11, 9, 10.5, 1000, 10500)
            """
        )
        conn.execute(
            """
            INSERT INTO stock_basic(symbol, code, name, status, stock_type, updated_at)
            VALUES ('000001', 'SZ.000001', 'Sample Bank', '1', '1', '2026-05-18T00:00:00')
            """
        )
        conn.execute(
            """
            INSERT INTO stock_boards(board_code, board_name, board_type, source, fetched_at)
            VALUES ('IND001', 'Banking', 'industry', 'cached', '2026-05-18T00:00:00')
            """
        )
        conn.execute(
            """
            INSERT INTO stock_board_members(symbol, board_code, board_type, board_name, source, fetched_at)
            VALUES ('000001', 'IND001', 'industry', 'Banking', 'cached', '2026-05-18T00:00:00')
            """
        )
        conn.commit()

    monkeypatch.setattr(
        engine,
        "sync_stock_basic",
        lambda: [
            {
                "symbol": "000001",
                "code": "SZ.000001",
                "name": "Sample Bank",
                "status": "1",
                "stock_type": "1",
                "market": "SZ",
                "list_date": "1991-04-03",
                "out_date": "",
            }
        ],
    )
    monkeypatch.setattr(
        engine,
        "_fetch_board_metadata",
        lambda board_type, local_symbols, emit: ([], []),
    )

    result = engine.sync_stock_metadata()

    with sqlite3.connect(engine.db_path) as conn:
        board_count = conn.execute("SELECT COUNT(*) FROM stock_boards").fetchone()[0]
        member_count = conn.execute("SELECT COUNT(*) FROM stock_board_members").fetchone()[0]

    assert result["local_symbols"] == 1
    assert board_count == 1
    assert member_count == 1


def test_stock_filter_options_include_synced_sub_industry_boards(tmp_path) -> None:
    engine = DataEngine(
        Settings(
            db_path=str(tmp_path / "industry-filter.db"),
            start_date="2024-01-01",
            feishu_webhook_url="https://example.com/hook",
        )
    )
    with sqlite3.connect(engine.db_path) as conn:
        conn.execute(
            """
            INSERT INTO stock_boards(board_code, board_name, board_type, source, fetched_at)
            VALUES ('BK1036', '半导体', 'industry', 'test', '2026-05-19T00:00:00')
            """
        )
        conn.execute(
            """
            INSERT INTO stock_boards(board_code, board_name, board_type, source, fetched_at)
            VALUES ('BK0421', '银行', 'industry', 'test', '2026-05-19T00:00:00')
            """
        )
        conn.commit()

    options = engine.list_stock_filter_options()

    assert {"code": "BK1036", "name": "半导体"} in options["industries"]
    assert {"code": "BK0421", "name": "银行"} in options["industries"]


def test_sync_stock_metadata_imports_jqdata_industries_and_concepts(
    tmp_path,
    monkeypatch,
) -> None:
    class FakeJQData:
        def __init__(self) -> None:
            self.industry_name = ""

        def get_industries(self, name: str):
            self.industry_name = name
            return pd.DataFrame(
                [{"name": "金融", "start_date": "2005-01-01"}],
                index=["HY001"],
            )

        def get_industry_stocks(self, code: str, date: str | None = None):
            assert code == "HY001"
            assert date == "2026-01-02"
            return ["000001.XSHE", "600000.XSHG"]

        def get_concepts(self):
            return pd.DataFrame(
                [{"name": "大金融"}, {"name": "低价股"}],
                index=["GN001", "GN002"],
            )

        def get_concept_stocks(self, code: str, date: str | None = None):
            assert date == "2026-01-02"
            return {
                "GN001": ["000001.XSHE"],
                "GN002": ["000001.XSHE", "300750.XSHE"],
            }[code]

    fake_jq = FakeJQData()
    engine = DataEngine(
        Settings(
            db_path=str(tmp_path / "jqdata-board-mapping.db"),
            start_date="2024-01-01",
            feishu_webhook_url="https://example.com/hook",
            jqdata_username="user",
            jqdata_password="password",
            jqdata_industry="jq_l1",
        )
    )
    with sqlite3.connect(engine.db_path) as conn:
        conn.execute(
            """
            INSERT INTO stock_daily(symbol, date, open, high, low, close, volume, turnover)
            VALUES ('000001', '2026-01-02', 10, 11, 9, 10.5, 1000, 10500)
            """
        )
        conn.execute(
            """
            INSERT INTO stock_daily(symbol, date, open, high, low, close, volume, turnover)
            VALUES ('600000', '2026-01-02', 10, 11, 9, 10.5, 1000, 10500)
            """
        )
        conn.commit()

    monkeypatch.setattr(
        engine,
        "sync_stock_basic",
        lambda: [
            {
                "symbol": "000001",
                "code": "SZ.000001",
                "name": "Sample Bank",
                "status": "1",
                "stock_type": "1",
                "market": "SZ",
                "list_date": "1991-04-03",
                "out_date": "",
            }
        ],
    )
    monkeypatch.setattr(engine, "_get_jqdata_client", lambda: fake_jq)

    result = engine.sync_stock_metadata()

    with sqlite3.connect(engine.db_path) as conn:
        boards = conn.execute(
            "SELECT board_name, board_type FROM stock_boards ORDER BY board_type, board_name"
        ).fetchall()
        members = conn.execute(
            "SELECT symbol, board_name, board_type FROM stock_board_members ORDER BY board_type, board_name, symbol"
        ).fetchall()

    assert result["boards"] == 3
    assert result["members"] == 4
    assert fake_jq.industry_name == "jq_l1"
    assert boards == [("低价股", "concept"), ("大金融", "concept"), ("金融", "industry")]
    assert members == [
        ("000001", "低价股", "concept"),
        ("000001", "大金融", "concept"),
        ("000001", "金融", "industry"),
        ("600000", "金融", "industry"),
    ]


def test_sync_stock_metadata_preserves_cache_when_jqdata_credentials_missing(
    tmp_path,
    monkeypatch,
) -> None:
    engine = DataEngine(
        Settings(
            db_path=str(tmp_path / "jqdata-missing-credentials.db"),
            start_date="2024-01-01",
            feishu_webhook_url="https://example.com/hook",
        )
    )
    with sqlite3.connect(engine.db_path) as conn:
        conn.execute(
            """
            INSERT INTO stock_daily(symbol, date, open, high, low, close, volume, turnover)
            VALUES ('000001', '2026-01-01', 10, 11, 9, 10.5, 1000, 10500)
            """
        )
        conn.execute(
            """
            INSERT INTO stock_boards(board_code, board_name, board_type, source, fetched_at)
            VALUES ('GN001', '大金融', 'concept', 'cached', '2026-05-20T00:00:00')
            """
        )
        conn.execute(
            """
            INSERT INTO stock_board_members(symbol, board_code, board_type, board_name, source, fetched_at)
            VALUES ('000001', 'GN001', 'concept', '大金融', 'cached', '2026-05-20T00:00:00')
            """
        )
        conn.commit()

    monkeypatch.setattr(
        engine,
        "sync_stock_basic",
        lambda: [
            {
                "symbol": "000001",
                "code": "SZ.000001",
                "name": "Sample Bank",
                "status": "1",
                "stock_type": "1",
                "market": "SZ",
                "list_date": "1991-04-03",
                "out_date": "",
            }
        ],
    )

    result = engine.sync_stock_metadata()

    with sqlite3.connect(engine.db_path) as conn:
        board_count = conn.execute("SELECT COUNT(*) FROM stock_boards").fetchone()[0]
        member_count = conn.execute("SELECT COUNT(*) FROM stock_board_members").fetchone()[0]

    assert result["board_failures"] == 2
    assert board_count == 1
    assert member_count == 1


def test_normalize_stock_symbol_handles_common_exchange_formats() -> None:
    assert _normalize_stock_symbol("000001.SZ") == "000001"
    assert _normalize_stock_symbol("SH.600000") == "600000"
    assert _normalize_stock_symbol("300750") == "300750"
