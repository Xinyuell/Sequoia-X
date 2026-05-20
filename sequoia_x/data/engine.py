"""数据引擎模块：负责 SQLite 行情数据存储与 baostock 增量同步。"""

import json
import sqlite3
from collections.abc import Callable
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from sequoia_x.core.config import Settings
from sequoia_x.core.logger import get_logger

logger = get_logger(__name__)


_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_daily (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol   TEXT    NOT NULL,
    date     TEXT    NOT NULL,
    open     REAL,
    high     REAL,
    low      REAL,
    close    REAL,
    volume   REAL,
    turnover REAL,
    UNIQUE (symbol, date)
);
"""

_CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_symbol_date ON stock_daily (symbol, date);
"""

_CREATE_STOCK_BASIC_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_basic (
    symbol     TEXT PRIMARY KEY,
    code       TEXT,
    name       TEXT,
    status     TEXT,
    stock_type TEXT,
    market     TEXT,
    list_date  TEXT,
    out_date   TEXT,
    industry_board_code TEXT,
    industry_board_name TEXT,
    concept_board_codes_json TEXT,
    concept_board_names_json TEXT,
    board_updated_at TEXT,
    updated_at TEXT NOT NULL
);
"""

_CREATE_STOCK_BASIC_NAME_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_stock_basic_name ON stock_basic (name);
"""

_STOCK_BASIC_EXTRA_COLUMNS = {
    "market": "TEXT",
    "list_date": "TEXT",
    "out_date": "TEXT",
    "industry_board_code": "TEXT",
    "industry_board_name": "TEXT",
    "concept_board_codes_json": "TEXT",
    "concept_board_names_json": "TEXT",
    "board_updated_at": "TEXT",
}

_CREATE_STOCK_BOARDS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_boards (
    board_code TEXT NOT NULL,
    board_name TEXT NOT NULL,
    board_type TEXT NOT NULL,
    source     TEXT,
    fetched_at TEXT NOT NULL,
    PRIMARY KEY (board_type, board_code)
);
"""

_CREATE_STOCK_BOARD_MEMBERS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_board_members (
    symbol     TEXT NOT NULL,
    board_code TEXT NOT NULL,
    board_type TEXT NOT NULL,
    board_name TEXT NOT NULL,
    source     TEXT,
    fetched_at TEXT NOT NULL,
    PRIMARY KEY (symbol, board_type, board_code)
);
"""

_CREATE_STOCK_BOARDS_TYPE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_stock_boards_type_name ON stock_boards (board_type, board_name);
"""

_CREATE_STOCK_BOARD_MEMBERS_SYMBOL_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_stock_board_members_symbol ON stock_board_members (symbol);
"""

_CREATE_STOCK_BOARD_MEMBERS_BOARD_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_stock_board_members_board ON stock_board_members (board_type, board_code);
"""

def _latest_weekday_on_or_before(value: date) -> date:
    while value.weekday() >= 5:
        value -= timedelta(days=1)
    return value


def _query_bs_history_logged_in(bs, bs_code: str, start: str, end: str) -> list:
    rs = bs.query_history_k_data_plus(
        bs_code,
        "date,open,high,low,close,volume,amount",
        start_date=start,
        end_date=end,
        frequency="d",
        adjustflag="1",
    )
    if rs.error_code != "0":
        raise RuntimeError(rs.error_msg)
    rows = []
    while rs.next():
        rows.append(rs.get_row_data())
    return rows


def _query_bs_with_timeout(bs, bs_code: str, start: str, end: str, timeout: int = 30):
    """在独立线程中执行 baostock 查询，超时抛出 TimeoutError。

    这个轻量路径只用于已经运行在独立进程内的批量增量同步 worker。
    WebUI 历史回填使用 _BaostockHistorySession，超时会硬终止子进程。
    """
    import threading

    result = [None, None]

    def _query():
        try:
            result[1] = _query_bs_history_logged_in(bs, bs_code, start, end)
        except Exception as exc:
            result[0] = exc

    thread = threading.Thread(target=_query, daemon=True)
    thread.start()
    thread.join(timeout=timeout)
    if thread.is_alive():
        raise TimeoutError(f"baostock 查询超时 ({timeout}s): {bs_code} {start}~{end}")
    if result[0] is not None:
        raise result[0]
    return result[1]


def _bs_history_session_worker(task_queue, result_queue) -> None:
    import baostock as bs

    lg = bs.login()
    if lg.error_code != "0":
        result_queue.put(("__ready__", "error", f"baostock 登录失败: {lg.error_msg}"))
        return

    result_queue.put(("__ready__", "ok", None))
    try:
        while True:
            task = task_queue.get()
            if task is None:
                return

            task_id, bs_code, start, end = task
            try:
                rows = _query_bs_history_logged_in(bs, bs_code, start, end)
            except Exception as exc:
                result_queue.put((task_id, "error", str(exc)))
            else:
                result_queue.put((task_id, "ok", rows))
    finally:
        try:
            bs.logout()
        except Exception:
            pass


class _BaostockHistorySession:
    """Run baostock history queries in a killable child process."""

    def __init__(self, timeout: float = 30, worker_target: Callable[..., None] | None = None) -> None:
        from multiprocessing import get_context

        self.timeout = timeout
        self.worker_target = worker_target or _bs_history_session_worker
        self._ctx = get_context("spawn")
        self._process = None
        self._task_queue = None
        self._result_queue = None
        self._task_seq = 0

    def start(self) -> None:
        self._ensure_worker()

    def restart(self) -> None:
        self._terminate_worker()
        self._start_worker()

    def query(self, bs_code: str, start: str, end: str) -> list:
        from queue import Empty

        self._ensure_worker()
        self._task_seq += 1
        task_id = f"history-{self._task_seq}"
        self._task_queue.put((task_id, bs_code, start, end))

        try:
            while True:
                response_id, status, payload = self._result_queue.get(timeout=self.timeout)
                if response_id != task_id:
                    continue
                break
        except Empty as exc:
            self._terminate_worker()
            raise TimeoutError(f"baostock 查询超时 ({self.timeout}s): {bs_code} {start}~{end}") from exc

        if status == "ok":
            return payload
        raise RuntimeError(str(payload))

    def close(self) -> None:
        process = self._process
        if process is not None and process.is_alive() and self._task_queue is not None:
            try:
                self._task_queue.put(None)
                process.join(timeout=2)
            except Exception:
                pass
        self._terminate_worker()

    def _ensure_worker(self) -> None:
        if self._process is None or not self._process.is_alive():
            self._start_worker()

    def _start_worker(self) -> None:
        from queue import Empty

        self._task_queue = self._ctx.Queue()
        self._result_queue = self._ctx.Queue()
        self._process = self._ctx.Process(
            target=self.worker_target,
            args=(self._task_queue, self._result_queue),
            daemon=True,
        )
        self._process.start()

        try:
            response_id, status, payload = self._result_queue.get(timeout=self.timeout)
        except Empty as exc:
            self._terminate_worker()
            raise TimeoutError(f"baostock worker 启动超时 ({self.timeout}s)") from exc

        if response_id != "__ready__" or status != "ok":
            self._terminate_worker()
            raise RuntimeError(str(payload or "baostock worker 启动失败"))

    def _terminate_worker(self) -> None:
        process = self._process
        if process is not None and process.is_alive():
            process.terminate()
            process.join(timeout=2)
            if process.is_alive():
                process.kill()
                process.join(timeout=1)

        for queue in (self._task_queue, self._result_queue):
            if queue is None:
                continue
            try:
                queue.cancel_join_thread()
                queue.close()
            except Exception:
                pass

        self._process = None
        self._task_queue = None
        self._result_queue = None


def _bs_fetch_batch(tasks: list) -> list:
    """多进程 worker：独立 login，批量拉取 baostock 数据。"""
    import time

    import baostock as bs

    bs.login()
    results = []
    for idx, (symbol, bs_code, start, end) in enumerate(tasks):
        if idx > 0:
            time.sleep(0.5)
        for attempt in range(3):
            try:
                rows = _query_bs_with_timeout(bs, bs_code, start, end, timeout=45)
                for row in rows:
                    results.append([symbol] + row)
                break
            except TimeoutError:
                time.sleep(2)
                continue
            except Exception:
                time.sleep(2 ** attempt)
    bs.logout()
    return results


class DataEngine:
    """行情数据引擎，负责 SQLite 存储和 baostock 数据同步。"""

    def __init__(self, settings: Settings) -> None:
        self.db_path: str = settings.db_path
        self.start_date: str = settings.start_date
        self.tushare_token: str = settings.tushare_token
        self.jqdata_username: str = settings.jqdata_username
        self.jqdata_password: str = settings.jqdata_password
        self.jqdata_industry: str = settings.jqdata_industry
        self._ts_pro = None
        self._init_db()

    def _init_db(self) -> None:
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(_CREATE_TABLE_SQL)
            conn.execute(_CREATE_INDEX_SQL)
            conn.execute(_CREATE_STOCK_BASIC_TABLE_SQL)
            self._ensure_columns(conn, "stock_basic", _STOCK_BASIC_EXTRA_COLUMNS)
            self._backfill_stock_basic_derived_fields(conn)
            conn.execute(_CREATE_STOCK_BASIC_NAME_INDEX_SQL)
            conn.execute(_CREATE_STOCK_BOARDS_TABLE_SQL)
            conn.execute(_CREATE_STOCK_BOARD_MEMBERS_TABLE_SQL)
            conn.execute(_CREATE_STOCK_BOARDS_TYPE_INDEX_SQL)
            conn.execute(_CREATE_STOCK_BOARD_MEMBERS_SYMBOL_INDEX_SQL)
            conn.execute(_CREATE_STOCK_BOARD_MEMBERS_BOARD_INDEX_SQL)
            conn.commit()
        finally:
            conn.close()
        logger.info(f"数据库初始化完成：{self.db_path}")

    @staticmethod
    def _ensure_columns(
        conn: sqlite3.Connection,
        table: str,
        columns: dict[str, str],
    ) -> None:
        existing = {
            row[1]
            for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
        }
        for column, definition in columns.items():
            if column not in existing:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    @staticmethod
    def _backfill_stock_basic_derived_fields(conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            UPDATE stock_basic
            SET market = CASE
                WHEN LOWER(COALESCE(code, '')) LIKE 'sh.%' OR symbol LIKE '6%' OR symbol LIKE '9%' THEN 'SH'
                WHEN LOWER(COALESCE(code, '')) LIKE 'bj.%' OR symbol LIKE '4%' OR symbol LIKE '8%' THEN 'BJ'
                ELSE 'SZ'
            END
            WHERE market IS NULL OR market = ''
            """
        )

    def _get_last_date(self, symbol: str) -> str | None:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT MAX(date) FROM stock_daily WHERE symbol = ?",
                (symbol,),
            ).fetchone()
        return row[0] if row and row[0] else None

    def _get_stale_symbols(
        self, symbols: list[str], before_date: str
    ) -> tuple[list[str], dict[str, str | None]]:
        """用一条 SQL 找出需要更新的股票及其最后数据日期。

        Returns:
            (stale_symbols, last_date_map): 需要更新的股票列表，以及每只股票的最后日期（None 表示无数据）
        """
        if not symbols:
            return [], {}
        placeholders = ",".join(["?"] * len(symbols))
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                f"""
                SELECT symbol, MAX(date) AS last_date
                FROM stock_daily
                WHERE symbol IN ({placeholders})
                GROUP BY symbol
                """,
                symbols,
            ).fetchall()
        date_map: dict[str, str | None] = {row[0]: row[1] for row in rows}
        stale = [s for s in symbols if date_map.get(s, None) is None or date_map[s] < before_date]
        for s in symbols:
            if s not in date_map:
                date_map[s] = None
        return stale, date_map

    # ── Tushare 数据源 ──

    @staticmethod
    def _ts_code(symbol: str) -> str:
        """将纯数字代码转为 Tushare 格式。"""
        if symbol.startswith(("6", "9")):
            return f"{symbol}.SH"
        if symbol.startswith(("4", "8")):
            return f"{symbol}.BJ"
        return f"{symbol}.SZ"

    @staticmethod
    def _market_from_code(code: str, symbol: str | None = None) -> str:
        normalized = (code or "").upper()
        plain_symbol = symbol or normalized.split(".")[-1]
        if normalized.startswith("SH.") or normalized.endswith(".SH") or plain_symbol.startswith(("6", "9")):
            return "SH"
        if normalized.startswith("BJ.") or normalized.endswith(".BJ") or plain_symbol.startswith(("4", "8")):
            return "BJ"
        return "SZ"

    def _get_ts_pro(self):
        """懒初始化 Tushare Pro 客户端，未配置 token 返回 None。"""
        if not self.tushare_token:
            return None
        if self._ts_pro is None:
            import tushare as ts

            ts.set_token(self.tushare_token)
            self._ts_pro = ts.pro_api()
        return self._ts_pro

    def _ts_fetch_stock_list(self) -> list[dict[str, str]] | None:
        """通过 Tushare 获取全市场 A 股列表。失败返回 None。"""
        pro = self._get_ts_pro()
        if pro is None:
            return None
        try:
            df = pro.stock_basic(
                exchange="",
                list_status="L",
                fields="ts_code,symbol,name,list_date",
            )
            if df is None or df.empty:
                return None
            records: list[dict[str, str]] = []
            for _, row in df.iterrows():
                records.append(
                    {
                        "symbol": str(row["symbol"]),
                        "code": str(row["ts_code"]),
                        "name": str(row["name"]),
                        "status": "1",
                        "stock_type": "1",
                        "market": self._market_from_code(str(row["ts_code"]), str(row["symbol"])),
                        "list_date": _format_compact_date(row.get("list_date")),
                        "out_date": "",
                    }
                )
            logger.info(f"Tushare 获取股票列表完成，共 {len(records)} 只")
            return records
        except Exception as exc:
            logger.warning(f"Tushare 获取股票列表失败: {exc}")
            return None

    def _ts_fetch_history(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame | None:
        """通过 Tushare 获取单只股票历史日 K 线（后复权）。失败返回 None。"""
        pro = self._get_ts_pro()
        if pro is None:
            return None
        try:
            ts_code = self._ts_code(symbol)
            df = pro.daily(
                ts_code=ts_code,
                start_date=start_date.replace("-", ""),
                end_date=end_date.replace("-", ""),
                fields="trade_date,open,high,low,close,vol,amount",
            )
            if df is None or df.empty:
                return None
            df = df.rename(
                columns={
                    "trade_date": "date",
                    "vol": "volume",
                    "amount": "turnover",
                }
            )
            df["symbol"] = symbol
            df["date"] = pd.to_datetime(df["date"], format="%Y%m%d").dt.strftime("%Y-%m-%d")
            for col in ["open", "high", "low", "close", "volume", "turnover"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df = df.dropna(subset=["close"])
            df = df[df["volume"] > 0]
            return df[["symbol", "date", "open", "high", "low", "close", "volume", "turnover"]]
        except Exception as exc:
            logger.warning(f"[{symbol}] Tushare 获取历史数据失败: {exc}")
            return None

    def _ts_fetch_daily_all(self, trade_date: str) -> pd.DataFrame | None:
        """通过 Tushare 获取全市场某一天的日 K 线数据。失败返回 None。"""
        pro = self._get_ts_pro()
        if pro is None:
            return None
        try:
            df = pro.daily(trade_date=trade_date.replace("-", ""))
            if df is None or df.empty:
                return None
            df = df.rename(
                columns={
                    "ts_code": "code",
                    "trade_date": "date",
                    "vol": "volume",
                    "amount": "turnover",
                }
            )
            df["symbol"] = df["code"].str.split(".").str[1]
            df["date"] = pd.to_datetime(df["date"], format="%Y%m%d").dt.strftime("%Y-%m-%d")
            for col in ["open", "high", "low", "close", "volume", "turnover"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df = df.dropna(subset=["close"])
            df = df[df["volume"] > 0]
            return df[["symbol", "date", "open", "high", "low", "close", "volume", "turnover"]]
        except Exception as exc:
            logger.warning(f"Tushare 获取每日全量数据失败: {exc}")
            return None

    def get_ohlcv(self, symbol: str, end_date: str | None = None) -> pd.DataFrame:
        with sqlite3.connect(self.db_path) as conn:
            if end_date:
                df = pd.read_sql(
                    "SELECT * FROM stock_daily WHERE symbol = ? AND date <= ? ORDER BY date",
                    conn,
                    params=(symbol, end_date),
                )
            else:
                df = pd.read_sql(
                    "SELECT * FROM stock_daily WHERE symbol = ? ORDER BY date",
                    conn,
                    params=(symbol,),
                )
        return df

    @staticmethod
    def _to_baostock_code(symbol: str) -> str:
        """将纯数字代码转为 baostock 格式；优先使用 stock_basic 中的原始交易所代码。"""
        if symbol.startswith(("6", "9")):
            prefix = "sh"
        elif symbol.startswith(("4", "8")):
            prefix = "bj"
        else:
            prefix = "sz"
        return f"{prefix}.{symbol}"

    def _get_baostock_code_map(self) -> dict[str, str]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT symbol, code FROM stock_basic WHERE code IS NOT NULL AND code <> ''"
            ).fetchall()
        return {symbol: code for symbol, code in rows}

    # ── 数据同步 ──

    def sync_today_bulk(self, today_str: str | None = None) -> int:
        """增量同步当日数据：Tushare 优先（1 次调用），Baostock 多进程容灾。"""
        today = date.fromisoformat(today_str) if today_str else date.today()
        today_str = today.isoformat()
        latest_local_date = self._get_latest_local_date()

        if latest_local_date and latest_local_date >= today_str:
            logger.info(f"本地数据已包含 {today_str}，无需每日增量更新")
            return 0

        latest_expected_trade_date = _latest_weekday_on_or_before(today).isoformat()
        if today.weekday() >= 5 and latest_local_date and latest_local_date >= latest_expected_trade_date:
            logger.info(
                f"{today_str} 是周末，且本地已更新到 {latest_local_date}，无需每日增量更新"
            )
            return 0

        # 先尝试 Tushare（1 次 API 调用获取全市场当日数据）
        df = self._ts_fetch_daily_all(today_str)
        if df is not None and not df.empty:
            count = len(df)
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("DELETE FROM stock_daily WHERE date = ?", (today_str,))
            self._write_ohlcv_df(df)
            logger.info(f"sync_today_bulk (Tushare): 写入 {count} 条数据")
            return count

        logger.info("Tushare 不可用，回退到 Baostock 多进程增量同步")
        return self._bs_sync_today_bulk(today_str)

    def _get_latest_local_date(self) -> str | None:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute("SELECT MAX(date) FROM stock_daily").fetchone()
        return row[0] if row and row[0] else None

    def _bs_sync_today_bulk(self, today_str: str) -> int:
        """Baostock 多进程增量同步。"""
        from datetime import date, timedelta
        from multiprocessing import Pool

        code_by_symbol = self._get_baostock_code_map()

        tasks = []
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT symbol, MAX(date) FROM stock_daily GROUP BY symbol"
            ).fetchall()

        if not rows:
            logger.warning("本地无股票数据，请先执行 --backfill")
            return 0

        for symbol, last_date in rows:
            if last_date and last_date >= today_str:
                continue
            start = today_str
            if last_date:
                start = (date.fromisoformat(last_date) + timedelta(days=1)).strftime("%Y-%m-%d")
            tasks.append(
                (
                    symbol,
                    code_by_symbol.get(symbol) or self._to_baostock_code(symbol),
                    start,
                    today_str,
                )
            )

        if not tasks:
            logger.info("所有股票已是最新，无需更新")
            return 0

        logger.info(f"需要更新 {len(tasks)} 只股票，启动多进程并行拉取...")

        n_workers = min(8, len(tasks))
        chunks = [tasks[i::n_workers] for i in range(n_workers)]

        with Pool(n_workers) as pool:
            batch_results = pool.map(_bs_fetch_batch, chunks)

        all_rows = []
        for batch in batch_results:
            all_rows.extend(batch)

        if not all_rows:
            logger.info("无新数据（可能非交易日）")
            return 0

        df = pd.DataFrame(
            all_rows,
            columns=["symbol", "date", "open", "high", "low", "close", "volume", "turnover"],
        )
        for col in ["open", "high", "low", "close", "volume", "turnover"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["close"])
        df = df[df["volume"] > 0]

        count = len(df)
        with sqlite3.connect(self.db_path) as conn:
            for d in df["date"].unique().tolist():
                conn.execute("DELETE FROM stock_daily WHERE date = ?", (d,))
            df.to_sql("stock_daily", conn, if_exists="append", index=False, method="multi", chunksize=500)
            conn.commit()

        logger.info(f"sync_today_bulk (Baostock): 写入 {count} 条数据")
        return count

    def backfill(
        self,
        symbols: list[str],
        start_date: str | None = None,
        full_refresh: bool = False,
        progress_callback: Callable[..., None] | None = None,
        source: str = "auto",
    ) -> dict[str, int | str | bool | list[str]]:
        """批量回填历史日 K 线数据（后复权），支持双数据源。

        source:
          - "auto": Baostock 优先，Tushare 容灾（默认）
          - "tushare": 强制 Tushare
          - "baostock": 仅 Baostock，不启用 Tushare 容灾
        """
        import time
        from datetime import date, timedelta

        today_str = date.today().strftime("%Y-%m-%d")
        yesterday_str = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
        effective_start_date = start_date or self.start_date
        date.fromisoformat(effective_start_date)
        requested_total = len(symbols)

        def _emit(message: str | None = None, **progress: Any) -> None:
            if progress_callback is None:
                return
            progress_callback(
                message=message,
                total=requested_total,
                start_date=effective_start_date,
                end_date=today_str,
                full_refresh=full_refresh,
                **progress,
            )

        # 强制 Tushare
        if source == "tushare":
            _emit("Tushare 全量回填", processed=0, success=0, skipped=0, failed=0, rows_written=0)
            result = self._ts_backfill_all(
                symbols,
                effective_start_date,
                today_str,
                full_refresh,
                requested_total,
                _emit,
            )
            return result or {
                "symbol_count": requested_total,
                "success": 0,
                "skipped": 0,
                "failed": requested_total,
                "rows_written": 0,
                "start_date": effective_start_date,
                "end_date": today_str,
                "full_refresh": full_refresh,
                "failed_symbols": [],
            }

        max_retries = 2
        reconnect_interval = 100
        query_timeout = 15
        code_by_symbol = self._get_baostock_code_map()

        _emit(
            "正在连接 baostock",
            processed=0,
            success=0,
            skipped=0,
            failed=0,
            rows_written=0,
        )

        history_session = _BaostockHistorySession(timeout=query_timeout)
        try:
            history_session.start()
        except Exception as exc:
            # Baostock 登录失败，尝试用 Tushare 全量接管
            logger.warning(f"Baostock 连接失败，尝试 Tushare 接管回填: {exc}")
            result = self._ts_backfill_all(
                symbols,
                effective_start_date,
                today_str,
                full_refresh,
                requested_total,
                _emit,
            )
            if result is not None:
                return result
            _emit(
                "baostock 登录失败，Tushare 亦不可用，更新终止",
                processed=0,
                success=0,
                skipped=0,
                failed=requested_total,
                rows_written=0,
            )
            return {
                "symbol_count": requested_total,
                "success": 0,
                "skipped": 0,
                "failed": requested_total,
                "rows_written": 0,
                "start_date": effective_start_date,
                "end_date": today_str,
                "full_refresh": full_refresh,
                "failed_symbols": symbols,
            }

        try:
            # 预过滤：一条 SQL 找出真正需要更新的股票，避免逐只检查 5000+ 只
            if full_refresh:
                working_symbols = list(symbols)
                last_date_map: dict[str, str | None] = {}
                skipped_before = 0
            else:
                working_symbols, last_date_map = self._get_stale_symbols(symbols, yesterday_str)
                skipped_before = requested_total - len(working_symbols)
                if skipped_before > 0:
                    logger.info(
                        f"预过滤：{skipped_before} 只已是最新，{len(working_symbols)} 只需要更新"
                    )
                    _emit(
                        f"预过滤完成，{len(working_symbols)} 只需要更新",
                        processed=skipped_before,
                        success=0,
                        skipped=skipped_before,
                        failed=0,
                        rows_written=0,
                    )

            working_total = len(working_symbols)
            success = 0
            skipped = 0
            failed = 0
            rows_written = 0
            since_reconnect = 0
            failed_symbols: list[str] = []

            for i, symbol in enumerate(working_symbols):
                processed = skipped_before + i
                last_date = last_date_map.get(symbol) if not full_refresh else None
                if full_refresh or not last_date:
                    start = effective_start_date
                else:
                    start = (date.fromisoformat(last_date) + timedelta(days=1)).strftime("%Y-%m-%d")
                    if start < effective_start_date:
                        start = effective_start_date

                if start > today_str:
                    skipped += 1
                    _emit(
                        f"已跳过 {symbol}，无需更新",
                        processed=processed + 1,
                        success=success,
                        skipped=skipped,
                        failed=failed,
                        rows_written=rows_written,
                        current_symbol=symbol,
                        current_start_date=start,
                        current_action="无需更新，跳过",
                    )
                    time.sleep(0.3)
                    continue

                since_reconnect += 1
                if since_reconnect >= reconnect_interval:
                    time.sleep(1)
                    try:
                        history_session.restart()
                    except Exception as exc:
                        logger.error(f"重连失败，终止回填: {exc}")
                        remaining_failed = [working_symbols[j] for j in range(i, working_total)]
                        failed_symbols.extend(remaining_failed)
                        self._save_failed_symbols(failed_symbols)
                        _emit(
                            "baostock 重连失败，更新终止",
                            processed=processed,
                            success=success,
                            skipped=skipped,
                            failed=failed + len(remaining_failed),
                            rows_written=rows_written,
                            current_symbol=symbol,
                            current_start_date=start,
                            current_action="重连失败",
                        )
                        return {
                            "symbol_count": requested_total,
                            "success": success,
                            "skipped": skipped,
                            "failed": failed + len(remaining_failed),
                            "rows_written": rows_written,
                            "start_date": effective_start_date,
                            "end_date": today_str,
                            "full_refresh": full_refresh,
                            "failed_symbols": failed_symbols,
                        }
                    since_reconnect = 0

                bs_code = code_by_symbol.get(symbol) or self._to_baostock_code(symbol)
                _emit(
                    f"正在更新 {symbol}（{processed + 1}/{requested_total}）",
                    processed=processed,
                    success=success,
                    skipped=skipped,
                    failed=failed,
                    rows_written=rows_written,
                    current_symbol=symbol,
                    current_start_date=start,
                    current_action="请求历史 K 线",
                )

                # 带重试和超时的查询
                rows = []
                query_ok = False
                for attempt in range(max_retries):
                    try:
                        rows = history_session.query(bs_code, start, today_str)
                        query_ok = True
                        break
                    except TimeoutError:
                        if attempt < max_retries - 1:
                            wait = 2 ** (attempt + 1)
                            logger.warning(
                                f"[{symbol}] 第{attempt + 1}次超时，{wait}s 后重试"
                            )
                            time.sleep(wait)
                            try:
                                history_session.restart()
                            except Exception as exc:
                                logger.warning(f"[{symbol}] baostock 重启失败: {exc}")
                        else:
                            logger.warning(f"[{symbol}] {max_retries}次均超时，跳过")
                    except Exception as exc:
                        if attempt < max_retries - 1:
                            wait = 2 ** (attempt + 1)
                            logger.warning(
                                f"[{symbol}] 第{attempt + 1}次失败: {exc}，{wait}s 后重试"
                            )
                            time.sleep(wait)
                            try:
                                history_session.restart()
                            except Exception as restart_exc:
                                logger.warning(f"[{symbol}] baostock 重启失败: {restart_exc}")
                        else:
                            logger.warning(f"[{symbol}] {max_retries}次重试均失败，跳过")

                if not query_ok:
                    # Baostock 全部重试失败，尝试 Tushare 容灾（source=baostock 时跳过）
                    if source != "baostock":
                        logger.info(f"[{symbol}] Baostock 失败，尝试 Tushare 容灾")
                        df_ts = self._ts_fetch_history(symbol, start, today_str)
                    else:
                        df_ts = None
                    if df_ts is not None and not df_ts.empty:
                        logger.info(f"[{symbol}] Baostock 失败，Tushare 容灾成功")
                        self._write_ohlcv_df(df_ts)
                        success += 1
                        rows_written += len(df_ts)
                        _emit(
                            f"已写入 {symbol}：{len(df_ts)} 行 (Tushare)",
                            processed=processed + 1,
                            success=success,
                            skipped=skipped,
                            failed=failed,
                            rows_written=rows_written,
                            current_symbol=symbol,
                            current_start_date=start,
                            current_action="Tushare 容灾写入完成",
                            current_rows=len(df_ts),
                        )
                        if (i + 1) % 500 == 0:
                            logger.info(
                                f"已处理 {i + 1}/{working_total}，"
                                f"成功 {success} 跳过 {skipped} 失败 {failed}"
                            )
                        time.sleep(0.3)
                        continue
                    failed += 1
                    failed_symbols.append(symbol)
                    _emit(
                        f"{symbol} 更新失败，继续下一只",
                        processed=processed + 1,
                        success=success,
                        skipped=skipped,
                        failed=failed,
                        rows_written=rows_written,
                        current_symbol=symbol,
                        current_start_date=start,
                        current_action="查询失败",
                    )
                    time.sleep(1.0)
                    continue

                if not rows:
                    skipped += 1
                    _emit(
                        f"{symbol} 无新增数据，已跳过",
                        processed=processed + 1,
                        success=success,
                        skipped=skipped,
                        failed=failed,
                        rows_written=rows_written,
                        current_symbol=symbol,
                        current_start_date=start,
                        current_action="无数据，跳过",
                    )
                    time.sleep(0.3)
                    continue

                df = pd.DataFrame(rows, columns=["date", "open", "high", "low", "close", "volume", "amount"])
                for col in ["open", "high", "low", "close", "volume", "amount"]:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                df = df.dropna(subset=["close"])
                df = df[df["volume"] > 0]

                if df.empty:
                    skipped += 1
                    _emit(
                        f"{symbol} 无有效交易数据，已跳过",
                        processed=processed + 1,
                        success=success,
                        skipped=skipped,
                        failed=failed,
                        rows_written=rows_written,
                        current_symbol=symbol,
                        current_start_date=start,
                        current_action="无有效数据，跳过",
                    )
                    time.sleep(0.3)
                    continue

                df["symbol"] = symbol
                df = df.rename(columns={"amount": "turnover"})
                df = df[["symbol", "date", "open", "high", "low", "close", "volume", "turnover"]]

                with sqlite3.connect(self.db_path) as conn:
                    conn.executemany(
                        """
                        INSERT OR REPLACE INTO stock_daily
                            (symbol, date, open, high, low, close, volume, turnover)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        df.itertuples(index=False, name=None),
                    )
                    conn.commit()

                success += 1
                rows_written += len(df)
                _emit(
                    f"已写入 {symbol}：{len(df)} 行",
                    processed=processed + 1,
                    success=success,
                    skipped=skipped,
                    failed=failed,
                    rows_written=rows_written,
                    current_symbol=symbol,
                    current_start_date=start,
                    current_action="写入完成",
                    current_rows=len(df),
                )

                if (i + 1) % 500 == 0:
                    logger.info(
                        f"已处理 {i + 1}/{working_total}，"
                        f"成功 {success} 跳过 {skipped} 失败 {failed}"
                    )

                time.sleep(0.3)

        finally:
            history_session.close()

        total_skipped = skipped_before + skipped
        total_processed = requested_total
        if failed_symbols:
            self._save_failed_symbols(failed_symbols)
        logger.info(
            f"回填完成 — 成功: {success} | 跳过: {total_skipped} | 失败: {failed}"
        )
        _emit(
            "历史 K 线更新完成",
            processed=total_processed,
            success=success,
            skipped=total_skipped,
            failed=failed,
            rows_written=rows_written,
            current_action="完成",
        )
        return {
            "symbol_count": total_processed,
            "success": success,
            "skipped": total_skipped,
            "failed": failed,
            "rows_written": rows_written,
            "start_date": effective_start_date,
            "end_date": today_str,
            "full_refresh": full_refresh,
            "failed_symbols": failed_symbols,
        }

    # ── 股票列表 ──

    def get_all_symbols(self) -> list[str]:
        """通过 baostock 获取全市场 A 股代码列表，并同步本地股票名称。"""
        records = self.sync_stock_basic()
        return [record["symbol"] for record in records]

    def sync_stock_basic(self) -> list[dict[str, str]]:
        """同步全市场 A 股基础信息，Tushare 优先，Baostock 容灾。"""
        records = self._ts_fetch_stock_list()
        if records:
            self._write_stock_basic(records)
            return records

        logger.info("Tushare 不可用，回退到 Baostock 获取股票列表")
        records = self._bs_fetch_stock_list()
        if records:
            self._write_stock_basic(records)
        return records

    def _ts_backfill_all(
        self,
        symbols: list[str],
        start_date: str,
        end_date: str,
        full_refresh: bool,
        total: int,
        emit,
    ) -> dict | None:
        """Tushare 全量接管回填（当 Baostock 完全不可用时）。"""
        import time

        pro = self._get_ts_pro()
        if pro is None:
            return None

        # 预过滤
        if full_refresh:
            working_symbols = list(symbols)
            last_date_map: dict[str, str | None] = {}
            skipped_before = 0
        else:
            from datetime import date as _date, timedelta as _timedelta

            yesterday_str = (_date.today() - _timedelta(days=1)).strftime("%Y-%m-%d")
            working_symbols, last_date_map = self._get_stale_symbols(symbols, yesterday_str)
            skipped_before = total - len(working_symbols)
            if skipped_before > 0:
                logger.info(
                    f"Tushare 预过滤：{skipped_before} 只已是最新，{len(working_symbols)} 只需要更新"
                )

        total = len(working_symbols)
        success = 0
        skipped = 0
        failed = 0
        rows_written = 0
        ts_calls = 0
        max_ts_calls = 450  # 留余量

        for i, symbol in enumerate(working_symbols):
            if ts_calls >= max_ts_calls:
                logger.warning("Tushare 当日调用次数已达上限，停止回填")
                break

            processed = skipped_before + i
            last_date = last_date_map.get(symbol) if not full_refresh else None
            if full_refresh or not last_date:
                start = start_date
            else:
                start = last_date
                if start < start_date:
                    start = start_date

            if start > end_date:
                skipped += 1
                continue

            emit(
                f"Tushare 正在更新 {symbol}（{processed + 1}/{total}）",
                processed=processed,
                success=success,
                skipped=skipped,
                failed=failed,
                rows_written=rows_written,
                current_symbol=symbol,
                current_start_date=start,
                current_action="Tushare 请求历史 K 线",
            )

            df = self._ts_fetch_history(symbol, start, end_date)
            ts_calls += 1

            if df is not None and not df.empty:
                self._write_ohlcv_df(df)
                success += 1
                rows_written += len(df)
                emit(
                    f"Tushare 已写入 {symbol}：{len(df)} 行",
                    processed=processed + 1,
                    success=success,
                    skipped=skipped,
                    failed=failed,
                    rows_written=rows_written,
                    current_symbol=symbol,
                    current_action="Tushare 写入完成",
                    current_rows=len(df),
                )
            else:
                failed += 1

            time.sleep(0.3)

        total_skipped = skipped_before + skipped
        logger.info(
            f"Tushare 回填完成 — 成功: {success} | 跳过: {total_skipped} | 失败: {failed}"
        )
        emit(
            "Tushare 历史 K 线更新完成",
            processed=total + skipped_before,
            success=success,
            skipped=total_skipped,
            failed=failed,
            rows_written=rows_written,
            current_action="完成",
        )
        return {
            "symbol_count": total + skipped_before,
            "success": success,
            "skipped": total_skipped,
            "failed": failed,
            "rows_written": rows_written,
            "start_date": start_date,
            "end_date": end_date,
            "full_refresh": full_refresh,
        }

    def _write_ohlcv_df(self, df: "pd.DataFrame") -> None:
        """将 OHLCV DataFrame 写入 SQLite。"""
        with sqlite3.connect(self.db_path) as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO stock_daily
                    (symbol, date, open, high, low, close, volume, turnover)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                df.itertuples(index=False, name=None),
            )
            conn.commit()

    def _write_stock_basic(self, records: list[dict[str, str]]) -> None:
        updated_at = datetime.now().isoformat(timespec="seconds")
        with sqlite3.connect(self.db_path) as conn:
            conn.executemany(
                """
                INSERT INTO stock_basic
                    (
                        symbol, code, name, status, stock_type,
                        market, list_date, out_date, updated_at
                    )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol) DO UPDATE SET
                    code = excluded.code,
                    name = excluded.name,
                    status = excluded.status,
                    stock_type = excluded.stock_type,
                    market = COALESCE(excluded.market, stock_basic.market),
                    list_date = COALESCE(excluded.list_date, stock_basic.list_date),
                    out_date = COALESCE(excluded.out_date, stock_basic.out_date),
                    updated_at = excluded.updated_at
                """,
                [
                    (
                        record["symbol"],
                        record["code"],
                        record["name"],
                        record.get("status", "1"),
                        record.get("stock_type", "1"),
                        record.get("market") or self._market_from_code(
                            record.get("code", ""),
                            record.get("symbol"),
                        ),
                        record.get("list_date") or None,
                        record.get("out_date") or None,
                        updated_at,
                    )
                    for record in records
                ],
            )
            conn.commit()

    def _failed_stocks_path(self) -> Path:
        return Path(self.db_path).parent / "failed_stocks.json"

    def _save_failed_symbols(self, symbols: list[str]) -> None:
        import json

        path = self._failed_stocks_path()
        existing: list[str] = []
        if path.exists():
            try:
                existing = json.loads(path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                existing = []
        merged = list(dict.fromkeys(existing + symbols))
        path.write_text(json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info(f"已保存 {len(symbols)} 只失败股票到 {path}（累计 {len(merged)} 只）")

    def get_failed_symbols(self) -> list[str]:
        import json

        path = self._failed_stocks_path()
        if not path.exists():
            return []
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return []

    def clear_failed_symbols(self) -> None:
        path = self._failed_stocks_path()
        if path.exists():
            path.unlink()
            logger.info("已清除失败股票记录")

    # ── 股票画像与板块 ──

    def sync_stock_metadata(self, progress_callback: Callable[..., None] | None = None) -> dict[str, Any]:
        """同步股票基础画像和行业/概念板块，只保留本地已有行情股票。"""
        local_symbols = set(self.get_local_symbols())
        if not local_symbols:
            return {"local_symbols": 0, "boards": 0, "members": 0}

        def emit(message: str, **kwargs: Any) -> None:
            if progress_callback is not None:
                progress_callback(message=message, **kwargs)

        emit("正在同步股票基础资料", total=0, processed=0)
        basic_records = self.sync_stock_basic()
        emit("正在同步行业和概念板块", total=2, processed=0)

        board_count = 0
        member_count = 0
        board_failures = 0
        for board_type in ("industry", "concept"):
            board_label = "行业" if board_type == "industry" else "概念"
            try:
                boards, members = self._fetch_board_metadata(board_type, local_symbols, emit)
            except Exception as exc:
                board_failures += 1
                logger.warning(f"{board_label}板块同步失败，保留本地缓存: {exc}")
                emit(
                    f"{board_label}板块同步失败，已保留本地缓存",
                    total=2,
                    processed=1 if board_type == "industry" else 2,
                    boards=board_count,
                    members=member_count,
                    board_failures=board_failures,
                    current_action="保留板块缓存",
                )
                continue
            if not boards or not members:
                board_failures += 1
                logger.warning(f"{board_label}板块同步返回空结果，保留本地缓存")
                emit(
                    f"{board_label}板块同步返回空结果，已保留本地缓存",
                    total=2,
                    processed=1 if board_type == "industry" else 2,
                    boards=board_count,
                    members=member_count,
                    board_failures=board_failures,
                    current_action="保留板块缓存",
                )
                continue
            self._write_stock_boards(boards, members, board_type)
            board_count += len(boards)
            member_count += len(members)
            emit(
                f"{board_label}板块同步完成",
                total=2,
                processed=1 if board_type == "industry" else 2,
                boards=board_count,
                members=member_count,
                board_failures=board_failures,
            )

        self._refresh_stock_basic_board_cache()
        return {
            "local_symbols": len(local_symbols),
            "basic_records": len(basic_records),
            "boards": board_count,
            "members": member_count,
            "board_failures": board_failures,
        }

    def _fetch_board_metadata(
        self,
        board_type: str,
        local_symbols: set[str],
        emit: Callable[..., None],
    ) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
        jq = self._get_jqdata_client()
        if jq is None:
            return [], []
        reference_date = self._latest_local_date()
        if board_type == "industry":
            return self._fetch_jqdata_industries(jq, local_symbols, emit, reference_date)
        return self._fetch_jqdata_concepts(jq, local_symbols, emit, reference_date)

    def _get_jqdata_client(self) -> Any | None:
        if not self.jqdata_username or not self.jqdata_password:
            logger.warning("未配置 JQDATA_USERNAME/JQDATA_PASSWORD，无法同步行业和概念板块")
            return None
        try:
            import jqdatasdk as jq
        except ImportError as exc:
            logger.warning(f"未安装 jqdatasdk，无法同步 JoinQuant 板块数据: {exc}")
            return None
        try:
            jq.auth(self.jqdata_username, self.jqdata_password)
        except Exception as exc:
            logger.warning(f"JoinQuant 登录失败: {exc}")
            return None
        return jq

    def _latest_local_date(self) -> str | None:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute("SELECT MAX(date) FROM stock_daily").fetchone()
        return str(row[0]) if row and row[0] else None

    def _fetch_jqdata_industries(
        self,
        jq: Any,
        local_symbols: set[str],
        emit: Callable[..., None],
        reference_date: str | None,
    ) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
        try:
            board_df = jq.get_industries(name=self.jqdata_industry)
        except Exception as exc:
            logger.warning(f"JoinQuant 一级行业列表同步失败: {exc}")
            return [], []

        boards: list[dict[str, str]] = []
        members: list[dict[str, str]] = []
        board_items = list(
            _jq_board_items(
                board_df,
                code_columns=["industry_code", "code", "index_code"],
                name_columns=["industry_name", "name", "板块名称"],
            )
        )
        total = len(board_items)
        for index, (raw_board_code, board_name) in enumerate(board_items, start=1):
            board_code = f"JQ:{raw_board_code}"
            emit(
                f"正在同步{board_name}成分股",
                total=total,
                processed=index - 1,
                current_action="同步 JoinQuant 一级行业成分",
            )
            try:
                stock_codes = jq.get_industry_stocks(raw_board_code, date=reference_date)
            except Exception as exc:
                logger.warning(f"[{board_name}] JoinQuant 行业成分同步失败: {exc}")
                continue
            board_members = _board_members_from_codes(
                stock_codes,
                board_code,
                board_name,
                local_symbols,
            )
            if board_members:
                boards.append({"board_code": board_code, "board_name": board_name})
                members.extend(board_members)
        return boards, members

    def _fetch_jqdata_concepts(
        self,
        jq: Any,
        local_symbols: set[str],
        emit: Callable[..., None],
        reference_date: str | None,
    ) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
        try:
            board_df = jq.get_concepts()
        except Exception as exc:
            logger.warning(f"JoinQuant 概念板块列表同步失败: {exc}")
            return [], []

        boards: list[dict[str, str]] = []
        members: list[dict[str, str]] = []
        board_items = list(
            _jq_board_items(
                board_df,
                code_columns=["concept_code", "code", "id"],
                name_columns=["concept_name", "name", "板块名称"],
            )
        )
        total = len(board_items)
        for index, (raw_board_code, board_name) in enumerate(board_items, start=1):
            board_code = f"JQ:{raw_board_code}"
            emit(
                f"正在同步{board_name}成分股",
                total=total,
                processed=index - 1,
                current_action="同步 JoinQuant 概念成分",
            )
            try:
                stock_codes = jq.get_concept_stocks(raw_board_code, date=reference_date)
            except Exception as exc:
                logger.warning(f"[{board_name}] JoinQuant 概念成分同步失败: {exc}")
                continue
            board_members = _board_members_from_codes(
                stock_codes,
                board_code,
                board_name,
                local_symbols,
            )
            if board_members:
                boards.append({"board_code": board_code, "board_name": board_name})
                members.extend(board_members)
        return boards, members

    def _write_stock_boards(
        self,
        boards: list[dict[str, str]],
        members: list[dict[str, str]],
        board_type: str,
    ) -> None:
        fetched_at = datetime.now().isoformat(timespec="seconds")
        with sqlite3.connect(self.db_path) as conn:
            conn.executemany(
                """
                INSERT INTO stock_boards(board_code, board_name, board_type, source, fetched_at)
                VALUES (?, ?, ?, 'jqdata', ?)
                ON CONFLICT(board_type, board_code) DO UPDATE SET
                    board_name = excluded.board_name,
                    source = excluded.source,
                    fetched_at = excluded.fetched_at
                """,
                [
                    (board["board_code"], board["board_name"], board_type, fetched_at)
                    for board in boards
                ],
            )
            conn.execute("DELETE FROM stock_board_members WHERE board_type = ?", (board_type,))
            conn.executemany(
                """
                INSERT OR REPLACE INTO stock_board_members
                    (symbol, board_code, board_type, board_name, source, fetched_at)
                VALUES (?, ?, ?, ?, 'jqdata', ?)
                """,
                [
                    (
                        member["symbol"],
                        member["board_code"],
                        board_type,
                        member["board_name"],
                        fetched_at,
                    )
                    for member in members
                ],
            )
            conn.commit()

    def _refresh_stock_basic_board_cache(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            industry_rows = conn.execute(
                """
                SELECT symbol, board_code, board_name
                FROM stock_board_members
                WHERE board_type = 'industry'
                ORDER BY symbol, board_name
                """
            ).fetchall()
            concept_rows = conn.execute(
                """
                SELECT symbol, board_code, board_name
                FROM stock_board_members
                WHERE board_type = 'concept'
                ORDER BY symbol, board_name
                """
            ).fetchall()
            industry_map = {
                row["symbol"]: (row["board_code"], row["board_name"])
                for row in industry_rows
            }
            concept_map: dict[str, dict[str, list[str]]] = {}
            for row in concept_rows:
                item = concept_map.setdefault(row["symbol"], {"codes": [], "names": []})
                item["codes"].append(row["board_code"])
                item["names"].append(row["board_name"])

            updated_at = datetime.now().isoformat(timespec="seconds")
            for symbol, (board_code, board_name) in industry_map.items():
                conn.execute(
                    """
                    UPDATE stock_basic
                    SET industry_board_code = ?,
                        industry_board_name = ?,
                        board_updated_at = ?
                    WHERE symbol = ?
                    """,
                    (board_code, board_name, updated_at, symbol),
                )
            for symbol, concepts in concept_map.items():
                conn.execute(
                    """
                    UPDATE stock_basic
                    SET concept_board_codes_json = ?,
                        concept_board_names_json = ?,
                        board_updated_at = ?
                    WHERE symbol = ?
                    """,
                    (
                        json.dumps(concepts["codes"], ensure_ascii=False),
                        json.dumps(concepts["names"], ensure_ascii=False),
                        updated_at,
                        symbol,
                    ),
                )
            conn.commit()

    def list_stock_filter_options(self) -> dict[str, list[dict[str, str]]]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            boards = conn.execute(
                """
                SELECT board_type, board_code, board_name
                FROM stock_boards
                WHERE board_type IN ('industry', 'concept')
                ORDER BY board_type, board_name
                """
            ).fetchall()
            markets = conn.execute(
                """
                SELECT DISTINCT market
                FROM stock_basic
                WHERE market IS NOT NULL AND market <> ''
                ORDER BY market
                """
            ).fetchall()

        industries = [
            {"code": row["board_code"], "name": row["board_name"]}
            for row in boards
            if row["board_type"] == "industry"
        ]
        concepts = [
            {"code": row["board_code"], "name": row["board_name"]}
            for row in boards
            if row["board_type"] == "concept"
        ]
        return {
            "industries": industries,
            "concepts": concepts,
            "markets": [
                {"value": row["market"], "label": _market_label(row["market"])}
                for row in markets
            ],
        }

    def get_filtered_symbols(
        self,
        filters: dict[str, Any] | None = None,
        reference_date: str | None = None,
    ) -> list[str]:
        filters = filters or {}
        ordered_symbols = self.get_local_symbols()
        eligible = set(ordered_symbols)
        if not eligible:
            return []

        industry_codes = _as_string_list(filters.get("industry_board_codes"))
        concept_codes = _as_string_list(filters.get("concept_board_codes"))
        markets = _as_string_list(filters.get("markets"))
        min_listed_trade_days = int(filters.get("min_listed_trade_days") or 0)
        min_avg_turnover_20 = float(filters.get("min_avg_turnover_20") or 0)
        exclude_risks = set(_as_string_list(filters.get("exclude_risks")))

        with sqlite3.connect(self.db_path) as conn:
            if industry_codes:
                eligible &= self._symbols_for_boards(conn, "industry", industry_codes)
            if concept_codes:
                eligible &= self._symbols_for_boards(conn, "concept", concept_codes)
            if markets:
                eligible &= self._symbols_for_markets(conn, markets)
            if min_listed_trade_days > 0:
                eligible &= self._symbols_with_min_trading_days(
                    conn,
                    min_listed_trade_days,
                    reference_date,
                )
            if min_avg_turnover_20 > 0:
                eligible &= self._symbols_with_min_avg_turnover(
                    conn,
                    min_avg_turnover_20 * 10_000,
                    reference_date,
                )
            if exclude_risks:
                eligible -= self._symbols_for_excluded_risks(conn, exclude_risks, reference_date)

        return [symbol for symbol in ordered_symbols if symbol in eligible]

    @staticmethod
    def _symbols_for_boards(
        conn: sqlite3.Connection,
        board_type: str,
        board_codes: list[str],
    ) -> set[str]:
        placeholders = ",".join(["?"] * len(board_codes))
        rows = conn.execute(
            f"""
            SELECT DISTINCT symbol
            FROM stock_board_members
            WHERE board_type = ? AND board_code IN ({placeholders})
            """,
            (board_type, *board_codes),
        ).fetchall()
        return {row[0] for row in rows}

    @staticmethod
    def _symbols_for_markets(conn: sqlite3.Connection, markets: list[str]) -> set[str]:
        placeholders = ",".join(["?"] * len(markets))
        rows = conn.execute(
            f"""
            SELECT symbol
            FROM stock_basic
            WHERE market IN ({placeholders})
            """,
            markets,
        ).fetchall()
        return {row[0] for row in rows}

    @staticmethod
    def _symbols_with_min_trading_days(
        conn: sqlite3.Connection,
        min_days: int,
        reference_date: str | None,
    ) -> set[str]:
        params: tuple[Any, ...]
        date_filter = ""
        if reference_date:
            date_filter = "WHERE date <= ?"
            params = (reference_date, min_days)
        else:
            params = (min_days,)
        rows = conn.execute(
            f"""
            SELECT symbol
            FROM stock_daily
            {date_filter}
            GROUP BY symbol
            HAVING COUNT(*) >= ?
            """,
            params,
        ).fetchall()
        return {row[0] for row in rows}

    @staticmethod
    def _symbols_with_min_avg_turnover(
        conn: sqlite3.Connection,
        min_turnover: float,
        reference_date: str | None,
    ) -> set[str]:
        date_filter = "WHERE date <= ?" if reference_date else ""
        params: tuple[Any, ...] = (reference_date, min_turnover) if reference_date else (min_turnover,)
        rows = conn.execute(
            f"""
            WITH ranked AS (
                SELECT
                    symbol,
                    turnover,
                    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS row_number
                FROM stock_daily
                {date_filter}
            ),
            avg20 AS (
                SELECT symbol, AVG(turnover) AS avg_turnover
                FROM ranked
                WHERE row_number <= 20
                GROUP BY symbol
            )
            SELECT symbol
            FROM avg20
            WHERE avg_turnover >= ?
            """,
            params,
        ).fetchall()
        return {row[0] for row in rows}

    @staticmethod
    def _symbols_for_excluded_risks(
        conn: sqlite3.Connection,
        risks: set[str],
        reference_date: str | None,
    ) -> set[str]:
        excluded: set[str] = set()
        if "delisted" in risks:
            rows = conn.execute(
                """
                SELECT symbol
                FROM stock_basic
                WHERE COALESCE(status, '1') <> '1'
                   OR (out_date IS NOT NULL AND out_date <> '')
                """
            ).fetchall()
            excluded.update(row[0] for row in rows)
        if "st" in risks:
            rows = conn.execute(
                """
                SELECT symbol
                FROM stock_basic
                WHERE UPPER(COALESCE(name, '')) LIKE '%ST%'
                """
            ).fetchall()
            excluded.update(row[0] for row in rows)
        if "suspended" in risks:
            params: tuple[Any, ...] = (reference_date,) if reference_date else ()
            date_filter = "WHERE date <= ?" if reference_date else ""
            row = conn.execute(
                f"SELECT MAX(date) FROM stock_daily {date_filter}",
                params,
            ).fetchone()
            market_latest = row[0] if row else None
            if market_latest:
                rows = conn.execute(
                    """
                    SELECT symbol
                    FROM stock_daily
                    GROUP BY symbol
                    HAVING MAX(date) < ?
                    """,
                    (market_latest,),
                ).fetchall()
                excluded.update(row[0] for row in rows)
        return excluded

    def _bs_fetch_stock_list(self) -> list[dict[str, str]]:
        """Baostock 获取全市场 A 股列表。"""
        import baostock as bs

        lg = bs.login()
        if lg.error_code != "0":
            logger.error(f"Baostock 登录失败: {lg.error_msg}")
            return []

        try:
            rs = bs.query_stock_basic(code_name="", code="")
            fields = getattr(rs, "fields", [])
            records: list[dict[str, str]] = []
            while rs.next():
                row = rs.get_row_data()
                data = dict(zip(fields, row))
                code = data.get("code") or (row[0] if len(row) > 0 else "")
                name = data.get("code_name") or (row[1] if len(row) > 1 else "")
                list_date = data.get("ipoDate") or (row[2] if len(row) > 2 else "")
                out_date = data.get("outDate") or (row[3] if len(row) > 3 else "")
                stock_type = data.get("type") or (row[4] if len(row) > 4 else "")
                status = data.get("status") or (row[5] if len(row) > 5 else "")
                if not code or "." not in code:
                    continue
                if status and status != "1":
                    continue
                if stock_type and stock_type != "1":
                    continue

                symbol = code.split(".")[1]
                records.append(
                    {
                        "symbol": symbol,
                        "code": code,
                        "name": name or symbol,
                        "status": status,
                        "stock_type": stock_type,
                        "market": self._market_from_code(code, symbol),
                        "list_date": _normalize_date_text(list_date),
                        "out_date": _normalize_date_text(out_date),
                    }
                )

            logger.info(f"Baostock 获取股票列表完成，共 {len(records)} 只")
            return records
        except Exception as e:
            logger.error(f"Baostock 获取股票列表失败: {e}")
            return []
        finally:
            bs.logout()

    def get_local_symbols(self) -> list[str]:
        conn = sqlite3.connect(self.db_path)
        try:
            rows = conn.execute(
                "SELECT DISTINCT symbol FROM stock_daily"
            ).fetchall()
        finally:
            conn.close()
        return [row[0] for row in rows]

    def list_local_stocks(
        self,
        query: str | None = None,
        limit: int = 80,
        offset: int = 0,
    ) -> list[dict[str, object]]:
        """Return locally stored stock coverage and latest quote rows."""
        limit = max(1, min(limit, 500))
        offset = max(0, offset)
        filters = ""
        params: list[object] = []
        if query:
            filters = "WHERE c.symbol LIKE ? OR b.name LIKE ?"
            pattern = f"%{query.strip()}%"
            params.extend([pattern, pattern])

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                f"""
                WITH coverage AS (
                    SELECT
                        symbol,
                        COUNT(*) AS row_count,
                        MIN(date) AS earliest_date,
                        MAX(date) AS latest_date
                    FROM stock_daily
                    GROUP BY symbol
                ),
                latest AS (
                    SELECT sd.*
                    FROM stock_daily sd
                    JOIN coverage c
                      ON c.symbol = sd.symbol
                     AND c.latest_date = sd.date
                )
                SELECT
                    c.symbol,
                    COALESCE(b.name, c.symbol) AS name,
                    COALESCE(b.code, '') AS code,
                    c.row_count,
                    c.earliest_date,
                    c.latest_date,
                    latest.open,
                    latest.high,
                    latest.low,
                    latest.close,
                    latest.volume,
                    latest.turnover
                FROM coverage c
                LEFT JOIN stock_basic b ON b.symbol = c.symbol
                LEFT JOIN latest ON latest.symbol = c.symbol
                {filters}
                ORDER BY c.latest_date DESC, c.symbol ASC
                LIMIT ? OFFSET ?
                """,
                (*params, limit, offset),
            ).fetchall()

        return [dict(row) for row in rows]

    def get_ohlcv_tail(self, symbol: str, limit: int = 120) -> list[dict[str, object]]:
        """Return the latest OHLCV rows for a symbol in ascending date order."""
        limit = max(1, min(limit, 500))
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT symbol, date, open, high, low, close, volume, turnover
                FROM (
                    SELECT symbol, date, open, high, low, close, volume, turnover
                    FROM stock_daily
                    WHERE symbol = ?
                    ORDER BY date DESC
                    LIMIT ?
                )
                ORDER BY date ASC
                """,
                (symbol, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_stock_summary(
        self,
        symbol: str,
        end_date: str | None = None,
    ) -> dict[str, object] | None:
        """Return local stock name, coverage, and latest quote for one symbol."""
        conn = sqlite3.connect(self.db_path)
        try:
            conn.row_factory = sqlite3.Row
            params: tuple[object, ...]
            date_filter = ""
            if end_date:
                date_filter = "AND date <= ?"
                params = (symbol, end_date)
            else:
                params = (symbol,)
            row = conn.execute(
                f"""
                WITH coverage AS (
                    SELECT
                        symbol,
                        COUNT(*) AS row_count,
                        MIN(date) AS earliest_date,
                        MAX(date) AS latest_date
                    FROM stock_daily
                    WHERE symbol = ?
                    {date_filter}
                    GROUP BY symbol
                )
                SELECT
                    c.symbol,
                    COALESCE(b.name, c.symbol) AS name,
                    COALESCE(b.code, '') AS code,
                    c.row_count,
                    c.earliest_date,
                    c.latest_date,
                    sd.open,
                    sd.high,
                    sd.low,
                    sd.close,
                    sd.volume,
                    sd.turnover
                FROM coverage c
                LEFT JOIN stock_basic b ON b.symbol = c.symbol
                LEFT JOIN stock_daily sd
                  ON sd.symbol = c.symbol
                 AND sd.date = c.latest_date
                """,
                params,
            ).fetchone()
        finally:
            conn.close()
        return dict(row) if row else None

    def get_ohlcv_series(
        self,
        symbol: str,
        period: str = "day",
        limit: int | None = None,
    ) -> list[dict[str, object]]:
        """Return OHLCV rows, optionally aggregated to week/month/quarter/year."""
        period = period.lower()
        if period not in {"day", "week", "month", "quarter", "year"}:
            raise ValueError(f"Unsupported period: {period}")

        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql(
                """
                SELECT symbol, date, open, high, low, close, volume, turnover
                FROM stock_daily
                WHERE symbol = ?
                ORDER BY date ASC
                """,
                conn,
                params=(symbol,),
            )

        if df.empty:
            return []

        for col in ["open", "high", "low", "close", "volume", "turnover"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["date", "open", "high", "low", "close"])
        if df.empty:
            return []

        if period != "day":
            rule = {
                "week": "W-FRI",
                "month": "ME",
                "quarter": "QE",
                "year": "YE",
            }[period]
            df["trade_date"] = pd.to_datetime(df["date"])
            df = (
                df.set_index("trade_date")
                .resample(rule)
                .agg(
                    {
                        "symbol": "last",
                        "date": "last",
                        "open": "first",
                        "high": "max",
                        "low": "min",
                        "close": "last",
                        "volume": "sum",
                        "turnover": "sum",
                    }
                )
                .dropna(subset=["date", "open", "high", "low", "close"])
                .reset_index(drop=True)
            )

        if limit is not None and limit > 0:
            df = df.tail(min(limit, 10000))

        return [
            {
                "symbol": str(row.symbol),
                "date": str(row.date),
                "open": _clean_float(row.open),
                "high": _clean_float(row.high),
                "low": _clean_float(row.low),
                "close": _clean_float(row.close),
                "volume": _clean_float(row.volume),
                "turnover": _clean_float(row.turnover),
            }
            for row in df.itertuples(index=False)
        ]


def _clean_float(value: object) -> float | None:
    try:
        if pd.isna(value):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_date_text(value: object) -> str:
    text = str(value or "").strip()
    if not text or text in {"None", "nan", "NaT"}:
        return ""
    return _format_compact_date(text)


def _format_compact_date(value: object) -> str:
    text = str(value or "").strip()
    if not text or text in {"None", "nan", "NaT"}:
        return ""
    if len(text) == 8 and text.isdigit():
        return f"{text[0:4]}-{text[4:6]}-{text[6:8]}"
    return text


def _pick_text(row: Any, candidates: list[str]) -> str:
    for key in candidates:
        try:
            value = row.get(key)
        except AttributeError:
            value = None
        if value is not None and not pd.isna(value):
            text = str(value).strip()
            if text:
                return text
    return ""


def _normalize_stock_symbol(value: object) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return ""
    parts = text.replace("_", ".").replace("-", ".").split(".")
    for part in parts:
        if part.isdigit():
            return part.zfill(6)
    digits = "".join(char for char in text if char.isdigit())
    if 1 <= len(digits) <= 6:
        return digits.zfill(6)
    return text


def _board_type_label(board_type: str) -> str:
    return "行业" if board_type == "industry" else "概念"


def _jq_board_items(
    boards: Any,
    code_columns: list[str],
    name_columns: list[str],
) -> list[tuple[str, str]]:
    if boards is None:
        return []
    if isinstance(boards, dict):
        items: list[tuple[str, str]] = []
        for code, value in boards.items():
            if isinstance(value, dict):
                name = _pick_text(value, name_columns)
            else:
                name = str(value).strip()
            if str(code).strip() and name:
                items.append((str(code).strip(), name))
        return items
    if not isinstance(boards, pd.DataFrame) or boards.empty:
        return []

    items = []
    for index, row in boards.iterrows():
        code = _pick_text(row, code_columns) or str(index).strip()
        name = _pick_text(row, name_columns)
        if code and name:
            items.append((code, name))
    return items


def _board_members_from_codes(
    codes: Any,
    board_code: str,
    board_name: str,
    local_symbols: set[str],
) -> list[dict[str, str]]:
    if codes is None:
        return []
    if isinstance(codes, pd.DataFrame):
        for column in ("code", "ts_code", "symbol"):
            if column in codes.columns:
                values = codes[column].tolist()
                break
        else:
            values = codes.iloc[:, 0].tolist()
    elif isinstance(codes, pd.Series):
        values = codes.tolist()
    else:
        try:
            values = list(codes)
        except TypeError:
            values = [codes]

    members: list[dict[str, str]] = []
    seen: set[str] = set()
    for code in values:
        symbol = _normalize_stock_symbol(code)
        if symbol not in local_symbols or symbol in seen:
            continue
        seen.add(symbol)
        members.append(
            {
                "symbol": symbol,
                "board_code": board_code,
                "board_name": board_name,
            }
        )
    return members


def _market_label(market: str) -> str:
    return {
        "SH": "上海",
        "SZ": "深圳",
        "BJ": "北京",
    }.get(market, market)


def _as_string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        values = [value]
    else:
        try:
            values = list(value)
        except TypeError:
            values = [value]
    return [str(item).strip() for item in values if str(item).strip()]
