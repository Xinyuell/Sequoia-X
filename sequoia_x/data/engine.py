"""数据引擎模块：负责 SQLite 行情数据存储与 baostock 增量同步。"""

import sqlite3
from pathlib import Path

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
    updated_at TEXT NOT NULL
);
"""

_CREATE_STOCK_BASIC_NAME_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_stock_basic_name ON stock_basic (name);
"""


def _bs_fetch_batch(tasks: list) -> list:
    """多进程 worker：独立 login，批量拉取 baostock 数据。"""
    import baostock as bs
    bs.login()
    results = []
    for symbol, bs_code, start, end in tasks:
        rs = bs.query_history_k_data_plus(
            bs_code,
            "date,open,high,low,close,volume,amount",
            start_date=start,
            end_date=end,
            frequency="d",
            adjustflag="1",  # 后复权
        )
        if rs.error_code != "0":
            continue
        while rs.next():
            results.append([symbol] + rs.get_row_data())
    bs.logout()
    return results


class DataEngine:
    """行情数据引擎，负责 SQLite 存储和 baostock 数据同步。"""

    def __init__(self, settings: Settings) -> None:
        self.db_path: str = settings.db_path
        self.start_date: str = settings.start_date
        self._init_db()

    def _init_db(self) -> None:
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(_CREATE_TABLE_SQL)
            conn.execute(_CREATE_INDEX_SQL)
            conn.execute(_CREATE_STOCK_BASIC_TABLE_SQL)
            conn.execute(_CREATE_STOCK_BASIC_NAME_INDEX_SQL)
            conn.commit()
        logger.info(f"数据库初始化完成：{self.db_path}")

    def _get_last_date(self, symbol: str) -> str | None:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT MAX(date) FROM stock_daily WHERE symbol = ?",
                (symbol,),
            ).fetchone()
        return row[0] if row and row[0] else None

    def get_ohlcv(self, symbol: str) -> pd.DataFrame:
        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql(
                "SELECT * FROM stock_daily WHERE symbol = ? ORDER BY date",
                conn,
                params=(symbol,),
            )
        return df

    @staticmethod
    def _to_baostock_code(symbol: str) -> str:
        """将纯数字代码转为 baostock 格式：6/9开头 -> sh，其余 -> sz。"""
        prefix = "sh" if symbol.startswith(("6", "9")) else "sz"
        return f"{prefix}.{symbol}"

    # ── 数据同步 ──

    def sync_today_bulk(self) -> int:
        """多进程并行通过 baostock 拉取增量数据（后复权），写入 SQLite。"""
        from datetime import date, timedelta
        from multiprocessing import Pool

        today_str = date.today().strftime("%Y-%m-%d")

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
            tasks.append((symbol, self._to_baostock_code(symbol), start, today_str))

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

        df = pd.DataFrame(all_rows, columns=["symbol", "date", "open", "high", "low", "close", "volume", "turnover"])
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

        logger.info(f"sync_today_bulk: 写入 {count} 条数据")
        return count

    def backfill(
        self,
        symbols: list[str],
        start_date: str | None = None,
        full_refresh: bool = False,
    ) -> dict[str, int | str | bool]:
        """通过 baostock 批量回填历史日 K 线数据（后复权）。

        容错机制：
        - 单只股票失败自动重试 3 次，间隔递增（2s/4s/8s）
        - 每 200 只股票自动重连 baostock（防止长连接超时）
        - 已入库的自动 skip，中断后可重跑续传
        """
        import time
        from datetime import date, timedelta

        import baostock as bs

        today_str = date.today().strftime("%Y-%m-%d")
        effective_start_date = start_date or self.start_date
        date.fromisoformat(effective_start_date)
        max_retries = 3
        reconnect_interval = 200  # 每处理 N 只股票重连一次

        def _login():
            lg = bs.login()
            if lg.error_code != "0":
                logger.error(f"baostock 登录失败: {lg.error_msg}")
                return False
            return True

        if not _login():
            return {
                "symbol_count": len(symbols),
                "success": 0,
                "skipped": 0,
                "failed": len(symbols),
                "rows_written": 0,
                "start_date": effective_start_date,
                "end_date": today_str,
                "full_refresh": full_refresh,
            }

        success = 0
        skipped = 0
        failed = 0
        rows_written = 0
        since_reconnect = 0

        try:
            for i, symbol in enumerate(symbols):
                last_date = self._get_last_date(symbol)
                if not full_refresh and last_date and last_date >= today_str:
                    skipped += 1
                    if (i + 1) % 500 == 0:
                        logger.info(
                            f"已处理 {i + 1}/{len(symbols)}，"
                            f"成功 {success} 跳过 {skipped} 失败 {failed}"
                        )
                    continue

                # 定期重连，防止长连接超时
                if full_refresh or not last_date:
                    start = effective_start_date
                else:
                    start = (date.fromisoformat(last_date) + timedelta(days=1)).strftime("%Y-%m-%d")
                    if start < effective_start_date:
                        start = effective_start_date

                if start > today_str:
                    skipped += 1
                    continue

                since_reconnect += 1
                if since_reconnect >= reconnect_interval:
                    bs.logout()
                    time.sleep(1)
                    if not _login():
                        logger.error("重连失败，终止回填")
                        return {
                            "symbol_count": len(symbols),
                            "success": success,
                            "skipped": skipped,
                            "failed": failed + len(symbols) - i,
                            "rows_written": rows_written,
                            "start_date": effective_start_date,
                            "end_date": today_str,
                            "full_refresh": full_refresh,
                        }
                    since_reconnect = 0

                bs_code = self._to_baostock_code(symbol)

                # 带重试的查询
                rows = []
                query_ok = False
                for attempt in range(max_retries):
                    try:
                        rs = bs.query_history_k_data_plus(
                            bs_code,
                            "date,open,high,low,close,volume,amount",
                            start_date=start,
                            end_date=today_str,
                            frequency="d",
                            adjustflag="1",  # 后复权
                        )

                        if rs.error_code != "0":
                            raise RuntimeError(rs.error_msg)

                        rows = []
                        while rs.next():
                            rows.append(rs.get_row_data())
                        query_ok = True
                        break

                    except Exception as exc:
                        if attempt < max_retries - 1:
                            wait = 2 ** (attempt + 1)
                            logger.warning(
                                f"[{symbol}] 第{attempt + 1}次失败: {exc}，{wait}s 后重试"
                            )
                            time.sleep(wait)
                            # 重连 baostock
                            bs.logout()
                            time.sleep(1)
                            _login()
                        else:
                            logger.warning(f"[{symbol}] {max_retries}次重试均失败，跳过")

                if not query_ok:
                    failed += 1
                    continue

                if not rows:
                    skipped += 1
                    continue

                df = pd.DataFrame(rows, columns=rs.fields)
                for col in ["open", "high", "low", "close", "volume", "amount"]:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                df = df.dropna(subset=["close"])
                df = df[df["volume"] > 0]

                if df.empty:
                    skipped += 1
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

                if (i + 1) % 500 == 0:
                    logger.info(
                        f"已处理 {i + 1}/{len(symbols)}，"
                        f"成功 {success} 跳过 {skipped} 失败 {failed}"
                    )

        finally:
            bs.logout()

        logger.info(f"回填完成 — 成功: {success} | 跳过: {skipped} | 失败: {failed}")
        return {
            "symbol_count": len(symbols),
            "success": success,
            "skipped": skipped,
            "failed": failed,
            "rows_written": rows_written,
            "start_date": effective_start_date,
            "end_date": today_str,
            "full_refresh": full_refresh,
        }

    # ── 股票列表 ──

    def get_all_symbols(self) -> list[str]:
        """通过 baostock 获取全市场 A 股代码列表，并同步本地股票名称。"""
        records = self.sync_stock_basic()
        return [record["symbol"] for record in records]

    def sync_stock_basic(self) -> list[dict[str, str]]:
        """同步全市场 A 股基础信息，返回本次获取到的股票记录。"""
        import baostock as bs
        from datetime import datetime

        lg = bs.login()
        if lg.error_code != "0":
            logger.error(f"baostock 登录失败: {lg.error_msg}")
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
                    }
                )

            updated_at = datetime.now().isoformat(timespec="seconds")
            with sqlite3.connect(self.db_path) as conn:
                conn.executemany(
                    """
                    INSERT INTO stock_basic
                        (symbol, code, name, status, stock_type, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(symbol) DO UPDATE SET
                        code = excluded.code,
                        name = excluded.name,
                        status = excluded.status,
                        stock_type = excluded.stock_type,
                        updated_at = excluded.updated_at
                    """,
                    [
                        (
                            record["symbol"],
                            record["code"],
                            record["name"],
                            record["status"],
                            record["stock_type"],
                            updated_at,
                        )
                        for record in records
                    ],
                )
                conn.commit()

            logger.info(f"获取股票列表完成，共 {len(records)} 只")
            return records
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            return []
        finally:
            bs.logout()

    def get_local_symbols(self) -> list[str]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT DISTINCT symbol FROM stock_daily"
            ).fetchall()
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
