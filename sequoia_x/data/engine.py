"""数据引擎模块：负责 SQLite 行情数据存储与 baostock 增量同步。"""

import sqlite3
from collections.abc import Callable
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
    updated_at TEXT NOT NULL
);
"""

_CREATE_STOCK_BASIC_NAME_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_stock_basic_name ON stock_basic (name);
"""


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
                rs = bs.query_history_k_data_plus(
                    bs_code,
                    "date,open,high,low,close,volume,amount",
                    start_date=start,
                    end_date=end,
                    frequency="d",
                    adjustflag="1",  # 后复权
                )
                if rs.error_code != "0":
                    time.sleep(2 ** attempt)
                    continue
                while rs.next():
                    results.append([symbol] + rs.get_row_data())
                break
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
        self._ts_pro = None
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
                fields="ts_code,symbol,name",
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

    def sync_today_bulk(self) -> int:
        """增量同步当日数据：Tushare 优先（1 次调用），Baostock 多进程容灾。"""
        from datetime import date

        today_str = date.today().strftime("%Y-%m-%d")

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
    ) -> dict[str, int | str | bool]:
        """批量回填历史日 K 线数据（后复权），支持双数据源。

        source:
          - "auto": Baostock 优先，Tushare 容灾（默认）
          - "tushare": 强制 Tushare
          - "baostock": 仅 Baostock，不启用 Tushare 容灾
        """
        import time
        from datetime import date, timedelta

        today_str = date.today().strftime("%Y-%m-%d")
        effective_start_date = start_date or self.start_date
        date.fromisoformat(effective_start_date)
        total = len(symbols)

        def _emit(message: str | None = None, **progress: Any) -> None:
            if progress_callback is None:
                return
            progress_callback(
                message=message,
                total=total,
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
                total,
                _emit,
            )
            return result or {
                "symbol_count": total,
                "success": 0,
                "skipped": 0,
                "failed": total,
                "rows_written": 0,
                "start_date": effective_start_date,
                "end_date": today_str,
                "full_refresh": full_refresh,
            }

        import baostock as bs

        max_retries = 3
        reconnect_interval = 100
        code_by_symbol = self._get_baostock_code_map()

        def _login():
            lg = bs.login()
            if lg.error_code != "0":
                logger.error(f"baostock 登录失败: {lg.error_msg}")
                return False
            return True

        _emit(
            "正在连接 baostock",
            processed=0,
            success=0,
            skipped=0,
            failed=0,
            rows_written=0,
        )

        if not _login():
            # Baostock 登录失败，尝试用 Tushare 全量接管
            logger.warning("Baostock 登录失败，尝试 Tushare 接管回填")
            result = self._ts_backfill_all(
                symbols,
                effective_start_date,
                today_str,
                full_refresh,
                total,
                _emit,
            )
            if result is not None:
                return result
            _emit(
                "baostock 登录失败，Tushare 亦不可用，更新终止",
                processed=0,
                success=0,
                skipped=0,
                failed=total,
                rows_written=0,
            )
            return {
                "symbol_count": total,
                "success": 0,
                "skipped": 0,
                "failed": total,
                "rows_written": 0,
                "start_date": effective_start_date,
                "end_date": today_str,
                "full_refresh": full_refresh,
            }

        # 预过滤：一条 SQL 找出真正需要更新的股票，避免逐只检查 5000+ 只
        if full_refresh:
            working_symbols = list(symbols)
            last_date_map: dict[str, str | None] = {}
            skipped_before = 0
        else:
            working_symbols, last_date_map = self._get_stale_symbols(symbols, today_str)
            skipped_before = total - len(working_symbols)
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

        total = len(working_symbols)
        success = 0
        skipped = 0
        failed = 0
        rows_written = 0
        since_reconnect = 0

        try:
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
                    bs.logout()
                    time.sleep(1)
                    if not _login():
                        logger.error("重连失败，终止回填")
                        _emit(
                            "baostock 重连失败，更新终止",
                            processed=processed,
                            success=success,
                            skipped=skipped,
                            failed=failed + total - i,
                            rows_written=rows_written,
                            current_symbol=symbol,
                            current_start_date=start,
                            current_action="重连失败",
                        )
                        return {
                            "symbol_count": total,
                            "success": success,
                            "skipped": skipped,
                            "failed": failed + total - i,
                            "rows_written": rows_written,
                            "start_date": effective_start_date,
                            "end_date": today_str,
                            "full_refresh": full_refresh,
                        }
                    since_reconnect = 0

                bs_code = code_by_symbol.get(symbol) or self._to_baostock_code(symbol)
                _emit(
                    f"正在更新 {symbol}（{processed + 1}/{total}）",
                    processed=processed,
                    success=success,
                    skipped=skipped,
                    failed=failed,
                    rows_written=rows_written,
                    current_symbol=symbol,
                    current_start_date=start,
                    current_action="请求历史 K 线",
                )

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
                    # Baostock 全部重试失败，尝试 Tushare 容灾（source=baostock 时跳过）
                    if source != "baostock":
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
                                f"已处理 {i + 1}/{total}，"
                                f"成功 {success} 跳过 {skipped} 失败 {failed}"
                            )
                        time.sleep(0.3)
                        continue
                    failed += 1
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

                df = pd.DataFrame(rows, columns=rs.fields)
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
                        f"已处理 {i + 1}/{total}，"
                        f"成功 {success} 跳过 {skipped} 失败 {failed}"
                    )

                time.sleep(0.3)

        finally:
            bs.logout()

        total_skipped = skipped_before + skipped
        total_processed = total + skipped_before
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
            working_symbols, last_date_map = self._get_stale_symbols(symbols, end_date)
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
        from datetime import datetime

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
                        record.get("status", "1"),
                        record.get("stock_type", "1"),
                        updated_at,
                    )
                    for record in records
                ],
            )
            conn.commit()

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

            logger.info(f"Baostock 获取股票列表完成，共 {len(records)} 只")
            return records
        except Exception as e:
            logger.error(f"Baostock 获取股票列表失败: {e}")
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

    def get_stock_summary(self, symbol: str) -> dict[str, object] | None:
        """Return local stock name, coverage, and latest quote for one symbol."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """
                WITH coverage AS (
                    SELECT
                        symbol,
                        COUNT(*) AS row_count,
                        MIN(date) AS earliest_date,
                        MAX(date) AS latest_date
                    FROM stock_daily
                    WHERE symbol = ?
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
                (symbol,),
            ).fetchone()
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
