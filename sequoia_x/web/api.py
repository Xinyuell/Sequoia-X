"""FastAPI route handlers for the local WebUI."""

import sqlite3
from datetime import date
from inspect import Parameter, signature
from pathlib import Path
from statistics import median
from typing import Any, Literal

import pandas as pd
from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, Field

from sequoia_x.data.engine import DataEngine
from sequoia_x.strategy.registry import (
    ParameterValidationError,
    get_strategy_definition,
    list_strategy_metadata,
)
from sequoia_x.strategy.result import StrategyResultRow
from sequoia_x.web.jobs import InMemoryJobManager, JobAlreadyRunningError, JobNotFoundError


class StrategyRunRequest(BaseModel):
    parameters: dict[str, Any] = Field(default_factory=dict)
    reference_date: date | None = None
    backtest_days: list[int] = Field(default_factory=lambda: [1, 3, 5])
    filters: dict[str, Any] = Field(default_factory=dict)


class BackfillRequest(BaseModel):
    start_date: date | None = None
    full_refresh: bool = False
    source: Literal["auto", "tushare", "baostock"] = "auto"


def create_api_router() -> APIRouter:
    router = APIRouter(prefix="/api")

    @router.get("/data/summary")
    def data_summary(request: Request) -> dict[str, Any]:
        return summarize_market_data(_engine(request))

    @router.get("/stocks")
    def stocks(
        request: Request,
        query: str | None = Query(default=None, max_length=40),
        limit: int = Query(default=80, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
    ) -> list[dict[str, Any]]:
        return _engine(request).list_local_stocks(query=query, limit=limit, offset=offset)

    @router.get("/stocks/{symbol}/ohlcv")
    def stock_ohlcv(
        request: Request,
        symbol: str,
        period: str = Query(default="day", pattern="^(day|week|month|quarter|year)$"),
        limit: int = Query(default=10000, ge=1, le=10000),
    ) -> dict[str, Any]:
        if not symbol.isdigit() or len(symbol) != 6:
            raise HTTPException(status_code=422, detail="symbol must be a 6-digit code")
        rows = _engine(request).get_ohlcv_series(symbol=symbol, period=period, limit=limit)
        if not rows:
            raise HTTPException(status_code=404, detail="No local OHLCV data for symbol")
        stock = _engine(request).get_stock_summary(symbol) or {"symbol": symbol}
        return {
            "symbol": symbol,
            "period": period,
            "total": len(rows),
            "stock": stock,
            "rows": rows,
        }

    @router.get("/stock-filters")
    def stock_filters(request: Request) -> dict[str, Any]:
        return _engine(request).list_stock_filter_options()

    @router.post("/data/metadata")
    def start_metadata_sync(request: Request) -> dict[str, str]:
        engine = _engine(request)

        def work(progress: Any) -> dict[str, Any]:
            progress(message="正在同步股票行业、概念和基础资料")
            result = engine.sync_stock_metadata(progress_callback=progress)
            progress(message="股票行业、概念和基础资料同步完成", **result)
            return result

        return _start_job(request, "metadata", "股票画像同步已排队", work)

    @router.post("/data/backfill")
    def start_backfill(request: Request, payload: BackfillRequest | None = None) -> dict[str, str]:
        engine = _engine(request)
        payload = payload or BackfillRequest()
        start_date = payload.start_date.isoformat() if payload.start_date else None

        def work(progress: Any) -> dict[str, Any]:
            progress(
                message="正在同步全市场股票列表",
                total=0,
                processed=0,
                success=0,
                skipped=0,
                failed=0,
                rows_written=0,
                full_refresh=payload.full_refresh,
                start_date=start_date,
            )
            symbols = engine.get_all_symbols()
            progress(
                message="已获取全市场股票列表，开始更新历史 K 线",
                total=len(symbols),
                processed=0,
                success=0,
                skipped=0,
                failed=0,
                rows_written=0,
                full_refresh=payload.full_refresh,
                start_date=start_date,
            )
            result = engine.backfill(
                symbols,
                start_date=start_date,
                full_refresh=payload.full_refresh,
                progress_callback=progress,
                source=payload.source,
            )
            if result is None:
                return {"symbol_count": len(symbols)}
            return dict(result)

        return _start_job(request, "backfill", "历史 K 线更新已排队", work)

    @router.post("/data/sync")
    def start_sync(request: Request) -> dict[str, str]:
        engine = _engine(request)

        def work(progress: Any) -> dict[str, Any]:
            progress(message="正在执行每日增量更新")
            count = engine.sync_today_bulk()
            progress(message="每日增量更新完成", rows_written=count)
            return {"row_count": count}

        return _start_job(request, "sync", "每日增量更新已排队", work)

    @router.get("/jobs/{job_id}")
    def job_status(request: Request, job_id: str) -> dict[str, Any]:
        try:
            return _jobs(request).get(job_id).to_dict()
        except JobNotFoundError as exc:
            raise HTTPException(status_code=404, detail="Job not found") from exc

    @router.get("/strategies")
    def strategies() -> list[dict[str, Any]]:
        return list_strategy_metadata()

    @router.post("/strategies/{strategy_key}/run")
    def run_strategy(
        request: Request,
        strategy_key: str,
        payload: StrategyRunRequest,
    ) -> dict[str, Any]:
        return _run_strategy(request, strategy_key, payload)

    @router.post("/strategies/{strategy_key}/run-job")
    def start_strategy_job(
        request: Request,
        strategy_key: str,
        payload: StrategyRunRequest,
    ) -> dict[str, str]:
        try:
            definition = get_strategy_definition(strategy_key)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=f"Unknown strategy: {strategy_key}") from exc

        def work(progress: Any) -> dict[str, Any]:
            progress(
                message=f"{definition.name} 正在运行",
                total=0,
                processed=0,
                matched=0,
                current_action="准备策略",
                strategy_key=definition.key,
                strategy_name=definition.name,
            )
            result = _run_strategy(request, strategy_key, payload, progress_callback=progress)
            progress(
                message=f"{definition.name} 运行完成",
                matched=result["total"],
                current_action="完成",
                strategy_key=definition.key,
                strategy_name=definition.name,
            )
            return result

        return _start_job(request, "strategy", f"{definition.name} 已排队", work)

    return router


def _run_strategy(
    request: Request,
    strategy_key: str,
    payload: StrategyRunRequest,
    progress_callback: Any | None = None,
) -> dict[str, Any]:
    engine = _engine(request)
    reference_date = payload.reference_date.isoformat() if payload.reference_date else None
    eligible_symbols = engine.get_filtered_symbols(payload.filters, reference_date=reference_date)
    strategy_engine = _FilteredEngine(engine, eligible_symbols)
    try:
        definition = get_strategy_definition(strategy_key)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Unknown strategy: {strategy_key}") from exc

    try:
        strategy, parameters = definition.create(
            engine=strategy_engine,
            settings=request.app.state.settings,
            raw_parameters=payload.parameters,
            reference_date=reference_date,
        )
    except ParameterValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    if hasattr(strategy, "run_with_details"):
        if progress_callback is not None and _accepts_keyword(strategy.run_with_details, "progress_callback"):
            detail_rows = strategy.run_with_details(progress_callback=progress_callback)
        else:
            detail_rows = strategy.run_with_details()
    else:
        detail_rows = [
            _symbol_to_result_row(engine, symbol, end_date=reference_date)
            for symbol in strategy.run()
        ]

    rows = [
        row.to_dict() if isinstance(row, StrategyResultRow) else row
        for row in detail_rows
    ]
    eligible_set = set(eligible_symbols)
    rows = [row for row in rows if row.get("symbol") in eligible_set]
    rows = [_with_stock_summary(engine, row) for row in rows]
    backtest_days = _normalize_backtest_days(payload.backtest_days)
    backtest = _attach_backtest_returns(engine, rows, backtest_days)

    return {
        "strategy_key": definition.key,
        "strategy_name": definition.name,
        "parameters": parameters,
        "reference_date": reference_date,
        "filter_summary": {
            "eligible_symbols": len(eligible_symbols),
            "filters": payload.filters,
        },
        "backtest": backtest,
        "total": len(rows),
        "rows": rows,
    }


class _FilteredEngine:
    def __init__(self, engine: DataEngine, symbols: list[str]) -> None:
        self._engine = engine
        self._symbols = symbols

    def get_local_symbols(self) -> list[str]:
        return list(self._symbols)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._engine, name)


def _accepts_keyword(func: Any, keyword: str) -> bool:
    try:
        parameters = signature(func).parameters.values()
    except (TypeError, ValueError):
        return False
    return any(
        parameter.kind == Parameter.VAR_KEYWORD or parameter.name == keyword
        for parameter in parameters
    )


def _with_stock_summary(engine: DataEngine, row: dict[str, Any]) -> dict[str, Any]:
    symbol = row.get("symbol")
    if not isinstance(symbol, str):
        return row
    row_date = row.get("latest_date")
    stock = engine.get_stock_summary(symbol, end_date=row_date if isinstance(row_date, str) else None)
    if not stock:
        return row
    merged = dict(row)
    merged["name"] = stock.get("name")
    merged["code"] = stock.get("code")
    merged["stock"] = stock
    if merged.get("latest_date") is None:
        merged["latest_date"] = stock.get("latest_date")
    if merged.get("close") is None:
        merged["close"] = stock.get("close")
    return merged


def _normalize_backtest_days(raw_days: list[int] | None) -> list[int]:
    days = raw_days or [1, 3, 5]
    normalized: list[int] = []
    for raw_day in days:
        try:
            day = int(raw_day)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=422, detail="backtest_days must be positive integers") from exc
        if day <= 0 or day > 120:
            raise HTTPException(status_code=422, detail="backtest_days must be between 1 and 120")
        if day not in normalized:
            normalized.append(day)
    if len(normalized) > 8:
        raise HTTPException(status_code=422, detail="backtest_days supports at most 8 horizons")
    return normalized or [1, 3, 5]


def _attach_backtest_returns(
    engine: DataEngine,
    rows: list[dict[str, Any]],
    horizons: list[int],
) -> dict[str, Any]:
    for row in rows:
        row["backtest_returns"] = {}
        row["backtest_valid"] = False
        row["backtest_invalid_reason"] = "missing_signal_price"
    if not rows:
        return {
            "horizons": horizons,
            "overview": {"total": 0, "valid": 0, "invalid": 0},
            "summary": [],
            "distribution": _build_backtest_distribution(rows, horizons),
        }

    max_horizon = max(horizons)
    conn = sqlite3.connect(engine.db_path)
    try:
        conn.row_factory = sqlite3.Row
        for row in rows:
            symbol = row.get("symbol")
            base_date = row.get("latest_date")
            base_close = _optional_float(row.get("close"))
            if not isinstance(symbol, str) or not isinstance(base_date, str):
                row["backtest_invalid_reason"] = "missing_signal_price"
                continue
            if base_close is None or base_close <= 0:
                row["backtest_invalid_reason"] = "missing_signal_price"
                continue

            quote_rows = conn.execute(
                """
                SELECT date, close
                FROM stock_daily
                WHERE symbol = ? AND date >= ?
                ORDER BY date ASC
                LIMIT ?
                """,
                (symbol, base_date, max_horizon + 1),
            ).fetchall()
            if not quote_rows or str(quote_rows[0]["date"]) != base_date:
                row["backtest_invalid_reason"] = "missing_signal_price"
                continue

            returns: dict[str, float] = {}
            saw_missing_target = False
            for day in horizons:
                if len(quote_rows) <= day:
                    continue
                future_close = _optional_float(quote_rows[day]["close"])
                if future_close is None or future_close <= 0:
                    saw_missing_target = True
                    continue
                returns[str(day)] = round((future_close - base_close) / base_close * 100, 4)
            row["backtest_returns"] = returns
            if returns:
                row["backtest_valid"] = True
                row["backtest_invalid_reason"] = None
            elif saw_missing_target:
                row["backtest_invalid_reason"] = "missing_target_price"
            else:
                row["backtest_invalid_reason"] = "insufficient_future_data"
    finally:
        conn.close()

    valid = sum(1 for row in rows if row.get("backtest_valid") is True)
    return {
        "horizons": horizons,
        "overview": {
            "total": len(rows),
            "valid": valid,
            "invalid": len(rows) - valid,
        },
        "summary": _summarize_backtest(rows, horizons),
        "distribution": _build_backtest_distribution(rows, horizons),
    }


def _summarize_backtest(rows: list[dict[str, Any]], horizons: list[int]) -> list[dict[str, Any]]:
    total = len(rows)
    summary = []
    for day in horizons:
        key = str(day)
        values = [
            float(row["backtest_returns"][key])
            for row in rows
            if isinstance(row.get("backtest_returns"), dict)
            and _is_number(row["backtest_returns"].get(key))
        ]
        evaluated = len(values)
        win_count = sum(1 for value in values if value > 0)
        gt_1 = sum(1 for value in values if value >= 1)
        lt_minus_1 = sum(1 for value in values if value <= -1)
        flat_between_1 = sum(1 for value in values if -1 <= value <= 1)
        gt_5 = sum(1 for value in values if value >= 5)
        lt_minus_5 = sum(1 for value in values if value <= -5)
        gt_10 = sum(1 for value in values if value >= 10)
        lt_minus_10 = sum(1 for value in values if value <= -10)
        summary.append(
            {
                "days": day,
                "sample_count": evaluated,
                "evaluated": evaluated,
                "missing": total - evaluated,
                "average_pct": round(sum(values) / evaluated, 4) if evaluated else None,
                "median_pct": round(float(median(values)), 4) if evaluated else None,
                "win_count": win_count,
                "win_rate": _ratio(win_count, evaluated),
                "gt_1_count": gt_1,
                "gt_1_rate": _ratio(gt_1, evaluated),
                "lt_minus_1_count": lt_minus_1,
                "lt_minus_1_rate": _ratio(lt_minus_1, evaluated),
                "flat_count": flat_between_1,
                "flat_rate": _ratio(flat_between_1, evaluated),
                "gt_5_count": gt_5,
                "gt_5_rate": _ratio(gt_5, evaluated),
                "lt_minus_5_count": lt_minus_5,
                "lt_minus_5_rate": _ratio(lt_minus_5, evaluated),
                "gt_10_count": gt_10,
                "gt_10_rate": _ratio(gt_10, evaluated),
                "lt_minus_10_count": lt_minus_10,
                "lt_minus_10_rate": _ratio(lt_minus_10, evaluated),
                "up_gt_1": gt_1,
                "up_gt_1_ratio": _ratio(gt_1, evaluated),
                "down_gt_1": lt_minus_1,
                "down_gt_1_ratio": _ratio(lt_minus_1, evaluated),
                "flat_between_1": flat_between_1,
                "flat_between_1_ratio": _ratio(flat_between_1, evaluated),
                "up_gt_10": gt_10,
                "up_gt_10_ratio": _ratio(gt_10, evaluated),
                "down_gt_10": lt_minus_10,
                "down_gt_10_ratio": _ratio(lt_minus_10, evaluated),
            }
        )
    return summary


def _build_backtest_distribution(
    rows: list[dict[str, Any]],
    horizons: list[int],
) -> list[dict[str, Any]]:
    buckets = [
        ("<= -10%", lambda value: value <= -10),
        ("-10% ~ -5%", lambda value: -10 < value <= -5),
        ("-5% ~ -1%", lambda value: -5 < value < -1),
        ("-1% ~ 1%", lambda value: -1 <= value <= 1),
        ("1% ~ 5%", lambda value: 1 < value < 5),
        ("5% ~ 10%", lambda value: 5 <= value < 10),
        (">= 10%", lambda value: value >= 10),
    ]
    values_by_horizon: dict[str, list[float]] = {}
    for day in horizons:
        key = str(day)
        values_by_horizon[key] = [
            float(row["backtest_returns"][key])
            for row in rows
            if isinstance(row.get("backtest_returns"), dict)
            and _is_number(row["backtest_returns"].get(key))
        ]

    distribution = []
    for bucket, predicate in buckets:
        counts: dict[str, int] = {}
        rates: dict[str, float] = {}
        for day in horizons:
            key = str(day)
            values = values_by_horizon[key]
            count = sum(1 for value in values if predicate(value))
            counts[key] = count
            rates[key] = _ratio(count, len(values))
        distribution.append({"bucket": bucket, "counts": counts, "rates": rates})
    return distribution


def _ratio(count: int, total: int) -> float:
    return round(count / total * 100, 2) if total else 0.0


def _is_number(value: Any) -> bool:
    return _optional_float(value) is not None


def summarize_market_data(engine: DataEngine) -> dict[str, Any]:
    db_path = Path(engine.db_path)
    summary: dict[str, Any] = {
        "db_path": str(db_path),
        "symbol_count": 0,
        "row_count": 0,
        "earliest_date": None,
        "latest_date": None,
        "has_data": False,
    }
    if not db_path.exists():
        return summary

    try:
        with sqlite3.connect(db_path) as conn:
            row = conn.execute(
                """
                SELECT
                    COUNT(DISTINCT symbol),
                    COUNT(*),
                    MIN(date),
                    MAX(date)
                FROM stock_daily
                """
            ).fetchone()
    except sqlite3.Error:
        return summary

    if row is None:
        return summary

    symbol_count, row_count, earliest_date, latest_date = row
    summary.update(
        {
            "symbol_count": int(symbol_count or 0),
            "row_count": int(row_count or 0),
            "earliest_date": earliest_date,
            "latest_date": latest_date,
            "has_data": bool(row_count),
        }
    )
    return summary


def _start_job(
    request: Request,
    kind: str,
    message: str,
    work: Any,
) -> dict[str, str]:
    try:
        job = _jobs(request).start(kind=kind, message=message, work=work)
    except JobAlreadyRunningError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"job_id": job.job_id, "status": job.status}


def _symbol_to_result_row(
    engine: DataEngine,
    symbol: str,
    end_date: str | None = None,
) -> StrategyResultRow:
    try:
        df = engine.get_ohlcv(symbol, end_date=end_date)
    except Exception:
        return StrategyResultRow(symbol=symbol)

    if df is None or df.empty:
        return StrategyResultRow(symbol=symbol)

    latest = df.tail(1).iloc[0]
    close = _optional_float(latest.get("close"))
    return StrategyResultRow(
        symbol=symbol,
        latest_date=str(latest.get("date")) if latest.get("date") is not None else None,
        close=close,
        metrics={},
    )


def _optional_float(value: Any) -> float | None:
    try:
        converted = pd.to_numeric(value, errors="coerce")
    except Exception:
        return None
    if pd.isna(converted):
        return None
    return float(converted)


def _engine(request: Request) -> DataEngine:
    return request.app.state.engine


def _jobs(request: Request) -> InMemoryJobManager:
    return request.app.state.jobs
