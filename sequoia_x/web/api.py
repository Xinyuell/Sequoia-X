"""FastAPI route handlers for the local WebUI."""

import sqlite3
from datetime import date
from inspect import Parameter, signature
from pathlib import Path
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
        try:
            definition = get_strategy_definition(strategy_key)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=f"Unknown strategy: {strategy_key}") from exc

        try:
            strategy, parameters = definition.create(
                engine=_engine(request),
                settings=request.app.state.settings,
                raw_parameters=payload.parameters,
                reference_date=payload.reference_date.isoformat() if payload.reference_date else None,
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
                _symbol_to_result_row(_engine(request), symbol)
                for symbol in strategy.run()
            ]

        rows = [
            row.to_dict() if isinstance(row, StrategyResultRow) else row
            for row in detail_rows
        ]
        rows = [_with_stock_summary(_engine(request), row) for row in rows]

        return {
            "strategy_key": definition.key,
            "strategy_name": definition.name,
            "parameters": parameters,
            "total": len(rows),
            "rows": rows,
        }


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
    stock = engine.get_stock_summary(symbol)
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


def _symbol_to_result_row(engine: DataEngine, symbol: str) -> StrategyResultRow:
    try:
        df = engine.get_ohlcv(symbol)
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
