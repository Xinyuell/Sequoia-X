"""FastAPI route handlers for the local WebUI."""

import sqlite3
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import APIRouter, HTTPException, Request
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


class BackfillRequest(BaseModel):
    start_date: date | None = None
    full_refresh: bool = False


def create_api_router() -> APIRouter:
    router = APIRouter(prefix="/api")

    @router.get("/data/summary")
    def data_summary(request: Request) -> dict[str, Any]:
        return summarize_market_data(_engine(request))

    @router.post("/data/backfill")
    def start_backfill(request: Request, payload: BackfillRequest | None = None) -> dict[str, str]:
        engine = _engine(request)
        payload = payload or BackfillRequest()
        start_date = payload.start_date.isoformat() if payload.start_date else None

        def work() -> dict[str, Any]:
            symbols = engine.get_all_symbols()
            result = engine.backfill(
                symbols,
                start_date=start_date,
                full_refresh=payload.full_refresh,
            )
            if result is None:
                return {"symbol_count": len(symbols)}
            return dict(result)

        return _start_job(request, "backfill", "Historical backfill queued", work)

    @router.post("/data/sync")
    def start_sync(request: Request) -> dict[str, str]:
        engine = _engine(request)

        def work() -> dict[str, Any]:
            count = engine.sync_today_bulk()
            return {"row_count": count}

        return _start_job(request, "sync", "Incremental sync queued", work)

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
        try:
            definition = get_strategy_definition(strategy_key)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=f"Unknown strategy: {strategy_key}") from exc

        try:
            strategy, parameters = definition.create(
                engine=_engine(request),
                settings=request.app.state.settings,
                raw_parameters=payload.parameters,
            )
        except ParameterValidationError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        if hasattr(strategy, "run_with_details"):
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

        return {
            "strategy_key": definition.key,
            "strategy_name": definition.name,
            "parameters": parameters,
            "total": len(rows),
            "rows": rows,
        }

    return router


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
