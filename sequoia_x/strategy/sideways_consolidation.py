"""Configurable sideways consolidation strategy."""

from math import isfinite

import pandas as pd

from sequoia_x.core.logger import get_logger
from sequoia_x.strategy.base import BaseStrategy
from sequoia_x.strategy.result import StrategyResultRow

logger = get_logger(__name__)


class SidewaysConsolidationStrategy(BaseStrategy):
    """Select stocks consolidating in a narrow range and near the range high."""

    webhook_key: str = "sideways_consolidation"

    def __init__(
        self,
        *args,
        lookback_days: int = 20,
        max_amplitude_pct: float = 12.0,
        near_high_pct: float = 3.0,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.lookback_days = lookback_days
        self.max_amplitude_pct = max_amplitude_pct
        self.near_high_pct = near_high_pct

    def run(self) -> list[str]:
        return [row.symbol for row in self.run_with_details()]

    def run_with_details(self) -> list[StrategyResultRow]:
        rows: list[StrategyResultRow] = []

        for symbol in self.engine.get_local_symbols():
            try:
                row = self._evaluate_symbol(symbol)
            except Exception as exc:
                logger.warning(f"[{symbol}] SidewaysConsolidationStrategy failed: {exc}")
                continue
            if row is not None:
                rows.append(row)

        rows.sort(
            key=lambda row: (
                float(row.metrics.get("distance_to_high_pct", 0)),
                float(row.metrics.get("amplitude_pct", 0)),
                row.symbol,
            )
        )
        logger.info(f"SidewaysConsolidationStrategy selected {len(rows)} stocks")
        return rows

    def _evaluate_symbol(self, symbol: str) -> StrategyResultRow | None:
        df = self.engine.get_ohlcv(symbol)
        if df is None or len(df) < self.lookback_days:
            return None

        required = ["date", "high", "low", "close"]
        if any(column not in df.columns for column in required):
            return None

        window = df.tail(self.lookback_days).copy()
        window[["high", "low", "close"]] = window[["high", "low", "close"]].apply(
            pd.to_numeric,
            errors="coerce",
        )
        if window[["high", "low", "close"]].isna().any().any():
            return None

        window_high = float(window["high"].max())
        window_low = float(window["low"].min())
        latest = window.iloc[-1]
        latest_close = float(latest["close"])

        if (
            window_low <= 0
            or window_high <= 0
            or latest_close <= 0
            or not all(isfinite(value) for value in [window_high, window_low, latest_close])
        ):
            return None

        amplitude_pct = (window_high - window_low) / window_low * 100
        distance_to_high_pct = (window_high - latest_close) / window_high * 100

        if amplitude_pct > self.max_amplitude_pct:
            return None
        if distance_to_high_pct > self.near_high_pct:
            return None

        return StrategyResultRow(
            symbol=symbol,
            latest_date=str(latest["date"]),
            close=round(latest_close, 4),
            metrics={
                "window_high": round(window_high, 4),
                "window_low": round(window_low, 4),
                "amplitude_pct": round(amplitude_pct, 4),
                "distance_to_high_pct": round(distance_to_high_pct, 4),
            },
        )

