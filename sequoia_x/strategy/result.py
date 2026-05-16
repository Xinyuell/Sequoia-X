"""Result objects shared by WebUI-capable strategies."""

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class StrategyResultRow:
    """One stock selected by a strategy, with optional display metrics."""

    symbol: str
    latest_date: str | None = None
    close: float | None = None
    metrics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "latest_date": self.latest_date,
            "close": self.close,
            "metrics": self.metrics,
        }

