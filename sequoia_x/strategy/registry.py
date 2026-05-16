"""Strategy registry and parameter validation for WebUI workflows."""

from collections.abc import Callable
from dataclasses import dataclass, field
from math import isfinite
from typing import Any, Literal

from sequoia_x.core.config import Settings
from sequoia_x.data.engine import DataEngine
from sequoia_x.strategy.base import BaseStrategy
from sequoia_x.strategy.high_tight_flag import HighTightFlagStrategy
from sequoia_x.strategy.limit_up_shakeout import LimitUpShakeoutStrategy
from sequoia_x.strategy.ma_volume import MaVolumeStrategy
from sequoia_x.strategy.private_placement import PrivatePlacementStrategy
from sequoia_x.strategy.rps_breakout import RpsBreakoutStrategy
from sequoia_x.strategy.sideways_consolidation import SidewaysConsolidationStrategy
from sequoia_x.strategy.turtle_trade import TurtleTradeStrategy
from sequoia_x.strategy.uptrend_limit_down import UptrendLimitDownStrategy

ParameterType = Literal["integer", "number", "boolean", "choice"]
StrategyFactory = Callable[[DataEngine, Settings, dict[str, Any], str | None], BaseStrategy]


class ParameterValidationError(ValueError):
    """Raised when strategy parameter input cannot be accepted."""


@dataclass(frozen=True)
class ParameterOption:
    value: Any
    label: str

    def to_dict(self) -> dict[str, Any]:
        return {"value": self.value, "label": self.label}


@dataclass(frozen=True)
class ParameterDefinition:
    key: str
    label: str
    type: ParameterType
    default: Any
    min_value: float | int | None = None
    max_value: float | int | None = None
    step: float | int | None = None
    unit: str | None = None
    options: list[ParameterOption] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "key": self.key,
            "label": self.label,
            "type": self.type,
            "default": self.default,
        }
        if self.min_value is not None:
            payload["min"] = self.min_value
        if self.max_value is not None:
            payload["max"] = self.max_value
        if self.step is not None:
            payload["step"] = self.step
        if self.unit is not None:
            payload["unit"] = self.unit
        if self.options:
            payload["options"] = [option.to_dict() for option in self.options]
        return payload

    def validate(self, raw_value: Any) -> Any:
        value = self.default if raw_value is None else raw_value
        if self.type == "integer":
            value = self._as_integer(value)
        elif self.type == "number":
            value = self._as_number(value)
        elif self.type == "boolean":
            value = self._as_boolean(value)
        elif self.type == "choice":
            value = self._as_choice(value)
        else:
            raise ParameterValidationError(f"{self.key} has unsupported type {self.type}")

        if self.min_value is not None and value < self.min_value:
            raise ParameterValidationError(f"{self.key} must be >= {self.min_value}")
        if self.max_value is not None and value > self.max_value:
            raise ParameterValidationError(f"{self.key} must be <= {self.max_value}")
        return value

    def _as_integer(self, value: Any) -> int:
        if isinstance(value, bool):
            raise ParameterValidationError(f"{self.key} must be an integer")
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            if value.is_integer():
                return int(value)
            raise ParameterValidationError(f"{self.key} must be an integer")
        if isinstance(value, str):
            text = value.strip()
            if text and text.lstrip("-").isdigit():
                return int(text)
        raise ParameterValidationError(f"{self.key} must be an integer")

    def _as_number(self, value: Any) -> float:
        if isinstance(value, bool):
            raise ParameterValidationError(f"{self.key} must be a number")
        try:
            converted = float(value)
        except (TypeError, ValueError) as exc:
            raise ParameterValidationError(f"{self.key} must be a number") from exc
        if not isfinite(converted):
            raise ParameterValidationError(f"{self.key} must be a number")
        return converted

    def _as_boolean(self, value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            text = value.strip().lower()
            if text in {"true", "1", "yes", "on"}:
                return True
            if text in {"false", "0", "no", "off"}:
                return False
        raise ParameterValidationError(f"{self.key} must be a boolean")

    def _as_choice(self, value: Any) -> Any:
        allowed = {option.value for option in self.options}
        if value in allowed:
            return value
        raise ParameterValidationError(f"{self.key} must be one of {sorted(allowed)}")


@dataclass(frozen=True)
class StrategyDefinition:
    key: str
    name: str
    description: str
    parameters: list[ParameterDefinition]
    factory: StrategyFactory

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "name": self.name,
            "description": self.description,
            "parameters": [parameter.to_dict() for parameter in self.parameters],
        }

    def validate_parameters(self, raw_parameters: dict[str, Any] | None) -> dict[str, Any]:
        raw_parameters = raw_parameters or {}
        known_keys = {parameter.key for parameter in self.parameters}
        unknown_keys = sorted(set(raw_parameters) - known_keys)
        if unknown_keys:
            raise ParameterValidationError(f"Unknown parameter: {unknown_keys[0]}")
        return {
            parameter.key: parameter.validate(raw_parameters.get(parameter.key))
            for parameter in self.parameters
        }

    def create(
        self,
        engine: DataEngine,
        settings: Settings,
        raw_parameters: dict[str, Any] | None = None,
        reference_date: str | None = None,
    ) -> tuple[BaseStrategy, dict[str, Any]]:
        parameters = self.validate_parameters(raw_parameters)
        return self.factory(engine, settings, parameters, reference_date), parameters


def _class_factory(strategy_class: type[BaseStrategy]) -> StrategyFactory:
    def create(
        engine: DataEngine,
        settings: Settings,
        parameters: dict[str, Any],
        reference_date: str | None,
    ) -> BaseStrategy:
        return strategy_class(engine=engine, settings=settings, reference_date=reference_date)

    return create


def _sideways_factory(
    engine: DataEngine,
    settings: Settings,
    parameters: dict[str, Any],
    reference_date: str | None,
) -> SidewaysConsolidationStrategy:
    return SidewaysConsolidationStrategy(
        engine=engine,
        settings=settings,
        reference_date=reference_date,
        **parameters,
    )


BUILTIN_STRATEGIES: tuple[StrategyDefinition, ...] = (
    StrategyDefinition(
        key="sideways_consolidation",
        name="横盘振荡",
        description="筛选近期横盘整理且收盘价接近区间高点的股票。",
        parameters=[
            ParameterDefinition(
                key="lookback_days",
                label="横盘交易日",
                type="integer",
                default=20,
                min_value=5,
                max_value=2000,
                step=1,
                unit="日",
            ),
            ParameterDefinition(
                key="max_amplitude_pct",
                label="最大区间振幅",
                type="number",
                default=12.0,
                min_value=1.0,
                max_value=80.0,
                step=0.5,
                unit="%",
            ),
            ParameterDefinition(
                key="min_distance_pct",
                label="距区间高点不低于",
                type="number",
                default=0.0,
                step=0.5,
                unit="%",
            ),
            ParameterDefinition(
                key="max_distance_pct",
                label="距区间高点不超过",
                type="number",
                default=3.0,
                step=0.5,
                unit="%",
            ),
        ],
        factory=_sideways_factory,
    ),
    StrategyDefinition(
        key="ma_volume",
        name="均线放量",
        description="5 日均线上穿 20 日均线，并伴随成交量放大。",
        parameters=[],
        factory=_class_factory(MaVolumeStrategy),
    ),
    StrategyDefinition(
        key="turtle_trade",
        name="海龟突破",
        description="筛选突破前期高点、流动性充足且动量确认的股票。",
        parameters=[],
        factory=_class_factory(TurtleTradeStrategy),
    ),
    StrategyDefinition(
        key="high_tight_flag",
        name="高旗形整理",
        description="筛选强动量后的高位收敛和缩量整理形态。",
        parameters=[],
        factory=_class_factory(HighTightFlagStrategy),
    ),
    StrategyDefinition(
        key="limit_up_shakeout",
        name="涨停洗盘",
        description="筛选涨停后放量洗盘但支撑未破的股票。",
        parameters=[],
        factory=_class_factory(LimitUpShakeoutStrategy),
    ),
    StrategyDefinition(
        key="uptrend_limit_down",
        name="上升趋势跌停",
        description="筛选上升趋势中的放量急跌机会。",
        parameters=[],
        factory=_class_factory(UptrendLimitDownStrategy),
    ),
    StrategyDefinition(
        key="rps_breakout",
        name="RPS 突破",
        description="筛选相对强度靠前且接近滚动高点的股票。",
        parameters=[],
        factory=_class_factory(RpsBreakoutStrategy),
    ),
    StrategyDefinition(
        key="private_placement",
        name="定增公告",
        description="筛选近期定向增发公告相关股票。",
        parameters=[],
        factory=_class_factory(PrivatePlacementStrategy),
    ),
)


def get_strategy_registry() -> dict[str, StrategyDefinition]:
    return {strategy.key: strategy for strategy in BUILTIN_STRATEGIES}


def list_strategy_metadata() -> list[dict[str, Any]]:
    return [strategy.to_dict() for strategy in BUILTIN_STRATEGIES]


def get_strategy_definition(key: str) -> StrategyDefinition:
    registry = get_strategy_registry()
    try:
        return registry[key]
    except KeyError as exc:
        raise KeyError(f"Unknown strategy: {key}") from exc
