import pytest

from sequoia_x.core.config import Settings
from sequoia_x.data.engine import DataEngine
from sequoia_x.strategy.registry import (
    ParameterValidationError,
    get_strategy_definition,
    list_strategy_metadata,
)
from sequoia_x.strategy.sideways_consolidation import SidewaysConsolidationStrategy


def test_registry_lists_built_in_strategies() -> None:
    keys = {strategy["key"] for strategy in list_strategy_metadata()}

    assert "sideways_consolidation" in keys
    assert "ma_volume" in keys
    assert "rps_breakout" in keys


def test_sideways_parameters_accept_defaults_and_numeric_strings(tmp_path) -> None:
    settings = Settings(
        db_path=str(tmp_path / "test.db"),
        start_date="2024-01-01",
        feishu_webhook_url="https://example.com/hook",
    )
    engine = DataEngine(settings)
    definition = get_strategy_definition("sideways_consolidation")

    strategy, parameters = definition.create(
        engine=engine,
        settings=settings,
        raw_parameters={
            "lookback_days": "30",
            "max_amplitude_pct": "9.5",
        },
    )

    assert isinstance(strategy, SidewaysConsolidationStrategy)
    assert parameters == {
        "lookback_days": 30,
        "max_amplitude_pct": 9.5,
        "min_distance_pct": 0.0,
        "max_distance_pct": 3.0,
    }


def test_sideways_parameters_reject_out_of_range() -> None:
    definition = get_strategy_definition("sideways_consolidation")

    with pytest.raises(ParameterValidationError, match="lookback_days"):
        definition.validate_parameters({"lookback_days": 2})

