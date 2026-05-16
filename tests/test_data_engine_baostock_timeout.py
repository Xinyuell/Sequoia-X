import time

import pytest

from sequoia_x.core.config import Settings
from sequoia_x.data.engine import DataEngine
from sequoia_x.data.engine import _BaostockHistorySession


def _hanging_history_worker(task_queue, result_queue) -> None:
    result_queue.put(("__ready__", "ok", None))
    while True:
        task = task_queue.get()
        if task is None:
            return
        time.sleep(10)


def test_baostock_history_session_times_out_stuck_query_quickly() -> None:
    session = _BaostockHistorySession(timeout=0.2, worker_target=_hanging_history_worker)

    started = time.monotonic()
    try:
        with pytest.raises(TimeoutError):
            session.query("sz.000001", "2024-01-01", "2024-01-02")
    finally:
        session.close()

    assert time.monotonic() - started < 2


def test_backfill_progress_total_stays_original_symbol_count(tmp_path, monkeypatch) -> None:
    settings = Settings(
        db_path=str(tmp_path / "market.db"),
        start_date="2024-01-01",
        feishu_webhook_url="https://example.com/hook",
    )
    engine = DataEngine(settings)

    class FakeHistorySession:
        def __init__(self, timeout):
            self.timeout = timeout

        def start(self) -> None:
            return None

        def restart(self) -> None:
            return None

        def query(self, bs_code: str, start: str, end: str) -> list:
            return [[end, "1", "2", "1", "1.5", "100", "1000"]]

        def close(self) -> None:
            return None

    monkeypatch.setattr("sequoia_x.data.engine._BaostockHistorySession", FakeHistorySession)
    monkeypatch.setattr(
        engine,
        "_get_stale_symbols",
        lambda symbols, before_date: (["000002"], {"000002": None}),
    )

    progress_events = []
    result = engine.backfill(
        ["000001", "000002", "000003"],
        start_date="2024-01-01",
        progress_callback=lambda **values: progress_events.append(values),
        source="baostock",
    )

    assert result["symbol_count"] == 3
    assert progress_events[-1]["processed"] == 3
    assert {event["total"] for event in progress_events} == {3}
