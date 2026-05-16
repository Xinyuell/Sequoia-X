import pytest
from threading import Event

from sequoia_x.web.jobs import InMemoryJobManager, JobAlreadyRunningError


def test_job_manager_records_success() -> None:
    manager = InMemoryJobManager(run_async=False)

    job = manager.start("sync", "queued", lambda: {"row_count": 3})
    stored = manager.get(job.job_id)

    assert stored.status == "succeeded"
    assert stored.result == {"row_count": 3}
    assert stored.error is None
    assert stored.started_at is not None
    assert stored.finished_at is not None


def test_job_manager_records_failure() -> None:
    manager = InMemoryJobManager(run_async=False)

    def fail() -> None:
        raise RuntimeError("boom")

    job = manager.start("sync", "queued", fail)
    stored = manager.get(job.job_id)

    assert stored.status == "failed"
    assert stored.error == "boom"


def test_job_manager_rejects_second_running_job() -> None:
    manager = InMemoryJobManager(run_async=True)
    blocker = Event()
    started = Event()

    def never_finishes() -> None:
        started.set()
        blocker.wait(timeout=5)

    manager.start("backfill", "queued", never_finishes)
    started.wait(timeout=1)

    with pytest.raises(JobAlreadyRunningError):
        manager.start("sync", "queued", lambda: None)

    assert started.is_set()
    blocker.set()
