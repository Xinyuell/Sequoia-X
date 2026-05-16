"""Small in-memory job manager for local WebUI actions."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from threading import Lock, Thread
from typing import Any, Literal
from uuid import uuid4

JobStatus = Literal["queued", "running", "succeeded", "failed"]


class JobAlreadyRunningError(RuntimeError):
    """Raised when a new data job is requested while one is active."""


class JobNotFoundError(KeyError):
    """Raised when a requested job id does not exist."""


@dataclass
class JobRecord:
    job_id: str
    kind: str
    status: JobStatus
    message: str
    started_at: datetime | None = None
    finished_at: datetime | None = None
    result: Any = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "kind": self.kind,
            "status": self.status,
            "message": self.message,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "result": self.result,
            "error": self.error,
        }


class InMemoryJobManager:
    def __init__(self, run_async: bool = True) -> None:
        self.run_async = run_async
        self._jobs: dict[str, JobRecord] = {}
        self._lock = Lock()

    def start(self, kind: str, message: str, work: Callable[[], Any]) -> JobRecord:
        with self._lock:
            if any(job.status in {"queued", "running"} for job in self._jobs.values()):
                raise JobAlreadyRunningError("A data job is already running")

            timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            job = JobRecord(
                job_id=f"{kind}-{timestamp}-{uuid4().hex[:8]}",
                kind=kind,
                status="queued",
                message=message,
            )
            self._jobs[job.job_id] = job

        if self.run_async:
            Thread(target=self._run, args=(job.job_id, work), daemon=True).start()
        else:
            self._run(job.job_id, work)
        return job

    def get(self, job_id: str) -> JobRecord:
        with self._lock:
            try:
                return self._jobs[job_id]
            except KeyError as exc:
                raise JobNotFoundError(job_id) from exc

    def _run(self, job_id: str, work: Callable[[], Any]) -> None:
        with self._lock:
            job = self._jobs[job_id]
            job.status = "running"
            job.started_at = datetime.now()
            job.message = f"{job.kind} is running"

        try:
            result = work()
        except Exception as exc:
            with self._lock:
                job.status = "failed"
                job.error = str(exc)
                job.message = f"{job.kind} failed"
                job.finished_at = datetime.now()
            return

        with self._lock:
            job.status = "succeeded"
            job.result = result
            job.message = f"{job.kind} completed"
            job.finished_at = datetime.now()

