"""Small in-memory job manager for local WebUI actions."""

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from inspect import Parameter, signature
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
    progress: dict[str, Any] = field(default_factory=dict)

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
            "progress": self.progress,
        }


class InMemoryJobManager:
    def __init__(self, run_async: bool = True) -> None:
        self.run_async = run_async
        self._jobs: dict[str, JobRecord] = {}
        self._lock = Lock()

    def start(self, kind: str, message: str, work: Callable[..., Any]) -> JobRecord:
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

    def update_progress(self, job_id: str, message: str | None = None, **progress: Any) -> None:
        with self._lock:
            job = self._jobs[job_id]
            if message is not None:
                job.message = message
            job.progress.update({key: value for key, value in progress.items() if value is not None})

    def _run(self, job_id: str, work: Callable[..., Any]) -> None:
        with self._lock:
            job = self._jobs[job_id]
            job.status = "running"
            job.started_at = datetime.now()
            job.message = f"{job.kind} is running"

        def progress(message: str | None = None, **values: Any) -> None:
            self.update_progress(job_id, message=message, **values)

        try:
            if _accepts_progress_callback(work):
                result = work(progress)
            else:
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


def _accepts_progress_callback(work: Callable[..., Any]) -> bool:
    try:
        parameters = signature(work).parameters.values()
    except (TypeError, ValueError):
        return False

    return any(
        parameter.kind
        in {
            Parameter.POSITIONAL_ONLY,
            Parameter.POSITIONAL_OR_KEYWORD,
            Parameter.KEYWORD_ONLY,
            Parameter.VAR_POSITIONAL,
            Parameter.VAR_KEYWORD,
        }
        for parameter in parameters
    )
