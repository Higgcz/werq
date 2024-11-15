import json
import logging
import shutil
import time
import traceback
from abc import ABC, abstractmethod
from collections.abc import Generator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, NewType, Optional

from filelock import BaseFileLock, FileLock

from .exceptions import JobStateError

logger = logging.getLogger(__name__)


class JobState(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


RESULT_DIR = Path("completed")
RESULTS_FILE = "results.json"
ERROR_FILE = "error.txt"

JobID = NewType("JobID", str)


@dataclass
class Job:
    id: JobID
    params: dict[str, Any]
    state: JobState = JobState.QUEUED
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    error: Optional[str] = None
    progress: float = 0.0

    # Serialization
    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "state": self.state.value,
            "params": self.params,
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "error": self.error,
            "progress": self.progress,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Job":
        return cls(
            id=JobID(data["id"]),
            state=JobState(data["state"]),
            params=data["params"],
            created_at=datetime.fromisoformat(data["created_at"]),
            started_at=datetime.fromisoformat(data["started_at"]) if data["started_at"] else None,
            finished_at=datetime.fromisoformat(data["finished_at"]) if data["finished_at"] else None,
            error=data.get("error"),
            progress=data.get("progress", 0.0),
        )

    def __str__(self) -> str:
        return f"Job {self.id} ({self.state})"

    @property
    def duration(self) -> Optional[float]:
        """Get the duration of the job in minutes."""
        match self.state:
            case JobState.QUEUED:
                return (datetime.now() - self.created_at).total_seconds() / 60.0
            case JobState.RUNNING:
                if self.started_at:
                    return (datetime.now() - self.started_at).total_seconds() / 60.0
                return None
            case JobState.COMPLETED | JobState.FAILED:
                if self.started_at and self.finished_at:
                    return (self.finished_at - self.started_at).total_seconds() / 60.0
                return None

    # State transitions
    def start(self) -> None:
        if self.state != JobState.QUEUED:
            raise JobStateError(f"Cannot start job {self.id} in state {self.state}")
        self.state = JobState.RUNNING
        self.started_at = datetime.now()

    def complete(self) -> None:
        if self.state != JobState.RUNNING:
            raise JobStateError(f"Cannot complete job {self.id} in state {self.state}")
        self.update_progress(1.0)
        self.state = JobState.COMPLETED
        self.finished_at = datetime.now()

    def fail(self, err: str) -> None:
        if self.state != JobState.RUNNING:
            raise JobStateError(f"Cannot fail job {self.id} in state {self.state}")
        self.state = JobState.FAILED
        self.finished_at = datetime.now()
        self.error = err

    def update_progress(self, progress: float) -> None:
        if self.state != JobState.RUNNING:
            raise JobStateError(f"Cannot update progress for job {self.id} in state {self.state}")
        self.progress = progress

    # Path handling
    def get_job_file(self, base_dir: Path) -> Path:
        """Get the JSON job file path for a job."""
        return base_dir / self.state.value / f"{self.id}.json"

    def get_result_dir(self, base_dir: Path) -> Path:
        """Get the result directory for a job."""
        return base_dir / RESULT_DIR / self.id

    def save(self, base_dir: Path) -> None:
        """Save job to a JSON file."""
        job_file = self.get_job_file(base_dir)
        job_file.write_text(json.dumps(self.to_dict(), indent=2))

    def load_result(self, base_dir: Path) -> dict[str, Any]:
        result_file = self.get_result_dir(base_dir) / RESULTS_FILE
        if not result_file.exists():
            return {}
        return json.loads(result_file.read_text())


class JobQueue:
    def __init__(self, base_dir: str | Path) -> None:
        """
        Initialize job queue with base directory.
        """
        self.base_dir = Path(base_dir)
        self._ensure_directories()

    def _ensure_directories(self) -> None:
        """Create necessary directory structure."""
        for dir_name in [RESULT_DIR] + [status.value for status in JobState]:
            (self.base_dir / dir_name).mkdir(parents=True, exist_ok=True)

    def _generate_job_id(self) -> JobID:
        """
        Generate a unique job ID.

        The ID is based on the current time in nanoseconds
        """
        return JobID(str(int(time.time_ns())))

    @contextmanager
    def _with_lock(self, file_path: Path) -> Generator[BaseFileLock, None, None]:
        """Get a file lock for a given file path."""
        lock_path = file_path.with_suffix(".lock")
        # Remove the state directory from the lock path
        lock_path = Path(*lock_path.parts[:-2], lock_path.name)
        yield FileLock(lock_path)
        lock_path.unlink(missing_ok=True)

    def submit(self, params: dict[str, Any]) -> Job:
        """Submit a new job to the queue."""
        job_id = self._generate_job_id()

        job = Job(id=job_id, params=params)

        with self._with_lock(job.get_job_file(self.base_dir)):
            job.save(self.base_dir)

        return job

    def pop_next(self) -> Optional[Job]:
        """Pop the next job from the queue."""
        queue_dir = self.base_dir / JobState.QUEUED.value
        jobs = sorted(queue_dir.glob("*.json"))

        if not jobs:
            return None

        # Get the first job from the queue
        with self._with_lock(jobs[0]):
            try:
                job = Job.from_dict(json.loads(jobs[0].read_text()))
            except Exception as e:
                logger.error(f"Error reading job file {jobs[0]}: {e}")
                logger.debug(traceback.format_exc())
                shutil.move(jobs[0], self.base_dir / JobState.FAILED.value)
                return None

            try:
                job_file = job.get_job_file(self.base_dir)
                # Start the job - change state to running
                job.start()
                # Remove the job from the queue
                job_file.unlink()

                # Move the job to the running directory
                job.save(self.base_dir)
            except Exception as e:
                error = traceback.format_exc()
                logger.error(f"Error starting job {job.id}: {e}")
                logger.debug(error)
                self.fail(job, f"Error starting job: {str(e)}", error)
                return None

        return job

    def complete(self, job: Job) -> None:
        """Mark job as completed."""
        job_file = job.get_job_file(self.base_dir)
        with self._with_lock(job_file):
            job.complete()
            # Remove the job from the running directory
            job_file.unlink()
            job.save(self.base_dir)

    def fail(self, job: Job, error_msg: str, error_traceback: str) -> None:
        """Mark job as failed."""
        job_file = job.get_job_file(self.base_dir)
        with self._with_lock(job_file):
            job.fail(error_msg)
            # Remove the job from the running directory
            job_file.unlink()
            job.save(self.base_dir)

        error_file = job.get_result_dir(self.base_dir) / ERROR_FILE
        error_file.write_text(f"{error_msg}\n\n{error_traceback}")

    def get_result_dir(self, job: Job) -> Path:
        """Get the result directory for a job."""
        return job.get_result_dir(self.base_dir)

    def update_progress(self, job: Job, progress: float) -> None:
        """Update job progress."""
        job_file = job.get_job_file(self.base_dir)
        with self._with_lock(job_file):
            job.update_progress(progress)
            job.save(self.base_dir)

    def get_job(self, job_id: JobID) -> Optional[Job]:
        """Get a job by ID."""
        for state in JobState:
            job_file = self.base_dir / state.value / f"{job_id}.json"
            if job_file.exists():
                with self._with_lock(job_file):
                    try:
                        return Job.from_dict(json.loads(job_file.read_text()))
                    except Exception as e:
                        logger.error(f"Error reading job {job_id}: {e}")
        return None

    def list_jobs(self, *states: JobState, reverse: bool = False) -> list[Job]:
        """List all jobs in the queue."""
        jobs = []
        states = states or tuple(JobState)
        for state in states:
            for job_file in (self.base_dir / state.value).glob("*.json"):
                with self._with_lock(job_file):
                    try:
                        jobs.append(Job.from_dict(json.loads(job_file.read_text())))
                    except Exception as e:
                        logger.error(f"Error reading job {job_file}: {e}")
        return sorted(jobs, key=lambda job: job.created_at, reverse=reverse)

    def delete(self, job: Job, delete_result_dir: bool = True) -> bool:
        """Delete a job and its result directory."""
        deleted = False
        job_file = job.get_job_file(self.base_dir)
        with self._with_lock(job_file):
            if job_file.exists():
                job_file.unlink()
                deleted = True

            # Delete result directory if exists
            result_dir = job.get_result_dir(self.base_dir)
            if delete_result_dir and result_dir.exists():
                shutil.rmtree(result_dir)
        return deleted


class Worker(ABC):
    def __init__(self, queue: JobQueue, stop_when_done: bool = False) -> None:
        """Initialize worker with a job queue."""
        self.queue = queue
        self.stop_when_done = stop_when_done

    @abstractmethod
    def process_job(self, job: Job, result_dir: Path) -> Mapping[str, Any]:
        """
        Process a single job. This method should be implemented by the user.

        Args:
            job (Job): Job to process
            result_dir (Path): Directory to store results

        Returns:
            Mapping[str, Any]: Results of the job
        """
        raise NotImplementedError("Worker.process_job must be implemented")

    def run(self, poll_interval: float = 1.0) -> None:
        """
        Main worker loop.

        Args:
            poll_interval: Time to wait between checking for new jobs (seconds)
        """
        no_jobs_count = 0
        while True:
            try:
                job = self.queue.pop_next()
                if job:
                    result_dir = self.queue.get_result_dir(job)
                    result_dir.mkdir(parents=True, exist_ok=True)

                    try:
                        results = self.process_job(job, result_dir)
                        self.queue.complete(job)

                        with open(result_dir / RESULTS_FILE, "w") as f:
                            json.dump(results, f)

                    except Exception as e:
                        error = traceback.format_exc()
                        logger.error(f"Error processing job {job.id}: {e}")
                        logger.debug(error)
                        self.queue.fail(job, str(e), error)
                else:
                    no_jobs_count += 1
                    if self.stop_when_done and no_jobs_count > 4:
                        logger.error("No jobs found. Stopping worker.")
                        break

            except Exception as e:
                logger.error(f"Critical worker error: {e}")
                logger.debug(traceback.format_exc())
                # Continue running despite errors

            time.sleep(poll_interval)
