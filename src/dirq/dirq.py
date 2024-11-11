import atexit
import json
import shutil
import time
import traceback
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Generic, NewType, Optional, TypeVar

from filelock import BaseFileLock, FileLock

from .exceptions import JobStateError, JobValidationError


class JobState(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


RESULT_DIR = Path("completed")

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

    # State transitions
    def start(self) -> None:
        if self.state != JobState.QUEUED:
            raise JobStateError(f"Cannot start job {self.id} in state {self.state}")
        self.state = JobState.RUNNING
        self.started_at = datetime.now()

    def complete(self) -> None:
        if self.state != JobState.RUNNING:
            raise JobStateError(f"Cannot complete job {self.id} in state {self.state}")
        self.update_progress(100.0)
        self.state = JobState.COMPLETED
        self.finished_at = datetime.now()

    def fail(self, err: str) -> None:
        if self.state != JobState.RUNNING:
            raise JobStateError(f"Cannot fail job {self.id} in state {self.state}")
        self.state = JobState.FAILED
        self.finished_at = datetime.now()
        self.error = err

    # Progress tracking
    def update_progress(self, progress: float) -> None:
        if self.state != JobState.RUNNING:
            raise JobStateError(f"Cannot update progress for job {self.id} in state {self.state}")
        self.progress = progress

    # Path handling
    def get_job_file(self, base_dir: Path) -> Path:
        return base_dir / self.state.value / f"{self.id}.json"

    def get_result_dir(self, base_dir: Path) -> Path:
        return base_dir / RESULT_DIR / self.id


T = TypeVar("T")


class LockedFile(Generic[T]):
    """Helper class for handling file operations with locking."""

    _active_locks: list[BaseFileLock] = []  # Class variable to track all locks

    @classmethod
    def cleanup_locks(cls) -> None:
        """Clean up all lock files."""
        for lock in cls._active_locks:
            try:
                if lock.is_locked:
                    lock.release()
                if Path(lock.lock_file).exists():
                    Path(lock.lock_file).unlink()
            except Exception as e:
                print(f"Error cleaning up lock {lock.lock_file}: {e}")

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.lock_path = Path(str(path) + ".lock")
        self.lock = FileLock(self.lock_path)
        LockedFile._active_locks.append(self.lock)

    @contextmanager
    def open_locked(self, mode: str = "r"):
        """Context manager for file operations with locking."""
        try:
            with self.lock:
                with open(self.path, mode) as f:
                    yield f
        finally:
            if self.lock.is_locked:
                self.lock.release()
            if self.lock_path.exists():
                try:
                    self.lock_path.unlink()
                except Exception:
                    pass

    def read_json(self) -> T:
        """Read and parse JSON file with locking."""
        try:
            with self.open_locked("r") as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            raise JobValidationError(f"Invalid JSON in {self.path}: {e}") from e

    def write_json(self, data: T) -> None:
        """Write JSON data with locking."""
        with self.open_locked("w") as f:
            json.dump(data, f, indent=2)

    def move_to(self, new_path: Path) -> None:
        """Move file to new location with locking."""
        try:
            with self.lock:
                shutil.move(str(self.path), str(new_path))
        finally:
            if self.lock.is_locked:
                self.lock.release()
            if self.lock_path.exists():
                try:
                    self.lock_path.unlink()
                except Exception:
                    pass

    def remove(self) -> None:
        """Remove file with locking."""
        try:
            with self.lock:
                self.path.unlink(missing_ok=True)
        finally:
            if self.lock.is_locked:
                self.lock.release()
            if self.lock_path.exists():
                try:
                    self.lock_path.unlink()
                except Exception:
                    pass


# Register cleanup on exit
atexit.register(LockedFile.cleanup_locks)


class JobQueue:
    def __init__(self, base_dir: Path) -> None:
        """
        Initialize job queue with base directory.
        """
        self.base_dir = Path(base_dir)
        self._ensure_directories()
        self._cleanup_stale_locks()

    def _ensure_directories(self) -> None:
        """Create necessary directory structure."""
        for dir_name in [RESULT_DIR] + [status.value for status in JobState]:
            (self.base_dir / dir_name).mkdir(parents=True, exist_ok=True)

    def _cleanup_stale_locks(self) -> None:
        """Clean up any stale lock files."""
        # TODO: Might no be necessary with FileLock improvements
        for lock_file in self.base_dir.rglob("*.lock"):
            try:
                lock_file.unlink()
            except Exception:
                pass

    def _generate_job_id(self) -> JobID:
        """Generate a unique job ID."""
        return JobID(f"job_{int(time.time() * 1000)}")

    def submit(self, params: dict[str, Any]) -> JobID:
        """Submit a new job to the queue."""
        job_id = self._generate_job_id()

        job = Job(id=job_id, params=params)

        job_file = LockedFile(job.get_job_file(self.base_dir))
        job_file.write_json(job.to_dict())

        return job.id

    def pop_next(self) -> Optional[Job]:
        """Pop the next job from the queue."""
        queue_dir = self.base_dir / JobState.QUEUED.value
        jobs = sorted(queue_dir.glob("*.json"))

        if not jobs:
            return None

        # Get the first job from the queue
        job_file = LockedFile(jobs[0])
        try:
            job = Job.from_dict(job_file.read_json())
        except Exception as e:
            print(f"Error reading job file {jobs[0]}: {e}")
            job_file.move_to(self.base_dir / JobState.FAILED.value / jobs[0].name)
            return None

        try:
            # Start the job - change state to running
            job.start()
            # Remove the job from the queue
            job_file.remove()

            # Move the job to the running directory
            job_file = LockedFile(job.get_job_file(self.base_dir))
            job_file.write_json(job.to_dict())
        except Exception as e:
            print(f"Error starting job {job.id}: {e}")
            self.fail(job, f"Error starting job: {str(e)}")
            return None

        return job

    def complete(self, job: Job) -> None:
        """Mark job as completed."""
        job_file = LockedFile(job.get_job_file(self.base_dir))

        job.complete()
        # Remove the job from the running directory
        job_file.remove()

        job_file = LockedFile(job.get_job_file(self.base_dir))
        job_file.write_json(job.to_dict())

    def fail(self, job: Job, error: str) -> None:
        """Mark job as failed."""
        job_file = LockedFile(job.get_job_file(self.base_dir))
        job.fail(error)
        # Remove the job from the running directory
        job_file.remove()
        job_file = LockedFile(job.get_job_file(self.base_dir))
        job_file.write_json(job.to_dict())

    def get_result_dir(self, job: Job) -> Path:
        """Get the result directory for a job."""
        return job.get_result_dir(self.base_dir)

    def update_progress(self, job: Job, progress: float) -> None:
        """Update job progress."""
        job_file = LockedFile(job.get_job_file(self.base_dir))
        job.update_progress(progress)
        job_file.write_json(job.to_dict())

    def get_job(self, job_id: JobID) -> Optional[Job]:
        """Get a job by ID."""
        for state in JobState:
            job_file = self.base_dir / state.value / f"{job_id}.json"
            if job_file.exists():
                try:
                    return Job.from_dict(json.loads(job_file.read_text()))
                except Exception as e:
                    print(f"Error reading job {job_id}: {e}")
        return None

    def list_jobs(self, *states: JobState) -> list[Job]:
        """List all jobs in the queue."""
        jobs = []
        states = states or tuple(JobState)
        for state in states:
            for job_file in (self.base_dir / state.value).glob("*.json"):
                try:
                    jobs.append(Job.from_dict(json.loads(job_file.read_text())))
                except Exception as e:
                    print(f"Error reading job {job_file}: {e}")
        return jobs


class Worker(ABC):
    def __init__(self, queue: JobQueue, stop_when_done: bool = False) -> None:
        """Initialize worker with a job queue."""
        self.queue = queue
        self.stop_when_done = stop_when_done

    @abstractmethod
    def process_job(self, job: Job, result_dir: Path) -> None:
        """
        Process a single job. This method should be implemented by the user.

        Args:
            job: Job object
            result_dir: Directory where results should be saved
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
                        self.process_job(job, result_dir)
                        self.queue.complete(job)
                    except Exception as e:
                        self.queue.fail(job, str(e))
                else:
                    no_jobs_count += 1
                    if self.stop_when_done and no_jobs_count > 4:
                        print("No jobs found. Stopping worker.")
                        break

            except Exception as e:
                print(f"Critical worker error: {e}")
                print(traceback.format_exc())
                # Continue running despite errors

            time.sleep(poll_interval)
