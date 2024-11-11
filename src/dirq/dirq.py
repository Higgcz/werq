import atexit
import json
import shutil
import time
import traceback
from contextlib import contextmanager
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Generic, List, Optional, Tuple, TypeVar

import pandas as pd
from filelock import FileLock

from .exceptions import JobNotFoundException, JobStateError, JobValidationError


class JobStatus(Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


T = TypeVar("T")


class LockedFile(Generic[T]):
    """Helper class for handling file operations with locking."""

    _active_locks: List[FileLock] = []  # Class variable to track all locks

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
            raise JobValidationError(f"Invalid JSON in {self.path}: {e}")

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
        """Initialize job queue with base directory."""
        self.base_dir = Path(base_dir)
        self._ensure_directories()
        self._cleanup_stale_locks()

    def _ensure_directories(self) -> None:
        """Create necessary directory structure."""
        for dir_name in ["queue", "running", "completed", "failed"]:
            (self.base_dir / dir_name).mkdir(parents=True, exist_ok=True)

    def _cleanup_stale_locks(self) -> None:
        """Clean up any stale lock files."""
        for lock_file in self.base_dir.rglob("*.lock"):
            try:
                lock_file.unlink()
            except Exception:
                pass

    def _generate_job_id(self) -> str:
        """Generate a unique job ID."""
        return f"job_{int(time.time() * 1000)}"

    def _move_to_failed(self, job_id: str, error_msg: str, source_dir: Optional[Path] = None) -> None:
        """Move a job to the failed directory with error information."""
        failed_dir = self.base_dir / "failed" / job_id
        failed_dir.mkdir(parents=True, exist_ok=True)

        # If we know the source directory, move its contents
        if source_dir and source_dir.exists():
            for item in source_dir.iterdir():
                if not item.name.endswith(".lock"):
                    shutil.move(str(item), str(failed_dir / item.name))

        # Write or update status file
        status_file = LockedFile(failed_dir / "status.json")
        try:
            if status_file.path.exists():
                job_data = status_file.read_json()
            else:
                job_data = {"id": job_id}

            job_data.update(
                {"status": JobStatus.FAILED.value, "error": error_msg, "failed_at": datetime.now().isoformat()}
            )

            status_file.write_json(job_data)
        except Exception as e:
            print(f"Error updating failed job status: {e}")

    def submit_job(self, params: Dict[str, Any]) -> str:
        """Submit a new job to the queue."""
        job_id = self._generate_job_id()
        job_file = LockedFile(self.base_dir / "queue" / f"{job_id}.json")

        job_data = {
            "id": job_id,
            "status": JobStatus.QUEUED.value,
            "created_at": datetime.now().isoformat(),
            "params": params,
        }

        try:
            job_file.write_json(job_data)
            return job_id
        except Exception as e:
            self._move_to_failed(job_id, f"Failed to submit job: {str(e)}")
            raise JobValidationError(f"Failed to submit job: {str(e)}") from e

    def get_jobs_df(self) -> pd.DataFrame:
        """Get all jobs as a DataFrame."""
        jobs = []

        # Helper to safely read job data
        def safe_read_job(file_path: Path, status_override: Optional[str] = None) -> None:
            try:
                job_file = LockedFile(file_path)
                data = job_file.read_json()
                if status_override:
                    data["status"] = status_override
                jobs.append(data)
            except Exception as e:
                print(f"Error reading job file {file_path}: {e}")

        # Get jobs from each directory
        for job_path in (self.base_dir / "queue").glob("*.json"):
            if not job_path.name.endswith(".lock"):
                safe_read_job(job_path)

        for status_dir in ["running", "completed", "failed"]:
            for job_dir in (self.base_dir / status_dir).iterdir():
                status_file = job_dir / "status.json"
                if status_file.exists():
                    safe_read_job(status_file)

        if not jobs:
            return pd.DataFrame(
                columns=pd.Index(
                    [
                        "id",
                        "status",
                        "created_at",
                        "started_at",
                        "completed_at",
                        "failed_at",
                        "progress",
                        "error",
                        "params",
                    ]
                )
            )

        df = pd.DataFrame(jobs)

        # Convert timestamp columns to datetime
        for col in ["created_at", "started_at", "completed_at", "failed_at"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])

        return df

    def get_next_job(self) -> Optional[Tuple[str, Dict[str, Any]]]:
        """Get next job from queue. Returns (job_id, params) or None if queue is empty."""
        queue_dir = self.base_dir / "queue"

        # Get first job file (if any)
        job_files = [f for f in queue_dir.glob("*.json") if not f.name.endswith(".lock")]
        if not job_files:
            return None

        job_file = LockedFile(job_files[0])

        try:
            job_data = job_file.read_json()

            # Move job to running directory
            job_id = job_data["id"]
            running_dir = self.base_dir / "running" / job_id
            running_dir.mkdir(parents=True, exist_ok=True)

            # Update status
            job_data["status"] = JobStatus.RUNNING.value
            job_data["started_at"] = datetime.now().isoformat()

            # Write status to running directory
            status_file = LockedFile(running_dir / "status.json")
            status_file.write_json(job_data)

            # Remove from queue
            job_file.remove()

            return job_id, job_data["params"]

        except Exception as e:
            # Move to failed if we can get the job ID
            try:
                job_id = job_data["id"]
                self._move_to_failed(job_id, f"Error starting job: {str(e)}")
            except Exception:
                pass

            # Clean up
            try:
                job_file.remove()
            except Exception:
                pass

            raise JobStateError(f"Error getting next job: {str(e)}") from e

    def update_progress(self, job_id: str, progress: float) -> None:
        """Update job progress (0-100)."""
        status_file = LockedFile(self.base_dir / "running" / job_id / "status.json")
        if not status_file.path.exists():
            raise JobNotFoundException(f"Job {job_id} not found or not running")

        try:
            job_data = status_file.read_json()
            job_data["progress"] = progress
            status_file.write_json(job_data)
        except Exception as e:
            print(f"Error updating progress for job {job_id}: {e}")

    def complete_job(self, job_id: str, success: bool, error: Optional[str] = None) -> None:
        """Mark job as completed or failed."""
        running_dir = self.base_dir / "running" / job_id
        if not running_dir.exists():
            raise JobNotFoundException(f"Job {job_id} not found or not running")

        try:
            status_file = LockedFile(running_dir / "status.json")
            job_data = status_file.read_json()

            if success:
                job_data["status"] = JobStatus.COMPLETED.value
                job_data["completed_at"] = datetime.now().isoformat()
                target_dir = self.base_dir / "completed" / job_id
            else:
                job_data["status"] = JobStatus.FAILED.value
                job_data["failed_at"] = datetime.now().isoformat()
                if error:
                    job_data["error"] = error
                target_dir = self.base_dir / "failed" / job_id

            # Move entire directory
            target_dir.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(running_dir), str(target_dir))

            # Update status file in new location
            new_status_file = LockedFile(target_dir / "status.json")
            new_status_file.write_json(job_data)

        except Exception as e:
            error_msg = f"Error completing job: {str(e)}\n{traceback.format_exc()}"
            self._move_to_failed(job_id, error_msg, running_dir)
            raise JobStateError(f"Error completing job: {str(e)}") from e

    def get_result_path(self, job_id: str) -> Optional[Path]:
        """Get path to job results directory if they exist."""
        # Check both completed and failed directories
        for status in ["completed", "failed"]:
            job_dir = self.base_dir / status / job_id
            if job_dir.exists():
                return job_dir / "results"
        return None


class Worker:
    def __init__(self, queue: JobQueue) -> None:
        """Initialize worker with a job queue."""
        self.queue = queue

    def process_job(self, job_id: str, params: Dict[str, Any], result_dir: Path) -> None:
        """
        Process a single job. This method should be implemented by the user.

        Args:
            job_id: Unique identifier of the job
            params: Job parameters
            result_dir: Directory where results should be saved
        """
        raise NotImplementedError("Worker.process_job must be implemented")

    def run(self, poll_interval: float = 1.0) -> None:
        """
        Main worker loop.

        Args:
            poll_interval: Time to wait between checking for new jobs (seconds)
        """
        while True:
            try:
                job = self.queue.get_next_job()
                if job:
                    job_id, params = job
                    result_dir = self.queue.base_dir / "running" / job_id / "results"
                    result_dir.mkdir(parents=True, exist_ok=True)

                    try:
                        self.process_job(job_id, params, result_dir)
                        self.queue.complete_job(job_id, success=True)
                    except Exception as e:
                        error_msg = f"Error processing job: {str(e)}\n{traceback.format_exc()}"
                        self.queue.complete_job(job_id, success=False, error=error_msg)
            except Exception as e:
                print(f"Critical worker error: {e}")
                print(traceback.format_exc())
                # Continue running despite errors

            time.sleep(poll_interval)
