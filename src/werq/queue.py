"""Directory-based job queue implementation.

This module provides a simple job queue system that uses the filesystem for storage.
Jobs progress through different states (queued, running, completed, failed) and
results are stored in a directory structure.

Key classes:
- Job: Represents a single job with its parameters and state
- JobQueue: Manages the queue of jobs and their state transitions
- Worker: Abstract base class for implementing job processors
"""

import json
import logging
import os
import shutil
import time
import traceback
from abc import ABC, abstractmethod
from collections.abc import Callable, Generator, Mapping
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
    """Enumeration of possible job states.

    Attributes:
        QUEUED: Job is waiting to be processed
        RUNNING: Job is currently being processed
        COMPLETED: Job has finished successfully
        FAILED: Job has failed during processing
    """

    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


RESULT_DIR_DEFAULT = "completed_results"
RESULTS_FILE = "results.json"
ERROR_FILE = "error.txt"


def get_result_dir_name() -> str:
    """Get the result directory name from environment or use default.

    Returns:
        str: The result directory name (from WERQ_RESULTS_DIR env var or default)
    """
    return os.environ.get("WERQ_RESULTS_DIR", RESULT_DIR_DEFAULT)


JobID = NewType("JobID", str)
PathLike = str | Path


@dataclass
class Job:
    """Represents a job in the queue system.

    A job progresses through different states (queued -> running -> completed/failed)
    and maintains metadata about its execution including timing and progress.

    Attributes:
        id: Unique identifier for the job
        params: Dictionary of parameters for the job
        state: Current state of the job (queued, running, completed, failed)
        created_at: Timestamp when the job was created
        started_at: Timestamp when the job started running (None if not started)
        finished_at: Timestamp when the job finished (None if not finished)
        error: Error message if the job failed (None if not failed)
        progress: Float between 0 and 1 indicating job progress
    """

    id: JobID
    params: dict[str, Any]
    state: JobState = JobState.QUEUED
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    error: Optional[str] = None
    worker_name: Optional[str] = None
    progress: float = 0.0

    # Serialization
    def to_dict(self) -> dict[str, Any]:
        """Convert the job to a dictionary for serialization.

        Returns:
            Dictionary representation of the job
        """
        return {
            "id": self.id,
            "state": self.state.value,
            "params": self.params,
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "error": self.error,
            "worker_name": self.worker_name,
            "progress": self.progress,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Job":
        """Create a Job instance from a dictionary.

        Args:
            data: Dictionary containing job data

        Returns:
            New Job instance
        """
        return cls(
            id=JobID(data["id"]),
            state=JobState(data["state"]),
            params=data["params"],
            created_at=datetime.fromisoformat(data["created_at"]),
            started_at=datetime.fromisoformat(data["started_at"]) if data["started_at"] else None,
            finished_at=datetime.fromisoformat(data["finished_at"]) if data["finished_at"] else None,
            error=data.get("error"),
            worker_name=data.get("worker_name"),
            progress=data.get("progress", 0.0),
        )

    def __str__(self) -> str:
        """Return a string representation of the job.

        Returns:
            str: String in format 'Job <id> (<state>)'
        """
        return f"Job {self.id} ({self.state})"

    @property
    def duration(self) -> Optional[float]:
        """Get the duration of the job in minutes.

        Returns:
            float: Duration in minutes if the job has started, None otherwise
        """
        match self.state:
            case JobState.RUNNING:
                if self.started_at:
                    return (datetime.now() - self.started_at).total_seconds() / 60.0
                return None
            case JobState.COMPLETED | JobState.FAILED:
                if self.started_at and self.finished_at:
                    return (self.finished_at - self.started_at).total_seconds() / 60.0
                return None
            case _:
                return None

    # State transitions
    def start(self, worker_name: Optional[str] = None) -> None:
        """Start the job by transitioning it to the running state.

        Args:
            worker_name: Optional name of the worker processing this job

        Raises:
            JobStateError: If the job is not in the queued state
        """
        if self.state != JobState.QUEUED:
            raise JobStateError(f"Cannot start job {self.id} in state {self.state}")
        self.state = JobState.RUNNING
        self.started_at = datetime.now()
        self.worker_name = worker_name

    def complete(self) -> None:
        """Mark the job as completed.

        Sets progress to 100% and records completion timestamp.

        Raises:
            JobStateError: If the job is not in the running state
        """
        if self.state != JobState.RUNNING:
            raise JobStateError(f"Cannot complete job {self.id} in state {self.state}")
        self.update_progress(1.0)
        self.state = JobState.COMPLETED
        self.finished_at = datetime.now()

    def fail(self, err: str) -> None:
        """Mark the job as failed with an error message.

        Args:
            err: Error message describing the failure

        Raises:
            JobStateError: If the job is not in the running state
        """
        if self.state != JobState.RUNNING:
            raise JobStateError(f"Cannot fail job {self.id} in state {self.state}")
        self.state = JobState.FAILED
        self.finished_at = datetime.now()
        self.error = err

    def update_progress(self, progress: float) -> None:
        """Update the job's progress.

        Args:
            progress: Progress value between 0.0 and 1.0

        Raises:
            JobStateError: If the job is not in the running state
        """
        if self.state != JobState.RUNNING:
            raise JobStateError(f"Cannot update progress for job {self.id} in state {self.state}")
        self.progress = progress

    # Path handling
    def get_job_file(self, base_dir: PathLike) -> Path:
        """Get the JSON job file path for a job.

        Args:
            base_dir: Base directory of the job queue

        Returns:
            Path to the job's JSON file within its state directory
        """
        return Path(base_dir) / self.state.value / f"{self.id}.json"

    def get_result_dir(self, base_dir: PathLike) -> Path:
        """Get the result directory for a job.

        Args:
            base_dir: Base directory of the job queue

        Returns:
            Path to the job's result directory
        """
        return Path(base_dir) / get_result_dir_name() / self.id

    def get_error_file(self, base_dir: PathLike) -> Path:
        """Get the error file path for a job.

        Args:
            base_dir: Base directory of the job queue

        Returns:
            Path to the job's error file within its result directory
        """
        return self.get_result_dir(base_dir) / ERROR_FILE

    def save(self, base_dir: PathLike) -> None:
        """Save job metadata to a JSON file.

        Args:
            base_dir: Base directory of the job queue
        """
        job_file = self.get_job_file(base_dir)
        job_file.write_text(json.dumps(self.to_dict(), indent=2))

    def load_result(self, base_dir: PathLike) -> dict[str, Any]:
        """Load the job's results from its result file.

        Args:
            base_dir: Base directory of the job queue

        Returns:
            Dictionary containing the job's results, or empty dict if no results exist
        """
        result_file = self.get_result_dir(base_dir) / RESULTS_FILE
        if not result_file.exists():
            return {}
        return json.loads(result_file.read_text())


class JobQueue:
    """Manages a directory-based job queue.

    The JobQueue handles job submissions, state transitions, and result storage.
    It uses a directory structure to maintain job states and file locks for
    thread/process safety.

    The directory structure is:
        base_dir/
            queued/            - New jobs waiting to be processed
            running/           - Jobs currently being processed
            completed/         - Successfully completed jobs (metadata only)
            failed/            - Failed jobs (metadata only)
            completed_results/ - Directory containing job results and errors

    The results directory can be customized via the WERQ_RESULTS_DIR environment variable.
    """

    def __init__(self, base_dir: PathLike) -> None:
        """Initialize job queue with base directory.

        Args:
            base_dir: Path to the directory where job files and results will be stored
        """
        self.base_dir = Path(base_dir)
        self._ensure_directories()

    def _ensure_directories(self) -> None:
        """Create necessary directory structure.

        Creates the following directories if they don't exist:
        - queued/: For new jobs
        - running/: For jobs being processed
        - completed/: For finished jobs
        - failed/: For failed jobs
        - completed_results/: For job results (configurable via WERQ_RESULTS_DIR)
        """
        for dir_name in [get_result_dir_name()] + [status.value for status in JobState]:
            (self.base_dir / dir_name).mkdir(parents=True, exist_ok=True)

    def _generate_job_id(self) -> JobID:
        """Generate a unique job ID based on current timestamp.

        The ID is based on the current time in nanoseconds to ensure uniqueness
        across multiple processes.

        Returns:
            JobID: A unique identifier for a new job
        """
        return JobID(str(int(time.time_ns())))

    @contextmanager
    def _with_lock(self, file_path: str | Path) -> Generator[BaseFileLock, None, None]:
        """Get a file lock for a given file path.

        Creates a lock file to ensure thread/process-safe operations on job files.
        The lock is automatically released when exiting the context.

        Args:
            file_path: Path to the file that needs to be locked

        Yields:
            BaseFileLock: A file lock object that can be used in a with statement
        """
        lock_path = Path(file_path).with_suffix(".lock")
        with FileLock(lock_path) as lock_file:
            yield lock_file

        lock_path.unlink(missing_ok=True)

    def submit(self, params: dict[str, Any]) -> Job:
        """Submit a new job to the queue.

        Args:
            params: Dictionary of parameters for the job

        Returns:
            Job: The newly created job
        """
        job_id = self._generate_job_id()

        job = Job(id=job_id, params=params)

        with self._with_lock(job.get_job_file(self.base_dir)):
            job.save(self.base_dir)

        return job

    def resubmit(self, job_id: JobID, name: Optional[str] = None) -> Job:
        """Resubmit an existing job with the same parameters.

        Creates a new job with the same parameters as an existing job,
        optionally with a new name.

        Args:
            job_id: ID of the job to resubmit
            name: Optional new name for the resubmitted job

        Returns:
            Job: The newly created job

        Raises:
            ValueError: If the job is not found
        """
        original_job = self.get_job(job_id)
        if original_job is None:
            raise ValueError(f"Job {job_id} not found")

        # Copy parameters and optionally update name
        params = original_job.params.copy()
        if name is not None:
            params["name"] = name

        return self.submit(params)

    def pop_next(self, worker_name: Optional[str] = None) -> Optional[Job]:
        """Pop the next job from the queue.

        Gets the oldest queued job and transitions it to the running state.
        If there are no jobs in the queue, returns None.

        Args:
            worker_name: Optional name of the worker processing this job

        Returns:
            Job: The next job to process, or None if queue is empty

        Raises:
            JobStateError: If the job cannot be transitioned to running state
        """
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
                job.start(worker_name)
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

    def complete(self, job: Job, result: Optional[Mapping[str, Any]] = None) -> None:
        """Mark job as completed and save its results.

        Args:
            job: Job to mark as completed
            result: Optional dictionary of results to save. If provided, results
                   will be saved to a JSON file in the job's result directory.

        Raises:
            JobStateError: If the job cannot be transitioned to completed state
        """
        job_file = job.get_job_file(self.base_dir)
        with self._with_lock(job_file):
            job.complete()
            # Remove the job from the running directory
            job_file.unlink()
            job.save(self.base_dir)

            # Save results if provided
            if result:
                self.update_result(job, result)

    def fail(self, job: Job, error_msg: str, error_traceback: str) -> None:
        """Mark job as failed and save error information.

        Args:
            job: Job to mark as failed
            error_msg: Short error message describing the failure
            error_traceback: Full error traceback for debugging

        Raises:
            JobStateError: If the job cannot be transitioned to failed state
        """
        job_file = job.get_job_file(self.base_dir)
        with self._with_lock(job_file):
            job.fail(error_msg)
            # Remove the job from the running directory
            job_file.unlink()
            job.save(self.base_dir)

        error_file = job.get_error_file(self.base_dir)
        error_file.parent.mkdir(parents=True, exist_ok=True)
        error_file.write_text(f"{error_msg}\n\n{error_traceback}")

    def get_result_dir(self, job: Job) -> Path:
        """Get the result directory for a job.

        Args:
            job: The job to get the result directory for

        Returns:
            Path: Path to the directory where job results are stored
        """
        return job.get_result_dir(self.base_dir)

    def update_progress(self, job: Job, progress: float) -> None:
        """Update job progress.

        Args:
            job: The job to update
            progress: Progress value between 0.0 and 1.0

        Raises:
            JobStateError: If the job is not in the running state
        """
        job_file = job.get_job_file(self.base_dir)
        with self._with_lock(job_file):
            job.update_progress(progress)
            job.save(self.base_dir)

    def update_result(self, job: Job, result: Mapping[str, Any]) -> None:
        """Update job results by saving them to a JSON file.

        Args:
            job: The job to update results for
            result: Dictionary of results to save
        """
        result_dir = job.get_result_dir(self.base_dir)
        result_dir.mkdir(parents=True, exist_ok=True)
        with open(result_dir / RESULTS_FILE, "w") as f:
            json.dump(result, f)

    def get_job(self, job_id: JobID) -> Optional[Job]:
        """Get a job by its ID.

        Searches through all state directories to find the job.

        Args:
            job_id: ID of the job to find

        Returns:
            Job if found, None otherwise
        """
        for state in JobState:
            job_file = self.base_dir / state.value / f"{job_id}.json"
            if job_file.exists():
                with self._with_lock(job_file):
                    try:
                        return Job.from_dict(json.loads(job_file.read_text()))
                    except Exception as e:
                        logger.error(f"Error reading job {job_id}: {e}")
        return None

    def list_jobs(
        self,
        *states: JobState,
        filter_func: Optional[Callable[[Job], bool]] = None,
        reverse: bool = False,
    ) -> list[Job]:
        """List all jobs in the queue, optionally filtering them based on their parameters.

        Args:
            *states: Job states to include
            filter_func: Function to filter jobs
            reverse: Whether to reverse the order of the jobs

        Returns:
            list[Job]: List of jobs
        """
        jobs = []
        states = states or tuple(JobState)
        for state in states:
            for job_file in (self.base_dir / state.value).glob("*.json"):
                with self._with_lock(job_file):
                    try:
                        job = Job.from_dict(json.loads(job_file.read_text()))
                        if filter_func is None or filter_func(job):
                            jobs.append(job)
                    except Exception as e:
                        logger.error(f"Error reading job {job_file}: {e}")
        return sorted(jobs, key=lambda job: job.created_at, reverse=reverse)

    def delete(self, job: Job, delete_result_dir: bool = True) -> bool:
        """Delete a job and optionally its result directory.

        Args:
            job: The job to delete
            delete_result_dir: If True, also delete the job's result directory

        Returns:
            bool: True if the job was deleted, False if it didn't exist
        """
        deleted = False
        job_file = job.get_job_file(self.base_dir)
        with self._with_lock(job_file):
            if job_file.exists():
                job_file.unlink()
                deleted = True

            # Delete result directory if exists
            result_dir = job.get_result_dir(self.base_dir)
            if delete_result_dir and result_dir.exists():
                shutil.rmtree(result_dir, ignore_errors=True)
        return deleted


class Worker(ABC):
    """Abstract base class for job processing workers.

    Workers handle the actual processing of jobs from the queue. Subclasses must
    implement the process_job method to define how jobs are processed.

    The worker can run continuously or stop when the queue is empty based on
    the stop_when_done parameter.
    """

    def __init__(self, queue: JobQueue, stop_when_done: bool = False, name: Optional[str] = None) -> None:
        """Initialize worker with a job queue.

        Args:
            queue: The job queue to process jobs from
            stop_when_done: If True, worker will stop when queue is empty
            name: Optional name for this worker. If None, will use env var or generate one
        """
        self.queue = queue
        self.stop_when_done = stop_when_done
        self._provided_name = name

    @property
    def name(self) -> str:
        """Get the name of this worker.

        Returns the name in the following order of precedence:
        1. Name provided in constructor
        2. Environment variable WERQ_WORKER_NAME
        3. Generated name using class name and process ID
        """
        import multiprocessing
        import os

        if self._provided_name:
            return self._provided_name

        # Check environment variable
        env_name = os.environ.get("WERQ_WORKER_NAME")
        if env_name:
            return env_name

        # Generate a name based on class name and process ID
        return f"{self.__class__.__name__}-pid{multiprocessing.current_process().pid}"

    @abstractmethod
    def process_job(self, job: Job, *, result_dir: Path) -> Mapping[str, Any]:
        """Process a single job. This method should be implemented by the user.

        Args:
            job (Job): Job to process
            result_dir (Path): Directory to store results

        Returns:
            Mapping[str, Any]: Results of the job
        """
        raise NotImplementedError("Worker.process_job must be implemented")

    def run(self, poll_interval: float = 1.0) -> None:
        """Main worker loop that processes jobs from the queue.

        Continuously polls the queue for new jobs and processes them.
        If stop_when_done is True, exits after queue has been empty for a while.

        Args:
            poll_interval: Time to wait between checking for new jobs (seconds)

        Note:
            This method runs indefinitely unless stop_when_done is True
        """
        no_jobs_count = 0
        while True:
            try:
                job = self.queue.pop_next(worker_name=self.name)
                if job:
                    result_dir = self.queue.get_result_dir(job)
                    result_dir.mkdir(parents=True, exist_ok=True)

                    try:
                        results = self.process_job(job=job, result_dir=result_dir)
                        self.queue.complete(job, results)

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
