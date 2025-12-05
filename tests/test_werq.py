"""Tests for the werq package.

This module contains integration tests for the werq job queue system.
Tests cover the complete job lifecycle including:
- Job submission and retrieval
- State transitions
- Worker processing
- Error handling
- Concurrent access
"""

import json
import shutil
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest

from werq.exceptions import JobStateError
from werq.queue import Job, JobQueue, JobState, Worker


@pytest.fixture
def temp_dir(tmp_path):
    """Create a temporary directory for jobs."""
    jobs_dir = tmp_path / "jobs"
    yield jobs_dir
    # Cleanup
    if jobs_dir.exists():
        shutil.rmtree(jobs_dir)


@pytest.fixture
def queue(temp_dir) -> JobQueue:
    """Create a JobQueue instance with temporary directory."""
    return JobQueue(temp_dir)


def test_job_submission(queue: JobQueue):
    """Test basic job submission functionality.

    Tests that:
    - Job file is created in the correct location
    - Job data is properly serialized
    - Job attributes are correctly set
    """
    params = {"test_param": "value"}
    job = queue.submit(params)

    # Check job file exists
    job_file = queue.base_dir / JobState.QUEUED.value / f"{job.id}.json"
    assert job_file.exists()
    assert job.get_job_file(queue.base_dir) == job_file

    # Check job data
    job_data = json.loads(job_file.read_text())
    assert job_data["id"] == job.id
    assert job_data["state"] == JobState.QUEUED.value
    assert job_data["params"] == params
    assert "created_at" in job_data


def test_list_jobs_empty(queue: JobQueue):
    """Test lising jobs with an empty queue."""
    jobs = queue.list_jobs()
    assert isinstance(jobs, list)
    assert len(jobs) == 0


def test_list_jobs(queue: JobQueue):
    """Test listing jobs with multiple jobs."""
    # Submit jobs
    job1 = queue.submit({"test": 1})
    job2 = queue.submit({"test": 2})

    jobs = queue.list_jobs()
    assert len(jobs) == 2
    assert jobs[0].id == job1.id and jobs[1].id == job2.id

    # Get one job running
    queue.pop_next()

    jobs = queue.list_jobs()
    assert len(jobs) == 2
    assert jobs[0].state == JobState.RUNNING.value and jobs[1].state == JobState.QUEUED.value


def test_job_lifecycle(queue: JobQueue):
    """Test complete job lifecycle."""
    # Submit job
    job_test = queue.submit({"test": "lifecycle"})

    # Verify queued state
    job_out = queue.get_job(job_test.id)
    assert job_out is not None
    assert job_out.id == job_test.id and job_out.state == JobState.QUEUED.value

    # Start job
    job_running = queue.pop_next()
    assert job_running is not None
    assert job_running.id == job_test.id
    assert job_running.state == JobState.RUNNING.value
    assert job_running.params == job_test.params

    # Verify running state
    job_out = queue.get_job(job_test.id)
    assert job_out is not None
    assert job_out.id == job_test.id and job_out.state == JobState.RUNNING.value

    # Update progress
    queue.update_progress(job_out, 0.5)
    job_out = queue.get_job(job_test.id)
    assert job_out is not None
    assert job_out.progress == 0.5

    # Complete job
    queue.complete(job_out)

    # Verify completed state
    job_out = queue.get_job(job_test.id)
    assert job_out is not None
    assert job_out.id == job_test.id and job_out.state == JobState.COMPLETED.value
    assert job_out.progress == 1.0
    assert job_out.finished_at is not None


def test_failed_job(queue: JobQueue):
    """Test job failure handling."""
    job_test = queue.submit({"test": "failure"})
    job_out = queue.pop_next()  # Start job
    assert job_out is not None

    error_msg = "Test error message"
    error_traceback = "Traceback (most recent call last) ..."
    queue.fail(job_out, error_msg, error_traceback)

    job_out = queue.get_job(job_test.id)
    assert job_out is not None
    assert job_out.state == JobState.FAILED.value
    assert job_out.error == error_msg

    # Check error traceback
    # error_file = job_out.get_error_file(queue.base_dir)
    # assert error_file.exists()
    # assert error_file.read_text() == f"{error_msg}\n\n{error_traceback}"


def test_concurrent_job_processing(queue: JobQueue):
    """Test that jobs are processed one at a time."""
    jobs = [queue.submit({"test": i}) for i in range(3)]
    assert len(queue.list_jobs()) == 3

    # Get first job
    job_out = queue.pop_next()
    assert job_out is not None
    assert job_out.id == jobs[0].id

    # Try to get progress of non-running job
    with pytest.raises(JobStateError):
        queue.update_progress(jobs[1], 0.5)

    # Complete first job and get next
    queue.complete(job_out)
    job_out = queue.pop_next()
    assert job_out is not None
    assert job_out.id == jobs[1].id


class TestWorker(Worker):
    """Test worker implementation for testing job processing.

    This worker implementation tracks processed jobs and can be configured
    to fail on demand for testing error handling.

    Attributes:
        should_fail: If True, the worker will raise an error when processing jobs
        processed_jobs: List of job IDs that have been processed
    """

    def __init__(self, queue: JobQueue, should_fail: bool = False):
        """Initialize the test worker.

        Args:
            queue: The job queue to process jobs from
            should_fail: If True, process_job will raise a ValueError
        """
        super().__init__(queue, stop_when_done=True)
        self.should_fail = should_fail
        self.processed_jobs = []

    def process_job(self, job: Job, *, result_dir: Path) -> Mapping[str, Any]:
        """Process a single job.

        Args:
            job: The job to process
            result_dir: Directory where job results should be stored

        Returns:
            Mapping[str, Any]: The job parameters as results

        Raises:
            ValueError: If should_fail is True
        """
        self.processed_jobs.append(job.id)
        if self.should_fail:
            raise ValueError("Test failure")

        # Simulate some work
        for i in range(3):
            self.queue.update_progress(job, (i + 1) * 0.33)
            time.sleep(0.1)

        return job.params


def test_worker_processing(queue: JobQueue):
    """Test worker job processing."""
    # Submit test job
    job = queue.submit({"test": "worker"})

    # Create and run worker for a short time
    worker = TestWorker(queue)
    worker.run(poll_interval=0.1)  # This will run forever, so we'll need to handle that

    # Verify job was completed
    job_out = queue.get_job(job.id)
    assert job_out is not None
    assert job_out.state == JobState.COMPLETED.value

    # Check results
    result_path = queue.get_result_dir(job)
    assert result_path.exists()
    assert (result_path / "results.json").exists()


def test_worker_failure(queue: JobQueue):
    """Test worker handling of job failures."""
    job = queue.submit({"test": "failure"})

    worker = TestWorker(queue, should_fail=True)
    worker.run(poll_interval=0.1)  # This will run forever

    job_out = queue.get_job(job.id)
    assert job_out is not None
    assert job_out.state == JobState.FAILED.value
    assert job_out.error and "Test failure" in job_out.error


def test_job_duration(queue: JobQueue):
    """Test job duration calculation in different states."""
    job = queue.submit({"test": "duration"})
    assert job.duration is None  # Queued job has no duration

    job = queue.pop_next()
    assert job is not None
    assert job.duration is not None  # Running job has duration
    assert job.duration >= 0

    queue.complete(job)
    job = queue.get_job(job.id)
    assert job is not None
    assert job.duration is not None  # Completed job has duration
    assert job.duration >= 0

    # Test failed job duration
    job2 = queue.submit({"test": "duration2"})
    job2 = queue.pop_next()
    assert job2 is not None
    queue.fail(job2, "Test error", "Test traceback")
    job2 = queue.get_job(job2.id)
    assert job2 is not None
    assert job2.duration is not None
    assert job2.duration >= 0


def test_invalid_state_transitions(queue: JobQueue):
    """Test invalid job state transitions."""
    job = queue.submit({"test": "state"})

    # Can't complete a queued job
    with pytest.raises(JobStateError):
        queue.complete(job)

    # Can't fail a queued job
    with pytest.raises(JobStateError):
        queue.fail(job, "error", "traceback")

    # Can't update progress of queued job
    with pytest.raises(JobStateError):
        queue.update_progress(job, 0.5)

    # Start the job
    job = queue.pop_next()
    assert job is not None

    # Can't start a running job
    with pytest.raises(JobStateError):
        job.start()

    # Complete the job
    queue.complete(job)

    # Can't update completed job
    with pytest.raises(JobStateError):
        queue.update_progress(job, 0.5)


def test_list_jobs_filtering(queue: JobQueue):
    """Test job listing with filters and ordering."""
    # Create jobs in different states
    job1 = queue.submit({"test": 1})
    job2 = queue.submit({"test": 2})
    job3 = queue.submit({"test": 3})

    assert job1 is not None and job2 is not None and job3 is not None

    # Get one running and one completed
    running_job = queue.pop_next()
    assert running_job is not None
    queue.complete(running_job)
    queue.pop_next()

    # Test filtering by state
    queued_jobs = queue.list_jobs(JobState.QUEUED)
    assert len(queued_jobs) == 1
    assert queued_jobs[0].id == job3.id

    completed_jobs = queue.list_jobs(JobState.COMPLETED)
    assert len(completed_jobs) == 1
    assert completed_jobs[0].id == job1.id

    # Test multiple states
    multi_state = queue.list_jobs(JobState.RUNNING, JobState.COMPLETED)
    assert len(multi_state) == 2

    # Test reverse ordering
    all_jobs_reverse = queue.list_jobs(reverse=True)
    assert len(all_jobs_reverse) == 3
    assert all_jobs_reverse[0].id == job3.id

    # Test with filter function
    filtered = queue.list_jobs(filter_func=lambda j: j.params["test"] > 1)
    assert len(filtered) == 2
    assert all(j.params["test"] > 1 for j in filtered)


def test_delete_job(queue: JobQueue):
    """Test job deletion functionality."""
    job = queue.submit({"test": "delete"})

    # Create some result data
    job = queue.pop_next()
    assert job is not None
    queue.complete(job, {"result": "test"})

    # Verify job exists
    assert queue.get_job(job.id) is not None
    assert job.get_result_dir(queue.base_dir).exists()

    # Delete without result dir
    assert queue.delete(job, delete_result_dir=False)
    assert queue.get_job(job.id) is None
    assert job.get_result_dir(queue.base_dir).exists()

    # Delete non-existent job
    assert not queue.delete(job)

    # Create new job and delete with result dir
    job = queue.submit({"test": "delete2"})
    job = queue.pop_next()
    assert job is not None
    queue.complete(job, {"result": "test"})

    assert queue.delete(job, delete_result_dir=True)
    assert queue.get_job(job.id) is None
    assert not job.get_result_dir(queue.base_dir).exists()


def test_lock_file_cleanup(queue: JobQueue, tmp_path: Path):
    """Test that lock files are properly cleaned up."""
    # Lock file should be created and removed
    with queue._with_lock(tmp_path) as lock:
        print(lock.lock_file)
        lock_path = tmp_path.with_suffix(".lock")
        print(lock_path)
        assert str(lock_path) == lock.lock_file
        assert lock_path.is_file()

    assert not lock_path.exists()


def test_resubmit_job(queue: JobQueue):
    """Test resubmitting an existing job."""
    # Submit and complete original job
    original = queue.submit({"test": "resubmit", "name": "original"})
    original = queue.pop_next()
    assert original is not None
    queue.complete(original)

    # Resubmit without new name
    new_job = queue.resubmit(original.id)
    assert new_job.id != original.id
    assert new_job.params["test"] == "resubmit"
    assert new_job.params["name"] == "original"
    assert new_job.state == JobState.QUEUED

    # Resubmit with new name
    new_job2 = queue.resubmit(original.id, name="renamed")
    assert new_job2.params["name"] == "renamed"


def test_resubmit_nonexistent_job(queue: JobQueue):
    """Test resubmitting a job that doesn't exist."""
    from werq.queue import JobID

    with pytest.raises(ValueError):
        queue.resubmit(JobID("nonexistent"))


def test_results_dir_env_var(tmp_path, monkeypatch):
    """Test WERQ_RESULTS_DIR environment variable."""
    monkeypatch.setenv("WERQ_RESULTS_DIR", "custom_results")
    queue = JobQueue(tmp_path / "jobs")

    job = queue.submit({"test": "env"})
    result_dir = job.get_result_dir(queue.base_dir)
    assert "custom_results" in str(result_dir)


def test_worker_name_tracking(queue: JobQueue):
    """Test that worker name is recorded on jobs."""
    job = queue.submit({"test": "worker_name"})
    job = queue.pop_next(worker_name="test-worker")
    assert job is not None

    assert job.worker_name == "test-worker"

    # Verify persisted
    job = queue.get_job(job.id)
    assert job is not None
    assert job.worker_name == "test-worker"


def test_load_result(queue: JobQueue):
    """Test loading job results."""
    job = queue.submit({"test": "results"})
    job = queue.pop_next()
    assert job is not None
    queue.complete(job, {"output": "test data"})

    job = queue.get_job(job.id)
    assert job is not None
    result = job.load_result(queue.base_dir)
    assert result["output"] == "test data"


def test_error_file_written(queue: JobQueue):
    """Test that error file is written on failure."""
    job = queue.submit({"test": "error"})
    job = queue.pop_next()
    assert job is not None
    queue.fail(job, "Error message", "Full traceback")

    error_file = job.get_error_file(queue.base_dir)
    assert error_file.exists()
    content = error_file.read_text()
    assert "Error message" in content
    assert "Full traceback" in content
