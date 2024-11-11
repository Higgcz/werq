import shutil
import time
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import pytest

from dirq.dirq import JobQueue, JobStatus, LockedFile, Worker


@pytest.fixture
def temp_dir(tmp_path):
    """Create a temporary directory for jobs."""
    jobs_dir = tmp_path / "jobs"
    yield jobs_dir
    # Cleanup
    if jobs_dir.exists():
        shutil.rmtree(jobs_dir)


@pytest.fixture
def queue(temp_dir):
    """Create a JobQueue instance with temporary directory."""
    return JobQueue(temp_dir)


def test_job_submission(queue):
    """Test basic job submission."""
    params = {"test_param": "value"}
    job_id = queue.submit_job(params)

    # Check job file exists
    job_file = queue.base_dir / "queue" / f"{job_id}.json"
    assert job_file.exists()

    # Check job data
    job_data = LockedFile(job_file).read_json()
    assert job_data["id"] == job_id
    assert job_data["status"] == JobStatus.QUEUED.value
    assert job_data["params"] == params
    assert "created_at" in job_data


def test_get_jobs_df_empty(queue):
    """Test getting jobs DataFrame when no jobs exist."""
    df = queue.get_jobs_df()
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0
    assert all(
        col in df.columns
        for col in ["id", "status", "created_at", "started_at", "completed_at", "progress", "error", "params"]
    )


def test_get_jobs_df_with_jobs(queue):
    """Test getting jobs DataFrame with various job states."""
    # Submit jobs
    job1 = queue.submit_job({"test": 1})
    job2 = queue.submit_job({"test": 2})

    # Get one job running
    queue.get_next_job()

    df = queue.get_jobs_df()
    assert len(df) == 2
    assert df["status"].value_counts().to_dict() == {JobStatus.QUEUED.value: 1, JobStatus.RUNNING.value: 1}


def test_job_lifecycle(queue):
    """Test complete job lifecycle."""
    # Submit job
    job_id = queue.submit_job({"test": "lifecycle"})

    # Verify queued state
    df = queue.get_jobs_df()
    assert df.loc[df["id"] == job_id, "status"].iloc[0] == JobStatus.QUEUED.value

    # Start job
    got_job_id, params = queue.get_next_job()
    assert got_job_id == job_id
    assert params == {"test": "lifecycle"}

    # Verify running state
    df = queue.get_jobs_df()
    assert df.loc[df["id"] == job_id, "status"].iloc[0] == JobStatus.RUNNING.value

    # Update progress
    queue.update_progress(job_id, 50.0)
    df = queue.get_jobs_df()
    assert df.loc[df["id"] == job_id, "progress"].iloc[0] == 50.0

    # Complete job
    queue.complete_job(job_id, success=True)

    # Verify completed state
    df = queue.get_jobs_df()
    job_row = df.loc[df["id"] == job_id].iloc[0]
    assert job_row["status"] == JobStatus.COMPLETED.value
    assert pd.notna(job_row["completed_at"])


def test_failed_job(queue):
    """Test job failure handling."""
    job_id = queue.submit_job({"test": "failure"})
    queue.get_next_job()

    error_msg = "Test error message"
    queue.complete_job(job_id, success=False, error=error_msg)

    df = queue.get_jobs_df()
    job_row = df.loc[df["id"] == job_id].iloc[0]
    assert job_row["status"] == JobStatus.FAILED.value
    assert job_row["error"] == error_msg


def test_concurrent_job_processing(queue):
    """Test that jobs are processed one at a time."""
    job_ids = [queue.submit_job({"test": i}) for i in range(3)]

    # Get first job
    job1_id, _ = queue.get_next_job()
    assert job1_id == job_ids[0]

    # Try to get progress of non-running job
    with pytest.raises(ValueError):
        queue.update_progress(job_ids[1], 50.0)

    # Complete first job and get next
    queue.complete_job(job1_id, success=True)
    job2_id, _ = queue.get_next_job()
    assert job2_id == job_ids[1]


class TestWorker(Worker):
    """Test worker implementation."""

    def __init__(self, queue: JobQueue, should_fail: bool = False):
        super().__init__(queue)
        self.should_fail = should_fail
        self.processed_jobs = []

    def process_job(self, job_id: str, params: Dict[str, Any], result_dir: Path):
        self.processed_jobs.append(job_id)
        if self.should_fail:
            raise ValueError("Test failure")

        # Simulate some work
        for i in range(3):
            self.queue.update_progress(job_id, (i + 1) * 33.3)
            time.sleep(0.1)

        # Save result
        with open(result_dir / "result.txt", "w") as f:
            f.write(str(params))


def test_worker_processing(queue):
    """Test worker job processing."""
    # Submit test job
    job_id = queue.submit_job({"test": "worker"})

    # Create and run worker for a short time
    worker = TestWorker(queue)
    worker.run(poll_interval=0.1)  # This will run forever, so we'll need to handle that

    # Verify job was completed
    df = queue.get_jobs_df()
    job_row = df.loc[df["id"] == job_id].iloc[0]
    assert job_row["status"] == JobStatus.COMPLETED.value

    # Check results
    result_path = queue.get_result_path(job_id)
    assert result_path.exists()
    assert (result_path / "result.txt").exists()


def test_worker_failure(queue):
    """Test worker handling of job failures."""
    job_id = queue.submit_job({"test": "failure"})

    worker = TestWorker(queue, should_fail=True)
    worker.run(poll_interval=0.1)  # This will run forever

    df = queue.get_jobs_df()
    job_row = df.loc[df["id"] == job_id].iloc[0]
    assert job_row["status"] == JobStatus.FAILED.value
    assert "Test failure" in job_row["error"]


# Additional test ideas:
# - Test file locking (concurrent access)
# - Test invalid job IDs
# - Test malformed job files
# - Test directory permissions
# - Test cleanup of lock files
# - Test worker interruption
