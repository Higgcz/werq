import json
import shutil
import time
from pathlib import Path

import pytest

from dirq.dirq import Job, JobQueue, JobState, Worker
from dirq.exceptions import JobStateError


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


def test_list_jobs_empty(queue):
    """Test lising jobs with an empty queue."""
    jobs = queue.list_jobs()
    assert isinstance(jobs, list)
    assert len(jobs) == 0


def test_list_jobs(queue):
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


def test_job_lifecycle(queue):
    """Test complete job lifecycle."""
    # Submit job
    job_test = queue.submit({"test": "lifecycle"})

    # Verify queued state
    job_out = queue.get_job(job_test.id)
    assert job_out.id == job_test.id and job_out.state == JobState.QUEUED.value

    # Start job
    job_running = queue.pop_next()
    assert job_running.id == job_test.id
    assert job_running.state == JobState.RUNNING.value
    assert job_running.params == job_test.params

    # Verify running state
    job_out = queue.get_job(job_test.id)
    assert job_out.id == job_test.id and job_out.state == JobState.RUNNING.value

    # Update progress
    queue.update_progress(job_out, 50.0)
    job_out = queue.get_job(job_test.id)
    assert job_out.progress == 50.0

    # Complete job
    queue.complete(job_out)

    # Verify completed state
    job_out = queue.get_job(job_test.id)
    assert job_out.id == job_test.id and job_out.state == JobState.COMPLETED.value
    assert job_out.progress == 100.0
    assert job_out.finished_at is not None


def test_failed_job(queue):
    """Test job failure handling."""
    job_test = queue.submit({"test": "failure"})
    job_out = queue.pop_next()  # Start job

    error_msg = "Test error message"
    queue.fail(job_out, error_msg)

    job_out = queue.get_job(job_test.id)
    assert job_out.state == JobState.FAILED.value
    assert job_out.error == error_msg


def test_concurrent_job_processing(queue):
    """Test that jobs are processed one at a time."""
    jobs = [queue.submit({"test": i}) for i in range(3)]
    assert len(queue.list_jobs()) == 3

    # Get first job
    job_out = queue.pop_next()
    assert job_out.id == jobs[0].id

    # Try to get progress of non-running job
    with pytest.raises(JobStateError):
        queue.update_progress(jobs[1], 50.0)

    # Complete first job and get next
    queue.complete(job_out)
    job_out = queue.pop_next()
    assert job_out.id == jobs[1].id


class TestWorker(Worker):
    """Test worker implementation."""

    def __init__(self, queue: JobQueue, should_fail: bool = False):
        super().__init__(queue, stop_when_done=True)
        self.should_fail = should_fail
        self.processed_jobs = []

    def process_job(self, job: Job, result_dir: Path) -> None:
        self.processed_jobs.append(job.id)
        if self.should_fail:
            raise ValueError("Test failure")

        # Simulate some work
        for i in range(3):
            self.queue.update_progress(job, (i + 1) * 33.3)
            time.sleep(0.1)

        # Save result
        with open(result_dir / "result.txt", "w") as f:
            f.write(str(job.params))


def test_worker_processing(queue):
    """Test worker job processing."""
    # Submit test job
    job = queue.submit({"test": "worker"})

    # Create and run worker for a short time
    worker = TestWorker(queue)
    worker.run(poll_interval=0.1)  # This will run forever, so we'll need to handle that

    # Verify job was completed
    job_out = queue.get_job(job.id)
    assert job_out.state == JobState.COMPLETED.value

    # Check results
    result_path = queue.get_result_dir(job)
    assert result_path.exists()
    assert (result_path / "result.txt").exists()


def test_worker_failure(queue):
    """Test worker handling of job failures."""
    job = queue.submit({"test": "failure"})

    worker = TestWorker(queue, should_fail=True)
    worker.run(poll_interval=0.1)  # This will run forever

    job_out = queue.get_job(job.id)
    assert job_out.state == JobState.FAILED.value
    assert "Test failure" in job_out.error


# Additional test ideas:
# - Test file locking (concurrent access)
# - Test invalid job IDs
# - Test malformed job files
# - Test directory permissions
# - Test cleanup of lock files
# - Test worker interruption
