"""Job submission and monitoring commands."""

import json
import shlex
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

from rich.console import Console
from rich.table import Table

from werq import Job, JobQueue, JobState, WerqError
from werq.queue import JobID


def submit_command(
    jobs_dir: Path, file_or_command: list[str], monitor: bool = False, name: Optional[str] = None, **kwargs
) -> None:
    """Handle the submit command."""
    try:
        # Check if file or command
        match file_or_command:
            case [file] if file.endswith(".json") and Path(file).is_file():
                params = json.loads(Path(file).read_text())
            case _:
                params = {
                    "command": (shlex.join(file_or_command) if len(file_or_command) > 1 else file_or_command[0]),
                    "type": "shell",
                }

        # Add name if provided
        if name:
            params["name"] = name

        # Initialize queue and submit job
        queue = JobQueue(jobs_dir)
        job = queue.submit(params)
        print(f"Submitted job: {job.id} ({params})")

        # Monitor if requested
        if monitor:
            monitor_job(queue, job.id)

    except json.JSONDecodeError as e:
        print(f"Error reading parameters file: {e}")
    except WerqError as e:
        print(f"Job submission failed: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")


DEFAULT_COLUMNS = ("id", "name", "state", "created_at", "started_at", "finished_at", "worker_name", "progress", "error")


def _format_job_row(job: Job, columns: tuple[str, ...]) -> dict[str, str]:
    """Format a job object into a row dict with string values."""

    def fmt_datetime(dt: datetime | None) -> str:
        return dt.strftime("%Y-%m-%d %H:%M:%S") if dt else ""

    row = {
        "id": job.id,
        "name": job.params.get("name", ""),
        "state": job.state.value.upper(),
        "created_at": fmt_datetime(job.created_at),
        "started_at": fmt_datetime(job.started_at),
        "finished_at": fmt_datetime(job.finished_at),
        "worker_name": job.worker_name or "",
        "progress": str(job.progress),
        "error": (job.error or "").split("\n", 1)[0],
    }

    return {col: row.get(col, "") for col in columns}


def list_command(
    jobs_dir: Path, limit: Optional[int] = None, columns: tuple[str, ...] = DEFAULT_COLUMNS, **kwargs
) -> None:
    """Handle the list command."""
    try:
        queue = JobQueue(jobs_dir)
        jobs = queue.list_jobs()

        if not jobs:
            print("No jobs found.")
            return

        # Sort by creation time (newest first)
        jobs = sorted(jobs, key=lambda j: j.created_at, reverse=True)

        # Apply limit
        if limit:
            jobs = jobs[:limit]

        # Define status colors
        state_colors = {
            "COMPLETED": "bright_black",
            "FAILED": "red",
            "RUNNING": "yellow",
            "QUEUED": "blue",
        }

        # Create a rich table
        table = Table(title="Jobs", title_style="bold magenta")

        # Add columns with styles
        for column in columns:
            table.add_column(column, style="cyan", no_wrap=True, max_width=30)

        # Add rows with status-based styles
        for job in jobs:
            row = _format_job_row(job, columns)
            style = state_colors.get(row["state"], "white")
            table.add_row(*[row[col] for col in columns], style=style)

        # Print the table
        console = Console()
        console.print(table)

    except Exception as e:
        print(f"Error listing jobs: {e}")


def monitor_job(queue: JobQueue, job_id: str, interval: float = 1.0) -> None:
    """Monitor a specific job until completion."""
    print(f"\nMonitoring job {job_id}:")
    try:
        while True:
            job = queue.get_job(JobID(job_id))

            if not job:
                print(f"Job {job_id} not found!")
                break

            state = job.state
            progress = job.progress

            print(f"\rState: {state.value.upper()} | Progress: {progress:.1f}%", end="")

            if state in [JobState.COMPLETED, JobState.FAILED]:
                print("\nJob finished!")
                if state == JobState.FAILED:
                    print(f"Error: {job.error}")

                result_path = queue.get_result_dir(job)
                if result_path and result_path.exists():
                    print(f"Results available at: {result_path}")
                break

            time.sleep(interval)

    except KeyboardInterrupt:
        print("\nMonitoring stopped.")
    except Exception as e:
        print(f"\nError monitoring job: {e}")


def monitor_command(jobs_dir: Path, job_id: str, **kwargs) -> None:
    """Handle the monitor command."""
    try:
        queue = JobQueue(jobs_dir)
        monitor_job(queue, job_id)
    except Exception as e:
        print(f"Error starting monitoring: {e}")


def rm_command(jobs_dir: Path, job_id: str, **kwargs) -> None:
    """Handle the rm command."""
    try:
        queue = JobQueue(jobs_dir)
        job = queue.get_job(JobID(job_id))
        if not job:
            print(f"Job {job_id} not found")
            return

        queue.delete(job)
        print(f"Job {job_id} deleted successfully")
    except Exception as e:
        print(f"Error deleting job: {e}")


def resubmit_command(jobs_dir: Path, job_id: str, name: Optional[str] = None, **kwargs) -> None:
    """Resubmit an existing job."""
    try:
        queue = JobQueue(jobs_dir)
        new_job = queue.resubmit(JobID(job_id), name=name)
        print(f"Resubmitted job {job_id} as new job: {new_job.id}")
    except ValueError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"Error resubmitting job: {e}")


def info_command(jobs_dir: Path, job_id: str, **kwargs) -> None:
    """Show information about the job."""
    try:
        console = Console()

        queue = JobQueue(jobs_dir)
        job = queue.get_job(JobID(job_id))
        if not job:
            console.print(f"Job {job_id} not found!")
            return

        # Display table with job information
        table = Table(title="Job Information", title_style="bold magenta")
        table.add_column("Field", style="cyan", no_wrap=True)
        table.add_column("Value", style="magenta", no_wrap=True)

        for key, value in job.to_dict().items():
            if key == "error":
                continue
            table.add_row(key, str(value))

        console.print(table)

        # Display the error
        if job.error:
            console.print(f"\nError:\n{job.error}", style="red")

    except Exception as e:
        print(f"Error getting job information: {e}")
