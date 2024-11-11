"""Job submission and monitoring commands."""

import json
import time
from typing import Any

from rich import print
from rich.console import Console
from rich.table import Table

from dirq import DirQException, JobQueue, JobStatus


def submit_command(args: Any) -> None:
    """Handle the submit command."""
    try:
        # Read parameters
        with open(args.params_file) as f:
            params = json.load(f)

        # Initialize queue and submit job
        queue = JobQueue(args.jobs_dir)
        job_id = queue.submit_job(params)
        print(f"Submitted job: {job_id}")

        # Monitor if requested
        if args.monitor:
            monitor_job(queue, job_id)

    except json.JSONDecodeError as e:
        print(f"Error reading parameters file: {e}")
    except DirQException as e:
        print(f"Job submission failed: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")


def list_command(args: Any) -> None:
    """Handle the list command."""
    try:
        queue = JobQueue(args.jobs_dir)
        df = queue.get_jobs_df()

        if len(df) == 0:
            print("No jobs found.")
            return

        # Format timestamps
        for col in ["created_at", "started_at", "completed_at", "failed_at"]:
            if (col in df.columns) and (df[col].dtype == "datetime64[ns]"):
                df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")

        # Sort by creation time
        df = df.sort_values("created_at", ascending=False)

        if args.limit:
            df = df.head(args.limit)

        df.status = df.status.str.upper()
        df.drop("params", axis=1, inplace=True)

        # Define status colors
        status_colors = {
            "COMPLETED": "bright_black",
            "FAILED": "red",
            "RUNNING": "yellow",
            "QUEUED": "blue",
        }

        # Create a rich table
        table = Table(title="Jobs", title_style="bold magenta")

        # Add columns with styles
        for column in df.columns:
            table.add_column(column, style="cyan", no_wrap=True)

        # Add rows with status-based styles
        for _, row in df.iterrows():
            status = row["status"]
            style = status_colors.get(status, "white")
            table.add_row(*[str(value) for value in row], style=style)

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
            df = queue.get_jobs_df()
            job = df[df["id"] == job_id]

            if len(job) == 0:
                print(f"Job {job_id} not found!")
                break

            job = job.iloc[0]
            status = job["status"]
            progress = job.get("progress", 0)

            print(f"\rStatus: {status} | Progress: {progress:.1f}%", end="")

            if status in [JobStatus.COMPLETED.value, JobStatus.FAILED.value]:
                print("\nJob finished!")
                if status == JobStatus.FAILED.value:
                    print(f"Error: {job['error']}")

                result_path = queue.get_result_path(job_id)
                if result_path and result_path.exists():
                    print(f"Results available at: {result_path}")
                break

            time.sleep(interval)

    except KeyboardInterrupt:
        print("\nMonitoring stopped.")
    except Exception as e:
        print(f"\nError monitoring job: {e}")


def monitor_command(args: Any) -> None:
    """Handle the monitor command."""
    try:
        queue = JobQueue(args.jobs_dir)
        monitor_job(queue, args.job_id)
    except Exception as e:
        print(f"Error starting monitoring: {e}")
