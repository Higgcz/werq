"""Job submission and monitoring commands."""

import json
import shlex
import time
from pathlib import Path
from typing import Any

import pandas as pd
from rich.console import Console
from rich.table import Table

from dirq import DirQError, JobQueue, JobState
from dirq.dirq import JobID


def submit_command(args: Any) -> None:
    """Handle the submit command."""
    try:
        # Check if file or command
        match args.file_or_command:
            case [file] if file.endswith(".json") and Path(file).is_file():
                params = json.loads(Path(file).read_text())
            case _:
                params = {"command": shlex.join(args.file_or_command), "type": "shell"}

        # Initialize queue and submit job
        queue = JobQueue(args.jobs_dir)
        job_id = queue.submit(params)
        print(f"Submitted job: {job_id} ({params})")

        # Monitor if requested
        if args.monitor:
            monitor_job(queue, job_id)

    except json.JSONDecodeError as e:
        print(f"Error reading parameters file: {e}")
    except DirQError as e:
        print(f"Job submission failed: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")


def list_command(args: Any) -> None:
    """Handle the list command."""
    try:
        queue = JobQueue(args.jobs_dir)
        jobs = queue.list_jobs()

        # Convert jobs to a DataFrame
        df = pd.DataFrame(jobs)

        if len(df) == 0:
            print("No jobs found.")
            return

        # Format timestamps
        for col in ["created_at", "started_at", "finished_at", "failed_at"]:
            if (col in df.columns) and (df[col].dtype == "datetime64[ns]"):
                df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")

        # Sort by creation time
        df = df.sort_values("created_at", ascending=False)

        if args.limit:
            df = df.head(args.limit)

        df.state = df.state.str.upper()
        df.drop("params", axis=1, inplace=True)

        # Show only first line of long error messages
        df.error = df.error.str.split("\n").str[0]

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
        for column in df.columns:
            table.add_column(column, style="cyan", no_wrap=True, max_width=30)

        # Add rows with status-based styles
        for _, row in df.iterrows():
            state = str(row["state"])
            style = state_colors.get(state, "white")
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


def monitor_command(args: Any) -> None:
    """Handle the monitor command."""
    try:
        queue = JobQueue(args.jobs_dir)
        monitor_job(queue, args.job_id)
    except Exception as e:
        print(f"Error starting monitoring: {e}")
