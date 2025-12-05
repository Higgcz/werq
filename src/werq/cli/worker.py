"""Worker command implementation."""

import subprocess
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.table import Table

from werq import Job, JobQueue, Worker


class ShellWorker(Worker):
    """Shell worker implementation for CLI usage."""

    def process_job(self, job: Job, *, result_dir: Path) -> Mapping[str, Any]:
        """Process a job by executing a shell command."""
        print(f"\nProcessing job {job.id}")
        print(f"Parameters: {job.params}")

        # Get job parameters or use defaults
        command = job.params.get("command")

        if not command:
            raise ValueError("Missing 'command' parameter in job!")

        # Execute the shell command
        try:
            result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True, cwd=result_dir)
        except subprocess.CalledProcessError as e:
            raise ValueError(f"Command failed with exit code {e.returncode}: {e.stderr}") from e

        return {"command": command, "output": result.stdout}


def worker_command(
    jobs_dir: Path,
    name: str = "shell",
    list_workers: bool = False,
    poll_interval: float = 1.0,
    rm: bool = False,
    **kwargs,
) -> None:
    """Handle the worker command."""
    available_workers = {
        "shell": ShellWorker,
    }

    console = Console()
    if list_workers:
        table = Table(title="Available Workers")
        table.add_column("Name", style="bold")

        for name in available_workers:
            table.add_row(name)

        console.print(table)
        console.print("Use `-n/--name` to start a specific worker.")
        console.print("You can pass a module path to start a custom worker.")
        console.print("Example: `werq worker -n mymodule.MyWorker`")
        return
    try:
        # Try to load the worker class
        # if the worker class is not available, assume it's a module path and try to import
        worker_class = available_workers.get(name, None)
        if worker_class is None:
            import importlib

            module, class_name = name.rsplit(".", 1)
            worker_class = importlib.import_module(module).__dict__[class_name]

        queue = JobQueue(jobs_dir)
        worker = worker_class(queue, stop_when_done=rm)

        console.print(f"Starting worker '{worker.__class__.__name__}' (jobs dir: {jobs_dir})")

        try:
            worker.run(poll_interval=poll_interval)
        except KeyboardInterrupt:
            console.print("\nWorker stopped.")

    except Exception as e:
        console.print(f"Error starting worker: {e}")
