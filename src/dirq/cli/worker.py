"""Worker command implementation."""

import subprocess
import time
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.table import Table

from dirq import Job, JobQueue, Worker


class DefaultWorker(Worker):
    """Default worker implementation for CLI usage."""

    def process_job(self, job: Job, result_dir: Path) -> None:
        """Process a job with progress reporting."""
        print(f"\nProcessing job {job.id}")
        print(f"Parameters: {job.params}")

        # Get job parameters or use defaults
        steps = job.params.get("steps", 10)
        sleep_time = job.params.get("sleep", 1.0)

        # Simulate work with progress updates
        for i in range(steps):
            time.sleep(sleep_time)
            progress = (i + 1) * 100 / steps
            self.queue.update_progress(job, progress)
            print(f"\rProgress: {progress:.1f}%", end="")

        print("\nJob completed!")

        # Save results
        with open(result_dir / "result.txt", "w") as f:
            f.write(f"Processed job with {steps} steps\n")
            f.write(f"Parameters: {job.params}\n")


class ShellWorker(Worker):
    """Shell worker implementation for CLI usage."""

    def process_job(self, job: Job, result_dir: Path) -> None:
        """Process a job by executing a shell command."""
        print(f"\nProcessing job {job.id}")
        print(f"Parameters: {job.params}")

        # Get job parameters or use defaults
        command = job.params.get("command")

        if not command:
            raise ValueError("Missing 'command' parameter in job!")

        # Execute the shell command
        try:
            result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)

            # Save results
            with open(result_dir / "result.txt", "w") as f:
                f.write(f"Executed command: {command}\n")
                f.write(f"Output: {result.stdout}\n")
        except subprocess.CalledProcessError as e:
            raise ValueError(f"Command failed with exit code {e.returncode}: {e.stderr}") from e


def worker_command(args: Any) -> None:
    """Handle the worker command."""
    available_workers = {
        "shell": ShellWorker,
        "default": DefaultWorker,
    }

    console = Console()
    if args.list:
        table = Table(title="Available Workers")
        table.add_column("Name", style="bold")

        for name in available_workers:
            table.add_row(name)

        console.print(table)
        console.print("Use `-n/--name` to start a specific worker.")
        console.print("You can pass a module path to start a custom worker.")
        console.print("Example: `dirq worker -n mymodule.MyWorker`")
        return
    try:
        # Try to load the worker class
        # if the worker class is not available, assume it's a module path and try to import
        worker_class = available_workers.get(args.name, None)
        if worker_class is None:
            import importlib

            module, class_name = args.name.rsplit(".", 1)
            worker_class = importlib.import_module(module).__dict__[class_name]

        queue = JobQueue(args.jobs_dir)
        worker = worker_class(queue, stop_when_done=args.rm)

        console.print(f"Starting worker '{worker.__class__.__name__}' (jobs dir: {args.jobs_dir})")

        try:
            worker.run(poll_interval=args.poll_interval)
        except KeyboardInterrupt:
            console.print("\nWorker stopped.")

    except Exception as e:
        console.print(f"Error starting worker: {e}")
