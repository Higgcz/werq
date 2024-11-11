"""Worker command implementation."""

import subprocess
import time
from pathlib import Path
from typing import Any

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
    try:
        queue = JobQueue(args.jobs_dir)
        # worker = DefaultWorker(queue, stop_when_done=args.rm)
        worker = ShellWorker(queue, stop_when_done=args.rm)

        print(f"Starting worker '{worker.__class__.__name__}' (jobs dir: {args.jobs_dir})")
        # print(f"Starting worker (jobs dir: {args.jobs_dir})")

        try:
            worker.run(poll_interval=args.poll_interval)
        except KeyboardInterrupt:
            print("\nWorker stopped.")

    except Exception as e:
        print(f"Error starting worker: {e}")
