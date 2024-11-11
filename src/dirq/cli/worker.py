"""Worker command implementation."""

import time
from pathlib import Path
from typing import Any, Dict

from dirq import JobQueue, Worker


class DefaultWorker(Worker):
    """Default worker implementation for CLI usage."""

    def process_job(self, job_id: str, params: Dict[str, Any], result_dir: Path) -> None:
        """Process a job with progress reporting."""
        print(f"\nProcessing job {job_id}")
        print(f"Parameters: {params}")

        # Get job parameters or use defaults
        steps = params.get("steps", 10)
        sleep_time = params.get("sleep", 1.0)

        # Simulate work with progress updates
        for i in range(steps):
            time.sleep(sleep_time)
            progress = (i + 1) * 100 / steps
            self.queue.update_progress(job_id, progress)
            print(f"\rProgress: {progress:.1f}%", end="")

        print("\nJob completed!")

        # Save results
        with open(result_dir / "result.txt", "w") as f:
            f.write(f"Processed job with {steps} steps\n")
            f.write(f"Parameters: {params}\n")


def worker_command(args: Any) -> None:
    """Handle the worker command."""
    print(f"Starting worker (jobs dir: {args.jobs_dir})")
    try:
        queue = JobQueue(args.jobs_dir)
        worker = DefaultWorker(queue)

        try:
            worker.run(poll_interval=args.poll_interval)
        except KeyboardInterrupt:
            print("\nWorker stopped.")

    except Exception as e:
        print(f"Error starting worker: {e}")
