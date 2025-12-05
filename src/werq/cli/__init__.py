"""Command line interface for werq."""

import argparse
import logging
from pathlib import Path
from typing import Optional, Sequence

from rich.logging import RichHandler

from .submit import info_command, list_command, monitor_command, resubmit_command, rm_command, submit_command
from .worker import worker_command


def setup_logging(verbose: bool) -> None:
    """Setup logging for the CLI."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            RichHandler(rich_tracebacks=True, markup=True),
        ],
    )


def main(args: Optional[Sequence[str]] = None) -> None:
    """Main entry point for the werq CLI."""
    parser = argparse.ArgumentParser(description="werq - Simple directory-based job queue system")

    parser.add_argument(
        "--jobs-dir",
        type=Path,
        default=Path("jobs"),
        help="Jobs directory (default: ./jobs)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # Submit command
    submit_parser = subparsers.add_parser(
        "submit",
        help="Submit new job",
        description="The `werq submit` command is used to submit jobs to a job queue. "
        "It can handle both JSON parameter files and shell commands. "
        "The command initializes the job queue, submits the job, and optionally monitors the job's progress.",
        epilog="Examples:\n"
        "  # Submitting a job using a JSON parameter file\n"
        "  werq submit job_params.json\n\n"
        "  # Submitting a shell command as a job\n"
        "  werq submit echo 'Hello, World!'\n\n"
        "  # Submitting a job and monitoring its progress\n"
        "  werq submit job_params.json --monitor",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    submit_parser.add_argument("file_or_command", help="Parameters file or shell command", nargs="+")
    submit_parser.add_argument("--name", "-n", help="Optional name for the job")
    submit_parser.add_argument("--monitor", action="store_true", help="Monitor job after submission")
    submit_parser.set_defaults(func=submit_command)

    # List command
    list_parser = subparsers.add_parser("list", help="List all jobs")
    list_parser.add_argument("-n", "--limit", type=int, help="Limit number of jobs to show")
    list_parser.set_defaults(func=list_command)

    # Monitor command
    monitor_parser = subparsers.add_parser("monitor", help="Monitor specific job")
    monitor_parser.add_argument("job_id", help="Job ID to monitor")
    monitor_parser.set_defaults(func=monitor_command)

    # Worker command
    worker_parser = subparsers.add_parser("worker", help="Start a worker")
    worker_parser.add_argument(
        "-l", "--list", action="store_true", help="List available workers and exit", dest="list_workers"
    )
    worker_parser.add_argument("-n", "--name", default="shell", help="Worker name to start (default: shell)")
    worker_parser.add_argument(
        "--poll-interval", type=float, default=1.0, help="Poll interval in seconds (default: 1.0)"
    )
    worker_parser.add_argument("--rm", action="store_true", help="Kill worker after all jobs are done")
    worker_parser.set_defaults(func=worker_command)

    # Info command
    info_parser = subparsers.add_parser("info", help="Show information about the job")
    info_parser.add_argument("job_id", help="Job ID to show information")
    info_parser.set_defaults(func=info_command)

    # Rm command
    rm_parser = subparsers.add_parser("rm", help="Remove a job")
    rm_parser.add_argument("job_id", help="Job ID to remove")
    rm_parser.set_defaults(func=rm_command)

    # Resubmit command
    resubmit_parser = subparsers.add_parser("resubmit", help="Resubmit an existing job")
    resubmit_parser.add_argument("job_id", help="Job ID to resubmit")
    resubmit_parser.add_argument("--name", "-n", help="Optional new name for the resubmitted job")
    resubmit_parser.set_defaults(func=resubmit_command)

    # Parse and execute
    parsed_args = parser.parse_args(args)
    setup_logging(parsed_args.verbose)

    # Convert namespace to dict and call the function
    parsed_args.func(**vars(parsed_args))
