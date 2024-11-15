"""Command line interface for dirq."""

import argparse
from pathlib import Path
from typing import Optional, Sequence

from .submit import info_command, list_command, monitor_command, submit_command
from .worker import worker_command


def main(args: Optional[Sequence[str]] = None) -> None:
    """Main entry point for the dirq CLI."""
    parser = argparse.ArgumentParser(description="dirq - Simple directory-based job queue system")

    parser.add_argument(
        "--jobs-dir",
        type=Path,
        default=Path("jobs"),
        help="Jobs directory (default: ./jobs)",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # Submit command
    submit_parser = subparsers.add_parser(
        "submit",
        help="Submit new job",
        description="The `dirq submit` command is used to submit jobs to a job queue. "
        "It can handle both JSON parameter files and shell commands. "
        "The command initializes the job queue, submits the job, and optionally monitors the job's progress.",
        epilog="Examples:\n"
        "  # Submitting a job using a JSON parameter file\n"
        "  dirq submit job_params.json\n\n"
        "  # Submitting a shell command as a job\n"
        "  dirq submit echo 'Hello, World!'\n\n"
        "  # Submitting a job and monitoring its progress\n"
        "  dirq submit job_params.json --monitor",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    submit_parser.add_argument("file_or_command", help="Parameters file or shell command", nargs="+")
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
    worker_parser.add_argument("--list", action="store_true", help="List available workers and exit")
    worker_parser.add_argument("-n", "--name", help="Worker name to start")
    worker_parser.add_argument(
        "--poll-interval", type=float, default=1.0, help="Poll interval in seconds (default: 1.0)"
    )
    worker_parser.add_argument("--rm", action="store_true", help="Kill worker after all jobs are done")
    worker_parser.set_defaults(func=worker_command)

    # Info command
    info_parser = subparsers.add_parser("info", help="Show information about the job")
    info_parser.add_argument("job_id", help="Job ID to show information")
    info_parser.set_defaults(func=info_command)

    # Parse and execute
    parsed_args = parser.parse_args(args)
    parsed_args.func(parsed_args)
