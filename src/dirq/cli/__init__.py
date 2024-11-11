"""Command line interface for dirq."""

import argparse
from pathlib import Path
from typing import Optional, Sequence

from .submit import list_command, monitor_command, submit_command
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
    submit_parser = subparsers.add_parser("submit", help="Submit new job")
    submit_parser.add_argument("params_file", type=Path, help="JSON file with job parameters", metavar="FILE.json")
    submit_parser.add_argument("--monitor", action="store_true", help="Monitor job after submission")
    submit_parser.add_argument("--sh", nargs="+", help="Command to run in shell")
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
        "--poll-interval", type=float, default=1.0, help="Poll interval in seconds (default: 1.0)"
    )
    worker_parser.add_argument("--rm", action="store_true", help="Kill worker after all jobs are done")
    worker_parser.set_defaults(func=worker_command)

    # Parse and execute
    parsed_args = parser.parse_args(args)
    parsed_args.func(parsed_args)
