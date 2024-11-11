"""
dirq - Simple directory-based job queue system.

This package provides a simple, file-system based job queue system
that's perfect for small to medium workloads where simplicity
and ease of use are priorities.
"""

from .dirq import Job, JobQueue, JobState, Worker
from .exceptions import (
    DirQException,
    JobNotFoundException,
    JobStateError,
    JobValidationError,
)

__version__ = "0.1.0"
__all__ = [
    "DirQException",
    "Job",
    "JobNotFoundException",
    "JobQueue",
    "JobState",
    "JobStateError",
    "JobValidationError",
    "Worker",
]
