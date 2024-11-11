"""
dirq - Simple directory-based job queue system.

This package provides a simple, file-system based job queue system
that's perfect for small to medium workloads where simplicity
and ease of use are priorities.
"""

from .dirq import JobQueue, JobStatus, Worker
from .exceptions import (
    DirQException,
    JobNotFoundException,
    JobStateError,
    JobValidationError,
)

__version__ = "0.1.0"
__all__ = [
    "JobQueue",
    "Worker",
    "JobStatus",
    "DirQException",
    "JobNotFoundException",
    "JobStateError",
    "JobValidationError",
]
