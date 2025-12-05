"""werq - Simple directory-based job queue system.

This package provides a simple, file-system based job queue system
that's perfect for small to medium workloads where simplicity
and ease of use are priorities.
"""

from .exceptions import JobStateError, JobValidationError, WerqError
from .queue import Job, JobID, JobQueue, JobState, Worker

__all__ = (
    "WerqError",
    "Job",
    "JobID",
    "JobQueue",
    "JobState",
    "JobStateError",
    "JobValidationError",
    "Worker",
    "__author__",
    "__email__",
    "__version__",
)
__author__ = "Vojtech Micka"
__email__ = "micka.vojtech@gmail.com"

try:
    from .__version import __version__ as __version__
except ImportError:
    __version__ = "0.0.0"
