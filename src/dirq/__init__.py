"""
dirq - Simple directory-based job queue system.

This package provides a simple, file-system based job queue system
that's perfect for small to medium workloads where simplicity
and ease of use are priorities.
"""

from .dirq import Job, JobID, JobQueue, JobState, Worker
from .exceptions import DirQError, JobStateError, JobValidationError

__version__ = "0.1.0"
__all__ = (
    "DirQError",
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
__email__ = "vojtech@nnaisense.com"

try:
    from .__version import __version__ as __version__
except ImportError:
    import sys

    print(
        "Please install the package to ensure correct behavior.\nFrom root folder:\n\tpip install -e .",
        file=sys.stderr,
    )
    __version__ = "undefined"
