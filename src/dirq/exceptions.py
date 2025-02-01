"""Custom exceptions for dirq."""

from typing import Optional


class DirQError(Exception):
    """Base exception for all dirq exceptions.

    This is the parent class for all custom exceptions in the dirq package.
    It can optionally wrap another exception to preserve the original error context.

    Args:
        message: Human-readable error description
        original_error: Optional underlying exception that caused this error
    """

    def __init__(self, message: str, original_error: Optional[Exception] = None) -> None:
        super().__init__(message)
        self.original_error = original_error


class JobStateError(DirQError):
    """Raised when attempting an invalid state transition or operation on a job.

    This exception is raised in situations like:
    - Trying to complete a job that isn't running
    - Attempting to update progress of a completed job
    - Invalid state transitions
    """

    pass


class JobValidationError(DirQError):
    """Raised when job parameters or data are invalid.

    This exception is raised when:
    - Job parameters fail validation
    - Required fields are missing
    - Data types are incorrect
    """

    pass
