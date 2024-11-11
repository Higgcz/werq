"""Custom exceptions for dirq."""

from typing import Optional


class DirQException(Exception):
    """Base exception for all dirq exceptions."""

    def __init__(self, message: str, original_error: Optional[Exception] = None) -> None:
        super().__init__(message)
        self.original_error = original_error


class JobNotFoundException(DirQException):
    """Raised when a job is not found or not accessible."""

    pass


class JobStateError(DirQException):
    """Raised when attempting an invalid state transition or operation on a job."""

    pass


class JobValidationError(DirQException):
    """Raised when job parameters or data are invalid."""

    pass
