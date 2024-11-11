"""Custom exceptions for dirq."""

from typing import Optional


class DirQError(Exception):
    """Base exception for all dirq exceptions."""

    def __init__(self, message: str, original_error: Optional[Exception] = None) -> None:
        super().__init__(message)
        self.original_error = original_error


class JobStateError(DirQError):
    """Raised when attempting an invalid state transition or operation on a job."""

    pass


class JobValidationError(DirQError):
    """Raised when job parameters or data are invalid."""

    pass
