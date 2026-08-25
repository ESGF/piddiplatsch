"""Custom exceptions for piddiplatsch."""


class PiddiplatschError(Exception):
    """Base class for all piddiplatsch exceptions."""


class LookupError(PiddiplatschError):
    """Raised when a dataset version lookup via STAC or other lookup fails."""


class ValidationError(PiddiplatschError):
    """Raised when input data or a record fails validation."""


class MaxErrorsExceededError(PiddiplatschError):
    """Raised when the consumer reaches its max error limit."""


class TransientExternalError(PiddiplatschError):
    """Raised when an external dependency (e.g., STAC) fails transiently after retries."""


class StopOnTransientSkipError(PiddiplatschError):
    """Raised by the pipeline to stop consumption due to transient external failures when policy dictates fail-fast."""


class JsonlReadError(PiddiplatschError):
    """Raised when a JSONL input contains a malformed record."""
