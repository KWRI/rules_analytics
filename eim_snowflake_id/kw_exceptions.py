from datetime import datetime, timezone
from typing import Optional


class KWException(Exception):
    def __init__(self, message: str, error_type: Optional[str] = None, error_ts: Optional[datetime] = None):
        self.message = message
        self.error_type = error_type or self.__class__.__name__
        self.error_ts = error_ts or datetime.now(timezone.utc)

    def __eq__(self, other):
        if issubclass(other.__class__, KWException):
            return self.message == other.message and self.error_type == other.error_type

        return NotImplemented


class InvalidAlgorithmConfig(KWException):
    """Invalid configuration for core snowflake algorithm."""

    def __init__(self, message: str, **kwargs):
        super().__init__(message=message, error_type=self.__class__.__name__, **kwargs)


class InvalidWorkerID(KWException):
    """Invalid value for worker id."""

    def __init__(self, message: str, **kwargs):
        super().__init__(message=message, error_type=self.__class__.__name__, **kwargs)


class InvalidSystemClock(KWException):
    """System clock is moving backward."""

    def __init__(self, message: str, **kwargs):
        super().__init__(message=message, error_type=self.__class__.__name__, **kwargs)


class InvalidUnixTimestamp(KWException):
    """Timestamp is not a positive integer"""

    def __init__(self, message: str, **kwargs):
        super().__init__(message=message, error_type=self.__class__.__name__, **kwargs)


class InvalidFlakeID(KWException):
    """Flake Id is not a n-bit positive integer"""

    def __init__(self, message: str, **kwargs):
        super().__init__(message=message, error_type=self.__class__.__name__, **kwargs)
