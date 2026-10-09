"""Retryable vs non-retryable error classification for TMDB activities."""

from __future__ import annotations

from temporalio.exceptions import ApplicationError


class NonRetryableCaptureError(ApplicationError):
    def __init__(self, message: str) -> None:
        super().__init__(message, non_retryable=True, type="NonRetryableCaptureError")


class RetryableCaptureError(ApplicationError):
    def __init__(self, message: str) -> None:
        super().__init__(message, non_retryable=False, type="RetryableCaptureError")


class NonRetryableSilverError(ApplicationError):
    def __init__(self, message: str) -> None:
        super().__init__(message, non_retryable=True, type="NonRetryableSilverError")


class RetryableSilverError(ApplicationError):
    def __init__(self, message: str) -> None:
        super().__init__(message, non_retryable=False, type="RetryableSilverError")


class NonRetryableGoldenBuildError(ApplicationError):
    def __init__(self, message: str) -> None:
        super().__init__(
            message,
            non_retryable=True,
            type="NonRetryableGoldenBuildError",
        )


class RetryableGoldenBuildError(ApplicationError):
    def __init__(self, message: str) -> None:
        super().__init__(
            message,
            non_retryable=False,
            type="RetryableGoldenBuildError",
        )


def classify_capture_exception(exc: BaseException) -> ApplicationError:
    if isinstance(exc, ApplicationError):
        return exc
    message = str(exc) or exc.__class__.__name__
    transient_markers = (
        "timed out",
        "timeout",
        "temporar",
        "connection reset",
        "connection refused",
        "broken pipe",
        "503",
        "502",
        "429",
        "rate limit",
        "SlowDown",
        "Throttl",
        "ServiceUnavailable",
    )
    lowered = message.lower()
    if any(
        marker.lower() in message or marker.lower() in lowered
        for marker in transient_markers
    ):
        return RetryableCaptureError(message)
    if isinstance(exc, (ValueError, TypeError, KeyError)):
        return NonRetryableCaptureError(message)
    return RetryableCaptureError(message)


def classify_silver_exception(exc: BaseException) -> ApplicationError:
    if isinstance(exc, ApplicationError):
        return exc
    message = str(exc) or exc.__class__.__name__
    if "ended in CANCELLED" in message:
        return RetryableSilverError(message)
    if "ended in FAILED" in message or isinstance(exc, ValueError):
        # Validation failures are non-retryable; EMR FAILED may be retryable.
        if isinstance(exc, ValueError):
            return NonRetryableSilverError(message)
        return RetryableSilverError(message)
    transient_markers = (
        "timeout",
        "Throttl",
        "SlowDown",
        "ServiceUnavailable",
        "TooManyRequests",
    )
    if any(marker.lower() in message.lower() for marker in transient_markers):
        return RetryableSilverError(message)
    return RetryableSilverError(message)


def classify_golden_build_exception(exc: BaseException) -> ApplicationError:
    if isinstance(exc, ApplicationError):
        return exc
    message = str(exc) or exc.__class__.__name__
    lowered = message.lower()
    if isinstance(exc, (ValueError, TypeError, KeyError, AssertionError)):
        return NonRetryableGoldenBuildError(message)
    if "ended in failed" in lowered or "ended in cancelled" in lowered:
        # Iceberg stages are commit-last and maxAttempts=1. A failed EMR
        # execution is diagnosed and reviewed rather than automatically rerun.
        return NonRetryableGoldenBuildError(message)
    transient_markers = (
        "timeout",
        "timed out",
        "throttl",
        "slowdown",
        "serviceunavailable",
        "toomanyrequests",
        "connection reset",
    )
    if any(marker in lowered for marker in transient_markers):
        return RetryableGoldenBuildError(message)
    return RetryableGoldenBuildError(message)
