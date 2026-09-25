"""Anonymous, bounded exact-ID resolution against the public EIDR resolver."""

from __future__ import annotations

import math
import threading
import time
from contextlib import suppress
from datetime import UTC
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from video_media_catalog.community_sources import eidr_rights_profile
from video_media_catalog.eidr import normalize_eidr_id
from video_media_catalog.eidr_backfill import (
    DEFAULT_MAX_XML_BYTES,
    MAX_LOOKUP_BATCH_IDS,
    MAX_MAX_XML_BYTES,
    EidrBackfillError,
    EidrExactLookupResult,
    EidrLookupStatus,
    EidrProviderAuthorization,
    build_eidr_provider_authorization,
)
from video_media_catalog.models import ObjectRef

EIDR_PUBLIC_PROVIDER_ID = "eidr-public-exact-resolution"
EIDR_PUBLIC_RESOLVER_ORIGIN = "https://resolve.eidr.org"
EIDR_PUBLIC_RESOLVER_HOST = "resolve.eidr.org"
EIDR_PUBLIC_RESOLVER_PATH_PREFIX = "/EIDR/object/"
EIDR_PUBLIC_RESOLVER_QUERY = "type=Full&followAlias=false"
DEFAULT_EIDR_PUBLIC_USER_AGENT = (
    "video-media-catalog-eidr-public/1.0 "
    "(https://github.com/pandong912/video-media-catalog)"
)
DEFAULT_REQUEST_TIMEOUT_SECONDS = 30.0
DEFAULT_MINIMUM_REQUEST_INTERVAL_SECONDS = 0.5
DEFAULT_MAX_ATTEMPTS = 5
DEFAULT_RETRY_INITIAL_BACKOFF_SECONDS = 1.0
DEFAULT_RETRY_MAX_BACKOFF_SECONDS = 30.0
DEFAULT_MAX_RETRY_AFTER_SECONDS = 60.0
MAX_TIMEOUT_SECONDS = 120.0
MAX_MINIMUM_REQUEST_INTERVAL_SECONDS = 60.0
MAX_ATTEMPTS = 10
MAX_BACKOFF_SECONDS = 300.0
MAX_REQUEST_ID_LENGTH = 256


class EidrPublicProviderError(EidrBackfillError):
    """Fail-closed public resolver error."""


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class _RetryableStatus(Exception):
    def __init__(self, status: int, headers: Any) -> None:
        self.status = status
        self.headers = headers
        super().__init__(str(status))


def _bounded_finite(
    value: float,
    *,
    label: str,
    minimum: float,
    maximum: float,
) -> float:
    normalized = float(value)
    if not math.isfinite(normalized) or normalized < minimum or normalized > maximum:
        raise ValueError(f"{label} must be between {minimum} and {maximum}")
    return normalized


def eidr_public_resolution_url(eidr_id: str) -> str:
    """Return the sole URL shape accepted by the anonymous provider."""

    normalized = normalize_eidr_id(eidr_id)
    return (
        f"{EIDR_PUBLIC_RESOLVER_ORIGIN}{EIDR_PUBLIC_RESOLVER_PATH_PREFIX}"
        f"{normalized}?{EIDR_PUBLIC_RESOLVER_QUERY}"
    )


def _validate_resolution_url(url: str, *, eidr_id: str) -> str:
    expected = eidr_public_resolution_url(eidr_id)
    parsed = urlsplit(url)
    try:
        port = parsed.port
    except ValueError as exc:
        raise EidrPublicProviderError("EIDR resolver URL has an invalid port") from exc
    expected_path = f"{EIDR_PUBLIC_RESOLVER_PATH_PREFIX}{normalize_eidr_id(eidr_id)}"
    if (
        parsed.scheme != "https"
        or parsed.hostname != EIDR_PUBLIC_RESOLVER_HOST
        or port not in {None, 443}
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path != expected_path
        or parsed.query != EIDR_PUBLIC_RESOLVER_QUERY
        or parsed.fragment
    ):
        raise EidrPublicProviderError(
            "EIDR resolver redirected outside the approved exact-ID endpoint"
        )
    return expected


def _content_length(headers: Any) -> int | None:
    value = headers.get("Content-Length") if headers is not None else None
    if value is None:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise EidrPublicProviderError(
            "EIDR resolver returned an invalid Content-Length"
        ) from exc
    if parsed < 0:
        raise EidrPublicProviderError(
            "EIDR resolver returned an invalid Content-Length"
        )
    return parsed


def _retry_after_seconds(
    headers: Any,
    *,
    now: float,
    maximum: float,
) -> float | None:
    value = headers.get("Retry-After") if headers is not None else None
    if value is None:
        return None
    normalized = str(value).strip()
    try:
        seconds = float(normalized)
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(normalized)
        except (TypeError, ValueError, OverflowError):
            return None
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=UTC)
        seconds = retry_at.timestamp() - now
    if not math.isfinite(seconds):
        return None
    return min(maximum, max(0.0, seconds))


class EidrPublicProvider:
    """Anonymous provider for one fixed EIDR exact-resolution endpoint."""

    def __init__(
        self,
        *,
        authorization_object: ObjectRef,
        authorization_issued_at: str,
        user_agent: str = DEFAULT_EIDR_PUBLIC_USER_AGENT,
        request_timeout_seconds: float = DEFAULT_REQUEST_TIMEOUT_SECONDS,
        minimum_request_interval_seconds: float = (
            DEFAULT_MINIMUM_REQUEST_INTERVAL_SECONDS
        ),
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        max_xml_bytes: int = DEFAULT_MAX_XML_BYTES,
        retry_initial_backoff_seconds: float = (DEFAULT_RETRY_INITIAL_BACKOFF_SECONDS),
        retry_max_backoff_seconds: float = DEFAULT_RETRY_MAX_BACKOFF_SECONDS,
        max_retry_after_seconds: float = DEFAULT_MAX_RETRY_AFTER_SECONDS,
        opener: Any | None = None,
        clock=time.monotonic,
        wall_clock=time.time,
        sleeper=time.sleep,
    ) -> None:
        normalized_agent = user_agent.strip()
        if not normalized_agent or len(normalized_agent) > 256:
            raise ValueError("a bounded identifying User-Agent is required")
        self.request_timeout_seconds = _bounded_finite(
            request_timeout_seconds,
            label="request_timeout_seconds",
            minimum=0.001,
            maximum=MAX_TIMEOUT_SECONDS,
        )
        self.minimum_request_interval_seconds = _bounded_finite(
            minimum_request_interval_seconds,
            label="minimum_request_interval_seconds",
            minimum=0.0,
            maximum=MAX_MINIMUM_REQUEST_INTERVAL_SECONDS,
        )
        if (
            isinstance(max_attempts, bool)
            or not isinstance(max_attempts, int)
            or not 1 <= max_attempts <= MAX_ATTEMPTS
        ):
            raise ValueError(f"max_attempts must be between 1 and {MAX_ATTEMPTS}")
        if (
            isinstance(max_xml_bytes, bool)
            or not isinstance(max_xml_bytes, int)
            or not 0 < max_xml_bytes <= MAX_MAX_XML_BYTES
        ):
            raise ValueError("max_xml_bytes is outside the supported bound")
        self.retry_initial_backoff_seconds = _bounded_finite(
            retry_initial_backoff_seconds,
            label="retry_initial_backoff_seconds",
            minimum=0.001,
            maximum=MAX_BACKOFF_SECONDS,
        )
        self.retry_max_backoff_seconds = _bounded_finite(
            retry_max_backoff_seconds,
            label="retry_max_backoff_seconds",
            minimum=0.001,
            maximum=MAX_BACKOFF_SECONDS,
        )
        if self.retry_initial_backoff_seconds > self.retry_max_backoff_seconds:
            raise ValueError(
                "retry_initial_backoff_seconds must not exceed "
                "retry_max_backoff_seconds"
            )
        self.max_retry_after_seconds = _bounded_finite(
            max_retry_after_seconds,
            label="max_retry_after_seconds",
            minimum=0.0,
            maximum=MAX_BACKOFF_SECONDS,
        )
        policy = eidr_rights_profile()
        self.authorization: EidrProviderAuthorization = (
            build_eidr_provider_authorization(
                provider_id=EIDR_PUBLIC_PROVIDER_ID,
                authorization_object=authorization_object,
                policy_id=policy.policy_id,
                policy_digest=policy.digest,
                exact_lookup_allowed=True,
                complete_feed_allowed=False,
                issued_at=authorization_issued_at,
            )
        )
        self.user_agent = normalized_agent
        self.max_attempts = max_attempts
        self.max_xml_bytes = max_xml_bytes
        self.opener = opener or build_opener(_NoRedirect)
        self.clock = clock
        self.wall_clock = wall_clock
        self.sleeper = sleeper
        self._last_request_at: float | None = None
        self._throttle_lock = threading.Lock()

    def lookup_exact(
        self,
        *,
        eidr_ids: tuple[str, ...],
        request_id: str,
    ) -> tuple[EidrExactLookupResult, ...]:
        """Resolve only caller-supplied EIDR IDs; title search is impossible."""

        if not isinstance(request_id, str) or not 0 < len(request_id) <= (
            MAX_REQUEST_ID_LENGTH
        ):
            raise ValueError("request_id must be a non-empty bounded string")
        if not isinstance(eidr_ids, tuple) or len(eidr_ids) > MAX_LOOKUP_BATCH_IDS:
            raise ValueError("eidr_ids must be a bounded tuple")
        normalized = tuple(normalize_eidr_id(item) for item in eidr_ids)
        return tuple(self._lookup_one(item) for item in normalized)

    def _lookup_one(self, eidr_id: str) -> EidrExactLookupResult:
        url = eidr_public_resolution_url(eidr_id)
        retries = 0
        rate_limits = 0
        for attempt in range(1, self.max_attempts + 1):
            self._throttle()
            request = Request(
                url,
                headers={
                    "Accept": "application/xml",
                    "Accept-Encoding": "identity",
                    "User-Agent": self.user_agent,
                },
                method="GET",
            )
            response = None
            try:
                try:
                    response = self.opener.open(
                        request,
                        timeout=self.request_timeout_seconds,
                    )
                except HTTPError as exc:
                    response = exc
                final_url = response.geturl()
                _validate_resolution_url(final_url, eidr_id=eidr_id)
                status = int(response.status)
                if 300 <= status < 400:
                    location = (
                        response.headers.get("Location")
                        if response.headers is not None
                        else None
                    )
                    if location is not None:
                        _validate_resolution_url(
                            urljoin(url, str(location)),
                            eidr_id=eidr_id,
                        )
                    raise EidrPublicProviderError(
                        "EIDR resolver redirects are not accepted"
                    )
                if status == 404:
                    return EidrExactLookupResult(
                        eidr_id=eidr_id,
                        status=EidrLookupStatus.NOT_FOUND,
                        attempt_count=attempt,
                        retry_count=retries,
                        rate_limit_count=rate_limits,
                    )
                if status == 429 or 500 <= status < 600:
                    raise _RetryableStatus(status, response.headers)
                if status != 200:
                    raise EidrPublicProviderError(
                        f"EIDR resolver failed closed with HTTP {status}"
                    )
                self._validate_xml_content_type(response.headers)
                declared = _content_length(response.headers)
                if declared is not None and declared > self.max_xml_bytes:
                    raise EidrPublicProviderError(
                        "EIDR resolver XML exceeds the configured byte bound"
                    )
                body = response.read(self.max_xml_bytes + 1)
                if not isinstance(body, bytes) or not body:
                    raise EidrPublicProviderError(
                        "EIDR resolver returned an empty or invalid XML body"
                    )
                if len(body) > self.max_xml_bytes:
                    raise EidrPublicProviderError(
                        "EIDR resolver XML exceeds the configured byte bound"
                    )
                return EidrExactLookupResult(
                    eidr_id=eidr_id,
                    status=EidrLookupStatus.FOUND,
                    xml=body,
                    attempt_count=attempt,
                    retry_count=retries,
                    rate_limit_count=rate_limits,
                )
            except _RetryableStatus as exc:
                if exc.status == 429:
                    rate_limits += 1
                if attempt == self.max_attempts:
                    raise EidrPublicProviderError(
                        f"EIDR resolver exhausted retries after HTTP {exc.status}"
                    ) from exc
                retries += 1
                self.sleeper(self._retry_delay(exc.headers, attempt))
            except (URLError, TimeoutError, ConnectionError, OSError) as exc:
                if attempt == self.max_attempts:
                    raise EidrPublicProviderError(
                        "EIDR resolver request exhausted network retries"
                    ) from exc
                retries += 1
                self.sleeper(self._retry_delay(None, attempt))
            finally:
                if response is not None:
                    with suppress(Exception):
                        response.close()
        raise AssertionError("EIDR public provider retry loop did not terminate")

    @staticmethod
    def _validate_xml_content_type(headers: Any) -> None:
        value = headers.get("Content-Type") if headers is not None else None
        media_type = str(value or "").partition(";")[0].strip().lower()
        if media_type not in {"application/xml", "text/xml"}:
            raise EidrPublicProviderError(
                "EIDR resolver success response must have an XML Content-Type"
            )

    def _throttle(self) -> None:
        with self._throttle_lock:
            now = self.clock()
            if self._last_request_at is not None:
                remaining = self.minimum_request_interval_seconds - (
                    now - self._last_request_at
                )
                if remaining > 0:
                    self.sleeper(remaining)
            self._last_request_at = self.clock()

    def _retry_delay(self, headers: Any, attempt: int) -> float:
        retry_after = _retry_after_seconds(
            headers,
            now=float(self.wall_clock()),
            maximum=self.max_retry_after_seconds,
        )
        if retry_after is not None:
            return retry_after
        return min(
            self.retry_max_backoff_seconds,
            self.retry_initial_backoff_seconds * (2 ** (attempt - 1)),
        )


# A descriptive compatibility alias for callers that spell out the endpoint role.
EidrPublicExactResolutionProvider = EidrPublicProvider
