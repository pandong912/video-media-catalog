"""Bounded HTTPS acquisition for allowlisted official dataset files."""

from __future__ import annotations

import time
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


@dataclass(frozen=True)
class OfficialDownload:
    url: str
    path: Path
    size_bytes: int
    retry_count: int
    rate_limit_count: int


class OfficialHttpsDownloader:
    """Download one fixed-origin file without cookies, redirects, or scraping."""

    def __init__(
        self,
        *,
        allowed_host: str,
        allowed_path_prefix: str,
        user_agent: str,
        timeout_seconds: float = 60,
        max_attempts: int = 5,
        opener: Any | None = None,
        sleeper=time.sleep,
    ) -> None:
        if not allowed_host or not allowed_path_prefix.startswith("/"):
            raise ValueError("official download origin is invalid")
        if not user_agent.strip() or len(user_agent) > 256:
            raise ValueError("a bounded identifying User-Agent is required")
        if timeout_seconds <= 0 or max_attempts < 1:
            raise ValueError("download timeout and attempts must be positive")
        self.allowed_host = allowed_host
        self.allowed_path_prefix = allowed_path_prefix
        self.user_agent = user_agent.strip()
        self.timeout_seconds = timeout_seconds
        self.max_attempts = max_attempts
        self.opener = opener or build_opener(_NoRedirect)
        self.sleeper = sleeper

    def validate_url(self, url: str) -> str:
        parsed = urlsplit(url)
        if (
            parsed.scheme != "https"
            or parsed.hostname != self.allowed_host
            or parsed.port not in {None, 443}
            or not parsed.path.startswith(self.allowed_path_prefix)
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("dataset URL is outside the approved official origin")
        return url

    def download(
        self,
        url: str,
        destination: Path,
        *,
        max_bytes: int,
    ) -> OfficialDownload:
        requested = self.validate_url(url)
        if max_bytes < 1:
            raise ValueError("max_bytes must be positive")
        retries = 0
        rate_limits = 0
        for attempt in range(1, self.max_attempts + 1):
            request = Request(
                requested,
                headers={
                    "Accept": "application/octet-stream",
                    "Accept-Encoding": "identity",
                    "User-Agent": self.user_agent,
                },
            )
            response = None
            try:
                response = self.opener.open(request, timeout=self.timeout_seconds)
                if self.validate_url(response.geturl()) != requested:
                    raise ValueError("official file request was redirected")
                size = self._copy_response(response, destination, max_bytes=max_bytes)
                return OfficialDownload(
                    url=requested,
                    path=destination,
                    size_bytes=size,
                    retry_count=retries,
                    rate_limit_count=rate_limits,
                )
            except HTTPError as exc:
                response = exc
                status = int(exc.code)
                retryable = status in {408, 429, 500, 502, 503, 504}
                if not retryable or attempt == self.max_attempts:
                    raise RuntimeError(
                        f"official dataset request failed with HTTP {status}"
                    ) from exc
                if status == 429:
                    rate_limits += 1
                retries += 1
                self.sleeper(_retry_delay(exc.headers, attempt))
            except URLError as exc:
                if attempt == self.max_attempts:
                    raise RuntimeError("official dataset request failed") from exc
                retries += 1
                self.sleeper(min(30.0, float(2 ** (attempt - 1))))
            finally:
                if response is not None:
                    with suppress(Exception):
                        response.close()
        raise AssertionError("official download retry loop did not terminate")

    @staticmethod
    def _copy_response(response: Any, destination: Path, *, max_bytes: int) -> int:
        destination.parent.mkdir(parents=True, exist_ok=True)
        size = 0
        try:
            with destination.open("wb") as handle:
                while True:
                    chunk = response.read(min(8 * 1024 * 1024, max_bytes - size + 1))
                    if not chunk:
                        break
                    size += len(chunk)
                    if size > max_bytes:
                        raise ValueError(
                            "official dataset exceeds configured byte limit"
                        )
                    handle.write(chunk)
        except Exception:
            destination.unlink(missing_ok=True)
            raise
        if size == 0:
            destination.unlink(missing_ok=True)
            raise ValueError("official dataset response is empty")
        return size


def _retry_delay(headers: Any, attempt: int) -> float:
    value = headers.get("Retry-After") if headers is not None else None
    if value is not None:
        try:
            return min(60.0, max(0.0, float(value)))
        except ValueError:
            pass
    return min(30.0, float(2 ** (attempt - 1)))
