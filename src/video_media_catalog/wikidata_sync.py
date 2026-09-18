"""Immutable streaming synchronization for official dated Wikidata dumps."""

from __future__ import annotations

import hashlib
import math
import re
import time
import uuid
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Any, BinaryIO, Protocol
from urllib.error import HTTPError
from urllib.parse import urljoin, urlsplit
from urllib.request import (
    HTTPRedirectHandler,
    Request,
    build_opener,
)

from pydantic import BaseModel, ConfigDict, Field, field_validator

from video_media_catalog.object_store import S3Location, _etag

DEFAULT_MAX_DUMP_BYTES = 200 * 1024**3
DEFAULT_UPLOAD_PART_BYTES = 64 * 1024**2
DEFAULT_COPY_PART_BYTES = 512 * 1024**2
DEFAULT_RANGE_ATTEMPTS = 5
DEFAULT_RETRY_INITIAL_BACKOFF_SECONDS = 1.0
DEFAULT_RETRY_MAX_BACKOFF_SECONDS = 30.0
MIN_MULTIPART_PART_BYTES = 5 * 1024**2
MAX_MULTIPART_PART_BYTES = 5 * 1024**3
MAX_MULTIPART_PARTS = 10_000
STREAM_CHUNK_BYTES = 1024 * 1024
MAX_CHECKSUM_BYTES = 64 * 1024
OFFICIAL_DUMP_HOST = "dumps.wikimedia.org"

_DUMP_FILENAME = re.compile(r"^wikidata-([0-9]{8})-all\.json\.bz2$")
_SHA1 = re.compile(r"^[0-9a-f]{40}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_STRONG_ETAG = re.compile(r'^"[\x21\x23-\x7e\x80-\xff]*"$')
_CONTENT_RANGE = re.compile(
    r"bytes ([0-9]+)-([0-9]+)/([0-9]+)",
    flags=re.IGNORECASE,
)


class WikidataSyncError(RuntimeError):
    """A stable, non-secret synchronization failure."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class OfficialDump:
    source_url: str
    date: str
    filename: str

    @property
    def checksum_url(self) -> str:
        directory = self.source_url.rsplit("/", 1)[0]
        return f"{directory}/wikidata-{self.date}-sha1sums.txt"


def validate_official_dump_url(url: str) -> OfficialDump:
    """Require the canonical HTTPS URL for one dated official entity dump."""

    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname != OFFICIAL_DUMP_HOST
        or parsed.netloc != OFFICIAL_DUMP_HOST
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(
            "dump URL must be canonical HTTPS dumps.wikimedia.org without "
            "credentials, port, query, or fragment"
        )
    filename = parsed.path.rsplit("/", 1)[-1]
    match = _DUMP_FILENAME.fullmatch(filename)
    if match is None:
        raise ValueError(
            "dump filename must be wikidata-YYYYMMDD-all.json.bz2; latest is forbidden"
        )
    date = match.group(1)
    try:
        datetime.strptime(date, "%Y%m%d")
    except ValueError as exc:
        raise ValueError("dump filename contains an invalid calendar date") from exc
    expected_path = f"/wikidatawiki/entities/{date}/{filename}"
    if parsed.path != expected_path:
        raise ValueError(
            "dump URL path must be /wikidatawiki/entities/YYYYMMDD/<dated filename>"
        )
    return OfficialDump(url, date, filename)


def _validate_checksum_url(url: str, dump: OfficialDump) -> None:
    if url != dump.checksum_url:
        raise ValueError(
            "checksum redirect must preserve the canonical dated sha1sums URL"
        )
    parsed = urlsplit(url)
    if parsed.hostname != OFFICIAL_DUMP_HOST:
        raise ValueError("checksum URL host is not allowlisted")


class HttpResponse(Protocol):
    status: int
    headers: Mapping[str, str]
    body: BinaryIO


class HttpTransport(Protocol):
    def open(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
    ) -> HttpResponse: ...


@dataclass
class StreamingHttpResponse:
    status: int
    headers: Mapping[str, str]
    body: BinaryIO


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(
        self,
        req: Request,
        fp: BinaryIO,
        code: int,
        msg: str,
        headers: Mapping[str, str],
        newurl: str,
    ) -> None:
        return None


class StdlibHttpTransport:
    """HTTPS transport with redirects deliberately exposed to the caller."""

    def __init__(self, *, timeout_seconds: float = 60.0) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self.timeout_seconds = timeout_seconds
        self._opener = build_opener(_NoRedirect)

    def open(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
    ) -> StreamingHttpResponse:
        request_headers = {
            "User-Agent": "video-media-catalog-wikidata-sync/1.0",
        }
        request_headers.update(headers or {})
        request_headers["Accept-Encoding"] = "identity"
        request = Request(
            url,
            method=method,
            headers=request_headers,
        )
        try:
            response = self._opener.open(request, timeout=self.timeout_seconds)
        except HTTPError as exc:
            response = exc
        return StreamingHttpResponse(
            status=int(response.status),
            headers=response.headers,
            body=response,
        )


def _header(headers: Mapping[str, str], name: str) -> str | None:
    wanted = name.lower()
    for key, value in headers.items():
        if key.lower() == wanted:
            return str(value)
    return None


def _open_with_redirects(
    http: HttpTransport,
    url: str,
    *,
    validate: Callable[[str], None],
    max_redirects: int,
    method: str = "GET",
    headers: Mapping[str, str] | None = None,
    expected_status: int | None = 200,
    require_identity_encoding: bool = True,
) -> HttpResponse:
    current = url
    for redirect_count in range(max_redirects + 1):
        validate(current)
        response = http.open(method, current, headers=headers)
        if response.status in {301, 302, 303, 307, 308}:
            location = _header(response.headers, "Location")
            with suppress(Exception):
                response.body.close()
            if not location:
                raise WikidataSyncError(
                    "INVALID_REDIRECT", "official response omitted redirect location"
                )
            if redirect_count == max_redirects:
                raise WikidataSyncError(
                    "TOO_MANY_REDIRECTS", "official response exceeded redirect limit"
                )
            current = urljoin(current, location)
            try:
                validate(current)
            except ValueError as exc:
                raise WikidataSyncError(
                    "UNSAFE_REDIRECT",
                    "official response redirected outside the strict allowlist",
                ) from exc
            continue
        if expected_status is not None and response.status != expected_status:
            with suppress(Exception):
                response.body.close()
            raise WikidataSyncError(
                "UPSTREAM_HTTP_ERROR",
                f"official server returned HTTP {response.status}",
            )
        encoding = (
            (_header(response.headers, "Content-Encoding") or "identity")
            .strip()
            .lower()
        )
        if require_identity_encoding and encoding != "identity":
            with suppress(Exception):
                response.body.close()
            raise WikidataSyncError(
                "UNEXPECTED_CONTENT_ENCODING",
                "official response must not apply HTTP content encoding",
            )
        return response
    raise AssertionError("redirect loop terminated unexpectedly")


def fetch_official_sha1(
    dump: OfficialDump,
    *,
    http: HttpTransport,
    max_redirects: int = 3,
) -> str:
    """Fetch and strictly parse the official sidecar SHA-1."""

    response = _open_with_redirects(
        http,
        dump.checksum_url,
        validate=lambda value: _validate_checksum_url(value, dump),
        max_redirects=max_redirects,
    )
    try:
        payload = response.body.read(MAX_CHECKSUM_BYTES + 1)
    finally:
        response.body.close()
    if len(payload) > MAX_CHECKSUM_BYTES:
        raise WikidataSyncError(
            "INVALID_UPSTREAM_SHA1", "official SHA-1 sidecar exceeds its size limit"
        )
    try:
        text = payload.decode("ascii").strip()
    except UnicodeDecodeError as exc:
        raise WikidataSyncError(
            "INVALID_UPSTREAM_SHA1", "official SHA-1 sidecar is not ASCII"
        ) from exc
    matches: list[str] = []
    for line in text.splitlines():
        match = re.fullmatch(r"([0-9a-fA-F]{40})\s+\*?([^\s]+)", line.strip())
        if match is None:
            raise WikidataSyncError(
                "INVALID_UPSTREAM_SHA1",
                "official SHA-1 sums file has invalid syntax",
            )
        if match.group(2).rsplit("/", 1)[-1] == dump.filename:
            matches.append(match.group(1).lower())
    if len(matches) != 1:
        raise WikidataSyncError(
            "INVALID_UPSTREAM_SHA1",
            "official SHA-1 sums file must contain the dump filename exactly once",
        )
    return matches[0]


@dataclass(frozen=True)
class _OfficialDumpMetadata:
    size: int
    validator_name: str
    validator_value: str


def _valid_last_modified(value: str) -> bool:
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError, OverflowError):
        return False
    return parsed.tzinfo is not None and parsed.utcoffset() is not None


def _head_official_dump(
    dump: OfficialDump,
    *,
    http: HttpTransport,
    max_bytes: int,
    max_redirects: int,
) -> _OfficialDumpMetadata:
    response = _open_with_redirects(
        http,
        dump.source_url,
        validate=lambda value: validate_official_dump_url(value),
        max_redirects=max_redirects,
        method="HEAD",
    )
    try:
        content_length = (_header(response.headers, "Content-Length") or "").strip()
        if re.fullmatch(r"[0-9]+", content_length) is None:
            raise WikidataSyncError(
                "INVALID_CONTENT_LENGTH",
                "official HEAD Content-Length is missing or invalid",
            )
        size = int(content_length)
        if size < 1:
            raise WikidataSyncError(
                "INVALID_CONTENT_LENGTH",
                "official HEAD Content-Length must be positive",
            )
        if size > max_bytes:
            raise WikidataSyncError(
                "OBJECT_TOO_LARGE",
                "official HEAD Content-Length exceeds the configured limit",
            )

        accept_ranges = {
            token.strip().lower()
            for token in (_header(response.headers, "Accept-Ranges") or "").split(",")
        }
        if "bytes" not in accept_ranges:
            raise WikidataSyncError(
                "UPSTREAM_RANGE_UNSUPPORTED",
                "official HEAD response does not advertise byte ranges",
            )

        etag = (_header(response.headers, "ETag") or "").strip()
        if _STRONG_ETAG.fullmatch(etag):
            return _OfficialDumpMetadata(size, "ETag", etag)

        last_modified = (_header(response.headers, "Last-Modified") or "").strip()
        if last_modified and _valid_last_modified(last_modified):
            return _OfficialDumpMetadata(
                size,
                "Last-Modified",
                last_modified,
            )
        raise WikidataSyncError(
            "MISSING_UPSTREAM_VALIDATOR",
            "official HEAD requires a strong ETag or valid Last-Modified",
        )
    finally:
        response.body.close()


class WikidataSyncResult(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
        str_strip_whitespace=True,
    )

    uri: str
    size_bytes: int = Field(gt=0, alias="sizeBytes")
    sha256: str
    etag: str
    object_version: str = Field(alias="versionId")
    source_url: str = Field(alias="sourceUrl")
    upstream_sha1: str = Field(alias="upstreamSha1")
    dump_date: str = Field(alias="dumpDate")
    reused: bool = False

    @field_validator("sha256")
    @classmethod
    def validate_sha256(cls, value: str) -> str:
        normalized = value.lower()
        if _SHA256.fullmatch(normalized) is None:
            raise ValueError("sha256 must contain 64 lowercase hex digits")
        return normalized

    @field_validator("upstream_sha1")
    @classmethod
    def validate_sha1(cls, value: str) -> str:
        normalized = value.lower()
        if _SHA1.fullmatch(normalized) is None:
            raise ValueError("upstream_sha1 must contain 40 lowercase hex digits")
        return normalized

    @field_validator("etag", "object_version")
    @classmethod
    def require_immutable_metadata(cls, value: str) -> str:
        if not value:
            raise ValueError("S3 VersionId and ETag are required")
        return value


def _error_code(exc: BaseException) -> tuple[str, int | None]:
    response = getattr(exc, "response", {})
    code = str(response.get("Error", {}).get("Code", ""))
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return code, status if isinstance(status, int) else None


def _is_missing(exc: BaseException) -> bool:
    code, status = _error_code(exc)
    return code in {"404", "NoSuchKey", "NotFound"} or status == 404


def _is_precondition_failure(exc: BaseException) -> bool:
    code, status = _error_code(exc)
    return code in {"PreconditionFailed", "ConditionalRequestConflict"} or status in {
        409,
        412,
    }


def _require_part_size(value: int, *, name: str) -> None:
    if not MIN_MULTIPART_PART_BYTES <= value <= MAX_MULTIPART_PART_BYTES:
        raise ValueError(f"{name} must be between 5 MiB and 5 GiB")


def _required_metadata(response: Mapping[str, Any]) -> tuple[str, str]:
    etag = _etag(response.get("ETag"))
    version = response.get("VersionId")
    if etag is None or not isinstance(version, str) or not version or version == "null":
        raise WikidataSyncError(
            "S3_VERSIONING_REQUIRED",
            "destination bucket must return a non-empty ETag and VersionId",
        )
    return etag, version


def _result_from_head(
    *,
    location: S3Location,
    response: Mapping[str, Any],
    dump: OfficialDump,
    upstream_sha1: str,
    max_bytes: int,
    reused: bool,
) -> WikidataSyncResult:
    size = int(response.get("ContentLength", -1))
    metadata = {
        str(key).lower(): str(value)
        for key, value in (response.get("Metadata") or {}).items()
    }
    sha256 = metadata.get("sha256", "").lower()
    expected = {
        "upstream-sha1": upstream_sha1,
        "source-url": dump.source_url,
        "dump-date": dump.date,
    }
    if (
        size <= 0
        or size > max_bytes
        or _SHA256.fullmatch(sha256) is None
        or any(metadata.get(key) != value for key, value in expected.items())
    ):
        raise WikidataSyncError(
            "IMMUTABLE_OBJECT_CONFLICT",
            "destination exists but does not match the immutable dump identity",
        )
    etag, version = _required_metadata(response)
    return WikidataSyncResult(
        uri=location.uri,
        size_bytes=size,
        sha256=sha256,
        etag=etag,
        object_version=version,
        source_url=dump.source_url,
        upstream_sha1=upstream_sha1,
        dump_date=dump.date,
        reused=reused,
    )


def _head_existing(
    s3: Any,
    location: S3Location,
    *,
    dump: OfficialDump,
    upstream_sha1: str,
    max_bytes: int,
) -> WikidataSyncResult | None:
    try:
        response = s3.head_object(
            Bucket=location.bucket,
            Key=location.key,
            ChecksumMode="ENABLED",
        )
    except Exception as exc:
        if _is_missing(exc):
            return None
        raise
    result = _result_from_head(
        location=location,
        response=response,
        dump=dump,
        upstream_sha1=upstream_sha1,
        max_bytes=max_bytes,
        reused=True,
    )
    downloaded = s3.get_object(
        Bucket=location.bucket,
        Key=location.key,
        VersionId=result.object_version,
        ChecksumMode="ENABLED",
    )
    body = downloaded.get("Body")
    if body is None or not hasattr(body, "read"):
        raise WikidataSyncError(
            "IMMUTABLE_OBJECT_CONFLICT",
            "existing destination cannot be content-verified",
        )
    sha1 = hashlib.sha1(usedforsecurity=False)
    sha256 = hashlib.sha256()
    size = 0
    try:
        while chunk := body.read(STREAM_CHUNK_BYTES):
            size += len(chunk)
            if size > max_bytes:
                raise WikidataSyncError(
                    "OBJECT_TOO_LARGE",
                    "existing destination exceeds configured size limit",
                )
            sha1.update(chunk)
            sha256.update(chunk)
    finally:
        body.close()
    if (
        size != result.size_bytes
        or sha1.hexdigest() != upstream_sha1
        or sha256.hexdigest() != result.sha256
    ):
        raise WikidataSyncError(
            "IMMUTABLE_OBJECT_CONFLICT",
            "existing destination bytes differ from immutable identity",
        )
    return result


def _abort(s3: Any, location: S3Location, upload_id: str | None) -> None:
    if upload_id is None:
        return
    with suppress(Exception):
        s3.abort_multipart_upload(
            Bucket=location.bucket,
            Key=location.key,
            UploadId=upload_id,
        )


class _RetryableRangeError(Exception):
    """An upstream range failure that is safe to retry from its first byte."""


def _validate_range_headers(
    response: HttpResponse,
    *,
    start: int,
    end: int,
    metadata: _OfficialDumpMetadata,
) -> None:
    encoding = (
        (_header(response.headers, "Content-Encoding") or "identity").strip().lower()
    )
    if encoding != "identity":
        raise WikidataSyncError(
            "UNEXPECTED_CONTENT_ENCODING",
            "official range response must not apply HTTP content encoding",
        )

    expected_length = end - start + 1
    content_length = (_header(response.headers, "Content-Length") or "").strip()
    if re.fullmatch(r"[0-9]+", content_length) is None:
        raise WikidataSyncError(
            "INVALID_CONTENT_LENGTH",
            "official range Content-Length is missing or invalid",
        )
    if int(content_length) != expected_length:
        raise WikidataSyncError(
            "UPSTREAM_RANGE_PROTOCOL_ERROR",
            "official range Content-Length does not match the requested bytes",
        )

    content_range = (_header(response.headers, "Content-Range") or "").strip()
    match = _CONTENT_RANGE.fullmatch(content_range)
    if match is None or tuple(map(int, match.groups())) != (
        start,
        end,
        metadata.size,
    ):
        raise WikidataSyncError(
            "UPSTREAM_RANGE_PROTOCOL_ERROR",
            "official Content-Range does not exactly match the requested bytes",
        )

    validator = (_header(response.headers, metadata.validator_name) or "").strip()
    if validator != metadata.validator_value:
        raise WikidataSyncError(
            "UPSTREAM_VALIDATOR_CHANGED",
            "official dump validator changed after HEAD",
        )


def _read_range_once(
    *,
    http: HttpTransport,
    dump: OfficialDump,
    metadata: _OfficialDumpMetadata,
    start: int,
    end: int,
    max_redirects: int,
) -> bytearray:
    request_headers = {
        "Range": f"bytes={start}-{end}",
        "If-Range": metadata.validator_value,
    }
    try:
        response = _open_with_redirects(
            http,
            dump.source_url,
            validate=lambda value: validate_official_dump_url(value),
            max_redirects=max_redirects,
            headers=request_headers,
            expected_status=None,
            require_identity_encoding=False,
        )
    except WikidataSyncError:
        raise
    except Exception as exc:
        raise _RetryableRangeError("official range request failed") from exc

    try:
        if response.status in {408, 429} or 500 <= response.status <= 599:
            raise _RetryableRangeError(
                f"official server returned retryable HTTP {response.status}"
            )
        if 400 <= response.status <= 499:
            raise WikidataSyncError(
                "UPSTREAM_HTTP_ERROR",
                f"official range request returned HTTP {response.status}",
            )
        if response.status != 206:
            raise WikidataSyncError(
                "UPSTREAM_RANGE_PROTOCOL_ERROR",
                f"official range request returned HTTP {response.status}, not 206",
            )

        _validate_range_headers(
            response,
            start=start,
            end=end,
            metadata=metadata,
        )
        expected_length = end - start + 1
        buffer = bytearray()
        while len(buffer) <= expected_length:
            read_size = min(
                STREAM_CHUNK_BYTES,
                expected_length + 1 - len(buffer),
            )
            try:
                chunk = response.body.read(read_size)
            except Exception as exc:
                raise _RetryableRangeError(
                    "official range response read failed"
                ) from exc
            if not chunk:
                break
            if not isinstance(chunk, bytes):
                raise WikidataSyncError(
                    "UPSTREAM_READ_ERROR",
                    "official response returned non-byte data",
                )
            buffer.extend(chunk)
        if len(buffer) < expected_length:
            raise _RetryableRangeError("official range response ended early")
        if len(buffer) > expected_length:
            raise WikidataSyncError(
                "UPSTREAM_RANGE_PROTOCOL_ERROR",
                "official range response exceeded its declared length",
            )
        return buffer
    finally:
        with suppress(Exception):
            response.body.close()


def _read_range_with_retries(
    *,
    http: HttpTransport,
    dump: OfficialDump,
    metadata: _OfficialDumpMetadata,
    start: int,
    end: int,
    max_redirects: int,
    attempts: int,
    initial_backoff_seconds: float,
    max_backoff_seconds: float,
    sleeper: Callable[[float], None],
) -> bytearray:
    delay = initial_backoff_seconds
    last_error: _RetryableRangeError | None = None
    for attempt in range(1, attempts + 1):
        try:
            return _read_range_once(
                http=http,
                dump=dump,
                metadata=metadata,
                start=start,
                end=end,
                max_redirects=max_redirects,
            )
        except _RetryableRangeError as exc:
            last_error = exc
        if attempt < attempts:
            sleeper(delay)
            delay = min(max_backoff_seconds, delay * 2)
    raise WikidataSyncError(
        "UPSTREAM_RANGE_RETRIES_EXHAUSTED",
        (f"official byte range {start}-{end} failed after {attempts} attempts"),
    ) from last_error


def _upload_ranges_to_staging(
    *,
    s3: Any,
    location: S3Location,
    http: HttpTransport,
    dump: OfficialDump,
    metadata: _OfficialDumpMetadata,
    upstream_sha1: str,
    part_bytes: int,
    max_redirects: int,
    range_attempts: int,
    retry_initial_backoff_seconds: float,
    retry_max_backoff_seconds: float,
    sleeper: Callable[[float], None],
) -> tuple[int, str, Mapping[str, Any]]:
    part_count = (metadata.size + part_bytes - 1) // part_bytes
    if part_count > MAX_MULTIPART_PARTS:
        raise WikidataSyncError(
            "TOO_MANY_PARTS",
            "dump exceeds the multipart part-count limit",
        )
    response = s3.create_multipart_upload(
        Bucket=location.bucket,
        Key=location.key,
        ContentType="application/x-bzip2",
        Metadata={
            "source-url": dump.source_url,
            "upstream-sha1": upstream_sha1,
            "dump-date": dump.date,
        },
    )
    upload_id = str(response["UploadId"])
    parts: list[dict[str, Any]] = []
    sha1 = hashlib.sha1(usedforsecurity=False)
    sha256 = hashlib.sha256()
    try:
        for start in range(0, metadata.size, part_bytes):
            end = min(metadata.size, start + part_bytes) - 1
            part = _read_range_with_retries(
                http=http,
                dump=dump,
                metadata=metadata,
                start=start,
                end=end,
                max_redirects=max_redirects,
                attempts=range_attempts,
                initial_backoff_seconds=retry_initial_backoff_seconds,
                max_backoff_seconds=retry_max_backoff_seconds,
                sleeper=sleeper,
            )
            if start == 0 and not part.startswith(b"BZh"):
                raise WikidataSyncError(
                    "INVALID_DUMP_CONTENT",
                    "official dump is not a non-empty bzip2 stream",
                )
            sha1.update(part)
            sha256.update(part)
            part_number = len(parts) + 1
            uploaded = s3.upload_part(
                Bucket=location.bucket,
                Key=location.key,
                UploadId=upload_id,
                PartNumber=part_number,
                Body=part,
                ContentLength=len(part),
            )
            raw_etag = uploaded.get("ETag")
            if _etag(raw_etag) is None:
                raise WikidataSyncError(
                    "S3_MULTIPART_ERROR", "S3 upload_part omitted ETag"
                )
            parts.append({"PartNumber": part_number, "ETag": str(raw_etag)})
        if sha1.hexdigest() != upstream_sha1:
            raise WikidataSyncError(
                "UPSTREAM_SHA1_MISMATCH",
                "downloaded dump differs from the official SHA-1",
            )
        completed = s3.complete_multipart_upload(
            Bucket=location.bucket,
            Key=location.key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
        upload_id = None
        return metadata.size, sha256.hexdigest(), completed
    finally:
        _abort(s3, location, upload_id)


def _copy_staging_to_final(
    *,
    s3: Any,
    staging: S3Location,
    staging_version: str | None,
    destination: S3Location,
    size: int,
    sha256: str,
    dump: OfficialDump,
    upstream_sha1: str,
    copy_part_bytes: int,
    max_bytes: int,
) -> WikidataSyncResult:
    created = s3.create_multipart_upload(
        Bucket=destination.bucket,
        Key=destination.key,
        ContentType="application/x-bzip2",
        Metadata={
            "sha256": sha256,
            "source-url": dump.source_url,
            "upstream-sha1": upstream_sha1,
            "dump-date": dump.date,
        },
    )
    upload_id = str(created["UploadId"])
    source: dict[str, str] = {"Bucket": staging.bucket, "Key": staging.key}
    if staging_version:
        source["VersionId"] = staging_version
    parts: list[dict[str, Any]] = []
    published_version: str | None = None
    try:
        for start in range(0, size, copy_part_bytes):
            part_number = len(parts) + 1
            if part_number > MAX_MULTIPART_PARTS:
                raise WikidataSyncError(
                    "TOO_MANY_PARTS", "dump exceeds the multipart copy part limit"
                )
            end = min(size, start + copy_part_bytes) - 1
            copied = s3.upload_part_copy(
                Bucket=destination.bucket,
                Key=destination.key,
                UploadId=upload_id,
                PartNumber=part_number,
                CopySource=source,
                CopySourceRange=f"bytes={start}-{end}",
            )
            copy_result = copied.get("CopyPartResult") or {}
            raw_etag = copy_result.get("ETag")
            if _etag(raw_etag) is None:
                raise WikidataSyncError(
                    "S3_MULTIPART_ERROR", "S3 upload_part_copy omitted ETag"
                )
            parts.append({"PartNumber": part_number, "ETag": str(raw_etag)})
        try:
            completed = s3.complete_multipart_upload(
                Bucket=destination.bucket,
                Key=destination.key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
                IfNoneMatch="*",
            )
            published_version = completed.get("VersionId")
            upload_id = None
        except Exception as exc:
            if not _is_precondition_failure(exc):
                raise
            _abort(s3, destination, upload_id)
            upload_id = None
            existing = _head_existing(
                s3,
                destination,
                dump=dump,
                upstream_sha1=upstream_sha1,
                max_bytes=max_bytes,
            )
            if (
                existing is None
                or existing.sha256 != sha256
                or existing.size_bytes != size
            ):
                raise WikidataSyncError(
                    "IMMUTABLE_OBJECT_CONFLICT",
                    "destination was concurrently published with different content",
                ) from exc
            return existing
        if (
            not isinstance(published_version, str)
            or not published_version
            or published_version == "null"
        ):
            raise WikidataSyncError(
                "S3_VERSIONING_REQUIRED",
                "destination bucket did not return a VersionId",
            )
        head_request: dict[str, Any] = {
            "Bucket": destination.bucket,
            "Key": destination.key,
            "VersionId": published_version,
            "ChecksumMode": "ENABLED",
        }
        head = s3.head_object(**head_request)
        result = _result_from_head(
            location=destination,
            response=head,
            dump=dump,
            upstream_sha1=upstream_sha1,
            max_bytes=max_bytes,
            reused=False,
        )
        if result.sha256 != sha256 or result.size_bytes != size:
            raise WikidataSyncError(
                "S3_UPLOAD_MISMATCH", "published dump metadata failed verification"
            )
        return result
    finally:
        _abort(s3, destination, upload_id)


def sync_official_dump(
    *,
    source_url: str,
    destination_prefix: str,
    s3: Any,
    http: HttpTransport,
    max_bytes: int = DEFAULT_MAX_DUMP_BYTES,
    upload_part_bytes: int = DEFAULT_UPLOAD_PART_BYTES,
    copy_part_bytes: int = DEFAULT_COPY_PART_BYTES,
    max_redirects: int = 3,
    range_attempts: int = DEFAULT_RANGE_ATTEMPTS,
    retry_initial_backoff_seconds: float = (DEFAULT_RETRY_INITIAL_BACKOFF_SECONDS),
    retry_max_backoff_seconds: float = DEFAULT_RETRY_MAX_BACKOFF_SECONDS,
    sleeper: Callable[[float], None] | None = None,
    staging_token_factory: Callable[[], str] | None = None,
) -> WikidataSyncResult:
    """Range-download, verify, and immutably publish one official dated dump."""

    dump = validate_official_dump_url(source_url)
    prefix = S3Location.parse(destination_prefix.rstrip("/") + "/placeholder")
    if max_bytes < 1:
        raise ValueError("max_bytes must be positive")
    _require_part_size(upload_part_bytes, name="upload_part_bytes")
    _require_part_size(copy_part_bytes, name="copy_part_bytes")
    if max_redirects < 0:
        raise ValueError("max_redirects must not be negative")
    if range_attempts < 1:
        raise ValueError("range_attempts must be at least 1")
    if (
        not math.isfinite(retry_initial_backoff_seconds)
        or retry_initial_backoff_seconds <= 0
    ):
        raise ValueError("retry_initial_backoff_seconds must be finite and positive")
    if not math.isfinite(retry_max_backoff_seconds) or retry_max_backoff_seconds <= 0:
        raise ValueError("retry_max_backoff_seconds must be finite and positive")
    if retry_initial_backoff_seconds > retry_max_backoff_seconds:
        raise ValueError(
            "retry_initial_backoff_seconds must not exceed retry_max_backoff_seconds"
        )

    upstream_sha1 = fetch_official_sha1(
        dump,
        http=http,
        max_redirects=max_redirects,
    )
    base_key = prefix.key.rsplit("/", 1)[0]
    destination = S3Location(
        prefix.bucket,
        (
            f"{base_key}/date={dump.date}/sha1={upstream_sha1}/{dump.filename}"
            if base_key
            else f"date={dump.date}/sha1={upstream_sha1}/{dump.filename}"
        ),
    )
    existing = _head_existing(
        s3,
        destination,
        dump=dump,
        upstream_sha1=upstream_sha1,
        max_bytes=max_bytes,
    )
    if existing is not None:
        return existing

    metadata = _head_official_dump(
        dump,
        http=http,
        max_bytes=max_bytes,
        max_redirects=max_redirects,
    )

    token_factory = staging_token_factory or (lambda: uuid.uuid4().hex)
    range_sleeper = time.sleep if sleeper is None else sleeper
    staging = S3Location(
        prefix.bucket,
        (
            f"{base_key}/_staging/wikidata-sync/{dump.date}/{token_factory()}"
            if base_key
            else f"_staging/wikidata-sync/{dump.date}/{token_factory()}"
        ),
    )
    staging_completed = False
    staging_version: str | None = None
    try:
        size, sha256, completed = _upload_ranges_to_staging(
            s3=s3,
            location=staging,
            http=http,
            dump=dump,
            metadata=metadata,
            upstream_sha1=upstream_sha1,
            part_bytes=upload_part_bytes,
            max_redirects=max_redirects,
            range_attempts=range_attempts,
            retry_initial_backoff_seconds=retry_initial_backoff_seconds,
            retry_max_backoff_seconds=retry_max_backoff_seconds,
            sleeper=range_sleeper,
        )
        staging_completed = True
        staging_version = completed.get("VersionId")
        return _copy_staging_to_final(
            s3=s3,
            staging=staging,
            staging_version=staging_version,
            destination=destination,
            size=size,
            sha256=sha256,
            dump=dump,
            upstream_sha1=upstream_sha1,
            copy_part_bytes=copy_part_bytes,
            max_bytes=max_bytes,
        )
    finally:
        if staging_completed:
            request: dict[str, Any] = {
                "Bucket": staging.bucket,
                "Key": staging.key,
            }
            if staging_version:
                request["VersionId"] = staging_version
            with suppress(Exception):
                s3.delete_object(**request)
