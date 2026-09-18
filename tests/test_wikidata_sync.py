from __future__ import annotations

import hashlib
import io
from collections import Counter
from collections.abc import Mapping
from typing import ClassVar

import pytest

from video_media_catalog.wikidata_sync import (
    DEFAULT_RANGE_ATTEMPTS,
    DEFAULT_RETRY_INITIAL_BACKOFF_SECONDS,
    DEFAULT_RETRY_MAX_BACKOFF_SECONDS,
    MIN_MULTIPART_PART_BYTES,
    StreamingHttpResponse,
    WikidataSyncError,
    sync_official_dump,
    validate_official_dump_url,
)
from video_media_catalog.wikidata_sync_cli import build_parser

SOURCE_URL = (
    "https://dumps.wikimedia.org/wikidatawiki/entities/20260901/"
    "wikidata-20260901-all.json.bz2"
)
CHECKSUM_URL = (
    "https://dumps.wikimedia.org/wikidatawiki/entities/20260901/"
    "wikidata-20260901-sha1sums.txt"
)


class FakeHttp:
    def __init__(
        self,
        responses: dict[
            tuple[str, str, str | None],
            list[tuple[int, dict[str, str], bytes]],
        ],
    ) -> None:
        self.responses = responses
        self.opens: Counter[tuple[str, str, str | None]] = Counter()
        self.calls: list[tuple[str, str, dict[str, str]]] = []

    def open(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
    ) -> StreamingHttpResponse:
        request_headers = dict(headers or {})
        key = (method, url, request_headers.get("Range"))
        self.opens[key] += 1
        self.calls.append((method, url, request_headers))
        scripted = self.responses[key]
        status, response_headers, payload = (
            scripted.pop(0) if len(scripted) > 1 else scripted[0]
        )
        return StreamingHttpResponse(
            status,
            response_headers,
            io.BytesIO(payload),
        )


class MissingObject(Exception):
    response: ClassVar[dict] = {
        "Error": {"Code": "NoSuchKey"},
        "ResponseMetadata": {"HTTPStatusCode": 404},
    }


class PreconditionFailure(Exception):
    response: ClassVar[dict] = {
        "Error": {"Code": "PreconditionFailed"},
        "ResponseMetadata": {"HTTPStatusCode": 412},
    }


class FakeS3:
    def __init__(self) -> None:
        self.objects: dict[str, dict] = {}
        self.uploads: dict[str, dict] = {}
        self.aborted: list[str] = []
        self.sequence = 0

    def head_object(self, *, Key, **_request):
        if Key not in self.objects:
            raise MissingObject
        return dict(self.objects[Key])

    def get_object(self, *, Key, **_request):
        if Key not in self.objects:
            raise MissingObject
        value = dict(self.objects[Key])
        value["Body"] = io.BytesIO(value["Body"])
        return value

    def create_multipart_upload(self, *, Key, Metadata, **_request):
        self.sequence += 1
        upload_id = f"upload-{self.sequence}"
        self.uploads[upload_id] = {
            "key": Key,
            "metadata": Metadata,
            "parts": {},
        }
        return {"UploadId": upload_id}

    def upload_part(self, *, UploadId, PartNumber, Body, **_request):
        self.uploads[UploadId]["parts"][PartNumber] = bytes(Body)
        return {"ETag": f'"part-{PartNumber}"'}

    def upload_part_copy(
        self,
        *,
        UploadId,
        PartNumber,
        CopySource,
        CopySourceRange,
        **_request,
    ):
        source = self.objects[CopySource["Key"]]["Body"]
        start, end = (
            int(value) for value in CopySourceRange.removeprefix("bytes=").split("-")
        )
        self.uploads[UploadId]["parts"][PartNumber] = source[start : end + 1]
        return {"CopyPartResult": {"ETag": f'"copy-{PartNumber}"'}}

    def complete_multipart_upload(
        self,
        *,
        UploadId,
        MultipartUpload,
        IfNoneMatch=None,
        **_request,
    ):
        upload = self.uploads.pop(UploadId)
        key = upload["key"]
        if IfNoneMatch == "*" and key in self.objects:
            raise PreconditionFailure
        body = b"".join(
            upload["parts"][part["PartNumber"]] for part in MultipartUpload["Parts"]
        )
        version = f"version-{len(self.objects) + 1}"
        value = {
            "Body": body,
            "ContentLength": len(body),
            "ETag": f'"etag-{len(body)}"',
            "VersionId": version,
            "Metadata": upload["metadata"],
        }
        self.objects[key] = value
        return {"ETag": value["ETag"], "VersionId": version}

    def abort_multipart_upload(self, *, UploadId, **_request):
        self.aborted.append(UploadId)
        self.uploads.pop(UploadId, None)

    def delete_object(self, *, Key, **_request):
        self.objects.pop(Key, None)


@pytest.mark.parametrize(
    "url",
    [
        SOURCE_URL.replace("https://", "http://"),
        SOURCE_URL.replace("dumps.wikimedia.org", "evil.example"),
        SOURCE_URL.replace("20260901-all", "latest-all"),
        SOURCE_URL + "?download=1",
        SOURCE_URL + "#fragment",
        SOURCE_URL.replace(
            "dumps.wikimedia.org",
            "user:password@dumps.wikimedia.org",
        ),
        SOURCE_URL.replace("/20260901/", "/latest/"),
    ],
)
def test_official_url_rejects_unsafe_variants(url: str) -> None:
    with pytest.raises(ValueError):
        validate_official_dump_url(url)


def test_redirect_to_non_allowlisted_host_is_rejected() -> None:
    http = FakeHttp(
        {
            ("GET", CHECKSUM_URL, None): [
                (
                    302,
                    {"Location": "https://evil.example/checksum"},
                    b"",
                )
            ]
        }
    )
    with pytest.raises(WikidataSyncError, match="allowlist"):
        sync_official_dump(
            source_url=SOURCE_URL,
            destination_prefix="s3://catalog/raw",
            s3=FakeS3(),
            http=http,
        )


def _range_headers(
    *,
    start: int,
    end: int,
    total: int,
    validator_name: str,
    validator_value: str,
) -> dict[str, str]:
    return {
        "Content-Length": str(end - start + 1),
        "Content-Range": f"bytes {start}-{end}/{total}",
        validator_name: validator_value,
    }


def _http(
    payload: bytes,
    *,
    sha1: str | None = None,
    part_bytes: int = MIN_MULTIPART_PART_BYTES,
    validator_name: str = "ETag",
    validator_value: str = '"dump-v1"',
    head_headers: dict[str, str] | None = None,
    range_responses: dict[
        str,
        list[tuple[int, dict[str, str], bytes]],
    ]
    | None = None,
) -> FakeHttp:
    digest = sha1 or hashlib.sha1(payload, usedforsecurity=False).hexdigest()
    effective_head_headers = head_headers or {
        "Content-Length": str(len(payload)),
        "Accept-Ranges": "bytes",
        validator_name: validator_value,
    }
    responses: dict[
        tuple[str, str, str | None],
        list[tuple[int, dict[str, str], bytes]],
    ] = {
        ("GET", CHECKSUM_URL, None): [
            (
                200,
                {},
                f"{digest}  wikidata-20260901-all.json.bz2\n".encode(),
            )
        ],
        ("HEAD", SOURCE_URL, None): [(200, effective_head_headers, b"")],
    }
    for start in range(0, len(payload), part_bytes):
        end = min(len(payload), start + part_bytes) - 1
        requested = f"bytes={start}-{end}"
        default = (
            206,
            _range_headers(
                start=start,
                end=end,
                total=len(payload),
                validator_name=validator_name,
                validator_value=validator_value,
            ),
            payload[start : end + 1],
        )
        responses[("GET", SOURCE_URL, requested)] = (range_responses or {}).get(
            requested
        ) or [default]
    return FakeHttp(responses)


def test_streaming_multipart_hashes_and_reuses_complete_object() -> None:
    payload = b"BZh" + b"fixture-dump-content"
    http = _http(payload)
    s3 = FakeS3()

    first = sync_official_dump(
        source_url=SOURCE_URL,
        destination_prefix="s3://catalog/raw",
        s3=s3,
        http=http,
        upload_part_bytes=MIN_MULTIPART_PART_BYTES,
        copy_part_bytes=MIN_MULTIPART_PART_BYTES,
        staging_token_factory=lambda: "fixed",
    )
    second = sync_official_dump(
        source_url=SOURCE_URL,
        destination_prefix="s3://catalog/raw",
        s3=s3,
        http=http,
        upload_part_bytes=MIN_MULTIPART_PART_BYTES,
        copy_part_bytes=MIN_MULTIPART_PART_BYTES,
    )

    assert first.sha256 == hashlib.sha256(payload).hexdigest()
    assert first.size_bytes == len(payload)
    assert first.etag and first.object_version
    assert first.source_url == SOURCE_URL
    assert not first.reused
    assert second.reused
    assert second.uri == first.uri
    requested = f"bytes=0-{len(payload) - 1}"
    assert http.opens[("HEAD", SOURCE_URL, None)] == 1
    assert http.opens[("GET", SOURCE_URL, requested)] == 1
    assert [method for method, url, _headers in http.calls if url == SOURCE_URL] == [
        "HEAD",
        "GET",
    ]
    assert list(s3.objects) == [
        (
            "raw/date=20260901/sha1="
            f"{hashlib.sha1(payload, usedforsecurity=False).hexdigest()}/"
            "wikidata-20260901-all.json.bz2"
        )
    ]


def test_sha1_mismatch_aborts_multipart_without_publishing() -> None:
    payload = b"BZh" + b"corrupt"
    s3 = FakeS3()

    with pytest.raises(WikidataSyncError, match="SHA-1"):
        sync_official_dump(
            source_url=SOURCE_URL,
            destination_prefix="s3://catalog/raw",
            s3=s3,
            http=_http(payload, sha1="0" * 40),
            upload_part_bytes=MIN_MULTIPART_PART_BYTES,
            copy_part_bytes=MIN_MULTIPART_PART_BYTES,
            staging_token_factory=lambda: "fixed",
        )

    assert s3.aborted == ["upload-1"]
    assert not s3.objects
    assert not s3.uploads


def test_existing_identity_with_different_bytes_is_a_conflict() -> None:
    payload = b"BZh" + b"immutable"
    s3 = FakeS3()
    arguments = {
        "source_url": SOURCE_URL,
        "destination_prefix": "s3://catalog/raw",
        "s3": s3,
        "http": _http(payload),
        "upload_part_bytes": MIN_MULTIPART_PART_BYTES,
        "copy_part_bytes": MIN_MULTIPART_PART_BYTES,
    }
    result = sync_official_dump(**arguments)
    key = result.uri.removeprefix("s3://catalog/")
    original = s3.objects[key]["Body"]
    s3.objects[key]["Body"] = original[:-1] + bytes([original[-1] ^ 1])

    with pytest.raises(WikidataSyncError, match="bytes differ"):
        sync_official_dump(**arguments)


def test_head_precedes_sequential_exact_range_requests() -> None:
    payload = (
        b"BZh"
        + b"a" * (MIN_MULTIPART_PART_BYTES - 3)
        + b"b" * MIN_MULTIPART_PART_BYTES
        + b"tail"
    )
    http = _http(payload)

    result = sync_official_dump(
        source_url=SOURCE_URL,
        destination_prefix="s3://catalog/raw",
        s3=FakeS3(),
        http=http,
        upload_part_bytes=MIN_MULTIPART_PART_BYTES,
        copy_part_bytes=MIN_MULTIPART_PART_BYTES,
    )

    dump_calls = [
        (method, headers.get("Range"))
        for method, url, headers in http.calls
        if url == SOURCE_URL
    ]
    assert dump_calls == [
        ("HEAD", None),
        ("GET", f"bytes=0-{MIN_MULTIPART_PART_BYTES - 1}"),
        (
            "GET",
            (f"bytes={MIN_MULTIPART_PART_BYTES}-{2 * MIN_MULTIPART_PART_BYTES - 1}"),
        ),
        (
            "GET",
            (f"bytes={2 * MIN_MULTIPART_PART_BYTES}-{len(payload) - 1}"),
        ),
    ]
    assert result.size_bytes == len(payload)
    assert result.sha256 == hashlib.sha256(payload).hexdigest()


@pytest.mark.parametrize("failure", ["short-read", "http-503"])
def test_only_failed_range_is_retried(failure: str) -> None:
    payload = b"BZh" + b"a" * (MIN_MULTIPART_PART_BYTES - 3) + b"second"
    start = MIN_MULTIPART_PART_BYTES
    end = len(payload) - 1
    requested = f"bytes={start}-{end}"
    successful_headers = _range_headers(
        start=start,
        end=end,
        total=len(payload),
        validator_name="ETag",
        validator_value='"dump-v1"',
    )
    part = payload[start : end + 1]
    failed_response = (
        (206, successful_headers, part[:-1])
        if failure == "short-read"
        else (503, {}, b"temporarily unavailable")
    )
    http = _http(
        payload,
        range_responses={
            requested: [
                failed_response,
                (206, successful_headers, part),
            ]
        },
    )
    delays: list[float] = []

    result = sync_official_dump(
        source_url=SOURCE_URL,
        destination_prefix="s3://catalog/raw",
        s3=FakeS3(),
        http=http,
        upload_part_bytes=MIN_MULTIPART_PART_BYTES,
        copy_part_bytes=MIN_MULTIPART_PART_BYTES,
        sleeper=delays.append,
    )

    requested_ranges = [
        headers["Range"]
        for method, url, headers in http.calls
        if method == "GET" and url == SOURCE_URL
    ]
    assert requested_ranges == [
        f"bytes=0-{MIN_MULTIPART_PART_BYTES - 1}",
        requested,
        requested,
    ]
    assert delays == [DEFAULT_RETRY_INITIAL_BACKOFF_SECONDS]
    assert result.sha256 == hashlib.sha256(payload).hexdigest()


def test_etag_change_aborts_active_multipart() -> None:
    payload = b"BZh-etag-change"
    requested = f"bytes=0-{len(payload) - 1}"
    changed_headers = _range_headers(
        start=0,
        end=len(payload) - 1,
        total=len(payload),
        validator_name="ETag",
        validator_value='"dump-v2"',
    )
    s3 = FakeS3()

    with pytest.raises(WikidataSyncError) as raised:
        sync_official_dump(
            source_url=SOURCE_URL,
            destination_prefix="s3://catalog/raw",
            s3=s3,
            http=_http(
                payload,
                range_responses={
                    requested: [(206, changed_headers, payload)],
                },
            ),
            upload_part_bytes=MIN_MULTIPART_PART_BYTES,
            copy_part_bytes=MIN_MULTIPART_PART_BYTES,
        )

    assert raised.value.code == "UPSTREAM_VALIDATOR_CHANGED"
    assert s3.aborted == ["upload-1"]
    assert not s3.uploads
    assert not s3.objects


def test_inexact_content_range_aborts_active_multipart() -> None:
    payload = b"BZh-content-range"
    requested = f"bytes=0-{len(payload) - 1}"
    wrong_headers = _range_headers(
        start=1,
        end=len(payload),
        total=len(payload),
        validator_name="ETag",
        validator_value='"dump-v1"',
    )
    s3 = FakeS3()

    with pytest.raises(WikidataSyncError) as raised:
        sync_official_dump(
            source_url=SOURCE_URL,
            destination_prefix="s3://catalog/raw",
            s3=s3,
            http=_http(
                payload,
                range_responses={
                    requested: [(206, wrong_headers, payload)],
                },
            ),
            upload_part_bytes=MIN_MULTIPART_PART_BYTES,
            copy_part_bytes=MIN_MULTIPART_PART_BYTES,
        )

    assert raised.value.code == "UPSTREAM_RANGE_PROTOCOL_ERROR"
    assert s3.aborted == ["upload-1"]
    assert not s3.uploads


def test_range_retry_exhaustion_aborts_active_multipart() -> None:
    payload = b"BZh-retry-exhaustion"
    requested = f"bytes=0-{len(payload) - 1}"
    headers = _range_headers(
        start=0,
        end=len(payload) - 1,
        total=len(payload),
        validator_name="ETag",
        validator_value='"dump-v1"',
    )
    http = _http(
        payload,
        range_responses={
            requested: [(206, headers, payload[:-1])],
        },
    )
    s3 = FakeS3()
    delays: list[float] = []

    with pytest.raises(WikidataSyncError) as raised:
        sync_official_dump(
            source_url=SOURCE_URL,
            destination_prefix="s3://catalog/raw",
            s3=s3,
            http=http,
            upload_part_bytes=MIN_MULTIPART_PART_BYTES,
            copy_part_bytes=MIN_MULTIPART_PART_BYTES,
            range_attempts=3,
            retry_initial_backoff_seconds=0.25,
            retry_max_backoff_seconds=0.5,
            sleeper=delays.append,
        )

    assert raised.value.code == "UPSTREAM_RANGE_RETRIES_EXHAUSTED"
    assert http.opens[("GET", SOURCE_URL, requested)] == 3
    assert delays == [0.25, 0.5]
    assert s3.aborted == ["upload-1"]
    assert not s3.uploads
    assert not s3.objects


def test_head_rejects_dump_over_maximum_before_multipart() -> None:
    payload = b"BZh-too-large"
    http = _http(payload)
    s3 = FakeS3()

    with pytest.raises(WikidataSyncError) as raised:
        sync_official_dump(
            source_url=SOURCE_URL,
            destination_prefix="s3://catalog/raw",
            s3=s3,
            http=http,
            max_bytes=len(payload) - 1,
            upload_part_bytes=MIN_MULTIPART_PART_BYTES,
            copy_part_bytes=MIN_MULTIPART_PART_BYTES,
        )

    assert raised.value.code == "OBJECT_TOO_LARGE"
    assert not s3.uploads
    assert not any(
        method == "GET" and url == SOURCE_URL for method, url, _headers in http.calls
    )


def test_head_requires_byte_range_support_before_multipart() -> None:
    payload = b"BZh-no-ranges"
    http = _http(
        payload,
        head_headers={
            "Content-Length": str(len(payload)),
            "Accept-Ranges": "none",
            "ETag": '"dump-v1"',
        },
    )
    s3 = FakeS3()

    with pytest.raises(WikidataSyncError) as raised:
        sync_official_dump(
            source_url=SOURCE_URL,
            destination_prefix="s3://catalog/raw",
            s3=s3,
            http=http,
            upload_part_bytes=MIN_MULTIPART_PART_BYTES,
            copy_part_bytes=MIN_MULTIPART_PART_BYTES,
        )

    assert raised.value.code == "UPSTREAM_RANGE_UNSUPPORTED"
    assert not s3.uploads


def test_last_modified_can_stabilize_ranges_without_strong_etag() -> None:
    payload = b"BZh-last-modified"
    last_modified = "Tue, 01 Sep 2026 00:00:00 GMT"

    result = sync_official_dump(
        source_url=SOURCE_URL,
        destination_prefix="s3://catalog/raw",
        s3=FakeS3(),
        http=_http(
            payload,
            validator_name="Last-Modified",
            validator_value=last_modified,
        ),
        upload_part_bytes=MIN_MULTIPART_PART_BYTES,
        copy_part_bytes=MIN_MULTIPART_PART_BYTES,
    )

    assert result.sha256 == hashlib.sha256(payload).hexdigest()


def test_cli_exposes_range_retry_configuration() -> None:
    parser = build_parser()
    required = [
        "--source-url",
        SOURCE_URL,
        "--destination-prefix",
        "s3://catalog/raw",
    ]

    defaults = parser.parse_args(required)
    configured = parser.parse_args(
        [
            *required,
            "--range-attempts",
            "7",
            "--retry-initial-backoff-seconds",
            "0.5",
            "--retry-max-backoff-seconds",
            "9",
        ]
    )

    assert defaults.range_attempts == DEFAULT_RANGE_ATTEMPTS
    assert (
        defaults.retry_initial_backoff_seconds == DEFAULT_RETRY_INITIAL_BACKOFF_SECONDS
    )
    assert defaults.retry_max_backoff_seconds == DEFAULT_RETRY_MAX_BACKOFF_SECONDS
    assert configured.range_attempts == 7
    assert configured.retry_initial_backoff_seconds == 0.5
    assert configured.retry_max_backoff_seconds == 9


@pytest.mark.parametrize(
    "retry_options",
    [
        {"range_attempts": 0},
        {"retry_initial_backoff_seconds": 0},
        {"retry_initial_backoff_seconds": float("nan")},
        {"retry_max_backoff_seconds": 0},
        {"retry_max_backoff_seconds": float("inf")},
        {
            "retry_initial_backoff_seconds": 2,
            "retry_max_backoff_seconds": 1,
        },
    ],
)
def test_range_retry_configuration_rejects_invalid_boundaries(
    retry_options: dict[str, float | int],
) -> None:
    with pytest.raises(ValueError):
        sync_official_dump(
            source_url=SOURCE_URL,
            destination_prefix="s3://catalog/raw",
            s3=FakeS3(),
            http=_http(b"BZh-boundary"),
            upload_part_bytes=MIN_MULTIPART_PART_BYTES,
            copy_part_bytes=MIN_MULTIPART_PART_BYTES,
            **retry_options,
        )
