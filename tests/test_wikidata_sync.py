from __future__ import annotations

import hashlib
import io
from collections import Counter
from typing import ClassVar

import pytest

from video_media_catalog.wikidata_sync import (
    MIN_MULTIPART_PART_BYTES,
    StreamingHttpResponse,
    WikidataSyncError,
    sync_official_dump,
    validate_official_dump_url,
)

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
        responses: dict[str, tuple[int, dict[str, str], bytes]],
    ) -> None:
        self.responses = responses
        self.opens: Counter[str] = Counter()

    def open(self, method: str, url: str) -> StreamingHttpResponse:
        assert method == "GET"
        self.opens[url] += 1
        status, headers, payload = self.responses[url]
        return StreamingHttpResponse(status, headers, io.BytesIO(payload))


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
            CHECKSUM_URL: (
                302,
                {"Location": "https://evil.example/checksum"},
                b"",
            )
        }
    )
    with pytest.raises(WikidataSyncError, match="allowlist"):
        sync_official_dump(
            source_url=SOURCE_URL,
            destination_prefix="s3://catalog/raw",
            s3=FakeS3(),
            http=http,
        )


def _http(payload: bytes, *, sha1: str | None = None) -> FakeHttp:
    digest = sha1 or hashlib.sha1(payload, usedforsecurity=False).hexdigest()
    return FakeHttp(
        {
            CHECKSUM_URL: (
                200,
                {},
                f"{digest}  wikidata-20260901-all.json.bz2\n".encode(),
            ),
            SOURCE_URL: (
                200,
                {"Content-Length": str(len(payload))},
                payload,
            ),
        }
    )


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
    assert http.opens[SOURCE_URL] == 1
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
