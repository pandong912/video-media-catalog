from __future__ import annotations

import base64
import hashlib
import io
from pathlib import Path
from typing import ClassVar

import pytest

from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.object_store import (
    BoundedObjectStore,
    ObjectStoreError,
    conditional_publish_bytes,
)


class FakeClient:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.last_body: io.BytesIO | None = None
        self.last_request: dict | None = None

    def get_object(self, **request):
        self.last_request = request
        self.last_body = io.BytesIO(self.payload)
        digest = hashlib.sha256(self.payload).digest()
        return {
            "Body": self.last_body,
            "ContentLength": len(self.payload),
            "ETag": '"etag-1"',
            "VersionId": "version-1",
            "ChecksumSHA256": base64.b64encode(digest).decode(),
        }

    def head_object(self, **request):
        self.last_request = request
        digest = hashlib.sha256(self.payload).digest()
        return {
            "ContentLength": len(self.payload),
            "ETag": '"etag-1"',
            "VersionId": "version-1",
            "ChecksumSHA256": base64.b64encode(digest).decode(),
        }


class PreconditionFailure(Exception):
    response: ClassVar[dict] = {
        "Error": {"Code": "PreconditionFailed"},
        "ResponseMetadata": {"HTTPStatusCode": 412},
    }


class ConflictClient(FakeClient):
    def put_object(self, **request):
        raise PreconditionFailure


class UnversionedClient(FakeClient):
    def put_object(self, **request):
        self.payload = request["Body"].read()
        return {"ETag": '"etag-1"'}

    def head_object(self, **request):
        response = super().head_object(**request)
        response.pop("VersionId")
        return response


def _ref(payload: bytes) -> ObjectRef:
    return ObjectRef(
        uri="s3://bucket/source",
        format="OBJECT_FORMAT_OTHER",
        media_type="application/octet-stream",
        checksum=Checksum(value=hashlib.sha256(payload).hexdigest()),
        size_bytes=len(payload),
        etag="etag-1",
        object_version="version-1",
    )


def test_s3_download_is_bounded_verified_and_closes_body(
    tmp_path: Path,
) -> None:
    payload = b"bounded-source"
    client = FakeClient(payload)
    store = BoundedObjectStore(client=client)
    destination = tmp_path / "source"

    downloaded = store.download(_ref(payload), destination, max_bytes=len(payload))

    assert downloaded.sha256 == hashlib.sha256(payload).hexdigest()
    assert destination.read_bytes() == payload
    assert client.last_body is not None and client.last_body.closed
    assert client.last_request is not None
    assert client.last_request["VersionId"] == "version-1"
    assert client.last_request["ChecksumMode"] == "ENABLED"


def test_s3_download_rejects_declared_size_before_streaming(
    tmp_path: Path,
) -> None:
    payload = b"too-large"
    client = FakeClient(payload)
    store = BoundedObjectStore(client=client)

    with pytest.raises(ObjectStoreError, match="configured limit"):
        store.download(
            _ref(payload),
            tmp_path / "source",
            max_bytes=len(payload) - 1,
        )

    assert client.last_body is not None and client.last_body.closed


def test_s3_metadata_verification_checks_native_checksum() -> None:
    payload = b"metadata-only"
    client = FakeClient(payload)
    store = BoundedObjectStore(client=client)

    store.verify(_ref(payload), max_bytes=len(payload))

    assert client.last_request is not None
    assert client.last_request["VersionId"] == "version-1"
    assert client.last_request["ChecksumMode"] == "ENABLED"


def test_conditional_write_conflict_reverifies_existing_bytes(
    tmp_path: Path,
) -> None:
    payload = b"same-output"
    source = tmp_path / "output"
    source.write_bytes(payload)
    client = ConflictClient(payload)
    store = BoundedObjectStore(client=client)

    result = store.upload_file(
        source,
        "s3://bucket/output",
        media_type="application/json",
        object_format="OBJECT_FORMAT_JSON",
        max_bytes=1024,
    )

    assert result.reused
    assert result.object_ref.checksum.value == hashlib.sha256(payload).hexdigest()
    assert client.last_body is not None and client.last_body.closed


def test_s3_conditional_write_rejects_same_key_with_different_bytes(
    tmp_path: Path,
) -> None:
    source = tmp_path / "output"
    source.write_bytes(b"new-output")
    client = ConflictClient(b"existing-output")
    store = BoundedObjectStore(client=client)

    with pytest.raises(ObjectStoreError) as raised:
        store.publish_file_conditional(
            source,
            "s3://bucket/output",
            media_type="application/json",
            object_format="OBJECT_FORMAT_JSON",
            max_bytes=1024,
        )

    assert raised.value.code == "IMMUTABLE_OBJECT_CONFLICT"
    assert client.last_body is not None and client.last_body.closed


def test_file_conditional_write_reuses_identical_and_rejects_conflict(
    tmp_path: Path,
) -> None:
    store = BoundedObjectStore(client=object())
    destination = (tmp_path / "output.json").as_uri()
    arguments = {
        "destination_uri": destination,
        "media_type": "application/json",
        "object_format": "OBJECT_FORMAT_JSON",
        "max_bytes": 1024,
    }

    first = store.publish_bytes_conditional(b"same", **arguments)
    repeated = store.publish_bytes_conditional(b"same", **arguments)
    assert not first.reused
    assert repeated.reused

    with pytest.raises(ObjectStoreError) as raised:
        store.publish_bytes_conditional(b"different", **arguments)
    assert raised.value.code == "IMMUTABLE_OBJECT_CONFLICT"


def test_conditional_s3_control_publish_requires_versioned_object_ref() -> None:
    payload = b"control"
    store = BoundedObjectStore(client=UnversionedClient(payload))

    with pytest.raises(ObjectStoreError) as raised:
        conditional_publish_bytes(
            store,
            payload,
            "s3://bucket/control.json",
            media_type="application/json",
            object_format="OBJECT_FORMAT_JSON",
            max_bytes=1024,
        )

    assert raised.value.code == "IMMUTABLE_METADATA_REQUIRED"
