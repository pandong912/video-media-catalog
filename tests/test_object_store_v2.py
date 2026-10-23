from __future__ import annotations

import base64
import hashlib
import io
from pathlib import Path
from typing import Any

import pytest

from video_media_catalog.execution_contracts import ObjectRefV2
from video_media_catalog.object_store import (
    ExactObjectStoreV2,
    ObjectRefV2WriteTarget,
    ObjectStoreError,
)

TENANT = "018f47c2-4a3b-7a10-8a11-000000000001"
PROJECT = "018f47c2-4a3b-7a10-8a11-000000000002"
BINDING = "018f47c2-4a3b-7a10-8a11-000000000003"
PLANE = "018f47c2-4a3b-7a10-8a11-000000000004"


class S3Client:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def _metadata(self) -> dict[str, Any]:
        return {
            "ContentLength": len(self.payload),
            "ETag": '"etag-1"',
            "VersionId": "version-1",
            "ChecksumSHA256": base64.b64encode(
                hashlib.sha256(self.payload).digest()
            ).decode(),
        }

    def head_object(self, **_request: Any) -> dict[str, Any]:
        return self._metadata()

    def get_object(self, **_request: Any) -> dict[str, Any]:
        return {**self._metadata(), "Body": io.BytesIO(self.payload)}

    def put_object(self, **request: Any) -> dict[str, str]:
        self.payload = bytes(request["Body"])
        return {"ETag": '"etag-1"', "VersionId": "version-1"}


class OssClient:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def _metadata(self) -> dict[str, Any]:
        return {
            "content_length": len(self.payload),
            "etag": "etag-oss",
            "version_id": "version-oss-1",
            "metadata": {"sha256": hashlib.sha256(self.payload).hexdigest()},
        }

    def head_object(self, **_request: Any) -> dict[str, Any]:
        return self._metadata()

    def get_object(self, **_request: Any) -> dict[str, Any]:
        return {**self._metadata(), "body": io.BytesIO(self.payload)}

    def put_object(self, **request: Any) -> dict[str, str]:
        self.payload = bytes(request["data"])
        return {"etag": "etag-oss", "version_id": "version-oss-1"}


def _ref(payload: bytes, provider: str) -> ObjectRefV2:
    aws = provider == "AWS_S3"
    return ObjectRefV2.create(
        object_ref_id="018f47c2-4a3b-7a10-8a11-000000000010",
        tenant_id=TENANT,
        project_id=PROJECT,
        data_plane_binding_id=BINDING,
        data_plane_id=PLANE,
        provider=provider,  # type: ignore[arg-type]
        uri=(
            "s3://catalog-bucket/execution/object.json"
            if aws
            else "oss://catalog-bucket/execution/object.json"
        ),
        object_key="execution/object.json",
        format="JSON",
        media_type="application/json",
        checksum=hashlib.sha256(payload).hexdigest(),
        size_bytes=len(payload),
        etag="etag-1" if aws else "etag-oss",
        version_token="version-1" if aws else "version-oss-1",
        schema_digest="b" * 64,
        encryption_context_digest="c" * 64,
        created_at="2026-10-23T00:00:00Z",
    )


@pytest.mark.parametrize("provider", ["AWS_S3", "ALIBABA_OSS"])
def test_v2_verify_and_download_require_exact_version_and_sha256(
    tmp_path: Path,
    provider: str,
) -> None:
    payload = b"exact-provider-object"
    store = ExactObjectStoreV2(
        s3_client=S3Client(payload),
        oss_client=OssClient(payload),
    )
    reference = _ref(payload, provider)

    assert store.verify(reference, max_bytes=1_024) == reference
    materialized = store.download(
        reference,
        tmp_path / provider,
        max_bytes=1_024,
    )
    assert materialized.sha256 == hashlib.sha256(payload).hexdigest()

    changed = reference.model_copy(update={"version_token": "wrong-version"})
    with pytest.raises(ObjectStoreError) as raised:
        store.verify(changed, max_bytes=1_024)
    assert raised.value.code == "OBJECT_VERSION_MISMATCH"

    changed = reference.model_copy(update={"etag": "wrong-etag"})
    with pytest.raises(ObjectStoreError) as raised:
        store.verify(changed, max_bytes=1_024)
    assert raised.value.code == "OBJECT_ETAG_MISMATCH"

    changed = reference.model_copy(update={"checksum": "0" * 64})
    with pytest.raises(ObjectStoreError) as raised:
        store.verify(changed, max_bytes=1_024)
    assert raised.value.code == "OBJECT_CHECKSUM_MISMATCH"


@pytest.mark.parametrize(
    ("provider", "uri", "etag", "version"),
    [
        (
            "AWS_S3",
            "s3://catalog-bucket/execution/receipt.json",
            "etag-1",
            "version-1",
        ),
        (
            "ALIBABA_OSS",
            "oss://catalog-bucket/execution/receipt.json",
            "etag-oss",
            "version-oss-1",
        ),
    ],
)
def test_v2_publish_returns_exact_provider_identity(
    provider: str,
    uri: str,
    etag: str,
    version: str,
) -> None:
    payload = b'{"receipt":"external-authority"}\n'
    store = ExactObjectStoreV2(
        s3_client=S3Client(payload),
        oss_client=OssClient(payload),
    )
    result = store.publish_bytes(
        payload,
        ObjectRefV2WriteTarget(
            object_ref_id="018f47c2-4a3b-7a10-8a11-000000000011",
            tenant_id=TENANT,
            project_id=PROJECT,
            data_plane_binding_id=BINDING,
            data_plane_id=PLANE,
            provider=provider,  # type: ignore[arg-type]
            uri=uri,
            object_key="execution/receipt.json",
            format="JSON",
            media_type="application/json",
            schema_digest="d" * 64,
            encryption_context_digest="e" * 64,
            created_at="2026-10-23T00:10:00Z",
        ),
        max_bytes=1_024,
    )

    assert result.object_ref.provider == provider
    assert result.object_ref.etag == etag
    assert result.object_ref.version_token == version
    assert result.object_ref.checksum == hashlib.sha256(payload).hexdigest()
