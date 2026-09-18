from __future__ import annotations

import base64
import hashlib
import io
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from video_media_catalog.index_cli import (
    _read_snapshot_set,
    _snapshot_set_object_ref,
    build_parser,
)
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.object_store import BoundedObjectStore, ObjectStoreError
from video_media_catalog.search_index import (
    INDEX_MAPPINGS,
    MAPPING_DIGEST,
    BulkResult,
    IndexBuildManifest,
    S3IndexManifestPublisher,
    bulk_partition,
    derive_build_id,
    switch_read_alias,
    versioned_index_name,
)

SNAPSHOT_URI = "s3://catalog-control/runs/snapshot-set.json"


class SnapshotClient:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.head_requests: list[dict[str, Any]] = []
        self.get_requests: list[dict[str, Any]] = []

    def _metadata(self) -> dict[str, Any]:
        return {
            "ContentLength": len(self.payload),
            "ETag": '"snapshot-etag"',
            "VersionId": "snapshot-version",
            "ChecksumSHA256": base64.b64encode(
                hashlib.sha256(self.payload).digest()
            ).decode(),
        }

    def head_object(self, **request: Any) -> dict[str, Any]:
        self.head_requests.append(request)
        return self._metadata()

    def get_object(self, **request: Any) -> dict[str, Any]:
        self.get_requests.append(request)
        return {"Body": io.BytesIO(self.payload), **self._metadata()}


def _snapshot_ref(payload: bytes, **overrides: Any) -> ObjectRef:
    values = {
        "uri": SNAPSHOT_URI,
        "format": "OBJECT_FORMAT_JSON",
        "media_type": "application/vnd.video-governance.snapshot-set+json",
        "checksum": Checksum(value=hashlib.sha256(payload).hexdigest()),
        "size_bytes": len(payload),
        "etag": "snapshot-etag",
        "object_version": "snapshot-version",
        **overrides,
    }
    return ObjectRef(**values)


class FakeIndices:
    def __init__(self, aliases: list[str] | None = None) -> None:
        self.aliases = aliases or []
        self.alias_updates: list[dict[str, Any]] = []

    def get_alias(self, *, name: str) -> dict[str, Any]:
        assert name == "media-catalog-entities-read"
        return {index: {"aliases": {name: {}}} for index in self.aliases}

    def update_aliases(self, *, body: dict[str, Any]) -> None:
        self.alias_updates.append(body)


class FakeClient:
    def __init__(self, aliases: list[str] | None = None) -> None:
        self.indices = FakeIndices(aliases)


def test_mapping_is_strict_with_nested_multilingual_fields() -> None:
    assert INDEX_MAPPINGS["dynamic"] == "strict"
    assert INDEX_MAPPINGS["_meta"]["mappingDigest"] == MAPPING_DIGEST
    properties = INDEX_MAPPINGS["properties"]
    assert properties["names"]["type"] == "nested"
    assert properties["names"]["dynamic"] == "strict"
    assert properties["externalIdentifiers"]["type"] == "nested"
    assert properties["attributes"]["dynamic"] == "strict"


@pytest.mark.parametrize("hash_prefix", ["sha256:", "sha256:hex:"])
def test_index_parser_accepts_complete_snapshot_object_ref(
    hash_prefix: str,
) -> None:
    parsed = build_parser().parse_args(
        [
            "--snapshot-set-uri",
            SNAPSHOT_URI,
            "--snapshot-set-hash",
            hash_prefix + "a" * 64,
            "--snapshot-set-version",
            "snapshot-version",
            "--snapshot-set-etag",
            '"snapshot-etag"',
            "--snapshot-set-size",
            "4096",
        ]
    )

    reference = _snapshot_set_object_ref(parsed)

    assert reference.checksum.value == "a" * 64
    assert reference.object_version == "snapshot-version"
    assert reference.etag == "snapshot-etag"


def test_index_parser_requires_all_snapshot_object_ref_fields() -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args(["--snapshot-set-uri", SNAPSHOT_URI])


def test_snapshot_set_is_verified_before_parsing(fixture_dir: Path) -> None:
    payload = (
        fixture_dir / "control" / "media_catalog_snapshot_set.v1.json"
    ).read_bytes()
    client = SnapshotClient(payload)
    reference = _snapshot_ref(payload)

    snapshot = _read_snapshot_set(
        reference,
        aws_region="us-east-1",
        s3_endpoint=None,
        s3_path_style_access=False,
        store=BoundedObjectStore(client=client),
    )

    assert snapshot.snapshot_set_id
    assert client.head_requests[0]["VersionId"] == "snapshot-version"
    assert client.get_requests[0]["VersionId"] == "snapshot-version"


@pytest.mark.parametrize(
    "overrides",
    [
        {"checksum": Checksum(value="b" * 64)},
        {"size_bytes": 1},
        {"etag": "tampered-etag"},
        {"object_version": "tampered-version"},
    ],
)
def test_snapshot_set_rejects_tampered_object_ref(
    fixture_dir: Path,
    overrides: dict[str, Any],
) -> None:
    payload = (
        fixture_dir / "control" / "media_catalog_snapshot_set.v1.json"
    ).read_bytes()
    reference = _snapshot_ref(payload, **overrides)

    with pytest.raises(ObjectStoreError):
        _read_snapshot_set(
            reference,
            aws_region="us-east-1",
            s3_endpoint=None,
            s3_path_style_access=False,
            store=BoundedObjectStore(client=SnapshotClient(payload)),
        )


def test_index_name_is_stable_for_the_same_six_snapshot_ids() -> None:
    tables = [
        SimpleNamespace(
            table_name=f"media.catalog.{name}",
            snapshot_id=number,
        )
        for number, name in enumerate(
            (
                "catalog_source_record",
                "catalog_entity",
                "catalog_name",
                "catalog_external_identifier",
                "catalog_relation",
                "catalog_ingest_error",
            ),
            start=1,
        )
    ]
    first = SimpleNamespace(tables=tables)
    second = SimpleNamespace(tables=list(reversed(tables)))

    first_id = derive_build_id(
        snapshot_set=first,
        config_digest="sha256:" + "b" * 64,
    )
    second_id = derive_build_id(
        snapshot_set=second,
        config_digest="sha256:" + "b" * 64,
    )

    assert first_id == second_id
    assert (
        versioned_index_name("media-catalog-entities-v1", first_id)
        == f"media-catalog-entities-v1-{first_id[:24]}"
    )


def test_alias_switch_is_one_atomic_request_and_keeps_old_indices() -> None:
    client = FakeClient(["media-catalog-entities-v1-old"])

    changed = switch_read_alias(
        client,
        alias="media-catalog-entities-read",
        target_index="media-catalog-entities-v1-new",
    )

    assert changed is True
    assert client.indices.alias_updates == [
        {
            "actions": [
                {
                    "remove": {
                        "index": "media-catalog-entities-v1-old",
                        "alias": "media-catalog-entities-read",
                    }
                },
                {
                    "add": {
                        "index": "media-catalog-entities-v1-new",
                        "alias": "media-catalog-entities-read",
                        "is_write_index": False,
                    }
                },
            ]
        }
    ]


def test_bulk_partition_reports_partial_failures() -> None:
    seen_actions: list[dict[str, Any]] = []

    def fake_streaming_bulk(
        _client: Any,
        actions: Any,
        **kwargs: Any,
    ):
        assert kwargs["raise_on_error"] is False
        assert kwargs["max_chunk_bytes"] == 4096
        seen_actions.extend(actions)
        yield True, {"index": {"_id": "one", "status": 201}}
        yield (
            False,
            {
                "index": {
                    "_id": "two",
                    "status": 429,
                    "error": {"type": "rejected_execution_exception"},
                }
            },
        )

    result = bulk_partition(
        [
            {"entityKey": "one", "displayName": "One"},
            {"entityKey": "two", "displayName": "Two"},
        ],
        client=object(),
        index_name="media-catalog-entities-v1-build",
        chunk_size=100,
        max_chunk_bytes=4096,
        request_timeout=5,
        streaming_bulk=fake_streaming_bulk,
    )

    assert result == BulkResult(
        document_count=1,
        error_count=1,
        errors=[
            {
                "id": "two",
                "status": 429,
                "error": {"type": "rejected_execution_exception"},
            }
        ],
    )
    assert [action["_id"] for action in seen_actions] == ["one", "two"]


class FakeS3:
    def __init__(self) -> None:
        self.payload: bytes | None = None
        self.put_request: dict[str, Any] | None = None

    def put_object(self, **kwargs: Any) -> None:
        self.put_request = kwargs
        self.payload = kwargs["Body"]

    def get_object(self, **_: Any) -> dict[str, Any]:
        assert self.payload is not None
        return {
            "Body": io.BytesIO(self.payload),
            "ContentLength": len(self.payload),
            "ChecksumSHA256": base64.b64encode(
                hashlib.sha256(self.payload).digest()
            ).decode(),
        }


def test_manifest_is_conditionally_written_and_verified() -> None:
    s3 = FakeS3()
    publisher = S3IndexManifestPublisher(
        "s3://audit/index-builds/build.json",
        client=s3,
    )
    manifest = IndexBuildManifest(
        status="COMPLETED",
        build_id="a" * 64,
        source_snapshot_set_id="snapshot-set",
        source_snapshot_set_uri="s3://control/snapshot-set.json",
        source_snapshot_set=ObjectRef(
            uri="s3://control/snapshot-set.json",
            format="OBJECT_FORMAT_JSON",
            media_type="application/vnd.video-governance.snapshot-set+json",
            checksum=Checksum(value="c" * 64),
            size_bytes=4096,
            etag="snapshot-etag",
            object_version="snapshot-version",
        ),
        table_snapshots=[
            {"tableName": f"media.catalog.{name}", "snapshotId": number}
            for number, name in enumerate(
                (
                    "catalog_source_record",
                    "catalog_entity",
                    "catalog_name",
                    "catalog_external_identifier",
                    "catalog_relation",
                    "catalog_ingest_error",
                ),
                start=1,
            )
        ],
        mapping_digest=MAPPING_DIGEST,
        config_digest="sha256:" + "b" * 64,
        document_count=10,
        error_count=0,
        index="media-catalog-entities-v1-build",
        alias="media-catalog-entities-read",
        started_at="2026-09-18T00:00:00Z",
        completed_at="2026-09-18T00:01:00Z",
    )

    assert publisher.publish(manifest) == manifest
    assert s3.put_request is not None
    assert s3.put_request["IfNoneMatch"] == "*"
    assert b'"sourceSnapshotSet":' in s3.put_request["Body"]
    assert b'"objectVersion":"snapshot-version"' in s3.put_request["Body"]
