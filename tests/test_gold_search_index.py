from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from video_media_catalog.gold_ingest import GOLD_RELEASE_COMMIT_MEDIA_TYPE
from video_media_catalog.gold_search_index import (
    GOLD_AFFECTED_ENTITY_MANIFEST_MEDIA_TYPE,
    GOLD_PARTITION_RECEIPT_MEDIA_TYPE,
    INDEX_MAPPINGS,
    MAPPING_DIGEST,
    RESEARCH_INDEX_PREFIX,
    RESEARCH_READ_ALIAS,
    GoldAffectedEntityManifest,
    GoldIndexBuildManifest,
    GoldIndexPartitionReceipt,
    GoldPartitionReceiptPublisher,
    ensure_gold_index,
    ensure_incremental_gold_baseline,
    gold_bulk_partition,
    gold_index_config_digest,
    gold_index_config_identity,
    gold_index_name,
    reconcile_full_gold_index_counts,
    validate_gold_index_owner,
)
from video_media_catalog.gold_tables import GOLD_DATA_COLUMNS
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.opensearch_client import OpenSearchConnection


class FakeIndices:
    def __init__(self) -> None:
        self.created = {}

    def exists(self, *, index: str) -> bool:
        return index in self.created

    def create(self, *, index: str, body):
        self.created[index] = body

    def get_mapping(self, *, index: str):
        return {index: {"mappings": self.created[index]["mappings"]}}

    def refresh(self, *, index: str) -> None:
        assert index in self.created


class FakeClient:
    def __init__(self) -> None:
        self.indices = FakeIndices()
        self.total_count = 0
        self.owner_count = 0

    def count(self, *, index: str, body=None):
        assert index in self.indices.created
        return {"count": self.total_count if body is None else self.owner_count}


def test_research_mapping_and_identity_are_isolated_from_v1() -> None:
    assert INDEX_MAPPINGS["dynamic"] == "strict"
    assert RESEARCH_READ_ALIAS == "media-catalog-research-read"
    assert RESEARCH_INDEX_PREFIX == "media-catalog-research"
    digest = gold_index_config_digest(
        read_alias=RESEARCH_READ_ALIAS,
        index_prefix=RESEARCH_INDEX_PREFIX,
        owner_subject="owner-123",
        shards=1,
        replicas=0,
        bulk_chunk_size=100,
        bulk_max_chunk_bytes=5 * 1024 * 1024,
        image_digest="sha256:" + ("a" * 64),
    )
    name = gold_index_name(
        RESEARCH_INDEX_PREFIX,
        "b" * 64,
    )
    assert digest.startswith("sha256:")
    assert digest == (
        "sha256:a2a1483cd29e41cb78cda4fba16036cb99d434612a17dc40315118646381f6cc"
    )
    assert name.startswith("media-catalog-research-")
    assert "sourceBadges" in INDEX_MAPPINGS["properties"]
    assert "winningAssertions" in INDEX_MAPPINGS["properties"]
    assert "rights" in INDEX_MAPPINGS["properties"]
    assert "conflicts" in INDEX_MAPPINGS["properties"]
    assert "ownerSubject" in INDEX_MAPPINGS["properties"]
    assert INDEX_MAPPINGS["properties"]["externalIdentifiers"]["properties"]["url"] == {
        "type": "keyword",
        "index": False,
    }
    with pytest.raises(ValueError, match="fixed"):
        gold_index_config_digest(
            read_alias="media-catalog-community-v2-shadow-read",
            index_prefix="media-catalog-community-v2",
            owner_subject="owner-123",
            shards=1,
            replicas=0,
            bulk_chunk_size=100,
            bulk_max_chunk_bytes=5 * 1024 * 1024,
            image_digest="sha256:" + ("a" * 64),
        )
    assert INDEX_MAPPINGS["_meta"]["mappingDigest"] == MAPPING_DIGEST


def test_gold_index_creation_reuses_compatible_mapping() -> None:
    client = FakeClient()
    assert ensure_gold_index(
        client,
        index_name="media-catalog-research-build",
        owner_subject="owner-123",
        shards=1,
        replicas=0,
    )
    assert not ensure_gold_index(
        client,
        index_name="media-catalog-research-build",
        owner_subject="owner-123",
        shards=1,
        replicas=0,
    )
    metadata = client.indices.created["media-catalog-research-build"]["mappings"][
        "_meta"
    ]
    assert metadata["ownerSubject"] == "owner-123"
    with pytest.raises(RuntimeError, match="owner"):
        ensure_gold_index(
            client,
            index_name="media-catalog-research-build",
            owner_subject="owner-456",
            shards=1,
            replicas=0,
        )


def test_gold_index_owner_validation_rejects_mixed_documents() -> None:
    client = FakeClient()
    ensure_gold_index(
        client,
        index_name="media-catalog-research-build",
        owner_subject="owner-123",
        shards=1,
        replicas=0,
    )
    client.total_count = 2
    client.owner_count = 1
    with pytest.raises(RuntimeError, match="document owners"):
        validate_gold_index_owner(
            client,
            index_name="media-catalog-research-build",
            owner_subject="owner-123",
            expected_document_count=2,
        )
    client.owner_count = 2
    assert (
        validate_gold_index_owner(
            client,
            index_name="media-catalog-research-build",
            owner_subject="owner-123",
            expected_document_count=2,
        )
        == 2
    )


def test_gold_index_manifest_binds_release_commit() -> None:
    reference = ObjectRef(
        uri="s3://bucket/release-commit.json",
        format="OBJECT_FORMAT_JSON",
        media_type=GOLD_RELEASE_COMMIT_MEDIA_TYPE,
        checksum=Checksum(value="a" * 64),
        size_bytes=100,
        etag="etag",
        object_version="version",
    )
    config_identity = gold_index_config_identity(
        read_alias=RESEARCH_READ_ALIAS,
        index_prefix=RESEARCH_INDEX_PREFIX,
        owner_subject="owner-123",
        shards=1,
        replicas=0,
        bulk_chunk_size=100,
        bulk_max_chunk_bytes=5 * 1024 * 1024,
        image_digest="sha256:" + ("e" * 64),
    )
    manifest = GoldIndexBuildManifest(
        build_id="b" * 64,
        release_plan_id="sha256:" + ("c" * 64),
        owner_subject="owner-123",
        context_id="research",
        release_commit=reference,
        table_snapshot_ids={
            table: (10 if table == "community_gold_entity" else None)
            for table in GOLD_DATA_COLUMNS
        },
        mapping_digest=MAPPING_DIGEST,
        config_identity=config_identity,
        config_digest=config_identity.digest,
        document_count=1,
        gold_entity_count=1,
        successful_document_count=1,
        failed_document_count=0,
        concrete_index_document_count=1,
        partition_receipt_count=1,
        partition_receipt_digest="sha256:" + ("f" * 64),
        index="media-catalog-research-build",
        alias=RESEARCH_READ_ALIAS,
        completed_at="2026-09-19T00:00:00Z",
    )
    assert manifest == type(manifest).model_validate_json(manifest.json_bytes())
    invalid = manifest.model_copy(
        update={"owner_subject": "owner-456"},
    ).model_dump(mode="python")
    with pytest.raises(ValueError, match="config identity"):
        GoldIndexBuildManifest.model_validate(invalid)


class _Transport:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


class _BulkClient:
    def __init__(self) -> None:
        self.transport = _Transport()


def test_parallel_gold_bulk_workers_keep_chunks_bounded_and_retrying() -> None:
    clients: list[_BulkClient] = []
    calls: list[dict[str, Any]] = []

    def client_factory(_connection: OpenSearchConnection) -> _BulkClient:
        client = _BulkClient()
        clients.append(client)
        return client

    def fake_streaming_bulk(_client: Any, actions: Any, **kwargs: Any):
        calls.append(kwargs)
        for action in actions:
            yield True, {action["_op_type"]: {"_id": action["_id"], "status": 201}}

    connection = OpenSearchConnection(
        endpoint="http://localhost:9200",
        allow_insecure=True,
        timeout_seconds=5,
    )
    result = gold_bulk_partition(
        ({"entityKey": f"entity-{number}", "displayName": "x"} for number in range(9)),
        connection=connection,
        index_name="media-catalog-research-build",
        operation="FULL",
        chunk_size=2,
        max_chunk_bytes=1024,
        workers=3,
        client_factory=client_factory,
        streaming_bulk=fake_streaming_bulk,
    )

    assert result.input_document_count == 9
    assert result.bulk_result.document_count == 9
    assert result.bulk_result.error_count == 0
    assert len(clients) == 3
    assert all(client.transport.closed for client in clients)
    assert all(call["chunk_size"] == 2 for call in calls)
    assert all(call["max_chunk_bytes"] == 1024 for call in calls)
    assert all(call["max_retries"] == 3 for call in calls)
    assert all(call["raise_on_exception"] is False for call in calls)


def test_gold_bulk_rejects_one_document_larger_than_byte_bound() -> None:
    client = _BulkClient()
    connection = OpenSearchConnection(
        endpoint="http://localhost:9200",
        allow_insecure=True,
    )

    def consume_actions(_client: Any, actions: Any, **_kwargs: Any):
        yield from ((True, {}) for _ in actions)

    with pytest.raises(ValueError, match="exceeds bulk-max-chunk-bytes"):
        gold_bulk_partition(
            [{"entityKey": "entity-1", "displayName": "x" * 1000}],
            connection=connection,
            index_name="media-catalog-research-build",
            operation="FULL",
            chunk_size=10,
            max_chunk_bytes=128,
            workers=1,
            client_factory=lambda _: client,
            streaming_bulk=consume_actions,
        )
    assert client.transport.closed


def _release_reference(path: Path) -> ObjectRef:
    return ObjectRef(
        uri=path.as_uri(),
        format="OBJECT_FORMAT_JSON",
        media_type=GOLD_RELEASE_COMMIT_MEDIA_TYPE,
        checksum=Checksum(value="a" * 64),
        size_bytes=100,
    )


def test_partition_receipt_is_immutable_and_content_bound(tmp_path: Path) -> None:
    config = gold_index_config_identity(
        read_alias=RESEARCH_READ_ALIAS,
        index_prefix=RESEARCH_INDEX_PREFIX,
        owner_subject="owner-123",
        shards=1,
        replicas=0,
        bulk_chunk_size=100,
        bulk_max_chunk_bytes=4096,
        image_digest="sha256:" + ("b" * 64),
    )
    receipt = GoldIndexPartitionReceipt(
        build_id="c" * 64,
        partition_id=0,
        partition_count=2,
        operation="FULL",
        release_commit=_release_reference(tmp_path / "release.json"),
        mapping_digest=MAPPING_DIGEST,
        config_identity=config,
        config_digest=config.digest,
        image_digest=config.image_digest,
        index="media-catalog-research-build",
        input_document_count=3,
        successful_document_count=3,
        input_digest="sha256:" + ("d" * 64),
    )
    publisher = GoldPartitionReceiptPublisher((tmp_path / "receipt.json").as_uri())

    assert publisher.publish(receipt) == receipt
    assert publisher.publish(receipt) == receipt
    assert publisher.read_optional() == receipt
    assert GOLD_PARTITION_RECEIPT_MEDIA_TYPE.endswith(".v2+json")

    conflict = receipt.model_copy(
        update={
            "input_document_count": 4,
            "successful_document_count": 4,
            "input_digest": "sha256:" + ("e" * 64),
        }
    )
    with pytest.raises(RuntimeError, match="receipt conflicts"):
        publisher.publish(conflict)


def test_affected_entity_manifest_is_sorted_unique_and_explicit(
    tmp_path: Path,
) -> None:
    release = _release_reference(tmp_path / "release.json")
    manifest = GoldAffectedEntityManifest(
        release_plan_id="sha256:" + ("1" * 64),
        owner_subject="owner-123",
        release_commit=release,
        base_index="media-catalog-research-base",
        base_release_plan_id="sha256:" + ("2" * 64),
        base_document_count=10,
        operations=[
            {"entityKey": "sha256:" + ("3" * 64), "operation": "UPSERT"},
            {"entityKey": "sha256:" + ("4" * 64), "operation": "DELETE"},
        ],
        generated_at="2026-09-20T00:00:00Z",
    )
    assert manifest.upsert_entity_keys == ("sha256:" + ("3" * 64),)
    assert manifest.delete_entity_keys == ("sha256:" + ("4" * 64),)
    assert manifest.digest.startswith("sha256:")
    assert GOLD_AFFECTED_ENTITY_MANIFEST_MEDIA_TYPE.endswith(".v2+json")

    invalid = manifest.model_dump(mode="python")
    invalid["operations"] = list(reversed(invalid["operations"]))
    with pytest.raises(ValueError, match="unique and sorted"):
        GoldAffectedEntityManifest.model_validate(invalid)


class _IncrementalIndices:
    def __init__(self, client: _IncrementalClient) -> None:
        self.client = client

    def get_alias(self, *, name: str) -> dict[str, Any]:
        return {self.client.alias_target: {"aliases": {name: {}}}}

    def get_mapping(self, *, index: str) -> dict[str, Any]:
        assert index in self.client.counts
        return {
            index: {
                "mappings": {
                    "_meta": {
                        "mappingDigest": MAPPING_DIGEST,
                        "ownerSubject": "owner-123",
                    }
                }
            }
        }

    def refresh(self, *, index: str) -> None:
        assert index in self.client.counts


class _IncrementalTasks:
    def __init__(self, client: _IncrementalClient) -> None:
        self.client = client

    def get(self, *, task_id: str, **_kwargs: Any) -> dict[str, Any]:
        return {
            "completed": True,
            "response": self.client.task_responses[task_id],
        }


class _IncrementalClient:
    def __init__(self) -> None:
        self.alias_target = "media-catalog-research-base"
        self.counts = {self.alias_target: 2, "media-catalog-research-target": 0}
        self.indices = _IncrementalIndices(self)
        self.tasks = _IncrementalTasks(self)
        self.task_responses: dict[str, dict[str, Any]] = {}
        self.reindex_calls = 0
        self.update_calls = 0

    def count(self, *, index: str, body: Any = None) -> dict[str, int]:
        _ = body
        return {"count": self.counts[index]}

    def reindex(self, **_kwargs: Any) -> dict[str, str]:
        self.reindex_calls += 1
        self.counts["media-catalog-research-target"] = 2
        self.task_responses["node:reindex"] = {
            "total": 2,
            "created": 2,
            "updated": 0,
            "version_conflicts": 0,
            "failures": [],
            "timed_out": False,
        }
        return {"task": "node:reindex"}

    def update_by_query(self, **_kwargs: Any) -> dict[str, str]:
        self.update_calls += 1
        self.task_responses["node:update"] = {
            "total": 2,
            "updated": 2,
            "noops": 0,
            "version_conflicts": 0,
            "failures": [],
            "timed_out": False,
        }
        return {"task": "node:update"}


def test_incremental_baseline_copies_to_new_index_and_resumes(
    tmp_path: Path,
) -> None:
    release = _release_reference(tmp_path / "release.json")
    affected_reference = ObjectRef(
        uri=(tmp_path / "affected.json").as_uri(),
        format="OBJECT_FORMAT_JSON",
        media_type=GOLD_AFFECTED_ENTITY_MANIFEST_MEDIA_TYPE,
        checksum=Checksum(value="f" * 64),
        size_bytes=100,
    )
    target_release = "sha256:" + ("1" * 64)
    affected = GoldAffectedEntityManifest(
        release_plan_id=target_release,
        owner_subject="owner-123",
        release_commit=release,
        base_index="media-catalog-research-base",
        base_release_plan_id="sha256:" + ("2" * 64),
        base_document_count=2,
        operations=[],
        generated_at="2026-09-20T00:00:00Z",
    )
    config = gold_index_config_identity(
        read_alias=RESEARCH_READ_ALIAS,
        index_prefix=RESEARCH_INDEX_PREFIX,
        owner_subject="owner-123",
        shards=1,
        replicas=0,
        bulk_chunk_size=100,
        bulk_max_chunk_bytes=4096,
        image_digest="sha256:" + ("b" * 64),
    )
    client = _IncrementalClient()
    arguments = {
        "alias": RESEARCH_READ_ALIAS,
        "index_name": "media-catalog-research-target",
        "build_id": "c" * 64,
        "release_commit": SimpleNamespace(release_plan_id=target_release),
        "release_commit_reference": release,
        "affected_manifest": affected,
        "affected_manifest_reference": affected_reference,
        "config_identity": config,
        "receipt_prefix": tmp_path.as_uri(),
        "request_timeout": 30,
    }

    receipt, reused = ensure_incremental_gold_baseline(client, **arguments)
    assert not reused
    assert receipt.operation == "BASELINE"
    assert client.reindex_calls == 1
    assert client.update_calls == 1
    assert client.counts["media-catalog-research-base"] == 2

    client.alias_target = "media-catalog-research-target"
    resumed, reused = ensure_incremental_gold_baseline(client, **arguments)
    assert reused
    assert resumed == receipt
    assert client.reindex_calls == 1
    assert client.update_calls == 1


def test_full_count_reconciliation_fails_closed_on_any_drift() -> None:
    reconciled = reconcile_full_gold_index_counts(
        gold_entity_count=10,
        projected_document_count=10,
        successful_document_count=10,
        failed_document_count=0,
        concrete_index_document_count=10,
    )
    assert reconciled.concrete_index_document_count == 10

    for override in (
        {"projected_document_count": 9},
        {"successful_document_count": 9},
        {"failed_document_count": 1},
        {"concrete_index_document_count": 9},
    ):
        values = {
            "gold_entity_count": 10,
            "projected_document_count": 10,
            "successful_document_count": 10,
            "failed_document_count": 0,
            "concrete_index_document_count": 10,
            **override,
        }
        with pytest.raises(RuntimeError, match="reconciliation failed"):
            reconcile_full_gold_index_counts(**values)
