from __future__ import annotations

import pytest

from video_media_catalog.gold_ingest import GOLD_RELEASE_COMMIT_MEDIA_TYPE
from video_media_catalog.gold_search_index import (
    INDEX_MAPPINGS,
    MAPPING_DIGEST,
    RESEARCH_INDEX_PREFIX,
    RESEARCH_READ_ALIAS,
    GoldIndexBuildManifest,
    ensure_gold_index,
    gold_index_config_digest,
    gold_index_config_identity,
    gold_index_name,
    validate_gold_index_owner,
)
from video_media_catalog.gold_tables import GOLD_DATA_COLUMNS
from video_media_catalog.models import Checksum, ObjectRef


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
    assert name.startswith("media-catalog-research-")
    assert "sourceBadges" in INDEX_MAPPINGS["properties"]
    assert "winningAssertions" in INDEX_MAPPINGS["properties"]
    assert "rights" in INDEX_MAPPINGS["properties"]
    assert "conflicts" in INDEX_MAPPINGS["properties"]
    assert "ownerSubject" in INDEX_MAPPINGS["properties"]
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
