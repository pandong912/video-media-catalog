from __future__ import annotations

from video_media_catalog.gold_ingest import GOLD_RELEASE_COMMIT_MEDIA_TYPE
from video_media_catalog.gold_search_index import (
    INDEX_MAPPINGS,
    MAPPING_DIGEST,
    SHADOW_READ_ALIAS,
    GoldIndexBuildManifest,
    ensure_gold_index,
    gold_index_config_digest,
    gold_index_name,
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


class FakeClient:
    def __init__(self) -> None:
        self.indices = FakeIndices()


def test_gold_shadow_mapping_and_identity_are_isolated_from_v1() -> None:
    assert INDEX_MAPPINGS["dynamic"] == "strict"
    assert SHADOW_READ_ALIAS == "media-catalog-community-v2-shadow-read"
    digest = gold_index_config_digest(
        read_alias=SHADOW_READ_ALIAS,
        index_prefix="media-catalog-community-v2",
        shards=1,
        replicas=0,
        bulk_chunk_size=100,
        bulk_max_chunk_bytes=5 * 1024 * 1024,
        image_digest="sha256:" + ("a" * 64),
    )
    name = gold_index_name(
        "media-catalog-community-v2",
        "b" * 64,
    )
    assert digest.startswith("sha256:")
    assert name.startswith("media-catalog-community-v2-")
    assert INDEX_MAPPINGS["_meta"]["mappingDigest"] == MAPPING_DIGEST


def test_gold_index_creation_reuses_compatible_mapping() -> None:
    client = FakeClient()
    assert ensure_gold_index(
        client,
        index_name="media-catalog-community-v2-build",
        shards=1,
        replicas=0,
    )
    assert not ensure_gold_index(
        client,
        index_name="media-catalog-community-v2-build",
        shards=1,
        replicas=0,
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
    manifest = GoldIndexBuildManifest(
        build_id="b" * 64,
        release_plan_id="sha256:" + ("c" * 64),
        release_commit=reference,
        table_snapshot_ids={
            table: (10 if table == "community_gold_entity" else None)
            for table in GOLD_DATA_COLUMNS
        },
        mapping_digest=MAPPING_DIGEST,
        config_digest="sha256:" + ("d" * 64),
        document_count=1,
        index="media-catalog-community-v2-build",
        alias=SHADOW_READ_ALIAS,
        completed_at="2026-09-19T00:00:00Z",
    )
    assert manifest == type(manifest).model_validate_json(manifest.json_bytes())
