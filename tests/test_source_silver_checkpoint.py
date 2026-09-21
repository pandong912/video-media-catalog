from __future__ import annotations

import pytest

from video_media_catalog.community_sources import build_community_registry
from video_media_catalog.community_tables import DATA_TABLE_COLUMNS
from video_media_catalog.connector import (
    ChangeSemantics,
    Completeness,
    DeleteCoverage,
    Serialization,
    TransportKind,
    build_connector_batch_manifest,
    build_connector_record_set_manifest,
)
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.source_silver import unpersist_source_silver_frames
from video_media_catalog.source_silver_checkpoint import (
    DEFAULT_SOURCE_SILVER_CHECKPOINT_GROUP_SIZE,
    SourceSilverCheckpointFrames,
    SourceSilverMapperIdentity,
    SourceSilverSchemaIdentity,
    build_source_silver_checkpoint_identity,
    source_silver_checkpoint_groups,
)
from video_media_catalog.tvmaze import (
    TVMAZE_CONNECTOR_ID,
    TVMAZE_POLICY_ID,
    TVMAZE_SOURCE_PRODUCT_ID,
    TVMAZE_SOURCE_SYSTEM_ID,
    tvmaze_rights_profile,
)


def _object(index: int) -> ObjectRef:
    return ObjectRef(
        uri=f"file:///tmp/source-silver-{index:04d}.ndjson",
        format="OBJECT_FORMAT_OTHER",
        media_type=(
            "application/vnd.video-media-catalog.connector-record-envelope.v2+ndjson"
        ),
        checksum=Checksum(value=f"{index + 1:064x}"),
        size_bytes=index + 1,
        created_at="2026-09-20T00:00:00Z",
    )


def _capture(
    shard_count: int,
):
    policy = tvmaze_rights_profile()
    raw_object = ObjectRef(
        uri="file:///tmp/source-silver-raw.json",
        format="OBJECT_FORMAT_JSON",
        media_type="application/json",
        checksum=Checksum(value="f" * 64),
        size_bytes=1,
        created_at="2026-09-20T00:00:00Z",
    )
    batch = build_connector_batch_manifest(
        source_system_id=TVMAZE_SOURCE_SYSTEM_ID,
        source_product_id=TVMAZE_SOURCE_PRODUCT_ID,
        connector_id=TVMAZE_CONNECTOR_ID,
        connector_version="1.0.0",
        image_digest="sha256:" + ("a" * 64),
        config_digest="sha256:" + ("b" * 64),
        policy_id=TVMAZE_POLICY_ID,
        policy_digest=policy.digest,
        transport_kind=TransportKind.API,
        serialization=Serialization.JSON,
        change_semantics=ChangeSemantics.FULL_SNAPSHOT,
        completeness=Completeness.COMPLETE,
        delete_coverage=DeleteCoverage.SNAPSHOT_DIFF,
        coverage_scope={"endpoint": "/shows"},
        raw_objects=(raw_object,),
        acquired_at="2026-09-20T00:00:00Z",
        record_count=shard_count,
        error_count=0,
    )
    record_set = build_connector_record_set_manifest(
        batch_id=batch.batch_id,
        source_product_id=batch.source_product_id,
        policy_id=batch.policy_id,
        policy_digest=batch.policy_digest,
        record_objects=tuple(_object(index) for index in range(shard_count)),
        record_count=shard_count,
        first_envelope_key=(None if shard_count == 0 else "sha256:" + ("1" * 64)),
        last_envelope_key=(None if shard_count == 0 else "sha256:" + ("2" * 64)),
        created_at=batch.acquired_at,
    )
    return batch, record_set


def test_checkpoint_identity_is_deterministic_and_binds_ordered_sources() -> None:
    registry = build_community_registry()
    batch, record_set = _capture(2)

    first = build_source_silver_checkpoint_identity(
        registry_digest=registry.digest,
        batch=batch,
        record_set=record_set,
    )
    replay = build_source_silver_checkpoint_identity(
        registry_digest=registry.digest,
        batch=batch,
        record_set=record_set,
    )
    reordered = build_source_silver_checkpoint_identity(
        registry_digest=registry.digest,
        batch=batch,
        record_set=record_set.model_copy(
            update={"record_objects": tuple(reversed(record_set.record_objects))}
        ),
    )

    assert first == replay
    assert first.checkpoint_id == replay.checkpoint_id
    assert reordered.record_set_id == first.record_set_id
    assert reordered.checkpoint_id != first.checkpoint_id


def test_checkpoint_identity_invalidates_mapper_schema_and_runtime_digests() -> None:
    registry = build_community_registry()
    batch, record_set = _capture(2)
    baseline = build_source_silver_checkpoint_identity(
        registry_digest=registry.digest,
        batch=batch,
        record_set=record_set,
    )

    variants = (
        build_source_silver_checkpoint_identity(
            registry_digest=registry.digest,
            batch=batch,
            record_set=record_set,
            mapper_identity=SourceSilverMapperIdentity(
                mapper_id=baseline.mapper_identity.mapper_id,
                mapper_version="99.0.0",
            ),
        ),
        build_source_silver_checkpoint_identity(
            registry_digest=registry.digest,
            batch=batch,
            record_set=record_set,
            schema_identity=SourceSilverSchemaIdentity(
                schema_version="99.0",
                schema_digest="sha256:" + ("3" * 64),
            ),
        ),
        build_source_silver_checkpoint_identity(
            registry_digest="sha256:" + ("4" * 64),
            batch=batch,
            record_set=record_set,
        ),
        build_source_silver_checkpoint_identity(
            registry_digest=registry.digest,
            batch=batch.model_copy(update={"policy_digest": "sha256:" + ("5" * 64)}),
            record_set=record_set,
        ),
        build_source_silver_checkpoint_identity(
            registry_digest=registry.digest,
            batch=batch.model_copy(update={"image_digest": "sha256:" + ("6" * 64)}),
            record_set=record_set,
        ),
        build_source_silver_checkpoint_identity(
            registry_digest=registry.digest,
            batch=batch.model_copy(update={"config_digest": "sha256:" + ("7" * 64)}),
            record_set=record_set,
        ),
    )

    assert all(item.checkpoint_id != baseline.checkpoint_id for item in variants)
    assert len({item.checkpoint_id for item in variants}) == len(variants)


def test_checkpoint_groups_are_deterministic_and_default_to_64_shards() -> None:
    registry = build_community_registry()
    batch, record_set = _capture(DEFAULT_SOURCE_SILVER_CHECKPOINT_GROUP_SIZE + 1)
    identity = build_source_silver_checkpoint_identity(
        registry_digest=registry.digest,
        batch=batch,
        record_set=record_set,
    )

    groups = source_silver_checkpoint_groups(identity)
    replay = source_silver_checkpoint_groups(identity)

    assert groups == replay
    assert [len(group.source_objects) for group in groups] == [64, 1]
    assert [group.first_shard_index for group in groups] == [0, 64]
    assert [group.group_index for group in groups] == [0, 1]
    assert len({group.group_id for group in groups}) == 2


def test_lazy_mapping_does_not_load_skipped_or_zero_count_tables() -> None:
    expected_counts = {table: 0 for table in DATA_TABLE_COLUMNS}
    expected_counts["community_field_assertion"] = 3
    frames = SourceSilverCheckpointFrames(
        spark=object(),
        receipts=(),
        expected_counts=expected_counts,
        run_id="sha256:" + ("8" * 64),
    )

    assert tuple(frames) == ("community_field_assertion",)
    assert frames.loaded_tables == frozenset()
    assert frames.get("community_external_id_index") is None
    assert frames.loaded_tables == frozenset()

    unpersist_source_silver_frames(frames)
    with pytest.raises(RuntimeError, match="released"):
        frames["community_field_assertion"]


def test_unpersist_helper_releases_ordinary_frame_dict() -> None:
    class StubFrame:
        released = False

        def unpersist(self) -> None:
            self.released = True

    frame = StubFrame()
    unpersist_source_silver_frames({"community_source_record": frame})
    assert frame.released
