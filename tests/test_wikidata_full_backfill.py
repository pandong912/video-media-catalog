from __future__ import annotations

import pytest
from pydantic import ValidationError

from video_media_catalog.community_sources import wikidata_rights_profile
from video_media_catalog.connector import (
    CaptureWindowStatus,
    ConnectorRecordShard,
    build_connector_record_set_partition_manifest,
    build_connector_sharded_record_set_manifest,
)
from video_media_catalog.constants import MEDIA_ENTITY_TYPES
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.v1_adapters import WIKIDATA_SOURCE_PRODUCT_ID
from video_media_catalog.wikidata_full_backfill import (
    FullMediaBackfillConfig,
    WikidataFullMediaProfile,
    build_full_media_capture_receipt,
    build_wikidata_full_media_commit,
    full_media_build_digest,
    full_media_coverage_scope,
    plan_full_media_epoch_windows,
)


def _object(name: str = "part.ndjson", *, size: int = 128) -> ObjectRef:
    return ObjectRef(
        uri=f"file:///tmp/{name}",
        format="OBJECT_FORMAT_OTHER",
        media_type="application/octet-stream",
        checksum=Checksum(value="a" * 64),
        size_bytes=size,
        created_at="2026-09-20T00:00:00Z",
    )


def _dump() -> ObjectRef:
    return ObjectRef(
        uri="s3://bucket/wikidata/dump.json.bz2",
        format="OBJECT_FORMAT_OTHER",
        media_type="application/octet-stream",
        checksum=Checksum(value="b" * 64),
        size_bytes=1024,
        etag="abc",
        object_version="v1",
        created_at="2026-09-20T00:00:00Z",
    )


def _profile(**overrides) -> WikidataFullMediaProfile:
    dump = _dump()
    config = FullMediaBackfillConfig()
    image_digest = "sha256:" + ("c" * 64)
    build_digest = full_media_build_digest(
        dump=dump,
        config_digest=config.digest,
        image_digest=image_digest,
    )
    coverage_scope = full_media_coverage_scope()
    values = {
        "build_digest": build_digest,
        "config_digest": config.digest,
        "image_digest": image_digest,
        "batch_id": "sha256:" + ("d" * 64),
        "dump": dump,
        "coverage_scope": coverage_scope,
        "coverage_scope_digest": coverage_scope,
        "root_counts": {entity_type: 1 for entity_type in sorted(MEDIA_ENTITY_TYPES)},
        "selected_type_counts": {
            "MOVIE": 4,
            "TV_SERIES": 1,
            "TV_SEASON": 1,
            "TV_EPISODE": 1,
            "PERSON": 2,
            "ORGANIZATION": 1,
            "UNKNOWN": 0,
        },
        "parent_count": 1,
        "credit_person_count": 2,
        "credit_organization_count": 1,
        "record_count": 10,
        "relation_edge_count": 12,
        "parent_edge_count": 2,
        "credit_edge_count": 4,
        "envelope_bytes": 4096,
        "estimated_shards": 1,
        "estimated_partitions": 1,
        "estimated_epochs": 1,
    }
    values.update(overrides)
    from video_media_catalog.v2_contracts import digest_identity

    values["coverage_scope_digest"] = digest_identity(values["coverage_scope"])
    return WikidataFullMediaProfile.model_validate(values)


def test_full_media_config_and_coverage_scope_are_bounded() -> None:
    config = FullMediaBackfillConfig(
        target_shard_bytes=1024,
        max_shard_bytes=2048,
        max_shards_per_partition=2,
        max_partitions_per_epoch=2,
        max_epochs=2,
    )
    assert config.max_supported_shards == 8
    scope = full_media_coverage_scope()
    assert set(scope["rootEntityTypes"]) == set(MEDIA_ENTITY_TYPES)


def test_epoch_window_planner_covers_every_epoch_without_truncation() -> None:
    profile = _profile(estimated_epochs=3)
    plans = plan_full_media_epoch_windows(
        profile,
        dump_date="20260914",
        max_epochs_per_window=1,
    )
    assert len(plans) == 3
    assert [plan.item_count for plan in plans] == [1, 1, 1]
    assert plans[0].source_product_id == WIKIDATA_SOURCE_PRODUCT_ID


def test_capture_receipt_binds_batch_and_dump_window() -> None:
    batch_object = _object("batch.json", size=256)
    batch_object = batch_object.model_copy(
        update={
            "format": "OBJECT_FORMAT_JSON",
            "media_type": "application/vnd.video-media-catalog.connector-batch.v2+json",
        }
    )
    config = FullMediaBackfillConfig()
    image_digest = "sha256:" + ("e" * 64)
    policy = wikidata_rights_profile()
    build_digest = full_media_build_digest(
        dump=_dump(),
        config_digest=config.digest,
        image_digest=image_digest,
    )
    receipt = build_full_media_capture_receipt(
        batch_object=batch_object,
        build_digest=build_digest,
        config_digest=config.digest,
        image_digest=image_digest,
        policy_digest=policy.digest,
        dump_date="20260914",
        record_count=10,
    )
    replay = build_full_media_capture_receipt(
        batch_object=batch_object,
        build_digest=build_digest,
        config_digest=config.digest,
        image_digest=image_digest,
        policy_digest=policy.digest,
        dump_date="20260914",
        record_count=10,
    )
    assert receipt == replay
    assert receipt.status == CaptureWindowStatus.COMMITTED
    assert receipt.watermark == "20260914"


def test_sharded_record_set_manifest_round_trip() -> None:
    shard = ConnectorRecordShard(
        shard_index=0,
        object_ref=_object(),
        record_count=2,
        first_envelope_key="sha256:" + ("1" * 64),
        last_envelope_key="sha256:" + ("2" * 64),
    )
    partition = build_connector_record_set_partition_manifest(
        batch_id="sha256:" + ("3" * 64),
        source_product_id=WIKIDATA_SOURCE_PRODUCT_ID,
        policy_id="wikidata-structured-data-cc0",
        policy_digest="sha256:" + ("4" * 64),
        epoch_index=0,
        partition_index=0,
        shards=(shard,),
        shard_count=1,
        record_count=2,
        size_bytes=shard.object_ref.size_bytes,
        first_envelope_key=shard.first_envelope_key,
        last_envelope_key=shard.last_envelope_key,
        created_at="2026-09-20T00:00:00Z",
    )
    epoch_ref = _object("epoch.json", size=512)
    record_set = build_connector_sharded_record_set_manifest(
        batch_id=partition.batch_id,
        source_product_id=partition.source_product_id,
        policy_id=partition.policy_id,
        policy_digest=partition.policy_digest,
        epoch_objects=(epoch_ref,),
        epoch_count=1,
        partition_count=1,
        shard_count=1,
        record_count=2,
        size_bytes=shard.object_ref.size_bytes,
        first_envelope_key=shard.first_envelope_key,
        last_envelope_key=shard.last_envelope_key,
        created_at=partition.created_at,
    )
    assert record_set == build_connector_sharded_record_set_manifest(
        **record_set.model_dump(mode="python")
    )


def test_backfill_commit_requires_watermark_and_receipt() -> None:
    profile = _profile()
    with pytest.raises(ValidationError):
        build_wikidata_full_media_commit(
            build_digest=profile.build_digest,
            config_digest=profile.config_digest,
            image_digest=profile.image_digest,
            dump=profile.dump,
            batch_manifest=_object("batch.json"),
            record_set_manifest=_object("record-set.json"),
            profile=profile,
            created_at="2026-09-20T00:00:00Z",
        )
