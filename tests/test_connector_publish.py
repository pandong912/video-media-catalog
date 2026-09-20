from __future__ import annotations

from pathlib import Path

import pytest

from video_media_catalog.connector import (
    ChangeSemantics,
    Completeness,
    DeleteCoverage,
    RecordOperation,
    Serialization,
    TransportKind,
    build_connector_batch_manifest,
    build_connector_record_envelope,
)
from video_media_catalog.connector_publish import (
    ENVELOPE_KEY_INDEX_CACHE_KIB,
    _EnvelopeKeyIndex,
    finalize_connector_partitions,
    publish_connector_batch_manifest,
    publish_connector_capture,
    publish_connector_partition,
)
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.object_store import BoundedObjectStore
from video_media_catalog.tvmaze import tvmaze_rights_profile


def _raw_object() -> ObjectRef:
    return ObjectRef(
        uri="file:///tmp/page.json",
        format="OBJECT_FORMAT_JSON",
        media_type="application/json",
        checksum=Checksum(value="a" * 64),
        size_bytes=123,
        created_at="2026-09-19T00:00:00Z",
    )


def _batch(record_count: int):
    policy = tvmaze_rights_profile()
    return build_connector_batch_manifest(
        source_system_id="tvmaze",
        source_product_id="tvmaze-public-api",
        connector_id="tvmaze-show-index",
        connector_version="1.0.0",
        image_digest="sha256:" + ("b" * 64),
        config_digest="sha256:" + ("c" * 64),
        policy_id=policy.policy_id,
        policy_digest=policy.digest,
        transport_kind=TransportKind.API,
        serialization=Serialization.JSON,
        change_semantics=ChangeSemantics.FULL_SNAPSHOT,
        completeness=Completeness.COMPLETE,
        delete_coverage=DeleteCoverage.SNAPSHOT_DIFF,
        coverage_scope={"endpoint": "/shows", "pages": [0]},
        raw_objects=(_raw_object(),),
        acquired_at="2026-09-19T00:00:00Z",
        record_count=record_count,
        error_count=0,
    )


def _envelope(batch, source_record_id: str):
    return build_connector_record_envelope(
        payload={"id": source_record_id},
        batch_id=batch.batch_id,
        source_system_id=batch.source_system_id,
        source_product_id=batch.source_product_id,
        source_namespace_id="tvmaze-show",
        source_record_id=source_record_id,
        operation=RecordOperation.UPSERT,
        observed_at=batch.acquired_at,
        ingested_at=batch.acquired_at,
        payload_schema="tvmaze-show-v1",
        raw_object=batch.raw_objects[0],
        source_location=f"/{source_record_id}",
        policy_id=batch.policy_id,
        policy_digest=batch.policy_digest,
    )


def test_envelope_key_index_is_disk_backed_and_page_cache_bounded() -> None:
    with _EnvelopeKeyIndex() as index:
        database_path = index.database_path
        assert database_path.exists()
        assert index.cache_limit_bytes == ENVELOPE_KEY_INDEX_CACHE_KIB * 1024
        assert index.uses_file_temp_storage
        assert index.journal_mode == "off"
        assert not any(
            isinstance(value, (set, list, dict)) for value in vars(index).values()
        )
        for number in range(25_000):
            index.add(f"sha256:{number:064x}")
        with pytest.raises(ValueError, match="duplicate envelope keys"):
            index.add("sha256:" + ("0" * 64))
    assert not database_path.exists()


def test_publisher_detects_duplicate_from_an_earlier_shard(
    tmp_path: Path,
) -> None:
    batch = _batch(3)
    first = _envelope(batch, "1")
    second = _envelope(batch, "2")
    shard_bytes = max(len(first.json_bytes()), len(second.json_bytes()))
    destination = tmp_path / "capture"

    with pytest.raises(ValueError, match="duplicate envelope keys"):
        publish_connector_capture(
            destination_prefix=destination.as_uri(),
            batch=batch,
            envelopes=(first, second, first),
            store=BoundedObjectStore(),
            record_shard_bytes=shard_bytes,
        )

    batch_path = (
        destination
        / batch.source_system_id
        / "batches"
        / batch.batch_id.removeprefix("sha256:")
    )
    assert any((batch_path / "records" / "shard=00000").iterdir())
    assert not (batch_path / "record-set.json").exists()


def test_partition_publisher_finalizes_one_flat_record_set(tmp_path: Path) -> None:
    batch = _batch(4)
    destination = tmp_path / "capture"
    store = BoundedObjectStore()
    batch_object = publish_connector_batch_manifest(
        destination_prefix=destination.as_uri(),
        batch=batch,
        store=store,
    )
    first_rows = (
        _envelope(batch, "title.basics:1"),
        _envelope(batch, "title.basics:2"),
    )
    second_rows = (
        _envelope(batch, "title.ratings:1"),
        _envelope(batch, "title.ratings:2"),
    )
    shard_bytes = max(len(item.json_bytes()) for item in (*first_rows, *second_rows))
    first = publish_connector_partition(
        destination_prefix=destination.as_uri(),
        batch=batch,
        partition_label="title-basics",
        key_namespace="title.basics:",
        partition_index=0,
        envelopes=first_rows,
        expected_record_count=2,
        store=store,
        record_shard_bytes=shard_bytes,
    )
    second = publish_connector_partition(
        destination_prefix=destination.as_uri(),
        batch=batch,
        partition_label="title-ratings",
        key_namespace="title.ratings:",
        partition_index=1,
        envelopes=second_rows,
        expected_record_count=2,
        store=store,
        record_shard_bytes=shard_bytes,
    )

    capture = finalize_connector_partitions(
        destination_prefix=destination.as_uri(),
        batch=batch,
        batch_manifest_object=batch_object,
        partitions=(second, first),
        store=store,
    )

    assert capture.record_set_manifest.record_count == 4
    assert capture.record_set_manifest.first_envelope_key == (
        first.manifest.first_envelope_key
    )
    assert capture.record_set_manifest.last_envelope_key == (
        second.manifest.last_envelope_key
    )
    assert capture.record_set_manifest.record_objects == tuple(
        shard.object_ref
        for partition in (first, second)
        for shard in partition.manifest.shards
    )
    assert (
        destination
        / batch.source_system_id
        / "batches"
        / batch.batch_id.removeprefix("sha256:")
        / "record-set.json"
    ).exists()


def test_partition_finalizer_rejects_overlapping_key_namespaces(
    tmp_path: Path,
) -> None:
    batch = _batch(2)
    destination = tmp_path / "capture"
    store = BoundedObjectStore()
    batch_object = publish_connector_batch_manifest(
        destination_prefix=destination.as_uri(),
        batch=batch,
        store=store,
    )
    partitions = tuple(
        publish_connector_partition(
            destination_prefix=destination.as_uri(),
            batch=batch,
            partition_label=f"partition-{index}",
            key_namespace="overlap:",
            partition_index=index,
            envelopes=(_envelope(batch, f"overlap:{index}"),),
            expected_record_count=1,
            store=store,
        )
        for index in range(2)
    )

    with pytest.raises(ValueError, match="key namespaces must be disjoint"):
        finalize_connector_partitions(
            destination_prefix=destination.as_uri(),
            batch=batch,
            batch_manifest_object=batch_object,
            partitions=partitions,
            store=store,
        )

    assert not (
        destination
        / batch.source_system_id
        / "batches"
        / batch.batch_id.removeprefix("sha256:")
        / "record-set.json"
    ).exists()


def test_partition_publisher_rejects_duplicate_without_commit(
    tmp_path: Path,
) -> None:
    batch = _batch(3)
    destination = tmp_path / "capture"
    first = _envelope(batch, "title.basics:1")
    second = _envelope(batch, "title.basics:2")

    with pytest.raises(ValueError, match="duplicate envelope keys"):
        publish_connector_partition(
            destination_prefix=destination.as_uri(),
            batch=batch,
            partition_label="title-basics",
            key_namespace="title.basics:",
            partition_index=0,
            envelopes=(first, second, first),
            expected_record_count=3,
            store=BoundedObjectStore(),
            record_shard_bytes=max(
                len(first.json_bytes()),
                len(second.json_bytes()),
            ),
        )

    partition = (
        destination
        / batch.source_system_id
        / "batches"
        / batch.batch_id.removeprefix("sha256:")
        / "record-set"
        / "partitions"
        / "partition=00000-title-basics.json"
    )
    assert not partition.exists()


def test_partition_finalizer_rejects_overlapping_namespace_prefixes(
    tmp_path: Path,
) -> None:
    batch = _batch(2)
    destination = tmp_path / "capture"
    store = BoundedObjectStore()
    batch_object = publish_connector_batch_manifest(
        destination_prefix=destination.as_uri(),
        batch=batch,
        store=store,
    )
    envelope = _envelope(batch, "overlap:child:1")
    partitions = (
        publish_connector_partition(
            destination_prefix=destination.as_uri(),
            batch=batch,
            partition_label="parent",
            key_namespace="overlap:",
            partition_index=0,
            envelopes=(envelope,),
            expected_record_count=1,
            store=store,
        ),
        publish_connector_partition(
            destination_prefix=destination.as_uri(),
            batch=batch,
            partition_label="child",
            key_namespace="overlap:child:",
            partition_index=1,
            envelopes=(envelope,),
            expected_record_count=1,
            store=store,
        ),
    )

    with pytest.raises(ValueError, match="must not overlap"):
        finalize_connector_partitions(
            destination_prefix=destination.as_uri(),
            batch=batch,
            batch_manifest_object=batch_object,
            partitions=partitions,
            store=store,
        )


def test_partition_publisher_rejects_record_outside_key_namespace(
    tmp_path: Path,
) -> None:
    batch = _batch(1)
    with pytest.raises(ValueError, match="outside its partition key namespace"):
        publish_connector_partition(
            destination_prefix=(tmp_path / "capture").as_uri(),
            batch=batch,
            partition_label="title-basics",
            key_namespace="title.basics:",
            partition_index=0,
            envelopes=(_envelope(batch, "title.ratings:1"),),
            expected_record_count=1,
            store=BoundedObjectStore(),
        )


def test_partition_finalizer_rejects_missing_index(tmp_path: Path) -> None:
    batch = _batch(2)
    destination = tmp_path / "capture"
    store = BoundedObjectStore()
    batch_object = publish_connector_batch_manifest(
        destination_prefix=destination.as_uri(),
        batch=batch,
        store=store,
    )
    partitions = tuple(
        publish_connector_partition(
            destination_prefix=destination.as_uri(),
            batch=batch,
            partition_label=f"partition-{index}",
            key_namespace=f"partition-{index}:",
            partition_index=index,
            envelopes=(_envelope(batch, f"partition-{index}:1"),),
            expected_record_count=1,
            store=store,
        )
        for index in (0, 2)
    )

    with pytest.raises(ValueError, match="contiguous indexes"):
        finalize_connector_partitions(
            destination_prefix=destination.as_uri(),
            batch=batch,
            batch_manifest_object=batch_object,
            partitions=partitions,
            store=store,
        )
