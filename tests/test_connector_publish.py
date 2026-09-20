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
    publish_connector_capture,
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
