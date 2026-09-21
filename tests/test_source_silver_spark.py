from __future__ import annotations

import hashlib
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import pytest

from video_media_catalog import source_silver, source_silver_checkpoint
from video_media_catalog.community_sources import build_community_registry
from video_media_catalog.connector import (
    ChangeSemantics,
    Completeness,
    DeleteCoverage,
    RecordOperation,
    Serialization,
    TransportKind,
    build_connector_batch_manifest,
    build_connector_record_envelope,
    build_connector_record_set_manifest,
)
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.source_silver import (
    _spark_input_uri,
    build_source_silver_dataframes,
    unpersist_source_silver_frames,
)
from video_media_catalog.source_silver_checkpoint import (
    SOURCE_SILVER_CHECKPOINT_TABLES,
    SourceSilverCheckpointGroupReceipt,
    build_source_silver_checkpoint_identity,
    checkpoint_table_schema,
    source_silver_checkpoint_group_receipt_uri,
    source_silver_checkpoint_group_root_uri,
    source_silver_checkpoint_groups,
    summarize_checkpoint_frame,
)
from video_media_catalog.storage import local_path
from video_media_catalog.tvmaze import (
    TVMAZE_CONNECTOR_ID,
    TVMAZE_POLICY_ID,
    TVMAZE_SOURCE_PRODUCT_ID,
    TVMAZE_SOURCE_SYSTEM_ID,
    tvmaze_rights_profile,
)


def test_spark_input_uri_decodes_percent_encoded_file_paths(tmp_path: Path) -> None:
    path = tmp_path / "sha256=abc" / "sha256:deadbeef.ndjson"
    path.parent.mkdir(parents=True)
    path.write_text('{"value":"ok"}\n', encoding="utf-8")
    parsed = urlparse(path.as_uri())
    encoded_path = parsed.path.replace("=", "%3D").replace(":", "%3A")
    encoded = urlunparse((parsed.scheme, parsed.netloc, encoded_path, "", "", ""))
    assert _spark_input_uri(encoded) == str(path)


@pytest.fixture(scope="module")
def spark():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("source-silver-checkpoint-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def _file_object(
    path: Path,
    payload: bytes,
    *,
    object_format: str,
    media_type: str,
) -> ObjectRef:
    path.write_bytes(payload)
    return ObjectRef(
        uri=path.as_uri(),
        format=object_format,
        media_type=media_type,
        checksum=Checksum(value=hashlib.sha256(payload).hexdigest()),
        size_bytes=len(payload),
        created_at="2026-09-20T00:00:00Z",
    )


def _checkpoint_capture(tmp_path: Path):
    policy = tvmaze_rights_profile()
    raw_object = _file_object(
        tmp_path / "raw.json",
        b"{}",
        object_format="OBJECT_FORMAT_JSON",
        media_type="application/json",
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
        record_count=2,
        error_count=0,
    )
    envelopes = tuple(
        build_connector_record_envelope(
            payload={
                "id": index,
                "name": f"Example {index}",
                "type": "Scripted",
                "language": "English",
                "updated": 1_700_000_000 + index,
                "genres": ["Drama"],
                "externals": {"imdb": f"tt{index:07d}"},
            },
            batch_id=batch.batch_id,
            source_system_id=batch.source_system_id,
            source_product_id=batch.source_product_id,
            source_namespace_id="tvmaze-show",
            source_record_id=str(index),
            source_revision=str(1_700_000_000 + index),
            operation=RecordOperation.UPSERT,
            observed_at=batch.acquired_at,
            ingested_at=batch.acquired_at,
            payload_schema="tvmaze-show-v1",
            raw_object=raw_object,
            source_location=f"/shows/{index}",
            policy_id=batch.policy_id,
            policy_digest=batch.policy_digest,
        )
        for index in (1, 2)
    )
    record_objects = tuple(
        _file_object(
            tmp_path / f"records-{index}.ndjson",
            envelope.json_bytes(),
            object_format="OBJECT_FORMAT_OTHER",
            media_type=(
                "application/vnd.video-media-catalog."
                "connector-record-envelope.v2+ndjson"
            ),
        )
        for index, envelope in enumerate(envelopes)
    )
    record_set = build_connector_record_set_manifest(
        batch_id=batch.batch_id,
        source_product_id=batch.source_product_id,
        policy_id=batch.policy_id,
        policy_digest=batch.policy_digest,
        record_objects=record_objects,
        record_count=len(envelopes),
        first_envelope_key=envelopes[0].envelope_key,
        last_envelope_key=envelopes[-1].envelope_key,
        created_at=batch.acquired_at,
    )
    return build_community_registry(), batch, record_set


def _unpersist(frames) -> None:
    unpersist_source_silver_frames(frames)


@pytest.mark.spark
def test_file_checkpoint_resumes_mapper_and_fails_closed_on_drift(
    spark,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry, batch, record_set = _checkpoint_capture(tmp_path)
    prefix = (tmp_path / "checkpoints").as_uri()
    identity = build_source_silver_checkpoint_identity(
        registry_digest=registry.digest,
        batch=batch,
        record_set=record_set,
        group_size=1,
    )
    groups = source_silver_checkpoint_groups(identity)

    partial = (
        local_path(
            source_silver_checkpoint_group_root_uri(
                prefix,
                identity,
                groups[0],
            )
        )
        / "attempts"
        / ("attempt=" + ("0" * 32))
        / "table=community_source_record"
        / "part-00000.parquet"
    )
    partial.parent.mkdir(parents=True)
    partial.write_bytes(b"interrupted")

    first_progress = []
    first_run, first_frames = build_source_silver_dataframes(
        spark,
        registry=registry,
        batch=batch,
        record_set=record_set,
        checkpoint_prefix=prefix,
        checkpoint_group_size=1,
        checkpoint_progress=first_progress.append,
    )
    try:
        assert first_frames.loaded_tables == {"community_source_record"}
        first_counts = {
            table: (0 if (frame := first_frames.get(table)) is None else frame.count())
            for table in SOURCE_SILVER_CHECKPOINT_TABLES
        }
        first_source_ids = sorted(
            row.source_record_id
            for row in first_frames["community_source_record"].collect()
        )
    finally:
        _unpersist(first_frames)

    receipts = tuple(
        SourceSilverCheckpointGroupReceipt.model_validate_json(
            local_path(
                source_silver_checkpoint_group_receipt_uri(
                    prefix,
                    identity,
                    group,
                )
            ).read_bytes()
        )
        for group in groups
    )
    aggregated = {
        table: sum(receipt.counts[table] for receipt in receipts)
        for table in SOURCE_SILVER_CHECKPOINT_TABLES
    }
    assert aggregated == {
        table: first_run.expected_counts[table]
        for table in SOURCE_SILVER_CHECKPOINT_TABLES
    }
    assert aggregated == first_counts
    assert first_source_ids == ["1", "2"]
    assert [item.status for item in first_progress] == [
        "MATERIALIZED",
        "MATERIALIZED",
    ]
    assert [(item.completed_groups, item.total_groups) for item in first_progress] == [
        (1, 2),
        (2, 2),
    ]
    assert all(
        local_path(item.success_object.uri).is_file()
        for receipt in receipts
        for item in receipt.tables
    )
    field_receipt = receipts[0].tables[1]
    field_projection = spark.read.schema(
        checkpoint_table_schema(field_receipt.table_name)
    ).parquet(*[str(local_path(item.uri)) for item in field_receipt.data_objects])
    assert "run_id" not in field_projection.columns
    assert summarize_checkpoint_frame(
        field_projection.orderBy("assertion_id", ascending=False),
        field_receipt.table_name,
    ) == (field_receipt.row_count, field_receipt.row_digest)

    def forbidden_summary(_frame, _table):
        raise AssertionError("receipt hit must trust the persisted row digest")

    def forbidden_mapper(_envelope):
        raise AssertionError("mapped mapper should not run on receipt hit")

    monkeypatch.setattr(
        source_silver_checkpoint,
        "summarize_checkpoint_frame",
        forbidden_summary,
    )
    monkeypatch.setattr(
        source_silver,
        "mapper_for_product",
        lambda _source_product_id: forbidden_mapper,
    )
    replay_progress = []
    replay_run, replay_frames = build_source_silver_dataframes(
        spark,
        registry=registry,
        batch=batch,
        record_set=record_set,
        checkpoint_prefix=prefix,
        checkpoint_group_size=1,
        checkpoint_progress=replay_progress.append,
    )
    try:
        assert replay_run.run_id == first_run.run_id
        assert replay_run.expected_counts == first_run.expected_counts
        assert replay_frames.loaded_tables == {"community_source_record"}
        assert replay_frames.get("community_external_id_index") is None
        assert tuple(replay_frames)
        assert replay_frames.loaded_tables == {"community_source_record"}
        assert (
            sorted(
                row.source_record_id
                for row in replay_frames["community_source_record"].collect()
            )
            == first_source_ids
        )
    finally:
        _unpersist(replay_frames)
    assert [item.status for item in replay_progress] == ["REUSED", "REUSED"]

    drifted = local_path(receipts[0].tables[0].data_objects[0].uri)
    original = drifted.read_bytes()
    drifted.write_bytes(original + b"drift")
    with pytest.raises(RuntimeError, match="output metadata changed"):
        build_source_silver_dataframes(
            spark,
            registry=registry,
            batch=batch,
            record_set=record_set,
            checkpoint_prefix=prefix,
            checkpoint_group_size=1,
        )
    drifted.write_bytes(original)

    partial_success = local_path(receipts[0].tables[0].success_object.uri)
    partial_success.unlink()
    with pytest.raises(RuntimeError, match="_SUCCESS"):
        build_source_silver_dataframes(
            spark,
            registry=registry,
            batch=batch,
            record_set=record_set,
            checkpoint_prefix=prefix,
            checkpoint_group_size=1,
        )
