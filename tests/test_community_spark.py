from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from video_media_catalog.community_ingest import CommunityIngestRun
from video_media_catalog.community_rows import ingest_run_row
from video_media_catalog.community_sources import build_community_registry
from video_media_catalog.community_spark import community_table_schema
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
from video_media_catalog.identity_spark import build_identity_resolution_dataframes
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.source_silver import build_source_silver_dataframes
from video_media_catalog.tvmaze import (
    TVMAZE_CONNECTOR_ID,
    TVMAZE_POLICY_ID,
    TVMAZE_SOURCE_PRODUCT_ID,
    TVMAZE_SOURCE_SYSTEM_ID,
    tvmaze_rights_profile,
)


def _identity_lifecycle_inputs(
    spark: SparkSession,
    run: CommunityIngestRun,
    visible: dict[str, object],
) -> dict[str, object]:
    return {
        "source_records": visible["community_source_record"],
        "ingest_runs": spark.createDataFrame(
            [ingest_run_row(run)],
            schema=community_table_schema("community_ingest_run"),
        ),
        "committed_source_run_ids": (run.run_id,),
    }


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    scratch = tmp_path_factory.mktemp("community-identity-checkpoints")
    session = (
        SparkSession.builder.master("local[2]")
        .appName("community-catalog-v2-unit-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setCheckpointDir(scratch.as_posix())
    yield session
    session.stop()


def _object(path: Path, *, media_type: str, object_format: str) -> ObjectRef:
    payload = path.read_bytes()
    return ObjectRef(
        uri=path.as_uri(),
        format=object_format,
        media_type=media_type,
        checksum=Checksum(value=hashlib.sha256(payload).hexdigest()),
        size_bytes=len(payload),
        created_at="2026-09-19T00:00:00Z",
    )


@pytest.mark.spark
def test_shared_imdb_blocking_key_unifies_unassigned_source_nodes(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    raw_payload = json.dumps(
        [
            {
                "id": 1,
                "name": "Example One",
                "type": "Scripted",
                "language": "English",
                "updated": 1_700_000_000,
                "genres": ["Drama"],
                "externals": {"imdb": "tt0000001"},
            },
            {
                "id": 2,
                "name": "Example Two",
                "type": "Scripted",
                "language": "English",
                "updated": 1_700_000_001,
                "genres": ["Mystery"],
                "externals": {"imdb": "tt0000001"},
            },
        ]
    ).encode()
    raw_path = tmp_path / "page.json"
    raw_path.write_bytes(raw_payload)
    raw_object = _object(
        raw_path,
        media_type="application/json",
        object_format="OBJECT_FORMAT_JSON",
    )
    policy = tvmaze_rights_profile()
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
        acquired_at="2026-09-19T00:00:00Z",
        record_count=2,
        error_count=0,
    )
    envelopes = [
        build_connector_record_envelope(
            payload=item,
            batch_id=batch.batch_id,
            source_system_id=batch.source_system_id,
            source_product_id=batch.source_product_id,
            source_namespace_id="tvmaze-show",
            source_record_id=str(item["id"]),
            source_revision=str(item["updated"]),
            operation=RecordOperation.UPSERT,
            source_modified_at="2023-11-14T22:13:20Z",
            observed_at=batch.acquired_at,
            ingested_at=batch.acquired_at,
            payload_schema="tvmaze-show-v1",
            raw_object=raw_object,
            source_location=f"/page/0/item/{item['id']}",
            policy_id=batch.policy_id,
            policy_digest=batch.policy_digest,
        )
        for item in json.loads(raw_payload)
    ]
    record_path = tmp_path / "records.ndjson"
    record_path.write_bytes(b"".join(envelope.json_bytes() for envelope in envelopes))
    record_object = _object(
        record_path,
        media_type=(
            "application/vnd.video-media-catalog.connector-record-envelope.v2+ndjson"
        ),
        object_format="OBJECT_FORMAT_OTHER",
    )
    record_set = build_connector_record_set_manifest(
        batch_id=batch.batch_id,
        source_product_id=batch.source_product_id,
        policy_id=batch.policy_id,
        policy_digest=batch.policy_digest,
        record_objects=(record_object,),
        record_count=2,
        first_envelope_key=envelopes[0].envelope_key,
        last_envelope_key=envelopes[1].envelope_key,
        created_at=batch.acquired_at,
    )
    run, frames = build_source_silver_dataframes(
        spark,
        registry=build_community_registry(),
        batch=batch,
        record_set=record_set,
    )
    identity_frames = None
    try:
        identity_run, identity_frames = build_identity_resolution_dataframes(
            spark,
            visible_silver=frames,
            input_id="sha256:" + ("8" * 64),
            image_digest="sha256:" + ("7" * 64),
            config_digest="sha256:" + ("6" * 64),
            started_at="2026-09-19T00:00:00Z",
            **_identity_lifecycle_inputs(spark, run, frames),
        )
        memberships = identity_frames["community_entity_membership"].collect()
        assert identity_run.expected_counts["community_entity_membership"] == 2
        assert identity_frames["community_entity_ledger"].count() == 1
        assert len({row.entity_key for row in memberships}) == 1
        assert identity_frames["community_identity_conflict"].count() == 0
    finally:
        if identity_frames is not None:
            for frame in identity_frames.values():
                frame.unpersist()
        for frame in frames.values():
            frame.unpersist()
