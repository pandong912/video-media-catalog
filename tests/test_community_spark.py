from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

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
from video_media_catalog.identity_spark import (
    build_identity_resolution_dataframes,
)
from video_media_catalog.models import Checksum, ObjectRef, SnapshotSet
from video_media_catalog.source_silver import (
    build_source_silver_dataframes,
)
from video_media_catalog.tvmaze import (
    TVMAZE_CONNECTOR_ID,
    TVMAZE_POLICY_ID,
    TVMAZE_SOURCE_PRODUCT_ID,
    TVMAZE_SOURCE_SYSTEM_ID,
    tvmaze_rights_profile,
)
from video_media_catalog.v1_migration import build_v1_key_migration


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("community-catalog-v2-unit-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
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
def test_tvmaze_record_set_projects_to_silver_dataframes(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    raw_payload = json.dumps(
        [
            {
                "id": 1,
                "name": "Example",
                "type": "Scripted",
                "language": "English",
                "updated": 1_700_000_000,
                "genres": ["Drama"],
                "externals": {"imdb": "tt0000001"},
            }
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
        record_count=1,
        error_count=0,
    )
    envelope = build_connector_record_envelope(
        payload=json.loads(raw_payload)[0],
        batch_id=batch.batch_id,
        source_system_id=batch.source_system_id,
        source_product_id=batch.source_product_id,
        source_namespace_id="tvmaze-show",
        source_record_id="1",
        source_revision="1700000000",
        operation=RecordOperation.UPSERT,
        source_modified_at="2023-11-14T22:13:20Z",
        observed_at=batch.acquired_at,
        ingested_at=batch.acquired_at,
        payload_schema="tvmaze-show-v1",
        raw_object=raw_object,
        source_location="/page/0/item/0",
        policy_id=batch.policy_id,
        policy_digest=batch.policy_digest,
    )
    record_path = tmp_path / "records.ndjson"
    record_path.write_bytes(envelope.json_bytes())
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
        record_count=1,
        first_envelope_key=envelope.envelope_key,
        last_envelope_key=envelope.envelope_key,
        created_at=batch.acquired_at,
    )
    run, frames = build_source_silver_dataframes(
        spark,
        batch=batch,
        record_set=record_set,
    )
    identity_frames = None
    try:
        assert frames["community_source_record"].count() == 1
        assert frames["community_field_assertion"].count() >= 3
        assert frames["community_identifier_assertion"].count() == 2
        assert run.expected_counts["community_entity_type_assertion"] == 1
        v1_key = "sha256:" + ("9" * 64)
        identity_run, identity_frames = build_identity_resolution_dataframes(
            spark,
            visible_silver=frames,
            v1_external_identifiers=spark.createDataFrame(
                [(v1_key, "imdb", "tt0000001")],
                "entity_key STRING, scheme STRING, value STRING",
            ),
            v1_entities=spark.createDataFrame(
                [(v1_key, "TV_SERIES")],
                "entity_key STRING, entity_type STRING",
            ),
            input_id="sha256:" + ("8" * 64),
            image_digest="sha256:" + ("7" * 64),
            config_digest="sha256:" + ("6" * 64),
            started_at="2026-09-19T00:00:00Z",
        )
        assert identity_frames["community_entity_ledger"].count() == 0
        memberships = identity_frames["community_entity_membership"].collect()
        assert memberships[0].entity_key == v1_key
        assert identity_run.expected_counts["community_entity_membership"] == 1
    finally:
        if identity_frames is not None:
            for frame in identity_frames.values():
                frame.unpersist()
        for frame in frames.values():
            frame.unpersist()


@pytest.mark.spark
def test_v1_migration_preserves_every_key(
    spark: SparkSession,
    fixture_dir: Path,
) -> None:
    snapshot_set = SnapshotSet.model_validate_json(
        (fixture_dir / "control/media_catalog_snapshot_set.v1.json").read_bytes()
    )
    v1_tables = {
        "catalog_source_record": spark.createDataFrame(
            [("sha256:" + ("1" * 64),)],
            "record_key STRING",
        ),
        "catalog_entity": spark.createDataFrame(
            [("sha256:" + ("2" * 64), "MOVIE")],
            "entity_key STRING, entity_type STRING",
        ),
        "catalog_name": spark.createDataFrame(
            [("sha256:" + ("3" * 64),)],
            "name_key STRING",
        ),
        "catalog_external_identifier": spark.createDataFrame(
            [("sha256:" + ("4" * 64),)],
            "identifier_key STRING",
        ),
        "catalog_relation": spark.createDataFrame(
            [("sha256:" + ("5" * 64),)],
            "relation_key STRING",
        ),
        "catalog_ingest_error": spark.createDataFrame(
            [("sha256:" + ("6" * 64),)],
            "error_key STRING",
        ),
    }
    run, frames = build_v1_key_migration(
        spark,
        snapshot_set=snapshot_set,
        v1_tables=v1_tables,
    )
    entities = frames["community_entity_ledger"].collect()
    mappings = frames["community_legacy_key_map"].collect()
    assert entities[0].entity_key == "sha256:" + ("2" * 64)
    assert entities[0].imported_v1 is True
    assert len(mappings) == 6
    assert all(row.legacy_key == row.target_key for row in mappings)
    assert run.expected_counts["community_legacy_key_map"] == 6
