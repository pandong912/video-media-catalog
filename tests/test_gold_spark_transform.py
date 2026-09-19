from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from video_media_catalog.community_release import ReleasePolicyContext
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
from video_media_catalog.gold import community_display_policy
from video_media_catalog.gold_quality import GoldQualityStatus
from video_media_catalog.gold_spark_transform import build_distributed_gold
from video_media_catalog.identity_spark import (
    build_identity_resolution_dataframes,
)
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.rights import PolicyZone
from video_media_catalog.tvmaze import (
    TVMAZE_CONNECTOR_ID,
    TVMAZE_POLICY_ID,
    TVMAZE_SOURCE_PRODUCT_ID,
    TVMAZE_SOURCE_SYSTEM_ID,
    tvmaze_rights_profile,
)
from video_media_catalog.tvmaze_silver import (
    build_tvmaze_silver_dataframes,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("community-gold-spark-unit-test")
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
def test_distributed_silver_identity_and_gold_pipeline(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    show = {
        "id": 1,
        "name": "Example",
        "type": "Scripted",
        "language": "English",
        "status": "Running",
        "updated": 1_700_000_000,
        "genres": ["Drama"],
        "externals": {},
    }
    raw_path = tmp_path / "page.json"
    raw_path.write_text(json.dumps([show]))
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
        payload=show,
        batch_id=batch.batch_id,
        source_system_id=batch.source_system_id,
        source_product_id=batch.source_product_id,
        source_namespace_id="tvmaze-show",
        source_record_id="1",
        source_revision="1700000000",
        operation=RecordOperation.UPSERT,
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
    silver_run, silver_frames = build_tvmaze_silver_dataframes(
        spark,
        batch=batch,
        record_set=record_set,
    )
    identity_frames = None
    gold_build = None
    try:
        identity_run, identity_frames = build_identity_resolution_dataframes(
            spark,
            visible_silver=silver_frames,
            v1_external_identifiers=spark.createDataFrame(
                [],
                "entity_key STRING, scheme STRING, value STRING",
            ),
            v1_entities=spark.createDataFrame(
                [],
                "entity_key STRING, entity_type STRING",
            ),
            input_id="sha256:" + ("c" * 64),
            image_digest="sha256:" + ("d" * 64),
            config_digest="sha256:" + ("e" * 64),
            started_at="2026-09-19T00:00:00Z",
        )
        combined = {
            table: silver_frames[table].unionByName(identity_frames[table])
            for table in silver_frames
        }
        gold_build = build_distributed_gold(
            spark,
            visible_silver=combined,
            registry=build_community_registry(),
            policy_context=ReleasePolicyContext(
                context_id="public-sharealike",
                audience="public",
                purpose="catalog",
                as_of="2026-09-19T00:00:00Z",
                allowed_zones=(PolicyZone.OPEN_SHAREALIKE,),
            ),
            field_policy=community_display_policy(),
            committed_run_ids=(silver_run.run_id, identity_run.run_id),
            silver_snapshot_ids={"community_field_assertion": 10},
            identity_snapshot_ids={"community_entity_membership": 11},
            resolver_digest="sha256:" + ("f" * 64),
            image_digest="sha256:" + ("1" * 64),
            config_digest="sha256:" + ("2" * 64),
            planned_at="2026-09-19T00:00:00Z",
        )
        assert gold_build.quality_report.status == GoldQualityStatus.PASS
        assert gold_build.dataframes["community_gold_entity"].count() == 1
        assert gold_build.dataframes["community_gold_field"].count() >= 4
        assert gold_build.dataframes["community_gold_identifier"].count() == 1
        assert gold_build.attribution_manifest.entries[0].claim_count >= 5
    finally:
        if gold_build is not None:
            gold_build.unpersist()
        if identity_frames is not None:
            for frame in identity_frames.values():
                frame.unpersist()
        for frame in silver_frames.values():
            frame.unpersist()
