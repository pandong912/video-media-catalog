from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

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
from video_media_catalog.identity_spark import (
    MAX_EXACT_BLOCKING_NODE_CANDIDATE_KEYS,
    assign_exact_blocking_component_ids,
    build_identity_resolution_dataframes,
)
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.source_silver import build_source_silver_dataframes
from video_media_catalog.tvmaze import (
    TVMAZE_CONNECTOR_ID,
    TVMAZE_POLICY_ID,
    TVMAZE_SOURCE_PRODUCT_ID,
    TVMAZE_SOURCE_SYSTEM_ID,
    tvmaze_rights_profile,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("identity-exact-blocking-regression")
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
def test_exact_blocking_label_propagation_converges_on_long_chain(
    spark: SparkSession,
) -> None:
    node_count = 13
    node_ids = [f"ns\x1fnode-{index:02d}\x1fSERIES" for index in range(node_count)]
    nodes = spark.createDataFrame(
        [(node_id,) for node_id in node_ids], "node_id STRING"
    )
    edges: list[tuple[str, str]] = []
    for index in range(node_count - 1):
        blocking_key = f"shared-key-{index:02d}"
        edges.append((node_ids[index], blocking_key))
        edges.append((node_ids[index + 1], blocking_key))
    blocking_edges = spark.createDataFrame(edges, "node_id STRING, blocking_key STRING")

    labels = assign_exact_blocking_component_ids(nodes, blocking_edges)
    rows = labels.collect()
    expected_component_id = min(node_ids)
    assert len(rows) == node_count
    assert {row.component_id for row in rows} == {expected_component_id}


@pytest.mark.spark
def test_exact_blocking_rejects_single_node_with_too_many_candidates(
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
    _, silver_frames = build_source_silver_dataframes(
        spark,
        registry=build_community_registry(),
        batch=batch,
        record_set=record_set,
    )
    overflow_count = MAX_EXACT_BLOCKING_NODE_CANDIDATE_KEYS + 1
    entity_keys = tuple(f"sha256:{index:064x}" for index in range(overflow_count))
    identity_frames = None
    try:
        _, identity_frames = build_identity_resolution_dataframes(
            spark,
            visible_silver=silver_frames,
            v1_external_identifiers=spark.createDataFrame(
                [(entity_key, "imdb", "tt0000001") for entity_key in entity_keys],
                "entity_key STRING, scheme STRING, value STRING",
            ),
            v1_entities=spark.createDataFrame(
                [(entity_key, "TV_SERIES") for entity_key in entity_keys],
                "entity_key STRING, entity_type STRING",
            ),
            input_id="sha256:" + ("8" * 64),
            image_digest="sha256:" + ("7" * 64),
            config_digest="sha256:" + ("6" * 64),
            started_at="2026-09-19T00:00:00Z",
        )
        conflicts = identity_frames["community_identity_conflict"].collect()
        assert len(conflicts) == 1
        assert conflicts[0].reason == "EXACT_BLOCKING_NODE_CANDIDATE_LIMIT_EXCEEDED"
        assert identity_frames["community_entity_membership"].count() == 0
    finally:
        if identity_frames is not None:
            for frame in identity_frames.values():
                frame.unpersist()
        for frame in silver_frames.values():
            frame.unpersist()
