from __future__ import annotations

import csv
import gzip
import hashlib
import json
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from video_media_catalog.connector import (
    ChangeSemantics,
    Completeness,
    ConnectorRecordEnvelope,
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
from video_media_catalog.storage import local_path
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
    conflict_frames = None
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
        index_rows = identity_frames["community_external_id_index"].collect()
        assert {"imdb-title", "tvmaze-show"}.issubset(
            {row.namespace_id for row in index_rows}
        )

        competing_key = "sha256:" + ("a" * 64)
        conflict_run, conflict_frames = build_identity_resolution_dataframes(
            spark,
            visible_silver=frames,
            v1_external_identifiers=spark.createDataFrame(
                [
                    (v1_key, "imdb", "tt0000001"),
                    (competing_key, "imdb", "tt0000001"),
                ],
                "entity_key STRING, scheme STRING, value STRING",
            ),
            v1_entities=spark.createDataFrame(
                [
                    (v1_key, "TV_SERIES"),
                    (competing_key, "TV_SERIES"),
                ],
                "entity_key STRING, entity_type STRING",
            ),
            input_id="sha256:" + ("5" * 64),
            image_digest="sha256:" + ("4" * 64),
            config_digest="sha256:" + ("3" * 64),
            started_at="2026-09-19T00:00:00Z",
        )
        conflicts = conflict_frames["community_identity_conflict"].collect()
        assert len(conflicts) == 1
        assert conflict_run.expected_counts["community_identity_conflict"] == 1
        assert conflict_frames["community_entity_membership"].count() == 0
        assert set(json.loads(conflicts[0].candidate_entity_keys_json)) == {
            v1_key,
            competing_key,
        }
    finally:
        if conflict_frames is not None:
            for frame in conflict_frames.values():
                frame.unpersist()
        if identity_frames is not None:
            for frame in identity_frames.values():
                frame.unpersist()
        for frame in frames.values():
            frame.unpersist()


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
    envelopes = []
    for item in json.loads(raw_payload):
        envelopes.append(
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
        )
    record_path = tmp_path / "records.ndjson"
    record_path.write_bytes(
        b"".join(envelope.json_bytes() + b"\n" for envelope in envelopes)
    )
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
    _run, frames = build_source_silver_dataframes(
        spark,
        batch=batch,
        record_set=record_set,
    )
    identity_frames = None
    try:
        identity_run, identity_frames = build_identity_resolution_dataframes(
            spark,
            visible_silver=frames,
            v1_external_identifiers=spark.createDataFrame(
                [],
                "entity_key STRING, scheme STRING, value STRING",
            ),
            v1_entities=spark.createDataFrame(
                [],
                "entity_key STRING, entity_type STRING",
            ),
            input_id="sha256:" + ("8" * 64),
            image_digest="sha256:" + ("7" * 64),
            config_digest="sha256:" + ("6" * 64),
            started_at="2026-09-19T00:00:00Z",
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


def _imdb_series_datasets(tmp_path: Path) -> dict[str, Path]:
    from video_media_catalog.imdb import IMDB_DATASET_COLUMNS, IMDB_DATASET_FILES

    rows = {
        "title.basics.tsv.gz": [
            "tt0000099",
            "tvseries",
            "Series Example",
            "Series Example",
            "0",
            "2020",
            r"\N",
            "45",
            "Drama",
        ],
        "title.akas.tsv.gz": [
            "tt0000099",
            "1",
            "Series Alias",
            "US",
            "en",
            "imdbDisplay",
            r"\N",
            "0",
        ],
        "title.episode.tsv.gz": ["tt0000002", "tt0000099", "1", "1"],
        "title.crew.tsv.gz": ["tt0000099", r"\N", r"\N"],
        "title.principals.tsv.gz": [
            "tt0000099",
            "1",
            "nm0000001",
            "actor",
            r"\N",
            '["Lead"]',
        ],
        "title.ratings.tsv.gz": ["tt0000099", "7.5", "100"],
        "name.basics.tsv.gz": [
            "nm0000001",
            "Example Person",
            "1980",
            r"\N",
            "actor",
            "tt0000099",
        ],
    }
    paths: dict[str, Path] = {}
    for dataset in IMDB_DATASET_FILES:
        path = tmp_path / dataset
        path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(path, mode="wt", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle, delimiter="\t", lineterminator="\n")
            writer.writerow(IMDB_DATASET_COLUMNS[dataset])
            writer.writerow(rows[dataset])
        paths[dataset] = path
    return paths


@pytest.mark.spark
def test_imdb_series_identifier_joins_v1_series_entity(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    from video_media_catalog.imdb import map_imdb_record
    from video_media_catalog.imdb_sync import capture_imdb_snapshot
    from video_media_catalog.object_store import BoundedObjectStore
    from video_media_catalog.source_silver import build_source_silver_dataframes

    result = capture_imdb_snapshot(
        dataset_paths=_imdb_series_datasets(tmp_path / "imdb-inputs"),
        destination_prefix=(tmp_path / "imdb-output").as_uri(),
        acquired_at="2026-09-20T00:00:00Z",
        image_digest="sha256:" + ("a" * 64),
        config_digest="sha256:" + ("b" * 64),
        store=BoundedObjectStore(client=object()),
        record_shard_bytes=32 * 1024,
    )
    records = [
        ConnectorRecordEnvelope.model_validate_json(line)
        for reference in result.record_set_manifest.record_objects
        for line in local_path(reference.uri).read_bytes().splitlines()
    ]
    mapped = map_imdb_record(records[0])
    assert mapped.source_node.referent_kind == "EDITORIAL_WORK"
    assert mapped.identifier_assertions[0].referent_kind == "SERIES"
    _run, frames = build_source_silver_dataframes(
        spark,
        batch=result.batch_manifest,
        record_set=result.record_set_manifest,
    )
    identity_frames = None
    try:
        v1_key = "sha256:" + ("9" * 64)
        identity_run, identity_frames = build_identity_resolution_dataframes(
            spark,
            visible_silver=frames,
            v1_external_identifiers=spark.createDataFrame(
                [(v1_key, "imdb", "tt0000099")],
                "entity_key STRING, scheme STRING, value STRING",
            ),
            v1_entities=spark.createDataFrame(
                [(v1_key, "TV_SERIES")],
                "entity_key STRING, entity_type STRING",
            ),
            input_id="sha256:" + ("8" * 64),
            image_digest="sha256:" + ("7" * 64),
            config_digest="sha256:" + ("6" * 64),
            started_at="2026-09-20T00:00:00Z",
        )
        memberships = identity_frames["community_entity_membership"].collect()
        assert identity_run.expected_counts["community_entity_membership"] == 1
        assert memberships[0].entity_key == v1_key
        assert identity_frames["community_entity_ledger"].count() == 0
        assert identity_frames["community_identity_conflict"].count() == 0
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
