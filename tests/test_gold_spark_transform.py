from __future__ import annotations

import hashlib
import json
from pathlib import Path
from unittest.mock import patch

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from video_media_catalog.assertions import (
    AssertionProvenance,
    SourceNodeRef,
    build_identifier_assertion,
    build_relationship_assertion,
)
from video_media_catalog.canonical import canonical_json
from video_media_catalog.community_ingest import CommunityIngestRun
from video_media_catalog.community_rows import (
    identifier_assertion_row,
    ingest_run_row,
    relationship_assertion_row,
)
from video_media_catalog.community_sources import build_community_registry
from video_media_catalog.community_spark import (
    community_table_schema,
    create_community_dataframes,
)
from video_media_catalog.community_tables import DATA_TABLE_COLUMNS
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
from video_media_catalog.gold import (
    research_context,
    research_policy,
)
from video_media_catalog.gold_quality import GoldQualityStatus
from video_media_catalog.gold_spark_transform import (
    _materialize_resolution_values,
    _project_identifier_assertions,
    _resolve_fields_rdd,
    _resolved_memberships,
    build_distributed_gold,
)
from video_media_catalog.identifier_blocking import (
    exact_id_namespace_rows,
    project_identifier_blocking,
)
from video_media_catalog.identity_spark import (
    build_identity_resolution_dataframes,
)
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.source_lifecycle import (
    build_source_lifecycle_events,
    persist_latest_source_record_states,
)
from video_media_catalog.source_silver import build_source_silver_rows
from video_media_catalog.tmdb import (
    TMDB_CHANGES_CONNECTOR_ID,
    TMDB_MOVIE_NAMESPACE_ID,
    TMDB_POLICY_ID,
    TMDB_SOURCE_PRODUCT_ID,
    TMDB_SOURCE_SYSTEM_ID,
    tmdb_rights_profile,
)
from video_media_catalog.tvmaze import (
    TVMAZE_CONNECTOR_ID,
    TVMAZE_DELTA_CONNECTOR_ID,
    TVMAZE_POLICY_ID,
    TVMAZE_SOURCE_PRODUCT_ID,
    TVMAZE_SOURCE_SYSTEM_ID,
    tvmaze_rights_profile,
)
from video_media_catalog.tvmaze_silver import (
    build_tvmaze_silver_dataframes,
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
def spark(tmp_path_factory: pytest.TempPathFactory):
    session = (
        SparkSession.builder.master("local[2]")
        .appName("community-gold-spark-unit-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setCheckpointDir(
        str(tmp_path_factory.mktemp("gold-spark-checkpoints"))
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
def test_resolution_shuffle_uses_reliable_checkpoint(
    spark: SparkSession,
) -> None:
    values = spark.sparkContext.parallelize(
        [("alpha", 1), ("alpha", 2), ("beta", 3)],
        2,
    ).reduceByKey(lambda left, right: left + right)

    materialized = _materialize_resolution_values(
        values,
        label="test resolution",
    )
    try:
        assert materialized.isCheckpointed()
        assert materialized.getCheckpointFile()
        assert dict(materialized.collect()) == {"alpha": 3, "beta": 3}
    finally:
        materialized.unpersist()


@pytest.mark.spark
def test_field_resolution_streams_single_values_after_checkpoint(
    spark: SparkSession,
) -> None:
    priority = canonical_json(["src-a", "src-b"])

    def row(
        label: str,
        *,
        value: str,
        source_product_id: str,
    ) -> dict[str, object]:
        assertion_id = "sha256:" + hashlib.sha256(label.encode()).hexdigest()
        return {
            "resolved_entity_key": "entity-a",
            "predicate": "title",
            "scope_json": "{}",
            "resolution_operator": "SINGLE",
            "source_priority_json": priority,
            "value_type": "STRING",
            "value_json": canonical_json(value),
            "assertion_id": assertion_id,
            "source_product_id": source_product_id,
            "provenance_json": canonical_json(
                {"sourcePath": "$.title", "citationKeys": []}
            ),
            "citation_keys_json": "[]",
            "source_name": source_product_id,
            "source_record_id": f"record-{label}",
            "observed_at": "2026-10-01T00:00:00Z",
            "rights_policy_id": "policy",
            "rights_zone": "research_private",
            "rights_license_id": "license",
            "rights_license_uri": None,
            "rights_attribution_text": "attr",
            "source_documentation_url": "https://example.test",
            "rights_share_alike": False,
        }

    drafts = _resolve_fields_rdd(
        spark.sparkContext.parallelize(
            [
                row("alpha-primary", value="Alpha", source_product_id="src-a"),
                row("alpha-secondary", value="Alpha", source_product_id="src-b"),
                row("beta-secondary", value="Beta", source_product_id="src-b"),
            ],
            2,
        )
    ).collect()

    assert len(drafts) == 1
    kind, draft = drafts[0]
    assert kind == "field"
    assert draft.value == "Alpha"
    assert len(draft.assertion_ids) == 3


@pytest.mark.spark
def test_spark_identifier_projection_matches_in_memory_blocking(
    spark: SparkSession,
) -> None:
    raw = (
        ("1", "imdb-title", "tt0042816", "EDITORIAL_WORK", "EDITORIAL_WORK", "MOVIE"),
        ("2", "imdb-title", "TT0042816", "EDITORIAL_WORK", "EPISODE", "TV_EPISODE"),
        ("3", "imdb-name", "nm0000001", "PERSON", "AGENT", "PERSON"),
        ("4", "imdb-name", "nm0000002", "PERSON", "AGENT", "ORGANIZATION"),
        ("5", "imdb-name", "nm0000003", "PERSON", "SERIES", "TV_SERIES"),
        ("6", "imdb", "TT0042816", "EDITORIAL_WORK", "EDITORIAL_WORK", "MOVIE"),
    )
    rows = [
        {
            "assertion_id": "sha256:" + (source_id * 64),
            "namespace_id": namespace_id,
            "value": value,
            "referent_kind": referent_kind,
            "entity_level": entity_level,
            "entity_kind": entity_kind,
            "resolved_entity_key": "sha256:" + f"{int(source_id):064x}",
        }
        for (
            source_id,
            namespace_id,
            value,
            referent_kind,
            entity_level,
            entity_kind,
        ) in raw
    ]
    registry = build_community_registry()
    projected = _project_identifier_assertions(
        spark,
        spark.createDataFrame(rows),
        registry=registry,
    )
    actual = {
        (
            row.assertion_id,
            row.blocking_namespace_id,
            row.normalized_value,
            row.display_value,
            row.blocking_issuer,
            row.blocking_referent_kind,
        )
        for row in projected.collect()
    }
    namespace_rows = exact_id_namespace_rows(registry)
    expected = set()
    for row, spec in zip(rows, raw, strict=True):
        projection = project_identifier_blocking(
            namespace_rows=namespace_rows,
            namespace_id=spec[1],
            value=spec[2],
            assertion_referent_kind=spec[3],
            entity_level=spec[4],
            entity_kind=spec[5],
        )
        if projection is not None:
            expected.add(
                (
                    row["assertion_id"],
                    projection.namespace_id,
                    projection.normalized_value,
                    projection.display_value,
                    projection.issuer,
                    projection.blocking_referent_kind,
                )
            )

    assert actual == expected
    assert len(actual) == 4
    assert {(item[2], item[3]) for item in actual if item[1] == "imdb-title"} == {
        ("TT0042816", "tt0042816")
    }
    assert {item[5] for item in actual} == {"EDITORIAL_WORK", "EPISODE", "AGENT"}


def _source_capture(
    *,
    acquired_at: str,
    records: tuple[tuple[str, RecordOperation, str | None], ...],
    change_semantics: ChangeSemantics = ChangeSemantics.DELTA,
    delete_coverage: DeleteCoverage = DeleteCoverage.EXPLICIT,
    coverage_scope: dict[str, object] | None = None,
):
    registry = build_community_registry()
    policy = tvmaze_rights_profile()
    identity = hashlib.sha256(
        repr((acquired_at, records, change_semantics, delete_coverage)).encode()
    ).hexdigest()
    raw_object = ObjectRef(
        uri=f"file:///tmp/gold-source-{identity}.json",
        format="OBJECT_FORMAT_JSON",
        media_type="application/json",
        checksum=Checksum(value=identity),
        size_bytes=1,
        created_at=acquired_at,
    )
    batch = build_connector_batch_manifest(
        source_system_id=TVMAZE_SOURCE_SYSTEM_ID,
        source_product_id=TVMAZE_SOURCE_PRODUCT_ID,
        connector_id=(
            TVMAZE_CONNECTOR_ID
            if change_semantics == ChangeSemantics.FULL_SNAPSHOT
            else TVMAZE_DELTA_CONNECTOR_ID
        ),
        connector_version="1.0.0",
        image_digest="sha256:" + ("a" * 64),
        config_digest="sha256:" + ("b" * 64),
        policy_id=TVMAZE_POLICY_ID,
        policy_digest=policy.digest,
        transport_kind=TransportKind.API,
        serialization=Serialization.JSON,
        change_semantics=change_semantics,
        completeness=Completeness.COMPLETE,
        delete_coverage=delete_coverage,
        coverage_scope=coverage_scope or {"endpoint": "/shows"},
        raw_objects=(raw_object,),
        acquired_at=acquired_at,
        record_count=len(records),
        error_count=0,
    )
    envelopes = tuple(
        build_connector_record_envelope(
            **(
                {
                    "payload": {
                        "id": int(source_id),
                        "name": title,
                        "type": "Scripted",
                        "language": "English",
                        "externals": {},
                    }
                }
                if operation == RecordOperation.UPSERT
                else {}
            ),
            batch_id=batch.batch_id,
            source_system_id=batch.source_system_id,
            source_product_id=batch.source_product_id,
            source_namespace_id="tvmaze-show",
            source_record_id=source_id,
            source_revision=acquired_at,
            operation=operation,
            observed_at=acquired_at,
            ingested_at=acquired_at,
            payload_schema="tvmaze-show-v1",
            raw_object=raw_object,
            source_location=f"/shows/{source_id}",
            policy_id=batch.policy_id,
            policy_digest=batch.policy_digest,
        )
        for source_id, operation, title in records
    )
    record_set = build_connector_record_set_manifest(
        batch_id=batch.batch_id,
        source_product_id=batch.source_product_id,
        policy_id=batch.policy_id,
        policy_digest=batch.policy_digest,
        record_objects=(raw_object,),
        record_count=len(envelopes),
        first_envelope_key=envelopes[0].envelope_key,
        last_envelope_key=envelopes[-1].envelope_key,
        created_at=acquired_at,
    )
    return build_source_silver_rows(
        registry=registry,
        batch=batch,
        record_set=record_set,
        envelopes=envelopes,
    )


def _tmdb_source_capture(*, acquired_at: str):
    policy = tmdb_rights_profile()
    raw_object = ObjectRef(
        uri="file:///tmp/tmdb-gold-detail.json",
        format="OBJECT_FORMAT_JSON",
        media_type="application/json",
        checksum=Checksum(value=hashlib.sha256(b"tmdb-gold").hexdigest()),
        size_bytes=9,
        created_at=acquired_at,
    )
    batch = build_connector_batch_manifest(
        source_system_id=TMDB_SOURCE_SYSTEM_ID,
        source_product_id=TMDB_SOURCE_PRODUCT_ID,
        connector_id=TMDB_CHANGES_CONNECTOR_ID,
        connector_version="1.0.0",
        image_digest="sha256:" + ("1" * 64),
        config_digest="sha256:" + ("2" * 64),
        policy_id=TMDB_POLICY_ID,
        policy_digest=policy.digest,
        transport_kind=TransportKind.API,
        serialization=Serialization.JSON,
        change_semantics=ChangeSemantics.DELTA,
        completeness=Completeness.COMPLETE,
        delete_coverage=DeleteCoverage.EXPLICIT,
        coverage_scope={"endpoint": "/3/movie/101"},
        raw_objects=(raw_object,),
        acquired_at=acquired_at,
        record_count=1,
        error_count=0,
    )
    envelope = build_connector_record_envelope(
        payload={
            "entityKind": "movie",
            "detail": {
                "id": 101,
                "title": "Cross-source movie",
                "imdb_id": "tt0000101",
                "external_ids": {"wikidata_id": "Q101"},
            },
        },
        batch_id=batch.batch_id,
        source_system_id=TMDB_SOURCE_SYSTEM_ID,
        source_product_id=TMDB_SOURCE_PRODUCT_ID,
        source_namespace_id=TMDB_MOVIE_NAMESPACE_ID,
        source_record_id="101",
        source_revision=acquired_at,
        operation=RecordOperation.UPSERT,
        observed_at=acquired_at,
        ingested_at=acquired_at,
        payload_schema="tmdb-changes-detail-v1",
        raw_object=raw_object,
        source_location="/3/movie/101",
        policy_id=TMDB_POLICY_ID,
        policy_digest=policy.digest,
    )
    record_set = build_connector_record_set_manifest(
        batch_id=batch.batch_id,
        source_product_id=TMDB_SOURCE_PRODUCT_ID,
        policy_id=TMDB_POLICY_ID,
        policy_digest=policy.digest,
        record_objects=(raw_object,),
        record_count=1,
        first_envelope_key=envelope.envelope_key,
        last_envelope_key=envelope.envelope_key,
        created_at=acquired_at,
    )
    return build_source_silver_rows(
        registry=build_community_registry(),
        batch=batch,
        record_set=record_set,
        envelopes=(envelope,),
    )


def _assertion_provenance_for_source(
    rows: dict[str, list[dict[str, object]]],
    source_id: str,
    source_path: str,
) -> AssertionProvenance:
    base = next(
        row
        for row in rows["community_identifier_assertion"]
        if row["subject_source_id"] == source_id
    )
    return AssertionProvenance.model_validate_json(
        str(base["provenance_json"])
    ).model_copy(update={"source_path": source_path})


def _append_identifier_assertion(
    run: CommunityIngestRun,
    rows: dict[str, list[dict[str, object]]],
    *,
    source_id: str,
    value: str,
    source_path: str,
):
    assertion = build_identifier_assertion(
        subject=SourceNodeRef(
            namespace_id="tvmaze-show",
            source_id=source_id,
            referent_kind="SERIES",
        ),
        namespace_id="imdb-title",
        value=value,
        issuer="IMDb",
        referent_kind="EDITORIAL_WORK",
        provenance=_assertion_provenance_for_source(rows, source_id, source_path),
    )
    rows["community_identifier_assertion"].append(
        identifier_assertion_row(run.run_id, assertion)
    )
    return assertion


def _append_parent_relation(
    run: CommunityIngestRun,
    rows: dict[str, list[dict[str, object]]],
    *,
    source_id: str,
    parent_source_id: str,
) -> None:
    assertion = build_relationship_assertion(
        subject=SourceNodeRef(
            namespace_id="tvmaze-show",
            source_id=source_id,
            referent_kind="SERIES",
        ),
        predicate="part_of_series",
        object=SourceNodeRef(
            namespace_id="tvmaze-show",
            source_id=parent_source_id,
            referent_kind="SERIES",
        ),
        provenance=_assertion_provenance_for_source(
            rows,
            source_id,
            "/parent",
        ),
    )
    rows["community_relationship_assertion"].append(
        relationship_assertion_row(run.run_id, assertion)
    )


def _visible_silver(
    spark: SparkSession,
    captures,
    *,
    source_ids: tuple[str, ...],
    entity_classifications: dict[str, tuple[str, str]] | None = None,
):
    rows = {table: [] for table in DATA_TABLE_COLUMNS}
    source_runs = []
    for run, capture_rows in captures:
        source_runs.append(run)
        for table in DATA_TABLE_COLUMNS:
            rows[table].extend(capture_rows[table])

    identity_run_id = "sha256:" + ("c" * 64)
    for source_id in source_ids:
        suffix = int(source_id)
        entity_key = "sha256:" + f"{suffix:064x}"
        entity_level, entity_kind = (entity_classifications or {}).get(
            source_id,
            ("SERIES", "TV_SERIES"),
        )
        rows["community_entity_ledger"].append(
            {
                "entity_key": entity_key,
                "run_id": identity_run_id,
                "allocation_id": "01a081e8-6420-7000-8000-000000000202",
                "entity_level": entity_level,
                "entity_kind": entity_kind,
                "status": "ACTIVE",
                "created_at": "2026-09-18T00:00:00Z",
                "first_release_id": None,
                "imported_v1": False,
            }
        )
        rows["community_entity_membership"].append(
            {
                "membership_key": "sha256:" + f"{suffix + 100:064x}",
                "run_id": identity_run_id,
                "source_namespace_id": "tvmaze-show",
                "source_id": source_id,
                "source_referent_kind": "SERIES",
                "entity_key": entity_key,
                "decision_id": "sha256:" + f"{suffix + 200:064x}",
                "valid_from": "2026-09-18T00:00:00Z",
                "valid_to": None,
            }
        )
    visible = create_community_dataframes(spark, rows)
    visible["community_ingest_run"] = spark.createDataFrame(
        [ingest_run_row(run) for run in source_runs],
        schema=community_table_schema("community_ingest_run"),
    )
    committed = (*(run.run_id for run in source_runs), identity_run_id)
    return visible, committed


def _gold(
    spark: SparkSession,
    *,
    visible,
    committed_run_ids: tuple[str, ...],
    as_of: str,
    field_policy=None,
):
    return build_distributed_gold(
        spark,
        visible_silver=visible,
        registry=build_community_registry(),
        policy_context=research_context(as_of=as_of),
        field_policy=field_policy or research_policy(),
        committed_run_ids=committed_run_ids,
        silver_snapshot_ids={"community_field_assertion": 20},
        identity_snapshot_ids={"community_entity_membership": 21},
        resolver_digest="sha256:" + ("d" * 64),
        image_digest="sha256:" + ("e" * 64),
        config_digest="sha256:" + ("f" * 64),
        planned_at=as_of,
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
        registry=build_community_registry(),
        batch=batch,
        record_set=record_set,
    )
    identity_frames = None
    gold_build = None
    try:
        identity_run, identity_frames = build_identity_resolution_dataframes(
            spark,
            visible_silver=silver_frames,
            input_id="sha256:" + ("c" * 64),
            image_digest="sha256:" + ("d" * 64),
            config_digest="sha256:" + ("e" * 64),
            started_at="2026-09-19T00:00:00Z",
            **_identity_lifecycle_inputs(spark, silver_run, silver_frames),
        )
        combined = {
            table: silver_frames[table].unionByName(identity_frames[table])
            for table in silver_frames
        }
        combined["community_ingest_run"] = spark.createDataFrame(
            [ingest_run_row(silver_run), ingest_run_row(identity_run)],
            schema=community_table_schema("community_ingest_run"),
        )
        gold_build = build_distributed_gold(
            spark,
            visible_silver=combined,
            registry=build_community_registry(),
            policy_context=research_context(
                as_of="2026-09-19T00:00:00Z",
            ),
            field_policy=research_policy(),
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


@pytest.mark.spark
def test_gold_keeps_tmdb_imdb_wikidata_identifiers_on_stable_entity(
    spark: SparkSession,
) -> None:
    source_run, rows = _tmdb_source_capture(acquired_at="2026-10-08T00:00:00Z")
    identity_run_id = "sha256:" + ("3" * 64)
    stable_entity = "sha256:" + ("4" * 64)
    rows["community_entity_ledger"].append(
        {
            "entity_key": stable_entity,
            "run_id": identity_run_id,
            "allocation_id": "01a081e8-6420-7000-8000-000000000101",
            "entity_level": "EDITORIAL_WORK",
            "entity_kind": "MOVIE",
            "status": "ACTIVE",
            "created_at": "2026-09-20T00:00:00Z",
            "first_release_id": None,
            "imported_v1": False,
        }
    )
    rows["community_entity_membership"].append(
        {
            "membership_key": "sha256:" + ("5" * 64),
            "run_id": identity_run_id,
            "source_namespace_id": TMDB_MOVIE_NAMESPACE_ID,
            "source_id": "101",
            "source_referent_kind": "MOVIE",
            "entity_key": stable_entity,
            "decision_id": "sha256:" + ("6" * 64),
            "valid_from": "2026-10-08T00:00:00Z",
            "valid_to": None,
        }
    )
    visible = create_community_dataframes(spark, rows)
    visible["community_ingest_run"] = spark.createDataFrame(
        [ingest_run_row(source_run)],
        schema=community_table_schema("community_ingest_run"),
    )

    build = _gold(
        spark,
        visible=visible,
        committed_run_ids=(source_run.run_id, identity_run_id),
        as_of="2026-10-09T00:00:00Z",
    )
    try:
        identifiers = {
            (row["namespace_id"], row["value"], row["entity_key"])
            for row in build.dataframes["community_gold_identifier"].collect()
        }
        assert ("imdb-title", "tt0000101", stable_entity) in identifiers
        assert ("wikidata-item", "Q101", stable_entity) in identifiers
        assert ("tmdb-movie", "101", stable_entity) in identifiers
        assert any(
            entry.source_product_id == TMDB_SOURCE_PRODUCT_ID and entry.claim_count >= 4
            for entry in build.attribution_manifest.entries
        )
        assert build.quality_report.duplicate_external_id_count == 0
    finally:
        build.unpersist()


@pytest.mark.spark
def test_gold_withholds_duplicate_blocking_key_and_audits_full_lineage(
    spark: SparkSession,
) -> None:
    run, rows = _source_capture(
        acquired_at="2026-09-20T00:00:00Z",
        records=(
            ("1", RecordOperation.UPSERT, "Season one"),
            ("2", RecordOperation.UPSERT, "Season two"),
            ("3", RecordOperation.UPSERT, "Parent series"),
        ),
    )
    assertions = (
        _append_identifier_assertion(
            run,
            rows,
            source_id="1",
            value="tt0042816",
            source_path="/externalIds/0",
        ),
        _append_identifier_assertion(
            run,
            rows,
            source_id="1",
            value="TT0042816",
            source_path="/externalIds/1",
        ),
        _append_identifier_assertion(
            run,
            rows,
            source_id="2",
            value="TT0042816",
            source_path="/externalIds/0",
        ),
    )
    _append_parent_relation(run, rows, source_id="1", parent_source_id="3")
    _append_parent_relation(run, rows, source_id="2", parent_source_id="3")
    visible, committed = _visible_silver(
        spark,
        ((run, rows),),
        source_ids=("1", "2", "3"),
        entity_classifications={
            "1": ("SEASON", "TV_SEASON"),
            "2": ("SEASON", "TV_SEASON"),
        },
    )
    policy = research_policy().model_copy(update={"max_conflict_ratio": 1.0})

    build = _gold(
        spark,
        visible=visible,
        committed_run_ids=committed,
        as_of="2026-09-20T01:00:00Z",
        field_policy=policy,
    )
    try:
        assert (
            build.dataframes["community_gold_identifier"]
            .where("namespace_id = 'imdb-title'")
            .count()
            == 0
        )
        conflict_rows = (
            build.dataframes["community_gold_conflict"]
            .where("reason = 'DUPLICATE_EXTERNAL_IDENTIFIER'")
            .collect()
        )
        assert len(conflict_rows) == 1
        conflict = conflict_rows[0]
        candidates = json.loads(conflict.candidate_values_json)
        assertion_ids = json.loads(conflict.assertion_ids_json)
        trace = json.loads(conflict.trace_json)
        assert candidates == sorted(
            (
                "sha256:" + f"{1:064x}",
                "sha256:" + f"{2:064x}",
            )
        )
        assert conflict.entity_key == candidates[0]
        assert conflict.predicate == "imdb-title"
        assert assertion_ids == sorted(item.assertion_id for item in assertions)
        assert trace["namespaceId"] == "imdb-title"
        assert trace["normalizedValue"] == "TT0042816"
        assert trace["blockingReferentKind"] == "SEASON"
        assert trace["entityCandidates"] == candidates
        assert [item["assertionId"] for item in trace["assertions"]] == assertion_ids
        assert build.quality_report.duplicate_external_id_count == 0
        assert build.quality_report.conflict_count >= 1
        assert build.quality_report.withheld_assertion_count >= len(assertions)
        assert build.quality_report.attribution_counts == (
            build.quality_report.eligible_policy_counts
        )
        assert build.quality_report.status == GoldQualityStatus.PASS
    finally:
        build.unpersist()


@pytest.mark.spark
def test_resolved_memberships_reject_conflicting_closure_times(
    spark: SparkSession,
) -> None:
    run_id = "sha256:" + ("8" * 64)
    entity_key = "sha256:" + ("9" * 64)
    decision_id = "sha256:" + ("a" * 64)
    common = {
        "run_id": run_id,
        "source_namespace_id": "tvmaze-show",
        "source_id": "1",
        "source_referent_kind": "SERIES",
        "entity_key": entity_key,
        "decision_id": decision_id,
        "valid_from": "2026-09-18T00:00:00Z",
    }
    memberships = spark.createDataFrame(
        [
            {
                **common,
                "membership_key": "sha256:" + ("b" * 64),
                "valid_to": None,
            },
            {
                **common,
                "membership_key": "sha256:" + ("c" * 64),
                "valid_to": "2026-09-19T00:00:00Z",
            },
            {
                **common,
                "membership_key": "sha256:" + ("d" * 64),
                "valid_to": "2026-09-21T00:00:00Z",
            },
        ],
        schema=community_table_schema("community_entity_membership"),
    )
    ledger = spark.createDataFrame(
        [
            {
                "entity_key": entity_key,
                "run_id": run_id,
                "allocation_id": "01a081e8-6420-7000-8000-000000000202",
                "entity_level": "SERIES",
                "entity_kind": "TV_SERIES",
                "status": "ACTIVE",
                "created_at": "2026-09-18T00:00:00Z",
                "first_release_id": None,
                "imported_v1": False,
            }
        ],
        schema=community_table_schema("community_entity_ledger"),
    )
    redirects = spark.createDataFrame(
        [],
        schema=community_table_schema("community_entity_redirect"),
    )

    with pytest.raises(ValueError, match="conflicting closure times"):
        _resolved_memberships(
            silver={
                "community_entity_membership": memberships,
                "community_entity_ledger": ledger,
                "community_entity_redirect": redirects,
            },
            as_of="2026-09-20T00:00:00Z",
            max_redirect_hops=4,
        )


@pytest.mark.spark
def test_membership_closure_supersedes_open_version(
    spark: SparkSession,
) -> None:
    run_id = "sha256:" + ("3" * 64)
    entity_key = "sha256:" + ("4" * 64)
    decision_id = "sha256:" + ("5" * 64)
    common = {
        "run_id": run_id,
        "source_namespace_id": "tvmaze-show",
        "source_id": "1",
        "source_referent_kind": "SERIES",
        "entity_key": entity_key,
        "decision_id": decision_id,
        "valid_from": "2026-09-18T00:00:00Z",
    }
    memberships = spark.createDataFrame(
        [
            {
                **common,
                "membership_key": "sha256:" + ("6" * 64),
                "valid_to": None,
            },
            {
                **common,
                "membership_key": "sha256:" + ("7" * 64),
                "valid_to": "2026-09-19T00:00:00Z",
            },
        ],
        schema=community_table_schema("community_entity_membership"),
    )
    ledger = spark.createDataFrame(
        [
            {
                "entity_key": entity_key,
                "run_id": run_id,
                "allocation_id": "01a081e8-6420-7000-8000-000000000202",
                "entity_level": "SERIES",
                "entity_kind": "TV_SERIES",
                "status": "ACTIVE",
                "created_at": "2026-09-18T00:00:00Z",
                "first_release_id": None,
                "imported_v1": False,
            }
        ],
        schema=community_table_schema("community_entity_ledger"),
    )
    redirects = spark.createDataFrame(
        [],
        schema=community_table_schema("community_entity_redirect"),
    )

    resolved = _resolved_memberships(
        silver={
            "community_entity_membership": memberships,
            "community_entity_ledger": ledger,
            "community_entity_redirect": redirects,
        },
        as_of="2026-09-20T00:00:00Z",
        max_redirect_hops=4,
    )
    try:
        assert resolved.count() == 0
    finally:
        resolved.unpersist()


@pytest.mark.spark
def test_gold_uses_only_latest_upsert_per_source_record(
    spark: SparkSession,
) -> None:
    initial = _source_capture(
        acquired_at="2026-09-20T00:00:00Z",
        records=tuple(
            (str(source_id), RecordOperation.UPSERT, f"Old {source_id}")
            for source_id in range(1, 6)
        ),
    )
    changes = _source_capture(
        acquired_at="2026-09-20T01:00:00Z",
        records=(
            ("1", RecordOperation.DELETE, None),
            ("2", RecordOperation.RETRACT, None),
            ("3", RecordOperation.EXPIRE, None),
            ("5", RecordOperation.UPSERT, "New 5"),
        ),
    )
    uncommitted = _source_capture(
        acquired_at="2026-09-20T01:30:00Z",
        records=(("4", RecordOperation.DELETE, None),),
    )
    visible, committed = _visible_silver(
        spark,
        (initial, changes, uncommitted),
        source_ids=("1", "2", "3", "4", "5"),
    )
    committed = (initial[0].run_id, changes[0].run_id, committed[-1])

    build = _gold(
        spark,
        visible=visible,
        committed_run_ids=committed,
        as_of="2026-09-20T02:00:00Z",
    )
    try:
        titles = {
            json.loads(row.value_json)
            for row in build.dataframes["community_gold_field"]
            .where("predicate = 'title'")
            .collect()
        }
        assert titles == {"Old 4", "New 5"}
    finally:
        build.unpersist()


@pytest.mark.spark
def test_snapshot_diff_closes_records_missing_from_next_snapshot(
    spark: SparkSession,
) -> None:
    first = _source_capture(
        acquired_at="2026-09-20T00:00:00Z",
        records=(
            ("1", RecordOperation.UPSERT, "Removed"),
            ("2", RecordOperation.UPSERT, "First"),
        ),
        change_semantics=ChangeSemantics.FULL_SNAPSHOT,
        delete_coverage=DeleteCoverage.SNAPSHOT_DIFF,
    )
    delta = _source_capture(
        acquired_at="2026-09-20T00:30:00Z",
        records=(("3", RecordOperation.UPSERT, "Delta only"),),
    )
    second = _source_capture(
        acquired_at="2026-09-20T01:00:00Z",
        records=(("2", RecordOperation.UPSERT, "Second"),),
        change_semantics=ChangeSemantics.FULL_SNAPSHOT,
        delete_coverage=DeleteCoverage.SNAPSHOT_DIFF,
    )
    visible, committed = _visible_silver(
        spark,
        (first, delta, second),
        source_ids=("1", "2", "3"),
    )

    build = _gold(
        spark,
        visible=visible,
        committed_run_ids=committed,
        as_of="2026-09-20T02:00:00Z",
    )
    try:
        titles = {
            json.loads(row.value_json)
            for row in build.dataframes["community_gold_field"]
            .where("predicate = 'title'")
            .collect()
        }
        assert titles == {"Second"}
    finally:
        build.unpersist()


@pytest.mark.spark
def test_snapshot_diff_does_not_delete_other_coverage_records(
    spark: SparkSession,
) -> None:
    first = _source_capture(
        acquired_at="2026-09-20T00:00:00Z",
        records=(("1", RecordOperation.UPSERT, "Scope A old"),),
        change_semantics=ChangeSemantics.FULL_SNAPSHOT,
        delete_coverage=DeleteCoverage.SNAPSHOT_DIFF,
        coverage_scope={"endpoint": "/scope-a"},
    )
    other = _source_capture(
        acquired_at="2026-09-20T00:30:00Z",
        records=(("9", RecordOperation.UPSERT, "Scope B survives"),),
        coverage_scope={"endpoint": "/scope-b"},
    )
    second = _source_capture(
        acquired_at="2026-09-20T01:00:00Z",
        records=(("1", RecordOperation.UPSERT, "Scope A new"),),
        change_semantics=ChangeSemantics.FULL_SNAPSHOT,
        delete_coverage=DeleteCoverage.SNAPSHOT_DIFF,
        coverage_scope={"endpoint": "/scope-a"},
    )
    visible, committed = _visible_silver(
        spark,
        (first, other, second),
        source_ids=("1", "9"),
    )

    build = _gold(
        spark,
        visible=visible,
        committed_run_ids=committed,
        as_of="2026-09-20T02:00:00Z",
    )
    try:
        titles = {
            json.loads(row.value_json)
            for row in build.dataframes["community_gold_field"]
            .where("predicate = 'title'")
            .collect()
        }
        assert titles == {"Scope A new", "Scope B survives"}
    finally:
        build.unpersist()


@pytest.mark.spark
def test_identity_revokes_open_membership_when_source_is_deleted(
    spark: SparkSession,
) -> None:
    entity_key = "sha256:" + ("1" * 64)
    decision_id = "sha256:" + ("2" * 64)
    membership_key = "sha256:" + ("3" * 64)
    baseline = _source_capture(
        acquired_at="2026-09-20T00:00:00Z",
        records=(("1", RecordOperation.UPSERT, "Deleted source"),),
    )
    deletion = _source_capture(
        acquired_at="2026-09-20T01:00:00Z",
        records=(("1", RecordOperation.DELETE, None),),
    )
    rows = {
        table: [*baseline[1][table], *deletion[1][table]]
        for table in DATA_TABLE_COLUMNS
    }
    rows["community_entity_membership"].append(
        {
            "membership_key": membership_key,
            "run_id": "sha256:" + ("4" * 64),
            "source_namespace_id": "tvmaze-show",
            "source_id": "1",
            "source_referent_kind": "SERIES",
            "entity_key": entity_key,
            "decision_id": decision_id,
            "valid_from": "2026-09-20T00:00:00Z",
            "valid_to": None,
        }
    )
    visible = create_community_dataframes(spark, rows)
    ingest_runs = spark.createDataFrame(
        [ingest_run_row(baseline[0]), ingest_run_row(deletion[0])],
        schema=community_table_schema("community_ingest_run"),
    )
    identity_frames = None
    try:
        _, identity_frames = build_identity_resolution_dataframes(
            spark,
            visible_silver=visible,
            input_id="sha256:" + ("8" * 64),
            image_digest="sha256:" + ("7" * 64),
            config_digest="sha256:" + ("6" * 64),
            started_at="2026-09-20T02:00:00Z",
            source_records=visible["community_source_record"],
            ingest_runs=ingest_runs,
            committed_source_run_ids=(baseline[0].run_id, deletion[0].run_id),
        )
        evidence_rows = identity_frames["community_identity_evidence"].collect()
        assert len(evidence_rows) == 1
        assert evidence_rows[0].kind == "LIFECYCLE_REVOKE"
        decision_rows = identity_frames["community_identity_decision"].collect()
        assert len(decision_rows) == 1
        assert decision_rows[0].status == "REVOKE"
        assert decision_rows[0].entity_key == entity_key
        assert evidence_rows[0].evidence_key in json.loads(
            decision_rows[0].evidence_keys_json
        )
        membership_rows = identity_frames["community_entity_membership"].collect()
        assert len(membership_rows) == 1
        assert membership_rows[0].entity_key == entity_key
        assert membership_rows[0].decision_id == decision_id
        assert membership_rows[0].source_id == "1"
        assert membership_rows[0].valid_to == "2026-09-20T01:00:00Z"
        assert membership_rows[0].membership_key != membership_key
        assert identity_frames["community_identity_conflict"].count() == 0
    finally:
        if identity_frames is not None:
            for frame in identity_frames.values():
                frame.unpersist()


@pytest.mark.spark
def test_identity_skips_deleted_source_assertions(
    spark: SparkSession,
) -> None:
    baseline = _source_capture(
        acquired_at="2026-09-20T00:00:00Z",
        records=(("1", RecordOperation.UPSERT, "Deleted source"),),
    )
    deletion = _source_capture(
        acquired_at="2026-09-20T01:00:00Z",
        records=(("1", RecordOperation.DELETE, None),),
    )
    rows = {
        table: [*baseline[1][table], *deletion[1][table]]
        for table in DATA_TABLE_COLUMNS
    }
    visible = create_community_dataframes(spark, rows)
    ingest_runs = spark.createDataFrame(
        [ingest_run_row(baseline[0]), ingest_run_row(deletion[0])],
        schema=community_table_schema("community_ingest_run"),
    )
    identity_frames = None
    try:
        _, identity_frames = build_identity_resolution_dataframes(
            spark,
            visible_silver=visible,
            input_id="sha256:" + ("8" * 64),
            image_digest="sha256:" + ("7" * 64),
            config_digest="sha256:" + ("6" * 64),
            started_at="2026-09-20T02:00:00Z",
            source_records=visible["community_source_record"],
            ingest_runs=ingest_runs,
            committed_source_run_ids=(baseline[0].run_id, deletion[0].run_id),
        )
        assert identity_frames["community_entity_membership"].count() == 0
        assert identity_frames["community_external_id_index"].count() == 0
    finally:
        if identity_frames is not None:
            for frame in identity_frames.values():
                frame.unpersist()


@pytest.mark.spark
def test_snapshot_diff_without_prior_coverage_fails_closed(
    spark: SparkSession,
) -> None:
    delta = _source_capture(
        acquired_at="2026-09-20T00:00:00Z",
        records=(("1", RecordOperation.UPSERT, "Could leak"),),
    )
    snapshot = _source_capture(
        acquired_at="2026-09-20T01:00:00Z",
        records=(("2", RecordOperation.UPSERT, "Present"),),
        change_semantics=ChangeSemantics.FULL_SNAPSHOT,
        delete_coverage=DeleteCoverage.SNAPSHOT_DIFF,
    )
    visible, committed = _visible_silver(
        spark,
        (delta, snapshot),
        source_ids=("1", "2"),
    )

    with pytest.raises(ValueError, match="prior complete snapshot"):
        _gold(
            spark,
            visible=visible,
            committed_run_ids=committed,
            as_of="2026-09-20T02:00:00Z",
        )


@pytest.mark.spark
def test_gold_rejects_assertion_policy_not_owned_by_source_product(
    spark: SparkSession,
) -> None:
    run, rows = _source_capture(
        acquired_at="2026-09-20T00:00:00Z",
        records=(("1", RecordOperation.UPSERT, "Spoofed"),),
    )
    other = next(
        profile
        for profile in build_community_registry().rights_profiles
        if profile.policy_id == "wikidata-structured-data-cc0"
    )
    for row in rows["community_source_record"]:
        row["policy_id"] = other.policy_id
        row["policy_digest"] = other.digest
    for table in (
        "community_field_assertion",
        "community_identifier_assertion",
        "community_relationship_assertion",
        "community_entity_type_assertion",
    ):
        for row in rows[table]:
            row["policy_id"] = other.policy_id
            row["policy_digest"] = other.digest
            provenance = json.loads(row["provenance_json"])
            provenance["policyId"] = other.policy_id
            provenance["policyDigest"] = other.digest
            row["provenance_json"] = canonical_json(provenance)

    visible, committed = _visible_silver(
        spark,
        ((run, rows),),
        source_ids=("1",),
    )
    run_row = ingest_run_row(run)
    manifest = json.loads(run_row["manifest_json"])
    batch = manifest["inputManifest"]["batchManifest"]
    batch["policyId"] = other.policy_id
    batch["policyDigest"] = other.digest
    run_row["policy_id"] = other.policy_id
    run_row["policy_digest"] = other.digest
    run_row["manifest_json"] = canonical_json(manifest)
    visible["community_ingest_run"] = spark.createDataFrame(
        [run_row],
        schema=community_table_schema("community_ingest_run"),
    )

    with pytest.raises(ValueError, match="source product rights policy"):
        _gold(
            spark,
            visible=visible,
            committed_run_ids=committed,
            as_of="2026-09-20T01:00:00Z",
        )


@pytest.mark.spark
def test_identity_builds_shared_lifecycle_projection_once(
    spark: SparkSession,
) -> None:
    baseline = _source_capture(
        acquired_at="2026-09-20T00:00:00Z",
        records=(("1", RecordOperation.UPSERT, "Active source"),),
    )
    deletion = _source_capture(
        acquired_at="2026-09-20T01:00:00Z",
        records=(("1", RecordOperation.DELETE, None),),
    )
    rows = {
        table: [*baseline[1][table], *deletion[1][table]]
        for table in DATA_TABLE_COLUMNS
    }
    visible = create_community_dataframes(spark, rows)
    ingest_runs = spark.createDataFrame(
        [ingest_run_row(baseline[0]), ingest_run_row(deletion[0])],
        schema=community_table_schema("community_ingest_run"),
    )
    build_calls = {"count": 0}
    original = build_source_lifecycle_events

    def counted_build(*args, **kwargs):
        build_calls["count"] += 1
        return original(*args, **kwargs)

    identity_frames = None
    with patch(
        "video_media_catalog.source_lifecycle.build_source_lifecycle_events",
        side_effect=counted_build,
    ):
        _, identity_frames = build_identity_resolution_dataframes(
            spark,
            visible_silver=visible,
            input_id="sha256:" + ("8" * 64),
            image_digest="sha256:" + ("7" * 64),
            config_digest="sha256:" + ("6" * 64),
            started_at="2026-09-20T02:00:00Z",
            source_records=visible["community_source_record"],
            ingest_runs=ingest_runs,
            committed_source_run_ids=(baseline[0].run_id, deletion[0].run_id),
        )
    try:
        assert build_calls["count"] == 1
    finally:
        if identity_frames is not None:
            for frame in identity_frames.values():
                frame.unpersist()


@pytest.mark.spark
def test_persist_latest_source_record_states_materializes_once(
    spark: SparkSession,
) -> None:
    capture = _source_capture(
        acquired_at="2026-09-20T00:00:00Z",
        records=(("1", RecordOperation.UPSERT, "Active source"),),
    )
    visible = create_community_dataframes(spark, capture[1])
    ingest_runs = spark.createDataFrame(
        [ingest_run_row(capture[0])],
        schema=community_table_schema("community_ingest_run"),
    )
    build_calls = {"count": 0}
    original = build_source_lifecycle_events

    def counted_build(*args, **kwargs):
        build_calls["count"] += 1
        return original(*args, **kwargs)

    with patch(
        "video_media_catalog.source_lifecycle.build_source_lifecycle_events",
        side_effect=counted_build,
    ):
        bound, latest = persist_latest_source_record_states(
            source_records=visible["community_source_record"],
            ingest_runs=ingest_runs,
            committed_run_ids=(capture[0].run_id,),
            registry=build_community_registry(),
            as_of="2026-09-20T01:00:00Z",
            repartition_count=4,
        )
        try:
            assert build_calls["count"] == 1
            assert bound.rdd.getNumPartitions() == 4
            assert "payload_json" not in bound.columns
            assert "raw_object_json" not in bound.columns
            assert "envelope_key" in bound.columns
            assert "_batch_acquired_at" in bound.columns
            assert latest.count() == 1
            assert bound.storageLevel.useDisk
            assert not bound.storageLevel.useMemory
            assert latest.storageLevel.useDisk
            assert not latest.storageLevel.useMemory
        finally:
            latest.unpersist()
            bound.unpersist()

    with pytest.raises(ValueError, match="repartition_count must be positive"):
        persist_latest_source_record_states(
            source_records=visible["community_source_record"],
            ingest_runs=ingest_runs,
            committed_run_ids=(capture[0].run_id,),
            registry=build_community_registry(),
            as_of="2026-09-20T01:00:00Z",
            repartition_count=0,
        )


@pytest.mark.spark
def test_source_lifecycle_accepts_compatible_historical_registry(
    spark: SparkSession,
) -> None:
    capture = _source_capture(
        acquired_at="2026-09-20T00:00:00Z",
        records=(("1", RecordOperation.UPSERT, "Historical registry source"),),
    )
    manifest = dict(capture[0].input_manifest)
    manifest["registryDigest"] = "sha256:" + ("9" * 64)
    historical_run = capture[0].model_copy(update={"input_manifest": manifest})
    visible = create_community_dataframes(spark, capture[1])
    ingest_runs = spark.createDataFrame(
        [ingest_run_row(historical_run)],
        schema=community_table_schema("community_ingest_run"),
    )

    bound, latest = persist_latest_source_record_states(
        source_records=visible["community_source_record"],
        ingest_runs=ingest_runs,
        committed_run_ids=(historical_run.run_id,),
        registry=build_community_registry(),
        as_of="2026-09-20T01:00:00Z",
    )
    try:
        assert bound.count() == 1
        assert latest.count() == 1
    finally:
        latest.unpersist()
        bound.unpersist()


@pytest.mark.spark
def test_source_lifecycle_rejects_changed_product_binding(
    spark: SparkSession,
) -> None:
    capture = _source_capture(
        acquired_at="2026-09-20T00:00:00Z",
        records=(("1", RecordOperation.UPSERT, "Changed connector source"),),
    )
    manifest = dict(capture[0].input_manifest)
    batch_manifest = dict(manifest["batchManifest"])
    batch_manifest["connectorId"] = "unregistered-tvmaze-connector"
    manifest["batchManifest"] = batch_manifest
    incompatible_run = capture[0].model_copy(update={"input_manifest": manifest})
    visible = create_community_dataframes(spark, capture[1])
    ingest_runs = spark.createDataFrame(
        [ingest_run_row(incompatible_run)],
        schema=community_table_schema("community_ingest_run"),
    )

    with pytest.raises(ValueError, match="source product rights policy"):
        persist_latest_source_record_states(
            source_records=visible["community_source_record"],
            ingest_runs=ingest_runs,
            committed_run_ids=(incompatible_run.run_id,),
            registry=build_community_registry(),
            as_of="2026-09-20T01:00:00Z",
        )
