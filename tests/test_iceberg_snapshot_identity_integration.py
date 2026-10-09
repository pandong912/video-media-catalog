from __future__ import annotations

import hashlib
import os

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from video_media_catalog.community_rows import (
    external_id_index_row,
    ingest_run_row,
)
from video_media_catalog.community_sources import build_community_registry
from video_media_catalog.community_spark import (
    community_table_schema,
    create_community_dataframes,
)
from video_media_catalog.community_tables import (
    DATA_TABLE_COLUMNS,
    build_community_table_mapping,
)
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
from video_media_catalog.iceberg import (
    CatalogConfig,
    execute_iceberg_sql,
    find_owned_snapshot_id,
)
from video_media_catalog.identity_spark import build_identity_resolution_dataframes
from video_media_catalog.identity_v2 import build_external_id_index_entry
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.source_silver import build_source_silver_rows
from video_media_catalog.tmdb import (
    TMDB_CHANGES_CONNECTOR_ID,
    TMDB_MOVIE_NAMESPACE_ID,
    TMDB_POLICY_ID,
    TMDB_SOURCE_PRODUCT_ID,
    TMDB_SOURCE_SYSTEM_ID,
    tmdb_rights_profile,
)

RUN_ID = "sha256:" + ("a" * 64)
OTHER_RUN_ID = "sha256:" + ("b" * 64)
RUN_SNAPSHOT_PROPERTY = "video-media-catalog.run-id"
ROLLBACK_PROPERTY = "video-media-catalog.rollback-run-id"
CATALOG = "local_identity"
NAMESPACE = "snapshot_identity"


@pytest.fixture(scope="module")
def iceberg_spark(tmp_path_factory: pytest.TempPathFactory):
    warehouse = tmp_path_factory.mktemp("iceberg-warehouse")
    package = os.environ.get(
        "VMC_ICEBERG_SPARK_PACKAGE",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1",
    )
    config = CatalogConfig(
        catalog_name=CATALOG,
        namespace=NAMESPACE,
        warehouse=warehouse.as_uri(),
    )
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("iceberg-snapshot-identity-integration")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.jars.packages", package)
        .config("spark.sql.session.timeZone", "UTC")
    )
    spark = config.configure_builder(builder).getOrCreate()
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{CATALOG}`.`{NAMESPACE}`")
    yield spark
    spark.stop()


def _table_identifiers(table: str) -> tuple[str, str]:
    return (
        f"`{CATALOG}`.`{NAMESPACE}`.`{table}`",
        f"{CATALOG}.{NAMESPACE}.{table}",
    )


def _create_table(spark: SparkSession, table: str) -> tuple[str, str]:
    identifier, name = _table_identifiers(table)
    spark.sql(
        f"""
        CREATE TABLE {identifier} (
            row_key STRING NOT NULL,
            run_id STRING NOT NULL
        )
        USING iceberg
        TBLPROPERTIES ('format-version' = '2')
        """
    )
    return identifier, name


def _current_snapshot_id(spark: SparkSession, identifier: str) -> int:
    rows = spark.sql(
        f"""
        SELECT snapshot_id
        FROM {identifier}.history
        ORDER BY made_current_at DESC
        LIMIT 1
        """
    ).collect()
    assert len(rows) == 1
    return int(rows[0]["snapshot_id"])


@pytest.mark.spark
@pytest.mark.integration
def test_local_iceberg_rollback_marker_allows_zero_probe_and_same_run_retry(
    iceberg_spark: SparkSession,
) -> None:
    identifier, name = _create_table(iceberg_spark, "marker_retry")
    execute_iceberg_sql(
        iceberg_spark,
        f"INSERT INTO {identifier} VALUES ('failed', '{RUN_ID}')",
        snapshot_properties={RUN_SNAPSHOT_PROPERTY: RUN_ID},
    )
    execute_iceberg_sql(
        iceberg_spark,
        f"DELETE FROM {identifier} WHERE run_id = '{RUN_ID}'",
        snapshot_properties={ROLLBACK_PROPERTY: RUN_ID},
    )

    snapshot_columns = set(iceberg_spark.table(f"{name}.snapshots").columns)
    assert "sequence_number" not in snapshot_columns
    assert {
        "committed_at",
        "snapshot_id",
        "parent_id",
        "operation",
        "manifest_list",
        "summary",
    }.issubset(snapshot_columns)
    assert (
        find_owned_snapshot_id(
            iceberg_spark,
            table_identifier=identifier,
            table_name=name,
            primary_key="row_key",
            identity_column="run_id",
            identity_value=RUN_ID,
            snapshot_property=RUN_SNAPSHOT_PROPERTY,
            expected_row_count=0,
        )
        is None
    )

    execute_iceberg_sql(
        iceberg_spark,
        f"INSERT INTO {identifier} VALUES ('retry', '{RUN_ID}')",
        snapshot_properties={RUN_SNAPSHOT_PROPERTY: RUN_ID},
    )
    retry_snapshot_id = _current_snapshot_id(iceberg_spark, identifier)
    assert (
        find_owned_snapshot_id(
            iceberg_spark,
            table_identifier=identifier,
            table_name=name,
            primary_key="row_key",
            identity_column="run_id",
            identity_value=RUN_ID,
            snapshot_property=RUN_SNAPSHOT_PROPERTY,
            expected_row_count=1,
        )
        == retry_snapshot_id
    )


@pytest.mark.spark
@pytest.mark.integration
def test_local_iceberg_pointer_rollback_excludes_abandoned_run_snapshot(
    iceberg_spark: SparkSession,
) -> None:
    identifier, name = _create_table(iceberg_spark, "pointer_retry")
    iceberg_spark.sql(f"INSERT INTO {identifier} VALUES ('baseline', '{OTHER_RUN_ID}')")
    baseline_snapshot_id = _current_snapshot_id(iceberg_spark, identifier)
    execute_iceberg_sql(
        iceberg_spark,
        f"INSERT INTO {identifier} VALUES ('abandoned', '{RUN_ID}')",
        snapshot_properties={RUN_SNAPSHOT_PROPERTY: RUN_ID},
    )
    abandoned_snapshot_id = _current_snapshot_id(iceberg_spark, identifier)
    iceberg_spark.sql(
        f"""
        CALL `{CATALOG}`.system.rollback_to_snapshot(
            table => '{NAMESPACE}.pointer_retry',
            snapshot_id => {baseline_snapshot_id}
        )
        """
    ).collect()
    execute_iceberg_sql(
        iceberg_spark,
        f"INSERT INTO {identifier} VALUES ('retry', '{RUN_ID}')",
        snapshot_properties={RUN_SNAPSHOT_PROPERTY: RUN_ID},
    )
    retry_snapshot_id = _current_snapshot_id(iceberg_spark, identifier)

    ancestors = {
        int(row["snapshot_id"])
        for row in iceberg_spark.sql(
            f"""
            SELECT snapshot_id
            FROM {identifier}.history
            WHERE is_current_ancestor
            """
        ).collect()
    }
    assert abandoned_snapshot_id not in ancestors
    assert retry_snapshot_id in ancestors
    assert (
        find_owned_snapshot_id(
            iceberg_spark,
            table_identifier=identifier,
            table_name=name,
            primary_key="row_key",
            identity_column="run_id",
            identity_value=RUN_ID,
            snapshot_property=RUN_SNAPSHOT_PROPERTY,
            expected_row_count=1,
        )
        == retry_snapshot_id
    )


@pytest.mark.spark
@pytest.mark.integration
def test_tmdb_incremental_identity_reuses_cross_source_entity_and_keeps_conflict(
    iceberg_spark: SparkSession,
) -> None:
    acquired_at = "2026-10-08T00:00:00Z"
    policy = tmdb_rights_profile()
    payloads = (
        {
            "entityKind": "movie",
            "detail": {
                "id": 101,
                "title": "Stable merge",
                "imdb_id": "tt0000101",
                "external_ids": {"wikidata_id": "Q101"},
            },
        },
        {
            "entityKind": "movie",
            "detail": {
                "id": 102,
                "title": "Conflicting merge",
                "imdb_id": "tt0000102",
                "external_ids": {"wikidata_id": "Q102"},
            },
        },
    )
    raw = ObjectRef(
        uri="file:///tmp/tmdb-identity-detail.json",
        format="OBJECT_FORMAT_JSON",
        media_type="application/json",
        checksum=Checksum(value=hashlib.sha256(b"tmdb").hexdigest()),
        size_bytes=4,
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
        coverage_scope={"endpoint": "/3/movie/changes"},
        raw_objects=(raw,),
        acquired_at=acquired_at,
        record_count=2,
        error_count=0,
    )
    envelopes = tuple(
        build_connector_record_envelope(
            payload=payload,
            batch_id=batch.batch_id,
            source_system_id=TMDB_SOURCE_SYSTEM_ID,
            source_product_id=TMDB_SOURCE_PRODUCT_ID,
            source_namespace_id=TMDB_MOVIE_NAMESPACE_ID,
            source_record_id=str(payload["detail"]["id"]),
            source_revision=acquired_at,
            operation=RecordOperation.UPSERT,
            observed_at=acquired_at,
            ingested_at=acquired_at,
            payload_schema="tmdb-changes-detail-v1",
            raw_object=raw,
            source_location=f"/3/movie/{payload['detail']['id']}",
            policy_id=TMDB_POLICY_ID,
            policy_digest=policy.digest,
        )
        for payload in payloads
    )
    record_set = build_connector_record_set_manifest(
        batch_id=batch.batch_id,
        source_product_id=TMDB_SOURCE_PRODUCT_ID,
        policy_id=TMDB_POLICY_ID,
        policy_digest=policy.digest,
        record_objects=(raw,),
        record_count=2,
        first_envelope_key=envelopes[0].envelope_key,
        last_envelope_key=envelopes[-1].envelope_key,
        created_at=acquired_at,
    )
    source_run, rows = build_source_silver_rows(
        registry=build_community_registry(),
        batch=batch,
        record_set=record_set,
        envelopes=envelopes,
    )
    baseline_run_id = "sha256:" + ("3" * 64)
    stable_entity = "sha256:" + ("4" * 64)
    conflicting_entity = "sha256:" + ("5" * 64)
    for suffix, entity_key in enumerate(
        (stable_entity, conflicting_entity),
        start=1,
    ):
        rows["community_entity_ledger"].append(
            {
                "entity_key": entity_key,
                "run_id": baseline_run_id,
                "allocation_id": f"01a081e8-6420-7000-8000-{suffix:012d}",
                "entity_level": "EDITORIAL_WORK",
                "entity_kind": "MOVIE",
                "status": "ACTIVE",
                "created_at": "2026-09-20T00:00:00Z",
                "first_release_id": None,
                "imported_v1": False,
            }
        )
    index_values = (
        ("imdb-title", "TT0000101", stable_entity, 11),
        ("wikidata-item", "Q101", stable_entity, 12),
        ("imdb-title", "TT0000102", stable_entity, 13),
        ("wikidata-item", "Q102", conflicting_entity, 14),
    )
    for namespace_id, value, entity_key, suffix in index_values:
        entry = build_external_id_index_entry(
            materialization_id="sha256:" + ("6" * 64),
            namespace_id=namespace_id,
            normalized_value=value,
            referent_kind="EDITORIAL_WORK",
            entity_key=entity_key,
            assertion_keys=("sha256:" + f"{suffix:064x}",),
            observed_at="2026-09-20T00:00:00Z",
            policy_id=TMDB_POLICY_ID,
            policy_digest=policy.digest,
        )
        rows["community_external_id_index"].append(
            external_id_index_row(baseline_run_id, entry)
        )
    visible = create_community_dataframes(
        iceberg_spark,
        {table: rows[table] for table in DATA_TABLE_COLUMNS},
    )
    ingest_runs = iceberg_spark.createDataFrame(
        [ingest_run_row(source_run)],
        schema=community_table_schema("community_ingest_run"),
    )
    identity_frames = None
    try:
        _, identity_frames = build_identity_resolution_dataframes(
            iceberg_spark,
            visible_silver=visible,
            input_id="sha256:" + ("7" * 64),
            image_digest="sha256:" + ("8" * 64),
            config_digest="sha256:" + ("9" * 64),
            started_at="2026-10-09T00:00:00Z",
            source_records=visible["community_source_record"],
            ingest_runs=ingest_runs,
            committed_source_run_ids=(source_run.run_id,),
            identity_generation_id="pure-source-2026-09-r1",
            identity_mode="incremental",
            table_mapping=build_community_table_mapping("pure-source-2026-09-r1"),
        )
        stable_rows = (
            identity_frames["community_entity_membership"]
            .where("source_namespace_id = 'tmdb-movie' AND source_id = '101'")
            .collect()
        )
        assert len(stable_rows) == 1
        assert stable_rows[0]["entity_key"] == stable_entity
        assert (
            identity_frames["community_identity_conflict"]
            .where("source_namespace_id = 'tmdb-movie' AND source_id = '102'")
            .count()
            >= 1
        )
        assert (
            identity_frames["community_entity_membership"]
            .where("source_id = '102'")
            .count()
            == 0
        )
        assert identity_frames["community_entity_ledger"].count() == 0
    finally:
        if identity_frames is not None:
            for frame in identity_frames.values():
                frame.unpersist()
