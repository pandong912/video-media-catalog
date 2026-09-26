from __future__ import annotations

import os

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from video_media_catalog.iceberg import (
    CatalogConfig,
    execute_iceberg_sql,
    find_owned_snapshot_id,
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
