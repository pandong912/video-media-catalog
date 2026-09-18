from __future__ import annotations

import os
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from video_media_catalog.iceberg import CatalogConfig, MediaCatalogTables
from video_media_catalog.landing import extract_landing
from video_media_catalog.models import OutputCommit, SnapshotSet
from video_media_catalog.spark_cli import build_parser, run


@pytest.mark.spark
@pytest.mark.integration
def test_local_hadoop_iceberg_insert_only_merge_is_idempotent(
    tmp_path: Path,
) -> None:
    if os.environ.get("RUN_ICEBERG_INTEGRATION") != "1":
        pytest.skip("set RUN_ICEBERG_INTEGRATION=1 to download/use Iceberg runtime")
    packages = os.environ.get(
        "ICEBERG_SPARK_PACKAGES",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1",
    )
    config = CatalogConfig(
        catalog_name="media_it",
        namespace="catalog_v1",
        warehouse=(tmp_path / "warehouse").as_uri(),
    )
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("video-media-catalog-iceberg-integration")
        .config("spark.ui.enabled", "false")
        .config("spark.jars.packages", packages)
    )
    spark = config.configure_builder(builder).getOrCreate()
    try:
        tables = MediaCatalogTables(spark, config)
        tables.create_tables()
        frame = spark.createDataFrame(
            [
                (
                    "sha256:" + "1" * 64,
                    "MOVIE",
                    "wikidata",
                    "Q1",
                    "{}",
                )
            ],
            """
            entity_key STRING,
            entity_type STRING,
            canonical_source STRING,
            canonical_source_id STRING,
            attributes_json STRING
            """,
        )
        tables.merge_insert_only("catalog_entity", frame)
        tables.merge_insert_only("catalog_entity", frame)
        assert spark.table(tables.table_identifier("catalog_entity")).count() == 1
        snapshots = tables.capture_snapshots(
            {
                "catalog_source_record": 0,
                "catalog_entity": 1,
                "catalog_name": 0,
                "catalog_external_identifier": 0,
                "catalog_relation": 0,
                "catalog_ingest_error": 0,
            },
            empty_committed_at="2026-09-18T06:00:00Z",
        )
        entity_snapshot = next(
            item for item in snapshots if item.table_name.endswith(".catalog_entity")
        )
        assert entity_snapshot.snapshot_id is not None
        assert entity_snapshot.operation == "append"
    finally:
        spark.stop()


@pytest.mark.spark
@pytest.mark.integration
def test_end_to_end_spark_commit_last(tmp_path: Path, fixture_dir: Path) -> None:
    if os.environ.get("RUN_ICEBERG_INTEGRATION") != "1":
        pytest.skip("set RUN_ICEBERG_INTEGRATION=1 to run end-to-end commit")
    packages = os.environ.get(
        "ICEBERG_SPARK_PACKAGES",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1",
    )
    landing_root = tmp_path / "landing"
    extract_landing(
        output_uri=str(landing_root),
        wikidata_uri=str(fixture_dir / "wikidata.json"),
        eidr_xml_uri=str(fixture_dir / "eidr.xml"),
        shard_records=5,
    )
    args = build_parser().parse_args(
        [
            "--master",
            "local[2]",
            "--shuffle-partitions",
            "2",
            "--spark-packages",
            packages,
            "--landing-manifest-uri",
            (landing_root / "landing-manifest.json").as_uri(),
            "--catalog-type",
            "hadoop",
            "--catalog-name",
            "media_e2e",
            "--namespace",
            "catalog_v1",
            "--warehouse",
            (tmp_path / "warehouse").as_uri(),
            "--manifest-uri",
            "s3://catalog-input/source-manifest.parquet",
            "--manifest-hash",
            "sha256:hex:" + "a" * 64,
            "--manifest-version",
            "manifest-v1",
            "--manifest-etag",
            "manifest-etag",
            "--manifest-size",
            "4096",
            "--run-id",
            "01a081e8-6420-7000-8000-000000000202",
            "--job-spec-id",
            "01a081e8-6420-7000-8000-000000000203",
            "--tenant-id",
            "01a081e8-6420-7000-8000-000000000204",
            "--attempt",
            "1",
            "--output-prefix",
            (tmp_path / "run").as_uri(),
            "--executor-image",
            "registry.example/catalog@sha256:" + "c" * 64,
            "--stage",
            "media-catalog-commit",
            "--no-quality-report-required",
        ]
    )
    result = run(args)

    control = tmp_path / "run" / "attempt=1" / "stage=media-catalog-commit"
    snapshot_path = control / "snapshot-set.json"
    commit_path = control / "output.commit.json"
    snapshot = SnapshotSet.model_validate_json(snapshot_path.read_bytes())
    commit = OutputCommit.model_validate_json(commit_path.read_bytes())
    assert result["commitId"] == commit.commit_id
    assert commit.output_manifest.uri == snapshot_path.as_uri()
    assert len(snapshot.tables) == 6
    assert snapshot.output_count >= 10
