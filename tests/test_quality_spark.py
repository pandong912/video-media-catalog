from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from video_media_catalog.landing import extract_landing
from video_media_catalog.quality import (
    QualityConfig,
    QualityReport,
    QualitySummary,
    compute_quality_metrics,
    quality_violations,
)
from video_media_catalog.validate_cli import build_parser, run


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("video-media-catalog-quality-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def _valid_frames(spark: SparkSession) -> dict:
    return {
        "catalog_source_record": spark.createDataFrame(
            [("source-1",)],
            "record_key STRING",
        ),
        "catalog_entity": spark.createDataFrame(
            [("entity-1",), ("entity-2",)],
            "entity_key STRING",
        ),
        "catalog_name": spark.createDataFrame(
            [("name-1", "entity-1"), ("name-2", "entity-2")],
            "name_key STRING, entity_key STRING",
        ),
        "catalog_external_identifier": spark.createDataFrame(
            [("identifier-1",)],
            "identifier_key STRING",
        ),
        "catalog_relation": spark.createDataFrame(
            [("relation-1", "entity-1", "entity-2")],
            (
                "relation_key STRING, subject_entity_key STRING, "
                "object_entity_key STRING"
            ),
        ),
        "catalog_ingest_error": spark.createDataFrame(
            [],
            "error_key STRING",
        ),
    }


@pytest.mark.spark
def test_distributed_quality_metrics_pass(spark: SparkSession) -> None:
    metrics = compute_quality_metrics(_valid_frames(spark))
    violations = quality_violations(
        metrics,
        QualityConfig(
            expected_entity_count=2,
            entity_count_tolerance_percent=5,
            minimum_name_coverage=1,
        ),
    )

    assert metrics.table_counts["catalog_entity"] == 2
    assert metrics.name_coverage == 1
    assert violations == []


@pytest.mark.spark
def test_distributed_quality_metrics_find_all_failure_classes(
    spark: SparkSession,
) -> None:
    frames = _valid_frames(spark)
    frames["catalog_source_record"] = spark.createDataFrame(
        [(None,)],
        "record_key STRING",
    )
    frames["catalog_entity"] = spark.createDataFrame(
        [("entity-1",), ("entity-1",)],
        "entity_key STRING",
    )
    frames["catalog_name"] = spark.createDataFrame(
        [("name-1", "entity-1")],
        "name_key STRING, entity_key STRING",
    )
    frames["catalog_relation"] = spark.createDataFrame(
        [
            ("relation-1", "missing-subject", "entity-1"),
            ("relation-2", "entity-1", "missing-object"),
        ],
        ("relation_key STRING, subject_entity_key STRING, object_entity_key STRING"),
    )
    frames["catalog_ingest_error"] = spark.createDataFrame(
        [("error-1",)],
        "error_key STRING",
    )

    metrics = compute_quality_metrics(frames)
    violations = quality_violations(
        metrics,
        QualityConfig(minimum_name_coverage=0.75),
    )

    assert metrics.null_primary_keys["catalog_source_record"] == 1
    assert metrics.duplicate_primary_keys["catalog_entity"] == 1
    assert metrics.ingest_error_count == 1
    assert metrics.dangling_relation_subjects == 1
    assert metrics.dangling_relation_objects == 1
    assert metrics.name_coverage == 0.5
    assert any(value.startswith("NULL_PRIMARY_KEY") for value in violations)
    assert any(value.startswith("DUPLICATE_PRIMARY_KEY") for value in violations)
    assert any(value.startswith("INGEST_ERRORS") for value in violations)
    assert any(value.startswith("DANGLING_RELATION_SUBJECTS") for value in violations)
    assert any(value.startswith("DANGLING_RELATION_OBJECTS") for value in violations)
    assert any(value.startswith("NAME_COVERAGE_BELOW_MINIMUM") for value in violations)


@pytest.mark.spark
def test_validate_cli_passes_and_publishes_commit_last_summary(
    spark: SparkSession,
    fixture_dir: Path,
    tmp_path: Path,
) -> None:
    landing_root = tmp_path / "landing"
    extract_landing(
        output_uri=str(landing_root),
        wikidata_uri=str(fixture_dir / "wikidata.json"),
        shard_records=5,
    )
    parsed = build_parser().parse_args(
        [
            "--landing-manifest-uri",
            (landing_root / "landing-manifest.json").as_uri(),
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
            "media-catalog-validate",
            "--expected-entity-count",
            "9",
            "--entity-count-tolerance-percent",
            "0",
            "--minimum-name-coverage",
            "1",
            "--max-closure-iterations",
            "8",
        ]
    )

    report = run(parsed, spark=spark)

    quality_root = tmp_path / "run" / "attempt=1" / "stage=media-catalog-validate"
    stored_report = QualityReport.model_validate_json(
        (quality_root / "quality-report.json").read_bytes()
    )
    summary = QualitySummary.model_validate_json(
        (quality_root / "quality-summary.json").read_bytes()
    )
    assert report.status == "PASS"
    assert report.table_counts["catalog_entity"] == 9
    assert stored_report == report
    assert summary.status == "PASS"
    assert summary.report.checksum.value
