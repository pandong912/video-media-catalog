from __future__ import annotations

import hashlib

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import SparkSession

from video_media_catalog.community_iceberg import CommunityCatalogTables
from video_media_catalog.community_snapshot import build_committed_run_digest
from video_media_catalog.iceberg import CatalogConfig
from video_media_catalog.research_silver_cli import _validate_epoch_delta


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[2]")
        .appName("silver-epoch-summary-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.mark.spark
def test_committed_run_summary_collects_deterministic_buckets(
    spark: SparkSession,
    tmp_path,
) -> None:
    runs = (
        "sha256:aa" + ("0" * 62),
        "sha256:aa" + ("1" * 62),
        "sha256:bb" + ("2" * 62),
    )
    frame = spark.createDataFrame(
        [(run_id,) for run_id in reversed(runs)], "run_id STRING"
    )
    tables = CommunityCatalogTables(
        spark,
        CatalogConfig(
            catalog_name="media",
            namespace="video_media_catalog",
            warehouse=(tmp_path / "warehouse").as_uri(),
        ),
    )
    count, digest = tables.committed_run_summary(frame)
    aa_digest = hashlib.sha256("\n".join(runs[:2]).encode()).hexdigest()
    bb_digest = hashlib.sha256(runs[2].encode()).hexdigest()
    expected = build_committed_run_digest(
        run_count=3,
        buckets=(
            ("aa", 2, "sha256:" + aa_digest),
            ("bb", 1, "sha256:" + bb_digest),
        ),
    )
    assert count == 3
    assert digest == expected


@pytest.mark.spark
def test_committed_run_summary_rejects_duplicate_commit_rows(
    spark: SparkSession,
    tmp_path,
) -> None:
    run_id = "sha256:aa" + ("0" * 62)
    frame = spark.createDataFrame([(run_id,), (run_id,)], "run_id STRING")
    tables = CommunityCatalogTables(
        spark,
        CatalogConfig(
            catalog_name="media",
            namespace="video_media_catalog",
            warehouse=(tmp_path / "warehouse").as_uri(),
        ),
    )
    with pytest.raises(RuntimeError, match="duplicate run IDs"):
        tables.committed_run_summary(frame)


@pytest.mark.spark
def test_epoch_delta_is_validated_with_distributed_anti_joins(
    spark: SparkSession,
) -> None:
    parent_id = "sha256:aa" + ("0" * 62)
    delta_id = "sha256:bb" + ("1" * 62)
    parent = spark.createDataFrame([(parent_id,)], "run_id STRING")
    current = spark.createDataFrame(
        [(parent_id,), (delta_id,)],
        "run_id STRING",
    )
    _validate_epoch_delta(
        spark,
        current_runs=current,
        parent_runs=parent,
        delta_run_ids=(delta_id,),
    )
    with pytest.raises(ValueError, match="do not match"):
        _validate_epoch_delta(
            spark,
            current_runs=current,
            parent_runs=parent,
            delta_run_ids=(parent_id,),
        )
