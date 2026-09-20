from __future__ import annotations

import pytest

from video_media_catalog.identity_benchmark import (
    run_identity_synthetic_benchmark,
)
from video_media_catalog.identity_benchmark_cli import (
    benchmark_node_count,
    build_parser,
    main,
)


def test_large_benchmark_profiles_require_explicit_confirmation() -> None:
    parsed = build_parser().parse_args(["--scale", "1m"])
    with pytest.raises(ValueError, match="confirm-large-scale"):
        benchmark_node_count(parsed)

    confirmed = build_parser().parse_args(["--scale", "5m", "--confirm-large-scale"])
    assert benchmark_node_count(confirmed) == 5_000_000
    with pytest.raises(SystemExit):
        main(["--scale", "1m"])


@pytest.mark.spark
def test_small_synthetic_benchmark_reports_required_metrics() -> None:
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("identity-synthetic-benchmark-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    try:
        result = run_identity_synthetic_benchmark(
            spark,
            node_count=32,
            component_size=4,
            conflict_every_components=2,
            partitions=2,
        )
    finally:
        spark.stop()

    assert result["nodeCount"] == 32
    assert result["edgeCount"] == 32
    assert result["componentCount"] == 8
    assert result["runtimeSeconds"] > 0
    assert set(result["shuffleMetrics"]) == {
        "available",
        "bytesWritten",
        "diskBytesSpilled",
        "jobCount",
        "memoryBytesSpilled",
        "readBytes",
        "recordsRead",
        "recordsWritten",
        "stageCount",
    }
    assert result["shuffleMetrics"]["available"]
    assert result["shuffleMetrics"]["bytesWritten"] > 0
    assert result["shuffleMetrics"]["recordsWritten"] > 0
    assert result["conflictCountsByReason"]["MULTIPLE_EXACT_IDENTIFIER_CANDIDATES"] > 0
