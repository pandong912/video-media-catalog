"""Independent Spark quality-gate stage with no Iceberg writes."""

from __future__ import annotations

import argparse
import os
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

from video_media_catalog.canonical import canonical_json
from video_media_catalog.commit import ControlPublisher, control_publisher
from video_media_catalog.quality import (
    VALIDATE_STAGE,
    QualityConfig,
    QualityReport,
    build_quality_report,
    compute_quality_metrics,
    publish_quality_report,
    read_pending_quality_report,
    read_quality_gate,
)
from video_media_catalog.spark_input import (
    configure_s3a_builder,
    load_landing_frame,
    load_landing_input,
    runtime_arguments,
)
from video_media_catalog.spark_transform import transform_landing


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="video-media-catalog-validate",
        description="Validate transformed catalog quality without writing Iceberg.",
    )
    parser.add_argument("--manifest-uri", required=True)
    parser.add_argument("--manifest-hash", required=True)
    parser.add_argument("--manifest-version", required=True)
    parser.add_argument("--manifest-etag", required=True)
    parser.add_argument("--manifest-size", required=True, type=int)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--job-spec-id", required=True)
    parser.add_argument("--tenant-id", required=True)
    parser.add_argument("--attempt", required=True, type=int)
    parser.add_argument("--output-prefix", required=True)
    parser.add_argument("--executor-image", required=True)
    parser.add_argument("--stage", required=True)
    parser.add_argument("--landing-manifest-uri")
    parser.add_argument("--expected-entity-count", type=int, default=0)
    parser.add_argument(
        "--entity-count-tolerance-percent",
        type=float,
        default=5.0,
    )
    parser.add_argument("--minimum-name-coverage", type=float, default=0.0)
    parser.add_argument("--max-closure-iterations", type=int, default=64)
    parser.add_argument(
        "--max-landing-shard-bytes",
        type=int,
        default=5 * 1024 * 1024 * 1024,
    )
    parser.add_argument("--shuffle-partitions", type=int)
    parser.add_argument("--master", help="optional Spark master, e.g. local[2]")
    parser.add_argument(
        "--app-name",
        default="video-media-catalog-validate",
    )
    parser.add_argument("--spark-packages")
    parser.add_argument("--aws-region", default=os.environ.get("AWS_REGION"))
    parser.add_argument("--s3-endpoint", default=os.environ.get("S3_ENDPOINT"))
    parser.add_argument(
        "--s3-path-style-access",
        action="store_true",
        default=os.environ.get("S3_PATH_STYLE", "").lower() in {"1", "true", "yes"},
    )
    return parser


def _spark_session(parsed: argparse.Namespace) -> Any:
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(parsed.app_name)
    if parsed.master:
        builder = builder.master(parsed.master)
    builder = configure_s3a_builder(
        builder,
        aws_region=parsed.aws_region,
        s3_endpoint=parsed.s3_endpoint,
        s3_path_style_access=parsed.s3_path_style_access,
    )
    if parsed.shuffle_partitions is not None:
        builder = builder.config(
            "spark.sql.shuffle.partitions",
            str(parsed.shuffle_partitions),
        )
    if parsed.spark_packages:
        builder = builder.config("spark.jars.packages", parsed.spark_packages)
    return builder.getOrCreate()


def run(
    parsed: argparse.Namespace,
    *,
    spark: Any | None = None,
    publisher: ControlPublisher | None = None,
) -> QualityReport:
    runtime = runtime_arguments(parsed)
    if parsed.stage != VALIDATE_STAGE:
        raise ValueError(f"--stage must be {VALIDATE_STAGE}")
    if parsed.shuffle_partitions is not None and parsed.shuffle_partitions < 1:
        raise ValueError("shuffle-partitions must be positive")
    config = QualityConfig(
        expected_entity_count=parsed.expected_entity_count,
        entity_count_tolerance_percent=parsed.entity_count_tolerance_percent,
        minimum_name_coverage=parsed.minimum_name_coverage,
        max_closure_iterations=parsed.max_closure_iterations,
    )
    landing_input = load_landing_input(
        runtime,
        landing_manifest_uri=parsed.landing_manifest_uri,
        aws_region=parsed.aws_region,
        s3_endpoint=parsed.s3_endpoint,
        s3_path_style_access=parsed.s3_path_style_access,
        max_landing_shard_bytes=parsed.max_landing_shard_bytes,
    )
    quality_publisher = publisher or control_publisher(
        runtime.stage_prefix(VALIDATE_STAGE),
        aws_region=parsed.aws_region,
        s3_endpoint=parsed.s3_endpoint,
        s3_path_style_access=parsed.s3_path_style_access,
    )
    existing = read_quality_gate(
        publisher=quality_publisher,
        runtime=runtime,
        landing_input=landing_input,
        expected_config_digest=config.digest,
        required=False,
        require_pass=False,
    )
    if existing is not None:
        return existing.report
    pending = read_pending_quality_report(
        publisher=quality_publisher,
        runtime=runtime,
        landing_input=landing_input,
        expected_config_digest=config.digest,
    )
    if pending is not None:
        publish_quality_report(
            publisher=quality_publisher,
            runtime=runtime,
            landing_input=landing_input,
            report=pending,
        )
        return pending

    started_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    owns_spark = spark is None
    session = spark or _spark_session(parsed)
    try:
        landing = load_landing_frame(session, landing_input)
        frames = transform_landing(
            session,
            landing,
            max_closure_iterations=config.max_closure_iterations,
        )
        metrics = compute_quality_metrics(frames)
        report = build_quality_report(
            runtime=runtime,
            landing_input=landing_input,
            config=config,
            metrics=metrics,
            started_at=started_at,
        )
        publish_quality_report(
            publisher=quality_publisher,
            runtime=runtime,
            landing_input=landing_input,
            report=report,
        )
        return report
    finally:
        if owns_spark:
            session.stop()


def main(argv: Sequence[str] | None = None) -> int:
    report = run(build_parser().parse_args(argv))
    print(
        canonical_json(
            report.model_dump(mode="json", by_alias=True, exclude_none=False)
        )
    )
    return 0 if report.status == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
