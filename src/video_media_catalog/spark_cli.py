"""Strict Spark/Iceberg media-catalog commit worker."""

from __future__ import annotations

import argparse
import os
import time
from collections.abc import Sequence
from typing import Any

from video_media_catalog.canonical import canonical_json, sha256_digest
from video_media_catalog.commit import control_publisher, publish_commit
from video_media_catalog.constants import STAGE
from video_media_catalog.iceberg import CatalogConfig, MediaCatalogTables
from video_media_catalog.identity import uuid7_timestamp_iso
from video_media_catalog.quality import (
    VALIDATE_STAGE,
    QualityConfig,
    read_quality_gate,
)
from video_media_catalog.spark_input import (
    load_landing_frame,
    load_landing_input,
    runtime_arguments,
)
from video_media_catalog.spark_transform import transform_landing


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="video-media-catalog-spark",
        description="Transform landing shards and commit six Iceberg tables.",
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
    parser.add_argument(
        "--landing-manifest-uri",
        help="override the automatically derived extract-stage manifest",
    )
    parser.add_argument(
        "--catalog-name",
        default=os.environ.get(
            "MEDIA_CATALOG_CATALOG_NAME",
            os.environ.get("MEDIA_CATALOG_CATALOG", "media"),
        ),
    )
    parser.add_argument(
        "--namespace",
        default=os.environ.get("MEDIA_CATALOG_NAMESPACE", "media_catalog"),
    )
    parser.add_argument(
        "--catalog-type",
        choices=("hadoop", "glue"),
        default=os.environ.get(
            "MEDIA_CATALOG_CATALOG_TYPE",
            os.environ.get("MEDIA_CATALOG_TYPE", "glue"),
        ),
    )
    parser.add_argument(
        "--warehouse",
        default=os.environ.get(
            "MEDIA_CATALOG_WAREHOUSE_URI",
            os.environ.get("MEDIA_CATALOG_WAREHOUSE"),
        ),
    )
    parser.add_argument("--aws-region", default=os.environ.get("AWS_REGION"))
    parser.add_argument("--s3-endpoint", default=os.environ.get("S3_ENDPOINT"))
    parser.add_argument(
        "--s3-path-style-access",
        action="store_true",
        default=os.environ.get("S3_PATH_STYLE", "").lower() in {"1", "true", "yes"},
    )
    parser.add_argument("--master", help="optional Spark master, e.g. local[2]")
    parser.add_argument("--app-name", default="video-media-catalog")
    parser.add_argument(
        "--config-digest",
        help="sha256 digest of non-secret runtime config; derived when omitted",
    )
    parser.add_argument("--max-closure-iterations", type=int, default=64)
    parser.add_argument("--expected-entity-count", type=int, default=0)
    parser.add_argument(
        "--entity-count-tolerance-percent",
        type=float,
        default=5.0,
    )
    parser.add_argument("--minimum-name-coverage", type=float, default=0.0)
    parser.add_argument(
        "--max-landing-shard-bytes",
        type=int,
        default=5 * 1024 * 1024 * 1024,
    )
    parser.add_argument("--shuffle-partitions", type=int)
    parser.add_argument(
        "--spark-packages",
        help="optional Maven coordinates for local development",
    )
    quality = parser.add_mutually_exclusive_group()
    quality.add_argument(
        "--quality-report-required",
        dest="quality_report_required",
        action="store_true",
        default=True,
        help="require a bound PASS validation report before any Iceberg write",
    )
    quality.add_argument(
        "--no-quality-report-required",
        dest="quality_report_required",
        action="store_false",
        help="explicitly disable the quality gate for tests/backward compatibility",
    )
    return parser


def run(parsed: argparse.Namespace) -> dict[str, Any]:
    runtime = runtime_arguments(parsed)
    if parsed.stage != STAGE:
        raise ValueError(f"--stage must be {STAGE}")
    if not parsed.warehouse:
        raise ValueError("--warehouse or MEDIA_CATALOG_WAREHOUSE_URI is required")
    if parsed.max_closure_iterations < 1:
        raise ValueError("max-closure-iterations must be positive")
    if parsed.max_landing_shard_bytes < 1:
        raise ValueError("max-landing-shard-bytes must be positive")
    if parsed.shuffle_partitions is not None and parsed.shuffle_partitions < 1:
        raise ValueError("shuffle-partitions must be positive")

    landing_input = load_landing_input(
        runtime,
        landing_manifest_uri=parsed.landing_manifest_uri,
        aws_region=parsed.aws_region,
        s3_endpoint=parsed.s3_endpoint,
        s3_path_style_access=parsed.s3_path_style_access,
        max_landing_shard_bytes=parsed.max_landing_shard_bytes,
    )
    quality_config_digest: str | None = None
    if parsed.quality_report_required:
        quality_config = QualityConfig(
            expected_entity_count=parsed.expected_entity_count,
            entity_count_tolerance_percent=parsed.entity_count_tolerance_percent,
            minimum_name_coverage=parsed.minimum_name_coverage,
            max_closure_iterations=parsed.max_closure_iterations,
        )
        quality_gate = read_quality_gate(
            publisher=control_publisher(
                runtime.stage_prefix(VALIDATE_STAGE),
                aws_region=parsed.aws_region,
                s3_endpoint=parsed.s3_endpoint,
                s3_path_style_access=parsed.s3_path_style_access,
            ),
            runtime=runtime,
            landing_input=landing_input,
            expected_config_digest=quality_config.digest,
        )
        assert quality_gate is not None
        quality_config_digest = quality_gate.report.config_digest
    config = CatalogConfig(
        catalog_name=parsed.catalog_name,
        namespace=parsed.namespace,
        warehouse=parsed.warehouse,
        catalog_type=parsed.catalog_type,
        aws_region=parsed.aws_region,
        s3_endpoint=parsed.s3_endpoint,
        s3_path_style_access=parsed.s3_path_style_access,
    )
    nonsecret_config = {
        "catalogName": config.catalog_name,
        "namespace": config.namespace,
        "warehouse": config.warehouse,
        "catalogType": config.catalog_type,
        "awsRegion": config.aws_region,
        "s3Endpoint": config.s3_endpoint,
        "s3PathStyleAccess": config.s3_path_style_access,
        "maxClosureIterations": parsed.max_closure_iterations,
        "shufflePartitions": parsed.shuffle_partitions,
    }
    if parsed.quality_report_required:
        nonsecret_config.update(
            {
                "qualityReportRequired": True,
                "qualityConfigDigest": quality_config_digest,
            }
        )
    config_digest = parsed.config_digest or sha256_digest(
        canonical_json(nonsecret_config)
    )

    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(parsed.app_name)
    if parsed.master:
        builder = builder.master(parsed.master)
    builder = config.configure_builder(builder)
    if parsed.shuffle_partitions is not None:
        builder = builder.config(
            "spark.sql.shuffle.partitions",
            str(parsed.shuffle_partitions),
        )
    if parsed.spark_packages:
        builder = builder.config("spark.jars.packages", parsed.spark_packages)
    spark = builder.getOrCreate()
    started_ns = time.monotonic_ns()
    stage_time = uuid7_timestamp_iso(runtime.run_id)
    try:
        landing = load_landing_frame(spark, landing_input)
        frames = transform_landing(
            spark,
            landing,
            max_closure_iterations=parsed.max_closure_iterations,
        )
        catalog = MediaCatalogTables(spark, config)
        catalog.create_tables()
        row_counts = catalog.merge_all(frames)
        snapshots = catalog.capture_snapshots(
            row_counts,
            empty_committed_at=stage_time,
        )
        commit_prefix = runtime.stage_prefix(STAGE)
        snapshot, commit = publish_commit(
            publisher=control_publisher(
                commit_prefix,
                aws_region=parsed.aws_region,
                s3_endpoint=parsed.s3_endpoint,
                s3_path_style_access=parsed.s3_path_style_access,
            ),
            runtime=runtime,
            tables=snapshots,
            row_counts=row_counts,
            config_digest=config_digest,
            stage_time=stage_time,
            started_ns=started_ns,
        )
        return {
            "snapshotSetId": snapshot.snapshot_set_id,
            "commitId": commit.commit_id,
            "outputCount": commit.output_count,
            "outputManifest": commit.output_manifest.uri,
        }
    finally:
        spark.stop()


def main(argv: Sequence[str] | None = None) -> int:
    parsed = build_parser().parse_args(argv)
    print(canonical_json(run(parsed)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
