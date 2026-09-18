"""Strict Spark/Iceberg media-catalog commit worker."""

from __future__ import annotations

import argparse
import os
import time
from collections.abc import Sequence
from typing import Any
from urllib.parse import urlparse

from video_media_catalog.canonical import canonical_json, sha256_digest
from video_media_catalog.commit import (
    MAX_CONTROL_BYTES,
    control_publisher,
    publish_commit,
)
from video_media_catalog.constants import STAGE
from video_media_catalog.iceberg import CatalogConfig, MediaCatalogTables
from video_media_catalog.identity import uuid7_timestamp_iso
from video_media_catalog.landing import validate_landing_manifest
from video_media_catalog.models import (
    Checksum,
    LandingManifest,
    LandingSummary,
    ObjectRef,
)
from video_media_catalog.object_store import BoundedObjectStore
from video_media_catalog.runtime_args import RuntimeArguments, join_uri
from video_media_catalog.spark_transform import transform_landing
from video_media_catalog.storage import digest_file, local_path


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
    return parser


def _runtime_arguments(parsed: argparse.Namespace) -> RuntimeArguments:
    return RuntimeArguments.model_validate(
        {field: getattr(parsed, field) for field in RuntimeArguments.model_fields}
    )


def _read_s3_bytes(
    uri: str,
    *,
    region: str | None,
    endpoint_url: str | None,
    path_style_access: bool,
) -> bytes:
    import boto3
    from botocore.config import Config

    parsed = urlparse(uri)
    response = boto3.client(
        "s3",
        region_name=region,
        endpoint_url=endpoint_url,
        config=Config(
            signature_version="s3v4",
            retries={"max_attempts": 5, "mode": "standard"},
            s3={"addressing_style": ("path" if path_style_access else "virtual")},
        ),
    ).get_object(
        Bucket=parsed.netloc,
        Key=parsed.path.lstrip("/"),
        ChecksumMode="ENABLED",
    )
    length = int(response.get("ContentLength", 0))
    if length > MAX_CONTROL_BYTES:
        response["Body"].close()
        raise ValueError("landing manifest exceeds maximum size")
    body = response["Body"]
    try:
        payload = body.read(MAX_CONTROL_BYTES + 1)
    finally:
        body.close()
    if len(payload) > MAX_CONTROL_BYTES:
        raise ValueError("landing manifest exceeds maximum size")
    return payload


def _read_landing_manifest(
    uri: str,
    *,
    region: str | None,
    endpoint_url: str | None = None,
    path_style_access: bool = False,
) -> tuple[LandingManifest, str, int]:
    parsed = urlparse(uri)
    if parsed.scheme == "s3":
        payload = _read_s3_bytes(
            uri,
            region=region,
            endpoint_url=endpoint_url,
            path_style_access=path_style_access,
        )
        digest = sha256_digest(payload)
        size = len(payload)
    else:
        path = local_path(uri).resolve()
        if path.stat().st_size > MAX_CONTROL_BYTES:
            raise ValueError("landing manifest exceeds maximum size")
        payload = path.read_bytes()
        digest, size = digest_file(path)
    manifest = LandingManifest.model_validate_json(payload)
    validate_landing_manifest(manifest)
    return manifest, digest, size


def _read_landing_summary(
    uri: str,
    *,
    region: str | None,
    endpoint_url: str | None = None,
    path_style_access: bool = False,
) -> LandingSummary:
    parsed = urlparse(uri)
    if parsed.scheme == "s3":
        payload = _read_s3_bytes(
            uri,
            region=region,
            endpoint_url=endpoint_url,
            path_style_access=path_style_access,
        )
    else:
        path = local_path(uri).resolve()
        if path.stat().st_size > MAX_CONTROL_BYTES:
            raise ValueError("landing summary exceeds maximum size")
        payload = path.read_bytes()
    return LandingSummary.model_validate_json(payload)


def _spark_uri(uri: str) -> str:
    return "s3a://" + uri[len("s3://") :] if uri.startswith("s3://") else uri


def _empty_landing(spark: Any) -> Any:
    return spark.createDataFrame(
        [],
        """
        record_key STRING NOT NULL,
        source STRING NOT NULL,
        source_record_id STRING NOT NULL,
        source_revision STRING,
        modified STRING,
        source_hash STRING NOT NULL,
        payload_json STRING NOT NULL
        """,
    )


def run(parsed: argparse.Namespace) -> dict[str, Any]:
    runtime = _runtime_arguments(parsed)
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

    landing_manifest_uri = parsed.landing_manifest_uri or join_uri(
        runtime.stage_prefix("media-catalog-extract"),
        "landing-manifest.json",
    )
    manifest, manifest_digest, _ = _read_landing_manifest(
        landing_manifest_uri,
        region=parsed.aws_region,
        endpoint_url=parsed.s3_endpoint,
        path_style_access=parsed.s3_path_style_access,
    )
    summary_uri = join_uri(
        landing_manifest_uri.rsplit("/", 1)[0],
        "landing-summary.json",
    )
    summary = _read_landing_summary(
        summary_uri,
        region=parsed.aws_region,
        endpoint_url=parsed.s3_endpoint,
        path_style_access=parsed.s3_path_style_access,
    )
    if (
        summary.manifest_uri != landing_manifest_uri
        or summary.manifest_checksum != manifest_digest
        or summary.manifest_id != manifest.manifest_id
        or summary.record_count != manifest.record_count
        or summary.shard_count != len(manifest.shards)
    ):
        raise ValueError("landing summary does not bind the landing manifest")
    expected_input_digest = f"sha256:hex:{runtime.manifest_hash}"
    if manifest.input_manifest_digest != expected_input_digest and (
        parsed.landing_manifest_uri is None
        or urlparse(landing_manifest_uri).scheme == "s3"
    ):
        raise ValueError("landing manifest does not bind the JobSpec input manifest")
    object_store: BoundedObjectStore | None = None
    for shard in manifest.shards:
        reference = ObjectRef(
            uri=shard.uri,
            format="OBJECT_FORMAT_PARQUET",
            media_type="application/vnd.apache.parquet",
            checksum=Checksum(value=shard.checksum),
            size_bytes=shard.size_bytes,
            etag=shard.etag,
            object_version=shard.object_version,
        )
        if urlparse(shard.uri).scheme == "file":
            digest, size = digest_file(local_path(shard.uri))
            if (
                size > parsed.max_landing_shard_bytes
                or size != shard.size_bytes
                or digest != shard.checksum
            ):
                raise ValueError(
                    "local landing shard differs from its immutable declaration"
                )
        else:
            if object_store is None:
                object_store = BoundedObjectStore(
                    region=parsed.aws_region,
                    endpoint_url=parsed.s3_endpoint,
                    path_style_access=parsed.s3_path_style_access,
                )
            object_store.verify(
                reference,
                max_bytes=parsed.max_landing_shard_bytes,
            )
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
        shard_uris = [_spark_uri(shard.uri) for shard in manifest.shards]
        landing = (
            spark.read.parquet(*shard_uris) if shard_uris else _empty_landing(spark)
        ).persist()
        actual_record_count = landing.count()
        if actual_record_count != manifest.record_count:
            raise ValueError(
                "landing record count mismatch: "
                f"manifest={manifest.record_count}, "
                f"actual={actual_record_count}"
            )
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
