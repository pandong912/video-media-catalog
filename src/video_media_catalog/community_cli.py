"""Spark CLI for committing community connector records to Silver v2."""

from __future__ import annotations

import argparse
import re
import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from video_media_catalog.canonical import canonical_json
from video_media_catalog.community_iceberg import CommunityCatalogTables
from video_media_catalog.community_sources import build_community_registry
from video_media_catalog.connector import (
    ConnectorBatchManifest,
    ConnectorRecordSetManifest,
)
from video_media_catalog.iceberg import CatalogConfig
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.object_store import (
    BoundedObjectStore,
    RuntimeObjectStore,
)
from video_media_catalog.record_shard_materialization import (
    MAX_RECORD_SHARD_COUNT,
    materialize_record_shards,
    resolve_record_staging_prefix,
)
from video_media_catalog.source_silver import (
    build_source_silver_dataframes,
    mapper_for_product,
)

CONTROL_MAX_BYTES = 16 * 1024 * 1024
DEFAULT_RECORD_OBJECT_MAX_BYTES = 16 * 1024 * 1024
DEFAULT_RAW_OBJECT_MAX_BYTES = 32 * 1024**3


def _add_object_args(parser: argparse.ArgumentParser, prefix: str) -> None:
    dashed = prefix.replace("_", "-")
    parser.add_argument(f"--{dashed}-uri", required=True)
    parser.add_argument(f"--{dashed}-hash", required=True)
    parser.add_argument(f"--{dashed}-size", required=True, type=int)
    parser.add_argument(f"--{dashed}-version", default="")
    parser.add_argument(f"--{dashed}-etag", default="")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="video-media-catalog-community-spark",
        description="Commit a verified community connector record set to Silver v2.",
    )
    _add_object_args(parser, "batch_manifest")
    _add_object_args(parser, "record_set_manifest")
    parser.add_argument("--committed-at", required=True)
    parser.add_argument("--catalog-name", default="media")
    parser.add_argument("--namespace", default="video_media_catalog")
    parser.add_argument(
        "--catalog-type",
        choices=("hadoop", "glue"),
        default="glue",
    )
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("--aws-region")
    parser.add_argument("--s3-endpoint")
    parser.add_argument("--s3-path-style-access", action="store_true")
    parser.add_argument(
        "--s3-credentials-provider",
        choices=("web-identity", "default"),
        default="web-identity",
    )
    parser.add_argument("--master")
    parser.add_argument("--app-name", default="community-catalog-v2")
    parser.add_argument("--shuffle-partitions", type=int)
    parser.add_argument("--spark-packages")
    parser.add_argument(
        "--max-record-object-bytes",
        type=int,
        default=DEFAULT_RECORD_OBJECT_MAX_BYTES,
    )
    parser.add_argument(
        "--max-raw-object-bytes",
        type=int,
        default=DEFAULT_RAW_OBJECT_MAX_BYTES,
    )
    parser.add_argument(
        "--record-staging-prefix",
        help=(
            "required S3 prefix for checksum-addressed record shard staging when "
            "record shards use s3://; must stay within the catalog warehouse "
            "bucket under warehouse/research/control or "
            "landing/research/materialized-record-shards"
        ),
    )
    parser.add_argument(
        "--max-record-shards",
        type=int,
        default=MAX_RECORD_SHARD_COUNT,
    )
    return parser


def _object_ref(
    parsed: argparse.Namespace,
    prefix: str,
    *,
    media_type: str,
) -> ObjectRef:
    uri = str(getattr(parsed, f"{prefix}_uri"))
    match = re.fullmatch(
        r"(?:sha256:hex:|sha256:)?([0-9a-fA-F]{64})",
        str(getattr(parsed, f"{prefix}_hash")),
    )
    if match is None:
        raise ValueError(f"{prefix} hash must contain 64 SHA-256 hex digits")
    size = int(getattr(parsed, f"{prefix}_size"))
    if not 0 < size <= CONTROL_MAX_BYTES:
        raise ValueError(f"{prefix} size must be between 1 byte and 16 MiB")
    version = str(getattr(parsed, f"{prefix}_version")).strip() or None
    etag = str(getattr(parsed, f"{prefix}_etag")).strip().strip('"') or None
    scheme = urlsplit(uri).scheme
    if scheme not in {"file", "s3"}:
        raise ValueError(f"{prefix} URI must use file:// or s3://")
    if scheme == "s3" and (version is None or etag is None):
        raise ValueError(f"{prefix} S3 object requires version and ETag")
    return ObjectRef(
        uri=uri,
        format="OBJECT_FORMAT_JSON",
        media_type=media_type,
        checksum=Checksum(value=match.group(1).lower()),
        size_bytes=size,
        etag=etag,
        object_version=version,
    )


def _read_model[T: (ConnectorBatchManifest, ConnectorRecordSetManifest)](
    store: RuntimeObjectStore,
    reference: ObjectRef,
    model: type[T],
) -> T:
    store.verify(reference, max_bytes=CONTROL_MAX_BYTES)
    with tempfile.TemporaryDirectory(prefix="community-catalog-control-") as directory:
        materialized = store.download(
            reference,
            Path(directory) / "object.json",
            max_bytes=CONTROL_MAX_BYTES,
        )
        return model.model_validate_json(materialized.path.read_bytes())


def run(parsed: argparse.Namespace) -> dict[str, Any]:
    if min(parsed.max_record_object_bytes, parsed.max_raw_object_bytes) < 1:
        raise ValueError("raw and record object byte limits must be positive")
    if parsed.max_record_shards < 1:
        raise ValueError("max-record-shards must be positive")
    if parsed.shuffle_partitions is not None and parsed.shuffle_partitions < 1:
        raise ValueError("shuffle-partitions must be positive")
    batch_ref = _object_ref(
        parsed,
        "batch_manifest",
        media_type="application/vnd.video-media-catalog.connector-batch.v2+json",
    )
    record_set_ref = _object_ref(
        parsed,
        "record_set_manifest",
        media_type="application/vnd.video-media-catalog.record-set.v2+json",
    )
    local_inputs = all(
        urlsplit(reference.uri).scheme == "file"
        for reference in (batch_ref, record_set_ref)
    )
    store = BoundedObjectStore(
        region=parsed.aws_region,
        endpoint_url=parsed.s3_endpoint,
        path_style_access=parsed.s3_path_style_access,
        client=object() if local_inputs else None,
    )
    batch = _read_model(store, batch_ref, ConnectorBatchManifest)
    record_set = _read_model(
        store,
        record_set_ref,
        ConnectorRecordSetManifest,
    )
    embedded_references = (*batch.raw_objects, *record_set.record_objects)
    if local_inputs and any(
        urlsplit(reference.uri).scheme == "s3" for reference in embedded_references
    ):
        store = BoundedObjectStore(
            region=parsed.aws_region,
            endpoint_url=parsed.s3_endpoint,
            path_style_access=parsed.s3_path_style_access,
        )
    staging_prefix = resolve_record_staging_prefix(
        parsed.record_staging_prefix,
        warehouse=parsed.warehouse,
        references=record_set.record_objects,
    )
    mapper_for_product(batch.source_product_id)

    config = CatalogConfig(
        catalog_name=parsed.catalog_name,
        namespace=parsed.namespace,
        warehouse=parsed.warehouse,
        catalog_type=parsed.catalog_type,
        aws_region=parsed.aws_region,
        s3_endpoint=parsed.s3_endpoint,
        s3_path_style_access=parsed.s3_path_style_access,
        s3_credentials_provider=parsed.s3_credentials_provider,
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
    frames = None
    with tempfile.TemporaryDirectory(prefix="community-catalog-scratch-") as scratch:
        scratch_dir = Path(scratch)
        for reference in batch.raw_objects:
            store.verify(reference, max_bytes=parsed.max_raw_object_bytes)
        for reference in record_set.record_objects:
            store.verify(
                reference,
                max_bytes=parsed.max_record_object_bytes,
            )
        materialized_shards = materialize_record_shards(
            store,
            record_set.record_objects,
            staging_prefix=staging_prefix,
            scratch_dir=scratch_dir,
            max_bytes=parsed.max_record_object_bytes,
            max_shards=parsed.max_record_shards,
        )
        try:
            ingest_run, frames = build_source_silver_dataframes(
                spark,
                registry=build_community_registry(),
                batch=batch,
                record_set=record_set,
                materialized_shards=materialized_shards,
            )
            commit = CommunityCatalogTables(spark, config).stage_and_commit(
                run=ingest_run,
                dataframes=frames,
                committed_at=parsed.committed_at,
            )
            return {
                "runId": ingest_run.run_id,
                "commitKey": commit.commit_key,
                "sourceProductId": ingest_run.source_product_id,
                "tableCounts": commit.table_counts,
                "tableSnapshotIds": commit.table_snapshot_ids,
            }
        finally:
            if frames is not None:
                for frame in frames.values():
                    frame.unpersist()
            spark.stop()


def main(argv: Sequence[str] | None = None) -> int:
    print(canonical_json(run(build_parser().parse_args(argv))))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
