"""Spark entry point for rebuilding the OpenSearch search projection."""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import tempfile
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from video_media_catalog.canonical import canonical_json
from video_media_catalog.commit import MAX_CONTROL_BYTES, SNAPSHOT_SET_MEDIA_TYPE
from video_media_catalog.constants import CURATED_TABLE_KEYS
from video_media_catalog.iceberg import (
    TABLE_COLUMNS,
    CatalogConfig,
    MediaCatalogTables,
)
from video_media_catalog.models import Checksum, ObjectRef, SnapshotSet
from video_media_catalog.object_store import BoundedObjectStore, S3Location
from video_media_catalog.opensearch_client import (
    OpenSearchConnection,
    create_opensearch_client,
)
from video_media_catalog.search_index import (
    INDEX_PREFIX,
    MAPPING_DIGEST,
    READ_ALIAS,
    S3IndexManifestPublisher,
    completed_manifest,
    derive_build_id,
    distributed_bulk_index,
    ensure_versioned_index,
    failed_manifest,
    index_config_digest,
    index_document_count,
    switch_read_alias,
    table_snapshot_identity,
    versioned_index_name,
)
from video_media_catalog.search_projection import build_projection


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="video-media-catalog-index",
        description="Rebuild a versioned OpenSearch index from six Iceberg snapshots.",
    )
    parser.add_argument(
        "--snapshot-set-uri",
        required=True,
    )
    parser.add_argument("--snapshot-set-hash", required=True)
    parser.add_argument("--snapshot-set-version", required=True)
    parser.add_argument("--snapshot-set-etag", required=True)
    parser.add_argument("--snapshot-set-size", required=True, type=int)
    parser.add_argument(
        "--manifest-prefix",
        default=os.environ.get("MEDIA_CATALOG_INDEX_MANIFEST_PREFIX"),
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
    parser.add_argument(
        "--opensearch-endpoint",
        default=os.environ.get("MEDIA_CATALOG_OPENSEARCH_ENDPOINT"),
    )
    parser.add_argument(
        "--opensearch-service",
        choices=("es", "aoss"),
        default=os.environ.get("MEDIA_CATALOG_OPENSEARCH_SERVICE", "es"),
    )
    parser.add_argument(
        "--read-alias",
        default=os.environ.get("MEDIA_CATALOG_READ_ALIAS", READ_ALIAS),
    )
    parser.add_argument(
        "--index-prefix",
        default=os.environ.get("MEDIA_CATALOG_INDEX_PREFIX", INDEX_PREFIX),
    )
    parser.add_argument("--aws-region", default=os.environ.get("AWS_REGION"))
    parser.add_argument("--s3-endpoint", default=os.environ.get("S3_ENDPOINT"))
    parser.add_argument(
        "--s3-path-style-access",
        action="store_true",
        default=os.environ.get("S3_PATH_STYLE", "").lower() in {"1", "true", "yes"},
    )
    parser.add_argument(
        "--allow-insecure-opensearch",
        action="store_true",
        help="allow http only for an explicitly configured local endpoint",
    )
    parser.add_argument("--shards", type=int, default=3)
    parser.add_argument("--replicas", type=int, default=1)
    parser.add_argument("--bulk-chunk-size", type=int, default=500)
    parser.add_argument("--bulk-partitions", type=int)
    parser.add_argument("--request-timeout-seconds", type=float, default=30.0)
    parser.add_argument("--shuffle-partitions", type=int)
    parser.add_argument("--master", help="optional Spark master, e.g. local[2]")
    parser.add_argument("--app-name", default="video-media-catalog-index")
    parser.add_argument(
        "--spark-packages",
        help="optional Maven coordinates for local development",
    )
    return parser


def _required(value: str | None, message: str) -> str:
    if value is None or not value.strip():
        raise ValueError(message)
    return value


def _snapshot_set_object_ref(parsed: argparse.Namespace) -> ObjectRef:
    uri = _required(parsed.snapshot_set_uri, "--snapshot-set-uri is required")
    S3Location.parse(uri)
    match = re.fullmatch(
        r"(?:sha256:hex:|sha256:)([0-9a-fA-F]{64})",
        _required(parsed.snapshot_set_hash, "--snapshot-set-hash is required"),
    )
    if match is None:
        raise ValueError("snapshot set hash must use sha256:hex:<hex> or sha256:<hex>")
    version = _required(
        parsed.snapshot_set_version,
        "--snapshot-set-version is required",
    ).strip()
    etag = (
        _required(
            parsed.snapshot_set_etag,
            "--snapshot-set-etag is required",
        )
        .strip()
        .strip('"')
    )
    if not version or not etag:
        raise ValueError("snapshot set version and ETag must be non-empty")
    if not 0 < parsed.snapshot_set_size <= MAX_CONTROL_BYTES:
        raise ValueError("snapshot set size must be between 1 byte and 16 MiB")
    return ObjectRef(
        uri=uri,
        format="OBJECT_FORMAT_JSON",
        media_type=SNAPSHOT_SET_MEDIA_TYPE,
        checksum=Checksum(value=match.group(1).lower()),
        size_bytes=parsed.snapshot_set_size,
        etag=etag.strip('"'),
        object_version=version,
    )


def _read_snapshot_set(
    object_ref: ObjectRef,
    *,
    aws_region: str | None,
    s3_endpoint: str | None,
    s3_path_style_access: bool,
    store: BoundedObjectStore | None = None,
) -> SnapshotSet:
    if object_ref.size_bytes > MAX_CONTROL_BYTES:
        raise ValueError("snapshot set exceeds the 16 MiB limit")
    store = store or BoundedObjectStore(
        region=aws_region,
        endpoint_url=s3_endpoint,
        path_style_access=s3_path_style_access,
    )
    store.verify(object_ref, max_bytes=MAX_CONTROL_BYTES)
    with tempfile.TemporaryDirectory(prefix="media-catalog-snapshot-set-") as root:
        materialized = store.download(
            object_ref,
            Path(root) / "snapshot-set.json",
            max_bytes=MAX_CONTROL_BYTES,
        )
        return SnapshotSet.model_validate_json(materialized.path.read_bytes())


def _manifest_uri(prefix: str, build_id: str) -> str:
    parsed = urlsplit(prefix)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError("index manifest prefix must be an s3:// URI")
    root = prefix.rstrip("/")
    return f"{root}/index-build-{build_id}.json"


def _failed_manifest_uri(prefix: str, build_id: str, started_at: str) -> str:
    _manifest_uri(prefix, build_id)
    attempt_id = hashlib.sha256(started_at.encode("utf-8")).hexdigest()[:16]
    return f"{prefix.rstrip('/')}/failed/index-build-{build_id}-{attempt_id}.json"


def _load_snapshot_tables(
    spark: Any,
    *,
    catalog: MediaCatalogTables,
    snapshot_set: SnapshotSet,
) -> dict[str, Any]:
    declared = {
        table.table_name.rsplit(".", 1)[-1]: table for table in snapshot_set.tables
    }
    if set(declared) != set(CURATED_TABLE_KEYS):
        raise ValueError("snapshot set must contain all six curated tables")
    frames = {}
    for table_name in TABLE_COLUMNS:
        snapshot = declared[table_name]
        expected_name = catalog.table_name(table_name)
        if snapshot.table_name != expected_name:
            raise ValueError(
                f"snapshot table {snapshot.table_name!r} does not match "
                f"configured table {expected_name!r}"
            )
        if snapshot.snapshot_id is None:
            frames[table_name] = spark.table(expected_name).limit(0)
        else:
            frames[table_name] = (
                spark.read.format("iceberg")
                .option("snapshot-id", str(snapshot.snapshot_id))
                .load(expected_name)
            )
    return frames


def _close_client(client: Any) -> None:
    transport = getattr(client, "transport", None)
    if transport is not None and hasattr(transport, "close"):
        transport.close()


def run(parsed: argparse.Namespace) -> dict[str, Any]:
    snapshot_set_object_ref = _snapshot_set_object_ref(parsed)
    snapshot_set_uri = snapshot_set_object_ref.uri
    manifest_prefix = _required(
        parsed.manifest_prefix,
        "--manifest-prefix or MEDIA_CATALOG_INDEX_MANIFEST_PREFIX is required",
    )
    warehouse = _required(
        parsed.warehouse,
        "--warehouse or MEDIA_CATALOG_WAREHOUSE_URI is required",
    )
    endpoint = _required(
        parsed.opensearch_endpoint,
        "--opensearch-endpoint or MEDIA_CATALOG_OPENSEARCH_ENDPOINT is required",
    )
    if parsed.bulk_partitions is not None and parsed.bulk_partitions < 1:
        raise ValueError("bulk-partitions must be positive")
    if parsed.shuffle_partitions is not None and parsed.shuffle_partitions < 1:
        raise ValueError("shuffle-partitions must be positive")

    snapshot_set = _read_snapshot_set(
        snapshot_set_object_ref,
        aws_region=parsed.aws_region,
        s3_endpoint=parsed.s3_endpoint,
        s3_path_style_access=parsed.s3_path_style_access,
    )
    config_digest = index_config_digest(
        read_alias=parsed.read_alias,
        index_prefix=parsed.index_prefix,
        shards=parsed.shards,
        replicas=parsed.replicas,
        bulk_chunk_size=parsed.bulk_chunk_size,
    )
    build_id = derive_build_id(
        snapshot_set=snapshot_set,
        config_digest=config_digest,
    )
    index_name = versioned_index_name(parsed.index_prefix, build_id)
    manifest_uri = _manifest_uri(manifest_prefix, build_id)
    manifest_publisher = S3IndexManifestPublisher(
        manifest_uri,
        aws_region=parsed.aws_region,
        endpoint_url=parsed.s3_endpoint,
        path_style_access=parsed.s3_path_style_access,
    )
    connection = OpenSearchConnection(
        endpoint=endpoint,
        aws_region=parsed.aws_region,
        service=parsed.opensearch_service,
        timeout_seconds=parsed.request_timeout_seconds,
        allow_insecure=parsed.allow_insecure_opensearch,
    )
    client = create_opensearch_client(connection)
    existing_manifest = manifest_publisher.read_optional()
    if existing_manifest is not None:
        try:
            if (
                existing_manifest.status != "COMPLETED"
                or existing_manifest.build_id != build_id
                or existing_manifest.index != index_name
                or existing_manifest.alias != parsed.read_alias
                or existing_manifest.config_digest != config_digest
                or existing_manifest.mapping_digest != MAPPING_DIGEST
                or existing_manifest.error_count != 0
                or existing_manifest.source_snapshot_set_id
                != snapshot_set.snapshot_set_id
                or existing_manifest.source_snapshot_set_uri != snapshot_set_uri
                or existing_manifest.source_snapshot_set != snapshot_set_object_ref
                or [
                    table.model_dump(mode="json", by_alias=True)
                    for table in existing_manifest.table_snapshots
                ]
                != table_snapshot_identity(snapshot_set)
            ):
                raise RuntimeError("existing manifest conflicts with this index build")
            if not client.indices.exists(index=index_name):
                raise RuntimeError("manifest target index does not exist")
            ensure_versioned_index(
                client,
                index_name=index_name,
                shards=parsed.shards,
                replicas=parsed.replicas,
            )
            actual_count = index_document_count(client, index_name=index_name)
            if actual_count != existing_manifest.document_count:
                raise RuntimeError(
                    "existing index count does not match its immutable manifest"
                )
            switch_read_alias(
                client,
                alias=parsed.read_alias,
                target_index=index_name,
            )
            return {
                "buildId": build_id,
                "index": index_name,
                "alias": parsed.read_alias,
                "documentCount": actual_count,
                "errorCount": 0,
                "manifestUri": manifest_uri,
                "reused": True,
            }
        finally:
            _close_client(client)

    try:
        ensure_versioned_index(
            client,
            index_name=index_name,
            shards=parsed.shards,
            replicas=parsed.replicas,
        )
        catalog_config = CatalogConfig(
            catalog_name=parsed.catalog_name,
            namespace=parsed.namespace,
            warehouse=warehouse,
            catalog_type=parsed.catalog_type,
            aws_region=parsed.aws_region,
            s3_endpoint=parsed.s3_endpoint,
            s3_path_style_access=parsed.s3_path_style_access,
        )

        from pyspark.sql import SparkSession

        builder = SparkSession.builder.appName(parsed.app_name)
        if parsed.master:
            builder = builder.master(parsed.master)
        builder = catalog_config.configure_builder(builder)
        if parsed.shuffle_partitions is not None:
            builder = builder.config(
                "spark.sql.shuffle.partitions",
                str(parsed.shuffle_partitions),
            )
        if parsed.spark_packages:
            builder = builder.config("spark.jars.packages", parsed.spark_packages)
        spark = builder.getOrCreate()
    except Exception:
        _close_client(client)
        raise
    documents = None
    started_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")

    def publish_failure(document_count: int, error_count: int) -> None:
        S3IndexManifestPublisher(
            _failed_manifest_uri(
                manifest_prefix,
                build_id,
                started_at,
            ),
            aws_region=parsed.aws_region,
            endpoint_url=parsed.s3_endpoint,
            path_style_access=parsed.s3_path_style_access,
        ).publish(
            failed_manifest(
                snapshot_set=snapshot_set,
                snapshot_set_object_ref=snapshot_set_object_ref,
                build_id=build_id,
                config_digest=config_digest,
                document_count=document_count,
                error_count=error_count,
                index_name=index_name,
                alias=parsed.read_alias,
                started_at=started_at,
            )
        )

    try:
        catalog = MediaCatalogTables(spark, catalog_config)
        tables = _load_snapshot_tables(
            spark,
            catalog=catalog,
            snapshot_set=snapshot_set,
        )
        documents = build_projection(spark, tables).persist()
        expected_count = documents.count()
        bulk_result = distributed_bulk_index(
            documents,
            connection=connection,
            index_name=index_name,
            chunk_size=parsed.bulk_chunk_size,
            partitions=parsed.bulk_partitions,
        )
        if bulk_result.error_count or bulk_result.document_count != expected_count:
            publish_failure(
                bulk_result.document_count,
                bulk_result.error_count
                + int(
                    bulk_result.error_count == 0
                    and bulk_result.document_count != expected_count
                ),
            )
            raise RuntimeError(
                "OpenSearch bulk indexing failed: "
                f"success={bulk_result.document_count}, "
                f"errors={bulk_result.error_count}, "
                f"expected={expected_count}, "
                f"samples={canonical_json(bulk_result.errors)}"
            )
        actual_count = index_document_count(client, index_name=index_name)
        if actual_count != expected_count:
            publish_failure(actual_count, 1)
            raise RuntimeError(
                "OpenSearch document count mismatch: "
                f"expected={expected_count}, actual={actual_count}"
            )
        manifest = manifest_publisher.publish(
            completed_manifest(
                snapshot_set=snapshot_set,
                snapshot_set_object_ref=snapshot_set_object_ref,
                build_id=build_id,
                config_digest=config_digest,
                document_count=actual_count,
                index_name=index_name,
                alias=parsed.read_alias,
                started_at=started_at,
            )
        )
        switch_read_alias(
            client,
            alias=parsed.read_alias,
            target_index=index_name,
        )
        return {
            "buildId": build_id,
            "index": index_name,
            "alias": parsed.read_alias,
            "documentCount": actual_count,
            "errorCount": 0,
            "manifestUri": manifest_uri,
            "completedAt": manifest.completed_at,
            "reused": False,
        }
    finally:
        try:
            if documents is not None:
                documents.unpersist()
        finally:
            try:
                spark.stop()
            finally:
                _close_client(client)


def main(argv: Sequence[str] | None = None) -> int:
    parsed = build_parser().parse_args(argv)
    print(canonical_json(run(parsed)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
