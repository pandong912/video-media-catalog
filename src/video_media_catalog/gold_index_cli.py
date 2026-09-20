"""Build an isolated shadow OpenSearch index from one Gold release commit."""

from __future__ import annotations

import argparse
import re
import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from video_media_catalog.canonical import canonical_json
from video_media_catalog.gold_iceberg import CommunityGoldTables
from video_media_catalog.gold_ingest import (
    GOLD_RELEASE_COMMIT_MEDIA_TYPE,
    GoldReleaseCommit,
)
from video_media_catalog.gold_search_index import (
    MAPPING_DIGEST,
    RESEARCH_INDEX_PREFIX,
    RESEARCH_READ_ALIAS,
    GoldIndexBuildManifest,
    derive_gold_build_id,
    ensure_gold_index,
    gold_index_config_digest,
    gold_index_name,
)
from video_media_catalog.gold_search_projection import (
    build_gold_search_projection,
)
from video_media_catalog.gold_tables import GOLD_DATA_COLUMNS
from video_media_catalog.iceberg import CatalogConfig
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.object_store import BoundedObjectStore
from video_media_catalog.opensearch_client import (
    OpenSearchConnection,
    create_opensearch_client,
)
from video_media_catalog.runtime_args import join_uri
from video_media_catalog.search_index import (
    DEFAULT_MAX_BULK_BYTES,
    distributed_bulk_index,
    index_document_count,
    switch_read_alias,
)
from video_media_catalog.v2_contracts import require_oidc_subject

CONTROL_MAX_BYTES = 16 * 1024 * 1024
INDEX_MANIFEST_MEDIA_TYPE = (
    "application/vnd.video-media-catalog.gold-index-build.v2+json"
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="video-media-catalog-gold-index",
        description="Build the owner-only research Gold v2 index.",
    )
    parser.add_argument("--release-commit-uri", required=True)
    parser.add_argument("--release-commit-hash", required=True)
    parser.add_argument("--release-commit-size", type=int, required=True)
    parser.add_argument("--release-commit-version", default="")
    parser.add_argument("--release-commit-etag", default="")
    parser.add_argument("--manifest-prefix", required=True)
    parser.add_argument("--completed-at", required=True)
    parser.add_argument("--image-digest", required=True)
    parser.add_argument("--owner-subject", required=True)
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
    parser.add_argument("--opensearch-endpoint", required=True)
    parser.add_argument("--opensearch-service", choices=("es", "aoss"), default="es")
    parser.add_argument("--read-alias", default=RESEARCH_READ_ALIAS)
    parser.add_argument("--index-prefix", default=RESEARCH_INDEX_PREFIX)
    parser.add_argument("--shards", type=int, default=1)
    parser.add_argument("--replicas", type=int, default=0)
    parser.add_argument("--bulk-chunk-size", type=int, default=100)
    parser.add_argument(
        "--bulk-max-chunk-bytes",
        type=int,
        default=DEFAULT_MAX_BULK_BYTES,
    )
    parser.add_argument("--bulk-partitions", type=int)
    parser.add_argument("--request-timeout-seconds", type=float, default=30)
    parser.add_argument("--allow-insecure-opensearch", action="store_true")
    parser.add_argument("--master")
    parser.add_argument("--app-name", default="media-catalog-research-index")
    parser.add_argument("--shuffle-partitions", type=int)
    parser.add_argument("--spark-packages")
    return parser


def _release_ref(parsed: argparse.Namespace) -> ObjectRef:
    match = re.fullmatch(
        r"(?:sha256:hex:|sha256:)?([0-9a-fA-F]{64})",
        parsed.release_commit_hash,
    )
    if match is None:
        raise ValueError("release commit hash must contain 64 hex digits")
    if not 0 < parsed.release_commit_size <= CONTROL_MAX_BYTES:
        raise ValueError("release commit size must be between 1 byte and 16 MiB")
    scheme = urlsplit(parsed.release_commit_uri).scheme
    if scheme not in {"file", "s3"}:
        raise ValueError("release commit URI must use file:// or s3://")
    version = parsed.release_commit_version.strip() or None
    etag = parsed.release_commit_etag.strip().strip('"') or None
    if scheme == "s3" and (version is None or etag is None):
        raise ValueError("S3 release commit requires version and ETag")
    return ObjectRef(
        uri=parsed.release_commit_uri,
        format="OBJECT_FORMAT_JSON",
        media_type=GOLD_RELEASE_COMMIT_MEDIA_TYPE,
        checksum=Checksum(value=match.group(1).lower()),
        size_bytes=parsed.release_commit_size,
        etag=etag,
        object_version=version,
    )


def _read_commit(store, reference: ObjectRef) -> GoldReleaseCommit:
    store.verify(reference, max_bytes=CONTROL_MAX_BYTES)
    with tempfile.TemporaryDirectory(prefix="research-gold-release-") as directory:
        materialized = store.download(
            reference,
            Path(directory) / "release-commit.json",
            max_bytes=CONTROL_MAX_BYTES,
        )
        return GoldReleaseCommit.model_validate_json(materialized.path.read_bytes())


def _close(client: Any) -> None:
    transport = getattr(client, "transport", None)
    if transport is not None and hasattr(transport, "close"):
        transport.close()


def run(parsed: argparse.Namespace) -> dict[str, Any]:
    if (
        parsed.read_alias != RESEARCH_READ_ALIAS
        or parsed.index_prefix != RESEARCH_INDEX_PREFIX
    ):
        raise ValueError("research index and alias names are fixed")
    owner_subject = require_oidc_subject(parsed.owner_subject)
    reference = _release_ref(parsed)
    if urlsplit(parsed.manifest_prefix).scheme not in {"file", "s3"}:
        raise ValueError("manifest-prefix must use file:// or s3://")
    local = (
        urlsplit(reference.uri).scheme == "file"
        and urlsplit(parsed.manifest_prefix).scheme == "file"
    )
    store = BoundedObjectStore(
        region=parsed.aws_region,
        endpoint_url=parsed.s3_endpoint,
        path_style_access=parsed.s3_path_style_access,
        client=object() if local else None,
    )
    commit = _read_commit(store, reference)
    if commit.owner_subject != owner_subject:
        raise ValueError("release commit belongs to another OIDC subject")
    if commit.context_id != "research":
        raise ValueError("release commit is not a research release")
    embedded = (commit.quality_report, commit.attribution_manifest)
    if local and any(urlsplit(item.uri).scheme == "s3" for item in embedded):
        store = BoundedObjectStore(
            region=parsed.aws_region,
            endpoint_url=parsed.s3_endpoint,
            path_style_access=parsed.s3_path_style_access,
        )
    for item in embedded:
        store.verify(item, max_bytes=CONTROL_MAX_BYTES)

    config = CatalogConfig(
        catalog_name=parsed.catalog_name,
        namespace=parsed.namespace,
        warehouse=parsed.warehouse,
        catalog_type=parsed.catalog_type,
        aws_region=parsed.aws_region,
        s3_endpoint=parsed.s3_endpoint,
        s3_path_style_access=parsed.s3_path_style_access,
    )
    config_digest = gold_index_config_digest(
        read_alias=parsed.read_alias,
        index_prefix=parsed.index_prefix,
        owner_subject=owner_subject,
        shards=parsed.shards,
        replicas=parsed.replicas,
        bulk_chunk_size=parsed.bulk_chunk_size,
        bulk_max_chunk_bytes=parsed.bulk_max_chunk_bytes,
        image_digest=parsed.image_digest,
    )
    build_id = derive_gold_build_id(
        commit=commit,
        config_digest=config_digest,
    )
    index_name = gold_index_name(parsed.index_prefix, build_id)
    connection = OpenSearchConnection(
        endpoint=parsed.opensearch_endpoint,
        aws_region=parsed.aws_region,
        service=parsed.opensearch_service,
        timeout_seconds=parsed.request_timeout_seconds,
        allow_insecure=parsed.allow_insecure_opensearch,
    )
    client = create_opensearch_client(connection)

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
    documents = None
    try:
        ensure_gold_index(
            client,
            index_name=index_name,
            shards=parsed.shards,
            replicas=parsed.replicas,
        )
        tables = CommunityGoldTables(spark, config)
        frames = {}
        for table in GOLD_DATA_COLUMNS:
            snapshot_id = commit.table_snapshot_ids[table]
            frames[table] = (
                spark.table(tables.table_name(table)).limit(0)
                if snapshot_id is None
                else spark.read.format("iceberg")
                .option("snapshot-id", str(snapshot_id))
                .load(tables.table_name(table))
                .where(f"release_plan_id = '{commit.release_plan_id}'")
            )
        documents = build_gold_search_projection(
            spark,
            gold_tables=frames,
            release_plan_id=commit.release_plan_id,
        ).persist()
        expected_count = documents.count()
        current_count = index_document_count(client, index_name=index_name)
        if current_count > expected_count:
            raise RuntimeError("existing shadow index contains unexpected documents")
        if current_count != expected_count:
            result = distributed_bulk_index(
                documents,
                connection=connection,
                index_name=index_name,
                chunk_size=parsed.bulk_chunk_size,
                max_chunk_bytes=parsed.bulk_max_chunk_bytes,
                partitions=parsed.bulk_partitions,
            )
            if result.error_count or result.document_count != expected_count:
                raise RuntimeError(
                    "Gold shadow bulk indexing failed: "
                    f"success={result.document_count}, "
                    f"errors={result.error_count}, expected={expected_count}"
                )
        actual_count = index_document_count(client, index_name=index_name)
        if actual_count != expected_count:
            raise RuntimeError("Gold shadow document count mismatch")
        manifest = GoldIndexBuildManifest(
            build_id=build_id,
            release_plan_id=commit.release_plan_id,
            owner_subject=commit.owner_subject,
            context_id=commit.context_id,
            release_commit=reference,
            table_snapshot_ids=commit.table_snapshot_ids,
            mapping_digest=MAPPING_DIGEST,
            config_digest=config_digest,
            document_count=actual_count,
            index=index_name,
            alias=parsed.read_alias,
            completed_at=parsed.completed_at,
        )
        manifest_ref = store.upload_bytes(
            manifest.json_bytes(),
            join_uri(
                parsed.manifest_prefix,
                f"gold-index-build-{build_id}.json",
            ),
            media_type=INDEX_MANIFEST_MEDIA_TYPE,
            object_format="OBJECT_FORMAT_JSON",
            max_bytes=CONTROL_MAX_BYTES,
        ).object_ref
        switch_read_alias(
            client,
            alias=parsed.read_alias,
            target_index=index_name,
        )
        return {
            "buildId": build_id,
            "index": index_name,
            "alias": parsed.read_alias,
            "contextId": commit.context_id,
            "documentCount": actual_count,
            "manifest": manifest_ref.model_dump(
                mode="json", by_alias=True, exclude_none=True
            ),
        }
    finally:
        if documents is not None:
            documents.unpersist()
        spark.stop()
        _close(client)


def main(argv: Sequence[str] | None = None) -> int:
    print(canonical_json(run(build_parser().parse_args(argv))))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
