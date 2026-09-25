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
from video_media_catalog.gold_index_scale import (
    SYNTHETIC_SCALE_DOCUMENTS,
    synthetic_gold_sizing_plan,
)
from video_media_catalog.gold_ingest import (
    GOLD_RELEASE_COMMIT_MEDIA_TYPE,
    GoldReleaseCommit,
)
from video_media_catalog.gold_search_index import (
    DEFAULT_GOLD_BULK_PARTITIONS,
    DEFAULT_GOLD_BULK_WORKERS,
    GOLD_AFFECTED_ENTITY_MANIFEST_MEDIA_TYPE,
    MAPPING_DIGEST,
    MAX_GOLD_BULK_CHUNK_ACTIONS,
    MAX_GOLD_BULK_CHUNK_BYTES,
    MAX_GOLD_BULK_PARTITIONS,
    MAX_GOLD_BULK_WORKERS,
    MAX_GOLD_INCREMENTAL_TASK_SECONDS,
    RESEARCH_INDEX_PREFIX,
    RESEARCH_READ_ALIAS,
    GoldAffectedEntityManifest,
    GoldIndexBuildManifest,
    derive_gold_build_id,
    distributed_gold_bulk_index,
    ensure_gold_index,
    ensure_incremental_gold_baseline,
    gold_index_config_identity,
    gold_index_name,
    gold_partition_receipt_set_digest,
    reconcile_full_gold_index_counts,
    validate_affected_entity_results,
    validate_gold_affected_entity_manifest,
    validate_gold_index_contents,
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
from video_media_catalog.opensearch_index import (
    DEFAULT_MAX_BULK_BYTES,
    current_alias_indices,
    index_document_count,
    switch_read_alias,
)
from video_media_catalog.storage import join_uri

CONTROL_MAX_BYTES = 16 * 1024 * 1024
INDEX_MANIFEST_MEDIA_TYPE = (
    "application/vnd.video-media-catalog.gold-index-build.v2+json"
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="video-media-catalog-gold-index",
        description="Build the shared authenticated research Gold v2 index.",
    )
    parser.add_argument("--release-commit-uri", required=True)
    parser.add_argument("--release-commit-hash", required=True)
    parser.add_argument("--release-commit-size", type=int, required=True)
    parser.add_argument("--release-commit-version", default="")
    parser.add_argument("--release-commit-etag", default="")
    parser.add_argument("--manifest-prefix", required=True)
    parser.add_argument("--completed-at", required=True)
    parser.add_argument("--image-digest", required=True)
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
    parser.add_argument(
        "--bulk-partitions",
        type=int,
        default=DEFAULT_GOLD_BULK_PARTITIONS,
    )
    parser.add_argument(
        "--bulk-workers",
        type=int,
        default=DEFAULT_GOLD_BULK_WORKERS,
    )
    parser.add_argument("--checkpoint-prefix")
    parser.add_argument("--enable-incremental", action="store_true")
    parser.add_argument("--affected-entity-manifest-uri")
    parser.add_argument("--affected-entity-manifest-hash")
    parser.add_argument("--affected-entity-manifest-size", type=int)
    parser.add_argument("--affected-entity-manifest-version", default="")
    parser.add_argument("--affected-entity-manifest-etag", default="")
    parser.add_argument(
        "--incremental-task-timeout-seconds",
        type=int,
        default=6 * 60 * 60,
    )
    parser.add_argument("--request-timeout-seconds", type=float, default=30)
    parser.add_argument("--allow-insecure-opensearch", action="store_true")
    parser.add_argument("--master")
    parser.add_argument("--app-name", default="media-catalog-research-index")
    parser.add_argument("--shuffle-partitions", type=int)
    parser.add_argument("--spark-packages")
    return parser


def build_sizing_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="video-media-catalog-gold-index-plan",
        description="Plan offline synthetic Gold index sizing without OpenSearch.",
    )
    parser.add_argument(
        "--scale",
        action="append",
        choices=tuple(SYNTHETIC_SCALE_DOCUMENTS),
    )
    parser.add_argument("--sample-size", type=int, default=1000)
    parser.add_argument(
        "--bulk-partitions",
        type=int,
        default=DEFAULT_GOLD_BULK_PARTITIONS,
    )
    parser.add_argument(
        "--bulk-workers",
        type=int,
        default=DEFAULT_GOLD_BULK_WORKERS,
    )
    parser.add_argument("--bulk-chunk-size", type=int, default=500)
    parser.add_argument(
        "--bulk-max-chunk-bytes",
        type=int,
        default=DEFAULT_MAX_BULK_BYTES,
    )
    return parser


def _control_ref(
    *,
    uri: str | None,
    checksum: str | None,
    size: int | None,
    version: str,
    etag: str,
    media_type: str,
    label: str,
) -> ObjectRef:
    if uri is None or checksum is None or size is None:
        raise ValueError(f"{label} URI, hash, and size are required")
    match = re.fullmatch(
        r"(?:sha256:hex:|sha256:)?([0-9a-fA-F]{64})",
        checksum,
    )
    if match is None:
        raise ValueError(f"{label} hash must contain 64 hex digits")
    if not 0 < size <= CONTROL_MAX_BYTES:
        raise ValueError(f"{label} size must be between 1 byte and 16 MiB")
    scheme = urlsplit(uri).scheme
    if scheme not in {"file", "s3"}:
        raise ValueError(f"{label} URI must use file:// or s3://")
    normalized_version = version.strip() or None
    normalized_etag = etag.strip().strip('"') or None
    if scheme == "s3" and (normalized_version is None or normalized_etag is None):
        raise ValueError(f"S3 {label} requires version and ETag")
    return ObjectRef(
        uri=uri,
        format="OBJECT_FORMAT_JSON",
        media_type=media_type,
        checksum=Checksum(value=match.group(1).lower()),
        size_bytes=size,
        etag=normalized_etag,
        object_version=normalized_version,
    )


def _release_ref(parsed: argparse.Namespace) -> ObjectRef:
    return _control_ref(
        uri=parsed.release_commit_uri,
        checksum=parsed.release_commit_hash,
        size=parsed.release_commit_size,
        version=parsed.release_commit_version,
        etag=parsed.release_commit_etag,
        media_type=GOLD_RELEASE_COMMIT_MEDIA_TYPE,
        label="release commit",
    )


def _affected_ref(parsed: argparse.Namespace) -> ObjectRef | None:
    values = (
        parsed.affected_entity_manifest_uri,
        parsed.affected_entity_manifest_hash,
        parsed.affected_entity_manifest_size,
        parsed.affected_entity_manifest_version,
        parsed.affected_entity_manifest_etag,
    )
    if not parsed.enable_incremental:
        if any(value not in {None, ""} for value in values):
            raise ValueError(
                "affected-entity manifest arguments require --enable-incremental"
            )
        return None
    return _control_ref(
        uri=parsed.affected_entity_manifest_uri,
        checksum=parsed.affected_entity_manifest_hash,
        size=parsed.affected_entity_manifest_size,
        version=parsed.affected_entity_manifest_version,
        etag=parsed.affected_entity_manifest_etag,
        media_type=GOLD_AFFECTED_ENTITY_MANIFEST_MEDIA_TYPE,
        label="affected-entity manifest",
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


def _read_affected_manifest(
    store: BoundedObjectStore,
    reference: ObjectRef,
) -> GoldAffectedEntityManifest:
    store.verify(reference, max_bytes=CONTROL_MAX_BYTES)
    with tempfile.TemporaryDirectory(prefix="research-gold-affected-") as directory:
        materialized = store.download(
            reference,
            Path(directory) / "affected-entities.json",
            max_bytes=CONTROL_MAX_BYTES,
        )
        return GoldAffectedEntityManifest.model_validate_json(
            materialized.path.read_bytes()
        )


def _validate_bulk_settings(parsed: argparse.Namespace) -> None:
    if not 1 <= parsed.bulk_partitions <= MAX_GOLD_BULK_PARTITIONS:
        raise ValueError(
            f"bulk-partitions must be between 1 and {MAX_GOLD_BULK_PARTITIONS}"
        )
    if not 1 <= parsed.bulk_workers <= MAX_GOLD_BULK_WORKERS:
        raise ValueError(f"bulk-workers must be between 1 and {MAX_GOLD_BULK_WORKERS}")
    if not 1 <= parsed.bulk_chunk_size <= MAX_GOLD_BULK_CHUNK_ACTIONS:
        raise ValueError(
            f"bulk-chunk-size must be between 1 and {MAX_GOLD_BULK_CHUNK_ACTIONS}"
        )
    if not 1 <= parsed.bulk_max_chunk_bytes <= MAX_GOLD_BULK_CHUNK_BYTES:
        raise ValueError(
            f"bulk-max-chunk-bytes must be between 1 and {MAX_GOLD_BULK_CHUNK_BYTES}"
        )
    task_timeout = getattr(parsed, "incremental_task_timeout_seconds", 6 * 60 * 60)
    if not 1 <= task_timeout <= MAX_GOLD_INCREMENTAL_TASK_SECONDS:
        raise ValueError("incremental-task-timeout-seconds must be between 1 and 86400")


def _close(client: Any) -> None:
    transport = getattr(client, "transport", None)
    if transport is not None and hasattr(transport, "close"):
        transport.close()


def _sizing_result(parsed: argparse.Namespace) -> dict[str, Any]:
    _validate_bulk_settings(parsed)
    scales = parsed.scale or list(SYNTHETIC_SCALE_DOCUMENTS)
    plans = [
        synthetic_gold_sizing_plan(
            target_document_count=SYNTHETIC_SCALE_DOCUMENTS[scale],
            sample_size=parsed.sample_size,
            bulk_partitions=parsed.bulk_partitions,
            bulk_workers=parsed.bulk_workers,
            bulk_chunk_size=parsed.bulk_chunk_size,
            bulk_max_chunk_bytes=parsed.bulk_max_chunk_bytes,
        ).model_dump(mode="json", by_alias=True)
        for scale in scales
    ]
    return {"plans": plans}


def run(parsed: argparse.Namespace) -> dict[str, Any]:
    if (
        parsed.read_alias != RESEARCH_READ_ALIAS
        or parsed.index_prefix != RESEARCH_INDEX_PREFIX
    ):
        raise ValueError("research index and alias names are fixed")
    _validate_bulk_settings(parsed)
    reference = _release_ref(parsed)
    affected_reference = _affected_ref(parsed)
    if urlsplit(parsed.manifest_prefix).scheme not in {"file", "s3"}:
        raise ValueError("manifest-prefix must use file:// or s3://")
    checkpoint_prefix = parsed.checkpoint_prefix or parsed.manifest_prefix
    if urlsplit(checkpoint_prefix).scheme not in {"file", "s3"}:
        raise ValueError("checkpoint-prefix must use file:// or s3://")
    control_references = [reference]
    if affected_reference is not None:
        control_references.append(affected_reference)
    local = all(
        urlsplit(item.uri).scheme == "file" for item in control_references
    ) and all(
        urlsplit(prefix).scheme == "file"
        for prefix in (parsed.manifest_prefix, checkpoint_prefix)
    )
    store = BoundedObjectStore(
        region=parsed.aws_region,
        endpoint_url=parsed.s3_endpoint,
        path_style_access=parsed.s3_path_style_access,
        client=object() if local else None,
    )
    commit = _read_commit(store, reference)
    if commit.context_id != "research":
        raise ValueError("release commit is not a research release")
    affected_manifest = (
        None
        if affected_reference is None
        else _read_affected_manifest(store, affected_reference)
    )
    if affected_manifest is not None and affected_reference is not None:
        validate_gold_affected_entity_manifest(
            affected_manifest,
            manifest_reference=affected_reference,
            release_commit=commit,
            release_commit_reference=reference,
        )
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
    config_identity = gold_index_config_identity(
        read_alias=parsed.read_alias,
        index_prefix=parsed.index_prefix,
        shards=parsed.shards,
        replicas=parsed.replicas,
        bulk_chunk_size=parsed.bulk_chunk_size,
        bulk_max_chunk_bytes=parsed.bulk_max_chunk_bytes,
        image_digest=parsed.image_digest,
    )
    config_digest = config_identity.digest
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

    spark = None
    persisted: list[Any] = []
    try:
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
        gold_entity_count = frames["community_gold_entity"].count()
        if gold_entity_count != commit.table_counts["community_gold_entity"]:
            raise RuntimeError(
                "Gold entity count differs from its immutable release commit"
            )
        receipts = []
        reused_receipt_count = 0
        build_mode = "FULL"
        projected_document_count: int
        successful_document_count: int
        failed_document_count: int
        affected_operation_count = 0

        if affected_manifest is None:
            documents = build_gold_search_projection(
                spark,
                gold_tables=frames,
                release_plan_id=commit.release_plan_id,
            ).persist()
            persisted.append(documents)
            projected_document_count = documents.count()
            result = distributed_gold_bulk_index(
                documents,
                connection=connection,
                index_name=index_name,
                build_id=build_id,
                operation="FULL",
                release_commit=reference,
                affected_entity_manifest=None,
                config_identity=config_identity,
                receipt_prefix=checkpoint_prefix,
                partitions=parsed.bulk_partitions,
                workers=parsed.bulk_workers,
                aws_region=parsed.aws_region,
                s3_endpoint=parsed.s3_endpoint,
                s3_path_style_access=parsed.s3_path_style_access,
            )
            if result.input_document_count != projected_document_count:
                raise RuntimeError(
                    "Gold partition input count differs from the projection count"
                )
            successful_document_count = result.document_count
            failed_document_count = result.error_count
            receipts.extend(result.partition_receipts)
            reused_receipt_count += result.reused_partition_count
            actual_count = index_document_count(client, index_name=index_name)
            try:
                reconcile_full_gold_index_counts(
                    gold_entity_count=gold_entity_count,
                    projected_document_count=projected_document_count,
                    successful_document_count=successful_document_count,
                    failed_document_count=failed_document_count,
                    concrete_index_document_count=actual_count,
                )
            except RuntimeError as exc:
                raise RuntimeError(
                    f"{exc}; samples={canonical_json(result.errors)}"
                ) from exc
            if len(receipts) != parsed.bulk_partitions:
                raise RuntimeError(
                    "Gold full build did not publish one receipt per partition"
                )
        else:
            if affected_reference is None:
                raise AssertionError("incremental manifest reference is missing")
            build_mode = "INCREMENTAL"
            baseline_receipt, baseline_reused = ensure_incremental_gold_baseline(
                client,
                alias=parsed.read_alias,
                index_name=index_name,
                build_id=build_id,
                release_commit=commit,
                release_commit_reference=reference,
                affected_manifest=affected_manifest,
                affected_manifest_reference=affected_reference,
                config_identity=config_identity,
                receipt_prefix=checkpoint_prefix,
                request_timeout=parsed.request_timeout_seconds,
                task_timeout_seconds=parsed.incremental_task_timeout_seconds,
                aws_region=parsed.aws_region,
                s3_endpoint=parsed.s3_endpoint,
                s3_path_style_access=parsed.s3_path_style_access,
            )
            receipts.append(baseline_receipt)
            reused_receipt_count += int(baseline_reused)

            upsert_keys = affected_manifest.upsert_entity_keys
            delete_keys = affected_manifest.delete_entity_keys
            upsert_key_frame = spark.createDataFrame(
                [(entity_key,) for entity_key in upsert_keys],
                "entity_key string",
            )
            delete_key_frame = spark.createDataFrame(
                [(entity_key,) for entity_key in delete_keys],
                "entity_key string",
            )
            gold_keys = frames["community_gold_entity"].select("entity_key")
            if (
                upsert_key_frame.join(
                    gold_keys,
                    "entity_key",
                    "left_anti",
                )
                .limit(1)
                .count()
            ):
                raise RuntimeError(
                    "affected UPSERT entity is absent from the target Gold release"
                )
            if (
                delete_key_frame.join(
                    gold_keys,
                    "entity_key",
                    "inner",
                )
                .limit(1)
                .count()
            ):
                raise RuntimeError(
                    "affected DELETE entity still exists in the target Gold release"
                )

            documents = build_gold_search_projection(
                spark,
                gold_tables=frames,
                release_plan_id=commit.release_plan_id,
                affected_entity_keys=upsert_key_frame,
            ).persist()
            persisted.append(documents)
            projected_document_count = documents.count()
            upsert_result = distributed_gold_bulk_index(
                documents,
                connection=connection,
                index_name=index_name,
                build_id=build_id,
                operation="UPSERT",
                release_commit=reference,
                affected_entity_manifest=affected_reference,
                config_identity=config_identity,
                receipt_prefix=checkpoint_prefix,
                partitions=parsed.bulk_partitions,
                workers=parsed.bulk_workers,
                aws_region=parsed.aws_region,
                s3_endpoint=parsed.s3_endpoint,
                s3_path_style_access=parsed.s3_path_style_access,
            )
            delete_documents = delete_key_frame.selectExpr("entity_key AS entityKey")
            delete_result = distributed_gold_bulk_index(
                delete_documents,
                connection=connection,
                index_name=index_name,
                build_id=build_id,
                operation="DELETE",
                release_commit=reference,
                affected_entity_manifest=affected_reference,
                config_identity=config_identity,
                receipt_prefix=checkpoint_prefix,
                partitions=parsed.bulk_partitions,
                workers=parsed.bulk_workers,
                aws_region=parsed.aws_region,
                s3_endpoint=parsed.s3_endpoint,
                s3_path_style_access=parsed.s3_path_style_access,
            )
            affected_operation_count = len(affected_manifest.operations)
            successful_document_count = (
                upsert_result.document_count + delete_result.document_count
            )
            failed_document_count = (
                upsert_result.error_count + delete_result.error_count
            )
            receipts.extend(upsert_result.partition_receipts)
            receipts.extend(delete_result.partition_receipts)
            reused_receipt_count += upsert_result.reused_partition_count
            reused_receipt_count += delete_result.reused_partition_count
            actual_count = index_document_count(client, index_name=index_name)
            if (
                projected_document_count != len(upsert_keys)
                or upsert_result.input_document_count != len(upsert_keys)
                or delete_result.input_document_count != len(delete_keys)
                or successful_document_count != affected_operation_count
                or failed_document_count != 0
                or actual_count != gold_entity_count
                or len(receipts) != 1 + (2 * parsed.bulk_partitions)
            ):
                errors = [*upsert_result.errors, *delete_result.errors]
                raise RuntimeError(
                    "Gold incremental count reconciliation failed: "
                    f"goldEntities={gold_entity_count}, "
                    f"projectedUpserts={projected_document_count}, "
                    f"successfulOperations={successful_document_count}, "
                    f"failedOperations={failed_document_count}, "
                    f"expectedOperations={affected_operation_count}, "
                    f"concreteIndex={actual_count}, "
                    f"samples={canonical_json(errors[:20])}"
                )
            validate_affected_entity_results(
                client,
                index_name=index_name,
                upsert_entity_keys=upsert_keys,
                delete_entity_keys=delete_keys,
            )

        actual_count = validate_gold_index_contents(
            client,
            index_name=index_name,
            release_plan_id=commit.release_plan_id,
            expected_document_count=gold_entity_count,
        )
        receipt_digest = gold_partition_receipt_set_digest(receipts)
        manifest = GoldIndexBuildManifest(
            build_mode=build_mode,
            build_id=build_id,
            release_plan_id=commit.release_plan_id,
            context_id=commit.context_id,
            release_commit=reference,
            affected_entity_manifest=affected_reference,
            table_snapshot_ids=commit.table_snapshot_ids,
            mapping_digest=MAPPING_DIGEST,
            config_identity=config_identity,
            config_digest=config_digest,
            document_count=actual_count,
            gold_entity_count=gold_entity_count,
            successful_document_count=successful_document_count,
            failed_document_count=failed_document_count,
            concrete_index_document_count=actual_count,
            partition_receipt_count=len(receipts),
            partition_receipt_digest=receipt_digest,
            checkpoint_prefix=checkpoint_prefix,
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
        pre_alias_count = validate_gold_index_contents(
            client,
            index_name=index_name,
            release_plan_id=commit.release_plan_id,
            expected_document_count=gold_entity_count,
        )
        if build_mode == "FULL":
            reconcile_full_gold_index_counts(
                gold_entity_count=gold_entity_count,
                projected_document_count=projected_document_count,
                successful_document_count=successful_document_count,
                failed_document_count=failed_document_count,
                concrete_index_document_count=pre_alias_count,
            )
        elif (
            pre_alias_count != gold_entity_count
            or successful_document_count != affected_operation_count
            or failed_document_count != 0
        ):
            raise RuntimeError(
                "Gold incremental counts drifted before alias publication"
            )
        if affected_manifest is not None and current_alias_indices(
            client,
            alias=parsed.read_alias,
        ) not in ([affected_manifest.base_index], [index_name]):
            raise RuntimeError("Gold incremental base alias changed during the build")
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
            "buildMode": build_mode,
            "goldEntityCount": gold_entity_count,
            "documentCount": actual_count,
            "successfulDocumentCount": successful_document_count,
            "failedDocumentCount": failed_document_count,
            "partitionReceiptCount": len(receipts),
            "reusedPartitionReceiptCount": reused_receipt_count,
            "partitionReceiptDigest": receipt_digest,
            "manifest": manifest_ref.model_dump(
                mode="json", by_alias=True, exclude_none=True
            ),
        }
    finally:
        for frame in persisted:
            frame.unpersist()
        if spark is not None:
            spark.stop()
        _close(client)


def main(argv: Sequence[str] | None = None) -> int:
    print(canonical_json(run(build_parser().parse_args(argv))))
    return 0


def sizing_main(argv: Sequence[str] | None = None) -> int:
    print(canonical_json(_sizing_result(build_sizing_parser().parse_args(argv))))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
