"""Spark CLI for committing community connector records to Silver v2."""

from __future__ import annotations

import argparse
import json
import re
import sys
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from video_media_catalog.canonical import canonical_json
from video_media_catalog.community_iceberg import CommunityCatalogTables
from video_media_catalog.community_sources import build_community_registry
from video_media_catalog.connector import (
    ConnectorBatchManifest,
    ConnectorRecordSetEpochManifest,
    ConnectorRecordSetManifest,
    ConnectorRecordSetPartitionManifest,
    ConnectorShardedRecordSetManifest,
)
from video_media_catalog.iceberg import CatalogConfig
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.object_store import (
    BoundedObjectStore,
    RuntimeObjectStore,
)
from video_media_catalog.record_shard_materialization import (
    MAX_MATERIALIZE_WORKERS,
    MAX_RECORD_SHARD_COUNT,
    PrevalidatedRecordSetGrant,
    RecordShardMaterializationProgress,
    materialize_record_shards,
    prevalidated_grant_applies,
    resolve_record_staging_prefix,
)
from video_media_catalog.source_silver import (
    build_source_silver_dataframes,
    mapper_for_product,
    unpersist_source_silver_frames,
)
from video_media_catalog.source_silver_checkpoint import (
    DEFAULT_SOURCE_SILVER_CHECKPOINT_GROUP_SIZE,
    MAX_SOURCE_SILVER_CHECKPOINT_GROUP_SIZE,
    SourceSilverCheckpointConfig,
    SourceSilverCheckpointProgress,
    resolve_source_silver_checkpoint_prefix,
)
from video_media_catalog.v2_contracts import require_rfc3339, require_sha256

CONTROL_MAX_BYTES = 16 * 1024 * 1024
MAX_RECORD_OBJECT_MAX_BYTES = 128 * 1024 * 1024
DEFAULT_RECORD_OBJECT_MAX_BYTES = MAX_RECORD_OBJECT_MAX_BYTES
DEFAULT_RAW_OBJECT_MAX_BYTES = 32 * 1024**3
RECORD_SET_MEDIA_TYPE = "application/vnd.video-media-catalog.record-set.v2+json"
SHARDED_RECORD_SET_MEDIA_TYPE = (
    "application/vnd.video-media-catalog.sharded-record-set.v2.1+json"
)
_PREVALIDATED_GRANT_ARGUMENTS = (
    "prevalidated_record_set_id",
    "prevalidated_batch_id",
    "prevalidated_record_count",
    "prevalidated_shard_count",
    "prevalidated_size_bytes",
    "prevalidated_expires_at",
)


def _prevalidated_digest(value: str) -> str:
    try:
        return require_sha256(value, label="prevalidated digest")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _prevalidated_expiry(value: str) -> str:
    try:
        return require_rfc3339(value, label="prevalidated expires_at")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _materialize_workers(value: str) -> int:
    parsed = _positive_int(value)
    if parsed > MAX_MATERIALIZE_WORKERS:
        raise argparse.ArgumentTypeError(
            f"value must not exceed {MAX_MATERIALIZE_WORKERS}"
        )
    return parsed


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
    parser.add_argument(
        "--prevalidated-record-set-id",
        type=_prevalidated_digest,
    )
    parser.add_argument(
        "--prevalidated-batch-id",
        type=_prevalidated_digest,
    )
    parser.add_argument("--prevalidated-record-count", type=_positive_int)
    parser.add_argument("--prevalidated-shard-count", type=_positive_int)
    parser.add_argument("--prevalidated-size-bytes", type=_positive_int)
    parser.add_argument(
        "--prevalidated-expires-at",
        type=_prevalidated_expiry,
    )
    parser.add_argument(
        "--materialize-workers",
        type=_materialize_workers,
        default=1,
    )
    parser.add_argument("--silver-checkpoint-prefix")
    parser.add_argument(
        "--checkpoint-group-size",
        type=_positive_int,
        default=DEFAULT_SOURCE_SILVER_CHECKPOINT_GROUP_SIZE,
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


def _read_control_payload(
    store: RuntimeObjectStore,
    reference: ObjectRef,
) -> bytes:
    store.verify(reference, max_bytes=CONTROL_MAX_BYTES)
    with tempfile.TemporaryDirectory(prefix="community-catalog-control-") as directory:
        materialized = store.download(
            reference,
            Path(directory) / "object.json",
            max_bytes=CONTROL_MAX_BYTES,
        )
        return materialized.path.read_bytes()


def _read_model[
    T: (
        ConnectorBatchManifest,
        ConnectorRecordSetManifest,
        ConnectorRecordSetEpochManifest,
        ConnectorRecordSetPartitionManifest,
        ConnectorShardedRecordSetManifest,
    )
](
    store: RuntimeObjectStore,
    reference: ObjectRef,
    model: type[T],
) -> T:
    return model.model_validate_json(_read_control_payload(store, reference))


@dataclass(frozen=True)
class ResolvedConnectorRecordSet:
    """Validated root identity plus flattened immutable record shard references."""

    schema_version: str
    record_set_id: str
    batch_id: str
    source_product_id: str
    policy_id: str
    policy_digest: str
    record_objects: tuple[ObjectRef, ...]
    record_count: int
    first_envelope_key: str
    last_envelope_key: str
    created_at: str


def _require_record_set_binding(
    child: ConnectorRecordSetEpochManifest | ConnectorRecordSetPartitionManifest,
    root: ConnectorShardedRecordSetManifest,
    *,
    label: str,
) -> None:
    if (
        child.batch_id != root.batch_id
        or child.source_product_id != root.source_product_id
        or child.policy_id != root.policy_id
        or child.policy_digest != root.policy_digest
        or child.created_at != root.created_at
    ):
        raise ValueError(f"{label} does not bind the sharded record set")


def _resolve_sharded_record_set(
    store: RuntimeObjectStore,
    root: ConnectorShardedRecordSetManifest,
) -> ResolvedConnectorRecordSet:
    epochs: list[
        tuple[
            ConnectorRecordSetEpochManifest,
            list[ConnectorRecordSetPartitionManifest],
        ]
    ] = []
    epoch_indexes: list[int] = []
    for epoch_reference in root.epoch_objects:
        epoch = _read_model(
            store,
            epoch_reference,
            ConnectorRecordSetEpochManifest,
        )
        _require_record_set_binding(epoch, root, label="record-set epoch")
        epoch_indexes.append(epoch.epoch_index)
        partitions: list[ConnectorRecordSetPartitionManifest] = []
        partition_indexes: list[int] = []
        for partition_reference in epoch.partition_objects:
            partition = _read_model(
                store,
                partition_reference,
                ConnectorRecordSetPartitionManifest,
            )
            _require_record_set_binding(
                partition,
                root,
                label="record-set partition",
            )
            if partition.epoch_index != epoch.epoch_index:
                raise ValueError("record-set partition belongs to another epoch")
            partition_indexes.append(partition.partition_index)
            partitions.append(partition)
        if partition_indexes != sorted(set(partition_indexes)):
            raise ValueError(
                "record-set partition indexes must be unique and increasing"
            )
        if (
            epoch.partition_count != len(partitions)
            or epoch.shard_count != sum(item.shard_count for item in partitions)
            or epoch.record_count != sum(item.record_count for item in partitions)
            or epoch.size_bytes != sum(item.size_bytes for item in partitions)
            or epoch.first_envelope_key != partitions[0].first_envelope_key
            or epoch.last_envelope_key != partitions[-1].last_envelope_key
        ):
            raise ValueError("record-set epoch totals do not match its partitions")
        epochs.append((epoch, partitions))
    if epoch_indexes != sorted(set(epoch_indexes)):
        raise ValueError("record-set epoch indexes must be unique and increasing")

    partitions = [
        partition for _, epoch_partitions in epochs for partition in epoch_partitions
    ]
    shards = [shard for partition in partitions for shard in partition.shards]
    shard_indexes = [item.shard_index for item in shards]
    shard_uris = [item.object_ref.uri for item in shards]
    if shard_indexes != sorted(set(shard_indexes)):
        raise ValueError("record-set shard indexes must be unique and increasing")
    if len(shard_uris) != len(set(shard_uris)):
        raise ValueError("sharded record set contains duplicate shard object URIs")
    if (
        root.epoch_count != len(epochs)
        or root.partition_count != len(partitions)
        or root.shard_count != len(shards)
        or root.record_count != sum(item.record_count for item in shards)
        or root.size_bytes != sum(item.object_ref.size_bytes for item in shards)
        or root.first_envelope_key != shards[0].first_envelope_key
        or root.last_envelope_key != shards[-1].last_envelope_key
    ):
        raise ValueError("sharded record-set totals do not match its manifests")
    return ResolvedConnectorRecordSet(
        schema_version=root.schema_version,
        record_set_id=root.record_set_id,
        batch_id=root.batch_id,
        source_product_id=root.source_product_id,
        policy_id=root.policy_id,
        policy_digest=root.policy_digest,
        record_objects=tuple(item.object_ref for item in shards),
        record_count=root.record_count,
        first_envelope_key=root.first_envelope_key,
        last_envelope_key=root.last_envelope_key,
        created_at=root.created_at,
    )


def _read_record_set(
    store: RuntimeObjectStore,
    reference: ObjectRef,
) -> tuple[ObjectRef, ConnectorRecordSetManifest | ResolvedConnectorRecordSet]:
    payload = _read_control_payload(store, reference)
    value = json.loads(payload)
    if not isinstance(value, dict):
        raise ValueError("record-set manifest must be a JSON object")
    if "epochObjects" not in value:
        return reference, ConnectorRecordSetManifest.model_validate(value)
    sharded = ConnectorShardedRecordSetManifest.model_validate(value)
    return (
        reference.model_copy(update={"media_type": SHARDED_RECORD_SET_MEDIA_TYPE}),
        _resolve_sharded_record_set(store, sharded),
    )


def _has_prevalidated_record_set_grant(parsed: argparse.Namespace) -> bool:
    supplied = [
        getattr(parsed, argument) is not None
        for argument in _PREVALIDATED_GRANT_ARGUMENTS
    ]
    if any(supplied) and not all(supplied):
        raise ValueError(
            "prevalidated record-set grant arguments must be provided together"
        )
    return all(supplied)


def _prevalidated_record_set_grant(
    parsed: argparse.Namespace,
    *,
    batch_manifest_ref: ObjectRef,
    record_set_manifest_ref: ObjectRef,
    staging_prefix: str | None,
) -> PrevalidatedRecordSetGrant | None:
    if not _has_prevalidated_record_set_grant(parsed):
        return None
    if staging_prefix is None:
        raise ValueError(
            "prevalidated record-set grant requires --record-staging-prefix"
        )
    return PrevalidatedRecordSetGrant(
        record_set_id=str(parsed.prevalidated_record_set_id),
        batch_id=str(parsed.prevalidated_batch_id),
        record_count=int(parsed.prevalidated_record_count),
        shard_count=int(parsed.prevalidated_shard_count),
        size_bytes=int(parsed.prevalidated_size_bytes),
        expires_at=str(parsed.prevalidated_expires_at),
        batch_manifest_ref=batch_manifest_ref,
        record_set_manifest_ref=record_set_manifest_ref,
        staging_prefix=staging_prefix,
    )


def run(parsed: argparse.Namespace) -> dict[str, Any]:
    if min(parsed.max_record_object_bytes, parsed.max_raw_object_bytes) < 1:
        raise ValueError("raw and record object byte limits must be positive")
    if parsed.max_record_object_bytes > MAX_RECORD_OBJECT_MAX_BYTES:
        raise ValueError("max-record-object-bytes exceeds the reviewed 128 MiB cap")
    if not 1 <= parsed.max_record_shards <= MAX_RECORD_SHARD_COUNT:
        raise ValueError("max-record-shards is outside the reviewed bound")
    if not 1 <= parsed.materialize_workers <= MAX_MATERIALIZE_WORKERS:
        raise ValueError(
            "materialize-workers is outside the reviewed bound "
            f"of 1..{MAX_MATERIALIZE_WORKERS}"
        )
    if not (
        1 <= parsed.checkpoint_group_size <= MAX_SOURCE_SILVER_CHECKPOINT_GROUP_SIZE
    ):
        raise ValueError(
            "checkpoint-group-size is outside the reviewed bound "
            f"of 1..{MAX_SOURCE_SILVER_CHECKPOINT_GROUP_SIZE}"
        )
    _has_prevalidated_record_set_grant(parsed)
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
        media_type=RECORD_SET_MEDIA_TYPE,
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
    record_set_ref, record_set = _read_record_set(store, record_set_ref)
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
    checkpoint_prefix = resolve_source_silver_checkpoint_prefix(
        parsed.silver_checkpoint_prefix,
        warehouse=parsed.warehouse,
    )
    prevalidated_grant = _prevalidated_record_set_grant(
        parsed,
        batch_manifest_ref=batch_ref,
        record_set_manifest_ref=record_set_ref,
        staging_prefix=staging_prefix,
    )
    prevalidated_reuse = False
    if prevalidated_grant is not None:
        prevalidated_reuse = prevalidated_grant_applies(
            prevalidated_grant,
            batch=batch,
            record_set=record_set,
            batch_manifest_ref=batch_ref,
            record_set_manifest_ref=record_set_ref,
            staging_prefix=staging_prefix,
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
    checkpoint_counts = {"MATERIALIZED": 0, "REUSED": 0}

    def materialization_progress(
        event: RecordShardMaterializationProgress,
    ) -> None:
        if (
            event.completed_shards == 1
            or event.completed_shards == event.total_shards
            or event.completed_shards % 25 == 0
        ):
            print(
                canonical_json(
                    {
                        "completedShards": event.completed_shards,
                        "event": "record-shard-materialization-progress",
                        "shardIndex": event.shard_index,
                        "status": event.status,
                        "totalShards": event.total_shards,
                    }
                ),
                file=sys.stderr,
                flush=True,
            )

    def checkpoint_progress(event: SourceSilverCheckpointProgress) -> None:
        checkpoint_counts[event.status] += 1
        print(
            canonical_json(
                {
                    "checkpointId": event.checkpoint_id,
                    "completedGroups": event.completed_groups,
                    "event": "source-silver-checkpoint-progress",
                    "groupId": event.group_id,
                    "groupIndex": event.group_index,
                    "status": event.status,
                    "totalGroups": event.total_groups,
                }
            ),
            file=sys.stderr,
            flush=True,
        )

    with tempfile.TemporaryDirectory(prefix="community-catalog-scratch-") as scratch:
        scratch_dir = Path(scratch)
        for reference in batch.raw_objects:
            store.verify(reference, max_bytes=parsed.max_raw_object_bytes)
        if not prevalidated_reuse:
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
            workers=parsed.materialize_workers,
            prevalidated_grant=prevalidated_grant,
            batch=batch,
            record_set=record_set,
            batch_manifest_ref=batch_ref,
            record_set_manifest_ref=record_set_ref,
            progress_callback=materialization_progress,
        )
        try:
            ingest_run, frames = build_source_silver_dataframes(
                spark,
                registry=build_community_registry(),
                batch=batch,
                record_set=record_set,
                materialized_shards=materialized_shards,
                checkpoint_config=(
                    None
                    if checkpoint_prefix is None
                    else SourceSilverCheckpointConfig(
                        aws_region=parsed.aws_region,
                        s3_endpoint=parsed.s3_endpoint,
                        s3_path_style_access=parsed.s3_path_style_access,
                    )
                ),
                checkpoint_prefix=checkpoint_prefix,
                checkpoint_group_size=parsed.checkpoint_group_size,
                checkpoint_progress=(
                    checkpoint_progress if checkpoint_prefix is not None else None
                ),
            )
            commit = CommunityCatalogTables(spark, config).stage_and_commit(
                run=ingest_run,
                dataframes=frames,
                committed_at=parsed.committed_at,
            )
            result = {
                "runId": ingest_run.run_id,
                "commitKey": commit.commit_key,
                "sourceProductId": ingest_run.source_product_id,
                "tableCounts": commit.table_counts,
                "tableSnapshotIds": commit.table_snapshot_ids,
                "recordShardMaterialization": {
                    "reuseCount": sum(
                        shard.prevalidated_reuse for shard in materialized_shards
                    ),
                    "fullCount": sum(
                        not shard.prevalidated_reuse for shard in materialized_shards
                    ),
                },
            }
            if checkpoint_prefix is not None:
                result["sourceSilverCheckpoint"] = {
                    "materializedGroupCount": checkpoint_counts["MATERIALIZED"],
                    "reusedGroupCount": checkpoint_counts["REUSED"],
                    "totalGroupCount": sum(checkpoint_counts.values()),
                }
            return result
        finally:
            unpersist_source_silver_frames(frames)
            spark.stop()


def main(argv: Sequence[str] | None = None) -> int:
    print(canonical_json(run(build_parser().parse_args(argv))))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
