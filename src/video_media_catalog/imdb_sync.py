"""Immutable capture of official IMDb non-commercial TSV snapshots."""

from __future__ import annotations

import hashlib
import multiprocessing
from collections.abc import Iterator, Mapping
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from video_media_catalog.canonical import deterministic_key
from video_media_catalog.connector import (
    ChangeSemantics,
    Completeness,
    ConnectorBatchManifest,
    ConnectorRecordEnvelope,
    DeleteCoverage,
    RecordOperation,
    Serialization,
    SourceWindow,
    TransportKind,
    build_connector_batch_manifest,
    build_connector_record_envelope,
)
from video_media_catalog.connector_publish import (
    PublishedConnectorCapture,
    PublishedConnectorPartition,
    finalize_connector_partitions,
    load_connector_partition,
    publish_connector_batch_manifest,
    publish_connector_capture,
    publish_connector_partition,
)
from video_media_catalog.imdb import (
    IMDB_CONNECTOR_ID,
    IMDB_DATASET_FILES,
    IMDB_DATASET_ORIGIN,
    IMDB_POLICY_ID,
    IMDB_RECORD_NAMESPACE_ID,
    IMDB_SOURCE_PRODUCT_ID,
    IMDB_SOURCE_SYSTEM_ID,
    imdb_record_id,
    imdb_rights_profile,
    iter_imdb_rows,
)
from video_media_catalog.models import ObjectRef
from video_media_catalog.object_store import BoundedObjectStore, RuntimeObjectStore
from video_media_catalog.runtime_args import join_uri
from video_media_catalog.v2_contracts import require_rfc3339, require_sha256

DEFAULT_MAX_DATASET_BYTES = 5 * 1024**3
DEFAULT_IMDB_RECORD_SHARD_BYTES = 128 * 1024 * 1024
DEFAULT_IMDB_DATASET_PARALLELISM = len(IMDB_DATASET_FILES)
IMDB_PARALLEL_PUBLISHER_VERSION = "imdb-dataset-process-pool-v1"


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def imdb_dataset_partition_label(dataset: str) -> str:
    if dataset not in IMDB_DATASET_FILES:
        raise ValueError(f"unsupported IMDb dataset: {dataset}")
    return dataset.removesuffix(".tsv.gz").replace(".", "-")


def imdb_dataset_key_namespace(dataset: str) -> str:
    if dataset not in IMDB_DATASET_FILES:
        raise ValueError(f"unsupported IMDb dataset: {dataset}")
    return f"{dataset.removesuffix('.tsv.gz')}:"


@dataclass(frozen=True)
class ImdbDatasetInspectionTask:
    dataset: str
    dataset_path: str


def _inspect_imdb_dataset(
    task: ImdbDatasetInspectionTask,
) -> tuple[str, int, str]:
    path = Path(task.dataset_path)
    count = sum(1 for _ in iter_imdb_rows(path, task.dataset))
    if count < 1:
        raise ValueError(f"IMDb {task.dataset} contains no records")
    return task.dataset, count, _file_sha256(path)


def _prepare_imdb_batch(
    *,
    dataset_paths: Mapping[str, Path],
    destination_prefix: str,
    acquired_at: str,
    image_digest: str,
    config_digest: str,
    store: RuntimeObjectStore,
    retry_count: int,
    rate_limit_count: int,
    max_dataset_bytes: int,
    window_start: str | None,
    window_end: str | None,
    cursor: str | None,
    watermark: str | None,
    dataset_parallelism: int = 1,
) -> tuple[ConnectorBatchManifest, tuple[ObjectRef, ...], dict[str, int]]:
    acquired = require_rfc3339(acquired_at, label="acquired_at")
    image = require_sha256(image_digest, label="image_digest")
    config = require_sha256(config_digest, label="config_digest")
    if set(dataset_paths) != set(IMDB_DATASET_FILES):
        raise ValueError("IMDb snapshot requires all official datasets in fixed order")
    if not 1 <= dataset_parallelism <= len(IMDB_DATASET_FILES):
        raise ValueError("dataset_parallelism must be between 1 and 7")
    if max_dataset_bytes < 1 or retry_count < 0 or rate_limit_count < 0:
        raise ValueError("IMDb capture limits and counters are invalid")
    if (window_start is None) != (window_end is None):
        raise ValueError("IMDb window_start and window_end must be provided together")
    source_window = (
        SourceWindow(start=window_start, end=window_end)
        if window_start is not None and window_end is not None
        else None
    )
    if cursor is not None:
        cursor = cursor.strip()
        if not cursor or len(cursor) > 1024:
            raise ValueError("IMDb cursor must be non-empty and bounded")
    policy = imdb_rights_profile()
    capture_id = deterministic_key(
        "imdb-official-tsv-capture-v1",
        {
            "acquiredAt": acquired,
            "imageDigest": image,
            "configDigest": config,
            "policyDigest": policy.digest,
        },
    ).removeprefix("sha256:")

    inspection_tasks = tuple(
        ImdbDatasetInspectionTask(dataset, str(dataset_paths[dataset]))
        for dataset in IMDB_DATASET_FILES
    )
    if dataset_parallelism == 1:
        inspections = [_inspect_imdb_dataset(task) for task in inspection_tasks]
    else:
        with ProcessPoolExecutor(
            max_workers=dataset_parallelism,
            mp_context=multiprocessing.get_context("spawn"),
        ) as executor:
            inspections = list(executor.map(_inspect_imdb_dataset, inspection_tasks))
    inspected = {dataset: (count, digest) for dataset, count, digest in inspections}

    raw_objects: list[ObjectRef] = []
    record_counts: dict[str, int] = {}
    for dataset in IMDB_DATASET_FILES:
        path = dataset_paths[dataset]
        count, digest = inspected[dataset]
        record_counts[dataset] = count
        uploaded = store.upload_file(
            path,
            join_uri(
                destination_prefix,
                "imdb",
                "captures",
                capture_id,
                dataset,
                f"{digest}.tsv.gz",
            ),
            media_type="application/gzip",
            object_format="OBJECT_FORMAT_OTHER",
            max_bytes=max_dataset_bytes,
        ).object_ref
        if uploaded.checksum.value != digest.removeprefix("sha256:"):
            raise RuntimeError(
                f"IMDb {dataset} changed while its capture was being published"
            )
        raw_objects.append(uploaded)
    batch = build_connector_batch_manifest(
        source_system_id=IMDB_SOURCE_SYSTEM_ID,
        source_product_id=IMDB_SOURCE_PRODUCT_ID,
        connector_id=IMDB_CONNECTOR_ID,
        connector_version="1.0.0",
        image_digest=image,
        config_digest=config,
        policy_id=IMDB_POLICY_ID,
        policy_digest=policy.digest,
        transport_kind=TransportKind.DUMP,
        serialization=Serialization.TSV,
        change_semantics=ChangeSemantics.FULL_SNAPSHOT,
        completeness=Completeness.COMPLETE,
        delete_coverage=DeleteCoverage.SNAPSHOT_DIFF,
        coverage_scope={
            "origin": IMDB_DATASET_ORIGIN,
            "datasets": list(IMDB_DATASET_FILES),
            "usage": "research",
            **({"cursor": cursor} if cursor is not None else {}),
        },
        source_window=source_window,
        watermark_after=watermark,
        raw_objects=tuple(raw_objects),
        acquired_at=acquired,
        record_count=sum(record_counts.values()),
        error_count=0,
        # Attempt-level transport counters must not change the immutable batch
        # identity or prevent partition checkpoint reuse on a retry.
        retry_count=0,
        rate_limit_count=0,
    )
    return batch, tuple(raw_objects), record_counts


def _iter_imdb_dataset_envelopes(
    *,
    batch: ConnectorBatchManifest,
    dataset: str,
    path: Path,
    raw_object: ObjectRef,
) -> Iterator[ConnectorRecordEnvelope]:
    for index, row in enumerate(iter_imdb_rows(path, dataset), start=2):
        yield build_connector_record_envelope(
            payload={"dataset": dataset, "row": row},
            batch_id=batch.batch_id,
            source_system_id=batch.source_system_id,
            source_product_id=batch.source_product_id,
            source_namespace_id=IMDB_RECORD_NAMESPACE_ID,
            source_record_id=imdb_record_id(dataset, row),
            operation=RecordOperation.UPSERT,
            observed_at=batch.acquired_at,
            ingested_at=batch.acquired_at,
            payload_schema="imdb-official-tsv-row-v1",
            raw_object=raw_object,
            source_location=f"/{dataset}/line/{index}",
            policy_id=batch.policy_id,
            policy_digest=batch.policy_digest,
        )


def capture_imdb_snapshot(
    *,
    dataset_paths: Mapping[str, Path],
    destination_prefix: str,
    acquired_at: str,
    image_digest: str,
    config_digest: str,
    store: RuntimeObjectStore,
    retry_count: int = 0,
    rate_limit_count: int = 0,
    max_dataset_bytes: int = DEFAULT_MAX_DATASET_BYTES,
    record_shard_bytes: int = DEFAULT_IMDB_RECORD_SHARD_BYTES,
    window_start: str | None = None,
    window_end: str | None = None,
    cursor: str | None = None,
    watermark: str | None = None,
) -> PublishedConnectorCapture:
    """Publish all seven official files as one complete replayable snapshot."""

    batch, raw_objects, _ = _prepare_imdb_batch(
        dataset_paths=dataset_paths,
        destination_prefix=destination_prefix,
        acquired_at=acquired_at,
        image_digest=image_digest,
        config_digest=config_digest,
        store=store,
        retry_count=retry_count,
        rate_limit_count=rate_limit_count,
        max_dataset_bytes=max_dataset_bytes,
        window_start=window_start,
        window_end=window_end,
        cursor=cursor,
        watermark=watermark,
        dataset_parallelism=1,
    )

    def envelopes() -> Iterator[ConnectorRecordEnvelope]:
        for dataset, path, raw_object in zip(
            IMDB_DATASET_FILES,
            (dataset_paths[item] for item in IMDB_DATASET_FILES),
            raw_objects,
            strict=True,
        ):
            yield from _iter_imdb_dataset_envelopes(
                batch=batch,
                dataset=dataset,
                path=path,
                raw_object=raw_object,
            )

    return publish_connector_capture(
        destination_prefix=destination_prefix,
        batch=batch,
        envelopes=envelopes(),
        store=store,
        record_shard_bytes=record_shard_bytes,
    )


@dataclass(frozen=True)
class ImdbDatasetPublishTask:
    batch_json: str
    dataset: str
    dataset_path: str
    raw_object_json: str
    destination_prefix: str
    partition_index: int
    expected_record_count: int
    record_shard_bytes: int
    aws_region: str | None
    s3_endpoint: str | None
    s3_path_style_access: bool


def _partition_store(task: ImdbDatasetPublishTask) -> BoundedObjectStore:
    if urlsplit(task.destination_prefix).scheme == "file":
        return BoundedObjectStore(client=object())
    return BoundedObjectStore(
        region=task.aws_region,
        endpoint_url=task.s3_endpoint,
        path_style_access=task.s3_path_style_access,
    )


def _publish_imdb_dataset_task(
    task: ImdbDatasetPublishTask,
) -> dict[str, Any]:
    return _publish_imdb_dataset(task, store=_partition_store(task))


def _publish_imdb_dataset(
    task: ImdbDatasetPublishTask,
    *,
    store: RuntimeObjectStore,
) -> dict[str, Any]:
    batch = ConnectorBatchManifest.model_validate_json(task.batch_json)
    raw_object = ObjectRef.model_validate_json(task.raw_object_json)
    partition = publish_connector_partition(
        destination_prefix=task.destination_prefix,
        batch=batch,
        partition_label=imdb_dataset_partition_label(task.dataset),
        key_namespace=imdb_dataset_key_namespace(task.dataset),
        partition_index=task.partition_index,
        envelopes=_iter_imdb_dataset_envelopes(
            batch=batch,
            dataset=task.dataset,
            path=Path(task.dataset_path),
            raw_object=raw_object,
        ),
        expected_record_count=task.expected_record_count,
        store=store,
        record_shard_bytes=task.record_shard_bytes,
    )
    return partition.model_dump(mode="json", by_alias=True, exclude_none=True)


def capture_imdb_snapshot_parallel(
    *,
    dataset_paths: Mapping[str, Path],
    destination_prefix: str,
    acquired_at: str,
    image_digest: str,
    config_digest: str,
    store: RuntimeObjectStore,
    dataset_parallelism: int = DEFAULT_IMDB_DATASET_PARALLELISM,
    retry_count: int = 0,
    rate_limit_count: int = 0,
    max_dataset_bytes: int = DEFAULT_MAX_DATASET_BYTES,
    record_shard_bytes: int = DEFAULT_IMDB_RECORD_SHARD_BYTES,
    window_start: str | None = None,
    window_end: str | None = None,
    cursor: str | None = None,
    watermark: str | None = None,
    aws_region: str | None = None,
    s3_endpoint: str | None = None,
    s3_path_style_access: bool = False,
) -> PublishedConnectorCapture:
    """Publish seven exact IMDb dataset partitions with a spawn process pool."""

    if not 1 <= dataset_parallelism <= len(IMDB_DATASET_FILES):
        raise ValueError("dataset_parallelism must be between 1 and 7")
    key_namespaces = tuple(
        imdb_dataset_key_namespace(dataset) for dataset in IMDB_DATASET_FILES
    )
    if len(key_namespaces) != len(set(key_namespaces)):
        raise RuntimeError("IMDb dataset key namespaces are not disjoint")

    batch, raw_objects, record_counts = _prepare_imdb_batch(
        dataset_paths=dataset_paths,
        destination_prefix=destination_prefix,
        acquired_at=acquired_at,
        image_digest=image_digest,
        config_digest=config_digest,
        store=store,
        retry_count=retry_count,
        rate_limit_count=rate_limit_count,
        max_dataset_bytes=max_dataset_bytes,
        window_start=window_start,
        window_end=window_end,
        cursor=cursor,
        watermark=watermark,
        dataset_parallelism=dataset_parallelism,
    )
    batch_manifest_object = publish_connector_batch_manifest(
        destination_prefix=destination_prefix,
        batch=batch,
        store=store,
    )
    completed: dict[int, PublishedConnectorPartition] = {}
    if isinstance(store, BoundedObjectStore):
        for partition_index, dataset in enumerate(IMDB_DATASET_FILES):
            existing = load_connector_partition(
                destination_prefix=destination_prefix,
                batch=batch,
                partition_label=imdb_dataset_partition_label(dataset),
                key_namespace=imdb_dataset_key_namespace(dataset),
                partition_index=partition_index,
                store=store,
            )
            if existing is None:
                continue
            if existing.manifest.record_count != record_counts[dataset]:
                raise ValueError(
                    "existing IMDb partition count differs from official dataset"
                )
            completed[partition_index] = existing
    tasks = tuple(
        ImdbDatasetPublishTask(
            batch_json=batch.json_bytes().decode("utf-8"),
            dataset=dataset,
            dataset_path=str(dataset_paths[dataset]),
            raw_object_json=raw_object.json_bytes().decode("utf-8"),
            destination_prefix=destination_prefix,
            partition_index=partition_index,
            expected_record_count=record_counts[dataset],
            record_shard_bytes=record_shard_bytes,
            aws_region=aws_region,
            s3_endpoint=s3_endpoint,
            s3_path_style_access=s3_path_style_access,
        )
        for partition_index, (dataset, raw_object) in enumerate(
            zip(IMDB_DATASET_FILES, raw_objects, strict=True)
        )
        if partition_index not in completed
    )
    if dataset_parallelism == 1:
        partition_values = [_publish_imdb_dataset(task, store=store) for task in tasks]
    else:
        with ProcessPoolExecutor(
            max_workers=dataset_parallelism,
            mp_context=multiprocessing.get_context("spawn"),
        ) as executor:
            partition_values = list(executor.map(_publish_imdb_dataset_task, tasks))
    partitions = tuple(
        sorted(
            (
                *completed.values(),
                *(
                    PublishedConnectorPartition.model_validate(value)
                    for value in partition_values
                ),
            ),
            key=lambda item: item.manifest.partition_index,
        )
    )
    return finalize_connector_partitions(
        destination_prefix=destination_prefix,
        batch=batch,
        batch_manifest_object=batch_manifest_object,
        partitions=partitions,
        store=store,
    )
