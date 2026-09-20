"""Immutable capture of official IMDb non-commercial TSV snapshots."""

from __future__ import annotations

import hashlib
from collections.abc import Iterator, Mapping
from pathlib import Path

from video_media_catalog.canonical import deterministic_key
from video_media_catalog.connector import (
    ChangeSemantics,
    Completeness,
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
    DEFAULT_RECORD_SHARD_BYTES,
    PublishedConnectorCapture,
    publish_connector_capture,
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
from video_media_catalog.object_store import RuntimeObjectStore
from video_media_catalog.runtime_args import join_uri
from video_media_catalog.v2_contracts import require_rfc3339, require_sha256

DEFAULT_MAX_DATASET_BYTES = 5 * 1024**3


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


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
    record_shard_bytes: int = DEFAULT_RECORD_SHARD_BYTES,
    window_start: str | None = None,
    window_end: str | None = None,
    cursor: str | None = None,
    watermark: str | None = None,
) -> PublishedConnectorCapture:
    """Publish all seven official files as one complete replayable snapshot."""

    acquired = require_rfc3339(acquired_at, label="acquired_at")
    image = require_sha256(image_digest, label="image_digest")
    config = require_sha256(config_digest, label="config_digest")
    if set(dataset_paths) != set(IMDB_DATASET_FILES):
        raise ValueError("IMDb snapshot requires all official datasets in fixed order")
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

    raw_objects: list[ObjectRef] = []
    record_counts: dict[str, int] = {}
    for dataset in IMDB_DATASET_FILES:
        path = dataset_paths[dataset]
        count = sum(1 for _ in iter_imdb_rows(path, dataset))
        if count < 1:
            raise ValueError(f"IMDb {dataset} contains no records")
        record_counts[dataset] = count
        digest = _file_sha256(path)
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
        retry_count=retry_count,
        rate_limit_count=rate_limit_count,
    )

    def envelopes() -> Iterator[ConnectorRecordEnvelope]:
        for dataset, path, raw_object in zip(
            IMDB_DATASET_FILES,
            (dataset_paths[item] for item in IMDB_DATASET_FILES),
            raw_objects,
            strict=True,
        ):
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

    return publish_connector_capture(
        destination_prefix=destination_prefix,
        batch=batch,
        envelopes=envelopes(),
        store=store,
        record_shard_bytes=record_shard_bytes,
    )
