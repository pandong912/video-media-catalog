"""Commit-last publication for replayable connector batches and record sets."""

from __future__ import annotations

import re
import sqlite3
from collections.abc import Iterable, Sequence
from contextlib import suppress
from pathlib import Path
from tempfile import TemporaryDirectory
from types import TracebackType
from typing import Self
from urllib.parse import urlsplit

from video_media_catalog.canonical import sha256_digest
from video_media_catalog.connector import (
    MAX_RECORD_SHARDS_PER_PARTITION,
    CaptureWindowReceipt,
    CaptureWindowStatus,
    ConnectorBatchManifest,
    ConnectorRecordEnvelope,
    ConnectorRecordSetManifest,
    ConnectorRecordSetPartitionManifest,
    ConnectorRecordShard,
    SourceWatermark,
    build_connector_record_set_manifest,
    build_connector_record_set_partition_manifest,
    capture_window_slot_id,
    source_watermark_from_receipt,
    validate_envelope_against_batch,
)
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.object_store import (
    BoundedObjectStore,
    RuntimeObjectStore,
    S3Location,
    UploadResult,
    conditional_publish_bytes,
)
from video_media_catalog.runtime_args import join_uri
from video_media_catalog.storage import local_path
from video_media_catalog.v2_contracts import V2ContractModel

CONTROL_OBJECT_MAX_BYTES = 16 * 1024 * 1024
DEFAULT_RECORD_SHARD_BYTES = 8 * 1024 * 1024
DEFAULT_MAX_FINAL_RECORD_OBJECTS = 8192
ENVELOPE_KEY_INDEX_CACHE_KIB = 2 * 1024
_ENVELOPE_KEY_INDEX_COMMIT_INTERVAL = 100_000
_PARTITION_LABEL_PATTERN = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")


class _EnvelopeKeyIndex:
    """Exact disk-backed uniqueness index with a fixed SQLite page cache."""

    def __init__(self) -> None:
        self._directory: TemporaryDirectory[str] | None = None
        self._database_path: Path | None = None
        self._connection: sqlite3.Connection | None = None
        self._pending = 0

    @property
    def database_path(self) -> Path:
        if self._database_path is None:
            raise RuntimeError("envelope key index is not open")
        return self._database_path

    @property
    def cache_limit_bytes(self) -> int:
        if self._connection is None:
            raise RuntimeError("envelope key index is not open")
        configured = int(self._connection.execute("PRAGMA cache_size").fetchone()[0])
        if configured < 0:
            return abs(configured) * 1024
        page_size = int(self._connection.execute("PRAGMA page_size").fetchone()[0])
        return configured * page_size

    @property
    def uses_file_temp_storage(self) -> bool:
        if self._connection is None:
            raise RuntimeError("envelope key index is not open")
        configured = self._connection.execute("PRAGMA temp_store").fetchone()[0]
        return int(configured) == 1

    @property
    def journal_mode(self) -> str:
        if self._connection is None:
            raise RuntimeError("envelope key index is not open")
        return str(self._connection.execute("PRAGMA journal_mode").fetchone()[0])

    def __enter__(self) -> Self:
        directory = TemporaryDirectory(prefix="connector-envelope-keys-")
        database_path = Path(directory.name) / "keys.sqlite3"
        connection: sqlite3.Connection | None = None
        try:
            connection = sqlite3.connect(
                database_path,
                cached_statements=1,
            )
            connection.execute(f"PRAGMA cache_size = -{ENVELOPE_KEY_INDEX_CACHE_KIB}")
            connection.execute("PRAGMA temp_store = FILE")
            # The index is disposable and a partition is committed only after a
            # clean close, so crash-recovery journaling adds I/O without safety.
            connection.execute("PRAGMA journal_mode = OFF")
            connection.execute("PRAGMA synchronous = OFF")
            connection.execute("PRAGMA locking_mode = EXCLUSIVE")
            connection.execute(
                """
                CREATE TABLE envelope_keys (
                    envelope_key TEXT PRIMARY KEY NOT NULL
                ) WITHOUT ROWID
                """
            )
        except BaseException:
            if connection is not None:
                with suppress(sqlite3.Error):
                    connection.close()
            with suppress(OSError):
                directory.cleanup()
            raise
        self._directory = directory
        self._database_path = database_path
        self._connection = connection
        return self

    def add(self, envelope_key: str) -> None:
        if self._connection is None:
            raise RuntimeError("envelope key index is not open")
        try:
            self._connection.execute(
                "INSERT INTO envelope_keys(envelope_key) VALUES (?)",
                (envelope_key,),
            )
        except sqlite3.IntegrityError:
            raise ValueError(
                "connector output contains duplicate envelope keys"
            ) from None
        self._pending += 1
        if self._pending >= _ENVELOPE_KEY_INDEX_COMMIT_INTERVAL:
            self._connection.commit()
            self._pending = 0

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        connection = self._connection
        directory = self._directory
        self._connection = None
        self._directory = None
        self._database_path = None
        try:
            if connection is not None:
                if exc_type is None:
                    try:
                        connection.commit()
                    finally:
                        connection.close()
                else:
                    with suppress(sqlite3.Error):
                        connection.rollback()
                    with suppress(sqlite3.Error):
                        connection.close()
        finally:
            if directory is not None:
                if exc_type is None:
                    directory.cleanup()
                else:
                    with suppress(OSError):
                        directory.cleanup()


class PublishedConnectorCapture(V2ContractModel):
    batch_manifest: ConnectorBatchManifest
    batch_manifest_object: ObjectRef
    record_set_manifest: ConnectorRecordSetManifest
    record_set_manifest_object: ObjectRef


class PublishedConnectorPartition(V2ContractModel):
    """One committed, exactly de-duplicated connector record partition."""

    partition_label: str
    key_namespace: str
    manifest: ConnectorRecordSetPartitionManifest
    manifest_object: ObjectRef


class ConnectorPartitionCheckpoint(V2ContractModel):
    """Namespace-bound partition receipt used for retry recovery."""

    schema_version: str = "1.0"
    partition_label: str
    key_namespace: str
    manifest: ConnectorRecordSetPartitionManifest


def _connector_batch_prefix(
    destination_prefix: str,
    batch: ConnectorBatchManifest,
) -> str:
    return join_uri(
        destination_prefix,
        batch.source_system_id,
        "batches",
        batch.batch_id.removeprefix("sha256:"),
    )


def connector_partition_manifest_uri(
    *,
    destination_prefix: str,
    batch: ConnectorBatchManifest,
    partition_label: str,
    partition_index: int,
) -> str:
    partition_path = f"partition={partition_index:05d}-{partition_label}"
    return join_uri(
        _connector_batch_prefix(destination_prefix, batch),
        "record-set",
        "partitions",
        f"{partition_path}.json",
    )


def publish_connector_batch_manifest(
    *,
    destination_prefix: str,
    batch: ConnectorBatchManifest,
    store: RuntimeObjectStore,
) -> ObjectRef:
    """Publish the immutable batch declaration before any normalized records."""

    return store.upload_bytes(
        batch.json_bytes(),
        join_uri(
            _connector_batch_prefix(destination_prefix, batch),
            "batch-manifest.json",
        ),
        media_type="application/vnd.video-media-catalog.connector-batch.v2+json",
        object_format="OBJECT_FORMAT_JSON",
        max_bytes=CONTROL_OBJECT_MAX_BYTES,
    ).object_ref


def _publish_envelope_shards(
    *,
    shard_prefix: str,
    batch: ConnectorBatchManifest,
    envelopes: Iterable[ConnectorRecordEnvelope],
    store: RuntimeObjectStore,
    record_shard_bytes: int,
    expected_record_count: int,
    max_shards: int | None,
    key_namespace: str | None,
) -> tuple[ConnectorRecordShard, ...]:
    if record_shard_bytes < 1:
        raise ValueError("record_shard_bytes must be positive")
    if expected_record_count < 0:
        raise ValueError("expected_record_count must be non-negative")
    if max_shards is not None and max_shards < 1:
        raise ValueError("max_shards must be positive")

    shard = bytearray()
    shard_index = 0
    shard_record_count = 0
    shard_first_key: str | None = None
    shard_last_key: str | None = None
    shards: list[ConnectorRecordShard] = []
    record_count = 0

    def flush_shard() -> None:
        nonlocal shard
        nonlocal shard_index
        nonlocal shard_record_count
        nonlocal shard_first_key
        nonlocal shard_last_key
        if not shard:
            return
        if max_shards is not None and len(shards) >= max_shards:
            raise ValueError("connector partition exceeds configured shard limit")
        assert shard_first_key is not None
        assert shard_last_key is not None
        payload = bytes(shard)
        digest = sha256_digest(payload)
        reference = store.upload_bytes(
            payload,
            join_uri(
                shard_prefix,
                f"shard={shard_index:05d}",
                f"{digest.removeprefix('sha256:')}.ndjson",
            ),
            media_type=(
                "application/vnd.video-media-catalog."
                "connector-record-envelope.v2+ndjson"
            ),
            object_format="OBJECT_FORMAT_OTHER",
            max_bytes=record_shard_bytes,
        ).object_ref
        shards.append(
            ConnectorRecordShard(
                shard_index=shard_index,
                object_ref=reference,
                record_count=shard_record_count,
                first_envelope_key=shard_first_key,
                last_envelope_key=shard_last_key,
            )
        )
        shard = bytearray()
        shard_index += 1
        shard_record_count = 0
        shard_first_key = None
        shard_last_key = None

    with _EnvelopeKeyIndex() as envelope_keys:
        for envelope in envelopes:
            validate_envelope_against_batch(batch, envelope)
            if key_namespace is not None and not envelope.source_record_id.startswith(
                key_namespace
            ):
                raise ValueError(
                    "connector envelope is outside its partition key namespace"
                )
            envelope_keys.add(envelope.envelope_key)
            line = envelope.json_bytes()
            if len(line) > record_shard_bytes:
                raise ValueError("one connector record exceeds shard byte limit")
            if shard and len(shard) + len(line) > record_shard_bytes:
                flush_shard()
            shard.extend(line)
            shard_first_key = shard_first_key or envelope.envelope_key
            shard_last_key = envelope.envelope_key
            shard_record_count += 1
            record_count += 1
    flush_shard()

    if record_count != expected_record_count:
        raise ValueError("record envelope count does not match batch declaration")
    return tuple(shards)


def publish_connector_partition(
    *,
    destination_prefix: str,
    batch: ConnectorBatchManifest,
    partition_label: str,
    key_namespace: str,
    partition_index: int,
    envelopes: Iterable[ConnectorRecordEnvelope],
    expected_record_count: int,
    store: RuntimeObjectStore,
    record_shard_bytes: int = DEFAULT_RECORD_SHARD_BYTES,
) -> PublishedConnectorPartition:
    """Publish one independently replayable partition without finalizing the batch."""

    if expected_record_count < 1:
        raise ValueError("connector partition must contain records")
    if _PARTITION_LABEL_PATTERN.fullmatch(partition_label) is None:
        raise ValueError("partition_label must be a lowercase DNS label")
    if (
        not key_namespace
        or len(key_namespace) > 256
        or any(character.isspace() for character in key_namespace)
    ):
        raise ValueError("key_namespace must be non-empty and contain no whitespace")
    if partition_index < 0:
        raise ValueError("partition_index must be non-negative")

    batch_prefix = _connector_batch_prefix(destination_prefix, batch)
    partition_path = f"partition={partition_index:05d}-{partition_label}"
    shards = _publish_envelope_shards(
        shard_prefix=join_uri(batch_prefix, "records", partition_path),
        batch=batch,
        envelopes=envelopes,
        store=store,
        record_shard_bytes=record_shard_bytes,
        expected_record_count=expected_record_count,
        max_shards=MAX_RECORD_SHARDS_PER_PARTITION,
        key_namespace=key_namespace,
    )
    manifest = build_connector_record_set_partition_manifest(
        batch_id=batch.batch_id,
        source_product_id=batch.source_product_id,
        policy_id=batch.policy_id,
        policy_digest=batch.policy_digest,
        epoch_index=0,
        partition_index=partition_index,
        shards=shards,
        shard_count=len(shards),
        record_count=expected_record_count,
        size_bytes=sum(item.object_ref.size_bytes for item in shards),
        first_envelope_key=shards[0].first_envelope_key,
        last_envelope_key=shards[-1].last_envelope_key,
        created_at=batch.acquired_at,
    )
    checkpoint = ConnectorPartitionCheckpoint(
        partition_label=partition_label,
        key_namespace=key_namespace,
        manifest=manifest,
    )
    manifest_object = store.upload_bytes(
        checkpoint.json_bytes(),
        connector_partition_manifest_uri(
            destination_prefix=destination_prefix,
            batch=batch,
            partition_label=partition_label,
            partition_index=partition_index,
        ),
        media_type=(
            "application/vnd.video-media-catalog.connector-partition-checkpoint.v1+json"
        ),
        object_format="OBJECT_FORMAT_JSON",
        max_bytes=CONTROL_OBJECT_MAX_BYTES,
    ).object_ref
    return PublishedConnectorPartition(
        partition_label=partition_label,
        key_namespace=key_namespace,
        manifest=manifest,
        manifest_object=manifest_object,
    )


def _missing_object(exc: BaseException) -> bool:
    response = getattr(exc, "response", {})
    code = str(response.get("Error", {}).get("Code", ""))
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return code in {"404", "NoSuchKey", "NotFound"} or status == 404


def _read_partition_manifest(
    *,
    uri: str,
    store: BoundedObjectStore,
) -> tuple[bytes, ObjectRef] | None:
    if urlsplit(uri).scheme == "file":
        path = local_path(uri)
        if not path.exists():
            return None
        size = path.stat().st_size
        if not 0 < size <= CONTROL_OBJECT_MAX_BYTES:
            raise ValueError("connector partition manifest exceeds size limit")
        payload = path.read_bytes()
        if len(payload) != size:
            raise ValueError("connector partition manifest size changed while reading")
        return payload, ObjectRef(
            uri=uri,
            format="OBJECT_FORMAT_JSON",
            media_type=(
                "application/vnd.video-media-catalog."
                "connector-partition-checkpoint.v1+json"
            ),
            checksum=Checksum(value=sha256_digest(payload).removeprefix("sha256:")),
            size_bytes=len(payload),
        )

    location = S3Location.parse(uri)
    try:
        response = store.client.get_object(
            Bucket=location.bucket,
            Key=location.key,
            ChecksumMode="ENABLED",
        )
    except Exception as exc:
        if _missing_object(exc):
            return None
        raise
    body = response.get("Body")
    if body is None or not hasattr(body, "read"):
        raise RuntimeError("connector partition manifest has no readable body")
    try:
        declared = int(response.get("ContentLength", -1))
        if not 0 < declared <= CONTROL_OBJECT_MAX_BYTES:
            raise ValueError("connector partition manifest exceeds size limit")
        payload = body.read(CONTROL_OBJECT_MAX_BYTES + 1)
    finally:
        body.close()
    if len(payload) != declared:
        raise ValueError("connector partition manifest size changed while reading")
    version = response.get("VersionId")
    etag = str(response.get("ETag", "")).strip('"')
    if not isinstance(version, str) or not version or not etag:
        raise ValueError("S3 connector partition manifest is not version-bound")
    return payload, ObjectRef(
        uri=uri,
        format="OBJECT_FORMAT_JSON",
        media_type=(
            "application/vnd.video-media-catalog.connector-partition-checkpoint.v1+json"
        ),
        checksum=Checksum(value=sha256_digest(payload).removeprefix("sha256:")),
        size_bytes=len(payload),
        etag=etag,
        object_version=version,
    )


def load_connector_partition(
    *,
    destination_prefix: str,
    batch: ConnectorBatchManifest,
    partition_label: str,
    key_namespace: str,
    partition_index: int,
    store: BoundedObjectStore,
) -> PublishedConnectorPartition | None:
    """Load and verify one completed partition for retry-safe capture resume."""

    uri = connector_partition_manifest_uri(
        destination_prefix=destination_prefix,
        batch=batch,
        partition_label=partition_label,
        partition_index=partition_index,
    )
    existing = _read_partition_manifest(uri=uri, store=store)
    if existing is None:
        return None
    payload, reference = existing
    checkpoint = ConnectorPartitionCheckpoint.model_validate_json(payload)
    if (
        checkpoint.partition_label != partition_label
        or checkpoint.key_namespace != key_namespace
    ):
        raise ValueError("existing connector partition namespace binding differs")
    manifest = checkpoint.manifest
    if (
        manifest.batch_id != batch.batch_id
        or manifest.source_product_id != batch.source_product_id
        or manifest.policy_id != batch.policy_id
        or manifest.policy_digest != batch.policy_digest
        or manifest.partition_index != partition_index
        or manifest.created_at != batch.acquired_at
    ):
        raise ValueError("existing connector partition is not bound to the batch")
    store.verify(reference, max_bytes=CONTROL_OBJECT_MAX_BYTES)
    for shard in manifest.shards:
        store.verify(shard.object_ref, max_bytes=shard.object_ref.size_bytes)
    return PublishedConnectorPartition(
        partition_label=partition_label,
        key_namespace=key_namespace,
        manifest=manifest,
        manifest_object=reference,
    )


def finalize_connector_partitions(
    *,
    destination_prefix: str,
    batch: ConnectorBatchManifest,
    batch_manifest_object: ObjectRef,
    partitions: Sequence[PublishedConnectorPartition],
    store: RuntimeObjectStore,
    max_record_objects: int = DEFAULT_MAX_FINAL_RECORD_OBJECTS,
) -> PublishedConnectorCapture:
    """Commit ordered disjoint partitions as one flat v2.0 record set."""

    if max_record_objects < 1:
        raise ValueError("max_record_objects must be positive")
    if not partitions:
        raise ValueError("connector finalization requires partitions")
    ordered = sorted(partitions, key=lambda item: item.manifest.partition_index)
    indexes = [item.manifest.partition_index for item in ordered]
    if indexes != list(range(len(ordered))):
        raise ValueError("connector partitions must have contiguous indexes")
    labels = [item.partition_label for item in ordered]
    if len(labels) != len(set(labels)):
        raise ValueError("connector partition labels must be unique")
    key_namespaces = [item.key_namespace for item in ordered]
    if len(key_namespaces) != len(set(key_namespaces)):
        raise ValueError("connector partition key namespaces must be disjoint")
    for index, namespace in enumerate(key_namespaces):
        if any(
            namespace.startswith(other) or other.startswith(namespace)
            for other in key_namespaces[index + 1 :]
        ):
            raise ValueError("connector partition key namespaces must not overlap")
    store.verify(batch_manifest_object, max_bytes=CONTROL_OBJECT_MAX_BYTES)

    record_objects: list[ObjectRef] = []
    record_count = 0
    for partition in ordered:
        manifest = partition.manifest
        if (
            manifest.batch_id != batch.batch_id
            or manifest.source_product_id != batch.source_product_id
            or manifest.policy_id != batch.policy_id
            or manifest.policy_digest != batch.policy_digest
            or manifest.created_at != batch.acquired_at
        ):
            raise ValueError("connector partition is not bound to the batch")
        store.verify(
            partition.manifest_object,
            max_bytes=CONTROL_OBJECT_MAX_BYTES,
        )
        record_objects.extend(item.object_ref for item in manifest.shards)
        record_count += manifest.record_count

    if len(record_objects) > max_record_objects:
        raise ValueError("connector record set exceeds configured object limit")
    if len({item.uri for item in record_objects}) != len(record_objects):
        raise ValueError("connector partitions contain duplicate record object URIs")
    if record_count != batch.record_count:
        raise ValueError("partition record count does not match batch declaration")

    record_set = build_connector_record_set_manifest(
        batch_id=batch.batch_id,
        source_product_id=batch.source_product_id,
        policy_id=batch.policy_id,
        policy_digest=batch.policy_digest,
        record_objects=tuple(record_objects),
        record_count=record_count,
        first_envelope_key=ordered[0].manifest.first_envelope_key,
        last_envelope_key=ordered[-1].manifest.last_envelope_key,
        created_at=batch.acquired_at,
    )
    record_set_manifest_object = store.upload_bytes(
        record_set.json_bytes(),
        join_uri(
            _connector_batch_prefix(destination_prefix, batch),
            "record-set.json",
        ),
        media_type="application/vnd.video-media-catalog.record-set.v2+json",
        object_format="OBJECT_FORMAT_JSON",
        max_bytes=CONTROL_OBJECT_MAX_BYTES,
    ).object_ref
    return PublishedConnectorCapture(
        batch_manifest=batch,
        batch_manifest_object=batch_manifest_object,
        record_set_manifest=record_set,
        record_set_manifest_object=record_set_manifest_object,
    )


def publish_connector_capture(
    *,
    destination_prefix: str,
    batch: ConnectorBatchManifest,
    envelopes: Iterable[ConnectorRecordEnvelope],
    store: RuntimeObjectStore,
    record_shard_bytes: int = DEFAULT_RECORD_SHARD_BYTES,
) -> PublishedConnectorCapture:
    """Publish a batch, bounded envelope shards, then its record-set marker."""

    batch_prefix = _connector_batch_prefix(destination_prefix, batch)
    batch_manifest_object = publish_connector_batch_manifest(
        destination_prefix=destination_prefix,
        batch=batch,
        store=store,
    )
    shards = _publish_envelope_shards(
        shard_prefix=join_uri(batch_prefix, "records"),
        batch=batch,
        envelopes=envelopes,
        store=store,
        record_shard_bytes=record_shard_bytes,
        expected_record_count=batch.record_count,
        max_shards=None,
        key_namespace=None,
    )
    record_set = build_connector_record_set_manifest(
        batch_id=batch.batch_id,
        source_product_id=batch.source_product_id,
        policy_id=batch.policy_id,
        policy_digest=batch.policy_digest,
        record_objects=tuple(item.object_ref for item in shards),
        record_count=batch.record_count,
        first_envelope_key=(shards[0].first_envelope_key if shards else None),
        last_envelope_key=(shards[-1].last_envelope_key if shards else None),
        created_at=batch.acquired_at,
    )
    record_set_manifest_object = store.upload_bytes(
        record_set.json_bytes(),
        join_uri(batch_prefix, "record-set.json"),
        media_type="application/vnd.video-media-catalog.record-set.v2+json",
        object_format="OBJECT_FORMAT_JSON",
        max_bytes=CONTROL_OBJECT_MAX_BYTES,
    ).object_ref
    return PublishedConnectorCapture(
        batch_manifest=batch,
        batch_manifest_object=batch_manifest_object,
        record_set_manifest=record_set,
        record_set_manifest_object=record_set_manifest_object,
    )


class PublishedCaptureWindowControl(V2ContractModel):
    """Immutable watermark followed by its commit-last window receipt."""

    source_watermark: SourceWatermark
    source_watermark_object: ObjectRef
    receipt: CaptureWindowReceipt
    receipt_object: ObjectRef


def publish_source_watermark(
    *,
    destination_prefix: str,
    watermark: SourceWatermark,
    store: RuntimeObjectStore,
) -> UploadResult:
    """Publish one content-addressed watermark, reusing an identical replay."""

    return conditional_publish_bytes(
        store,
        watermark.json_bytes(),
        join_uri(
            destination_prefix,
            watermark.source_product_id,
            "control",
            "watermarks",
            watermark.watermark_id.removeprefix("sha256:"),
            "watermark.json",
        ),
        media_type=("application/vnd.video-media-catalog.source-watermark.v1+json"),
        object_format="OBJECT_FORMAT_JSON",
        max_bytes=CONTROL_OBJECT_MAX_BYTES,
    )


def _publish_receipt_bytes(
    *,
    destination_prefix: str,
    receipt: CaptureWindowReceipt,
    store: RuntimeObjectStore,
) -> UploadResult:
    return conditional_publish_bytes(
        store,
        receipt.json_bytes(),
        join_uri(
            destination_prefix,
            receipt.source_product_id,
            "control",
            "capture-windows",
            capture_window_slot_id(receipt).removeprefix("sha256:"),
            "receipt.json",
        ),
        media_type=(
            "application/vnd.video-media-catalog.capture-window-receipt.v1+json"
        ),
        object_format="OBJECT_FORMAT_JSON",
        max_bytes=CONTROL_OBJECT_MAX_BYTES,
    )


def publish_capture_window_receipt(
    *,
    destination_prefix: str,
    receipt: CaptureWindowReceipt,
    store: RuntimeObjectStore,
) -> UploadResult:
    """Verify the immutable batch before publishing the final receipt marker."""

    if receipt.status == CaptureWindowStatus.FAILED:
        raise ValueError("failed capture receipt cannot be published as a commit")
    if receipt.batch_object is None:
        raise ValueError("committed capture receipt requires a batch ObjectRef")
    store.verify(
        receipt.batch_object,
        max_bytes=CONTROL_OBJECT_MAX_BYTES,
    )
    return _publish_receipt_bytes(
        destination_prefix=destination_prefix,
        receipt=receipt,
        store=store,
    )


def publish_capture_window_commit(
    *,
    destination_prefix: str,
    receipt: CaptureWindowReceipt,
    store: RuntimeObjectStore,
) -> PublishedCaptureWindowControl:
    """Publish watermark first and the verified window receipt strictly last."""

    if receipt.status == CaptureWindowStatus.FAILED:
        raise ValueError("failed capture receipt cannot advance source control")
    if receipt.batch_object is None:
        raise ValueError("committed capture receipt requires a batch ObjectRef")
    store.verify(
        receipt.batch_object,
        max_bytes=CONTROL_OBJECT_MAX_BYTES,
    )
    watermark = source_watermark_from_receipt(receipt)
    watermark_result = publish_source_watermark(
        destination_prefix=destination_prefix,
        watermark=watermark,
        store=store,
    )
    receipt_result = _publish_receipt_bytes(
        destination_prefix=destination_prefix,
        receipt=receipt,
        store=store,
    )
    return PublishedCaptureWindowControl(
        source_watermark=watermark,
        source_watermark_object=watermark_result.object_ref,
        receipt=receipt,
        receipt_object=receipt_result.object_ref,
    )
