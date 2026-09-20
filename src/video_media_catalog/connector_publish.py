"""Commit-last publication for replayable connector batches and record sets."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from contextlib import suppress
from pathlib import Path
from tempfile import TemporaryDirectory
from types import TracebackType
from typing import Self

from video_media_catalog.canonical import sha256_digest
from video_media_catalog.connector import (
    ConnectorBatchManifest,
    ConnectorRecordEnvelope,
    ConnectorRecordSetManifest,
    build_connector_record_set_manifest,
    validate_envelope_against_batch,
)
from video_media_catalog.models import ObjectRef
from video_media_catalog.object_store import RuntimeObjectStore
from video_media_catalog.runtime_args import join_uri
from video_media_catalog.v2_contracts import V2ContractModel

CONTROL_OBJECT_MAX_BYTES = 16 * 1024 * 1024
DEFAULT_RECORD_SHARD_BYTES = 8 * 1024 * 1024
ENVELOPE_KEY_INDEX_CACHE_KIB = 2 * 1024
_ENVELOPE_KEY_INDEX_COMMIT_INTERVAL = 10_000


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
            connection.execute("PRAGMA journal_mode = DELETE")
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


def publish_connector_capture(
    *,
    destination_prefix: str,
    batch: ConnectorBatchManifest,
    envelopes: Iterable[ConnectorRecordEnvelope],
    store: RuntimeObjectStore,
    record_shard_bytes: int = DEFAULT_RECORD_SHARD_BYTES,
) -> PublishedConnectorCapture:
    """Publish a batch, bounded envelope shards, then its record-set marker."""

    if record_shard_bytes < 1:
        raise ValueError("record_shard_bytes must be positive")
    batch_prefix = join_uri(
        destination_prefix,
        batch.source_system_id,
        "batches",
        batch.batch_id.removeprefix("sha256:"),
    )
    batch_manifest_object = store.upload_bytes(
        batch.json_bytes(),
        join_uri(batch_prefix, "batch-manifest.json"),
        media_type="application/vnd.video-media-catalog.connector-batch.v2+json",
        object_format="OBJECT_FORMAT_JSON",
        max_bytes=CONTROL_OBJECT_MAX_BYTES,
    ).object_ref

    shard = bytearray()
    shard_index = 0
    record_objects: list[ObjectRef] = []
    first_key: str | None = None
    last_key: str | None = None
    record_count = 0

    def flush_shard() -> None:
        nonlocal shard, shard_index
        if not shard:
            return
        payload = bytes(shard)
        digest = sha256_digest(payload)
        record_objects.append(
            store.upload_bytes(
                payload,
                join_uri(
                    batch_prefix,
                    "records",
                    f"shard={shard_index:05d}",
                    f"{digest}.ndjson",
                ),
                media_type=(
                    "application/vnd.video-media-catalog."
                    "connector-record-envelope.v2+ndjson"
                ),
                object_format="OBJECT_FORMAT_OTHER",
                max_bytes=record_shard_bytes,
            ).object_ref
        )
        shard = bytearray()
        shard_index += 1

    with _EnvelopeKeyIndex() as envelope_keys:
        for envelope in envelopes:
            validate_envelope_against_batch(batch, envelope)
            envelope_keys.add(envelope.envelope_key)
            line = envelope.json_bytes()
            if len(line) > record_shard_bytes:
                raise ValueError("one connector record exceeds shard byte limit")
            if shard and len(shard) + len(line) > record_shard_bytes:
                flush_shard()
            shard.extend(line)
            first_key = first_key or envelope.envelope_key
            last_key = envelope.envelope_key
            record_count += 1
    flush_shard()

    if record_count != batch.record_count:
        raise ValueError("record envelope count does not match batch manifest")
    record_set = build_connector_record_set_manifest(
        batch_id=batch.batch_id,
        source_product_id=batch.source_product_id,
        policy_id=batch.policy_id,
        policy_digest=batch.policy_digest,
        record_objects=tuple(record_objects),
        record_count=record_count,
        first_envelope_key=first_key,
        last_envelope_key=last_key,
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
