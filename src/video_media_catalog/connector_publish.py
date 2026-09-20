"""Commit-last publication for replayable connector batches and record sets."""

from __future__ import annotations

from collections.abc import Iterable

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
    seen_envelope_keys: set[str] = set()

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

    for envelope in envelopes:
        validate_envelope_against_batch(batch, envelope)
        if envelope.envelope_key in seen_envelope_keys:
            raise ValueError("connector output contains duplicate envelope keys")
        seen_envelope_keys.add(envelope.envelope_key)
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
