"""Immutable source pins and checkpoints for research-domain connectors."""

from __future__ import annotations

import re
from collections.abc import Callable
from enum import StrEnum
from typing import Any, Self

from pydantic import ValidationInfo, field_validator, model_validator

from video_media_catalog.canonical import deterministic_key
from video_media_catalog.connector import (
    ChangeSemantics,
    Completeness,
    ConnectorBatchManifest,
    ConnectorRecordEnvelope,
    DeleteCoverage,
    RecordOperation,
    Serialization,
    TransportKind,
    build_connector_batch_manifest,
    build_connector_record_envelope,
)
from video_media_catalog.models import ObjectRef
from video_media_catalog.v2_contracts import (
    V2ContractModel,
    digest_identity,
    require_rfc3339,
    require_sha256,
    require_slug,
)

_ZERO_DIGEST = "sha256:" + ("0" * 64)
_GIT_COMMIT = re.compile(r"^[0-9a-f]{40}(?:[0-9a-f]{24})?$")


class SnapshotRevisionKind(StrEnum):
    GIT_COMMIT = "GIT_COMMIT"
    DUMP_OBJECT = "DUMP_OBJECT"
    API_CHECKPOINT = "API_CHECKPOINT"


class SourceSnapshotPin(V2ContractModel):
    pin_id: str
    source_system_id: str
    source_product_id: str
    connector_id: str
    revision_kind: SnapshotRevisionKind
    source_revision: str
    schema_id: str
    raw_objects: tuple[ObjectRef, ...]
    captured_at: str

    @field_validator("pin_id")
    @classmethod
    def validate_pin_id(cls, value: str) -> str:
        return require_sha256(value, label="source snapshot pin")

    @field_validator(
        "source_system_id",
        "source_product_id",
        "connector_id",
        "schema_id",
    )
    @classmethod
    def validate_ids(cls, value: str) -> str:
        return require_slug(value, label="source snapshot reference")

    @field_validator("captured_at")
    @classmethod
    def validate_captured_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @field_validator("source_revision")
    @classmethod
    def validate_source_revision(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized or len(normalized) > 1024:
            raise ValueError("snapshot source revision must be non-empty and bounded")
        return normalized

    @field_validator("raw_objects")
    @classmethod
    def validate_raw_objects(
        cls,
        value: tuple[ObjectRef, ...],
    ) -> tuple[ObjectRef, ...]:
        if not value:
            raise ValueError("source snapshot pin requires immutable raw objects")
        uris = tuple(item.uri for item in value)
        if len(uris) != len(set(uris)):
            raise ValueError("source snapshot pin contains duplicate object URIs")
        return tuple(sorted(value, key=lambda item: item.uri))

    @model_validator(mode="after")
    def validate_identity(self, info: ValidationInfo) -> Self:
        if (
            self.revision_kind == SnapshotRevisionKind.GIT_COMMIT
            and _GIT_COMMIT.fullmatch(self.source_revision) is None
        ):
            raise ValueError("Git snapshots require a full 40 or 64 hex commit")
        if (info.context or {}).get("skip_identity"):
            return self
        expected = deterministic_key(
            "research-source-snapshot-pin-v1",
            _pin_identity(self),
        )
        if self.pin_id != expected:
            raise ValueError("pin_id does not match immutable source snapshot")
        return self


def _pin_identity(pin: SourceSnapshotPin) -> dict[str, Any]:
    return {
        "sourceSystemId": pin.source_system_id,
        "sourceProductId": pin.source_product_id,
        "connectorId": pin.connector_id,
        "revisionKind": pin.revision_kind.value,
        "sourceRevision": pin.source_revision,
        "schemaId": pin.schema_id,
        "rawObjects": tuple(
            item.model_dump(mode="json", by_alias=True, exclude_none=True)
            for item in pin.raw_objects
        ),
        "capturedAt": pin.captured_at,
    }


def build_source_snapshot_pin(**values: Any) -> SourceSnapshotPin:
    provisional = SourceSnapshotPin.model_validate(
        {"pin_id": _ZERO_DIGEST, **values},
        context={"skip_identity": True},
    )
    normalized = provisional.model_dump(mode="python")
    normalized["pin_id"] = deterministic_key(
        "research-source-snapshot-pin-v1",
        _pin_identity(provisional),
    )
    return SourceSnapshotPin.model_validate(normalized)


class SnapshotCheckpoint(V2ContractModel):
    checkpoint_id: str
    pin_id: str
    source_product_id: str
    source_revision: str
    record_count: int
    last_record_id: str | None = None
    records_digest: str
    completed_at: str

    @field_validator("checkpoint_id", "pin_id", "records_digest")
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("source_product_id")
    @classmethod
    def validate_product(cls, value: str) -> str:
        return require_slug(value, label="snapshot checkpoint product")

    @field_validator("completed_at")
    @classmethod
    def validate_completed_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @model_validator(mode="after")
    def validate_checkpoint(self, info: ValidationInfo) -> Self:
        if self.record_count < 0:
            raise ValueError("snapshot checkpoint record_count cannot be negative")
        if (self.record_count == 0) != (self.last_record_id is None):
            raise ValueError("snapshot checkpoint last record must match record count")
        if (info.context or {}).get("skip_identity"):
            return self
        expected = deterministic_key(
            "research-snapshot-checkpoint-v1",
            _checkpoint_identity(self),
        )
        if self.checkpoint_id != expected:
            raise ValueError("checkpoint_id does not match snapshot checkpoint")
        return self


def _checkpoint_identity(checkpoint: SnapshotCheckpoint) -> dict[str, Any]:
    return {
        "pinId": checkpoint.pin_id,
        "sourceProductId": checkpoint.source_product_id,
        "sourceRevision": checkpoint.source_revision,
        "recordCount": checkpoint.record_count,
        "lastRecordId": checkpoint.last_record_id,
        "recordsDigest": checkpoint.records_digest,
        "completedAt": checkpoint.completed_at,
    }


def build_snapshot_checkpoint(**values: Any) -> SnapshotCheckpoint:
    provisional = SnapshotCheckpoint.model_validate(
        {"checkpoint_id": _ZERO_DIGEST, **values},
        context={"skip_identity": True},
    )
    normalized = provisional.model_dump(mode="python")
    normalized["checkpoint_id"] = deterministic_key(
        "research-snapshot-checkpoint-v1",
        _checkpoint_identity(provisional),
    )
    return SnapshotCheckpoint.model_validate(normalized)


class CapturedSnapshot(V2ContractModel):
    pin: SourceSnapshotPin
    batch_manifest: ConnectorBatchManifest
    envelopes: tuple[ConnectorRecordEnvelope, ...]
    checkpoint: SnapshotCheckpoint


def capture_pinned_records(
    *,
    pin: SourceSnapshotPin,
    records: tuple[dict[str, Any], ...],
    source_namespace: Callable[[dict[str, Any]], str],
    source_record_id: Callable[[dict[str, Any]], str],
    policy_id: str,
    policy_digest: str,
    image_digest: str,
    config_digest: str,
    connector_version: str = "1.0.0",
    serialization: Serialization = Serialization.JSON,
) -> CapturedSnapshot:
    """Normalize one immutable source pin without reading mutable source state."""

    ordered = tuple(sorted(records, key=lambda item: source_record_id(item)))
    record_ids = tuple(source_record_id(item) for item in ordered)
    if len(record_ids) != len(set(record_ids)):
        raise ValueError("pinned snapshot contains duplicate source record IDs")
    scope = {
        "pinId": pin.pin_id,
        "sourceRevision": pin.source_revision,
        "schemaId": pin.schema_id,
    }
    batch = build_connector_batch_manifest(
        source_system_id=pin.source_system_id,
        source_product_id=pin.source_product_id,
        connector_id=pin.connector_id,
        connector_version=connector_version,
        image_digest=image_digest,
        config_digest=config_digest,
        policy_id=policy_id,
        policy_digest=policy_digest,
        transport_kind=TransportKind.DUMP,
        serialization=serialization,
        change_semantics=ChangeSemantics.FULL_SNAPSHOT,
        completeness=Completeness.COMPLETE,
        delete_coverage=DeleteCoverage.SNAPSHOT_DIFF,
        coverage_scope=scope,
        watermark_after=pin.source_revision,
        raw_objects=pin.raw_objects,
        acquired_at=pin.captured_at,
        record_count=len(ordered),
        error_count=0,
    )
    raw_object = pin.raw_objects[0]
    envelopes = tuple(
        build_connector_record_envelope(
            payload=record,
            batch_id=batch.batch_id,
            source_system_id=pin.source_system_id,
            source_product_id=pin.source_product_id,
            source_namespace_id=source_namespace(record),
            source_record_id=record_id,
            source_revision=pin.source_revision,
            operation=RecordOperation.UPSERT,
            observed_at=pin.captured_at,
            ingested_at=pin.captured_at,
            payload_schema=pin.schema_id,
            raw_object=raw_object,
            source_location=f"/records/{index}",
            policy_id=policy_id,
            policy_digest=policy_digest,
        )
        for index, (record, record_id) in enumerate(
            zip(ordered, record_ids, strict=True)
        )
    )
    records_digest = digest_identity(
        {
            "pinId": pin.pin_id,
            "envelopeKeys": tuple(item.envelope_key for item in envelopes),
        }
    )
    checkpoint = build_snapshot_checkpoint(
        pin_id=pin.pin_id,
        source_product_id=pin.source_product_id,
        source_revision=pin.source_revision,
        record_count=len(envelopes),
        last_record_id=record_ids[-1] if record_ids else None,
        records_digest=records_digest,
        completed_at=pin.captured_at,
    )
    return CapturedSnapshot(
        pin=pin,
        batch_manifest=batch,
        envelopes=envelopes,
        checkpoint=checkpoint,
    )
