"""Supplier-neutral connector batch and record-envelope contracts."""

from __future__ import annotations

import json
from collections.abc import Iterable
from enum import StrEnum
from typing import Any, Protocol, Self
from urllib.parse import urlsplit

from pydantic import Field, ValidationInfo, field_validator, model_validator

from video_media_catalog.canonical import (
    canonical_json,
    deterministic_key,
    source_hash,
)
from video_media_catalog.models import ObjectRef
from video_media_catalog.v2_contracts import (
    V2ContractModel,
    digest_identity,
    parse_rfc3339,
    require_rfc3339,
    require_sha256,
    require_slug,
)


class TransportKind(StrEnum):
    DUMP = "DUMP"
    API = "API"
    FEED = "FEED"


class Serialization(StrEnum):
    JSON = "JSON"
    JSON_LINES = "JSON_LINES"
    XML = "XML"
    TSV = "TSV"
    PARQUET = "PARQUET"
    RDF_TURTLE = "RDF_TURTLE"
    RDF_NQUADS = "RDF_NQUADS"


class ChangeSemantics(StrEnum):
    FULL_SNAPSHOT = "FULL_SNAPSHOT"
    DELTA = "DELTA"
    LEASED_SNAPSHOT = "LEASED_SNAPSHOT"
    LEASED_DELTA = "LEASED_DELTA"


class Completeness(StrEnum):
    COMPLETE = "COMPLETE"
    PARTIAL = "PARTIAL"


class DeleteCoverage(StrEnum):
    EXPLICIT = "EXPLICIT"
    SNAPSHOT_DIFF = "SNAPSHOT_DIFF"
    NONE = "NONE"


class RecordOperation(StrEnum):
    UPSERT = "UPSERT"
    DELETE = "DELETE"
    RETRACT = "RETRACT"
    EXPIRE = "EXPIRE"
    INFERRED_ABSENCE = "INFERRED_ABSENCE"


_ZERO_DIGEST = "sha256:" + ("0" * 64)


def _validate_immutable_ref(reference: ObjectRef, *, label: str) -> None:
    scheme = urlsplit(reference.uri).scheme
    if scheme not in {"file", "s3"}:
        raise ValueError(f"{label} must use file:// or s3://")
    if scheme == "s3" and (reference.etag is None or reference.object_version is None):
        raise ValueError(f"{label} S3 ObjectRef requires ETag and object version")
    if reference.size_bytes <= 0:
        raise ValueError(f"{label} must be non-empty")


class SourceWindow(V2ContractModel):
    start: str
    end: str

    @field_validator("start", "end")
    @classmethod
    def validate_timestamp(cls, value: str) -> str:
        return require_rfc3339(value)

    @model_validator(mode="after")
    def validate_order(self) -> Self:
        if parse_rfc3339(self.end) < parse_rfc3339(self.start):
            raise ValueError("source window end must not precede start")
        return self


class ConnectorBatchManifest(V2ContractModel):
    schema_version: str = "2.0"
    batch_id: str
    source_system_id: str
    source_product_id: str
    connector_id: str
    connector_version: str
    image_digest: str
    config_digest: str
    policy_id: str
    policy_digest: str
    transport_kind: TransportKind
    serialization: Serialization
    change_semantics: ChangeSemantics
    completeness: Completeness
    delete_coverage: DeleteCoverage
    coverage_scope: dict[str, Any]
    coverage_scope_digest: str
    source_window: SourceWindow | None = None
    watermark_before: str | None = None
    watermark_after: str | None = None
    raw_objects: tuple[ObjectRef, ...]
    acquired_at: str
    replayable_until: str | None = None
    record_count: int = Field(ge=0)
    error_count: int = Field(ge=0)
    retry_count: int = Field(default=0, ge=0)
    rate_limit_count: int = Field(default=0, ge=0)

    @field_validator("batch_id", "image_digest", "config_digest", "policy_digest")
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator(
        "source_system_id",
        "source_product_id",
        "connector_id",
        "policy_id",
    )
    @classmethod
    def validate_id(cls, value: str) -> str:
        return require_slug(value, label="connector reference")

    @field_validator("connector_version")
    @classmethod
    def validate_connector_version(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized or len(normalized) > 64:
            raise ValueError("connector_version must be non-empty")
        return normalized

    @field_validator("coverage_scope_digest")
    @classmethod
    def validate_coverage_digest(cls, value: str) -> str:
        return require_sha256(value, label="coverage_scope_digest")

    @field_validator("acquired_at", "replayable_until")
    @classmethod
    def validate_timestamps(cls, value: str | None) -> str | None:
        return None if value is None else require_rfc3339(value)

    @field_validator("watermark_before", "watermark_after")
    @classmethod
    def normalize_watermark(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized or len(normalized) > 1024:
            raise ValueError("watermark must be non-empty and bounded")
        return normalized

    @model_validator(mode="after")
    def validate_manifest(self, info: ValidationInfo) -> Self:
        if not self.coverage_scope:
            raise ValueError("coverage_scope must not be empty")
        expected_scope = digest_identity(self.coverage_scope)
        if self.coverage_scope_digest != expected_scope:
            raise ValueError("coverage_scope_digest does not bind coverage_scope")
        if not self.raw_objects:
            raise ValueError("connector batch requires raw_objects")
        uris = [item.uri for item in self.raw_objects]
        if len(uris) != len(set(uris)):
            raise ValueError("connector batch contains duplicate raw object URIs")
        for item in self.raw_objects:
            _validate_immutable_ref(item, label="raw connector object")
        leased = self.change_semantics in {
            ChangeSemantics.LEASED_SNAPSHOT,
            ChangeSemantics.LEASED_DELTA,
        }
        if leased != (self.replayable_until is not None):
            raise ValueError(
                "leased change semantics require exactly one replayable_until"
            )
        if self.replayable_until is not None and parse_rfc3339(
            self.replayable_until
        ) <= parse_rfc3339(self.acquired_at):
            raise ValueError("replayable_until must follow acquired_at")
        if self.delete_coverage == DeleteCoverage.SNAPSHOT_DIFF and (
            self.change_semantics != ChangeSemantics.FULL_SNAPSHOT
            or self.completeness != Completeness.COMPLETE
        ):
            raise ValueError("snapshot-diff deletion requires a complete full snapshot")
        if not (info.context or {}).get("skip_identity") and (
            self.batch_id
            != deterministic_key("connector-batch-v2", _batch_identity(self))
        ):
            raise ValueError("batch_id does not match immutable manifest identity")
        return self


def _object_identity(reference: ObjectRef) -> dict[str, Any]:
    return reference.model_dump(mode="json", by_alias=True, exclude_none=True)


def _batch_identity(manifest: ConnectorBatchManifest) -> dict[str, Any]:
    return {
        "schemaVersion": manifest.schema_version,
        "sourceSystemId": manifest.source_system_id,
        "sourceProductId": manifest.source_product_id,
        "connectorId": manifest.connector_id,
        "connectorVersion": manifest.connector_version,
        "imageDigest": manifest.image_digest,
        "configDigest": manifest.config_digest,
        "policyId": manifest.policy_id,
        "policyDigest": manifest.policy_digest,
        "transportKind": manifest.transport_kind.value,
        "serialization": manifest.serialization.value,
        "changeSemantics": manifest.change_semantics.value,
        "completeness": manifest.completeness.value,
        "deleteCoverage": manifest.delete_coverage.value,
        "coverageScope": manifest.coverage_scope,
        "coverageScopeDigest": manifest.coverage_scope_digest,
        "sourceWindow": (
            None
            if manifest.source_window is None
            else manifest.source_window.model_dump(
                mode="json", by_alias=True, exclude_none=True
            )
        ),
        "watermarkBefore": manifest.watermark_before,
        "watermarkAfter": manifest.watermark_after,
        "rawObjects": [_object_identity(item) for item in manifest.raw_objects],
        "acquiredAt": manifest.acquired_at,
        "replayableUntil": manifest.replayable_until,
        "recordCount": manifest.record_count,
        "errorCount": manifest.error_count,
        "retryCount": manifest.retry_count,
        "rateLimitCount": manifest.rate_limit_count,
    }


def build_connector_batch_manifest(**values: Any) -> ConnectorBatchManifest:
    """Build and verify a deterministic connector batch manifest."""

    values = dict(values)
    scope = values.get("coverage_scope")
    if not isinstance(scope, dict) or not scope:
        raise ValueError("coverage_scope must be a non-empty object")
    values["coverage_scope_digest"] = digest_identity(scope)
    provisional = ConnectorBatchManifest.model_validate(
        {**values, "batch_id": _ZERO_DIGEST},
        context={"skip_identity": True},
    )
    normalized = provisional.model_dump(mode="python")
    normalized["batch_id"] = deterministic_key(
        "connector-batch-v2", _batch_identity(provisional)
    )
    return ConnectorBatchManifest.model_validate(normalized)


class ConnectorRecordEnvelope(V2ContractModel):
    schema_version: str = "2.0"
    envelope_key: str
    batch_id: str
    source_system_id: str
    source_product_id: str
    source_namespace_id: str
    source_record_id: str
    source_revision: str | None = None
    operation: RecordOperation
    source_modified_at: str | None = None
    observed_at: str
    ingested_at: str
    valid_from: str | None = None
    valid_to: str | None = None
    expires_at: str | None = None
    payload_schema: str
    source_hash: str
    payload_json: str | None = None
    payload_object: ObjectRef | None = None
    raw_object: ObjectRef
    source_location: str
    policy_id: str
    policy_digest: str
    citation_keys: tuple[str, ...] = ()

    @field_validator("envelope_key", "batch_id", "source_hash", "policy_digest")
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator(
        "source_system_id",
        "source_product_id",
        "source_namespace_id",
        "policy_id",
    )
    @classmethod
    def validate_id(cls, value: str) -> str:
        return require_slug(value, label="record envelope reference")

    @field_validator(
        "source_modified_at",
        "observed_at",
        "ingested_at",
        "valid_from",
        "valid_to",
        "expires_at",
    )
    @classmethod
    def validate_timestamp(cls, value: str | None) -> str | None:
        return None if value is None else require_rfc3339(value)

    @field_validator("source_record_id", "payload_schema", "source_location")
    @classmethod
    def validate_required_text(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized or len(normalized) > 2048:
            raise ValueError("record envelope text fields must be non-empty")
        return normalized

    @field_validator("source_revision")
    @classmethod
    def normalize_revision(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized or len(normalized) > 1024:
            raise ValueError("source_revision must be non-empty and bounded")
        return normalized

    @field_validator("citation_keys")
    @classmethod
    def normalize_citation_keys(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(
            sorted({require_sha256(item, label="citation key") for item in value})
        )

    @model_validator(mode="after")
    def validate_envelope(self, info: ValidationInfo) -> Self:
        has_inline = self.payload_json is not None
        has_object = self.payload_object is not None
        if self.operation == RecordOperation.UPSERT and has_inline == has_object:
            raise ValueError("UPSERT requires exactly one payload representation")
        if self.operation != RecordOperation.UPSERT and has_inline and has_object:
            raise ValueError("non-UPSERT envelope cannot contain two payloads")
        _validate_immutable_ref(self.raw_object, label="raw record object")
        if self.payload_object is not None:
            _validate_immutable_ref(
                self.payload_object,
                label="record payload object",
            )
        if self.payload_json is not None:
            try:
                parsed = json.loads(self.payload_json)
            except json.JSONDecodeError as exc:
                raise ValueError("payload_json must be valid JSON") from exc
            if canonical_json(parsed) != self.payload_json:
                raise ValueError("payload_json must use canonical JSON")
            if source_hash(parsed) != self.source_hash:
                raise ValueError("source_hash does not bind payload_json")
        if (
            self.valid_from
            and self.valid_to
            and parse_rfc3339(self.valid_to) < parse_rfc3339(self.valid_from)
        ):
            raise ValueError("valid_to must not precede valid_from")
        if self.expires_at and parse_rfc3339(self.expires_at) <= parse_rfc3339(
            self.observed_at
        ):
            raise ValueError("expires_at must follow observed_at")
        if not (info.context or {}).get("skip_identity"):
            expected = deterministic_key(
                "connector-record-envelope-v2", _envelope_identity(self)
            )
            if self.envelope_key != expected:
                raise ValueError("envelope_key does not match record identity")
        return self


def _envelope_identity(envelope: ConnectorRecordEnvelope) -> dict[str, Any]:
    return {
        "batchId": envelope.batch_id,
        "sourceSystemId": envelope.source_system_id,
        "sourceProductId": envelope.source_product_id,
        "sourceNamespaceId": envelope.source_namespace_id,
        "sourceRecordId": envelope.source_record_id,
        "sourceRevision": envelope.source_revision,
        "operation": envelope.operation.value,
        "sourceHash": envelope.source_hash,
        "payloadSchema": envelope.payload_schema,
        "policyId": envelope.policy_id,
        "policyDigest": envelope.policy_digest,
    }


def build_connector_record_envelope(
    *,
    payload: Any | None = None,
    **values: Any,
) -> ConnectorRecordEnvelope:
    """Build a deterministic record envelope from a captured source record."""

    values = dict(values)
    if payload is not None:
        values["payload_json"] = canonical_json(payload)
        values["source_hash"] = source_hash(payload)
    elif "source_hash" not in values:
        values["source_hash"] = digest_identity(
            {
                "operation": str(values.get("operation")),
                "sourceRecordId": values.get("source_record_id"),
            }
        )
    provisional = ConnectorRecordEnvelope.model_validate(
        {**values, "envelope_key": _ZERO_DIGEST},
        context={"skip_identity": True},
    )
    normalized = provisional.model_dump(mode="python")
    normalized["envelope_key"] = deterministic_key(
        "connector-record-envelope-v2", _envelope_identity(provisional)
    )
    return ConnectorRecordEnvelope.model_validate(normalized)


class ConnectorRecordSetManifest(V2ContractModel):
    """Commit-last marker for normalized record-envelope objects."""

    schema_version: str = "2.0"
    record_set_id: str
    batch_id: str
    source_product_id: str
    policy_id: str
    policy_digest: str
    record_objects: tuple[ObjectRef, ...]
    record_count: int = Field(ge=0)
    first_envelope_key: str | None = None
    last_envelope_key: str | None = None
    created_at: str

    @field_validator(
        "record_set_id",
        "batch_id",
        "policy_digest",
        "first_envelope_key",
        "last_envelope_key",
    )
    @classmethod
    def validate_digest(cls, value: str | None) -> str | None:
        return None if value is None else require_sha256(value)

    @field_validator("source_product_id", "policy_id")
    @classmethod
    def validate_id(cls, value: str) -> str:
        return require_slug(value, label="record-set reference")

    @field_validator("created_at")
    @classmethod
    def validate_created_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @model_validator(mode="after")
    def validate_record_set(self, info: ValidationInfo) -> Self:
        if self.record_count > 0 and not self.record_objects:
            raise ValueError("non-empty record set requires record_objects")
        for item in self.record_objects:
            _validate_immutable_ref(item, label="record-set object")
        uris = [item.uri for item in self.record_objects]
        if len(uris) != len(set(uris)):
            raise ValueError("record set contains duplicate object URIs")
        if self.record_count == 0:
            if (
                self.first_envelope_key is not None
                or self.last_envelope_key is not None
            ):
                raise ValueError("empty record set cannot declare key bounds")
        elif self.first_envelope_key is None or self.last_envelope_key is None:
            raise ValueError("non-empty record set requires key bounds")
        if not (info.context or {}).get("skip_identity"):
            expected = deterministic_key(
                "connector-record-set-v2", _record_set_identity(self)
            )
            if self.record_set_id != expected:
                raise ValueError("record_set_id does not match record-set identity")
        return self


def _record_set_identity(manifest: ConnectorRecordSetManifest) -> dict[str, Any]:
    return {
        "schemaVersion": manifest.schema_version,
        "batchId": manifest.batch_id,
        "sourceProductId": manifest.source_product_id,
        "policyId": manifest.policy_id,
        "policyDigest": manifest.policy_digest,
        "recordObjects": [_object_identity(item) for item in manifest.record_objects],
        "recordCount": manifest.record_count,
        "firstEnvelopeKey": manifest.first_envelope_key,
        "lastEnvelopeKey": manifest.last_envelope_key,
        "createdAt": manifest.created_at,
    }


def build_connector_record_set_manifest(
    **values: Any,
) -> ConnectorRecordSetManifest:
    values = dict(values)
    provisional = ConnectorRecordSetManifest.model_validate(
        {**values, "record_set_id": _ZERO_DIGEST},
        context={"skip_identity": True},
    )
    normalized = provisional.model_dump(mode="python")
    normalized["record_set_id"] = deterministic_key(
        "connector-record-set-v2", _record_set_identity(provisional)
    )
    return ConnectorRecordSetManifest.model_validate(normalized)


def validate_envelopes_against_batch(
    manifest: ConnectorBatchManifest,
    envelopes: Iterable[ConnectorRecordEnvelope],
) -> tuple[ConnectorRecordEnvelope, ...]:
    """Bind decoded records to the source, policy, and deletion semantics."""

    result = tuple(envelopes)
    if len(result) != manifest.record_count:
        raise ValueError("record envelope count does not match batch manifest")
    seen: set[str] = set()
    for envelope in result:
        if envelope.envelope_key in seen:
            raise ValueError("connector output contains duplicate envelope keys")
        seen.add(envelope.envelope_key)
        validate_envelope_against_batch(manifest, envelope)
    return result


def validate_envelope_against_batch(
    manifest: ConnectorBatchManifest,
    envelope: ConnectorRecordEnvelope,
) -> ConnectorRecordEnvelope:
    """Validate one envelope without materializing a potentially large batch."""

    if (
        envelope.batch_id != manifest.batch_id
        or envelope.source_system_id != manifest.source_system_id
        or envelope.source_product_id != manifest.source_product_id
        or envelope.policy_id != manifest.policy_id
        or envelope.policy_digest != manifest.policy_digest
    ):
        raise ValueError("record envelope does not bind its batch manifest")
    if envelope.operation == RecordOperation.INFERRED_ABSENCE and (
        manifest.completeness != Completeness.COMPLETE
        or manifest.change_semantics != ChangeSemantics.FULL_SNAPSHOT
        or manifest.delete_coverage != DeleteCoverage.SNAPSHOT_DIFF
    ):
        raise ValueError("inferred absence requires complete snapshot-diff semantics")
    return envelope


class CommunityConnector(Protocol):
    source_product_id: str

    def decode(
        self,
        manifest: ConnectorBatchManifest,
        raw_payloads: Iterable[bytes],
    ) -> Iterable[ConnectorRecordEnvelope]: ...
