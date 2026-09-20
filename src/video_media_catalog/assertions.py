"""Typed, source-owned assertions used by the v2 identity and Gold layers."""

from __future__ import annotations

import json
import re
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Any, Self

from pydantic import Field, field_validator, model_validator

from video_media_catalog.canonical import canonical_json, deterministic_key
from video_media_catalog.v2_contracts import (
    V2ContractModel,
    parse_rfc3339,
    require_https_url,
    require_rfc3339,
    require_sha256,
    require_slug,
)


class ValueType(StrEnum):
    STRING = "STRING"
    BOOLEAN = "BOOLEAN"
    INTEGER = "INTEGER"
    DECIMAL = "DECIMAL"
    DATE = "DATE"
    TIMESTAMP = "TIMESTAMP"
    DURATION = "DURATION"
    JSON = "JSON"


class AssertionStatus(StrEnum):
    ACTIVE = "ACTIVE"
    RETRACTED = "RETRACTED"
    EXPIRED = "EXPIRED"


class SourceNodeRef(V2ContractModel):
    namespace_id: str
    source_id: str
    referent_kind: str

    @field_validator("namespace_id")
    @classmethod
    def validate_namespace(cls, value: str) -> str:
        return require_slug(value, label="namespace_id")

    @field_validator("source_id", "referent_kind")
    @classmethod
    def validate_text(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized or len(normalized) > 2048:
            raise ValueError("source node fields must be non-empty and bounded")
        return normalized


class AssertionProvenance(V2ContractModel):
    envelope_key: str
    source_path: str
    mapper_id: str
    mapper_version: str
    policy_id: str
    policy_digest: str
    observed_at: str
    source_modified_at: str | None = None
    valid_from: str | None = None
    valid_to: str | None = None
    citation_keys: tuple[str, ...] = ()
    confidence: float | None = Field(default=None, ge=0, le=1)

    @field_validator("envelope_key", "policy_digest")
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("mapper_id", "policy_id")
    @classmethod
    def validate_ids(cls, value: str) -> str:
        return require_slug(value, label="assertion provenance reference")

    @field_validator("source_path", "mapper_version")
    @classmethod
    def validate_text(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized or len(normalized) > 2048:
            raise ValueError("assertion provenance text must be non-empty")
        return normalized

    @field_validator(
        "observed_at",
        "source_modified_at",
        "valid_from",
        "valid_to",
    )
    @classmethod
    def validate_timestamp(cls, value: str | None) -> str | None:
        return None if value is None else require_rfc3339(value)

    @field_validator("citation_keys")
    @classmethod
    def normalize_citations(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(
            sorted({require_sha256(item, label="citation key") for item in value})
        )

    @model_validator(mode="after")
    def validate_validity(self) -> Self:
        if (
            self.valid_from
            and self.valid_to
            and parse_rfc3339(self.valid_to) < parse_rfc3339(self.valid_from)
        ):
            raise ValueError("valid_to must not precede valid_from")
        return self


class Citation(V2ContractModel):
    citation_key: str
    source_product_id: str
    source_record_id: str
    source_url: str
    title: str | None = None
    publisher: str | None = None
    retrieved_at: str
    license_uri: str | None = None
    attribution_text: str | None = None
    raw_object_hash: str

    @field_validator("citation_key", "raw_object_hash")
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("source_product_id")
    @classmethod
    def validate_product(cls, value: str) -> str:
        return require_slug(value, label="source_product_id")

    @field_validator("source_record_id")
    @classmethod
    def validate_source_record_id(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized or len(normalized) > 2048:
            raise ValueError("source_record_id must be non-empty and bounded")
        return normalized

    @field_validator("title", "publisher", "attribution_text")
    @classmethod
    def normalize_optional_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized or len(normalized) > 2000:
            raise ValueError("citation text must be non-empty and bounded")
        return normalized

    @field_validator("source_url", "license_uri")
    @classmethod
    def validate_url(cls, value: str | None) -> str | None:
        return None if value is None else require_https_url(value)

    @field_validator("retrieved_at")
    @classmethod
    def validate_retrieved_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        expected = deterministic_key("citation-v2", _citation_identity(self))
        if self.citation_key != expected:
            raise ValueError("citation_key does not match citation identity")
        return self


def _citation_identity(citation: Citation) -> dict[str, Any]:
    return {
        "sourceProductId": citation.source_product_id,
        "sourceRecordId": citation.source_record_id,
        "sourceUrl": citation.source_url,
        "title": citation.title,
        "publisher": citation.publisher,
        "retrievedAt": citation.retrieved_at,
        "licenseUri": citation.license_uri,
        "attributionText": citation.attribution_text,
        "rawObjectHash": citation.raw_object_hash,
    }


def build_citation(
    *,
    source_product_id: str,
    source_record_id: str,
    source_url: str,
    retrieved_at: str,
    raw_object_hash: str,
    title: str | None = None,
    publisher: str | None = None,
    license_uri: str | None = None,
    attribution_text: str | None = None,
) -> Citation:
    values = {
        "source_product_id": require_slug(source_product_id, label="source_product_id"),
        "source_record_id": source_record_id.strip(),
        "source_url": require_https_url(source_url),
        "title": None if title is None else title.strip(),
        "publisher": None if publisher is None else publisher.strip(),
        "retrieved_at": require_rfc3339(retrieved_at),
        "license_uri": (
            None if license_uri is None else require_https_url(license_uri)
        ),
        "attribution_text": (
            None if attribution_text is None else attribution_text.strip()
        ),
        "raw_object_hash": require_sha256(raw_object_hash, label="raw_object_hash"),
    }
    provisional = Citation.model_construct(
        citation_key="sha256:" + ("0" * 64),
        **values,
    )
    return Citation(
        citation_key=deterministic_key("citation-v2", _citation_identity(provisional)),
        **values,
    )


def _assertion_identity(
    *,
    kind: str,
    subject: SourceNodeRef,
    predicate: str,
    value: Any,
    qualifiers: dict[str, Any],
    provenance: AssertionProvenance,
) -> dict[str, Any]:
    return {
        "kind": kind,
        "subject": subject.model_dump(mode="json", by_alias=True),
        "predicate": predicate,
        "value": value,
        "qualifiers": qualifiers,
        "envelopeKey": provenance.envelope_key,
        "sourcePath": provenance.source_path,
        "mapperId": provenance.mapper_id,
        "mapperVersion": provenance.mapper_version,
        "policyId": provenance.policy_id,
    }


class FieldAssertion(V2ContractModel):
    assertion_id: str
    subject: SourceNodeRef
    predicate: str
    value_type: ValueType
    value_json: str
    qualifiers: dict[str, Any] = Field(default_factory=dict)
    status: AssertionStatus = AssertionStatus.ACTIVE
    provenance: AssertionProvenance

    @field_validator("assertion_id")
    @classmethod
    def validate_assertion_id(cls, value: str) -> str:
        return require_sha256(value, label="assertion_id")

    @field_validator("predicate")
    @classmethod
    def validate_predicate(cls, value: str) -> str:
        return require_slug(value, label="predicate")

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        try:
            parsed = json.loads(self.value_json)
        except json.JSONDecodeError as exc:
            raise ValueError("value_json must be valid JSON") from exc
        if canonical_json(parsed) != self.value_json:
            raise ValueError("value_json must use canonical JSON")
        _validate_typed_value(self.value_type, parsed)
        canonical_json(self.qualifiers)
        expected = deterministic_key(
            "field-assertion-v2",
            _assertion_identity(
                kind="field",
                subject=self.subject,
                predicate=self.predicate,
                value=parsed,
                qualifiers=self.qualifiers,
                provenance=self.provenance,
            ),
        )
        if self.assertion_id != expected:
            raise ValueError("assertion_id does not match field identity")
        return self


def _validate_typed_value(value_type: ValueType, value: Any) -> None:
    valid = True
    if value_type == ValueType.STRING:
        valid = isinstance(value, str)
    elif value_type == ValueType.BOOLEAN:
        valid = isinstance(value, bool)
    elif value_type == ValueType.INTEGER:
        valid = isinstance(value, int) and not isinstance(value, bool)
    elif value_type == ValueType.DECIMAL:
        valid = (
            isinstance(value, int | float | str)
            and not isinstance(value, bool)
            and str(value).strip() != ""
        )
        if valid:
            try:
                Decimal(str(value))
            except InvalidOperation:
                valid = False
    elif value_type == ValueType.DATE:
        valid = (
            isinstance(value, str)
            and re.fullmatch(
                r"[0-9]{4}(?:-[0-9]{2}(?:-[0-9]{2})?)?",
                value,
            )
            is not None
        )
    elif value_type == ValueType.TIMESTAMP:
        if not isinstance(value, str):
            valid = False
        else:
            try:
                parse_rfc3339(value)
            except ValueError:
                valid = False
    elif value_type == ValueType.DURATION:
        valid = isinstance(value, str) and value.startswith("P")
    if not valid:
        raise ValueError(f"value does not match {value_type.value}")


def build_field_assertion(
    *,
    subject: SourceNodeRef,
    predicate: str,
    value_type: ValueType,
    value: Any,
    provenance: AssertionProvenance,
    qualifiers: dict[str, Any] | None = None,
    status: AssertionStatus = AssertionStatus.ACTIVE,
) -> FieldAssertion:
    normalized_qualifiers = qualifiers or {}
    identity = _assertion_identity(
        kind="field",
        subject=subject,
        predicate=predicate,
        value=value,
        qualifiers=normalized_qualifiers,
        provenance=provenance,
    )
    return FieldAssertion(
        assertion_id=deterministic_key("field-assertion-v2", identity),
        subject=subject,
        predicate=predicate,
        value_type=value_type,
        value_json=canonical_json(value),
        qualifiers=normalized_qualifiers,
        status=status,
        provenance=provenance,
    )


class IdentifierAssertion(V2ContractModel):
    assertion_id: str
    subject: SourceNodeRef
    namespace_id: str
    value: str
    issuer: str
    referent_kind: str
    status: AssertionStatus = AssertionStatus.ACTIVE
    provenance: AssertionProvenance

    @field_validator("assertion_id")
    @classmethod
    def validate_assertion_id(cls, value: str) -> str:
        return require_sha256(value, label="assertion_id")

    @field_validator("namespace_id")
    @classmethod
    def validate_namespace_id(cls, value: str) -> str:
        return require_slug(value, label="namespace_id")

    @field_validator("value", "issuer", "referent_kind")
    @classmethod
    def validate_text(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized or len(normalized) > 2048:
            raise ValueError("identifier assertion text must be non-empty")
        return normalized

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        expected = deterministic_key(
            "identifier-assertion-v2",
            _assertion_identity(
                kind="identifier",
                subject=self.subject,
                predicate=self.namespace_id,
                value={
                    "value": self.value,
                    "issuer": self.issuer,
                    "referentKind": self.referent_kind,
                },
                qualifiers={},
                provenance=self.provenance,
            ),
        )
        if self.assertion_id != expected:
            raise ValueError("assertion_id does not match identifier identity")
        return self


def build_identifier_assertion(
    *,
    subject: SourceNodeRef,
    namespace_id: str,
    value: str,
    issuer: str,
    referent_kind: str,
    provenance: AssertionProvenance,
    status: AssertionStatus = AssertionStatus.ACTIVE,
) -> IdentifierAssertion:
    identity = _assertion_identity(
        kind="identifier",
        subject=subject,
        predicate=namespace_id,
        value={
            "value": value,
            "issuer": issuer,
            "referentKind": referent_kind,
        },
        qualifiers={},
        provenance=provenance,
    )
    return IdentifierAssertion(
        assertion_id=deterministic_key("identifier-assertion-v2", identity),
        subject=subject,
        namespace_id=namespace_id,
        value=value,
        issuer=issuer,
        referent_kind=referent_kind,
        status=status,
        provenance=provenance,
    )


class RelationshipAssertion(V2ContractModel):
    assertion_id: str
    subject: SourceNodeRef
    predicate: str
    object: SourceNodeRef
    qualifiers: dict[str, Any] = Field(default_factory=dict)
    status: AssertionStatus = AssertionStatus.ACTIVE
    provenance: AssertionProvenance

    @field_validator("assertion_id")
    @classmethod
    def validate_assertion_id(cls, value: str) -> str:
        return require_sha256(value, label="assertion_id")

    @field_validator("predicate")
    @classmethod
    def validate_predicate(cls, value: str) -> str:
        return require_slug(value, label="predicate")

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        value = self.object.model_dump(mode="json", by_alias=True)
        canonical_json(self.qualifiers)
        expected = deterministic_key(
            "relationship-assertion-v2",
            _assertion_identity(
                kind="relationship",
                subject=self.subject,
                predicate=self.predicate,
                value=value,
                qualifiers=self.qualifiers,
                provenance=self.provenance,
            ),
        )
        if self.assertion_id != expected:
            raise ValueError("assertion_id does not match relationship identity")
        return self


def build_relationship_assertion(
    *,
    subject: SourceNodeRef,
    predicate: str,
    object: SourceNodeRef,
    provenance: AssertionProvenance,
    qualifiers: dict[str, Any] | None = None,
    status: AssertionStatus = AssertionStatus.ACTIVE,
) -> RelationshipAssertion:
    normalized_qualifiers = qualifiers or {}
    normalized_predicate = require_slug(predicate, label="predicate")
    identity = _assertion_identity(
        kind="relationship",
        subject=subject,
        predicate=normalized_predicate,
        value=object.model_dump(mode="json", by_alias=True),
        qualifiers=normalized_qualifiers,
        provenance=provenance,
    )
    return RelationshipAssertion(
        assertion_id=deterministic_key("relationship-assertion-v2", identity),
        subject=subject,
        predicate=normalized_predicate,
        object=object,
        qualifiers=normalized_qualifiers,
        status=status,
        provenance=provenance,
    )


class EntityTypeAssertion(V2ContractModel):
    assertion_id: str
    subject: SourceNodeRef
    entity_type: str
    status: AssertionStatus = AssertionStatus.ACTIVE
    provenance: AssertionProvenance

    @field_validator("assertion_id")
    @classmethod
    def validate_assertion_id(cls, value: str) -> str:
        return require_sha256(value, label="assertion_id")

    @field_validator("entity_type")
    @classmethod
    def validate_entity_type(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not normalized or len(normalized) > 128:
            raise ValueError("entity_type must be non-empty")
        return normalized

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        expected = deterministic_key(
            "entity-type-assertion-v2",
            _assertion_identity(
                kind="entity_type",
                subject=self.subject,
                predicate="entity_type",
                value=self.entity_type,
                qualifiers={},
                provenance=self.provenance,
            ),
        )
        if self.assertion_id != expected:
            raise ValueError("assertion_id does not match entity-type identity")
        return self


def build_entity_type_assertion(
    *,
    subject: SourceNodeRef,
    entity_type: str,
    provenance: AssertionProvenance,
    status: AssertionStatus = AssertionStatus.ACTIVE,
) -> EntityTypeAssertion:
    normalized = entity_type.strip().upper()
    identity = _assertion_identity(
        kind="entity_type",
        subject=subject,
        predicate="entity_type",
        value=normalized,
        qualifiers={},
        provenance=provenance,
    )
    return EntityTypeAssertion(
        assertion_id=deterministic_key("entity-type-assertion-v2", identity),
        subject=subject,
        entity_type=normalized,
        status=status,
        provenance=provenance,
    )
