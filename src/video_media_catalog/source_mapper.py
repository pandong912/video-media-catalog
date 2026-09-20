"""Shared source-to-assertion mapper primitives for connector products."""

from __future__ import annotations

from typing import Any

from pydantic import Field

from video_media_catalog.assertions import (
    AssertionProvenance,
    EntityTypeAssertion,
    FieldAssertion,
    IdentifierAssertion,
    RelationshipAssertion,
    SourceNodeRef,
    ValueType,
    build_entity_type_assertion,
    build_field_assertion,
    build_identifier_assertion,
    build_relationship_assertion,
)
from video_media_catalog.connector import ConnectorRecordEnvelope
from video_media_catalog.v2_contracts import V2ContractModel


class MappedAssertions(V2ContractModel):
    """All Silver assertion families emitted for one source record."""

    source_node: SourceNodeRef
    field_assertions: tuple[FieldAssertion, ...] = ()
    identifier_assertions: tuple[IdentifierAssertion, ...] = ()
    relationship_assertions: tuple[RelationshipAssertion, ...] = ()
    entity_type_assertions: tuple[EntityTypeAssertion, ...] = ()
    omitted_asset_count: int = Field(default=0, ge=0)


class AssertionBuilder:
    """Build assertions with one immutable envelope provenance boundary."""

    def __init__(
        self,
        *,
        envelope: ConnectorRecordEnvelope,
        source_node: SourceNodeRef,
        mapper_id: str,
        mapper_version: str,
    ) -> None:
        self.envelope = envelope
        self.source_node = source_node
        self.mapper_id = mapper_id
        self.mapper_version = mapper_version
        self.fields: list[FieldAssertion] = []
        self.identifiers: list[IdentifierAssertion] = []
        self.relationships: list[RelationshipAssertion] = []
        self.entity_types: list[EntityTypeAssertion] = []

    def provenance(
        self,
        source_path: str,
        *,
        confidence: float | None = None,
    ) -> AssertionProvenance:
        return AssertionProvenance(
            envelope_key=self.envelope.envelope_key,
            source_path=source_path,
            mapper_id=self.mapper_id,
            mapper_version=self.mapper_version,
            policy_id=self.envelope.policy_id,
            policy_digest=self.envelope.policy_digest,
            observed_at=self.envelope.observed_at,
            source_modified_at=self.envelope.source_modified_at,
            valid_from=self.envelope.valid_from,
            valid_to=self.envelope.valid_to,
            citation_keys=self.envelope.citation_keys,
            confidence=confidence,
        )

    def add_field(
        self,
        predicate: str,
        value_type: ValueType,
        value: Any,
        source_path: str,
        qualifiers: dict[str, Any] | None = None,
    ) -> None:
        if value is None or value == "":
            return
        self.fields.append(
            build_field_assertion(
                subject=self.source_node,
                predicate=predicate,
                value_type=value_type,
                value=value,
                qualifiers=qualifiers,
                provenance=self.provenance(source_path),
            )
        )

    def add_identifier(
        self,
        namespace_id: str,
        value: Any,
        issuer: str,
        referent_kind: str,
        source_path: str,
    ) -> None:
        if value is None or value == "":
            return
        self.identifiers.append(
            build_identifier_assertion(
                subject=self.source_node,
                namespace_id=namespace_id,
                value=str(value),
                issuer=issuer,
                referent_kind=referent_kind,
                provenance=self.provenance(source_path),
            )
        )

    def add_relationship(
        self,
        predicate: str,
        object_node: SourceNodeRef,
        source_path: str,
        qualifiers: dict[str, Any] | None = None,
    ) -> None:
        self.relationships.append(
            build_relationship_assertion(
                subject=self.source_node,
                predicate=predicate,
                object=object_node,
                qualifiers=qualifiers,
                provenance=self.provenance(source_path),
            )
        )

    def add_entity_type(self, entity_type: str, source_path: str) -> None:
        self.entity_types.append(
            build_entity_type_assertion(
                subject=self.source_node,
                entity_type=entity_type,
                provenance=self.provenance(source_path),
            )
        )

    def build(self, *, omitted_asset_count: int = 0) -> MappedAssertions:
        return MappedAssertions(
            source_node=self.source_node,
            field_assertions=tuple(self.fields),
            identifier_assertions=tuple(self.identifiers),
            relationship_assertions=tuple(self.relationships),
            entity_type_assertions=tuple(self.entity_types),
            omitted_asset_count=omitted_asset_count,
        )
