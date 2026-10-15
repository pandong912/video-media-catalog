"""Silver mapper for News/Journalism and Learning/Instruction reference slices."""

from __future__ import annotations

import json
import re
from typing import Any

from video_media_catalog.assertions import SourceNodeRef, ValueType
from video_media_catalog.connector import ConnectorRecordEnvelope, RecordOperation
from video_media_catalog.predicate_registry import (
    PredicateKind,
    ResearchDomain,
    load_research_predicate_registry,
)
from video_media_catalog.reference_ontology import (
    ReferenceDomain,
    load_global_reference_ontology,
)
from video_media_catalog.source_mapper import AssertionBuilder, MappedAssertions
from video_media_catalog.v2_contracts import require_rfc3339

WIKIDATA_REFERENCE_MAPPER_ID = "wikidata-global-reference-source-mapper"

_REFERENCE_DOMAINS = frozenset(
    {
        ResearchDomain.NEWS,
        ResearchDomain.JOURNALISM,
        ResearchDomain.LEARNING,
        ResearchDomain.INSTRUCTION,
    }
)
_WIKIDATA_QID = re.compile(r"Q[1-9][0-9]*")

_FIELD_SPECS: tuple[tuple[str, ValueType, str], ...] = (
    ("publication_time", ValueType.TIMESTAMP, "publicationTime"),
    ("claim_text", ValueType.STRING, "claimText"),
    ("claim_status", ValueType.STRING, "claimStatus"),
    ("correction_reason", ValueType.STRING, "correctionReason"),
    ("reporting_status", ValueType.STRING, "reportingStatus"),
    ("source_kind", ValueType.STRING, "sourceKind"),
    ("story_section", ValueType.STRING, "storySection"),
    ("learning_level", ValueType.STRING, "learningLevel"),
    ("learning_duration", ValueType.DURATION, "learningDuration"),
    ("objective_text", ValueType.STRING, "objectiveText"),
    ("instruction_text", ValueType.STRING, "instructionText"),
    ("step_ordinal", ValueType.INTEGER, "stepOrdinal"),
    ("demonstration_kind", ValueType.STRING, "demonstrationKind"),
)


def _active_payload(envelope: ConnectorRecordEnvelope) -> dict[str, Any]:
    if (
        envelope.source_system_id != "wikidata"
        or envelope.source_product_id != "wikidata-json-dump"
        or envelope.source_namespace_id != "wikidata-item"
        or envelope.payload_schema != "wikidata-global-reference-entity"
        or envelope.operation != RecordOperation.UPSERT
        or envelope.payload_json is None
    ):
        raise ValueError("record is not an active Wikidata reference envelope")
    payload = json.loads(envelope.payload_json)
    if not isinstance(payload, dict):
        raise ValueError("Wikidata reference payload must be an object")
    if str(payload.get("id") or "") != envelope.source_record_id:
        raise ValueError("Wikidata reference payload identity does not match envelope")
    return payload


def _domain(payload: dict[str, Any]) -> ResearchDomain:
    try:
        domain = ResearchDomain(str(payload.get("researchDomain") or "").upper())
    except ValueError as exc:
        raise ValueError("Wikidata reference record has an unsupported domain") from exc
    if domain not in _REFERENCE_DOMAINS:
        raise ValueError("Wikidata reference record is outside reference slices")
    return domain


def _supersedes(payload: dict[str, Any], predicate: str) -> str | None:
    values = payload.get("supersedes")
    if not isinstance(values, dict):
        return None
    value = values.get(predicate)
    return value if isinstance(value, str) else None


def _add_titles(
    builder: AssertionBuilder,
    payload: dict[str, Any],
) -> None:
    labels = payload.get("labels")
    if isinstance(labels, dict):
        for language, label in sorted(labels.items()):
            value = label.get("value") if isinstance(label, dict) else label
            if isinstance(value, str) and value.strip():
                builder.add_field(
                    "title",
                    ValueType.STRING,
                    value.strip(),
                    f"/labels/{language}",
                    {"language": str(language), "titleRole": "PRIMARY"},
                )
    descriptions = payload.get("descriptions")
    if isinstance(descriptions, dict):
        for language, description in sorted(descriptions.items()):
            value = (
                description.get("value")
                if isinstance(description, dict)
                else description
            )
            if isinstance(value, str) and value.strip():
                builder.add_field(
                    "description",
                    ValueType.STRING,
                    value.strip(),
                    f"/descriptions/{language}",
                    {"language": str(language)},
                )


def map_wikidata_reference_record(
    envelope: ConnectorRecordEnvelope,
) -> MappedAssertions:
    """Map one reviewed reference record without creating tenant authority."""

    payload = _active_payload(envelope)
    domain = _domain(payload)
    ontology = load_global_reference_ontology()
    predicates = load_research_predicate_registry()
    type_concept_id = str(payload.get("entityTypeHint") or "").strip().upper()
    concept = ontology.concept_for(type_concept_id)
    ontology_domain = ReferenceDomain(domain.value)
    if ontology_domain not in concept.domains:
        raise ValueError("reference type concept is not registered for its domain")

    record_id = envelope.source_record_id
    node = SourceNodeRef(
        namespace_id="wikidata-item",
        source_id=record_id,
        referent_kind=type_concept_id,
    )
    builder = AssertionBuilder(
        envelope=envelope,
        source_node=node,
        mapper_id=WIKIDATA_REFERENCE_MAPPER_ID,
        mapper_version="1.0.0",
    )
    if _WIKIDATA_QID.fullmatch(record_id):
        builder.add_identifier(
            "wikidata-item",
            record_id,
            "Wikidata",
            type_concept_id,
            "/id",
        )
    builder.add_entity_type(type_concept_id, "/entityTypeHint")
    _add_titles(builder, payload)

    for predicate, value_type, key in _FIELD_SPECS:
        value = payload.get(key)
        if value is None or value == "":
            continue
        definition = predicates.definition_for(predicate)
        if (
            definition.kind != PredicateKind.FIELD
            or definition.value_type != value_type
            or domain not in definition.domains
        ):
            raise ValueError(f"reference field registry mismatch: {predicate}")
        if value_type == ValueType.TIMESTAMP:
            value = require_rfc3339(str(value), label=key)
        builder.add_field(
            predicate,
            value_type,
            value,
            f"/{key}",
            supersedes_assertion_id=_supersedes(payload, predicate),
        )

    relations = payload.get("relations")
    if relations is not None and not isinstance(relations, list):
        raise ValueError("reference relations must be an array")
    for index, relation in enumerate(relations or []):
        if not isinstance(relation, dict):
            raise ValueError("reference relation must be an object")
        predicate = str(relation.get("predicate") or "").strip().lower()
        definition = predicates.definition_for(predicate)
        if (
            definition.kind != PredicateKind.RELATIONSHIP
            or domain not in definition.domains
        ):
            raise ValueError(f"reference relation registry mismatch: {predicate}")
        target_id = str(relation.get("targetId") or "").strip()
        target_type = str(relation.get("targetType") or "").strip().upper()
        if not target_id:
            raise ValueError("reference relation requires targetId")
        target_concept = ontology.concept_for(target_type)
        if ontology_domain not in target_concept.domains:
            raise ValueError("reference relation target is outside its domain")
        qualifiers = relation.get("qualifiers") or {}
        if not isinstance(qualifiers, dict):
            raise ValueError("reference relation qualifiers must be an object")
        builder.add_relationship(
            predicate,
            SourceNodeRef(
                namespace_id="wikidata-item",
                source_id=target_id,
                referent_kind=target_type,
            ),
            f"/relations/{index}",
            qualifiers,
            supersedes_assertion_id=_supersedes(
                payload,
                f"{predicate}:{target_id}",
            ),
        )
    return builder.build()
