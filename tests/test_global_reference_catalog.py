from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator
from pydantic import ValidationError

from video_media_catalog.assertions import FieldAssertion, SourceNodeRef
from video_media_catalog.canonical import canonical_json
from video_media_catalog.catalog_source_policy import (
    CatalogSourceCapability,
    CatalogSourcePolicyEntry,
    load_catalog_source_policy,
)
from video_media_catalog.community_sources import (
    build_community_registry,
    wikidata_rights_profile,
)
from video_media_catalog.connector import (
    RecordOperation,
    build_connector_record_envelope,
    build_connector_record_set_manifest,
)
from video_media_catalog.gold import (
    GOLD_RELEASE_PLAN_MEDIA_TYPE,
    GoldResolutionStatus,
    build_gold_release_plan,
    research_context,
    research_policy,
)
from video_media_catalog.gold_ingest import (
    ATTRIBUTION_MEDIA_TYPE,
    GOLD_QUALITY_MEDIA_TYPE,
    GOLD_RELEASE_COMMIT_MEDIA_TYPE,
    build_gold_release_commit,
)
from video_media_catalog.gold_resolution import resolve_gold_draft
from video_media_catalog.gold_search_index import (
    GOLD_INDEX_BUILD_MANIFEST_MEDIA_TYPE,
    GOLD_INDEX_VALIDATION_RECEIPT_MEDIA_TYPE,
    MAPPING_DIGEST,
    RESEARCH_INDEX_PREFIX,
    RESEARCH_READ_ALIAS,
    GoldIndexBuildManifest,
    GoldIndexValidationReceipt,
    gold_index_config_identity,
    gold_index_name,
)
from video_media_catalog.gold_search_projection import project_gold_entity
from video_media_catalog.gold_tables import GOLD_DATA_COLUMNS
from video_media_catalog.identity_resolution import (
    build_identity_index,
    resolve_or_allocate_source_node,
)
from video_media_catalog.identity_v2 import EntityLevel
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.predicate_registry import (
    PredicateKind,
    ResearchDomain,
    load_research_predicate_registry,
)
from video_media_catalog.reference_cli import _build_release, _coordinates
from video_media_catalog.reference_fixture import (
    MaterializedReferenceFixture,
    load_reference_fixture,
    materialize_reference_fixture,
)
from video_media_catalog.reference_ontology import (
    CanonicalSubjectRoot,
    load_global_reference_ontology,
)
from video_media_catalog.reference_release import (
    ReferenceCapability,
    ReferenceSlice,
    ReferenceSliceSourcePin,
    build_global_reference_release_manifest,
)
from video_media_catalog.reference_sources import (
    map_wikidata_reference_record,
)
from video_media_catalog.source_silver import build_source_silver_rows
from video_media_catalog.v2_contracts import V2ContractModel

FIXTURE_ROOT = Path("tests/fixtures/research")
TIMESTAMP = "2026-10-15T00:00:00Z"
SOURCE_POLICY_DIGEST = (
    "sha256:f752400d08b822099c77c489d605b21fb72b4b9b325c6bcce681ac31a361c859"
)


def _fixture(name: str) -> MaterializedReferenceFixture:
    return materialize_reference_fixture(load_reference_fixture(FIXTURE_ROOT / name))


def _ref(
    name: str,
    *,
    media_type: str,
    checksum: str,
) -> ObjectRef:
    return ObjectRef(
        uri=f"s3://catalog-reference/releases/{name}.json",
        format="OBJECT_FORMAT_JSON",
        media_type=media_type,
        checksum=Checksum(value=checksum * 64),
        size_bytes=512,
        etag=f"{name}-etag",
        object_version=f"{name}-version",
    )


def _model_ref(
    name: str,
    *,
    model: V2ContractModel,
    media_type: str,
) -> ObjectRef:
    payload = model.json_bytes()
    return ObjectRef(
        uri=f"s3://catalog-reference/releases/{name}.json",
        format="OBJECT_FORMAT_JSON",
        media_type=media_type,
        checksum=Checksum(value=hashlib.sha256(payload).hexdigest()),
        size_bytes=len(payload),
        etag=f"{name}-etag",
        object_version=f"{name}-version",
    )


def _reference_envelope(
    *,
    payload: dict[str, object],
    observed_at: str,
    valid_from: str,
    valid_to: str | None,
    citation_key: str,
) -> object:
    rights = wikidata_rights_profile()
    return build_connector_record_envelope(
        payload=payload,
        batch_id="sha256:" + ("a" * 64),
        source_system_id="wikidata",
        source_product_id="wikidata-json-dump",
        source_namespace_id="wikidata-item",
        source_record_id=str(payload["id"]),
        source_revision=observed_at,
        operation=RecordOperation.UPSERT,
        source_modified_at=observed_at,
        observed_at=observed_at,
        ingested_at=observed_at,
        valid_from=valid_from,
        valid_to=valid_to,
        payload_schema="wikidata-global-reference-entity",
        raw_object=ObjectRef(
            uri="file:///tmp/wikidata-reference-correction.json",
            format="OBJECT_FORMAT_JSON",
            media_type="application/json",
            checksum=Checksum(value="b" * 64),
            size_bytes=256,
        ),
        source_location=f"/entities/{payload['id']}",
        policy_id=rights.policy_id,
        policy_digest=rights.digest,
        citation_keys=(citation_key,),
    )


def _resolve_fields(
    assertions: tuple[FieldAssertion, ...],
    *,
    level: EntityLevel,
    kind: str,
):
    rights = wikidata_rights_profile()
    node = assertions[0].subject
    identity = resolve_or_allocate_source_node(
        source_node=node,
        entity_level=level,
        entity_kind=kind,
        exact_candidate_entity_keys=(),
        assertion_keys=tuple(item.assertion_id for item in assertions),
        observed_at=TIMESTAMP,
        policy_id=rights.policy_id,
        policy_digest=rights.digest,
        decision_policy_version="research-identity-v1",
        decided_by="research-identity-resolver",
    )
    index = build_identity_index(
        entities=identity.entities,
        memberships=identity.memberships,
        redirects=(),
        as_of="2026-10-31T00:00:00Z",
    )
    return resolve_gold_draft(
        identity_index=index,
        field_assertions=assertions,
        identifier_assertions=(),
        relationship_assertions=(),
        rights_profiles=(rights,),
        policy_context=research_context(as_of="2026-10-31T00:00:00Z"),
        field_policy=research_policy(),
    )


def _wikidata_trace(assertion_id: str, source_path: str) -> str:
    return canonical_json(
        {
            "assertions": [
                {
                    "assertionId": assertion_id,
                    "sourceProductId": "wikidata-json-dump",
                    "sourceName": "Wikidata JSON entity dump",
                    "sourceRecordId": "Q135249048",
                    "sourcePath": source_path,
                    "observedAt": TIMESTAMP,
                    "citationKeys": ["sha256:" + ("a" * 64)],
                    "rights": {
                        "policyId": "wikidata-structured-data-cc0",
                        "policyZone": "open_cc0",
                        "licenseId": "CC0-1.0",
                        "licenseUri": (
                            "https://creativecommons.org/publicdomain/zero/1.0/"
                        ),
                        "attributionText": "Wikidata structured data (CC0).",
                        "sourceUrl": "https://www.wikidata.org/",
                        "shareAlike": False,
                    },
                }
            ]
        }
    )


def _projection_row(
    *,
    entity_kind: str,
    predicate: str,
    text: str,
    relation_predicate: str | None = None,
) -> dict[str, object]:
    title_id = "sha256:" + ("b" * 64)
    value_id = "sha256:" + ("c" * 64)
    fields = [
        {
            "predicate": "title",
            "value_json": canonical_json(f"{entity_kind} fixture"),
            "qualifiers_json": '{"language":"en","titleRole":"PRIMARY"}',
            "resolution_status": "SELECTED",
            "selected_assertion_id": title_id,
            "assertion_ids_json": canonical_json([title_id]),
            "trace_json": _wikidata_trace(title_id, "/labels/en"),
        },
        {
            "predicate": predicate,
            "value_json": canonical_json(text),
            "qualifiers_json": "{}",
            "resolution_status": "SELECTED",
            "selected_assertion_id": value_id,
            "assertion_ids_json": canonical_json([value_id]),
            "trace_json": _wikidata_trace(value_id, f"/{predicate}"),
        },
    ]
    relations = []
    relation_summary = []
    if relation_predicate is not None:
        relation_id = "sha256:" + ("d" * 64)
        relations.append(
            {
                "predicate": relation_predicate,
                "object_entity_key": "sha256:" + ("e" * 64),
                "object_entity_level": "REPORTED_EVENT",
                "qualifiers_json": "{}",
                "assertion_ids_json": canonical_json([relation_id]),
                "trace_json": _wikidata_trace(relation_id, "/relations/0"),
            }
        )
        relation_summary.append({"predicate": relation_predicate, "count": 1})
    return {
        "entity_key": "sha256:" + ("f" * 64),
        "entity_level": entity_kind,
        "entity_kind": entity_kind,
        "status": "ACTIVE",
        "release_plan_id": "sha256:" + ("1" * 64),
        "source_node_count": 1,
        "fields": fields,
        "identifiers": [],
        "relations": relations,
        "relation_summary": relation_summary,
        "observations": [],
        "conflicts": [],
        "conflict_count": 0,
    }


def test_reference_ontology_has_terminal_roots_and_versioned_concepts() -> None:
    ontology = load_global_reference_ontology()
    predicates = load_research_predicate_registry()

    assert set(ontology.root_types) == set(CanonicalSubjectRoot)
    assert ontology.authority == "GLOBAL_REFERENCE_ONLY"
    assert ontology.tenant_world_identity_authority is False
    assert ontology.predicate_registry_version == predicates.registry_version
    assert all(item.concept_version == "1.0.0" for item in ontology.type_concepts)
    assert {
        "NEWS_PROGRAM",
        "NEWS_EDITION",
        "NEWS_STORY",
        "REPORTED_EVENT",
        "INFORMATION_SOURCE",
        "CLAIM",
        "CORRECTION",
        "COURSE",
        "MODULE",
        "LESSON",
        "LEARNING_CONCEPT",
        "LEARNING_OBJECTIVE",
        "PREREQUISITE",
        "PROCEDURE_STEP",
        "DEMONSTRATION",
    } <= {item.type_concept_id for item in ontology.type_concepts}
    assert all(item.predicate_version == "1.0.0" for item in predicates.predicates)
    assert predicates.definition_for("corrects_claim").kind == (
        PredicateKind.RELATIONSHIP
    )
    assert ResearchDomain.NEWS in predicates.definition_for("claim_text").domains
    assert (
        ResearchDomain.LEARNING in predicates.definition_for("objective_text").domains
    )


def test_pinned_news_and_learning_fixtures_are_deterministic_and_cited() -> None:
    names = (
        "wikidata-news-reference.v1.json",
        "wikidata-learning-reference.v1.json",
    )
    materialized = tuple(_fixture(name) for name in names)
    replayed = tuple(_fixture(name) for name in names)

    assert materialized == replayed
    assert [len(item.captured.envelopes) for item in materialized] == [7, 8]
    assert all(
        envelope.citation_keys
        for item in materialized
        for envelope in item.captured.envelopes
    )
    mapped_types = {
        assertion.entity_type
        for item in materialized
        for mapped in item.mapped
        for assertion in mapped.entity_type_assertions
    }
    assert {
        "NEWS_PROGRAM",
        "NEWS_EDITION",
        "NEWS_STORY",
        "REPORTED_EVENT",
        "INFORMATION_SOURCE",
        "CLAIM",
        "CORRECTION",
        "COURSE",
        "MODULE",
        "LESSON",
        "LEARNING_CONCEPT",
        "LEARNING_OBJECTIVE",
        "PREREQUISITE",
        "PROCEDURE_STEP",
        "DEMONSTRATION",
    } == mapped_types
    assert all(
        assertion.provenance.citation_keys
        for item in materialized
        for mapped in item.mapped
        for assertion in (
            *mapped.field_assertions,
            *mapped.identifier_assertions,
            *mapped.relationship_assertions,
            *mapped.entity_type_assertions,
        )
    )


def test_reference_capture_pin_reaches_silver_run() -> None:
    fixture = _fixture("wikidata-news-reference.v1.json")
    captured = fixture.captured
    record_set = build_connector_record_set_manifest(
        batch_id=captured.batch_manifest.batch_id,
        source_product_id=captured.batch_manifest.source_product_id,
        policy_id=captured.batch_manifest.policy_id,
        policy_digest=captured.batch_manifest.policy_digest,
        record_objects=captured.pin.raw_objects,
        record_count=len(captured.envelopes),
        first_envelope_key=captured.envelopes[0].envelope_key,
        last_envelope_key=captured.envelopes[-1].envelope_key,
        created_at=captured.pin.captured_at,
    )

    run, rows = build_source_silver_rows(
        registry=build_community_registry(),
        batch=captured.batch_manifest,
        record_set=record_set,
        envelopes=captured.envelopes,
    )

    assert run.input_manifest["sourceSnapshotPinId"] == captured.pin.pin_id
    assert len(rows["community_entity_type_assertion"]) == len(captured.envelopes)
    assert {
        (envelope.source_record_id, envelope.source_location)
        for envelope in captured.envelopes
    } == {
        (envelope.source_record_id, f"/records/{envelope.source_record_id}")
        for envelope in captured.envelopes
    }
    assert all(
        assertion.provenance.source_path.startswith("/")
        for mapped in fixture.mapped
        for assertion in mapped.field_assertions
    )


def test_reference_exact_candidate_reuse_and_ambiguity_abstention() -> None:
    node = SourceNodeRef(
        namespace_id="wikidata-item",
        source_id="Q135249048",
        referent_kind="NEWS_STORY",
    )
    common = {
        "source_node": node,
        "entity_level": EntityLevel.NEWS_STORY,
        "entity_kind": "NEWS_STORY",
        "assertion_keys": ("sha256:" + ("c" * 64),),
        "observed_at": TIMESTAMP,
        "policy_id": "wikidata-structured-data-cc0",
        "policy_digest": "sha256:" + ("d" * 64),
        "decision_policy_version": "research-identity-v1",
        "decided_by": "research-identity-resolver",
    }
    candidate = "sha256:" + ("e" * 64)
    accepted = resolve_or_allocate_source_node(
        **common,
        exact_candidate_entity_keys=(candidate,),
    )
    ambiguous = resolve_or_allocate_source_node(
        **common,
        exact_candidate_entity_keys=(candidate, "sha256:" + ("f" * 64)),
    )

    assert accepted.memberships[0].entity_key == candidate
    assert ambiguous.memberships == ()
    assert ambiguous.decisions == ()
    assert ambiguous.conflicts[0].reason == "MULTIPLE_EXACT_IDENTIFIER_CANDIDATES"


def test_news_correction_keeps_bitemporal_history_and_citation_lineage() -> None:
    citation = "sha256:" + ("1" * 64)
    first_payload: dict[str, object] = {
        "id": "Q1348305",
        "researchDomain": "NEWS",
        "entityTypeHint": "CORRECTION",
        "correctionReason": "Initial correction.",
    }
    first = next(
        item
        for item in map_wikidata_reference_record(
            _reference_envelope(
                payload=first_payload,
                observed_at="2026-10-01T00:00:00Z",
                valid_from="2024-12-20T00:00:00Z",
                valid_to="2024-12-21T00:00:00Z",
                citation_key=citation,
            )
        ).field_assertions
        if item.predicate == "correction_reason"
    )
    second_payload = {
        **first_payload,
        "correctionReason": "Clarified correction.",
        "supersedes": {"correction_reason": first.assertion_id},
    }
    second = next(
        item
        for item in map_wikidata_reference_record(
            _reference_envelope(
                payload=second_payload,
                observed_at="2026-10-15T00:00:00Z",
                valid_from="2024-12-21T00:00:00Z",
                valid_to=None,
                citation_key=citation,
            )
        ).field_assertions
        if item.predicate == "correction_reason"
    )
    draft = _resolve_fields(
        (first, second),
        level=EntityLevel.CORRECTION,
        kind="CORRECTION",
    )

    assert draft.fields[0].value == "Clarified correction."
    assert len(draft.observations) == 2
    history = {item.assertion_id: item for item in draft.observations}
    assert history[first.assertion_id].valid_to == "2024-12-21T00:00:00Z"
    assert history[second.assertion_id].valid_from == "2024-12-21T00:00:00Z"
    assert history[second.assertion_id].supersedes_assertion_id == first.assertion_id
    assert history[first.assertion_id].is_winning is False
    assert history[second.assertion_id].is_winning is True
    assert all(item.trace["citationKeys"] == (citation,) for item in draft.observations)


def test_equal_rank_claim_conflict_abstains() -> None:
    citation = "sha256:" + ("2" * 64)
    payload = {
        "id": "Q135249048",
        "researchDomain": "NEWS",
        "entityTypeHint": "NEWS_STORY",
        "storySection": "media",
    }
    first = next(
        item
        for item in map_wikidata_reference_record(
            _reference_envelope(
                payload=payload,
                observed_at=TIMESTAMP,
                valid_from="2024-12-19T00:00:00Z",
                valid_to=None,
                citation_key=citation,
            )
        ).field_assertions
        if item.predicate == "story_section"
    )
    second = next(
        item
        for item in map_wikidata_reference_record(
            _reference_envelope(
                payload={**payload, "storySection": "politics"},
                observed_at=TIMESTAMP,
                valid_from="2024-12-19T00:00:00Z",
                valid_to=None,
                citation_key=citation,
            )
        ).field_assertions
        if item.predicate == "story_section"
    )
    draft = _resolve_fields(
        (first, second),
        level=EntityLevel.NEWS_STORY,
        kind="NEWS_STORY",
    )

    assert draft.fields[0].value is None
    assert draft.conflicts[0].reason == "MULTIPLE_ELIGIBLE_VALUES"
    assert draft.conflicts[0].candidate_values == ["media", "politics"]


@pytest.mark.parametrize(
    ("entity_kind", "predicate", "text", "expected_slice"),
    (
        (
            "NEWS_STORY",
            "claim_text",
            "A cited reported claim.",
            "NEWS/JOURNALISM",
        ),
        (
            "LESSON",
            "instruction_text",
            "Guide the learner through one reviewed step.",
            "LEARNING/INSTRUCTION",
        ),
    ),
)
def test_reference_slices_contribute_bounded_search_candidates(
    entity_kind: str,
    predicate: str,
    text: str,
    expected_slice: str,
) -> None:
    document = project_gold_entity(
        _projection_row(
            entity_kind=entity_kind,
            predicate=predicate,
            text=text,
            relation_predicate=(
                "reports_event" if entity_kind == "NEWS_STORY" else None
            ),
        )
    )

    assert document["authorityScope"] == "GLOBAL_REFERENCE_ONLY"
    assert document["referenceCapabilities"] == [
        "DISPLAY",
        "IDENTITY",
        "RESEARCH",
    ]
    assert document["referenceSlices"] == [expected_slice]
    assert document["canonicalSubjectRoot"] == "WORK"
    assert document["typeConcepts"][0]["typeConceptId"] == entity_kind
    assert any(
        item["predicate"] == predicate and item["valueText"] == text
        for item in document["referenceCandidates"]
    )
    assert document["overflow"]["referenceCandidates"] == 0
    if entity_kind == "NEWS_STORY":
        assert document["referenceRelations"][0]["predicate"] == "reports_event"
        assert {"NEWS", "JOURNALISM"} <= set(document["domains"])
    else:
        assert {"LEARNING", "INSTRUCTION"} <= set(document["domains"])
    assert "worldMaterialization" not in document
    assert "worldPublication" not in document


def test_learning_objective_set_union_keeps_all_values() -> None:
    citation = "sha256:" + ("3" * 64)
    payload = {
        "id": "Q939957",
        "researchDomain": "LEARNING",
        "entityTypeHint": "LEARNING_OBJECTIVE",
        "objectiveText": "Identify the concept.",
    }
    first = next(
        item
        for item in map_wikidata_reference_record(
            _reference_envelope(
                payload=payload,
                observed_at=TIMESTAMP,
                valid_from="2024-01-01T00:00:00Z",
                valid_to=None,
                citation_key=citation,
            )
        ).field_assertions
        if item.predicate == "objective_text"
    )
    second = next(
        item
        for item in map_wikidata_reference_record(
            _reference_envelope(
                payload={
                    **payload,
                    "objectiveText": "Apply the reviewed procedure.",
                },
                observed_at=TIMESTAMP,
                valid_from="2024-01-01T00:00:00Z",
                valid_to=None,
                citation_key=citation,
            )
        ).field_assertions
        if item.predicate == "objective_text"
    )
    draft = _resolve_fields(
        (first, second),
        level=EntityLevel.LEARNING_OBJECTIVE,
        kind="LEARNING_OBJECTIVE",
    )

    assert {item.value for item in draft.fields} == {
        "Apply the reviewed procedure.",
        "Identify the concept.",
    }
    assert draft.conflicts == ()
    assert all(item.status == GoldResolutionStatus.SET for item in draft.fields)


def test_search_contribution_requires_source_policy_gold_capability(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = CatalogSourcePolicyEntry.allows

    def allows(
        source: CatalogSourcePolicyEntry,
        capability: CatalogSourceCapability,
    ) -> bool:
        if (
            source.source_product_id == "wikidata-json-dump"
            and capability == CatalogSourceCapability.GOLD_PROJECTION
        ):
            return False
        return original(source, capability)

    monkeypatch.setattr(CatalogSourcePolicyEntry, "allows", allows)
    document = project_gold_entity(
        _projection_row(
            entity_kind="NEWS_STORY",
            predicate="claim_text",
            text="Policy-gated claim.",
        )
    )

    assert document["referenceCandidates"] == []
    assert document["referenceRelations"] == []


def test_reference_release_is_deterministic_validated_and_world_write_free(
    tmp_path: Path,
) -> None:
    news = _fixture("wikidata-news-reference.v1.json")
    learning = _fixture("wikidata-learning-reference.v1.json")
    source_policy = load_catalog_source_policy()
    ontology = load_global_reference_ontology()
    predicates = load_research_predicate_registry()
    table_counts = {
        table: (15 if table == "community_gold_entity" else 0)
        for table in GOLD_DATA_COLUMNS
    }
    table_snapshot_ids = {
        table: (10 if table == "community_gold_entity" else None)
        for table in GOLD_DATA_COLUMNS
    }
    plan = build_gold_release_plan(
        policy_context=research_context(as_of=TIMESTAMP),
        committed_run_ids=("sha256:" + ("1" * 64),),
        silver_snapshot_ids={"community_ingest_run": 8},
        identity_snapshot_ids={"community_entity_ledger": 9},
        rights_registry_digest="sha256:" + ("2" * 64),
        source_policy_digest=source_policy.digest,
        field_policy_digest=research_policy().digest,
        resolver_digest="sha256:" + ("3" * 64),
        image_digest="sha256:" + ("4" * 64),
        config_digest="sha256:" + ("5" * 64),
        expected_counts=table_counts,
        planned_at=TIMESTAMP,
        domain_coverage={
            "NEWS": 4,
            "JOURNALISM": 4,
            "LEARNING": 4,
            "INSTRUCTION": 4,
        },
        source_snapshot_digests={
            "wikidata-json-dump": "sha256:" + ("6" * 64),
        },
        source_snapshot_pin_ids={
            "wikidata-json-dump": (
                news.captured.pin.pin_id,
                learning.captured.pin.pin_id,
            ),
        },
        reference_ontology_digest=ontology.digest,
        reference_predicate_registry_digest=predicates.digest,
    )
    plan_ref = _model_ref(
        "gold-plan",
        model=plan,
        media_type=GOLD_RELEASE_PLAN_MEDIA_TYPE,
    )
    quality_report = _ref(
        "gold-quality",
        media_type=GOLD_QUALITY_MEDIA_TYPE,
        checksum="7",
    )
    attribution_manifest = _ref(
        "gold-attribution",
        media_type=ATTRIBUTION_MEDIA_TYPE,
        checksum="8",
    )
    commit = build_gold_release_commit(
        release_plan_id=plan.release_plan_id,
        committed_at=TIMESTAMP,
        table_counts=table_counts,
        table_snapshot_ids=table_snapshot_ids,
        quality_report=quality_report,
        attribution_manifest=attribution_manifest,
        source_policy_digest=source_policy.digest,
    )
    commit_ref = _model_ref(
        "gold-release",
        model=commit,
        media_type=GOLD_RELEASE_COMMIT_MEDIA_TYPE,
    )
    config = gold_index_config_identity(
        read_alias=RESEARCH_READ_ALIAS,
        index_prefix=RESEARCH_INDEX_PREFIX,
        shards=1,
        replicas=0,
        bulk_chunk_size=100,
        bulk_max_chunk_bytes=5 * 1024 * 1024,
        image_digest="sha256:" + ("9" * 64),
    )
    build_id = "a" * 64
    index = gold_index_name(RESEARCH_INDEX_PREFIX, build_id)
    build = GoldIndexBuildManifest(
        build_id=build_id,
        release_plan_id=plan.release_plan_id,
        release_commit=commit_ref,
        table_snapshot_ids=table_snapshot_ids,
        mapping_digest=MAPPING_DIGEST,
        source_policy_digest=source_policy.digest,
        config_identity=config,
        config_digest=config.digest,
        document_count=15,
        gold_entity_count=15,
        successful_document_count=15,
        concrete_index_document_count=15,
        partition_receipt_count=1,
        partition_receipt_digest="sha256:" + ("b" * 64),
        index=index,
        alias=RESEARCH_READ_ALIAS,
        completed_at=TIMESTAMP,
    )
    build_ref = _model_ref(
        "index-build",
        model=build,
        media_type=GOLD_INDEX_BUILD_MANIFEST_MEDIA_TYPE,
    )
    validation = GoldIndexValidationReceipt(
        build_manifest=build_ref,
        build_id=build_id,
        release_plan_id=plan.release_plan_id,
        mapping_digest=MAPPING_DIGEST,
        config_digest=config.digest,
        index=index,
        alias=RESEARCH_READ_ALIAS,
        document_count=15,
        validated_at=TIMESTAMP,
    )
    validation_ref = _model_ref(
        "index-validation",
        model=validation,
        media_type=GOLD_INDEX_VALIDATION_RECEIPT_MEDIA_TYPE,
    )
    values = {
        "source_snapshots": (news.captured.pin, learning.captured.pin),
        "slice_source_pins": (
            ReferenceSliceSourcePin(
                reference_slice=ReferenceSlice.NEWS_JOURNALISM,
                source_snapshot_pin_id=news.captured.pin.pin_id,
            ),
            ReferenceSliceSourcePin(
                reference_slice=ReferenceSlice.LEARNING_INSTRUCTION,
                source_snapshot_pin_id=learning.captured.pin.pin_id,
            ),
        ),
        "created_at": TIMESTAMP,
    }
    release = build_global_reference_release_manifest(
        gold_plan=plan,
        gold_plan_reference=plan_ref,
        gold_commit=commit,
        gold_commit_reference=commit_ref,
        index_build=build,
        index_validation=validation,
        index_build_manifest=build_ref,
        index_validation_receipt=validation_ref,
        **values,
    )
    replayed = build_global_reference_release_manifest(
        gold_plan=plan,
        gold_plan_reference=plan_ref,
        gold_commit=commit,
        gold_commit_reference=commit_ref,
        index_build=build,
        index_validation=validation,
        index_build_manifest=build_ref,
        index_validation_receipt=validation_ref,
        **values,
    )
    with pytest.raises(ValueError, match="ObjectRef does not bind"):
        build_global_reference_release_manifest(
            gold_plan=plan,
            gold_plan_reference=_ref(
                "wrong-gold-plan",
                media_type=GOLD_RELEASE_PLAN_MEDIA_TYPE,
                checksum="c",
            ),
            gold_commit=commit,
            gold_commit_reference=commit_ref,
            index_build=build,
            index_validation=validation,
            index_build_manifest=build_ref,
            index_validation_receipt=validation_ref,
            **values,
        )

    assert release == replayed
    assert release.source_policy_digest == SOURCE_POLICY_DIGEST
    assert set(release.capabilities) == set(ReferenceCapability)
    assert release.tenant_world_identity_authority is False
    assert release.world_zero_write_attestation.model_dump() == {
        "tenant_canonical_write_count": 0,
        "world_materialization_count": 0,
        "world_publication_count": 0,
        "alias_activation_count": 0,
    }
    rendered = release.model_dump(mode="json", by_alias=True, exclude_none=True)
    release_schema = json.loads(
        Path("contracts/catalog/global-reference-release.v1.schema.json").read_text(
            encoding="utf-8"
        )
    )
    Draft202012Validator.check_schema(release_schema)
    Draft202012Validator(release_schema).validate(rendered)
    assert set(release_schema["required"]) <= set(rendered)
    assert "activationReceipt" not in json.dumps(rendered)
    with pytest.raises(ValidationError, match="extra"):
        type(release).model_validate({**rendered, "worldPublicationCapability": True})
    with pytest.raises(ValidationError, match="integer zero"):
        type(release).model_validate(
            {
                **rendered,
                "worldZeroWriteAttestation": {
                    **rendered["worldZeroWriteAttestation"],
                    "worldPublicationCount": False,
                },
            }
        )
    bundle = {
        "manifest": {
            "sourceSnapshots": [
                item.model_dump(mode="json", by_alias=True)
                for item in values["source_snapshots"]
            ],
            "sliceSourcePins": [
                item.model_dump(mode="json", by_alias=True)
                for item in values["slice_source_pins"]
            ],
            "goldReleasePlan": plan_ref.model_dump(
                mode="json",
                by_alias=True,
                exclude_none=True,
            ),
            "goldReleaseCommit": commit_ref.model_dump(
                mode="json",
                by_alias=True,
                exclude_none=True,
            ),
            "createdAt": TIMESTAMP,
        },
        "goldReleasePlan": plan.model_dump(mode="json", by_alias=True),
        "goldReleaseCommit": commit.model_dump(mode="json", by_alias=True),
        "indexBuild": build.model_dump(mode="json", by_alias=True),
        "indexValidation": validation.model_dump(mode="json", by_alias=True),
        "indexBuildManifest": build_ref.model_dump(
            mode="json",
            by_alias=True,
            exclude_none=True,
        ),
        "indexValidationReceipt": validation_ref.model_dump(
            mode="json",
            by_alias=True,
            exclude_none=True,
        ),
    }
    bundle_path = tmp_path / "release-bundle.json"
    bundle_path.write_text(json.dumps(bundle), encoding="utf-8")
    assert _build_release(str(bundle_path))["releaseId"] == release.release_id


def test_source_policy_v1_boundary_is_unchanged_and_has_no_v2() -> None:
    policy = load_catalog_source_policy()
    coordinates = _coordinates()
    release_schema = json.loads(
        Path("contracts/catalog/global-reference-release.v1.schema.json").read_text(
            encoding="utf-8"
        )
    )

    assert policy.digest == SOURCE_POLICY_DIGEST
    assert coordinates["sourcePolicy"]["digest"] == SOURCE_POLICY_DIGEST
    assert coordinates["searchProjection"]["mappingDigest"] == MAPPING_DIGEST
    assert coordinates["release"]["capabilities"] == [
        "DISPLAY",
        "IDENTITY",
        "RESEARCH",
    ]
    assert policy.canonical_registry.schema_version.endswith(".v1")
    assert not Path("contracts/catalog/catalog-source-policy.v2.json").exists()
    schema_text = json.dumps(release_schema, sort_keys=True)
    assert "MATERIALIZATION" not in schema_text
    assert "PUBLICATION" not in schema_text
