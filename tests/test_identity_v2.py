from __future__ import annotations

import pytest

from video_media_catalog.assertions import SourceNodeRef
from video_media_catalog.identity_v2 import (
    DecisionStatus,
    EntityLevel,
    EvidenceKind,
    LegacyKeyMap,
    allocate_entity,
    allocate_source_entity,
    build_entity_membership,
    build_entity_redirect,
    build_identity_decision,
    build_identity_evidence,
    import_v1_entity,
    select_merge_survivor,
    validate_redirect_graph,
)

UUID7_A = "01a081e8-6420-7000-8000-000000000202"
UUID7_B = "01a081e8-6420-7000-8000-000000000203"
TIMESTAMP = "2026-09-19T00:00:00Z"


def _node(source_id: str = "1") -> SourceNodeRef:
    return SourceNodeRef(
        namespace_id="tvmaze-show",
        source_id=source_id,
        referent_kind="SERIES",
    )


def test_new_entity_allocation_is_source_independent() -> None:
    entity = allocate_entity(
        allocation_id=UUID7_A,
        entity_level=EntityLevel.SERIES,
        entity_kind="TV_SERIES",
        created_at=TIMESTAMP,
    )
    assert entity.entity_key.startswith("sha256:")
    assert "tvmaze" not in entity.json_bytes().decode()


def test_source_allocation_is_retry_stable_and_then_internal() -> None:
    first = allocate_source_entity(
        source_node=_node(),
        entity_level=EntityLevel.SERIES,
        entity_kind="TV_SERIES",
        first_observed_at=TIMESTAMP,
    )
    second = allocate_source_entity(
        source_node=_node(),
        entity_level=EntityLevel.SERIES,
        entity_kind="TV_SERIES",
        first_observed_at=TIMESTAMP,
    )
    assert first == second
    assert first.allocation_id is not None
    assert "tvmaze" not in first.entity_key


def test_v1_import_preserves_published_key_verbatim() -> None:
    legacy_key = "sha256:" + ("a" * 64)
    entity = import_v1_entity(
        entity_key=legacy_key,
        entity_level=EntityLevel.EDITORIAL_WORK,
        entity_kind="MOVIE",
        created_at=TIMESTAMP,
    )
    assert entity.entity_key == legacy_key
    assert entity.imported_v1
    mapping = LegacyKeyMap(
        legacy_key=legacy_key,
        legacy_kind="entity",
        target_key=legacy_key,
        imported_at=TIMESTAMP,
        source_snapshot_set_id=UUID7_A,
    )
    assert mapping.target_key == legacy_key


def test_identity_evidence_decision_and_membership_are_replayable() -> None:
    entity = allocate_entity(
        allocation_id=UUID7_A,
        entity_level=EntityLevel.SERIES,
        entity_kind="TV_SERIES",
        created_at=TIMESTAMP,
    )
    evidence = build_identity_evidence(
        kind=EvidenceKind.EXACT_IDENTIFIER,
        source_node=_node(),
        candidate_entity_key=entity.entity_key,
        assertion_keys=("sha256:" + ("b" * 64),),
        observed_at=TIMESTAMP,
        policy_id="tvmaze-api-cc-by-sa",
        policy_digest="sha256:" + ("c" * 64),
        confidence=0.999,
    )
    decision = build_identity_decision(
        status=DecisionStatus.ACCEPT,
        source_node=_node(),
        entity_key=entity.entity_key,
        evidence_keys=(evidence.evidence_key,),
        policy_version="identity-policy-v1",
        decided_by="identity-resolver-v1",
        decided_at=TIMESTAMP,
        reason="exact compatible identifier",
    )
    membership = build_entity_membership(
        source_node=_node(),
        entity_key=entity.entity_key,
        decision_id=decision.decision_id,
        valid_from=TIMESTAMP,
    )
    assert membership.entity_key == entity.entity_key
    assert evidence == type(evidence).model_validate_json(evidence.json_bytes())
    assert decision == type(decision).model_validate_json(decision.json_bytes())


def test_merge_survivor_preserves_earliest_published_entity() -> None:
    first = allocate_entity(
        allocation_id=UUID7_A,
        entity_level=EntityLevel.SERIES,
        entity_kind="TV_SERIES",
        created_at="2026-09-18T00:00:00Z",
    )
    second = allocate_entity(
        allocation_id=UUID7_B,
        entity_level=EntityLevel.SERIES,
        entity_kind="TV_SERIES",
        created_at=TIMESTAMP,
    )
    assert select_merge_survivor((second, first)) == first


def test_redirect_graph_rejects_cycles() -> None:
    decision = "sha256:" + ("d" * 64)
    first = build_entity_redirect(
        source_entity_key="sha256:" + ("a" * 64),
        target_entity_key="sha256:" + ("b" * 64),
        effective_at=TIMESTAMP,
        decision_id=decision,
    )
    second = build_entity_redirect(
        source_entity_key="sha256:" + ("b" * 64),
        target_entity_key="sha256:" + ("a" * 64),
        effective_at=TIMESTAMP,
        decision_id=decision,
    )
    with pytest.raises(ValueError, match="cycle"):
        validate_redirect_graph((first, second))
