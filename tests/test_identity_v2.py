from __future__ import annotations

import pytest

from video_media_catalog.assertions import SourceNodeRef
from video_media_catalog.community_rows import (
    entity_merge_event_row,
    entity_split_event_row,
    external_id_index_row,
)
from video_media_catalog.community_tables import DATA_TABLE_COLUMNS
from video_media_catalog.identity_v2 import (
    DecisionStatus,
    EntityLevel,
    EntitySplitAssignment,
    EvidenceKind,
    ParentConstraint,
    allocate_entity,
    allocate_source_entity,
    build_entity_membership,
    build_entity_merge,
    build_entity_redirect,
    build_entity_split_event,
    build_external_id_index_entry,
    build_identity_decision,
    build_identity_evidence,
    build_parent_constrained_evidence,
    resolve_redirect_target,
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


def test_parent_constrained_evidence_binds_parent_and_numbering() -> None:
    child = SourceNodeRef(
        namespace_id="tmdb-tv",
        source_id="200:1:2",
        referent_kind="EPISODE",
    )
    constraint = ParentConstraint(
        child_level=EntityLevel.EPISODE,
        parent_level=EntityLevel.SEASON,
        parent_source_node=SourceNodeRef(
            namespace_id="tmdb-tv",
            source_id="200:1",
            referent_kind="SEASON",
        ),
        parent_entity_key="sha256:" + ("1" * 64),
        parent_membership_key="sha256:" + ("2" * 64),
        relationship_assertion_key="sha256:" + ("3" * 64),
        ordinal_assertion_keys=("sha256:" + ("4" * 64),),
        season_number="1",
        episode_number="2",
    )
    evidence = build_parent_constrained_evidence(
        source_node=child,
        candidate_entity_key="sha256:" + ("5" * 64),
        parent_constraint=constraint,
        assertion_keys=("sha256:" + ("6" * 64),),
        observed_at=TIMESTAMP,
        policy_id="internal-key-continuity",
        policy_digest="sha256:" + ("7" * 64),
        confidence=1.0,
    )
    assert evidence.kind == EvidenceKind.PARENT_CONSTRAINED
    assert set(evidence.assertion_keys) == {
        "sha256:" + ("3" * 64),
        "sha256:" + ("4" * 64),
        "sha256:" + ("6" * 64),
    }
    assert evidence.details["parentConstraint"]["episodeNumber"] == "2"


def test_external_id_index_separates_block_from_entry_identity() -> None:
    first = build_external_id_index_entry(
        materialization_id="sha256:" + ("0" * 64),
        namespace_id="imdb-title",
        normalized_value="TT0000001",
        referent_kind="SERIES",
        entity_key="sha256:" + ("1" * 64),
        assertion_keys=("sha256:" + ("2" * 64),),
        observed_at=TIMESTAMP,
        policy_id="internal-key-continuity",
        policy_digest="sha256:" + ("3" * 64),
    )
    second = build_external_id_index_entry(
        materialization_id="sha256:" + ("0" * 64),
        namespace_id="imdb-title",
        normalized_value="TT0000001",
        referent_kind="SERIES",
        entity_key="sha256:" + ("4" * 64),
        assertion_keys=("sha256:" + ("5" * 64),),
        observed_at=TIMESTAMP,
        policy_id="internal-key-continuity",
        policy_digest="sha256:" + ("3" * 64),
    )
    rematerialized = build_external_id_index_entry(
        materialization_id="sha256:" + ("6" * 64),
        namespace_id="imdb-title",
        normalized_value="TT0000001",
        referent_kind="SERIES",
        entity_key=first.entity_key,
        assertion_keys=first.assertion_keys,
        observed_at=TIMESTAMP,
        policy_id="internal-key-continuity",
        policy_digest="sha256:" + ("3" * 64),
    )
    assert first.blocking_key == second.blocking_key
    assert first.index_entry_key != second.index_entry_key
    assert first.blocking_key == rematerialized.blocking_key
    assert first.index_entry_key != rematerialized.index_entry_key
    row = external_id_index_row("sha256:" + ("f" * 64), first)
    assert set(row) == set(DATA_TABLE_COLUMNS["community_external_id_index"])


def test_merge_emits_stable_survivor_event_and_redirects() -> None:
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
    result = build_entity_merge(
        entries=(second, first),
        decision_id="sha256:" + ("d" * 64),
        effective_at=TIMESTAMP,
        merged_by="reviewer",
        reason="duplicate exact IDs",
    )
    assert result.event.survivor_entity_key == first.entity_key
    assert len(result.redirects) == 1
    assert (
        resolve_redirect_target(second.entity_key, result.redirects) == first.entity_key
    )
    row = entity_merge_event_row("sha256:" + ("f" * 64), result.event)
    assert set(row) == set(DATA_TABLE_COLUMNS["community_entity_merge_event"])


def test_split_event_requires_explicit_source_assignments() -> None:
    first_target = "sha256:" + ("1" * 64)
    second_target = "sha256:" + ("2" * 64)
    event = build_entity_split_event(
        source_entity_key="sha256:" + ("3" * 64),
        assignments=(
            EntitySplitAssignment(
                source_node=_node("1"),
                target_entity_key=first_target,
                decision_id="sha256:" + ("4" * 64),
            ),
            EntitySplitAssignment(
                source_node=_node("2"),
                target_entity_key=second_target,
                decision_id="sha256:" + ("5" * 64),
            ),
        ),
        effective_at=TIMESTAMP,
        split_by="reviewer",
        reason="source nodes describe different series",
    )
    assert event.target_entity_keys == (first_target, second_target)
    assert len(event.assignments) == 2
    row = entity_split_event_row("sha256:" + ("f" * 64), event)
    assert set(row) == set(DATA_TABLE_COLUMNS["community_entity_split_event"])
