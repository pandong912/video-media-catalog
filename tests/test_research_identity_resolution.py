from __future__ import annotations

from video_media_catalog.assertions import SourceNodeRef
from video_media_catalog.identity_resolution import (
    ExactBlockingKey,
    SourceNodeResolutionInput,
    component_ids_for_exact_blocking_keys,
    resolve_exact_blocking_component,
    resolve_or_allocate_source_node,
    review_parent_constrained_source_node,
)
from video_media_catalog.identity_v2 import (
    DecisionStatus,
    EntityLevel,
    ParentConstraint,
)

TIMESTAMP = "2026-10-15T00:00:00Z"
POLICY_DIGEST = "sha256:" + ("a" * 64)
ASSERTION = "sha256:" + ("b" * 64)


def _sports_event(source_id: str = "event-1") -> SourceNodeRef:
    return SourceNodeRef(
        namespace_id="openfootball-event",
        source_id=source_id,
        referent_kind="SPORTS_EVENT",
    )


def test_unique_sports_exact_id_accepts_but_ambiguity_abstains() -> None:
    accepted = resolve_or_allocate_source_node(
        source_node=_sports_event(),
        entity_level=EntityLevel.SPORTS_EVENT,
        entity_kind="SPORTS_EVENT",
        exact_candidate_entity_keys=("sha256:" + ("1" * 64),),
        assertion_keys=(ASSERTION,),
        observed_at=TIMESTAMP,
        policy_id="openfootball-cc0",
        policy_digest=POLICY_DIGEST,
        decision_policy_version="research-identity-v1",
        decided_by="research-identity-resolver",
    )
    conflicted = resolve_or_allocate_source_node(
        source_node=_sports_event("event-2"),
        entity_level=EntityLevel.SPORTS_EVENT,
        entity_kind="SPORTS_EVENT",
        exact_candidate_entity_keys=(
            "sha256:" + ("1" * 64),
            "sha256:" + ("2" * 64),
        ),
        assertion_keys=(ASSERTION,),
        observed_at=TIMESTAMP,
        policy_id="openfootball-cc0",
        policy_digest=POLICY_DIGEST,
        decision_policy_version="research-identity-v1",
        decided_by="research-identity-resolver",
    )

    assert len(accepted.memberships) == 1
    assert accepted.evidence[0].confidence == 1.0
    assert conflicted.conflicts[0].reason == "MULTIPLE_EXACT_IDENTIFIER_CANDIDATES"
    assert conflicted.memberships == ()
    assert conflicted.decisions == ()


def test_sports_parent_constrained_match_is_review_only() -> None:
    participant = SourceNodeRef(
        namespace_id="openfootball-team",
        source_id="arsenal",
        referent_kind="SPORTS_PARTICIPANT",
    )
    constraint = ParentConstraint(
        child_level=EntityLevel.SPORTS_EVENT,
        parent_level=EntityLevel.SPORTS_SEASON,
        parent_source_node=SourceNodeRef(
            namespace_id="openfootball-season",
            source_id="eng.1/2025-26",
            referent_kind="SPORTS_SEASON",
        ),
        parent_entity_key="sha256:" + ("3" * 64),
        parent_membership_key="sha256:" + ("4" * 64),
        relationship_assertion_key="sha256:" + ("5" * 64),
        ordinal_assertion_keys=("sha256:" + ("6" * 64),),
        event_date="2026-10-15",
        participant_source_nodes=(participant,),
    )
    result = review_parent_constrained_source_node(
        source_node=_sports_event(),
        candidate_entity_key="sha256:" + ("7" * 64),
        parent_constraint=constraint,
        assertion_keys=(ASSERTION,),
        observed_at=TIMESTAMP,
        policy_id="openfootball-cc0",
        policy_digest=POLICY_DIGEST,
        decision_policy_version="research-identity-v1",
        decided_by="research-identity-resolver",
    )

    assert result.decisions[0].status == DecisionStatus.UNCERTAIN
    assert result.memberships == ()
    assert result.entities == ()


def test_animation_exact_cross_id_reuses_film_work_component() -> None:
    film_node = SourceNodeRef(
        namespace_id="imdb-title",
        source_id="tt1234567",
        referent_kind="EDITORIAL_WORK",
    )
    animation_node = SourceNodeRef(
        namespace_id="wikidata-item",
        source_id="Q200000001",
        referent_kind="EDITORIAL_WORK",
    )
    candidate = "sha256:" + ("8" * 64)
    common = {
        "entity_level": EntityLevel.EDITORIAL_WORK,
        "entity_kind": "EDITORIAL_WORK",
        "exact_candidate_entity_keys": (candidate,),
        "assertion_keys": (ASSERTION,),
        "observed_at": TIMESTAMP,
        "policy_id": "wikidata-structured-data-cc0",
        "policy_digest": POLICY_DIGEST,
    }
    results = resolve_exact_blocking_component(
        (
            SourceNodeResolutionInput(source_node=film_node, **common),
            SourceNodeResolutionInput(source_node=animation_node, **common),
        ),
        decision_policy_version="research-identity-v1",
        decided_by="research-identity-resolver",
    )

    assert len(results) == 2
    assert {result.memberships[0].entity_key for result in results} == {candidate}


def test_characters_do_not_merge_from_equal_names_without_exact_id() -> None:
    first = SourceNodeRef(
        namespace_id="madb-character",
        source_id="character-1",
        referent_kind="CHARACTER",
    )
    second = SourceNodeRef(
        namespace_id="wikidata-item",
        source_id="Q200000003",
        referent_kind="CHARACTER",
    )
    blocks = component_ids_for_exact_blocking_keys(
        {
            "first": (),
            "second": (),
        }
    )
    exact_blocks = component_ids_for_exact_blocking_keys(
        {
            "first": (ExactBlockingKey("madb-character", "character-1", "CHARACTER"),),
            "second": (ExactBlockingKey("wikidata-item", "Q200000003", "CHARACTER"),),
        }
    )

    assert first.source_id != second.source_id
    assert blocks == {"first": "first", "second": "second"}
    assert exact_blocks == {"first": "first", "second": "second"}
