from __future__ import annotations

import pytest

from video_media_catalog.assertions import SourceNodeRef
from video_media_catalog.identity_resolution import (
    build_identity_index,
    resolve_or_allocate_source_node,
)
from video_media_catalog.identity_v2 import (
    DecisionStatus,
    EntityLevel,
    build_entity_membership,
    build_entity_redirect,
    build_identity_decision,
    import_v1_entity,
)

TIMESTAMP = "2026-09-19T00:00:00Z"


def _node(value: str = "1") -> SourceNodeRef:
    return SourceNodeRef(
        namespace_id="tvmaze-show",
        source_id=value,
        referent_kind="SERIES",
    )


def _entity(value: str):
    return import_v1_entity(
        entity_key="sha256:" + (value * 64),
        entity_level=EntityLevel.SERIES,
        entity_kind="TV_SERIES",
        created_at=TIMESTAMP,
    )


def test_identity_resolution_accepts_one_exact_candidate() -> None:
    entity = _entity("1")
    result = resolve_or_allocate_source_node(
        source_node=_node(),
        entity_level=EntityLevel.SERIES,
        entity_kind="TV_SERIES",
        exact_candidate_entity_keys=(entity.entity_key,),
        assertion_keys=("sha256:" + ("a" * 64),),
        observed_at=TIMESTAMP,
        policy_id="tvmaze-api-cc-by-sa",
        policy_digest="sha256:" + ("b" * 64),
        decision_policy_version="1",
        decided_by="exact-resolver-v1",
    )
    assert not result.entities
    assert result.memberships[0].entity_key == entity.entity_key
    assert not result.conflicts


def test_identity_resolution_allocates_unmatched_source_once() -> None:
    first = resolve_or_allocate_source_node(
        source_node=_node(),
        entity_level=EntityLevel.SERIES,
        entity_kind="TV_SERIES",
        exact_candidate_entity_keys=(),
        assertion_keys=("sha256:" + ("a" * 64),),
        observed_at=TIMESTAMP,
        policy_id="tvmaze-api-cc-by-sa",
        policy_digest="sha256:" + ("b" * 64),
        decision_policy_version="1",
        decided_by="bootstrap-resolver-v1",
    )
    second = resolve_or_allocate_source_node(
        source_node=_node(),
        entity_level=EntityLevel.SERIES,
        entity_kind="TV_SERIES",
        exact_candidate_entity_keys=(),
        assertion_keys=("sha256:" + ("a" * 64),),
        observed_at=TIMESTAMP,
        policy_id="tvmaze-api-cc-by-sa",
        policy_digest="sha256:" + ("b" * 64),
        decision_policy_version="1",
        decided_by="bootstrap-resolver-v1",
    )
    assert first == second
    assert first.entities[0].entity_key == first.memberships[0].entity_key


def test_identity_resolution_quarantines_multiple_exact_candidates() -> None:
    result = resolve_or_allocate_source_node(
        source_node=_node(),
        entity_level=EntityLevel.SERIES,
        entity_kind="TV_SERIES",
        exact_candidate_entity_keys=(
            "sha256:" + ("1" * 64),
            "sha256:" + ("2" * 64),
        ),
        assertion_keys=("sha256:" + ("a" * 64),),
        observed_at=TIMESTAMP,
        policy_id="tvmaze-api-cc-by-sa",
        policy_digest="sha256:" + ("b" * 64),
        decision_policy_version="1",
        decided_by="exact-resolver-v1",
    )
    assert result.conflicts
    assert not result.memberships


def test_identity_index_resolves_redirect_and_rejects_ambiguity() -> None:
    first = _entity("1")
    second = _entity("2")
    decision = build_identity_decision(
        status=DecisionStatus.ACCEPT,
        source_node=_node(),
        entity_key=first.entity_key,
        evidence_keys=("sha256:" + ("a" * 64),),
        policy_version="1",
        decided_by="test",
        decided_at=TIMESTAMP,
        reason="test",
    )
    membership = build_entity_membership(
        source_node=_node(),
        entity_key=first.entity_key,
        decision_id=decision.decision_id,
        valid_from=TIMESTAMP,
    )
    redirect = build_entity_redirect(
        source_entity_key=first.entity_key,
        target_entity_key=second.entity_key,
        effective_at=TIMESTAMP,
        decision_id=decision.decision_id,
    )
    index = build_identity_index(
        entities=(first, second),
        memberships=(membership,),
        redirects=(redirect,),
        as_of=TIMESTAMP,
    )
    assert index.resolve_source_node(_node()) == second.entity_key

    competing = build_entity_membership(
        source_node=_node(),
        entity_key=second.entity_key,
        decision_id=decision.decision_id,
        valid_from=TIMESTAMP,
    )
    with pytest.raises(ValueError, match="multiple active"):
        build_identity_index(
            entities=(first, second),
            memberships=(membership, competing),
            redirects=(),
            as_of=TIMESTAMP,
        )
