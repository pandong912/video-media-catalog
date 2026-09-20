from __future__ import annotations

import pytest
from pydantic import ValidationError

from video_media_catalog.assertions import SourceNodeRef
from video_media_catalog.community_ingest import IngestRunKind
from video_media_catalog.community_snapshot import SILVER_SNAPSHOT_MEDIA_TYPE
from video_media_catalog.identity_curation import (
    CurationEntityAllocation,
    CurationSplitAssignment,
    IdentityCurationAction,
    IdentityCurationOperation,
    IdentityCurationState,
    PinnedSilverSnapshot,
    build_identity_curation_manifest,
    materialize_identity_curation,
)
from video_media_catalog.identity_v2 import (
    EntityLevel,
    allocate_entity,
    build_entity_membership,
    build_entity_redirect,
    build_identity_conflict,
)
from video_media_catalog.models import Checksum, ObjectRef

TIMESTAMP = "2026-09-20T00:00:00Z"
EARLIER = "2026-09-19T00:00:00Z"
UUID7_A = "01a081e8-6420-7000-8000-000000000202"
UUID7_B = "01a081e8-6420-7000-8000-000000000203"
UUID7_C = "01a081e8-6420-7000-8000-000000000204"


def _digest(character: str) -> str:
    return "sha256:" + (character * 64)


def _node(source_id: str) -> SourceNodeRef:
    return SourceNodeRef(
        namespace_id="tvmaze-show",
        source_id=source_id,
        referent_kind="SERIES",
    )


def _entity(allocation_id: str, *, created_at: str = EARLIER):
    return allocate_entity(
        allocation_id=allocation_id,
        entity_level=EntityLevel.SERIES,
        entity_kind="TV_SERIES",
        created_at=created_at,
    )


def _pinned() -> PinnedSilverSnapshot:
    return PinnedSilverSnapshot(
        object=ObjectRef(
            uri="file:///tmp/community-silver.json",
            format="OBJECT_FORMAT_JSON",
            media_type=SILVER_SNAPSHOT_MEDIA_TYPE,
            checksum=Checksum(value="a" * 64),
            size_bytes=100,
        ),
        snapshot_set_id=_digest("b"),
    )


def _conflict(
    source_node: SourceNodeRef,
    candidates: tuple[str, ...],
):
    return build_identity_conflict(
        materialization_id=_digest("c"),
        source_node=source_node,
        candidate_entity_keys=candidates,
        assertion_keys=(_digest("d"),),
        reason="MULTIPLE_EXACT_IDENTIFIER_CANDIDATES",
        observed_at=EARLIER,
        policy_id="internal-key-continuity",
        policy_digest=_digest("e"),
    )


def _manifest(operation: IdentityCurationOperation):
    return build_identity_curation_manifest(
        pinned_silver_snapshot=_pinned(),
        operations=(operation,),
        operator_subject="reviewer-123",
        reason="manual identity review",
        operated_at=TIMESTAMP,
        config_digest=_digest("f"),
        image_digest=_digest("1"),
    )


def _operation(
    action: IdentityCurationAction,
    conflict,
    **values,
) -> IdentityCurationOperation:
    return IdentityCurationOperation(
        action=action,
        conflict_key=conflict.conflict_key,
        source_node=conflict.source_node,
        assertion_keys=conflict.assertion_keys,
        **values,
    )


def test_accept_and_reject_are_deterministic_and_replayable() -> None:
    first = _entity(UUID7_A)
    second = _entity(UUID7_B)
    conflict = _conflict(_node("unresolved"), (first.entity_key, second.entity_key))
    state = IdentityCurationState(
        conflicts=(conflict,),
        entities=(first, second),
    )
    accepted = _manifest(
        _operation(
            IdentityCurationAction.ACCEPT,
            conflict,
            candidate_entity_key=first.entity_key,
        )
    )
    first_result = materialize_identity_curation(accepted, state)
    second_result = materialize_identity_curation(accepted, state)

    assert first_result == second_result
    assert tuple(item.decision_id for item in first_result.decisions) == (
        accepted.decision_keys
    )
    assert first_result.memberships[0].entity_key == first.entity_key
    assert first_result.evidence[0].details["conflictKey"] == conflict.conflict_key
    assert type(accepted).model_validate_json(accepted.json_bytes()) == accepted
    with pytest.raises(ValueError, match="already has a committed review"):
        materialize_identity_curation(
            accepted,
            IdentityCurationState(
                conflicts=(conflict,),
                entities=(first, second),
                evidence=first_result.evidence,
            ),
        )

    rejected = _manifest(
        _operation(
            IdentityCurationAction.REJECT,
            conflict,
            candidate_entity_key=second.entity_key,
        )
    )
    rejected_result = materialize_identity_curation(rejected, state)
    assert rejected_result.decisions[0].status.value == "REJECT"
    assert rejected_result.memberships == ()
    assert IngestRunKind.IDENTITY_CURATION.value == "IDENTITY_CURATION"


def test_conflict_and_candidate_validation_fail_closed() -> None:
    series = _entity(UUID7_A)
    conflict = _conflict(_node("unresolved"), (series.entity_key,))
    manifest = _manifest(
        _operation(
            IdentityCurationAction.ACCEPT,
            conflict,
            candidate_entity_key=series.entity_key,
        )
    )
    with pytest.raises(ValueError, match="absent from the pinned"):
        materialize_identity_curation(
            manifest,
            IdentityCurationState(conflicts=(), entities=(series,)),
        )

    agent = allocate_entity(
        allocation_id=UUID7_B,
        entity_level=EntityLevel.AGENT,
        entity_kind="PERSON",
        created_at=EARLIER,
    )
    incompatible_conflict = _conflict(_node("agent"), (agent.entity_key,))
    incompatible = _manifest(
        _operation(
            IdentityCurationAction.ACCEPT,
            incompatible_conflict,
            candidate_entity_key=agent.entity_key,
        )
    )
    with pytest.raises(ValueError, match="types are incompatible"):
        materialize_identity_curation(
            incompatible,
            IdentityCurationState(
                conflicts=(incompatible_conflict,),
                entities=(agent,),
            ),
        )


def test_merge_uses_stable_survivor_and_emits_redirect() -> None:
    survivor = _entity(UUID7_A, created_at="2026-09-18T00:00:00Z")
    retired = _entity(UUID7_B)
    conflict = _conflict(
        _node("merge"),
        (retired.entity_key, survivor.entity_key),
    )
    operation = _operation(
        IdentityCurationAction.MERGE,
        conflict,
        entity_keys=(retired.entity_key, survivor.entity_key),
        expected_survivor_entity_key=survivor.entity_key,
    )
    result = materialize_identity_curation(
        _manifest(operation),
        IdentityCurationState(
            conflicts=(conflict,),
            entities=(retired, survivor),
        ),
    )

    assert result.merge_events[0].survivor_entity_key == survivor.entity_key
    assert result.redirects[0].source_entity_key == retired.entity_key
    assert result.redirects[0].target_entity_key == survivor.entity_key
    assert result.memberships[0].entity_key == survivor.entity_key

    wrong_survivor = _manifest(
        _operation(
            IdentityCurationAction.MERGE,
            conflict,
            entity_keys=(retired.entity_key, survivor.entity_key),
            expected_survivor_entity_key=retired.entity_key,
        )
    )
    with pytest.raises(ValueError, match="stable survivor"):
        materialize_identity_curation(
            wrong_survivor,
            IdentityCurationState(
                conflicts=(conflict,),
                entities=(retired, survivor),
            ),
        )


def test_redirect_rejects_a_cycle_with_pinned_history() -> None:
    first = _entity(UUID7_A)
    second = _entity(UUID7_B)
    conflict = _conflict(_node("redirect"), (first.entity_key, second.entity_key))
    operation = _operation(
        IdentityCurationAction.REDIRECT,
        conflict,
        source_entity_key=first.entity_key,
        target_entity_key=second.entity_key,
    )
    existing = build_entity_redirect(
        source_entity_key=second.entity_key,
        target_entity_key=first.entity_key,
        effective_at=EARLIER,
        decision_id=_digest("9"),
    )

    with pytest.raises(ValueError, match="cycle"):
        materialize_identity_curation(
            _manifest(operation),
            IdentityCurationState(
                conflicts=(conflict,),
                entities=(first, second),
                redirects=(existing,),
            ),
        )


def test_split_closes_old_memberships_and_covers_every_source_once() -> None:
    source = _entity(UUID7_A)
    new_target = _entity(UUID7_C)
    first_node = _node("1")
    second_node = _node("2")
    conflict_node = _node("3")
    conflict = _conflict(conflict_node, (source.entity_key,))
    old_memberships = (
        build_entity_membership(
            source_node=first_node,
            entity_key=source.entity_key,
            decision_id=_digest("4"),
            valid_from=EARLIER,
        ),
        build_entity_membership(
            source_node=second_node,
            entity_key=source.entity_key,
            decision_id=_digest("5"),
            valid_from=EARLIER,
        ),
    )
    operation = _operation(
        IdentityCurationAction.SPLIT,
        conflict,
        source_entity_key=source.entity_key,
        split_assignments=(
            CurationSplitAssignment(
                source_node=first_node,
                target_entity_key=source.entity_key,
            ),
            CurationSplitAssignment(
                source_node=second_node,
                target_entity_key=new_target.entity_key,
            ),
            CurationSplitAssignment(
                source_node=conflict_node,
                target_entity_key=new_target.entity_key,
            ),
        ),
        new_entities=(
            CurationEntityAllocation(
                entity_key=new_target.entity_key,
                allocation_id=UUID7_C,
                entity_level=EntityLevel.SERIES,
                entity_kind="TV_SERIES",
            ),
        ),
    )
    result = materialize_identity_curation(
        _manifest(operation),
        IdentityCurationState(
            conflicts=(conflict,),
            entities=(source,),
            memberships=old_memberships,
        ),
    )

    assert result.entities[0].entity_key == new_target.entity_key
    assert result.entities[0].created_at == TIMESTAMP
    assert len(result.decisions) == 3
    assert len(result.memberships) == 5
    assert sum(item.valid_to == TIMESTAMP for item in result.memberships) == 2
    assert result.split_events[0].target_entity_keys == tuple(
        sorted((source.entity_key, new_target.entity_key))
    )

    incomplete = _manifest(
        _operation(
            IdentityCurationAction.SPLIT,
            conflict,
            source_entity_key=source.entity_key,
            split_assignments=(
                CurationSplitAssignment(
                    source_node=first_node,
                    target_entity_key=source.entity_key,
                ),
                CurationSplitAssignment(
                    source_node=conflict_node,
                    target_entity_key=new_target.entity_key,
                ),
            ),
            new_entities=operation.new_entities,
        )
    )
    with pytest.raises(ValueError, match="cover each current member"):
        materialize_identity_curation(
            incomplete,
            IdentityCurationState(
                conflicts=(conflict,),
                entities=(source,),
                memberships=old_memberships,
            ),
        )


def test_split_contract_rejects_duplicate_source_assignment() -> None:
    source = _entity(UUID7_A)
    target = _entity(UUID7_B)
    conflict = _conflict(_node("1"), (source.entity_key,))
    with pytest.raises(ValidationError, match="more than once"):
        _operation(
            IdentityCurationAction.SPLIT,
            conflict,
            source_entity_key=source.entity_key,
            split_assignments=(
                CurationSplitAssignment(
                    source_node=conflict.source_node,
                    target_entity_key=source.entity_key,
                ),
                CurationSplitAssignment(
                    source_node=conflict.source_node,
                    target_entity_key=target.entity_key,
                ),
            ),
        )
