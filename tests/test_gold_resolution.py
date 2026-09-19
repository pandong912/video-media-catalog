from __future__ import annotations

import pytest

from video_media_catalog.assertions import (
    AssertionProvenance,
    SourceNodeRef,
    ValueType,
    build_field_assertion,
    build_identifier_assertion,
    build_relationship_assertion,
)
from video_media_catalog.community_release import ReleasePolicyContext
from video_media_catalog.gold import (
    GoldResolutionStatus,
    build_gold_release_plan,
    community_display_policy,
)
from video_media_catalog.gold_resolution import resolve_gold_draft
from video_media_catalog.identity_resolution import (
    build_identity_index,
    resolve_or_allocate_source_node,
)
from video_media_catalog.identity_v2 import EntityLevel
from video_media_catalog.rights import (
    PolicyZone,
    RightsProfile,
    UsageAction,
)
from video_media_catalog.tvmaze import tvmaze_rights_profile

TIMESTAMP = "2026-09-19T00:00:00Z"


def _node(value: str) -> SourceNodeRef:
    return SourceNodeRef(
        namespace_id="tvmaze-show",
        source_id=value,
        referent_kind="SERIES",
    )


def _resolved(node: SourceNodeRef):
    return resolve_or_allocate_source_node(
        source_node=node,
        entity_level=EntityLevel.SERIES,
        entity_kind="TV_SERIES",
        exact_candidate_entity_keys=(),
        assertion_keys=("sha256:" + (node.source_id * 64),),
        observed_at=TIMESTAMP,
        policy_id="tvmaze-api-cc-by-sa",
        policy_digest=tvmaze_rights_profile().digest,
        decision_policy_version="1",
        decided_by="test-resolver",
    )


def _provenance(node: SourceNodeRef, path: str) -> AssertionProvenance:
    policy = tvmaze_rights_profile()
    return AssertionProvenance(
        envelope_key="sha256:" + (node.source_id * 64),
        source_path=path,
        mapper_id="tvmaze-show-mapper",
        mapper_version="1",
        policy_id=policy.policy_id,
        policy_digest=policy.digest,
        observed_at=TIMESTAMP,
    )


def _context() -> ReleasePolicyContext:
    return ReleasePolicyContext(
        context_id="public-sharealike",
        audience="public",
        purpose="catalog",
        as_of=TIMESTAMP,
        allowed_zones=(PolicyZone.OPEN_SHAREALIKE,),
    )


def test_gold_resolution_selects_sets_and_preserves_conflicts() -> None:
    first_node = _node("1")
    second_node = _node("2")
    first = _resolved(first_node)
    second = _resolved(second_node)
    index = build_identity_index(
        entities=(*first.entities, *second.entities),
        memberships=(*first.memberships, *second.memberships),
        redirects=(),
        as_of=TIMESTAMP,
    )
    fields = (
        build_field_assertion(
            subject=first_node,
            predicate="title",
            value_type=ValueType.STRING,
            value="Example",
            qualifiers={"language": "en", "titleRole": "PRIMARY"},
            provenance=_provenance(first_node, "/name"),
        ),
        build_field_assertion(
            subject=first_node,
            predicate="genre",
            value_type=ValueType.STRING,
            value="Drama",
            qualifiers={"vocabulary": "tvmaze"},
            provenance=_provenance(first_node, "/genres/0"),
        ),
        build_field_assertion(
            subject=first_node,
            predicate="genre",
            value_type=ValueType.STRING,
            value="Mystery",
            qualifiers={"vocabulary": "tvmaze"},
            provenance=_provenance(first_node, "/genres/1"),
        ),
        build_field_assertion(
            subject=first_node,
            predicate="status",
            value_type=ValueType.STRING,
            value="Running",
            provenance=_provenance(first_node, "/status/0"),
        ),
        build_field_assertion(
            subject=first_node,
            predicate="status",
            value_type=ValueType.STRING,
            value="Ended",
            provenance=_provenance(first_node, "/status/1"),
        ),
    )
    identifiers = (
        build_identifier_assertion(
            subject=first_node,
            namespace_id="imdb-title",
            value="tt0000001",
            issuer="IMDb",
            referent_kind="SERIES",
            provenance=_provenance(first_node, "/externals/imdb"),
        ),
    )
    relations = (
        build_relationship_assertion(
            subject=first_node,
            predicate="related_to",
            object=second_node,
            provenance=_provenance(first_node, "/related/0"),
        ),
    )
    policy = community_display_policy().model_copy(
        update={"max_conflict_ratio": 1.0, "max_unresolved_identity_ratio": 1.0}
    )
    draft = resolve_gold_draft(
        identity_index=index,
        field_assertions=fields,
        identifier_assertions=identifiers,
        relationship_assertions=relations,
        rights_profiles=(tvmaze_rights_profile(),),
        policy_context=_context(),
        field_policy=policy,
    )
    draft.validate_quality(policy)
    with pytest.raises(ValueError, match="conflict ratio"):
        draft.validate_quality(community_display_policy())
    assert len(draft.conflicts) == 1
    assert sum(field.status == GoldResolutionStatus.SET for field in draft.fields) == 2
    assert draft.identifiers[0].value == "tt0000001"
    assert len(draft.relations) == 1

    plan = build_gold_release_plan(
        policy_context=_context(),
        committed_run_ids=("sha256:" + ("a" * 64),),
        silver_snapshot_ids={"community_field_assertion": 10},
        identity_snapshot_ids={"community_entity_membership": 11},
        rights_registry_digest="sha256:" + ("b" * 64),
        field_policy_digest=policy.digest,
        resolver_digest="sha256:" + ("c" * 64),
        image_digest="sha256:" + ("d" * 64),
        config_digest="sha256:" + ("e" * 64),
        expected_counts=draft.expected_counts,
        planned_at=TIMESTAMP,
    )
    rows = draft.materialize(plan, index)
    assert len(rows["community_gold_entity"]) == 2
    assert len(rows["community_gold_conflict"]) == 1


def test_gold_resolution_fails_closed_on_policy_digest_mismatch() -> None:
    node = _node("1")
    resolved = _resolved(node)
    index = build_identity_index(
        entities=resolved.entities,
        memberships=resolved.memberships,
        redirects=(),
        as_of=TIMESTAMP,
    )
    assertion = build_field_assertion(
        subject=node,
        predicate="title",
        value_type=ValueType.STRING,
        value="Example",
        provenance=_provenance(node, "/name").model_copy(
            update={"policy_digest": "sha256:" + ("f" * 64)}
        ),
    )
    with pytest.raises(ValueError, match="policy digest"):
        resolve_gold_draft(
            identity_index=index,
            field_assertions=(assertion,),
            identifier_assertions=(),
            relationship_assertions=(),
            rights_profiles=(tvmaze_rights_profile(),),
            policy_context=_context(),
            field_policy=community_display_policy(),
        )


def test_gold_resolution_withholds_expired_leased_assertion() -> None:
    node = _node("1")
    resolved = _resolved(node)
    index = build_identity_index(
        entities=resolved.entities,
        memberships=resolved.memberships,
        redirects=(),
        as_of=TIMESTAMP,
    )
    profile = RightsProfile(
        policy_id="leased-community",
        policy_version="1",
        zone=PolicyZone.FEDERATED_EPHEMERAL,
        license_id="API-TERMS",
        terms_url="https://example.com/terms",
        permissions=(
            UsageAction.STORE,
            UsageAction.DISPLAY,
            UsageAction.SEARCH,
        ),
        audiences=("public",),
        max_cache_age_days=30,
        purge_on_termination=True,
    )
    assertion = build_field_assertion(
        subject=node,
        predicate="title",
        value_type=ValueType.STRING,
        value="Expired",
        provenance=AssertionProvenance(
            envelope_key="sha256:" + ("1" * 64),
            source_path="/name",
            mapper_id="leased-mapper",
            mapper_version="1",
            policy_id=profile.policy_id,
            policy_digest=profile.digest,
            observed_at="2026-08-01T00:00:00Z",
        ),
    )
    context = ReleasePolicyContext(
        context_id="leased-public",
        audience="public",
        purpose="catalog",
        as_of=TIMESTAMP,
        allowed_zones=(PolicyZone.FEDERATED_EPHEMERAL,),
    )
    draft = resolve_gold_draft(
        identity_index=index,
        field_assertions=(assertion,),
        identifier_assertions=(),
        relationship_assertions=(),
        rights_profiles=(profile,),
        policy_context=context,
        field_policy=community_display_policy(),
    )
    assert not draft.fields
    assert draft.withheld_assertion_count == 1
