"""Reversible source-node membership resolution for Gold construction."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Self

from video_media_catalog.assertions import SourceNodeRef
from video_media_catalog.canonical import deterministic_key
from video_media_catalog.identity_v2 import (
    EntityLedgerEntry,
    EntityLevel,
    EntityMembership,
    EntityRedirect,
    EvidenceKind,
    IdentityConflict,
    IdentityDecision,
    IdentityEvidence,
    ParentConstraint,
    allocate_source_entity,
    build_accept_decision,
    build_entity_membership,
    build_identity_conflict,
    build_identity_evidence,
    build_parent_constrained_evidence,
    build_reject_decision,
    build_revoke_decision,
    close_entity_membership,
    validate_redirect_graph,
)
from video_media_catalog.v2_contracts import (
    V2ContractModel,
    parse_rfc3339,
    require_sha256,
)


def source_node_key(node: SourceNodeRef) -> tuple[str, str, str]:
    return node.namespace_id, node.source_id, node.referent_kind


_REFERENT_KIND_BY_ENTITY_TYPE = {
    "WORK": "EDITORIAL_WORK",
    "MOVIE": "EDITORIAL_WORK",
    "EDITORIAL_WORK": "EDITORIAL_WORK",
    "SERIES": "SERIES",
    "TV_SERIES": "SERIES",
    "SEASON": "SEASON",
    "TV_SEASON": "SEASON",
    "EPISODE": "EPISODE",
    "TV_EPISODE": "EPISODE",
    "EDIT": "EDIT",
    "MANIFESTATION": "MANIFESTATION",
    "PERSON": "AGENT",
    "AGENT": "AGENT",
    "ORGANIZATION": "ORGANIZATION",
}

_REFERENT_KIND_ALIASES = {
    "MOVIE": "EDITORIAL_WORK",
    "EDITORIAL_WORK": "EDITORIAL_WORK",
    "TV_SERIES": "SERIES",
    "SERIES": "SERIES",
    "TV_SEASON": "SEASON",
    "SEASON": "SEASON",
    "TV_EPISODE": "EPISODE",
    "EPISODE": "EPISODE",
    "PERSON": "AGENT",
    "AGENT": "AGENT",
    **_REFERENT_KIND_BY_ENTITY_TYPE,
}

_EDITORIAL_BLOCKING_KINDS = frozenset(
    {
        "EDITORIAL_WORK",
        "MOVIE",
        "SERIES",
        "SEASON",
        "EPISODE",
        "EDIT",
        "MANIFESTATION",
    }
)
_AGENT_BLOCKING_KINDS = frozenset({"PERSON", "AGENT", "ORGANIZATION"})


def canonical_referent_kind(value: str) -> str:
    """Normalize source-specific kinds to exact-ID blocking domains."""

    normalized = value.strip().upper()
    return _REFERENT_KIND_ALIASES.get(normalized, normalized)


def referent_kind_for_entity_type(entity_type: str) -> str:
    """Derive registry blocking referent kind from a resolved entity type."""

    return canonical_referent_kind(entity_type)


def referent_kinds_compatible(identifier_kind: str, blocking_kind: str) -> bool:
    """Allow safe normalization within one blocking domain, never across domains."""

    identifier = canonical_referent_kind(identifier_kind)
    blocking = canonical_referent_kind(blocking_kind)
    if identifier == blocking:
        return True
    editorial = (
        identifier in _EDITORIAL_BLOCKING_KINDS
        and blocking in _EDITORIAL_BLOCKING_KINDS
    )
    agent = identifier in _AGENT_BLOCKING_KINDS and blocking in _AGENT_BLOCKING_KINDS
    return editorial or agent


def source_node_entity_compatible(
    source_node: SourceNodeRef,
    entity: EntityLedgerEntry,
) -> bool:
    """Require a source node and ledger entity to share one exact type domain."""

    source_kind = canonical_referent_kind(source_node.referent_kind)
    expected_level = {
        "EDITORIAL_WORK": EntityLevel.EDITORIAL_WORK,
        "SERIES": EntityLevel.SERIES,
        "SEASON": EntityLevel.SEASON,
        "EPISODE": EntityLevel.EPISODE,
        "EDIT": EntityLevel.EDIT,
        "MANIFESTATION": EntityLevel.MANIFESTATION,
        "AGENT": EntityLevel.AGENT,
        "ORGANIZATION": EntityLevel.AGENT,
    }.get(source_kind)
    if expected_level is None:
        expected_level = next(
            (
                level
                for level in EntityLevel
                if level != EntityLevel.UNKNOWN and level.value == source_kind
            ),
            None,
        )
    if expected_level is None or entity.entity_level != expected_level:
        return False
    entity_kind = canonical_referent_kind(entity.entity_kind)
    if expected_level == EntityLevel.AGENT:
        return entity_kind == "AGENT" or entity_kind == source_kind
    return entity_kind == source_kind


@dataclass(frozen=True)
class ExactBlockingKey:
    namespace_id: str
    normalized_value: str
    referent_kind: str


@dataclass(frozen=True)
class SourceNodeResolutionInput:
    source_node: SourceNodeRef
    entity_level: EntityLevel
    entity_kind: str
    exact_candidate_entity_keys: tuple[str, ...]
    assertion_keys: tuple[str, ...]
    observed_at: str
    policy_id: str
    policy_digest: str


def _union_find_component_ids(
    node_ids: Iterable[str],
    blocking_links: Iterable[tuple[str, str]],
) -> dict[str, str]:
    """Return deterministic component ids using the lexicographically smallest node."""

    parent = {node_id: node_id for node_id in node_ids}
    if not parent:
        return {}

    def find(node_id: str) -> str:
        root = node_id
        while parent[root] != root:
            root = parent[root]
        while parent[node_id] != node_id:
            next_node = parent[node_id]
            parent[node_id] = root
            node_id = next_node
        return root

    def union(left: str, right: str) -> None:
        left_root = find(left)
        right_root = find(right)
        if left_root == right_root:
            return
        if left_root < right_root:
            parent[right_root] = left_root
        else:
            parent[left_root] = right_root

    for left, right in blocking_links:
        if left in parent and right in parent:
            union(left, right)

    return {node_id: find(node_id) for node_id in parent}


def component_ids_for_exact_blocking_keys(
    node_blocking_keys: dict[str, tuple[ExactBlockingKey, ...]],
) -> dict[str, str]:
    """Group source nodes that share any registry exact blocking key."""

    blocking_links: list[tuple[str, str]] = []
    index_by_key: dict[tuple[str, str, str], list[str]] = {}
    for node_id, keys in node_blocking_keys.items():
        for key in keys:
            bucket = index_by_key.setdefault(
                (key.namespace_id, key.normalized_value, key.referent_kind),
                [],
            )
            for peer in bucket:
                blocking_links.append((node_id, peer))
            bucket.append(node_id)
    return _union_find_component_ids(node_blocking_keys, blocking_links)


def resolve_exact_blocking_component(
    nodes: tuple[SourceNodeResolutionInput, ...],
    *,
    decision_policy_version: str,
    decided_by: str,
    materialization_id: str | None = None,
) -> tuple[IdentityResolutionResult, ...]:
    """Resolve one connected component with unified candidate matching or allocation."""

    ordered = tuple(
        sorted(nodes, key=lambda item: source_node_key(item.source_node)),
    )
    if not ordered:
        return ()
    if len(ordered) == 1:
        node = ordered[0]
        return (
            resolve_or_allocate_source_node(
                source_node=node.source_node,
                entity_level=node.entity_level,
                entity_kind=node.entity_kind,
                exact_candidate_entity_keys=node.exact_candidate_entity_keys,
                assertion_keys=node.assertion_keys,
                observed_at=node.observed_at,
                policy_id=node.policy_id,
                policy_digest=node.policy_digest,
                decision_policy_version=decision_policy_version,
                decided_by=decided_by,
                materialization_id=materialization_id,
            ),
        )

    candidates = tuple(
        sorted(
            {
                require_sha256(item, label="candidate entity key")
                for node in ordered
                for item in node.exact_candidate_entity_keys
            }
        )
    )
    if len(candidates) > 1:
        return tuple(
            IdentityResolutionResult(
                conflicts=(
                    build_identity_conflict(
                        materialization_id=materialization_id,
                        source_node=node.source_node,
                        candidate_entity_keys=candidates,
                        assertion_keys=node.assertion_keys,
                        reason="MULTIPLE_EXACT_IDENTIFIER_CANDIDATES",
                        observed_at=node.observed_at,
                        policy_id=node.policy_id,
                        policy_digest=node.policy_digest,
                    ),
                )
            ).require_consistent()
            for node in ordered
        )

    if candidates:
        entity_key = candidates[0]
        return tuple(
            resolve_or_allocate_source_node(
                source_node=node.source_node,
                entity_level=node.entity_level,
                entity_kind=node.entity_kind,
                exact_candidate_entity_keys=(entity_key,),
                assertion_keys=node.assertion_keys,
                observed_at=node.observed_at,
                policy_id=node.policy_id,
                policy_digest=node.policy_digest,
                decision_policy_version=decision_policy_version,
                decided_by=decided_by,
                materialization_id=materialization_id,
            )
            for node in ordered
        )

    anchor = ordered[0]
    anchor_result = resolve_or_allocate_source_node(
        source_node=anchor.source_node,
        entity_level=anchor.entity_level,
        entity_kind=anchor.entity_kind,
        exact_candidate_entity_keys=(),
        assertion_keys=anchor.assertion_keys,
        observed_at=anchor.observed_at,
        policy_id=anchor.policy_id,
        policy_digest=anchor.policy_digest,
        decision_policy_version=decision_policy_version,
        decided_by=decided_by,
        materialization_id=materialization_id,
    )
    if anchor_result.conflicts:
        return (anchor_result,) * len(ordered)
    entity_key = anchor_result.memberships[0].entity_key
    shared_entities = anchor_result.entities
    results: list[IdentityResolutionResult] = [anchor_result]
    for node in ordered[1:]:
        evidence = build_identity_evidence(
            kind=EvidenceKind.SOURCE_ENTITY_BOOTSTRAP,
            source_node=node.source_node,
            candidate_entity_key=entity_key,
            assertion_keys=node.assertion_keys,
            observed_at=node.observed_at,
            policy_id=node.policy_id,
            policy_digest=node.policy_digest,
            confidence=None,
            details={"sharedAllocationAnchor": source_node_key(anchor.source_node)},
        )
        results.append(
            accept_identity_candidate(
                source_node=node.source_node,
                entity_key=entity_key,
                evidence_keys=(evidence.evidence_key,),
                policy_version=decision_policy_version,
                decided_by=decided_by,
                decided_at=node.observed_at,
                reason="shared exact blocking component allocation",
                entities=(),
                evidence=(evidence,),
            ).require_consistent()
        )
    if shared_entities:
        results[0] = IdentityResolutionResult(
            entities=shared_entities,
            evidence=anchor_result.evidence,
            decisions=anchor_result.decisions,
            memberships=anchor_result.memberships,
        ).require_consistent()
    return tuple(results)


def build_oversized_blocking_component_conflict(
    *,
    source_node: SourceNodeRef,
    component_id: str,
    component_node_count: int,
    assertion_keys: tuple[str, ...],
    observed_at: str,
    policy_id: str,
    policy_digest: str,
    materialization_id: str | None = None,
    max_component_size: int,
) -> IdentityResolutionResult:
    """Fail closed when a connected component exceeds the resolution bound."""

    return IdentityResolutionResult(
        conflicts=(
            build_identity_conflict(
                materialization_id=materialization_id,
                source_node=source_node,
                candidate_entity_keys=(
                    deterministic_key(
                        "oversized-exact-blocking-component-v2",
                        {"componentId": component_id},
                    ),
                ),
                assertion_keys=assertion_keys,
                reason="EXACT_BLOCKING_COMPONENT_TOO_LARGE",
                observed_at=observed_at,
                policy_id=policy_id,
                policy_digest=policy_digest,
                details={
                    "componentId": component_id,
                    "componentNodeCount": component_node_count,
                    "maxComponentSize": max_component_size,
                },
            ),
        )
    ).require_consistent()


def resolve_shared_blocking_member(
    *,
    source_node: SourceNodeRef,
    entity_key: str,
    anchor_source_node: SourceNodeRef,
    assertion_keys: tuple[str, ...],
    observed_at: str,
    policy_id: str,
    policy_digest: str,
    decision_policy_version: str,
    decided_by: str,
) -> IdentityResolutionResult:
    """Attach a non-anchor node to the anchor's shared component allocation."""

    evidence = build_identity_evidence(
        kind=EvidenceKind.SOURCE_ENTITY_BOOTSTRAP,
        source_node=source_node,
        candidate_entity_key=entity_key,
        assertion_keys=assertion_keys,
        observed_at=observed_at,
        policy_id=policy_id,
        policy_digest=policy_digest,
        confidence=None,
        details={"sharedAllocationAnchor": source_node_key(anchor_source_node)},
    )
    return accept_identity_candidate(
        source_node=source_node,
        entity_key=entity_key,
        evidence_keys=(evidence.evidence_key,),
        policy_version=decision_policy_version,
        decided_by=decided_by,
        decided_at=observed_at,
        reason="shared exact blocking component allocation",
        evidence=(evidence,),
    ).require_consistent()


@dataclass(frozen=True)
class IdentityIndex:
    entities: dict[str, EntityLedgerEntry]
    source_memberships: dict[tuple[str, str, str], str]
    redirects: dict[str, str]

    def resolve_entity_key(self, entity_key: str) -> str:
        seen: set[str] = set()
        current = entity_key
        while current in self.redirects:
            if current in seen:
                raise ValueError("identity redirect graph contains a cycle")
            seen.add(current)
            current = self.redirects[current]
        return current

    def resolve_source_node(self, node: SourceNodeRef) -> str | None:
        entity_key = self.source_memberships.get(source_node_key(node))
        return None if entity_key is None else self.resolve_entity_key(entity_key)


def build_identity_index(
    *,
    entities: tuple[EntityLedgerEntry, ...],
    memberships: tuple[EntityMembership, ...],
    redirects: tuple[EntityRedirect, ...],
    as_of: str,
) -> IdentityIndex:
    instant = parse_rfc3339(as_of, label="identity as_of")
    validate_redirect_graph(redirects)
    entity_map = {entity.entity_key: entity for entity in entities}
    if len(entity_map) != len(entities):
        raise ValueError("identity ledger contains duplicate entity keys")
    redirect_map = {
        redirect.source_entity_key: redirect.target_entity_key
        for redirect in redirects
        if parse_rfc3339(redirect.effective_at) <= instant
    }
    for source, target in redirect_map.items():
        if source not in entity_map or target not in entity_map:
            raise ValueError("identity redirect references an unknown entity")

    membership_versions: dict[
        tuple[tuple[str, str, str], str, str, str],
        EntityMembership,
    ] = {}
    for membership in memberships:
        version_key = (
            source_node_key(membership.source_node),
            membership.entity_key,
            membership.decision_id,
            membership.valid_from,
        )
        existing_version = membership_versions.get(version_key)
        if existing_version is None:
            membership_versions[version_key] = membership
        elif existing_version != membership:
            valid_to_values = {
                existing_version.valid_to,
                membership.valid_to,
            }
            closed_values = {value for value in valid_to_values if value is not None}
            if len(closed_values) > 1:
                raise ValueError("membership has conflicting closure times")
            membership_versions[version_key] = (
                membership if membership.valid_to is not None else existing_version
            )

    active: dict[tuple[str, str, str], str] = {}
    for membership in membership_versions.values():
        if parse_rfc3339(membership.valid_from) > instant:
            continue
        if (
            membership.valid_to is not None
            and parse_rfc3339(membership.valid_to) <= instant
        ):
            continue
        key = source_node_key(membership.source_node)
        existing = active.get(key)
        if existing is not None and existing != membership.entity_key:
            raise ValueError("source node has multiple active entity memberships")
        if membership.entity_key not in entity_map:
            raise ValueError("identity membership references an unknown entity")
        active[key] = membership.entity_key
    return IdentityIndex(entity_map, active, redirect_map)


class IdentityResolutionResult(V2ContractModel):
    entities: tuple[EntityLedgerEntry, ...] = ()
    evidence: tuple[IdentityEvidence, ...] = ()
    decisions: tuple[IdentityDecision, ...] = ()
    memberships: tuple[EntityMembership, ...] = ()
    conflicts: tuple[IdentityConflict, ...] = ()

    def require_consistent(self) -> Self:
        if self.conflicts and (self.entities or self.decisions or self.memberships):
            raise ValueError("conflicted identity result cannot assign membership")
        return self


def accept_identity_candidate(
    *,
    source_node: SourceNodeRef,
    entity_key: str,
    evidence_keys: tuple[str, ...],
    policy_version: str,
    decided_by: str,
    decided_at: str,
    reason: str,
    entities: tuple[EntityLedgerEntry, ...] = (),
    evidence: tuple[IdentityEvidence, ...] = (),
) -> IdentityResolutionResult:
    """Accept a candidate and open one effective membership."""

    decision = build_accept_decision(
        source_node=source_node,
        entity_key=entity_key,
        evidence_keys=evidence_keys,
        policy_version=policy_version,
        decided_by=decided_by,
        decided_at=decided_at,
        reason=reason,
    )
    membership = build_entity_membership(
        source_node=source_node,
        entity_key=entity_key,
        decision_id=decision.decision_id,
        valid_from=decided_at,
    )
    return IdentityResolutionResult(
        entities=entities,
        evidence=evidence,
        decisions=(decision,),
        memberships=(membership,),
    ).require_consistent()


def reject_identity_candidate(
    *,
    source_node: SourceNodeRef,
    entity_key: str,
    evidence_keys: tuple[str, ...],
    policy_version: str,
    decided_by: str,
    decided_at: str,
    reason: str,
) -> IdentityResolutionResult:
    """Reject a candidate without creating or changing membership."""

    decision = build_reject_decision(
        source_node=source_node,
        entity_key=entity_key,
        evidence_keys=evidence_keys,
        policy_version=policy_version,
        decided_by=decided_by,
        decided_at=decided_at,
        reason=reason,
    )
    return IdentityResolutionResult(decisions=(decision,)).require_consistent()


def build_lifecycle_revoke_evidence(
    *,
    source_node: SourceNodeRef,
    candidate_entity_key: str,
    assertion_keys: tuple[str, ...],
    observed_at: str,
    policy_id: str,
    policy_digest: str,
    envelope_key: str | None,
    operation: str,
    lifecycle_observed_at: str,
) -> IdentityEvidence:
    """Build evidence binding a lifecycle retraction to historical assertions."""

    return build_identity_evidence(
        kind=EvidenceKind.LIFECYCLE_REVOKE,
        source_node=source_node,
        candidate_entity_key=candidate_entity_key,
        assertion_keys=assertion_keys,
        observed_at=observed_at,
        policy_id=policy_id,
        policy_digest=policy_digest,
        details={
            "envelopeKey": envelope_key,
            "operation": operation,
            "lifecycleObservedAt": lifecycle_observed_at,
        },
    )


def revoke_identity_membership(
    *,
    membership: EntityMembership,
    evidence_keys: tuple[str, ...],
    policy_version: str,
    decided_by: str,
    decided_at: str,
    reason: str,
    evidence: tuple[IdentityEvidence, ...] = (),
) -> IdentityResolutionResult:
    """Revoke an ACCEPT decision and emit the closed membership version."""

    if membership.valid_to is not None:
        raise ValueError("only an active membership can be revoked")
    if parse_rfc3339(decided_at) < parse_rfc3339(membership.valid_from):
        raise ValueError("revocation cannot precede membership")
    decision = build_revoke_decision(
        source_node=membership.source_node,
        entity_key=membership.entity_key,
        evidence_keys=evidence_keys,
        policy_version=policy_version,
        decided_by=decided_by,
        decided_at=decided_at,
        reason=reason,
    )
    closed = close_entity_membership(membership, closed_at=decided_at)
    return IdentityResolutionResult(
        evidence=evidence,
        decisions=(decision,),
        memberships=(closed,),
    ).require_consistent()


def resolve_or_allocate_source_node(
    *,
    source_node: SourceNodeRef,
    entity_level: EntityLevel,
    entity_kind: str,
    exact_candidate_entity_keys: tuple[str, ...],
    assertion_keys: tuple[str, ...],
    observed_at: str,
    policy_id: str,
    policy_digest: str,
    decision_policy_version: str,
    decided_by: str,
    materialization_id: str | None = None,
) -> IdentityResolutionResult:
    """Accept one exact candidate, allocate none, and quarantine ambiguity."""

    candidates = tuple(
        sorted(
            {
                require_sha256(item, label="candidate entity key")
                for item in exact_candidate_entity_keys
            }
        )
    )
    if len(candidates) > 1:
        return IdentityResolutionResult(
            conflicts=(
                build_identity_conflict(
                    materialization_id=materialization_id,
                    source_node=source_node,
                    candidate_entity_keys=candidates,
                    assertion_keys=assertion_keys,
                    reason="MULTIPLE_EXACT_IDENTIFIER_CANDIDATES",
                    observed_at=observed_at,
                    policy_id=policy_id,
                    policy_digest=policy_digest,
                ),
            )
        ).require_consistent()

    if candidates:
        entity_key = candidates[0]
        entities: tuple[EntityLedgerEntry, ...] = ()
        kind = EvidenceKind.EXACT_IDENTIFIER
        reason = "exact compatible identifier"
    else:
        entity = allocate_source_entity(
            source_node=source_node,
            entity_level=entity_level,
            entity_kind=entity_kind,
            first_observed_at=observed_at,
        )
        entity_key = entity.entity_key
        entities = (entity,)
        kind = EvidenceKind.SOURCE_ENTITY_BOOTSTRAP
        reason = "new source entity allocation"

    evidence = build_identity_evidence(
        kind=kind,
        source_node=source_node,
        candidate_entity_key=entity_key,
        assertion_keys=assertion_keys,
        observed_at=observed_at,
        policy_id=policy_id,
        policy_digest=policy_digest,
        confidence=1.0 if candidates else None,
    )
    return accept_identity_candidate(
        source_node=source_node,
        entity_key=entity_key,
        evidence_keys=(evidence.evidence_key,),
        policy_version=decision_policy_version,
        decided_by=decided_by,
        decided_at=observed_at,
        reason=reason,
        entities=entities,
        evidence=(evidence,),
    )


def resolve_parent_constrained_source_node(
    *,
    source_node: SourceNodeRef,
    entity_level: EntityLevel,
    entity_kind: str,
    parent_constraint: ParentConstraint,
    assertion_keys: tuple[str, ...],
    observed_at: str,
    policy_id: str,
    policy_digest: str,
    decision_policy_version: str,
    decided_by: str,
    entity_key: str | None = None,
) -> IdentityResolutionResult:
    """Resolve a child only from an exact parent membership and ordinals."""

    if entity_level != parent_constraint.child_level:
        raise ValueError("parent constraint child level differs from source type")
    entities: tuple[EntityLedgerEntry, ...] = ()
    if entity_key is None:
        entity = allocate_source_entity(
            source_node=source_node,
            entity_level=entity_level,
            entity_kind=entity_kind,
            first_observed_at=observed_at,
        )
        entity_key = entity.entity_key
        entities = (entity,)
    evidence = build_parent_constrained_evidence(
        source_node=source_node,
        candidate_entity_key=entity_key,
        parent_constraint=parent_constraint,
        assertion_keys=assertion_keys,
        observed_at=observed_at,
        policy_id=policy_id,
        policy_digest=policy_digest,
        confidence=1.0,
    )
    return accept_identity_candidate(
        source_node=source_node,
        entity_key=entity_key,
        evidence_keys=(evidence.evidence_key,),
        policy_version=decision_policy_version,
        decided_by=decided_by,
        decided_at=observed_at,
        reason="exact parent membership and ordinal constraint",
        entities=entities,
        evidence=(evidence,),
    )
