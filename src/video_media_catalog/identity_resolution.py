"""Reversible source-node membership resolution for Gold construction."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Self

from pydantic import field_validator

from video_media_catalog.assertions import SourceNodeRef
from video_media_catalog.identity_v2 import (
    DecisionStatus,
    EntityLedgerEntry,
    EntityLevel,
    EntityMembership,
    EntityRedirect,
    EvidenceKind,
    IdentityDecision,
    IdentityEvidence,
    allocate_source_entity,
    build_entity_membership,
    build_identity_decision,
    build_identity_evidence,
    validate_redirect_graph,
)
from video_media_catalog.v2_contracts import (
    V2ContractModel,
    parse_rfc3339,
    require_rfc3339,
    require_sha256,
)


def source_node_key(node: SourceNodeRef) -> tuple[str, str, str]:
    return node.namespace_id, node.source_id, node.referent_kind


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

    active: dict[tuple[str, str, str], str] = {}
    for membership in memberships:
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


class IdentityConflict(V2ContractModel):
    source_node: SourceNodeRef
    candidate_entity_keys: tuple[str, ...]
    assertion_keys: tuple[str, ...]
    reason: str
    observed_at: str

    @field_validator("candidate_entity_keys", "assertion_keys")
    @classmethod
    def normalize_keys(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(
            sorted({require_sha256(item, label="identity key") for item in value})
        )
        if not normalized:
            raise ValueError("identity conflict requires keys")
        return normalized

    @field_validator("observed_at")
    @classmethod
    def validate_observed_at(cls, value: str) -> str:
        return require_rfc3339(value)


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
                IdentityConflict(
                    source_node=source_node,
                    candidate_entity_keys=candidates,
                    assertion_keys=assertion_keys,
                    reason="MULTIPLE_EXACT_IDENTIFIER_CANDIDATES",
                    observed_at=observed_at,
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
    decision = build_identity_decision(
        status=DecisionStatus.ACCEPT,
        source_node=source_node,
        entity_key=entity_key,
        evidence_keys=(evidence.evidence_key,),
        policy_version=decision_policy_version,
        decided_by=decided_by,
        decided_at=observed_at,
        reason=reason,
    )
    membership = build_entity_membership(
        source_node=source_node,
        entity_key=entity_key,
        decision_id=decision.decision_id,
        valid_from=observed_at,
    )
    return IdentityResolutionResult(
        entities=entities,
        evidence=(evidence,),
        decisions=(decision,),
        memberships=(membership,),
    ).require_consistent()
