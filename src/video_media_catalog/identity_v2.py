"""Persistent v2 entity allocation, membership decisions, and redirects."""

from __future__ import annotations

import hashlib
import uuid
from datetime import datetime
from enum import StrEnum
from typing import Self

from pydantic import Field, field_validator, model_validator

from video_media_catalog.assertions import SourceNodeRef
from video_media_catalog.canonical import canonical_json, deterministic_key
from video_media_catalog.identity import require_canonical_uuid7
from video_media_catalog.v2_contracts import (
    V2ContractModel,
    parse_rfc3339,
    require_rfc3339,
    require_sha256,
    require_slug,
)


class EntityLevel(StrEnum):
    UNKNOWN = "UNKNOWN"
    EDITORIAL_WORK = "EDITORIAL_WORK"
    SERIES = "SERIES"
    SEASON = "SEASON"
    EPISODE = "EPISODE"
    EDIT = "EDIT"
    MANIFESTATION = "MANIFESTATION"
    RELEASE_EVENT = "RELEASE_EVENT"
    BROADCAST_EVENT = "BROADCAST_EVENT"
    AVAILABILITY_OFFER = "AVAILABILITY_OFFER"
    ONLINE_PUBLICATION = "ONLINE_PUBLICATION"
    AGENT = "AGENT"
    PLATFORM_ACCOUNT = "PLATFORM_ACCOUNT"
    REGULATORY_RECORD = "REGULATORY_RECORD"
    MEDIA_ASSET = "MEDIA_ASSET"


class EntityStatus(StrEnum):
    ACTIVE = "ACTIVE"
    MERGED = "MERGED"
    SPLIT = "SPLIT"
    TOMBSTONED = "TOMBSTONED"


class DecisionStatus(StrEnum):
    ACCEPT = "ACCEPT"
    REJECT = "REJECT"
    UNCERTAIN = "UNCERTAIN"
    REVOKE = "REVOKE"


class EvidenceKind(StrEnum):
    SOURCE_ENTITY_BOOTSTRAP = "SOURCE_ENTITY_BOOTSTRAP"
    REGISTRY_IDENTIFIER = "REGISTRY_IDENTIFIER"
    SOURCE_REDIRECT = "SOURCE_REDIRECT"
    EXACT_IDENTIFIER = "EXACT_IDENTIFIER"
    PARENT_CONSTRAINED = "PARENT_CONSTRAINED"
    LIFECYCLE_REVOKE = "LIFECYCLE_REVOKE"
    HUMAN_REVIEW = "HUMAN_REVIEW"
    SIMILARITY_CANDIDATE = "SIMILARITY_CANDIDATE"
    MIGRATED_V1 = "MIGRATED_V1"


_HIERARCHY_REFERENT_KINDS = {
    # Some source namespaces (notably IMDb title IDs) use one broad
    # EDITORIAL_WORK referent kind. Production resolution additionally joins
    # the source's exact entity-type assertion before constructing a constraint.
    EntityLevel.SERIES: {"EDITORIAL_WORK", "SERIES", "TV_SERIES"},
    EntityLevel.SEASON: {"EDITORIAL_WORK", "SEASON", "TV_SEASON"},
    EntityLevel.EPISODE: {"EDITORIAL_WORK", "EPISODE", "TV_EPISODE"},
}


def _node_matches_level(node: SourceNodeRef, level: EntityLevel) -> bool:
    return node.referent_kind.strip().upper() in _HIERARCHY_REFERENT_KINDS.get(
        level,
        set(),
    )


class ParentConstraint(V2ContractModel):
    """Typed hierarchy and ordinal facts required by parent-constrained evidence."""

    child_level: EntityLevel
    parent_level: EntityLevel
    parent_source_node: SourceNodeRef
    parent_entity_key: str
    parent_membership_key: str
    relationship_assertion_key: str
    ordinal_assertion_keys: tuple[str, ...]
    season_number: str | None = None
    episode_number: str | None = None

    @field_validator(
        "parent_entity_key",
        "parent_membership_key",
        "relationship_assertion_key",
    )
    @classmethod
    def validate_key(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("ordinal_assertion_keys")
    @classmethod
    def normalize_ordinal_assertions(
        cls,
        value: tuple[str, ...],
    ) -> tuple[str, ...]:
        normalized = tuple(
            sorted(
                {require_sha256(item, label="ordinal assertion key") for item in value}
            )
        )
        if not normalized:
            raise ValueError("parent constraint requires ordinal assertion keys")
        return normalized

    @field_validator("season_number", "episode_number")
    @classmethod
    def normalize_number(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized or len(normalized) > 64:
            raise ValueError("parent constraint numbers must be non-empty and bounded")
        return normalized

    @model_validator(mode="after")
    def validate_hierarchy(self) -> Self:
        if not _node_matches_level(self.parent_source_node, self.parent_level):
            raise ValueError("parent source node is incompatible with parent level")
        if self.child_level == EntityLevel.SEASON:
            if self.parent_level != EntityLevel.SERIES or self.season_number is None:
                raise ValueError("season evidence requires a series and season number")
            if self.episode_number is not None:
                raise ValueError("season evidence cannot contain an episode number")
        elif self.child_level == EntityLevel.EPISODE:
            if self.parent_level not in {EntityLevel.SERIES, EntityLevel.SEASON}:
                raise ValueError("episode evidence requires a series or season parent")
            if self.episode_number is None:
                raise ValueError("episode evidence requires an episode number")
            if self.parent_level == EntityLevel.SERIES and self.season_number is None:
                raise ValueError(
                    "series-constrained episode evidence requires a season number"
                )
        else:
            raise ValueError("parent constraints apply only to seasons and episodes")
        return self


class EntityLedgerEntry(V2ContractModel):
    entity_key: str
    allocation_id: str | None = None
    entity_level: EntityLevel
    entity_kind: str
    status: EntityStatus = EntityStatus.ACTIVE
    created_at: str
    first_release_id: str | None = None
    imported_v1: bool = False

    @field_validator("entity_key")
    @classmethod
    def validate_entity_key(cls, value: str) -> str:
        return require_sha256(value, label="entity_key")

    @field_validator("allocation_id")
    @classmethod
    def validate_allocation_id(cls, value: str | None) -> str | None:
        return None if value is None else require_canonical_uuid7(value)

    @field_validator("entity_kind")
    @classmethod
    def validate_entity_kind(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not normalized or len(normalized) > 128:
            raise ValueError("entity_kind must be non-empty")
        return normalized

    @field_validator("created_at")
    @classmethod
    def validate_created_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @field_validator("first_release_id")
    @classmethod
    def validate_release_id(cls, value: str | None) -> str | None:
        return (
            None if value is None else require_sha256(value, label="first_release_id")
        )

    @model_validator(mode="after")
    def validate_allocation(self) -> Self:
        if self.imported_v1 and self.allocation_id is not None:
            raise ValueError("imported v1 entities preserve keys without allocation_id")
        if not self.imported_v1 and self.allocation_id is None:
            raise ValueError("new v2 entities require allocation_id")
        if self.allocation_id is not None:
            expected = deterministic_key(
                "catalog-entity-anchor-v2",
                {"allocationId": self.allocation_id},
            )
            if self.entity_key != expected:
                raise ValueError("entity_key does not match allocation_id")
        return self


def allocate_entity(
    *,
    allocation_id: str,
    entity_level: EntityLevel,
    entity_kind: str,
    created_at: str,
    first_release_id: str | None = None,
) -> EntityLedgerEntry:
    allocation = require_canonical_uuid7(allocation_id)
    return EntityLedgerEntry(
        entity_key=deterministic_key(
            "catalog-entity-anchor-v2",
            {"allocationId": allocation},
        ),
        allocation_id=allocation,
        entity_level=entity_level,
        entity_kind=entity_kind,
        created_at=created_at,
        first_release_id=first_release_id,
    )


def allocate_source_entity(
    *,
    source_node: SourceNodeRef,
    entity_level: EntityLevel,
    entity_kind: str,
    first_observed_at: str,
    first_release_id: str | None = None,
) -> EntityLedgerEntry:
    """Allocate once from a source-node request; the persisted key is authoritative."""

    timestamp = parse_rfc3339(first_observed_at)
    timestamp_ms = int(timestamp.timestamp() * 1000)
    payload = canonical_json(
        {
            "kind": "community-source-entity-allocation-v2",
            "sourceNode": source_node.model_dump(mode="json", by_alias=True),
        }
    ).encode()
    random_bits = int.from_bytes(hashlib.sha256(payload).digest()[:10], "big")
    random_bits &= (1 << 74) - 1
    random_a = (random_bits >> 62) & 0xFFF
    random_b = random_bits & ((1 << 62) - 1)
    value = (
        (timestamp_ms << 80) | (0x7 << 76) | (random_a << 64) | (0b10 << 62) | random_b
    )
    return allocate_entity(
        allocation_id=str(uuid.UUID(int=value)),
        entity_level=entity_level,
        entity_kind=entity_kind,
        created_at=first_observed_at,
        first_release_id=first_release_id,
    )


def import_v1_entity(
    *,
    entity_key: str,
    entity_level: EntityLevel,
    entity_kind: str,
    created_at: str,
    first_release_id: str | None = None,
) -> EntityLedgerEntry:
    """Preserve a published v1 key without reinterpreting its hash input."""

    return EntityLedgerEntry(
        entity_key=entity_key,
        entity_level=entity_level,
        entity_kind=entity_kind,
        created_at=created_at,
        first_release_id=first_release_id,
        imported_v1=True,
    )


class IdentityEvidence(V2ContractModel):
    evidence_key: str
    kind: EvidenceKind
    source_node: SourceNodeRef
    candidate_entity_key: str
    assertion_keys: tuple[str, ...]
    observed_at: str
    policy_id: str
    policy_digest: str
    confidence: float | None = Field(default=None, ge=0, le=1)
    details: dict[str, object] = Field(default_factory=dict)

    @field_validator(
        "evidence_key",
        "candidate_entity_key",
        "policy_digest",
    )
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("assertion_keys")
    @classmethod
    def normalize_assertion_keys(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(
            sorted({require_sha256(item, label="assertion key") for item in value})
        )
        if not normalized:
            raise ValueError("identity evidence requires assertion_keys")
        return normalized

    @field_validator("observed_at")
    @classmethod
    def validate_observed_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @field_validator("policy_id")
    @classmethod
    def validate_policy_id(cls, value: str) -> str:
        return require_slug(value, label="policy_id")

    @model_validator(mode="after")
    def validate_evidence_identity(self) -> Self:
        expected = deterministic_key(
            "identity-evidence-v2",
            {
                "kind": self.kind.value,
                "sourceNode": self.source_node.model_dump(mode="json", by_alias=True),
                "candidateEntityKey": self.candidate_entity_key,
                "assertionKeys": self.assertion_keys,
                "policyId": self.policy_id,
                "details": self.details,
            },
        )
        if self.evidence_key != expected:
            raise ValueError("evidence_key does not match identity evidence")
        return self


def build_identity_evidence(
    *,
    kind: EvidenceKind,
    source_node: SourceNodeRef,
    candidate_entity_key: str,
    assertion_keys: tuple[str, ...],
    observed_at: str,
    policy_id: str,
    policy_digest: str,
    confidence: float | None = None,
    details: dict[str, object] | None = None,
) -> IdentityEvidence:
    normalized_details = details or {}
    identity = {
        "kind": kind.value,
        "sourceNode": source_node.model_dump(mode="json", by_alias=True),
        "candidateEntityKey": candidate_entity_key,
        "assertionKeys": tuple(sorted(set(assertion_keys))),
        "policyId": policy_id,
        "details": normalized_details,
    }
    return IdentityEvidence(
        evidence_key=deterministic_key("identity-evidence-v2", identity),
        kind=kind,
        source_node=source_node,
        candidate_entity_key=candidate_entity_key,
        assertion_keys=assertion_keys,
        observed_at=observed_at,
        policy_id=policy_id,
        policy_digest=policy_digest,
        confidence=confidence,
        details=normalized_details,
    )


def build_parent_constrained_evidence(
    *,
    source_node: SourceNodeRef,
    candidate_entity_key: str,
    parent_constraint: ParentConstraint,
    assertion_keys: tuple[str, ...],
    observed_at: str,
    policy_id: str,
    policy_digest: str,
    confidence: float | None = None,
) -> IdentityEvidence:
    """Build evidence that binds child identity to parent identity and ordinals."""

    if not _node_matches_level(source_node, parent_constraint.child_level):
        raise ValueError("child source node is incompatible with child level")
    hierarchy_keys = {
        parent_constraint.relationship_assertion_key,
        *parent_constraint.ordinal_assertion_keys,
        *assertion_keys,
    }
    return build_identity_evidence(
        kind=EvidenceKind.PARENT_CONSTRAINED,
        source_node=source_node,
        candidate_entity_key=candidate_entity_key,
        assertion_keys=tuple(sorted(hierarchy_keys)),
        observed_at=observed_at,
        policy_id=policy_id,
        policy_digest=policy_digest,
        confidence=confidence,
        details={
            "parentConstraint": parent_constraint.model_dump(
                mode="json",
                by_alias=True,
                exclude_none=True,
            )
        },
    )


class IdentityConflict(V2ContractModel):
    conflict_key: str
    materialization_id: str
    source_node: SourceNodeRef
    candidate_entity_keys: tuple[str, ...]
    assertion_keys: tuple[str, ...]
    reason: str
    observed_at: str
    policy_id: str
    policy_digest: str
    details: dict[str, object] = Field(default_factory=dict)

    @field_validator("conflict_key", "materialization_id", "policy_digest")
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("candidate_entity_keys", "assertion_keys")
    @classmethod
    def normalize_keys(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(
            sorted(
                {require_sha256(item, label="identity conflict key") for item in value}
            )
        )
        if not normalized:
            raise ValueError("identity conflict requires keys")
        return normalized

    @field_validator("reason")
    @classmethod
    def validate_reason(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized or len(normalized) > 2000:
            raise ValueError("identity conflict reason must be non-empty")
        return normalized

    @field_validator("observed_at")
    @classmethod
    def validate_observed_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @field_validator("policy_id")
    @classmethod
    def validate_policy_id(cls, value: str) -> str:
        return require_slug(value, label="policy_id")

    @model_validator(mode="after")
    def validate_conflict_identity(self) -> Self:
        expected = deterministic_key(
            "identity-conflict-v2",
            {
                "sourceNode": self.source_node.model_dump(mode="json", by_alias=True),
                "materializationId": self.materialization_id,
                "candidateEntityKeys": self.candidate_entity_keys,
                "assertionKeys": self.assertion_keys,
                "reason": self.reason,
                "observedAt": self.observed_at,
                "policyId": self.policy_id,
                "policyDigest": self.policy_digest,
                "details": self.details,
            },
        )
        if self.conflict_key != expected:
            raise ValueError("conflict_key does not match identity conflict")
        return self


def build_identity_conflict(
    *,
    materialization_id: str | None = None,
    source_node: SourceNodeRef,
    candidate_entity_keys: tuple[str, ...],
    assertion_keys: tuple[str, ...],
    reason: str,
    observed_at: str,
    policy_id: str,
    policy_digest: str,
    details: dict[str, object] | None = None,
) -> IdentityConflict:
    normalized_candidates = tuple(sorted(set(candidate_entity_keys)))
    normalized_assertions = tuple(sorted(set(assertion_keys)))
    normalized_time = require_rfc3339(observed_at)
    normalized_details = details or {}
    normalized_policy_id = require_slug(policy_id, label="policy_id")
    normalized_materialization_id = (
        deterministic_key(
            "identity-resolution-context-v2",
            {
                "sourceNode": source_node.model_dump(mode="json", by_alias=True),
                "observedAt": normalized_time,
                "policyDigest": require_sha256(policy_digest),
            },
        )
        if materialization_id is None
        else require_sha256(materialization_id, label="materialization_id")
    )
    identity = {
        "sourceNode": source_node.model_dump(mode="json", by_alias=True),
        "materializationId": normalized_materialization_id,
        "candidateEntityKeys": normalized_candidates,
        "assertionKeys": normalized_assertions,
        "reason": reason.strip(),
        "observedAt": normalized_time,
        "policyId": normalized_policy_id,
        "policyDigest": require_sha256(policy_digest),
        "details": normalized_details,
    }
    return IdentityConflict(
        conflict_key=deterministic_key("identity-conflict-v2", identity),
        materialization_id=normalized_materialization_id,
        source_node=source_node,
        candidate_entity_keys=normalized_candidates,
        assertion_keys=normalized_assertions,
        reason=reason,
        observed_at=normalized_time,
        policy_id=normalized_policy_id,
        policy_digest=policy_digest,
        details=normalized_details,
    )


class ExternalIdIndexEntry(V2ContractModel):
    index_entry_key: str
    blocking_key: str
    materialization_id: str
    namespace_id: str
    normalized_value: str
    referent_kind: str
    entity_key: str
    assertion_keys: tuple[str, ...]
    observed_at: str
    policy_id: str
    policy_digest: str

    @field_validator(
        "index_entry_key",
        "blocking_key",
        "materialization_id",
        "entity_key",
        "policy_digest",
    )
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("namespace_id", "policy_id")
    @classmethod
    def validate_slug(cls, value: str) -> str:
        return require_slug(value, label="external ID index reference")

    @field_validator("normalized_value")
    @classmethod
    def validate_value(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized or normalized != value or len(normalized) > 2048:
            raise ValueError("normalized external ID must be trimmed and non-empty")
        return normalized

    @field_validator("referent_kind")
    @classmethod
    def normalize_referent_kind(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not normalized or len(normalized) > 128:
            raise ValueError("external ID referent kind must be non-empty")
        return normalized

    @field_validator("assertion_keys")
    @classmethod
    def normalize_assertion_keys(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(
            sorted({require_sha256(item, label="assertion key") for item in value})
        )
        if not normalized:
            raise ValueError("external ID index entry requires assertion keys")
        return normalized

    @field_validator("observed_at")
    @classmethod
    def validate_observed_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @model_validator(mode="after")
    def validate_index_identity(self) -> Self:
        blocking_identity = {
            "namespaceId": self.namespace_id,
            "normalizedValue": self.normalized_value,
            "referentKind": self.referent_kind,
        }
        if self.blocking_key != deterministic_key(
            "external-id-block-v2",
            blocking_identity,
        ):
            raise ValueError("blocking_key does not match external ID block")
        expected = deterministic_key(
            "external-id-index-entry-v2",
            {
                **blocking_identity,
                "materializationId": self.materialization_id,
                "entityKey": self.entity_key,
                "assertionKeys": self.assertion_keys,
                "observedAt": self.observed_at,
                "policyId": self.policy_id,
                "policyDigest": self.policy_digest,
            },
        )
        if self.index_entry_key != expected:
            raise ValueError("index_entry_key does not match external ID index entry")
        return self


def build_external_id_index_entry(
    *,
    materialization_id: str,
    namespace_id: str,
    normalized_value: str,
    referent_kind: str,
    entity_key: str,
    assertion_keys: tuple[str, ...],
    observed_at: str,
    policy_id: str,
    policy_digest: str,
) -> ExternalIdIndexEntry:
    normalized_namespace = require_slug(namespace_id, label="namespace_id")
    normalized_referent = referent_kind.strip().upper()
    normalized_assertions = tuple(sorted(set(assertion_keys)))
    normalized_policy_id = require_slug(policy_id, label="policy_id")
    blocking_identity = {
        "namespaceId": normalized_namespace,
        "normalizedValue": normalized_value.strip(),
        "referentKind": normalized_referent,
    }
    identity = {
        **blocking_identity,
        "materializationId": require_sha256(
            materialization_id,
            label="materialization_id",
        ),
        "entityKey": entity_key,
        "assertionKeys": normalized_assertions,
        "observedAt": require_rfc3339(observed_at),
        "policyId": normalized_policy_id,
        "policyDigest": require_sha256(policy_digest),
    }
    return ExternalIdIndexEntry(
        index_entry_key=deterministic_key(
            "external-id-index-entry-v2",
            identity,
        ),
        blocking_key=deterministic_key(
            "external-id-block-v2",
            blocking_identity,
        ),
        materialization_id=materialization_id,
        namespace_id=normalized_namespace,
        normalized_value=normalized_value.strip(),
        referent_kind=normalized_referent,
        entity_key=entity_key,
        assertion_keys=normalized_assertions,
        observed_at=observed_at,
        policy_id=normalized_policy_id,
        policy_digest=policy_digest,
    )


class IdentityDecision(V2ContractModel):
    decision_id: str
    status: DecisionStatus
    source_node: SourceNodeRef
    entity_key: str
    evidence_keys: tuple[str, ...]
    policy_version: str
    decided_by: str
    decided_at: str
    reason: str

    @field_validator("decision_id", "entity_key")
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("evidence_keys")
    @classmethod
    def normalize_evidence_keys(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(
            sorted({require_sha256(item, label="evidence key") for item in value})
        )
        if not normalized:
            raise ValueError("identity decision requires evidence_keys")
        return normalized

    @field_validator("policy_version", "decided_by", "reason")
    @classmethod
    def validate_text(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized or len(normalized) > 2000:
            raise ValueError("identity decision text must be non-empty")
        return normalized

    @field_validator("decided_at")
    @classmethod
    def validate_decided_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @model_validator(mode="after")
    def validate_decision_identity(self) -> Self:
        expected = deterministic_key(
            "identity-decision-v2",
            {
                "status": self.status.value,
                "sourceNode": self.source_node.model_dump(mode="json", by_alias=True),
                "entityKey": self.entity_key,
                "evidenceKeys": self.evidence_keys,
                "policyVersion": self.policy_version,
                "decidedBy": self.decided_by,
                "decidedAt": self.decided_at,
                "reason": self.reason,
            },
        )
        if self.decision_id != expected:
            raise ValueError("decision_id does not match identity decision")
        return self


def build_identity_decision(
    *,
    status: DecisionStatus,
    source_node: SourceNodeRef,
    entity_key: str,
    evidence_keys: tuple[str, ...],
    policy_version: str,
    decided_by: str,
    decided_at: str,
    reason: str,
) -> IdentityDecision:
    identity = {
        "status": status.value,
        "sourceNode": source_node.model_dump(mode="json", by_alias=True),
        "entityKey": entity_key,
        "evidenceKeys": tuple(sorted(set(evidence_keys))),
        "policyVersion": policy_version,
        "decidedBy": decided_by,
        "decidedAt": require_rfc3339(decided_at),
        "reason": reason,
    }
    return IdentityDecision(
        decision_id=deterministic_key("identity-decision-v2", identity),
        status=status,
        source_node=source_node,
        entity_key=entity_key,
        evidence_keys=evidence_keys,
        policy_version=policy_version,
        decided_by=decided_by,
        decided_at=decided_at,
        reason=reason,
    )


def build_accept_decision(**values) -> IdentityDecision:
    """Build an immutable ACCEPT review decision."""

    return build_identity_decision(status=DecisionStatus.ACCEPT, **values)


def build_reject_decision(**values) -> IdentityDecision:
    """Build an immutable REJECT review decision."""

    return build_identity_decision(status=DecisionStatus.REJECT, **values)


def build_revoke_decision(**values) -> IdentityDecision:
    """Build an immutable REVOKE review decision."""

    return build_identity_decision(status=DecisionStatus.REVOKE, **values)


class EntityMembership(V2ContractModel):
    membership_key: str
    source_node: SourceNodeRef
    entity_key: str
    decision_id: str
    valid_from: str
    valid_to: str | None = None

    @field_validator("membership_key", "entity_key", "decision_id")
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("valid_from", "valid_to")
    @classmethod
    def validate_timestamp(cls, value: str | None) -> str | None:
        return None if value is None else require_rfc3339(value)

    @model_validator(mode="after")
    def validate_membership(self) -> Self:
        if self.valid_to is not None and parse_rfc3339(self.valid_to) < parse_rfc3339(
            self.valid_from
        ):
            raise ValueError("membership valid_to must not precede valid_from")
        expected = deterministic_key(
            "entity-membership-v2",
            {
                "sourceNode": self.source_node.model_dump(mode="json", by_alias=True),
                "entityKey": self.entity_key,
                "decisionId": self.decision_id,
                "validFrom": self.valid_from,
                "validTo": self.valid_to,
            },
        )
        if self.membership_key != expected:
            raise ValueError("membership_key does not match membership identity")
        return self


def build_entity_membership(
    *,
    source_node: SourceNodeRef,
    entity_key: str,
    decision_id: str,
    valid_from: str,
    valid_to: str | None = None,
) -> EntityMembership:
    normalized_from = require_rfc3339(valid_from)
    normalized_to = None if valid_to is None else require_rfc3339(valid_to)
    identity = {
        "sourceNode": source_node.model_dump(mode="json", by_alias=True),
        "entityKey": entity_key,
        "decisionId": decision_id,
        "validFrom": normalized_from,
        "validTo": normalized_to,
    }
    return EntityMembership(
        membership_key=deterministic_key("entity-membership-v2", identity),
        source_node=source_node,
        entity_key=entity_key,
        decision_id=decision_id,
        valid_from=normalized_from,
        valid_to=normalized_to,
    )


def close_entity_membership(
    membership: EntityMembership,
    *,
    closed_at: str,
) -> EntityMembership:
    """Emit the immutable closed version of one active membership."""

    if membership.valid_to is not None:
        raise ValueError("only an active membership can be closed")
    normalized_time = require_rfc3339(closed_at, label="closed_at")
    if parse_rfc3339(normalized_time) < parse_rfc3339(membership.valid_from):
        raise ValueError("membership closure cannot precede valid_from")
    return build_entity_membership(
        source_node=membership.source_node,
        entity_key=membership.entity_key,
        decision_id=membership.decision_id,
        valid_from=membership.valid_from,
        valid_to=normalized_time,
    )


class LegacyKeyMap(V2ContractModel):
    legacy_key: str
    legacy_kind: str
    target_key: str
    imported_at: str
    source_snapshot_set_id: str

    @field_validator("legacy_key", "target_key")
    @classmethod
    def validate_key(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("source_snapshot_set_id")
    @classmethod
    def validate_snapshot_set_id(cls, value: str) -> str:
        return require_canonical_uuid7(value)

    @field_validator("legacy_kind")
    @classmethod
    def validate_kind(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not normalized or len(normalized) > 128:
            raise ValueError("legacy_kind must be non-empty")
        return normalized

    @field_validator("imported_at")
    @classmethod
    def validate_imported_at(cls, value: str) -> str:
        return require_rfc3339(value)


class EntityRedirect(V2ContractModel):
    redirect_key: str
    source_entity_key: str
    target_entity_key: str
    effective_at: str
    decision_id: str

    @field_validator(
        "redirect_key",
        "source_entity_key",
        "target_entity_key",
        "decision_id",
    )
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("effective_at")
    @classmethod
    def validate_effective_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @model_validator(mode="after")
    def validate_redirect(self) -> Self:
        if self.source_entity_key == self.target_entity_key:
            raise ValueError("entity cannot redirect to itself")
        expected = deterministic_key(
            "entity-redirect-v2",
            {
                "sourceEntityKey": self.source_entity_key,
                "targetEntityKey": self.target_entity_key,
                "effectiveAt": self.effective_at,
                "decisionId": self.decision_id,
            },
        )
        if self.redirect_key != expected:
            raise ValueError("redirect_key does not match redirect identity")
        return self


def build_entity_redirect(
    *,
    source_entity_key: str,
    target_entity_key: str,
    effective_at: str,
    decision_id: str,
) -> EntityRedirect:
    normalized_time = require_rfc3339(effective_at)
    identity = {
        "sourceEntityKey": source_entity_key,
        "targetEntityKey": target_entity_key,
        "effectiveAt": normalized_time,
        "decisionId": decision_id,
    }
    return EntityRedirect(
        redirect_key=deterministic_key("entity-redirect-v2", identity),
        source_entity_key=source_entity_key,
        target_entity_key=target_entity_key,
        effective_at=normalized_time,
        decision_id=decision_id,
    )


class EntityMergeEvent(V2ContractModel):
    merge_event_key: str
    entity_keys: tuple[str, ...]
    survivor_entity_key: str
    redirect_keys: tuple[str, ...]
    decision_id: str
    effective_at: str
    merged_by: str
    reason: str

    @field_validator("merge_event_key", "survivor_entity_key", "decision_id")
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("entity_keys")
    @classmethod
    def normalize_entity_keys(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(
            sorted({require_sha256(item, label="merge entity key") for item in value})
        )
        if len(normalized) < 2:
            raise ValueError("merge event requires at least two entities")
        return normalized

    @field_validator("redirect_keys")
    @classmethod
    def normalize_redirect_keys(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(
            sorted({require_sha256(item, label="redirect key") for item in value})
        )
        if not normalized:
            raise ValueError("merge event requires redirects")
        return normalized

    @field_validator("effective_at")
    @classmethod
    def validate_effective_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @field_validator("merged_by", "reason")
    @classmethod
    def validate_text(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized or len(normalized) > 2000:
            raise ValueError("merge event text must be non-empty")
        return normalized

    @model_validator(mode="after")
    def validate_merge(self) -> Self:
        if self.survivor_entity_key not in self.entity_keys:
            raise ValueError("merge survivor must be one of the merged entities")
        if len(self.redirect_keys) != len(self.entity_keys) - 1:
            raise ValueError("merge event requires one redirect per retired entity")
        expected = deterministic_key(
            "entity-merge-event-v2",
            _merge_event_identity(self),
        )
        if self.merge_event_key != expected:
            raise ValueError("merge_event_key does not match merge event")
        return self


def _merge_event_identity(event: EntityMergeEvent) -> dict[str, object]:
    return {
        "entityKeys": event.entity_keys,
        "survivorEntityKey": event.survivor_entity_key,
        "redirectKeys": event.redirect_keys,
        "decisionId": event.decision_id,
        "effectiveAt": event.effective_at,
        "mergedBy": event.merged_by,
        "reason": event.reason,
    }


def build_entity_merge_event(
    *,
    entity_keys: tuple[str, ...],
    survivor_entity_key: str,
    redirect_keys: tuple[str, ...],
    decision_id: str,
    effective_at: str,
    merged_by: str,
    reason: str,
) -> EntityMergeEvent:
    values = {
        "entity_keys": tuple(sorted(set(entity_keys))),
        "survivor_entity_key": survivor_entity_key,
        "redirect_keys": tuple(sorted(set(redirect_keys))),
        "decision_id": decision_id,
        "effective_at": require_rfc3339(effective_at),
        "merged_by": merged_by.strip(),
        "reason": reason.strip(),
    }
    provisional = EntityMergeEvent.model_construct(
        merge_event_key="sha256:" + ("0" * 64),
        **values,
    )
    return EntityMergeEvent(
        merge_event_key=deterministic_key(
            "entity-merge-event-v2",
            _merge_event_identity(provisional),
        ),
        **values,
    )


class EntityMergeResult(V2ContractModel):
    event: EntityMergeEvent
    redirects: tuple[EntityRedirect, ...]

    @model_validator(mode="after")
    def validate_result(self) -> Self:
        redirect_keys = tuple(sorted(item.redirect_key for item in self.redirects))
        if redirect_keys != self.event.redirect_keys:
            raise ValueError("merge result redirects do not match the event")
        return self


class EntitySplitAssignment(V2ContractModel):
    source_node: SourceNodeRef
    target_entity_key: str
    decision_id: str

    @field_validator("target_entity_key", "decision_id")
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)


class EntitySplitEvent(V2ContractModel):
    split_event_key: str
    source_entity_key: str
    target_entity_keys: tuple[str, ...]
    assignments: tuple[EntitySplitAssignment, ...]
    effective_at: str
    split_by: str
    reason: str

    @field_validator("split_event_key", "source_entity_key")
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("target_entity_keys")
    @classmethod
    def normalize_target_keys(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(
            sorted({require_sha256(item, label="split target key") for item in value})
        )
        if len(normalized) < 2:
            raise ValueError("split event requires at least two target entities")
        return normalized

    @field_validator("assignments")
    @classmethod
    def normalize_assignments(
        cls,
        value: tuple[EntitySplitAssignment, ...],
    ) -> tuple[EntitySplitAssignment, ...]:
        return tuple(
            sorted(
                value,
                key=lambda item: (
                    item.source_node.namespace_id,
                    item.source_node.source_id,
                    item.source_node.referent_kind,
                    item.target_entity_key,
                ),
            )
        )

    @field_validator("effective_at")
    @classmethod
    def validate_effective_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @field_validator("split_by", "reason")
    @classmethod
    def validate_text(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized or len(normalized) > 2000:
            raise ValueError("split event text must be non-empty")
        return normalized

    @model_validator(mode="after")
    def validate_split(self) -> Self:
        source_nodes = {
            (
                assignment.source_node.namespace_id,
                assignment.source_node.source_id,
                assignment.source_node.referent_kind,
            )
            for assignment in self.assignments
        }
        if len(source_nodes) != len(self.assignments):
            raise ValueError("split event assigns one source node more than once")
        assignment_targets = {
            assignment.target_entity_key for assignment in self.assignments
        }
        if assignment_targets != set(self.target_entity_keys):
            raise ValueError("split assignments must cover every target entity")
        expected = deterministic_key(
            "entity-split-event-v2",
            _split_event_identity(self),
        )
        if self.split_event_key != expected:
            raise ValueError("split_event_key does not match split event")
        return self


def _split_event_identity(event: EntitySplitEvent) -> dict[str, object]:
    return {
        "sourceEntityKey": event.source_entity_key,
        "targetEntityKeys": event.target_entity_keys,
        "assignments": tuple(
            item.model_dump(mode="json", by_alias=True) for item in event.assignments
        ),
        "effectiveAt": event.effective_at,
        "splitBy": event.split_by,
        "reason": event.reason,
    }


def build_entity_split_event(
    *,
    source_entity_key: str,
    assignments: tuple[EntitySplitAssignment, ...],
    effective_at: str,
    split_by: str,
    reason: str,
) -> EntitySplitEvent:
    normalized_assignments = tuple(
        sorted(
            assignments,
            key=lambda item: (
                item.source_node.namespace_id,
                item.source_node.source_id,
                item.source_node.referent_kind,
                item.target_entity_key,
            ),
        )
    )
    values = {
        "source_entity_key": source_entity_key,
        "target_entity_keys": tuple(
            sorted({item.target_entity_key for item in normalized_assignments})
        ),
        "assignments": normalized_assignments,
        "effective_at": require_rfc3339(effective_at),
        "split_by": split_by.strip(),
        "reason": reason.strip(),
    }
    provisional = EntitySplitEvent.model_construct(
        split_event_key="sha256:" + ("0" * 64),
        **values,
    )
    return EntitySplitEvent(
        split_event_key=deterministic_key(
            "entity-split-event-v2",
            _split_event_identity(provisional),
        ),
        **values,
    )


def select_merge_survivor(
    entries: tuple[EntityLedgerEntry, ...],
) -> EntityLedgerEntry:
    if len(entries) < 2:
        raise ValueError("merge survivor selection requires at least two entities")
    if len({entry.entity_key for entry in entries}) != len(entries):
        raise ValueError("merge candidates must be unique")
    return min(
        entries,
        key=lambda entry: (
            datetime.fromisoformat(entry.created_at.replace("Z", "+00:00")),
            entry.entity_key,
        ),
    )


def build_entity_merge(
    *,
    entries: tuple[EntityLedgerEntry, ...],
    decision_id: str,
    effective_at: str,
    merged_by: str,
    reason: str,
) -> EntityMergeResult:
    """Select the stable survivor and emit redirects plus one merge event."""

    levels = {entry.entity_level for entry in entries}
    if len(levels) != 1:
        raise ValueError("merge candidates must have the same entity level")
    if any(entry.status != EntityStatus.ACTIVE for entry in entries):
        raise ValueError("merge candidates must be active")
    if len({entry.entity_kind for entry in entries}) != 1:
        raise ValueError("merge candidates must have compatible entity kinds")
    survivor = select_merge_survivor(entries)
    redirects = tuple(
        build_entity_redirect(
            source_entity_key=entry.entity_key,
            target_entity_key=survivor.entity_key,
            effective_at=effective_at,
            decision_id=decision_id,
        )
        for entry in sorted(entries, key=lambda item: item.entity_key)
        if entry.entity_key != survivor.entity_key
    )
    event = build_entity_merge_event(
        entity_keys=tuple(item.entity_key for item in entries),
        survivor_entity_key=survivor.entity_key,
        redirect_keys=tuple(item.redirect_key for item in redirects),
        decision_id=decision_id,
        effective_at=effective_at,
        merged_by=merged_by,
        reason=reason,
    )
    return EntityMergeResult(event=event, redirects=redirects)


def validate_redirect_graph(redirects: tuple[EntityRedirect, ...]) -> None:
    targets: dict[str, str] = {}
    for redirect in redirects:
        existing = targets.get(redirect.source_entity_key)
        if existing is not None and existing != redirect.target_entity_key:
            raise ValueError("one entity key redirects to multiple targets")
        targets[redirect.source_entity_key] = redirect.target_entity_key
    for start in targets:
        seen: set[str] = set()
        current = start
        while current in targets:
            if current in seen:
                raise ValueError("entity redirect graph contains a cycle")
            seen.add(current)
            current = targets[current]


def resolve_redirect_target(
    entity_key: str,
    redirects: tuple[EntityRedirect, ...],
) -> str:
    """Resolve an entity key through a validated redirect chain."""

    current = require_sha256(entity_key, label="entity_key")
    validate_redirect_graph(redirects)
    targets = {
        redirect.source_entity_key: redirect.target_entity_key for redirect in redirects
    }
    while current in targets:
        current = targets[current]
    return current
