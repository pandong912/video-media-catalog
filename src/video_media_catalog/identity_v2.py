"""Persistent v2 entity allocation, membership decisions, and redirects."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Self

from pydantic import Field, field_validator, model_validator

from video_media_catalog.assertions import SourceNodeRef
from video_media_catalog.canonical import deterministic_key
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
    REGISTRY_IDENTIFIER = "REGISTRY_IDENTIFIER"
    SOURCE_REDIRECT = "SOURCE_REDIRECT"
    EXACT_IDENTIFIER = "EXACT_IDENTIFIER"
    PARENT_CONSTRAINED = "PARENT_CONSTRAINED"
    HUMAN_REVIEW = "HUMAN_REVIEW"
    SIMILARITY_CANDIDATE = "SIMILARITY_CANDIDATE"
    MIGRATED_V1 = "MIGRATED_V1"


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
