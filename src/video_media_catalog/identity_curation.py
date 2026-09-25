"""Immutable, snapshot-pinned human identity curation."""

from __future__ import annotations

from enum import StrEnum
from typing import Any, Literal, Self
from urllib.parse import urlsplit

from pydantic import ValidationInfo, field_validator, model_validator

from video_media_catalog.assertions import SourceNodeRef
from video_media_catalog.canonical import deterministic_key
from video_media_catalog.community_ingest import (
    CommunityIngestRun,
    IngestRunKind,
    build_community_ingest_run,
)
from video_media_catalog.community_rows import (
    entity_ledger_row,
    entity_membership_row,
    entity_merge_event_row,
    entity_redirect_row,
    entity_split_event_row,
    identity_decision_row,
    identity_evidence_row,
)
from video_media_catalog.community_snapshot import (
    CONTROL_MAX_BYTES,
    SILVER_SNAPSHOT_MEDIA_TYPE,
)
from video_media_catalog.community_spark import community_table_schema
from video_media_catalog.community_tables import DATA_TABLE_COLUMNS
from video_media_catalog.identity import require_canonical_uuid7
from video_media_catalog.identity_resolution import (
    source_node_entity_compatible,
    source_node_key,
)
from video_media_catalog.identity_v2 import (
    DecisionStatus,
    EntityLedgerEntry,
    EntityLevel,
    EntityMembership,
    EntityMergeEvent,
    EntityRedirect,
    EntitySplitAssignment,
    EntitySplitEvent,
    EntityStatus,
    EvidenceKind,
    IdentityConflict,
    IdentityDecision,
    IdentityEvidence,
    allocate_entity,
    build_entity_membership,
    build_entity_merge,
    build_entity_redirect,
    build_entity_split_event,
    build_identity_decision,
    build_identity_evidence,
    close_entity_membership,
    validate_redirect_graph,
)
from video_media_catalog.models import ObjectRef
from video_media_catalog.source_lifecycle import (
    select_effective_membership_versions,
)
from video_media_catalog.v2_contracts import (
    V2ContractModel,
    parse_rfc3339,
    require_oidc_subject,
    require_rfc3339,
    require_sha256,
)

IDENTITY_CURATION_MANIFEST_MEDIA_TYPE = (
    "application/vnd.video-media-catalog.identity-curation-manifest.v2+json"
)
IDENTITY_CURATION_POLICY_ID = "identity-human-curation"
IDENTITY_CURATION_POLICY_VERSION = "identity-curation-v2"
MAX_CURATION_OPERATIONS = 256
MAX_SPLIT_ASSIGNMENTS = 4_096
MAX_REDIRECT_HOPS = 64
MAX_REDIRECT_ROWS = 16_384
MAX_CURATION_STATE_ROWS = 16_384
_ZERO_DIGEST = "sha256:" + ("0" * 64)


def _validate_immutable_json_ref(
    reference: ObjectRef,
    *,
    media_type: str,
    label: str,
) -> ObjectRef:
    if reference.model_extra:
        raise ValueError(f"{label} ObjectRef contains unexpected fields")
    if (
        reference.format != "OBJECT_FORMAT_JSON"
        or reference.media_type != media_type
        or not 0 < reference.size_bytes <= CONTROL_MAX_BYTES
        or len(reference.uri) > 2_048
    ):
        raise ValueError(f"{label} must be a non-empty JSON ObjectRef")
    parsed = urlsplit(reference.uri)
    if (
        parsed.scheme not in {"file", "s3"}
        or parsed.query
        or parsed.fragment
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise ValueError(f"{label} must use a bounded file:// or s3:// object")
    if parsed.scheme == "file":
        if not parsed.path:
            raise ValueError(f"{label} file ObjectRef requires a path")
        if reference.object_version is not None or reference.etag is not None:
            raise ValueError(f"{label} file ObjectRef cannot declare S3 metadata")
    else:
        if not parsed.netloc or not parsed.path.lstrip("/"):
            raise ValueError(f"{label} S3 ObjectRef must identify one object")
        if not reference.object_version or not reference.etag:
            raise ValueError(f"{label} S3 ObjectRef requires VersionId and ETag")
    for name, value in (
        ("VersionId", reference.object_version),
        ("ETag", reference.etag),
    ):
        if value is not None and (value != value.strip() or len(value) > 1_024):
            raise ValueError(f"{label} {name} is invalid")
    return reference


class IdentityCurationAction(StrEnum):
    ACCEPT = "ACCEPT"
    REJECT = "REJECT"
    MERGE = "MERGE"
    SPLIT = "SPLIT"
    REDIRECT = "REDIRECT"


class PinnedSilverSnapshot(V2ContractModel):
    object: ObjectRef
    snapshot_set_id: str

    @field_validator("object")
    @classmethod
    def validate_object(cls, value: ObjectRef) -> ObjectRef:
        return _validate_immutable_json_ref(
            value,
            media_type=SILVER_SNAPSHOT_MEDIA_TYPE,
            label="pinned Silver snapshot",
        )

    @field_validator("snapshot_set_id")
    @classmethod
    def validate_snapshot_set_id(cls, value: str) -> str:
        return require_sha256(value, label="snapshot_set_id")


class CurationEntityAllocation(V2ContractModel):
    entity_key: str
    allocation_id: str
    entity_level: EntityLevel
    entity_kind: str

    @field_validator("entity_key")
    @classmethod
    def validate_entity_key(cls, value: str) -> str:
        return require_sha256(value, label="entity_key")

    @field_validator("allocation_id")
    @classmethod
    def validate_allocation_id(cls, value: str) -> str:
        return require_canonical_uuid7(value)

    @field_validator("entity_kind")
    @classmethod
    def validate_entity_kind(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not normalized or len(normalized) > 128:
            raise ValueError("entity_kind must be non-empty and bounded")
        return normalized

    @model_validator(mode="after")
    def validate_key(self) -> Self:
        expected = deterministic_key(
            "catalog-entity-anchor-v2",
            {"allocationId": self.allocation_id},
        )
        if self.entity_key != expected:
            raise ValueError("new entity_key does not match allocation_id")
        return self

    def ledger_entry(self, *, created_at: str) -> EntityLedgerEntry:
        return allocate_entity(
            allocation_id=self.allocation_id,
            entity_level=self.entity_level,
            entity_kind=self.entity_kind,
            created_at=created_at,
        )


class CurationSplitAssignment(V2ContractModel):
    source_node: SourceNodeRef
    target_entity_key: str

    @field_validator("target_entity_key")
    @classmethod
    def validate_target_entity_key(cls, value: str) -> str:
        return require_sha256(value, label="target_entity_key")


class IdentityCurationOperation(V2ContractModel):
    action: IdentityCurationAction
    conflict_key: str
    source_node: SourceNodeRef
    assertion_keys: tuple[str, ...]
    candidate_entity_key: str | None = None
    entity_keys: tuple[str, ...] = ()
    expected_survivor_entity_key: str | None = None
    source_entity_key: str | None = None
    target_entity_key: str | None = None
    split_assignments: tuple[CurationSplitAssignment, ...] = ()
    new_entities: tuple[CurationEntityAllocation, ...] = ()

    @field_validator(
        "conflict_key",
        "candidate_entity_key",
        "expected_survivor_entity_key",
        "source_entity_key",
        "target_entity_key",
    )
    @classmethod
    def validate_optional_key(cls, value: str | None) -> str | None:
        return None if value is None else require_sha256(value)

    @field_validator("assertion_keys", "entity_keys")
    @classmethod
    def normalize_keys(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(sorted({require_sha256(item) for item in value}))

    @field_validator("split_assignments")
    @classmethod
    def normalize_assignments(
        cls,
        value: tuple[CurationSplitAssignment, ...],
    ) -> tuple[CurationSplitAssignment, ...]:
        if len(value) > MAX_SPLIT_ASSIGNMENTS:
            raise ValueError("split assignment count exceeds the curation bound")
        return tuple(
            sorted(
                value,
                key=lambda item: (
                    *source_node_key(item.source_node),
                    item.target_entity_key,
                ),
            )
        )

    @field_validator("new_entities")
    @classmethod
    def normalize_new_entities(
        cls,
        value: tuple[CurationEntityAllocation, ...],
    ) -> tuple[CurationEntityAllocation, ...]:
        return tuple(sorted(value, key=lambda item: item.entity_key))

    @model_validator(mode="after")
    def validate_action_payload(self) -> Self:
        if not self.assertion_keys:
            raise ValueError("curation operation requires conflict assertion keys")
        simple_unused = (
            self.entity_keys
            or self.expected_survivor_entity_key is not None
            or self.source_entity_key is not None
            or self.target_entity_key is not None
            or self.split_assignments
            or self.new_entities
        )
        if self.action in {
            IdentityCurationAction.ACCEPT,
            IdentityCurationAction.REJECT,
        }:
            if self.candidate_entity_key is None or simple_unused:
                raise ValueError(f"{self.action.value} requires only one candidate")
            return self
        if self.candidate_entity_key is not None:
            raise ValueError(f"{self.action.value} cannot declare candidate_entity_key")
        if self.action == IdentityCurationAction.MERGE:
            if (
                len(self.entity_keys) < 2
                or self.expected_survivor_entity_key not in self.entity_keys
                or self.source_entity_key is not None
                or self.target_entity_key is not None
                or self.split_assignments
                or self.new_entities
            ):
                raise ValueError(
                    "MERGE requires entities and an explicit expected survivor"
                )
            return self
        if self.action == IdentityCurationAction.REDIRECT:
            if (
                self.source_entity_key is None
                or self.target_entity_key is None
                or self.source_entity_key == self.target_entity_key
                or self.entity_keys
                or self.expected_survivor_entity_key is not None
                or self.split_assignments
                or self.new_entities
            ):
                raise ValueError("REDIRECT requires one distinct source and target")
            return self
        if (
            self.source_entity_key is None
            or self.target_entity_key is not None
            or self.entity_keys
            or self.expected_survivor_entity_key is not None
            or len(self.split_assignments) < 2
        ):
            raise ValueError("SPLIT requires a source and explicit assignments")
        assignment_nodes = {
            source_node_key(item.source_node) for item in self.split_assignments
        }
        if len(assignment_nodes) != len(self.split_assignments):
            raise ValueError("SPLIT assigns one source node more than once")
        targets = {item.target_entity_key for item in self.split_assignments}
        if len(targets) < 2:
            raise ValueError("SPLIT requires at least two target entities")
        new_keys = {item.entity_key for item in self.new_entities}
        if len(new_keys) != len(self.new_entities) or not new_keys.issubset(targets):
            raise ValueError("SPLIT new entities must be unique assigned targets")
        return self


class IdentityCurationManifest(V2ContractModel):
    schema_version: Literal["2.0"] = "2.0"
    manifest_id: str
    pinned_silver_snapshot: PinnedSilverSnapshot
    operations: tuple[IdentityCurationOperation, ...]
    conflict_keys: tuple[str, ...]
    decision_keys: tuple[str, ...]
    operator_subject: str
    reason: str
    operated_at: str
    config_digest: str
    image_digest: str

    @field_validator(
        "manifest_id",
        "config_digest",
        "image_digest",
    )
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("conflict_keys", "decision_keys")
    @classmethod
    def normalize_keys(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(sorted({require_sha256(item) for item in value}))

    @field_validator("operations")
    @classmethod
    def normalize_operations(
        cls,
        value: tuple[IdentityCurationOperation, ...],
    ) -> tuple[IdentityCurationOperation, ...]:
        if not value or len(value) > MAX_CURATION_OPERATIONS:
            raise ValueError(
                f"curation manifest requires 1..{MAX_CURATION_OPERATIONS} operations"
            )
        return tuple(
            sorted(
                value,
                key=lambda item: (
                    item.conflict_key,
                    item.action.value,
                ),
            )
        )

    @field_validator("operator_subject")
    @classmethod
    def validate_operator_subject(cls, value: str) -> str:
        return require_oidc_subject(value, label="operator_subject")

    @field_validator("reason")
    @classmethod
    def validate_reason(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized or len(normalized) > 2_000:
            raise ValueError("curation reason must be non-empty and bounded")
        return normalized

    @field_validator("operated_at")
    @classmethod
    def validate_operated_at(cls, value: str) -> str:
        return require_rfc3339(value, label="operated_at")

    @model_validator(mode="after")
    def validate_manifest(self, info: ValidationInfo) -> Self:
        operation_conflicts = tuple(
            sorted(operation.conflict_key for operation in self.operations)
        )
        if len(set(operation_conflicts)) != len(operation_conflicts):
            raise ValueError("one conflict may be curated only once per manifest")
        if self.conflict_keys != operation_conflicts:
            raise ValueError("conflict_keys do not match curation operations")
        if (
            sum(len(operation.split_assignments) for operation in self.operations)
            > MAX_SPLIT_ASSIGNMENTS
        ):
            raise ValueError("total split assignments exceed the curation bound")
        context = info.context or {}
        if not context.get("skip_decisions"):
            expected_decisions = _expected_decision_keys(self)
            if self.decision_keys != expected_decisions:
                raise ValueError("decision_keys do not match curation operations")
        if not context.get("skip_identity"):
            expected = deterministic_key(
                "identity-curation-manifest-v2",
                _manifest_identity(self),
            )
            if self.manifest_id != expected:
                raise ValueError("manifest_id does not match curation manifest")
        return self


def _manifest_identity(manifest: IdentityCurationManifest) -> dict[str, Any]:
    return {
        "schemaVersion": manifest.schema_version,
        "pinnedSilverSnapshot": manifest.pinned_silver_snapshot.model_dump(
            mode="json",
            by_alias=True,
            exclude_none=True,
        ),
        "operations": tuple(
            operation.model_dump(mode="json", by_alias=True, exclude_none=True)
            for operation in manifest.operations
        ),
        "conflictKeys": manifest.conflict_keys,
        "decisionKeys": manifest.decision_keys,
        "operatorSubject": manifest.operator_subject,
        "reason": manifest.reason,
        "operatedAt": manifest.operated_at,
        "configDigest": manifest.config_digest,
        "imageDigest": manifest.image_digest,
    }


def _operation_review_material(
    manifest: IdentityCurationManifest,
    operation: IdentityCurationOperation,
) -> tuple[tuple[IdentityEvidence, IdentityDecision], ...]:
    if operation.action in {
        IdentityCurationAction.ACCEPT,
        IdentityCurationAction.REJECT,
    }:
        pairs = ((operation.source_node, operation.candidate_entity_key),)
        status = (
            DecisionStatus.ACCEPT
            if operation.action == IdentityCurationAction.ACCEPT
            else DecisionStatus.REJECT
        )
    elif operation.action == IdentityCurationAction.MERGE:
        pairs = ((operation.source_node, operation.expected_survivor_entity_key),)
        status = DecisionStatus.ACCEPT
    elif operation.action == IdentityCurationAction.REDIRECT:
        pairs = ((operation.source_node, operation.target_entity_key),)
        status = DecisionStatus.ACCEPT
    else:
        pairs = tuple(
            (assignment.source_node, assignment.target_entity_key)
            for assignment in operation.split_assignments
        )
        status = DecisionStatus.ACCEPT

    material = []
    for source_node, entity_key in pairs:
        assert entity_key is not None
        evidence = build_identity_evidence(
            kind=EvidenceKind.HUMAN_REVIEW,
            source_node=source_node,
            candidate_entity_key=entity_key,
            assertion_keys=operation.assertion_keys,
            observed_at=manifest.operated_at,
            policy_id=IDENTITY_CURATION_POLICY_ID,
            policy_digest=manifest.config_digest,
            confidence=1.0,
            details={
                "action": operation.action.value,
                "conflictKey": operation.conflict_key,
            },
        )
        decision = build_identity_decision(
            status=status,
            source_node=source_node,
            entity_key=entity_key,
            evidence_keys=(evidence.evidence_key,),
            policy_version=IDENTITY_CURATION_POLICY_VERSION,
            decided_by=manifest.operator_subject,
            decided_at=manifest.operated_at,
            reason=manifest.reason,
        )
        material.append((evidence, decision))
    return tuple(material)


def _expected_decision_keys(
    manifest: IdentityCurationManifest,
) -> tuple[str, ...]:
    return tuple(
        sorted(
            decision.decision_id
            for operation in manifest.operations
            for _, decision in _operation_review_material(manifest, operation)
        )
    )


def build_identity_curation_manifest(**values: Any) -> IdentityCurationManifest:
    operations = tuple(
        IdentityCurationOperation.model_validate(operation)
        for operation in (values.get("operations") or ())
    )
    values = {**values, "operations": operations}
    derived_conflicts = tuple(
        sorted(operation.conflict_key for operation in operations)
    )
    provided_conflicts = values.get("conflict_keys")
    if (
        provided_conflicts is not None
        and tuple(sorted(provided_conflicts)) != derived_conflicts
    ):
        raise ValueError("provided conflict_keys do not match operations")
    provisional = IdentityCurationManifest.model_validate(
        {
            **values,
            "manifest_id": _ZERO_DIGEST,
            "conflict_keys": derived_conflicts,
            "decision_keys": (),
        },
        context={"skip_identity": True, "skip_decisions": True},
    )
    decisions = _expected_decision_keys(provisional)
    provided_decisions = values.get("decision_keys")
    if (
        provided_decisions is not None
        and tuple(sorted(provided_decisions)) != decisions
    ):
        raise ValueError("provided decision_keys do not match operations")
    normalized = provisional.model_dump(mode="python")
    normalized["decision_keys"] = decisions
    with_decisions = IdentityCurationManifest.model_validate(
        normalized,
        context={"skip_identity": True},
    )
    normalized["manifest_id"] = deterministic_key(
        "identity-curation-manifest-v2",
        _manifest_identity(with_decisions),
    )
    return IdentityCurationManifest.model_validate(normalized)


class IdentityCurationManifestRef(V2ContractModel):
    manifest_id: str
    object: ObjectRef

    @field_validator("manifest_id")
    @classmethod
    def validate_manifest_id(cls, value: str) -> str:
        return require_sha256(value, label="manifest_id")

    @field_validator("object")
    @classmethod
    def validate_object(cls, value: ObjectRef) -> ObjectRef:
        return _validate_immutable_json_ref(
            value,
            media_type=IDENTITY_CURATION_MANIFEST_MEDIA_TYPE,
            label="identity curation manifest",
        )


def build_identity_curation_manifest_ref(
    manifest: IdentityCurationManifest,
    reference: ObjectRef,
) -> IdentityCurationManifestRef:
    return IdentityCurationManifestRef(
        manifest_id=manifest.manifest_id,
        object=reference,
    )


class IdentityCurationState(V2ContractModel):
    conflicts: tuple[IdentityConflict, ...]
    entities: tuple[EntityLedgerEntry, ...]
    evidence: tuple[IdentityEvidence, ...] = ()
    memberships: tuple[EntityMembership, ...] = ()
    redirects: tuple[EntityRedirect, ...] = ()


class IdentityCurationResult(V2ContractModel):
    entities: tuple[EntityLedgerEntry, ...] = ()
    evidence: tuple[IdentityEvidence, ...] = ()
    decisions: tuple[IdentityDecision, ...] = ()
    memberships: tuple[EntityMembership, ...] = ()
    redirects: tuple[EntityRedirect, ...] = ()
    merge_events: tuple[EntityMergeEvent, ...] = ()
    split_events: tuple[EntitySplitEvent, ...] = ()


def _unique_by_key(values, key_name: str, *, label: str) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for value in values:
        key = getattr(value, key_name)
        if key in result:
            raise ValueError(f"pinned snapshot contains duplicate {label} {key}")
        result[key] = value
    return result


def _active_memberships(
    memberships: tuple[EntityMembership, ...],
    *,
    as_of: str,
) -> dict[tuple[str, str, str], EntityMembership]:
    instant = parse_rfc3339(as_of)
    versions: dict[
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
        existing = versions.get(version_key)
        if existing is None:
            versions[version_key] = membership
            continue
        closed = {
            value
            for value in (existing.valid_to, membership.valid_to)
            if value is not None
        }
        if len(closed) > 1:
            raise ValueError("membership has conflicting closure times")
        if membership.valid_to is not None:
            versions[version_key] = membership

    active: dict[tuple[str, str, str], EntityMembership] = {}
    for membership in versions.values():
        if parse_rfc3339(membership.valid_from) > instant:
            continue
        if (
            membership.valid_to is not None
            and parse_rfc3339(membership.valid_to) <= instant
        ):
            continue
        node_key = source_node_key(membership.source_node)
        if node_key in active:
            raise ValueError("source node has multiple active memberships")
        active[node_key] = membership
    return active


def _dedupe_output(values, key_name: str, *, label: str) -> tuple[Any, ...]:
    indexed: dict[str, Any] = {}
    for value in values:
        key = getattr(value, key_name)
        existing = indexed.get(key)
        if existing is not None and existing != value:
            raise ValueError(f"curation produced conflicting {label} {key}")
        indexed[key] = value
    return tuple(indexed[key] for key in sorted(indexed))


def materialize_identity_curation(
    manifest: IdentityCurationManifest,
    state: IdentityCurationState,
) -> IdentityCurationResult:
    """Validate a pinned review and atomically materialize all identity events."""

    conflicts = _unique_by_key(
        state.conflicts,
        "conflict_key",
        label="conflict",
    )
    entities = _unique_by_key(state.entities, "entity_key", label="entity")
    active_memberships = _active_memberships(
        state.memberships,
        as_of=manifest.operated_at,
    )
    reviewed_conflicts = {
        conflict_key
        for item in state.evidence
        if item.kind == EvidenceKind.HUMAN_REVIEW
        and isinstance(
            conflict_key := item.details.get("conflictKey"),
            str,
        )
    }
    new_entities: list[EntityLedgerEntry] = []
    evidence: list[IdentityEvidence] = []
    decisions: list[IdentityDecision] = []
    memberships: list[EntityMembership] = []
    redirects: list[EntityRedirect] = []
    merge_events: list[EntityMergeEvent] = []
    split_events: list[EntitySplitEvent] = []
    opened_nodes: set[tuple[str, str, str]] = set()
    closed_nodes: set[tuple[str, str, str]] = set()

    def require_entity(
        entity_key: str,
        *,
        source_node: SourceNodeRef | None = None,
    ) -> EntityLedgerEntry:
        entity = entities.get(entity_key)
        if entity is None:
            raise ValueError(
                f"curation entity is absent from pinned snapshot: {entity_key}"
            )
        if entity.status != EntityStatus.ACTIVE:
            raise ValueError("curation candidates must be active entities")
        if source_node is not None and not source_node_entity_compatible(
            source_node,
            entity,
        ):
            raise ValueError("source node and candidate entity types are incompatible")
        return entity

    def require_unassigned(node: SourceNodeRef) -> None:
        key = source_node_key(node)
        if key in active_memberships or key in opened_nodes:
            raise ValueError("curation source node already has an active membership")
        opened_nodes.add(key)

    for operation in manifest.operations:
        conflict = conflicts.get(operation.conflict_key)
        if conflict is None:
            raise ValueError(
                "curation conflict is absent from the pinned Silver snapshot"
            )
        if operation.conflict_key in reviewed_conflicts:
            raise ValueError("curation conflict already has a committed review")
        if (
            conflict.source_node != operation.source_node
            or conflict.assertion_keys != operation.assertion_keys
        ):
            raise ValueError("curation operation does not match its pinned conflict")
        if parse_rfc3339(manifest.operated_at) < parse_rfc3339(conflict.observed_at):
            raise ValueError("curation operation cannot precede its conflict")
        conflict_candidates = set(conflict.candidate_entity_keys)
        review_material = _operation_review_material(manifest, operation)

        if operation.action in {
            IdentityCurationAction.ACCEPT,
            IdentityCurationAction.REJECT,
        }:
            assert operation.candidate_entity_key is not None
            if operation.candidate_entity_key not in conflict_candidates:
                raise ValueError("curation candidate is absent from pinned conflict")
            require_entity(
                operation.candidate_entity_key,
                source_node=operation.source_node,
            )
            operation_evidence, decision = review_material[0]
            evidence.append(operation_evidence)
            decisions.append(decision)
            if operation.action == IdentityCurationAction.ACCEPT:
                require_unassigned(operation.source_node)
                memberships.append(
                    build_entity_membership(
                        source_node=operation.source_node,
                        entity_key=operation.candidate_entity_key,
                        decision_id=decision.decision_id,
                        valid_from=manifest.operated_at,
                    )
                )
            continue

        if operation.action == IdentityCurationAction.MERGE:
            if set(operation.entity_keys) != conflict_candidates:
                raise ValueError(
                    "MERGE must account for every pinned conflict candidate"
                )
            merge_entities = tuple(
                require_entity(entity_key) for entity_key in operation.entity_keys
            )
            assert operation.expected_survivor_entity_key is not None
            require_entity(
                operation.expected_survivor_entity_key,
                source_node=operation.source_node,
            )
            operation_evidence, decision = review_material[0]
            merge = build_entity_merge(
                entries=merge_entities,
                decision_id=decision.decision_id,
                effective_at=manifest.operated_at,
                merged_by=manifest.operator_subject,
                reason=manifest.reason,
            )
            if (
                merge.event.survivor_entity_key
                != operation.expected_survivor_entity_key
            ):
                raise ValueError("MERGE expected survivor is not the stable survivor")
            require_unassigned(operation.source_node)
            evidence.append(operation_evidence)
            decisions.append(decision)
            memberships.append(
                build_entity_membership(
                    source_node=operation.source_node,
                    entity_key=merge.event.survivor_entity_key,
                    decision_id=decision.decision_id,
                    valid_from=manifest.operated_at,
                )
            )
            redirects.extend(merge.redirects)
            merge_events.append(merge.event)
            continue

        if operation.action == IdentityCurationAction.REDIRECT:
            assert operation.source_entity_key is not None
            assert operation.target_entity_key is not None
            if not {
                operation.source_entity_key,
                operation.target_entity_key,
            }.issubset(conflict_candidates):
                raise ValueError("REDIRECT entities are absent from pinned conflict")
            source_entity = require_entity(operation.source_entity_key)
            target_entity = require_entity(
                operation.target_entity_key,
                source_node=operation.source_node,
            )
            if (
                source_entity.entity_level != target_entity.entity_level
                or source_entity.entity_kind != target_entity.entity_kind
            ):
                raise ValueError("REDIRECT entities have incompatible types")
            operation_evidence, decision = review_material[0]
            require_unassigned(operation.source_node)
            evidence.append(operation_evidence)
            decisions.append(decision)
            memberships.append(
                build_entity_membership(
                    source_node=operation.source_node,
                    entity_key=operation.target_entity_key,
                    decision_id=decision.decision_id,
                    valid_from=manifest.operated_at,
                )
            )
            redirects.append(
                build_entity_redirect(
                    source_entity_key=operation.source_entity_key,
                    target_entity_key=operation.target_entity_key,
                    effective_at=manifest.operated_at,
                    decision_id=decision.decision_id,
                )
            )
            continue

        assert operation.source_entity_key is not None
        if operation.source_entity_key not in conflict_candidates:
            raise ValueError("SPLIT source is absent from pinned conflict")
        source_entity = require_entity(operation.source_entity_key)
        for allocation in operation.new_entities:
            if allocation.entity_key in entities:
                raise ValueError("SPLIT new entity already exists in pinned snapshot")
            entry = allocation.ledger_entry(created_at=manifest.operated_at)
            entities[entry.entity_key] = entry
            new_entities.append(entry)

        assignment_nodes = {
            source_node_key(assignment.source_node)
            for assignment in operation.split_assignments
        }
        current_source_memberships = {
            node_key: membership
            for node_key, membership in active_memberships.items()
            if membership.entity_key == operation.source_entity_key
        }
        expected_nodes = set(current_source_memberships)
        expected_nodes.add(source_node_key(conflict.source_node))
        if assignment_nodes != expected_nodes:
            raise ValueError(
                "SPLIT assignments must cover each current member and conflict source"
            )
        conflict_membership = active_memberships.get(
            source_node_key(conflict.source_node)
        )
        if (
            conflict_membership is not None
            and conflict_membership.entity_key != operation.source_entity_key
        ):
            raise ValueError("SPLIT conflict source belongs to another entity")

        material_by_node = {
            source_node_key(decision.source_node): (item_evidence, decision)
            for item_evidence, decision in review_material
        }
        split_assignments = []
        for assignment in operation.split_assignments:
            target = require_entity(
                assignment.target_entity_key,
                source_node=assignment.source_node,
            )
            if (
                target.entity_level != source_entity.entity_level
                or target.entity_kind != source_entity.entity_kind
            ):
                raise ValueError("SPLIT target type differs from the source entity")
            node_key = source_node_key(assignment.source_node)
            if node_key in opened_nodes:
                raise ValueError("one source node is assigned by multiple operations")
            opened_nodes.add(node_key)
            item_evidence, decision = material_by_node[node_key]
            evidence.append(item_evidence)
            decisions.append(decision)
            memberships.append(
                build_entity_membership(
                    source_node=assignment.source_node,
                    entity_key=assignment.target_entity_key,
                    decision_id=decision.decision_id,
                    valid_from=manifest.operated_at,
                )
            )
            split_assignments.append(
                EntitySplitAssignment(
                    source_node=assignment.source_node,
                    target_entity_key=assignment.target_entity_key,
                    decision_id=decision.decision_id,
                )
            )

        for node_key, membership in current_source_memberships.items():
            if node_key in closed_nodes:
                raise ValueError("one active membership is closed more than once")
            closed_nodes.add(node_key)
            memberships.append(
                close_entity_membership(
                    membership,
                    closed_at=manifest.operated_at,
                )
            )
        split_events.append(
            build_entity_split_event(
                source_entity_key=operation.source_entity_key,
                assignments=tuple(split_assignments),
                effective_at=manifest.operated_at,
                split_by=manifest.operator_subject,
                reason=manifest.reason,
            )
        )

    active_redirects = tuple(
        redirect
        for redirect in state.redirects
        if parse_rfc3339(redirect.effective_at) <= parse_rfc3339(manifest.operated_at)
    )
    validate_redirect_graph((*active_redirects, *redirects))
    actual_decisions = tuple(sorted(item.decision_id for item in decisions))
    if actual_decisions != manifest.decision_keys:
        raise RuntimeError("materialized decisions differ from curation manifest")

    return IdentityCurationResult(
        entities=_dedupe_output(new_entities, "entity_key", label="entity"),
        evidence=_dedupe_output(evidence, "evidence_key", label="evidence"),
        decisions=_dedupe_output(decisions, "decision_id", label="decision"),
        memberships=_dedupe_output(
            memberships,
            "membership_key",
            label="membership",
        ),
        redirects=_dedupe_output(redirects, "redirect_key", label="redirect"),
        merge_events=_dedupe_output(
            merge_events,
            "merge_event_key",
            label="merge event",
        ),
        split_events=_dedupe_output(
            split_events,
            "split_event_key",
            label="split event",
        ),
    )


def _entity_from_row(row: Any) -> EntityLedgerEntry:
    return EntityLedgerEntry(
        entity_key=row["entity_key"],
        allocation_id=row["allocation_id"],
        entity_level=row["entity_level"],
        entity_kind=row["entity_kind"],
        status=row["status"],
        created_at=row["created_at"],
        first_release_id=row["first_release_id"],
    )


def _membership_from_row(row: Any) -> EntityMembership:
    return EntityMembership(
        membership_key=row["membership_key"],
        source_node=SourceNodeRef(
            namespace_id=row["source_namespace_id"],
            source_id=row["source_id"],
            referent_kind=row["source_referent_kind"],
        ),
        entity_key=row["entity_key"],
        decision_id=row["decision_id"],
        valid_from=row["valid_from"],
        valid_to=row["valid_to"],
    )


def _redirect_from_row(row: Any) -> EntityRedirect:
    return EntityRedirect(
        redirect_key=row["redirect_key"],
        source_entity_key=row["source_entity_key"],
        target_entity_key=row["target_entity_key"],
        effective_at=row["effective_at"],
        decision_id=row["decision_id"],
    )


def _required_entity_keys(
    manifest: IdentityCurationManifest,
) -> tuple[str, ...]:
    keys: set[str] = set()
    new_keys: set[str] = set()
    for operation in manifest.operations:
        if operation.candidate_entity_key is not None:
            keys.add(operation.candidate_entity_key)
        keys.update(operation.entity_keys)
        if operation.source_entity_key is not None:
            keys.add(operation.source_entity_key)
        if operation.target_entity_key is not None:
            keys.add(operation.target_entity_key)
        keys.update(
            assignment.target_entity_key for assignment in operation.split_assignments
        )
        new_keys.update(entity.entity_key for entity in operation.new_entities)
    return tuple(sorted(keys - new_keys))


def _relevant_membership_frame(
    visible_silver: dict[str, Any],
    *,
    manifest: IdentityCurationManifest,
) -> Any:
    from pyspark.sql import functions as F

    predicate = F.lit(False)
    split_sources = tuple(
        sorted(
            {
                operation.source_entity_key
                for operation in manifest.operations
                if operation.action == IdentityCurationAction.SPLIT
                and operation.source_entity_key is not None
            }
        )
    )
    if split_sources:
        predicate = predicate | F.col("entity_key").isin(*split_sources)
    nodes = {
        source_node_key(operation.source_node) for operation in manifest.operations
    }
    nodes.update(
        source_node_key(assignment.source_node)
        for operation in manifest.operations
        for assignment in operation.split_assignments
    )
    for namespace_id, source_id, referent_kind in sorted(nodes):
        predicate = predicate | (
            (F.col("source_namespace_id") == namespace_id)
            & (F.col("source_id") == source_id)
            & (F.col("source_referent_kind") == referent_kind)
        )
    return select_effective_membership_versions(
        visible_silver["community_entity_membership"].where(predicate),
        as_of=manifest.operated_at,
    )


def _collect_redirect_closure(
    frame: Any,
    *,
    start_keys: tuple[str, ...],
    as_of: str,
) -> tuple[EntityRedirect, ...]:
    from pyspark.sql import functions as F

    if not start_keys:
        return ()
    eligible = frame.where(
        F.to_timestamp("effective_at") <= F.to_timestamp(F.lit(as_of))
    )
    frontier = set(start_keys)
    visited: set[str] = set()
    redirects: list[EntityRedirect] = []
    for hop in range(MAX_REDIRECT_HOPS + 1):
        sources = tuple(sorted(frontier - visited))
        if not sources:
            result = _dedupe_output(
                redirects,
                "redirect_key",
                label="pinned redirect",
            )
            validate_redirect_graph(result)
            return result
        rows = (
            eligible.where(F.col("source_entity_key").isin(*sources))
            .limit(MAX_REDIRECT_ROWS + 1)
            .collect()
        )
        if len(redirects) + len(rows) > MAX_REDIRECT_ROWS:
            raise ValueError("redirect closure exceeds the curation bound")
        if rows and hop == MAX_REDIRECT_HOPS:
            raise ValueError("redirect chain exceeds the curation hop limit")
        visited.update(sources)
        current = tuple(_redirect_from_row(row) for row in rows)
        redirects.extend(current)
        frontier = {redirect.target_entity_key for redirect in current}
    raise RuntimeError("redirect traversal did not terminate")


def _load_identity_curation_state(
    visible_silver: dict[str, Any],
    *,
    manifest: IdentityCurationManifest,
) -> IdentityCurationState:
    from pyspark.sql import functions as F

    required = {
        "community_identity_conflict",
        "community_identity_evidence",
        "community_entity_ledger",
        "community_entity_membership",
        "community_entity_redirect",
    }
    if not required.issubset(visible_silver):
        raise ValueError("identity curation is missing pinned Silver tables")

    conflict_rows = (
        visible_silver["community_identity_conflict"]
        .where(F.col("conflict_key").isin(*manifest.conflict_keys))
        .select("conflict_key", "conflict_json")
        .limit(len(manifest.conflict_keys) + 1)
        .collect()
    )
    if len(conflict_rows) != len(manifest.conflict_keys):
        raise ValueError(
            "curation conflicts are missing or duplicated in pinned Silver"
        )
    conflicts = tuple(
        IdentityConflict.model_validate_json(row["conflict_json"])
        for row in conflict_rows
    )
    if {conflict.conflict_key for conflict in conflicts} != set(manifest.conflict_keys):
        raise ValueError("pinned conflict JSON does not match conflict rows")

    evidence_rows = (
        visible_silver["community_identity_evidence"]
        .where(F.col("kind") == EvidenceKind.HUMAN_REVIEW.value)
        .where(
            F.get_json_object("details_json", "$.conflictKey").isin(
                *manifest.conflict_keys
            )
        )
        .select("evidence_json")
        .limit(MAX_CURATION_STATE_ROWS + 1)
        .collect()
    )
    if len(evidence_rows) > MAX_CURATION_STATE_ROWS:
        raise ValueError("existing review evidence exceeds the curation bound")
    existing_evidence = tuple(
        IdentityEvidence.model_validate_json(row["evidence_json"])
        for row in evidence_rows
    )

    entity_keys = _required_entity_keys(manifest)
    entity_rows = (
        visible_silver["community_entity_ledger"]
        .where(F.col("entity_key").isin(*entity_keys))
        .limit(len(entity_keys) + 1)
        .collect()
        if entity_keys
        else []
    )
    entities = tuple(_entity_from_row(row) for row in entity_rows)

    membership_rows = (
        _relevant_membership_frame(
            visible_silver,
            manifest=manifest,
        )
        .limit(MAX_CURATION_STATE_ROWS + 1)
        .collect()
    )
    if len(membership_rows) > MAX_CURATION_STATE_ROWS:
        raise ValueError("relevant membership state exceeds the curation bound")
    memberships = tuple(_membership_from_row(row) for row in membership_rows)

    redirect_start_keys = tuple(
        sorted(
            {
                key
                for operation in manifest.operations
                for key in (
                    *operation.entity_keys,
                    operation.source_entity_key,
                    operation.target_entity_key,
                    operation.expected_survivor_entity_key,
                )
                if key is not None
            }
        )
    )
    redirects = _collect_redirect_closure(
        visible_silver["community_entity_redirect"],
        start_keys=redirect_start_keys,
        as_of=manifest.operated_at,
    )
    return IdentityCurationState(
        conflicts=conflicts,
        entities=entities,
        evidence=existing_evidence,
        memberships=memberships,
        redirects=redirects,
    )


def build_identity_curation_dataframes(
    spark: Any,
    *,
    visible_silver: dict[str, Any],
    manifest: IdentityCurationManifest,
    manifest_ref: IdentityCurationManifestRef,
) -> tuple[CommunityIngestRun, dict[str, Any]]:
    """Build one bounded curation run over exact pinned Silver snapshots."""

    if manifest_ref.manifest_id != manifest.manifest_id:
        raise ValueError("curation manifest ObjectRef binds another manifest")
    state = _load_identity_curation_state(
        visible_silver,
        manifest=manifest,
    )
    result = materialize_identity_curation(manifest, state)
    values_by_table = {
        "community_entity_ledger": result.entities,
        "community_identity_evidence": result.evidence,
        "community_identity_decision": result.decisions,
        "community_entity_membership": result.memberships,
        "community_entity_redirect": result.redirects,
        "community_entity_merge_event": result.merge_events,
        "community_entity_split_event": result.split_events,
    }
    expected_counts = {table: 0 for table in DATA_TABLE_COLUMNS}
    for table, values in values_by_table.items():
        expected_counts[table] = len(values)

    run = build_community_ingest_run(
        run_kind=IngestRunKind.IDENTITY_CURATION,
        source_product_id="identity-curation-v2",
        input_id=manifest.manifest_id,
        policy_id=IDENTITY_CURATION_POLICY_ID,
        policy_digest=manifest.config_digest,
        image_digest=manifest.image_digest,
        config_digest=manifest.config_digest,
        started_at=manifest.operated_at,
        expected_counts=expected_counts,
        input_manifest={
            "curationManifest": manifest_ref.model_dump(
                mode="json",
                by_alias=True,
                exclude_none=True,
            ),
            "pinnedSilverSnapshot": manifest.pinned_silver_snapshot.model_dump(
                mode="json",
                by_alias=True,
                exclude_none=True,
            ),
            "conflictKeys": manifest.conflict_keys,
            "decisionKeys": manifest.decision_keys,
            "operatorSubject": manifest.operator_subject,
            "operatedAt": manifest.operated_at,
        },
    )
    row_builders = {
        "community_entity_ledger": entity_ledger_row,
        "community_identity_evidence": identity_evidence_row,
        "community_identity_decision": identity_decision_row,
        "community_entity_membership": entity_membership_row,
        "community_entity_redirect": entity_redirect_row,
        "community_entity_merge_event": entity_merge_event_row,
        "community_entity_split_event": entity_split_event_row,
    }
    dataframes = {}
    try:
        for table in DATA_TABLE_COLUMNS:
            values = values_by_table.get(table, ())
            builder = row_builders.get(table)
            rows = (
                []
                if builder is None
                else [builder(run.run_id, value) for value in values]
            )
            frame = spark.createDataFrame(
                rows,
                schema=community_table_schema(table),
            ).persist()
            if frame.count() != expected_counts[table]:
                frame.unpersist()
                raise RuntimeError(f"{table} curation materialization changed")
            dataframes[table] = frame
        return run, dataframes
    except Exception:
        for frame in dataframes.values():
            frame.unpersist()
        raise
