"""Policy-specific Gold v2 contracts and deterministic row identities."""

from __future__ import annotations

import json
from enum import StrEnum
from typing import Any, Self

from pydantic import (
    Field,
    ValidationInfo,
    field_serializer,
    field_validator,
    model_validator,
)

from video_media_catalog.canonical import canonical_json, deterministic_key
from video_media_catalog.community_release import ReleasePolicyContext
from video_media_catalog.gold_tables import GOLD_DATA_COLUMNS
from video_media_catalog.rights import PolicyZone, UsageAction
from video_media_catalog.v2_contracts import (
    V2ContractModel,
    digest_identity,
    require_https_url,
    require_oidc_subject,
    require_rfc3339,
    require_sha256,
    require_slug,
)

_ZERO_DIGEST = "sha256:" + ("0" * 64)
PERSONAL_RESEARCH_CONTEXT_ID = "personal-research"
PERSONAL_RESEARCH_AUDIENCE = "personal"
PERSONAL_RESEARCH_PURPOSE = "research"
PERSONAL_RESEARCH_ALLOWED_ZONES = tuple(
    sorted(
        (
            PolicyZone.OPEN_CC0,
            PolicyZone.OPEN_ATTRIBUTED,
            PolicyZone.OPEN_SHAREALIKE,
            PolicyZone.PUBLIC_REGISTRY,
            PolicyZone.RESEARCH_PRIVATE,
        ),
        key=str,
    )
)


class ResolutionOperator(StrEnum):
    SINGLE = "SINGLE"
    SET_UNION = "SET_UNION"
    KEEP_ALL = "KEEP_ALL"
    NEVER_RESOLVE = "NEVER_RESOLVE"


class GoldResolutionStatus(StrEnum):
    SELECTED = "SELECTED"
    SET = "SET"
    CONFLICTED = "CONFLICTED"
    WITHHELD = "WITHHELD"


class FieldPolicyRule(V2ContractModel):
    predicate: str
    operator: ResolutionOperator
    scope_qualifiers: tuple[str, ...] = ()

    @field_validator("predicate")
    @classmethod
    def validate_predicate(cls, value: str) -> str:
        return require_slug(value, label="predicate")

    @field_validator("scope_qualifiers")
    @classmethod
    def normalize_scope(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(sorted({item.strip() for item in value if item.strip()}))


class GoldResolutionPolicy(V2ContractModel):
    policy_id: str
    policy_version: str
    requested_actions: tuple[UsageAction, ...]
    rules: tuple[FieldPolicyRule, ...]
    default_operator: ResolutionOperator = ResolutionOperator.NEVER_RESOLVE
    max_conflict_ratio: float = Field(default=0.05, ge=0, le=1)
    max_unresolved_identity_ratio: float = Field(default=0.05, ge=0, le=1)

    @field_validator("policy_id")
    @classmethod
    def validate_policy_id(cls, value: str) -> str:
        return require_slug(value, label="Gold policy_id")

    @field_validator("policy_version")
    @classmethod
    def validate_version(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized or len(normalized) > 64:
            raise ValueError("Gold policy_version must be non-empty")
        return normalized

    @field_validator("requested_actions")
    @classmethod
    def normalize_actions(
        cls, value: tuple[UsageAction, ...]
    ) -> tuple[UsageAction, ...]:
        normalized = tuple(sorted(set(value), key=str))
        if not normalized:
            raise ValueError("Gold policy requires requested_actions")
        return normalized

    @field_serializer("requested_actions", when_used="json")
    def serialize_actions(self, value: tuple[UsageAction, ...]) -> list[str]:
        return [item.value for item in value]

    @field_validator("rules")
    @classmethod
    def sort_rules(
        cls, value: tuple[FieldPolicyRule, ...]
    ) -> tuple[FieldPolicyRule, ...]:
        return tuple(sorted(value, key=lambda item: item.predicate))

    @model_validator(mode="after")
    def validate_rules(self) -> Self:
        predicates = [rule.predicate for rule in self.rules]
        if len(predicates) != len(set(predicates)):
            raise ValueError("Gold policy contains duplicate predicate rules")
        return self

    @property
    def digest(self) -> str:
        return digest_identity(
            self.model_dump(mode="json", by_alias=True, exclude_none=True)
        )

    def rule_for(self, predicate: str) -> FieldPolicyRule:
        return next(
            (rule for rule in self.rules if rule.predicate == predicate),
            FieldPolicyRule(
                predicate=predicate,
                operator=self.default_operator,
            ),
        )


def personal_research_context(
    *,
    as_of: str,
    territories: tuple[str, ...] = ("*",),
) -> ReleasePolicyContext:
    return ReleasePolicyContext(
        context_id=PERSONAL_RESEARCH_CONTEXT_ID,
        audience=PERSONAL_RESEARCH_AUDIENCE,
        purpose=PERSONAL_RESEARCH_PURPOSE,
        territories=territories,
        as_of=as_of,
        allowed_zones=PERSONAL_RESEARCH_ALLOWED_ZONES,
    )


def personal_research_policy() -> GoldResolutionPolicy:
    return GoldResolutionPolicy(
        policy_id="personal-research-display-v1",
        policy_version="1.0.0",
        requested_actions=(
            UsageAction.STORE,
            UsageAction.TRANSFORM,
            UsageAction.DISPLAY,
            UsageAction.SEARCH,
        ),
        rules=(
            FieldPolicyRule(
                predicate="title",
                operator=ResolutionOperator.SINGLE,
                scope_qualifiers=("language", "titleRole"),
            ),
            FieldPolicyRule(
                predicate="format",
                operator=ResolutionOperator.SINGLE,
            ),
            FieldPolicyRule(
                predicate="language",
                operator=ResolutionOperator.SINGLE,
            ),
            FieldPolicyRule(
                predicate="status",
                operator=ResolutionOperator.SINGLE,
            ),
            FieldPolicyRule(
                predicate="premiered",
                operator=ResolutionOperator.SINGLE,
            ),
            FieldPolicyRule(
                predicate="ended",
                operator=ResolutionOperator.SINGLE,
            ),
            FieldPolicyRule(
                predicate="runtime_minutes",
                operator=ResolutionOperator.SINGLE,
            ),
            FieldPolicyRule(
                predicate="average_runtime_minutes",
                operator=ResolutionOperator.SINGLE,
            ),
            FieldPolicyRule(
                predicate="genre",
                operator=ResolutionOperator.SET_UNION,
                scope_qualifiers=("vocabulary",),
            ),
        ),
    )


class GoldRightsLineage(V2ContractModel):
    policy_id: str
    policy_zone: PolicyZone
    license_id: str
    license_uri: str | None = None
    attribution_text: str
    source_url: str
    share_alike: bool = False

    @field_validator("policy_id")
    @classmethod
    def validate_policy_id(cls, value: str) -> str:
        return require_slug(value, label="policy_id")

    @field_validator("license_id", "attribution_text")
    @classmethod
    def validate_text(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized or len(normalized) > 2000:
            raise ValueError("Gold rights lineage text must be non-empty")
        return normalized

    @field_validator("license_uri", "source_url")
    @classmethod
    def validate_url(cls, value: str | None) -> str | None:
        return None if value is None else require_https_url(value)


class GoldAssertionLineage(V2ContractModel):
    assertion_id: str
    source_product_id: str
    source_name: str
    source_record_id: str
    source_path: str
    observed_at: str
    citation_keys: tuple[str, ...] = ()
    rights: GoldRightsLineage

    @field_validator("assertion_id")
    @classmethod
    def validate_assertion_id(cls, value: str) -> str:
        return require_sha256(value, label="assertion_id")

    @field_validator("source_product_id")
    @classmethod
    def validate_source_product_id(cls, value: str) -> str:
        return require_slug(value, label="source_product_id")

    @field_validator("source_name", "source_record_id", "source_path")
    @classmethod
    def validate_text(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized or len(normalized) > 2048:
            raise ValueError("Gold assertion lineage text must be non-empty")
        return normalized

    @field_validator("observed_at")
    @classmethod
    def validate_observed_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @field_validator("citation_keys")
    @classmethod
    def normalize_citation_keys(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(
            sorted({require_sha256(item, label="citation key") for item in value})
        )


def _validate_counts(value: dict[str, int]) -> dict[str, int]:
    if set(value) != set(GOLD_DATA_COLUMNS):
        raise ValueError("Gold counts must contain every Gold data table")
    if any(isinstance(count, bool) or count < 0 for count in value.values()):
        raise ValueError("Gold counts must be non-negative integers")
    return dict(sorted(value.items()))


class GoldReleasePlan(V2ContractModel):
    schema_version: str = "2.0"
    release_plan_id: str
    owner_subject: str
    policy_context: ReleasePolicyContext
    committed_run_ids: tuple[str, ...]
    silver_snapshot_ids: dict[str, int | None]
    identity_snapshot_ids: dict[str, int | None]
    rights_registry_digest: str
    field_policy_digest: str
    resolver_digest: str
    image_digest: str
    config_digest: str
    expected_counts: dict[str, int]
    planned_at: str

    @field_validator(
        "release_plan_id",
        "rights_registry_digest",
        "field_policy_digest",
        "resolver_digest",
        "image_digest",
        "config_digest",
    )
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("owner_subject")
    @classmethod
    def validate_owner_subject(cls, value: str) -> str:
        return require_oidc_subject(value)

    @field_validator("committed_run_ids")
    @classmethod
    def normalize_runs(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(
            sorted({require_sha256(item, label="run_id") for item in value})
        )
        if not normalized:
            raise ValueError("Gold release requires committed runs")
        return normalized

    @field_validator("silver_snapshot_ids", "identity_snapshot_ids")
    @classmethod
    def validate_snapshots(cls, value: dict[str, int | None]) -> dict[str, int | None]:
        if not value:
            raise ValueError("Gold release requires pinned snapshot IDs")
        if any(
            snapshot is not None and (isinstance(snapshot, bool) or snapshot <= 0)
            for snapshot in value.values()
        ):
            raise ValueError("Gold snapshot IDs must be positive when present")
        return dict(sorted(value.items()))

    @field_validator("expected_counts")
    @classmethod
    def validate_expected_counts(cls, value: dict[str, int]) -> dict[str, int]:
        return _validate_counts(value)

    @field_validator("planned_at")
    @classmethod
    def validate_planned_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @model_validator(mode="after")
    def validate_plan(self, info: ValidationInfo) -> Self:
        context = self.policy_context
        if (
            context.context_id != PERSONAL_RESEARCH_CONTEXT_ID
            or context.audience != PERSONAL_RESEARCH_AUDIENCE
            or context.purpose != PERSONAL_RESEARCH_PURPOSE
            or context.allowed_zones != PERSONAL_RESEARCH_ALLOWED_ZONES
        ):
            raise ValueError(
                "Gold releases must use the single personal-research context"
            )
        if not (info.context or {}).get("skip_identity"):
            expected = deterministic_key(
                "community-gold-release-plan-v2",
                _plan_identity(self),
            )
            if self.release_plan_id != expected:
                raise ValueError("release_plan_id does not match plan identity")
        return self


def _plan_identity(plan: GoldReleasePlan) -> dict[str, Any]:
    return {
        "schemaVersion": plan.schema_version,
        "ownerSubject": plan.owner_subject,
        "policyContext": plan.policy_context.model_dump(
            mode="json", by_alias=True, exclude_none=True
        ),
        "committedRunIds": plan.committed_run_ids,
        "silverSnapshotIds": plan.silver_snapshot_ids,
        "identitySnapshotIds": plan.identity_snapshot_ids,
        "rightsRegistryDigest": plan.rights_registry_digest,
        "fieldPolicyDigest": plan.field_policy_digest,
        "resolverDigest": plan.resolver_digest,
        "imageDigest": plan.image_digest,
        "configDigest": plan.config_digest,
        "expectedCounts": plan.expected_counts,
        "plannedAt": plan.planned_at,
    }


def build_gold_release_plan(**values: Any) -> GoldReleasePlan:
    provisional = GoldReleasePlan.model_validate(
        {**values, "release_plan_id": _ZERO_DIGEST},
        context={"skip_identity": True},
    )
    normalized = provisional.model_dump(mode="python")
    normalized["release_plan_id"] = deterministic_key(
        "community-gold-release-plan-v2",
        _plan_identity(provisional),
    )
    return GoldReleasePlan.model_validate(normalized)


class GoldEntity(V2ContractModel):
    row_key: str
    release_plan_id: str
    entity_key: str
    entity_level: str
    entity_kind: str
    status: str
    source_node_count: int = Field(ge=0)
    trace_json: str

    @field_validator("row_key", "release_plan_id", "entity_key")
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("entity_level", "entity_kind", "status")
    @classmethod
    def validate_text(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not normalized or len(normalized) > 128:
            raise ValueError("Gold entity fields must be non-empty")
        return normalized

    @model_validator(mode="after")
    def validate_row(self) -> Self:
        _require_canonical_json(self.trace_json, "trace_json")
        expected = deterministic_key(
            "community-gold-entity-v2",
            {
                "releasePlanId": self.release_plan_id,
                "entityKey": self.entity_key,
            },
        )
        if self.row_key != expected:
            raise ValueError("row_key does not match Gold entity identity")
        return self


def build_gold_entity(
    *,
    release_plan_id: str,
    entity_key: str,
    entity_level: str,
    entity_kind: str,
    status: str,
    source_node_count: int,
    trace: dict[str, Any],
) -> GoldEntity:
    return GoldEntity(
        row_key=deterministic_key(
            "community-gold-entity-v2",
            {
                "releasePlanId": release_plan_id,
                "entityKey": entity_key,
            },
        ),
        release_plan_id=release_plan_id,
        entity_key=entity_key,
        entity_level=entity_level,
        entity_kind=entity_kind,
        status=status,
        source_node_count=source_node_count,
        trace_json=canonical_json(trace),
    )


def _require_canonical_json(value: str, label: str) -> Any:
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{label} must be valid JSON") from exc
    if canonical_json(parsed) != value:
        raise ValueError(f"{label} must use canonical JSON")
    return parsed


def trace_with_assertion_lineage(
    trace: dict[str, Any],
    lineage: tuple[GoldAssertionLineage, ...],
) -> dict[str, Any]:
    normalized = tuple(sorted(lineage, key=lambda item: item.assertion_id))
    if len({item.assertion_id for item in normalized}) != len(normalized):
        raise ValueError("Gold trace contains duplicate assertion lineage")
    return {
        **trace,
        "assertions": [
            item.model_dump(mode="json", by_alias=True, exclude_none=True)
            for item in normalized
        ],
    }


def assertion_lineage_from_trace(value: str) -> tuple[GoldAssertionLineage, ...]:
    trace = _require_canonical_json(value, "trace_json")
    assertions = trace.get("assertions", [])
    if not isinstance(assertions, list):
        raise ValueError("Gold trace assertions must be a list")
    normalized = tuple(
        sorted(
            (GoldAssertionLineage.model_validate(item) for item in assertions),
            key=lambda item: item.assertion_id,
        )
    )
    if len({item.assertion_id for item in normalized}) != len(normalized):
        raise ValueError("Gold trace contains duplicate assertion lineage")
    return normalized


class GoldField(V2ContractModel):
    resolution_key: str
    release_plan_id: str
    entity_key: str
    predicate: str
    scope_hash: str
    value_type: str
    value_json: str | None = None
    qualifiers_json: str
    resolution_status: GoldResolutionStatus
    selected_assertion_id: str | None = None
    assertion_ids: tuple[str, ...]
    trace_json: str

    @field_validator(
        "resolution_key",
        "release_plan_id",
        "entity_key",
        "scope_hash",
        "selected_assertion_id",
    )
    @classmethod
    def validate_digest(cls, value: str | None) -> str | None:
        return None if value is None else require_sha256(value)

    @field_validator("predicate")
    @classmethod
    def validate_predicate(cls, value: str) -> str:
        return require_slug(value, label="predicate")

    @field_validator("value_type")
    @classmethod
    def validate_value_type(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not normalized or len(normalized) > 128:
            raise ValueError("Gold value_type must be non-empty")
        return normalized

    @field_validator("assertion_ids")
    @classmethod
    def normalize_assertions(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(
            sorted({require_sha256(item, label="assertion_id") for item in value})
        )
        if not normalized:
            raise ValueError("Gold field requires assertion_ids")
        return normalized

    @model_validator(mode="after")
    def validate_row(self) -> Self:
        qualifiers = _require_canonical_json(self.qualifiers_json, "qualifiers_json")
        if self.value_json is not None:
            _require_canonical_json(self.value_json, "value_json")
        _require_canonical_json(self.trace_json, "trace_json")
        if self.resolution_status == GoldResolutionStatus.SELECTED:
            if self.value_json is None or self.selected_assertion_id is None:
                raise ValueError("SELECTED Gold field requires value and winner")
            if self.selected_assertion_id not in self.assertion_ids:
                raise ValueError("selected assertion must be part of field lineage")
        elif self.resolution_status == GoldResolutionStatus.SET:
            if self.value_json is None:
                raise ValueError("SET Gold field requires a value")
        elif self.selected_assertion_id is not None:
            raise ValueError("non-selected Gold field cannot name a winner")
        expected = deterministic_key(
            "community-gold-field-v2",
            {
                "releasePlanId": self.release_plan_id,
                "entityKey": self.entity_key,
                "predicate": self.predicate,
                "scopeHash": self.scope_hash,
                "value": (
                    None if self.value_json is None else json.loads(self.value_json)
                ),
                "qualifiers": qualifiers,
                "status": self.resolution_status.value,
            },
        )
        if self.resolution_key != expected:
            raise ValueError("resolution_key does not match Gold field identity")
        return self


class GoldIdentifier(V2ContractModel):
    resolution_key: str
    release_plan_id: str
    entity_key: str
    namespace_id: str
    value: str
    issuer: str
    referent_kind: str
    assertion_ids: tuple[str, ...]
    trace_json: str

    @field_validator("resolution_key", "release_plan_id", "entity_key")
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("namespace_id")
    @classmethod
    def validate_namespace(cls, value: str) -> str:
        return require_slug(value, label="namespace_id")

    @field_validator("value", "issuer", "referent_kind")
    @classmethod
    def validate_text(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized or len(normalized) > 2048:
            raise ValueError("Gold identifier fields must be non-empty")
        return normalized

    @field_validator("assertion_ids")
    @classmethod
    def normalize_assertions(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(
            sorted({require_sha256(item, label="assertion_id") for item in value})
        )
        if not normalized:
            raise ValueError("Gold identifier requires assertion_ids")
        return normalized

    @model_validator(mode="after")
    def validate_row(self) -> Self:
        _require_canonical_json(self.trace_json, "trace_json")
        expected = deterministic_key(
            "community-gold-identifier-v2",
            {
                "releasePlanId": self.release_plan_id,
                "entityKey": self.entity_key,
                "namespaceId": self.namespace_id,
                "value": self.value,
                "issuer": self.issuer,
                "referentKind": self.referent_kind,
            },
        )
        if self.resolution_key != expected:
            raise ValueError("resolution_key does not match Gold identifier identity")
        return self


class GoldRelation(V2ContractModel):
    resolution_key: str
    release_plan_id: str
    subject_entity_key: str
    predicate: str
    object_entity_key: str
    qualifiers_json: str
    assertion_ids: tuple[str, ...]
    trace_json: str

    @field_validator(
        "resolution_key",
        "release_plan_id",
        "subject_entity_key",
        "object_entity_key",
    )
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("predicate")
    @classmethod
    def validate_predicate(cls, value: str) -> str:
        return require_slug(value, label="predicate")

    @field_validator("assertion_ids")
    @classmethod
    def normalize_assertions(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(
            sorted({require_sha256(item, label="assertion_id") for item in value})
        )
        if not normalized:
            raise ValueError("Gold relation requires assertion_ids")
        return normalized

    @model_validator(mode="after")
    def validate_row(self) -> Self:
        qualifiers = _require_canonical_json(self.qualifiers_json, "qualifiers_json")
        _require_canonical_json(self.trace_json, "trace_json")
        expected = deterministic_key(
            "community-gold-relation-v2",
            {
                "releasePlanId": self.release_plan_id,
                "subjectEntityKey": self.subject_entity_key,
                "predicate": self.predicate,
                "objectEntityKey": self.object_entity_key,
                "qualifiers": qualifiers,
            },
        )
        if self.resolution_key != expected:
            raise ValueError("resolution_key does not match Gold relation identity")
        return self


class GoldConflict(V2ContractModel):
    conflict_key: str
    release_plan_id: str
    entity_key: str
    predicate: str
    scope_hash: str
    reason: str
    assertion_ids: tuple[str, ...]
    candidate_values_json: str
    trace_json: str

    @field_validator(
        "conflict_key",
        "release_plan_id",
        "entity_key",
        "scope_hash",
    )
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("predicate")
    @classmethod
    def validate_predicate(cls, value: str) -> str:
        return require_slug(value, label="predicate")

    @field_validator("reason")
    @classmethod
    def validate_reason(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not normalized or len(normalized) > 256:
            raise ValueError("Gold conflict reason must be non-empty")
        return normalized

    @field_validator("assertion_ids")
    @classmethod
    def normalize_assertions(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(
            sorted({require_sha256(item, label="assertion_id") for item in value})
        )
        if not normalized:
            raise ValueError("Gold conflict requires assertion_ids")
        return normalized

    @model_validator(mode="after")
    def validate_row(self) -> Self:
        candidates = _require_canonical_json(
            self.candidate_values_json, "candidate_values_json"
        )
        _require_canonical_json(self.trace_json, "trace_json")
        expected = deterministic_key(
            "community-gold-conflict-v2",
            {
                "releasePlanId": self.release_plan_id,
                "entityKey": self.entity_key,
                "predicate": self.predicate,
                "scopeHash": self.scope_hash,
                "reason": self.reason,
                "candidates": candidates,
            },
        )
        if self.conflict_key != expected:
            raise ValueError("conflict_key does not match Gold conflict identity")
        return self


def build_gold_field(
    *,
    release_plan_id: str,
    entity_key: str,
    predicate: str,
    value_type: str,
    value: Any | None,
    qualifiers: dict[str, Any],
    resolution_status: GoldResolutionStatus,
    assertion_ids: tuple[str, ...],
    trace: dict[str, Any],
    selected_assertion_id: str | None = None,
) -> GoldField:
    scope_hash = digest_identity({"predicate": predicate, "qualifiers": qualifiers})
    value_json = None if value is None else canonical_json(value)
    identity = {
        "releasePlanId": release_plan_id,
        "entityKey": entity_key,
        "predicate": predicate,
        "scopeHash": scope_hash,
        "value": value,
        "qualifiers": qualifiers,
        "status": resolution_status.value,
    }
    return GoldField(
        resolution_key=deterministic_key("community-gold-field-v2", identity),
        release_plan_id=release_plan_id,
        entity_key=entity_key,
        predicate=predicate,
        scope_hash=scope_hash,
        value_type=value_type,
        value_json=value_json,
        qualifiers_json=canonical_json(qualifiers),
        resolution_status=resolution_status,
        selected_assertion_id=selected_assertion_id,
        assertion_ids=assertion_ids,
        trace_json=canonical_json(trace),
    )


def build_gold_identifier(
    *,
    release_plan_id: str,
    entity_key: str,
    namespace_id: str,
    value: str,
    issuer: str,
    referent_kind: str,
    assertion_ids: tuple[str, ...],
    trace: dict[str, Any],
) -> GoldIdentifier:
    identity = {
        "releasePlanId": release_plan_id,
        "entityKey": entity_key,
        "namespaceId": namespace_id,
        "value": value,
        "issuer": issuer,
        "referentKind": referent_kind,
    }
    return GoldIdentifier(
        resolution_key=deterministic_key("community-gold-identifier-v2", identity),
        release_plan_id=release_plan_id,
        entity_key=entity_key,
        namespace_id=namespace_id,
        value=value,
        issuer=issuer,
        referent_kind=referent_kind,
        assertion_ids=assertion_ids,
        trace_json=canonical_json(trace),
    )


def build_gold_relation(
    *,
    release_plan_id: str,
    subject_entity_key: str,
    predicate: str,
    object_entity_key: str,
    qualifiers: dict[str, Any],
    assertion_ids: tuple[str, ...],
    trace: dict[str, Any],
) -> GoldRelation:
    identity = {
        "releasePlanId": release_plan_id,
        "subjectEntityKey": subject_entity_key,
        "predicate": predicate,
        "objectEntityKey": object_entity_key,
        "qualifiers": qualifiers,
    }
    return GoldRelation(
        resolution_key=deterministic_key("community-gold-relation-v2", identity),
        release_plan_id=release_plan_id,
        subject_entity_key=subject_entity_key,
        predicate=predicate,
        object_entity_key=object_entity_key,
        qualifiers_json=canonical_json(qualifiers),
        assertion_ids=assertion_ids,
        trace_json=canonical_json(trace),
    )


def build_gold_conflict(
    *,
    release_plan_id: str,
    entity_key: str,
    predicate: str,
    qualifiers: dict[str, Any],
    reason: str,
    assertion_ids: tuple[str, ...],
    candidate_values: list[Any],
    trace: dict[str, Any],
) -> GoldConflict:
    scope_hash = digest_identity({"predicate": predicate, "qualifiers": qualifiers})
    identity = {
        "releasePlanId": release_plan_id,
        "entityKey": entity_key,
        "predicate": predicate,
        "scopeHash": scope_hash,
        "reason": reason,
        "candidates": candidate_values,
    }
    return GoldConflict(
        conflict_key=deterministic_key("community-gold-conflict-v2", identity),
        release_plan_id=release_plan_id,
        entity_key=entity_key,
        predicate=predicate,
        scope_hash=scope_hash,
        reason=reason,
        assertion_ids=assertion_ids,
        candidate_values_json=canonical_json(candidate_values),
        trace_json=canonical_json(trace),
    )
