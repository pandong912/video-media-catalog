"""Immutable quality report for a policy-specific Gold release plan."""

from __future__ import annotations

from enum import StrEnum
from typing import Any, Self

from pydantic import Field, ValidationInfo, field_validator, model_validator

from video_media_catalog.canonical import deterministic_key
from video_media_catalog.gold import GoldReleasePlan, GoldResolutionPolicy
from video_media_catalog.gold_resolution import GoldResolutionDraft
from video_media_catalog.gold_tables import GOLD_DATA_COLUMNS
from video_media_catalog.v2_contracts import (
    V2ContractModel,
    require_rfc3339,
    require_sha256,
)

_ZERO_DIGEST = "sha256:" + ("0" * 64)


class GoldQualityStatus(StrEnum):
    PASS = "PASS"
    FAILED = "FAILED"


class GoldQualityReport(V2ContractModel):
    schema_version: str = "2.0"
    report_id: str
    release_plan_id: str
    field_policy_digest: str
    table_counts: dict[str, int]
    conflict_count: int = Field(ge=0)
    conflict_ratio: float = Field(ge=0, le=1)
    withheld_assertion_count: int = Field(ge=0)
    unresolved_identity_count: int = Field(ge=0)
    unresolved_identity_ratio: float = Field(ge=0, le=1)
    eligible_policy_counts: dict[str, int]
    violations: tuple[str, ...]
    status: GoldQualityStatus
    created_at: str

    @field_validator(
        "report_id",
        "release_plan_id",
        "field_policy_digest",
    )
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("table_counts")
    @classmethod
    def validate_counts(cls, value: dict[str, int]) -> dict[str, int]:
        if set(value) != set(GOLD_DATA_COLUMNS):
            raise ValueError("quality counts must contain every Gold data table")
        if any(isinstance(count, bool) or count < 0 for count in value.values()):
            raise ValueError("quality counts must be non-negative integers")
        return dict(sorted(value.items()))

    @field_validator("created_at")
    @classmethod
    def validate_created_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @model_validator(mode="after")
    def validate_report(self, info: ValidationInfo) -> Self:
        expected_status = (
            GoldQualityStatus.FAILED if self.violations else GoldQualityStatus.PASS
        )
        if self.status != expected_status:
            raise ValueError("quality status does not match violations")
        if not (info.context or {}).get("skip_identity"):
            expected = deterministic_key(
                "community-gold-quality-report-v2",
                _report_identity(self),
            )
            if self.report_id != expected:
                raise ValueError("report_id does not match quality identity")
        return self


def _report_identity(report: GoldQualityReport) -> dict[str, Any]:
    return {
        "schemaVersion": report.schema_version,
        "releasePlanId": report.release_plan_id,
        "fieldPolicyDigest": report.field_policy_digest,
        "tableCounts": report.table_counts,
        "conflictCount": report.conflict_count,
        "conflictRatio": report.conflict_ratio,
        "withheldAssertionCount": report.withheld_assertion_count,
        "unresolvedIdentityCount": report.unresolved_identity_count,
        "unresolvedIdentityRatio": report.unresolved_identity_ratio,
        "eligiblePolicyCounts": report.eligible_policy_counts,
        "violations": report.violations,
        "status": report.status.value,
        "createdAt": report.created_at,
    }


def build_gold_quality_report(
    *,
    plan: GoldReleasePlan,
    draft: GoldResolutionDraft,
    policy: GoldResolutionPolicy,
    created_at: str,
) -> GoldQualityReport:
    table_counts = draft.expected_counts
    if table_counts != plan.expected_counts:
        raise ValueError("Gold draft counts do not match release plan")
    field_total = len(draft.fields)
    conflict_ratio = len(draft.conflicts) / field_total if field_total else 0.0
    identity_denominator = len(draft.entity_keys) + draft.unresolved_identity_count
    unresolved_ratio = (
        draft.unresolved_identity_count / identity_denominator
        if identity_denominator
        else 0.0
    )
    violations = []
    if conflict_ratio > policy.max_conflict_ratio:
        violations.append(
            "FIELD_CONFLICT_RATIO:"
            f"{conflict_ratio:.12g}>{policy.max_conflict_ratio:.12g}"
        )
    if unresolved_ratio > policy.max_unresolved_identity_ratio:
        violations.append(
            "UNRESOLVED_IDENTITY_RATIO:"
            f"{unresolved_ratio:.12g}>"
            f"{policy.max_unresolved_identity_ratio:.12g}"
        )
    values = {
        "report_id": _ZERO_DIGEST,
        "release_plan_id": plan.release_plan_id,
        "field_policy_digest": policy.digest,
        "table_counts": table_counts,
        "conflict_count": len(draft.conflicts),
        "conflict_ratio": conflict_ratio,
        "withheld_assertion_count": draft.withheld_assertion_count,
        "unresolved_identity_count": draft.unresolved_identity_count,
        "unresolved_identity_ratio": unresolved_ratio,
        "eligible_policy_counts": dict(sorted(draft.eligible_policy_counts.items())),
        "violations": tuple(violations),
        "status": (GoldQualityStatus.FAILED if violations else GoldQualityStatus.PASS),
        "created_at": created_at,
    }
    provisional = GoldQualityReport.model_validate(
        values,
        context={"skip_identity": True},
    )
    values["report_id"] = deterministic_key(
        "community-gold-quality-report-v2",
        _report_identity(provisional),
    )
    return GoldQualityReport.model_validate(values)
