"""Immutable quality report for a policy-specific Gold release plan."""

from __future__ import annotations

import math
from enum import StrEnum
from typing import Any, Self

from pydantic import Field, ValidationInfo, field_validator, model_validator

from video_media_catalog.canonical import deterministic_key
from video_media_catalog.gold import GoldReleasePlan, GoldResolutionPolicy
from video_media_catalog.gold_freshness import (
    ReleaseFreshnessMatrix,
    build_release_freshness_matrix,
    disabled_release_freshness_policy,
)
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


class GoldBuildMode(StrEnum):
    RELEASE = "RELEASE"
    CANDIDATE_BACKFILL = "CANDIDATE_BACKFILL"


class GoldQualityReport(V2ContractModel):
    schema_version: str = "2.0"
    report_id: str
    release_plan_id: str
    field_policy_digest: str
    config_digest: str
    build_mode: GoldBuildMode = GoldBuildMode.RELEASE
    release_freshness: ReleaseFreshnessMatrix
    table_counts: dict[str, int]
    conflict_count: int = Field(ge=0)
    conflict_ratio: float = Field(ge=0, le=1)
    withheld_assertion_count: int = Field(ge=0)
    unresolved_identity_count: int = Field(ge=0)
    unresolved_identity_ratio: float = Field(ge=0, le=1)
    orphan_episode_count: int = Field(default=0, ge=0)
    orphan_season_count: int = Field(default=0, ge=0)
    duplicate_external_id_count: int = Field(default=0, ge=0)
    eligible_policy_counts: dict[str, int]
    attribution_counts: dict[str, int]
    violations: tuple[str, ...]
    status: GoldQualityStatus
    created_at: str

    @field_validator(
        "report_id",
        "release_plan_id",
        "field_policy_digest",
        "config_digest",
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

    @field_validator("eligible_policy_counts", "attribution_counts")
    @classmethod
    def validate_policy_counts(cls, value: dict[str, int]) -> dict[str, int]:
        if any(isinstance(count, bool) or count < 0 for count in value.values()):
            raise ValueError("quality policy counts must be non-negative integers")
        return dict(sorted(value.items()))

    @field_validator("violations")
    @classmethod
    def normalize_violations(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(sorted(set(value)))

    @field_validator("created_at")
    @classmethod
    def validate_created_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @model_validator(mode="after")
    def validate_report(self, info: ValidationInfo) -> Self:
        denominator = _identity_lookup_denominator(
            self.table_counts,
            self.unresolved_identity_count,
        )
        expected_unresolved_ratio = (
            self.unresolved_identity_count / denominator if denominator else 0.0
        )
        if not math.isclose(
            self.unresolved_identity_ratio,
            expected_unresolved_ratio,
            rel_tol=0.0,
            abs_tol=1e-12,
        ):
            raise ValueError(
                "unresolved identity ratio does not match Gold identity lookups"
            )
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
        "configDigest": report.config_digest,
        "buildMode": report.build_mode.value,
        "releaseFreshness": report.release_freshness.model_dump(
            mode="json", by_alias=True, exclude_none=True
        ),
        "tableCounts": report.table_counts,
        "conflictCount": report.conflict_count,
        "conflictRatio": report.conflict_ratio,
        "withheldAssertionCount": report.withheld_assertion_count,
        "unresolvedIdentityCount": report.unresolved_identity_count,
        "unresolvedIdentityRatio": report.unresolved_identity_ratio,
        "orphanEpisodeCount": report.orphan_episode_count,
        "orphanSeasonCount": report.orphan_season_count,
        "duplicateExternalIdCount": report.duplicate_external_id_count,
        "eligiblePolicyCounts": report.eligible_policy_counts,
        "attributionCounts": report.attribution_counts,
        "violations": report.violations,
        "status": report.status.value,
        "createdAt": report.created_at,
    }


def _identity_lookup_denominator(
    table_counts: dict[str, int],
    unresolved_identity_count: int,
) -> int:
    """Count resolved and unresolved assertion-to-entity lookup endpoints."""

    resolved = (
        table_counts["community_gold_field"]
        + table_counts["community_gold_identifier"]
        + 2 * table_counts["community_gold_relation"]
    )
    return resolved + unresolved_identity_count


def build_gold_quality_report(
    *,
    plan: GoldReleasePlan,
    draft: GoldResolutionDraft,
    policy: GoldResolutionPolicy,
    created_at: str,
    release_freshness: ReleaseFreshnessMatrix | None = None,
    build_mode: GoldBuildMode = GoldBuildMode.RELEASE,
) -> GoldQualityReport:
    table_counts = draft.expected_counts
    if table_counts != plan.expected_counts:
        raise ValueError("Gold draft counts do not match release plan")
    return build_gold_quality_report_from_metrics(
        plan=plan,
        policy=policy,
        table_counts=table_counts,
        conflict_count=len(draft.conflicts),
        field_count=len(draft.fields),
        resolution_count=(
            len(draft.fields)
            + len(draft.relations)
            + sum(
                conflict.reason == "MULTIPLE_ELIGIBLE_RELATION_TARGETS"
                for conflict in draft.conflicts
            )
        ),
        withheld_assertion_count=draft.withheld_assertion_count,
        unresolved_identity_count=draft.unresolved_identity_count,
        entity_count=len(draft.entity_keys),
        eligible_policy_counts=draft.eligible_policy_counts,
        attribution_counts=(draft.attribution_counts or draft.eligible_policy_counts),
        orphan_episode_count=draft.orphan_episode_count,
        orphan_season_count=draft.orphan_season_count,
        duplicate_external_id_count=draft.duplicate_external_id_count,
        release_freshness=release_freshness,
        build_mode=build_mode,
        created_at=created_at,
    )


def build_gold_quality_report_from_metrics(
    *,
    plan: GoldReleasePlan,
    policy: GoldResolutionPolicy,
    table_counts: dict[str, int],
    conflict_count: int,
    field_count: int,
    withheld_assertion_count: int,
    unresolved_identity_count: int,
    entity_count: int,
    eligible_policy_counts: dict[str, int],
    created_at: str,
    attribution_counts: dict[str, int] | None = None,
    orphan_episode_count: int = 0,
    orphan_season_count: int = 0,
    duplicate_external_id_count: int = 0,
    release_freshness: ReleaseFreshnessMatrix | None = None,
    build_mode: GoldBuildMode = GoldBuildMode.RELEASE,
    resolution_count: int | None = None,
) -> GoldQualityReport:
    if table_counts != plan.expected_counts:
        raise ValueError("Gold metrics counts do not match release plan")
    if any(
        isinstance(value, bool) or value < 0
        for value in (
            conflict_count,
            field_count,
            withheld_assertion_count,
            unresolved_identity_count,
            entity_count,
            orphan_episode_count,
            orphan_season_count,
            duplicate_external_id_count,
            resolution_count if resolution_count is not None else 0,
        )
    ):
        raise ValueError("Gold quality metrics must be non-negative")
    conflict_denominator = field_count if resolution_count is None else resolution_count
    if conflict_count > conflict_denominator:
        raise ValueError("Gold conflict count exceeds resolution count")
    conflict_ratio = (
        conflict_count / conflict_denominator if conflict_denominator else 0.0
    )
    identity_denominator = _identity_lookup_denominator(
        table_counts,
        unresolved_identity_count,
    )
    unresolved_ratio = (
        unresolved_identity_count / identity_denominator
        if identity_denominator
        else 0.0
    )
    violations = []
    if policy.digest != plan.field_policy_digest:
        raise ValueError("Gold quality policy does not bind the release plan")
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
    if orphan_episode_count:
        violations.append(f"ORPHAN_EPISODE_COUNT:{orphan_episode_count}")
    if orphan_season_count:
        violations.append(f"ORPHAN_SEASON_COUNT:{orphan_season_count}")
    if duplicate_external_id_count:
        violations.append(f"DUPLICATE_EXTERNAL_ID_COUNT:{duplicate_external_id_count}")
    normalized_attribution_counts = dict(
        sorted(
            (
                eligible_policy_counts
                if attribution_counts is None
                else attribution_counts
            ).items()
        )
    )
    if normalized_attribution_counts != dict(sorted(eligible_policy_counts.items())):
        violations.append("ATTRIBUTION_COUNTS_MISMATCH")
    freshness = release_freshness or build_release_freshness_matrix(
        ingest_runs=(),
        policy=disabled_release_freshness_policy(),
        as_of=plan.policy_context.as_of,
    )
    violations.extend(
        f"SOURCE_FRESHNESS:{item}" for item in freshness.blocking_violations
    )
    values = {
        "report_id": _ZERO_DIGEST,
        "release_plan_id": plan.release_plan_id,
        "field_policy_digest": policy.digest,
        "config_digest": plan.config_digest,
        "build_mode": build_mode,
        "release_freshness": freshness,
        "table_counts": table_counts,
        "conflict_count": conflict_count,
        "conflict_ratio": conflict_ratio,
        "withheld_assertion_count": withheld_assertion_count,
        "unresolved_identity_count": unresolved_identity_count,
        "unresolved_identity_ratio": unresolved_ratio,
        "orphan_episode_count": orphan_episode_count,
        "orphan_season_count": orphan_season_count,
        "duplicate_external_id_count": duplicate_external_id_count,
        "eligible_policy_counts": dict(sorted(eligible_policy_counts.items())),
        "attribution_counts": normalized_attribution_counts,
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
