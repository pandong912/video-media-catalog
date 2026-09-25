"""Release-time source freshness and coverage policy for Gold builds."""

from __future__ import annotations

from collections.abc import Iterable
from enum import StrEnum
from typing import Any, Self

from pydantic import Field, field_validator, model_validator

from video_media_catalog.community_ingest import CommunityIngestRun, IngestRunKind
from video_media_catalog.connector import ChangeSemantics, Completeness
from video_media_catalog.v2_contracts import (
    V2ContractModel,
    digest_identity,
    parse_rfc3339,
    require_rfc3339,
    require_sha256,
    require_slug,
)


class FreshnessStatus(StrEnum):
    PASS = "PASS"
    FAILED = "FAILED"
    OPTIONAL = "OPTIONAL"
    NOT_REQUIRED = "NOT_REQUIRED"


class SourceFreshnessRequirement(V2ContractModel):
    source_product_id: str
    required: bool
    feed_freshness_required: bool = True
    slo_hours: int | None = Field(default=None, gt=0)
    require_complete_baseline: bool = True

    @field_validator("source_product_id")
    @classmethod
    def validate_source_product_id(cls, value: str) -> str:
        return require_slug(value, label="freshness source_product_id")

    @model_validator(mode="after")
    def validate_requirement(self) -> Self:
        if self.feed_freshness_required != (self.slo_hours is not None):
            raise ValueError(
                "feed freshness and an SLO must either both be configured or omitted"
            )
        return self


class ReleaseFreshnessPolicy(V2ContractModel):
    policy_id: str = "research-gold-source-freshness-v1"
    policy_version: str = "1.0.0"
    requirements: tuple[SourceFreshnessRequirement, ...]

    @field_validator("policy_id")
    @classmethod
    def validate_policy_id(cls, value: str) -> str:
        return require_slug(value, label="freshness policy_id")

    @field_validator("policy_version")
    @classmethod
    def validate_policy_version(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized or len(normalized) > 64:
            raise ValueError("freshness policy_version must be non-empty")
        return normalized

    @field_validator("requirements")
    @classmethod
    def normalize_requirements(
        cls,
        value: tuple[SourceFreshnessRequirement, ...],
    ) -> tuple[SourceFreshnessRequirement, ...]:
        normalized = tuple(sorted(value, key=lambda item: item.source_product_id))
        source_ids = [item.source_product_id for item in normalized]
        if len(source_ids) != len(set(source_ids)):
            raise ValueError("freshness policy contains duplicate sources")
        return normalized

    @property
    def digest(self) -> str:
        return digest_identity(
            self.model_dump(mode="json", by_alias=True, exclude_none=True)
        )


def research_release_freshness_policy(
    *,
    tmdb_slo_hours: int = 36,
    tvmaze_slo_hours: int = 36,
    imdb_slo_hours: int = 10 * 24,
    wikidata_slo_hours: int = 45 * 24,
) -> ReleaseFreshnessPolicy:
    return ReleaseFreshnessPolicy(
        requirements=(
            SourceFreshnessRequirement(
                source_product_id="tmdb-research",
                required=True,
                slo_hours=tmdb_slo_hours,
            ),
            SourceFreshnessRequirement(
                source_product_id="tvmaze-public-api",
                required=True,
                slo_hours=tvmaze_slo_hours,
            ),
            SourceFreshnessRequirement(
                source_product_id="imdb-non-commercial-datasets",
                required=True,
                slo_hours=imdb_slo_hours,
            ),
            SourceFreshnessRequirement(
                source_product_id="wikidata-json-dump",
                required=True,
                slo_hours=wikidata_slo_hours,
            ),
            SourceFreshnessRequirement(
                source_product_id="eidr-public-registry",
                required=False,
                feed_freshness_required=False,
                slo_hours=None,
                require_complete_baseline=True,
            ),
            SourceFreshnessRequirement(
                source_product_id="douban-id-only",
                required=False,
                feed_freshness_required=False,
                slo_hours=None,
                require_complete_baseline=False,
            ),
        )
    )


def disabled_release_freshness_policy() -> ReleaseFreshnessPolicy:
    return ReleaseFreshnessPolicy(
        policy_id="research-gold-source-freshness-disabled",
        requirements=(),
    )


def scope_release_freshness_policy(
    policy: ReleaseFreshnessPolicy,
    *,
    selected_source_product_ids: Iterable[str],
) -> ReleaseFreshnessPolicy:
    """Make absent sources non-blocking for an immutable bounded run set."""

    selected = {
        require_slug(value, label="selected freshness source_product_id")
        for value in selected_source_product_ids
    }
    return policy.model_copy(
        update={
            "requirements": tuple(
                requirement
                if not requirement.required or requirement.source_product_id in selected
                else requirement.model_copy(update={"required": False})
                for requirement in policy.requirements
            )
        }
    )


class SourceCoverageWatermark(V2ContractModel):
    run_id: str
    watermark: str
    acquired_at: str
    age_hours: float = Field(ge=0)
    coverage_digest: str
    completeness: Completeness
    change_semantics: ChangeSemantics

    @field_validator("run_id", "coverage_digest")
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("watermark")
    @classmethod
    def validate_watermark(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized or len(normalized) > 1024:
            raise ValueError("source watermark must be non-empty and bounded")
        return normalized

    @field_validator("acquired_at")
    @classmethod
    def validate_acquired_at(cls, value: str) -> str:
        return require_rfc3339(value)


class SourceFreshnessResult(V2ContractModel):
    source_product_id: str
    required: bool
    feed_freshness_required: bool
    slo_hours: int | None = Field(default=None, gt=0)
    latest_complete: SourceCoverageWatermark | None = None
    latest_delta: SourceCoverageWatermark | None = None
    latest_partial: SourceCoverageWatermark | None = None
    effective_age_hours: float | None = Field(default=None, ge=0)
    violations: tuple[str, ...] = ()
    status: FreshnessStatus

    @field_validator("source_product_id")
    @classmethod
    def validate_source_product_id(cls, value: str) -> str:
        return require_slug(value, label="freshness source_product_id")

    @field_validator("violations")
    @classmethod
    def normalize_violations(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(sorted(set(value)))


class ReleaseFreshnessMatrix(V2ContractModel):
    policy_digest: str
    as_of: str
    sources: tuple[SourceFreshnessResult, ...]
    blocking_violations: tuple[str, ...] = ()

    @field_validator("policy_digest")
    @classmethod
    def validate_policy_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("as_of")
    @classmethod
    def validate_as_of(cls, value: str) -> str:
        return require_rfc3339(value)

    @field_validator("sources")
    @classmethod
    def normalize_sources(
        cls,
        value: tuple[SourceFreshnessResult, ...],
    ) -> tuple[SourceFreshnessResult, ...]:
        normalized = tuple(sorted(value, key=lambda item: item.source_product_id))
        source_ids = [item.source_product_id for item in normalized]
        if len(source_ids) != len(set(source_ids)):
            raise ValueError("freshness matrix contains duplicate sources")
        return normalized

    @field_validator("blocking_violations")
    @classmethod
    def normalize_blocking_violations(
        cls,
        value: tuple[str, ...],
    ) -> tuple[str, ...]:
        return tuple(sorted(set(value)))

    @model_validator(mode="after")
    def validate_blocking_violations(self) -> Self:
        expected = tuple(
            sorted(
                violation
                for source in self.sources
                if source.required
                for violation in source.violations
            )
        )
        if self.blocking_violations != expected:
            raise ValueError(
                "freshness blocking violations do not match source results"
            )
        return self

    @property
    def digest(self) -> str:
        return digest_identity(
            self.model_dump(mode="json", by_alias=True, exclude_none=True)
        )


def _batch_manifest(run: CommunityIngestRun) -> dict[str, Any] | None:
    if run.run_kind != IngestRunKind.SOURCE_ASSERTIONS:
        return None
    value = run.input_manifest.get("batchManifest")
    if not isinstance(value, dict):
        return None
    if value.get("sourceProductId") != run.source_product_id:
        raise ValueError("freshness batch source does not match its ingest run")
    return value


def _watermark(
    *,
    run: CommunityIngestRun,
    batch: dict[str, Any],
    as_of: str,
) -> SourceCoverageWatermark | None:
    acquired_at = batch.get("acquiredAt")
    coverage_digest = batch.get("coverageScopeDigest")
    semantics = batch.get("changeSemantics")
    completeness = batch.get("completeness")
    if not all(
        isinstance(value, str)
        for value in (
            acquired_at,
            coverage_digest,
            semantics,
            completeness,
        )
    ):
        return None
    source_window = batch.get("sourceWindow")
    source_window_end = (
        source_window.get("end") if isinstance(source_window, dict) else None
    )
    watermark = batch.get("watermarkAfter") or source_window_end or acquired_at
    acquired = parse_rfc3339(acquired_at)
    current = parse_rfc3339(as_of)
    try:
        age_basis = parse_rfc3339(str(watermark))
    except ValueError:
        age_basis = acquired
    age_hours = max(0.0, (current - age_basis).total_seconds() / 3600)
    return SourceCoverageWatermark(
        run_id=run.run_id,
        watermark=str(watermark),
        acquired_at=acquired_at,
        age_hours=round(age_hours, 6),
        coverage_digest=coverage_digest,
        completeness=Completeness(completeness),
        change_semantics=ChangeSemantics(semantics),
    )


def _latest(
    values: list[SourceCoverageWatermark],
) -> SourceCoverageWatermark | None:
    if not values:
        return None
    return max(
        values,
        key=lambda item: (parse_rfc3339(item.acquired_at), item.run_id),
    )


def build_release_freshness_matrix(
    *,
    ingest_runs: Iterable[CommunityIngestRun],
    policy: ReleaseFreshnessPolicy,
    as_of: str,
) -> ReleaseFreshnessMatrix:
    current = require_rfc3339(as_of)
    observations: dict[str, dict[str, list[SourceCoverageWatermark]]] = {}
    for raw_run in ingest_runs:
        run = (
            raw_run
            if isinstance(raw_run, CommunityIngestRun)
            else CommunityIngestRun.model_validate(raw_run)
        )
        batch = _batch_manifest(run)
        if batch is None:
            continue
        source_product_id = run.source_product_id
        if source_product_id not in {
            item.source_product_id for item in policy.requirements
        }:
            continue
        watermark = _watermark(run=run, batch=batch, as_of=current)
        if watermark is None:
            continue
        kinds = observations.setdefault(
            source_product_id,
            {"complete": [], "delta": [], "partial": []},
        )
        if watermark.completeness == Completeness.PARTIAL:
            kinds["partial"].append(watermark)
        elif watermark.change_semantics in {
            ChangeSemantics.DELTA,
            ChangeSemantics.LEASED_DELTA,
        }:
            kinds["delta"].append(watermark)
        else:
            kinds["complete"].append(watermark)

    results = []
    for requirement in policy.requirements:
        source_observations = observations.get(
            requirement.source_product_id,
            {"complete": [], "delta": [], "partial": []},
        )
        complete = _latest(source_observations["complete"])
        delta = _latest(source_observations["delta"])
        partial = _latest(source_observations["partial"])
        violations = []
        effective = _latest([item for item in (complete, delta) if item is not None])
        if requirement.feed_freshness_required:
            if requirement.require_complete_baseline and complete is None:
                violations.append(
                    f"{requirement.source_product_id}:COMPLETE_BASELINE_MISSING"
                )
            if effective is None:
                violations.append(
                    f"{requirement.source_product_id}:FRESHNESS_WATERMARK_MISSING"
                )
            elif (
                requirement.slo_hours is not None
                and effective.age_hours >= requirement.slo_hours
            ):
                violations.append(
                    f"{requirement.source_product_id}:FRESHNESS_SLO:"
                    f"{effective.age_hours:.6g}>={requirement.slo_hours}"
                )
        if not requirement.feed_freshness_required:
            status = FreshnessStatus.NOT_REQUIRED
        elif violations and requirement.required:
            status = FreshnessStatus.FAILED
        elif violations:
            status = FreshnessStatus.OPTIONAL
        else:
            status = FreshnessStatus.PASS
        results.append(
            SourceFreshnessResult(
                source_product_id=requirement.source_product_id,
                required=requirement.required,
                feed_freshness_required=requirement.feed_freshness_required,
                slo_hours=requirement.slo_hours,
                latest_complete=complete,
                latest_delta=delta,
                latest_partial=partial,
                effective_age_hours=(
                    None if effective is None else effective.age_hours
                ),
                violations=tuple(violations),
                status=status,
            )
        )
    blockers = tuple(
        sorted(
            violation
            for result in results
            if result.required
            for violation in result.violations
        )
    )
    return ReleaseFreshnessMatrix(
        policy_digest=policy.digest,
        as_of=current,
        sources=tuple(results),
        blocking_violations=blockers,
    )
