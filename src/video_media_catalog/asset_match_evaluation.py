"""Offline golden-set evaluation for review-only asset matching."""

from __future__ import annotations

import re
from collections import defaultdict
from typing import Literal, Self

from pydantic import Field, field_validator, model_validator

from video_media_catalog.asset_matching import (
    MATCH_ALGORITHM_ID,
    AssetMatchManifest,
    AssetMatchRequest,
    MatchConfidenceTier,
)
from video_media_catalog.canonical import canonical_json, sha256_digest
from video_media_catalog.v2_contracts import (
    V2ContractModel,
    require_rfc3339,
)

_ENTITY_KEY = re.compile(r"^sha256:[0-9a-f]{64}$")
_COHORT = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")
_DEFAULT_MINIMUM_CASES_BY_TYPE = {
    "MOVIE": 100,
    "TV_SERIES": 40,
    "TV_SEASON": 40,
    "TV_EPISODE": 100,
}


class GoldenMatchCase(V2ContractModel):
    request: AssetMatchRequest
    expected_entity_key: str
    cohorts: tuple[str, ...] = Field(default=(), max_length=16)

    @field_validator("expected_entity_key")
    @classmethod
    def validate_entity_key(cls, value: str) -> str:
        if _ENTITY_KEY.fullmatch(value) is None:
            raise ValueError("expected_entity_key must be a SHA-256 key")
        return value

    @field_validator("cohorts")
    @classmethod
    def validate_cohorts(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(sorted(set(value)))
        if len(normalized) != len(value) or any(
            _COHORT.fullmatch(item) is None for item in normalized
        ):
            raise ValueError("cohorts must be unique lowercase identifiers")
        return normalized


class GoldenMatchSet(V2ContractModel):
    schema_version: Literal["1.0"] = "1.0"
    cases: tuple[GoldenMatchCase, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def require_unique_assets(self) -> Self:
        asset_ids = [case.request.asset_version_id for case in self.cases]
        if len(set(asset_ids)) != len(asset_ids):
            raise ValueError("golden cases must have unique asset_version_id values")
        return self

    @property
    def digest(self) -> str:
        return sha256_digest(
            canonical_json(
                self.model_dump(mode="json", by_alias=True, exclude_none=True)
            )
        )


class MatchEvaluationGates(V2ContractModel):
    minimum_cases: int = Field(default=300, gt=0)
    minimum_cases_by_type: dict[str, int] = Field(
        default_factory=lambda: dict(_DEFAULT_MINIMUM_CASES_BY_TYPE)
    )
    minimum_top1_accuracy: float = Field(default=0.9, ge=0, le=1)
    minimum_recall_at_5: float = Field(default=0.97, ge=0, le=1)
    minimum_exact_id_top1_accuracy: float = Field(
        default=0.995,
        ge=0,
        le=1,
    )
    maximum_false_positive_rate: float = Field(default=0.005, ge=0, le=1)

    @field_validator("minimum_cases_by_type")
    @classmethod
    def validate_type_minimums(cls, value: dict[str, int]) -> dict[str, int]:
        if set(value) != set(_DEFAULT_MINIMUM_CASES_BY_TYPE):
            raise ValueError("minimum_cases_by_type must contain four content types")
        if any(isinstance(count, bool) or count < 0 for count in value.values()):
            raise ValueError("type minimums must be non-negative integers")
        return {key: int(value[key]) for key in sorted(value)}

    @model_validator(mode="after")
    def validate_case_budget(self) -> Self:
        if sum(self.minimum_cases_by_type.values()) > self.minimum_cases:
            raise ValueError("type minimums must not exceed minimum_cases")
        return self

    @property
    def digest(self) -> str:
        return sha256_digest(
            canonical_json(self.model_dump(mode="json", by_alias=True))
        )


class MatchEvaluationMetrics(V2ContractModel):
    case_count: int = Field(ge=0)
    top1_accuracy: float = Field(ge=0, le=1)
    recall_at_5: float = Field(ge=0, le=1)
    no_candidate_rate: float = Field(ge=0, le=1)
    exact_id_case_count: int = Field(ge=0)
    exact_id_top1_accuracy: float = Field(ge=0, le=1)
    proposed_accept_count: int = Field(ge=0)
    false_positive_rate: float = Field(ge=0, le=1)


class MatchEvaluationReport(V2ContractModel):
    schema_version: Literal["1.0"] = "1.0"
    algorithm_id: Literal["catalog-asset-match-v1"] = MATCH_ALGORITHM_ID
    golden_set_digest: str
    gate_digest: str
    release_plan_id: str
    generated_at: str
    overall: MatchEvaluationMetrics
    cohorts: dict[str, MatchEvaluationMetrics]
    violations: tuple[str, ...]
    status: Literal["PASS", "FAILED"]

    @field_validator("golden_set_digest", "gate_digest", "release_plan_id")
    @classmethod
    def validate_digests(cls, value: str) -> str:
        if _ENTITY_KEY.fullmatch(value) is None:
            raise ValueError("evaluation digests must be SHA-256 keys")
        return value

    @field_validator("generated_at")
    @classmethod
    def validate_generated_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @model_validator(mode="after")
    def validate_status(self) -> Self:
        expected = "FAILED" if self.violations else "PASS"
        if self.status != expected:
            raise ValueError("evaluation status does not match violations")
        return self

    def json_bytes(self) -> bytes:
        return (
            canonical_json(
                self.model_dump(mode="json", by_alias=True, exclude_none=True)
            )
            + "\n"
        ).encode()


def _metrics(
    cases: list[GoldenMatchCase],
    manifests: dict[str, AssetMatchManifest],
) -> MatchEvaluationMetrics:
    if not cases:
        return MatchEvaluationMetrics(
            case_count=0,
            top1_accuracy=1,
            recall_at_5=1,
            no_candidate_rate=0,
            exact_id_case_count=0,
            exact_id_top1_accuracy=1,
            proposed_accept_count=0,
            false_positive_rate=0,
        )
    top1 = 0
    recall_at_5 = 0
    no_candidate = 0
    exact_cases = 0
    exact_top1 = 0
    proposed_accepts = 0
    false_positives = 0
    for case in cases:
        manifest = manifests[case.request.asset_version_id]
        keys = [candidate.entity_key for candidate in manifest.candidates]
        top_is_correct = bool(keys) and keys[0] == case.expected_entity_key
        top1 += top_is_correct
        recall_at_5 += case.expected_entity_key in keys[:5]
        no_candidate += not keys
        if case.request.external_identifiers:
            exact_cases += 1
            exact_top1 += top_is_correct
        proposed = bool(manifest.candidates) and manifest.candidates[0].tier in {
            MatchConfidenceTier.EXACT,
            MatchConfidenceTier.HIGH,
        }
        proposed_accepts += proposed
        false_positives += proposed and not top_is_correct
    count = len(cases)
    return MatchEvaluationMetrics(
        case_count=count,
        top1_accuracy=top1 / count,
        recall_at_5=recall_at_5 / count,
        no_candidate_rate=no_candidate / count,
        exact_id_case_count=exact_cases,
        exact_id_top1_accuracy=(exact_top1 / exact_cases if exact_cases else 1),
        proposed_accept_count=proposed_accepts,
        false_positive_rate=(
            false_positives / proposed_accepts if proposed_accepts else 0
        ),
    )


def evaluate_asset_match_manifests(
    *,
    golden_set: GoldenMatchSet,
    manifests: list[AssetMatchManifest],
    release_plan_id: str,
    generated_at: str,
    gates: MatchEvaluationGates | None = None,
) -> MatchEvaluationReport:
    gates = gates or MatchEvaluationGates()
    by_asset = {manifest.asset_version_id: manifest for manifest in manifests}
    if len(by_asset) != len(manifests):
        raise ValueError("evaluation manifests contain duplicate assets")
    expected_assets = {case.request.asset_version_id for case in golden_set.cases}
    if set(by_asset) != expected_assets:
        raise ValueError("evaluation manifests do not cover the golden set exactly")
    for case in golden_set.cases:
        manifest = by_asset[case.request.asset_version_id]
        if manifest.request_digest != case.request.digest:
            raise ValueError("manifest request digest does not match golden case")
        if manifest.release_plan_id != release_plan_id:
            raise ValueError("evaluation manifests cross Gold release plans")

    cases = list(golden_set.cases)
    overall = _metrics(cases, by_asset)
    grouped: dict[str, list[GoldenMatchCase]] = defaultdict(list)
    for case in cases:
        grouped[f"type.{case.request.content_type.value.lower()}"].append(case)
        for cohort in case.cohorts:
            grouped[cohort].append(case)
    cohort_metrics = {
        cohort: _metrics(values, by_asset) for cohort, values in sorted(grouped.items())
    }
    violations = []
    if overall.case_count < gates.minimum_cases:
        violations.append(f"CASE_COUNT:{overall.case_count}<{gates.minimum_cases}")
    for entity_type, minimum in gates.minimum_cases_by_type.items():
        observed = sum(case.request.content_type.value == entity_type for case in cases)
        if observed < minimum:
            violations.append(f"TYPE_CASE_COUNT_{entity_type}:{observed}<{minimum}")
    if overall.top1_accuracy < gates.minimum_top1_accuracy:
        violations.append(
            "TOP1_ACCURACY:"
            f"{overall.top1_accuracy:.12g}<"
            f"{gates.minimum_top1_accuracy:.12g}"
        )
    if overall.recall_at_5 < gates.minimum_recall_at_5:
        violations.append(
            f"RECALL_AT_5:{overall.recall_at_5:.12g}<{gates.minimum_recall_at_5:.12g}"
        )
    if overall.exact_id_top1_accuracy < gates.minimum_exact_id_top1_accuracy:
        violations.append(
            "EXACT_ID_TOP1_ACCURACY:"
            f"{overall.exact_id_top1_accuracy:.12g}<"
            f"{gates.minimum_exact_id_top1_accuracy:.12g}"
        )
    if overall.false_positive_rate > gates.maximum_false_positive_rate:
        violations.append(
            "FALSE_POSITIVE_RATE:"
            f"{overall.false_positive_rate:.12g}>"
            f"{gates.maximum_false_positive_rate:.12g}"
        )
    return MatchEvaluationReport(
        golden_set_digest=golden_set.digest,
        gate_digest=gates.digest,
        release_plan_id=release_plan_id,
        generated_at=generated_at,
        overall=overall,
        cohorts=cohort_metrics,
        violations=tuple(violations),
        status="FAILED" if violations else "PASS",
    )
