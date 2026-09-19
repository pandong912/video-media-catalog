"""Deterministic content-first reference catalog selection."""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Literal, Self
from urllib.parse import urlsplit

from pydantic import Field, field_validator, model_validator

from video_media_catalog.canonical import canonical_json, sha256_digest
from video_media_catalog.constants import (
    CREDIT_ORGANIZATION_PROPERTIES,
    CREDIT_PERSON_PROPERTIES,
)
from video_media_catalog.models import ObjectRef
from video_media_catalog.transform import (
    _qid_values,
    _statement_value,
    _statements,
)
from video_media_catalog.v2_contracts import V2ContractModel, require_rfc3339
from video_media_catalog.wikidata_subset import (
    RelationReference,
    qid_number,
    relation_references,
    wikipedia_sitelink_count,
)

REFERENCE_SELECTION_ALGORITHM_ID = "reference-catalog-selection-v1"
DEFAULT_CONTENT_QUOTAS = {
    "MOVIE": 30_000,
    "TV_SERIES": 2_000,
    "TV_SEASON": 10_000,
    "TV_EPISODE": 58_000,
}
DEFAULT_AGENT_LIMITS = {
    "PERSON": 30_000,
    "ORGANIZATION": 5_000,
}
CONTENT_TYPES = frozenset(DEFAULT_CONTENT_QUOTAS)
AGENT_TYPES = frozenset(DEFAULT_AGENT_LIMITS)
REFERENCE_PARENT_PROPERTIES = frozenset({"P179", "P361", "P4908"})


class AssetDemandProfile(V2ContractModel):
    schema_version: str = "1.0"
    sample_count: int = Field(gt=0)
    source_manifest_digest: str
    content_type_weights: dict[str, float] = Field(default_factory=dict)
    language_weights: dict[str, float] = Field(default_factory=dict)
    country_weights: dict[str, float] = Field(default_factory=dict)
    decade_weights: dict[str, float] = Field(default_factory=dict)
    created_at: str

    @field_validator("source_manifest_digest")
    @classmethod
    def validate_digest(cls, value: str) -> str:
        if not value.startswith("sha256:") or len(value) != len("sha256:") + 64:
            raise ValueError("source_manifest_digest must be SHA-256")
        int(value.removeprefix("sha256:"), 16)
        return value

    @field_validator(
        "content_type_weights",
        "language_weights",
        "country_weights",
        "decade_weights",
    )
    @classmethod
    def normalize_weights(cls, value: dict[str, float]) -> dict[str, float]:
        if any(
            isinstance(weight, bool) or not 0 <= weight <= 1
            for weight in value.values()
        ):
            raise ValueError("demand weights must be between 0 and 1")
        return dict(sorted(value.items()))

    @field_validator("created_at")
    @classmethod
    def validate_created_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @property
    def digest(self) -> str:
        return sha256_digest(
            canonical_json(
                self.model_dump(
                    mode="json",
                    by_alias=True,
                    exclude_none=True,
                )
            )
        )


class ReferenceSelectionConfig(V2ContractModel):
    content_quotas: dict[str, int] = Field(
        default_factory=lambda: dict(DEFAULT_CONTENT_QUOTAS)
    )
    agent_limits: dict[str, int] = Field(
        default_factory=lambda: dict(DEFAULT_AGENT_LIMITS)
    )
    demand_profile_digest: str | None = None

    @field_validator("content_quotas")
    @classmethod
    def validate_content_quotas(cls, value: dict[str, int]) -> dict[str, int]:
        if set(value) != CONTENT_TYPES:
            raise ValueError("content_quotas must contain four content types")
        if any(isinstance(count, bool) or count < 0 for count in value.values()):
            raise ValueError("content quotas must be non-negative integers")
        return {key: int(value[key]) for key in sorted(value)}

    @field_validator("agent_limits")
    @classmethod
    def validate_agent_limits(cls, value: dict[str, int]) -> dict[str, int]:
        if set(value) != AGENT_TYPES:
            raise ValueError("agent_limits must contain PERSON and ORGANIZATION")
        if any(isinstance(count, bool) or count < 0 for count in value.values()):
            raise ValueError("agent limits must be non-negative integers")
        return {key: int(value[key]) for key in sorted(value)}

    @field_validator("demand_profile_digest")
    @classmethod
    def validate_profile_digest(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if not value.startswith("sha256:") or len(value) != 71:
            raise ValueError("demand_profile_digest must be SHA-256")
        int(value.removeprefix("sha256:"), 16)
        return value

    @property
    def content_target_count(self) -> int:
        return sum(self.content_quotas.values())

    @property
    def digest(self) -> str:
        return sha256_digest(
            canonical_json(
                {
                    "algorithmId": REFERENCE_SELECTION_ALGORITHM_ID,
                    "contentQuotas": self.content_quotas,
                    "agentLimits": self.agent_limits,
                    "demandProfileDigest": self.demand_profile_digest,
                }
            )
        )


@dataclass(frozen=True)
class ReferenceCandidate:
    qid: str
    entity_type: str
    sitelink_count: int = 0
    completeness_score: int = 0
    exact_identifier_count: int = 0
    demand_score: int = 0
    has_title: bool = False
    has_release: bool = False
    has_runtime: bool = False
    has_language: bool = False
    has_country: bool = False
    has_external_id: bool = False
    parent_qids: tuple[str, ...] = ()
    relations: tuple[RelationReference, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        qid_number(self.qid)
        if any(
            value < 0
            for value in (
                self.sitelink_count,
                self.completeness_score,
                self.exact_identifier_count,
                self.demand_score,
            )
        ):
            raise ValueError("candidate scores must be non-negative")


@dataclass(frozen=True)
class ReferenceSelectionResult:
    content_qids: tuple[str, ...]
    agent_qids: tuple[str, ...]
    entity_types: dict[str, str]
    hierarchy_coverage: dict[str, str]
    fallback_counts: dict[str, int]

    @property
    def selected_qids(self) -> tuple[str, ...]:
        return tuple(
            sorted(
                (*self.content_qids, *self.agent_qids),
                key=qid_number,
            )
        )

    @property
    def content_count(self) -> int:
        return len(self.content_qids)

    @property
    def agent_count(self) -> int:
        return len(self.agent_qids)

    @property
    def counts_by_type(self) -> dict[str, int]:
        counts = Counter(self.entity_types.values())
        return {
            entity_type: counts.get(entity_type, 0)
            for entity_type in sorted(CONTENT_TYPES | AGENT_TYPES)
        }


class ReferenceQualityThresholds(V2ContractModel):
    minimum_title_coverage: float = Field(default=0.995, ge=0, le=1)
    minimum_movie_release_coverage: float = Field(default=0.98, ge=0, le=1)
    minimum_episode_parent_coverage: float = Field(default=0.99, ge=0, le=1)

    @property
    def digest(self) -> str:
        return sha256_digest(
            canonical_json(
                self.model_dump(
                    mode="json",
                    by_alias=True,
                    exclude_none=True,
                )
            )
        )


def reference_subset_build_digest(
    config: ReferenceSelectionConfig,
    thresholds: ReferenceQualityThresholds,
) -> str:
    return sha256_digest(
        canonical_json(
            {
                "algorithmId": REFERENCE_SELECTION_ALGORITHM_ID,
                "selectionConfigDigest": config.digest,
                "qualityThresholdsDigest": thresholds.digest,
            }
        )
    )


class ReferenceSelectionAudit(V2ContractModel):
    schema_version: Literal["1.0"] = "1.0"
    algorithm_id: Literal["reference-catalog-selection-v1"] = (
        REFERENCE_SELECTION_ALGORITHM_ID
    )
    config_digest: str
    demand_profile_digest: str | None = None
    content_count: int = Field(ge=0)
    agent_count: int = Field(ge=0)
    counts_by_type: dict[str, int]
    fallback_counts: dict[str, int]
    hierarchy_counts: dict[str, int]
    field_coverage: dict[str, dict[str, float]]
    violations: tuple[str, ...]
    status: Literal["PASS", "FAILED"]

    @model_validator(mode="after")
    def validate_status(self) -> Self:
        expected = "FAILED" if self.violations else "PASS"
        if self.status != expected:
            raise ValueError("audit status does not match violations")
        return self

    def json_bytes(self) -> bytes:
        return (
            canonical_json(
                self.model_dump(mode="json", by_alias=True, exclude_none=True)
            )
            + "\n"
        ).encode()


class ReferenceSubsetAuditManifest(V2ContractModel):
    schema_version: Literal["1.0"] = "1.0"
    status: Literal["COMPLETE"] = "COMPLETE"
    algorithm_id: Literal["reference-catalog-selection-v1"] = (
        REFERENCE_SELECTION_ALGORITHM_ID
    )
    config_digest: str
    quality_thresholds_digest: str
    build_digest: str
    demand_profile_digest: str | None = None
    demand_profile: ObjectRef | None = None
    dump: ObjectRef
    subset: ObjectRef
    source_manifest: ObjectRef
    content_quotas: dict[str, int]
    agent_limits: dict[str, int]
    quality_thresholds: ReferenceQualityThresholds
    selected_count: int = Field(ge=0)
    dependency_rows: int = Field(ge=0)
    output_rows: int = Field(gt=0)
    pruned_relation_statements: int = Field(ge=0)
    normalization_staging_uri: str
    quality: ReferenceSelectionAudit

    @field_validator(
        "config_digest",
        "quality_thresholds_digest",
        "build_digest",
        "demand_profile_digest",
    )
    @classmethod
    def validate_digests(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if not value.startswith("sha256:") or len(value) != 71:
            raise ValueError("digest must be sha256:<64 lowercase hex>")
        int(value.removeprefix("sha256:"), 16)
        return value

    @field_validator(
        "demand_profile",
        "dump",
        "subset",
        "source_manifest",
        mode="before",
    )
    @classmethod
    def reject_object_ref_extras(cls, value: Any) -> Any:
        if value is None:
            return value
        aliases = {
            field.alias or name for name, field in ObjectRef.model_fields.items()
        }
        allowed = set(ObjectRef.model_fields) | aliases
        extras = (
            set(value) - allowed
            if isinstance(value, Mapping)
            else set(getattr(value, "__pydantic_extra__", {}) or {})
        )
        if extras:
            raise ValueError(f"ObjectRef contains unexpected fields: {sorted(extras)}")
        return value

    @field_validator("normalization_staging_uri")
    @classmethod
    def validate_staging_uri(cls, value: str) -> str:
        parsed = urlsplit(value)
        if (
            parsed.scheme != "s3"
            or not parsed.netloc
            or not parsed.path.strip("/")
            or parsed.query
            or parsed.fragment
            or parsed.username is not None
            or parsed.password is not None
        ):
            raise ValueError("normalization_staging_uri must be a safe S3 URI")
        return value

    @model_validator(mode="after")
    def validate_complete_manifest(self) -> Self:
        config = ReferenceSelectionConfig(
            content_quotas=self.content_quotas,
            agent_limits=self.agent_limits,
            demand_profile_digest=self.demand_profile_digest,
        )
        if config.digest != self.config_digest:
            raise ValueError("config_digest does not bind selection config")
        if self.quality_thresholds.digest != self.quality_thresholds_digest:
            raise ValueError("quality_thresholds_digest does not bind thresholds")
        if (
            reference_subset_build_digest(config, self.quality_thresholds)
            != self.build_digest
        ):
            raise ValueError("build_digest does not bind selection and quality config")
        if (
            self.quality.config_digest != self.config_digest
            or self.quality.demand_profile_digest != self.demand_profile_digest
            or self.quality.status != "PASS"
        ):
            raise ValueError("quality report does not approve this selection")
        if self.selected_count != (
            self.quality.content_count + self.quality.agent_count
        ):
            raise ValueError("selected_count does not match quality counts")
        if self.output_rows != self.selected_count + self.dependency_rows:
            raise ValueError("output_rows must equal selected and dependency rows")
        if (self.demand_profile is None) != (self.demand_profile_digest is None):
            raise ValueError("demand profile reference and digest must be paired")
        references = {
            "dump": self.dump,
            "subset": self.subset,
            "source_manifest": self.source_manifest,
        }
        if self.demand_profile is not None:
            references["demand_profile"] = self.demand_profile
        for name, reference in references.items():
            if (
                reference.etag is None
                or reference.object_version is None
                or reference.object_version == "null"
                or reference.size_bytes <= 0
            ):
                raise ValueError(f"{name} must be a non-empty immutable ObjectRef")
        if (
            self.source_manifest.format != "OBJECT_FORMAT_PARQUET"
            or self.source_manifest.media_type != "application/vnd.apache.parquet"
        ):
            raise ValueError("source_manifest must reference Parquet")
        for name, reference in {"dump": self.dump, "subset": self.subset}.items():
            if (
                reference.format != "OBJECT_FORMAT_JSON"
                or reference.media_type != "application/x-bzip2"
            ):
                raise ValueError(f"{name} must reference Wikidata JSON bzip2")
        if self.subset.size_bytes > 4 * 1024**3:
            raise ValueError("subset exceeds the 4 GiB compressed object limit")
        if self.demand_profile is not None and (
            self.demand_profile.format != "OBJECT_FORMAT_JSON"
            or self.demand_profile.media_type != "application/json"
        ):
            raise ValueError("demand_profile must reference JSON")
        return self

    def json_bytes(self) -> bytes:
        return (
            canonical_json(
                self.model_dump(mode="json", by_alias=True, exclude_none=True)
            )
            + "\n"
        ).encode()


def build_reference_selection_audit(
    *,
    result: ReferenceSelectionResult,
    candidates: list[ReferenceCandidate],
    config: ReferenceSelectionConfig,
    thresholds: ReferenceQualityThresholds | None = None,
) -> ReferenceSelectionAudit:
    thresholds = thresholds or ReferenceQualityThresholds()
    by_qid = {candidate.qid: candidate for candidate in candidates}
    selected = [by_qid[qid] for qid in result.content_qids]

    def coverage(entity_type: str, attribute: str) -> float:
        values = [
            candidate for candidate in selected if candidate.entity_type == entity_type
        ]
        if not values:
            return 1.0
        return sum(bool(getattr(value, attribute)) for value in values) / len(values)

    field_coverage = {
        entity_type: {
            "title": coverage(entity_type, "has_title"),
            "release": coverage(entity_type, "has_release"),
            "runtime": coverage(entity_type, "has_runtime"),
            "language": coverage(entity_type, "has_language"),
            "country": coverage(entity_type, "has_country"),
            "externalId": coverage(entity_type, "has_external_id"),
        }
        for entity_type in sorted(CONTENT_TYPES)
    }
    hierarchy_counts = Counter(result.hierarchy_coverage.values())
    episode_count = result.counts_by_type["TV_EPISODE"]
    episode_complete = sum(
        1
        for qid, value in result.hierarchy_coverage.items()
        if result.entity_types[qid] == "TV_EPISODE" and value == "COMPLETE"
    )
    episode_parent_coverage = episode_complete / episode_count if episode_count else 1.0
    all_title_coverage = (
        sum(candidate.has_title for candidate in selected) / len(selected)
        if selected
        else 1.0
    )
    violations = []
    if all_title_coverage < thresholds.minimum_title_coverage:
        violations.append(
            "TITLE_COVERAGE:"
            f"{all_title_coverage:.12g}<"
            f"{thresholds.minimum_title_coverage:.12g}"
        )
    movie_release = field_coverage["MOVIE"]["release"]
    if movie_release < thresholds.minimum_movie_release_coverage:
        violations.append(
            "MOVIE_RELEASE_COVERAGE:"
            f"{movie_release:.12g}<"
            f"{thresholds.minimum_movie_release_coverage:.12g}"
        )
    if episode_parent_coverage < thresholds.minimum_episode_parent_coverage:
        violations.append(
            "EPISODE_PARENT_COVERAGE:"
            f"{episode_parent_coverage:.12g}<"
            f"{thresholds.minimum_episode_parent_coverage:.12g}"
        )
    return ReferenceSelectionAudit(
        config_digest=config.digest,
        demand_profile_digest=config.demand_profile_digest,
        content_count=result.content_count,
        agent_count=result.agent_count,
        counts_by_type=result.counts_by_type,
        fallback_counts=dict(sorted(result.fallback_counts.items())),
        hierarchy_counts={
            key: hierarchy_counts.get(key, 0) for key in ("COMPLETE", "PARTIAL")
        },
        field_coverage=field_coverage,
        violations=tuple(violations),
        status="FAILED" if violations else "PASS",
    )


def candidate_rank(candidate: ReferenceCandidate) -> tuple[int, ...]:
    return (
        -candidate.demand_score,
        -candidate.completeness_score,
        -candidate.exact_identifier_count,
        -candidate.sitelink_count,
        qid_number(candidate.qid),
    )


def _has_value(payload: dict, property_id: str) -> bool:
    return any(
        _statement_value(statement) is not None
        for statement in _statements(payload, property_id)
    )


def _release_decade(payload: dict) -> str | None:
    for statement in _statements(payload, "P577"):
        value = _statement_value(statement)
        if not isinstance(value, dict):
            continue
        time = value.get("time")
        if not isinstance(time, str):
            continue
        match = re.match(r"^[+-]([0-9]{4})", time)
        if match:
            year = int(match.group(1))
            return f"{(year // 10) * 10}s"
    return None


def reference_candidate(
    payload: dict,
    entity_type: str,
    demand_profile: AssetDemandProfile | None = None,
) -> ReferenceCandidate:
    relations = relation_references(payload)
    parents = tuple(
        sorted(
            {
                relation.target_qid
                for relation in relations
                if relation.property_id in REFERENCE_PARENT_PROPERTIES
            },
            key=qid_number,
        )
    )
    labels = payload.get("labels")
    has_title = isinstance(labels, dict) and any(
        isinstance(value, dict) and bool(value.get("value"))
        for value in labels.values()
    )
    exact_ids = sum(
        len(_statements(payload, property_id)) for property_id in ("P345", "P2704")
    )
    completeness = (
        (25 if has_title else 0)
        + (15 if _has_value(payload, "P577") else 0)
        + (15 if _has_value(payload, "P2047") else 0)
        + (10 if _qid_values(payload, "P364") else 0)
        + (10 if _qid_values(payload, "P495") else 0)
        + (10 if _qid_values(payload, "P136") else 0)
        + (15 if exact_ids else 0)
        + (10 if entity_type in {"TV_SEASON", "TV_EPISODE"} and parents else 0)
    )
    demand_score = 0
    if demand_profile is not None:
        demand_score += round(
            demand_profile.content_type_weights.get(entity_type, 0) * 1000
        )
        demand_score += round(
            max(
                (
                    demand_profile.language_weights.get(qid, 0)
                    for qid in _qid_values(payload, "P364")
                ),
                default=0,
            )
            * 500
        )
        demand_score += round(
            max(
                (
                    demand_profile.country_weights.get(qid, 0)
                    for qid in _qid_values(payload, "P495")
                ),
                default=0,
            )
            * 500
        )
        decade = _release_decade(payload)
        if decade is not None:
            demand_score += round(demand_profile.decade_weights.get(decade, 0) * 500)
    return ReferenceCandidate(
        qid=str(payload.get("id") or ""),
        entity_type=entity_type,
        sitelink_count=wikipedia_sitelink_count(payload),
        completeness_score=completeness,
        exact_identifier_count=exact_ids,
        demand_score=demand_score,
        has_title=has_title,
        has_release=_has_value(payload, "P577"),
        has_runtime=_has_value(payload, "P2047"),
        has_language=bool(_qid_values(payload, "P364")),
        has_country=bool(_qid_values(payload, "P495")),
        has_external_id=bool(exact_ids),
        parent_qids=parents,
        relations=relations,
    )


def _take(
    candidates: list[ReferenceCandidate],
    *,
    limit: int,
    hierarchy_support: Counter[str] | None = None,
) -> list[ReferenceCandidate]:
    return sorted(
        candidates,
        key=lambda candidate: (
            -candidate.demand_score,
            -(0 if hierarchy_support is None else hierarchy_support[candidate.qid]),
            -candidate.completeness_score,
            -candidate.exact_identifier_count,
            -candidate.sitelink_count,
            qid_number(candidate.qid),
        ),
    )[:limit]


def select_reference_candidates(
    candidates: list[ReferenceCandidate],
    config: ReferenceSelectionConfig | None = None,
) -> ReferenceSelectionResult:
    config = config or ReferenceSelectionConfig()
    by_qid = {candidate.qid: candidate for candidate in candidates}
    if len(by_qid) != len(candidates):
        raise ValueError("reference candidates contain duplicate QIDs")

    selected_content: dict[str, str] = {}
    hierarchy_coverage: dict[str, str] = {}
    fallback_counts = {entity_type: 0 for entity_type in CONTENT_TYPES}
    hierarchy_support: Counter[str] = Counter(
        parent_qid
        for candidate in candidates
        if candidate.entity_type in {"TV_SEASON", "TV_EPISODE"}
        for parent_qid in candidate.parent_qids
    )

    def select_type(
        entity_type: str,
        preferred_parent_qids: set[str] | None = None,
    ) -> None:
        quota = config.content_quotas[entity_type]
        available = [
            candidate
            for candidate in candidates
            if candidate.entity_type == entity_type
        ]
        if len(available) < quota:
            raise ValueError(f"not enough {entity_type} candidates for quota {quota}")
        preferred = (
            []
            if preferred_parent_qids is None
            else [
                candidate
                for candidate in available
                if set(candidate.parent_qids) & preferred_parent_qids
            ]
        )
        support = (
            hierarchy_support if entity_type in {"TV_SERIES", "TV_SEASON"} else None
        )
        chosen = _take(
            preferred,
            limit=quota,
            hierarchy_support=support,
        )
        chosen_qids = {candidate.qid for candidate in chosen}
        if len(chosen) < quota:
            fallback = _take(
                [
                    candidate
                    for candidate in available
                    if candidate.qid not in chosen_qids
                ],
                limit=quota - len(chosen),
                hierarchy_support=support,
            )
            chosen.extend(fallback)
            fallback_counts[entity_type] = len(fallback)
        for candidate in chosen:
            selected_content[candidate.qid] = entity_type
            if entity_type in {"TV_SEASON", "TV_EPISODE"}:
                hierarchy_coverage[candidate.qid] = (
                    "COMPLETE"
                    if preferred_parent_qids is not None
                    and bool(set(candidate.parent_qids) & preferred_parent_qids)
                    else "PARTIAL"
                )

    select_type("MOVIE")
    select_type("TV_SERIES")
    selected_series = {
        qid
        for qid, entity_type in selected_content.items()
        if entity_type == "TV_SERIES"
    }
    select_type("TV_SEASON", selected_series)
    selected_hierarchy = selected_series | {
        qid
        for qid, entity_type in selected_content.items()
        if entity_type == "TV_SEASON"
    }
    select_type("TV_EPISODE", selected_hierarchy)

    if len(selected_content) != config.content_target_count:
        raise RuntimeError("selected content count differs from configured target")

    reference_counts: Counter[str] = Counter()
    hints: dict[str, set[str]] = {}
    for qid in selected_content:
        for relation in by_qid[qid].relations:
            if relation.property_id in CREDIT_PERSON_PROPERTIES:
                entity_type = "PERSON"
            elif relation.property_id in CREDIT_ORGANIZATION_PROPERTIES:
                entity_type = "ORGANIZATION"
            else:
                continue
            reference_counts[relation.target_qid] += 1
            hints.setdefault(relation.target_qid, set()).add(entity_type)

    selected_agents: dict[str, str] = {}
    for entity_type in ("PERSON", "ORGANIZATION"):
        ranked = []
        for qid, count in reference_counts.items():
            candidate = by_qid.get(qid)
            if candidate is None:
                continue
            resolved_type = candidate.entity_type
            if resolved_type == "UNKNOWN" and entity_type in hints[qid]:
                resolved_type = entity_type
            if resolved_type != entity_type:
                continue
            ranked.append((candidate, count))
        ranked.sort(
            key=lambda item: (
                -item[1],
                *candidate_rank(item[0]),
            )
        )
        for candidate, _ in ranked[: config.agent_limits[entity_type]]:
            selected_agents[candidate.qid] = entity_type

    entity_types = {**selected_content, **selected_agents}
    return ReferenceSelectionResult(
        content_qids=tuple(sorted(selected_content, key=qid_number)),
        agent_qids=tuple(sorted(selected_agents, key=qid_number)),
        entity_types=entity_types,
        hierarchy_coverage=hierarchy_coverage,
        fallback_counts=fallback_counts,
    )
