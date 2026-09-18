"""Spark-independent deterministic Wikidata subset rules and contracts."""

from __future__ import annotations

import json
import re
from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any, Literal
from urllib.parse import urlsplit

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_serializer,
    field_validator,
    model_validator,
)

from video_media_catalog.canonical import canonical_json, sha256_digest
from video_media_catalog.constants import (
    CREDIT_ORGANIZATION_PROPERTIES,
    CREDIT_PERSON_PROPERTIES,
    MEDIA_ENTITY_TYPES,
    RELATION_PROPERTIES,
)
from video_media_catalog.models import ObjectRef
from video_media_catalog.source_manifest import SourceManifestEntry
from video_media_catalog.transform import (
    _classification_closure,
    _qid_values,
    _statement_value,
    _statements,
    _wikidata_type,
)
from video_media_catalog.wikidata import normalize_wikidata_entity

SUBSET_ALGORITHM_ID = "video-media-catalog-wikidata-subset-v1"
NORMALIZATION_ALGORITHM_ID = "video-media-catalog-wikidata-normalization-v1"
SUBSET_SCHEMA_VERSION = "1.0"
DEFAULT_TARGET_COUNT = 100_000
DEFAULT_WORK_QUOTAS = {
    "MOVIE": 30_000,
    "TV_SERIES": 15_000,
    "TV_SEASON": 10_000,
    "TV_EPISODE": 25_000,
}
ENTITY_BUDGET_TYPES = frozenset(
    {
        "MOVIE",
        "TV_SERIES",
        "TV_SEASON",
        "TV_EPISODE",
        "PERSON",
        "ORGANIZATION",
    }
)
HIERARCHY_PROPERTIES = frozenset(
    set(RELATION_PROPERTIES)
    - set(CREDIT_PERSON_PROPERTIES)
    - set(CREDIT_ORGANIZATION_PROPERTIES)
)
_QID = re.compile(r"^Q([1-9][0-9]*)$")
_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")


def qid_number(qid: str) -> int:
    match = _QID.fullmatch(qid)
    if match is None:
        raise ValueError(f"invalid Wikidata item id: {qid!r}")
    return int(match.group(1))


def wikipedia_sitelink_count(payload: Mapping[str, Any]) -> int:
    """Count language-Wikipedia sitelinks, excluding non-Wikipedia projects."""

    sitelinks = payload.get("sitelinks")
    if not isinstance(sitelinks, Mapping):
        return 0
    excluded = {
        "commonswiki",
        "incubatorwiki",
        "mediawiki",
        "metawiki",
        "specieswiki",
        "testwiki",
        "wikidatawiki",
    }
    return sum(
        1
        for site in sitelinks
        if isinstance(site, str)
        and re.fullmatch(r"[a-z0-9_-]+wiki", site) is not None
        and site not in excluded
    )


class SubsetSelectionConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    target_count: int = Field(default=DEFAULT_TARGET_COUNT, gt=0)
    work_quotas: dict[str, int] = Field(
        default_factory=lambda: dict(DEFAULT_WORK_QUOTAS)
    )

    @field_validator("work_quotas")
    @classmethod
    def validate_work_quotas(cls, value: dict[str, int]) -> dict[str, int]:
        if set(value) != set(DEFAULT_WORK_QUOTAS):
            raise ValueError(
                "work_quotas must contain exactly MOVIE, TV_SERIES, "
                "TV_SEASON, and TV_EPISODE"
            )
        if any(isinstance(count, bool) or count < 0 for count in value.values()):
            raise ValueError("work quotas must be non-negative integers")
        return {entity_type: int(value[entity_type]) for entity_type in sorted(value)}

    @model_validator(mode="after")
    def validate_total(self) -> SubsetSelectionConfig:
        if sum(self.work_quotas.values()) > self.target_count:
            raise ValueError("work quota total must not exceed target_count")
        return self

    @property
    def work_target_count(self) -> int:
        return sum(self.work_quotas.values())

    @property
    def digest(self) -> str:
        return sha256_digest(
            canonical_json(
                {
                    "algorithmId": SUBSET_ALGORITHM_ID,
                    "targetCount": self.target_count,
                    "workQuotas": self.work_quotas,
                }
            )
        )


@dataclass(frozen=True)
class RelationReference:
    property_id: str
    target_qid: str

    def __post_init__(self) -> None:
        if self.property_id not in RELATION_PROPERTIES:
            raise ValueError(f"unsupported relation property: {self.property_id}")
        qid_number(self.target_qid)


@dataclass(frozen=True)
class SelectionCandidate:
    qid: str
    entity_type: str
    sitelink_count: int = 0
    relations: tuple[RelationReference, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        qid_number(self.qid)
        if self.sitelink_count < 0:
            raise ValueError("sitelink_count must be non-negative")


@dataclass(frozen=True)
class SelectionResult:
    selected_qids: tuple[str, ...]
    entity_types: Mapping[str, str]
    work_count: int
    hierarchy_count: int
    credit_count: int
    fallback_count: int

    @property
    def selected_count(self) -> int:
        return len(self.selected_qids)

    @property
    def counts_by_type(self) -> dict[str, int]:
        counts = Counter(self.entity_types[qid] for qid in self.selected_qids)
        return {
            entity_type: counts.get(entity_type, 0)
            for entity_type in sorted(ENTITY_BUDGET_TYPES)
        }


def _candidate_rank(candidate: SelectionCandidate) -> tuple[int, int]:
    return (-candidate.sitelink_count, qid_number(candidate.qid))


def _append_candidates(
    selected: dict[str, str],
    candidates: Iterable[SelectionCandidate],
    *,
    limit: int,
    type_override: Mapping[str, str] | None = None,
) -> int:
    added = 0
    for candidate in candidates:
        if len(selected) >= limit or candidate.qid in selected:
            continue
        entity_type = (
            type_override[candidate.qid]
            if type_override is not None
            else candidate.entity_type
        )
        if entity_type not in ENTITY_BUDGET_TYPES:
            continue
        selected[candidate.qid] = entity_type
        added += 1
    return added


def select_subset_candidates(
    candidates: Iterable[SelectionCandidate],
    config: SubsetSelectionConfig | None = None,
) -> SelectionResult:
    """Select a strict deterministic entity budget independent of input order."""

    config = config or SubsetSelectionConfig()
    by_qid: dict[str, SelectionCandidate] = {}
    for candidate in candidates:
        if candidate.qid in by_qid:
            raise ValueError(f"duplicate selection candidate: {candidate.qid}")
        by_qid[candidate.qid] = candidate

    selected: dict[str, str] = {}
    works = [
        candidate
        for candidate in by_qid.values()
        if candidate.entity_type in MEDIA_ENTITY_TYPES
    ]
    for entity_type in DEFAULT_WORK_QUOTAS:
        ranked = sorted(
            (candidate for candidate in works if candidate.entity_type == entity_type),
            key=_candidate_rank,
        )
        for candidate in ranked[: config.work_quotas[entity_type]]:
            selected[candidate.qid] = entity_type

    initial_quota_count = len(selected)
    refill_needed = config.work_target_count - initial_quota_count
    if refill_needed > 0:
        refill = sorted(
            (candidate for candidate in works if candidate.qid not in selected),
            key=_candidate_rank,
        )
        for candidate in refill[:refill_needed]:
            selected[candidate.qid] = candidate.entity_type
    work_count = len(selected)
    selected_work_qids = set(selected)

    relation_rows = [
        relation for qid in selected_work_qids for relation in by_qid[qid].relations
    ]
    hierarchy_qids = {
        relation.target_qid
        for relation in relation_rows
        if relation.property_id in HIERARCHY_PROPERTIES
    }
    hierarchy = sorted(
        (
            by_qid[qid]
            for qid in hierarchy_qids
            if qid in by_qid
            and by_qid[qid].entity_type in ENTITY_BUDGET_TYPES
            and qid not in selected
        ),
        key=_candidate_rank,
    )
    hierarchy_count = _append_candidates(
        selected,
        hierarchy,
        limit=config.target_count,
    )

    reference_counts: Counter[str] = Counter()
    hinted_types: dict[str, set[str]] = {}
    for relation in relation_rows:
        if relation.property_id in CREDIT_PERSON_PROPERTIES:
            hint = "PERSON"
        elif relation.property_id in CREDIT_ORGANIZATION_PROPERTIES:
            hint = "ORGANIZATION"
        else:
            continue
        reference_counts[relation.target_qid] += 1
        hinted_types.setdefault(relation.target_qid, set()).add(hint)

    credit_types: dict[str, str] = {}
    credit_candidates: list[SelectionCandidate] = []
    for qid in reference_counts:
        candidate = by_qid.get(qid)
        if candidate is None or qid in selected:
            continue
        entity_type = candidate.entity_type
        if entity_type == "UNKNOWN":
            hints = hinted_types[qid]
            entity_type = "PERSON" if "PERSON" in hints else "ORGANIZATION"
        if entity_type not in ENTITY_BUDGET_TYPES:
            continue
        credit_types[qid] = entity_type
        credit_candidates.append(candidate)
    credit_candidates.sort(
        key=lambda candidate: (
            -reference_counts[candidate.qid],
            *_candidate_rank(candidate),
        )
    )
    credit_count = _append_candidates(
        selected,
        credit_candidates,
        limit=config.target_count,
        type_override=credit_types,
    )

    fallback = sorted(
        (
            candidate
            for candidate in by_qid.values()
            if candidate.qid not in selected
            and candidate.entity_type in ENTITY_BUDGET_TYPES
        ),
        key=_candidate_rank,
    )
    fallback_count = _append_candidates(
        selected,
        fallback,
        limit=config.target_count,
    )
    selected_qids = tuple(sorted(selected, key=qid_number))
    return SelectionResult(
        selected_qids=selected_qids,
        entity_types=dict(selected),
        work_count=work_count,
        hierarchy_count=hierarchy_count,
        credit_count=credit_count,
        fallback_count=fallback_count,
    )


def relation_references(payload: Mapping[str, Any]) -> tuple[RelationReference, ...]:
    result: list[RelationReference] = []
    for property_id in RELATION_PROPERTIES:
        for statement in _statements(dict(payload), property_id):
            target = _statement_value(statement)
            if isinstance(target, str) and _QID.fullmatch(target):
                result.append(RelationReference(property_id, target))
    return tuple(result)


def selection_candidate(
    payload: Mapping[str, Any],
    entity_type: str,
) -> SelectionCandidate:
    qid = str(payload.get("id") or "")
    return SelectionCandidate(
        qid=qid,
        entity_type=entity_type,
        sitelink_count=wikipedia_sitelink_count(payload),
        relations=relation_references(payload),
    )


def classify_payloads(
    payloads: Iterable[Mapping[str, Any]],
) -> list[SelectionCandidate]:
    """Small-fixture helper using exactly the runtime P31/P279 semantics."""

    by_qid = {
        str(payload["id"]): dict(payload)
        for payload in payloads
        if isinstance(payload.get("id"), str)
    }
    closure = _classification_closure(by_qid)
    return [
        selection_candidate(payload, _wikidata_type(payload, closure))
        for _, payload in sorted(by_qid.items(), key=lambda item: qid_number(item[0]))
    ]


def classification_dependency_qids(
    selected_qids: Iterable[str],
    payload_by_qid: Mapping[str, Mapping[str, Any]],
) -> tuple[str, ...]:
    """Return available P31/P279 class rows needed by selected payloads."""

    selected = set(selected_qids)
    pending = {
        target
        for qid in selected
        if (payload := payload_by_qid.get(qid)) is not None
        for target in _qid_values(dict(payload), "P31")
    }
    dependencies: set[str] = set()
    while pending:
        qid = min(pending, key=qid_number)
        pending.remove(qid)
        if qid in selected or qid in dependencies:
            continue
        payload = payload_by_qid.get(qid)
        if payload is None:
            continue
        dependencies.add(qid)
        pending.update(_qid_values(dict(payload), "P279"))
    return tuple(sorted(dependencies, key=qid_number))


def prune_relation_statements(
    payload: Mapping[str, Any],
    selected_qids: Iterable[str],
) -> tuple[dict[str, Any], int]:
    """Remove every relation statement that cannot resolve inside the budget."""

    selected = set(selected_qids)
    result = dict(payload)
    claims = payload.get("claims")
    if not isinstance(claims, Mapping):
        result["claims"] = {}
        return result, 0
    pruned = 0
    copied_claims: dict[str, Any] = {}
    for property_id, raw_statements in claims.items():
        if property_id not in RELATION_PROPERTIES:
            copied_claims[str(property_id)] = raw_statements
            continue
        if not isinstance(raw_statements, list):
            continue
        kept: list[dict[str, Any]] = []
        for statement in raw_statements:
            target = (
                _statement_value(statement) if isinstance(statement, dict) else None
            )
            if isinstance(target, str) and target in selected:
                kept.append(statement)
            else:
                pruned += 1
        if kept:
            copied_claims[str(property_id)] = kept
    result["claims"] = copied_claims
    return result, pruned


def prepare_subset_payload(
    payload: Mapping[str, Any],
    selected_qids: Iterable[str],
    *,
    dependency_row: bool,
) -> tuple[dict[str, Any], int]:
    """Prune relations and keep dependency classes outside catalog_entity."""

    result, pruned = prune_relation_statements(payload, selected_qids)
    if dependency_row:
        claims = result.get("claims")
        if isinstance(claims, dict):
            claims.pop("P31", None)
    return result, pruned


def parse_and_normalize_dump_line(raw_line: str) -> dict[str, Any] | None:
    """Parse one official array line without relying on its dump position."""

    text = raw_line.strip().lstrip("\ufeff")
    if not text:
        return None
    if text.startswith("["):
        text = text[1:].lstrip()
    if text.endswith("]"):
        text = text[:-1].rstrip()
    if text.endswith(","):
        text = text[:-1].rstrip()
    if not text:
        return None
    value = json.loads(text)
    if not isinstance(value, dict):
        raise ValueError("Wikidata dump line must contain one JSON object")
    return normalize_wikidata_entity(value)


def _to_camel(value: str) -> str:
    first, *rest = value.split("_")
    return first + "".join(part.capitalize() for part in rest)


class SubsetAuditManifest(BaseModel):
    model_config = ConfigDict(
        alias_generator=_to_camel,
        extra="forbid",
        populate_by_name=True,
    )

    schema_version: Literal["1.0"] = SUBSET_SCHEMA_VERSION
    status: Literal["COMPLETE"] = "COMPLETE"
    algorithm_id: Literal["video-media-catalog-wikidata-subset-v1"] = (
        SUBSET_ALGORITHM_ID
    )
    config_digest: str
    dump: ObjectRef
    subset: ObjectRef
    source_manifest: ObjectRef
    target_count: int = Field(gt=0)
    work_quotas: dict[str, int]
    selected_count: int = Field(ge=0)
    selected_counts: dict[str, int]
    dependency_rows: int = Field(ge=0)
    pruned_relation_statements: int = Field(ge=0)
    normalization_staging_uri: str

    @field_validator("dump", "subset", "source_manifest", mode="before")
    @classmethod
    def reject_object_ref_extras(cls, value: Any) -> Any:
        aliases = {
            field.alias or name for name, field in ObjectRef.model_fields.items()
        }
        allowed = set(ObjectRef.model_fields) | aliases
        if isinstance(value, Mapping):
            extras = set(value) - allowed
        else:
            extras = set(getattr(value, "__pydantic_extra__", {}) or {})
        if extras:
            raise ValueError(f"ObjectRef contains unexpected fields: {sorted(extras)}")
        return value

    @field_validator("config_digest")
    @classmethod
    def validate_digest(cls, value: str) -> str:
        if _DIGEST.fullmatch(value) is None:
            raise ValueError("config_digest must be sha256:<64 lowercase hex>")
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

    @field_validator("selected_counts")
    @classmethod
    def validate_selected_counts(cls, value: dict[str, int]) -> dict[str, int]:
        if set(value) != set(ENTITY_BUDGET_TYPES):
            raise ValueError("selected_counts must contain exactly six entity types")
        if any(isinstance(count, bool) or count < 0 for count in value.values()):
            raise ValueError("selected counts must be non-negative integers")
        return value

    @model_validator(mode="after")
    def validate_complete_manifest(self) -> SubsetAuditManifest:
        config = SubsetSelectionConfig(
            target_count=self.target_count,
            work_quotas=self.work_quotas,
        )
        if config.digest != self.config_digest:
            raise ValueError("config_digest does not bind subset selection config")
        if self.selected_count > self.target_count:
            raise ValueError("selected_count exceeds target_count")
        if sum(self.selected_counts.values()) != self.selected_count:
            raise ValueError("selected_counts do not sum to selected_count")
        for name, reference in {
            "dump": self.dump,
            "subset": self.subset,
            "source_manifest": self.source_manifest,
        }.items():
            if (
                reference.etag is None
                or reference.object_version is None
                or reference.object_version == "null"
            ):
                raise ValueError(f"{name} ObjectRef requires ETag and VersionId")
            if reference.size_bytes <= 0:
                raise ValueError(f"{name} ObjectRef must be non-empty")
        if (
            self.source_manifest.format != "OBJECT_FORMAT_PARQUET"
            or self.source_manifest.media_type != "application/vnd.apache.parquet"
        ):
            raise ValueError("source_manifest must reference a Parquet object")
        for name, reference in {"dump": self.dump, "subset": self.subset}.items():
            if (
                reference.format != "OBJECT_FORMAT_JSON"
                or reference.media_type != "application/x-bzip2"
            ):
                raise ValueError(f"{name} must reference Wikidata JSON bzip2")
        if self.subset.size_bytes > 4 * 1024**3:
            raise ValueError("subset exceeds the 4 GiB compressed object limit")
        return self

    @field_serializer(
        "target_count",
        "selected_count",
        "dependency_rows",
        "pruned_relation_statements",
        when_used="json",
    )
    def serialize_int64(self, value: int) -> str:
        return str(value)


def audit_json_bytes(manifest: SubsetAuditManifest) -> bytes:
    return (
        canonical_json(
            manifest.model_dump(mode="json", by_alias=True, exclude_none=True)
        ).encode("utf-8")
        + b"\n"
    )


def subset_source_manifest_entry(subset: ObjectRef) -> SourceManifestEntry:
    """Build the one-row runtime manifest entry for a completed subset."""

    if (
        subset.format != "OBJECT_FORMAT_JSON"
        or subset.media_type != "application/x-bzip2"
        or subset.etag is None
        or subset.object_version is None
        or subset.object_version == "null"
        or urlsplit(subset.uri).scheme != "s3"
    ):
        raise ValueError("subset must be a complete immutable S3 JSON bzip2 ObjectRef")
    return SourceManifestEntry(
        source="wikidata",
        uri=subset.uri,
        sha256=subset.checksum.value,
        size_bytes=subset.size_bytes,
        compression="bzip2",
        object_version=subset.object_version,
        etag=subset.etag,
        license="CC0-1.0",
    )


class NormalizedStagingManifest(BaseModel):
    """Commit marker for reusable deterministic normalized Parquet staging."""

    model_config = ConfigDict(
        alias_generator=_to_camel,
        extra="forbid",
        populate_by_name=True,
    )

    schema_version: Literal["1.0"] = SUBSET_SCHEMA_VERSION
    status: Literal["COMPLETE"] = "COMPLETE"
    algorithm_id: Literal["video-media-catalog-wikidata-normalization-v1"] = (
        NORMALIZATION_ALGORITHM_ID
    )
    dump: ObjectRef
    data_uri: str
    row_count: int = Field(gt=0)

    @model_validator(mode="after")
    def require_immutable_dump(self) -> NormalizedStagingManifest:
        if self.dump.etag is None or self.dump.object_version is None:
            raise ValueError("staging dump ObjectRef requires ETag and VersionId")
        return self

    @field_serializer("row_count", when_used="json")
    def serialize_row_count(self, value: int) -> str:
        return str(value)

    def json_bytes(self) -> bytes:
        return (
            canonical_json(
                self.model_dump(mode="json", by_alias=True, exclude_none=True)
            ).encode("utf-8")
            + b"\n"
        )
