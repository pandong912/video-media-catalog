"""Deterministic, review-only asset-to-reference candidate ranking."""

from __future__ import annotations

import re
import unicodedata
from difflib import SequenceMatcher
from enum import StrEnum
from typing import Any, Literal, Self

from pydantic import Field, field_validator, model_validator

from video_media_catalog.canonical import canonical_json, sha256_digest
from video_media_catalog.gold_api_models import GoldCatalogEntity
from video_media_catalog.identity import require_canonical_uuid7
from video_media_catalog.v2_contracts import (
    V2ContractModel,
    require_rfc3339,
)

MATCH_ALGORITHM_ID = "catalog-asset-match-v1"
_ENTITY_KEY = re.compile(r"^sha256:[0-9a-f]{64}$")
_NAMESPACE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")
_LANGUAGE = re.compile(r"^[A-Za-z]{2,8}(?:-[A-Za-z0-9]{1,8})*$")
_INDEX = re.compile(r"^[a-z0-9][a-z0-9._-]{0,254}$")
_LANGUAGE_ALIASES = {
    "arabic": "ar",
    "chinese": "zh",
    "dutch": "nl",
    "english": "en",
    "french": "fr",
    "german": "de",
    "hindi": "hi",
    "italian": "it",
    "japanese": "ja",
    "korean": "ko",
    "polish": "pl",
    "portuguese": "pt",
    "russian": "ru",
    "spanish": "es",
    "thai": "th",
    "turkish": "tr",
    "vietnamese": "vi",
}


def normalize_language_tag(value: str) -> str:
    normalized = value.strip().lower()
    normalized = _LANGUAGE_ALIASES.get(normalized, normalized)
    if _LANGUAGE.fullmatch(normalized) is None:
        raise ValueError("language must be a BCP 47 tag or supported language name")
    return normalized


class MatchContentType(StrEnum):
    MOVIE = "MOVIE"
    TV_SERIES = "TV_SERIES"
    TV_SEASON = "TV_SEASON"
    TV_EPISODE = "TV_EPISODE"
    UNKNOWN = "UNKNOWN"


class MatchConfidenceTier(StrEnum):
    EXACT = "EXACT"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class MatchDisposition(StrEnum):
    REVIEW_REQUIRED = "REVIEW_REQUIRED"
    NO_CANDIDATE = "NO_CANDIDATE"


class MatchExternalIdentifier(V2ContractModel):
    namespace: str
    value: str = Field(min_length=1, max_length=256)

    @field_validator("namespace")
    @classmethod
    def validate_namespace(cls, value: str) -> str:
        normalized = value.strip().lower()
        if _NAMESPACE.fullmatch(normalized) is None:
            raise ValueError("namespace must be a lowercase identifier")
        return normalized

    @field_validator("value")
    @classmethod
    def normalize_value(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("external identifier value must not be blank")
        return normalized


class AssetMatchRequest(V2ContractModel):
    schema_version: Literal["1.0"] = "1.0"
    asset_version_id: str
    content_type: MatchContentType = MatchContentType.UNKNOWN
    title: str | None = Field(default=None, max_length=512)
    alternate_titles: tuple[str, ...] = Field(
        default=(),
        max_length=32,
    )
    release_year: int | None = Field(default=None, ge=1870, le=2200)
    duration_seconds: int | None = Field(default=None, gt=0, le=86_400)
    season_number: int | None = Field(default=None, ge=0, le=100_000)
    episode_number: int | None = Field(default=None, ge=0, le=100_000)
    languages: tuple[str, ...] = Field(default=(), max_length=32)
    external_identifiers: tuple[MatchExternalIdentifier, ...] = Field(
        default=(),
        max_length=32,
    )

    @field_validator("asset_version_id")
    @classmethod
    def validate_asset_version_id(cls, value: str) -> str:
        return require_canonical_uuid7(value)

    @field_validator("title")
    @classmethod
    def normalize_title(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise ValueError("title must be absent or non-blank")
        return normalized

    @field_validator("alternate_titles")
    @classmethod
    def normalize_alternate_titles(
        cls,
        value: tuple[str, ...],
    ) -> tuple[str, ...]:
        normalized = tuple(
            dict.fromkeys(title.strip() for title in value if title.strip())
        )
        if len(normalized) != len(value):
            raise ValueError("alternate_titles must be unique and non-blank")
        return normalized

    @field_validator("languages")
    @classmethod
    def normalize_languages(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(sorted({normalize_language_tag(item) for item in value}))
        if len(normalized) != len(value):
            raise ValueError("languages must be unique BCP 47 tags")
        return normalized

    @field_validator("external_identifiers")
    @classmethod
    def unique_identifiers(
        cls,
        value: tuple[MatchExternalIdentifier, ...],
    ) -> tuple[MatchExternalIdentifier, ...]:
        ordered = tuple(sorted(value, key=lambda item: (item.namespace, item.value)))
        if len(set(ordered)) != len(ordered):
            raise ValueError("external_identifiers must be unique")
        return ordered

    @model_validator(mode="after")
    def require_identity_signal(self) -> Self:
        if self.title is None and not self.external_identifiers:
            raise ValueError("a title or external identifier is required")
        return self

    @property
    def digest(self) -> str:
        return sha256_digest(
            canonical_json(
                self.model_dump(mode="json", by_alias=True, exclude_none=True)
            )
        )


class CatalogMatchRecord(V2ContractModel):
    entity_key: str
    release_plan_id: str
    content_type: MatchContentType
    titles: tuple[str, ...] = Field(min_length=1, max_length=64)
    release_year: int | None = Field(default=None, ge=1870, le=2200)
    duration_seconds: int | None = Field(default=None, gt=0, le=86_400)
    season_number: int | None = Field(default=None, ge=0, le=100_000)
    episode_number: int | None = Field(default=None, ge=0, le=100_000)
    languages: tuple[str, ...] = Field(default=(), max_length=32)
    external_identifiers: tuple[MatchExternalIdentifier, ...] = Field(
        default=(),
        max_length=64,
    )

    @field_validator("entity_key", "release_plan_id")
    @classmethod
    def validate_entity_key(cls, value: str) -> str:
        if _ENTITY_KEY.fullmatch(value) is None:
            raise ValueError("entity key must be sha256:<64 lowercase hex>")
        return value

    @field_validator("titles")
    @classmethod
    def normalize_titles(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(
            dict.fromkeys(title.strip() for title in value if title.strip())
        )
        if len(normalized) != len(value):
            raise ValueError("titles must be unique and non-blank")
        return normalized

    @field_validator("languages")
    @classmethod
    def validate_languages(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(sorted({normalize_language_tag(item) for item in value}))
        if len(normalized) != len(value):
            raise ValueError("languages must be unique BCP 47 tags")
        return normalized

    @field_validator("external_identifiers")
    @classmethod
    def validate_identifiers(
        cls,
        value: tuple[MatchExternalIdentifier, ...],
    ) -> tuple[MatchExternalIdentifier, ...]:
        ordered = tuple(sorted(value, key=lambda item: (item.namespace, item.value)))
        if len(set(ordered)) != len(ordered):
            raise ValueError("external_identifiers must be unique")
        return ordered


class AssetMatchConfig(V2ContractModel):
    max_retrieval_records: int = Field(default=5000, gt=0, le=50_000)
    max_candidates: int = Field(default=20, gt=0, le=20)
    minimum_candidate_score: int = Field(default=300, ge=0, le=1000)
    high_score: int = Field(default=700, ge=0, le=1000)
    medium_score: int = Field(default=500, ge=0, le=1000)

    @model_validator(mode="after")
    def validate_threshold_order(self) -> Self:
        if not (self.minimum_candidate_score <= self.medium_score <= self.high_score):
            raise ValueError("candidate score thresholds must be ordered")
        return self

    @property
    def digest(self) -> str:
        return sha256_digest(
            canonical_json(self.model_dump(mode="json", by_alias=True))
        )


class AssetMatchEvidence(V2ContractModel):
    exact_identifier_matches: tuple[str, ...]
    identifier_conflicts: tuple[str, ...]
    matched_title: str | None = Field(default=None, max_length=512)
    title_similarity: float = Field(ge=0, le=1)
    type_match: bool | None
    year_delta: int | None = Field(default=None, ge=0)
    duration_delta_ratio: float | None = Field(default=None, ge=0)
    season_match: bool | None
    episode_match: bool | None
    language_overlap: tuple[str, ...]
    score_components: dict[str, int]


class AssetMatchCandidate(V2ContractModel):
    candidate_key: str
    entity_key: str
    content_type: MatchContentType
    display_title: str
    score: int = Field(ge=0, le=1000)
    confidence: float = Field(ge=0, le=1)
    tier: MatchConfidenceTier
    evidence: AssetMatchEvidence

    @field_validator("candidate_key", "entity_key")
    @classmethod
    def validate_keys(cls, value: str) -> str:
        if _ENTITY_KEY.fullmatch(value) is None:
            raise ValueError("candidate and entity keys must be SHA-256 keys")
        return value


class AssetMatchManifest(V2ContractModel):
    schema_version: Literal["1.0"] = "1.0"
    algorithm_id: Literal["catalog-asset-match-v1"] = MATCH_ALGORITHM_ID
    config_digest: str
    request_digest: str
    asset_version_id: str
    release_plan_id: str
    concrete_index: str
    generated_at: str
    retrieval_count: int = Field(ge=0)
    disposition: MatchDisposition
    candidates: tuple[AssetMatchCandidate, ...] = Field(max_length=20)

    @field_validator("config_digest", "request_digest", "release_plan_id")
    @classmethod
    def validate_digests(cls, value: str) -> str:
        if _ENTITY_KEY.fullmatch(value) is None:
            raise ValueError("digest must be sha256:<64 lowercase hex>")
        return value

    @field_validator("asset_version_id")
    @classmethod
    def validate_asset_id(cls, value: str) -> str:
        return require_canonical_uuid7(value)

    @field_validator("concrete_index")
    @classmethod
    def validate_concrete_index(cls, value: str) -> str:
        if _INDEX.fullmatch(value) is None:
            raise ValueError("concrete_index must be a concrete index name")
        return value

    @field_validator("generated_at")
    @classmethod
    def validate_generated_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @model_validator(mode="after")
    def validate_manifest(self) -> Self:
        expected = (
            MatchDisposition.REVIEW_REQUIRED
            if self.candidates
            else MatchDisposition.NO_CANDIDATE
        )
        if self.disposition != expected:
            raise ValueError("disposition does not match candidates")
        ordered = tuple(
            sorted(
                self.candidates,
                key=lambda item: (-item.score, item.entity_key),
            )
        )
        if self.candidates != ordered:
            raise ValueError("candidates must use deterministic score order")
        if len({item.entity_key for item in self.candidates}) != len(self.candidates):
            raise ValueError("candidate entity keys must be unique")
        return self

    def json_bytes(self) -> bytes:
        return (
            canonical_json(
                self.model_dump(mode="json", by_alias=True, exclude_none=True)
            )
            + "\n"
        ).encode()


class ConfirmedReferenceLinkProposal(V2ContractModel):
    entity_key: str
    method: Literal["CATALOG_MATCH_CONFIRMED"] = "CATALOG_MATCH_CONFIRMED"
    confidence: float = Field(ge=0, le=1)
    evidence: dict[str, Any]

    @field_validator("entity_key")
    @classmethod
    def validate_entity_key(cls, value: str) -> str:
        if _ENTITY_KEY.fullmatch(value) is None:
            raise ValueError("entity_key must be a SHA-256 key")
        return value

    @model_validator(mode="after")
    def enforce_control_plane_evidence_bound(self) -> Self:
        size = len(canonical_json(self.evidence).encode())
        if not 2 <= size <= 8192:
            raise ValueError("reference-link evidence must fit 8192 UTF-8 bytes")
        return self


def normalize_match_title(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value).casefold()
    normalized = "".join(
        character if character.isalnum() else " " for character in normalized
    )
    return " ".join(normalized.split())


def title_similarity(
    request_titles: tuple[str, ...],
    candidate_titles: tuple[str, ...],
) -> tuple[float, str | None]:
    best_score = 0.0
    best_title = None
    for request_title in request_titles:
        left = normalize_match_title(request_title)
        if not left:
            continue
        left_tokens = set(left.split())
        for candidate_title in candidate_titles:
            right = normalize_match_title(candidate_title)
            if not right:
                continue
            if left == right:
                score = 1.0
            else:
                right_tokens = set(right.split())
                union = left_tokens | right_tokens
                jaccard = len(left_tokens & right_tokens) / len(union) if union else 0
                sequence = SequenceMatcher(None, left, right).ratio()
                score = max(jaccard, sequence)
            if score > best_score or (
                score == best_score
                and best_title is not None
                and candidate_title < best_title
            ):
                best_score = score
                best_title = candidate_title
    return round(best_score, 6), best_title


def _identifier_evidence(
    request: AssetMatchRequest,
    record: CatalogMatchRecord,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    requested = {(item.namespace, item.value) for item in request.external_identifiers}
    candidate = {(item.namespace, item.value) for item in record.external_identifiers}
    matches = tuple(
        f"{namespace}:{value}" for namespace, value in sorted(requested & candidate)
    )
    request_by_namespace: dict[str, set[str]] = {}
    candidate_by_namespace: dict[str, set[str]] = {}
    for namespace, value in requested:
        request_by_namespace.setdefault(namespace, set()).add(value)
    for namespace, value in candidate:
        candidate_by_namespace.setdefault(namespace, set()).add(value)
    conflicts = tuple(
        namespace
        for namespace in sorted(request_by_namespace.keys() & candidate_by_namespace)
        if request_by_namespace[namespace].isdisjoint(candidate_by_namespace[namespace])
    )
    return matches, conflicts


def _type_score(
    request_type: MatchContentType,
    record_type: MatchContentType,
) -> tuple[int, bool | None]:
    if request_type == MatchContentType.UNKNOWN:
        return 0, None
    if request_type == record_type:
        return 80, True
    return -250, False


def _year_score(
    request_year: int | None,
    record_year: int | None,
) -> tuple[int, int | None]:
    if request_year is None or record_year is None:
        return 0, None
    delta = abs(request_year - record_year)
    if delta == 0:
        return 100, delta
    if delta == 1:
        return 70, delta
    if delta == 2:
        return 30, delta
    return -100, delta


def _duration_score(
    request_seconds: int | None,
    record_seconds: int | None,
) -> tuple[int, float | None]:
    if request_seconds is None or record_seconds is None:
        return 0, None
    delta = abs(request_seconds - record_seconds)
    ratio = delta / max(request_seconds, record_seconds)
    if delta <= 120 or ratio <= 0.05:
        return 80, round(ratio, 6)
    if ratio <= 0.1:
        return 40, round(ratio, 6)
    if ratio > 0.2:
        return -50, round(ratio, 6)
    return 0, round(ratio, 6)


def _number_score(
    requested: int | None,
    candidate: int | None,
) -> tuple[int, bool | None]:
    if requested is None or candidate is None:
        return 0, None
    return (100, True) if requested == candidate else (-250, False)


def rank_asset_candidate(
    request: AssetMatchRequest,
    record: CatalogMatchRecord,
    config: AssetMatchConfig,
) -> AssetMatchCandidate | None:
    request_titles = tuple(
        title
        for title in (request.title, *request.alternate_titles)
        if title is not None
    )
    similarity, matched_title = title_similarity(request_titles, record.titles)
    identifier_matches, identifier_conflicts = _identifier_evidence(
        request,
        record,
    )
    identifier_score = 600 if identifier_matches else 0
    if identifier_conflicts and not identifier_matches:
        identifier_score -= 700
    type_score, type_match = _type_score(
        request.content_type,
        record.content_type,
    )
    year_score, year_delta = _year_score(
        request.release_year,
        record.release_year,
    )
    duration_score, duration_delta = _duration_score(
        request.duration_seconds,
        record.duration_seconds,
    )
    season_score, season_match = _number_score(
        request.season_number,
        record.season_number,
    )
    episode_score, episode_match = _number_score(
        request.episode_number,
        record.episode_number,
    )
    language_overlap = tuple(sorted(set(request.languages) & set(record.languages)))
    components = {
        "duration": duration_score,
        "episode": episode_score,
        "externalIdentifier": identifier_score,
        "language": 20 if language_overlap else 0,
        "season": season_score,
        "title": round(similarity * 300),
        "type": type_score,
        "year": year_score,
    }
    score = min(1000, max(0, sum(components.values())))
    if score < config.minimum_candidate_score and not identifier_matches:
        return None
    if identifier_matches:
        tier = MatchConfidenceTier.EXACT
        confidence = round(score / 1000, 6)
    elif score >= config.high_score:
        tier = MatchConfidenceTier.HIGH
        confidence = round(score / 1000, 6)
    elif score >= config.medium_score:
        tier = MatchConfidenceTier.MEDIUM
        confidence = round(score / 1000, 6)
    else:
        tier = MatchConfidenceTier.LOW
        confidence = round(score / 1000, 6)
    candidate_key = sha256_digest(
        canonical_json(
            {
                "algorithmId": MATCH_ALGORITHM_ID,
                "configDigest": config.digest,
                "entityKey": record.entity_key,
                "releasePlanId": record.release_plan_id,
                "requestDigest": request.digest,
            }
        )
    )
    return AssetMatchCandidate(
        candidate_key=candidate_key,
        entity_key=record.entity_key,
        content_type=record.content_type,
        display_title=record.titles[0],
        score=score,
        confidence=confidence,
        tier=tier,
        evidence=AssetMatchEvidence(
            exact_identifier_matches=identifier_matches,
            identifier_conflicts=identifier_conflicts,
            matched_title=matched_title,
            title_similarity=similarity,
            type_match=type_match,
            year_delta=year_delta,
            duration_delta_ratio=duration_delta,
            season_match=season_match,
            episode_match=episode_match,
            language_overlap=language_overlap,
            score_components=components,
        ),
    )


def generate_asset_match_manifest(
    *,
    request: AssetMatchRequest,
    records: list[CatalogMatchRecord],
    release_plan_id: str,
    concrete_index: str,
    generated_at: str,
    config: AssetMatchConfig | None = None,
) -> AssetMatchManifest:
    config = config or AssetMatchConfig()
    if len(records) > config.max_retrieval_records:
        raise ValueError("retrieval set exceeds configured bound")
    if len({record.entity_key for record in records}) != len(records):
        raise ValueError("retrieval records contain duplicate entity keys")
    if any(record.release_plan_id != release_plan_id for record in records):
        raise ValueError("retrieval records cross Gold release plans")
    candidates = [
        candidate
        for record in records
        if (candidate := rank_asset_candidate(request, record, config)) is not None
    ]
    candidates.sort(key=lambda item: (-item.score, item.entity_key))
    selected = tuple(candidates[: config.max_candidates])
    return AssetMatchManifest(
        config_digest=config.digest,
        request_digest=request.digest,
        asset_version_id=request.asset_version_id,
        release_plan_id=release_plan_id,
        concrete_index=concrete_index,
        generated_at=generated_at,
        retrieval_count=len(records),
        disposition=(
            MatchDisposition.REVIEW_REQUIRED
            if selected
            else MatchDisposition.NO_CANDIDATE
        ),
        candidates=selected,
    )


def catalog_match_record_from_gold(
    entity: GoldCatalogEntity,
) -> CatalogMatchRecord:
    try:
        content_type = MatchContentType(entity.entity_kind)
    except ValueError:
        content_type = MatchContentType.UNKNOWN
    release_year = None
    for value in entity.attributes.premiered:
        match = re.match(r"^([0-9]{4})", value)
        if match is not None:
            release_year = int(match.group(1))
            break
    duration_seconds = None
    runtime_values = (
        entity.attributes.runtime_minutes or entity.attributes.average_runtime_minutes
    )
    for value in runtime_values:
        try:
            minutes = float(value)
        except ValueError:
            continue
        if minutes > 0:
            duration_seconds = round(minutes * 60)
            break
    languages = set()
    for value in (
        *entity.attributes.languages,
        *(title.language for title in entity.titles if title.language != "und"),
    ):
        try:
            languages.add(normalize_language_tag(value))
        except ValueError:
            continue
    return CatalogMatchRecord(
        entity_key=entity.entity_key,
        release_plan_id=entity.release_plan_id,
        content_type=content_type,
        titles=tuple(dict.fromkeys(title.value for title in entity.titles))
        or (entity.display_name,),
        release_year=release_year,
        duration_seconds=duration_seconds,
        languages=tuple(sorted(languages)),
        external_identifiers=tuple(
            MatchExternalIdentifier(namespace=namespace, value=value)
            for namespace, value in sorted(
                {(item.namespace, item.value) for item in entity.external_identifiers}
            )
        ),
    )


def confirmed_reference_link_proposal(
    *,
    manifest: AssetMatchManifest,
    candidate_key: str,
) -> ConfirmedReferenceLinkProposal:
    """Build a control-plane proposal only after an explicit candidate choice."""

    selected = next(
        (
            candidate
            for candidate in manifest.candidates
            if candidate.candidate_key == candidate_key
        ),
        None,
    )
    if selected is None:
        raise ValueError("candidate_key is not present in the match manifest")
    return ConfirmedReferenceLinkProposal(
        entity_key=selected.entity_key,
        confidence=selected.confidence,
        evidence={
            "algorithmId": manifest.algorithm_id,
            "candidateKey": selected.candidate_key,
            "concreteIndex": manifest.concrete_index,
            "evidence": selected.evidence.model_dump(
                mode="json",
                by_alias=True,
                exclude_none=True,
            ),
            "releasePlanId": manifest.release_plan_id,
            "requestDigest": manifest.request_digest,
            "score": selected.score,
            "tier": selected.tier.value,
        },
    )
