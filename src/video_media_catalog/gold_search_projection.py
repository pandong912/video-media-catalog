"""Bounded, deterministic OpenSearch documents from one Gold release."""

from __future__ import annotations

import json
import re
from collections import defaultdict
from collections.abc import Mapping
from functools import lru_cache
from typing import Any

from video_media_catalog.canonical import canonical_json
from video_media_catalog.catalog_source_policy import (
    CatalogSourceCapability,
    CatalogSourcePolicy,
    load_catalog_source_policy,
)
from video_media_catalog.douban import douban_jump_url
from video_media_catalog.eidr import normalize_eidr_id
from video_media_catalog.gold import (
    RESEARCH_CONTEXT_ID,
    GoldAssertionLineage,
    assertion_lineage_from_trace,
)
from video_media_catalog.reference_ontology import (
    ReferenceDomain,
    ReferenceTypeConcept,
    load_global_reference_ontology,
)
from video_media_catalog.v2_contracts import require_sha256

DISPLAY_LANGUAGES = ("zh-hans", "zh-hant", "zh", "en", "und")
MAX_TITLES = 64
MAX_IDENTIFIERS = 64
MAX_ATTRIBUTE_VALUES = 128
MAX_RELATION_TYPES = 64
MAX_SOURCE_BADGES = 32
MAX_WINNING_ASSERTIONS = 128
MAX_CITATION_KEYS = 16
MAX_RIGHTS_SUMMARIES = 32
MAX_CONFLICTS = 64
MAX_CONFLICT_ASSERTIONS = 32
MAX_CONFLICT_VALUES = 16
MAX_HIERARCHY_RELATION_CANDIDATES = 16
MAX_SPORTS_PARTICIPANTS = 64
MAX_CURRENT_RESULTS = 64
MAX_ANIMATION_RELATIONS = 128
MAX_MUSIC_CREDITS = 128
MAX_MUSIC_VARIANT_SIGNALS = 128
MAX_MUSIC_MEDIA_OBJECTS = 128
MAX_MUSIC_WORK_PARENTS = 16
MAX_MUSIC_EXACT_AUTHORITY_RELATION_SIGNALS = 1
MAX_OBSERVATION_HISTORY = 128
MAX_REFERENCE_CANDIDATES = 128
MAX_REFERENCE_RELATIONS = 128

_REFERENCE_CAPABILITIES = ("DISPLAY", "IDENTITY", "RESEARCH")
_NEWS_REFERENCE_DOMAINS = frozenset({ReferenceDomain.NEWS, ReferenceDomain.JOURNALISM})
_LEARNING_REFERENCE_DOMAINS = frozenset(
    {ReferenceDomain.LEARNING, ReferenceDomain.INSTRUCTION}
)
_MUSIC_REFERENCE_DOMAINS = frozenset({ReferenceDomain.MUSIC})

_SPORTS_FIELD_PREDICATES = frozenset(
    {
        "discipline",
        "scheduled_start",
        "event_date",
        "round_number",
        "event_status",
        "score",
        "period_score",
        "set_score",
        "placement",
        "outcome",
        "standing",
        "match_statistics",
        "lineup",
        "freeze_frame",
    }
)
_SPORTS_RESULT_PREDICATES = frozenset(
    {
        "score",
        "period_score",
        "set_score",
        "placement",
        "outcome",
        "standing",
    }
)
_SPORTS_RELATION_PREDICATES = frozenset(
    {
        "held_at",
        "participant",
        "part_of_competition",
        "part_of_sports_season",
        "part_of_stage",
    }
)
_ANIMATION_FIELD_PREDICATES = frozenset(
    {
        "animation_technique",
        "release_form",
        "animation_tradition",
    }
)
_ANIMATION_RELATION_PREDICATES = frozenset(
    {
        "adaptation_of",
        "character_appearance",
        "voice_performer",
        "production_credit",
    }
)

_PARENT_RELATION_PREDICATES = frozenset(
    {
        "part_of",
        "part_of_season",
        "part_of_series",
        "season",
    }
)
_SERIES_RELATION_PREDICATES = frozenset({"part_of", "part_of_series"})
_SEASON_RELATION_PREDICATES = frozenset({"part_of", "part_of_season", "season"})
_EXPLICIT_SERIES_RELATION_PREDICATES = frozenset({"part_of_series"})
_EXPLICIT_SEASON_RELATION_PREDICATES = frozenset({"part_of_season", "season"})

_ATTRIBUTE_PREDICATES = {
    "format": "formats",
    "language": "languages",
    "status": "statuses",
    "premiered": "premiered",
    "ended": "ended",
    "runtime_minutes": "runtimeMinutes",
    "average_runtime_minutes": "averageRuntimeMinutes",
    "genre": "genres",
}
_FILM_TV_CONTENT_TYPES = {"MOVIE", "TV_SERIES", "TV_SEASON", "TV_EPISODE"}
_MATCH_INT_PREDICATES = {
    "release_year": (1870, 9999),
    "season_number": (1, 2**31 - 1),
    "episode_number": (1, 2**31 - 1),
    "runtime_minutes": (1, 10_000),
}
_IDENTITY_FIELD_PREDICATES = {
    "title": "identity.title",
    "original_title": "identity.alias",
}
_PREMIERED_YEAR = re.compile(r"^(\d{4})(?:-\d{2}-\d{2})?$")


@lru_cache(maxsize=1)
def _projection_source_policy() -> CatalogSourcePolicy:
    return load_catalog_source_policy()


@lru_cache(maxsize=1)
def _reference_concepts() -> dict[str, ReferenceTypeConcept]:
    ontology = load_global_reference_ontology()
    return {item.type_concept_id: item for item in ontology.type_concepts}


def _reference_type_concept(entity_kind: str) -> ReferenceTypeConcept | None:
    return _reference_concepts().get(entity_kind.strip().upper())


def _reference_slices(concept: ReferenceTypeConcept | None) -> list[str]:
    if concept is None:
        return []
    domains = set(concept.domains)
    slices = []
    if domains & _NEWS_REFERENCE_DOMAINS:
        slices.append("NEWS/JOURNALISM")
    if domains & _LEARNING_REFERENCE_DOMAINS:
        slices.append("LEARNING/INSTRUCTION")
    if domains & _MUSIC_REFERENCE_DOMAINS:
        slices.append("MUSIC")
    return slices


def _reference_source_allowed(
    source_product_id: str,
    reference_slices: list[str],
) -> bool:
    if source_product_id == "wikidata-json-dump":
        return True
    return (
        source_product_id in {"musicbrainz-core-dump", "wikidata-music-reference"}
        and "MUSIC" in reference_slices
    )


def _external_identifier_url(
    namespace: str,
    identifier: str,
    referent_kind: str,
) -> str | None:
    if namespace.strip().lower() == "eidr-content":
        try:
            normalized = normalize_eidr_id(identifier)
        except ValueError:
            return None
        return f"https://doi.org/{normalized}"
    musicbrainz_entity = {
        "musicbrainz-artist": "artist",
        "musicbrainz-label": "label",
        "musicbrainz-work": "work",
        "musicbrainz-recording": "recording",
        "musicbrainz-release-group": "release-group",
        "musicbrainz-release": "release",
    }.get(namespace.strip().lower())
    if musicbrainz_entity is not None:
        return f"https://musicbrainz.org/{musicbrainz_entity}/{identifier}"
    if namespace.strip().lower() == "isrc":
        return f"https://musicbrainz.org/isrc/{identifier}"
    return douban_jump_url(namespace, identifier, referent_kind)


def _dict(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if hasattr(value, "asDict"):
        return dict(value.asDict(recursive=True))
    raise TypeError(f"expected row-like value, got {type(value).__name__}")


def _title_sort(value: dict[str, Any]) -> tuple[Any, ...]:
    language = str(value.get("language") or "und").lower().replace("_", "-")
    try:
        language_rank = DISPLAY_LANGUAGES.index(language)
    except ValueError:
        language_rank = len(DISPLAY_LANGUAGES)
    role = str(value.get("titleRole") or "OTHER").upper()
    role_rank = {"PRIMARY": 0, "TITLE": 1, "ALIAS": 10}.get(role, 5)
    text = str(value["value"])
    return language_rank, language, role_rank, text.casefold(), text


def _assertion_ids(value: dict[str, Any]) -> tuple[str, ...]:
    raw = value.get("assertion_ids_json")
    if not isinstance(raw, str):
        raise ValueError("Gold projection requires assertion_ids_json")
    parsed = json.loads(raw)
    if not isinstance(parsed, list) or not all(
        isinstance(item, str) for item in parsed
    ):
        raise ValueError("Gold assertion IDs must be a JSON string array")
    return tuple(sorted(set(parsed)))


def _lineage(value: dict[str, Any]) -> tuple[GoldAssertionLineage, ...]:
    trace = value.get("trace_json")
    if not isinstance(trace, str):
        raise ValueError("Gold projection requires trace_json")
    return assertion_lineage_from_trace(trace)


def _match_int(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def _unique_int(values: set[int]) -> int | None:
    if len(values) != 1:
        return None
    return next(iter(values))


def _premiered_year(value: Any) -> int | None:
    if not isinstance(value, str):
        return None
    match = _PREMIERED_YEAR.fullmatch(value)
    if match is None:
        return None
    year = int(match.group(1))
    if year < 1870 or year > 9999:
        return None
    return year


def _release_year(explicit: set[int], premiered: set[int]) -> int | None:
    if len(explicit) > 1:
        return None
    selected = _unique_int(explicit)
    inferred = _unique_int(premiered)
    if selected is not None and inferred is not None and selected != inferred:
        return None
    return selected if selected is not None else inferred


def _duration_us(minutes: set[int]) -> int | None:
    selected = _unique_int(minutes)
    if selected is None:
        return None
    return selected * 60_000_000


def _unique_json_scalar(
    values: list[dict[str, Any]],
    *,
    expected_type: type,
) -> Any | None:
    selected = {
        item["valueJson"]: item["value"]
        for item in values
        if isinstance(item["value"], expected_type)
        and not isinstance(item["value"], bool)
    }
    if len(selected) != 1:
        return None
    return next(iter(selected.values()))


def _unique_key(values: set[str]) -> str | None:
    return next(iter(values)) if len(values) == 1 else None


def _is_studio_relation(qualifiers: Mapping[str, Any]) -> bool:
    if str(qualifiers.get("wikidataProperty") or "").upper() == "P272":
        return True
    role = " ".join(
        str(qualifiers.get(key) or "").strip().casefold()
        for key in ("role", "department")
    )
    return "studio" in role or "production company" in role


def _content_type(value: Any) -> str | None:
    kind = str(value or "").strip().upper()
    if kind in _FILM_TV_CONTENT_TYPES:
        return kind
    return None


def _optional_entity_key(value: Any, *, label: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{label} must be a sha256 entity key or null")
    return require_sha256(value, label=label)


def _derive_hierarchy_keys(
    value: dict[str, Any],
) -> tuple[str | None, str | None, str | None, int]:
    """Derive only unique hierarchy keys and fail closed on bounded overflow."""

    precomputed = any(
        key in value
        for key in (
            "parent_entity_key",
            "series_entity_key",
            "season_entity_key",
            "hierarchy_relation_overflow",
        )
    )
    if precomputed:
        overflow = int(value.get("hierarchy_relation_overflow") or 0)
        if overflow < 0:
            raise ValueError("hierarchy relation overflow must be non-negative")
        if overflow:
            return None, None, None, overflow
        return (
            _optional_entity_key(
                value.get("parent_entity_key"),
                label="parent_entity_key",
            ),
            _optional_entity_key(
                value.get("series_entity_key"),
                label="series_entity_key",
            ),
            _optional_entity_key(
                value.get("season_entity_key"),
                label="season_entity_key",
            ),
            0,
        )

    series_candidates: set[str] = set()
    season_candidates: set[str] = set()
    parent_candidates: set[str] = set()
    for raw in value.get("relations") or ():
        relation = _dict(raw)
        predicate = str(relation.get("predicate") or "").strip().lower()
        if predicate not in _PARENT_RELATION_PREDICATES:
            continue
        object_key = _optional_entity_key(
            relation.get("object_entity_key", relation.get("objectEntityKey")),
            label="relation object_entity_key",
        )
        if object_key is None:
            continue
        parent_candidates.add(object_key)
        object_level = (
            str(
                relation.get("object_entity_level")
                or relation.get("objectEntityLevel")
                or ""
            )
            .strip()
            .upper()
        )
        if object_level == "SERIES" or (
            not object_level and predicate in _EXPLICIT_SERIES_RELATION_PREDICATES
        ):
            series_candidates.add(object_key)
        if object_level == "SEASON" or (
            not object_level and predicate in _EXPLICIT_SEASON_RELATION_PREDICATES
        ):
            season_candidates.add(object_key)

    candidate_count = len(parent_candidates)
    overflow = max(0, candidate_count - MAX_HIERARCHY_RELATION_CANDIDATES)
    if overflow:
        return None, None, None, overflow

    entity_level = str(value.get("entity_level") or "").strip().upper()
    series_key = next(iter(series_candidates)) if len(series_candidates) == 1 else None
    season_key = next(iter(season_candidates)) if len(season_candidates) == 1 else None
    parent_key = None
    if entity_level == "SEASON":
        parent_key = (
            series_key
            if series_key is not None
            else (
                next(iter(parent_candidates)) if len(parent_candidates) == 1 else None
            )
        )
        season_key = None
    elif entity_level == "EPISODE":
        if season_key is not None:
            parent_key = season_key
        elif not season_candidates and series_key is not None:
            parent_key = series_key
        elif not season_candidates and len(parent_candidates) == 1:
            parent_key = next(iter(parent_candidates))
    return parent_key, series_key, season_key, 0


def project_gold_entity(
    row: Any,
    *,
    source_policy_digest: str | None = None,
) -> dict[str, Any]:
    value = _dict(row)
    source_policy = _projection_source_policy()
    policy_digest = require_sha256(
        source_policy_digest
        or value.get("source_policy_digest")
        or source_policy.digest,
        label="source_policy_digest",
    )
    if policy_digest != source_policy.digest:
        raise ValueError(
            "Gold projection source_policy_digest differs from canonical policy"
        )
    (
        parent_entity_key,
        series_entity_key,
        season_entity_key,
        hierarchy_relation_overflow,
    ) = _derive_hierarchy_keys(value)
    titles = []
    attributes: dict[str, set[str]] = {
        name: set() for name in _ATTRIBUTE_PREDICATES.values()
    }
    lineage_by_id: dict[str, GoldAssertionLineage] = {}
    winning: list[dict[str, Any]] = []
    winning_ids: set[str] = set()
    citation_overflow = 0
    match_ints: dict[str, set[int]] = {name: set() for name in _MATCH_INT_PREDICATES}
    premiered_years: set[int] = set()
    selected_field_values: dict[str, list[dict[str, Any]]] = defaultdict(list)
    reference_candidate_context: dict[str, dict[str, Any]] = {}

    def register_lineage(items: tuple[GoldAssertionLineage, ...]) -> None:
        for item in items:
            existing = lineage_by_id.get(item.assertion_id)
            if existing is not None and existing != item:
                raise ValueError("assertion lineage changed within one Gold entity")
            lineage_by_id[item.assertion_id] = item

    def register_reference_candidate(
        assertion_ids: tuple[str, ...],
        *,
        kind: str,
        predicate: str,
        value_json: str | None,
        value_text: str | None,
        qualifiers_json: str | None,
        resolution_status: str,
        valid_from: str | None = None,
        valid_to: str | None = None,
    ) -> None:
        for assertion_id in assertion_ids:
            candidate = {
                "kind": kind,
                "predicate": predicate,
                "valueJson": value_json,
                "valueText": value_text,
                "qualifiersJson": qualifiers_json,
                "resolutionStatus": resolution_status,
                "validFrom": valid_from,
                "validTo": valid_to,
            }
            existing = reference_candidate_context.get(assertion_id)
            if existing is None:
                reference_candidate_context[assertion_id] = candidate
                continue
            for key, item in candidate.items():
                prior = existing.get(key)
                if prior is not None and item is not None and prior != item:
                    raise ValueError(
                        "assertion candidate changed within one Gold entity"
                    )
                if prior is None and item is not None:
                    existing[key] = item

    for raw in value.get("fields") or []:
        field = _dict(raw)
        field_lineage = _lineage(field)
        register_lineage(field_lineage)
        assertion_ids = _assertion_ids(field)
        parsed = json.loads(field["value_json"])
        predicate = str(field["predicate"])
        qualifiers = json.loads(field["qualifiers_json"])
        if not isinstance(qualifiers, dict):
            raise ValueError("Gold field qualifiers must be a JSON object")
        register_reference_candidate(
            assertion_ids,
            kind="FIELD",
            predicate=predicate,
            value_json=str(field["value_json"]),
            value_text=parsed if isinstance(parsed, str) else None,
            qualifiers_json=str(field["qualifiers_json"]),
            resolution_status=str(field["resolution_status"]),
        )
        if field.get("resolution_status") not in {"SELECTED", "SET"}:
            continue
        selected = field.get("selected_assertion_id")
        winner_ids = (
            (selected,) if field["resolution_status"] == "SELECTED" else assertion_ids
        )
        if not all(isinstance(item, str) for item in winner_ids):
            raise ValueError("selected Gold field has invalid assertion lineage")
        field_lineage_by_id = {item.assertion_id: item for item in field_lineage}
        missing = set(winner_ids) - set(field_lineage_by_id)
        if missing:
            raise ValueError("selected Gold value has incomplete assertion lineage")
        selected_field_values[predicate].append(
            {
                "value": parsed,
                "valueJson": str(field["value_json"]),
                "qualifiersJson": str(field["qualifiers_json"]),
                "resolutionStatus": str(field["resolution_status"]),
                "sourceProductIds": sorted(
                    {field_lineage_by_id[item].source_product_id for item in winner_ids}
                ),
            }
        )
        for assertion_id in winner_ids:
            item = field_lineage_by_id[assertion_id]
            source = source_policy.source(item.source_product_id)
            if source.allows(CatalogSourceCapability.MATERIALIZATION):
                canonical_predicate = _IDENTITY_FIELD_PREDICATES.get(predicate)
                if canonical_predicate is None or not (
                    source_policy.allows_materializable_predicate(
                        item.source_product_id,
                        canonical_predicate,
                    )
                ):
                    continue
            citations = list(item.citation_keys)
            citation_overflow += max(0, len(citations) - MAX_CITATION_KEYS)
            winning.append(
                {
                    "kind": "FIELD",
                    "assertionId": assertion_id,
                    "predicate": predicate,
                    "valueJson": field["value_json"],
                    "qualifiersJson": field["qualifiers_json"],
                    "resolutionStatus": field["resolution_status"],
                    "sourceProductId": source.source_id,
                    "sourceRecordId": item.source_record_id,
                    "sourcePath": item.source_path,
                    "observedAt": item.observed_at,
                    "citationKeys": citations[:MAX_CITATION_KEYS],
                    "citationOverflow": max(
                        0,
                        len(citations) - MAX_CITATION_KEYS,
                    ),
                }
            )
            winning_ids.add(assertion_id)
        if predicate == "title" and isinstance(parsed, str):
            titles.append(
                {
                    "value": parsed,
                    "language": str(qualifiers.get("language") or "und"),
                    "titleRole": str(qualifiers.get("titleRole") or "OTHER").upper(),
                }
            )
        attribute = _ATTRIBUTE_PREDICATES.get(predicate)
        if attribute is not None and parsed is not None:
            attributes[attribute].add(str(parsed))
        bounds = _MATCH_INT_PREDICATES.get(predicate)
        number = _match_int(parsed)
        if (
            bounds is not None
            and number is not None
            and bounds[0] <= number <= bounds[1]
        ):
            match_ints[predicate].add(number)
        if predicate == "premiered":
            year = _premiered_year(parsed)
            if year is not None:
                premiered_years.add(year)
    titles = sorted(
        {
            (item["value"], item["language"], item["titleRole"]): item
            for item in titles
        }.values(),
        key=_title_sort,
    )
    title_overflow = max(0, len(titles) - MAX_TITLES)
    titles = titles[:MAX_TITLES]
    canonical_id = str(value["entity_key"])
    display = titles[0]["value"] if titles else canonical_id
    display_language = titles[0]["language"] if titles else "und"

    raw_identifiers = [_dict(item) for item in (value.get("identifiers") or [])]
    identifier_candidates: list[dict[str, Any]] = []
    for identifier in raw_identifiers:
        identifier_lineage = _lineage(identifier)
        register_lineage(identifier_lineage)
        assertion_ids = _assertion_ids(identifier)
        identifier_lineage_by_id = {
            item.assertion_id: item for item in identifier_lineage
        }
        if set(assertion_ids) - set(identifier_lineage_by_id):
            raise ValueError("Gold identifier has incomplete assertion lineage")
        identifier_value = canonical_json(
            {
                "issuer": identifier["issuer"],
                "referentKind": identifier["referent_kind"],
                "value": identifier["value"],
            }
        )
        register_reference_candidate(
            assertion_ids,
            kind="IDENTIFIER",
            predicate=str(identifier["namespace_id"]),
            value_json=identifier_value,
            value_text=str(identifier["value"]),
            qualifiers_json="{}",
            resolution_status="ACCEPTED",
        )
        for assertion_id in assertion_ids:
            item = identifier_lineage_by_id[assertion_id]
            source = source_policy.source(item.source_product_id)
            try:
                namespace = source_policy.namespace(str(identifier["namespace_id"]))
            except ValueError:
                continue
            if namespace.source.source_id != source.source_id:
                continue
            citations = list(item.citation_keys)
            citation_overflow += max(0, len(citations) - MAX_CITATION_KEYS)
            winning.append(
                {
                    "kind": "IDENTIFIER",
                    "assertionId": assertion_id,
                    "predicate": str(identifier["namespace_id"]),
                    "valueJson": identifier_value,
                    "qualifiersJson": "{}",
                    "resolutionStatus": "ACCEPTED",
                    "sourceProductId": source.source_id,
                    "sourceRecordId": item.source_record_id,
                    "sourcePath": item.source_path,
                    "observedAt": item.observed_at,
                    "citationKeys": citations[:MAX_CITATION_KEYS],
                    "citationOverflow": max(
                        0,
                        len(citations) - MAX_CITATION_KEYS,
                    ),
                }
            )
            winning_ids.add(assertion_id)
            namespace_id = str(identifier["namespace_id"])
            identifier_value_text = str(identifier["value"])
            referent_kind = str(identifier["referent_kind"])
            identifier_candidates.append(
                {
                    "sourceProductId": source.source_id,
                    "namespace": namespace_id,
                    "value": identifier_value_text,
                    "issuer": str(identifier["issuer"]),
                    "referentKind": referent_kind,
                    "assertionId": assertion_id,
                    "citationKeys": citations[:MAX_CITATION_KEYS],
                    "url": _external_identifier_url(
                        namespace_id,
                        identifier_value_text,
                        referent_kind,
                    ),
                }
            )

    identifier_documents_by_key: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for document in sorted(
        identifier_candidates,
        key=lambda item: (
            item["namespace"],
            item["value"],
            item["issuer"],
            item["referentKind"],
            item["sourceProductId"],
            item["assertionId"],
        ),
    ):
        identifier_documents_by_key.setdefault(
            (
                document["namespace"],
                document["value"],
                document["issuer"],
                document["referentKind"],
            ),
            document,
        )
    all_identifier_documents = list(identifier_documents_by_key.values())
    identifier_overflow = max(
        0,
        len(all_identifier_documents) - MAX_IDENTIFIERS,
    )
    identifier_documents = all_identifier_documents[:MAX_IDENTIFIERS]
    relation_summary = sorted(
        (
            {
                "predicate": str(_dict(item)["predicate"]),
                "count": int(_dict(item)["count"]),
            }
            for item in (value.get("relation_summary") or [])
        ),
        key=lambda item: item["predicate"],
    )
    relation_overflow = max(0, len(relation_summary) - MAX_RELATION_TYPES)
    relation_summary = relation_summary[:MAX_RELATION_TYPES]
    relation_targets: dict[str, set[str]] = defaultdict(set)
    studio_targets: set[str] = set()
    music_work_parent_candidates: set[str] = set()
    music_credit_candidates: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    reference_relation_candidates: list[dict[str, Any]] = []
    for raw in value.get("relations") or []:
        relation = _dict(raw)
        predicate = str(relation.get("predicate") or "").strip().lower()
        object_entity_key = _optional_entity_key(
            relation.get("object_entity_key", relation.get("objectEntityKey")),
            label="relation object_entity_key",
        )
        if not predicate or object_entity_key is None:
            continue
        relation_targets[predicate].add(object_entity_key)
        qualifiers_raw = relation.get("qualifiers_json")
        if isinstance(qualifiers_raw, str):
            qualifiers = json.loads(qualifiers_raw)
        else:
            qualifiers = relation.get("qualifiers") or {}
        if not isinstance(qualifiers, Mapping):
            raise ValueError("Gold relation qualifiers must be a JSON object")
        if predicate == "production_credit" and _is_studio_relation(qualifiers):
            studio_targets.add(object_entity_key)
        if not isinstance(relation.get("trace_json"), str):
            continue
        relation_lineage = _lineage(relation)
        register_lineage(relation_lineage)
        assertion_ids = _assertion_ids(relation)
        relation_lineage_by_id = {item.assertion_id: item for item in relation_lineage}
        if set(assertion_ids) - set(relation_lineage_by_id):
            raise ValueError("Gold relation has incomplete assertion lineage")
        has_music_authority_lineage = any(
            _reference_source_allowed(item.source_product_id, ["MUSIC"])
            and source_policy.source(item.source_product_id).allows(
                CatalogSourceCapability.GOLD_PROJECTION
            )
            for item in relation_lineage
        )
        if has_music_authority_lineage and predicate == "recording_of":
            music_work_parent_candidates.add(object_entity_key)
        if has_music_authority_lineage and predicate == "credit":
            role = str(qualifiers.get("role") or "UNSPECIFIED").strip().upper()
            authority_property = str(qualifiers.get("wikidataProperty") or "").strip()
            agent_kind = str(qualifiers.get("agentKind") or "").strip().upper()
            if agent_kind and agent_kind not in {"PERSON", "ORGANIZATION"}:
                raise ValueError("Music credit agentKind is invalid")
            music_credit_candidates.setdefault(
                (object_entity_key, role, authority_property, agent_kind),
                {
                    "entityKey": object_entity_key,
                    "role": role,
                    "authorityProperty": authority_property or None,
                    "agentKind": agent_kind or None,
                },
            )
        relation_value_json = canonical_json({"objectEntityKey": object_entity_key})
        qualifiers_json = canonical_json(dict(qualifiers))
        register_reference_candidate(
            assertion_ids,
            kind="RELATIONSHIP",
            predicate=predicate,
            value_json=relation_value_json,
            value_text=None,
            qualifiers_json=qualifiers_json,
            resolution_status="SET",
        )
        for assertion_id in assertion_ids:
            item = relation_lineage_by_id[assertion_id]
            source = source_policy.source(item.source_product_id)
            citations = list(item.citation_keys)
            citation_overflow += max(0, len(citations) - MAX_CITATION_KEYS)
            winning.append(
                {
                    "kind": "RELATIONSHIP",
                    "assertionId": assertion_id,
                    "predicate": predicate,
                    "valueJson": relation_value_json,
                    "qualifiersJson": qualifiers_json,
                    "resolutionStatus": "SET",
                    "sourceProductId": source.source_id,
                    "sourceRecordId": item.source_record_id,
                    "sourcePath": item.source_path,
                    "observedAt": item.observed_at,
                    "citationKeys": citations[:MAX_CITATION_KEYS],
                    "citationOverflow": max(
                        0,
                        len(citations) - MAX_CITATION_KEYS,
                    ),
                }
            )
            winning_ids.add(assertion_id)
            if source.allows(CatalogSourceCapability.GOLD_PROJECTION):
                reference_relation_candidates.append(
                    {
                        "predicate": predicate,
                        "objectEntityKey": object_entity_key,
                        "assertionId": assertion_id,
                        "sourceProductId": source.source_id,
                        "observedAt": item.observed_at,
                        "citationKeys": citations[:MAX_CITATION_KEYS],
                        "citationOverflow": max(
                            0,
                            len(citations) - MAX_CITATION_KEYS,
                        ),
                        "_implementationSourceProductId": item.source_product_id,
                    }
                )

    current_results = sorted(
        (
            {
                "predicate": predicate,
                "valueJson": item["valueJson"],
                "qualifiersJson": item["qualifiersJson"],
                "resolutionStatus": item["resolutionStatus"],
            }
            for predicate in sorted(_SPORTS_RESULT_PREDICATES)
            for item in selected_field_values.get(predicate, ())
        ),
        key=lambda item: (
            item["predicate"],
            item["qualifiersJson"],
            item["valueJson"],
        ),
    )
    current_result_overflow = max(
        0,
        len(current_results) - MAX_CURRENT_RESULTS,
    )
    current_results = current_results[:MAX_CURRENT_RESULTS]

    observation_history = []
    for raw in value.get("observations") or []:
        observation = _dict(raw)
        observation_lineage = _lineage(observation)
        register_lineage(observation_lineage)
        assertion_id = require_sha256(
            str(observation["assertion_id"]),
            label="observation assertion_id",
        )
        lineage_by_assertion = {item.assertion_id: item for item in observation_lineage}
        if assertion_id not in lineage_by_assertion:
            raise ValueError("Gold observation has incomplete assertion lineage")
        item = lineage_by_assertion[assertion_id]
        citations = list(item.citation_keys)
        citation_overflow += max(0, len(citations) - MAX_CITATION_KEYS)
        is_winning = bool(observation.get("is_winning"))
        if is_winning:
            winning_ids.add(assertion_id)
        observation_value_json = str(observation["value_json"])
        observation_value = json.loads(observation_value_json)
        register_reference_candidate(
            (assertion_id,),
            kind="FIELD",
            predicate=str(observation["predicate"]),
            value_json=observation_value_json,
            value_text=(
                observation_value if isinstance(observation_value, str) else None
            ),
            qualifiers_json=str(observation["qualifiers_json"]),
            resolution_status=str(
                reference_candidate_context.get(assertion_id, {}).get(
                    "resolutionStatus",
                    "OBSERVED",
                )
            ),
            valid_from=observation.get("valid_from"),
            valid_to=observation.get("valid_to"),
        )
        observation_history.append(
            {
                "predicate": str(observation["predicate"]),
                "observationKey": require_sha256(
                    str(observation["observation_key"]),
                    label="observation_key",
                ),
                "assertionId": assertion_id,
                "valueJson": observation_value_json,
                "qualifiersJson": str(observation["qualifiers_json"]),
                "observedAt": str(observation["observed_at"]),
                "validFrom": observation.get("valid_from"),
                "validTo": observation.get("valid_to"),
                "supersedesAssertionId": observation.get("supersedes_assertion_id"),
                "isWinning": is_winning,
                "sourceProductId": source_policy.canonical_source_id(
                    item.source_product_id
                ),
                "citationKeys": citations[:MAX_CITATION_KEYS],
                "citationOverflow": max(
                    0,
                    len(citations) - MAX_CITATION_KEYS,
                ),
            }
        )
    observation_history.sort(
        key=lambda item: (
            item["observedAt"],
            item["predicate"],
            item["assertionId"],
        ),
        reverse=True,
    )
    observation_overflow = max(
        0,
        len(observation_history) - MAX_OBSERVATION_HISTORY,
    )
    observation_history = observation_history[:MAX_OBSERVATION_HISTORY]

    conflict_documents = []
    for raw in value.get("conflicts") or []:
        conflict = _dict(raw)
        conflict_lineage = _lineage(conflict)
        register_lineage(conflict_lineage)
        assertion_ids = _assertion_ids(conflict)
        if set(assertion_ids) - {item.assertion_id for item in conflict_lineage}:
            raise ValueError("Gold conflict has incomplete assertion lineage")
        candidates = json.loads(conflict["candidate_values_json"])
        if not isinstance(candidates, list):
            raise ValueError("Gold conflict candidates must be a list")
        for assertion_id in assertion_ids:
            if assertion_id not in reference_candidate_context:
                register_reference_candidate(
                    (assertion_id,),
                    kind="FIELD",
                    predicate=str(conflict["predicate"]),
                    value_json=None,
                    value_text=None,
                    qualifiers_json=None,
                    resolution_status="CONFLICT",
                )
        conflict_documents.append(
            {
                "predicate": str(conflict["predicate"]),
                "scopeHash": str(conflict["scope_hash"]),
                "reason": str(conflict["reason"]),
                "assertionIds": list(assertion_ids[:MAX_CONFLICT_ASSERTIONS]),
                "assertionOverflow": max(
                    0,
                    len(assertion_ids) - MAX_CONFLICT_ASSERTIONS,
                ),
                "candidateValuesJson": [
                    canonical_json(item) for item in candidates[:MAX_CONFLICT_VALUES]
                ],
                "candidateValueOverflow": max(
                    0,
                    len(candidates) - MAX_CONFLICT_VALUES,
                ),
                "sourceProductIds": sorted(
                    {
                        source_policy.canonical_source_id(item.source_product_id)
                        for item in conflict_lineage
                    }
                ),
            }
        )
    conflict_documents.sort(
        key=lambda item: (
            item["predicate"],
            item["scopeHash"],
            item["reason"],
        )
    )
    reported_conflict_count = int(value.get("conflict_count") or 0)
    if reported_conflict_count != len(conflict_documents):
        raise ValueError("Gold conflict count differs from conflict lineage")
    conflict_predicates = sorted({item["predicate"] for item in conflict_documents})

    badge_counts: dict[str, set[str]] = defaultdict(set)
    winning_badge_counts: dict[str, set[str]] = defaultdict(set)
    badge_identity: dict[str, tuple[str, str, set[str]]] = {}
    rights: dict[tuple[str, str], dict[str, Any]] = {}
    for assertion_id, item in lineage_by_id.items():
        source = source_policy.source(item.source_product_id)
        source_id = source.source_id
        badge_counts[source_id].add(assertion_id)
        if assertion_id in winning_ids:
            winning_badge_counts[source_id].add(assertion_id)
        badge = badge_identity.get(source_id)
        if badge is None:
            badge_identity[source_id] = (
                item.source_name,
                item.rights.source_url,
                {item.rights.policy_zone.value},
            )
        else:
            if badge[:2] != (item.source_name, item.rights.source_url):
                raise ValueError("source badge metadata changed within one entity")
            badge[2].add(item.rights.policy_zone.value)
        rights_key = (source_id, item.rights.policy_id)
        rights_document = {
            "sourceProductId": source_id,
            "policyId": item.rights.policy_id,
            "policyZone": item.rights.policy_zone.value,
            "licenseId": item.rights.license_id,
            "licenseUri": item.rights.license_uri,
            "attributionText": (
                source.canonical.attribution or item.rights.attribution_text
            ),
            "sourceUrl": item.rights.source_url,
            "shareAlike": item.rights.share_alike,
        }
        existing_rights = rights.get(rights_key)
        if existing_rights is not None and existing_rights != rights_document:
            raise ValueError("rights metadata changed within one Gold entity")
        rights[rights_key] = rights_document

    source_badges = [
        {
            "sourceProductId": source_id,
            "displayName": badge_identity[source_id][0],
            "sourceUrl": badge_identity[source_id][1],
            "policyZones": sorted(badge_identity[source_id][2]),
            "assertionCount": len(badge_counts[source_id]),
            "winningAssertionCount": len(winning_badge_counts[source_id]),
        }
        for source_id in sorted(badge_identity)
    ]
    rights_documents = [rights[key] for key in sorted(rights)]
    winning.sort(
        key=lambda item: (
            item["kind"],
            item["predicate"],
            item["assertionId"],
        )
    )
    normalized_attributes = {}
    attribute_overflow = {}
    for name, items in attributes.items():
        values = sorted(items)
        attribute_overflow[name] = max(0, len(values) - MAX_ATTRIBUTE_VALUES)
        normalized_attributes[name] = values[:MAX_ATTRIBUTE_VALUES]
    entity_kind = str(value["entity_kind"]).strip().upper()
    reference_concept = _reference_type_concept(entity_kind)
    all_predicates = (
        set(selected_field_values)
        | set(relation_targets)
        | {item["predicate"] for item in observation_history}
    )
    source_products = {item.source_product_id for item in lineage_by_id.values()}
    reference_slices = _reference_slices(reference_concept)
    contributes_reference_slice = bool(reference_slices)
    domains: set[str] = set()
    if entity_kind.startswith("SPORTS_") or all_predicates & (
        _SPORTS_FIELD_PREDICATES | _SPORTS_RELATION_PREDICATES
    ):
        domains.add("SPORTS")
    if (
        all_predicates & (_ANIMATION_FIELD_PREDICATES | _ANIMATION_RELATION_PREDICATES)
        or "madb-public-data" in source_products
    ):
        domains.add("ANIMATION")
    if _content_type(entity_kind) is not None:
        domains.add("FILM_TV")
    if reference_concept is not None:
        domains.update(
            domain.value
            for domain in reference_concept.domains
            if domain != ReferenceDomain.GENERAL
        )
    if not domains:
        domains.add("GENERAL")
    reference_candidates = []
    if reference_concept is not None and contributes_reference_slice:
        for assertion_id in sorted(reference_candidate_context):
            item = lineage_by_id[assertion_id]
            context = reference_candidate_context[assertion_id]
            source = source_policy.source(item.source_product_id)
            if not _reference_source_allowed(
                item.source_product_id,
                reference_slices,
            ) or not source.allows(CatalogSourceCapability.GOLD_PROJECTION):
                continue
            citations = list(item.citation_keys)
            reference_candidates.append(
                {
                    "kind": context["kind"],
                    "assertionId": assertion_id,
                    "predicate": context["predicate"],
                    "valueJson": context.get("valueJson"),
                    "valueText": context.get("valueText"),
                    "qualifiersJson": context.get("qualifiersJson"),
                    "resolutionStatus": context["resolutionStatus"],
                    "sourceProductId": source.source_id,
                    "sourceRecordId": item.source_record_id,
                    "sourcePath": item.source_path,
                    "observedAt": item.observed_at,
                    "validFrom": context.get("validFrom"),
                    "validTo": context.get("validTo"),
                    "citationKeys": citations[:MAX_CITATION_KEYS],
                    "citationOverflow": max(
                        0,
                        len(citations) - MAX_CITATION_KEYS,
                    ),
                }
            )
    reference_relation_candidates = [
        {
            key: value
            for key, value in candidate.items()
            if key != "_implementationSourceProductId"
        }
        for candidate in reference_relation_candidates
        if _reference_source_allowed(
            str(candidate["_implementationSourceProductId"]),
            reference_slices,
        )
    ]
    reference_relation_candidates.sort(
        key=lambda item: (
            item["predicate"],
            item["objectEntityKey"],
            item["assertionId"],
        )
    )
    if not contributes_reference_slice:
        reference_relation_candidates = []
    is_music = "MUSIC" in domains
    music_work_parent_keys = sorted(music_work_parent_candidates) if is_music else []
    music_credits = (
        [music_credit_candidates[key] for key in sorted(music_credit_candidates)]
        if is_music
        else []
    )
    music_agent_kind = (
        _unique_json_scalar(
            selected_field_values["music_agent_kind"],
            expected_type=str,
        )
        if is_music
        else None
    )
    if music_agent_kind not in {None, "PERSON", "ORGANIZATION"}:
        raise ValueError("Gold Music agent kind must be PERSON or ORGANIZATION")
    music_media_objects = []
    if is_music:
        music_media_objects = sorted(
            {
                str(item["value"])
                for item in selected_field_values["media_object"]
                if isinstance(item["value"], str)
                and any(
                    _reference_source_allowed(source_product_id, ["MUSIC"])
                    for source_product_id in item["sourceProductIds"]
                )
            }
        )
    music_variant_signals = []
    if is_music:
        for item in selected_field_values["media_variant"]:
            if not any(
                _reference_source_allowed(source_product_id, ["MUSIC"])
                for source_product_id in item["sourceProductIds"]
            ):
                continue
            qualifiers = json.loads(item["qualifiersJson"])
            if not isinstance(qualifiers, dict):
                raise ValueError("Music variant qualifiers must be a JSON object")
            music_variant_signals.append(
                {
                    "value": str(item["value"]),
                    "authorityProperty": (
                        str(qualifiers["authorityProperty"])
                        if qualifiers.get("authorityProperty")
                        else None
                    ),
                    "authorityRelation": (
                        str(qualifiers["authorityRelation"])
                        if qualifiers.get("authorityRelation")
                        else None
                    ),
                    "direction": (
                        str(qualifiers["direction"])
                        if qualifiers.get("direction")
                        else None
                    ),
                    "targetId": (
                        str(qualifiers["targetId"])
                        if qualifiers.get("targetId")
                        else None
                    ),
                    "targetTitle": (
                        str(qualifiers["targetTitle"])
                        if qualifiers.get("targetTitle")
                        else None
                    ),
                }
            )
    music_variant_signals = sorted(
        {canonical_json(item): item for item in music_variant_signals}.values(),
        key=canonical_json,
    )
    has_recording_identifier = any(
        item["referentKind"] == "MUSIC_RECORDING"
        and item["namespace"]
        in {
            "isrc",
            "musicbrainz-recording",
            "wikidata-item",
        }
        for item in identifier_documents
    )
    music_exact_eligible = (
        entity_kind == "MUSIC_RECORDING"
        and has_recording_identifier
        and reported_conflict_count == 0
        and len(music_variant_signals) <= MAX_MUSIC_EXACT_AUTHORITY_RELATION_SIGNALS
    )
    reference_candidate_overflow = max(
        0,
        len(reference_candidates) - MAX_REFERENCE_CANDIDATES,
    )
    reference_relation_overflow = max(
        0,
        len(reference_relation_candidates) - MAX_REFERENCE_RELATIONS,
    )
    participant_keys = sorted(relation_targets["participant"])
    character_keys = sorted(relation_targets["character_appearance"])
    studio_keys = sorted(studio_targets)
    return {
        "entityKey": canonical_id,
        "entityLevel": str(value["entity_level"]),
        "entityKind": entity_kind,
        "domains": sorted(domains),
        "status": str(value["status"]),
        "releasePlanId": str(value["release_plan_id"]),
        "contextId": RESEARCH_CONTEXT_ID,
        "sourcePolicyDigest": policy_digest,
        "canonicalSubjectRoot": (
            None
            if reference_concept is None
            else reference_concept.canonical_root.value
        ),
        "typeConcepts": (
            []
            if reference_concept is None
            else [
                {
                    "typeConceptId": reference_concept.type_concept_id,
                    "conceptVersion": reference_concept.concept_version,
                    "canonicalRoot": reference_concept.canonical_root.value,
                }
            ]
        ),
        "referenceSlices": reference_slices,
        "authorityScope": (
            None if reference_concept is None else "GLOBAL_REFERENCE_ONLY"
        ),
        "referenceCapabilities": (
            [] if reference_concept is None else list(_REFERENCE_CAPABILITIES)
        ),
        "referenceCandidates": reference_candidates[:MAX_REFERENCE_CANDIDATES],
        "referenceRelations": reference_relation_candidates[:MAX_REFERENCE_RELATIONS],
        "displayName": display,
        "displayLanguage": display_language,
        "contentType": _content_type(value.get("entity_kind")),
        "parentEntityKey": parent_entity_key,
        "seriesEntityKey": series_entity_key,
        "seasonEntityKey": season_entity_key,
        "releaseYear": _release_year(match_ints["release_year"], premiered_years),
        "seasonNumber": _unique_int(match_ints["season_number"]),
        "episodeNumber": _unique_int(match_ints["episode_number"]),
        "durationUs": _duration_us(match_ints["runtime_minutes"]),
        "discipline": _unique_json_scalar(
            selected_field_values["discipline"],
            expected_type=str,
        ),
        "competitionEntityKey": _unique_key(relation_targets["part_of_competition"]),
        "sportsSeasonEntityKey": _unique_key(relation_targets["part_of_sports_season"]),
        "stageEntityKey": _unique_key(relation_targets["part_of_stage"]),
        "startAt": _unique_json_scalar(
            selected_field_values["scheduled_start"],
            expected_type=str,
        ),
        "sportsStatus": _unique_json_scalar(
            selected_field_values["event_status"],
            expected_type=str,
        ),
        "participantEntityKeys": participant_keys[:MAX_SPORTS_PARTICIPANTS],
        "venueEntityKey": _unique_key(relation_targets["held_at"]),
        "currentResults": current_results,
        "releaseForm": _unique_json_scalar(
            selected_field_values["release_form"],
            expected_type=str,
        ),
        "animationTradition": _unique_json_scalar(
            selected_field_values["animation_tradition"],
            expected_type=str,
        ),
        "animationTechniques": sorted(
            {
                str(item["value"])
                for item in selected_field_values["animation_technique"]
                if isinstance(item["value"], str)
            }
        )[:MAX_ATTRIBUTE_VALUES],
        "studioEntityKeys": studio_keys[:MAX_ANIMATION_RELATIONS],
        "characterEntityKeys": character_keys[:MAX_ANIMATION_RELATIONS],
        "musicAgentKind": music_agent_kind,
        "musicExactEligible": music_exact_eligible if is_music else None,
        "musicMediaObjects": music_media_objects[:MAX_MUSIC_MEDIA_OBJECTS],
        "musicWorkParentEntityKeys": music_work_parent_keys[:MAX_MUSIC_WORK_PARENTS],
        "musicCredits": music_credits[:MAX_MUSIC_CREDITS],
        "musicVariantSignals": music_variant_signals[:MAX_MUSIC_VARIANT_SIGNALS],
        "titles": titles,
        "attributes": normalized_attributes,
        "externalIdentifiers": identifier_documents,
        "relationSummary": relation_summary,
        "sourceBadges": source_badges[:MAX_SOURCE_BADGES],
        "winningAssertions": winning[:MAX_WINNING_ASSERTIONS],
        "rights": rights_documents[:MAX_RIGHTS_SUMMARIES],
        "conflictCount": reported_conflict_count,
        "conflictPredicates": conflict_predicates,
        "conflicts": conflict_documents[:MAX_CONFLICTS],
        "observationHistory": observation_history,
        "sourceNodeCount": int(value.get("source_node_count") or 0),
        "overflow": {
            "titles": title_overflow,
            "externalIdentifiers": identifier_overflow,
            "relationTypes": relation_overflow,
            "sourceBadges": max(0, len(source_badges) - MAX_SOURCE_BADGES),
            "winningAssertions": max(
                0,
                len(winning) - MAX_WINNING_ASSERTIONS,
            ),
            "citationKeys": citation_overflow,
            "rights": max(0, len(rights_documents) - MAX_RIGHTS_SUMMARIES),
            "conflicts": max(0, len(conflict_documents) - MAX_CONFLICTS),
            "hierarchyRelations": hierarchy_relation_overflow,
            "sportsParticipants": max(
                0,
                len(participant_keys) - MAX_SPORTS_PARTICIPANTS,
            ),
            "currentResults": current_result_overflow,
            "animationStudios": max(
                0,
                len(studio_keys) - MAX_ANIMATION_RELATIONS,
            ),
            "animationCharacters": max(
                0,
                len(character_keys) - MAX_ANIMATION_RELATIONS,
            ),
            "musicMediaObjects": max(
                0,
                len(music_media_objects) - MAX_MUSIC_MEDIA_OBJECTS,
            ),
            "musicWorkParents": max(
                0,
                len(music_work_parent_keys) - MAX_MUSIC_WORK_PARENTS,
            ),
            "musicCredits": max(
                0,
                len(music_credits) - MAX_MUSIC_CREDITS,
            ),
            "musicVariantSignals": max(
                0,
                len(music_variant_signals) - MAX_MUSIC_VARIANT_SIGNALS,
            ),
            "observationHistory": observation_overflow,
            "referenceCandidates": reference_candidate_overflow,
            "referenceRelations": reference_relation_overflow,
            **attribute_overflow,
        },
    }


def projection_schema():
    from pyspark.sql.types import (
        ArrayType,
        BooleanType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    string = StringType()
    return StructType(
        [
            StructField("entityKey", string, False),
            StructField("entityLevel", string, False),
            StructField("entityKind", string, False),
            StructField("domains", ArrayType(string, False), False),
            StructField("status", string, False),
            StructField("releasePlanId", string, False),
            StructField("contextId", string, False),
            StructField("sourcePolicyDigest", string, False),
            StructField("canonicalSubjectRoot", string, True),
            StructField(
                "typeConcepts",
                ArrayType(
                    StructType(
                        [
                            StructField("typeConceptId", string, False),
                            StructField("conceptVersion", string, False),
                            StructField("canonicalRoot", string, False),
                        ]
                    ),
                    False,
                ),
                False,
            ),
            StructField("referenceSlices", ArrayType(string, False), False),
            StructField("authorityScope", string, True),
            StructField(
                "referenceCapabilities",
                ArrayType(string, False),
                False,
            ),
            StructField(
                "referenceCandidates",
                ArrayType(
                    StructType(
                        [
                            StructField("kind", string, False),
                            StructField("assertionId", string, False),
                            StructField("predicate", string, False),
                            StructField("valueJson", string, True),
                            StructField("valueText", string, True),
                            StructField("qualifiersJson", string, True),
                            StructField("resolutionStatus", string, False),
                            StructField("sourceProductId", string, False),
                            StructField("sourceRecordId", string, False),
                            StructField("sourcePath", string, False),
                            StructField("observedAt", string, False),
                            StructField("validFrom", string, True),
                            StructField("validTo", string, True),
                            StructField(
                                "citationKeys",
                                ArrayType(string, False),
                                False,
                            ),
                            StructField(
                                "citationOverflow",
                                IntegerType(),
                                False,
                            ),
                        ]
                    ),
                    False,
                ),
                False,
            ),
            StructField(
                "referenceRelations",
                ArrayType(
                    StructType(
                        [
                            StructField("predicate", string, False),
                            StructField("objectEntityKey", string, False),
                            StructField("assertionId", string, False),
                            StructField("sourceProductId", string, False),
                            StructField("observedAt", string, False),
                            StructField(
                                "citationKeys",
                                ArrayType(string, False),
                                False,
                            ),
                            StructField(
                                "citationOverflow",
                                IntegerType(),
                                False,
                            ),
                        ]
                    ),
                    False,
                ),
                False,
            ),
            StructField("displayName", string, False),
            StructField("displayLanguage", string, False),
            StructField("contentType", string, True),
            StructField("parentEntityKey", string, True),
            StructField("seriesEntityKey", string, True),
            StructField("seasonEntityKey", string, True),
            StructField("releaseYear", IntegerType(), True),
            StructField("seasonNumber", IntegerType(), True),
            StructField("episodeNumber", IntegerType(), True),
            StructField("durationUs", LongType(), True),
            StructField("discipline", string, True),
            StructField("competitionEntityKey", string, True),
            StructField("sportsSeasonEntityKey", string, True),
            StructField("stageEntityKey", string, True),
            StructField("startAt", string, True),
            StructField("sportsStatus", string, True),
            StructField(
                "participantEntityKeys",
                ArrayType(string, False),
                False,
            ),
            StructField("venueEntityKey", string, True),
            StructField(
                "currentResults",
                ArrayType(
                    StructType(
                        [
                            StructField("predicate", string, False),
                            StructField("valueJson", string, False),
                            StructField("qualifiersJson", string, False),
                            StructField("resolutionStatus", string, False),
                        ]
                    ),
                    False,
                ),
                False,
            ),
            StructField("releaseForm", string, True),
            StructField("animationTradition", string, True),
            StructField(
                "animationTechniques",
                ArrayType(string, False),
                False,
            ),
            StructField(
                "studioEntityKeys",
                ArrayType(string, False),
                False,
            ),
            StructField(
                "characterEntityKeys",
                ArrayType(string, False),
                False,
            ),
            StructField("musicAgentKind", string, True),
            StructField("musicExactEligible", BooleanType(), True),
            StructField(
                "musicMediaObjects",
                ArrayType(string, False),
                False,
            ),
            StructField(
                "musicWorkParentEntityKeys",
                ArrayType(string, False),
                False,
            ),
            StructField(
                "musicCredits",
                ArrayType(
                    StructType(
                        [
                            StructField("entityKey", string, False),
                            StructField("role", string, False),
                            StructField("authorityProperty", string, True),
                            StructField("agentKind", string, True),
                        ]
                    ),
                    False,
                ),
                False,
            ),
            StructField(
                "musicVariantSignals",
                ArrayType(
                    StructType(
                        [
                            StructField("value", string, False),
                            StructField("authorityProperty", string, True),
                            StructField("authorityRelation", string, True),
                            StructField("direction", string, True),
                            StructField("targetId", string, True),
                            StructField("targetTitle", string, True),
                        ]
                    ),
                    False,
                ),
                False,
            ),
            StructField(
                "titles",
                ArrayType(
                    StructType(
                        [
                            StructField("value", string, False),
                            StructField("language", string, False),
                            StructField("titleRole", string, False),
                        ]
                    ),
                    False,
                ),
                False,
            ),
            StructField(
                "attributes",
                StructType(
                    [
                        StructField(
                            name,
                            ArrayType(string, False),
                            False,
                        )
                        for name in _ATTRIBUTE_PREDICATES.values()
                    ]
                ),
                False,
            ),
            StructField(
                "externalIdentifiers",
                ArrayType(
                    StructType(
                        [
                            StructField("sourceProductId", string, False),
                            StructField("namespace", string, False),
                            StructField("value", string, False),
                            StructField("issuer", string, False),
                            StructField("referentKind", string, False),
                            StructField("assertionId", string, False),
                            StructField(
                                "citationKeys",
                                ArrayType(string, False),
                                False,
                            ),
                            StructField("url", string, True),
                        ]
                    ),
                    False,
                ),
                False,
            ),
            StructField(
                "relationSummary",
                ArrayType(
                    StructType(
                        [
                            StructField("predicate", string, False),
                            StructField("count", LongType(), False),
                        ]
                    ),
                    False,
                ),
                False,
            ),
            StructField(
                "sourceBadges",
                ArrayType(
                    StructType(
                        [
                            StructField("sourceProductId", string, False),
                            StructField("displayName", string, False),
                            StructField("sourceUrl", string, False),
                            StructField(
                                "policyZones",
                                ArrayType(string, False),
                                False,
                            ),
                            StructField("assertionCount", LongType(), False),
                            StructField(
                                "winningAssertionCount",
                                LongType(),
                                False,
                            ),
                        ]
                    ),
                    False,
                ),
                False,
            ),
            StructField(
                "winningAssertions",
                ArrayType(
                    StructType(
                        [
                            StructField("kind", string, False),
                            StructField("assertionId", string, False),
                            StructField("predicate", string, False),
                            StructField("valueJson", string, False),
                            StructField("qualifiersJson", string, False),
                            StructField("resolutionStatus", string, False),
                            StructField("sourceProductId", string, False),
                            StructField("sourceRecordId", string, False),
                            StructField("sourcePath", string, False),
                            StructField("observedAt", string, False),
                            StructField(
                                "citationKeys",
                                ArrayType(string, False),
                                False,
                            ),
                            StructField(
                                "citationOverflow",
                                IntegerType(),
                                False,
                            ),
                        ]
                    ),
                    False,
                ),
                False,
            ),
            StructField(
                "rights",
                ArrayType(
                    StructType(
                        [
                            StructField("sourceProductId", string, False),
                            StructField("policyId", string, False),
                            StructField("policyZone", string, False),
                            StructField("licenseId", string, False),
                            StructField("licenseUri", string, True),
                            StructField("attributionText", string, False),
                            StructField("sourceUrl", string, False),
                            StructField(
                                "shareAlike",
                                BooleanType(),
                                False,
                            ),
                        ]
                    ),
                    False,
                ),
                False,
            ),
            StructField("conflictCount", LongType(), False),
            StructField("conflictPredicates", ArrayType(string, False), False),
            StructField(
                "conflicts",
                ArrayType(
                    StructType(
                        [
                            StructField("predicate", string, False),
                            StructField("scopeHash", string, False),
                            StructField("reason", string, False),
                            StructField(
                                "assertionIds",
                                ArrayType(string, False),
                                False,
                            ),
                            StructField(
                                "assertionOverflow",
                                IntegerType(),
                                False,
                            ),
                            StructField(
                                "candidateValuesJson",
                                ArrayType(string, False),
                                False,
                            ),
                            StructField(
                                "candidateValueOverflow",
                                IntegerType(),
                                False,
                            ),
                            StructField(
                                "sourceProductIds",
                                ArrayType(string, False),
                                False,
                            ),
                        ]
                    ),
                    False,
                ),
                False,
            ),
            StructField(
                "observationHistory",
                ArrayType(
                    StructType(
                        [
                            StructField("predicate", string, False),
                            StructField("observationKey", string, False),
                            StructField("assertionId", string, False),
                            StructField("valueJson", string, False),
                            StructField("qualifiersJson", string, False),
                            StructField("observedAt", string, False),
                            StructField("validFrom", string, True),
                            StructField("validTo", string, True),
                            StructField(
                                "supersedesAssertionId",
                                string,
                                True,
                            ),
                            StructField("isWinning", BooleanType(), False),
                            StructField("sourceProductId", string, False),
                            StructField(
                                "citationKeys",
                                ArrayType(string, False),
                                False,
                            ),
                            StructField(
                                "citationOverflow",
                                IntegerType(),
                                False,
                            ),
                        ]
                    ),
                    False,
                ),
                False,
            ),
            StructField("sourceNodeCount", LongType(), False),
            StructField(
                "overflow",
                StructType(
                    [
                        StructField("titles", IntegerType(), False),
                        StructField("externalIdentifiers", IntegerType(), False),
                        StructField("relationTypes", IntegerType(), False),
                        StructField("sourceBadges", IntegerType(), False),
                        StructField("winningAssertions", IntegerType(), False),
                        StructField("citationKeys", IntegerType(), False),
                        StructField("rights", IntegerType(), False),
                        StructField("conflicts", IntegerType(), False),
                        StructField(
                            "hierarchyRelations",
                            IntegerType(),
                            False,
                        ),
                        StructField(
                            "sportsParticipants",
                            IntegerType(),
                            False,
                        ),
                        StructField(
                            "currentResults",
                            IntegerType(),
                            False,
                        ),
                        StructField(
                            "animationStudios",
                            IntegerType(),
                            False,
                        ),
                        StructField(
                            "animationCharacters",
                            IntegerType(),
                            False,
                        ),
                        StructField(
                            "musicMediaObjects",
                            IntegerType(),
                            False,
                        ),
                        StructField(
                            "musicWorkParents",
                            IntegerType(),
                            False,
                        ),
                        StructField(
                            "musicCredits",
                            IntegerType(),
                            False,
                        ),
                        StructField(
                            "musicVariantSignals",
                            IntegerType(),
                            False,
                        ),
                        StructField(
                            "observationHistory",
                            IntegerType(),
                            False,
                        ),
                        StructField(
                            "referenceCandidates",
                            IntegerType(),
                            False,
                        ),
                        StructField(
                            "referenceRelations",
                            IntegerType(),
                            False,
                        ),
                        *[
                            StructField(name, IntegerType(), False)
                            for name in _ATTRIBUTE_PREDICATES.values()
                        ],
                    ]
                ),
                False,
            ),
        ]
    )


def _build_hierarchy_projection(
    *,
    entities: Any,
    release_entities: Any,
    selected_relations: Any,
    release_relations: Any,
) -> Any:
    """Resolve unique direct/transitive parents without collecting relation lists."""

    from pyspark.sql import functions as F

    entity_levels = release_entities.select(
        F.col("entity_key").alias("_level_entity_key"),
        F.upper(F.col("entity_level")).alias("_entity_level"),
    ).dropDuplicates(["_level_entity_key"])

    def parent_edges(relations: Any) -> Any:
        return (
            relations.where(
                F.lower(F.col("predicate")).isin(*sorted(_PARENT_RELATION_PREDICATES))
            )
            .alias("r")
            .join(
                entity_levels.alias("o"),
                F.col("r.object_entity_key") == F.col("o._level_entity_key"),
                "inner",
            )
            .select(
                F.col("r.subject_entity_key").alias("entity_key"),
                F.col("r.object_entity_key").alias("object_entity_key"),
                F.lower(F.col("r.predicate")).alias("predicate"),
                F.col("o._entity_level").alias("object_entity_level"),
            )
            .dropDuplicates(
                [
                    "entity_key",
                    "predicate",
                    "object_entity_key",
                    "object_entity_level",
                ]
            )
        )

    direct_edges = parent_edges(selected_relations)
    all_edges = parent_edges(release_relations)
    direct_series = direct_edges.where(
        (F.col("object_entity_level") == "SERIES")
        & F.col("predicate").isin(*sorted(_SERIES_RELATION_PREDICATES))
    ).select(
        "entity_key",
        F.col("object_entity_key").alias("_series_candidate"),
    )
    direct_seasons = direct_edges.where(
        (F.col("object_entity_level") == "SEASON")
        & F.col("predicate").isin(*sorted(_SEASON_RELATION_PREDICATES))
    ).select(
        "entity_key",
        F.col("object_entity_key").alias("_season_candidate"),
    )
    season_stats = direct_seasons.groupBy("entity_key").agg(
        F.countDistinct("_season_candidate").alias("_season_candidate_count"),
        F.min("_season_candidate").alias("_unique_season_entity_key"),
    )
    unique_direct_seasons = season_stats.where(
        F.col("_season_candidate_count") == 1
    ).select(
        "entity_key",
        F.col("_unique_season_entity_key").alias("_season_candidate"),
    )

    unique_season_series = (
        all_edges.alias("e")
        .join(
            entity_levels.alias("s"),
            F.col("e.entity_key") == F.col("s._level_entity_key"),
            "inner",
        )
        .where(
            (F.col("s._entity_level") == "SEASON")
            & (F.col("e.object_entity_level") == "SERIES")
            & F.col("e.predicate").isin(*sorted(_SERIES_RELATION_PREDICATES))
        )
        .groupBy(F.col("e.entity_key").alias("_season_entity_key"))
        .agg(
            F.countDistinct("e.object_entity_key").alias("_season_series_count"),
            F.min("e.object_entity_key").alias("_season_series_candidate"),
        )
        .where(F.col("_season_series_count") == 1)
    )
    transitive_series = (
        unique_direct_seasons.alias("d")
        .join(
            unique_season_series.alias("s"),
            F.col("d._season_candidate") == F.col("s._season_entity_key"),
            "inner",
        )
        .select(
            F.col("d.entity_key").alias("entity_key"),
            F.col("s._season_series_candidate").alias("_series_candidate"),
        )
    )
    series_stats = (
        direct_series.unionByName(transitive_series)
        .dropDuplicates(["entity_key", "_series_candidate"])
        .groupBy("entity_key")
        .agg(
            F.countDistinct("_series_candidate").alias("_series_candidate_count"),
            F.min("_series_candidate").alias("_unique_series_entity_key"),
        )
    )
    relation_stats = direct_edges.groupBy("entity_key").agg(
        F.countDistinct("object_entity_key").alias(
            "_hierarchy_relation_candidate_count"
        )
    )
    projected = (
        entities.select("entity_key", F.upper("entity_level").alias("_subject_level"))
        .join(series_stats, "entity_key", "left")
        .join(season_stats, "entity_key", "left")
        .join(relation_stats, "entity_key", "left")
        .fillna(
            0,
            subset=[
                "_series_candidate_count",
                "_season_candidate_count",
                "_hierarchy_relation_candidate_count",
            ],
        )
        .withColumn(
            "hierarchy_relation_overflow",
            F.greatest(
                F.col("_hierarchy_relation_candidate_count")
                - F.lit(MAX_HIERARCHY_RELATION_CANDIDATES),
                F.lit(0),
            ).cast("integer"),
        )
    )
    safe = F.col("hierarchy_relation_overflow") == 0
    unique_series = safe & (F.col("_series_candidate_count") == 1)
    unique_season = safe & (F.col("_season_candidate_count") == 1)
    no_season = F.col("_season_candidate_count") == 0
    return projected.select(
        "entity_key",
        F.when(
            (F.col("_subject_level") == "SEASON") & unique_series,
            F.col("_unique_series_entity_key"),
        )
        .when(
            (F.col("_subject_level") == "EPISODE") & unique_season,
            F.col("_unique_season_entity_key"),
        )
        .when(
            (F.col("_subject_level") == "EPISODE") & no_season & unique_series,
            F.col("_unique_series_entity_key"),
        )
        .alias("parent_entity_key"),
        F.when(
            F.col("_subject_level").isin("SEASON", "EPISODE") & unique_series,
            F.col("_unique_series_entity_key"),
        ).alias("series_entity_key"),
        F.when(
            (F.col("_subject_level") == "EPISODE") & unique_season,
            F.col("_unique_season_entity_key"),
        ).alias("season_entity_key"),
        "hierarchy_relation_overflow",
    )


def build_gold_search_projection(
    spark: Any,
    *,
    gold_tables: Mapping[str, Any],
    release_plan_id: str,
    affected_entity_keys: Any | None = None,
    source_policy_digest: str | None = None,
):
    from pyspark.sql import functions as F

    policy_digest = require_sha256(
        source_policy_digest or load_catalog_source_policy().digest,
        label="source_policy_digest",
    )

    keys = (
        None
        if affected_entity_keys is None
        else F.broadcast(
            affected_entity_keys.select(
                F.col("entity_key").alias("_affected_entity_key")
            ).dropDuplicates()
        )
    )

    def affected(frame: Any, column: str) -> Any:
        if keys is None:
            return frame
        return frame.join(
            keys,
            frame[column] == keys["_affected_entity_key"],
            "inner",
        ).drop("_affected_entity_key")

    release_entities = gold_tables["community_gold_entity"].where(
        F.col("release_plan_id") == release_plan_id
    )
    entities = affected(
        release_entities,
        "entity_key",
    )
    fields = (
        affected(
            gold_tables["community_gold_field"].where(
                F.col("release_plan_id") == release_plan_id
            ),
            "entity_key",
        )
        .groupBy("entity_key")
        .agg(
            F.sort_array(
                F.collect_set(
                    F.struct(
                        "resolution_key",
                        "predicate",
                        "value_json",
                        "qualifiers_json",
                        "resolution_status",
                        "selected_assertion_id",
                        "assertion_ids_json",
                        "trace_json",
                    )
                )
            ).alias("fields")
        )
    )
    identifiers = (
        affected(
            gold_tables["community_gold_identifier"].where(
                F.col("release_plan_id") == release_plan_id
            ),
            "entity_key",
        )
        .groupBy("entity_key")
        .agg(
            F.sort_array(
                F.collect_set(
                    F.struct(
                        "namespace_id",
                        "value",
                        "issuer",
                        "referent_kind",
                        "assertion_ids_json",
                        "trace_json",
                    )
                )
            ).alias("identifiers")
        )
    )
    release_relations = gold_tables["community_gold_relation"].where(
        F.col("release_plan_id") == release_plan_id
    )
    selected_relations = affected(release_relations, "subject_entity_key")
    relation_summary = (
        selected_relations.groupBy("subject_entity_key", "predicate")
        .count()
        .groupBy("subject_entity_key")
        .agg(
            F.sort_array(F.collect_set(F.struct("predicate", "count"))).alias(
                "relation_summary"
            )
        )
    )
    relations = (
        selected_relations.groupBy("subject_entity_key")
        .agg(
            F.sort_array(
                F.collect_set(
                    F.struct(
                        "predicate",
                        "object_entity_key",
                        "qualifiers_json",
                        "assertion_ids_json",
                        "trace_json",
                    )
                )
            ).alias("relations")
        )
        .withColumnRenamed("subject_entity_key", "_relation_subject_entity_key")
    )
    hierarchy = _build_hierarchy_projection(
        entities=entities,
        release_entities=release_entities,
        selected_relations=selected_relations,
        release_relations=release_relations,
    )
    conflicts = (
        affected(
            gold_tables["community_gold_conflict"].where(
                F.col("release_plan_id") == release_plan_id
            ),
            "entity_key",
        )
        .groupBy("entity_key")
        .agg(
            F.count(F.lit(1)).alias("conflict_count"),
            F.sort_array(
                F.collect_set(
                    F.struct(
                        "predicate",
                        "scope_hash",
                        "reason",
                        "assertion_ids_json",
                        "candidate_values_json",
                        "trace_json",
                    )
                )
            ).alias("conflicts"),
        )
    )
    observations = (
        affected(
            gold_tables["community_gold_observation"].where(
                F.col("release_plan_id") == release_plan_id
            ),
            "entity_key",
        )
        .groupBy("entity_key")
        .agg(
            F.sort_array(
                F.collect_set(
                    F.struct(
                        "observation_key",
                        "assertion_id",
                        "predicate",
                        "value_json",
                        "qualifiers_json",
                        "observed_at",
                        "valid_from",
                        "valid_to",
                        "supersedes_assertion_id",
                        "is_winning",
                        "trace_json",
                    )
                )
            ).alias("observations")
        )
    )
    joined = (
        entities.join(fields, "entity_key", "left")
        .join(identifiers, "entity_key", "left")
        .join(
            relation_summary,
            entities.entity_key == relation_summary.subject_entity_key,
            "left",
        )
        .drop("subject_entity_key")
        .join(
            relations,
            entities.entity_key == relations._relation_subject_entity_key,
            "left",
        )
        .drop("_relation_subject_entity_key")
        .join(conflicts, "entity_key", "left")
        .join(observations, "entity_key", "left")
        .join(hierarchy, "entity_key", "left")
    )
    return spark.createDataFrame(
        joined.rdd.map(
            lambda row: project_gold_entity(
                row,
                source_policy_digest=policy_digest,
            )
        ),
        schema=projection_schema(),
    )
