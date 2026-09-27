"""Shared registry projection for exact external-identifier blocking."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from video_media_catalog.identity_v2 import EntityLevel
from video_media_catalog.source_registry import (
    RegistryStatus,
    SourceRegistrySnapshot,
)

_IDENTITY_ENTITY_TYPE_MAP = {
    "WORK": (EntityLevel.EDITORIAL_WORK, "EDITORIAL_WORK", "EDITORIAL_WORK"),
    "MOVIE": (EntityLevel.EDITORIAL_WORK, "MOVIE", "EDITORIAL_WORK"),
    "EDITORIAL_WORK": (
        EntityLevel.EDITORIAL_WORK,
        "EDITORIAL_WORK",
        "EDITORIAL_WORK",
    ),
    "SERIES": (EntityLevel.SERIES, "TV_SERIES", "SERIES"),
    "TV_SERIES": (EntityLevel.SERIES, "TV_SERIES", "SERIES"),
    "SEASON": (EntityLevel.SEASON, "TV_SEASON", "SEASON"),
    "TV_SEASON": (EntityLevel.SEASON, "TV_SEASON", "SEASON"),
    "EPISODE": (EntityLevel.EPISODE, "TV_EPISODE", "EPISODE"),
    "TV_EPISODE": (EntityLevel.EPISODE, "TV_EPISODE", "EPISODE"),
    "EDIT": (EntityLevel.EDIT, "EDIT", "EDIT"),
    "MANIFESTATION": (
        EntityLevel.MANIFESTATION,
        "MANIFESTATION",
        "MANIFESTATION",
    ),
    "PERSON": (EntityLevel.AGENT, "PERSON", "AGENT"),
    "AGENT": (EntityLevel.AGENT, "AGENT", "AGENT"),
    "ORGANIZATION": (EntityLevel.AGENT, "ORGANIZATION", "ORGANIZATION"),
}

_REFERENT_KIND_ALIASES = {
    entity_type: projection[2]
    for entity_type, projection in _IDENTITY_ENTITY_TYPE_MAP.items()
}

_EDITORIAL_BLOCKING_KINDS = frozenset(
    {
        "EDITORIAL_WORK",
        "MOVIE",
        "SERIES",
        "SEASON",
        "EPISODE",
        "EDIT",
        "MANIFESTATION",
    }
)
_AGENT_BLOCKING_KINDS = frozenset({"PERSON", "AGENT", "ORGANIZATION"})

_ENTITY_LEVEL_BY_BLOCKING_KIND = {
    "EDITORIAL_WORK": EntityLevel.EDITORIAL_WORK,
    "SERIES": EntityLevel.SERIES,
    "SEASON": EntityLevel.SEASON,
    "EPISODE": EntityLevel.EPISODE,
    "EDIT": EntityLevel.EDIT,
    "MANIFESTATION": EntityLevel.MANIFESTATION,
    "AGENT": EntityLevel.AGENT,
    "ORGANIZATION": EntityLevel.AGENT,
}

_LOWERCASE_DISPLAY_NAMESPACES = frozenset(
    {
        "imdb-title",
        "imdb-name",
        "imdb-company",
    }
)


def canonical_referent_kind(value: str) -> str:
    """Normalize source-specific kinds to exact-ID blocking domains."""

    normalized = value.strip().upper()
    return _REFERENT_KIND_ALIASES.get(normalized, normalized)


def referent_kind_for_entity_type(entity_type: str) -> str:
    """Derive a registry blocking referent kind from a resolved entity type."""

    return canonical_referent_kind(entity_type)


def identity_entity_type_map() -> dict[str, tuple[EntityLevel, str, str]]:
    """Return the exact Identity source-type projection used for blocking."""

    return dict(_IDENTITY_ENTITY_TYPE_MAP)


def referent_kinds_compatible(identifier_kind: str, blocking_kind: str) -> bool:
    """Allow normalization within one domain, never across editorial/agent domains."""

    identifier = canonical_referent_kind(identifier_kind)
    blocking = canonical_referent_kind(blocking_kind)
    if identifier == blocking:
        return True
    editorial = (
        identifier in _EDITORIAL_BLOCKING_KINDS
        and blocking in _EDITORIAL_BLOCKING_KINDS
    )
    agent = identifier in _AGENT_BLOCKING_KINDS and blocking in _AGENT_BLOCKING_KINDS
    return editorial or agent


def blocking_referent_kind_for_entity(
    entity_level: EntityLevel | str,
    entity_kind: str,
) -> str | None:
    """Return the exact registry slot only for a level/kind-consistent entity."""

    try:
        level = (
            entity_level
            if isinstance(entity_level, EntityLevel)
            else EntityLevel(str(entity_level).strip().upper())
        )
    except ValueError:
        return None
    blocking_kind = canonical_referent_kind(entity_kind)
    expected_level = _ENTITY_LEVEL_BY_BLOCKING_KIND.get(blocking_kind)
    if expected_level is None or expected_level != level:
        return None
    return blocking_kind


def exact_id_namespace_rows(
    registry: SourceRegistrySnapshot,
) -> tuple[dict[str, object], ...]:
    """Project active registry namespaces into exact-blocking join metadata."""

    active_systems = {
        system.source_system_id
        for system in registry.source_systems
        if system.status == RegistryStatus.ACTIVE
    }
    active_products = {
        product.source_product_id
        for product in registry.source_products
        if product.status == RegistryStatus.ACTIVE
        and product.source_system_id in active_systems
    }
    rows: dict[tuple[str, str, str], dict[str, object]] = {}
    for namespace in registry.source_namespaces:
        if namespace.source_product_id not in active_products:
            continue
        pattern = namespace.identifier_pattern
        if pattern is not None:
            pattern = (
                f"^(?:{pattern})$"
                if namespace.case_sensitive
                else f"(?i)^(?:{pattern})$"
            )
        for scheme in namespace.matching_schemes:
            for referent_kind in namespace.referent_kinds:
                canonical_kind = canonical_referent_kind(referent_kind)
                key = (namespace.namespace_id, scheme, canonical_kind)
                rows[key] = {
                    "namespace_id": namespace.namespace_id,
                    "scheme": scheme,
                    "referent_kind": canonical_kind,
                    "issuer": namespace.issuer,
                    "case_sensitive": namespace.case_sensitive,
                    "match_pattern": pattern,
                }
    return tuple(rows[key] for key in sorted(rows))


def normalize_identifier_value(value: str, *, case_sensitive: bool) -> str:
    """Normalize one registry-bound value for exact blocking."""

    candidate = value.strip()
    return candidate if case_sensitive else candidate.upper()


def identifier_display_value(
    namespace_id: str,
    normalized_value: str,
    *,
    case_sensitive: bool,
) -> str:
    """Choose a deterministic external display spelling for a blocking value."""

    if case_sensitive:
        return normalized_value
    if namespace_id in _LOWERCASE_DISPLAY_NAMESPACES:
        return normalized_value.lower()
    return normalized_value


@dataclass(frozen=True, order=True)
class IdentifierBlockingProjection:
    namespace_id: str
    normalized_value: str
    display_value: str
    issuer: str
    blocking_referent_kind: str


def project_identifier_blocking(
    *,
    namespace_rows: tuple[dict[str, object], ...],
    namespace_id: str,
    value: str,
    assertion_referent_kind: str,
    entity_level: EntityLevel | str,
    entity_kind: str,
) -> IdentifierBlockingProjection | None:
    """Bind an assertion to exactly one registry namespace/entity blocking slot."""

    scheme = namespace_id.strip().lower()
    candidate = value.strip()
    blocking_kind = blocking_referent_kind_for_entity(entity_level, entity_kind)
    if (
        not scheme
        or not candidate
        or blocking_kind is None
        or not referent_kinds_compatible(assertion_referent_kind, blocking_kind)
    ):
        return None

    projections: set[IdentifierBlockingProjection] = set()
    for row in namespace_rows:
        if row["scheme"] != scheme or row["referent_kind"] != blocking_kind:
            continue
        pattern = row["match_pattern"]
        if pattern is not None and re.fullmatch(str(pattern), candidate) is None:
            continue
        case_sensitive = bool(row["case_sensitive"])
        normalized_value = normalize_identifier_value(
            candidate,
            case_sensitive=case_sensitive,
        )
        canonical_namespace = str(row["namespace_id"])
        projections.add(
            IdentifierBlockingProjection(
                namespace_id=canonical_namespace,
                normalized_value=normalized_value,
                display_value=identifier_display_value(
                    canonical_namespace,
                    normalized_value,
                    case_sensitive=case_sensitive,
                ),
                issuer=str(row["issuer"]),
                blocking_referent_kind=blocking_kind,
            )
        )
    if len(projections) != 1:
        return None
    return next(iter(projections))


def projection_trace(projection: IdentifierBlockingProjection) -> dict[str, Any]:
    """Return the bounded audit fields shared by identifier/conflict traces."""

    return {
        "namespaceId": projection.namespace_id,
        "normalizedValue": projection.normalized_value,
        "blockingReferentKind": projection.blocking_referent_kind,
    }
