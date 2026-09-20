"""Wikidata-sourced Douban identifiers and deterministic outbound URLs."""

from __future__ import annotations

import re

from video_media_catalog.constants import (
    DOUBAN_LEGACY_NAMESPACE_ID,
    DOUBAN_LEGACY_SCHEME,
    DOUBAN_PERSON_NAMESPACE_ID,
    DOUBAN_WORK_NAMESPACE_ID,
)

DOUBAN_IDENTIFIER_PATTERN = r"[1-9][0-9]{0,31}"

_DOUBAN_IDENTIFIER = re.compile(DOUBAN_IDENTIFIER_PATTERN)
_WORK_REFERENT_KINDS = frozenset(
    {
        "WORK",
        "MOVIE",
        "EDITORIAL_WORK",
        "SERIES",
        "TV_SERIES",
        "SEASON",
        "TV_SEASON",
        "EPISODE",
        "TV_EPISODE",
        "EDIT",
        "MANIFESTATION",
    }
)
_PERSON_REFERENT_KINDS = frozenset({"AGENT", "PERSON"})
_LEGACY_NAMESPACES = frozenset(
    {
        DOUBAN_LEGACY_NAMESPACE_ID,
        DOUBAN_LEGACY_SCHEME,
    }
)


def normalize_douban_id(value: str) -> str:
    """Return one bounded positive decimal ID or reject it."""

    if not isinstance(value, str):
        raise ValueError("Douban ID must be a string")
    normalized = value.strip()
    if _DOUBAN_IDENTIFIER.fullmatch(normalized) is None:
        raise ValueError(f"invalid Douban ID: {value!r}")
    return normalized


def douban_jump_url(
    namespace_id: str,
    value: str,
    referent_kind: str,
) -> str | None:
    """Build a safe link only for a known namespace, kind, and numeric ID."""

    if not all(isinstance(item, str) for item in (namespace_id, value, referent_kind)):
        return None
    namespace = namespace_id.strip().lower()
    kind = referent_kind.strip().upper()
    if namespace == DOUBAN_WORK_NAMESPACE_ID:
        path = "subject" if kind in _WORK_REFERENT_KINDS else None
    elif namespace == DOUBAN_PERSON_NAMESPACE_ID:
        path = "celebrity" if kind in _PERSON_REFERENT_KINDS else None
    elif namespace in _LEGACY_NAMESPACES:
        if kind in _WORK_REFERENT_KINDS:
            path = "subject"
        elif kind in _PERSON_REFERENT_KINDS:
            path = "celebrity"
        else:
            path = None
    else:
        path = None
    if path is None:
        return None
    try:
        identifier = normalize_douban_id(value)
    except ValueError:
        return None
    return f"https://movie.douban.com/{path}/{identifier}/"
