"""Bounded OpenSearch query construction and signed pagination cursors."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import math
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from video_media_catalog.canonical import canonical_json_bytes

_ENTITY_KEY = re.compile(r"^sha256:[0-9a-f]{64}$")
_DESCRIPTION_LANGUAGE_ORDER = ("zh-hans", "zh", "en", "mul")
SEARCH_SOURCE_FIELDS = (
    "entityKey",
    "entityType",
    "displayName",
    "displayLanguage",
    "descriptions",
    "externalIdentifiers",
)


class InvalidCursor(ValueError):
    """A search-after cursor is malformed, changed, or used for another query."""


def _b64encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _b64decode(value: str) -> bytes:
    if not value or re.fullmatch(r"[A-Za-z0-9_-]+", value) is None:
        raise InvalidCursor("cursor encoding is invalid")
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


@dataclass(frozen=True)
class SearchParameters:
    q: str
    entity_type: str | None
    language: str | None
    page_size: int

    def fingerprint(self) -> str:
        payload = {
            "q": self.q,
            "entityType": self.entity_type,
            "language": self.language,
        }
        return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


class CursorCodec:
    def __init__(self, secret: bytes) -> None:
        if len(secret) < 32:
            raise ValueError("cursor signing secret must contain at least 32 bytes")
        self.secret = secret

    def encode(self, sort: list[Any], *, fingerprint: str) -> str:
        self._validate_sort(sort)
        payload = canonical_json_bytes(
            {"version": 1, "fingerprint": fingerprint, "sort": sort}
        )
        signature = hmac.digest(self.secret, payload, "sha256")
        return f"{_b64encode(payload)}.{_b64encode(signature)}"

    def decode(self, cursor: str, *, fingerprint: str) -> list[Any]:
        if len(cursor) > 4096 or cursor.count(".") != 1:
            raise InvalidCursor("cursor is invalid")
        encoded_payload, encoded_signature = cursor.split(".", 1)
        try:
            payload = _b64decode(encoded_payload)
            signature = _b64decode(encoded_signature)
        except (InvalidCursor, ValueError) as exc:
            raise InvalidCursor("cursor is invalid") from exc
        expected = hmac.digest(self.secret, payload, "sha256")
        if not hmac.compare_digest(signature, expected):
            raise InvalidCursor("cursor signature is invalid")
        try:
            value = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise InvalidCursor("cursor payload is invalid") from exc
        if (
            not isinstance(value, dict)
            or set(value) != {"version", "fingerprint", "sort"}
            or value["version"] != 1
            or value["fingerprint"] != fingerprint
            or not isinstance(value["sort"], list)
        ):
            raise InvalidCursor("cursor does not match this search")
        self._validate_sort(value["sort"])
        return value["sort"]

    @staticmethod
    def _validate_sort(sort: list[Any]) -> None:
        if len(sort) != 2:
            raise InvalidCursor("cursor sort tuple is invalid")
        score, entity_key = sort
        if (
            isinstance(score, bool)
            or not isinstance(score, int | float)
            or not math.isfinite(score)
            or not isinstance(entity_key, str)
            or _ENTITY_KEY.fullmatch(entity_key) is None
        ):
            raise InvalidCursor("cursor sort tuple is invalid")


def build_search_query(
    parameters: SearchParameters,
    *,
    search_after: list[Any] | None,
    timeout_ms: int,
) -> dict[str, Any]:
    """Construct only the fixed query grammar exposed by the HTTP API."""

    must: list[dict[str, Any]]
    if parameters.q:
        text_queries: list[dict[str, Any]] = [
            {
                "multi_match": {
                    "query": parameters.q,
                    "fields": ["displayName^5", "canonicalSourceId^2"],
                    "type": "best_fields",
                    "operator": "and",
                }
            },
            {
                "nested": {
                    "path": "names",
                    "score_mode": "max",
                    "query": {
                        "match": {
                            "names.value": {
                                "query": parameters.q,
                                "operator": "and",
                            }
                        }
                    },
                }
            },
            {
                "nested": {
                    "path": "descriptions",
                    "score_mode": "max",
                    "query": {
                        "match": {
                            "descriptions.value": {
                                "query": parameters.q,
                                "operator": "and",
                            }
                        }
                    },
                }
            },
            {
                "nested": {
                    "path": "sitelinks",
                    "score_mode": "max",
                    "query": {
                        "match": {
                            "sitelinks.title": {
                                "query": parameters.q,
                                "operator": "and",
                            }
                        }
                    },
                }
            },
        ]
        must = [{"bool": {"should": text_queries, "minimum_should_match": 1}}]
    else:
        must = [{"match_all": {}}]
    filters: list[dict[str, Any]] = []
    if parameters.entity_type is not None:
        filters.append({"term": {"entityType": parameters.entity_type}})
    if parameters.language is not None:
        filters.append(
            {
                "bool": {
                    "minimum_should_match": 1,
                    "should": [
                        {"term": {"displayLanguage": parameters.language}},
                        {
                            "nested": {
                                "path": "names",
                                "query": {
                                    "term": {"names.language": parameters.language}
                                },
                            }
                        },
                        {
                            "nested": {
                                "path": "descriptions",
                                "query": {
                                    "term": {
                                        "descriptions.language": parameters.language
                                    }
                                },
                            }
                        },
                    ],
                }
            }
        )
    body: dict[str, Any] = {
        "size": parameters.page_size,
        "track_total_hits": 10_000,
        "track_scores": True,
        "timeout": f"{timeout_ms}ms",
        "_source": list(SEARCH_SOURCE_FIELDS),
        "query": {
            "bool": {
                "must": must,
                "filter": filters,
            }
        },
        "sort": [
            {"_score": {"order": "desc"}},
            {"entityKey": {"order": "asc"}},
        ],
    }
    if search_after is not None:
        body["search_after"] = search_after
    return body


def build_external_identifier_query(
    *,
    scheme: str,
    value: str,
    timeout_ms: int,
) -> dict[str, Any]:
    return {
        "size": 2,
        "track_total_hits": True,
        "timeout": f"{timeout_ms}ms",
        "query": {
            "nested": {
                "path": "externalIdentifiers",
                "score_mode": "none",
                "query": {
                    "bool": {
                        "filter": [
                            {"term": {"externalIdentifiers.scheme": scheme}},
                            {"term": {"externalIdentifiers.value": value}},
                        ]
                    }
                },
            }
        },
        "sort": [{"entityKey": {"order": "asc"}}],
    }


def select_description(
    descriptions: Iterable[Mapping[str, Any]],
    *,
    requested_language: str | None,
    display_language: str | None,
) -> str | None:
    """Select a description using the UI's fixed language fallback."""

    normalized = []
    for description in descriptions:
        language = str(description.get("language") or "und").lower().replace("_", "-")
        value = description.get("value")
        if isinstance(value, str) and value:
            normalized.append((language, value))
    priorities = [
        language
        for language in (
            requested_language,
            display_language,
            *_DESCRIPTION_LANGUAGE_ORDER,
        )
        if language
    ]
    for language in dict.fromkeys(
        value.lower().replace("_", "-") for value in priorities
    ):
        for candidate_language, value in normalized:
            if candidate_language == language:
                return value
    return normalized[0][1] if normalized else None
