"""Fixed Gold v2 query grammar and concrete-index pagination cursor."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import math
import re
import time
from dataclasses import dataclass
from typing import Any

from video_media_catalog.api_search import InvalidCursor
from video_media_catalog.canonical import canonical_json_bytes
from video_media_catalog.gold_search_index import RESEARCH_INDEX_PREFIX

_ENTITY_KEY = re.compile(r"^sha256:[0-9a-f]{64}$")
_INDEX = re.compile(r"^[a-z0-9][a-z0-9_-]{0,254}$")

GOLD_SEARCH_SOURCE_FIELDS = (
    "entityKey",
    "entityLevel",
    "entityKind",
    "displayName",
    "displayLanguage",
    "releasePlanId",
    "contextId",
    "conflictCount",
    "externalIdentifiers",
    "sourceBadges",
)


def _b64encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode()


def _b64decode(value: str) -> bytes:
    if not value or re.fullmatch(r"[A-Za-z0-9_-]+", value) is None:
        raise InvalidCursor("cursor encoding is invalid")
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


@dataclass(frozen=True)
class GoldSearchParameters:
    q: str
    entity_level: str | None
    entity_kind: str | None
    language: str | None
    has_conflicts: bool | None
    page_size: int

    def fingerprint(self) -> str:
        return hashlib.sha256(
            canonical_json_bytes(
                {
                    "q": self.q,
                    "entityLevel": self.entity_level,
                    "entityKind": self.entity_kind,
                    "language": self.language,
                    "hasConflicts": self.has_conflicts,
                }
            )
        ).hexdigest()


@dataclass(frozen=True)
class GoldCursorState:
    index: str
    sort: list[Any]
    expires_at: int


class GoldCursorCodec:
    def __init__(
        self,
        secret: bytes,
        *,
        ttl_seconds: int,
        index_prefix: str = RESEARCH_INDEX_PREFIX,
        clock=time.time,
    ) -> None:
        if len(secret) < 32:
            raise ValueError("cursor secret must contain at least 32 bytes")
        if not 1 <= ttl_seconds <= 3600:
            raise ValueError("Gold cursor TTL must be between 1 and 3600 seconds")
        if _INDEX.fullmatch(index_prefix) is None:
            raise ValueError("Gold cursor index prefix is invalid")
        self.secret = secret
        self.ttl_seconds = ttl_seconds
        self.index_prefix = index_prefix + "-"
        self.clock = clock

    def encode(
        self,
        *,
        index: str,
        sort: list[Any],
        fingerprint: str,
    ) -> str:
        self._validate_index(index)
        self._validate_sort(sort)
        payload = canonical_json_bytes(
            {
                "version": 2,
                "fingerprint": fingerprint,
                "index": index,
                "sort": sort,
                "expiresAt": int(self.clock()) + self.ttl_seconds,
            }
        )
        signature = hmac.digest(self.secret, payload, "sha256")
        return f"{_b64encode(payload)}.{_b64encode(signature)}"

    def decode(self, cursor: str, *, fingerprint: str) -> GoldCursorState:
        if len(cursor) > 4096 or cursor.count(".") != 1:
            raise InvalidCursor("cursor is invalid")
        payload_value, signature_value = cursor.split(".", 1)
        try:
            payload = _b64decode(payload_value)
            signature = _b64decode(signature_value)
        except (InvalidCursor, ValueError) as exc:
            raise InvalidCursor("cursor is invalid") from exc
        if not hmac.compare_digest(
            signature,
            hmac.digest(self.secret, payload, "sha256"),
        ):
            raise InvalidCursor("cursor signature is invalid")
        try:
            value = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise InvalidCursor("cursor payload is invalid") from exc
        if (
            not isinstance(value, dict)
            or set(value)
            != {
                "version",
                "fingerprint",
                "index",
                "sort",
                "expiresAt",
            }
            or value["version"] != 2
            or value["fingerprint"] != fingerprint
            or not isinstance(value["index"], str)
            or not isinstance(value["sort"], list)
            or isinstance(value["expiresAt"], bool)
            or not isinstance(value["expiresAt"], int)
        ):
            raise InvalidCursor("cursor does not match this search")
        self._validate_index(value["index"])
        self._validate_sort(value["sort"])
        if value["expiresAt"] <= int(self.clock()):
            raise InvalidCursor("cursor has expired")
        return GoldCursorState(
            index=value["index"],
            sort=value["sort"],
            expires_at=value["expiresAt"],
        )

    def _validate_index(self, index: str) -> None:
        if _INDEX.fullmatch(index) is None or not index.startswith(self.index_prefix):
            raise InvalidCursor("cursor index is invalid")

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


def build_gold_search_query(
    parameters: GoldSearchParameters,
    *,
    search_after: list[Any] | None,
    timeout_ms: int,
) -> dict[str, Any]:
    if parameters.q:
        must = [
            {
                "bool": {
                    "minimum_should_match": 1,
                    "should": [
                        {
                            "match": {
                                "displayName": {
                                    "query": parameters.q,
                                    "operator": "and",
                                    "boost": 5,
                                }
                            }
                        },
                        {
                            "nested": {
                                "path": "titles",
                                "score_mode": "max",
                                "query": {
                                    "match": {
                                        "titles.value": {
                                            "query": parameters.q,
                                            "operator": "and",
                                        }
                                    }
                                },
                            }
                        },
                    ],
                }
            }
        ]
    else:
        must = [{"match_all": {}}]
    filters: list[dict[str, Any]] = []
    if parameters.entity_level is not None:
        filters.append({"term": {"entityLevel": parameters.entity_level}})
    if parameters.entity_kind is not None:
        filters.append({"term": {"entityKind": parameters.entity_kind}})
    if parameters.language is not None:
        filters.append(
            {
                "bool": {
                    "minimum_should_match": 1,
                    "should": [
                        {"term": {"displayLanguage": parameters.language}},
                        {
                            "nested": {
                                "path": "titles",
                                "query": {
                                    "term": {"titles.language": parameters.language}
                                },
                            }
                        },
                    ],
                }
            }
        )
    if parameters.has_conflicts is True:
        filters.append({"range": {"conflictCount": {"gt": 0}}})
    elif parameters.has_conflicts is False:
        filters.append({"term": {"conflictCount": 0}})
    body: dict[str, Any] = {
        "size": parameters.page_size,
        "track_total_hits": 10_000,
        "track_scores": True,
        "timeout": f"{timeout_ms}ms",
        "_source": list(GOLD_SEARCH_SOURCE_FIELDS),
        "query": {"bool": {"must": must, "filter": filters}},
        "sort": [
            {"_score": {"order": "desc"}},
            {"entityKey": {"order": "asc"}},
        ],
    }
    if search_after is not None:
        body["search_after"] = search_after
    return body


def build_gold_external_identifier_query(
    *,
    namespace: str,
    value: str,
    timeout_ms: int,
) -> dict[str, Any]:
    return {
        "size": 2,
        "track_total_hits": True,
        "timeout": f"{timeout_ms}ms",
        "query": {
            "bool": {
                "filter": [
                    {
                        "nested": {
                            "path": "externalIdentifiers",
                            "score_mode": "none",
                            "query": {
                                "bool": {
                                    "filter": [
                                        {
                                            "term": {
                                                "externalIdentifiers.namespace": (
                                                    namespace
                                                )
                                            }
                                        },
                                        {"term": {"externalIdentifiers.value": value}},
                                    ]
                                }
                            },
                        }
                    },
                ]
            }
        },
        "sort": [{"entityKey": {"order": "asc"}}],
    }
