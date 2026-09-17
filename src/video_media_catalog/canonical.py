"""Canonical JSON and deterministic identity helpers."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any


def _normalize(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("canonical JSON does not allow non-finite floats")
        return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Enum):
        return _normalize(value.value)
    if isinstance(value, Mapping):
        return {
            str(key): _normalize(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, set | frozenset):
        normalized = [_normalize(item) for item in value]
        return sorted(normalized, key=canonical_json)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_normalize(item) for item in value]
    if hasattr(value, "model_dump"):
        return _normalize(value.model_dump(mode="json", by_alias=True))
    raise TypeError(f"unsupported canonical JSON value: {type(value).__name__}")


def canonical_json(value: Any) -> str:
    """Serialize a value with stable ordering and no insignificant whitespace."""

    return json.dumps(
        _normalize(value),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    )


def canonical_json_bytes(value: Any, *, newline: bool = False) -> bytes:
    payload = canonical_json(value).encode("utf-8")
    return payload + (b"\n" if newline else b"")


def sha256_digest(value: bytes | str) -> str:
    payload = value.encode("utf-8") if isinstance(value, str) else value
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def source_hash(payload: Any) -> str:
    return sha256_digest(canonical_json_bytes(payload))


def deterministic_key(kind: str, identity: Mapping[str, Any]) -> str:
    """Build a domain-separated deterministic key."""

    return source_hash({"kind": kind, "identity": identity})
