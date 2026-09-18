"""Control-plane identity validation and stable UUIDv7 generation."""

from __future__ import annotations

import hashlib
import uuid
from datetime import UTC, datetime
from typing import Any

from video_media_catalog.canonical import canonical_json


def is_canonical_uuid7(value: str) -> bool:
    try:
        parsed = uuid.UUID(value)
    except (AttributeError, ValueError):
        return False
    return (
        parsed.version == 7 and parsed.variant == uuid.RFC_4122 and str(parsed) == value
    )


def require_canonical_uuid7(value: str) -> str:
    if not is_canonical_uuid7(value):
        raise ValueError("identity must be a canonical lowercase UUIDv7")
    return value


def stable_uuid7(
    *,
    kind: str,
    run_id: str,
    identity: dict[str, Any],
) -> str:
    """Use the run UUIDv7 timestamp and SHA-256 identity-derived random bits."""

    require_canonical_uuid7(run_id)
    run_uuid = uuid.UUID(run_id)
    timestamp_ms = run_uuid.int >> 80
    payload = canonical_json({"kind": kind, "identity": identity}).encode("utf-8")
    random_bits = int.from_bytes(hashlib.sha256(payload).digest()[:10], "big")
    random_bits &= (1 << 74) - 1
    random_a = (random_bits >> 62) & 0xFFF
    random_b = random_bits & ((1 << 62) - 1)
    value = (
        (timestamp_ms << 80) | (0x7 << 76) | (random_a << 64) | (0b10 << 62) | random_b
    )
    result = str(uuid.UUID(int=value))
    assert is_canonical_uuid7(result)
    return result


def uuid7_timestamp_iso(value: str) -> str:
    require_canonical_uuid7(value)
    timestamp_ms = uuid.UUID(value).int >> 80
    return (
        datetime.fromtimestamp(
            timestamp_ms / 1000,
            tz=UTC,
        )
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )
