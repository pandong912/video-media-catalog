"""Shared validation primitives for supplier-neutral v2 contracts."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict

from video_media_catalog.canonical import canonical_json_bytes, sha256_digest

_SLUG = re.compile(r"^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$")
_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
_CONTROL_CHARACTER = re.compile(r"[\x00-\x1f\x7f]")


def to_camel(value: str) -> str:
    first, *rest = value.split("_")
    return first + "".join(part.capitalize() for part in rest)


class V2ContractModel(BaseModel):
    """Strict lower-camel JSON model used by all v2 boundaries."""

    model_config = ConfigDict(
        alias_generator=to_camel,
        extra="forbid",
        frozen=True,
        populate_by_name=True,
    )

    def json_bytes(self) -> bytes:
        return canonical_json_bytes(
            self.model_dump(mode="json", by_alias=True, exclude_none=True),
            newline=True,
        )


def require_slug(value: str, *, label: str, max_length: int = 128) -> str:
    normalized = value.strip().lower()
    if len(normalized) > max_length or _SLUG.fullmatch(normalized) is None:
        raise ValueError(f"{label} must be a stable lowercase slug")
    return normalized


def require_sha256(value: str, *, label: str = "digest") -> str:
    normalized = value.strip().lower()
    if _SHA256.fullmatch(normalized) is None:
        raise ValueError(f"{label} must use sha256:<64 lowercase hex>")
    return normalized


def require_oidc_subject(value: str, *, label: str = "OIDC subject") -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > 512
        or _CONTROL_CHARACTER.search(value) is not None
    ):
        raise ValueError(f"{label} must be a non-empty bounded exact value")
    return value


def parse_rfc3339(value: str, *, label: str = "timestamp") -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{label} must be an RFC3339 timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{label} must include a timezone")
    return parsed.astimezone(UTC)


def require_rfc3339(value: str, *, label: str = "timestamp") -> str:
    return parse_rfc3339(value, label=label).isoformat().replace("+00:00", "Z")


def require_https_url(value: str, *, label: str = "URL") -> str:
    parsed = urlsplit(value)
    try:
        _ = parsed.port
    except ValueError as exc:
        raise ValueError(f"{label} is not a valid HTTPS URL") from exc
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        raise ValueError(f"{label} must be HTTPS without credentials or fragment")
    return value


def digest_identity(value: Any) -> str:
    return sha256_digest(canonical_json_bytes(value))
