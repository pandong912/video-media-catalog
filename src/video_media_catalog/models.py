"""Shared immutable object-reference contracts."""

from __future__ import annotations

import re
from typing import Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_serializer,
    field_validator,
)

from video_media_catalog.canonical import canonical_json_bytes


def _to_camel(value: str) -> str:
    first, *rest = value.split("_")
    return first + "".join(part.capitalize() for part in rest)


class ContractModel(BaseModel):
    """Accept additive fields and emit lower-camel JSON names."""

    model_config = ConfigDict(
        alias_generator=_to_camel,
        extra="allow",
        populate_by_name=True,
    )

    def json_bytes(self) -> bytes:
        return canonical_json_bytes(
            self.model_dump(mode="json", by_alias=True, exclude_none=True),
            newline=True,
        )


class Checksum(ContractModel):
    algorithm: Literal["CHECKSUM_ALGORITHM_SHA256"] = "CHECKSUM_ALGORITHM_SHA256"
    encoding: Literal["CHECKSUM_ENCODING_HEX"] = "CHECKSUM_ENCODING_HEX"
    value: str

    @field_validator("value")
    @classmethod
    def validate_value(cls, value: str) -> str:
        normalized = value.removeprefix("sha256:").lower()
        if re.fullmatch(r"[0-9a-f]{64}", normalized) is None:
            raise ValueError("checksum must be a SHA-256 hex digest")
        return normalized


class ObjectRef(ContractModel):
    uri: str
    format: Literal[
        "OBJECT_FORMAT_JSON",
        "OBJECT_FORMAT_PARQUET",
        "OBJECT_FORMAT_OTHER",
    ]
    media_type: str
    checksum: Checksum
    size_bytes: int = Field(ge=0)
    etag: str | None = None
    object_version: str | None = None
    created_at: str | None = None
    attributes: dict[str, str] = Field(
        default_factory=dict,
        exclude_if=lambda value: not value,
    )

    @field_serializer("size_bytes", when_used="json")
    def serialize_size(self, value: int) -> str:
        return str(value)
