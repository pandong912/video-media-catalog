"""Deterministic attribution manifests for open and community releases."""

from __future__ import annotations

from typing import Any, Self

from pydantic import Field, ValidationInfo, field_validator, model_validator

from video_media_catalog.canonical import deterministic_key
from video_media_catalog.v2_contracts import (
    V2ContractModel,
    require_https_url,
    require_rfc3339,
    require_sha256,
    require_slug,
)

_ZERO_DIGEST = "sha256:" + ("0" * 64)


class AttributionEntry(V2ContractModel):
    source_product_id: str
    policy_id: str
    attribution_text: str
    license_id: str
    license_uri: str | None = None
    source_url: str
    share_alike: bool = False
    claim_count: int = Field(default=0, ge=0)
    asset_count: int = Field(default=0, ge=0)

    @field_validator("source_product_id", "policy_id")
    @classmethod
    def validate_ids(cls, value: str) -> str:
        return require_slug(value, label="attribution reference")

    @field_validator("license_uri", "source_url")
    @classmethod
    def validate_urls(cls, value: str | None) -> str | None:
        return None if value is None else require_https_url(value)

    @field_validator("attribution_text", "license_id")
    @classmethod
    def validate_text(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized or len(normalized) > 2000:
            raise ValueError("attribution text fields must be non-empty")
        return normalized

    @model_validator(mode="after")
    def validate_counts(self) -> Self:
        if self.claim_count == 0 and self.asset_count == 0:
            raise ValueError("attribution entry must cover claims or assets")
        return self


class AttributionManifest(V2ContractModel):
    schema_version: str = "2.0"
    manifest_id: str
    release_id: str
    entries: tuple[AttributionEntry, ...]
    created_at: str

    @field_validator("manifest_id", "release_id")
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("entries")
    @classmethod
    def sort_entries(
        cls, value: tuple[AttributionEntry, ...]
    ) -> tuple[AttributionEntry, ...]:
        return tuple(
            sorted(
                value,
                key=lambda item: (item.source_product_id, item.policy_id),
            )
        )

    @field_validator("created_at")
    @classmethod
    def validate_created_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @model_validator(mode="after")
    def validate_manifest(self, info: ValidationInfo) -> Self:
        if not self.entries:
            raise ValueError("attribution manifest requires entries")
        keys = [(entry.source_product_id, entry.policy_id) for entry in self.entries]
        if len(keys) != len(set(keys)):
            raise ValueError("attribution manifest contains duplicate entries")
        if not (info.context or {}).get("skip_identity"):
            expected = deterministic_key(
                "attribution-manifest-v2",
                _manifest_identity(self),
            )
            if self.manifest_id != expected:
                raise ValueError("manifest_id does not match attribution identity")
        return self


def _manifest_identity(manifest: AttributionManifest) -> dict[str, Any]:
    return {
        "schemaVersion": manifest.schema_version,
        "releaseId": manifest.release_id,
        "entries": [
            entry.model_dump(mode="json", by_alias=True, exclude_none=True)
            for entry in manifest.entries
        ],
        "createdAt": manifest.created_at,
    }


def build_attribution_manifest(**values: Any) -> AttributionManifest:
    provisional = AttributionManifest.model_validate(
        {**values, "manifest_id": _ZERO_DIGEST},
        context={"skip_identity": True},
    )
    normalized = provisional.model_dump(mode="python")
    normalized["manifest_id"] = deterministic_key(
        "attribution-manifest-v2",
        _manifest_identity(provisional),
    )
    return AttributionManifest.model_validate(normalized)
