"""Policy-specific Gold release contracts for the community catalog v2."""

from __future__ import annotations

import re
from typing import Any, Self

from pydantic import Field, ValidationInfo, field_validator, model_validator

from video_media_catalog.canonical import deterministic_key
from video_media_catalog.models import ObjectRef
from video_media_catalog.rights import PolicyZone
from video_media_catalog.v2_contracts import (
    V2ContractModel,
    require_rfc3339,
    require_sha256,
    require_slug,
)

_TABLE_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)+$")
_ZERO_DIGEST = "sha256:" + ("0" * 64)


class ReleasePolicyContext(V2ContractModel):
    context_id: str
    audience: str
    purpose: str
    territories: tuple[str, ...] = ("*",)
    as_of: str
    allowed_zones: tuple[PolicyZone, ...]

    @field_validator("context_id")
    @classmethod
    def validate_context_id(cls, value: str) -> str:
        return require_slug(value, label="context_id")

    @field_validator("audience", "purpose")
    @classmethod
    def validate_text(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized or len(normalized) > 128:
            raise ValueError("release policy context fields must be non-empty")
        return normalized

    @field_validator("territories")
    @classmethod
    def normalize_territories(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(sorted({item.strip().upper() for item in value if item}))
        if not normalized:
            raise ValueError("release context requires territories")
        return normalized

    @field_validator("allowed_zones")
    @classmethod
    def normalize_zones(cls, value: tuple[PolicyZone, ...]) -> tuple[PolicyZone, ...]:
        normalized = tuple(sorted(set(value), key=str))
        if not normalized:
            raise ValueError("release context requires allowed_zones")
        if PolicyZone.QUARANTINE in normalized:
            raise ValueError("quarantine cannot be published")
        return normalized

    @field_validator("as_of")
    @classmethod
    def validate_as_of(cls, value: str) -> str:
        return require_rfc3339(value)


class ReleaseInput(V2ContractModel):
    source_product_id: str
    batch_id: str
    ingest_run_id: str
    silver_snapshot_id: int = Field(gt=0)
    watermark: str | None = None

    @field_validator("source_product_id")
    @classmethod
    def validate_product_id(cls, value: str) -> str:
        return require_slug(value, label="source_product_id")

    @field_validator("batch_id", "ingest_run_id")
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("watermark")
    @classmethod
    def validate_watermark(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized or len(normalized) > 1024:
            raise ValueError("watermark must be non-empty and bounded")
        return normalized


class GoldTableSnapshot(V2ContractModel):
    table_name: str
    snapshot_id: int = Field(gt=0)
    parent_snapshot_id: int | None = Field(default=None, gt=0)
    committed_at: str
    operation: str
    affected_record_count: int = Field(ge=0)
    total_record_count: int = Field(ge=0)
    schema_id: int = Field(ge=0)

    @field_validator("table_name")
    @classmethod
    def validate_table_name(cls, value: str) -> str:
        if _TABLE_NAME.fullmatch(value) is None:
            raise ValueError("table_name must be a fully qualified safe identifier")
        return value

    @field_validator("committed_at")
    @classmethod
    def validate_committed_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @field_validator("operation")
    @classmethod
    def validate_operation(cls, value: str) -> str:
        normalized = value.strip().lower()
        if not normalized or len(normalized) > 64:
            raise ValueError("table operation must be non-empty")
        return normalized

    @model_validator(mode="after")
    def validate_counts(self) -> Self:
        if self.affected_record_count > self.total_record_count:
            raise ValueError("affected record count exceeds snapshot total")
        return self


class CatalogReleaseManifest(V2ContractModel):
    schema_version: str = "2.0"
    release_id: str
    previous_release_id: str | None = None
    contract_id: str
    contract_version: str
    contract_digest: str
    policy_context: ReleasePolicyContext
    inputs: tuple[ReleaseInput, ...]
    identity_policy_digest: str
    field_policy_digest: str
    rights_registry_digest: str
    tables: tuple[GoldTableSnapshot, ...]
    quality_reports: tuple[ObjectRef, ...]
    attribution_manifest: ObjectRef | None = None
    created_at: str
    metrics: dict[str, str] = Field(default_factory=dict)

    @field_validator(
        "release_id",
        "previous_release_id",
        "contract_digest",
        "identity_policy_digest",
        "field_policy_digest",
        "rights_registry_digest",
    )
    @classmethod
    def validate_digest(cls, value: str | None) -> str | None:
        return None if value is None else require_sha256(value)

    @field_validator("contract_id")
    @classmethod
    def validate_contract_id(cls, value: str) -> str:
        return require_slug(value, label="contract_id")

    @field_validator("contract_version")
    @classmethod
    def validate_contract_version(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized or len(normalized) > 64:
            raise ValueError("contract_version must be non-empty")
        return normalized

    @field_validator("created_at")
    @classmethod
    def validate_created_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @field_validator("inputs")
    @classmethod
    def sort_inputs(cls, value: tuple[ReleaseInput, ...]) -> tuple[ReleaseInput, ...]:
        return tuple(
            sorted(
                value,
                key=lambda item: (
                    item.source_product_id,
                    item.batch_id,
                    item.ingest_run_id,
                ),
            )
        )

    @field_validator("tables")
    @classmethod
    def sort_tables(
        cls, value: tuple[GoldTableSnapshot, ...]
    ) -> tuple[GoldTableSnapshot, ...]:
        return tuple(sorted(value, key=lambda item: item.table_name))

    @field_validator("quality_reports")
    @classmethod
    def sort_quality_reports(
        cls, value: tuple[ObjectRef, ...]
    ) -> tuple[ObjectRef, ...]:
        return tuple(sorted(value, key=lambda item: item.uri))

    @model_validator(mode="after")
    def validate_release(self, info: ValidationInfo) -> Self:
        if self.previous_release_id == self.release_id:
            raise ValueError("release cannot supersede itself")
        if not self.inputs:
            raise ValueError("catalog release requires inputs")
        if not self.tables:
            raise ValueError("catalog release requires Gold table snapshots")
        input_keys = [(item.source_product_id, item.batch_id) for item in self.inputs]
        if len(input_keys) != len(set(input_keys)):
            raise ValueError("catalog release contains duplicate inputs")
        names = [table.table_name for table in self.tables]
        if len(names) != len(set(names)):
            raise ValueError("catalog release contains duplicate table snapshots")
        if not self.quality_reports:
            raise ValueError("catalog release requires quality reports")
        if any(reference.size_bytes <= 0 for reference in self.quality_reports):
            raise ValueError("quality report objects must be non-empty")
        if not (info.context or {}).get("skip_identity"):
            expected = deterministic_key(
                "community-catalog-release-v2", _release_identity(self)
            )
            if self.release_id != expected:
                raise ValueError("release_id does not match release identity")
        return self


def _release_identity(release: CatalogReleaseManifest) -> dict[str, Any]:
    return {
        "schemaVersion": release.schema_version,
        "previousReleaseId": release.previous_release_id,
        "contractId": release.contract_id,
        "contractVersion": release.contract_version,
        "contractDigest": release.contract_digest,
        "policyContext": release.policy_context.model_dump(
            mode="json", by_alias=True, exclude_none=True
        ),
        "inputs": [
            item.model_dump(mode="json", by_alias=True, exclude_none=True)
            for item in release.inputs
        ],
        "identityPolicyDigest": release.identity_policy_digest,
        "fieldPolicyDigest": release.field_policy_digest,
        "rightsRegistryDigest": release.rights_registry_digest,
        "tables": [
            table.model_dump(mode="json", by_alias=True, exclude_none=True)
            for table in release.tables
        ],
        "qualityReports": [
            reference.model_dump(mode="json", by_alias=True, exclude_none=True)
            for reference in release.quality_reports
        ],
        "attributionManifest": (
            None
            if release.attribution_manifest is None
            else release.attribution_manifest.model_dump(
                mode="json", by_alias=True, exclude_none=True
            )
        ),
        "createdAt": release.created_at,
        "metrics": release.metrics,
    }


def build_catalog_release_manifest(**values: Any) -> CatalogReleaseManifest:
    values = dict(values)
    provisional = CatalogReleaseManifest.model_validate(
        {**values, "release_id": _ZERO_DIGEST},
        context={"skip_identity": True},
    )
    normalized = provisional.model_dump(mode="python")
    normalized["release_id"] = deterministic_key(
        "community-catalog-release-v2", _release_identity(provisional)
    )
    return CatalogReleaseManifest.model_validate(normalized)
