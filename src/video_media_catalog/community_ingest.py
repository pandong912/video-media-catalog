"""Deterministic run and commit contracts for Silver v2 persistence."""

from __future__ import annotations

import json
from enum import StrEnum
from typing import Any, Self

from pydantic import ValidationInfo, field_validator, model_validator

from video_media_catalog.canonical import canonical_json, deterministic_key
from video_media_catalog.community_tables import DATA_TABLE_COLUMNS
from video_media_catalog.v2_contracts import (
    V2ContractModel,
    require_rfc3339,
    require_sha256,
    require_slug,
)

_ZERO_DIGEST = "sha256:" + ("0" * 64)


class IngestRunKind(StrEnum):
    SOURCE_ASSERTIONS = "SOURCE_ASSERTIONS"
    V1_KEY_MIGRATION = "V1_KEY_MIGRATION"
    IDENTITY_RESOLUTION = "IDENTITY_RESOLUTION"
    IDENTITY_CURATION = "IDENTITY_CURATION"


def _validate_counts(value: dict[str, int], *, label: str) -> dict[str, int]:
    if set(value) != set(DATA_TABLE_COLUMNS):
        raise ValueError(f"{label} must contain every v2 data table")
    if any(isinstance(count, bool) or count < 0 for count in value.values()):
        raise ValueError(f"{label} must contain non-negative integers")
    return dict(sorted(value.items()))


class CommunityIngestRun(V2ContractModel):
    schema_version: str = "2.0"
    run_id: str
    run_kind: IngestRunKind
    source_product_id: str
    input_id: str
    policy_id: str
    policy_digest: str
    image_digest: str
    config_digest: str
    started_at: str
    expected_counts: dict[str, int]
    input_manifest: dict[str, Any]

    @field_validator(
        "run_id",
        "input_id",
        "policy_digest",
        "image_digest",
        "config_digest",
    )
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("source_product_id", "policy_id")
    @classmethod
    def validate_id(cls, value: str) -> str:
        return require_slug(value, label="ingest run reference")

    @field_validator("started_at")
    @classmethod
    def validate_started_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @field_validator("expected_counts")
    @classmethod
    def validate_expected_counts(cls, value: dict[str, int]) -> dict[str, int]:
        return _validate_counts(value, label="expected_counts")

    @field_validator("input_manifest", mode="before")
    @classmethod
    def normalize_input_manifest(cls, value: Any) -> dict[str, Any]:
        normalized = json.loads(canonical_json(value))
        if not isinstance(normalized, dict):
            raise ValueError("input_manifest must be an object")
        return normalized

    @model_validator(mode="after")
    def validate_run(self, info: ValidationInfo) -> Self:
        if not self.input_manifest:
            raise ValueError("input_manifest must not be empty")
        if not (info.context or {}).get("skip_identity"):
            expected = deterministic_key(
                "community-ingest-run-v2",
                _run_identity(self),
            )
            if self.run_id != expected:
                raise ValueError("run_id does not match ingest identity")
        return self


def _run_identity(run: CommunityIngestRun) -> dict[str, Any]:
    return {
        "schemaVersion": run.schema_version,
        "runKind": run.run_kind.value,
        "sourceProductId": run.source_product_id,
        "inputId": run.input_id,
        "policyId": run.policy_id,
        "policyDigest": run.policy_digest,
        "imageDigest": run.image_digest,
        "configDigest": run.config_digest,
        "startedAt": run.started_at,
        "expectedCounts": run.expected_counts,
        "inputManifest": run.input_manifest,
    }


def build_community_ingest_run(**values: Any) -> CommunityIngestRun:
    provisional = CommunityIngestRun.model_validate(
        {**values, "run_id": _ZERO_DIGEST},
        context={"skip_identity": True},
    )
    normalized = provisional.model_dump(mode="python")
    normalized["run_id"] = deterministic_key(
        "community-ingest-run-v2",
        _run_identity(provisional),
    )
    return CommunityIngestRun.model_validate(normalized)


class CommunityIngestCommit(V2ContractModel):
    schema_version: str = "2.0"
    commit_key: str
    run_id: str
    committed_at: str
    table_counts: dict[str, int]
    table_snapshot_ids: dict[str, int | None]

    @field_validator("commit_key", "run_id")
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("committed_at")
    @classmethod
    def validate_committed_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @field_validator("table_counts")
    @classmethod
    def validate_table_counts(cls, value: dict[str, int]) -> dict[str, int]:
        return _validate_counts(value, label="table_counts")

    @field_validator("table_snapshot_ids")
    @classmethod
    def validate_snapshot_ids(
        cls, value: dict[str, int | None]
    ) -> dict[str, int | None]:
        if set(value) != set(DATA_TABLE_COLUMNS):
            raise ValueError("table_snapshot_ids must contain every v2 data table")
        if any(
            snapshot is not None and (isinstance(snapshot, bool) or snapshot <= 0)
            for snapshot in value.values()
        ):
            raise ValueError("table snapshot IDs must be positive when present")
        return dict(sorted(value.items()))

    @model_validator(mode="after")
    def validate_commit(self, info: ValidationInfo) -> Self:
        for table, count in self.table_counts.items():
            if count > 0 and self.table_snapshot_ids[table] is None:
                raise ValueError(f"{table} has rows but no containing snapshot ID")
        if not (info.context or {}).get("skip_identity"):
            expected = deterministic_key(
                "community-ingest-commit-v2",
                _commit_identity(self),
            )
            if self.commit_key != expected:
                raise ValueError("commit_key does not match commit identity")
        return self


def _commit_identity(commit: CommunityIngestCommit) -> dict[str, Any]:
    return {
        "schemaVersion": commit.schema_version,
        "runId": commit.run_id,
        "committedAt": commit.committed_at,
        "tableCounts": commit.table_counts,
        "tableSnapshotIds": commit.table_snapshot_ids,
    }


def build_community_ingest_commit(**values: Any) -> CommunityIngestCommit:
    provisional = CommunityIngestCommit.model_validate(
        {**values, "commit_key": _ZERO_DIGEST},
        context={"skip_identity": True},
    )
    normalized = provisional.model_dump(mode="python")
    normalized["commit_key"] = deterministic_key(
        "community-ingest-commit-v2",
        _commit_identity(provisional),
    )
    return CommunityIngestCommit.model_validate(normalized)
