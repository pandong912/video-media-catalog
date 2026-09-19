"""Commit-last metadata for policy-specific Gold Iceberg releases."""

from __future__ import annotations

from typing import Any, Self
from urllib.parse import urlsplit

from pydantic import ValidationInfo, field_validator, model_validator

from video_media_catalog.canonical import deterministic_key
from video_media_catalog.gold_tables import GOLD_DATA_COLUMNS
from video_media_catalog.models import ObjectRef
from video_media_catalog.v2_contracts import (
    V2ContractModel,
    require_rfc3339,
    require_sha256,
)

_ZERO_DIGEST = "sha256:" + ("0" * 64)
GOLD_QUALITY_MEDIA_TYPE = (
    "application/vnd.video-media-catalog.gold-quality-report.v2+json"
)
ATTRIBUTION_MEDIA_TYPE = (
    "application/vnd.video-media-catalog.attribution-manifest.v2+json"
)
GOLD_RELEASE_COMMIT_MEDIA_TYPE = (
    "application/vnd.video-media-catalog.gold-release-commit.v2+json"
)


def _validate_object_ref(
    reference: ObjectRef,
    *,
    media_type: str,
    label: str,
) -> None:
    scheme = urlsplit(reference.uri).scheme
    if scheme not in {"file", "s3"}:
        raise ValueError(f"{label} must use file:// or s3://")
    if reference.format != "OBJECT_FORMAT_JSON":
        raise ValueError(f"{label} must be JSON")
    if reference.media_type != media_type:
        raise ValueError(f"{label} has an unexpected media type")
    if reference.size_bytes <= 0:
        raise ValueError(f"{label} must be non-empty")
    if scheme == "s3" and (reference.etag is None or reference.object_version is None):
        raise ValueError(f"{label} S3 object must be immutable")


class GoldReleaseCommit(V2ContractModel):
    schema_version: str = "2.0"
    commit_key: str
    release_plan_id: str
    committed_at: str
    table_counts: dict[str, int]
    table_snapshot_ids: dict[str, int | None]
    quality_report: ObjectRef
    attribution_manifest: ObjectRef

    @field_validator("commit_key", "release_plan_id")
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("committed_at")
    @classmethod
    def validate_committed_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @field_validator("table_counts")
    @classmethod
    def validate_counts(cls, value: dict[str, int]) -> dict[str, int]:
        if set(value) != set(GOLD_DATA_COLUMNS):
            raise ValueError("Gold commit counts must contain every data table")
        if any(isinstance(count, bool) or count < 0 for count in value.values()):
            raise ValueError("Gold commit counts must be non-negative integers")
        return dict(sorted(value.items()))

    @field_validator("table_snapshot_ids")
    @classmethod
    def validate_snapshots(cls, value: dict[str, int | None]) -> dict[str, int | None]:
        if set(value) != set(GOLD_DATA_COLUMNS):
            raise ValueError("Gold snapshots must contain every data table")
        if any(
            snapshot is not None and (isinstance(snapshot, bool) or snapshot <= 0)
            for snapshot in value.values()
        ):
            raise ValueError("Gold snapshot IDs must be positive when present")
        return dict(sorted(value.items()))

    @model_validator(mode="after")
    def validate_commit(self, info: ValidationInfo) -> Self:
        _validate_object_ref(
            self.quality_report,
            media_type=GOLD_QUALITY_MEDIA_TYPE,
            label="quality report",
        )
        _validate_object_ref(
            self.attribution_manifest,
            media_type=ATTRIBUTION_MEDIA_TYPE,
            label="attribution manifest",
        )
        for table, count in self.table_counts.items():
            if count > 0 and self.table_snapshot_ids[table] is None:
                raise ValueError(f"{table} has rows but no snapshot ID")
        if not (info.context or {}).get("skip_identity"):
            expected = deterministic_key(
                "community-gold-release-commit-v2",
                _commit_identity(self),
            )
            if self.commit_key != expected:
                raise ValueError("Gold commit_key does not match identity")
        return self


def _commit_identity(commit: GoldReleaseCommit) -> dict[str, Any]:
    return {
        "schemaVersion": commit.schema_version,
        "releasePlanId": commit.release_plan_id,
        "committedAt": commit.committed_at,
        "tableCounts": commit.table_counts,
        "tableSnapshotIds": commit.table_snapshot_ids,
        "qualityReport": commit.quality_report.model_dump(
            mode="json", by_alias=True, exclude_none=True
        ),
        "attributionManifest": commit.attribution_manifest.model_dump(
            mode="json", by_alias=True, exclude_none=True
        ),
    }


def build_gold_release_commit(**values: Any) -> GoldReleaseCommit:
    provisional = GoldReleaseCommit.model_validate(
        {**values, "commit_key": _ZERO_DIGEST},
        context={"skip_identity": True},
    )
    normalized = provisional.model_dump(mode="python")
    normalized["commit_key"] = deterministic_key(
        "community-gold-release-commit-v2",
        _commit_identity(provisional),
    )
    return GoldReleaseCommit.model_validate(normalized)
