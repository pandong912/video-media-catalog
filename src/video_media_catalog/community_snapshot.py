"""Exact Silver snapshot set consumed by distributed Gold builds."""

from __future__ import annotations

from typing import Any, Self

from pydantic import ValidationInfo, field_validator, model_validator

from video_media_catalog.canonical import deterministic_key
from video_media_catalog.community_tables import DATA_TABLE_COLUMNS
from video_media_catalog.v2_contracts import (
    V2ContractModel,
    require_rfc3339,
    require_sha256,
)

CONTROL_MAX_BYTES = 16 * 1024 * 1024
MAX_COMMITTED_RUNS = 4_096
SILVER_SNAPSHOT_MEDIA_TYPE = (
    "application/vnd.video-media-catalog.silver-snapshot-set.v2+json"
)
_ZERO_DIGEST = "sha256:" + ("0" * 64)


class CommunitySilverSnapshotSet(V2ContractModel):
    schema_version: str = "2.0"
    snapshot_set_id: str
    committed_run_ids: tuple[str, ...]
    run_snapshot_id: int
    commit_snapshot_id: int
    data_snapshot_ids: dict[str, int | None]
    created_at: str

    @field_validator("snapshot_set_id")
    @classmethod
    def validate_snapshot_set_id(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("committed_run_ids")
    @classmethod
    def normalize_runs(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(
            sorted({require_sha256(item, label="run_id") for item in value})
        )
        if not normalized:
            raise ValueError("Silver snapshot set requires committed runs")
        if len(normalized) > MAX_COMMITTED_RUNS:
            raise ValueError(
                f"Silver snapshot set supports at most {MAX_COMMITTED_RUNS} runs"
            )
        return normalized

    @field_validator("run_snapshot_id", "commit_snapshot_id")
    @classmethod
    def validate_control_snapshot(cls, value: int) -> int:
        if isinstance(value, bool) or value <= 0:
            raise ValueError("control snapshot IDs must be positive")
        return value

    @field_validator("data_snapshot_ids")
    @classmethod
    def validate_data_snapshots(
        cls, value: dict[str, int | None]
    ) -> dict[str, int | None]:
        if set(value) != set(DATA_TABLE_COLUMNS):
            raise ValueError("Silver snapshot set requires every data table")
        if any(
            snapshot is not None and (isinstance(snapshot, bool) or snapshot <= 0)
            for snapshot in value.values()
        ):
            raise ValueError("Silver snapshot IDs must be positive when present")
        return dict(sorted(value.items()))

    @field_validator("created_at")
    @classmethod
    def validate_created_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @model_validator(mode="after")
    def validate_identity(self, info: ValidationInfo) -> Self:
        if not (info.context or {}).get("skip_identity"):
            expected = deterministic_key(
                "community-silver-snapshot-set-v2",
                _snapshot_identity(self),
            )
            if self.snapshot_set_id != expected:
                raise ValueError("snapshot_set_id does not match Silver snapshots")
        return self


def _snapshot_identity(snapshot: CommunitySilverSnapshotSet) -> dict[str, Any]:
    return {
        "schemaVersion": snapshot.schema_version,
        "committedRunIds": snapshot.committed_run_ids,
        "runSnapshotId": snapshot.run_snapshot_id,
        "commitSnapshotId": snapshot.commit_snapshot_id,
        "dataSnapshotIds": snapshot.data_snapshot_ids,
        "createdAt": snapshot.created_at,
    }


def build_community_silver_snapshot_set(
    **values: Any,
) -> CommunitySilverSnapshotSet:
    provisional = CommunitySilverSnapshotSet.model_validate(
        {**values, "snapshot_set_id": _ZERO_DIGEST},
        context={"skip_identity": True},
    )
    normalized = provisional.model_dump(mode="python")
    normalized["snapshot_set_id"] = deterministic_key(
        "community-silver-snapshot-set-v2",
        _snapshot_identity(provisional),
    )
    return CommunitySilverSnapshotSet.model_validate(normalized)
