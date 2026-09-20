"""Versioned exact Silver snapshot and epoch contracts."""

from __future__ import annotations

import json
import re
from collections.abc import Sequence
from typing import Any, Literal, Self
from urllib.parse import urlsplit

from pydantic import Field, ValidationInfo, field_validator, model_validator

from video_media_catalog.canonical import deterministic_key
from video_media_catalog.community_tables import DATA_TABLE_COLUMNS
from video_media_catalog.models import ObjectRef
from video_media_catalog.v2_contracts import (
    V2ContractModel,
    require_rfc3339,
    require_sha256,
    require_slug,
)

CONTROL_MAX_BYTES = 16 * 1024 * 1024
MAX_EPOCH_DELTA_RUNS = 4_096
SILVER_SNAPSHOT_MEDIA_TYPE = (
    "application/vnd.video-media-catalog.silver-snapshot-set.v2+json"
)
SILVER_EPOCH_MEDIA_TYPE = (
    "application/vnd.video-media-catalog.silver-epoch-manifest.v3+json"
)
COMMITTED_RUN_DIGEST_ALGORITHM = "sha256-bucketed-run-ids-v1"
_ZERO_DIGEST = "sha256:" + ("0" * 64)
_BUCKET = re.compile(r"^[0-9a-f]{2}$")


class CommunitySilverSnapshotSet(V2ContractModel):
    """Legacy v2 handoff.

    The model no longer imposes an artificial run-count limit. New production
    publications use ``CommunitySilverEpochManifest`` so historical run IDs do
    not live in a driver-resident control object.
    """

    schema_version: Literal["2.0"] = "2.0"
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


class CommunitySilverEpochReference(V2ContractModel):
    """Immutable reference to one already-published epoch manifest."""

    epoch_id: str
    object_ref: ObjectRef

    @field_validator("epoch_id")
    @classmethod
    def validate_epoch_id(cls, value: str) -> str:
        return require_sha256(value, label="epoch_id")

    @model_validator(mode="after")
    def validate_object_ref(self) -> Self:
        reference = self.object_ref
        if (
            reference.format != "OBJECT_FORMAT_JSON"
            or reference.media_type != SILVER_EPOCH_MEDIA_TYPE
            or not 0 < reference.size_bytes <= CONTROL_MAX_BYTES
        ):
            raise ValueError(
                "epoch reference must identify a bounded epoch JSON object"
            )
        parsed = urlsplit(reference.uri)
        scheme = parsed.scheme
        if (
            scheme not in {"file", "s3"}
            or parsed.query
            or parsed.fragment
            or parsed.username is not None
            or parsed.password is not None
        ):
            raise ValueError("epoch reference URI must use file:// or s3://")
        if scheme == "s3" and (not parsed.netloc or not parsed.path.lstrip("/")):
            raise ValueError("epoch reference must identify one S3 object")
        if scheme == "file" and not parsed.path:
            raise ValueError("epoch reference must identify one local object")
        if scheme == "s3" and (
            reference.object_version is None or reference.etag is None
        ):
            raise ValueError("S3 epoch reference requires object version and ETag")
        if scheme == "file" and (
            reference.object_version is not None or reference.etag is not None
        ):
            raise ValueError("file epoch reference cannot declare S3 metadata")
        return self


class CommunitySilverEpochManifest(V2ContractModel):
    """Bounded control manifest for an unbounded committed-run history."""

    schema_version: Literal["3.0"] = "3.0"
    epoch_id: str
    parent_epoch: CommunitySilverEpochReference | None = None
    baseline_epoch: CommunitySilverEpochReference | None = None
    delta_run_ids: tuple[str, ...] = ()
    run_snapshot_id: int
    commit_snapshot_id: int
    data_snapshot_ids: dict[str, int | None]
    source_watermarks: dict[str, str] = Field(default_factory=dict)
    committed_run_count: int = Field(gt=0)
    committed_run_digest: str
    committed_run_digest_algorithm: Literal["sha256-bucketed-run-ids-v1"] = (
        COMMITTED_RUN_DIGEST_ALGORITHM
    )
    created_at: str

    @field_validator("epoch_id", "committed_run_digest")
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("committed_run_count", mode="before")
    @classmethod
    def validate_committed_run_count(cls, value: Any) -> Any:
        if isinstance(value, bool):
            raise ValueError("committed_run_count must be a positive integer")
        return value

    @field_validator("delta_run_ids")
    @classmethod
    def normalize_delta_runs(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(
            sorted({require_sha256(item, label="delta run_id") for item in value})
        )
        if len(normalized) != len(value):
            raise ValueError("epoch delta contains duplicate run IDs")
        if len(normalized) > MAX_EPOCH_DELTA_RUNS:
            raise ValueError(
                f"epoch delta supports at most {MAX_EPOCH_DELTA_RUNS} run IDs"
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
            raise ValueError("Silver epoch requires every data table")
        if any(
            snapshot is not None and (isinstance(snapshot, bool) or snapshot <= 0)
            for snapshot in value.values()
        ):
            raise ValueError("Silver epoch snapshot IDs must be positive when present")
        return dict(sorted(value.items()))

    @field_validator("source_watermarks")
    @classmethod
    def normalize_source_watermarks(cls, value: dict[str, str]) -> dict[str, str]:
        normalized: dict[str, str] = {}
        for source_product_id, watermark in value.items():
            source = require_slug(source_product_id, label="source_product_id")
            current = watermark.strip()
            if not current or len(current) > 1024:
                raise ValueError("source watermark must be non-empty and bounded")
            if source in normalized:
                raise ValueError("source watermarks contain duplicate products")
            normalized[source] = current
        return dict(sorted(normalized.items()))

    @field_validator("created_at")
    @classmethod
    def validate_created_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @model_validator(mode="after")
    def validate_epoch(self, info: ValidationInfo) -> Self:
        if (self.parent_epoch is None) != (self.baseline_epoch is None):
            raise ValueError(
                "parent and baseline epoch references must both be present or absent"
            )
        if self.committed_run_count < len(self.delta_run_ids):
            raise ValueError("epoch delta exceeds committed run count")
        references = tuple(
            reference
            for reference in (self.parent_epoch, self.baseline_epoch)
            if reference is not None
        )
        if any(reference.epoch_id == self.epoch_id for reference in references):
            raise ValueError("epoch cannot reference itself")
        if not (info.context or {}).get("skip_identity"):
            expected = deterministic_key(
                "community-silver-epoch-v3",
                _epoch_identity(self),
            )
            if self.epoch_id != expected:
                raise ValueError("epoch_id does not match Silver epoch contents")
        return self


CommunitySilverManifest = CommunitySilverSnapshotSet | CommunitySilverEpochManifest


def _epoch_reference_identity(
    reference: CommunitySilverEpochReference | None,
) -> dict[str, Any] | None:
    if reference is None:
        return None
    return reference.model_dump(mode="json", by_alias=True, exclude_none=True)


def _epoch_identity(epoch: CommunitySilverEpochManifest) -> dict[str, Any]:
    return {
        "schemaVersion": epoch.schema_version,
        "parentEpoch": _epoch_reference_identity(epoch.parent_epoch),
        "baselineEpoch": _epoch_reference_identity(epoch.baseline_epoch),
        "deltaRunIds": epoch.delta_run_ids,
        "runSnapshotId": epoch.run_snapshot_id,
        "commitSnapshotId": epoch.commit_snapshot_id,
        "dataSnapshotIds": epoch.data_snapshot_ids,
        "sourceWatermarks": epoch.source_watermarks,
        "committedRunCount": epoch.committed_run_count,
        "committedRunDigest": epoch.committed_run_digest,
        "committedRunDigestAlgorithm": epoch.committed_run_digest_algorithm,
        "createdAt": epoch.created_at,
    }


def build_community_silver_epoch_manifest(
    **values: Any,
) -> CommunitySilverEpochManifest:
    provisional = CommunitySilverEpochManifest.model_validate(
        {**values, "epoch_id": _ZERO_DIGEST},
        context={"skip_identity": True},
    )
    normalized = provisional.model_dump(mode="python")
    normalized["epoch_id"] = deterministic_key(
        "community-silver-epoch-v3",
        _epoch_identity(provisional),
    )
    return CommunitySilverEpochManifest.model_validate(normalized)


def build_committed_run_digest(
    *,
    run_count: int,
    buckets: Sequence[tuple[str, int, str]],
) -> str:
    """Build the run-set digest from at most 256 deterministic bucket summaries."""

    if isinstance(run_count, bool) or run_count < 0:
        raise ValueError("run_count must be a non-negative integer")
    normalized = []
    seen: set[str] = set()
    total = 0
    for bucket, count, digest in sorted(buckets):
        if _BUCKET.fullmatch(bucket) is None or bucket in seen:
            raise ValueError("run digest buckets must be unique lowercase hex bytes")
        if isinstance(count, bool) or count <= 0:
            raise ValueError("run digest bucket counts must be positive")
        seen.add(bucket)
        total += count
        normalized.append(
            {
                "bucket": bucket,
                "runCount": count,
                "digest": require_sha256(digest, label="bucket digest"),
            }
        )
    if total != run_count:
        raise ValueError("run digest bucket counts do not match run_count")
    return deterministic_key(
        "community-silver-committed-runs-v1",
        {
            "algorithm": COMMITTED_RUN_DIGEST_ALGORITHM,
            "runCount": run_count,
            "buckets": normalized,
        },
    )


def parse_community_silver_manifest(
    payload: bytes | str,
) -> CommunitySilverManifest:
    """Read v2 snapshot sets and v3 epoch manifests without schema guessing."""

    try:
        raw = json.loads(payload)
    except (TypeError, ValueError) as exc:
        raise ValueError("Silver manifest must be valid JSON") from exc
    if not isinstance(raw, dict):
        raise ValueError("Silver manifest must be a JSON object")
    version = raw.get("schemaVersion", raw.get("schema_version", "2.0"))
    if version == "2.0":
        return CommunitySilverSnapshotSet.model_validate(raw)
    if version == "3.0":
        return CommunitySilverEpochManifest.model_validate(raw)
    raise ValueError(f"unsupported Silver manifest schema version: {version!r}")


def community_silver_manifest_id(manifest: CommunitySilverManifest) -> str:
    if isinstance(manifest, CommunitySilverEpochManifest):
        return manifest.epoch_id
    return manifest.snapshot_set_id
