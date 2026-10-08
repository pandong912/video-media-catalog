"""Serializable Temporal inputs and results for TMDB capture + Silver."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

CaptureKind = Literal["inventory", "changes"]
PipelineMode = Literal["bootstrap", "daily", "inventory-only"]


@dataclass(frozen=True)
class ObjectRefPayload:
    uri: str
    format: str
    media_type: str
    checksum_algorithm: str
    checksum_value: str
    size_bytes: int
    etag: str | None = None
    object_version: str | None = None

    @classmethod
    def from_mapping(cls, payload: dict[str, Any]) -> ObjectRefPayload:
        checksum = payload["checksum"]
        return cls(
            uri=str(payload["uri"]),
            format=str(payload["format"]),
            media_type=str(payload["mediaType"]),
            checksum_algorithm=str(checksum["algorithm"]),
            checksum_value=str(checksum["value"]),
            size_bytes=int(payload["sizeBytes"]),
            etag=payload.get("etag"),
            object_version=payload.get("objectVersion"),
        )

    def checksum_digest(self) -> str:
        if self.checksum_algorithm != "sha256":
            raise ValueError("TMDB manifests must use sha256 checksums")
        value = self.checksum_value
        return value if value.startswith("sha256:") else f"sha256:{value}"


@dataclass(frozen=True)
class CaptureUnit:
    kind: CaptureKind
    export_date: str | None = None
    window_start: str | None = None
    window_end: str | None = None
    window_cursor: str | None = None


@dataclass(frozen=True)
class CaptureResult:
    kind: CaptureKind
    batch_id: str
    record_set_id: str
    record_count: int
    batch_manifest: ObjectRefPayload
    record_set_manifest: ObjectRefPayload
    export_date: str | None = None
    window_start: str | None = None
    window_end: str | None = None
    window_cursor: str | None = None
    window_shard_index: int | None = None
    window_shard_count: int | None = None
    retry_count: int = 0
    rate_limit_count: int = 0


@dataclass(frozen=True)
class SilverResult:
    batch_id: str
    run_id: str
    commit_key: str
    job_run_id: str
    table_counts: dict[str, int] = field(default_factory=dict)
    table_snapshot_ids: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class PipelineEnv:
    destination_prefix: str
    image_digest: str
    user_agent: str
    aws_region: str
    warehouse_uri: str
    record_staging_prefix: str
    silver_checkpoint_prefix: str
    pipeline_summary_prefix: str
    emr_application_name: str
    emr_execution_role_arn: str
    emr_log_uri: str
    emr_entry_point: str
    catalog_type: str = "glue"
    catalog_name: str = "media"
    glue_namespace: str = "video_media_catalog"
    capture_concurrency: int = 2
    s3_endpoint: str | None = None
    s3_path_style_access: bool = False


@dataclass(frozen=True)
class PipelineInput:
    mode: PipelineMode
    env: PipelineEnv
    export_date: str | None = None
    window_start: str | None = None
    window_end: str | None = None
    acquired_at: str | None = None
    units: tuple[CaptureUnit, ...] = ()


@dataclass(frozen=True)
class PipelineResult:
    mode: PipelineMode
    acquired_at: str
    export_date: str | None
    window_start: str | None
    window_end: str | None
    captures: list[CaptureResult]
    silvers: list[SilverResult]
    summary_uri: str | None
    capture_count: int
