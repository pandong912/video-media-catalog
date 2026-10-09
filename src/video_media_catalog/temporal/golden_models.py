"""Serializable contracts for the research Golden Build workflow."""

from __future__ import annotations

from dataclasses import dataclass, field

from video_media_catalog.temporal.models import ObjectRefPayload


@dataclass(frozen=True)
class GoldenBuildEnv:
    aws_region: str
    warehouse_uri: str
    control_prefix: str
    gold_output_prefix: str
    pipeline_summary_prefix: str
    emr_application_name: str
    emr_execution_role_arn: str
    emr_log_uri: str
    research_silver_entry_point: str
    gold_entry_point: str
    image_digest: str
    catalog_type: str = "glue"
    catalog_name: str = "media"
    glue_namespace: str = "video_media_catalog"
    control_executor_instances: int = 4
    control_shuffle_partitions: int = 96
    build_executor_instances: int = 12
    build_shuffle_partitions: int = 288


@dataclass(frozen=True)
class FreshnessOverrideInput:
    reason: str
    imdb_acquired_at: str
    tvmaze_acquired_at: str
    max_slo_hours: int = 720


@dataclass(frozen=True)
class GoldenBuildInput:
    build_id: str
    identity_generation_id: str
    baseline_snapshot: ObjectRefPayload
    tmdb_pipeline_summary: ObjectRefPayload
    baseline_source_run_ids: tuple[str, ...]
    baseline_identity_run_id: str
    tmdb_source_run_ids: tuple[str, ...]
    source_watermarks: dict[str, str]
    freshness_override: FreshnessOverrideInput
    env: GoldenBuildEnv


@dataclass(frozen=True)
class GoldenPreflightResult:
    build_spec: ObjectRefPayload
    planned_at: str
    all_snapshot_run_ids: tuple[str, ...]
    tmdb_source_run_ids: tuple[str, ...]
    imdb_age_hours: float
    tvmaze_age_hours: float
    imdb_slo_hours: int
    tvmaze_slo_hours: int
    freshness_policy_digest: str
    identity_config_digest: str
    identity_resolution_config_digest: str
    identity_bound_config_digest: str


@dataclass(frozen=True)
class GoldenSnapshotResult:
    snapshot_set_id: str
    snapshot: ObjectRefPayload
    job_run_id: str


@dataclass(frozen=True)
class GoldenIdentityResult:
    run_id: str
    commit_key: str
    source_run_ids: tuple[str, ...]
    table_counts: dict[str, int]
    table_snapshot_ids: dict[str, int | None]
    job_run_id: str


@dataclass(frozen=True)
class GoldenEpochResult:
    epoch_id: str
    epoch: ObjectRefPayload
    committed_run_count: int
    committed_run_digest: str
    data_snapshot_ids: dict[str, int | None]
    job_run_id: str


@dataclass(frozen=True)
class GoldenReleaseResult:
    release_plan_id: str
    commit_key: str
    quality_report: ObjectRefPayload
    attribution_manifest: ObjectRefPayload
    release_commit: ObjectRefPayload
    table_counts: dict[str, int]
    table_snapshot_ids: dict[str, int | None]
    job_run_id: str


@dataclass(frozen=True)
class GoldenBuildResult:
    build_id: str
    planned_at: str
    committed_at: str
    build_spec: ObjectRefPayload
    snapshot: GoldenSnapshotResult
    identity: GoldenIdentityResult
    epoch: GoldenEpochResult
    release: GoldenReleaseResult
    summary_uri: str | None = None
    publication_status: dict[str, str] = field(
        default_factory=lambda: {
            "opensearch": "disabled",
            "canonical": "disabled",
            "cutover": "disabled",
        }
    )
