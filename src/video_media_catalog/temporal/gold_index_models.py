"""Serializable contracts for the Research Gold index projection workflow."""

from __future__ import annotations

from dataclasses import dataclass, field

from video_media_catalog.temporal.models import ObjectRefPayload


@dataclass(frozen=True)
class GoldIndexProjectionEnv:
    aws_region: str
    kubernetes_namespace: str
    builder_service_account: str
    builder_image: str
    warehouse_uri: str
    manifest_prefix: str
    result_prefix: str
    validation_prefix: str
    opensearch_endpoint: str
    opensearch_service: str = "es"
    catalog_type: str = "glue"
    catalog_name: str = "media"
    glue_namespace: str = "video_media_catalog"
    shards: int = 6
    replicas: int = 1
    bulk_chunk_size: int = 500
    bulk_max_chunk_bytes: int = 5 * 1024 * 1024
    bulk_partitions: int = 96
    bulk_workers: int = 4
    spark_master: str = "local[*]"
    shuffle_partitions: int = 384
    request_timeout_seconds: int = 30
    min_free_storage_gib: int = 100
    max_jvm_pressure_percent: int = 75
    max_cluster_pending_tasks: int = 10
    active_deadline_seconds: int = 43_200
    cpu_request: str = "4"
    cpu_limit: str = "8"
    memory_request: str = "24Gi"
    memory_limit: str = "32Gi"
    ephemeral_storage_request: str = "80Gi"
    ephemeral_storage_limit: str = "120Gi"
    node_selector: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class GoldIndexProjectionInput:
    projection_id: str
    expected_release_plan_id: str
    release_commit: ObjectRefPayload
    env: GoldIndexProjectionEnv


@dataclass(frozen=True)
class GoldIndexJobResult:
    job_name: str
    result_object: ObjectRefPayload
    build_manifest: ObjectRefPayload


@dataclass(frozen=True)
class GoldIndexProjectionResult:
    projection_id: str
    job_name: str
    build_manifest: ObjectRefPayload
    validation_receipt: ObjectRefPayload
    activation_status: str = "DISABLED"
