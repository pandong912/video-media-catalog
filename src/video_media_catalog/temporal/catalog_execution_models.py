"""Serializable P3 Catalog execution workflow contracts."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Literal

from video_media_catalog.execution_contracts import (
    ObjectRefV2,
    SparkProviderProfile,
    TransferPlanBinding,
    WorkloadIdentityBinding,
)

CATALOG_EXECUTION_CONTRACT = "video.media-catalog.execution.p3.v1"
PLATFORM_EXECUTION_NAMESPACE = "video-world-platform"
CATALOG_EXECUTION_NAMESPACE = "vw-media-catalog-research"

_UUID_V7 = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-"
    r"[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


def _uuid(value: str, label: str) -> str:
    if _UUID_V7.fullmatch(value) is None:
        raise ValueError(f"{label} must be a canonical UUIDv7")
    return value


def _sha(value: str, label: str) -> str:
    if _SHA256.fullmatch(value) is None:
        raise ValueError(f"{label} must be a lowercase SHA-256")
    return value


def _bounded(value: str, label: str, maximum: int) -> str:
    if not value or value != value.strip() or len(value) > maximum:
        raise ValueError(f"{label} must be canonical bounded text")
    return value


@dataclass(frozen=True)
class ObjectRefV2Payload:
    schema_version: str
    object_ref_id: str
    tenant_id: str
    project_id: str
    data_plane_binding_id: str
    data_plane_id: str
    provider: Literal["AWS_S3", "ALIBABA_OSS"]
    uri: str
    object_key: str
    format: Literal["BYTES", "JSON", "JSONL", "PARQUET", "PROTOBUF", "OTHER"]
    media_type: str
    checksum: str
    size_bytes: int
    etag: str
    version_token: str
    schema_digest: str | None
    encryption_context_digest: str
    identity_digest: str
    created_at: str

    @classmethod
    def from_contract(cls, value: ObjectRefV2) -> ObjectRefV2Payload:
        return cls(**value.model_dump())

    def to_contract(self) -> ObjectRefV2:
        return ObjectRefV2.model_validate(self.__dict__)


@dataclass(frozen=True)
class WorkloadIdentityPayload:
    cloud_provider: Literal["AWS", "ALIYUN"]
    identity_reference: str
    subject_digest: str
    audience: str
    semantic_digest: str

    @classmethod
    def from_contract(
        cls,
        value: WorkloadIdentityBinding,
    ) -> WorkloadIdentityPayload:
        return cls(**value.model_dump())

    def to_contract(self) -> WorkloadIdentityBinding:
        return WorkloadIdentityBinding.model_validate(self.__dict__)


@dataclass(frozen=True)
class SparkProviderProfilePayload:
    schema_version: str
    profile_key: str
    cloud_provider: Literal["AWS", "ALIYUN"]
    object_provider: Literal["AWS_S3", "ALIBABA_OSS"]
    backend_kind: Literal[
        "AWS_EMR_SERVERLESS",
        "ALIYUN_EMR_SERVERLESS_SPARK",
    ]
    region: str
    application_id: str
    workload_identity: WorkloadIdentityPayload
    max_input_bytes: int
    max_output_bytes: int
    transfer_only: bool
    profile_digest: str

    @classmethod
    def from_contract(
        cls,
        value: SparkProviderProfile,
    ) -> SparkProviderProfilePayload:
        payload = value.model_dump()
        payload["workload_identity"] = WorkloadIdentityPayload.from_contract(
            value.workload_identity
        )
        return cls(**payload)

    def to_contract(self) -> SparkProviderProfile:
        payload = dict(self.__dict__)
        payload["workload_identity"] = self.workload_identity.to_contract()
        return SparkProviderProfile.model_validate(payload)


@dataclass(frozen=True)
class TransferPlanPayload:
    schema_version: str
    plan_kind: Literal["TRANSFER"]
    transfer_plan_id: str
    transfer_plan_digest: str
    source_provider: Literal["AWS", "ALIYUN"]
    destination_provider: Literal["AWS", "ALIYUN"]
    source_object_identity_digests: tuple[str, ...]
    destination_prefix: str
    semantic_digest: str

    @classmethod
    def from_contract(cls, value: TransferPlanBinding) -> TransferPlanPayload:
        return cls(**value.model_dump())

    def to_contract(self) -> TransferPlanBinding:
        return TransferPlanBinding.model_validate(self.__dict__)


@dataclass(frozen=True)
class CompletionReceiptTarget:
    object_ref_id: str
    data_plane_binding_id: str
    data_plane_id: str
    uri: str
    object_key: str
    format: Literal["JSON"] = "JSON"
    media_type: str = (
        "application/vnd.video-media-catalog.execution-completion-receipt.v1+json"
    )
    schema_digest: str = ""
    encryption_context_digest: str = ""

    def __post_init__(self) -> None:
        _uuid(self.object_ref_id, "object_ref_id")
        _uuid(self.data_plane_binding_id, "data_plane_binding_id")
        _uuid(self.data_plane_id, "data_plane_id")
        _sha(self.schema_digest, "schema_digest")
        _sha(self.encryption_context_digest, "encryption_context_digest")


@dataclass(frozen=True)
class SparkJobSpec:
    entry_point: str
    entry_point_arguments: tuple[str, ...]
    spark_submit_parameters: str
    log_uri: str
    execution_timeout_minutes: int = 720
    max_attempts: int = 1

    def __post_init__(self) -> None:
        _bounded(self.entry_point, "entry_point", 2_048)
        _bounded(self.log_uri, "log_uri", 2_048)
        if not self.entry_point_arguments or len(self.entry_point_arguments) > 1_024:
            raise ValueError("entry_point_arguments must be bounded and non-empty")
        if self.execution_timeout_minutes < 1 or self.max_attempts < 1:
            raise ValueError("job timeout and attempts must be positive")


@dataclass(frozen=True)
class CatalogExecutionInput:
    contract_version: Literal["video.media-catalog.execution.p3.v1"]
    platform_namespace: str
    platform_workflow_id: str
    platform_contract_digest: str
    tenant_id: str
    project_id: str
    execution_plan_id: str
    plan_revision: int
    plan_digest: str
    execution_partition_id: str
    execution_attempt_id: str
    plan_kind: Literal["EXECUTION", "TRANSFER"]
    provider_profile: SparkProviderProfilePayload
    input_objects: tuple[ObjectRefV2Payload, ...]
    transfer_plan: TransferPlanPayload | None
    job_spec: SparkJobSpec
    completion_receipt_target: CompletionReceiptTarget
    expected_result_kind: Literal["EXTERNAL_AUTHORITY"]
    poll_interval_seconds: int = 15
    max_observations: int = 2_880

    def __post_init__(self) -> None:
        if self.platform_namespace != PLATFORM_EXECUTION_NAMESPACE:
            raise ValueError("Platform namespace is not allowlisted")
        _bounded(self.platform_workflow_id, "platform_workflow_id", 255)
        _sha(self.platform_contract_digest, "platform_contract_digest")
        for label, value in (
            ("tenant_id", self.tenant_id),
            ("project_id", self.project_id),
            ("execution_plan_id", self.execution_plan_id),
            ("execution_partition_id", self.execution_partition_id),
            ("execution_attempt_id", self.execution_attempt_id),
        ):
            _uuid(value, label)
        if self.plan_revision < 1:
            raise ValueError("plan_revision must be positive")
        _sha(self.plan_digest, "plan_digest")
        if not self.input_objects or len(self.input_objects) > 10_000:
            raise ValueError("input_objects must be bounded and non-empty")
        if self.poll_interval_seconds < 1 or self.poll_interval_seconds > 300:
            raise ValueError("poll interval must be within [1, 300] seconds")
        if self.max_observations < 1 or self.max_observations > 100_000:
            raise ValueError("max observations must be within [1, 100000]")


@dataclass(frozen=True)
class PreparedCatalogExecution:
    input: CatalogExecutionInput
    client_token: str
    job_name: str


@dataclass(frozen=True)
class CatalogExecutionStatus:
    state: Literal[
        "PREPARING",
        "SUBMITTING",
        "RUNNING",
        "CANCELLING",
        "SUCCESS",
        "FAILED",
        "CANCELLED",
    ]
    observation_count: int
    cancel_requested: bool
    backend_job_id: str | None
    provider_state: str | None


@dataclass(frozen=True)
class CatalogExecutionResult:
    state: Literal["SUCCESS", "FAILED", "CANCELLED"]
    backend_application_id: str
    backend_job_id: str
    completion_receipt: ObjectRefV2Payload
    completion_receipt_digest: str
    result_kind: Literal["EXTERNAL_AUTHORITY"] = "EXTERNAL_AUTHORITY"


def catalog_execution_workflow_id(input_data: CatalogExecutionInput) -> str:
    """Stable across retries and independent of provider payload."""

    digest = hashlib.sha256(
        (
            "video.media-catalog.execution-workflow-id.v1:"
            f"{input_data.tenant_id}:{input_data.execution_attempt_id}:"
            f"{input_data.plan_digest}"
        ).encode()
    ).hexdigest()[:24]
    return (
        f"vmc-execution/{input_data.tenant_id}/"
        f"attempt-{input_data.execution_attempt_id}/{digest}"
    )


def catalog_execution_client_token(input_data: CatalogExecutionInput) -> str:
    digest = hashlib.sha256(
        (
            "video.media-catalog.spark-client-token.v1:"
            f"{input_data.execution_attempt_id}:"
            f"{input_data.provider_profile.profile_digest}"
        ).encode()
    ).hexdigest()
    return f"vmc-p3-{digest[:56]}"


def catalog_execution_job_name(input_data: CatalogExecutionInput) -> str:
    digest = hashlib.sha256(
        (
            f"{input_data.execution_plan_id}:"
            f"{input_data.execution_partition_id}:"
            f"{input_data.execution_attempt_id}"
        ).encode()
    ).hexdigest()[:24]
    return f"vmc-p3-{digest}"
