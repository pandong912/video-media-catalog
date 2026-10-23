"""P3 Catalog execution activities; all provider and filesystem I/O lives here."""

from __future__ import annotations

import hashlib
import importlib
import json
import os
from importlib import resources
from pathlib import Path
from typing import Any

from temporalio import activity
from temporalio.exceptions import ApplicationError

from video_media_catalog.canonical import canonical_json_bytes
from video_media_catalog.execution_contracts import (
    CATALOG_COMPLETION_RECEIPT_MEDIA_TYPE,
    CatalogBackendCompletionReceipt,
    canonical_semantic_digest,
    validate_execution_data_locality,
)
from video_media_catalog.execution_p4 import (
    AuditEvent,
    CatalogExecutionHooks,
    MetricSample,
    NoopCatalogExecutionHooks,
)
from video_media_catalog.object_store import (
    ExactObjectStoreV2,
    ObjectRefV2WriteTarget,
)
from video_media_catalog.spark_backend import (
    AliyunEmrServerlessSparkBackend,
    AwsEmrServerlessSparkBackend,
    SparkBackendAdapter,
    SparkBackendIdentity,
    SparkBackendObservation,
    SparkCancelReceipt,
    SparkReconcileResult,
    SparkSubmitRequest,
)
from video_media_catalog.temporal.catalog_execution_models import (
    CATALOG_EXECUTION_CONTRACT,
    CatalogExecutionInput,
    CatalogExecutionResult,
    ObjectRefV2Payload,
    PreparedCatalogExecution,
    catalog_execution_client_token,
    catalog_execution_job_name,
)

_LOCK_NAME = "catalog-execution-p3.runtime-lock.v1.json"
_P4_LOCK_NAME = "catalog-execution-p4.runtime-lock.v1.json"
_MAX_COMPLETION_RECEIPT_BYTES = 256 * 1024


def _non_retryable(message: str) -> ApplicationError:
    return ApplicationError(
        message,
        non_retryable=True,
        type="CatalogExecutionContractRejected",
    )


def _runtime_lock_path(name: str = _LOCK_NAME) -> Path:
    source = Path(__file__).resolve().parents[4] / "contracts" / "execution" / name
    if source.is_file():
        return source
    packaged = resources.files("video_media_catalog").joinpath(
        "contracts",
        "execution",
        name,
    )
    return Path(str(packaged))


def load_catalog_execution_runtime_lock() -> dict[str, Any]:
    return json.loads(_runtime_lock_path().read_text(encoding="utf-8"))


def load_catalog_execution_p4_runtime_lock() -> dict[str, Any]:
    return json.loads(_runtime_lock_path(_P4_LOCK_NAME).read_text(encoding="utf-8"))


def validate_catalog_execution_p4_runtime(lock: dict[str, Any]) -> None:
    if lock.get("productionSloClaimAllowed") is not False:
        raise _non_retryable("Catalog P4 cannot claim a production SLO")
    manifest = lock.get("p4Manifest", {})
    if not isinstance(manifest, dict) or manifest.get("present") is not True:
        raise _non_retryable(
            "Platform P4 manifest is not pinned and production activation "
            "remains disabled"
        )
    allowlist = lock.get("platformP4ContractAllowlist")
    allowed_digest_values = (
        allowlist.get("allowedManifestLockBodyDigests", ())
        if isinstance(allowlist, dict)
        else ()
    )
    allowed_digests = (
        set(allowed_digest_values)
        if isinstance(allowed_digest_values, (list, tuple))
        else set()
    )
    if (
        manifest.get("contractVersion") != "video.world.execution-contracts.p4.v1"
        or manifest.get("status") != "PLANNED"
        or manifest.get("claimsRuntimeActivation") is not False
        or manifest.get("claimsProductionSlo") is not False
        or manifest.get("lockBodyDigest") not in allowed_digests
    ):
        raise _non_retryable("Platform P4 contract manifest is not allowlisted")
    if (
        lock.get("status") != "ACTIVE"
        or lock.get("runtimeActivationAllowed") is not True
        or lock.get("claimsRuntimeActivation") is not True
    ):
        raise _non_retryable(
            "Platform P4 manifest is pinned but production activation remains disabled"
        )


def validate_catalog_execution_runtime(
    input_data: CatalogExecutionInput,
    lock: dict[str, Any],
) -> None:
    cross_namespace = lock["crossNamespaceContract"]
    if (
        lock.get("status") != "ACTIVE"
        or lock.get("runtimeActivationAllowed") is not True
        or lock.get("claimsRuntimeActivation") is not True
        or lock.get("p3Manifest", {}).get("present") is not True
    ):
        raise _non_retryable(
            "Platform P3 manifest is pinned but production activation remains disabled"
        )
    if (
        input_data.contract_version != CATALOG_EXECUTION_CONTRACT
        or input_data.platform_namespace != cross_namespace["sourceNamespace"]
        or cross_namespace["targetNamespace"] != "vw-media-catalog-research"
        or cross_namespace["workflowType"] != "CatalogExecutionWorkflowV1"
        or cross_namespace["taskQueue"] != "vw-media-catalog-execution-v1"
        or cross_namespace["contractVersion"]
        != "video.world.catalog-execution-workflow.v1"
        or cross_namespace["catalogRuntimeContractVersion"]
        != CATALOG_EXECUTION_CONTRACT
    ):
        raise _non_retryable("cross-namespace Temporal contract is not allowlisted")
    if input_data.platform_contract_digest not in set(
        cross_namespace["allowedPlatformContractDigests"]
    ):
        raise _non_retryable("Platform contract digest is not allowlisted")
    if input_data.provider_profile.profile_digest not in set(
        lock["providerProfiles"]["catalogOwnedRuntimeProfileDigests"]
    ):
        raise _non_retryable("Catalog-owned provider profile digest is not allowlisted")
    boundary = lock["databaseBoundary"]
    if (
        boundary.get("catalogMayWritePlatformDatabase") is not False
        or boundary.get("platformMayWriteCatalogDatabase") is not False
    ):
        raise _non_retryable("cross-repository database boundary is not fail-closed")


def validate_catalog_execution_shape(input_data: CatalogExecutionInput) -> None:
    profile = input_data.provider_profile.to_contract()
    objects = tuple(item.to_contract() for item in input_data.input_objects)
    if any(
        item.tenant_id != input_data.tenant_id
        or item.project_id != input_data.project_id
        for item in objects
    ):
        raise _non_retryable("input ObjectRef v2 scope differs from execution")
    if sum(item.size_bytes for item in objects) > profile.max_input_bytes:
        raise _non_retryable("input bytes exceed the pinned provider profile")
    transfer = (
        None
        if input_data.transfer_plan is None
        else input_data.transfer_plan.to_contract()
    )
    try:
        validate_execution_data_locality(
            profile=profile,
            input_objects=objects,
            plan_kind=input_data.plan_kind,
            transfer_plan=transfer,
        )
    except ValueError as exc:
        raise _non_retryable(str(exc)) from exc
    target_scheme = "s3://" if profile.object_provider == "AWS_S3" else "oss://"
    if (
        not input_data.completion_receipt_target.uri.startswith(target_scheme)
        or input_data.completion_receipt_target.object_key
        != input_data.completion_receipt_target.uri.split("/", 3)[-1]
        or input_data.expected_result_kind != "EXTERNAL_AUTHORITY"
    ):
        raise _non_retryable(
            "completion receipt target or result kind violates provider authority"
        )


@activity.defn(name="PrepareCatalogExecutionV1")
def prepare_catalog_execution(
    input_data: CatalogExecutionInput,
) -> PreparedCatalogExecution:
    validate_catalog_execution_p4_runtime(load_catalog_execution_p4_runtime_lock())
    lock = load_catalog_execution_runtime_lock()
    validate_catalog_execution_runtime(input_data, lock)
    validate_catalog_execution_shape(input_data)
    return PreparedCatalogExecution(
        input=input_data,
        client_token=catalog_execution_client_token(input_data),
        job_name=catalog_execution_job_name(input_data),
    )


def _load_factory(variable: str) -> Any:
    target = os.environ.get(variable, "")
    if not target or ":" not in target:
        raise RuntimeError(
            f"{variable} must name an allowlisted module:function provider factory"
        )
    module_name, function_name = target.split(":", 1)
    if not module_name.startswith("video_media_catalog."):
        raise RuntimeError(f"{variable} must resolve inside video_media_catalog")
    return getattr(importlib.import_module(module_name), function_name)


def _hooks() -> CatalogExecutionHooks:
    if not os.environ.get("CATALOG_EXECUTION_HOOK_FACTORY"):
        return NoopCatalogExecutionHooks()
    hooks = _load_factory("CATALOG_EXECUTION_HOOK_FACTORY")()
    if not hasattr(hooks, "record_metric") or not hasattr(hooks, "record_audit"):
        raise RuntimeError("Catalog execution hook factory returned an invalid hook")
    return hooks


def _labels(prepared: PreparedCatalogExecution) -> tuple[tuple[str, str], ...]:
    profile = prepared.input.provider_profile
    return (
        ("backend", profile.backend_kind),
        ("cloud", profile.cloud_provider),
        ("profile", profile.profile_key),
    )


def _record(
    prepared: PreparedCatalogExecution,
    *,
    metric: str,
    value: int,
    event_type: str,
    outcome: str,
    backend_job_id: str | None = None,
    evidence_digest: str | None = None,
) -> None:
    hooks = _hooks()
    hooks.record_metric(
        MetricSample(
            name=metric,
            value=value,
            labels=_labels(prepared),
        )
    )
    hooks.record_audit(
        AuditEvent(
            event_type=event_type,
            outcome=outcome,
            execution_attempt_id=prepared.input.execution_attempt_id,
            backend_job_id=backend_job_id,
            evidence_digest=evidence_digest,
        )
    )


def _backend(prepared: PreparedCatalogExecution) -> SparkBackendAdapter:
    profile = prepared.input.provider_profile
    if profile.backend_kind == "AWS_EMR_SERVERLESS":
        import boto3

        return AwsEmrServerlessSparkBackend(
            boto3.client("emr-serverless", region_name=profile.region)
        )
    client = _load_factory("CATALOG_ALIYUN_EMR_CLIENT_FACTORY")(profile)
    return AliyunEmrServerlessSparkBackend(client)


def _store(prepared: PreparedCatalogExecution) -> ExactObjectStoreV2:
    profile = prepared.input.provider_profile
    if profile.object_provider == "AWS_S3":
        return ExactObjectStoreV2(aws_region=profile.region)
    client = _load_factory("CATALOG_ALIYUN_OSS_CLIENT_FACTORY")(profile)
    return ExactObjectStoreV2(oss_client=client)


def _heartbeat_identity(
    prepared: PreparedCatalogExecution,
) -> SparkBackendIdentity | None:
    if not activity.in_activity():
        return None
    heartbeat_details = getattr(activity.info(), "heartbeat_details", ())
    if isinstance(heartbeat_details, dict):
        heartbeat_details = (heartbeat_details,)
    for details in reversed(tuple(heartbeat_details)):
        if (
            isinstance(details, dict)
            and details.get("phase") == "catalog-spark-submitted"
            and details.get("clientToken") == prepared.client_token
            and details.get("provider") == prepared.input.provider_profile.backend_kind
        ):
            return SparkBackendIdentity(
                provider=details["provider"],
                application_id=details["applicationId"],
                job_run_id=details["jobRunId"],
                job_run_arn=details.get("jobRunArn"),
            )
    return None


def _heartbeat_submitted(
    prepared: PreparedCatalogExecution,
    identity: SparkBackendIdentity,
) -> None:
    if activity.in_activity():
        activity.heartbeat(
            {
                "phase": "catalog-spark-submitted",
                "clientToken": prepared.client_token,
                "provider": identity.provider,
                "applicationId": identity.application_id,
                "jobRunId": identity.job_run_id,
                "jobRunArn": identity.job_run_arn,
            }
        )


@activity.defn(name="SubmitCatalogSparkV1")
def submit_catalog_spark(
    prepared: PreparedCatalogExecution,
) -> SparkBackendIdentity:
    profile = prepared.input.provider_profile
    spec = prepared.input.job_spec
    request = SparkSubmitRequest(
        provider=profile.backend_kind,
        application_id=profile.application_id,
        workload_identity_reference=(profile.workload_identity.identity_reference),
        client_token=prepared.client_token,
        job_name=prepared.job_name,
        entry_point=spec.entry_point,
        entry_point_arguments=spec.entry_point_arguments,
        spark_submit_parameters=spec.spark_submit_parameters,
        execution_timeout_minutes=spec.execution_timeout_minutes,
        max_attempts=spec.max_attempts,
        log_uri=spec.log_uri,
    )
    backend = _backend(prepared)
    identity = _heartbeat_identity(prepared)
    if identity is not None:
        reconciled = backend.reconcile(identity)
        _heartbeat_submitted(prepared, reconciled.identity)
        _record(
            prepared,
            metric="catalog_execution_submit_resumed_total",
            value=1,
            event_type="SUBMIT_RESUMED",
            outcome=reconciled.disposition,
            backend_job_id=identity.job_run_id,
            evidence_digest=reconciled.reconcile_digest,
        )
        return reconciled.identity
    identity = backend.submit(request)
    _heartbeat_submitted(prepared, identity)
    _record(
        prepared,
        metric="catalog_execution_submit_total",
        value=1,
        event_type="SUBMIT",
        outcome="ACCEPTED",
        backend_job_id=identity.job_run_id,
    )
    return identity


@activity.defn(name="ReconcileCatalogSparkV1")
def reconcile_catalog_spark(
    prepared: PreparedCatalogExecution,
    identity: SparkBackendIdentity,
) -> SparkReconcileResult:
    provider_result = _backend(prepared).reconcile(identity)
    provider_observation = provider_result.observation
    result = SparkReconcileResult.create(
        SparkBackendObservation(
            identity=provider_observation.identity,
            provider_state=provider_observation.provider_state,
            outcome=provider_observation.outcome,
            state_details=provider_observation.state_details,
            observed_at=provider_observation.observed_at,
            resource_evidence_digest=(
                provider_observation.bound_resource_evidence_digest
            ),
            cost_evidence_digest=provider_observation.bound_cost_evidence_digest,
        )
    )
    if activity.in_activity():
        activity.heartbeat(
            {
                "phase": "catalog-spark-reconciled",
                "provider": identity.provider,
                "applicationId": identity.application_id,
                "jobRunId": identity.job_run_id,
                "disposition": result.disposition,
                "reconcileDigest": result.reconcile_digest,
            }
        )
    _record(
        prepared,
        metric="catalog_execution_reconcile_total",
        value=1,
        event_type="ORPHAN_RECONCILE",
        outcome=result.disposition,
        backend_job_id=identity.job_run_id,
        evidence_digest=result.reconcile_digest,
    )
    return result


@activity.defn(name="ObserveCatalogSparkV1")
def observe_catalog_spark(
    prepared: PreparedCatalogExecution,
    identity: SparkBackendIdentity,
) -> SparkBackendObservation:
    observation = _backend(prepared).observe(identity)
    if activity.in_activity():
        activity.heartbeat(
            {
                "phase": "catalog-spark-observed",
                "provider": identity.provider,
                "applicationId": identity.application_id,
                "jobRunId": identity.job_run_id,
                "state": observation.provider_state,
                "evidenceDigest": observation.evidence_digest,
            }
        )
    _record(
        prepared,
        metric="catalog_execution_observe_total",
        value=1,
        event_type="OBSERVE",
        outcome=observation.outcome,
        backend_job_id=identity.job_run_id,
        evidence_digest=observation.evidence_digest,
    )
    if observation.resource_evidence is not None:
        hooks = _hooks()
        for name, value in (
            (
                "catalog_execution_duration_milliseconds",
                observation.resource_evidence.duration_milliseconds,
            ),
            (
                "catalog_execution_input_bytes",
                observation.resource_evidence.input_bytes,
            ),
            (
                "catalog_execution_output_bytes",
                observation.resource_evidence.output_bytes,
            ),
            (
                "catalog_execution_shuffle_read_bytes",
                observation.resource_evidence.shuffle_read_bytes,
            ),
            (
                "catalog_execution_shuffle_write_bytes",
                observation.resource_evidence.shuffle_write_bytes,
            ),
        ):
            hooks.record_metric(
                MetricSample(name=name, value=value, labels=_labels(prepared))
            )
    # Provider SDK payloads can contain datetimes and other non-JSON values.
    # Keep Workflow history bounded and provider-neutral.
    return SparkBackendObservation(
        identity=observation.identity,
        provider_state=observation.provider_state,
        outcome=observation.outcome,
        state_details=observation.state_details,
        observed_at=observation.observed_at,
        resource_evidence_digest=observation.bound_resource_evidence_digest,
        cost_evidence_digest=observation.bound_cost_evidence_digest,
    )


@activity.defn(name="CancelCatalogSparkV1")
def cancel_catalog_spark(
    prepared: PreparedCatalogExecution,
    identity: SparkBackendIdentity,
    reason: str,
) -> SparkCancelReceipt:
    receipt = _backend(prepared).cancel(identity, reason=reason)
    _record(
        prepared,
        metric="catalog_execution_cancel_total",
        value=1,
        event_type="CANCEL",
        outcome=receipt.provider_state,
        backend_job_id=identity.job_run_id,
    )
    return receipt


@activity.defn(name="WriteCatalogCompletionReceiptV1")
def write_catalog_completion_receipt(
    prepared: PreparedCatalogExecution,
    identity: SparkBackendIdentity,
    observation: SparkBackendObservation,
) -> CatalogExecutionResult:
    if not observation.terminal or observation.outcome == "RUNNING":
        raise _non_retryable("completion receipt requires a terminal observation")
    if not observation.observed_at:
        raise _non_retryable("terminal observation must carry provider time")
    summary = {
        "applicationId": identity.application_id,
        "costEvidenceDigest": (observation.bound_cost_evidence_digest or None),
        "jobRunId": identity.job_run_id,
        "observationEvidenceDigest": observation.evidence_digest,
        "outcome": observation.outcome,
        "provider": identity.provider,
        "providerState": observation.provider_state,
        "resourceEvidenceDigest": (observation.bound_resource_evidence_digest or None),
        "stateDetails": observation.state_details,
    }
    summary_bytes = canonical_json_bytes(summary, newline=True)
    summary_digest = hashlib.sha256(summary_bytes).hexdigest()
    authority_result_digest = canonical_semantic_digest(
        "video.media-catalog.external-authority-result.v1",
        identity.provider,
        identity.application_id,
        identity.job_run_id,
        observation.outcome,
        summary_digest,
    )
    input_data = prepared.input
    receipt = CatalogBackendCompletionReceipt.create(
        tenant_id=input_data.tenant_id,
        project_id=input_data.project_id,
        execution_plan_id=input_data.execution_plan_id,
        execution_partition_id=input_data.execution_partition_id,
        execution_attempt_id=input_data.execution_attempt_id,
        provider_profile_digest=input_data.provider_profile.profile_digest,
        backend_kind=input_data.provider_profile.backend_kind,
        backend_application_id=identity.application_id,
        backend_job_id=identity.job_run_id,
        terminal_state=observation.outcome,
        authority_result_digest=authority_result_digest,
        summary_digest=summary_digest,
        observed_at=observation.observed_at,
    )
    payload = receipt.json_bytes()
    if len(payload) > _MAX_COMPLETION_RECEIPT_BYTES:
        raise _non_retryable("completion receipt exceeds its byte limit")
    target = input_data.completion_receipt_target
    published = _store(prepared).publish_bytes(
        payload,
        ObjectRefV2WriteTarget(
            object_ref_id=target.object_ref_id,
            tenant_id=input_data.tenant_id,
            project_id=input_data.project_id,
            data_plane_binding_id=target.data_plane_binding_id,
            data_plane_id=target.data_plane_id,
            provider=input_data.provider_profile.object_provider,
            uri=target.uri,
            object_key=target.object_key,
            format=target.format,
            media_type=CATALOG_COMPLETION_RECEIPT_MEDIA_TYPE,
            schema_digest=target.schema_digest,
            encryption_context_digest=target.encryption_context_digest,
            created_at=observation.observed_at,
        ),
        max_bytes=min(
            input_data.provider_profile.max_output_bytes,
            _MAX_COMPLETION_RECEIPT_BYTES,
        ),
    )
    result = CatalogExecutionResult(
        state=observation.outcome,
        backend_application_id=identity.application_id,
        backend_job_id=identity.job_run_id,
        completion_receipt=ObjectRefV2Payload.from_contract(published.object_ref),
        completion_receipt_digest=receipt.semantic_digest,
        resource_evidence_digest=(observation.bound_resource_evidence_digest or None),
        cost_evidence_digest=(observation.bound_cost_evidence_digest or None),
    )
    _record(
        prepared,
        metric="catalog_execution_completion_receipt_total",
        value=1,
        event_type="COMPLETION_RECEIPT",
        outcome=observation.outcome,
        backend_job_id=identity.job_run_id,
        evidence_digest=receipt.semantic_digest,
    )
    return result
