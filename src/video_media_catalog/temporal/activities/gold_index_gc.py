"""EKS Job launch and independent validation for Research index GC/restore."""

from __future__ import annotations

import hashlib
import re
from dataclasses import asdict
from typing import Any
from urllib.parse import urlsplit

from temporalio import activity
from temporalio.exceptions import ApplicationError

from video_media_catalog.canonical import canonical_json_bytes
from video_media_catalog.gold_index_gc import (
    GOLD_INDEX_ARCHIVE_RECEIPT_MEDIA_TYPE,
    GOLD_INDEX_GC_RESULT_MEDIA_TYPE,
    GOLD_INDEX_RESTORE_RECEIPT_MEDIA_TYPE,
    GoldIndexGcResultReceipt,
    GoldIndexRestoreReceipt,
    read_control_model,
    validate_gc_result,
    validate_restore_result,
)
from video_media_catalog.gold_search_index import (
    GOLD_INDEX_ACTIVATION_RECEIPT_MEDIA_TYPE,
    RESEARCH_INDEX_PREFIX,
)
from video_media_catalog.object_store import BoundedObjectStore
from video_media_catalog.opensearch_client import (
    OpenSearchConnection,
    create_opensearch_client,
)
from video_media_catalog.storage import join_uri
from video_media_catalog.temporal.activities.gold_index import (
    _close,
    _discover_ref,
    _image_digest,
    _KubernetesApi,
    _object_ref,
    _payload,
    _wait_for_job,
)
from video_media_catalog.temporal.gold_index_models import (
    GoldIndexGcInput,
    GoldIndexRestoreInput,
    GoldIndexRuntimeJobResult,
)
from video_media_catalog.temporal.models import ObjectRefPayload

_SAFE_ID = re.compile(r"^[a-z0-9][a-z0-9-]{0,62}$")
_SAFE_NAME = re.compile(r"^[a-z0-9][a-z0-9_-]{0,254}$")
_RUNTIME_UID = 10001
_CONFLICTING_JOB_WORKLOADS = frozenset(
    {
        ("research-gold-index", "builder"),
        ("catalog-gold-match", "matcher"),
        ("research-gold-index", "gc"),
        ("research-gold-index", "restore"),
    }
)


def _validate_ref(
    reference: ObjectRefPayload,
    *,
    label: str,
    media_type: str,
) -> None:
    if (
        reference.size_bytes < 1
        or reference.format != "OBJECT_FORMAT_JSON"
        or reference.media_type != media_type
    ):
        raise ValueError(f"{label} must be a non-empty JSON control object")
    reference.checksum_digest()
    if reference.uri.startswith("s3://") and (
        not reference.etag or not reference.object_version
    ):
        raise ValueError(f"S3 {label} must be version and ETag pinned")


def _validate_env(input_data: GoldIndexGcInput | GoldIndexRestoreInput) -> None:
    env = input_data.env
    if _SAFE_ID.fullmatch(input_data.operation_id) is None:
        raise ValueError("operation_id must be a safe lowercase identifier")
    for value, label in (
        (env.kubernetes_namespace, "Kubernetes namespace"),
        (env.gc_service_account, "GC service account"),
    ):
        if _SAFE_ID.fullmatch(value) is None:
            raise ValueError(f"{label} is invalid")
    _image_digest(env.gc_image)
    OpenSearchConnection(
        endpoint=env.opensearch_endpoint,
        aws_region=env.aws_region,
        service=env.opensearch_service,
        timeout_seconds=float(env.request_timeout_seconds),
    )
    for value, label in (
        (env.archive_prefix, "archive prefix"),
        (env.gc_result_prefix, "GC result prefix"),
        (env.restore_prefix, "restore prefix"),
    ):
        parsed = urlsplit(value)
        if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.lstrip("/"):
            raise ValueError(f"{label} must be a non-empty s3:// URI")
    if not 60 <= env.active_deadline_seconds <= 86_400:
        raise ValueError("active deadline must be between 60 seconds and 24 hours")
    if env.request_timeout_seconds < 1:
        raise ValueError("OpenSearch request timeout must be positive")
    if env.snapshot_timeout_seconds < 1 or env.snapshot_poll_seconds < 1:
        raise ValueError("snapshot timeout and poll interval must be positive")


def _validate_gc_input(input_data: GoldIndexGcInput) -> None:
    _validate_env(input_data)
    _validate_ref(
        input_data.activation_receipt,
        label="activation receipt",
        media_type=GOLD_INDEX_ACTIVATION_RECEIPT_MEDIA_TYPE,
    )
    for value, label in (
        (input_data.repository, "snapshot repository"),
        (input_data.snapshot, "snapshot name"),
        (input_data.old_index, "old index"),
        (input_data.active_index, "active index"),
    ):
        if _SAFE_NAME.fullmatch(value) is None:
            raise ValueError(f"{label} is invalid")
    if not all(
        value.startswith(RESEARCH_INDEX_PREFIX + "-")
        for value in (input_data.old_index, input_data.active_index)
    ):
        raise ValueError("GC indices must belong to the Research index family")
    if input_data.old_index == input_data.active_index:
        raise ValueError("old and active indices must differ")
    if (
        isinstance(input_data.delete_before_epoch_millis, bool)
        or input_data.delete_before_epoch_millis < 0
    ):
        raise ValueError("retention cutoff must be non-negative epoch milliseconds")
    if tuple(sorted(set(input_data.referenced_indices))) != (
        input_data.referenced_indices
    ):
        raise ValueError("referenced indices must be unique and sorted")
    if any(
        _SAFE_NAME.fullmatch(value) is None
        or not value.startswith(RESEARCH_INDEX_PREFIX + "-")
        for value in input_data.referenced_indices
    ):
        raise ValueError("referenced indices must belong to the Research family")
    if input_data.reuse_archive_receipt is not None:
        _validate_ref(
            input_data.reuse_archive_receipt,
            label="reused archive receipt",
            media_type=GOLD_INDEX_ARCHIVE_RECEIPT_MEDIA_TYPE,
        )


def _validate_restore_input(input_data: GoldIndexRestoreInput) -> None:
    _validate_env(input_data)
    _validate_ref(
        input_data.archive_receipt,
        label="archive receipt",
        media_type=GOLD_INDEX_ARCHIVE_RECEIPT_MEDIA_TYPE,
    )


def _archive_receipt_uri(input_data: GoldIndexGcInput) -> str:
    return join_uri(
        input_data.env.archive_prefix,
        f"gold-index-archive-{input_data.operation_id}.json",
    )


def _gc_result_uri(input_data: GoldIndexGcInput) -> str:
    return join_uri(
        input_data.env.gc_result_prefix,
        f"gold-index-gc-{input_data.operation_id}.json",
    )


def _restore_receipt_uri(input_data: GoldIndexRestoreInput) -> str:
    return join_uri(
        input_data.env.restore_prefix,
        f"gold-index-restore-{input_data.operation_id}.json",
    )


def _object_args(prefix: str, reference: ObjectRefPayload) -> list[str]:
    arguments = [
        f"--{prefix}-uri",
        reference.uri,
        f"--{prefix}-hash",
        reference.checksum_value,
        f"--{prefix}-size",
        str(reference.size_bytes),
    ]
    if reference.object_version:
        arguments.extend(
            [f"--{prefix}-version", reference.object_version]
        )
    if reference.etag:
        arguments.extend([f"--{prefix}-etag", reference.etag])
    return arguments


def _connection_args(
    input_data: GoldIndexGcInput | GoldIndexRestoreInput,
) -> list[str]:
    env = input_data.env
    return [
        "--aws-region",
        env.aws_region,
        "--opensearch-endpoint",
        env.opensearch_endpoint,
        "--opensearch-service",
        env.opensearch_service,
        "--request-timeout-seconds",
        str(env.request_timeout_seconds),
    ]


def _gc_job_args(
    input_data: GoldIndexGcInput,
    *,
    completed_at: str,
) -> list[str]:
    env = input_data.env
    arguments = [
        "archive-delete",
        *_connection_args(input_data),
        "--operation-id",
        input_data.operation_id,
        "--repository",
        input_data.repository,
        "--snapshot",
        input_data.snapshot,
        "--old-index",
        input_data.old_index,
        "--active-index",
        input_data.active_index,
        *_object_args("activation-receipt", input_data.activation_receipt),
        "--archive-receipt-uri",
        _archive_receipt_uri(input_data),
        "--gc-result-uri",
        _gc_result_uri(input_data),
        "--completed-at",
        completed_at,
        "--delete-before-epoch-millis",
        str(input_data.delete_before_epoch_millis),
        "--snapshot-timeout-seconds",
        str(env.snapshot_timeout_seconds),
        "--snapshot-poll-seconds",
        str(env.snapshot_poll_seconds),
        "--execute",
    ]
    for index_name in input_data.referenced_indices:
        arguments.extend(["--referenced-index", index_name])
    if input_data.reuse_archive_receipt is not None:
        arguments.extend(
            _object_args(
                "reuse-archive-receipt",
                input_data.reuse_archive_receipt,
            )
        )
    return arguments


def _restore_job_args(
    input_data: GoldIndexRestoreInput,
    *,
    completed_at: str,
) -> list[str]:
    env = input_data.env
    return [
        "restore-drill",
        *_connection_args(input_data),
        "--operation-id",
        input_data.operation_id,
        *_object_args("archive-receipt", input_data.archive_receipt),
        "--restore-receipt-uri",
        _restore_receipt_uri(input_data),
        "--completed-at",
        completed_at,
        "--restore-timeout-seconds",
        str(env.snapshot_timeout_seconds),
        "--restore-poll-seconds",
        str(env.snapshot_poll_seconds),
    ]


def _job_name(
    kind: str,
    input_data: GoldIndexGcInput | GoldIndexRestoreInput,
    *,
    completed_at: str,
) -> str:
    suffix = hashlib.sha256(
        canonical_json_bytes(
            {
                "kind": kind,
                "input": asdict(input_data),
                "completedAt": completed_at,
            }
        )
    ).hexdigest()[:12]
    max_prefix = 63 - len(f"research-index-{kind}--{suffix}")
    prefix = input_data.operation_id[:max_prefix].rstrip("-")
    return f"research-index-{kind}-{prefix}-{suffix}"


def _runtime_job_manifest(
    *,
    kind: str,
    input_data: GoldIndexGcInput | GoldIndexRestoreInput,
    completed_at: str,
    arguments: list[str],
) -> dict[str, Any]:
    env = input_data.env
    name = _job_name(kind, input_data, completed_at=completed_at)
    input_digest = "sha256:" + hashlib.sha256(
        canonical_json_bytes(
            {
                "kind": kind,
                "input": asdict(input_data),
                "completedAt": completed_at,
            }
        )
    ).hexdigest()
    labels = {
        "app.kubernetes.io/name": "research-gold-index",
        "app.kubernetes.io/component": kind,
        "video-world.io/operation-id": input_data.operation_id,
    }
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": name,
            "namespace": env.kubernetes_namespace,
            "labels": labels,
            "annotations": {
                "video-world.io/input-digest": input_digest,
                "video-world.io/runtime-operation": kind,
            },
        },
        "spec": {
            "backoffLimit": 0,
            "activeDeadlineSeconds": env.active_deadline_seconds,
            "ttlSecondsAfterFinished": 604_800,
            "template": {
                "metadata": {"labels": labels},
                "spec": {
                    "serviceAccountName": env.gc_service_account,
                    "automountServiceAccountToken": True,
                    "restartPolicy": "Never",
                    "terminationGracePeriodSeconds": 30,
                    "securityContext": {
                        "runAsNonRoot": True,
                        "runAsUser": _RUNTIME_UID,
                        "runAsGroup": _RUNTIME_UID,
                        "fsGroup": _RUNTIME_UID,
                        "seccompProfile": {"type": "RuntimeDefault"},
                    },
                    "nodeSelector": dict(sorted(env.node_selector.items())),
                    "containers": [
                        {
                            "name": kind,
                            "image": env.gc_image,
                            "imagePullPolicy": "IfNotPresent",
                            "command": ["video-media-catalog-gold-index-gc"],
                            "args": arguments,
                            "workingDir": "/work",
                            "env": [
                                {"name": "AWS_REGION", "value": env.aws_region},
                                {
                                    "name": "AWS_DEFAULT_REGION",
                                    "value": env.aws_region,
                                },
                                {"name": "HOME", "value": "/work"},
                                {"name": "TMPDIR", "value": "/work/tmp"},
                            ],
                            "resources": {
                                "requests": {
                                    "cpu": env.cpu_request,
                                    "memory": env.memory_request,
                                    "ephemeral-storage": (
                                        env.ephemeral_storage_request
                                    ),
                                },
                                "limits": {
                                    "cpu": env.cpu_limit,
                                    "memory": env.memory_limit,
                                    "ephemeral-storage": (
                                        env.ephemeral_storage_limit
                                    ),
                                },
                            },
                            "securityContext": {
                                "allowPrivilegeEscalation": False,
                                "readOnlyRootFilesystem": True,
                                "runAsNonRoot": True,
                                "runAsUser": _RUNTIME_UID,
                                "capabilities": {"drop": ["ALL"]},
                            },
                            "terminationMessagePolicy": "FallbackToLogsOnError",
                            "volumeMounts": [
                                {"name": "work", "mountPath": "/work"},
                                {"name": "tmp", "mountPath": "/work/tmp"},
                                {"name": "tmp", "mountPath": "/tmp"},
                            ],
                        }
                    ],
                    "volumes": [
                        {"name": "work", "emptyDir": {}},
                        {"name": "tmp", "emptyDir": {}},
                    ],
                },
            },
        },
    }


def _gc_job_manifest(
    input_data: GoldIndexGcInput,
    *,
    completed_at: str,
) -> dict[str, Any]:
    _validate_gc_input(input_data)
    return _runtime_job_manifest(
        kind="gc",
        input_data=input_data,
        completed_at=completed_at,
        arguments=_gc_job_args(input_data, completed_at=completed_at),
    )


def _restore_job_manifest(
    input_data: GoldIndexRestoreInput,
    *,
    completed_at: str,
) -> dict[str, Any]:
    _validate_restore_input(input_data)
    return _runtime_job_manifest(
        kind="restore",
        input_data=input_data,
        completed_at=completed_at,
        arguments=_restore_job_args(input_data, completed_at=completed_at),
    )


def _launch(
    *,
    manifest: dict[str, Any],
    namespace: str,
) -> str:
    api = _KubernetesApi()
    job_name = str(manifest["metadata"]["name"])
    labels = manifest["metadata"]["labels"]
    expected_workload = (
        str(labels["app.kubernetes.io/name"]),
        str(labels["app.kubernetes.io/component"]),
    )
    api.assert_no_conflicting_jobs(
        namespace=namespace,
        expected_name=job_name,
        expected_workload=expected_workload,
        blocked_workloads=_CONFLICTING_JOB_WORKLOADS,
    )
    api.create_or_get_job(manifest)
    _wait_for_job(api, namespace=namespace, name=job_name)
    return job_name


@activity.defn(name="SubmitGoldIndexGcJob")
def submit_gold_index_gc_job(
    input_data: GoldIndexGcInput,
    completed_at: str,
) -> GoldIndexRuntimeJobResult:
    manifest = _gc_job_manifest(input_data, completed_at=completed_at)
    job_name = _launch(
        manifest=manifest,
        namespace=input_data.env.kubernetes_namespace,
    )
    store = BoundedObjectStore(region=input_data.env.aws_region)
    receipt_ref = _discover_ref(
        store,
        uri=_gc_result_uri(input_data),
        media_type=GOLD_INDEX_GC_RESULT_MEDIA_TYPE,
    )
    receipt = read_control_model(
        store,
        receipt_ref,
        media_type=GOLD_INDEX_GC_RESULT_MEDIA_TYPE,
        model=GoldIndexGcResultReceipt,
    )
    if (
        receipt.operation_id != input_data.operation_id
        or receipt.old_index != input_data.old_index
        or receipt.active_index != input_data.active_index
        or receipt.repository != input_data.repository
        or receipt.snapshot != input_data.snapshot
        or receipt.activation_receipt
        != _object_ref(input_data.activation_receipt)
    ):
        raise ApplicationError(
            "GC Job returned a receipt for a different operation",
            non_retryable=True,
        )
    return GoldIndexRuntimeJobResult(
        job_name=job_name,
        receipt=_payload(receipt_ref),
    )


@activity.defn(name="SubmitGoldIndexRestoreJob")
def submit_gold_index_restore_job(
    input_data: GoldIndexRestoreInput,
    completed_at: str,
) -> GoldIndexRuntimeJobResult:
    manifest = _restore_job_manifest(input_data, completed_at=completed_at)
    job_name = _launch(
        manifest=manifest,
        namespace=input_data.env.kubernetes_namespace,
    )
    store = BoundedObjectStore(region=input_data.env.aws_region)
    receipt_ref = _discover_ref(
        store,
        uri=_restore_receipt_uri(input_data),
        media_type=GOLD_INDEX_RESTORE_RECEIPT_MEDIA_TYPE,
    )
    receipt = read_control_model(
        store,
        receipt_ref,
        media_type=GOLD_INDEX_RESTORE_RECEIPT_MEDIA_TYPE,
        model=GoldIndexRestoreReceipt,
    )
    if receipt.operation_id != input_data.operation_id:
        raise ApplicationError(
            "restore Job returned a receipt for a different operation",
            non_retryable=True,
        )
    return GoldIndexRuntimeJobResult(
        job_name=job_name,
        receipt=_payload(receipt_ref),
    )


def _client(
    input_data: GoldIndexGcInput | GoldIndexRestoreInput,
) -> Any:
    return create_opensearch_client(
        OpenSearchConnection(
            endpoint=input_data.env.opensearch_endpoint,
            aws_region=input_data.env.aws_region,
            service=input_data.env.opensearch_service,
            timeout_seconds=float(input_data.env.request_timeout_seconds),
        )
    )


@activity.defn(name="ValidateGoldIndexGc")
def validate_gold_index_gc(
    input_data: GoldIndexGcInput,
    job_result: GoldIndexRuntimeJobResult,
) -> ObjectRefPayload:
    _validate_gc_input(input_data)
    store = BoundedObjectStore(region=input_data.env.aws_region)
    receipt_ref = _object_ref(job_result.receipt)
    receipt = read_control_model(
        store,
        receipt_ref,
        media_type=GOLD_INDEX_GC_RESULT_MEDIA_TYPE,
        model=GoldIndexGcResultReceipt,
    )
    if (
        receipt.operation_id != input_data.operation_id
        or receipt.old_index != input_data.old_index
        or receipt.active_index != input_data.active_index
        or receipt.repository != input_data.repository
        or receipt.snapshot != input_data.snapshot
        or receipt.activation_receipt
        != _object_ref(input_data.activation_receipt)
    ):
        raise ApplicationError("GC receipt operation mismatch", non_retryable=True)
    client = _client(input_data)
    try:
        validate_gc_result(client, store, receipt)
    finally:
        _close(client)
    return job_result.receipt


@activity.defn(name="ValidateGoldIndexRestore")
def validate_gold_index_restore(
    input_data: GoldIndexRestoreInput,
    job_result: GoldIndexRuntimeJobResult,
) -> ObjectRefPayload:
    _validate_restore_input(input_data)
    store = BoundedObjectStore(region=input_data.env.aws_region)
    receipt_ref = _object_ref(job_result.receipt)
    receipt = read_control_model(
        store,
        receipt_ref,
        media_type=GOLD_INDEX_RESTORE_RECEIPT_MEDIA_TYPE,
        model=GoldIndexRestoreReceipt,
    )
    if (
        receipt.operation_id != input_data.operation_id
        or receipt.archive_receipt != _object_ref(input_data.archive_receipt)
    ):
        raise ApplicationError(
            "restore receipt operation mismatch",
            non_retryable=True,
        )
    client = _client(input_data)
    try:
        validate_restore_result(client, store, receipt)
    finally:
        _close(client)
    return job_result.receipt
