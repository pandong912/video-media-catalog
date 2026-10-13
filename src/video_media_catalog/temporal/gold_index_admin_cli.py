"""Administrative entrypoint for Research Gold projection, GC, and restore."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import timedelta
from typing import Any

from temporalio.client import Client
from temporalio.common import WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError

from video_media_catalog.temporal.gold_index_models import (
    GoldIndexGcEnv,
    GoldIndexGcInput,
    GoldIndexProjectionEnv,
    GoldIndexProjectionInput,
    GoldIndexRestoreInput,
    GoldIndexSnapshotGcInput,
)
from video_media_catalog.temporal.models import ObjectRefPayload
from video_media_catalog.temporal.names import (
    GOLD_INDEX_WORKFLOW_TASK_QUEUE,
    TEMPORAL_NAMESPACE,
)
from video_media_catalog.temporal.workflows.gold_index_gc import (
    GoldIndexGcWorkflow,
    GoldIndexRestoreWorkflow,
    GoldIndexSnapshotGcWorkflow,
)
from video_media_catalog.temporal.workflows.gold_index_projection import (
    GoldIndexProjectionWorkflow,
)


def _env_from_mapping(values: dict[str, Any]) -> GoldIndexProjectionEnv:
    return GoldIndexProjectionEnv(
        aws_region=str(values["awsRegion"]),
        kubernetes_namespace=str(values["kubernetesNamespace"]),
        builder_service_account=str(values["builderServiceAccount"]),
        builder_image=str(values["builderImage"]),
        warehouse_uri=str(values["warehouseUri"]),
        manifest_prefix=str(values["manifestPrefix"]),
        result_prefix=str(values["resultPrefix"]),
        validation_prefix=str(values["validationPrefix"]),
        opensearch_endpoint=str(values["opensearchEndpoint"]),
        opensearch_service=str(values.get("opensearchService", "es")),
        catalog_type=str(values.get("catalogType", "glue")),
        catalog_name=str(values.get("catalogName", "media")),
        glue_namespace=str(values.get("glueNamespace", "video_media_catalog")),
        shards=int(values.get("shards", 6)),
        replicas=int(values.get("replicas", 1)),
        bulk_chunk_size=int(values.get("bulkChunkSize", 500)),
        bulk_max_chunk_bytes=int(values.get("bulkMaxChunkBytes", 5 * 1024 * 1024)),
        bulk_partitions=int(values.get("bulkPartitions", 96)),
        bulk_workers=int(values.get("bulkWorkers", 4)),
        spark_master=str(values.get("sparkMaster", "local[*]")),
        spark_driver_memory=str(values.get("sparkDriverMemory", "20g")),
        shuffle_partitions=int(values.get("shufflePartitions", 384)),
        request_timeout_seconds=int(values.get("requestTimeoutSeconds", 30)),
        min_free_storage_gib=int(values.get("minFreeStorageGiB", 100)),
        max_jvm_pressure_percent=int(values.get("maxJvmPressurePercent", 75)),
        max_cluster_pending_tasks=int(values.get("maxClusterPendingTasks", 10)),
        active_deadline_seconds=int(values.get("activeDeadlineSeconds", 43_200)),
        cpu_request=str(values.get("cpuRequest", "4")),
        cpu_limit=str(values.get("cpuLimit", "8")),
        memory_request=str(values.get("memoryRequest", "24Gi")),
        memory_limit=str(values.get("memoryLimit", "32Gi")),
        ephemeral_storage_request=str(values.get("ephemeralStorageRequest", "80Gi")),
        ephemeral_storage_limit=str(values.get("ephemeralStorageLimit", "120Gi")),
        node_selector={
            str(key): str(value)
            for key, value in values.get("nodeSelector", {}).items()
        },
    )


def _gc_env_from_mapping(values: dict[str, Any]) -> GoldIndexGcEnv:
    return GoldIndexGcEnv(
        aws_region=str(values["awsRegion"]),
        kubernetes_namespace=str(values["kubernetesNamespace"]),
        gc_service_account=str(values["gcServiceAccount"]),
        gc_image=str(values["gcImage"]),
        archive_prefix=str(values["archivePrefix"]),
        gc_result_prefix=str(values["gcResultPrefix"]),
        restore_prefix=str(values["restorePrefix"]),
        opensearch_endpoint=str(values["opensearchEndpoint"]),
        opensearch_service=str(values.get("opensearchService", "es")),
        request_timeout_seconds=int(values.get("requestTimeoutSeconds", 30)),
        snapshot_timeout_seconds=int(values.get("snapshotTimeoutSeconds", 7200)),
        snapshot_poll_seconds=int(values.get("snapshotPollSeconds", 15)),
        active_deadline_seconds=int(values.get("activeDeadlineSeconds", 10_800)),
        cpu_request=str(values.get("cpuRequest", "500m")),
        cpu_limit=str(values.get("cpuLimit", "2")),
        memory_request=str(values.get("memoryRequest", "512Mi")),
        memory_limit=str(values.get("memoryLimit", "2Gi")),
        ephemeral_storage_request=str(values.get("ephemeralStorageRequest", "1Gi")),
        ephemeral_storage_limit=str(values.get("ephemeralStorageLimit", "4Gi")),
        node_selector={
            str(key): str(value)
            for key, value in values.get("nodeSelector", {}).items()
        },
    )


def _input_from_mapping(values: dict[str, Any]) -> GoldIndexProjectionInput:
    return GoldIndexProjectionInput(
        projection_id=str(values["projectionId"]),
        expected_release_plan_id=str(values["expectedReleasePlanId"]),
        release_commit=ObjectRefPayload.from_mapping(values["releaseCommit"]),
        env=_env_from_mapping(values["env"]),
    )


def _gc_input_from_mapping(values: dict[str, Any]) -> GoldIndexGcInput:
    raw_receipt = values.get("reuseArchiveReceipt")
    return GoldIndexGcInput(
        operation_id=str(values["operationId"]),
        old_index=str(values["oldIndex"]),
        active_index=str(values["activeIndex"]),
        repository=str(values["repository"]),
        snapshot=str(values["snapshot"]),
        delete_before_epoch_millis=int(values["deleteBeforeEpochMillis"]),
        activation_receipt=ObjectRefPayload.from_mapping(values["activationReceipt"]),
        additional_archive_indices=tuple(
            sorted(str(value) for value in values.get("additionalArchiveIndices", []))
        ),
        referenced_indices=tuple(
            sorted(str(value) for value in values.get("referencedIndices", []))
        ),
        reuse_archive_receipt=(
            None if raw_receipt is None else ObjectRefPayload.from_mapping(raw_receipt)
        ),
        env=_gc_env_from_mapping(values["env"]),
    )


def _restore_input_from_mapping(values: dict[str, Any]) -> GoldIndexRestoreInput:
    return GoldIndexRestoreInput(
        operation_id=str(values["operationId"]),
        archive_receipt=ObjectRefPayload.from_mapping(values["archiveReceipt"]),
        env=_gc_env_from_mapping(values["env"]),
    )


def _snapshot_gc_input_from_mapping(
    values: dict[str, Any],
) -> GoldIndexSnapshotGcInput:
    return GoldIndexSnapshotGcInput(
        operation_id=str(values["operationId"]),
        source_archive_receipt=ObjectRefPayload.from_mapping(
            values["sourceArchiveReceipt"]
        ),
        replacement_snapshot=str(values["replacementSnapshot"]),
        env=_gc_env_from_mapping(values["env"]),
    )


def _load_mapping(path: str, *, label: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"{label} config must be a JSON object")
    return payload


def _load_input(path: str) -> GoldIndexProjectionInput:
    return _input_from_mapping(_load_mapping(path, label="Gold index projection"))


def _load_gc_input(path: str) -> GoldIndexGcInput:
    return _gc_input_from_mapping(_load_mapping(path, label="Gold index GC"))


def _load_restore_input(path: str) -> GoldIndexRestoreInput:
    return _restore_input_from_mapping(_load_mapping(path, label="Gold index restore"))


def _load_snapshot_gc_input(path: str) -> GoldIndexSnapshotGcInput:
    return _snapshot_gc_input_from_mapping(
        _load_mapping(path, label="Gold index snapshot GC")
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="video-media-catalog-gold-index-temporal-admin"
    )
    parser.add_argument(
        "--temporal-address",
        default=os.environ.get(
            "TEMPORAL_ADDRESS",
            "temporal-frontend.temporal.svc.cluster.local:7233",
        ),
    )
    parser.add_argument(
        "--temporal-namespace",
        default=os.environ.get("TEMPORAL_NAMESPACE", TEMPORAL_NAMESPACE),
    )
    parser.add_argument(
        "--task-queue",
        default=os.environ.get(
            "TEMPORAL_GOLD_INDEX_TASK_QUEUE",
            GOLD_INDEX_WORKFLOW_TASK_QUEUE,
        ),
    )
    parser.add_argument(
        "--projection-config-file",
        default=os.environ.get(
            "GOLD_INDEX_PROJECTION_CONFIG_FILE",
            "/config/gold-index-projection.json",
        ),
    )
    parser.add_argument(
        "--gc-config-file",
        default=os.environ.get(
            "GOLD_INDEX_GC_CONFIG_FILE",
            "/config/gold-index-gc.json",
        ),
    )
    parser.add_argument(
        "--restore-config-file",
        default=os.environ.get(
            "GOLD_INDEX_RESTORE_CONFIG_FILE",
            "/config/gold-index-restore.json",
        ),
    )
    parser.add_argument(
        "--snapshot-gc-config-file",
        default=os.environ.get(
            "GOLD_INDEX_SNAPSHOT_GC_CONFIG_FILE",
            "/config/gold-index-snapshot-gc.json",
        ),
    )
    commands = parser.add_subparsers(dest="command", required=True)
    start = commands.add_parser(
        "start-projection",
        help="Start one pinned build and validation without alias activation",
    )
    start.add_argument("--workflow-id")
    start_gc = commands.add_parser(
        "start-gc",
        help="Start one archive, delete, and independent validation",
    )
    start_gc.add_argument("--workflow-id")
    start_restore = commands.add_parser(
        "start-restore",
        help="Start one original-name restore drill and validation",
    )
    start_restore.add_argument("--workflow-id")
    start_snapshot_gc = commands.add_parser(
        "start-snapshot-gc",
        help="Start one previous-only snapshot clone and source snapshot GC",
    )
    start_snapshot_gc.add_argument("--workflow-id")
    return parser


async def start_projection(parsed: argparse.Namespace) -> dict[str, Any]:
    input_data = _load_input(parsed.projection_config_file)
    workflow_id = (
        parsed.workflow_id or f"gold-index-projection-{input_data.projection_id}"
    )
    client = await Client.connect(
        parsed.temporal_address,
        namespace=parsed.temporal_namespace,
    )
    try:
        handle = await client.start_workflow(
            GoldIndexProjectionWorkflow.run,
            input_data,
            id=workflow_id,
            task_queue=parsed.task_queue,
            execution_timeout=timedelta(hours=24),
            id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
        )
    except WorkflowAlreadyStartedError:
        existing = client.get_workflow_handle(workflow_id)
        return {
            "workflowId": workflow_id,
            "runId": existing.result_run_id,
            "started": False,
        }
    return {
        "workflowId": handle.id,
        "runId": handle.result_run_id,
        "started": True,
    }


async def start_gc(parsed: argparse.Namespace) -> dict[str, Any]:
    input_data = _load_gc_input(parsed.gc_config_file)
    workflow_id = parsed.workflow_id or f"gold-index-gc-{input_data.operation_id}"
    client = await Client.connect(
        parsed.temporal_address,
        namespace=parsed.temporal_namespace,
    )
    try:
        handle = await client.start_workflow(
            GoldIndexGcWorkflow.run,
            input_data,
            id=workflow_id,
            task_queue=parsed.task_queue,
            execution_timeout=timedelta(hours=8),
            id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
        )
    except WorkflowAlreadyStartedError:
        existing = client.get_workflow_handle(workflow_id)
        return {
            "workflowId": workflow_id,
            "runId": existing.result_run_id,
            "started": False,
        }
    return {
        "workflowId": handle.id,
        "runId": handle.result_run_id,
        "started": True,
    }


async def start_restore(parsed: argparse.Namespace) -> dict[str, Any]:
    input_data = _load_restore_input(parsed.restore_config_file)
    workflow_id = parsed.workflow_id or f"gold-index-restore-{input_data.operation_id}"
    client = await Client.connect(
        parsed.temporal_address,
        namespace=parsed.temporal_namespace,
    )
    try:
        handle = await client.start_workflow(
            GoldIndexRestoreWorkflow.run,
            input_data,
            id=workflow_id,
            task_queue=parsed.task_queue,
            execution_timeout=timedelta(hours=8),
            id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
        )
    except WorkflowAlreadyStartedError:
        existing = client.get_workflow_handle(workflow_id)
        return {
            "workflowId": workflow_id,
            "runId": existing.result_run_id,
            "started": False,
        }
    return {
        "workflowId": handle.id,
        "runId": handle.result_run_id,
        "started": True,
    }


async def start_snapshot_gc(parsed: argparse.Namespace) -> dict[str, Any]:
    input_data = _load_snapshot_gc_input(parsed.snapshot_gc_config_file)
    workflow_id = (
        parsed.workflow_id or f"gold-index-snapshot-gc-{input_data.operation_id}"
    )
    client = await Client.connect(
        parsed.temporal_address,
        namespace=parsed.temporal_namespace,
    )
    try:
        handle = await client.start_workflow(
            GoldIndexSnapshotGcWorkflow.run,
            input_data,
            id=workflow_id,
            task_queue=parsed.task_queue,
            execution_timeout=timedelta(hours=8),
            id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
        )
    except WorkflowAlreadyStartedError:
        existing = client.get_workflow_handle(workflow_id)
        return {
            "workflowId": workflow_id,
            "runId": existing.result_run_id,
            "started": False,
        }
    return {
        "workflowId": handle.id,
        "runId": handle.result_run_id,
        "started": True,
    }


def main(argv: list[str] | None = None) -> int:
    parsed = build_parser().parse_args(argv)
    if parsed.command == "start-projection":
        result = asyncio.run(start_projection(parsed))
    elif parsed.command == "start-gc":
        result = asyncio.run(start_gc(parsed))
    elif parsed.command == "start-restore":
        result = asyncio.run(start_restore(parsed))
    else:
        result = asyncio.run(start_snapshot_gc(parsed))
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
