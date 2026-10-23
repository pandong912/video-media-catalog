"""Least-privilege worker roles for CatalogExecutionWorkflowV1."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from typing import Any

from temporalio.client import Client
from temporalio.worker import Worker

from video_media_catalog.execution_p4 import (
    CatalogExecutionHealth,
    evaluate_catalog_execution_health,
)
from video_media_catalog.temporal.activities.catalog_execution import (
    cancel_catalog_spark,
    load_catalog_execution_p4_runtime_lock,
    load_catalog_execution_runtime_lock,
    observe_catalog_spark,
    prepare_catalog_execution,
    reconcile_catalog_spark,
    submit_catalog_spark,
    write_catalog_completion_receipt,
)
from video_media_catalog.temporal.names import (
    CATALOG_EXECUTION_ALIYUN_TASK_QUEUE,
    CATALOG_EXECUTION_AWS_TASK_QUEUE,
    CATALOG_EXECUTION_WORKFLOW_TASK_QUEUE,
    TEMPORAL_NAMESPACE,
)
from video_media_catalog.temporal.workflows.catalog_execution import (
    CatalogExecutionWorkflowV1,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="video-media-catalog-execution-temporal-worker"
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
        "--role",
        choices=("orchestrator", "aws-emr", "aliyun-emr"),
        default=os.environ.get("CATALOG_EXECUTION_WORKER_ROLE", "orchestrator"),
    )
    parser.add_argument(
        "--activity-executor-workers",
        type=int,
        default=int(os.environ.get("TEMPORAL_ACTIVITY_EXECUTOR_WORKERS", "4")),
    )
    parser.add_argument(
        "--max-provider-activities",
        type=int,
        default=int(os.environ.get("TEMPORAL_MAX_PROVIDER_ACTIVITIES", "4")),
    )
    parser.add_argument("--health-check", action="store_true")
    return parser


def catalog_execution_health() -> CatalogExecutionHealth:
    return evaluate_catalog_execution_health(
        load_catalog_execution_runtime_lock(),
        load_catalog_execution_p4_runtime_lock(),
    )


def assert_p3_runtime_enabled() -> None:
    lock = load_catalog_execution_runtime_lock()
    p4_lock = load_catalog_execution_p4_runtime_lock()
    if (
        lock.get("status") != "ACTIVE"
        or lock.get("runtimeActivationAllowed") is not True
        or lock.get("claimsRuntimeActivation") is not True
        or lock.get("p3Manifest", {}).get("present") is not True
        or p4_lock.get("status") != "ACTIVE"
        or p4_lock.get("runtimeActivationAllowed") is not True
        or p4_lock.get("claimsRuntimeActivation") is not True
        or p4_lock.get("p4Manifest", {}).get("present") is not True
    ):
        raise RuntimeError(
            "Catalog execution manifests are pinned but production activation "
            "receipts are absent; production activation remains disabled"
        )


def worker_registration(role: str) -> dict[str, Any]:
    if role == "orchestrator":
        return {
            "task_queue": CATALOG_EXECUTION_WORKFLOW_TASK_QUEUE,
            "workflows": [CatalogExecutionWorkflowV1],
            "activities": [
                prepare_catalog_execution,
                write_catalog_completion_receipt,
            ],
        }
    if role == "aws-emr":
        return {
            "task_queue": CATALOG_EXECUTION_AWS_TASK_QUEUE,
            "workflows": [],
            "activities": [
                submit_catalog_spark,
                reconcile_catalog_spark,
                observe_catalog_spark,
                cancel_catalog_spark,
            ],
        }
    if role == "aliyun-emr":
        return {
            "task_queue": CATALOG_EXECUTION_ALIYUN_TASK_QUEUE,
            "workflows": [],
            "activities": [
                submit_catalog_spark,
                reconcile_catalog_spark,
                observe_catalog_spark,
                cancel_catalog_spark,
            ],
        }
    raise ValueError(f"unsupported Catalog execution worker role {role!r}")


async def run_worker(parsed: argparse.Namespace) -> None:
    if parsed.activity_executor_workers < 1:
        raise ValueError("activity-executor-workers must be positive")
    if parsed.max_provider_activities < 1:
        raise ValueError("max-provider-activities must be positive")
    assert_p3_runtime_enabled()
    registration = worker_registration(parsed.role)
    client = await Client.connect(
        parsed.temporal_address,
        namespace=parsed.temporal_namespace,
    )
    executor = ThreadPoolExecutor(max_workers=parsed.activity_executor_workers)
    worker = Worker(
        client,
        task_queue=registration["task_queue"],
        workflows=registration["workflows"],
        activities=registration["activities"],
        activity_executor=executor,
        max_concurrent_activities=(
            2 if parsed.role == "orchestrator" else parsed.max_provider_activities
        ),
    )
    async with worker:
        await asyncio.Future()


def main(argv: list[str] | None = None) -> int:
    parsed = build_parser().parse_args(argv)
    if parsed.health_check:
        health = catalog_execution_health()
        print(json.dumps(asdict(health), sort_keys=True, separators=(",", ":")))
        return 0 if health.status == "READY" else 1
    asyncio.run(run_worker(parsed))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
