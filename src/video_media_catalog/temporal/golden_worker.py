"""Dedicated Temporal Worker for research Identity, epoch, and Gold release."""

from __future__ import annotations

import argparse
import asyncio
import os
from concurrent.futures import ThreadPoolExecutor

from temporalio.client import Client
from temporalio.worker import Worker

from video_media_catalog.temporal.activities.golden import (
    prepare_golden_build,
    publish_golden_epoch,
    publish_golden_snapshot,
    resolve_golden_identity,
    submit_golden_release,
    write_golden_build_summary,
)
from video_media_catalog.temporal.names import (
    GOLDEN_EMR_TASK_QUEUE,
    GOLDEN_WORKFLOW_TASK_QUEUE,
    TEMPORAL_NAMESPACE,
)
from video_media_catalog.temporal.workflows.golden_build import GoldenBuildWorkflow


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="video-media-catalog-golden-temporal-worker")
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
            "TEMPORAL_GOLDEN_TASK_QUEUE",
            GOLDEN_WORKFLOW_TASK_QUEUE,
        ),
    )
    parser.add_argument(
        "--emr-task-queue",
        default=os.environ.get(
            "TEMPORAL_GOLDEN_EMR_TASK_QUEUE",
            GOLDEN_EMR_TASK_QUEUE,
        ),
    )
    parser.add_argument(
        "--activity-executor-workers",
        type=int,
        default=int(os.environ.get("TEMPORAL_ACTIVITY_EXECUTOR_WORKERS", "4")),
    )
    parser.add_argument(
        "--max-emr-activities",
        type=int,
        default=int(os.environ.get("TEMPORAL_MAX_GOLDEN_EMR_ACTIVITIES", "1")),
    )
    return parser


async def run_workers(parsed: argparse.Namespace) -> None:
    if parsed.activity_executor_workers < 1:
        raise ValueError("activity-executor-workers must be positive")
    if parsed.max_emr_activities != 1:
        raise ValueError("max-emr-activities must be 1 to protect Iceberg commits")
    client = await Client.connect(
        parsed.temporal_address,
        namespace=parsed.temporal_namespace,
    )
    executor = ThreadPoolExecutor(max_workers=parsed.activity_executor_workers)
    primary = Worker(
        client,
        task_queue=parsed.task_queue,
        workflows=[GoldenBuildWorkflow],
        activities=[prepare_golden_build, write_golden_build_summary],
        activity_executor=executor,
        max_concurrent_activities=2,
    )
    emr = Worker(
        client,
        task_queue=parsed.emr_task_queue,
        activities=[
            publish_golden_snapshot,
            resolve_golden_identity,
            publish_golden_epoch,
            submit_golden_release,
        ],
        activity_executor=executor,
        max_concurrent_activities=parsed.max_emr_activities,
    )
    async with primary, emr:
        await asyncio.Future()


def main(argv: list[str] | None = None) -> int:
    parsed = build_parser().parse_args(argv)
    asyncio.run(run_workers(parsed))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
