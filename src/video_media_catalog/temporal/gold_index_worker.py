"""Least-privilege Workers for the Research Gold index projection."""

from __future__ import annotations

import argparse
import asyncio
import os
from concurrent.futures import ThreadPoolExecutor

from temporalio.client import Client
from temporalio.worker import Worker

from video_media_catalog.temporal.activities.gold_index import (
    submit_gold_index_build_job,
    validate_gold_index_build,
)
from video_media_catalog.temporal.names import (
    GOLD_INDEX_LAUNCHER_TASK_QUEUE,
    GOLD_INDEX_VALIDATOR_TASK_QUEUE,
    GOLD_INDEX_WORKFLOW_TASK_QUEUE,
    TEMPORAL_NAMESPACE,
)
from video_media_catalog.temporal.workflows.gold_index_projection import (
    GoldIndexProjectionWorkflow,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="video-media-catalog-gold-index-temporal-worker"
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
        choices=("orchestrator", "launcher", "validator"),
        default=os.environ.get("GOLD_INDEX_WORKER_ROLE", "orchestrator"),
    )
    parser.add_argument(
        "--activity-executor-workers",
        type=int,
        default=int(os.environ.get("TEMPORAL_ACTIVITY_EXECUTOR_WORKERS", "2")),
    )
    return parser


async def run_worker(parsed: argparse.Namespace) -> None:
    if parsed.activity_executor_workers < 1:
        raise ValueError("activity-executor-workers must be positive")
    client = await Client.connect(
        parsed.temporal_address,
        namespace=parsed.temporal_namespace,
    )
    executor = ThreadPoolExecutor(max_workers=parsed.activity_executor_workers)
    if parsed.role == "orchestrator":
        worker = Worker(
            client,
            task_queue=GOLD_INDEX_WORKFLOW_TASK_QUEUE,
            workflows=[GoldIndexProjectionWorkflow],
        )
    elif parsed.role == "launcher":
        worker = Worker(
            client,
            task_queue=GOLD_INDEX_LAUNCHER_TASK_QUEUE,
            activities=[submit_gold_index_build_job],
            activity_executor=executor,
            max_concurrent_activities=1,
        )
    else:
        worker = Worker(
            client,
            task_queue=GOLD_INDEX_VALIDATOR_TASK_QUEUE,
            activities=[validate_gold_index_build],
            activity_executor=executor,
            max_concurrent_activities=2,
        )
    async with worker:
        await asyncio.Future()


def main(argv: list[str] | None = None) -> int:
    parsed = build_parser().parse_args(argv)
    asyncio.run(run_worker(parsed))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
