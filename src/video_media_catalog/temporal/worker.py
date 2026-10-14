"""Temporal worker entrypoint for TMDB capture and Source Silver."""

from __future__ import annotations

import argparse
import asyncio
import os
from concurrent.futures import ThreadPoolExecutor

from temporalio.client import Client
from temporalio.runtime import PrometheusConfig, Runtime, TelemetryConfig
from temporalio.worker import Worker

from video_media_catalog.temporal.activities.capture import (
    capture_tmdb_day,
    validate_tmdb_token,
)
from video_media_catalog.temporal.activities.eidr_public_research import (
    discover_eidr_public_research_ids,
    lookup_eidr_public_research,
    write_eidr_public_research_summary,
)
from video_media_catalog.temporal.activities.research_domain import (
    load_pinned_research_capture,
)
from video_media_catalog.temporal.activities.silver import (
    submit_source_silver,
    submit_source_silver_bundle,
)
from video_media_catalog.temporal.activities.summary import write_pipeline_summary
from video_media_catalog.temporal.names import (
    SILVER_TASK_QUEUE,
    TEMPORAL_NAMESPACE,
    WORKFLOW_TASK_QUEUE,
)
from video_media_catalog.temporal.workflows.eidr_public_research import (
    EidrPublicResearchWorkflow,
)
from video_media_catalog.temporal.workflows.research_domain import (
    ResearchCaptureWorkflow,
    ResearchDomainWorkflow,
    ResearchSilverWorkflow,
)
from video_media_catalog.temporal.workflows.tmdb_pipeline import (
    TmdbCaptureSilverWorkflow,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="video-media-catalog-tmdb-temporal-worker")
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
        default=os.environ.get("TEMPORAL_TASK_QUEUE", WORKFLOW_TASK_QUEUE),
    )
    parser.add_argument(
        "--silver-task-queue",
        default=os.environ.get("TEMPORAL_SILVER_TASK_QUEUE", SILVER_TASK_QUEUE),
    )
    parser.add_argument(
        "--max-capture-activities",
        type=int,
        default=int(os.environ.get("TEMPORAL_MAX_CAPTURE_ACTIVITIES", "2")),
    )
    parser.add_argument(
        "--max-silver-activities",
        type=int,
        default=int(os.environ.get("TEMPORAL_MAX_SILVER_ACTIVITIES", "1")),
    )
    parser.add_argument(
        "--activity-executor-workers",
        type=int,
        default=int(os.environ.get("TEMPORAL_ACTIVITY_EXECUTOR_WORKERS", "4")),
    )
    parser.add_argument(
        "--metrics-address",
        default=os.environ.get("TEMPORAL_METRICS_ADDRESS", "0.0.0.0:9090"),
    )
    return parser


async def run_workers(parsed: argparse.Namespace) -> None:
    if parsed.max_capture_activities < 1:
        raise ValueError("max-capture-activities must be positive")
    if parsed.max_silver_activities != 1:
        raise ValueError("max-silver-activities must be 1 to protect Iceberg commits")
    if parsed.activity_executor_workers < 1:
        raise ValueError("activity-executor-workers must be positive")
    if not parsed.metrics_address:
        raise ValueError("metrics-address must be non-empty")

    Runtime.set_default(
        Runtime(
            telemetry=TelemetryConfig(
                metrics=PrometheusConfig(
                    bind_address=parsed.metrics_address,
                    counters_total_suffix=True,
                    unit_suffix=True,
                    durations_as_seconds=True,
                ),
                global_tags={"service": "video-media-catalog-research"},
            )
        ),
        error_if_already_set=False,
    )
    client = await Client.connect(
        parsed.temporal_address,
        namespace=parsed.temporal_namespace,
    )
    executor = ThreadPoolExecutor(max_workers=parsed.activity_executor_workers)
    primary = Worker(
        client,
        task_queue=parsed.task_queue,
        workflows=[
            TmdbCaptureSilverWorkflow,
            EidrPublicResearchWorkflow,
            ResearchCaptureWorkflow,
            ResearchSilverWorkflow,
            ResearchDomainWorkflow,
        ],
        activities=[
            validate_tmdb_token,
            capture_tmdb_day,
            write_pipeline_summary,
            discover_eidr_public_research_ids,
            lookup_eidr_public_research,
            write_eidr_public_research_summary,
            load_pinned_research_capture,
        ],
        activity_executor=executor,
        max_concurrent_activities=parsed.max_capture_activities,
    )
    silver = Worker(
        client,
        task_queue=parsed.silver_task_queue,
        activities=[submit_source_silver, submit_source_silver_bundle],
        activity_executor=executor,
        max_concurrent_activities=parsed.max_silver_activities,
    )
    async with primary, silver:
        await asyncio.Future()


def main(argv: list[str] | None = None) -> int:
    parsed = build_parser().parse_args(argv)
    asyncio.run(run_workers(parsed))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
