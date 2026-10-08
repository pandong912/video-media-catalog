"""Idempotent Temporal schedule and bootstrap management for TMDB."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import timedelta
from typing import Any

from temporalio.client import (
    Client,
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleCalendarSpec,
    ScheduleRange,
    ScheduleSpec,
    ScheduleState,
    ScheduleUpdate,
    ScheduleUpdateInput,
)
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.service import RPCError, RPCStatusCode

from video_media_catalog.temporal.models import PipelineEnv, PipelineInput
from video_media_catalog.temporal.names import (
    BOOTSTRAP_EXPORT_DATE,
    BOOTSTRAP_WINDOW_END,
    BOOTSTRAP_WINDOW_START,
    BOOTSTRAP_WORKFLOW_ID,
    SCHEDULE_DAILY_CHANGES,
    SCHEDULE_MONTHLY_INVENTORY,
    TEMPORAL_NAMESPACE,
    WORKFLOW_TASK_QUEUE,
)
from video_media_catalog.temporal.plan import workflow_id_for
from video_media_catalog.temporal.workflows.tmdb_pipeline import (
    TmdbCaptureSilverWorkflow,
)


def _env_from_mapping(values: dict[str, Any]) -> PipelineEnv:
    return PipelineEnv(
        destination_prefix=str(values["destinationPrefix"]),
        image_digest=str(values["imageDigest"]),
        user_agent=str(values["userAgent"]),
        aws_region=str(values["awsRegion"]),
        warehouse_uri=str(values["warehouseUri"]),
        record_staging_prefix=str(values["recordStagingPrefix"]),
        silver_checkpoint_prefix=str(values["silverCheckpointPrefix"]),
        pipeline_summary_prefix=str(values["pipelineSummaryPrefix"]),
        emr_application_name=str(values["emrApplicationName"]),
        emr_execution_role_arn=str(values["emrExecutionRoleArn"]),
        emr_log_uri=str(values["emrLogUri"]),
        emr_entry_point=str(values["emrEntryPoint"]),
        catalog_type=str(values.get("catalogType", "glue")),
        catalog_name=str(values.get("catalogName", "media")),
        glue_namespace=str(values.get("glueNamespace", "video_media_catalog")),
        capture_concurrency=int(values.get("captureConcurrency", 2)),
        s3_endpoint=values.get("s3Endpoint"),
        s3_path_style_access=bool(values.get("s3PathStyleAccess", False)),
    )


def _load_env(path: str) -> PipelineEnv:
    with open(path, encoding="utf-8") as handle:
        return _env_from_mapping(json.load(handle))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="video-media-catalog-tmdb-temporal-admin")
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
        "--pipeline-env-file",
        default=os.environ.get("TMDB_PIPELINE_ENV_FILE", "/config/pipeline-env.json"),
    )
    sub = parser.add_subparsers(dest="command", required=True)

    ensure = sub.add_parser(
        "ensure-runtime",
        help="Upsert schedules and optionally start the fixed bootstrap workflow",
    )
    ensure.add_argument("--start-bootstrap", action="store_true")
    ensure.add_argument(
        "--bootstrap-export-date",
        default=BOOTSTRAP_EXPORT_DATE,
    )
    ensure.add_argument(
        "--bootstrap-window-start",
        default=BOOTSTRAP_WINDOW_START,
    )
    ensure.add_argument(
        "--bootstrap-window-end",
        default=BOOTSTRAP_WINDOW_END,
    )

    start = sub.add_parser("start-pipeline", help="Start one TMDB pipeline workflow")
    start.add_argument(
        "--mode",
        choices=("bootstrap", "daily", "inventory-only"),
        required=True,
    )
    start.add_argument("--export-date")
    start.add_argument("--window-start")
    start.add_argument("--window-end")
    start.add_argument("--acquired-at")
    start.add_argument("--workflow-id")
    return parser


async def _connect(parsed: argparse.Namespace) -> Client:
    return await Client.connect(
        parsed.temporal_address,
        namespace=parsed.temporal_namespace,
    )


def _daily_input(env: PipelineEnv) -> PipelineInput:
    # Schedule action fills dates at start time via a thin wrapper workflow id;
    # for calendar schedules we pass mode=daily and let the admin Job compute
    # yesterday bounds when creating the action payload each upsert.
    from datetime import UTC, datetime, timedelta

    yesterday = (datetime.now(UTC).date() - timedelta(days=1)).isoformat()
    return PipelineInput(
        mode="daily",
        env=env,
        window_start=yesterday,
        window_end=yesterday,
    )


def _inventory_input(env: PipelineEnv) -> PipelineInput:
    from datetime import UTC, datetime, timedelta

    # Monthly inventory uses the first-of-month schedule; export date is yesterday
    # relative to schedule fire time when ensure-runtime runs, then schedule
    # action keeps that static until next upsert. Prefer "previous day" semantics.
    export_date = (datetime.now(UTC).date() - timedelta(days=1)).isoformat()
    return PipelineInput(
        mode="inventory-only",
        env=env,
        export_date=export_date,
    )


async def _upsert_schedule(
    client: Client,
    *,
    schedule_id: str,
    spec: ScheduleSpec,
    input_data: PipelineInput,
    task_queue: str,
    note: str,
) -> None:
    workflow_id = workflow_id_for(
        mode=input_data.mode,
        export_date=input_data.export_date,
        window_start=input_data.window_start,
        window_end=input_data.window_end,
    )
    action = ScheduleActionStartWorkflow(
        TmdbCaptureSilverWorkflow.run,
        input_data,
        id=workflow_id,
        task_queue=task_queue,
        execution_timeout=timedelta(hours=24),
    )
    schedule = Schedule(
        action=action,
        spec=spec,
        state=ScheduleState(note=note, paused=False),
    )
    handle = client.get_schedule_handle(schedule_id)
    try:
        await client.create_schedule(schedule_id, schedule)
        return
    except RPCError as exc:
        if exc.status != RPCStatusCode.ALREADY_EXISTS:
            raise
        # Fall through to idempotent update when the schedule already exists.

    async def _updater(_input: ScheduleUpdateInput) -> ScheduleUpdate:
        return ScheduleUpdate(schedule=schedule)

    await handle.update(_updater)


async def ensure_runtime(parsed: argparse.Namespace) -> dict[str, Any]:
    env = _load_env(parsed.pipeline_env_file)
    client = await _connect(parsed)

    await _upsert_schedule(
        client,
        schedule_id=SCHEDULE_DAILY_CHANGES,
        spec=ScheduleSpec(
            calendars=[
                ScheduleCalendarSpec(
                    hour=(ScheduleRange(9),),
                    minute=(ScheduleRange(17),),
                )
            ]
        ),
        input_data=_daily_input(env),
        task_queue=parsed.task_queue,
        note="TMDB daily changes capture + Source Silver",
    )
    await _upsert_schedule(
        client,
        schedule_id=SCHEDULE_MONTHLY_INVENTORY,
        spec=ScheduleSpec(
            calendars=[
                ScheduleCalendarSpec(
                    day_of_month=(ScheduleRange(2),),
                    hour=(ScheduleRange(10),),
                    minute=(ScheduleRange(47),),
                )
            ]
        ),
        input_data=_inventory_input(env),
        task_queue=parsed.task_queue,
        note="TMDB monthly inventory capture + Source Silver",
    )

    bootstrap_started = False
    bootstrap_workflow_id = None
    if parsed.start_bootstrap:
        bootstrap_workflow_id = BOOTSTRAP_WORKFLOW_ID
        input_data = PipelineInput(
            mode="bootstrap",
            env=env,
            export_date=parsed.bootstrap_export_date,
            window_start=parsed.bootstrap_window_start,
            window_end=parsed.bootstrap_window_end,
        )
        try:
            await client.start_workflow(
                TmdbCaptureSilverWorkflow.run,
                input_data,
                id=bootstrap_workflow_id,
                task_queue=parsed.task_queue,
                execution_timeout=timedelta(hours=48),
            )
            bootstrap_started = True
        except WorkflowAlreadyStartedError:
            bootstrap_started = False

    return {
        "schedules": [SCHEDULE_DAILY_CHANGES, SCHEDULE_MONTHLY_INVENTORY],
        "bootstrapStarted": bootstrap_started,
        "bootstrapWorkflowId": bootstrap_workflow_id,
    }


async def start_pipeline(parsed: argparse.Namespace) -> dict[str, Any]:
    env = _load_env(parsed.pipeline_env_file)
    client = await _connect(parsed)
    input_data = PipelineInput(
        mode=parsed.mode,
        env=env,
        export_date=parsed.export_date,
        window_start=parsed.window_start,
        window_end=parsed.window_end,
        acquired_at=parsed.acquired_at,
    )
    workflow_id = parsed.workflow_id or workflow_id_for(
        mode=parsed.mode,
        export_date=parsed.export_date,
        window_start=parsed.window_start,
        window_end=parsed.window_end,
    )
    handle = await client.start_workflow(
        TmdbCaptureSilverWorkflow.run,
        input_data,
        id=workflow_id,
        task_queue=parsed.task_queue,
        execution_timeout=timedelta(hours=48),
    )
    return {"workflowId": handle.id, "runId": handle.result_run_id}


def main(argv: list[str] | None = None) -> int:
    parsed = build_parser().parse_args(argv)
    if parsed.command == "ensure-runtime":
        result = asyncio.run(ensure_runtime(parsed))
    elif parsed.command == "start-pipeline":
        result = asyncio.run(start_pipeline(parsed))
    else:
        raise SystemExit(f"unsupported command: {parsed.command}")
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
