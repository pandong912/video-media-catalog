"""Deterministic TMDB capture + sequential Source Silver workflow."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from video_media_catalog.temporal.activities.capture import (
        capture_tmdb_day,
        validate_tmdb_token,
    )
    from video_media_catalog.temporal.activities.silver import submit_source_silver
    from video_media_catalog.temporal.activities.summary import write_pipeline_summary
    from video_media_catalog.temporal.models import (
        CaptureResult,
        PipelineInput,
        PipelineResult,
        SilverResult,
    )
    from video_media_catalog.temporal.names import SILVER_TASK_QUEUE
    from video_media_catalog.temporal.plan import plan_capture_units


_CAPTURE_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=30),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=10),
    maximum_attempts=5,
)
_SILVER_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=60),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=15),
    maximum_attempts=3,
)
_TOKEN_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=10),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=2),
    maximum_attempts=3,
)


@workflow.defn(name="TmdbCaptureSilverWorkflow")
class TmdbCaptureSilverWorkflow:
    @workflow.run
    async def run(self, input_data: PipelineInput) -> PipelineResult:
        units = input_data.units or plan_capture_units(
            mode=input_data.mode,
            export_date=input_data.export_date,
            window_start=input_data.window_start,
            window_end=input_data.window_end,
        )
        if not units:
            raise ValueError("TMDB pipeline requires at least one capture unit")

        needs_token = any(unit.kind == "changes" for unit in units)
        if needs_token:
            await workflow.execute_activity(
                validate_tmdb_token,
                input_data.env.user_agent,
                start_to_close_timeout=timedelta(minutes=2),
                heartbeat_timeout=timedelta(minutes=1),
                retry_policy=_TOKEN_RETRY,
            )

        acquired_at = input_data.acquired_at or workflow.now().replace(
            microsecond=0
        ).isoformat().replace("+00:00", "Z")

        captures: list[CaptureResult] = []
        concurrency = max(1, input_data.env.capture_concurrency)
        pending: list[Any] = []
        for unit in units:
            pending.append(
                workflow.start_activity(
                    capture_tmdb_day,
                    args=[unit, input_data.env, acquired_at],
                    start_to_close_timeout=timedelta(hours=6),
                    heartbeat_timeout=timedelta(minutes=5),
                    retry_policy=_CAPTURE_RETRY,
                )
            )
            if len(pending) >= concurrency:
                captures.extend(await pending.pop(0))
        for handle in pending:
            captures.extend(await handle)

        silvers: list[SilverResult] = []
        for capture in captures:
            silver = await workflow.execute_activity(
                submit_source_silver,
                args=[capture, input_data.env, acquired_at],
                task_queue=SILVER_TASK_QUEUE,
                start_to_close_timeout=timedelta(hours=12),
                heartbeat_timeout=timedelta(minutes=5),
                retry_policy=_SILVER_RETRY,
            )
            silvers.append(silver)

        export_date = input_data.export_date
        window_start = input_data.window_start
        window_end = input_data.window_end
        if export_date is None:
            for item in captures:
                if item.export_date:
                    export_date = item.export_date
                    break
        if window_start is None:
            starts = [item.window_start for item in captures if item.window_start]
            if starts:
                window_start = min(starts)
        if window_end is None:
            ends = [item.window_end for item in captures if item.window_end]
            if ends:
                window_end = max(ends)

        result = PipelineResult(
            mode=input_data.mode,
            acquired_at=acquired_at,
            export_date=export_date,
            window_start=window_start,
            window_end=window_end,
            captures=captures,
            silvers=silvers,
            summary_uri=None,
            capture_count=len(captures),
        )
        summary_uri = await workflow.execute_activity(
            write_pipeline_summary,
            args=[result, input_data.env.pipeline_summary_prefix],
            start_to_close_timeout=timedelta(minutes=5),
            heartbeat_timeout=timedelta(minutes=2),
            retry_policy=_CAPTURE_RETRY,
        )
        return PipelineResult(
            mode=result.mode,
            acquired_at=result.acquired_at,
            export_date=result.export_date,
            window_start=result.window_start,
            window_end=result.window_end,
            captures=result.captures,
            silvers=result.silvers,
            summary_uri=summary_uri,
            capture_count=result.capture_count,
        )
