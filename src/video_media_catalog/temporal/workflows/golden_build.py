"""Deterministic orchestration for one pinned research Golden Build."""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from video_media_catalog.temporal.activities.golden import (
        prepare_golden_build,
        publish_golden_epoch,
        publish_golden_snapshot,
        resolve_golden_identity,
        submit_golden_release,
        write_golden_build_summary,
    )
    from video_media_catalog.temporal.golden_models import (
        GoldenBuildInput,
        GoldenBuildResult,
    )
    from video_media_catalog.temporal.names import GOLDEN_EMR_TASK_QUEUE


_CONTROL_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=30),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=5),
    maximum_attempts=3,
)
_EMR_RETRY = RetryPolicy(
    initial_interval=timedelta(minutes=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=10),
    maximum_attempts=2,
)


@workflow.defn(name="GoldenBuildWorkflow")
class GoldenBuildWorkflow:
    @workflow.run
    async def run(self, input_data: GoldenBuildInput) -> GoldenBuildResult:
        # Temporal time is replay-safe. A single timestamp keeps the immutable
        # stage identities stable across Worker restarts and activity retries.
        planned_at = (
            workflow.now().replace(microsecond=0).isoformat().replace("+00:00", "Z")
        )
        preflight = await workflow.execute_activity(
            prepare_golden_build,
            args=[input_data, planned_at],
            start_to_close_timeout=timedelta(minutes=10),
            heartbeat_timeout=timedelta(minutes=2),
            retry_policy=_CONTROL_RETRY,
        )
        snapshot = await workflow.execute_activity(
            publish_golden_snapshot,
            args=[input_data, preflight],
            task_queue=GOLDEN_EMR_TASK_QUEUE,
            start_to_close_timeout=timedelta(hours=12),
            heartbeat_timeout=timedelta(minutes=5),
            retry_policy=_EMR_RETRY,
        )
        identity = await workflow.execute_activity(
            resolve_golden_identity,
            args=[input_data, preflight, snapshot, planned_at, planned_at],
            task_queue=GOLDEN_EMR_TASK_QUEUE,
            start_to_close_timeout=timedelta(hours=12),
            heartbeat_timeout=timedelta(minutes=5),
            retry_policy=_EMR_RETRY,
        )
        epoch = await workflow.execute_activity(
            publish_golden_epoch,
            args=[input_data, preflight, identity, planned_at],
            task_queue=GOLDEN_EMR_TASK_QUEUE,
            start_to_close_timeout=timedelta(hours=12),
            heartbeat_timeout=timedelta(minutes=5),
            retry_policy=_EMR_RETRY,
        )
        release = await workflow.execute_activity(
            submit_golden_release,
            args=[input_data, preflight, epoch, planned_at],
            task_queue=GOLDEN_EMR_TASK_QUEUE,
            start_to_close_timeout=timedelta(hours=12),
            heartbeat_timeout=timedelta(minutes=5),
            retry_policy=_EMR_RETRY,
        )
        result = GoldenBuildResult(
            build_id=input_data.build_id,
            planned_at=planned_at,
            committed_at=planned_at,
            build_spec=preflight.build_spec,
            snapshot=snapshot,
            identity=identity,
            epoch=epoch,
            release=release,
        )
        summary_uri = await workflow.execute_activity(
            write_golden_build_summary,
            args=[
                result,
                input_data.env.pipeline_summary_prefix,
                input_data.env.aws_region,
            ],
            start_to_close_timeout=timedelta(minutes=10),
            heartbeat_timeout=timedelta(minutes=2),
            retry_policy=_CONTROL_RETRY,
        )
        return replace(result, summary_uri=summary_uri)
