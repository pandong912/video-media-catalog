"""Temporal orchestration for build-only Research Gold index projection."""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from video_media_catalog.temporal.activities.gold_index import (
        submit_gold_index_build_job,
        validate_gold_index_build,
    )
    from video_media_catalog.temporal.gold_index_models import (
        GoldIndexProjectionInput,
        GoldIndexProjectionResult,
    )
    from video_media_catalog.temporal.names import (
        GOLD_INDEX_LAUNCHER_TASK_QUEUE,
        GOLD_INDEX_VALIDATOR_TASK_QUEUE,
    )


_JOB_RETRY = RetryPolicy(
    initial_interval=timedelta(minutes=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=5),
    maximum_attempts=2,
)
_VALIDATION_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=30),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=5),
    maximum_attempts=3,
)


@workflow.defn(name="GoldIndexProjectionWorkflow")
class GoldIndexProjectionWorkflow:
    @workflow.run
    async def run(
        self,
        input_data: GoldIndexProjectionInput,
    ) -> GoldIndexProjectionResult:
        completed_at = (
            workflow.now().replace(microsecond=0).isoformat().replace("+00:00", "Z")
        )
        job = await workflow.execute_activity(
            submit_gold_index_build_job,
            args=[input_data, completed_at],
            task_queue=GOLD_INDEX_LAUNCHER_TASK_QUEUE,
            start_to_close_timeout=timedelta(hours=13),
            heartbeat_timeout=timedelta(minutes=2),
            retry_policy=_JOB_RETRY,
        )
        validated_at = (
            workflow.now().replace(microsecond=0).isoformat().replace("+00:00", "Z")
        )
        validation_receipt = await workflow.execute_activity(
            validate_gold_index_build,
            args=[input_data, job, validated_at],
            task_queue=GOLD_INDEX_VALIDATOR_TASK_QUEUE,
            start_to_close_timeout=timedelta(minutes=30),
            heartbeat_timeout=timedelta(minutes=2),
            retry_policy=_VALIDATION_RETRY,
        )
        return GoldIndexProjectionResult(
            projection_id=input_data.projection_id,
            job_name=job.job_name,
            build_manifest=job.build_manifest,
            validation_receipt=validation_receipt,
            activation_status="DISABLED",
        )
