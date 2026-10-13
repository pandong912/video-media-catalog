"""Deterministic Temporal orchestration for Research index GC and restore."""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from video_media_catalog.temporal.activities.gold_index_gc import (
        submit_gold_index_gc_job,
        submit_gold_index_restore_job,
        validate_gold_index_gc,
        validate_gold_index_restore,
    )
    from video_media_catalog.temporal.gold_index_models import (
        GoldIndexGcInput,
        GoldIndexGcWorkflowResult,
        GoldIndexRestoreInput,
        GoldIndexRestoreWorkflowResult,
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


def _workflow_time() -> str:
    return workflow.now().replace(microsecond=0).isoformat().replace("+00:00", "Z")


@workflow.defn(name="GoldIndexGcWorkflow")
class GoldIndexGcWorkflow:
    @workflow.run
    async def run(
        self,
        input_data: GoldIndexGcInput,
    ) -> GoldIndexGcWorkflowResult:
        job = await workflow.execute_activity(
            submit_gold_index_gc_job,
            args=[input_data, _workflow_time()],
            task_queue=GOLD_INDEX_LAUNCHER_TASK_QUEUE,
            start_to_close_timeout=timedelta(hours=4),
            heartbeat_timeout=timedelta(minutes=2),
            retry_policy=_JOB_RETRY,
        )
        receipt = await workflow.execute_activity(
            validate_gold_index_gc,
            args=[input_data, job],
            task_queue=GOLD_INDEX_VALIDATOR_TASK_QUEUE,
            start_to_close_timeout=timedelta(minutes=30),
            heartbeat_timeout=timedelta(minutes=2),
            retry_policy=_VALIDATION_RETRY,
        )
        return GoldIndexGcWorkflowResult(
            operation_id=input_data.operation_id,
            job_name=job.job_name,
            gc_result_receipt=receipt,
        )


@workflow.defn(name="GoldIndexRestoreWorkflow")
class GoldIndexRestoreWorkflow:
    @workflow.run
    async def run(
        self,
        input_data: GoldIndexRestoreInput,
    ) -> GoldIndexRestoreWorkflowResult:
        job = await workflow.execute_activity(
            submit_gold_index_restore_job,
            args=[input_data, _workflow_time()],
            task_queue=GOLD_INDEX_LAUNCHER_TASK_QUEUE,
            start_to_close_timeout=timedelta(hours=4),
            heartbeat_timeout=timedelta(minutes=2),
            retry_policy=_JOB_RETRY,
        )
        receipt = await workflow.execute_activity(
            validate_gold_index_restore,
            args=[input_data, job],
            task_queue=GOLD_INDEX_VALIDATOR_TASK_QUEUE,
            start_to_close_timeout=timedelta(minutes=30),
            heartbeat_timeout=timedelta(minutes=2),
            retry_policy=_VALIDATION_RETRY,
        )
        return GoldIndexRestoreWorkflowResult(
            operation_id=input_data.operation_id,
            job_name=job.job_name,
            restore_receipt=receipt,
        )
