"""Deterministic P3 orchestration for one Catalog Spark execution attempt."""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from video_media_catalog.spark_backend import SparkBackendObservation
    from video_media_catalog.temporal.activities.catalog_execution import (
        cancel_catalog_spark,
        observe_catalog_spark,
        prepare_catalog_execution,
        submit_catalog_spark,
        write_catalog_completion_receipt,
    )
    from video_media_catalog.temporal.catalog_execution_models import (
        CatalogExecutionInput,
        CatalogExecutionResult,
        CatalogExecutionStatus,
        PreparedCatalogExecution,
    )
    from video_media_catalog.temporal.names import (
        CATALOG_EXECUTION_ALIYUN_TASK_QUEUE,
        CATALOG_EXECUTION_AWS_TASK_QUEUE,
    )

_CONTROL_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=1),
    maximum_attempts=3,
)
_PROVIDER_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=10),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=2),
    maximum_attempts=5,
)


@workflow.defn(name="CatalogExecutionWorkflowV1")
class CatalogExecutionWorkflowV1:
    def __init__(self) -> None:
        self._status = CatalogExecutionStatus(
            state="PREPARING",
            observation_count=0,
            cancel_requested=False,
            backend_job_id=None,
            provider_state=None,
        )
        self._cancel_reason = ""

    @workflow.run
    async def run(
        self,
        input_data: CatalogExecutionInput,
    ) -> CatalogExecutionResult:
        prepared = await workflow.execute_activity(
            prepare_catalog_execution,
            input_data,
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=_CONTROL_RETRY,
        )
        self._set_status(state="SUBMITTING")
        provider_queue = self._provider_queue(prepared)
        identity = await workflow.execute_activity(
            submit_catalog_spark,
            prepared,
            task_queue=provider_queue,
            start_to_close_timeout=timedelta(minutes=5),
            heartbeat_timeout=timedelta(minutes=1),
            retry_policy=_PROVIDER_RETRY,
        )
        self._set_status(
            state="RUNNING",
            backend_job_id=identity.job_run_id,
        )
        cancel_sent = False
        observation: SparkBackendObservation | None = None
        for _ in range(input_data.max_observations):
            if self._status.cancel_requested and not cancel_sent:
                self._set_status(state="CANCELLING")
                await workflow.execute_activity(
                    cancel_catalog_spark,
                    args=[
                        prepared,
                        identity,
                        self._cancel_reason or "Platform requested cancellation",
                    ],
                    task_queue=provider_queue,
                    start_to_close_timeout=timedelta(minutes=2),
                    retry_policy=_PROVIDER_RETRY,
                )
                cancel_sent = True
            observation = await workflow.execute_activity(
                observe_catalog_spark,
                args=[prepared, identity],
                task_queue=provider_queue,
                start_to_close_timeout=timedelta(minutes=2),
                heartbeat_timeout=timedelta(minutes=1),
                retry_policy=_PROVIDER_RETRY,
            )
            self._set_status(
                state=(
                    "CANCELLING"
                    if cancel_sent and not observation.terminal
                    else observation.outcome
                ),
                observation_count=self._status.observation_count + 1,
                provider_state=observation.provider_state,
            )
            if observation.terminal:
                return await workflow.execute_activity(
                    write_catalog_completion_receipt,
                    args=[prepared, identity, observation],
                    start_to_close_timeout=timedelta(minutes=5),
                    heartbeat_timeout=timedelta(minutes=1),
                    retry_policy=_CONTROL_RETRY,
                )
            await workflow.sleep(timedelta(seconds=input_data.poll_interval_seconds))
        if not cancel_sent:
            await workflow.execute_activity(
                cancel_catalog_spark,
                args=[
                    prepared,
                    identity,
                    "bounded observation budget exhausted",
                ],
                task_queue=provider_queue,
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=_PROVIDER_RETRY,
            )
        raise ApplicationError(
            "bounded observation budget exhausted before terminal state",
            non_retryable=True,
            type="CatalogExecutionObservationBudgetExhausted",
        )

    @workflow.signal(name="cancel")
    def cancel(self, reason: str) -> None:
        if not reason or reason != reason.strip() or len(reason) > 512:
            raise ValueError("cancel reason must be canonical bounded text")
        if not self._status.cancel_requested:
            self._cancel_reason = reason
            self._set_status(cancel_requested=True)

    @workflow.query(name="status")
    def status(self) -> CatalogExecutionStatus:
        return self._status

    def _set_status(
        self,
        *,
        state: str | None = None,
        observation_count: int | None = None,
        cancel_requested: bool | None = None,
        backend_job_id: str | None = None,
        provider_state: str | None = None,
    ) -> None:
        self._status = CatalogExecutionStatus(
            state=self._status.state if state is None else state,  # type: ignore[arg-type]
            observation_count=(
                self._status.observation_count
                if observation_count is None
                else observation_count
            ),
            cancel_requested=(
                self._status.cancel_requested
                if cancel_requested is None
                else cancel_requested
            ),
            backend_job_id=(
                self._status.backend_job_id
                if backend_job_id is None
                else backend_job_id
            ),
            provider_state=(
                self._status.provider_state
                if provider_state is None
                else provider_state
            ),
        )

    @staticmethod
    def _provider_queue(prepared: PreparedCatalogExecution) -> str:
        if prepared.input.provider_profile.backend_kind == "AWS_EMR_SERVERLESS":
            return CATALOG_EXECUTION_AWS_TASK_QUEUE
        return CATALOG_EXECUTION_ALIYUN_TASK_QUEUE
