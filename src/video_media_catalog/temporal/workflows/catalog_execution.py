"""Deterministic P3 orchestration for one Catalog Spark execution attempt."""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from video_media_catalog.spark_backend import (
        SparkBackendIdentity,
        SparkBackendObservation,
    )
    from video_media_catalog.temporal.activities.catalog_execution import (
        cancel_catalog_spark,
        observe_catalog_spark,
        prepare_catalog_execution,
        reconcile_catalog_spark,
        submit_catalog_spark,
        write_catalog_completion_receipt,
    )
    from video_media_catalog.temporal.catalog_execution_models import (
        CatalogExecutionInput,
        CatalogExecutionRecoveryCheckpoint,
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
        self._checkpoint: CatalogExecutionRecoveryCheckpoint | None = None

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
            state="RECONCILING",
            backend_job_id=identity.job_run_id,
        )
        reconciled = await workflow.execute_activity(
            reconcile_catalog_spark,
            args=[prepared, identity],
            task_queue=provider_queue,
            start_to_close_timeout=timedelta(minutes=2),
            heartbeat_timeout=timedelta(minutes=1),
            retry_policy=_PROVIDER_RETRY,
        )
        observation = reconciled.observation
        self._set_status(
            state=(observation.outcome if observation.terminal else "RUNNING"),
            reconcile_count=self._status.reconcile_count + 1,
            provider_state=observation.provider_state,
        )
        self._capture_checkpoint(identity, observation)
        if observation.terminal:
            return await self._complete(prepared, identity, observation)
        cancel_sent = False
        for _ in range(input_data.max_observations):
            if self._status.cancel_requested and not cancel_sent:
                self._set_status(
                    state=(
                        "ROLLING_BACK"
                        if self._status.rollback_requested
                        else "CANCELLING"
                    )
                )
                await workflow.execute_activity(
                    cancel_catalog_spark,
                    args=[
                        prepared,
                        identity,
                        self._cancel_reason
                        or "Platform requested cancellation or rollback",
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
                    (
                        "ROLLING_BACK"
                        if self._status.rollback_requested
                        else "CANCELLING"
                    )
                    if cancel_sent and not observation.terminal
                    else observation.outcome
                ),
                observation_count=self._status.observation_count + 1,
                provider_state=observation.provider_state,
            )
            self._capture_checkpoint(identity, observation)
            if observation.terminal:
                return await self._complete(prepared, identity, observation)
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

    @workflow.signal(name="rollback")
    def rollback(self, reason: str) -> None:
        if not reason or reason != reason.strip() or len(reason) > 512:
            raise ValueError("rollback reason must be canonical bounded text")
        if not self._status.rollback_requested:
            self._cancel_reason = reason
            self._set_status(
                cancel_requested=True,
                rollback_requested=True,
            )

    @workflow.query(name="status")
    def status(self) -> CatalogExecutionStatus:
        return self._status

    @workflow.query(name="recoveryCheckpoint")
    def recovery_checkpoint(self) -> CatalogExecutionRecoveryCheckpoint | None:
        return self._checkpoint

    async def _complete(
        self,
        prepared: PreparedCatalogExecution,
        identity: SparkBackendIdentity,
        observation: SparkBackendObservation,
    ) -> CatalogExecutionResult:
        result = await workflow.execute_activity(
            write_catalog_completion_receipt,
            args=[prepared, identity, observation],
            start_to_close_timeout=timedelta(minutes=5),
            heartbeat_timeout=timedelta(minutes=1),
            retry_policy=_CONTROL_RETRY,
        )
        return replace(result, recovery_checkpoint=self._checkpoint)

    def _capture_checkpoint(
        self,
        identity: SparkBackendIdentity,
        observation: SparkBackendObservation,
    ) -> None:
        self._checkpoint = CatalogExecutionRecoveryCheckpoint.create(
            identity=identity,
            observation=observation,
            observation_count=self._status.observation_count,
            cancel_requested=self._status.cancel_requested,
            rollback_requested=self._status.rollback_requested,
        )
        self._set_status(
            checkpoint_digest=self._checkpoint.checkpoint_digest,
            resource_evidence_digest=self._checkpoint.resource_evidence_digest,
            cost_evidence_digest=self._checkpoint.cost_evidence_digest,
        )

    def _set_status(
        self,
        *,
        state: str | None = None,
        observation_count: int | None = None,
        cancel_requested: bool | None = None,
        backend_job_id: str | None = None,
        provider_state: str | None = None,
        reconcile_count: int | None = None,
        rollback_requested: bool | None = None,
        checkpoint_digest: str | None = None,
        resource_evidence_digest: str | None = None,
        cost_evidence_digest: str | None = None,
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
            reconcile_count=(
                self._status.reconcile_count
                if reconcile_count is None
                else reconcile_count
            ),
            rollback_requested=(
                self._status.rollback_requested
                if rollback_requested is None
                else rollback_requested
            ),
            checkpoint_digest=(
                self._status.checkpoint_digest
                if checkpoint_digest is None
                else checkpoint_digest
            ),
            resource_evidence_digest=(
                self._status.resource_evidence_digest
                if resource_evidence_digest is None
                else resource_evidence_digest
            ),
            cost_evidence_digest=(
                self._status.cost_evidence_digest
                if cost_evidence_digest is None
                else cost_evidence_digest
            ),
        )

    @staticmethod
    def _provider_queue(prepared: PreparedCatalogExecution) -> str:
        if prepared.input.provider_profile.backend_kind == "AWS_EMR_SERVERLESS":
            return CATALOG_EXECUTION_AWS_TASK_QUEUE
        return CATALOG_EXECUTION_ALIYUN_TASK_QUEUE
