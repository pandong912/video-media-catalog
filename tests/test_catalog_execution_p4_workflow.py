from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker
from test_catalog_execution_contracts import _input

from video_media_catalog.execution_p4 import (
    ObservedCostEvidence,
    ObservedResourceEvidence,
)
from video_media_catalog.spark_backend import (
    SparkBackendIdentity,
    SparkBackendObservation,
    SparkCancelReceipt,
    SparkReconcileResult,
)
from video_media_catalog.temporal.catalog_execution_models import (
    CatalogExecutionInput,
    CatalogExecutionResult,
    PreparedCatalogExecution,
    catalog_execution_workflow_id,
)
from video_media_catalog.temporal.names import CATALOG_EXECUTION_AWS_TASK_QUEUE
from video_media_catalog.temporal.workflows.catalog_execution import (
    CatalogExecutionWorkflowV1,
)


def test_retry_rollback_checkpoint_and_history_replay() -> None:
    asyncio.run(_retry_rollback_case())


async def _retry_rollback_case() -> None:
    input_data = _input()
    retry_started = asyncio.Event()
    release_retry = asyncio.Event()
    cancel_seen = asyncio.Event()
    observe_attempts = 0

    @activity.defn(name="PrepareCatalogExecutionV1")
    async def prepare(
        current: CatalogExecutionInput,
    ) -> PreparedCatalogExecution:
        return PreparedCatalogExecution(current, "stable-token", "stable-job")

    @activity.defn(name="SubmitCatalogSparkV1")
    async def submit(
        _prepared: PreparedCatalogExecution,
    ) -> SparkBackendIdentity:
        return SparkBackendIdentity(
            provider="AWS_EMR_SERVERLESS",
            application_id="app-1",
            job_run_id="job-1",
        )

    @activity.defn(name="ReconcileCatalogSparkV1")
    async def reconcile(
        _prepared: PreparedCatalogExecution,
        identity: SparkBackendIdentity,
    ) -> SparkReconcileResult:
        return SparkReconcileResult.create(
            SparkBackendObservation(
                identity=identity,
                provider_state="RUNNING",
                outcome="RUNNING",
            )
        )

    @activity.defn(name="ObserveCatalogSparkV1")
    async def observe(
        _prepared: PreparedCatalogExecution,
        identity: SparkBackendIdentity,
    ) -> SparkBackendObservation:
        nonlocal observe_attempts
        observe_attempts += 1
        if observe_attempts == 1:
            raise RuntimeError("synthetic transient provider failure")
        if not cancel_seen.is_set():
            retry_started.set()
            await release_retry.wait()
            return SparkBackendObservation(
                identity=identity,
                provider_state="RUNNING",
                outcome="RUNNING",
            )
        resource = ObservedResourceEvidence.create(
            scope="PROVIDER_OBSERVED",
            provider=identity.provider,
            duration_milliseconds=5_000,
            input_bytes=1_024,
            output_bytes=128,
            complete=True,
        )
        cost = ObservedCostEvidence.create(
            scope="PROVIDER_OBSERVED",
            provider=identity.provider,
            status="UNAVAILABLE",
            resource_evidence_digest=resource.evidence_digest,
        )
        return SparkBackendObservation(
            identity=identity,
            provider_state="CANCELLED",
            outcome="CANCELLED",
            observed_at="2026-10-24T00:10:00Z",
            resource_evidence_digest=resource.evidence_digest,
            cost_evidence_digest=cost.evidence_digest,
        )

    @activity.defn(name="CancelCatalogSparkV1")
    async def cancel(
        _prepared: PreparedCatalogExecution,
        identity: SparkBackendIdentity,
        _reason: str,
    ) -> SparkCancelReceipt:
        cancel_seen.set()
        return SparkCancelReceipt(identity, True, "CANCEL_REQUESTED")

    @activity.defn(name="WriteCatalogCompletionReceiptV1")
    async def receipt(
        prepared: PreparedCatalogExecution,
        identity: SparkBackendIdentity,
        observation: SparkBackendObservation,
    ) -> CatalogExecutionResult:
        return CatalogExecutionResult(
            state=observation.outcome,  # type: ignore[arg-type]
            backend_application_id=identity.application_id,
            backend_job_id=identity.job_run_id,
            completion_receipt=prepared.input.input_objects[0],
            completion_receipt_digest="7" * 64,
            resource_evidence_digest=(
                observation.bound_resource_evidence_digest or None
            ),
            cost_evidence_digest=(observation.bound_cost_evidence_digest or None),
        )

    async with (
        await WorkflowEnvironment.start_time_skipping() as env,
        Worker(
            env.client,
            task_queue="catalog-p4-recovery-test",
            workflows=[CatalogExecutionWorkflowV1],
            activities=[prepare, receipt],
        ),
        Worker(
            env.client,
            task_queue=CATALOG_EXECUTION_AWS_TASK_QUEUE,
            activities=[submit, reconcile, observe, cancel],
        ),
    ):
        handle = await env.client.start_workflow(
            CatalogExecutionWorkflowV1.run,
            input_data,
            id=catalog_execution_workflow_id(input_data),
            task_queue="catalog-p4-recovery-test",
            execution_timeout=timedelta(minutes=5),
        )
        await asyncio.wait_for(retry_started.wait(), timeout=15)
        await handle.signal(
            CatalogExecutionWorkflowV1.rollback,
            "disaster recovery operator rollback",
        )
        status = await handle.query(CatalogExecutionWorkflowV1.status)
        assert status.rollback_requested
        checkpoint = await handle.query(CatalogExecutionWorkflowV1.recovery_checkpoint)
        assert checkpoint is not None
        assert checkpoint.backend_job_id == "job-1"
        release_retry.set()
        result = await handle.result()
        history = await handle.fetch_history()

    assert observe_attempts >= 3
    assert cancel_seen.is_set()
    assert result.state == "CANCELLED"
    assert result.recovery_checkpoint is not None
    assert result.recovery_checkpoint.rollback_requested
    assert result.resource_evidence_digest is not None
    await Replayer(workflows=[CatalogExecutionWorkflowV1]).replay_workflow(history)
