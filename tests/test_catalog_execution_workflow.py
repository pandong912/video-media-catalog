from __future__ import annotations

import asyncio
from datetime import timedelta
from pathlib import Path

from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker
from test_catalog_execution_contracts import _input

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


def test_catalog_execution_observe_query_and_replay() -> None:
    asyncio.run(_success_case())


async def _success_case() -> None:
    input_data = _input()
    observe_started = asyncio.Event()
    release_observation = asyncio.Event()

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
        observe_started.set()
        await release_observation.wait()
        return SparkBackendObservation(
            identity=identity,
            provider_state="SUCCESS",
            outcome="SUCCESS",
            observed_at="2026-10-23T00:10:00Z",
        )

    @activity.defn(name="CancelCatalogSparkV1")
    async def cancel(
        _prepared: PreparedCatalogExecution,
        identity: SparkBackendIdentity,
        _reason: str,
    ) -> SparkCancelReceipt:
        raise AssertionError(f"unexpected cancellation for {identity.job_run_id}")

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
            completion_receipt_digest="9" * 64,
        )

    async with (
        await WorkflowEnvironment.start_time_skipping() as env,
        Worker(
            env.client,
            task_queue="catalog-execution-test",
            workflows=[CatalogExecutionWorkflowV1],
            activities=[prepare, receipt],
        ),
        Worker(
            env.client,
            task_queue=CATALOG_EXECUTION_AWS_TASK_QUEUE,
            activities=[submit, reconcile, observe, cancel],
        ),
    ):
        workflow_id = catalog_execution_workflow_id(input_data)
        handle = await env.client.start_workflow(
            CatalogExecutionWorkflowV1.run,
            input_data,
            id=workflow_id,
            task_queue="catalog-execution-test",
            execution_timeout=timedelta(minutes=5),
        )
        await asyncio.wait_for(observe_started.wait(), timeout=5)
        status = await handle.query(CatalogExecutionWorkflowV1.status)
        assert status.state == "RUNNING"
        assert status.backend_job_id == "job-1"
        checkpoint = await handle.query(CatalogExecutionWorkflowV1.recovery_checkpoint)
        assert checkpoint is not None
        assert checkpoint.backend_job_id == "job-1"
        release_observation.set()
        result = await handle.result()
        assert result.state == "SUCCESS"
        assert result.result_kind == "EXTERNAL_AUTHORITY"
        assert result.recovery_checkpoint is not None
        history = await handle.fetch_history()

    await Replayer(workflows=[CatalogExecutionWorkflowV1]).replay_workflow(history)


def test_catalog_execution_cancel_is_observed_before_completion() -> None:
    asyncio.run(_cancel_case())


async def _cancel_case() -> None:
    input_data = _input()
    observe_started = asyncio.Event()
    release_first_observation = asyncio.Event()
    cancellation_seen = asyncio.Event()

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
        if not cancellation_seen.is_set():
            observe_started.set()
            await release_first_observation.wait()
            return SparkBackendObservation(
                identity=identity,
                provider_state="RUNNING",
                outcome="RUNNING",
            )
        return SparkBackendObservation(
            identity=identity,
            provider_state="CANCELLED",
            outcome="CANCELLED",
            observed_at="2026-10-23T00:11:00Z",
        )

    @activity.defn(name="CancelCatalogSparkV1")
    async def cancel(
        _prepared: PreparedCatalogExecution,
        identity: SparkBackendIdentity,
        _reason: str,
    ) -> SparkCancelReceipt:
        cancellation_seen.set()
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
            completion_receipt_digest="8" * 64,
        )

    async with (
        await WorkflowEnvironment.start_time_skipping() as env,
        Worker(
            env.client,
            task_queue="catalog-cancel-test",
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
            task_queue="catalog-cancel-test",
            execution_timeout=timedelta(minutes=5),
        )
        await asyncio.wait_for(observe_started.wait(), timeout=5)
        await handle.signal(CatalogExecutionWorkflowV1.cancel, "operator requested")
        status = await handle.query(CatalogExecutionWorkflowV1.status)
        assert status.cancel_requested
        release_first_observation.set()
        result = await handle.result()

    assert cancellation_seen.is_set()
    assert result.state == "CANCELLED"


def test_workflow_source_contains_no_io_now_or_uuid() -> None:
    source = (
        Path(__file__).parents[1]
        / "src/video_media_catalog/temporal/workflows/catalog_execution.py"
    ).read_text()

    for forbidden in (
        "workflow.now",
        "datetime.now",
        "uuid",
        "boto3",
        "open(",
        "Path(",
        "ExactObjectStore",
    ):
        assert forbidden not in source
