"""One-shot EIDR discovery, public lookup, and bundled Source Silver."""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from video_media_catalog.temporal.activities.eidr_public_research import (
        discover_eidr_public_research_ids,
        lookup_eidr_public_research,
        write_eidr_public_research_summary,
    )
    from video_media_catalog.temporal.activities.silver import (
        submit_source_silver_bundle,
    )
    from video_media_catalog.temporal.eidr_models import (
        EidrPublicResearchInput,
        EidrPublicResearchResult,
    )
    from video_media_catalog.temporal.names import SILVER_TASK_QUEUE


_ONE_ATTEMPT = RetryPolicy(maximum_attempts=1)
_SILVER_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=60),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=15),
    maximum_attempts=3,
)
_SUMMARY_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=10),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=2),
    maximum_attempts=3,
)


@workflow.defn(name="EidrPublicResearchWorkflow")
class EidrPublicResearchWorkflow:
    @workflow.run
    async def run(
        self,
        input_data: EidrPublicResearchInput,
    ) -> EidrPublicResearchResult:
        discovery = await workflow.execute_activity(
            discover_eidr_public_research_ids,
            input_data,
            start_to_close_timeout=timedelta(hours=12),
            heartbeat_timeout=timedelta(minutes=5),
            retry_policy=_ONE_ATTEMPT,
        )
        lookup = await workflow.execute_activity(
            lookup_eidr_public_research,
            args=[input_data, discovery],
            start_to_close_timeout=timedelta(days=5),
            heartbeat_timeout=timedelta(minutes=2),
            retry_policy=_ONE_ATTEMPT,
        )

        silver_results = ()
        if lookup.source_silver_inputs:
            silver_results = tuple(
                await workflow.execute_activity(
                    submit_source_silver_bundle,
                    args=[
                        lookup.source_silver_inputs,
                        input_data.env,
                        input_data.acquired_at,
                    ],
                    task_queue=SILVER_TASK_QUEUE,
                    start_to_close_timeout=timedelta(days=3),
                    heartbeat_timeout=timedelta(minutes=5),
                    retry_policy=_SILVER_RETRY,
                )
            )
        summary = await workflow.execute_activity(
            write_eidr_public_research_summary,
            args=[input_data, discovery, lookup, silver_results],
            start_to_close_timeout=timedelta(minutes=5),
            heartbeat_timeout=timedelta(minutes=2),
            retry_policy=_SUMMARY_RETRY,
        )
        return EidrPublicResearchResult(
            discovery=discovery,
            lookup=lookup,
            silvers=silver_results,
            supplemental_source_run_ids=tuple(item.run_id for item in silver_results),
            summary=summary,
        )
