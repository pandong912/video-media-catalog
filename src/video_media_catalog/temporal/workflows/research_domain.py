"""Domain-profile orchestration for global-reference research releases."""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from video_media_catalog.temporal.activities.research_domain import (
        load_pinned_research_capture,
    )
    from video_media_catalog.temporal.activities.silver import (
        submit_source_silver_bundle,
    )
    from video_media_catalog.temporal.golden_models import (
        GoldenSourceFreshnessStatus,
        GoldenSourceRunState,
        GoldenSourceRunStatus,
    )
    from video_media_catalog.temporal.names import (
        SILVER_TASK_QUEUE,
    )
    from video_media_catalog.temporal.research_domain_models import (
        ResearchCaptureResult,
        ResearchDomainInput,
        ResearchDomainResult,
        ResearchSourceProfile,
        ResearchSourceResult,
    )
    from video_media_catalog.temporal.workflows.golden_build import (
        GoldenBuildWorkflow,
    )


def _retry_policy(profile: ResearchSourceProfile) -> RetryPolicy:
    return RetryPolicy(
        initial_interval=timedelta(seconds=profile.initial_retry_seconds),
        backoff_coefficient=2.0,
        maximum_interval=timedelta(seconds=profile.maximum_retry_seconds),
        maximum_attempts=profile.maximum_attempts,
    )


@workflow.defn(name="ResearchCaptureWorkflow")
class ResearchCaptureWorkflow:
    @workflow.run
    async def run(
        self,
        profile: ResearchSourceProfile,
    ) -> ResearchCaptureResult:
        return await workflow.execute_activity(
            load_pinned_research_capture,
            profile,
            start_to_close_timeout=timedelta(minutes=profile.capture_timeout_minutes),
            heartbeat_timeout=timedelta(minutes=2),
            retry_policy=_retry_policy(profile),
        )


@workflow.defn(name="ResearchSilverWorkflow")
class ResearchSilverWorkflow:
    @workflow.run
    async def run(
        self,
        profile: ResearchSourceProfile,
        capture: ResearchCaptureResult,
    ) -> ResearchSourceResult:
        if (
            capture.profile_id != profile.profile_id
            or capture.domain != profile.domain
            or capture.source_product_id != profile.source_product_id
            or capture.watermark != profile.watermark
        ):
            raise ValueError("research capture does not match its source profile")
        silvers = await workflow.execute_activity(
            submit_source_silver_bundle,
            args=[capture.captures, profile.env, capture.acquired_at],
            task_queue=SILVER_TASK_QUEUE,
            start_to_close_timeout=timedelta(minutes=profile.silver_timeout_minutes),
            heartbeat_timeout=timedelta(minutes=5),
            retry_policy=_retry_policy(profile),
        )
        return ResearchSourceResult(
            profile_id=profile.profile_id,
            domain=profile.domain,
            source_product_id=profile.source_product_id,
            watermark=profile.watermark,
            acquired_at=capture.acquired_at,
            capture_manifest=profile.capture_manifest,
            captures=capture.captures,
            silvers=tuple(silvers),
        )


def _golden_input(input_data: ResearchDomainInput):
    source_results = input_data.completed_sources
    supplemental = {
        *input_data.golden_build.supplemental_source_run_ids,
        *(silver.run_id for source in source_results for silver in source.silvers),
    }
    watermarks = {
        **input_data.golden_build.source_watermarks,
        **{source.source_product_id: source.watermark for source in source_results},
    }
    states = {
        state.source_product_id: state
        for state in input_data.golden_build.source_run_states
    }
    profiles = {profile.profile_id: profile for profile in input_data.sources}
    for source in source_results:
        profile = profiles[source.profile_id]
        prior = states.get(source.source_product_id)
        replacement = GoldenSourceRunState(
            source_product_id=source.source_product_id,
            run_ids=tuple(
                sorted(
                    {
                        *(prior.run_ids if prior is not None else ()),
                        *(silver.run_id for silver in source.silvers),
                    }
                )
            ),
            run_status=GoldenSourceRunStatus.COMMITTED,
            freshness_status=GoldenSourceFreshnessStatus.PASS,
            watermark=source.watermark,
            acquired_at=source.acquired_at,
            identity_spine=profile.identity_spine,
            materialization=profile.materialization,
            gold_contribution=profile.gold_contribution,
            capture_reference=source.capture_manifest,
        )
        states[source.source_product_id] = replacement
    return replace(
        input_data.golden_build,
        supplemental_source_run_ids=tuple(sorted(supplemental)),
        source_watermarks=watermarks,
        source_run_states=tuple(
            states[source_product_id] for source_product_id in sorted(states)
        ),
    )


@workflow.defn(name="ResearchDomainWorkflow")
class ResearchDomainWorkflow:
    @workflow.run
    async def run(
        self,
        input_data: ResearchDomainInput,
    ) -> ResearchDomainResult:
        completed = list(input_data.completed_sources)
        failed = list(input_data.failed_source_profile_ids)
        index = input_data.next_source_index
        processed_in_run = 0
        while index < len(input_data.sources):
            profile = input_data.sources[index]
            child_prefix = f"{workflow.info().workflow_id}/{index}-{profile.profile_id}"
            try:
                capture = await workflow.execute_child_workflow(
                    ResearchCaptureWorkflow.run,
                    profile,
                    id=f"{child_prefix}/capture",
                    execution_timeout=timedelta(
                        minutes=profile.capture_timeout_minutes + 30
                    ),
                )
                source = await workflow.execute_child_workflow(
                    ResearchSilverWorkflow.run,
                    args=[profile, capture],
                    id=f"{child_prefix}/silver",
                    execution_timeout=timedelta(
                        minutes=profile.silver_timeout_minutes + 30
                    ),
                )
                completed.append(source)
                workflow.metric_meter().create_counter(
                    "video_media_catalog_research_source_completed"
                ).add(
                    1,
                    {
                        "domain": profile.domain,
                        "source_product_id": profile.source_product_id,
                    },
                )
            except Exception:
                failed.append(profile.profile_id)
                workflow.metric_meter().create_counter(
                    "video_media_catalog_research_source_failures"
                ).add(
                    1,
                    {
                        "domain": profile.domain,
                        "source_product_id": profile.source_product_id,
                    },
                )
                workflow.logger.exception(
                    "Research source circuit breaker recorded a failure",
                    extra={
                        "profileId": profile.profile_id,
                        "failureCount": len(failed),
                        "failureBudget": input_data.failure_budget,
                    },
                )
                if len(failed) >= input_data.failure_budget:
                    raise
            index += 1
            processed_in_run += 1
            if index < len(input_data.sources) and (
                processed_in_run >= input_data.continue_as_new_after_sources
                or workflow.info().is_continue_as_new_suggested()
            ):
                workflow.continue_as_new(
                    args=[
                        replace(
                            input_data,
                            next_source_index=index,
                            completed_sources=tuple(completed),
                            failed_source_profile_ids=tuple(failed),
                        )
                    ]
                )
        if failed:
            raise RuntimeError(
                "research domain workflow exhausted sources with failures"
            )
        finalized = replace(
            input_data,
            next_source_index=index,
            completed_sources=tuple(completed),
        )
        golden = await workflow.execute_child_workflow(
            GoldenBuildWorkflow.run,
            _golden_input(finalized),
            id=f"{workflow.info().workflow_id}/golden",
            task_queue=input_data.golden_task_queue,
            execution_timeout=timedelta(hours=48),
        )
        workflow.metric_meter().create_counter(
            "video_media_catalog_research_domain_releases"
        ).add(1, {"domain": input_data.domain})
        return ResearchDomainResult(
            domain=input_data.domain,
            sources=tuple(completed),
            golden=golden,
        )
