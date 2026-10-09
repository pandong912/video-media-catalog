from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker
from test_temporal_golden_models import _input, _ref, _run

from video_media_catalog.temporal.golden_models import (
    GoldenBuildInput,
    GoldenBuildResult,
    GoldenEpochResult,
    GoldenIdentityResult,
    GoldenPreflightResult,
    GoldenReleaseResult,
    GoldenSnapshotResult,
)
from video_media_catalog.temporal.names import GOLDEN_EMR_TASK_QUEUE
from video_media_catalog.temporal.workflows.golden_build import GoldenBuildWorkflow


def test_golden_workflow_runs_commit_stages_sequentially() -> None:
    asyncio.run(_golden_workflow_case(reuse_identity=False))


def test_golden_workflow_reuses_pinned_identity() -> None:
    asyncio.run(_golden_workflow_case(reuse_identity=True))


async def _golden_workflow_case(*, reuse_identity: bool) -> None:
    calls: list[str] = []
    input_data = _input()

    @activity.defn(name="PrepareGoldenBuild")
    async def prepare(
        build_input: GoldenBuildInput,
        planned_at: str,
    ) -> GoldenPreflightResult:
        calls.append("prepare")
        tmdb_run_ids = tuple(sorted(build_input.tmdb_source_run_ids))
        return GoldenPreflightResult(
            build_spec=_ref("s3://bucket/build-spec.json"),
            planned_at=planned_at,
            all_snapshot_run_ids=tuple(
                sorted(
                    (
                        *build_input.baseline_source_run_ids,
                        build_input.baseline_identity_run_id,
                        *build_input.tmdb_source_run_ids,
                    )
                )
            ),
            tmdb_source_run_ids=tmdb_run_ids,
            imdb_age_hours=1.1,
            tvmaze_age_hours=1.2,
            imdb_slo_hours=2,
            tvmaze_slo_hours=2,
            freshness_policy_digest=_run(40),
            identity_config_digest=_run(30),
            identity_resolution_config_digest=_run(38),
            identity_bound_config_digest=_run(39),
            reused_identity=(
                GoldenIdentityResult(
                    run_id=_run(32),
                    commit_key=_run(33),
                    source_run_ids=tmdb_run_ids,
                    table_counts={"community_entity_ledger": 1},
                    table_snapshot_ids={"community_entity_ledger": 3},
                    job_run_id="reused-identity-job",
                )
                if reuse_identity
                else None
            ),
        )

    @activity.defn(name="PublishGoldenSnapshot")
    async def snapshot(
        _build_input: GoldenBuildInput,
        _preflight: GoldenPreflightResult,
    ) -> GoldenSnapshotResult:
        calls.append("snapshot")
        return GoldenSnapshotResult(
            snapshot_set_id=_run(31),
            snapshot=_ref("s3://bucket/snapshot.json"),
            job_run_id="snapshot-job",
        )

    @activity.defn(name="ResolveGoldenIdentity")
    async def identity(
        _build_input: GoldenBuildInput,
        preflight: GoldenPreflightResult,
        _snapshot: GoldenSnapshotResult,
        _started_at: str,
        _committed_at: str,
    ) -> GoldenIdentityResult:
        calls.append("identity")
        return GoldenIdentityResult(
            run_id=_run(32),
            commit_key=_run(33),
            source_run_ids=preflight.tmdb_source_run_ids,
            table_counts={"community_entity_ledger": 1},
            table_snapshot_ids={"community_entity_ledger": 3},
            job_run_id="identity-job",
        )

    @activity.defn(name="PublishGoldenEpoch")
    async def epoch(
        _build_input: GoldenBuildInput,
        _preflight: GoldenPreflightResult,
        _identity: GoldenIdentityResult,
        _created_at: str,
    ) -> GoldenEpochResult:
        calls.append("epoch")
        return GoldenEpochResult(
            epoch_id=_run(34),
            epoch=_ref("s3://bucket/epoch.json"),
            committed_run_count=20,
            committed_run_digest=_run(35),
            data_snapshot_ids={"community_ingest_run": 4},
            job_run_id="epoch-job",
        )

    @activity.defn(name="SubmitGoldenRelease")
    async def release(
        _build_input: GoldenBuildInput,
        _preflight: GoldenPreflightResult,
        _epoch: GoldenEpochResult,
        _committed_at: str,
    ) -> GoldenReleaseResult:
        calls.append("release")
        return GoldenReleaseResult(
            release_plan_id=_run(36),
            commit_key=_run(37),
            quality_report=_ref("s3://bucket/quality.json"),
            attribution_manifest=_ref("s3://bucket/attribution.json"),
            release_commit=_ref("s3://bucket/release.json"),
            table_counts={"community_gold_entity": 1},
            table_snapshot_ids={"community_gold_entity": 5},
            job_run_id="release-job",
        )

    @activity.defn(name="WriteGoldenBuildSummary")
    async def write_summary(
        _result: GoldenBuildResult,
        _prefix: str,
        _region: str,
    ) -> str:
        calls.append("summary")
        return "s3://bucket/summaries/build.json"

    async with (
        await WorkflowEnvironment.start_time_skipping() as env,
        Worker(
            env.client,
            task_queue="golden-test",
            workflows=[GoldenBuildWorkflow],
            activities=[prepare, write_summary],
        ),
        Worker(
            env.client,
            task_queue=GOLDEN_EMR_TASK_QUEUE,
            activities=[snapshot, identity, epoch, release],
        ),
    ):
        result = await env.client.execute_workflow(
            GoldenBuildWorkflow.run,
            input_data,
            id="golden-build-test",
            task_queue="golden-test",
            execution_timeout=timedelta(minutes=5),
        )

    expected_calls = [
        "prepare",
        "snapshot",
        "epoch",
        "release",
        "summary",
    ]
    if not reuse_identity:
        expected_calls.insert(2, "identity")
    assert calls == expected_calls
    assert result.summary_uri == "s3://bucket/summaries/build.json"
    assert result.publication_status == {
        "opensearch": "disabled",
        "canonical": "disabled",
        "cutover": "disabled",
    }
