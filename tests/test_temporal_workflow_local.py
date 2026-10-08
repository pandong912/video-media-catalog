from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from video_media_catalog.temporal.models import (
    CaptureResult,
    CaptureUnit,
    ObjectRefPayload,
    PipelineEnv,
    PipelineInput,
    PipelineResult,
    SilverResult,
)
from video_media_catalog.temporal.names import SILVER_TASK_QUEUE
from video_media_catalog.temporal.workflows.tmdb_pipeline import (
    TmdbCaptureSilverWorkflow,
)


def _ref(uri: str) -> ObjectRefPayload:
    return ObjectRefPayload(
        uri=uri,
        format="OBJECT_FORMAT_JSON",
        media_type="application/json",
        checksum_algorithm="sha256",
        checksum_value="a" * 64,
        size_bytes=12,
        etag='"etag"',
        object_version="v1",
    )


def test_manifest_checksum_algorithm_normalizes_to_sha256() -> None:
    canonical = _ref("s3://bucket/batch.json").checksum_digest()
    manifest = ObjectRefPayload(
        uri="s3://bucket/batch.json",
        format="OBJECT_FORMAT_JSON",
        media_type="application/json",
        checksum_algorithm="CHECKSUM_ALGORITHM_SHA256",
        checksum_value="a" * 64,
        size_bytes=12,
    )
    assert manifest.checksum_digest() == canonical == "sha256:" + ("a" * 64)
    with pytest.raises(ValueError, match="sha256 checksums"):
        ObjectRefPayload(
            uri="s3://bucket/batch.json",
            format="OBJECT_FORMAT_JSON",
            media_type="application/json",
            checksum_algorithm="md5",
            checksum_value="a" * 64,
            size_bytes=12,
        ).checksum_digest()


def _env() -> PipelineEnv:
    return PipelineEnv(
        destination_prefix="s3://bucket/landing/research/capture",
        image_digest="sha256:" + ("b" * 64),
        user_agent="video-media-catalog/test",
        aws_region="us-east-1",
        warehouse_uri="s3://bucket/warehouse",
        record_staging_prefix="s3://bucket/warehouse/research/control/record-shards",
        silver_checkpoint_prefix=(
            "s3://bucket/warehouse/research/control/source-silver-checkpoints"
        ),
        pipeline_summary_prefix="s3://bucket/landing/research/pipeline-summaries",
        emr_application_name="app",
        emr_execution_role_arn="arn:aws:iam::123:role/emr",
        emr_log_uri="s3://bucket/raw/logs/",
        emr_entry_point=(
            "local:///opt/video-media-catalog/src/video_media_catalog/community_cli.py"
        ),
        capture_concurrency=2,
    )


def test_bootstrap_runs_captures_then_sequential_silver(tmp_path) -> None:
    asyncio.run(_bootstrap_case(tmp_path))


async def _bootstrap_case(tmp_path) -> None:
    calls: list[str] = []

    @activity.defn(name="ValidateTmdbToken")
    async def validate_tmdb_token(user_agent: str) -> bool:
        calls.append(f"token:{user_agent}")
        return True

    @activity.defn(name="CaptureTmdbDay")
    async def capture_tmdb_day(
        unit: CaptureUnit,
        env: PipelineEnv,
        acquired_at: str | None = None,
    ) -> list[CaptureResult]:
        calls.append(f"capture:{unit.kind}:{unit.export_date or unit.window_start}")
        batch = f"batch-{unit.kind}-{unit.export_date or unit.window_start}"
        return [
            CaptureResult(
                kind=unit.kind,
                batch_id=batch,
                record_set_id=f"rs-{batch}",
                record_count=1,
                batch_manifest=_ref(f"s3://bucket/{batch}/batch.json"),
                record_set_manifest=_ref(f"s3://bucket/{batch}/rs.json"),
                export_date=unit.export_date,
                window_start=unit.window_start,
                window_end=unit.window_end,
            )
        ]

    @activity.defn(name="SubmitSourceSilver")
    async def submit_source_silver(
        capture: CaptureResult,
        env: PipelineEnv,
        committed_at: str | None = None,
    ) -> SilverResult:
        calls.append(f"silver:{capture.batch_id}")
        return SilverResult(
            batch_id=capture.batch_id,
            run_id="sha256:" + ("c" * 64),
            commit_key=f"commit-{capture.batch_id}",
            job_run_id=f"job-{capture.batch_id}",
            table_counts={"source_record": 1},
            table_snapshot_ids={"source_record": 1},
        )

    @activity.defn(name="WritePipelineSummary")
    async def write_pipeline_summary(
        result: PipelineResult,
        summary_prefix: str,
    ) -> str:
        path = tmp_path / "summary.json"
        path.write_text(
            f"{result.capture_count}:{len(result.silvers)}",
            encoding="utf-8",
        )
        calls.append("summary")
        return str(path)

    async with (
        await WorkflowEnvironment.start_time_skipping() as env,
        Worker(
            env.client,
            task_queue="tq",
            workflows=[TmdbCaptureSilverWorkflow],
            activities=[
                validate_tmdb_token,
                capture_tmdb_day,
                write_pipeline_summary,
            ],
        ),
        Worker(
            env.client,
            task_queue=SILVER_TASK_QUEUE,
            activities=[submit_source_silver],
        ),
    ):
        result = await env.client.execute_workflow(
            TmdbCaptureSilverWorkflow.run,
            PipelineInput(
                mode="bootstrap",
                env=_env(),
                export_date="2026-10-07",
                window_start="2026-10-06",
                window_end="2026-10-07",
                acquired_at="2026-10-08T00:00:00Z",
            ),
            id="test-bootstrap",
            task_queue="tq",
            execution_timeout=timedelta(minutes=5),
        )

    assert result.capture_count == 3
    assert len(result.silvers) == 3
    assert calls[0].startswith("token:")
    silver_calls = [item for item in calls if item.startswith("silver:")]
    assert silver_calls == [
        "silver:batch-inventory-2026-10-07",
        "silver:batch-changes-2026-10-06",
        "silver:batch-changes-2026-10-07",
    ]
    assert calls[-1] == "summary"
