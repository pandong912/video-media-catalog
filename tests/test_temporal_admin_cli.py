from __future__ import annotations

import asyncio
from typing import Any, cast

from temporalio.client import (
    Client,
    ScheduleAlreadyRunningError,
    ScheduleSpec,
)

from video_media_catalog.temporal import admin_cli
from video_media_catalog.temporal.models import PipelineEnv, PipelineInput


def _env() -> PipelineEnv:
    return PipelineEnv(
        destination_prefix="s3://bucket/capture",
        image_digest="sha256:" + ("d" * 64),
        user_agent="video-media-catalog-test",
        aws_region="us-east-1",
        warehouse_uri="s3://bucket/warehouse",
        record_staging_prefix="s3://bucket/warehouse/shards",
        silver_checkpoint_prefix="s3://bucket/warehouse/checkpoints",
        pipeline_summary_prefix="s3://bucket/summaries",
        emr_application_name="app",
        emr_execution_role_arn="arn:aws:iam::1:role/runtime",
        emr_log_uri="s3://bucket/logs",
        emr_entry_point="local:///opt/video-media-catalog/community_cli.py",
    )


class _ScheduleHandle:
    def __init__(self) -> None:
        self.updated = False

    async def update(self, updater: Any) -> None:
        update = await updater(None)
        assert update.schedule.state.note == "test schedule"
        self.updated = True


class _ExistingScheduleClient:
    def __init__(self) -> None:
        self.handle = _ScheduleHandle()

    def get_schedule_handle(self, schedule_id: str) -> _ScheduleHandle:
        assert schedule_id == "existing-schedule"
        return self.handle

    async def create_schedule(self, schedule_id: str, schedule: Any) -> None:
        assert schedule_id == "existing-schedule"
        raise ScheduleAlreadyRunningError()


def test_upsert_schedule_updates_sdk_wrapped_existing_schedule() -> None:
    client = _ExistingScheduleClient()

    asyncio.run(
        admin_cli._upsert_schedule(
            cast(Client, client),
            schedule_id="existing-schedule",
            spec=ScheduleSpec(),
            input_data=PipelineInput(mode="daily", env=_env()),
            task_queue="test-queue",
            note="test schedule",
        )
    )

    assert client.handle.updated is True
