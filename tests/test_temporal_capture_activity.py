from __future__ import annotations

import argparse
from typing import Any

import pytest

from video_media_catalog.temporal.activities import capture as capture_mod
from video_media_catalog.temporal.errors import NonRetryableCaptureError
from video_media_catalog.temporal.models import CaptureUnit, PipelineEnv


def _env() -> PipelineEnv:
    return PipelineEnv(
        destination_prefix="file:///tmp/tmdb-capture",
        image_digest="sha256:" + ("d" * 64),
        user_agent="ua",
        aws_region="us-east-1",
        warehouse_uri="s3://bucket/warehouse",
        record_staging_prefix="s3://bucket/warehouse/shards",
        silver_checkpoint_prefix="s3://bucket/warehouse/checkpoints",
        pipeline_summary_prefix="s3://bucket/landing/summaries",
        emr_application_name="app",
        emr_execution_role_arn="arn:aws:iam::1:role/r",
        emr_log_uri="s3://bucket/logs/",
        emr_entry_point="local:///opt/video-media-catalog/src/video_media_catalog/community_cli.py",
    )


def test_capture_day_inventory_uses_sync_runner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[str] = []

    def fake_run(parsed: argparse.Namespace) -> dict[str, object]:
        seen.append(parsed.mode)
        return {
            "batchId": "b1",
            "recordSetId": "r1",
            "recordCount": 2,
            "batchManifest": {
                "uri": "s3://b/batch.json",
                "format": "OBJECT_FORMAT_JSON",
                "mediaType": "application/json",
                "checksum": {"algorithm": "sha256", "value": "a" * 64},
                "sizeBytes": 1,
            },
            "recordSetManifest": {
                "uri": "s3://b/rs.json",
                "format": "OBJECT_FORMAT_JSON",
                "mediaType": "application/json",
                "checksum": {"algorithm": "sha256", "value": "b" * 64},
                "sizeBytes": 2,
            },
            "retryCount": 0,
            "rateLimitCount": 0,
        }

    monkeypatch.setattr(capture_mod, "run_sync", fake_run)
    results = capture_mod.capture_tmdb_day(
        CaptureUnit(kind="inventory", export_date="2026-10-07"),
        _env(),
        acquired_at="2026-10-08T00:00:00Z",
    )
    assert seen == ["daily-export"]
    assert results[0].batch_id == "b1"
    assert results[0].kind == "inventory"


def test_validate_token_requires_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MEDIA_CATALOG_TMDB_API_READ_TOKEN", raising=False)
    with pytest.raises(NonRetryableCaptureError, match="not configured"):
        capture_mod.validate_tmdb_token("ua")


def test_multi_shard_changes_day(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_shards(*_args: Any, **_kwargs: Any) -> list[dict[str, object]]:
        payloads = []
        for index in range(2):
            payloads.append(
                {
                    "batchId": f"b{index}",
                    "recordSetId": f"r{index}",
                    "recordCount": 1,
                    "windowCursor": f"cursor-{index}",
                    "windowShardIndex": index,
                    "windowShardCount": 2,
                    "batchManifest": {
                        "uri": f"s3://b/{index}/batch.json",
                        "format": "OBJECT_FORMAT_JSON",
                        "mediaType": "application/json",
                        "checksum": {"algorithm": "sha256", "value": "a" * 64},
                        "sizeBytes": 1,
                    },
                    "recordSetManifest": {
                        "uri": f"s3://b/{index}/rs.json",
                        "format": "OBJECT_FORMAT_JSON",
                        "mediaType": "application/json",
                        "checksum": {"algorithm": "sha256", "value": "b" * 64},
                        "sizeBytes": 2,
                    },
                }
            )
        return payloads

    monkeypatch.setattr(capture_mod, "_run_all_change_shards", fake_shards)
    results = capture_mod.capture_tmdb_day(
        CaptureUnit(
            kind="changes",
            window_start="2026-10-07",
            window_end="2026-10-07",
        ),
        _env(),
        acquired_at="2026-10-08T00:00:00Z",
    )
    assert len(results) == 2
    assert {item.window_shard_index for item in results} == {0, 1}
