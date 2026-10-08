from __future__ import annotations

import pytest

from video_media_catalog.temporal.activities.silver import (
    _client_token,
    _emr_namespace,
    _verify_summary,
)
from video_media_catalog.temporal.errors import NonRetryableSilverError
from video_media_catalog.temporal.models import (
    CaptureResult,
    ObjectRefPayload,
    PipelineEnv,
)


def test_emr_client_token_is_stable_per_workflow_execution() -> None:
    first = _client_token("sha256:" + ("a" * 64), "run-1")

    assert first == _client_token("sha256:" + ("a" * 64), "run-1")
    assert first != _client_token("sha256:" + ("a" * 64), "run-2")
    assert first.startswith("tmdb-ss-")
    assert len(first) <= 64


def test_source_silver_allows_null_snapshot_only_for_empty_tables() -> None:
    summary = {
        "runId": "sha256:" + ("a" * 64),
        "commitKey": "sha256:" + ("b" * 64),
        "tableCounts": {"empty": 0, "populated": 2},
        "tableSnapshotIds": {"empty": None, "populated": 123},
    }

    _verify_summary(summary, batch_id="sha256:" + ("c" * 64))

    summary["tableSnapshotIds"]["populated"] = None
    with pytest.raises(NonRetryableSilverError, match="rows but no snapshot"):
        _verify_summary(summary, batch_id="sha256:" + ("c" * 64))


def test_source_silver_uses_scaled_emr_profile() -> None:
    ref = ObjectRefPayload(
        uri="s3://bucket/manifest.json",
        format="OBJECT_FORMAT_JSON",
        media_type="application/json",
        checksum_algorithm="CHECKSUM_ALGORITHM_SHA256",
        checksum_value="a" * 64,
        size_bytes=12,
    )
    capture = CaptureResult(
        kind="changes",
        batch_id="sha256:" + ("b" * 64),
        record_set_id="sha256:" + ("c" * 64),
        record_count=1,
        batch_manifest=ref,
        record_set_manifest=ref,
    )
    env = PipelineEnv(
        destination_prefix="s3://bucket/capture",
        image_digest="sha256:" + ("d" * 64),
        user_agent="video-media-catalog/test",
        aws_region="us-east-1",
        warehouse_uri="s3://bucket/warehouse",
        record_staging_prefix="s3://bucket/staging",
        silver_checkpoint_prefix="s3://bucket/checkpoints",
        pipeline_summary_prefix="s3://bucket/summaries",
        emr_application_name="media-catalog",
        emr_execution_role_arn="arn:aws:iam::123456789012:role/emr",
        emr_log_uri="s3://bucket/logs",
        emr_entry_point="local:///opt/video-media-catalog/community_cli.py",
    )

    parsed = _emr_namespace(
        env=env,
        capture=capture,
        committed_at="2026-10-09T00:00:00Z",
        workflow_run_id="run-1",
    )

    assert parsed.executor_instances == 12
    assert parsed.shuffle_partitions == 288
