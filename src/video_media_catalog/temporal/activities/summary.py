"""Write TMDB pipeline summaries to private S3 landing."""

from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

from temporalio import activity

from video_media_catalog.canonical import canonical_json_bytes
from video_media_catalog.temporal.models import PipelineResult


def _join_prefix(prefix: str, *parts: str) -> str:
    base = prefix.rstrip("/")
    return "/".join((base, *parts))


@activity.defn(name="WritePipelineSummary")
def write_pipeline_summary(result: PipelineResult, summary_prefix: str) -> str:
    payload: dict[str, Any] = {
        "schemaVersion": "1.0",
        "mode": result.mode,
        "acquiredAt": result.acquired_at,
        "exportDate": result.export_date,
        "windowStart": result.window_start,
        "windowEnd": result.window_end,
        "captureCount": result.capture_count,
        "captures": [
            {
                "kind": item.kind,
                "batchId": item.batch_id,
                "recordSetId": item.record_set_id,
                "recordCount": item.record_count,
                "exportDate": item.export_date,
                "windowStart": item.window_start,
                "windowEnd": item.window_end,
                "windowCursor": item.window_cursor,
                "windowShardIndex": item.window_shard_index,
                "windowShardCount": item.window_shard_count,
                "batchManifest": {
                    "uri": item.batch_manifest.uri,
                    "format": item.batch_manifest.format,
                    "mediaType": item.batch_manifest.media_type,
                    "checksum": {
                        "algorithm": item.batch_manifest.checksum_algorithm,
                        "value": item.batch_manifest.checksum_value,
                    },
                    "sizeBytes": item.batch_manifest.size_bytes,
                    "etag": item.batch_manifest.etag,
                    "objectVersion": item.batch_manifest.object_version,
                },
                "recordSetManifest": {
                    "uri": item.record_set_manifest.uri,
                    "format": item.record_set_manifest.format,
                    "mediaType": item.record_set_manifest.media_type,
                    "checksum": {
                        "algorithm": item.record_set_manifest.checksum_algorithm,
                        "value": item.record_set_manifest.checksum_value,
                    },
                    "sizeBytes": item.record_set_manifest.size_bytes,
                    "etag": item.record_set_manifest.etag,
                    "objectVersion": item.record_set_manifest.object_version,
                },
            }
            for item in result.captures
        ],
        "sourceSilver": [
            {
                "batchId": item.batch_id,
                "runId": item.run_id,
                "commitKey": item.commit_key,
                "jobRunId": item.job_run_id,
                "tableCounts": item.table_counts,
                "tableSnapshotIds": item.table_snapshot_ids,
            }
            for item in result.silvers
        ],
        "opensearchPublication": "disabled",
        "identityPublication": "disabled",
        "goldPublication": "disabled",
    }
    body = canonical_json_bytes(payload)
    key_name = (
        f"{result.mode}-"
        f"{result.export_date or result.window_start or result.acquired_at[:10]}"
        f"-{result.acquired_at.replace(':', '').replace('+', '')}.json"
    )
    uri = _join_prefix(summary_prefix, "tmdb", key_name)
    parsed = urlparse(uri)
    if parsed.scheme == "file":
        from pathlib import Path

        path = Path(parsed.path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
    elif parsed.scheme == "s3":
        import boto3

        boto3.client("s3").put_object(
            Bucket=parsed.netloc,
            Key=parsed.path.lstrip("/"),
            Body=body,
            ContentType="application/json",
        )
    else:
        raise ValueError("pipeline summary prefix must use file:// or s3://")
    if activity.in_activity():
        activity.heartbeat({"phase": "summary-written", "uri": uri})
    return uri
