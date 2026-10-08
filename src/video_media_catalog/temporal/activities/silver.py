"""EMR Serverless Source Silver activity with heartbeat polling and verification."""

from __future__ import annotations

import argparse
import hashlib
import re
from datetime import UTC, datetime
from typing import Any

from temporalio import activity

from video_media_catalog.emr_driver_logs import read_source_silver_summary
from video_media_catalog.emr_serverless_cli import run as run_emr_submit
from video_media_catalog.temporal.errors import (
    NonRetryableSilverError,
    classify_silver_exception,
)
from video_media_catalog.temporal.models import (
    CaptureResult,
    PipelineEnv,
    SilverResult,
)

_CLIENT_TOKEN = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


def _now_rfc3339() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _client_token(batch_id: str, workflow_run_id: str) -> str:
    digest = hashlib.sha256(
        f"tmdb-source-silver:{batch_id}:{workflow_run_id}".encode()
    ).hexdigest()
    token = f"tmdb-ss-{digest[:54]}"
    if not _CLIENT_TOKEN.fullmatch(token):
        raise NonRetryableSilverError("failed to build EMR client token")
    return token


def _job_name(batch_id: str) -> str:
    digest = hashlib.sha256(batch_id.encode()).hexdigest()[:20]
    name = f"tmdb-ss-{digest}"
    return name[:64]


def _object_args(prefix: str, ref: Any) -> list[str]:
    args = [
        f"--{prefix}-uri",
        ref.uri,
        f"--{prefix}-hash",
        ref.checksum_digest(),
        f"--{prefix}-size",
        str(ref.size_bytes),
    ]
    if ref.object_version:
        args.extend([f"--{prefix}-version", ref.object_version])
    if ref.etag:
        args.extend([f"--{prefix}-etag", ref.etag])
    return args


def _emr_namespace(
    *,
    env: PipelineEnv,
    capture: CaptureResult,
    committed_at: str,
    workflow_run_id: str,
) -> argparse.Namespace:
    entry_args = [
        *_object_args("batch-manifest", capture.batch_manifest),
        *_object_args("record-set-manifest", capture.record_set_manifest),
        "--committed-at",
        committed_at,
        "--catalog-type",
        env.catalog_type,
        "--catalog-name",
        env.catalog_name,
        "--namespace",
        env.glue_namespace,
        "--warehouse",
        env.warehouse_uri,
        "--record-staging-prefix",
        env.record_staging_prefix,
        "--silver-checkpoint-prefix",
        env.silver_checkpoint_prefix,
        "--aws-region",
        env.aws_region,
        "--s3-credentials-provider",
        "default",
    ]
    return argparse.Namespace(
        application_name=env.emr_application_name,
        execution_role_arn=env.emr_execution_role_arn,
        job_name=_job_name(capture.batch_id),
        client_token=_client_token(capture.batch_id, workflow_run_id),
        entry_point=env.emr_entry_point,
        log_uri=env.emr_log_uri,
        aws_region=env.aws_region,
        poll_seconds=15,
        execution_timeout_minutes=720,
        max_attempts=1,
        driver_cores=4,
        driver_memory="16g",
        driver_memory_overhead=None,
        driver_disk="50G",
        executor_cores=4,
        executor_memory="24g",
        executor_memory_overhead=None,
        executor_disk="200G",
        executor_instances=4,
        shuffle_partitions=96,
        entry_point_arguments=entry_args,
    )


def _verify_summary(summary: dict[str, Any], *, batch_id: str) -> None:
    required = ("runId", "commitKey", "tableCounts", "tableSnapshotIds")
    missing = [key for key in required if key not in summary]
    if missing:
        raise NonRetryableSilverError(
            f"Source Silver summary missing fields: {', '.join(missing)}"
        )
    if not str(summary["runId"]).startswith("sha256:"):
        raise NonRetryableSilverError("Source Silver runId must be a sha256 digest")
    if not summary["commitKey"]:
        raise NonRetryableSilverError("Source Silver commitKey is empty")
    counts = summary["tableCounts"]
    if not isinstance(counts, dict) or not counts:
        raise NonRetryableSilverError(
            "Source Silver tableCounts must be a non-empty map"
        )
    snapshots = summary["tableSnapshotIds"]
    if not isinstance(snapshots, dict) or not snapshots:
        raise NonRetryableSilverError(
            "Source Silver tableSnapshotIds must be a non-empty map"
        )
    for table, count in counts.items():
        try:
            parsed_count = int(count)
        except (TypeError, ValueError) as exc:
            raise NonRetryableSilverError(
                f"Source Silver table count for {table} is invalid"
            ) from exc
        if parsed_count < 0:
            raise NonRetryableSilverError(
                f"Source Silver table count for {table} is negative"
            )
        snapshot_id = snapshots.get(table)
        if parsed_count > 0 and snapshot_id is None:
            raise NonRetryableSilverError(
                f"Source Silver table {table} has rows but no snapshot"
            )
        if snapshot_id is not None:
            try:
                parsed_snapshot_id = int(snapshot_id)
            except (TypeError, ValueError) as exc:
                raise NonRetryableSilverError(
                    f"Source Silver snapshot ID for {table} is invalid"
                ) from exc
            if parsed_snapshot_id <= 0:
                raise NonRetryableSilverError(
                    f"Source Silver snapshot ID for {table} is not positive"
                )
    # batch_id is retained for correlation in logs/heartbeats.
    _ = batch_id


@activity.defn(name="SubmitSourceSilver")
def submit_source_silver(
    capture: CaptureResult,
    env: PipelineEnv,
    committed_at: str | None = None,
) -> SilverResult:
    committed = committed_at or _now_rfc3339()
    workflow_run_id = (
        activity.info().workflow_run_id if activity.in_activity() else "standalone"
    )
    parsed = _emr_namespace(
        env=env,
        capture=capture,
        committed_at=committed,
        workflow_run_id=workflow_run_id,
    )

    def on_progress(event: dict[str, Any]) -> None:
        if activity.in_activity():
            activity.heartbeat(
                {
                    "phase": "emr-source-silver",
                    "batchId": capture.batch_id,
                    **event,
                }
            )

    def should_cancel() -> bool:
        if not activity.in_activity():
            return False
        return activity.is_cancelled()

    try:
        job_run = run_emr_submit(
            parsed,
            progress_callback=on_progress,
            should_cancel=should_cancel,
        )
        application_id = str(job_run["applicationId"])
        job_run_id = str(job_run["jobRunId"])
        on_progress(
            {
                "event": "reading-driver-stdout",
                "applicationId": application_id,
                "jobRunId": job_run_id,
            }
        )
        import boto3

        summary = read_source_silver_summary(
            s3_client=boto3.client("s3", region_name=env.aws_region),
            log_uri=env.emr_log_uri,
            application_id=application_id,
            job_run_id=job_run_id,
        )
        _verify_summary(summary, batch_id=capture.batch_id)
        return SilverResult(
            batch_id=capture.batch_id,
            run_id=str(summary["runId"]),
            commit_key=str(summary["commitKey"]),
            job_run_id=job_run_id,
            table_counts={str(k): int(v) for k, v in summary["tableCounts"].items()},
            table_snapshot_ids={
                str(k): None if v is None else int(v)
                for k, v in summary["tableSnapshotIds"].items()
            },
        )
    except Exception as exc:
        raise classify_silver_exception(exc) from exc
