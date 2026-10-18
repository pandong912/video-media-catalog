"""EMR Serverless Source Silver activity with heartbeat polling and verification."""

from __future__ import annotations

import argparse
import hashlib
import re
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

from temporalio import activity

from video_media_catalog.canonical import canonical_json_bytes
from video_media_catalog.community_cli import (
    CONTROL_MAX_BYTES,
    SOURCE_SILVER_BATCH_PLAN_MEDIA_TYPE,
)
from video_media_catalog.emr_driver_logs import read_source_silver_summary
from video_media_catalog.emr_serverless_cli import run as run_emr_submit
from video_media_catalog.object_store import (
    BoundedObjectStore,
    conditional_publish_bytes,
)
from video_media_catalog.storage import join_uri
from video_media_catalog.temporal.errors import (
    NonRetryableSilverError,
    classify_silver_exception,
)
from video_media_catalog.temporal.models import (
    CaptureResult,
    ObjectRefPayload,
    PipelineEnv,
    SilverResult,
)
from video_media_catalog.v2_contracts import require_sha256

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


def _bundle_client_token(plan_digest: str, workflow_run_id: str) -> str:
    digest = hashlib.sha256(
        f"source-silver-bundle:{plan_digest}:{workflow_run_id}".encode()
    ).hexdigest()
    token = f"ssb-{digest[:58]}"
    if not _CLIENT_TOKEN.fullmatch(token):
        raise NonRetryableSilverError("failed to build EMR bundle client token")
    return token


def _job_name(batch_id: str) -> str:
    digest = hashlib.sha256(batch_id.encode()).hexdigest()[:20]
    name = f"tmdb-ss-{digest}"
    return name[:64]


def _bundle_job_name(plan_digest: str) -> str:
    digest = hashlib.sha256(plan_digest.encode()).hexdigest()[:20]
    return f"ss-bundle-{digest}"[:64]


def _resume_job_run_id(
    *,
    phase: str,
    identity_key: str,
    identity_value: str,
) -> str | None:
    if not activity.in_activity():
        return None
    heartbeat_details = getattr(activity.info(), "heartbeat_details", ())
    if isinstance(heartbeat_details, dict):
        heartbeat_details = (heartbeat_details,)
    for details in reversed(tuple(heartbeat_details)):
        if (
            isinstance(details, dict)
            and details.get("phase") == phase
            and details.get(identity_key) == identity_value
            and details.get("jobRunId")
        ):
            return str(details["jobRunId"])
    return None


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
        executor_instances=env.emr_executor_instances,
        shuffle_partitions=env.emr_shuffle_partitions,
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
            resume_job_run_id=_resume_job_run_id(
                phase="emr-source-silver",
                identity_key="batchId",
                identity_value=capture.batch_id,
            ),
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


def _plan_object_mapping(reference: ObjectRefPayload) -> dict[str, Any]:
    mapping: dict[str, Any] = {
        "uri": reference.uri,
        "hash": reference.checksum_digest(),
        "size": reference.size_bytes,
    }
    if reference.object_version:
        mapping["version"] = reference.object_version
    if reference.etag:
        mapping["etag"] = reference.etag
    return mapping


def _publish_batch_plan(
    *,
    captures: Sequence[CaptureResult],
    env: PipelineEnv,
    committed_at: str,
) -> tuple[str, list[str], list[str]]:
    if not captures:
        raise NonRetryableSilverError(
            "Source Silver bundle requires at least one batch"
        )
    plan_batches = []
    batch_ids: list[str] = []
    for capture in captures:
        batch_id = require_sha256(capture.batch_id, label="source silver batchId")
        batch_ids.append(batch_id)
        plan_batches.append(
            {
                "batchId": batch_id,
                "batchManifest": _plan_object_mapping(capture.batch_manifest),
                "recordSetManifest": _plan_object_mapping(capture.record_set_manifest),
            }
        )
    body = canonical_json_bytes(
        {
            "schemaVersion": 1,
            "committedAt": committed_at,
            "batches": plan_batches,
        },
        newline=True,
    )
    plan_digest = "sha256:" + hashlib.sha256(body).hexdigest()
    destination = join_uri(
        env.destination_prefix,
        "source-silver-batch-plans",
        f"{plan_digest.removeprefix('sha256:')}.json",
    )
    store = BoundedObjectStore(region=env.aws_region)
    published = conditional_publish_bytes(
        store,
        body,
        destination,
        media_type=SOURCE_SILVER_BATCH_PLAN_MEDIA_TYPE,
        object_format="OBJECT_FORMAT_JSON",
        max_bytes=CONTROL_MAX_BYTES,
    )
    reference = published.object_ref
    entry_args = [
        *_object_args(
            "batch-plan",
            ObjectRefPayload(
                uri=reference.uri,
                format=reference.format,
                media_type=reference.media_type,
                checksum_algorithm=reference.checksum.algorithm,
                checksum_value=reference.checksum.value,
                size_bytes=reference.size_bytes,
                etag=reference.etag,
                object_version=reference.object_version,
            ),
        ),
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
    return plan_digest, batch_ids, entry_args


@activity.defn(name="SubmitSourceSilverBundle")
def submit_source_silver_bundle(
    captures: Sequence[CaptureResult],
    env: PipelineEnv,
    committed_at: str | None = None,
) -> list[SilverResult]:
    committed = committed_at or _now_rfc3339()
    workflow_run_id = (
        activity.info().workflow_run_id if activity.in_activity() else "standalone"
    )
    try:
        plan_digest, batch_ids, entry_args = _publish_batch_plan(
            captures=captures,
            env=env,
            committed_at=committed,
        )
        parsed = argparse.Namespace(
            application_name=env.emr_application_name,
            execution_role_arn=env.emr_execution_role_arn,
            job_name=_bundle_job_name(plan_digest),
            client_token=_bundle_client_token(plan_digest, workflow_run_id),
            entry_point=env.emr_entry_point,
            log_uri=env.emr_log_uri,
            aws_region=env.aws_region,
            poll_seconds=15,
            execution_timeout_minutes=4_320,
            max_attempts=1,
            driver_cores=4,
            driver_memory="16g",
            driver_memory_overhead=None,
            driver_disk="50G",
            executor_cores=4,
            executor_memory="24g",
            executor_memory_overhead=None,
            executor_disk="200G",
            executor_instances=env.emr_executor_instances,
            shuffle_partitions=env.emr_shuffle_partitions,
            entry_point_arguments=entry_args,
        )

        def on_progress(event: dict[str, Any]) -> None:
            if activity.in_activity():
                activity.heartbeat(
                    {
                        "phase": "emr-source-silver-bundle",
                        "batchCount": len(batch_ids),
                        "planDigest": plan_digest,
                        **event,
                    }
                )

        def should_cancel() -> bool:
            if not activity.in_activity():
                return False
            return activity.is_cancelled()

        job_run = run_emr_submit(
            parsed,
            progress_callback=on_progress,
            should_cancel=should_cancel,
            resume_job_run_id=_resume_job_run_id(
                phase="emr-source-silver-bundle",
                identity_key="planDigest",
                identity_value=plan_digest,
            ),
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
        batches = summary.get("batches")
        if not isinstance(batches, list) or len(batches) != len(batch_ids):
            raise NonRetryableSilverError(
                "Source Silver bundle summary batches do not match the plan"
            )
        results: list[SilverResult] = []
        for index, (expected_batch_id, batch_summary) in enumerate(
            zip(batch_ids, batches, strict=True)
        ):
            if not isinstance(batch_summary, dict):
                raise NonRetryableSilverError(
                    f"Source Silver bundle batches[{index}] must be an object"
                )
            actual_batch_id = require_sha256(
                str(batch_summary.get("batchId")),
                label=f"Source Silver bundle batches[{index}].batchId",
            )
            if actual_batch_id != expected_batch_id:
                raise NonRetryableSilverError(
                    f"Source Silver bundle batches[{index}] batchId mismatch"
                )
            _verify_summary(batch_summary, batch_id=actual_batch_id)
            results.append(
                SilverResult(
                    batch_id=actual_batch_id,
                    run_id=str(batch_summary["runId"]),
                    commit_key=str(batch_summary["commitKey"]),
                    job_run_id=job_run_id,
                    table_counts={
                        str(k): int(v) for k, v in batch_summary["tableCounts"].items()
                    },
                    table_snapshot_ids={
                        str(k): None if v is None else int(v)
                        for k, v in batch_summary["tableSnapshotIds"].items()
                    },
                )
            )
        return results
    except Exception as exc:
        raise classify_silver_exception(exc) from exc
