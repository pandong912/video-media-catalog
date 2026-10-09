"""Activities for one pinned multi-source research Golden Build."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlparse

from temporalio import activity

from video_media_catalog.canonical import canonical_json_bytes
from video_media_catalog.community_snapshot import (
    SILVER_EPOCH_MEDIA_TYPE,
    SILVER_SNAPSHOT_MEDIA_TYPE,
)
from video_media_catalog.community_sources import build_community_registry
from video_media_catalog.emr_driver_logs import read_driver_json_summary
from video_media_catalog.emr_serverless_cli import run as run_emr_submit
from video_media_catalog.gold_freshness import research_release_freshness_policy
from video_media_catalog.gold_ingest import (
    ATTRIBUTION_MEDIA_TYPE,
    GOLD_QUALITY_MEDIA_TYPE,
    GOLD_RELEASE_COMMIT_MEDIA_TYPE,
    GoldReleaseCommit,
)
from video_media_catalog.identity_spark import IdentityResolutionConfig
from video_media_catalog.storage import join_uri
from video_media_catalog.temporal.errors import (
    NonRetryableGoldenBuildError,
    classify_golden_build_exception,
)
from video_media_catalog.temporal.golden_models import (
    GoldenBuildInput,
    GoldenBuildResult,
    GoldenEpochResult,
    GoldenIdentityResult,
    GoldenPreflightResult,
    GoldenReleaseResult,
    GoldenSnapshotResult,
)
from video_media_catalog.temporal.models import ObjectRefPayload

_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
_BUILD_ID = re.compile(r"^[a-z0-9][a-z0-9-]{2,63}$")
_CONTROL_MAX_BYTES = 16 * 1024 * 1024
_IDENTITY_REUSE_MEDIA_TYPE = (
    "application/vnd.video-media-catalog.golden-identity-reuse.v1+json"
)
_EXPECTED_SOURCE_PRODUCTS = {
    "imdb-non-commercial-datasets",
    "tmdb-research",
    "tvmaze-public-api",
    "wikidata-json-dump",
}


def _digest(body: bytes) -> str:
    return "sha256:" + hashlib.sha256(body).hexdigest()


def _require_sha256(value: str, *, label: str) -> str:
    normalized = value.lower()
    if not _SHA256.fullmatch(normalized):
        raise NonRetryableGoldenBuildError(f"{label} must be a sha256 digest")
    return normalized


def _split_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.lstrip("/"):
        raise NonRetryableGoldenBuildError(f"expected an S3 object URI, got {uri}")
    return parsed.netloc, parsed.path.lstrip("/")


def _s3_client(region: str) -> Any:
    import boto3

    return boto3.client("s3", region_name=region)


def _emr_client(region: str) -> Any:
    import boto3

    return boto3.client("emr-serverless", region_name=region)


def _read_ref_bytes(
    client: Any,
    reference: ObjectRefPayload,
    *,
    label: str,
    max_bytes: int = _CONTROL_MAX_BYTES,
) -> bytes:
    bucket, key = _split_s3_uri(reference.uri)
    request: dict[str, Any] = {"Bucket": bucket, "Key": key}
    if reference.object_version:
        request["VersionId"] = reference.object_version
    response = client.get_object(**request)
    body = response["Body"].read(max_bytes + 1)
    if len(body) > max_bytes:
        raise NonRetryableGoldenBuildError(f"{label} exceeds {max_bytes} bytes")
    if len(body) != reference.size_bytes:
        raise NonRetryableGoldenBuildError(f"{label} size does not match ObjectRef")
    if _digest(body) != reference.checksum_digest().lower():
        raise NonRetryableGoldenBuildError(f"{label} checksum does not match ObjectRef")
    actual_etag = str(response.get("ETag", "")).strip('"')
    expected_etag = str(reference.etag or "").strip('"')
    if expected_etag and actual_etag != expected_etag:
        raise NonRetryableGoldenBuildError(f"{label} ETag does not match ObjectRef")
    actual_version = response.get("VersionId")
    if reference.object_version and actual_version != reference.object_version:
        raise NonRetryableGoldenBuildError(f"{label} version does not match ObjectRef")
    return body


def _read_ref_json(
    client: Any,
    reference: ObjectRefPayload,
    *,
    label: str,
) -> dict[str, Any]:
    try:
        payload = json.loads(_read_ref_bytes(client, reference, label=label))
    except json.JSONDecodeError as exc:
        raise NonRetryableGoldenBuildError(f"{label} is not valid JSON") from exc
    if not isinstance(payload, dict):
        raise NonRetryableGoldenBuildError(f"{label} must contain a JSON object")
    return payload


def _object_ref_from_put(
    *,
    uri: str,
    body: bytes,
    response: dict[str, Any],
    media_type: str,
) -> ObjectRefPayload:
    return ObjectRefPayload(
        uri=uri,
        format="OBJECT_FORMAT_JSON",
        media_type=media_type,
        checksum_algorithm="CHECKSUM_ALGORITHM_SHA256",
        checksum_value=hashlib.sha256(body).hexdigest(),
        size_bytes=len(body),
        etag=str(response["ETag"]).strip('"'),
        object_version=str(response["VersionId"]),
    )


def _object_ref_payload(reference: ObjectRefPayload) -> dict[str, Any]:
    return {
        "uri": reference.uri,
        "format": reference.format,
        "mediaType": reference.media_type,
        "checksum": {
            "algorithm": reference.checksum_algorithm,
            "value": reference.checksum_value,
        },
        "sizeBytes": reference.size_bytes,
        **({"etag": reference.etag} if reference.etag else {}),
        **(
            {"objectVersion": reference.object_version}
            if reference.object_version
            else {}
        ),
    }


def _put_immutable_json(
    client: Any,
    *,
    uri: str,
    payload: dict[str, Any],
    media_type: str,
) -> ObjectRefPayload:
    from botocore.exceptions import ClientError

    body = canonical_json_bytes(payload)
    bucket, key = _split_s3_uri(uri)
    try:
        current = client.get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") not in {
            "NoSuchKey",
            "404",
        }:
            raise
    else:
        existing = current["Body"].read(len(body) + 1)
        if existing != body:
            raise NonRetryableGoldenBuildError(
                f"immutable control object already exists with different bytes: {uri}"
            )
        return _object_ref_from_put(
            uri=uri,
            body=body,
            response=current,
            media_type=media_type,
        )
    response = client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType=media_type,
    )
    return _object_ref_from_put(
        uri=uri,
        body=body,
        response=response,
        media_type=media_type,
    )


def _parse_time(value: str, *, label: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise NonRetryableGoldenBuildError(f"{label} must be RFC3339") from exc
    if parsed.tzinfo is None:
        raise NonRetryableGoldenBuildError(f"{label} must contain a timezone")
    return parsed.astimezone(UTC)


def _identity_config_digests(build_id: str) -> tuple[str, str, str]:
    resolution = IdentityResolutionConfig()
    runtime = _digest(
        canonical_json_bytes(
            {
                "schemaVersion": "1.0",
                "buildId": build_id,
                "stage": "golden-incremental-identity",
                "identityMode": "incremental",
            }
        )
    )
    return (
        runtime,
        resolution.digest,
        resolution.bind_runtime_config(runtime),
    )


def _validate_preflight_documents(
    input_data: GoldenBuildInput,
    *,
    baseline: dict[str, Any],
    tmdb_summary: dict[str, Any],
    planned_at: str,
) -> GoldenPreflightResult:
    if not _BUILD_ID.fullmatch(input_data.build_id):
        raise NonRetryableGoldenBuildError("build_id must be a lower-case slug")
    baseline_sources = tuple(
        sorted(
            {
                _require_sha256(value, label="baseline source run ID")
                for value in input_data.baseline_source_run_ids
            }
        )
    )
    tmdb_runs = tuple(
        sorted(
            {
                _require_sha256(value, label="TMDB source run ID")
                for value in input_data.tmdb_source_run_ids
            }
        )
    )
    if len(baseline_sources) != len(input_data.baseline_source_run_ids):
        raise NonRetryableGoldenBuildError("baseline source run IDs must be unique")
    if len(tmdb_runs) != 15:
        raise NonRetryableGoldenBuildError(
            "Golden Build requires exactly 15 unique TMDB source runs"
        )
    identity_run = _require_sha256(
        input_data.baseline_identity_run_id,
        label="baseline Identity run ID",
    )
    expected_baseline_runs = set(baseline_sources) | {identity_run}
    if baseline.get("schemaVersion") != "2.0":
        raise NonRetryableGoldenBuildError("baseline snapshot must use schema v2")
    if baseline.get("identityGenerationId") != input_data.identity_generation_id:
        raise NonRetryableGoldenBuildError(
            "baseline snapshot belongs to another Identity generation"
        )
    if set(baseline.get("committedRunIds") or []) != expected_baseline_runs:
        raise NonRetryableGoldenBuildError(
            "baseline snapshot committed runs differ from pinned inputs"
        )
    table_mapping = baseline.get("tableMapping")
    if not isinstance(table_mapping, dict) or not table_mapping:
        raise NonRetryableGoldenBuildError("baseline snapshot lacks table mapping")
    summary_runs = tuple(
        sorted(
            str(item.get("runId")) for item in tmdb_summary.get("sourceSilver") or []
        )
    )
    if (
        tmdb_summary.get("mode") != "bootstrap"
        or tmdb_summary.get("captureCount") != 15
        or len(tmdb_summary.get("captures") or []) != 15
        or summary_runs != tmdb_runs
    ):
        raise NonRetryableGoldenBuildError(
            "TMDB pipeline summary does not match the pinned 15 Silver runs"
        )
    if set(input_data.source_watermarks) != _EXPECTED_SOURCE_PRODUCTS:
        raise NonRetryableGoldenBuildError(
            "source watermarks must contain TMDB, IMDb, TVmaze, and Wikidata"
        )
    override = input_data.freshness_override
    reason = override.reason.strip()
    if not reason or len(reason) > 512:
        raise NonRetryableGoldenBuildError(
            "freshness override reason must contain 1-512 characters"
        )
    if not 1 <= override.max_slo_hours <= 24 * 45:
        raise NonRetryableGoldenBuildError(
            "freshness override maximum must be between 1 and 1080 hours"
        )
    planned = _parse_time(planned_at, label="planned_at")
    imdb_age = (
        planned - _parse_time(override.imdb_acquired_at, label="IMDb acquired_at")
    ).total_seconds() / 3600
    tvmaze_age = (
        planned - _parse_time(override.tvmaze_acquired_at, label="TVmaze acquired_at")
    ).total_seconds() / 3600
    if imdb_age < 0 or tvmaze_age < 0:
        raise NonRetryableGoldenBuildError(
            "freshness override cannot reference a future source"
        )
    # Freshness fails on age >= SLO, so an exact whole-hour age still needs
    # the next integer hour. This is the smallest passing integer override.
    imdb_slo = max(1, math.floor(imdb_age) + 1)
    tvmaze_slo = max(1, math.floor(tvmaze_age) + 1)
    if max(imdb_slo, tvmaze_slo) > override.max_slo_hours:
        raise NonRetryableGoldenBuildError(
            "source age exceeds the bounded freshness override"
        )
    all_runs = tuple(sorted(expected_baseline_runs | set(tmdb_runs)))
    runtime_digest, resolution_digest, bound_digest = _identity_config_digests(
        input_data.build_id
    )
    freshness_policy_digest = research_release_freshness_policy(
        tmdb_slo_hours=36,
        tvmaze_slo_hours=tvmaze_slo,
        imdb_slo_hours=imdb_slo,
        wikidata_slo_hours=45 * 24,
    ).digest
    return GoldenPreflightResult(
        build_spec=ObjectRefPayload(
            uri="s3://pending/build-spec.json",
            format="OBJECT_FORMAT_JSON",
            media_type="application/json",
            checksum_algorithm="CHECKSUM_ALGORITHM_SHA256",
            checksum_value="0" * 64,
            size_bytes=1,
        ),
        planned_at=planned_at,
        all_snapshot_run_ids=all_runs,
        tmdb_source_run_ids=tmdb_runs,
        imdb_age_hours=imdb_age,
        tvmaze_age_hours=tvmaze_age,
        imdb_slo_hours=imdb_slo,
        tvmaze_slo_hours=tvmaze_slo,
        freshness_policy_digest=freshness_policy_digest,
        identity_config_digest=runtime_digest,
        identity_resolution_config_digest=resolution_digest,
        identity_bound_config_digest=bound_digest,
    )


def _argument_values(arguments: list[str], flag: str) -> tuple[str, ...]:
    values: list[str] = []
    for index, value in enumerate(arguments):
        if value != flag:
            continue
        if index + 1 >= len(arguments) or arguments[index + 1].startswith("--"):
            raise NonRetryableGoldenBuildError(
                f"reused Identity job has an invalid {flag} argument"
            )
        values.append(arguments[index + 1])
    return tuple(values)


def _single_argument(arguments: list[str], flag: str) -> str:
    values = _argument_values(arguments, flag)
    if len(values) != 1:
        raise NonRetryableGoldenBuildError(
            f"reused Identity job must contain exactly one {flag} argument"
        )
    return values[0]


def _validated_reused_identity(
    input_data: GoldenBuildInput,
    preflight: GoldenPreflightResult,
    payload: dict[str, Any],
) -> GoldenIdentityResult:
    if payload.get("schemaVersion") != "1.0":
        raise NonRetryableGoldenBuildError("Identity reuse receipt must use schema v1")
    source_build_id = str(payload.get("sourceBuildId") or "")
    if (
        not _BUILD_ID.fullmatch(source_build_id)
        or source_build_id == input_data.build_id
    ):
        raise NonRetryableGoldenBuildError(
            "Identity reuse receipt must name a prior Golden Build"
        )
    application_id = str(payload.get("applicationId") or "")
    job_run_id = str(payload.get("jobRunId") or "")
    image_digest = _require_sha256(
        str(payload.get("imageDigest") or ""),
        label="reused Identity image digest",
    )
    summary = payload.get("identitySummary")
    if not application_id or not job_run_id or not isinstance(summary, dict):
        raise NonRetryableGoldenBuildError(
            "Identity reuse receipt lacks job or summary evidence"
        )

    source_runtime, source_resolution, source_bound = _identity_config_digests(
        source_build_id
    )
    if (
        summary.get("context") != "research"
        or summary.get("stage") != "identity-resolution"
        or summary.get("identityGenerationId") != input_data.identity_generation_id
        or summary.get("identityMode") != "incremental"
        or tuple(sorted(summary.get("sourceRunIds") or ()))
        != preflight.tmdb_source_run_ids
        or summary.get("configDigest") != source_bound
        or summary.get("identityResolutionConfigDigest") != source_resolution
        or source_resolution != preflight.identity_resolution_config_digest
        or summary.get("registryDigest") != build_community_registry().digest
    ):
        raise NonRetryableGoldenBuildError(
            "reused Identity summary differs from pinned incremental inputs"
        )
    _validate_table_commit(summary, label="reused Identity")

    job = _emr_client(input_data.env.aws_region).get_job_run(
        applicationId=application_id,
        jobRunId=job_run_id,
    )["jobRun"]
    spark_submit = (job.get("jobDriver") or {}).get("sparkSubmit") or {}
    arguments = [str(value) for value in spark_submit.get("entryPointArguments") or ()]
    if (
        job.get("applicationId") != application_id
        or job.get("jobRunId") != job_run_id
        or job.get("state") != "SUCCESS"
        or job.get("name") != f"gold-identity-{source_build_id}"[:64]
        or job.get("executionRole") != input_data.env.emr_execution_role_arn
        or spark_submit.get("entryPoint") != input_data.env.research_silver_entry_point
        or not arguments
        or arguments[0] != "resolve-identity"
        or tuple(sorted(_argument_values(arguments, "--source-run-id")))
        != preflight.tmdb_source_run_ids
        or _single_argument(arguments, "--identity-generation-id")
        != input_data.identity_generation_id
        or _single_argument(arguments, "--identity-mode") != "incremental"
        or _single_argument(arguments, "--image-digest") != image_digest
        or _single_argument(arguments, "--config-digest") != source_runtime
    ):
        raise NonRetryableGoldenBuildError(
            "reused Identity EMR job differs from receipt provenance"
        )

    return GoldenIdentityResult(
        run_id=_require_sha256(
            str(summary.get("runId") or ""),
            label="reused Identity runId",
        ),
        commit_key=_require_sha256(
            str(summary.get("commitKey") or ""),
            label="reused Identity commitKey",
        ),
        source_run_ids=tuple(sorted(summary["sourceRunIds"])),
        table_counts={
            str(key): int(value) for key, value in summary["tableCounts"].items()
        },
        table_snapshot_ids={
            str(key): None if value is None else int(value)
            for key, value in summary["tableSnapshotIds"].items()
        },
        job_run_id=job_run_id,
    )


@activity.defn(name="PrepareGoldenBuild")
def prepare_golden_build(
    input_data: GoldenBuildInput,
    planned_at: str,
) -> GoldenPreflightResult:
    try:
        client = _s3_client(input_data.env.aws_region)
        baseline = _read_ref_json(
            client,
            input_data.baseline_snapshot,
            label="baseline Silver snapshot",
        )
        tmdb_summary = _read_ref_json(
            client,
            input_data.tmdb_pipeline_summary,
            label="TMDB pipeline summary",
        )
        preflight = _validate_preflight_documents(
            input_data,
            baseline=baseline,
            tmdb_summary=tmdb_summary,
            planned_at=planned_at,
        )
        if input_data.identity_reuse is not None:
            reference = input_data.identity_reuse
            if (
                reference.format != "OBJECT_FORMAT_JSON"
                or reference.media_type != _IDENTITY_REUSE_MEDIA_TYPE
                or reference.size_bytes <= 0
                or not reference.etag
                or not reference.object_version
            ):
                raise NonRetryableGoldenBuildError(
                    "Identity reuse receipt ObjectRef is incomplete"
                )
            reuse_payload = _read_ref_json(
                client,
                reference,
                label="Identity reuse receipt",
            )
            preflight = replace(
                preflight,
                reused_identity=_validated_reused_identity(
                    input_data,
                    preflight,
                    reuse_payload,
                ),
            )
        payload = {
            "schemaVersion": "1.0",
            "buildId": input_data.build_id,
            "plannedAt": planned_at,
            "identityGenerationId": input_data.identity_generation_id,
            "baselineSnapshot": _object_ref_payload(input_data.baseline_snapshot),
            "tmdbPipelineSummary": _object_ref_payload(
                input_data.tmdb_pipeline_summary
            ),
            "baselineSourceRunIds": list(input_data.baseline_source_run_ids),
            "baselineIdentityRunId": input_data.baseline_identity_run_id,
            "tmdbSourceRunIds": list(preflight.tmdb_source_run_ids),
            "allSnapshotRunIds": list(preflight.all_snapshot_run_ids),
            "sourceWatermarks": dict(sorted(input_data.source_watermarks.items())),
            "identityConfigDigest": preflight.identity_config_digest,
            "identityResolutionConfigDigest": (
                preflight.identity_resolution_config_digest
            ),
            "identityBoundConfigDigest": preflight.identity_bound_config_digest,
            **(
                {"identityReuse": _object_ref_payload(input_data.identity_reuse)}
                if input_data.identity_reuse is not None
                else {}
            ),
            "freshnessPolicyDigest": preflight.freshness_policy_digest,
            "freshnessOverride": {
                "reason": input_data.freshness_override.reason,
                "boundedMaxSloHours": (input_data.freshness_override.max_slo_hours),
                "imdbAcquiredAt": (input_data.freshness_override.imdb_acquired_at),
                "imdbActualAgeHours": preflight.imdb_age_hours,
                "imdbSloHours": preflight.imdb_slo_hours,
                "tvmazeAcquiredAt": (input_data.freshness_override.tvmaze_acquired_at),
                "tvmazeActualAgeHours": preflight.tvmaze_age_hours,
                "tvmazeSloHours": preflight.tvmaze_slo_hours,
            },
            "publication": {
                "opensearch": "disabled",
                "canonical": "disabled",
                "cutover": "disabled",
            },
        }
        uri = join_uri(
            input_data.env.control_prefix,
            "golden-builds",
            input_data.build_id,
            "build-spec.json",
        )
        build_spec = _put_immutable_json(
            client,
            uri=uri,
            payload=payload,
            media_type=(
                "application/vnd.video-media-catalog.golden-build-spec.v1+json"
            ),
        )
        activity.heartbeat(
            {"phase": "preflight-complete", "buildId": input_data.build_id}
        )
        return replace(preflight, build_spec=build_spec)
    except Exception as exc:
        raise classify_golden_build_exception(exc) from exc


def _object_args(prefix: str, reference: ObjectRefPayload) -> list[str]:
    args = [
        f"--{prefix}-uri",
        reference.uri,
        f"--{prefix}-hash",
        reference.checksum_digest(),
        f"--{prefix}-size",
        str(reference.size_bytes),
    ]
    if reference.object_version:
        args.extend([f"--{prefix}-version", reference.object_version])
    if reference.etag:
        args.extend([f"--{prefix}-etag", reference.etag])
    return args


def _catalog_args(input_data: GoldenBuildInput) -> list[str]:
    env = input_data.env
    return [
        "--catalog-type",
        env.catalog_type,
        "--catalog-name",
        env.catalog_name,
        "--namespace",
        env.glue_namespace,
        "--warehouse",
        env.warehouse_uri,
        "--aws-region",
        env.aws_region,
        "--s3-credentials-provider",
        "default",
    ]


def _stage_token(
    *,
    build_id: str,
    stage: str,
    workflow_run_id: str,
) -> str:
    digest = hashlib.sha256(
        f"golden:{build_id}:{stage}:{workflow_run_id}".encode()
    ).hexdigest()
    return f"gold-{stage[:8]}-{digest[:44]}"


def _emr_namespace(
    *,
    input_data: GoldenBuildInput,
    stage: str,
    entry_point: str,
    entry_args: list[str],
    workflow_run_id: str,
    control_profile: bool,
) -> argparse.Namespace:
    env = input_data.env
    return argparse.Namespace(
        application_name=env.emr_application_name,
        execution_role_arn=env.emr_execution_role_arn,
        job_name=f"gold-{stage}-{input_data.build_id}"[:64],
        client_token=_stage_token(
            build_id=input_data.build_id,
            stage=stage,
            workflow_run_id=workflow_run_id,
        ),
        entry_point=entry_point,
        log_uri=env.emr_log_uri,
        aws_region=env.aws_region,
        poll_seconds=15,
        execution_timeout_minutes=720,
        max_attempts=1,
        driver_cores=4,
        driver_memory="16g",
        driver_memory_overhead=None,
        driver_disk="50G",
        executor_cores=8,
        executor_memory="56g",
        executor_memory_overhead=None,
        executor_disk="400G",
        executor_instances=(
            env.control_executor_instances
            if control_profile
            else env.build_executor_instances
        ),
        shuffle_partitions=(
            env.control_shuffle_partitions
            if control_profile
            else env.build_shuffle_partitions
        ),
        entry_point_arguments=entry_args,
    )


def _run_emr_stage(
    *,
    input_data: GoldenBuildInput,
    stage: str,
    entry_point: str,
    entry_args: list[str],
    required_keys: tuple[str, ...],
    workflow_run_id: str,
    control_profile: bool,
) -> tuple[dict[str, Any], str]:
    parsed = _emr_namespace(
        input_data=input_data,
        stage=stage,
        entry_point=entry_point,
        entry_args=entry_args,
        workflow_run_id=workflow_run_id,
        control_profile=control_profile,
    )

    def on_progress(event: dict[str, Any]) -> None:
        activity.heartbeat(
            {
                "phase": f"golden-{stage}",
                "buildId": input_data.build_id,
                **event,
            }
        )

    heartbeat_details = getattr(activity.info(), "heartbeat_details", ())
    if isinstance(heartbeat_details, dict):
        heartbeat_details = (heartbeat_details,)
    resume_job_run_id: str | None = None
    for details in reversed(tuple(heartbeat_details)):
        if (
            isinstance(details, dict)
            and details.get("phase") == f"golden-{stage}"
            and details.get("buildId") == input_data.build_id
            and details.get("jobRunId")
        ):
            resume_job_run_id = str(details["jobRunId"])
            break

    job_run = run_emr_submit(
        parsed,
        progress_callback=on_progress,
        should_cancel=activity.is_cancelled,
        resume_job_run_id=resume_job_run_id,
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
    summary = read_driver_json_summary(
        s3_client=_s3_client(input_data.env.aws_region),
        log_uri=input_data.env.emr_log_uri,
        application_id=application_id,
        job_run_id=job_run_id,
        required_keys=required_keys,
        label=f"Golden Build {stage}",
    )
    return summary, job_run_id


def _validated_object_ref(
    payload: dict[str, Any],
    *,
    label: str,
    media_type: str,
) -> ObjectRefPayload:
    try:
        reference = ObjectRefPayload.from_mapping(payload)
    except (KeyError, TypeError, ValueError) as exc:
        raise NonRetryableGoldenBuildError(f"{label} ObjectRef is invalid") from exc
    if (
        reference.format != "OBJECT_FORMAT_JSON"
        or reference.media_type != media_type
        or reference.size_bytes <= 0
        or not reference.etag
        or not reference.object_version
    ):
        raise NonRetryableGoldenBuildError(f"{label} ObjectRef is incomplete")
    _require_sha256(reference.checksum_digest(), label=f"{label} checksum")
    return reference


def _validate_table_commit(summary: dict[str, Any], *, label: str) -> None:
    counts = summary.get("tableCounts")
    snapshots = summary.get("tableSnapshotIds")
    if not isinstance(counts, dict) or not counts:
        raise NonRetryableGoldenBuildError(f"{label} tableCounts is empty")
    if not isinstance(snapshots, dict) or not snapshots:
        raise NonRetryableGoldenBuildError(f"{label} tableSnapshotIds is empty")
    for table, count in counts.items():
        parsed_count = int(count)
        snapshot = snapshots.get(table)
        if parsed_count < 0 or (parsed_count > 0 and snapshot is None):
            raise NonRetryableGoldenBuildError(
                f"{label} table commit is invalid for {table}"
            )
        if snapshot is not None and int(snapshot) <= 0:
            raise NonRetryableGoldenBuildError(
                f"{label} snapshot is invalid for {table}"
            )


@activity.defn(name="PublishGoldenSnapshot")
def publish_golden_snapshot(
    input_data: GoldenBuildInput,
    preflight: GoldenPreflightResult,
) -> GoldenSnapshotResult:
    try:
        destination = join_uri(
            input_data.env.control_prefix,
            "golden-builds",
            input_data.build_id,
            "pre-identity-snapshot.json",
        )
        args = ["publish-snapshot"]
        for run_id in preflight.all_snapshot_run_ids:
            args.extend(["--run-id", run_id])
        args.extend(
            [
                "--snapshot-uri",
                destination,
                "--created-at",
                preflight.planned_at,
                "--identity-generation-id",
                input_data.identity_generation_id,
                *_catalog_args(input_data),
            ]
        )
        summary, job_run_id = _run_emr_stage(
            input_data=input_data,
            stage="snapshot",
            entry_point=input_data.env.research_silver_entry_point,
            entry_args=args,
            required_keys=("snapshotSetId", "committedRunIds", "silverSnapshot"),
            workflow_run_id=activity.info().workflow_run_id,
            control_profile=True,
        )
        if tuple(sorted(summary["committedRunIds"])) != tuple(
            sorted(preflight.all_snapshot_run_ids)
        ):
            raise NonRetryableGoldenBuildError(
                "published snapshot run IDs differ from build spec"
            )
        reference = _validated_object_ref(
            summary["silverSnapshot"],
            label="pre-identity snapshot",
            media_type=SILVER_SNAPSHOT_MEDIA_TYPE,
        )
        return GoldenSnapshotResult(
            snapshot_set_id=_require_sha256(
                str(summary["snapshotSetId"]),
                label="snapshotSetId",
            ),
            snapshot=reference,
            job_run_id=job_run_id,
        )
    except Exception as exc:
        raise classify_golden_build_exception(exc) from exc


@activity.defn(name="ResolveGoldenIdentity")
def resolve_golden_identity(
    input_data: GoldenBuildInput,
    preflight: GoldenPreflightResult,
    snapshot: GoldenSnapshotResult,
    started_at: str,
    committed_at: str,
) -> GoldenIdentityResult:
    try:
        args = [
            "resolve-identity",
            *_object_args("silver-snapshot", snapshot.snapshot),
            "--silver-snapshot-media-type",
            SILVER_SNAPSHOT_MEDIA_TYPE,
        ]
        for run_id in preflight.tmdb_source_run_ids:
            args.extend(["--source-run-id", run_id])
        args.extend(
            [
                "--identity-generation-id",
                input_data.identity_generation_id,
                "--identity-mode",
                "incremental",
                "--image-digest",
                input_data.env.image_digest,
                "--config-digest",
                preflight.identity_config_digest,
                "--started-at",
                started_at,
                "--committed-at",
                committed_at,
                *_catalog_args(input_data),
            ]
        )
        summary, job_run_id = _run_emr_stage(
            input_data=input_data,
            stage="identity",
            entry_point=input_data.env.research_silver_entry_point,
            entry_args=args,
            required_keys=(
                "runId",
                "commitKey",
                "configDigest",
                "identityResolutionConfigDigest",
                "tableCounts",
                "tableSnapshotIds",
            ),
            workflow_run_id=activity.info().workflow_run_id,
            control_profile=False,
        )
        if (
            summary.get("identityGenerationId") != input_data.identity_generation_id
            or summary.get("identityMode") != "incremental"
            or tuple(sorted(summary.get("sourceRunIds") or ()))
            != preflight.tmdb_source_run_ids
            or summary.get("configDigest") != preflight.identity_bound_config_digest
            or summary.get("identityResolutionConfigDigest")
            != preflight.identity_resolution_config_digest
        ):
            raise NonRetryableGoldenBuildError(
                "Identity summary differs from pinned incremental inputs"
            )
        _validate_table_commit(summary, label="Identity")
        return GoldenIdentityResult(
            run_id=_require_sha256(str(summary["runId"]), label="Identity runId"),
            commit_key=_require_sha256(
                str(summary["commitKey"]),
                label="Identity commitKey",
            ),
            source_run_ids=tuple(sorted(summary["sourceRunIds"])),
            table_counts={
                str(key): int(value) for key, value in summary["tableCounts"].items()
            },
            table_snapshot_ids={
                str(key): None if value is None else int(value)
                for key, value in summary["tableSnapshotIds"].items()
            },
            job_run_id=job_run_id,
        )
    except Exception as exc:
        raise classify_golden_build_exception(exc) from exc


@activity.defn(name="PublishGoldenEpoch")
def publish_golden_epoch(
    input_data: GoldenBuildInput,
    preflight: GoldenPreflightResult,
    identity: GoldenIdentityResult,
    created_at: str,
) -> GoldenEpochResult:
    try:
        destination = join_uri(
            input_data.env.control_prefix,
            "golden-builds",
            input_data.build_id,
            "silver-epoch.json",
        )
        args = [
            "publish-epoch",
            "--epoch-uri",
            destination,
            "--created-at",
            created_at,
            "--identity-generation-id",
            input_data.identity_generation_id,
        ]
        expected_runs = tuple(
            sorted((*preflight.all_snapshot_run_ids, identity.run_id))
        )
        for run_id in expected_runs:
            args.extend(["--delta-run-id", run_id])
        for source, watermark in sorted(input_data.source_watermarks.items()):
            args.extend(["--source-watermark", f"{source}={watermark}"])
        args.extend(_catalog_args(input_data))
        summary, job_run_id = _run_emr_stage(
            input_data=input_data,
            stage="epoch",
            entry_point=input_data.env.research_silver_entry_point,
            entry_args=args,
            required_keys=(
                "epochId",
                "silverEpoch",
                "deltaRunIds",
                "committedRunCount",
                "committedRunDigest",
                "dataSnapshotIds",
            ),
            workflow_run_id=activity.info().workflow_run_id,
            control_profile=True,
        )
        if summary.get("identityGenerationId") != input_data.identity_generation_id:
            raise NonRetryableGoldenBuildError(
                "epoch belongs to another Identity generation"
            )
        reference = _validated_object_ref(
            summary["silverEpoch"],
            label="Silver epoch",
            media_type=SILVER_EPOCH_MEDIA_TYPE,
        )
        committed_count = int(summary.get("committedRunCount") or 0)
        if tuple(
            sorted(summary.get("deltaRunIds") or ())
        ) != expected_runs or committed_count != len(expected_runs):
            raise NonRetryableGoldenBuildError(
                "epoch does not exactly contain the pinned runs and Identity commit"
            )
        snapshots = summary.get("dataSnapshotIds")
        if not isinstance(snapshots, dict) or not snapshots:
            raise NonRetryableGoldenBuildError("epoch dataSnapshotIds is empty")
        return GoldenEpochResult(
            epoch_id=_require_sha256(str(summary["epochId"]), label="epochId"),
            epoch=reference,
            committed_run_count=committed_count,
            committed_run_digest=_require_sha256(
                str(summary["committedRunDigest"]),
                label="committedRunDigest",
            ),
            data_snapshot_ids={
                str(key): None if value is None else int(value)
                for key, value in snapshots.items()
            },
            job_run_id=job_run_id,
        )
    except Exception as exc:
        raise classify_golden_build_exception(exc) from exc


def _validate_release_documents(
    *,
    client: Any,
    input_data: GoldenBuildInput,
    preflight: GoldenPreflightResult,
    quality_ref: ObjectRefPayload,
    attribution_ref: ObjectRefPayload,
    commit_ref: ObjectRefPayload,
    release_plan_id: str,
    commit_key: str,
    table_counts: dict[str, int],
    table_snapshot_ids: dict[str, int | None],
) -> None:
    quality = _read_ref_json(client, quality_ref, label="Gold quality report")
    if (
        quality.get("status") != "PASS"
        or quality.get("buildMode") != "RELEASE"
        or quality.get("releasePlanId") != release_plan_id
        or quality.get("violations") not in ([], ())
        or int(quality.get("duplicateExternalIdCount", -1)) != 0
    ):
        raise NonRetryableGoldenBuildError("Gold quality report did not pass")
    freshness = quality.get("releaseFreshness") or {}
    if (
        str(freshness.get("policyDigest", "")).lower()
        != preflight.freshness_policy_digest
    ):
        raise NonRetryableGoldenBuildError(
            "freshness policy digest differs from the build spec"
        )
    sources = {
        item.get("sourceProductId"): item
        for item in freshness.get("sources") or []
        if isinstance(item, dict)
    }
    if not _EXPECTED_SOURCE_PRODUCTS.issubset(sources):
        raise NonRetryableGoldenBuildError(
            "Gold quality report lacks one or more required sources"
        )
    override = freshness.get("overrideEvidence") or {}
    if override.get("reason") != input_data.freshness_override.reason.strip() or tuple(
        sorted(override.get("sourceProductIds") or ())
    ) != (
        "imdb-non-commercial-datasets",
        "tvmaze-public-api",
    ):
        raise NonRetryableGoldenBuildError(
            "Gold quality report lacks the pinned freshness override evidence"
        )
    expected_slos = {
        "imdb-non-commercial-datasets": preflight.imdb_slo_hours,
        "tvmaze-public-api": preflight.tvmaze_slo_hours,
    }
    for source, slo in expected_slos.items():
        row = sources[source]
        if (
            int(row.get("sloHours") or 0) != slo
            or row.get("status") != "PASS"
            or float(row.get("effectiveAgeHours") or 0) > slo
        ):
            raise NonRetryableGoldenBuildError(
                f"freshness override was not applied correctly for {source}"
            )
    attribution = _read_ref_json(
        client,
        attribution_ref,
        label="Gold attribution manifest",
    )
    attributed_sources = {
        str(item.get("sourceProductId"))
        for item in attribution.get("entries") or []
        if isinstance(item, dict)
    }
    if not _EXPECTED_SOURCE_PRODUCTS.issubset(attributed_sources):
        raise NonRetryableGoldenBuildError(
            "Gold attribution manifest lacks one or more required sources"
        )
    if attribution.get("releaseId") != release_plan_id:
        raise NonRetryableGoldenBuildError(
            "Gold attribution manifest belongs to another release"
        )
    commit_payload = _read_ref_json(
        client,
        commit_ref,
        label="Gold release commit",
    )
    try:
        commit = GoldReleaseCommit.model_validate(commit_payload)
    except (TypeError, ValueError) as exc:
        raise NonRetryableGoldenBuildError("Gold release commit is invalid") from exc
    if (
        commit.release_plan_id != release_plan_id
        or commit.commit_key != commit_key
        or commit.table_counts != table_counts
        or commit.table_snapshot_ids != table_snapshot_ids
        or commit.quality_report.uri != quality_ref.uri
        or (
            f"sha256:{commit.quality_report.checksum.value}"
            != quality_ref.checksum_digest()
        )
        or commit.attribution_manifest.uri != attribution_ref.uri
        or (
            f"sha256:{commit.attribution_manifest.checksum.value}"
            != attribution_ref.checksum_digest()
        )
    ):
        raise NonRetryableGoldenBuildError(
            "Gold release commit differs from the verified release evidence"
        )


@activity.defn(name="SubmitGoldenRelease")
def submit_golden_release(
    input_data: GoldenBuildInput,
    preflight: GoldenPreflightResult,
    epoch: GoldenEpochResult,
    committed_at: str,
) -> GoldenReleaseResult:
    try:
        env = input_data.env
        args = [
            *_object_args("silver-snapshot", epoch.epoch),
            "--silver-snapshot-media-type",
            SILVER_EPOCH_MEDIA_TYPE,
            "--output-prefix",
            env.gold_output_prefix,
            "--planned-at",
            preflight.planned_at,
            "--committed-at",
            committed_at,
            "--build-mode",
            "release",
            "--return-failed-quality-summary",
            "--image-digest",
            env.image_digest,
            "--tmdb-freshness-slo-hours",
            "36",
            "--tvmaze-freshness-slo-hours",
            str(preflight.tvmaze_slo_hours),
            "--imdb-freshness-slo-hours",
            str(preflight.imdb_slo_hours),
            "--wikidata-freshness-slo-hours",
            str(45 * 24),
            "--freshness-override-reason",
            input_data.freshness_override.reason.strip(),
            "--freshness-override-source-product-id",
            "imdb-non-commercial-datasets",
            "--freshness-override-source-product-id",
            "tvmaze-public-api",
            "--catalog-type",
            env.catalog_type,
            "--catalog-name",
            env.catalog_name,
            "--silver-namespace",
            env.glue_namespace,
            "--gold-namespace",
            env.glue_namespace,
            "--warehouse",
            env.warehouse_uri,
            "--aws-region",
            env.aws_region,
            "--s3-credentials-provider",
            "default",
        ]
        summary, job_run_id = _run_emr_stage(
            input_data=input_data,
            stage="release",
            entry_point=env.gold_entry_point,
            entry_args=args,
            required_keys=(
                "releasePlanId",
                "buildMode",
                "qualityStatus",
                "qualityReport",
                "committed",
            ),
            workflow_run_id=activity.info().workflow_run_id,
            control_profile=False,
        )
        if summary.get("committed") is not True:
            failed_quality_ref = _validated_object_ref(
                summary["qualityReport"],
                label="failed Gold quality report",
                media_type=GOLD_QUALITY_MEDIA_TYPE,
            )
            failed_quality = _read_ref_json(
                _s3_client(env.aws_region),
                failed_quality_ref,
                label="failed Gold quality report",
            )
            violations = tuple(failed_quality.get("violations") or ())
            if (
                summary.get("buildMode") != "RELEASE"
                or summary.get("qualityStatus") != "FAILED"
                or failed_quality.get("status") != "FAILED"
                or failed_quality.get("releasePlanId") != summary.get("releasePlanId")
                or not violations
            ):
                raise NonRetryableGoldenBuildError(
                    "Gold returned malformed failed quality evidence"
                )
            raise NonRetryableGoldenBuildError(
                "Gold quality gate failed for "
                f"{summary['releasePlanId']}: {', '.join(map(str, violations))}"
            )
        if (
            summary.get("buildMode") != "RELEASE"
            or summary.get("qualityStatus") != "PASS"
        ):
            raise NonRetryableGoldenBuildError("Gold release was not committed")
        missing_success_keys = {
            "commitKey",
            "releaseCommit",
            "attributionManifest",
            "tableCounts",
            "tableSnapshotIds",
        } - set(summary)
        if missing_success_keys:
            raise NonRetryableGoldenBuildError(
                "Gold release summary lacks committed evidence: "
                + ", ".join(sorted(missing_success_keys))
            )
        _validate_table_commit(summary, label="Gold release")
        quality_ref = _validated_object_ref(
            summary["qualityReport"],
            label="Gold quality report",
            media_type=GOLD_QUALITY_MEDIA_TYPE,
        )
        attribution_ref = _validated_object_ref(
            summary["attributionManifest"],
            label="Gold attribution manifest",
            media_type=ATTRIBUTION_MEDIA_TYPE,
        )
        commit_ref = _validated_object_ref(
            summary["releaseCommit"],
            label="Gold release commit",
            media_type=GOLD_RELEASE_COMMIT_MEDIA_TYPE,
        )
        release_plan_id = _require_sha256(
            str(summary["releasePlanId"]),
            label="releasePlanId",
        )
        commit_key = _require_sha256(
            str(summary["commitKey"]),
            label="Gold commitKey",
        )
        table_counts = {
            str(key): int(value) for key, value in summary["tableCounts"].items()
        }
        table_snapshot_ids = {
            str(key): None if value is None else int(value)
            for key, value in summary["tableSnapshotIds"].items()
        }
        _validate_release_documents(
            client=_s3_client(env.aws_region),
            input_data=input_data,
            preflight=preflight,
            quality_ref=quality_ref,
            attribution_ref=attribution_ref,
            commit_ref=commit_ref,
            release_plan_id=release_plan_id,
            commit_key=commit_key,
            table_counts=table_counts,
            table_snapshot_ids=table_snapshot_ids,
        )
        return GoldenReleaseResult(
            release_plan_id=release_plan_id,
            commit_key=commit_key,
            quality_report=quality_ref,
            attribution_manifest=attribution_ref,
            release_commit=commit_ref,
            table_counts=table_counts,
            table_snapshot_ids=table_snapshot_ids,
            job_run_id=job_run_id,
        )
    except Exception as exc:
        raise classify_golden_build_exception(exc) from exc


@activity.defn(name="WriteGoldenBuildSummary")
def write_golden_build_summary(
    result: GoldenBuildResult,
    summary_prefix: str,
    aws_region: str,
) -> str:
    try:
        payload = {
            "schemaVersion": "1.0",
            "buildId": result.build_id,
            "plannedAt": result.planned_at,
            "committedAt": result.committed_at,
            "buildSpec": _object_ref_payload(result.build_spec),
            "snapshot": {
                "snapshotSetId": result.snapshot.snapshot_set_id,
                "snapshot": _object_ref_payload(result.snapshot.snapshot),
                "jobRunId": result.snapshot.job_run_id,
            },
            "identity": {
                "runId": result.identity.run_id,
                "commitKey": result.identity.commit_key,
                "sourceRunIds": list(result.identity.source_run_ids),
                "tableCounts": result.identity.table_counts,
                "tableSnapshotIds": result.identity.table_snapshot_ids,
                "jobRunId": result.identity.job_run_id,
            },
            "epoch": {
                "epochId": result.epoch.epoch_id,
                "epoch": _object_ref_payload(result.epoch.epoch),
                "committedRunCount": result.epoch.committed_run_count,
                "committedRunDigest": result.epoch.committed_run_digest,
                "dataSnapshotIds": result.epoch.data_snapshot_ids,
                "jobRunId": result.epoch.job_run_id,
            },
            "release": {
                "releasePlanId": result.release.release_plan_id,
                "commitKey": result.release.commit_key,
                "qualityReport": _object_ref_payload(result.release.quality_report),
                "attributionManifest": _object_ref_payload(
                    result.release.attribution_manifest
                ),
                "releaseCommit": _object_ref_payload(result.release.release_commit),
                "tableCounts": result.release.table_counts,
                "tableSnapshotIds": result.release.table_snapshot_ids,
                "jobRunId": result.release.job_run_id,
            },
            "publication": result.publication_status,
        }
        uri = join_uri(summary_prefix, f"{result.build_id}.json")
        _put_immutable_json(
            _s3_client(aws_region),
            uri=uri,
            payload=payload,
            media_type=(
                "application/vnd.video-media-catalog.golden-build-summary.v1+json"
            ),
        )
        activity.heartbeat({"phase": "summary-written", "uri": uri})
        return uri
    except Exception as exc:
        raise classify_golden_build_exception(exc) from exc
