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
from video_media_catalog.catalog_source_policy import (
    CatalogSourceCapability,
    CatalogSourcePolicy,
    load_catalog_source_policy,
)
from video_media_catalog.community_snapshot import (
    SILVER_EPOCH_MEDIA_TYPE,
    SILVER_SNAPSHOT_MEDIA_TYPE,
)
from video_media_catalog.community_sources import build_community_registry
from video_media_catalog.emr_driver_logs import read_driver_json_summary
from video_media_catalog.emr_serverless_cli import run as run_emr_submit
from video_media_catalog.gold import (
    GOLD_RELEASE_PLAN_MEDIA_TYPE,
    GoldReleasePlan,
)
from video_media_catalog.gold_freshness import (
    research_release_freshness_policy,
    scope_release_freshness_policy,
)
from video_media_catalog.gold_ingest import (
    ATTRIBUTION_MEDIA_TYPE,
    GOLD_QUALITY_MEDIA_TYPE,
    GOLD_RELEASE_COMMIT_MEDIA_TYPE,
    GoldReleaseCommit,
)
from video_media_catalog.identity_spark import IdentityResolutionConfig
from video_media_catalog.predicate_registry import load_research_predicate_registry
from video_media_catalog.reference_ontology import load_global_reference_ontology
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
    GoldenSourceFreshnessStatus,
    GoldenSourceRunState,
    GoldenSourceRunStatus,
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
_GIB_RESOURCE = re.compile(r"^([1-9][0-9]*)[gG]$")
_GOLD_EMR_MAX_VCPU = 128
_GOLD_EMR_MAX_MEMORY_GIB = 768
_GOLD_EMR_MAX_DISK_GIB = 4000
_GOLD_DRIVER_CORES = 4
_GOLD_DRIVER_MEMORY = "16g"
_GOLD_DRIVER_MEMORY_OVERHEAD = "4g"
_GOLD_DRIVER_DISK = "50G"
_GOLD_REPLACEMENT_EXECUTORS = 1
_DEFAULT_GOLD_TMDB_FRESHNESS_SLO_HOURS = 36
_DEFAULT_GOLD_MAX_UNRESOLVED_IDENTITY_RATIO = 0.05
_MAX_GOLD_FRESHNESS_SLO_HOURS = 45 * 24


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


def _source_run_state_payload(state: GoldenSourceRunState) -> dict[str, Any]:
    return {
        "sourceProductId": state.source_product_id,
        "runIds": list(state.run_ids),
        "runStatus": state.run_status.value,
        "freshnessStatus": state.freshness_status.value,
        "watermark": state.watermark,
        "identitySpine": state.identity_spine,
        "materialization": state.materialization,
        "goldContribution": state.gold_contribution,
        **({"acquiredAt": state.acquired_at} if state.acquired_at else {}),
        **(
            {"captureReference": _object_ref_payload(state.capture_reference)}
            if state.capture_reference is not None
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


def _normalized_source_run_states(
    input_data: GoldenBuildInput,
    *,
    source_policy: CatalogSourcePolicy,
) -> tuple[GoldenSourceRunState, ...]:
    """Validate the mandatory unified capture/run/freshness handoff."""

    if not input_data.source_run_states:
        raise NonRetryableGoldenBuildError(
            "Golden Build requires explicit sourceRunStates; legacy source routing "
            "cannot prove identity, Gold, and materialization authorization"
        )

    normalized: list[GoldenSourceRunState] = []
    source_ids: set[str] = set()
    assigned_run_ids: set[str] = set()
    baseline_run_ids = set(input_data.baseline_source_run_ids)
    for raw in input_data.source_run_states:
        source_product_id = raw.source_product_id.strip().lower()
        if source_product_id in source_ids:
            raise NonRetryableGoldenBuildError(
                "source run states must contain unique source products"
            )
        source_ids.add(source_product_id)
        try:
            source_policy.source(source_product_id)
            run_status = GoldenSourceRunStatus(raw.run_status)
            freshness_status = GoldenSourceFreshnessStatus(raw.freshness_status)
        except ValueError as exc:
            raise NonRetryableGoldenBuildError(str(exc)) from exc

        run_ids = tuple(
            sorted(
                {
                    _require_sha256(value, label="source state run ID")
                    for value in raw.run_ids
                }
            )
        )
        if len(run_ids) != len(raw.run_ids):
            raise NonRetryableGoldenBuildError("source state run IDs must be unique")
        overlap = assigned_run_ids & set(run_ids)
        if overlap:
            raise NonRetryableGoldenBuildError(
                "one source run cannot belong to multiple source states"
            )
        assigned_run_ids.update(run_ids)

        if run_status == GoldenSourceRunStatus.COMMITTED:
            if not run_ids:
                raise NonRetryableGoldenBuildError(
                    "committed source states require run IDs"
                )
        elif (
            run_ids
            or raw.identity_spine
            or raw.materialization
            or raw.gold_contribution
        ):
            raise NonRetryableGoldenBuildError(
                "pending, failed, or excluded source states cannot carry "
                "identity, Gold, or materialization runs"
            )

        if (
            raw.gold_contribution
            and not raw.identity_spine
            and not set(run_ids).issubset(baseline_run_ids)
            and not (
                input_data.identity_reuse is not None
                and set(run_ids).issubset(set(input_data.tmdb_source_run_ids))
            )
        ):
            raise NonRetryableGoldenBuildError(
                "Gold-contributing source states without incremental Identity "
                "must be covered by the baseline Identity"
            )
        if raw.materialization and not raw.gold_contribution:
            raise NonRetryableGoldenBuildError(
                "materialization requires an enabled Gold contribution"
            )
        try:
            if raw.identity_spine:
                source_policy.require_source(
                    source_product_id,
                    CatalogSourceCapability.IDENTITY_SPINE,
                )
            if raw.gold_contribution:
                source_policy.require_source(
                    source_product_id,
                    CatalogSourceCapability.GOLD_PROJECTION,
                )
            if raw.materialization:
                source_policy.require_source(
                    source_product_id,
                    CatalogSourceCapability.MATERIALIZATION,
                )
        except ValueError as exc:
            raise NonRetryableGoldenBuildError(str(exc)) from exc
        if raw.gold_contribution:
            allowed_freshness = (
                {
                    GoldenSourceFreshnessStatus.PASS,
                    GoldenSourceFreshnessStatus.OVERRIDDEN,
                }
                if source_policy.allows_source(
                    source_product_id,
                    CatalogSourceCapability.FRESHNESS,
                )
                else {
                    GoldenSourceFreshnessStatus.PASS,
                    GoldenSourceFreshnessStatus.NOT_REQUIRED,
                }
            )
            if freshness_status not in allowed_freshness:
                raise NonRetryableGoldenBuildError(
                    f"{source_product_id} is not fresh enough for Gold contribution"
                )

        watermark = raw.watermark.strip()
        if len(watermark) > 1024:
            raise NonRetryableGoldenBuildError(
                "source state watermark exceeds 1024 characters"
            )
        expected_watermark = input_data.source_watermarks.get(source_product_id)
        if expected_watermark is not None and watermark != expected_watermark:
            raise NonRetryableGoldenBuildError(
                f"source state watermark differs for {source_product_id}"
            )
        acquired_at = raw.acquired_at
        if acquired_at is not None:
            acquired_at = (
                _parse_time(
                    acquired_at,
                    label=f"{source_product_id} acquired_at",
                )
                .replace(microsecond=0)
                .isoformat()
                .replace("+00:00", "Z")
            )
        reference = raw.capture_reference
        if reference is not None:
            parsed = urlparse(reference.uri)
            if (
                parsed.scheme not in {"file", "s3"}
                or reference.size_bytes <= 0
                or (
                    parsed.scheme == "s3"
                    and (not reference.etag or not reference.object_version)
                )
            ):
                raise NonRetryableGoldenBuildError(
                    "source capture reference must be an immutable file or S3 object"
                )
            _require_sha256(
                reference.checksum_digest(),
                label="source capture reference checksum",
            )
        normalized.append(
            GoldenSourceRunState(
                source_product_id=source_product_id,
                run_ids=run_ids,
                run_status=run_status,
                freshness_status=freshness_status,
                watermark=watermark,
                acquired_at=acquired_at,
                identity_spine=raw.identity_spine,
                materialization=raw.materialization,
                gold_contribution=raw.gold_contribution,
                capture_reference=reference,
            )
        )

    missing = _EXPECTED_SOURCE_PRODUCTS - source_ids
    if missing:
        raise NonRetryableGoldenBuildError(
            "explicit source run states must include IMDb, TVmaze, Wikidata, and TMDB"
        )
    states = {item.source_product_id: item for item in normalized}
    for source_product_id in _EXPECTED_SOURCE_PRODUCTS:
        state = states[source_product_id]
        if (
            state.run_status != GoldenSourceRunStatus.COMMITTED
            or state.freshness_status
            not in {
                GoldenSourceFreshnessStatus.PASS,
                GoldenSourceFreshnessStatus.OVERRIDDEN,
            }
        ):
            raise NonRetryableGoldenBuildError(
                f"{source_product_id} must be committed and fresh"
            )
    if not any(state.gold_contribution for state in normalized):
        raise NonRetryableGoldenBuildError(
            "explicit source run states require an enabled Gold contribution"
        )
    expected_override_sources = {
        "imdb-non-commercial-datasets",
        "tvmaze-public-api",
    }
    actual_override_sources = {
        source_product_id
        for source_product_id in _EXPECTED_SOURCE_PRODUCTS
        if states[source_product_id].freshness_status
        == GoldenSourceFreshnessStatus.OVERRIDDEN
    }
    if actual_override_sources != expected_override_sources:
        raise NonRetryableGoldenBuildError(
            "explicit freshness override states must match IMDb and TVmaze"
        )
    for source_product_id, expected_acquired_at in (
        (
            "imdb-non-commercial-datasets",
            input_data.freshness_override.imdb_acquired_at,
        ),
        (
            "tvmaze-public-api",
            input_data.freshness_override.tvmaze_acquired_at,
        ),
    ):
        state = states[source_product_id]
        if (
            state.freshness_status == GoldenSourceFreshnessStatus.OVERRIDDEN
            and state.acquired_at
            != _parse_time(
                expected_acquired_at,
                label=f"{source_product_id} override acquired_at",
            )
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z")
        ):
            raise NonRetryableGoldenBuildError(
                f"{source_product_id} override state differs from freshness evidence"
            )

    expected_run_ids = {
        _require_sha256(value, label="pinned source run ID")
        for value in (
            *input_data.baseline_source_run_ids,
            *input_data.tmdb_source_run_ids,
            *input_data.supplemental_source_run_ids,
        )
    }
    if assigned_run_ids != expected_run_ids:
        raise NonRetryableGoldenBuildError(
            "source run states must exactly assign every pinned source run"
        )
    if set(states["tmdb-research"].run_ids) != {
        _require_sha256(value, label="TMDB source run ID")
        for value in input_data.tmdb_source_run_ids
    }:
        raise NonRetryableGoldenBuildError(
            "TMDB source state differs from the pinned TMDB runs"
        )
    return tuple(sorted(normalized, key=lambda item: item.source_product_id))


def _identity_config_digests(
    build_id: str,
    *,
    source_policy_digest: str | None = None,
) -> tuple[str, str, str]:
    resolution = IdentityResolutionConfig()
    source_digest = (
        load_catalog_source_policy().digest
        if source_policy_digest is None
        else _require_sha256(
            source_policy_digest,
            label="source policy digest",
        )
    )
    runtime = _digest(
        canonical_json_bytes(
            {
                "schemaVersion": "1.0",
                "buildId": build_id,
                "stage": "golden-incremental-identity",
                "identityMode": "incremental",
                "sourcePolicyDigest": source_digest,
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
    env = input_data.env
    if not (1 <= env.gold_tmdb_freshness_slo_hours <= _MAX_GOLD_FRESHNESS_SLO_HOURS):
        raise NonRetryableGoldenBuildError(
            "Gold TMDB freshness SLO must be between 1 and 1080 hours"
        )
    if not 0 <= env.gold_max_unresolved_identity_ratio <= 1:
        raise NonRetryableGoldenBuildError(
            "Gold unresolved identity ratio threshold must be between 0 and 1"
        )
    quality_override_enabled = (
        env.gold_tmdb_freshness_slo_hours != _DEFAULT_GOLD_TMDB_FRESHNESS_SLO_HOURS
        or env.gold_max_unresolved_identity_ratio
        != _DEFAULT_GOLD_MAX_UNRESOLVED_IDENTITY_RATIO
    )
    quality_override_reason = env.gold_quality_override_reason.strip()
    if quality_override_enabled and not quality_override_reason:
        raise NonRetryableGoldenBuildError(
            "temporary Gold quality override requires an audit reason"
        )
    if not quality_override_enabled and quality_override_reason:
        raise NonRetryableGoldenBuildError(
            "temporary Gold quality override reason requires non-default thresholds"
        )
    if len(quality_override_reason) > 512:
        raise NonRetryableGoldenBuildError(
            "temporary Gold quality override reason exceeds 512 characters"
        )
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
    supplemental_runs = tuple(
        sorted(
            {
                _require_sha256(value, label="supplemental source run ID")
                for value in input_data.supplemental_source_run_ids
            }
        )
    )
    if len(baseline_sources) != len(input_data.baseline_source_run_ids):
        raise NonRetryableGoldenBuildError("baseline source run IDs must be unique")
    if len(tmdb_runs) != 15:
        raise NonRetryableGoldenBuildError(
            "Golden Build requires exactly 15 unique TMDB source runs"
        )
    if len(supplemental_runs) != len(input_data.supplemental_source_run_ids):
        raise NonRetryableGoldenBuildError("supplemental source run IDs must be unique")
    identity_run = _require_sha256(
        input_data.baseline_identity_run_id,
        label="baseline Identity run ID",
    )
    overlap = (
        (set(baseline_sources) & set(tmdb_runs))
        | (set(baseline_sources) & set(supplemental_runs))
        | (set(tmdb_runs) & set(supplemental_runs))
        | (
            {identity_run}
            & (set(baseline_sources) | set(tmdb_runs) | set(supplemental_runs))
        )
    )
    if overlap:
        raise NonRetryableGoldenBuildError(
            "baseline, TMDB, supplemental, and Identity run IDs must not overlap"
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
    source_policy = load_catalog_source_policy(registry=build_community_registry())
    source_run_states = _normalized_source_run_states(
        input_data,
        source_policy=source_policy,
    )
    all_runs = tuple(
        sorted(
            (
                {identity_run}
                | {
                    run_id
                    for state in source_run_states
                    if state.gold_contribution
                    for run_id in state.run_ids
                }
            )
            if source_run_states
            else expected_baseline_runs | set(tmdb_runs) | set(supplemental_runs)
        )
    )
    runtime_digest, resolution_digest, bound_digest = _identity_config_digests(
        input_data.build_id,
        source_policy_digest=source_policy.digest,
    )
    freshness_policy = research_release_freshness_policy(
        tmdb_slo_hours=input_data.env.gold_tmdb_freshness_slo_hours,
        tvmaze_slo_hours=tvmaze_slo,
        imdb_slo_hours=imdb_slo,
        wikidata_slo_hours=45 * 24,
    )
    if source_run_states:
        freshness_policy = scope_release_freshness_policy(
            freshness_policy,
            selected_source_product_ids={
                state.source_product_id
                for state in source_run_states
                if state.gold_contribution
            },
        )
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
        freshness_policy_digest=freshness_policy.digest,
        identity_config_digest=runtime_digest,
        identity_resolution_config_digest=resolution_digest,
        identity_bound_config_digest=bound_digest,
        source_policy_digest=source_policy.digest,
        source_run_states=source_run_states,
    )


def _incremental_identity_source_run_ids(
    input_data: GoldenBuildInput,
    preflight: GoldenPreflightResult,
) -> tuple[str, ...]:
    if preflight.source_run_states:
        return tuple(
            sorted(
                run_id
                for state in preflight.source_run_states
                if state.identity_spine
                for run_id in state.run_ids
            )
        )
    return tuple(
        sorted(
            {
                *preflight.tmdb_source_run_ids,
                *input_data.supplemental_source_run_ids,
            }
        )
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
    identity_source_run_ids = _incremental_identity_source_run_ids(
        input_data,
        preflight,
    )
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

    resolution = IdentityResolutionConfig()
    summary_source_run_ids = tuple(sorted(summary.get("sourceRunIds") or ()))
    # Require a concrete historical registry digest, but do not pin it to the
    # current process registry. Reuse extends an already-committed Identity
    # ledger head; later materialization may run under a newer registry.
    _require_sha256(
        str(summary.get("registryDigest") or ""),
        label="reused Identity registryDigest",
    )
    if (
        summary.get("context") != "research"
        or summary.get("stage") != "identity-resolution"
        or summary.get("identityGenerationId") != input_data.identity_generation_id
        or summary.get("identityMode") != "incremental"
        or summary_source_run_ids != identity_source_run_ids
        or summary.get("identityResolutionConfigDigest") != resolution.digest
        or resolution.digest != preflight.identity_resolution_config_digest
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
    job_policy_digests = _argument_values(arguments, "--source-policy-digest")
    if len(job_policy_digests) > 1:
        raise NonRetryableGoldenBuildError(
            "reused Identity job must not repeat --source-policy-digest"
        )
    summary_policy_digest = summary.get("sourcePolicyDigest")
    if summary_policy_digest not in (None, ""):
        summary_policy_digest = _require_sha256(
            str(summary_policy_digest),
            label="reused Identity sourcePolicyDigest",
        )
    else:
        summary_policy_digest = None
    if job_policy_digests and summary_policy_digest is None:
        raise NonRetryableGoldenBuildError(
            "reused Identity summary lacks sourcePolicyDigest present on the EMR job"
        )
    if (
        summary_policy_digest is not None
        and job_policy_digests
        and summary_policy_digest != job_policy_digests[0]
    ):
        raise NonRetryableGoldenBuildError(
            "reused Identity sourcePolicyDigest differs between summary and EMR job"
        )
    job_runtime = _single_argument(arguments, "--config-digest")
    if summary.get("configDigest") != resolution.bind_runtime_config(job_runtime):
        raise NonRetryableGoldenBuildError(
            "reused Identity configDigest does not match the EMR runtime digest"
        )
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
        != identity_source_run_ids
        or _single_argument(arguments, "--identity-generation-id")
        != input_data.identity_generation_id
        or _single_argument(arguments, "--identity-mode") != "incremental"
        or _single_argument(arguments, "--image-digest") != image_digest
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
            "supplementalSourceRunIds": list(input_data.supplemental_source_run_ids),
            "incrementalIdentitySourceRunIds": list(
                _incremental_identity_source_run_ids(input_data, preflight)
            ),
            "allSnapshotRunIds": list(preflight.all_snapshot_run_ids),
            "sourceWatermarks": dict(sorted(input_data.source_watermarks.items())),
            "sourcePolicyDigest": preflight.source_policy_digest,
            "sourceRunStates": [
                _source_run_state_payload(state)
                for state in preflight.source_run_states
            ],
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
            "goldQualityThresholds": {
                "tmdbFreshnessSloHours": (input_data.env.gold_tmdb_freshness_slo_hours),
                "maxUnresolvedIdentityRatio": (
                    input_data.env.gold_max_unresolved_identity_ratio
                ),
                "temporaryOverride": bool(
                    input_data.env.gold_quality_override_reason.strip()
                ),
                **(
                    {
                        "temporaryOverrideReason": (
                            input_data.env.gold_quality_override_reason.strip()
                        )
                    }
                    if input_data.env.gold_quality_override_reason.strip()
                    else {}
                ),
            },
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
        classified = classify_golden_build_exception(exc)
        if classified is exc:
            raise
        raise classified from exc


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


def _resource_gib(value: str, *, label: str) -> int:
    match = _GIB_RESOURCE.fullmatch(value.strip())
    if match is None:
        raise NonRetryableGoldenBuildError(
            f"{label} must be a positive whole GiB value such as 24g or 170G"
        )
    return int(match.group(1))


def _validate_gold_emr_capacity(input_data: GoldenBuildInput) -> None:
    """Keep the Gold request below its dedicated app, including replacement room."""

    env = input_data.env
    if (
        env.gold_emr_application_name
        and env.gold_emr_application_name == env.emr_application_name
    ):
        raise NonRetryableGoldenBuildError(
            "Gold release must use a dedicated EMR Serverless application"
        )
    if env.gold_executor_instances < 1 or env.gold_executor_cores < 1:
        raise NonRetryableGoldenBuildError(
            "Gold executor instances and cores must be positive"
        )
    if env.gold_shuffle_partitions < 1:
        raise NonRetryableGoldenBuildError("Gold shuffle partitions must be positive")

    executor_slots = env.gold_executor_instances + _GOLD_REPLACEMENT_EXECUTORS
    requested_vcpu = _GOLD_DRIVER_CORES + executor_slots * env.gold_executor_cores
    requested_memory = (
        _resource_gib(_GOLD_DRIVER_MEMORY, label="Gold driver memory")
        + _resource_gib(
            _GOLD_DRIVER_MEMORY_OVERHEAD,
            label="Gold driver memory overhead",
        )
        + executor_slots
        * (
            _resource_gib(
                env.gold_executor_memory,
                label="Gold executor memory",
            )
            + _resource_gib(
                env.gold_executor_memory_overhead,
                label="Gold executor memory overhead",
            )
        )
    )
    requested_disk = _resource_gib(
        _GOLD_DRIVER_DISK,
        label="Gold driver disk",
    ) + executor_slots * _resource_gib(
        env.gold_executor_disk,
        label="Gold executor disk",
    )
    requested = {
        "vCPU": (requested_vcpu, _GOLD_EMR_MAX_VCPU),
        "GiB memory": (requested_memory, _GOLD_EMR_MAX_MEMORY_GIB),
        "GiB disk": (requested_disk, _GOLD_EMR_MAX_DISK_GIB),
    }
    exceeded = [
        f"{label} {actual}>{limit}"
        for label, (actual, limit) in requested.items()
        if actual > limit
    ]
    if exceeded:
        raise NonRetryableGoldenBuildError(
            "Gold EMR request exceeds the dedicated application capacity "
            f"including {_GOLD_REPLACEMENT_EXECUTORS} replacement executor: "
            + ", ".join(exceeded)
        )


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
    gold_release = stage == "release"
    if gold_release:
        _validate_gold_emr_capacity(input_data)
    return argparse.Namespace(
        application_name=(
            (env.gold_emr_application_name or env.emr_application_name)
            if gold_release
            else env.emr_application_name
        ),
        execution_role_arn=env.emr_execution_role_arn,
        job_name=f"gold-{stage}-{input_data.build_id}"[:64],
        client_token=_stage_token(
            build_id=input_data.build_id,
            stage=stage,
            workflow_run_id=workflow_run_id,
        ),
        entry_point=entry_point,
        log_uri=(
            (env.gold_emr_log_uri or env.emr_log_uri)
            if gold_release
            else env.emr_log_uri
        ),
        aws_region=env.aws_region,
        poll_seconds=15,
        # Gold field-resolution shuffle can exceed 12h when FetchFailed retries
        # dominate; keep headroom above the single clean pass (~4-8h).
        execution_timeout_minutes=900,
        max_attempts=1,
        driver_cores=_GOLD_DRIVER_CORES,
        driver_memory=_GOLD_DRIVER_MEMORY,
        driver_memory_overhead=(_GOLD_DRIVER_MEMORY_OVERHEAD if gold_release else None),
        driver_disk=_GOLD_DRIVER_DISK,
        executor_cores=(env.gold_executor_cores if gold_release else 8),
        executor_memory=(env.gold_executor_memory if gold_release else "48g"),
        executor_memory_overhead=(
            env.gold_executor_memory_overhead if gold_release else None
        ),
        executor_disk=(env.gold_executor_disk if gold_release else "400G"),
        executor_instances=(
            env.gold_executor_instances
            if gold_release
            else (
                env.control_executor_instances
                if control_profile
                else env.build_executor_instances
            )
        ),
        shuffle_partitions=(
            env.gold_shuffle_partitions
            if gold_release
            else (
                env.control_shuffle_partitions
                if control_profile
                else env.build_shuffle_partitions
            )
        ),
        enable_speculation=gold_release,
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
        log_uri=parsed.log_uri,
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
        classified = classify_golden_build_exception(exc)
        if classified is exc:
            raise
        raise classified from exc


@activity.defn(name="ResolveGoldenIdentity")
def resolve_golden_identity(
    input_data: GoldenBuildInput,
    preflight: GoldenPreflightResult,
    snapshot: GoldenSnapshotResult,
    started_at: str,
    committed_at: str,
) -> GoldenIdentityResult:
    try:
        identity_source_run_ids = _incremental_identity_source_run_ids(
            input_data,
            preflight,
        )
        args = [
            "resolve-identity",
            *_object_args("silver-snapshot", snapshot.snapshot),
            "--silver-snapshot-media-type",
            SILVER_SNAPSHOT_MEDIA_TYPE,
        ]
        for run_id in identity_source_run_ids:
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
                "--source-policy-digest",
                preflight.source_policy_digest,
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
                "sourcePolicyDigest",
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
            != identity_source_run_ids
            or summary.get("configDigest") != preflight.identity_bound_config_digest
            or summary.get("identityResolutionConfigDigest")
            != preflight.identity_resolution_config_digest
            or summary.get("sourcePolicyDigest") != preflight.source_policy_digest
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
        classified = classify_golden_build_exception(exc)
        if classified is exc:
            raise
        raise classified from exc


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
        classified = classify_golden_build_exception(exc)
        if classified is exc:
            raise
        raise classified from exc


def _validate_release_documents(
    *,
    client: Any,
    input_data: GoldenBuildInput,
    preflight: GoldenPreflightResult,
    quality_ref: ObjectRefPayload,
    attribution_ref: ObjectRefPayload,
    plan_ref: ObjectRefPayload,
    commit_ref: ObjectRefPayload,
    release_plan_id: str,
    commit_key: str,
    table_counts: dict[str, int],
    table_snapshot_ids: dict[str, int | None],
) -> None:
    plan_payload = _read_ref_json(client, plan_ref, label="Gold release plan")
    try:
        plan = GoldReleasePlan.model_validate(plan_payload)
    except (TypeError, ValueError) as exc:
        raise NonRetryableGoldenBuildError("Gold release plan is invalid") from exc
    if plan.release_plan_id != release_plan_id or (
        bool(preflight.source_policy_digest)
        and plan.source_policy_digest != preflight.source_policy_digest
    ):
        raise NonRetryableGoldenBuildError(
            "Gold release plan differs from the verified release evidence"
        )
    reference_domains = {"NEWS", "JOURNALISM", "LEARNING", "INSTRUCTION"}
    has_reference_coverage = any(
        plan.domain_coverage.get(domain, 0) > 0 for domain in reference_domains
    )
    if has_reference_coverage or plan.source_snapshot_pin_ids:
        ontology = load_global_reference_ontology()
        predicates = load_research_predicate_registry()
        if (
            plan.reference_ontology_digest != ontology.digest
            or plan.reference_predicate_registry_digest != predicates.digest
        ):
            raise NonRetryableGoldenBuildError(
                "Gold release plan has unverified reference registry coordinates"
            )
    if has_reference_coverage and not plan.source_snapshot_pin_ids.get(
        "wikidata-json-dump"
    ):
        raise NonRetryableGoldenBuildError(
            "Gold reference coverage lacks a pinned Wikidata source snapshot"
        )
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
    gold_sources = (
        {
            state.source_product_id
            for state in preflight.source_run_states
            if state.gold_contribution
        }
        if preflight.source_run_states
        else _EXPECTED_SOURCE_PRODUCTS
    )
    if not set(plan.source_snapshot_pin_ids) <= gold_sources:
        raise NonRetryableGoldenBuildError(
            "Gold source snapshot pins differ from the contributing source set"
        )
    source_policy = load_catalog_source_policy()
    for source_product_id in gold_sources:
        expected_status = (
            {"PASS"}
            if source_policy.allows_source(
                source_product_id,
                CatalogSourceCapability.FRESHNESS,
            )
            else {"NOT_REQUIRED"}
        )
        if sources.get(source_product_id, {}).get("status") not in expected_status:
            raise NonRetryableGoldenBuildError(
                "Gold quality report contains a non-passing contributing source"
            )
    tmdb = sources["tmdb-research"]
    if "tmdb-research" in gold_sources and (
        int(tmdb.get("sloHours") or 0) != input_data.env.gold_tmdb_freshness_slo_hours
        or tmdb.get("status") != "PASS"
        or float(tmdb.get("effectiveAgeHours") or 0)
        >= input_data.env.gold_tmdb_freshness_slo_hours
    ):
        raise NonRetryableGoldenBuildError(
            "Gold quality report lacks the pinned TMDB freshness threshold"
        )
    unresolved_ratio = float(quality.get("unresolvedIdentityRatio", -1))
    if not (0 <= unresolved_ratio <= input_data.env.gold_max_unresolved_identity_ratio):
        raise NonRetryableGoldenBuildError(
            "Gold quality report exceeds the pinned unresolved identity threshold"
        )
    override = freshness.get("overrideEvidence") or {}
    expected_override_sources = (
        tuple(
            sorted(
                state.source_product_id
                for state in preflight.source_run_states
                if state.freshness_status == GoldenSourceFreshnessStatus.OVERRIDDEN
            )
        )
        if preflight.source_run_states
        else (
            "imdb-non-commercial-datasets",
            "tvmaze-public-api",
        )
    )
    if (
        override.get("reason") != input_data.freshness_override.reason.strip()
        or tuple(sorted(override.get("sourceProductIds") or ()))
        != expected_override_sources
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
        if int(row.get("sloHours") or 0) != slo:
            raise NonRetryableGoldenBuildError(
                f"freshness override was not applied correctly for {source}"
            )
        if source in gold_sources:
            if row.get("status") != "PASS" or (
                float(row.get("effectiveAgeHours") or 0) > slo
            ):
                raise NonRetryableGoldenBuildError(
                    f"freshness override was not applied correctly for {source}"
                )
        elif (
            row.get("status") not in {"OPTIONAL", "PASS"}
            or row.get("required") is not False
        ):
            raise NonRetryableGoldenBuildError(
                f"non-contributing freshness source was not scoped out: {source}"
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
    if preflight.source_run_states:
        if attributed_sources != gold_sources:
            raise NonRetryableGoldenBuildError(
                "Gold attribution manifest differs from the contributing source set"
            )
    elif not _EXPECTED_SOURCE_PRODUCTS.issubset(attributed_sources):
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
        or (
            bool(preflight.source_policy_digest)
            and commit.source_policy_digest != preflight.source_policy_digest
        )
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
            "--source-policy-digest",
            preflight.source_policy_digest,
            "--max-unresolved-identity-ratio",
            str(env.gold_max_unresolved_identity_ratio),
            "--tmdb-freshness-slo-hours",
            str(env.gold_tmdb_freshness_slo_hours),
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
                "releasePlan",
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
            "releasePlan",
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
        plan_ref = _validated_object_ref(
            summary["releasePlan"],
            label="Gold release plan",
            media_type=GOLD_RELEASE_PLAN_MEDIA_TYPE,
        )
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
            plan_ref=plan_ref,
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
            release_plan=plan_ref,
            quality_report=quality_ref,
            attribution_manifest=attribution_ref,
            release_commit=commit_ref,
            table_counts=table_counts,
            table_snapshot_ids=table_snapshot_ids,
            job_run_id=job_run_id,
        )
    except Exception as exc:
        classified = classify_golden_build_exception(exc)
        if classified is exc:
            raise
        raise classified from exc


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
                "releasePlan": _object_ref_payload(result.release.release_plan),
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
        classified = classify_golden_build_exception(exc)
        if classified is exc:
            raise
        raise classified from exc
