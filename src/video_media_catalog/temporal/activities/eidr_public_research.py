"""Activities for explicit, bounded EIDR public-research ingestion."""

from __future__ import annotations

import argparse
import hashlib
import threading
from contextvars import copy_context
from typing import Any

from temporalio import activity

from video_media_catalog.canonical import canonical_json_bytes, sha256_digest
from video_media_catalog.community_snapshot import CONTROL_MAX_BYTES
from video_media_catalog.eidr_backfill import (
    BACKFILL_RUN_MANIFEST_MEDIA_TYPE,
    DISCOVERED_ID_MANIFEST_MEDIA_TYPE,
    WIKIDATA_P2704_DISCOVERY_SCOPE,
    read_discovered_eidr_id_manifest,
    read_eidr_backfill_run_manifest,
    verify_eidr_backfill_run_manifest_objects,
)
from video_media_catalog.eidr_backfill_cli import (
    build_parser as build_eidr_parser,
)
from video_media_catalog.eidr_backfill_cli import run as run_eidr_cli
from video_media_catalog.emr_driver_logs import read_driver_json_summary
from video_media_catalog.emr_serverless_cli import run as run_emr_submit
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.object_store import (
    BoundedObjectStore,
    S3Location,
    conditional_publish_bytes,
)
from video_media_catalog.storage import join_uri
from video_media_catalog.temporal.eidr_models import (
    EidrDiscoveryResult,
    EidrLookupResult,
    EidrPublicResearchInput,
    eidr_public_research_workflow_id,
    normalize_pinned_s3_object,
)
from video_media_catalog.temporal.errors import (
    classify_capture_exception,
    classify_silver_exception,
)
from video_media_catalog.temporal.models import (
    CaptureResult,
    ObjectRefPayload,
    SilverResult,
)
from video_media_catalog.v2_contracts import require_sha256

EIDR_DISCOVERY_ENTRY_POINT = (
    "local:///opt/video-media-catalog/src/video_media_catalog/eidr_backfill_cli.py"
)
EIDR_PUBLIC_RESEARCH_SUMMARY_MEDIA_TYPE = (
    "application/vnd.video-media-catalog.eidr-public-research-summary.v1+json"
)
_CONNECTOR_BATCH_MEDIA_TYPE = (
    "application/vnd.video-media-catalog.connector-batch.v2+json"
)
_RECORD_SET_MEDIA_TYPE = "application/vnd.video-media-catalog.record-set.v2+json"
_RUNNABLE_EMR_STATES = {"CREATED", "STARTED", "STOPPED"}


def _heartbeat(details: dict[str, Any]) -> None:
    if activity.in_activity():
        activity.heartbeat(details)


class _HeartbeatPulse:
    def __init__(self, details: dict[str, Any], interval_seconds: float = 20) -> None:
        self._details = details
        self._interval = interval_seconds
        self._stop = threading.Event()
        context = copy_context()
        self._thread = threading.Thread(
            target=lambda: context.run(self._run),
            name="eidr-public-research-heartbeat",
            daemon=True,
        )

    def _run(self) -> None:
        tick = 0
        while not self._stop.wait(self._interval):
            tick += 1
            _heartbeat({**self._details, "tick": tick})

    def __enter__(self) -> _HeartbeatPulse:
        self._thread.start()
        return self

    def __exit__(self, *_exc: object) -> None:
        self._stop.set()
        self._thread.join(timeout=1)


def _object_ref(reference: ObjectRefPayload) -> ObjectRef:
    return ObjectRef(
        uri=reference.uri,
        format=reference.format,
        media_type=reference.media_type,
        checksum=Checksum(value=reference.checksum_digest()),
        size_bytes=reference.size_bytes,
        etag=reference.etag,
        object_version=reference.object_version,
    )


def _object_ref_payload(reference: ObjectRef) -> ObjectRefPayload:
    return ObjectRefPayload(
        uri=reference.uri,
        format=reference.format,
        media_type=reference.media_type,
        checksum_algorithm=reference.checksum.algorithm,
        checksum_value=reference.checksum.value,
        size_bytes=reference.size_bytes,
        etag=reference.etag,
        object_version=reference.object_version,
    )


def _object_ref_mapping(reference: ObjectRefPayload) -> dict[str, Any]:
    return {
        "uri": reference.uri,
        "format": reference.format,
        "mediaType": reference.media_type,
        "checksum": {
            "algorithm": reference.checksum_algorithm,
            "value": reference.checksum_value,
        },
        "sizeBytes": reference.size_bytes,
        "etag": reference.etag,
        "objectVersion": reference.object_version,
    }


def _object_args(prefix: str, reference: ObjectRefPayload) -> list[str]:
    return [
        f"--{prefix}-uri",
        reference.uri,
        f"--{prefix}-hash",
        reference.checksum_digest(),
        f"--{prefix}-size",
        str(reference.size_bytes),
        f"--{prefix}-version",
        str(reference.object_version),
        f"--{prefix}-etag",
        str(reference.etag).strip('"'),
    ]


def _runtime_store(input_data: EidrPublicResearchInput) -> BoundedObjectStore:
    return BoundedObjectStore(
        region=input_data.env.aws_region,
        endpoint_url=input_data.env.s3_endpoint,
        path_style_access=input_data.env.s3_path_style_access,
    )


def _require_int(payload: dict[str, Any], key: str) -> int:
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{key} must be an integer")
    if value < 0:
        raise ValueError(f"{key} must not be negative")
    return value


def _ref_from_mapping(
    value: Any,
    *,
    label: str,
    media_type: str,
) -> tuple[ObjectRefPayload, ObjectRef]:
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be an ObjectRef object")
    payload = normalize_pinned_s3_object(
        ObjectRefPayload.from_mapping(value),
        label=label,
        media_type=media_type,
    )
    return payload, _object_ref(payload)


def _require_under_prefix(uri: str, prefix: str, *, label: str) -> None:
    object_location = S3Location.parse(uri)
    prefix_location = S3Location.parse(prefix)
    prefix_key = prefix_location.key.rstrip("/")
    if (
        object_location.bucket != prefix_location.bucket
        or not object_location.key.startswith(f"{prefix_key}/")
    ):
        raise ValueError(f"{label} must be under the configured destination prefix")


def _discovery_client_token(
    input_data: EidrPublicResearchInput,
    workflow_run_id: str,
) -> str:
    digest = hashlib.sha256(
        (
            f"{eidr_public_research_workflow_id(input_data)}:"
            f"{workflow_run_id}:discovery"
        ).encode()
    ).hexdigest()
    return f"eidr-disc-{digest[:54]}"


def _discovery_job_name(input_data: EidrPublicResearchInput) -> str:
    digest = hashlib.sha256(
        (
            f"{input_data.source_release_id}:"
            f"{input_data.silver_snapshot.checksum_digest()}:"
            f"{input_data.emr_image_digest}"
        ).encode()
    ).hexdigest()
    return f"eidr-p2704-{digest[:20]}"


def _verify_emr_application_image(
    client: Any,
    input_data: EidrPublicResearchInput,
) -> None:
    applications: list[dict[str, Any]] = []
    request: dict[str, Any] = {}
    while True:
        response = client.list_applications(**request)
        applications.extend(response.get("applications", []))
        token = response.get("nextToken")
        if not token:
            break
        request["nextToken"] = token
    matches = [
        item
        for item in applications
        if item.get("name") == input_data.env.emr_application_name
        and item.get("state") in _RUNNABLE_EMR_STATES
    ]
    if len(matches) != 1:
        raise RuntimeError(
            "expected exactly one runnable EIDR discovery EMR application"
        )
    application = client.get_application(applicationId=str(matches[0]["id"])).get(
        "application",
        {},
    )
    image_uri = str((application.get("imageConfiguration") or {}).get("imageUri") or "")
    if not image_uri.endswith(f"@{input_data.emr_image_digest}"):
        raise RuntimeError(
            "EIDR discovery EMR application image does not match emr_image_digest"
        )


def _discovery_emr_namespace(
    input_data: EidrPublicResearchInput,
    *,
    workflow_run_id: str,
) -> argparse.Namespace:
    entry_args = [
        "extract-wikidata-p2704",
        *_object_args("silver-snapshot", input_data.silver_snapshot),
        "--source-release-id",
        input_data.source_release_id,
        "--destination-prefix",
        input_data.env.destination_prefix,
        "--created-at",
        input_data.acquired_at,
        "--page-size",
        str(input_data.discovery_page_size),
        "--aws-region",
        input_data.env.aws_region,
        "--catalog-type",
        "glue",
        "--catalog-name",
        input_data.env.catalog_name,
        "--namespace",
        input_data.env.glue_namespace,
        "--warehouse",
        input_data.env.warehouse_uri,
        "--s3-credentials-provider",
        "default",
    ]
    if input_data.env.s3_endpoint:
        entry_args.extend(["--s3-endpoint", input_data.env.s3_endpoint])
    if input_data.env.s3_path_style_access:
        entry_args.append("--s3-path-style-access")
    return argparse.Namespace(
        application_name=input_data.env.emr_application_name,
        execution_role_arn=input_data.env.emr_execution_role_arn,
        job_name=_discovery_job_name(input_data),
        client_token=_discovery_client_token(input_data, workflow_run_id),
        entry_point=EIDR_DISCOVERY_ENTRY_POINT,
        log_uri=input_data.env.emr_log_uri,
        aws_region=input_data.env.aws_region,
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
        executor_instances=input_data.env.emr_executor_instances,
        shuffle_partitions=input_data.env.emr_shuffle_partitions,
        enable_speculation=False,
        entry_point_arguments=entry_args,
    )


@activity.defn(name="DiscoverEidrPublicResearchIds")
def discover_eidr_public_research_ids(
    input_data: EidrPublicResearchInput,
) -> EidrDiscoveryResult:
    workflow_run_id = (
        activity.info().workflow_run_id if activity.in_activity() else "standalone"
    )
    parsed = _discovery_emr_namespace(
        input_data,
        workflow_run_id=workflow_run_id,
    )

    def on_progress(event: dict[str, Any]) -> None:
        _heartbeat({"phase": "eidr-discovery-emr", **event})

    def should_cancel() -> bool:
        return activity.in_activity() and activity.is_cancelled()

    try:
        import boto3

        emr_client = boto3.client(
            "emr-serverless",
            region_name=input_data.env.aws_region,
        )
        _verify_emr_application_image(emr_client, input_data)
        job_run = run_emr_submit(
            parsed,
            client=emr_client,
            progress_callback=on_progress,
            should_cancel=should_cancel,
        )
        application_id = str(job_run["applicationId"])
        job_run_id = str(job_run["jobRunId"])
        store = _runtime_store(input_data)
        summary = read_driver_json_summary(
            s3_client=store.client,
            log_uri=input_data.env.emr_log_uri,
            application_id=application_id,
            job_run_id=job_run_id,
            required_keys=("manifestId", "manifest", "idCount"),
            label="EIDR P2704 discovery",
        )
        manifest_id = require_sha256(
            str(summary["manifestId"]),
            label="discovery manifestId",
        )
        manifest_payload, manifest_ref = _ref_from_mapping(
            summary["manifest"],
            label="discovery manifest",
            media_type=DISCOVERED_ID_MANIFEST_MEDIA_TYPE,
        )
        _require_under_prefix(
            manifest_payload.uri,
            input_data.env.destination_prefix,
            label="discovery manifest",
        )
        id_count = _require_int(summary, "idCount")
        manifest = read_discovered_eidr_id_manifest(
            reference=manifest_ref,
            store=store,
        )
        if manifest.manifest_id != manifest_id or manifest.id_count != id_count:
            raise ValueError("EMR discovery summary does not match its manifest")
        if manifest.source.source_release_id != input_data.source_release_id:
            raise ValueError("discovery manifest source release does not match input")
        if manifest.source.silver_snapshot_object != _object_ref(
            input_data.silver_snapshot
        ):
            raise ValueError("discovery manifest Silver snapshot does not match input")
        if manifest.source.discovery_scope != WIKIDATA_P2704_DISCOVERY_SCOPE:
            raise ValueError("discovery manifest is not scoped to Wikidata P2704")
        if manifest.created_at != input_data.acquired_at:
            raise ValueError("discovery manifest timestamp does not match acquired_at")
        if "pageCount" in summary and _require_int(summary, "pageCount") != (
            manifest.page_count
        ):
            raise ValueError("EMR discovery pageCount does not match its manifest")
        return EidrDiscoveryResult(
            manifest_id=manifest_id,
            manifest=manifest_payload,
            id_count=id_count,
            page_count=manifest.page_count,
            job_run_id=job_run_id,
        )
    except Exception as exc:
        raise classify_silver_exception(exc) from exc


def _lookup_arguments(
    input_data: EidrPublicResearchInput,
    discovery: EidrDiscoveryResult,
) -> list[str]:
    arguments = [
        "lookup-public-research",
        *_object_args("manifest", discovery.manifest),
        "--destination-prefix",
        input_data.env.destination_prefix,
        "--acquired-at",
        input_data.acquired_at,
        "--image-digest",
        input_data.env.image_digest,
        "--batch-size",
        str(input_data.batch_size),
        "--max-batches",
        str(input_data.max_batches),
        "--max-duration-seconds",
        str(input_data.max_duration_seconds),
        "--max-ids",
        str(input_data.max_ids),
        *_object_args(
            "authorization-evidence",
            input_data.authorization_evidence,
        ),
        "--authorization-issued-at",
        input_data.authorization_issued_at,
        "--user-agent",
        input_data.env.user_agent,
        "--minimum-request-interval-seconds",
        str(input_data.minimum_request_interval_seconds),
        "--aws-region",
        input_data.env.aws_region,
    ]
    if input_data.env.s3_endpoint:
        arguments.extend(["--s3-endpoint", input_data.env.s3_endpoint])
    if input_data.env.s3_path_style_access:
        arguments.append("--s3-path-style-access")
    return arguments


def _validate_lookup_payload(
    payload: dict[str, Any],
    *,
    input_data: EidrPublicResearchInput,
    discovery: EidrDiscoveryResult,
    store: BoundedObjectStore,
) -> EidrLookupResult:
    if payload.get("profile") != WIKIDATA_P2704_DISCOVERY_SCOPE.profile_id:
        raise ValueError("lookup result does not use the public-research profile")
    if payload.get("sourceCompleteness") != "PARTIAL":
        raise ValueError("public-research lookup must remain PARTIAL")
    if payload.get("completeFeedAllowed") is not False:
        raise ValueError("public-research lookup cannot allow a complete feed")
    if not isinstance(payload.get("completed"), bool):
        raise ValueError("lookup completed must be a boolean")
    run_manifest_id = require_sha256(
        str(payload["runManifestId"]),
        label="lookup runManifestId",
    )
    run_payload, run_ref = _ref_from_mapping(
        payload["runManifest"],
        label="lookup run manifest",
        media_type=BACKFILL_RUN_MANIFEST_MEDIA_TYPE,
    )
    _require_under_prefix(
        run_payload.uri,
        input_data.env.destination_prefix,
        label="lookup run manifest",
    )
    run_manifest = read_eidr_backfill_run_manifest(
        reference=run_ref,
        store=store,
    )
    if run_manifest.run_manifest_id != run_manifest_id:
        raise ValueError("lookup result runManifestId does not match its manifest")
    if (
        run_manifest.discovered_manifest_id != discovery.manifest_id
        or run_manifest.discovered_manifest_object != _object_ref(discovery.manifest)
    ):
        raise ValueError("lookup run manifest does not bind the discovery result")
    if run_manifest.created_at != input_data.acquired_at:
        raise ValueError("lookup run manifest timestamp does not match acquired_at")
    if run_manifest.batch_count > input_data.max_batches:
        raise ValueError("lookup run manifest exceeds configured max_batches")
    if run_manifest.attempted_id_count > input_data.max_ids:
        raise ValueError("lookup run manifest exceeds configured max_ids")
    expected_values: dict[str, Any] = {
        "completed": run_manifest.completed,
        "stopReason": run_manifest.stop_reason.value,
        "batchCount": run_manifest.batch_count,
        "attemptedIdCount": run_manifest.attempted_id_count,
        "foundCount": run_manifest.found_count,
        "notFoundCount": run_manifest.not_found_count,
        "attemptCount": run_manifest.attempt_count,
        "retryCount": run_manifest.retry_count,
        "rateLimitCount": run_manifest.rate_limit_count,
    }
    for key, expected in expected_values.items():
        if payload.get(key) != expected:
            raise ValueError(f"lookup result {key} does not match its manifest")

    provided_inputs = payload.get("sourceSilverInputs")
    if not isinstance(provided_inputs, list):
        raise ValueError("lookup result sourceSilverInputs must be a list")
    batches = [
        batch
        for batch in run_manifest.batches
        if batch.connector_batch_object is not None
    ]
    if len(provided_inputs) != len(batches) or payload.get(
        "sourceSilverInputCount"
    ) != len(batches):
        raise ValueError("lookup sourceSilverInputs count does not match its manifest")

    captures: list[CaptureResult] = []
    for index, (provided, batch) in enumerate(
        zip(provided_inputs, batches, strict=True)
    ):
        if not isinstance(provided, dict):
            raise ValueError(f"sourceSilverInputs[{index}] must be an object")
        batch_payload, _ = _ref_from_mapping(
            provided.get("batchManifest"),
            label=f"sourceSilverInputs[{index}].batchManifest",
            media_type=_CONNECTOR_BATCH_MEDIA_TYPE,
        )
        record_payload, _ = _ref_from_mapping(
            provided.get("recordSetManifest"),
            label=f"sourceSilverInputs[{index}].recordSetManifest",
            media_type=_RECORD_SET_MEDIA_TYPE,
        )
        if batch_payload != _object_ref_payload(
            batch.connector_batch_object
        ) or record_payload != _object_ref_payload(batch.record_set_object):
            raise ValueError(
                f"sourceSilverInputs[{index}] does not match the run manifest"
            )
        assert batch.connector_batch_id is not None
        assert batch.record_set_id is not None
        captures.append(
            CaptureResult(
                kind="eidr",
                batch_id=batch.connector_batch_id,
                record_set_id=batch.record_set_id,
                record_count=batch.found_count,
                batch_manifest=batch_payload,
                record_set_manifest=record_payload,
                retry_count=batch.retry_count,
                rate_limit_count=batch.rate_limit_count,
            )
        )
    verify_eidr_backfill_run_manifest_objects(
        manifest=run_manifest,
        store=store,
    )
    return EidrLookupResult(
        run_manifest_id=run_manifest_id,
        run_manifest=run_payload,
        completed=run_manifest.completed,
        stop_reason=run_manifest.stop_reason.value,
        batch_count=run_manifest.batch_count,
        attempted_id_count=run_manifest.attempted_id_count,
        found_count=run_manifest.found_count,
        not_found_count=run_manifest.not_found_count,
        attempt_count=run_manifest.attempt_count,
        retry_count=run_manifest.retry_count,
        rate_limit_count=run_manifest.rate_limit_count,
        source_silver_inputs=tuple(captures),
    )


@activity.defn(name="LookupEidrPublicResearch")
def lookup_eidr_public_research(
    input_data: EidrPublicResearchInput,
    discovery: EidrDiscoveryResult,
) -> EidrLookupResult:
    try:
        normalize_pinned_s3_object(
            discovery.manifest,
            label="discovery manifest",
            media_type=DISCOVERED_ID_MANIFEST_MEDIA_TYPE,
        )
        parsed = build_eidr_parser().parse_args(
            _lookup_arguments(input_data, discovery)
        )
        store = _runtime_store(input_data)
        with _HeartbeatPulse(
            {
                "phase": "eidr-public-research-lookup",
                "manifestId": discovery.manifest_id,
            }
        ):
            payload = run_eidr_cli(parsed, store=store)
        if not isinstance(payload, dict):
            raise ValueError("lookup-public-research returned a non-object result")
        return _validate_lookup_payload(
            payload,
            input_data=input_data,
            discovery=discovery,
            store=store,
        )
    except Exception as exc:
        raise classify_capture_exception(exc) from exc


def _summary_payload(
    input_data: EidrPublicResearchInput,
    discovery: EidrDiscoveryResult,
    lookup: EidrLookupResult,
    silvers: tuple[SilverResult, ...],
) -> dict[str, Any]:
    supplemental_ids = [item.run_id for item in silvers]
    return {
        "schemaVersion": "1.0",
        "profile": WIKIDATA_P2704_DISCOVERY_SCOPE.profile_id,
        "sourceCompleteness": "PARTIAL",
        "completeFeedAllowed": False,
        "acquiredAt": input_data.acquired_at,
        "sourceReleaseId": input_data.source_release_id,
        "silverSnapshot": _object_ref_mapping(input_data.silver_snapshot),
        "authorizationEvidence": _object_ref_mapping(input_data.authorization_evidence),
        "authorizationIssuedAt": input_data.authorization_issued_at,
        "destinationPrefix": input_data.env.destination_prefix,
        "imageDigest": input_data.env.image_digest,
        "emrImageDigest": input_data.emr_image_digest,
        "limits": {
            "maxIds": input_data.max_ids,
            "maxBatches": input_data.max_batches,
            "batchSize": input_data.batch_size,
            "maxDurationSeconds": input_data.max_duration_seconds,
            "minimumRequestIntervalSeconds": (
                input_data.minimum_request_interval_seconds
            ),
        },
        "discovery": {
            "manifestId": discovery.manifest_id,
            "manifest": _object_ref_mapping(discovery.manifest),
            "idCount": discovery.id_count,
            "pageCount": discovery.page_count,
            "jobRunId": discovery.job_run_id,
        },
        "lookup": {
            "runManifestId": lookup.run_manifest_id,
            "runManifest": _object_ref_mapping(lookup.run_manifest),
            "completed": lookup.completed,
            "stopReason": lookup.stop_reason,
            "batchCount": lookup.batch_count,
            "attemptedIdCount": lookup.attempted_id_count,
            "foundCount": lookup.found_count,
            "notFoundCount": lookup.not_found_count,
            "attemptCount": lookup.attempt_count,
            "retryCount": lookup.retry_count,
            "rateLimitCount": lookup.rate_limit_count,
            "sourceSilverInputCount": len(lookup.source_silver_inputs),
            "sourceSilverInputs": [
                {
                    "batchId": item.batch_id,
                    "recordSetId": item.record_set_id,
                    "recordCount": item.record_count,
                    "batchManifest": _object_ref_mapping(item.batch_manifest),
                    "recordSetManifest": _object_ref_mapping(item.record_set_manifest),
                }
                for item in lookup.source_silver_inputs
            ],
        },
        "sourceSilver": [
            {
                "batchId": item.batch_id,
                "runId": item.run_id,
                "commitKey": item.commit_key,
                "jobRunId": item.job_run_id,
                "tableCounts": item.table_counts,
                "tableSnapshotIds": item.table_snapshot_ids,
            }
            for item in silvers
        ],
        "supplementalSourceRunIds": supplemental_ids,
        "goldenBuildHandoff": {
            "field": "supplementalSourceRunIds",
            "values": supplemental_ids,
        },
        "publication": {
            "gold": "not-started",
            "opensearch": "not-started",
            "cutover": "disabled",
        },
    }


@activity.defn(name="WriteEidrPublicResearchSummary")
def write_eidr_public_research_summary(
    input_data: EidrPublicResearchInput,
    discovery: EidrDiscoveryResult,
    lookup: EidrLookupResult,
    silvers: tuple[SilverResult, ...],
) -> ObjectRefPayload:
    try:
        payload = _summary_payload(input_data, discovery, lookup, silvers)
        summary_id = sha256_digest(canonical_json_bytes(payload))
        body = canonical_json_bytes(
            {
                **payload,
                "summaryId": summary_id,
            },
            newline=True,
        )
        store = _runtime_store(input_data)
        destination = join_uri(
            input_data.env.pipeline_summary_prefix,
            "eidr-public-research",
            f"{summary_id.removeprefix('sha256:')}.json",
        )
        published = conditional_publish_bytes(
            store,
            body,
            destination,
            media_type=EIDR_PUBLIC_RESEARCH_SUMMARY_MEDIA_TYPE,
            object_format="OBJECT_FORMAT_JSON",
            max_bytes=CONTROL_MAX_BYTES,
        )
        result = _object_ref_payload(published.object_ref)
        result = normalize_pinned_s3_object(
            result,
            label="EIDR public-research summary",
            media_type=EIDR_PUBLIC_RESEARCH_SUMMARY_MEDIA_TYPE,
        )
        _heartbeat(
            {
                "phase": "eidr-public-research-summary-written",
                "summaryId": summary_id,
                "uri": result.uri,
            }
        )
        return result
    except Exception as exc:
        raise classify_capture_exception(exc) from exc
