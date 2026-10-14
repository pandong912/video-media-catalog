from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

import pytest
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from video_media_catalog.community_snapshot import SILVER_SNAPSHOT_MEDIA_TYPE
from video_media_catalog.eidr_backfill import (
    BACKFILL_RUN_MANIFEST_MEDIA_TYPE,
    DISCOVERED_ID_MANIFEST_MEDIA_TYPE,
)
from video_media_catalog.temporal.activities.eidr_public_research import (
    EIDR_DISCOVERY_ENTRY_POINT,
    EIDR_PUBLIC_RESEARCH_SUMMARY_MEDIA_TYPE,
    _discovery_emr_namespace,
    _lookup_arguments,
    _summary_payload,
    _verify_emr_application_image,
)
from video_media_catalog.temporal.admin_cli import _eidr_input_from_mapping
from video_media_catalog.temporal.eidr_models import (
    EidrDiscoveryResult,
    EidrLookupResult,
    EidrPublicResearchInput,
    eidr_public_research_workflow_id,
)
from video_media_catalog.temporal.models import (
    CaptureResult,
    ObjectRefPayload,
    PipelineEnv,
    SilverResult,
)
from video_media_catalog.temporal.names import SILVER_TASK_QUEUE
from video_media_catalog.temporal.workflows.eidr_public_research import (
    EidrPublicResearchWorkflow,
)

_AUTH_MEDIA_TYPE = "application/json"
_BATCH_MEDIA_TYPE = "application/vnd.video-media-catalog.connector-batch.v2+json"
_RECORD_SET_MEDIA_TYPE = "application/vnd.video-media-catalog.record-set.v2+json"


def _ref(
    uri: str,
    media_type: str,
    *,
    digest: str = "a",
) -> ObjectRefPayload:
    return ObjectRefPayload(
        uri=uri,
        format="OBJECT_FORMAT_JSON",
        media_type=media_type,
        checksum_algorithm="CHECKSUM_ALGORITHM_SHA256",
        checksum_value=digest * 64,
        size_bytes=123,
        etag=f"etag-{digest}",
        object_version=f"version-{digest}",
    )


def _ref_mapping(reference: ObjectRefPayload) -> dict[str, Any]:
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


def _env() -> PipelineEnv:
    return PipelineEnv(
        destination_prefix="s3://bucket/landing/research/eidr",
        image_digest="sha256:" + ("b" * 64),
        user_agent="video-media-catalog/eidr-test",
        aws_region="us-east-1",
        warehouse_uri="s3://bucket/warehouse",
        record_staging_prefix="s3://bucket/warehouse/research/record-staging",
        silver_checkpoint_prefix="s3://bucket/warehouse/research/checkpoints",
        pipeline_summary_prefix="s3://bucket/landing/research/summaries",
        emr_application_name="media-catalog",
        emr_execution_role_arn="arn:aws:iam::123456789012:role/emr",
        emr_log_uri="s3://bucket/logs/emr",
        emr_entry_point=(
            "local:///opt/video-media-catalog/src/video_media_catalog/community_cli.py"
        ),
        catalog_type="glue",
        catalog_name="media",
        glue_namespace="video_media_catalog",
        emr_executor_instances=6,
        emr_shuffle_partitions=144,
    )


def _input() -> EidrPublicResearchInput:
    return EidrPublicResearchInput(
        env=_env(),
        silver_snapshot=_ref(
            "s3://bucket/control/silver-snapshot.json",
            SILVER_SNAPSHOT_MEDIA_TYPE,
            digest="c",
        ),
        source_release_id="sha256:" + ("d" * 64),
        authorization_evidence=_ref(
            "s3://bucket/control/eidr-authorization.json",
            _AUTH_MEDIA_TYPE,
            digest="e",
        ),
        authorization_issued_at="2026-10-12T00:00:00Z",
        acquired_at="2026-10-13T00:00:00Z",
        emr_image_digest="sha256:" + ("f" * 64),
    )


def _config() -> dict[str, Any]:
    input_data = _input()
    return {
        "env": {
            "destinationPrefix": input_data.env.destination_prefix,
            "imageDigest": input_data.env.image_digest,
            "userAgent": input_data.env.user_agent,
            "awsRegion": input_data.env.aws_region,
            "warehouseUri": input_data.env.warehouse_uri,
            "recordStagingPrefix": input_data.env.record_staging_prefix,
            "silverCheckpointPrefix": input_data.env.silver_checkpoint_prefix,
            "pipelineSummaryPrefix": input_data.env.pipeline_summary_prefix,
            "emrApplicationName": input_data.env.emr_application_name,
            "emrExecutionRoleArn": input_data.env.emr_execution_role_arn,
            "emrLogUri": input_data.env.emr_log_uri,
            "emrEntryPoint": input_data.env.emr_entry_point,
            "catalogType": "glue",
            "catalogName": input_data.env.catalog_name,
            "glueNamespace": input_data.env.glue_namespace,
            "emrExecutorInstances": input_data.env.emr_executor_instances,
            "emrShufflePartitions": input_data.env.emr_shuffle_partitions,
        },
        "silverSnapshot": _ref_mapping(input_data.silver_snapshot),
        "sourceReleaseId": input_data.source_release_id,
        "authorizationEvidence": _ref_mapping(input_data.authorization_evidence),
        "authorizationIssuedAt": input_data.authorization_issued_at,
        "acquiredAt": input_data.acquired_at,
        "emrImageDigest": input_data.emr_image_digest,
        "maxIds": 500,
        "maxBatches": 20,
        "batchSize": 25,
        "maxDurationSeconds": 3600,
        "minimumRequestIntervalSeconds": 1.0,
    }


def test_admin_parses_pinned_input_and_builds_stable_workflow_id() -> None:
    parsed = _eidr_input_from_mapping(_config())

    assert parsed.max_ids == 500
    assert parsed.silver_snapshot.object_version == "version-c"
    assert parsed.authorization_evidence.etag == "etag-e"
    assert eidr_public_research_workflow_id(parsed) == (
        eidr_public_research_workflow_id(_eidr_input_from_mapping(_config()))
    )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("maxIds", 4_096_001, "max_ids"),
        ("maxBatches", 4_097, "max_batches"),
        ("batchSize", 1_001, "batch_size"),
        ("maxDurationSeconds", 7 * 24 * 60 * 60 + 1, "max_duration_seconds"),
        (
            "minimumRequestIntervalSeconds",
            0.99,
            "minimum_request_interval_seconds",
        ),
    ],
)
def test_input_rejects_public_research_limit_expansion(
    field: str,
    value: int | float,
    message: str,
) -> None:
    config = _config()
    config[field] = value

    with pytest.raises(ValueError, match=message):
        _eidr_input_from_mapping(config)


def test_discovery_emr_args_pin_snapshot_and_glue_catalog() -> None:
    input_data = _input()
    parsed = _discovery_emr_namespace(
        input_data,
        workflow_run_id="temporal-run-1",
    )

    assert parsed.entry_point == EIDR_DISCOVERY_ENTRY_POINT
    assert parsed.entry_point_arguments[0] == "extract-wikidata-p2704"
    assert "--silver-snapshot-version" in parsed.entry_point_arguments
    assert "--silver-snapshot-etag" in parsed.entry_point_arguments
    assert (
        parsed.entry_point_arguments[
            parsed.entry_point_arguments.index("--catalog-type") + 1
        ]
        == "glue"
    )
    assert (
        parsed.entry_point_arguments[
            parsed.entry_point_arguments.index("--warehouse") + 1
        ]
        == input_data.env.warehouse_uri
    )
    assert parsed.executor_instances == input_data.env.emr_executor_instances
    assert parsed.shuffle_partitions == input_data.env.emr_shuffle_partitions


def test_discovery_rejects_emr_application_image_drift() -> None:
    input_data = _input()

    class EmrClient:
        def list_applications(self, **_request: Any) -> dict[str, Any]:
            return {
                "applications": [
                    {
                        "id": "app-1",
                        "name": input_data.env.emr_application_name,
                        "state": "STOPPED",
                    }
                ]
            }

        def get_application(self, **_request: Any) -> dict[str, Any]:
            return {
                "application": {
                    "imageConfiguration": {
                        "imageUri": (
                            "registry/video-media-catalog@sha256:" + ("0" * 64)
                        )
                    }
                }
            }

    with pytest.raises(RuntimeError, match="does not match emr_image_digest"):
        _verify_emr_application_image(EmrClient(), input_data)


def test_lookup_args_are_public_profile_without_watermark() -> None:
    input_data = _input()
    discovery = EidrDiscoveryResult(
        manifest_id="sha256:" + ("1" * 64),
        manifest=_ref(
            "s3://bucket/landing/research/eidr/manifest.json",
            DISCOVERED_ID_MANIFEST_MEDIA_TYPE,
            digest="1",
        ),
        id_count=2_000,
        page_count=1,
        job_run_id="job-discovery",
    )

    arguments = _lookup_arguments(input_data, discovery)

    assert arguments[0] == "lookup-public-research"
    assert "--watermark-uri" not in arguments
    assert arguments[arguments.index("--max-ids") + 1] == "250"
    assert arguments[arguments.index("--minimum-request-interval-seconds") + 1] == "1.0"


def test_workflow_runs_discovery_lookup_and_sequential_silver() -> None:
    asyncio.run(_workflow_case())


async def _workflow_case() -> None:
    calls: list[str] = []
    input_data = _input()
    discovery_result = EidrDiscoveryResult(
        manifest_id="sha256:" + ("1" * 64),
        manifest=_ref(
            "s3://bucket/landing/research/eidr/manifest.json",
            DISCOVERED_ID_MANIFEST_MEDIA_TYPE,
            digest="1",
        ),
        id_count=2,
        page_count=1,
        job_run_id="job-discovery",
    )
    captures = tuple(
        CaptureResult(
            kind="eidr",
            batch_id="sha256:" + (str(index) * 64),
            record_set_id="sha256:" + (str(index + 2) * 64),
            record_count=index,
            batch_manifest=_ref(
                f"s3://bucket/eidr/batch-{index}.json",
                _BATCH_MEDIA_TYPE,
                digest=str(index),
            ),
            record_set_manifest=_ref(
                f"s3://bucket/eidr/record-set-{index}.json",
                _RECORD_SET_MEDIA_TYPE,
                digest=str(index + 2),
            ),
        )
        for index in (1, 2)
    )
    lookup_result = EidrLookupResult(
        run_manifest_id="sha256:" + ("6" * 64),
        run_manifest=_ref(
            "s3://bucket/landing/research/eidr/run.json",
            BACKFILL_RUN_MANIFEST_MEDIA_TYPE,
            digest="6",
        ),
        completed=True,
        stop_reason="MANIFEST_EXHAUSTED",
        batch_count=2,
        attempted_id_count=2,
        found_count=2,
        not_found_count=0,
        attempt_count=2,
        retry_count=0,
        rate_limit_count=0,
        source_silver_inputs=captures,
    )

    @activity.defn(name="DiscoverEidrPublicResearchIds")
    async def discover(
        _input_data: EidrPublicResearchInput,
    ) -> EidrDiscoveryResult:
        calls.append("discovery")
        return discovery_result

    @activity.defn(name="LookupEidrPublicResearch")
    async def lookup(
        _input_data: EidrPublicResearchInput,
        _discovery: EidrDiscoveryResult,
    ) -> EidrLookupResult:
        calls.append("lookup")
        return lookup_result

    @activity.defn(name="SubmitSourceSilverBundle")
    async def silver_bundle(
        captures: tuple[CaptureResult, ...] | list[CaptureResult],
        _env: PipelineEnv,
        _committed_at: str | None = None,
    ) -> list[SilverResult]:
        calls.append(
            "silver-bundle:" + ",".join(capture.batch_id for capture in captures)
        )
        results: list[SilverResult] = []
        for index, capture in enumerate(captures, start=1):
            results.append(
                SilverResult(
                    batch_id=capture.batch_id,
                    run_id="sha256:" + (str(index + 6) * 64),
                    commit_key="sha256:" + (str(index + 7) * 64),
                    job_run_id="job-silver-bundle",
                    table_counts={"source_record": capture.record_count},
                    table_snapshot_ids={"source_record": index},
                )
            )
        return results

    @activity.defn(name="WriteEidrPublicResearchSummary")
    async def summary(
        _input_data: EidrPublicResearchInput,
        _discovery: EidrDiscoveryResult,
        _lookup: EidrLookupResult,
        silvers: tuple[SilverResult, ...],
    ) -> ObjectRefPayload:
        calls.append(f"summary:{len(silvers)}")
        payload = _summary_payload(
            _input_data,
            _discovery,
            _lookup,
            silvers,
        )
        assert payload["supplementalSourceRunIds"] == [item.run_id for item in silvers]
        assert payload["goldenBuildHandoff"]["field"] == ("supplementalSourceRunIds")
        return _ref(
            "s3://bucket/landing/research/summaries/eidr.json",
            EIDR_PUBLIC_RESEARCH_SUMMARY_MEDIA_TYPE,
            digest="9",
        )

    async with (
        await WorkflowEnvironment.start_time_skipping() as temporal,
        Worker(
            temporal.client,
            task_queue="eidr-tq",
            workflows=[EidrPublicResearchWorkflow],
            activities=[discover, lookup, summary],
        ),
        Worker(
            temporal.client,
            task_queue=SILVER_TASK_QUEUE,
            activities=[silver_bundle],
        ),
    ):
        result = await temporal.client.execute_workflow(
            EidrPublicResearchWorkflow.run,
            input_data,
            id="eidr-test-workflow",
            task_queue="eidr-tq",
            execution_timeout=timedelta(minutes=5),
        )

    assert calls == [
        "discovery",
        "lookup",
        "silver-bundle:" + ",".join(capture.batch_id for capture in captures),
        "summary:2",
    ]
    assert result.supplemental_source_run_ids == (
        "sha256:" + ("7" * 64),
        "sha256:" + ("8" * 64),
    )
    assert result.summary.media_type == EIDR_PUBLIC_RESEARCH_SUMMARY_MEDIA_TYPE
