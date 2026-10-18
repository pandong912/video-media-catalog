"""Idempotent administrative entrypoint for the research Golden Build."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import timedelta
from typing import Any

from temporalio.client import Client
from temporalio.common import WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError

from video_media_catalog.temporal.golden_models import (
    FreshnessOverrideInput,
    GoldenBuildEnv,
    GoldenBuildInput,
    GoldenIdentityMode,
    GoldenSourceFreshnessStatus,
    GoldenSourceRunState,
    GoldenSourceRunStatus,
)
from video_media_catalog.temporal.models import ObjectRefPayload
from video_media_catalog.temporal.names import (
    GOLDEN_WORKFLOW_TASK_QUEUE,
    TEMPORAL_NAMESPACE,
)
from video_media_catalog.temporal.workflows.golden_build import GoldenBuildWorkflow


def _mapping_bool(
    values: dict[str, Any],
    key: str,
    *,
    default: bool,
    label: str,
) -> bool:
    value = values.get(key, default)
    if not isinstance(value, bool):
        raise ValueError(f"{label}.{key} must be a boolean")
    return value


def _env_from_mapping(values: dict[str, Any]) -> GoldenBuildEnv:
    return GoldenBuildEnv(
        aws_region=str(values["awsRegion"]),
        warehouse_uri=str(values["warehouseUri"]),
        control_prefix=str(values["controlPrefix"]),
        gold_output_prefix=str(values["goldOutputPrefix"]),
        pipeline_summary_prefix=str(values["pipelineSummaryPrefix"]),
        emr_application_name=str(values["emrApplicationName"]),
        emr_execution_role_arn=str(values["emrExecutionRoleArn"]),
        emr_log_uri=str(values["emrLogUri"]),
        research_silver_entry_point=str(values["researchSilverEntryPoint"]),
        gold_entry_point=str(values["goldEntryPoint"]),
        image_digest=str(values["imageDigest"]),
        catalog_type=str(values.get("catalogType", "glue")),
        catalog_name=str(values.get("catalogName", "media")),
        glue_namespace=str(values.get("glueNamespace", "video_media_catalog")),
        control_executor_instances=int(values.get("controlExecutorInstances", 4)),
        control_shuffle_partitions=int(values.get("controlShufflePartitions", 96)),
        build_executor_instances=int(values.get("buildExecutorInstances", 7)),
        build_executor_cores=int(values.get("buildExecutorCores", 8)),
        build_executor_memory=str(values.get("buildExecutorMemory", "36g")),
        build_executor_memory_overhead=str(
            values.get("buildExecutorMemoryOverhead", "20g")
        ),
        build_executor_disk=str(values.get("buildExecutorDisk", "400G")),
        build_shuffle_partitions=int(values.get("buildShufflePartitions", 432)),
        gold_emr_application_name=str(
            values.get("goldEmrApplicationName", values["emrApplicationName"])
        ),
        gold_emr_log_uri=str(values.get("goldEmrLogUri", values["emrLogUri"])),
        gold_executor_instances=int(values.get("goldExecutorInstances", 20)),
        gold_executor_cores=int(values.get("goldExecutorCores", 4)),
        gold_executor_memory=str(values.get("goldExecutorMemory", "24g")),
        gold_executor_memory_overhead=str(
            values.get("goldExecutorMemoryOverhead", "6g")
        ),
        gold_executor_disk=str(values.get("goldExecutorDisk", "170G")),
        gold_shuffle_partitions=int(values.get("goldShufflePartitions", 1152)),
        gold_tmdb_freshness_slo_hours=int(values.get("goldTmdbFreshnessSloHours", 36)),
        gold_max_unresolved_identity_ratio=float(
            values.get("goldMaxUnresolvedIdentityRatio", 0.05)
        ),
        gold_quality_gate_enabled=_mapping_bool(
            values,
            "goldQualityGateEnabled",
            default=True,
            label="env",
        ),
        gold_quality_override_reason=str(values.get("goldQualityOverrideReason", "")),
    )


def _state_bool(values: dict[str, Any], key: str, *, default: bool) -> bool:
    return _mapping_bool(
        values,
        key,
        default=default,
        label="sourceRunStates",
    )


def _input_from_mapping(values: dict[str, Any]) -> GoldenBuildInput:
    override = values["freshnessOverride"]
    identity_reuse = values.get("identityReuse")
    source_run_states = []
    for raw in values.get("sourceRunStates", ()):
        capture_reference = raw.get("captureReference")
        materialization = _state_bool(
            raw,
            "materialization",
            default=False,
        )
        source_run_states.append(
            GoldenSourceRunState(
                source_product_id=str(raw["sourceProductId"]),
                run_ids=tuple(str(value) for value in raw.get("runIds", ())),
                run_status=GoldenSourceRunStatus(str(raw["runStatus"])),
                freshness_status=GoldenSourceFreshnessStatus(
                    str(raw["freshnessStatus"])
                ),
                watermark=str(raw.get("watermark", "")),
                acquired_at=(
                    None if raw.get("acquiredAt") is None else str(raw["acquiredAt"])
                ),
                identity_spine=_state_bool(
                    raw,
                    "identitySpine",
                    default=False,
                ),
                materialization=materialization,
                gold_contribution=_state_bool(
                    raw,
                    "goldContribution",
                    default=materialization,
                ),
                capture_reference=(
                    None
                    if capture_reference is None
                    else ObjectRefPayload.from_mapping(capture_reference)
                ),
            )
        )
    return GoldenBuildInput(
        build_id=str(values["buildId"]),
        identity_generation_id=str(values["identityGenerationId"]),
        baseline_snapshot=ObjectRefPayload.from_mapping(values["baselineSnapshot"]),
        tmdb_pipeline_summary=ObjectRefPayload.from_mapping(
            values["tmdbPipelineSummary"]
        ),
        baseline_source_run_ids=tuple(
            str(value) for value in values["baselineSourceRunIds"]
        ),
        baseline_identity_run_id=str(values["baselineIdentityRunId"]),
        baseline_identity_manifest=ObjectRefPayload.from_mapping(
            values["baselineIdentityManifest"]
        ),
        tmdb_source_run_ids=tuple(str(value) for value in values["tmdbSourceRunIds"]),
        source_watermarks={
            str(key): str(value) for key, value in values["sourceWatermarks"].items()
        },
        freshness_override=FreshnessOverrideInput(
            reason=str(override["reason"]),
            imdb_acquired_at=str(override["imdbAcquiredAt"]),
            tvmaze_acquired_at=str(override["tvmazeAcquiredAt"]),
            max_slo_hours=int(override.get("maxSloHours", 720)),
        ),
        env=_env_from_mapping(values["env"]),
        identity_mode=GoldenIdentityMode(
            str(values.get("identityMode", GoldenIdentityMode.INCREMENTAL.value))
        ),
        baseline_identity_source_run_ids=tuple(
            str(value) for value in values["baselineIdentitySourceRunIds"]
        ),
        supplemental_source_run_ids=tuple(
            str(value) for value in values.get("supplementalSourceRunIds", ())
        ),
        identity_reuse=(
            None
            if identity_reuse is None
            else ObjectRefPayload.from_mapping(identity_reuse)
        ),
        source_run_states=tuple(source_run_states),
    )


def _load_input(path: str) -> GoldenBuildInput:
    with open(path, encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError("Golden Build config must contain a JSON object")
    return _input_from_mapping(payload)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="video-media-catalog-golden-temporal-admin")
    parser.add_argument(
        "--temporal-address",
        default=os.environ.get(
            "TEMPORAL_ADDRESS",
            "temporal-frontend.temporal.svc.cluster.local:7233",
        ),
    )
    parser.add_argument(
        "--temporal-namespace",
        default=os.environ.get("TEMPORAL_NAMESPACE", TEMPORAL_NAMESPACE),
    )
    parser.add_argument(
        "--task-queue",
        default=os.environ.get(
            "TEMPORAL_GOLDEN_TASK_QUEUE",
            GOLDEN_WORKFLOW_TASK_QUEUE,
        ),
    )
    parser.add_argument(
        "--build-config-file",
        default=os.environ.get(
            "GOLDEN_BUILD_CONFIG_FILE",
            "/config/golden-build.json",
        ),
    )
    start = parser.add_subparsers(dest="command", required=True).add_parser(
        "start-build",
        help="Start the pinned Golden Build exactly once",
    )
    start.add_argument("--workflow-id")
    return parser


async def start_build(parsed: argparse.Namespace) -> dict[str, Any]:
    input_data = _load_input(parsed.build_config_file)
    workflow_id = parsed.workflow_id or f"golden-build-{input_data.build_id}"
    client = await Client.connect(
        parsed.temporal_address,
        namespace=parsed.temporal_namespace,
    )
    try:
        handle = await client.start_workflow(
            GoldenBuildWorkflow.run,
            input_data,
            id=workflow_id,
            task_queue=parsed.task_queue,
            execution_timeout=timedelta(hours=48),
            id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
        )
    except WorkflowAlreadyStartedError:
        existing = client.get_workflow_handle(workflow_id)
        return {
            "workflowId": workflow_id,
            "runId": existing.result_run_id,
            "started": False,
        }
    return {
        "workflowId": handle.id,
        "runId": handle.result_run_id,
        "started": True,
    }


def main(argv: list[str] | None = None) -> int:
    parsed = build_parser().parse_args(argv)
    result = asyncio.run(start_build(parsed))
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
