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
)
from video_media_catalog.temporal.models import ObjectRefPayload
from video_media_catalog.temporal.names import (
    GOLDEN_WORKFLOW_TASK_QUEUE,
    TEMPORAL_NAMESPACE,
)
from video_media_catalog.temporal.workflows.golden_build import GoldenBuildWorkflow


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
        build_executor_instances=int(values.get("buildExecutorInstances", 8)),
        build_shuffle_partitions=int(values.get("buildShufflePartitions", 432)),
        gold_emr_application_name=str(
            values.get("goldEmrApplicationName", values["emrApplicationName"])
        ),
        gold_emr_log_uri=str(values.get("goldEmrLogUri", values["emrLogUri"])),
        gold_executor_instances=int(values.get("goldExecutorInstances", 20)),
        gold_executor_cores=int(values.get("goldExecutorCores", 4)),
        gold_executor_memory=str(values.get("goldExecutorMemory", "24g")),
        gold_executor_memory_overhead=str(
            values.get("goldExecutorMemoryOverhead", "8g")
        ),
        gold_executor_disk=str(values.get("goldExecutorDisk", "170G")),
        gold_shuffle_partitions=int(values.get("goldShufflePartitions", 1152)),
    )


def _input_from_mapping(values: dict[str, Any]) -> GoldenBuildInput:
    override = values["freshnessOverride"]
    identity_reuse = values.get("identityReuse")
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
        identity_reuse=(
            None
            if identity_reuse is None
            else ObjectRefPayload.from_mapping(identity_reuse)
        ),
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
