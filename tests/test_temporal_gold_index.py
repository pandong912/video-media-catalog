from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import timedelta

import pytest
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from video_media_catalog.temporal.activities.gold_index import (
    _job_manifest,
    _KubernetesApi,
    _validate_input,
)
from video_media_catalog.temporal.gold_index_models import (
    GoldIndexJobResult,
    GoldIndexProjectionEnv,
    GoldIndexProjectionInput,
)
from video_media_catalog.temporal.models import ObjectRefPayload
from video_media_catalog.temporal.names import (
    GOLD_INDEX_LAUNCHER_TASK_QUEUE,
    GOLD_INDEX_VALIDATOR_TASK_QUEUE,
)
from video_media_catalog.temporal.workflows.gold_index_projection import (
    GoldIndexProjectionWorkflow,
)


def _ref(uri: str, *, media_type: str = "application/json") -> ObjectRefPayload:
    return ObjectRefPayload(
        uri=uri,
        format="OBJECT_FORMAT_JSON",
        media_type=media_type,
        checksum_algorithm="CHECKSUM_ALGORITHM_SHA256",
        checksum_value="a" * 64,
        size_bytes=100,
        etag="etag",
        object_version="version",
    )


def _input() -> GoldIndexProjectionInput:
    return GoldIndexProjectionInput(
        projection_id="research-r15",
        expected_release_plan_id="sha256:" + ("b" * 64),
        release_commit=_ref("s3://bucket/control/release.json"),
        env=GoldIndexProjectionEnv(
            aws_region="us-east-1",
            kubernetes_namespace="video-world-research",
            builder_service_account="research-index-builder",
            builder_image="registry/catalog@sha256:" + ("c" * 64),
            warehouse_uri="s3://bucket/warehouse",
            manifest_prefix="s3://bucket/control/index-builds",
            result_prefix="s3://bucket/control/index-results",
            validation_prefix="s3://bucket/control/index-validations",
            opensearch_endpoint="https://search.example.com",
            node_selector={"workload": "batch"},
        ),
    )


def test_gold_index_job_is_pinned_build_only_and_hardened() -> None:
    input_data = _input()
    _validate_input(input_data)

    manifest = _job_manifest(
        input_data,
        completed_at="2026-10-12T00:00:00Z",
    )
    pod = manifest["spec"]["template"]["spec"]
    container = pod["containers"][0]
    arguments = container["args"]
    environment = {item["name"]: item["value"] for item in container["env"]}

    assert len(manifest["metadata"]["name"]) <= 63
    assert pod["serviceAccountName"] == "research-index-builder"
    assert pod["automountServiceAccountToken"] is True
    assert container["image"].endswith("@" + ("sha256:" + ("c" * 64)))
    assert pod["securityContext"]["runAsUser"] == 10001
    assert pod["securityContext"]["runAsGroup"] == 10001
    assert pod["securityContext"]["fsGroup"] == 10001
    assert container["securityContext"]["runAsUser"] == 10001
    assert container["securityContext"]["readOnlyRootFilesystem"] is True
    assert container["workingDir"] == "/work"
    assert container["terminationMessagePolicy"] == "FallbackToLogsOnError"
    assert environment["SPARK_LOCAL_DIRS"] == "/work/spark"
    assert environment["PYSPARK_SUBMIT_ARGS"] == (
        "--driver-memory 20g pyspark-shell"
    )
    assert environment["TMPDIR"] == "/work/tmp"
    assert environment["HOME"] == "/work"
    assert {mount["mountPath"] for mount in container["volumeMounts"]} == {
        "/work",
        "/work/spark",
        "/work/tmp",
        "/tmp",
    }
    assert {volume["name"] for volume in pod["volumes"]} == {
        "work",
        "spark-local",
        "tmp",
    }
    assert "--result-uri" in arguments
    assert "--expected-old-index" not in arguments
    assert "promote" not in " ".join(arguments).lower()
    assert manifest["metadata"]["annotations"]["video-world.io/release-plan-id"] == (
        input_data.expected_release_plan_id
    )


def test_gold_index_job_rejects_mutable_image() -> None:
    input_data = _input()
    with pytest.raises(ValueError, match="pinned"):
        _validate_input(
            replace(
                input_data,
                env=replace(input_data.env, builder_image="registry/catalog:latest"),
            )
        )


def test_gold_index_launcher_rejects_a_second_active_builder() -> None:
    api = object.__new__(_KubernetesApi)
    api.request = lambda method, path: (  # type: ignore[method-assign]
        200,
        {
            "items": [
                {
                    "metadata": {"name": "research-index-other"},
                    "status": {"active": 1},
                }
            ]
        },
    )
    with pytest.raises(RuntimeError, match="another Research Gold index build"):
        api.assert_no_other_active_builder(
            namespace="video-world-research",
            expected_name="research-index-current",
        )


def test_gold_index_projection_workflow_never_promotes() -> None:
    asyncio.run(_workflow_case())


async def _workflow_case() -> None:
    calls: list[str] = []
    build_manifest = _ref(
        "s3://bucket/control/index-build.json",
        media_type=(
            "application/vnd.video-media-catalog.gold-index-build.v2+json"
        ),
    )
    validation = _ref(
        "s3://bucket/control/index-validation.json",
        media_type=(
            "application/vnd.video-media-catalog.gold-index-validation.v2+json"
        ),
    )

    @activity.defn(name="SubmitGoldIndexBuildJob")
    async def submit(
        _input_data: GoldIndexProjectionInput,
        _completed_at: str,
    ) -> GoldIndexJobResult:
        calls.append("build")
        return GoldIndexJobResult(
            job_name="research-index-r15",
            result_object=_ref("s3://bucket/control/index-result.json"),
            build_manifest=build_manifest,
        )

    @activity.defn(name="ValidateGoldIndexBuild")
    async def validate(
        _input_data: GoldIndexProjectionInput,
        _job_result: GoldIndexJobResult,
        _validated_at: str,
    ) -> ObjectRefPayload:
        calls.append("validate")
        return validation

    async with (
        await WorkflowEnvironment.start_time_skipping() as env,
        Worker(
            env.client,
            task_queue="gold-index-test",
            workflows=[GoldIndexProjectionWorkflow],
        ),
        Worker(
            env.client,
            task_queue=GOLD_INDEX_LAUNCHER_TASK_QUEUE,
            activities=[submit],
        ),
        Worker(
            env.client,
            task_queue=GOLD_INDEX_VALIDATOR_TASK_QUEUE,
            activities=[validate],
        ),
    ):
        result = await env.client.execute_workflow(
            GoldIndexProjectionWorkflow.run,
            _input(),
            id="gold-index-test",
            task_queue="gold-index-test",
            execution_timeout=timedelta(minutes=5),
        )

    assert calls == ["build", "validate"]
    assert result.build_manifest == build_manifest
    assert result.validation_receipt == validation
    assert result.activation_status == "DISABLED"
