from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import timedelta

import pytest
from botocore.exceptions import ClientError
from temporalio import activity
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from video_media_catalog.catalog_source_policy import load_catalog_source_policy
from video_media_catalog.gold_ingest import GOLD_RELEASE_COMMIT_MEDIA_TYPE
from video_media_catalog.gold_search_index import (
    GOLD_INDEX_BUILD_MANIFEST_MEDIA_TYPE,
    MAPPING_DIGEST,
    RESEARCH_INDEX_PREFIX,
    RESEARCH_READ_ALIAS,
    GoldIndexBuildManifest,
    gold_index_config_identity,
)
from video_media_catalog.gold_tables import GOLD_DATA_COLUMNS
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.temporal.activities import gold_index
from video_media_catalog.temporal.activities.gold_index import (
    _discover_optional_ref,
    _job_manifest,
    _KubernetesApi,
    _load_job_result,
    _validate_input,
    submit_gold_index_build_job,
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
        release_commit=_ref(
            "s3://bucket/control/release.json",
            media_type=GOLD_RELEASE_COMMIT_MEDIA_TYPE,
        ),
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
    assert environment["PYSPARK_SUBMIT_ARGS"] == ("--driver-memory 20g pyspark-shell")
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


def test_optional_result_only_swallows_s3_not_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_with(code: str, status: int):
        def fail(*_args: object, **_kwargs: object) -> ObjectRef:
            raise ClientError(
                {
                    "Error": {"Code": code, "Message": code},
                    "ResponseMetadata": {"HTTPStatusCode": status},
                },
                "HeadObject",
            )

        return fail

    monkeypatch.setattr(gold_index, "_discover_ref", fail_with("NoSuchKey", 404))
    assert (
        _discover_optional_ref(
            object(),  # type: ignore[arg-type]
            uri="s3://bucket/control/result.json",
            media_type="application/json",
        )
        is None
    )

    monkeypatch.setattr(gold_index, "_discover_ref", fail_with("AccessDenied", 403))
    with pytest.raises(ClientError):
        _discover_optional_ref(
            object(),  # type: ignore[arg-type]
            uri="s3://bucket/control/result.json",
            media_type="application/json",
        )


def _completed_build(
    input_data: GoldIndexProjectionInput,
) -> tuple[ObjectRef, ObjectRef, GoldIndexBuildManifest]:
    result_ref = ObjectRef(
        uri="s3://bucket/control/index-results/research-r15.json",
        format="OBJECT_FORMAT_JSON",
        media_type="application/vnd.video-media-catalog.gold-index-result.v1+json",
        checksum=Checksum(value="1" * 64),
        size_bytes=200,
        etag="result-etag",
        object_version="result-version",
    )
    build_ref = ObjectRef(
        uri="s3://bucket/control/index-builds/gold-index-build.json",
        format="OBJECT_FORMAT_JSON",
        media_type=GOLD_INDEX_BUILD_MANIFEST_MEDIA_TYPE,
        checksum=Checksum(value="2" * 64),
        size_bytes=300,
        etag="build-etag",
        object_version="build-version",
    )
    env = input_data.env
    config = gold_index_config_identity(
        read_alias=RESEARCH_READ_ALIAS,
        index_prefix=RESEARCH_INDEX_PREFIX,
        shards=env.shards,
        replicas=env.replicas,
        bulk_chunk_size=env.bulk_chunk_size,
        bulk_max_chunk_bytes=env.bulk_max_chunk_bytes,
        image_digest=env.builder_image.rpartition("@")[2],
    )
    release_commit = ObjectRef(
        uri=input_data.release_commit.uri,
        format=input_data.release_commit.format,
        media_type=GOLD_RELEASE_COMMIT_MEDIA_TYPE,
        checksum=Checksum(value=input_data.release_commit.checksum_value),
        size_bytes=input_data.release_commit.size_bytes,
        etag=input_data.release_commit.etag,
        object_version=input_data.release_commit.object_version,
    )
    build = GoldIndexBuildManifest(
        build_id="3" * 64,
        release_plan_id=input_data.expected_release_plan_id,
        release_commit=release_commit,
        table_snapshot_ids={table: 1 for table in GOLD_DATA_COLUMNS},
        mapping_digest=MAPPING_DIGEST,
        source_policy_digest=load_catalog_source_policy().digest,
        config_identity=config,
        config_digest=config.digest,
        document_count=1,
        gold_entity_count=1,
        successful_document_count=1,
        concrete_index_document_count=1,
        partition_receipt_count=1,
        partition_receipt_digest="sha256:" + ("4" * 64),
        index=RESEARCH_INDEX_PREFIX + "-" + ("3" * 24),
        alias=RESEARCH_READ_ALIAS,
        completed_at="2026-10-12T00:00:00Z",
    )
    return result_ref, build_ref, build


def test_existing_result_is_reused_only_when_all_pins_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_data = _input()
    result_ref, build_ref, build = _completed_build(input_data)

    monkeypatch.setattr(
        gold_index,
        "_discover_ref",
        lambda *_args, **_kwargs: result_ref,
    )

    def read_json(_store: object, reference: ObjectRef) -> dict[str, object]:
        if reference.uri == result_ref.uri:
            return {
                "activationStatus": "NOT_ACTIVATED",
                "manifest": build_ref.model_dump(
                    mode="json",
                    by_alias=True,
                    exclude_none=True,
                ),
            }
        assert reference.uri == build_ref.uri
        return build.model_dump(mode="json", by_alias=True, exclude_none=True)

    monkeypatch.setattr(gold_index, "_read_json", read_json)
    loaded = _load_job_result(
        input_data,
        object(),  # type: ignore[arg-type]
        job_name="research-index-reused",
        optional=False,
    )
    assert loaded is not None
    assert loaded.job_name == "research-index-reused"
    assert loaded.result_object.uri == result_ref.uri
    assert loaded.build_manifest.uri == build_ref.uri

    changed = replace(
        input_data,
        env=replace(input_data.env, shards=input_data.env.shards + 1),
    )
    with pytest.raises(ApplicationError, match="pinned index configuration"):
        _load_job_result(
            changed,
            object(),  # type: ignore[arg-type]
            job_name="research-index-reused",
            optional=False,
        )


def test_launcher_reuses_validated_result_before_kubernetes_job(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = GoldIndexJobResult(
        job_name="research-index-reused",
        result_object=_ref("s3://bucket/control/index-result.json"),
        build_manifest=_ref(
            "s3://bucket/control/index-build.json",
            media_type=GOLD_INDEX_BUILD_MANIFEST_MEDIA_TYPE,
        ),
    )
    observed: list[bool] = []

    monkeypatch.setattr(
        gold_index,
        "BoundedObjectStore",
        lambda *, region: object(),
    )

    def load(
        _input_data: GoldIndexProjectionInput,
        _store: object,
        *,
        job_name: str,
        optional: bool,
    ) -> GoldIndexJobResult:
        assert job_name.startswith("research-index-research-r15-")
        observed.append(optional)
        return expected

    monkeypatch.setattr(gold_index, "_load_job_result", load)

    def forbidden_api() -> _KubernetesApi:
        raise AssertionError("a validated existing result must not create a Job")

    monkeypatch.setattr(gold_index, "_KubernetesApi", forbidden_api)
    result = submit_gold_index_build_job(
        _input(),
        "2026-10-20T00:00:00Z",
    )
    assert result == expected
    assert observed == [True]


def test_gold_index_projection_workflow_never_promotes() -> None:
    asyncio.run(_workflow_case())


async def _workflow_case() -> None:
    calls: list[str] = []
    build_manifest = _ref(
        "s3://bucket/control/index-build.json",
        media_type=("application/vnd.video-media-catalog.gold-index-build.v2+json"),
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
