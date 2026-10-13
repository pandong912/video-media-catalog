from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import timedelta

import pytest
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from video_media_catalog.gold_index_gc import (
    GOLD_INDEX_ARCHIVE_RECEIPT_MEDIA_TYPE,
    GOLD_INDEX_GC_RESULT_MEDIA_TYPE,
    GOLD_INDEX_RESTORE_RECEIPT_MEDIA_TYPE,
)
from video_media_catalog.gold_search_index import (
    GOLD_INDEX_ACTIVATION_RECEIPT_MEDIA_TYPE,
)
from video_media_catalog.temporal.activities.gold_index import _KubernetesApi
from video_media_catalog.temporal.activities.gold_index_gc import (
    _CONFLICTING_JOB_WORKLOADS,
    _gc_job_manifest,
    _launch,
    _restore_job_manifest,
)
from video_media_catalog.temporal.gold_index_admin_cli import (
    _gc_input_from_mapping,
    _restore_input_from_mapping,
)
from video_media_catalog.temporal.gold_index_models import (
    GoldIndexGcEnv,
    GoldIndexGcInput,
    GoldIndexRestoreInput,
    GoldIndexRuntimeJobResult,
)
from video_media_catalog.temporal.models import ObjectRefPayload
from video_media_catalog.temporal.names import (
    GOLD_INDEX_LAUNCHER_TASK_QUEUE,
    GOLD_INDEX_VALIDATOR_TASK_QUEUE,
)
from video_media_catalog.temporal.workflows.gold_index_gc import (
    GoldIndexGcWorkflow,
    GoldIndexRestoreWorkflow,
)

ACTIVE = "media-catalog-research-" + ("a" * 24)
OLD = "media-catalog-research-" + ("b" * 24)


def _ref(uri: str, media_type: str) -> ObjectRefPayload:
    return ObjectRefPayload(
        uri=uri,
        format="OBJECT_FORMAT_JSON",
        media_type=media_type,
        checksum_algorithm="CHECKSUM_ALGORITHM_SHA256",
        checksum_value="c" * 64,
        size_bytes=100,
        etag="etag",
        object_version="version",
    )


def _env() -> GoldIndexGcEnv:
    return GoldIndexGcEnv(
        aws_region="us-east-1",
        kubernetes_namespace="video-world-research",
        gc_service_account="research-index-gc",
        gc_image="registry/catalog@sha256:" + ("d" * 64),
        archive_prefix="s3://bucket/control/index-archives",
        gc_result_prefix="s3://bucket/control/index-gc-results",
        restore_prefix="s3://bucket/control/index-restores",
        opensearch_endpoint="https://search.example.com",
        node_selector={"workload": "batch"},
    )


def _gc_input() -> GoldIndexGcInput:
    return GoldIndexGcInput(
        operation_id="legacy-958",
        old_index=OLD,
        active_index=ACTIVE,
        repository="research-archive",
        snapshot="legacy-958",
        delete_before_epoch_millis=50,
        activation_receipt=_ref(
            "s3://bucket/control/index-activations/activation.json",
            GOLD_INDEX_ACTIVATION_RECEIPT_MEDIA_TYPE,
        ),
        referenced_indices=(),
        env=_env(),
    )


def _restore_input() -> GoldIndexRestoreInput:
    return GoldIndexRestoreInput(
        operation_id="restore-958",
        archive_receipt=_ref(
            "s3://bucket/control/index-archives/archive.json",
            GOLD_INDEX_ARCHIVE_RECEIPT_MEDIA_TYPE,
        ),
        env=_env(),
    )


def test_gc_and_restore_jobs_use_gc_identity_and_exact_cli_args() -> None:
    gc_manifest = _gc_job_manifest(
        _gc_input(),
        completed_at="2026-10-13T00:00:00Z",
    )
    restore_manifest = _restore_job_manifest(
        _restore_input(),
        completed_at="2026-10-13T01:00:00Z",
    )

    gc_pod = gc_manifest["spec"]["template"]["spec"]
    gc_container = gc_pod["containers"][0]
    gc_args = gc_container["args"]
    assert len(gc_manifest["metadata"]["name"]) <= 63
    assert gc_pod["serviceAccountName"] == "research-index-gc"
    assert gc_container["image"].endswith("@sha256:" + ("d" * 64))
    assert gc_container["command"] == ["video-media-catalog-gold-index-gc"]
    assert gc_args[0] == "archive-delete"
    assert "--execute" in gc_args
    assert gc_args[gc_args.index("--old-index") + 1] == OLD
    assert gc_args[gc_args.index("--active-index") + 1] == ACTIVE
    assert "--activation-receipt-version" in gc_args
    assert "--activation-receipt-etag" in gc_args
    assert gc_container["securityContext"]["readOnlyRootFilesystem"] is True
    assert gc_manifest["metadata"]["labels"]["app.kubernetes.io/component"] == "gc"

    restore_pod = restore_manifest["spec"]["template"]["spec"]
    restore_container = restore_pod["containers"][0]
    restore_args = restore_container["args"]
    assert len(restore_manifest["metadata"]["name"]) <= 63
    assert restore_pod["serviceAccountName"] == "research-index-gc"
    assert restore_args[0] == "restore-drill"
    assert "--archive-receipt-version" in restore_args
    assert "--archive-receipt-etag" in restore_args
    assert not any("rename" in value for value in restore_args)
    assert (
        restore_manifest["metadata"]["labels"]["app.kubernetes.io/component"]
        == "restore"
    )
    assert (
        len(
            _gc_job_manifest(
                replace(_gc_input(), operation_id="a" * 63),
                completed_at="2026-10-13T00:00:00Z",
            )["metadata"]["name"]
        )
        <= 63
    )


def test_temporal_admin_json_contracts_parse_gc_and_restore() -> None:
    env = {
        "awsRegion": "us-east-1",
        "kubernetesNamespace": "video-world-research",
        "gcServiceAccount": "research-index-gc",
        "gcImage": "registry/catalog@sha256:" + ("d" * 64),
        "archivePrefix": "s3://bucket/control/index-archives",
        "gcResultPrefix": "s3://bucket/control/index-gc-results",
        "restorePrefix": "s3://bucket/control/index-restores",
        "opensearchEndpoint": "https://search.example.com",
    }
    archive = {
        "uri": "s3://bucket/control/index-archives/archive.json",
        "format": "OBJECT_FORMAT_JSON",
        "mediaType": GOLD_INDEX_ARCHIVE_RECEIPT_MEDIA_TYPE,
        "checksum": {
            "algorithm": "CHECKSUM_ALGORITHM_SHA256",
            "value": "c" * 64,
        },
        "sizeBytes": "100",
        "etag": "etag",
        "objectVersion": "version",
    }
    gc = _gc_input_from_mapping(
        {
            "operationId": "legacy-958",
            "oldIndex": OLD,
            "activeIndex": ACTIVE,
            "repository": "research-archive",
            "snapshot": "legacy-958",
            "deleteBeforeEpochMillis": 50,
            "activationReceipt": {
                **archive,
                "uri": ("s3://bucket/control/index-activations/activation.json"),
                "mediaType": GOLD_INDEX_ACTIVATION_RECEIPT_MEDIA_TYPE,
            },
            "referencedIndices": [],
            "reuseArchiveReceipt": archive,
            "env": env,
        }
    )
    restore = _restore_input_from_mapping(
        {
            "operationId": "restore-958",
            "archiveReceipt": archive,
            "env": env,
        }
    )

    assert gc.reuse_archive_receipt is not None
    assert gc.reuse_archive_receipt.object_version == "version"
    assert gc.activation_receipt.media_type == GOLD_INDEX_ACTIVATION_RECEIPT_MEDIA_TYPE
    assert restore.archive_receipt.etag == "etag"
    assert restore.env.gc_service_account == "research-index-gc"


@pytest.mark.parametrize(
    ("name", "component"),
    [
        ("research-gold-index", "builder"),
        ("catalog-gold-match", "matcher"),
        ("research-gold-index", "gc"),
    ],
)
def test_gc_preflight_rejects_active_conflicting_jobs(
    name: str,
    component: str,
) -> None:
    api = object.__new__(_KubernetesApi)
    api.request = lambda method, path: (  # type: ignore[method-assign]
        200,
        {
            "items": [
                {
                    "metadata": {
                        "name": f"active-{component}",
                        "labels": {
                            "app.kubernetes.io/name": name,
                            "app.kubernetes.io/component": component,
                        },
                    },
                    "status": {"active": 1},
                }
            ]
        },
    )

    with pytest.raises(RuntimeError, match="conflicting Kubernetes Jobs"):
        api.assert_no_conflicting_jobs(
            namespace="video-world-research",
            expected_name="expected-job",
            expected_workload=("research-gold-index", "gc"),
            blocked_workloads=_CONFLICTING_JOB_WORKLOADS,
        )


def test_runtime_job_preflight_runs_before_gc_and_restore_create(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []

    class _Api:
        def assert_no_conflicting_jobs(self, **values) -> None:
            events.append("preflight")
            assert (
                "research-gold-index",
                "builder",
            ) in values["blocked_workloads"]
            assert (
                "catalog-gold-match",
                "matcher",
            ) in values["blocked_workloads"]
            assert ("research-gold-index", "gc") in values["blocked_workloads"]
            assert values["expected_workload"][1] in {"gc", "restore"}

        def create_or_get_job(self, _manifest) -> None:
            events.append("create")

    import video_media_catalog.temporal.activities.gold_index_gc as activities

    monkeypatch.setattr(activities, "_KubernetesApi", _Api)
    monkeypatch.setattr(
        activities,
        "_wait_for_job",
        lambda *_args, **_kwargs: events.append("wait"),
    )
    manifests = (
        _gc_job_manifest(
            _gc_input(),
            completed_at="2026-10-13T00:00:00Z",
        ),
        _restore_job_manifest(
            _restore_input(),
            completed_at="2026-10-13T01:00:00Z",
        ),
    )
    for manifest in manifests:
        _launch(
            manifest=manifest,
            namespace="video-world-research",
        )

    assert events == [
        "preflight",
        "create",
        "wait",
        "preflight",
        "create",
        "wait",
    ]


def test_gc_preflight_allows_terminal_and_same_idempotent_job() -> None:
    api = object.__new__(_KubernetesApi)
    labels = {
        "app.kubernetes.io/name": "research-gold-index",
        "app.kubernetes.io/component": "gc",
    }
    api.request = lambda method, path: (  # type: ignore[method-assign]
        200,
        {
            "items": [
                {
                    "metadata": {"name": "completed-gc", "labels": labels},
                    "status": {"conditions": [{"type": "Complete", "status": "True"}]},
                },
                {
                    "metadata": {"name": "expected-gc", "labels": labels},
                    "status": {"active": 1},
                },
            ]
        },
    )

    api.assert_no_conflicting_jobs(
        namespace="video-world-research",
        expected_name="expected-gc",
        expected_workload=("research-gold-index", "gc"),
        blocked_workloads=_CONFLICTING_JOB_WORKLOADS,
    )


def test_gc_and_restore_workflows_use_launcher_then_validator() -> None:
    asyncio.run(_workflow_case())


async def _workflow_case() -> None:
    calls: list[str] = []
    gc_receipt = _ref(
        "s3://bucket/control/index-gc-results/gc.json",
        GOLD_INDEX_GC_RESULT_MEDIA_TYPE,
    )
    restore_receipt = _ref(
        "s3://bucket/control/index-restores/restore.json",
        GOLD_INDEX_RESTORE_RECEIPT_MEDIA_TYPE,
    )

    @activity.defn(name="SubmitGoldIndexGcJob")
    async def submit_gc(
        _input_data: GoldIndexGcInput,
        _completed_at: str,
    ) -> GoldIndexRuntimeJobResult:
        calls.append("submit-gc")
        return GoldIndexRuntimeJobResult("gc-job", gc_receipt)

    @activity.defn(name="ValidateGoldIndexGc")
    async def validate_gc(
        _input_data: GoldIndexGcInput,
        job: GoldIndexRuntimeJobResult,
    ) -> ObjectRefPayload:
        calls.append("validate-gc")
        return job.receipt

    @activity.defn(name="SubmitGoldIndexRestoreJob")
    async def submit_restore(
        _input_data: GoldIndexRestoreInput,
        _completed_at: str,
    ) -> GoldIndexRuntimeJobResult:
        calls.append("submit-restore")
        return GoldIndexRuntimeJobResult("restore-job", restore_receipt)

    @activity.defn(name="ValidateGoldIndexRestore")
    async def validate_restore(
        _input_data: GoldIndexRestoreInput,
        job: GoldIndexRuntimeJobResult,
    ) -> ObjectRefPayload:
        calls.append("validate-restore")
        return job.receipt

    async with (
        await WorkflowEnvironment.start_time_skipping() as temporal,
        Worker(
            temporal.client,
            task_queue="gold-index-gc-test",
            workflows=[GoldIndexGcWorkflow, GoldIndexRestoreWorkflow],
        ),
        Worker(
            temporal.client,
            task_queue=GOLD_INDEX_LAUNCHER_TASK_QUEUE,
            activities=[submit_gc, submit_restore],
        ),
        Worker(
            temporal.client,
            task_queue=GOLD_INDEX_VALIDATOR_TASK_QUEUE,
            activities=[validate_gc, validate_restore],
        ),
    ):
        gc_result = await temporal.client.execute_workflow(
            GoldIndexGcWorkflow.run,
            _gc_input(),
            id="gold-index-gc-test",
            task_queue="gold-index-gc-test",
            execution_timeout=timedelta(minutes=5),
        )
        restore_result = await temporal.client.execute_workflow(
            GoldIndexRestoreWorkflow.run,
            _restore_input(),
            id="gold-index-restore-test",
            task_queue="gold-index-gc-test",
            execution_timeout=timedelta(minutes=5),
        )

    assert calls == [
        "submit-gc",
        "validate-gc",
        "submit-restore",
        "validate-restore",
    ]
    assert gc_result.gc_result_receipt == gc_receipt
    assert restore_result.restore_receipt == restore_receipt
