from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from temporalio.exceptions import ApplicationError
from test_catalog_execution_contracts import _input, _profile, _ref
from test_spark_backend import _request

from video_media_catalog.execution_contracts import (
    TransferPlanBinding,
    validate_execution_data_locality,
)
from video_media_catalog.execution_p4 import (
    AuditEvent,
    DeterministicFailureInjector,
    InjectedCatalogExecutionFailure,
    MetricSample,
    ObservedCostEvidence,
    ObservedResourceEvidence,
    SyntheticLocalPerformanceEvidence,
)
from video_media_catalog.spark_backend import (
    AwsEmrServerlessSparkBackend,
    SparkBackendIdentity,
    SparkBackendObservation,
    SparkReconcileResult,
)
from video_media_catalog.temporal.activities import catalog_execution
from video_media_catalog.temporal.catalog_execution_models import (
    PreparedCatalogExecution,
)
from video_media_catalog.temporal.catalog_execution_worker import (
    catalog_execution_health,
)
from video_media_catalog.temporal.catalog_execution_worker import main as worker_main

ROOT = Path(__file__).parents[1]
P4_LOCK = ROOT / "contracts/execution/catalog-execution-p4.runtime-lock.v1.json"
SYNTHETIC_EVIDENCE = (
    ROOT / "readiness/evidence/catalog-execution-p4-synthetic-local.v1.json"
)
RUNBOOK = ROOT / "docs/runbooks/catalog-execution-p4-operations.md"


class EvidenceAwsClient:
    def __init__(self) -> None:
        self.submit_calls = 0

    def start_job_run(self, **_request: Any) -> dict[str, str]:
        self.submit_calls += 1
        return {"jobRunId": "job-stable", "arn": "arn:aws:emr:job-stable"}

    def get_job_run(self, **_request: Any) -> dict[str, Any]:
        return {
            "jobRun": {
                "state": "SUCCESS",
                "updatedAt": datetime(2026, 10, 24, 0, 10, tzinfo=UTC),
                "totalExecutionDurationSeconds": 120,
                "totalResourceUtilization": {
                    "vCPUHour": "0.5",
                    "memoryGBHour": "2.5",
                    "storageGBHour": "1.25",
                    "inputBytes": 4_096,
                    "outputBytes": 2_048,
                    "shuffleReadBytes": 1_024,
                    "shuffleWriteBytes": 512,
                },
                "totalExecutionCost": {
                    "amount": "1.25",
                    "currency": "USD",
                    "rateCardDigest": "a" * 64,
                },
            }
        }

    def cancel_job_run(self, **_request: Any) -> dict[str, str]:
        return {"state": "CANCEL_REQUESTED"}


def test_provider_observation_binds_resource_and_cost_evidence() -> None:
    backend = AwsEmrServerlessSparkBackend(EvidenceAwsClient())
    identity = backend.submit(_request("AWS_EMR_SERVERLESS"))

    observation = backend.observe(identity)

    assert observation.resource_evidence is not None
    assert observation.resource_evidence.duration_milliseconds == 120_000
    assert observation.resource_evidence.vcpu_milliseconds == 1_800_000
    assert observation.resource_evidence.memory_gb_milliseconds == 9_000_000
    assert observation.resource_evidence.shuffle_read_bytes == 1_024
    assert observation.cost_evidence is not None
    assert observation.cost_evidence.status == "PROVIDER_REPORTED"
    assert observation.cost_evidence.amount_micros == 1_250_000
    assert observation.cost_evidence.currency == "USD"
    assert len(observation.evidence_digest) == 64


def test_partial_provider_resource_evidence_is_not_marked_complete() -> None:
    client = EvidenceAwsClient()
    client.get_job_run = lambda **_request: {  # type: ignore[method-assign]
        "jobRun": {
            "state": "RUNNING",
            "totalExecutionDurationSeconds": 0,
        }
    }
    backend = AwsEmrServerlessSparkBackend(client)
    identity = backend.submit(_request("AWS_EMR_SERVERLESS"))

    observation = backend.observe(identity)

    assert observation.resource_evidence is not None
    assert observation.resource_evidence.duration_milliseconds == 0
    assert observation.resource_evidence.complete is False
    assert observation.cost_evidence is not None
    assert observation.cost_evidence.status == "UNAVAILABLE"


def test_cost_and_resource_evidence_fail_closed_on_noncanonical_values() -> None:
    resource = ObservedResourceEvidence.create(
        scope="PROVIDER_OBSERVED",
        provider="AWS_EMR_SERVERLESS",
    )
    with pytest.raises(ValueError, match="three-letter uppercase"):
        ObservedCostEvidence.create(
            scope="PROVIDER_OBSERVED",
            provider="AWS_EMR_SERVERLESS",
            status="PROVIDER_REPORTED",
            resource_evidence_digest=resource.evidence_digest,
            amount_micros=1,
            currency="usd",
        )
    with pytest.raises(ValueError, match="PROVIDER_OBSERVED"):
        ObservedCostEvidence.create(
            scope="SYNTHETIC_LOCAL",
            provider="AWS_EMR_SERVERLESS",
            status="PROVIDER_REPORTED",
            resource_evidence_digest=resource.evidence_digest,
            amount_micros=1,
            currency="USD",
        )
    with pytest.raises(ValueError, match="status is not supported"):
        ObservedCostEvidence.create(
            scope="SYNTHETIC_LOCAL",
            provider="AWS_EMR_SERVERLESS",
            status="ESTIMATED",  # type: ignore[arg-type]
            resource_evidence_digest=resource.evidence_digest,
        )


def test_lost_submit_response_is_retried_and_orphan_is_adopted() -> None:
    client = EvidenceAwsClient()
    injector = DeterministicFailureInjector({"submit.after": 1})
    backend = AwsEmrServerlessSparkBackend(
        client,
        failure_injector=injector,
    )
    request = _request("AWS_EMR_SERVERLESS")

    with pytest.raises(InjectedCatalogExecutionFailure, match=r"submit\.after"):
        backend.submit(request)
    identity = backend.submit(request)
    reconciled = backend.reconcile(identity)

    assert client.submit_calls == 2
    assert identity.job_run_id == "job-stable"
    assert reconciled.disposition == "ADOPTED_TERMINAL"
    assert reconciled.observation.terminal


@pytest.mark.parametrize(
    "stage",
    (
        "observe.before",
        "observe.after",
        "cancel.before",
        "cancel.after",
        "reconcile.before",
        "reconcile.after",
    ),
)
def test_failure_injection_covers_provider_control_boundaries(stage: str) -> None:
    backend = AwsEmrServerlessSparkBackend(
        EvidenceAwsClient(),
        failure_injector=DeterministicFailureInjector({stage: 1}),
    )
    identity = SparkBackendIdentity(
        provider="AWS_EMR_SERVERLESS",
        application_id="app-1",
        job_run_id="job-stable",
    )

    def invoke() -> object:
        if stage.startswith("observe."):
            return backend.observe(identity)
        if stage.startswith("cancel."):
            return backend.cancel(identity, reason="synthetic operator cancel")
        return backend.reconcile(identity)

    with pytest.raises(InjectedCatalogExecutionFailure, match=stage):
        invoke()
    assert invoke() is not None


class HookCollector:
    def __init__(self) -> None:
        self.metrics: list[MetricSample] = []
        self.events: list[AuditEvent] = []

    def record_metric(self, sample: MetricSample) -> None:
        self.metrics.append(sample)

    def record_audit(self, event: AuditEvent) -> None:
        self.events.append(event)


class ResumeBackend:
    def submit(self, _request: object) -> SparkBackendIdentity:
        raise AssertionError("checkpointed submit must not create a second job")

    def reconcile(
        self,
        identity: SparkBackendIdentity,
    ) -> SparkReconcileResult:
        return SparkReconcileResult.create(
            SparkBackendObservation(
                identity=identity,
                provider_state="RUNNING",
                outcome="RUNNING",
            )
        )


def test_submit_activity_resumes_heartbeat_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepared = PreparedCatalogExecution(_input(), "stable-token", "stable-job")
    details = {
        "phase": "catalog-spark-submitted",
        "clientToken": "stable-token",
        "provider": "AWS_EMR_SERVERLESS",
        "applicationId": "app-1",
        "jobRunId": "job-1",
        "jobRunArn": "arn:aws:emr:job-1",
    }
    heartbeats: list[dict[str, object]] = []
    hooks = HookCollector()
    monkeypatch.setattr(
        catalog_execution, "_backend", lambda _prepared: ResumeBackend()
    )
    monkeypatch.setattr(catalog_execution, "_hooks", lambda: hooks)
    monkeypatch.setattr(catalog_execution.activity, "in_activity", lambda: True)
    monkeypatch.setattr(
        catalog_execution.activity,
        "info",
        lambda: SimpleNamespace(heartbeat_details=(details,)),
    )
    monkeypatch.setattr(
        catalog_execution.activity,
        "heartbeat",
        lambda payload: heartbeats.append(payload),
    )

    identity = catalog_execution.submit_catalog_spark(prepared)

    assert identity.job_run_id == "job-1"
    assert heartbeats[-1]["phase"] == "catalog-spark-submitted"
    assert [sample.name for sample in hooks.metrics] == [
        "catalog_execution_submit_resumed_total"
    ]
    assert hooks.events[-1].event_type == "SUBMIT_RESUMED"


def test_observe_activity_emits_metrics_audit_and_serializable_digests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepared = PreparedCatalogExecution(_input(), "stable-token", "stable-job")
    backend = AwsEmrServerlessSparkBackend(EvidenceAwsClient())
    identity = SparkBackendIdentity(
        provider="AWS_EMR_SERVERLESS",
        application_id="app-1",
        job_run_id="job-stable",
    )
    hooks = HookCollector()
    monkeypatch.setattr(catalog_execution, "_backend", lambda _prepared: backend)
    monkeypatch.setattr(catalog_execution, "_hooks", lambda: hooks)

    observation = catalog_execution.observe_catalog_spark(prepared, identity)

    assert observation.resource_evidence is None
    assert observation.cost_evidence is None
    assert len(observation.resource_evidence_digest) == 64
    assert len(observation.cost_evidence_digest) == 64
    names = {sample.name for sample in hooks.metrics}
    assert {
        "catalog_execution_observe_total",
        "catalog_execution_duration_milliseconds",
        "catalog_execution_input_bytes",
        "catalog_execution_output_bytes",
        "catalog_execution_shuffle_read_bytes",
        "catalog_execution_shuffle_write_bytes",
    } <= names
    assert hooks.events[-1].event_type == "OBSERVE"
    assert hooks.events[-1].evidence_digest == observation.evidence_digest


def test_p4_lock_and_health_remain_fail_closed(
    capsys: pytest.CaptureFixture[str],
) -> None:
    lock = json.loads(P4_LOCK.read_text())

    assert lock["status"] == "MANIFEST_SYNCED_PLANNED"
    assert lock["runtimeActivationAllowed"] is False
    assert lock["p4Manifest"]["present"] is True
    assert lock["p4Manifest"]["status"] == "PLANNED"
    assert lock["p4Manifest"]["claimsRuntimeActivation"] is False
    assert lock["p4Manifest"]["claimsProductionSlo"] is False
    assert lock["platformProvenance"]["p4ArtifactCommit"] is None
    assert lock["platformProvenance"]["p4CommitReceipt"] is None
    assert lock["activationReceipt"] is None
    assert lock["productionSloClaimAllowed"] is False
    with pytest.raises(ApplicationError, match="manifest is pinned"):
        catalog_execution.validate_catalog_execution_p4_runtime(lock)
    missing_manifest = {**lock, "p4Manifest": {"present": False}}
    with pytest.raises(ApplicationError, match="manifest is not pinned"):
        catalog_execution.validate_catalog_execution_p4_runtime(missing_manifest)
    tampered_manifest = {
        **lock,
        "p4Manifest": {
            **lock["p4Manifest"],
            "lockBodyDigest": "0" * 64,
        },
    }
    with pytest.raises(ApplicationError, match="manifest is not allowlisted"):
        catalog_execution.validate_catalog_execution_p4_runtime(tampered_manifest)

    health = catalog_execution_health()
    assert health.status == "NOT_READY"
    assert health.p4_manifest_present is True
    assert "P4_MANIFEST_MISSING" not in health.reasons
    assert "P4_COMMITTED_PROVENANCE_MISSING" in health.reasons
    assert "PRODUCTION_ACTIVATION_DISABLED" in health.reasons
    assert worker_main(["--health-check"]) == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "NOT_READY"
    assert payload["p4_manifest_present"] is True
    assert payload["production_activation_allowed"] is False


def test_synthetic_local_evidence_is_reproducible_and_not_an_slo() -> None:
    payload = json.loads(SYNTHETIC_EVIDENCE.read_text())
    evidence = SyntheticLocalPerformanceEvidence.create(
        file_sizes=(64, 128, 256, 512, 4_096, 8_192, 65_536, 1_048_576),
        small_file_threshold_bytes=1_024,
        synthetic_duration_milliseconds=1_000,
    )

    assert evidence.as_json() == payload
    assert payload["scope"] == "SYNTHETIC_LOCAL"
    assert payload["productionSloClaimed"] is False
    lock = json.loads(P4_LOCK.read_text())
    assert (
        lock["syntheticLocalEvidence"]["sha256"]
        == hashlib.sha256(SYNTHETIC_EVIDENCE.read_bytes()).hexdigest()
    )
    assert "productionSloClaimed=false" in RUNBOOK.read_text()


def test_cross_cloud_transfer_requires_exact_bound_source_closure() -> None:
    source = _ref("AWS_S3")
    profile = _profile("ALIYUN", transfer_only=True)
    transfer = TransferPlanBinding.create(
        transfer_plan_id="018f47c2-4a3b-7a10-8a11-000000000012",
        transfer_plan_digest="3" * 64,
        source_provider="AWS",
        destination_provider="ALIYUN",
        source_object_identity_digests=(source.identity_digest,),
        destination_prefix="oss://catalog-bucket/transfers/",
    )

    assert validate_execution_data_locality(
        profile=profile,
        input_objects=(source,),
        plan_kind="TRANSFER",
        transfer_plan=transfer,
    ) == (source,)

    wrong_source = source.model_copy(update={"identity_digest": "4" * 64})
    with pytest.raises(ValueError, match="does not bind"):
        validate_execution_data_locality(
            profile=profile,
            input_objects=(wrong_source,),
            plan_kind="TRANSFER",
            transfer_plan=transfer,
        )


def test_p4_runtime_preserves_iceberg_commit_last_authority() -> None:
    lock = json.loads(P4_LOCK.read_text())
    assert lock["authorityBoundary"] == {
        "icebergCommitLastPreserved": True,
        "workflowMayCommitIceberg": False,
        "backendMayPublishCatalogRelease": False,
        "catalogMayWritePlatformDatabase": False,
        "platformMayWriteCatalogDatabase": False,
    }

    for relative in (
        "src/video_media_catalog/execution_p4.py",
        "src/video_media_catalog/spark_backend.py",
        "src/video_media_catalog/temporal/activities/catalog_execution.py",
        "src/video_media_catalog/temporal/workflows/catalog_execution.py",
    ):
        source = (ROOT / relative).read_text()
        assert "from video_media_catalog.iceberg" not in source
        assert "import video_media_catalog.iceberg" not in source
