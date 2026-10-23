from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError
from temporalio.exceptions import ApplicationError

from video_media_catalog.execution_contracts import (
    CatalogBackendCompletionReceipt,
    ExternalAuthorityResult,
    HumanDecisionResult,
    ObjectRefV2,
    SparkProviderProfile,
    TransferPlanBinding,
    WorkloadIdentityBinding,
    validate_execution_data_locality,
)
from video_media_catalog.temporal.activities.catalog_execution import (
    validate_catalog_execution_runtime,
)
from video_media_catalog.temporal.catalog_execution_models import (
    CatalogExecutionInput,
    CompletionReceiptTarget,
    ObjectRefV2Payload,
    SparkJobSpec,
    SparkProviderProfilePayload,
    catalog_execution_client_token,
    catalog_execution_workflow_id,
)

ROOT = Path(__file__).parents[1]
LOCK = ROOT / "contracts/execution/catalog-execution-p3.runtime-lock.v1.json"

TENANT = "018f47c2-4a3b-7a10-8a11-000000000001"
PROJECT = "018f47c2-4a3b-7a10-8a11-000000000002"
BINDING = "018f47c2-4a3b-7a10-8a11-000000000003"
PLANE = "018f47c2-4a3b-7a10-8a11-000000000004"
PLAN = "018f47c2-4a3b-7a10-8a11-000000000007"
PARTITION = "018f47c2-4a3b-7a10-8a11-000000000008"
ATTEMPT = "018f47c2-4a3b-7a10-8a11-000000000009"
P3_ARTIFACT_LOCK_DIGEST = (
    "3b2135ee9ec4102bcbb2a035d1375d14615be904f247e7e3b1ac941c11f467a3"
)


def _digest(value: str) -> str:
    return value * 64


def _ref(provider: str = "AWS_S3") -> ObjectRefV2:
    scheme = "s3" if provider == "AWS_S3" else "oss"
    return ObjectRefV2.create(
        object_ref_id="018f47c2-4a3b-7a10-8a11-000000000010",
        tenant_id=TENANT,
        project_id=PROJECT,
        data_plane_binding_id=BINDING,
        data_plane_id=PLANE,
        provider=provider,  # type: ignore[arg-type]
        uri=f"{scheme}://catalog-bucket/execution/input.json",
        object_key="execution/input.json",
        format="JSON",
        media_type="application/json",
        checksum=_digest("a"),
        size_bytes=128,
        etag="exact-etag",
        version_token="exact-version-1",
        schema_digest=_digest("b"),
        encryption_context_digest=_digest("c"),
        created_at="2026-10-23T00:00:00Z",
    )


def _profile(
    cloud: str = "AWS",
    *,
    transfer_only: bool = False,
) -> SparkProviderProfile:
    aws = cloud == "AWS"
    identity = WorkloadIdentityBinding.create(
        cloud_provider=cloud,  # type: ignore[arg-type]
        identity_reference=(
            "arn:aws:iam::123456789012:role/catalog-execution"
            if aws
            else "acs:ram::1234567890123456:role/catalog-execution"
        ),
        subject_digest=_digest("d"),
        audience="catalog-spark-runtime",
    )
    return SparkProviderProfile.create(
        profile_key=(
            "aws-emr-transfer"
            if transfer_only and aws
            else "aliyun-emr-transfer"
            if transfer_only
            else "aws-emr"
            if aws
            else "aliyun-emr"
        ),
        cloud_provider=cloud,  # type: ignore[arg-type]
        object_provider="AWS_S3" if aws else "ALIBABA_OSS",
        backend_kind=("AWS_EMR_SERVERLESS" if aws else "ALIYUN_EMR_SERVERLESS_SPARK"),
        region="us-east-1" if aws else "cn-hangzhou",
        application_id="catalog-runtime",
        workload_identity=identity,
        max_input_bytes=1_024,
        max_output_bytes=1_024,
        transfer_only=transfer_only,
    )


def _input(profile: SparkProviderProfile | None = None) -> CatalogExecutionInput:
    current = profile or _profile()
    return CatalogExecutionInput(
        contract_version="video.media-catalog.execution.p3.v1",
        platform_namespace="video-world-platform",
        platform_workflow_id=f"vw-execution/{TENANT}/{PLAN}",
        platform_contract_digest=P3_ARTIFACT_LOCK_DIGEST,
        tenant_id=TENANT,
        project_id=PROJECT,
        execution_plan_id=PLAN,
        plan_revision=1,
        plan_digest=_digest("f"),
        execution_partition_id=PARTITION,
        execution_attempt_id=ATTEMPT,
        plan_kind="EXECUTION",
        provider_profile=SparkProviderProfilePayload.from_contract(current),
        input_objects=(ObjectRefV2Payload.from_contract(_ref()),),
        transfer_plan=None,
        job_spec=SparkJobSpec(
            entry_point="local:///opt/catalog/job.py",
            entry_point_arguments=(
                "--input",
                "s3://catalog-bucket/execution/input.json",
            ),
            spark_submit_parameters="--conf spark.dynamicAllocation.enabled=false",
            log_uri="s3://catalog-bucket/logs/",
        ),
        completion_receipt_target=CompletionReceiptTarget(
            object_ref_id="018f47c2-4a3b-7a10-8a11-000000000011",
            data_plane_binding_id=BINDING,
            data_plane_id=PLANE,
            uri="s3://catalog-bucket/execution/receipt.json",
            object_key="execution/receipt.json",
            schema_digest=_digest("1"),
            encryption_context_digest=_digest("2"),
        ),
        expected_result_kind="EXTERNAL_AUTHORITY",
    )


def test_provider_profiles_bind_workload_identity_and_object_provider() -> None:
    aws = _profile()
    aliyun = _profile("ALIYUN")

    assert (
        SparkProviderProfile.model_validate(aws.model_dump(mode="json", by_alias=True))
        == aws
    )
    assert aliyun.object_provider == "ALIBABA_OSS"
    assert aliyun.backend_kind == "ALIYUN_EMR_SERVERLESS_SPARK"

    payload = aws.model_dump(mode="json", by_alias=True)
    payload["objectProvider"] = "ALIBABA_OSS"
    with pytest.raises(ValidationError, match="must agree"):
        SparkProviderProfile.model_validate(payload)


def test_cross_cloud_requires_exact_explicit_transfer_plan() -> None:
    source = _ref("AWS_S3")
    target_profile = _profile("ALIYUN", transfer_only=True)
    transfer = TransferPlanBinding.create(
        transfer_plan_id="018f47c2-4a3b-7a10-8a11-000000000012",
        transfer_plan_digest=_digest("3"),
        source_provider="AWS",
        destination_provider="ALIYUN",
        source_object_identity_digests=(source.identity_digest,),
        destination_prefix="oss://catalog-bucket/transfers/",
    )

    assert validate_execution_data_locality(
        profile=target_profile,
        input_objects=(source,),
        plan_kind="TRANSFER",
        transfer_plan=transfer,
    ) == (source,)
    with pytest.raises(ValueError, match="explicit TRANSFER"):
        validate_execution_data_locality(
            profile=_profile("ALIYUN"),
            input_objects=(source,),
            plan_kind="EXECUTION",
            transfer_plan=None,
        )
    with pytest.raises(ValueError, match="requires a transfer"):
        validate_execution_data_locality(
            profile=target_profile,
            input_objects=(source,),
            plan_kind="TRANSFER",
            transfer_plan=None,
        )


def test_external_completion_receipt_cannot_forge_world_staged() -> None:
    receipt = CatalogBackendCompletionReceipt.create(
        tenant_id=TENANT,
        project_id=PROJECT,
        execution_plan_id=PLAN,
        execution_partition_id=PARTITION,
        execution_attempt_id=ATTEMPT,
        provider_profile_digest=_profile().profile_digest,
        backend_kind="AWS_EMR_SERVERLESS",
        backend_application_id="app",
        backend_job_id="job",
        terminal_state="SUCCESS",
        authority_result_digest=_digest("4"),
        summary_digest=_digest("5"),
        observed_at="2026-10-23T00:10:00Z",
    )
    payload = json.loads(receipt.json_bytes())
    payload["resultKind"] = "WORLD_STAGED"

    with pytest.raises(ValidationError):
        CatalogBackendCompletionReceipt.model_validate(payload)


@pytest.mark.parametrize(
    "result",
    [
        HumanDecisionResult.create(
            work_item_id="018f47c2-4a3b-7a10-8a11-000000000013",
            final_revision=1,
            decision="APPROVED",
            actor_authority_digest=_digest("6"),
            rubric_digest=_digest("7"),
            evidence_digest=_digest("8"),
            artifact_fact_binding_digests=(),
        ),
        ExternalAuthorityResult.create(
            authority_kind="EXTERNAL_SYSTEM",
            authority_id="catalog-execution",
            authority_result_digest=_digest("9"),
            completion_receipt_object=_ref(),
            completion_receipt_digest=_digest("0"),
        ),
    ],
)
def test_human_and_external_results_cannot_forge_world_staged(
    result: HumanDecisionResult | ExternalAuthorityResult,
) -> None:
    payload = result.model_dump(mode="json", by_alias=True)
    payload["kind"] = "WORLD_STAGED"

    with pytest.raises(ValidationError):
        type(result).model_validate(payload)


def test_stable_workflow_identity_and_client_token() -> None:
    input_data = _input()

    assert catalog_execution_workflow_id(input_data) == catalog_execution_workflow_id(
        input_data
    )
    assert catalog_execution_client_token(input_data) == catalog_execution_client_token(
        input_data
    )


def test_platform_p3_lock_publishes_allowlists_but_disables_production() -> None:
    lock = json.loads(LOCK.read_text())
    assert lock["status"] == "MANIFEST_PUBLISHED"
    assert lock["runtimeActivationAllowed"] is False
    assert lock["p3Manifest"]["present"] is True
    assert lock["p3Manifest"]["lockBodyDigest"] == P3_ARTIFACT_LOCK_DIGEST
    assert lock["p3Manifest"]["sourceCommit"] is None
    assert lock["p3Manifest"]["publicationReceipt"] is None
    assert lock["platformProvenance"]["p3ArtifactCommit"] is None
    assert lock["platformProvenance"]["p3CommitReceipt"] is None
    assert lock["crossNamespaceContract"]["allowedPlatformContractDigests"] == [
        P3_ARTIFACT_LOCK_DIGEST
    ]
    assert len(lock["providerProfiles"]["allowedProfileDigests"]) == 3
    assert lock["providerProfiles"]["catalogOwnedRuntimeProfileDigests"] == []
    assert lock["activationReceipt"] is None
    assert lock["claimsRuntimeActivation"] is False

    with pytest.raises(ApplicationError, match="production activation"):
        validate_catalog_execution_runtime(_input(), lock)
