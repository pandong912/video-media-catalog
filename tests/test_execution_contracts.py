from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from video_media_catalog.execution_contracts import (
    ExecutedReceiptEntry,
    ExecutionInputManifest,
    ExecutionPlanManifest,
    ExecutionPlanRequest,
    ExternalAuthorityResult,
    HumanDecisionResult,
    ManifestInput,
    ObjectRefV2,
    PartitionDescriptor,
    PartitionReceipt,
    ReceiptSet,
    ReusedReceiptEntry,
    RootManifest,
    WorldStagedResult,
    canonical_semantic_digest,
    canonical_tuple_sha256,
)
from video_media_catalog.models import ObjectRef

CONTRACTS = Path(__file__).parents[1] / "contracts" / "execution"
TENANT = "018f47c2-4a3b-7a10-8a11-000000000001"
PROJECT = "018f47c2-4a3b-7a10-8a11-000000000002"
BINDING = "018f47c2-4a3b-7a10-8a11-000000000003"
PLANE = "018f47c2-4a3b-7a10-8a11-000000000004"
SOURCE = "018f47c2-4a3b-7a10-8a11-000000000006"
PLAN = "018f47c2-4a3b-7a10-8a11-000000000007"
PARTITION = "018f47c2-4a3b-7a10-8a11-000000000008"
ATTEMPT = "018f47c2-4a3b-7a10-8a11-000000000009"
RECEIPT = "018f47c2-4a3b-7a10-8a11-00000000000a"


def _digest(value: str) -> str:
    return value * 64


def _id(suffix: str) -> str:
    return f"018f47c2-4a3b-7a10-8a11-000000000{suffix}"


def _object_ref(
    *,
    suffix: str = "005",
    key: str = "execution/inputs/part-00000.json",
    checksum: str = _digest("a"),
    created_at: str = "2026-10-22T00:00:00Z",
    provider: str = "AWS_S3",
    media_type: str = "application/json",
) -> ObjectRefV2:
    scheme = "s3" if provider == "AWS_S3" else "oss"
    return ObjectRefV2.create(
        object_ref_id=_id(suffix),
        tenant_id=TENANT,
        project_id=PROJECT,
        data_plane_binding_id=BINDING,
        data_plane_id=PLANE,
        provider=provider,  # type: ignore[arg-type]
        uri=f"{scheme}://video-world-fixtures/{key}",
        object_key=key,
        format="JSON",
        media_type=media_type,
        checksum=checksum,
        size_bytes=128,
        etag="fixture-etag",
        version_token="fixture-version-1",
        schema_digest=_digest("b"),
        encryption_context_digest=_digest("c"),
        created_at=created_at,
    )


def _partition(
    *,
    input_projection_digest: str = _digest("6"),
    coverage_end: int = 1,
) -> PartitionDescriptor:
    return PartitionDescriptor(
        execution_partition_id=PARTITION,
        partition_ordinal=0,
        partition_key="part-00000",
        execution_stage="MEDIA_ANALYSIS",
        plan_request_digest=_digest("1"),
        workload_digest=_digest("4"),
        execution_profile_digest=_digest("5"),
        input_projection_digest=input_projection_digest,
        coverage_start=0,
        coverage_end=coverage_end,
        required_for_completion=True,
        estimated_input_bytes=128,
        estimated_output_bytes=256,
    )


def _world_staged(
    result_object: ObjectRefV2 | None = None,
) -> WorldStagedResult:
    return WorldStagedResult.create(
        objects=(
            result_object
            or _object_ref(
                suffix="00b",
                key="execution/results/part-00000.json",
                created_at="2026-10-22T00:00:02Z",
            ),
        ),
        output_digest=_digest("e"),
        schema_digest=_digest("b"),
        record_count=1,
        byte_count=256,
    )


def _human_decision() -> HumanDecisionResult:
    return HumanDecisionResult.create(
        work_item_id=_id("00b"),
        final_revision=1,
        decision="APPROVED",
        actor_authority_digest=_digest("a"),
        rubric_digest=_digest("b"),
        evidence_digest=_digest("c"),
        artifact_fact_binding_digests=(_digest("d"),),
    )


def _external_authority() -> ExternalAuthorityResult:
    return ExternalAuthorityResult.create(
        authority_kind="EXTERNAL_SYSTEM",
        authority_id="external-run-1",
        authority_result_digest=_digest("a"),
        completion_receipt_object=_object_ref(
            suffix="00b",
            key="execution/results/external-receipt.json",
            created_at="2026-10-22T00:00:02Z",
        ),
        completion_receipt_digest=_digest("b"),
    )


def _receipt(
    result: (
        WorldStagedResult | HumanDecisionResult | ExternalAuthorityResult | None
    ) = None,
    *,
    recorded_at: str = "2026-10-22T00:00:04Z",
) -> PartitionReceipt:
    return PartitionReceipt.create(
        receipt_id=RECEIPT,
        tenant_id=TENANT,
        execution_attempt_id=ATTEMPT,
        execution_plan_id=PLAN,
        execution_partition_id=PARTITION,
        plan_digest=_digest("2"),
        input_manifest_digest=_digest("3"),
        producer_digest=_digest("9"),
        result=result or _world_staged(),
        produced_at="2026-10-22T00:00:03Z",
        recorded_at=recorded_at,
    )


def _payload(model: Any) -> dict[str, Any]:
    return json.loads(model.json_bytes())


def _round_trip(model: Any) -> Any:
    return type(model).model_validate(_payload(model))


def test_canonical_semantic_digest_matches_platform_tuple_vector() -> None:
    assert canonical_semantic_digest("domain", "a", "b") == (
        "4a4bfb246554eeb8fe662be45beba9ac7b46eab3dda3a0bb37cc2b2f23c4316f"
    )
    assert canonical_semantic_digest("domain", "ab", "") != (
        canonical_semantic_digest("domain", "a", "b")
    )


def test_canonical_tuple_matches_all_platform_golden_vectors() -> None:
    golden = json.loads((CONTRACTS / "canonical-tuple-v1.golden.json").read_text())
    assert golden["algorithm"] == (
        "video.world.canonical-tuple.sha256-length-delimited-utf8.v1"
    )
    for vector in golden["vectors"]:
        assert canonical_tuple_sha256(vector["fields"]) == vector["digest"]


@pytest.mark.parametrize("provider", ["AWS_S3", "ALIBABA_OSS"])
def test_object_ref_v2_round_trips_without_replacing_object_ref_v1(
    provider: str,
) -> None:
    reference = _object_ref(provider=provider)
    replay = _object_ref(
        suffix="00c",
        created_at="2026-10-22T01:00:00Z",
        provider=provider,
    )

    assert _round_trip(reference) == reference
    assert reference.identity_digest == replay.identity_digest
    assert ObjectRefV2 is not ObjectRef
    assert reference.model_dump(by_alias=True)["schemaVersion"] == "2.0.0"
    legacy = ObjectRef(
        uri="s3://legacy-bucket/object.json",
        format="OBJECT_FORMAT_JSON",
        media_type="application/json",
        checksum={"value": _digest("a")},
        size_bytes=0,
        object_version=None,
    )
    assert legacy.model_dump(mode="json", by_alias=True)["sizeBytes"] == "0"


def _set(field: str, value: Any) -> Callable[[dict[str, Any]], None]:
    def mutate(payload: dict[str, Any]) -> None:
        payload[field] = value

    return mutate


@pytest.mark.parametrize(
    "mutate",
    [
        _set("schemaVersion", "2.0.1"),
        _set("checksum", "sha256:" + _digest("a")),
        _set("sizeBytes", 0),
        _set("sizeBytes", "128"),
        _set("mediaType", "Application/JSON"),
        _set("versionToken", "v" * 1_025),
        _set("versionToken", "latest"),
        _set("versionToken", "version token"),
        _set("provider", "AWS"),
        _set("provider", "ALIBABA_OSS"),
        _set(
            "uri",
            "s3://video-world-fixtures/execution/inputs/part-00000.json"
            "?X-Amz-Credential=secret",
        ),
        _set(
            "uri",
            "https://video-world-fixtures.invalid/execution/inputs/"
            "part-00000.json?credential=secret",
        ),
    ],
)
def test_object_ref_v2_rejects_non_exact_or_credential_bearing_values(
    mutate: Callable[[dict[str, Any]], None],
) -> None:
    payload = _payload(_object_ref())
    mutate(payload)
    with pytest.raises(ValidationError):
        ObjectRefV2.model_validate(payload)


def test_object_ref_v2_rejects_unknown_credential_fields() -> None:
    payload = _payload(_object_ref())
    payload["accessKeyId"] = "secret"
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        ObjectRefV2.model_validate(payload)


def test_input_manifest_round_trip_and_digest_validation() -> None:
    manifest = ExecutionInputManifest.create(
        tenant_id=TENANT,
        project_id=PROJECT,
        source_request_id=SOURCE,
        source_authority_digest=_digest("e"),
        execution_plan_id=PLAN,
        plan_revision=1,
        plan_request_digest=_digest("1"),
        execution_partition_id=PARTITION,
        partition_ordinal=0,
        partition_digest=_partition().workload_digest,
        inputs=(
            ManifestInput(
                ordinal=0,
                role="PRIMARY_INPUT",
                object=_object_ref(),
                content_semantic_digest=_digest("f"),
                record_count=1,
            ),
        ),
        created_at="2026-10-22T00:00:00Z",
    )

    assert _round_trip(manifest) == manifest
    tampered = _payload(manifest)
    tampered["semanticDigest"] = _digest("0")
    with pytest.raises(ValidationError, match="input manifest projection"):
        ExecutionInputManifest.model_validate(tampered)


def test_plan_request_round_trip_and_digest_validation() -> None:
    payload = json.loads(
        (CONTRACTS / "fixtures" / "valid" / "execution-plan-request.json").read_text()
    )
    request = ExecutionPlanRequest.model_validate(payload)
    fields = request.model_dump(
        exclude={"schema_version", "plan_request_digest"},
    )

    assert _round_trip(request) == request
    assert ExecutionPlanRequest.create(**fields) == request
    tampered = _payload(request)
    tampered["priorityClass"] = "CRITICAL"
    with pytest.raises(ValidationError, match="plan request projection"):
        ExecutionPlanRequest.model_validate(tampered)


@pytest.mark.parametrize(
    ("fixture_name", "model_type"),
    [
        ("object-ref-v2.json", ObjectRefV2),
        ("execution-plan-request.json", ExecutionPlanRequest),
        ("execution-input-manifest.json", ExecutionInputManifest),
        ("execution-plan-manifest.json", ExecutionPlanManifest),
        ("partition-receipt.json", PartitionReceipt),
        ("receipt-set.json", ReceiptSet),
        ("root-manifest.json", RootManifest),
    ],
)
def test_platform_valid_fixtures_round_trip_through_pydantic(
    fixture_name: str,
    model_type: type[Any],
) -> None:
    payload = json.loads((CONTRACTS / "fixtures" / "valid" / fixture_name).read_text())
    parsed = model_type.model_validate(payload)

    assert json.loads(parsed.json_bytes()) == payload


def test_platform_fixtures_close_plan_partition_receipt_and_result_identity() -> None:
    def parsed(name: str, model_type: type[Any]) -> Any:
        payload = json.loads((CONTRACTS / "fixtures" / "valid" / name).read_text())
        return model_type.model_validate(payload)

    request = parsed("execution-plan-request.json", ExecutionPlanRequest)
    input_manifest = parsed("execution-input-manifest.json", ExecutionInputManifest)
    plan = parsed("execution-plan-manifest.json", ExecutionPlanManifest)
    receipt = parsed("partition-receipt.json", PartitionReceipt)
    receipt_set = parsed("receipt-set.json", ReceiptSet)
    root = parsed("root-manifest.json", RootManifest)
    partition = plan.partitions[0]

    assert plan.planning_input_manifest_digest == request.input_manifest_semantic_digest
    assert (
        input_manifest.execution_plan_id,
        input_manifest.plan_revision,
        input_manifest.plan_request_digest,
    ) == (plan.execution_plan_id, plan.plan_revision, plan.plan_request_digest)
    assert (
        input_manifest.execution_partition_id,
        input_manifest.partition_ordinal,
        input_manifest.partition_digest,
    ) == (
        partition.execution_partition_id,
        partition.partition_ordinal,
        partition.workload_digest,
    )
    assert receipt.plan_digest == receipt_set.plan_digest == plan.plan_digest
    assert root.plan_manifest_semantic_digest == plan.semantic_digest
    assert root.receipt_set_semantic_digest == receipt_set.semantic_digest
    assert root.result_digest == receipt.result_digest
    assert isinstance(receipt.result, WorldStagedResult)
    assert tuple(item.identity_digest for item in root.result_objects) == tuple(
        item.identity_digest for item in receipt.result.objects
    )
    valid = CONTRACTS / "fixtures" / "valid"
    assert (
        root.plan_manifest_object.checksum
        == hashlib.sha256(
            (valid / "execution-plan-manifest.json").read_bytes()
        ).hexdigest()
    )
    assert (
        root.receipt_set_object.checksum
        == hashlib.sha256((valid / "receipt-set.json").read_bytes()).hexdigest()
    )


@pytest.mark.parametrize(
    ("result", "result_type"),
    [
        (_world_staged(), WorldStagedResult),
        (_human_decision(), HumanDecisionResult),
        (_external_authority(), ExternalAuthorityResult),
    ],
)
def test_partition_receipt_parses_all_tagged_result_variants(
    result: WorldStagedResult | HumanDecisionResult | ExternalAuthorityResult,
    result_type: type[
        WorldStagedResult | HumanDecisionResult | ExternalAuthorityResult
    ],
) -> None:
    receipt = _receipt(result)
    parsed = _round_trip(receipt)
    assert isinstance(parsed.result, result_type)
    assert parsed.result_kind == result.kind


def test_result_digest_rejects_self_reference_tampering() -> None:
    payload = _payload(_world_staged())
    payload["resultDigest"] = _digest("0")

    with pytest.raises(ValidationError, match="WORLD_STAGED projection"):
        WorldStagedResult.model_validate(payload)


def test_receipt_digest_excludes_self_time_and_binds_result_ref() -> None:
    receipt = _receipt()
    replay = _receipt(recorded_at="2026-10-22T00:10:04Z")
    changed_result = _world_staged(
        _object_ref(
            suffix="00d",
            key="execution/results/part-00000-v2.json",
            checksum=_digest("d"),
            created_at="2026-10-22T00:00:02Z",
        )
    )
    changed = _receipt(changed_result)

    assert receipt.semantic_digest == replay.semantic_digest
    assert receipt.semantic_digest != changed.semantic_digest

    tampered = _payload(receipt)
    tampered["semanticDigest"] = _digest("0")
    with pytest.raises(ValidationError, match="partition receipt projection"):
        PartitionReceipt.model_validate(tampered)


def test_plan_receipt_set_and_root_digests_bind_complete_closure() -> None:
    receipt = _receipt()
    assert isinstance(receipt.result, WorldStagedResult)
    plan = ExecutionPlanManifest.create(
        tenant_id=TENANT,
        project_id=PROJECT,
        execution_plan_id=PLAN,
        plan_revision=1,
        plan_request_digest=_digest("1"),
        planning_input_manifest_digest=_digest("3"),
        partitions=(_partition(),),
        dependency_digests=(_digest("d"),),
        barrier_digests=(_digest("e"),),
        created_at="2026-10-22T00:00:01Z",
    )
    replayed_plan = ExecutionPlanManifest.create(
        tenant_id=TENANT,
        project_id=PROJECT,
        execution_plan_id=PLAN,
        plan_revision=1,
        plan_request_digest=_digest("1"),
        planning_input_manifest_digest=_digest("3"),
        partitions=(_partition(),),
        dependency_digests=(_digest("d"),),
        barrier_digests=(_digest("e"),),
        created_at="2026-10-22T01:00:01Z",
    )
    changed_plan = ExecutionPlanManifest.create(
        tenant_id=TENANT,
        project_id=PROJECT,
        execution_plan_id=PLAN,
        plan_revision=1,
        plan_request_digest=_digest("1"),
        planning_input_manifest_digest=_digest("3"),
        partitions=(_partition(input_projection_digest=_digest("8")),),
        dependency_digests=(_digest("d"),),
        barrier_digests=(_digest("e"),),
        created_at="2026-10-22T00:00:01Z",
    )
    entry = ExecutedReceiptEntry(
        binding_kind="EXECUTED",
        partition_ordinal=0,
        execution_partition_id=PARTITION,
        execution_attempt_id=ATTEMPT,
        receipt_id=RECEIPT,
        result_kind=receipt.result_kind,
        result_digest=receipt.result_digest,
        receipt_semantic_digest=receipt.semantic_digest,
    )
    receipt_set = ReceiptSet.create(
        tenant_id=TENANT,
        execution_plan_id=PLAN,
        plan_digest=plan.plan_digest,
        receipts=(entry,),
        created_at="2026-10-22T00:00:05Z",
    )
    replayed_set = ReceiptSet.create(
        tenant_id=TENANT,
        execution_plan_id=PLAN,
        plan_digest=plan.plan_digest,
        receipts=(entry,),
        created_at="2026-10-22T01:00:05Z",
    )

    plan_object = _object_ref(
        suffix="00c",
        key="execution/plans/plan-manifest.json",
        media_type="application/vnd.video-world.execution-plan-manifest.v1+json",
    )
    receipt_set_object = _object_ref(
        suffix="00d",
        key="execution/receipts/receipt-set.json",
        media_type="application/vnd.video-world.receipt-set.v1+json",
    )
    root = RootManifest.create(
        tenant_id=TENANT,
        project_id=PROJECT,
        execution_plan_id=PLAN,
        plan_revision=1,
        plan_request_digest=_digest("1"),
        plan_digest=plan.plan_digest,
        plan_manifest_object=plan_object,
        plan_manifest_semantic_digest=plan.semantic_digest,
        receipt_set_object=receipt_set_object,
        receipt_set_semantic_digest=receipt_set.semantic_digest,
        result_kind=receipt.result_kind,
        result_digest=receipt.result_digest,
        result_objects=receipt.result.objects,
        created_at="2026-10-22T00:00:06Z",
    )
    replayed_root = RootManifest.create(
        tenant_id=TENANT,
        project_id=PROJECT,
        execution_plan_id=PLAN,
        plan_revision=1,
        plan_request_digest=_digest("1"),
        plan_digest=plan.plan_digest,
        plan_manifest_object=plan_object,
        plan_manifest_semantic_digest=plan.semantic_digest,
        receipt_set_object=receipt_set_object,
        receipt_set_semantic_digest=receipt_set.semantic_digest,
        result_kind=receipt.result_kind,
        result_digest=receipt.result_digest,
        result_objects=receipt.result.objects,
        created_at="2026-10-22T01:00:06Z",
    )
    changed_root = RootManifest.create(
        tenant_id=TENANT,
        project_id=PROJECT,
        execution_plan_id=PLAN,
        plan_revision=1,
        plan_request_digest=_digest("1"),
        plan_digest=plan.plan_digest,
        plan_manifest_object=plan_object,
        plan_manifest_semantic_digest=plan.semantic_digest,
        receipt_set_object=receipt_set_object,
        receipt_set_semantic_digest=receipt_set.semantic_digest,
        result_kind=receipt.result_kind,
        result_digest=receipt.result_digest,
        result_objects=(
            _object_ref(
                suffix="00e",
                key="execution/results/changed.json",
                checksum=_digest("f"),
            ),
        ),
        created_at="2026-10-22T00:00:06Z",
    )

    assert _round_trip(receipt_set) == receipt_set
    assert _round_trip(plan) == plan
    assert _round_trip(root) == root
    assert receipt_set.semantic_digest == replayed_set.semantic_digest
    assert plan.semantic_digest == replayed_plan.semantic_digest
    assert plan.semantic_digest != changed_plan.semantic_digest
    assert root.semantic_digest == replayed_root.semantic_digest
    assert root.semantic_digest != changed_root.semantic_digest

    incomplete_root = _payload(root)
    incomplete_root["resultObjects"] = []
    with pytest.raises(ValidationError, match="applicable result closure"):
        RootManifest.model_validate(incomplete_root)

    for model, model_type, message in (
        (receipt_set, ReceiptSet, "receipt-set projection"),
        (plan, ExecutionPlanManifest, "plan manifest projection"),
        (root, RootManifest, "complete root receipt closure"),
    ):
        tampered = _payload(model)
        tampered["semanticDigest"] = _digest("0")
        with pytest.raises(ValidationError, match=message):
            model_type.model_validate(tampered)


def test_receipt_set_parses_reused_binding_variant() -> None:
    reused = ReusedReceiptEntry(
        binding_kind="REUSED",
        partition_ordinal=0,
        execution_partition_id=PARTITION,
        source_plan_id=PLAN,
        source_partition_id=_id("00c"),
        source_receipt_tenant_id=TENANT,
        source_receipt_id=_id("00d"),
        result_kind="WORLD_STAGED",
        result_digest=_digest("7"),
        source_receipt_semantic_digest=_digest("8"),
        reuse_binding_id=_id("00e"),
        reuse_binding_digest=_digest("9"),
    )
    receipt_set = ReceiptSet.create(
        tenant_id=TENANT,
        execution_plan_id=PLAN,
        plan_digest=_digest("2"),
        receipts=(reused,),
        created_at="2026-10-22T00:00:05Z",
    )

    parsed = _round_trip(receipt_set)
    assert isinstance(parsed.receipts[0], ReusedReceiptEntry)


def test_timestamp_offsets_canonicalize_before_receipt_digest() -> None:
    utc_receipt = _receipt()
    offset_receipt = PartitionReceipt.create(
        receipt_id=RECEIPT,
        tenant_id=TENANT,
        execution_attempt_id=ATTEMPT,
        execution_plan_id=PLAN,
        execution_partition_id=PARTITION,
        plan_digest=_digest("2"),
        input_manifest_digest=_digest("3"),
        producer_digest=_digest("9"),
        result=_world_staged(),
        produced_at="2026-10-22T08:00:03+08:00",
        recorded_at="2026-10-22T08:00:04+08:00",
    )

    assert offset_receipt.produced_at == "2026-10-22T00:00:03Z"
    assert offset_receipt.semantic_digest == utc_receipt.semantic_digest
