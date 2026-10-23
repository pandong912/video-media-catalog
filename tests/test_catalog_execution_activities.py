from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import UTC, datetime

import pytest
from temporalio.converter import DataConverter
from test_catalog_execution_contracts import _input

from video_media_catalog.execution_contracts import (
    CatalogBackendCompletionReceipt,
    ObjectRefV2,
)
from video_media_catalog.object_store import ObjectRefV2UploadResult
from video_media_catalog.spark_backend import (
    SparkBackendIdentity,
    SparkBackendObservation,
)
from video_media_catalog.temporal.activities import catalog_execution
from video_media_catalog.temporal.catalog_execution_models import (
    PreparedCatalogExecution,
)


class ReceiptStore:
    def __init__(self) -> None:
        self.payload = b""

    def publish_bytes(self, payload, target, *, max_bytes):
        assert len(payload) <= max_bytes
        self.payload = payload
        return ObjectRefV2UploadResult(
            ObjectRefV2.create(
                object_ref_id=target.object_ref_id,
                tenant_id=target.tenant_id,
                project_id=target.project_id,
                data_plane_binding_id=target.data_plane_binding_id,
                data_plane_id=target.data_plane_id,
                provider=target.provider,
                uri=target.uri,
                object_key=target.object_key,
                format=target.format,
                media_type=target.media_type,
                checksum=hashlib.sha256(payload).hexdigest(),
                size_bytes=len(payload),
                etag="receipt-etag",
                version_token="receipt-version-1",
                schema_digest=target.schema_digest,
                encryption_context_digest=target.encryption_context_digest,
                created_at=target.created_at,
            ),
            False,
        )


class ObservationBackend:
    def observe(
        self,
        identity: SparkBackendIdentity,
    ) -> SparkBackendObservation:
        return SparkBackendObservation(
            identity=identity,
            provider_state="SUCCESS",
            outcome="SUCCESS",
            observed_at="2026-10-23T00:10:00Z",
            raw={"updatedAt": datetime(2026, 10, 23, 0, 10, tzinfo=UTC)},
        )


def test_observe_activity_returns_bounded_json_serializable_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepared = PreparedCatalogExecution(_input(), "stable-token", "stable-job")
    identity = SparkBackendIdentity(
        provider="AWS_EMR_SERVERLESS",
        application_id="app-1",
        job_run_id="job-1",
    )
    monkeypatch.setattr(
        catalog_execution,
        "_backend",
        lambda _prepared: ObservationBackend(),
    )

    observation = catalog_execution.observe_catalog_spark(prepared, identity)

    assert observation.raw == {}
    asyncio.run(DataConverter.default.encode([observation]))


def test_completion_activity_writes_bounded_external_receipt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_data = _input()
    prepared = PreparedCatalogExecution(input_data, "stable-token", "stable-job")
    identity = SparkBackendIdentity(
        provider="AWS_EMR_SERVERLESS",
        application_id="app-1",
        job_run_id="job-1",
    )
    observation = SparkBackendObservation(
        identity=identity,
        provider_state="SUCCESS",
        outcome="SUCCESS",
        state_details="completed",
        observed_at="2026-10-23T00:10:00Z",
    )
    store = ReceiptStore()
    monkeypatch.setattr(catalog_execution, "_store", lambda _prepared: store)

    result = catalog_execution.write_catalog_completion_receipt(
        prepared,
        identity,
        observation,
    )
    receipt = CatalogBackendCompletionReceipt.model_validate_json(store.payload)

    assert len(store.payload) < 256 * 1024
    assert receipt.result_kind == "EXTERNAL_AUTHORITY"
    assert result.result_kind == "EXTERNAL_AUTHORITY"
    assert result.completion_receipt_digest == receipt.semantic_digest
    assert result.completion_receipt.version_token == "receipt-version-1"


def test_backend_observation_rejects_unbounded_completion_details() -> None:
    identity = SparkBackendIdentity(
        provider="AWS_EMR_SERVERLESS",
        application_id="app-1",
        job_run_id="job-1",
    )

    with pytest.raises(ValueError, match="bounded receipt"):
        SparkBackendObservation(
            identity=identity,
            provider_state="FAILED",
            outcome="FAILED",
            state_details="x" * 4_097,
            observed_at="2026-10-23T00:10:00Z",
        )


def test_completion_receipt_json_has_no_world_staged_claim() -> None:
    input_data = _input()
    prepared = PreparedCatalogExecution(input_data, "stable-token", "stable-job")
    identity = SparkBackendIdentity(
        provider="AWS_EMR_SERVERLESS",
        application_id="app-1",
        job_run_id="job-1",
    )
    observation = SparkBackendObservation(
        identity=identity,
        provider_state="FAILED",
        outcome="FAILED",
        observed_at="2026-10-23T00:10:00Z",
    )
    store = ReceiptStore()
    original = catalog_execution._store
    try:
        catalog_execution._store = lambda _prepared: store
        catalog_execution.write_catalog_completion_receipt(
            prepared,
            identity,
            observation,
        )
    finally:
        catalog_execution._store = original

    payload = json.loads(store.payload)
    assert payload["resultKind"] == "EXTERNAL_AUTHORITY"
    assert "WORLD_STAGED" not in store.payload.decode()
