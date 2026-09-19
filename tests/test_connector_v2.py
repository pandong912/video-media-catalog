from __future__ import annotations

import pytest

from video_media_catalog.connector import (
    ChangeSemantics,
    Completeness,
    ConnectorBatchManifest,
    DeleteCoverage,
    RecordOperation,
    Serialization,
    TransportKind,
    build_connector_batch_manifest,
    build_connector_record_envelope,
    build_connector_record_set_manifest,
    validate_envelopes_against_batch,
)
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.tvmaze import tvmaze_rights_profile


def _object(name: str = "page.json") -> ObjectRef:
    return ObjectRef(
        uri=f"file:///tmp/{name}",
        format="OBJECT_FORMAT_JSON",
        media_type="application/json",
        checksum=Checksum(value="a" * 64),
        size_bytes=123,
        created_at="2026-09-19T00:00:00Z",
    )


def _batch(**overrides) -> ConnectorBatchManifest:
    policy = tvmaze_rights_profile()
    values = {
        "source_system_id": "tvmaze",
        "source_product_id": "tvmaze-public-api",
        "connector_id": "tvmaze-show-index",
        "connector_version": "1.0.0",
        "image_digest": "sha256:" + ("b" * 64),
        "config_digest": "sha256:" + ("c" * 64),
        "policy_id": policy.policy_id,
        "policy_digest": policy.digest,
        "transport_kind": TransportKind.API,
        "serialization": Serialization.JSON,
        "change_semantics": ChangeSemantics.FULL_SNAPSHOT,
        "completeness": Completeness.COMPLETE,
        "delete_coverage": DeleteCoverage.SNAPSHOT_DIFF,
        "coverage_scope": {"endpoint": "/shows", "pages": [0]},
        "raw_objects": (_object(),),
        "acquired_at": "2026-09-19T00:00:00Z",
        "record_count": 1,
        "error_count": 0,
    }
    values.update(overrides)
    return build_connector_batch_manifest(**values)


def test_batch_and_record_envelope_round_trip_is_deterministic() -> None:
    batch = _batch()
    second = ConnectorBatchManifest.model_validate_json(batch.json_bytes())
    assert batch == second

    envelope = build_connector_record_envelope(
        payload={"id": 1, "name": "Example"},
        batch_id=batch.batch_id,
        source_system_id=batch.source_system_id,
        source_product_id=batch.source_product_id,
        source_namespace_id="tvmaze-show",
        source_record_id="1",
        source_revision="10",
        operation=RecordOperation.UPSERT,
        observed_at=batch.acquired_at,
        ingested_at=batch.acquired_at,
        payload_schema="tvmaze-show-v1",
        raw_object=batch.raw_objects[0],
        source_location="/0",
        policy_id=batch.policy_id,
        policy_digest=batch.policy_digest,
    )
    assert validate_envelopes_against_batch(batch, (envelope,)) == (envelope,)
    assert envelope.payload_json == '{"id":1,"name":"Example"}'


def test_batch_rejects_snapshot_diff_for_partial_data() -> None:
    with pytest.raises(ValueError, match="snapshot-diff"):
        _batch(completeness=Completeness.PARTIAL)


def test_inferred_absence_is_rejected_without_complete_snapshot() -> None:
    partial = _batch(
        change_semantics=ChangeSemantics.DELTA,
        completeness=Completeness.PARTIAL,
        delete_coverage=DeleteCoverage.NONE,
    )
    envelope = build_connector_record_envelope(
        batch_id=partial.batch_id,
        source_system_id=partial.source_system_id,
        source_product_id=partial.source_product_id,
        source_namespace_id="tvmaze-show",
        source_record_id="1",
        operation=RecordOperation.INFERRED_ABSENCE,
        observed_at=partial.acquired_at,
        ingested_at=partial.acquired_at,
        payload_schema="tvmaze-show-v1",
        raw_object=partial.raw_objects[0],
        source_location="/0",
        policy_id=partial.policy_id,
        policy_digest=partial.policy_digest,
    )
    with pytest.raises(ValueError, match="inferred absence"):
        validate_envelopes_against_batch(partial, (envelope,))


def test_record_set_manifest_binds_normalized_objects() -> None:
    batch = _batch()
    record_set = build_connector_record_set_manifest(
        batch_id=batch.batch_id,
        source_product_id=batch.source_product_id,
        policy_id=batch.policy_id,
        policy_digest=batch.policy_digest,
        record_objects=(_object("records.ndjson"),),
        record_count=1,
        first_envelope_key="sha256:" + ("d" * 64),
        last_envelope_key="sha256:" + ("d" * 64),
        created_at=batch.acquired_at,
    )
    assert record_set.record_set_id.startswith("sha256:")
    assert record_set == type(record_set).model_validate_json(record_set.json_bytes())
