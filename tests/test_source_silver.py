from __future__ import annotations

import pytest

from video_media_catalog.community_sources import build_community_registry
from video_media_catalog.connector import (
    ChangeSemantics,
    Completeness,
    DeleteCoverage,
    RecordOperation,
    Serialization,
    TransportKind,
    build_connector_batch_manifest,
    build_connector_record_envelope,
    build_connector_record_set_manifest,
)
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.source_silver import build_source_silver_rows
from video_media_catalog.tvmaze import (
    TVMAZE_CONNECTOR_ID,
    TVMAZE_POLICY_ID,
    TVMAZE_SOURCE_PRODUCT_ID,
    TVMAZE_SOURCE_SYSTEM_ID,
    tvmaze_rights_profile,
)


def _object() -> ObjectRef:
    return ObjectRef(
        uri="file:///tmp/source-silver-record.json",
        format="OBJECT_FORMAT_JSON",
        media_type="application/json",
        checksum=Checksum(value="a" * 64),
        size_bytes=1,
        created_at="2026-09-20T00:00:00Z",
    )


def _capture(
    *,
    policy_id: str = TVMAZE_POLICY_ID,
    policy_digest: str | None = None,
    source_namespace_id: str = "tvmaze-show",
    connector_id: str = TVMAZE_CONNECTOR_ID,
):
    policy = tvmaze_rights_profile()
    raw_object = _object()
    batch = build_connector_batch_manifest(
        source_system_id=TVMAZE_SOURCE_SYSTEM_ID,
        source_product_id=TVMAZE_SOURCE_PRODUCT_ID,
        connector_id=connector_id,
        connector_version="1.0.0",
        image_digest="sha256:" + ("b" * 64),
        config_digest="sha256:" + ("c" * 64),
        policy_id=policy_id,
        policy_digest=policy_digest or policy.digest,
        transport_kind=TransportKind.API,
        serialization=Serialization.JSON,
        change_semantics=ChangeSemantics.DELTA,
        completeness=Completeness.COMPLETE,
        delete_coverage=DeleteCoverage.EXPLICIT,
        coverage_scope={"endpoint": "/shows"},
        raw_objects=(raw_object,),
        acquired_at="2026-09-20T00:00:00Z",
        record_count=1,
        error_count=0,
    )
    envelope = build_connector_record_envelope(
        payload={"id": 1, "name": "Example"},
        batch_id=batch.batch_id,
        source_system_id=batch.source_system_id,
        source_product_id=batch.source_product_id,
        source_namespace_id=source_namespace_id,
        source_record_id="1",
        operation=RecordOperation.UPSERT,
        observed_at=batch.acquired_at,
        ingested_at=batch.acquired_at,
        payload_schema="tvmaze-show-v1",
        raw_object=raw_object,
        source_location="/shows/1",
        policy_id=batch.policy_id,
        policy_digest=batch.policy_digest,
    )
    record_set = build_connector_record_set_manifest(
        batch_id=batch.batch_id,
        source_product_id=batch.source_product_id,
        policy_id=batch.policy_id,
        policy_digest=batch.policy_digest,
        record_objects=(raw_object,),
        record_count=1,
        first_envelope_key=envelope.envelope_key,
        last_envelope_key=envelope.envelope_key,
        created_at=batch.acquired_at,
    )
    return batch, record_set, envelope


def test_source_silver_rejects_another_registered_policy() -> None:
    registry = build_community_registry()
    other = next(
        profile
        for profile in registry.rights_profiles
        if profile.policy_id == "wikidata-structured-data-cc0"
    )
    batch, record_set, envelope = _capture(
        policy_id=other.policy_id,
        policy_digest=other.digest,
    )

    with pytest.raises(ValueError, match="product rights policy"):
        build_source_silver_rows(
            registry=registry,
            batch=batch,
            record_set=record_set,
            envelopes=(envelope,),
        )


def test_source_silver_rejects_namespace_owned_by_another_product() -> None:
    registry = build_community_registry()
    batch, record_set, envelope = _capture(source_namespace_id="tmdb-movie")

    with pytest.raises(ValueError, match="namespace"):
        build_source_silver_rows(
            registry=registry,
            batch=batch,
            record_set=record_set,
            envelopes=(envelope,),
        )


def test_source_silver_rejects_unregistered_product_connector() -> None:
    registry = build_community_registry()
    batch, record_set, envelope = _capture(
        connector_id="tvmaze-unregistered-research-connector"
    )

    with pytest.raises(ValueError, match="connector"):
        build_source_silver_rows(
            registry=registry,
            batch=batch,
            record_set=record_set,
            envelopes=(envelope,),
        )


def test_source_silver_pins_registry_and_batch_metadata() -> None:
    registry = build_community_registry()
    batch, record_set, envelope = _capture()

    run, rows = build_source_silver_rows(
        registry=registry,
        batch=batch,
        record_set=record_set,
        envelopes=(envelope,),
    )

    assert run.input_manifest["registryDigest"] == registry.digest
    assert run.input_manifest["batchManifest"]["batchId"] == batch.batch_id
    assert rows["community_source_record"][0]["envelope_key"] == envelope.envelope_key
