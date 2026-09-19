from __future__ import annotations

import hashlib
import json

import pytest

from video_media_catalog.connector import (
    ChangeSemantics,
    Completeness,
    DeleteCoverage,
    Serialization,
    TransportKind,
    build_connector_batch_manifest,
)
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.rights import UsageAction
from video_media_catalog.tvmaze import (
    TVMAZE_CONNECTOR_ID,
    TVMAZE_POLICY_ID,
    TVMAZE_SOURCE_PRODUCT_ID,
    TVMAZE_SOURCE_SYSTEM_ID,
    TVMazeShowConnector,
    map_tvmaze_show,
    tvmaze_rights_profile,
)


def _show(show_id: int = 1) -> dict[str, object]:
    return {
        "id": show_id,
        "name": "Example Show",
        "type": "Scripted",
        "language": "English",
        "status": "Ended",
        "premiered": "2020-01-02",
        "ended": "2020-02-03",
        "runtime": 45,
        "averageRuntime": 47,
        "genres": ["Drama", "Mystery"],
        "updated": 1_700_000_000,
        "externals": {
            "imdb": "tt1234567",
            "thetvdb": 123,
            "tvrage": None,
        },
        "image": {
            "medium": "https://static.tvmaze.com/example-medium.jpg",
            "original": "https://static.tvmaze.com/example.jpg",
        },
        "unknownFutureField": {"preserved": True},
    }


def _raw_object(raw: bytes, name: str = "page-0.json") -> ObjectRef:
    return ObjectRef(
        uri=f"file:///tmp/{name}",
        format="OBJECT_FORMAT_JSON",
        media_type="application/json",
        checksum=Checksum(value=hashlib.sha256(raw).hexdigest()),
        size_bytes=len(raw),
        created_at="2026-09-19T00:00:00Z",
    )


def _batch(raw_objects, record_count: int):
    policy = tvmaze_rights_profile()
    return build_connector_batch_manifest(
        source_system_id=TVMAZE_SOURCE_SYSTEM_ID,
        source_product_id=TVMAZE_SOURCE_PRODUCT_ID,
        connector_id=TVMAZE_CONNECTOR_ID,
        connector_version="1.0.0",
        image_digest="sha256:" + ("a" * 64),
        config_digest="sha256:" + ("b" * 64),
        policy_id=TVMAZE_POLICY_ID,
        policy_digest=policy.digest,
        transport_kind=TransportKind.API,
        serialization=Serialization.JSON,
        change_semantics=ChangeSemantics.FULL_SNAPSHOT,
        completeness=Completeness.COMPLETE,
        delete_coverage=DeleteCoverage.SNAPSHOT_DIFF,
        coverage_scope={"endpoint": "/shows"},
        raw_objects=raw_objects,
        acquired_at="2026-09-19T00:00:00Z",
        record_count=record_count,
        error_count=0,
    )


def test_tvmaze_connector_preserves_unknown_source_fields() -> None:
    raw = json.dumps([_show()]).encode()
    raw_object = _raw_object(raw)
    batch = _batch((raw_object,), 1)
    envelope = TVMazeShowConnector().decode(batch, (raw,))[0]

    assert envelope.source_record_id == "1"
    assert json.loads(envelope.payload_json)["unknownFutureField"] == {
        "preserved": True
    }
    assert envelope.source_location == "/page/0/item/0"


def test_tvmaze_mapper_emits_facts_and_omits_unreviewed_image() -> None:
    raw = json.dumps([_show()]).encode()
    batch = _batch((_raw_object(raw),), 1)
    envelope = TVMazeShowConnector().decode(batch, (raw,))[0]
    mapped = map_tvmaze_show(envelope)

    values = {
        (assertion.predicate, json.loads(assertion.value_json))
        for assertion in mapped.field_assertions
    }
    assert ("title", "Example Show") in values
    assert ("genre", "Drama") in values
    assert ("runtime_minutes", 45) in values
    identifiers = {
        (assertion.namespace_id, assertion.value)
        for assertion in mapped.identifier_assertions
    }
    assert ("tvmaze-show", "1") in identifiers
    assert ("imdb-title", "tt1234567") in identifiers
    assert mapped.omitted_asset_count == 1
    assert mapped.entity_type_assertions[0].entity_type == "SERIES"


def test_tvmaze_policy_does_not_silently_allow_ml() -> None:
    policy = tvmaze_rights_profile()
    assert UsageAction.ML_TRAIN not in policy.permissions
    assert policy.share_alike


def test_tvmaze_connector_rejects_duplicate_ids_across_pages() -> None:
    first = json.dumps([_show()]).encode()
    second = json.dumps([_show()]).encode()
    batch = _batch(
        (
            _raw_object(first, "page-0.json"),
            _raw_object(second, "page-1.json"),
        ),
        2,
    )
    with pytest.raises(ValueError, match="duplicate TVmaze show"):
        TVMazeShowConnector().decode(batch, (first, second))


def test_tvmaze_connector_rejects_unbound_raw_page() -> None:
    raw = json.dumps([_show()]).encode()
    batch = _batch((_raw_object(raw),), 1)
    with pytest.raises(ValueError, match="ObjectRef"):
        TVMazeShowConnector().decode(batch, (raw + b" ",))
