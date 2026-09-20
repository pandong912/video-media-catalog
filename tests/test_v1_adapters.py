from __future__ import annotations

import hashlib

from video_media_catalog.connector import ConnectorRecordEnvelope
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.object_store import BoundedObjectStore
from video_media_catalog.storage import local_path
from video_media_catalog.v1_adapters import (
    EIDR_CONNECTOR_ID,
    WIKIDATA_CONNECTOR_ID,
    capture_v1_adapter,
    map_eidr_record,
    map_wikidata_record,
)


def _object(path, *, media_type: str) -> ObjectRef:
    payload = path.read_bytes()
    return ObjectRef(
        uri=path.as_uri(),
        format="OBJECT_FORMAT_OTHER",
        media_type=media_type,
        checksum=Checksum(value=hashlib.sha256(payload).hexdigest()),
        size_bytes=len(payload),
    )


def _records(result) -> list[ConnectorRecordEnvelope]:
    return [
        ConnectorRecordEnvelope.model_validate_json(line)
        for reference in result.record_set_manifest.record_objects
        for line in local_path(reference.uri).read_bytes().splitlines()
    ]


def test_wikidata_v2_adapter_resolves_v1_type_closure(
    tmp_path,
    fixture_dir,
) -> None:
    source = fixture_dir / "wikidata.json"
    result = capture_v1_adapter(
        source="wikidata",
        input_path=source,
        raw_object=_object(source, media_type="application/json"),
        destination_prefix=tmp_path.as_uri(),
        acquired_at="2026-09-20T00:00:00Z",
        image_digest="sha256:" + ("a" * 64),
        config_digest="sha256:" + ("b" * 64),
        coverage_id="reference-subset-2026-09-20",
        store=BoundedObjectStore(client=object()),
    )

    assert result.batch_manifest.connector_id == WIKIDATA_CONNECTOR_ID
    envelope = next(
        item for item in _records(result) if item.source_record_id == "Q1001"
    )
    mapped = map_wikidata_record(envelope)
    assert mapped.entity_type_assertions[0].entity_type == "MOVIE"
    assert {
        (item.namespace_id, item.value) for item in mapped.identifier_assertions
    } >= {
        ("wikidata-item", "Q1001"),
        ("imdb-title", "tt0000001"),
        ("eidr-content", "10.5240/AAAA-BBBB-CCCC-DDDD-EEEE-C"),
    }
    assert any(
        item.predicate == "directed_by" for item in mapped.relationship_assertions
    )


def test_wikidata_series_keeps_source_node_but_maps_series_blocking_ids(
    tmp_path,
    fixture_dir,
) -> None:
    source = fixture_dir / "wikidata.json"
    result = capture_v1_adapter(
        source="wikidata",
        input_path=source,
        raw_object=_object(source, media_type="application/json"),
        destination_prefix=tmp_path.as_uri(),
        acquired_at="2026-09-20T00:00:00Z",
        image_digest="sha256:" + ("a" * 64),
        config_digest="sha256:" + ("b" * 64),
        coverage_id="reference-subset-2026-09-20",
        store=BoundedObjectStore(client=object()),
    )
    envelope = next(
        item for item in _records(result) if item.source_record_id == "Q1002"
    )
    mapped = map_wikidata_record(envelope)
    assert mapped.source_node.referent_kind == "EDITORIAL_WORK"
    assert mapped.entity_type_assertions[0].entity_type == "TV_SERIES"
    identifiers = {
        (item.namespace_id, item.referent_kind)
        for item in mapped.identifier_assertions
    }
    assert ("wikidata-item", "SERIES") in identifiers


def test_eidr_v2_adapter_defaults_to_partial_exact_lookup_scope(
    tmp_path,
    fixture_dir,
) -> None:
    source = fixture_dir / "eidr.xml"
    result = capture_v1_adapter(
        source="eidr",
        input_path=source,
        raw_object=_object(source, media_type="application/xml"),
        destination_prefix=tmp_path.as_uri(),
        acquired_at="2026-09-20T00:00:00Z",
        image_digest="sha256:" + ("a" * 64),
        config_digest="sha256:" + ("b" * 64),
        coverage_id="discovered-eidr-ids-2026-09-20",
        store=BoundedObjectStore(client=object()),
    )

    assert result.batch_manifest.connector_id == EIDR_CONNECTOR_ID
    assert result.batch_manifest.completeness.value == "PARTIAL"
    assert result.batch_manifest.delete_coverage.value == "NONE"
    envelope = _records(result)[0]
    mapped = map_eidr_record(envelope)
    assert mapped.entity_type_assertions[0].entity_type == "MOVIE"
    assert any(
        item.namespace_id == "imdb-title" and item.value == "tt0000001"
        for item in mapped.identifier_assertions
    )
