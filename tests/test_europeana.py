from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from video_media_catalog.connector import (
    RecordOperation,
    build_connector_record_envelope,
)
from video_media_catalog.europeana import (
    EUROPEANA_METADATA_LICENSE_URI,
    EUROPEANA_RECORD_NAMESPACE_ID,
    EUROPEANA_SOURCE_PRODUCT_ID,
    EUROPEANA_SOURCE_SYSTEM_ID,
    europeana_metadata_rights_profile,
    europeana_registry_entries,
    map_europeana_record,
    normalize_europeana_record_id,
)
from video_media_catalog.europeana_oai import (
    EuropeanaOAIError,
    parse_europeana_oai_page,
)
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.rights import PolicyZone
from video_media_catalog.source_silver_checkpoint import (
    mapper_identity_for_product,
)

FIXTURE = Path(__file__).parent / "fixtures" / "europeana_oai.xml"


def _envelope(payload: dict[str, object]):
    raw_bytes = FIXTURE.read_bytes()
    raw = ObjectRef(
        uri=FIXTURE.as_uri(),
        format="OBJECT_FORMAT_OTHER",
        media_type="application/xml",
        checksum=Checksum(value=hashlib.sha256(raw_bytes).hexdigest()),
        size_bytes=len(raw_bytes),
    )
    policy = europeana_metadata_rights_profile()
    return build_connector_record_envelope(
        payload=payload,
        batch_id="sha256:" + ("a" * 64),
        source_system_id=EUROPEANA_SOURCE_SYSTEM_ID,
        source_product_id=EUROPEANA_SOURCE_PRODUCT_ID,
        source_namespace_id=EUROPEANA_RECORD_NAMESPACE_ID,
        source_record_id=str(payload["id"]),
        source_revision=str(payload["datestamp"]),
        operation=RecordOperation.UPSERT,
        source_modified_at=str(payload["datestamp"]),
        observed_at="2026-09-26T00:00:05Z",
        ingested_at="2026-09-26T00:00:05Z",
        payload_schema="europeana-edm-record-v1",
        raw_object=raw,
        source_location="/oai/record/page/0/record/0",
        policy_id=policy.policy_id,
        policy_digest=policy.digest,
    )


def test_europeana_parser_preserves_metadata_and_object_rights_separately() -> None:
    page = parse_europeana_oai_page(FIXTURE.read_bytes())

    assert len(page.records) == 2
    assert page.resumption_token == "opaque-token-2"
    assert page.cursor == 0
    assert page.complete_list_size == 42
    first = page.records[0].payload
    assert first["id"] == "/123/item-1"
    assert first["metadataRights"] == {
        "licenseId": "CC0-1.0",
        "licenseUri": EUROPEANA_METADATA_LICENSE_URI,
        "appliesTo": "METADATA_ONLY",
    }
    object_rights = first["digitalObjectRights"]
    assert object_rights["status"] == "DECLARED"
    assert (
        "https://rightsstatements.org/vocab/InC/1.0/"
        in (object_rights["rightsStatements"])
    )
    assert "https://creativecommons.org/licenses/by/4.0/" in (object_rights["licenses"])
    assert first["binaryAcquisition"] == "DISABLED"
    assert first["mediaUrls"] == ["https://media.example/video/1.mp4"]
    assert set(first["previewUrls"]) == {
        "https://media.example/preview/1-small.jpg",
        "https://media.example/preview/1.jpg",
    }
    referenced = object_rights["referencedResources"]
    assert len(referenced) == 3
    media_rights = next(
        item
        for item in referenced
        if item["url"] == "https://media.example/video/1.mp4"
    )
    assert media_rights["rightsBasis"] == "RESOURCE"
    assert media_rights["status"] == "DECLARED"
    assert media_rights["rightsStatements"] == [
        "https://rightsstatements.org/vocab/InC/1.0/"
    ]
    assert page.records[1].payload["digitalObjectRights"]["status"] == (
        "MISSING_ASSUME_COPYRIGHT"
    )


def test_europeana_mapper_emits_long_tail_fields_and_exact_ids_only() -> None:
    payload = parse_europeana_oai_page(FIXTURE.read_bytes()).records[0].payload
    mapped = map_europeana_record(_envelope(payload))
    fields = {
        (assertion.predicate, assertion.value_json)
        for assertion in mapped.field_assertions
    }
    assert ("title", '"Example archival film"') in fields
    assert ("description", '"A preserved description."') in fields
    assert ("country", '"France"') in fields
    assert ("media_type", '"VIDEO"') in fields
    assert ("provider", '"Europeana Example Aggregator"') in fields
    assert ("data_provider", '"Example Data Provider"') in fields
    assert (
        "edm_rights",
        '"https://creativecommons.org/licenses/by/4.0/"',
    ) in fields
    assert (
        "dc_rights",
        '"https://rightsstatements.org/vocab/InC/1.0/"',
    ) in fields
    assert (
        "metadata_license_uri",
        '"https://creativecommons.org/publicdomain/zero/1.0/"',
    ) in fields
    linked_rights = [
        json.loads(assertion.value_json)
        for assertion in mapped.field_assertions
        if assertion.predicate == "linked_object_rights"
    ]
    assert len(linked_rights) == 3
    identifiers = {
        (assertion.namespace_id, assertion.value)
        for assertion in mapped.identifier_assertions
    }
    assert ("europeana-record", "/123/item-1") in identifiers
    assert ("eidr-content", "10.5240/AAAA-BBBB-CCCC-DDDD-EEEE-C") in identifiers
    assert ("imdb-title", "tt1234567") in identifiers
    assert all(value != "ARCHIVE-LOCAL-1" for _, value in identifiers)
    assert mapped.omitted_asset_count == 3


def test_europeana_registry_and_metadata_policy_are_explicit() -> None:
    system, product, namespace = europeana_registry_entries()
    policy = europeana_metadata_rights_profile()

    assert system.source_system_id == "europeana"
    assert product.source_product_id == "europeana-oai-edm"
    assert product.connector_ids == ("europeana-oai-pmh",)
    assert namespace.normalize("/123/item-1") == "/123/item-1"
    assert policy.zone == PolicyZone.OPEN_CC0
    assert "does not grant rights to linked digital objects" in policy.notes
    identity = mapper_identity_for_product(EUROPEANA_SOURCE_PRODUCT_ID)
    assert identity.mapper_id == "europeana-edm-source-mapper"
    assert identity.mapper_version == "1.0.0"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("/123/item-1", "/123/item-1"),
        (
            "http://data.europeana.eu/item/123/item-1",
            "/123/item-1",
        ),
        (
            "https://data.europeana.eu/item/123/item-1",
            "/123/item-1",
        ),
    ],
)
def test_normalize_europeana_record_id(value: str, expected: str) -> None:
    assert normalize_europeana_record_id(value) == expected


@pytest.mark.parametrize(
    "value",
    [
        "https://attacker.example/item/123/item-1",
        "https://data.europeana.eu/other/123/item-1",
        "/only-one-segment",
        "/123/item-1?x=1",
    ],
)
def test_normalize_europeana_record_id_rejects_invalid_values(value: str) -> None:
    with pytest.raises(ValueError):
        normalize_europeana_record_id(value)


def test_europeana_parser_rejects_malicious_xml() -> None:
    body = b"""<?xml version="1.0"?>
<!DOCTYPE x [<!ENTITY secret SYSTEM "file:///etc/passwd">]>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <responseDate>2026-09-26T00:00:00Z</responseDate>
  <ListRecords>&secret;</ListRecords>
</OAI-PMH>"""
    with pytest.raises(EuropeanaOAIError, match="DOCTYPE"):
        parse_europeana_oai_page(body)


def test_europeana_parser_rejects_oversize_normalized_record() -> None:
    with pytest.raises(EuropeanaOAIError, match=r"record .* byte bound"):
        parse_europeana_oai_page(
            FIXTURE.read_bytes(),
            max_record_bytes=128,
        )
