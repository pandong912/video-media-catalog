from __future__ import annotations

import json

import pytest

from video_media_catalog.community_sources import eidr_rights_profile
from video_media_catalog.connector import (
    RecordOperation,
    build_connector_record_envelope,
)
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.source_mappers import map_eidr_record
from video_media_catalog.source_silver_checkpoint import (
    mapper_identity_for_product,
)

EIDR_ID = "10.5240/AAAA-BBBB-CCCC-DDDD-EEEE-C"


def _envelope(payload: dict[str, object]):
    policy = eidr_rights_profile()
    raw = ObjectRef(
        uri="file:///tmp/eidr-source-mapper.xml",
        format="OBJECT_FORMAT_OTHER",
        media_type="application/xml",
        checksum=Checksum(value="a" * 64),
        size_bytes=1,
    )
    return build_connector_record_envelope(
        payload={"id": EIDR_ID, "titles": [], **payload},
        batch_id="sha256:" + ("b" * 64),
        source_system_id="eidr",
        source_product_id="eidr-public-registry",
        source_namespace_id="eidr-content",
        source_record_id=EIDR_ID,
        operation=RecordOperation.UPSERT,
        observed_at="2026-09-20T00:00:00Z",
        ingested_at="2026-09-20T00:00:00Z",
        payload_schema="eidr-record-exact-lookup-v1",
        raw_object=raw,
        source_location="/exact-lookups/test",
        policy_id=policy.policy_id,
        policy_digest=policy.digest,
    )


@pytest.mark.parametrize(
    ("record_type", "structural_type", "expected_type", "expected_kind"),
    [
        (None, "Abstraction", "WORK", "EDITORIAL_WORK"),
        ("EDIT", "Performance", "EDIT", "EDIT"),
        ("EDIT", "Digital", "MANIFESTATION", "MANIFESTATION"),
        ("SERIES", "Digital", "TV_SERIES", "SERIES"),
        ("SEASON", "Digital", "TV_SEASON", "SEASON"),
        ("EPISODE", "Digital", "TV_EPISODE", "EPISODE"),
    ],
)
def test_eidr_mapper_uses_official_structural_hierarchy(
    record_type: str | None,
    structural_type: str,
    expected_type: str,
    expected_kind: str,
) -> None:
    mapped = map_eidr_record(
        _envelope(
            {
                "recordType": record_type,
                "structuralType": structural_type,
            }
        )
    )

    assert mapped.source_node.referent_kind == expected_kind
    assert [item.entity_type for item in mapped.entity_type_assertions] == [
        expected_type
    ]
    structural = next(
        item for item in mapped.field_assertions if item.predicate == "structural_type"
    )
    assert json.loads(structural.value_json) == structural_type


def test_eidr_mapper_preserves_episode_parent_chain() -> None:
    parent = "10.5240/2222-2222-2222-2222-2222-A"
    mapped = map_eidr_record(
        _envelope(
            {
                "recordType": "EPISODE",
                "structuralType": "Abstraction",
                "parentRelations": [
                    {
                        "type": "PART_OF_SEASON",
                        "targetEidrId": parent,
                        "ordinal": "2",
                    }
                ],
            }
        )
    )

    relation = mapped.relationship_assertions[0]
    assert relation.predicate == "part_of_season"
    assert relation.object.source_id == parent
    assert relation.object.referent_kind == "SEASON"
    assert relation.qualifiers == {"ordinal": "2"}


def test_eidr_checkpoint_identity_matches_mapper_implementation() -> None:
    identity = mapper_identity_for_product("eidr-public-registry")
    assert identity.mapper_id == "eidr-source-mapper"
    assert identity.mapper_version == "2.0.0"
