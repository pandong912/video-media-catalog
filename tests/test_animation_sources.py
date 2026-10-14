from __future__ import annotations

import json
from pathlib import Path

from video_media_catalog.animation_sources import (
    MadbHttpResponse,
    MadbPublicClient,
    map_madb_animation_record,
)
from video_media_catalog.community_sources import (
    madb_rights_profile,
    wikidata_rights_profile,
)
from video_media_catalog.connector import (
    RecordOperation,
    build_connector_record_envelope,
)
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.research_snapshot import (
    SnapshotRevisionKind,
    build_source_snapshot_pin,
    capture_pinned_records,
)
from video_media_catalog.source_mappers import map_wikidata_research_record
from video_media_catalog.source_silver_checkpoint import (
    mapper_identity_for_product,
)

FIXTURES = Path(__file__).parent / "fixtures/research"


def _fixture(name: str) -> dict[str, object]:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _raw(name: str = "animation-fixture.json") -> ObjectRef:
    return ObjectRef(
        uri=f"file:///tmp/{name}",
        format="OBJECT_FORMAT_JSON",
        media_type="application/json",
        checksum=Checksum(value="e" * 64),
        size_bytes=1024,
        created_at="2026-10-15T00:00:00Z",
    )


def _envelope(
    payload: dict[str, object],
    *,
    system: str,
    product: str,
    namespace: str,
    record_id: str,
    policy,
):
    return build_connector_record_envelope(
        payload=payload,
        batch_id="sha256:" + ("f" * 64),
        source_system_id=system,
        source_product_id=product,
        source_namespace_id=namespace,
        source_record_id=record_id,
        source_revision="fixture-v1",
        operation=RecordOperation.UPSERT,
        observed_at="2026-10-15T00:00:00Z",
        ingested_at="2026-10-15T00:00:00Z",
        payload_schema="animation-fixture-v1",
        raw_object=_raw(),
        source_location="/fixture",
        policy_id=policy.policy_id,
        policy_digest=policy.digest,
    )


def test_madb_mapper_emits_profile_adaptation_character_voice_and_credits() -> None:
    payload = _fixture("madb-animation-work.v1.json")
    mapped = map_madb_animation_record(
        _envelope(
            payload,
            system="madb",
            product="madb-public-data",
            namespace="madb-work",
            record_id=str(payload["id"]),
            policy=madb_rights_profile(),
        )
    )
    fields = {item.predicate for item in mapped.field_assertions}
    relations = {item.predicate for item in mapped.relationship_assertions}

    assert {"release_form", "animation_technique", "animation_tradition"} <= fields
    assert {
        "adaptation_of",
        "character_appearance",
        "voice_performer",
        "production_credit",
    } <= relations
    tradition = next(
        item
        for item in mapped.field_assertions
        if item.predicate == "animation_tradition"
    )
    assert json.loads(tradition.value_json) == "ANIME"
    character = next(
        item
        for item in mapped.relationship_assertions
        if item.predicate == "character_appearance"
    )
    assert character.object.source_id == "MADB-CHAR-0001"


def test_madb_mapper_does_not_guess_unproven_animation_tradition() -> None:
    payload = _fixture("madb-animation-work.v1.json")
    payload["tradition"] = "ANIME"
    mapped = map_madb_animation_record(
        _envelope(
            payload,
            system="madb",
            product="madb-public-data",
            namespace="madb-work",
            record_id=str(payload["id"]),
            policy=madb_rights_profile(),
        )
    )
    tradition = next(
        item
        for item in mapped.field_assertions
        if item.predicate == "animation_tradition"
    )
    assert json.loads(tradition.value_json) == "UNKNOWN"


def test_wikidata_animation_reuses_film_identifiers_and_typed_relations() -> None:
    payload = _fixture("wikidata-animation-work.v1.json")
    mapped = map_wikidata_research_record(
        _envelope(
            payload,
            system="wikidata",
            product="wikidata-json-dump",
            namespace="wikidata-item",
            record_id=str(payload["id"]),
            policy=wikidata_rights_profile(),
        )
    )

    assert mapped.source_node.referent_kind == "EDITORIAL_WORK"
    assert ("imdb-title", "tt1234567") in {
        (item.namespace_id, item.value) for item in mapped.identifier_assertions
    }
    assert {
        "adaptation_of",
        "character_appearance",
        "voice_performer",
        "production_credit",
    } <= {item.predicate for item in mapped.relationship_assertions}


class _Transport:
    def __init__(self) -> None:
        self.responses = [
            MadbHttpResponse(status=429, body=b"{}", headers={}),
            MadbHttpResponse(
                status=200,
                body=b'{"id":"MADB-ANIME-0001"}',
                headers={},
            ),
        ]
        self.urls: list[str] = []

    def get(self, url: str) -> MadbHttpResponse:
        self.urls.append(url)
        return self.responses.pop(0)


def test_madb_client_is_fixed_origin_rate_limited_and_retry_bounded() -> None:
    transport = _Transport()
    sleeps: list[float] = []
    client = MadbPublicClient(
        transport=transport,
        max_attempts=2,
        min_interval_seconds=0.25,
        sleeper=sleeps.append,
    )

    payload, retries, rate_limits = client.fetch_json("/api/animation/1")

    assert payload["id"] == "MADB-ANIME-0001"
    assert retries == 1
    assert rate_limits == 1
    assert transport.urls == [
        "https://mediaarts-db.artmuseums.go.jp/api/animation/1",
        "https://mediaarts-db.artmuseums.go.jp/api/animation/1",
    ]
    assert sleeps == [0.25, 0.25]


def test_madb_archived_response_has_immutable_checkpoint() -> None:
    payload = _fixture("madb-animation-work.v1.json")
    policy = madb_rights_profile()
    pin = build_source_snapshot_pin(
        source_system_id="madb",
        source_product_id="madb-public-data",
        connector_id="madb-public-data-snapshot",
        revision_kind=SnapshotRevisionKind.API_CHECKPOINT,
        source_revision="cursor:animation:2026-10-15T00:00:00Z",
        schema_id="madb-animation-record",
        raw_objects=(_raw("madb-response.json"),),
        captured_at="2026-10-15T00:00:00Z",
    )
    captured = capture_pinned_records(
        pin=pin,
        records=(payload,),
        source_namespace=lambda _: "madb-work",
        source_record_id=lambda item: str(item["id"]),
        policy_id=policy.policy_id,
        policy_digest=policy.digest,
        image_digest="sha256:" + ("1" * 64),
        config_digest="sha256:" + ("2" * 64),
    )

    assert captured.checkpoint.record_count == 1
    assert captured.envelopes[0].raw_object == pin.raw_objects[0]
    assert mapper_identity_for_product("madb-public-data").mapper_version == "1.0.0"
