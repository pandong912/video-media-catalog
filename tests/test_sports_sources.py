from __future__ import annotations

import json
from pathlib import Path

import pytest

from video_media_catalog.community_sources import (
    openfootball_rights_profile,
    statsbomb_research_rights_profile,
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
from video_media_catalog.sports_sources import (
    map_openfootball_record,
    map_statsbomb_match_record,
    map_wikidata_sports_record,
)

FIXTURES = Path(__file__).parent / "fixtures/research"


def _fixture(name: str) -> dict[str, object]:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _raw(name: str = "sports-fixture.json") -> ObjectRef:
    return ObjectRef(
        uri=f"file:///tmp/{name}",
        format="OBJECT_FORMAT_JSON",
        media_type="application/json",
        checksum=Checksum(value="a" * 64),
        size_bytes=512,
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
    revision: str = "1",
):
    return build_connector_record_envelope(
        payload=payload,
        batch_id="sha256:" + ("b" * 64),
        source_system_id=system,
        source_product_id=product,
        source_namespace_id=namespace,
        source_record_id=record_id,
        source_revision=revision,
        operation=RecordOperation.UPSERT,
        observed_at="2026-10-15T00:00:00Z",
        ingested_at="2026-10-15T00:00:00Z",
        payload_schema="research-sports-fixture-v1",
        raw_object=_raw(),
        source_location="/fixture",
        policy_id=policy.policy_id,
        policy_digest=policy.digest,
    )


def test_git_snapshot_capture_is_pinned_checkpointed_and_replayable() -> None:
    payload = _fixture("openfootball-team-event.v1.json")
    policy = openfootball_rights_profile()
    pin = build_source_snapshot_pin(
        source_system_id="openfootball",
        source_product_id="openfootball-git",
        connector_id="openfootball-git-snapshot",
        revision_kind=SnapshotRevisionKind.GIT_COMMIT,
        source_revision="1" * 40,
        schema_id="openfootball-event",
        raw_objects=(_raw("openfootball.json"),),
        captured_at="2026-10-15T00:00:00Z",
    )
    values = {
        "pin": pin,
        "records": (payload,),
        "source_namespace": lambda _: "openfootball-event",
        "source_record_id": lambda item: str(item["id"]),
        "policy_id": policy.policy_id,
        "policy_digest": policy.digest,
        "image_digest": "sha256:" + ("c" * 64),
        "config_digest": "sha256:" + ("d" * 64),
    }

    first = capture_pinned_records(**values)
    replay = capture_pinned_records(**values)

    assert first == replay
    assert first.batch_manifest.watermark_after == "1" * 40
    assert first.envelopes[0].source_revision == "1" * 40
    assert first.checkpoint.last_record_id == payload["id"]
    with pytest.raises(ValueError, match="full 40 or 64"):
        build_source_snapshot_pin(
            **{
                **pin.model_dump(mode="python", exclude={"pin_id"}),
                "source_revision": "short",
            }
        )


def test_openfootball_mapper_preserves_team_score_and_correction_history() -> None:
    payload = _fixture("openfootball-team-event.v1.json")
    policy = openfootball_rights_profile()
    first = map_openfootball_record(
        _envelope(
            payload,
            system="openfootball",
            product="openfootball-git",
            namespace="openfootball-event",
            record_id=str(payload["id"]),
            policy=policy,
        )
    )
    score = next(item for item in first.field_assertions if item.predicate == "score")
    corrected = dict(payload)
    corrected["score"] = {"home": 2, "away": 2}
    corrected["supersedes"] = {"score": score.assertion_id}
    second = map_openfootball_record(
        _envelope(
            corrected,
            system="openfootball",
            product="openfootball-git",
            namespace="openfootball-event",
            record_id=str(payload["id"]),
            policy=policy,
            revision="2",
        )
    )
    corrected_score = next(
        item for item in second.field_assertions if item.predicate == "score"
    )

    assert (
        len(
            [
                item
                for item in first.relationship_assertions
                if item.predicate == "participant"
            ]
        )
        == 2
    )
    assert corrected_score.provenance.supersedes_assertion_id == score.assertion_id
    assert (
        corrected_score.provenance.observation_key == score.provenance.observation_key
    )


def test_wikidata_sports_mapper_covers_set_and_ranked_structures() -> None:
    policy = wikidata_rights_profile()
    set_payload = _fixture("wikidata-set-event.v1.json")
    set_envelope = _envelope(
        set_payload,
        system="wikidata",
        product="wikidata-json-dump",
        namespace="wikidata-item",
        record_id=str(set_payload["id"]),
        policy=policy,
    )
    set_event = map_wikidata_sports_record(set_envelope)
    ranked_payload = _fixture("wikidata-ranked-entry.v1.json")
    ranked_entry = map_wikidata_research_record(
        _envelope(
            ranked_payload,
            system="wikidata",
            product="wikidata-json-dump",
            namespace="wikidata-item",
            record_id=str(ranked_payload["id"]),
            policy=policy,
        )
    )

    assert set_event.source_node.referent_kind == "SPORTS_EVENT"
    assert any(item.predicate == "set_score" for item in set_event.field_assertions)
    assert (
        len(
            [
                item
                for item in set_event.relationship_assertions
                if item.predicate == "participant"
            ]
        )
        == 2
    )
    assert ranked_entry.source_node.referent_kind == "SPORTS_ENTRY"
    assert any(item.predicate == "placement" for item in ranked_entry.field_assertions)
    assert any(
        item.predicate == "part_of" for item in ranked_entry.relationship_assertions
    )


def test_statsbomb_mapper_keeps_research_detail_outside_identity_fields() -> None:
    payload = _fixture("statsbomb-match.v1.json")
    policy = statsbomb_research_rights_profile()
    mapped = map_statsbomb_match_record(
        _envelope(
            payload,
            system="statsbomb",
            product="statsbomb-open-data",
            namespace="statsbomb-match",
            record_id=str(payload["match_id"]),
            policy=policy,
        )
    )
    fields = {item.predicate for item in mapped.field_assertions}

    assert {"score", "match_statistics", "lineup", "freeze_frame"} <= fields
    assert {item.predicate for item in mapped.relationship_assertions} >= {
        "part_of_competition",
        "part_of_sports_season",
        "participant",
        "held_at",
    }
    assert mapper_identity_for_product("openfootball-git").mapper_version == "1.0.0"
    assert mapper_identity_for_product("statsbomb-open-data").mapper_version == "1.0.0"
