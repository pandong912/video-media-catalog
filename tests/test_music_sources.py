from __future__ import annotations

import json
from pathlib import Path

import pytest

from video_media_catalog.assertions import SourceNodeRef
from video_media_catalog.canonical import canonical_json
from video_media_catalog.community_sources import build_community_registry
from video_media_catalog.connector import build_connector_record_set_manifest
from video_media_catalog.gold import (
    PredicateKind,
    ResolutionOperator,
    research_policy,
)
from video_media_catalog.identity_resolution import (
    resolve_or_allocate_source_node,
    review_parent_constrained_source_node,
)
from video_media_catalog.identity_v2 import (
    DecisionStatus,
    EntityLevel,
    ParentConstraint,
)
from video_media_catalog.music_sources import map_musicbrainz_core_record
from video_media_catalog.reference_fixture import (
    load_reference_fixture,
    materialize_reference_fixture,
)
from video_media_catalog.source_silver import build_source_silver_rows
from video_media_catalog.source_silver_checkpoint import mapper_identity_for_product

FIXTURE_ROOT = Path("tests/fixtures/research")
TIMESTAMP = "2026-10-17T00:00:00Z"


def _materialize(name: str):
    return materialize_reference_fixture(load_reference_fixture(FIXTURE_ROOT / name))


def test_music_reference_capture_and_mapper_replay_are_immutable() -> None:
    musicbrainz = _materialize("musicbrainz-music-reference.v1.json")
    wikidata = _materialize("wikidata-music-reference.v1.json")

    assert musicbrainz == _materialize("musicbrainz-music-reference.v1.json")
    assert wikidata == _materialize("wikidata-music-reference.v1.json")
    assert musicbrainz.fixture.digest == (
        "sha256:9e10b0b04119f76f9f7073ecdb49fc536be3b2dca39be6267a9e54b2ce50373d"
    )
    assert musicbrainz.captured.pin.pin_id == (
        "sha256:dcdad6140f498eac0a569a8212ec96835fecc33d76666c6b542c65778ac8b7e7"
    )
    assert wikidata.fixture.digest == (
        "sha256:629828cb6f46a2cd68e21d032e95fa39fa8610ac7d7f9294487b50d167eb285c"
    )
    assert wikidata.captured.pin.pin_id == (
        "sha256:946966b7efd21408500156ac7afd6a449a5d8e4f45d5ad22e2a274bc2df0d1fb"
    )
    assert len(musicbrainz.captured.envelopes) == 6
    assert len(wikidata.captured.envelopes) == 2
    assert {
        assertion.provenance.mapper_id
        for mapped in musicbrainz.mapped
        for assertion in mapped.entity_type_assertions
    } == {"musicbrainz-core-source-mapper"}
    assert {
        assertion.provenance.mapper_version
        for mapped in musicbrainz.mapped
        for assertion in mapped.entity_type_assertions
    } == {"1.1.0"}
    assert {
        assertion.provenance.mapper_id
        for mapped in wikidata.mapped
        for assertion in mapped.entity_type_assertions
    } == {"wikidata-music-source-mapper"}
    assert {
        assertion.provenance.mapper_version
        for mapped in wikidata.mapped
        for assertion in mapped.entity_type_assertions
    } == {"1.1.0"}
    assert {
        assertion.entity_type
        for mapped in musicbrainz.mapped
        for assertion in mapped.entity_type_assertions
    } == {
        "MUSIC_ARTIST",
        "MUSIC_LABEL",
        "MUSICAL_WORK",
        "MUSIC_RECORDING",
        "MUSIC_RELEASE_GROUP",
        "MUSIC_RELEASE",
    }
    identifiers = {
        (assertion.namespace_id, assertion.value)
        for fixture in (musicbrainz, wikidata)
        for mapped in fixture.mapped
        for assertion in mapped.identifier_assertions
    }
    assert {
        ("isrc", "GBAYE9400055"),
        ("iswc", "T-010.346.288-9"),
        ("wikidata-item", "Q208909"),
        ("wikidata-item", "Q2469634"),
        ("wikidata-item", "Q44190"),
        (
            "musicbrainz-artist",
            "a74b1b7f-71a5-4011-9441-d0b5e4122711",
        ),
        (
            "musicbrainz-label",
            "df7d1c7f-ef95-425f-8eef-445b3d7bcbd9",
        ),
        (
            "musicbrainz-work",
            "cd9694f3-ade2-3ad6-a6e3-34edeb338928",
        ),
        (
            "musicbrainz-recording",
            "83cf16bc-01db-4766-9e3e-f3c796639cf2",
        ),
        (
            "musicbrainz-release-group",
            "96838f33-94ec-3d55-9147-f158e2247bf3",
        ),
        (
            "musicbrainz-release",
            "08a5dbf1-e22f-471a-a170-280fa8a4f3ba",
        ),
    } <= identifiers
    wikidata_label = next(
        mapped
        for mapped in wikidata.mapped
        if mapped.entity_type_assertions[0].entity_type == "MUSIC_LABEL"
    )
    assert {
        (assertion.namespace_id, assertion.value, assertion.referent_kind)
        for assertion in wikidata_label.identifier_assertions
    } == {
        (
            "musicbrainz-label",
            "df7d1c7f-ef95-425f-8eef-445b3d7bcbd9",
            "MUSIC_LABEL",
        ),
        ("wikidata-item", "Q208909", "MUSIC_LABEL"),
    }
    assert {
        assertion.predicate
        for mapped in musicbrainz.mapped
        for assertion in mapped.relationship_assertions
    } == {
        "recording_of",
        "release_membership",
        "credit",
        "performance_link",
    }
    assert {
        (
            assertion.object.namespace_id,
            assertion.object.source_id,
            assertion.object.referent_kind,
            assertion.qualifiers.get("role"),
        )
        for mapped in musicbrainz.mapped
        for assertion in mapped.relationship_assertions
        if assertion.object.referent_kind == "MUSIC_LABEL"
    } == {
        (
            "musicbrainz-label",
            "df7d1c7f-ef95-425f-8eef-445b3d7bcbd9",
            "MUSIC_LABEL",
            "LABEL",
        )
    }
    assert all(
        assertion.provenance.citation_keys
        for fixture in (musicbrainz, wikidata)
        for mapped in fixture.mapped
        for assertion in (
            *mapped.field_assertions,
            *mapped.identifier_assertions,
            *mapped.relationship_assertions,
            *mapped.entity_type_assertions,
        )
    )


@pytest.mark.parametrize(
    "excluded_key",
    (
        "tags",
        "genres",
        "ratings",
        "annotations",
        "annotationText",
        "supplementary",
        "liveFeed",
        "coverArtArchive",
        "coverArtUrl",
        "folksonomyTags",
    ),
)
def test_musicbrainz_mapper_rejects_non_core_data(excluded_key: str) -> None:
    fixture = _materialize("musicbrainz-music-reference.v1.json")
    envelope = fixture.captured.envelopes[0]
    payload = json.loads(envelope.payload_json or "{}")
    payload[excluded_key] = ["not-core"]
    mutated = envelope.model_copy(update={"payload_json": canonical_json(payload)})

    with pytest.raises(ValueError, match="excluded data"):
        map_musicbrainz_core_record(mutated)


def test_music_capture_pin_reaches_source_silver() -> None:
    fixture = _materialize("musicbrainz-music-reference.v1.json")
    captured = fixture.captured
    record_set = build_connector_record_set_manifest(
        batch_id=captured.batch_manifest.batch_id,
        source_product_id=captured.batch_manifest.source_product_id,
        policy_id=captured.batch_manifest.policy_id,
        policy_digest=captured.batch_manifest.policy_digest,
        record_objects=captured.pin.raw_objects,
        record_count=len(captured.envelopes),
        first_envelope_key=captured.envelopes[0].envelope_key,
        last_envelope_key=captured.envelopes[-1].envelope_key,
        created_at=captured.pin.captured_at,
    )

    run, rows = build_source_silver_rows(
        registry=build_community_registry(),
        batch=captured.batch_manifest,
        record_set=record_set,
        envelopes=captured.envelopes,
    )

    assert run.input_manifest["sourceSnapshotPinId"] == captured.pin.pin_id
    assert len(rows["community_source_record"]) == 6
    assert len(rows["community_entity_type_assertion"]) == 6


def test_music_uses_generic_gold_identifier_and_relationship_rules() -> None:
    policy = research_policy()

    assert mapper_identity_for_product("musicbrainz-core-dump").model_dump() == {
        "mapper_id": "musicbrainz-core-source-mapper",
        "mapper_version": "1.1.0",
    }
    assert mapper_identity_for_product("wikidata-json-dump").model_dump() == {
        "mapper_id": "wikidata-research-dispatcher",
        "mapper_version": "2.1.0",
    }
    for namespace in (
        "musicbrainz-artist",
        "musicbrainz-label",
        "musicbrainz-work",
        "musicbrainz-recording",
        "musicbrainz-release-group",
        "musicbrainz-release",
        "isrc",
        "iswc",
    ):
        assert (
            policy.rule_for(namespace, PredicateKind.IDENTIFIER).operator
            == ResolutionOperator.SET_UNION
        )
    for predicate in (
        "recording_of",
        "release_membership",
        "credit",
        "performance_link",
    ):
        assert (
            policy.rule_for(predicate, PredicateKind.RELATIONSHIP).operator
            == ResolutionOperator.SET_UNION
        )


def test_music_exact_id_parent_review_and_ambiguous_abstention() -> None:
    node = SourceNodeRef(
        namespace_id="isrc",
        source_id="GBAYE9400055",
        referent_kind="MUSIC_RECORDING",
    )
    candidate = "sha256:" + ("1" * 64)
    common = {
        "source_node": node,
        "entity_level": EntityLevel.MUSIC_RECORDING,
        "entity_kind": "MUSIC_RECORDING",
        "assertion_keys": ("sha256:" + ("2" * 64),),
        "observed_at": TIMESTAMP,
        "policy_id": "musicbrainz-core-data-cc0",
        "policy_digest": "sha256:" + ("3" * 64),
        "decision_policy_version": "research-identity-v1",
        "decided_by": "research-identity-resolver",
    }
    exact = resolve_or_allocate_source_node(
        **common,
        exact_candidate_entity_keys=(candidate,),
    )
    ambiguous = resolve_or_allocate_source_node(
        **common,
        exact_candidate_entity_keys=(
            candidate,
            "sha256:" + ("4" * 64),
        ),
    )
    parent = ParentConstraint(
        child_level=EntityLevel.MUSIC_RECORDING,
        parent_level=EntityLevel.MUSICAL_WORK,
        parent_source_node=SourceNodeRef(
            namespace_id="musicbrainz-work",
            source_id="cd9694f3-ade2-3ad6-a6e3-34edeb338928",
            referent_kind="MUSICAL_WORK",
        ),
        parent_entity_key="sha256:" + ("5" * 64),
        parent_membership_key="sha256:" + ("6" * 64),
        relationship_assertion_key="sha256:" + ("7" * 64),
        ordinal_assertion_keys=(),
    )
    review = review_parent_constrained_source_node(
        source_node=node,
        candidate_entity_key=candidate,
        parent_constraint=parent,
        assertion_keys=common["assertion_keys"],
        observed_at=TIMESTAMP,
        policy_id=common["policy_id"],
        policy_digest=common["policy_digest"],
        decision_policy_version=common["decision_policy_version"],
        decided_by=common["decided_by"],
    )

    assert exact.memberships[0].entity_key == candidate
    assert ambiguous.memberships == ()
    assert ambiguous.decisions == ()
    assert ambiguous.conflicts[0].reason == "MULTIPLE_EXACT_IDENTIFIER_CANDIDATES"
    assert review.memberships == ()
    assert review.decisions[0].status == DecisionStatus.UNCERTAIN


def test_music_label_exact_mbid_reuses_unique_candidate() -> None:
    candidate = "sha256:" + ("8" * 64)
    result = resolve_or_allocate_source_node(
        source_node=SourceNodeRef(
            namespace_id="musicbrainz-label",
            source_id="df7d1c7f-ef95-425f-8eef-445b3d7bcbd9",
            referent_kind="MUSIC_LABEL",
        ),
        entity_level=EntityLevel.MUSIC_LABEL,
        entity_kind="MUSIC_LABEL",
        exact_candidate_entity_keys=(candidate,),
        assertion_keys=("sha256:" + ("9" * 64),),
        observed_at=TIMESTAMP,
        policy_id="musicbrainz-core-data-cc0",
        policy_digest="sha256:" + ("a" * 64),
        decision_policy_version="research-identity-v1",
        decided_by="research-identity-resolver",
    )

    assert result.memberships[0].entity_key == candidate
    assert result.entities == ()
