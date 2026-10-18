from __future__ import annotations

import pytest

from video_media_catalog.assertions import SourceNodeRef, ValueType
from video_media_catalog.community_sources import (
    build_community_registry,
    openfootball_rights_profile,
)
from video_media_catalog.connector import (
    RecordOperation,
    build_connector_record_envelope,
)
from video_media_catalog.identity_v2 import EntityLevel, ParentConstraint
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.predicate_registry import (
    PredicateKind,
    PredicateResolutionOperator,
    ResearchDomain,
    load_research_predicate_registry,
)
from video_media_catalog.source_mapper import AssertionBuilder


def _raw() -> ObjectRef:
    return ObjectRef(
        uri="file:///tmp/openfootball-fixture.json",
        format="OBJECT_FORMAT_JSON",
        media_type="application/json",
        checksum=Checksum(value="a" * 64),
        size_bytes=128,
    )


def _envelope(*, revision: str):
    policy = openfootball_rights_profile()
    return build_connector_record_envelope(
        payload={"id": "eng.1/2025/match-1", "revision": revision},
        batch_id="sha256:" + ("b" * 64),
        source_system_id="openfootball",
        source_product_id="openfootball-git",
        source_namespace_id="openfootball-event",
        source_record_id="eng.1/2025/match-1",
        source_revision=revision,
        operation=RecordOperation.UPSERT,
        observed_at=f"2026-10-{revision.zfill(2)}T00:00:00Z",
        ingested_at=f"2026-10-{revision.zfill(2)}T00:00:00Z",
        payload_schema="openfootball-event",
        raw_object=_raw(),
        source_location="/events/0",
        policy_id=policy.policy_id,
        policy_digest=policy.digest,
    )


def test_research_predicate_registry_is_versioned_and_complete() -> None:
    registry = load_research_predicate_registry()

    assert registry.schema_version == "1.0"
    assert registry.digest.startswith("sha256:")
    score = registry.definition_for("score")
    assert score.domains == (ResearchDomain.SPORTS,)
    assert score.value_type == ValueType.JSON
    assert score.temporal
    assert score.resolution_operator == PredicateResolutionOperator.KEEP_ALL
    voice = registry.definition_for("voice_performer")
    assert voice.kind == PredicateKind.RELATIONSHIP
    assert voice.domains == (ResearchDomain.ANIMATION,)
    for predicate in (
        "recording_of",
        "release_membership",
        "credit",
        "performance_link",
    ):
        definition = registry.definition_for(predicate)
        assert definition.kind == PredicateKind.RELATIONSHIP
        assert definition.domains == (ResearchDomain.MUSIC,)
    with pytest.raises(ValueError, match="unregistered"):
        registry.definition_for("unreviewed_fact")


def test_silver_observation_key_is_stable_across_source_revisions() -> None:
    node = SourceNodeRef(
        namespace_id="openfootball-event",
        source_id="eng.1/2025/match-1",
        referent_kind="SPORTS_EVENT",
    )
    first = AssertionBuilder(
        envelope=_envelope(revision="1"),
        source_node=node,
        mapper_id="openfootball-source-mapper",
        mapper_version="1.0.0",
    )
    first.add_field("score", ValueType.JSON, {"home": 1, "away": 0}, "/score")
    second = AssertionBuilder(
        envelope=_envelope(revision="2"),
        source_node=node,
        mapper_id="openfootball-source-mapper",
        mapper_version="1.0.0",
    )
    second.add_field(
        "score",
        ValueType.JSON,
        {"home": 1, "away": 1},
        "/score",
        supersedes_assertion_id=first.fields[0].assertion_id,
    )

    assert (
        first.fields[0].provenance.observation_key
        == second.fields[0].provenance.observation_key
    )
    assert (
        second.fields[0].provenance.supersedes_assertion_id
        == first.fields[0].assertion_id
    )
    assert first.fields[0].assertion_id != second.fields[0].assertion_id


def test_sports_parent_constraint_requires_disambiguating_fact() -> None:
    common = {
        "child_level": EntityLevel.SPORTS_EVENT,
        "parent_level": EntityLevel.SPORTS_SEASON,
        "parent_source_node": SourceNodeRef(
            namespace_id="openfootball-competition",
            source_id="eng.1/2025",
            referent_kind="SPORTS_SEASON",
        ),
        "parent_entity_key": "sha256:" + ("1" * 64),
        "parent_membership_key": "sha256:" + ("2" * 64),
        "relationship_assertion_key": "sha256:" + ("3" * 64),
        "ordinal_assertion_keys": ("sha256:" + ("4" * 64),),
    }
    with pytest.raises(ValueError, match="date, round, or participant"):
        ParentConstraint(**common)

    constrained = ParentConstraint(**common, event_date="2026-10-15")
    assert constrained.child_level == EntityLevel.SPORTS_EVENT


def test_research_source_products_pin_schema_contracts() -> None:
    registry = build_community_registry()
    schemas = {item.schema_id for item in registry.schema_contracts}
    products = {item.source_product_id: item for item in registry.source_products}

    assert {
        "wikidata-research-entity",
        "wikidata-global-reference-entity",
        "openfootball-event",
        "statsbomb-open-data",
        "madb-animation-record",
        "musicbrainz-core-entity",
    } == schemas
    assert products["wikidata-json-dump"].schema_contract_ids == (
        "wikidata-global-reference-entity",
        "wikidata-research-entity",
    )
    assert products["wikidata-music-reference"].schema_contract_ids == (
        "wikidata-global-reference-entity",
    )
    assert products["openfootball-git"].schema_contract_ids == ("openfootball-event",)
    assert products["statsbomb-open-data"].schema_contract_ids == (
        "statsbomb-open-data",
    )
    assert products["musicbrainz-core-dump"].schema_contract_ids == (
        "musicbrainz-core-entity",
    )
