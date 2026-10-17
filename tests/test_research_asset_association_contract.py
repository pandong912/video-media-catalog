from __future__ import annotations

import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONTRACT = (
    ROOT / "contracts" / "fixtures" / "research_catalog_asset_association.v1.json"
)
SOURCE_FIXTURES = ROOT / "tests" / "fixtures" / "research"
POLICY = ROOT / "contracts" / "catalog" / "catalog-source-policy.v1.json"
CONTRACT_SHA256 = "3ecf37b6a44cf7ad269e5fd4931c730199f072bacb738ea672277f9ec416a5d5"


def _json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def test_asset_association_fixture_locks_the_cross_repository_pipeline() -> None:
    contract = _json(CONTRACT)

    assert hashlib.sha256(CONTRACT.read_bytes()).hexdigest() == CONTRACT_SHA256
    assert contract["contractId"] == (
        "video-world.research-catalog.asset-association.v1"
    )
    assert contract["pipelineStages"] == [
        "RAW",
        "SILVER_ASSERTIONS",
        "IDENTITY",
        "GOLD",
        "CONCRETE_INDEX",
        "EXACT_CANDIDATE",
        "CANONICAL_IDENTITY",
        "CONFIRMED_ASSET_LINK",
        "QUERY_API",
        "RESEARCH_CONSOLE",
    ]
    assert contract["requiredGates"] == {
        "validatedConcreteIndex": True,
        "uniqueExactExternalId": True,
        "score": 1000,
        "confidence": 1.0,
        "goldConflictCount": 0,
        "materializationAllowed": True,
        "existingConfirmedLink": False,
    }
    assert all(value is False for value in contract["closedGates"].values())


def test_contract_exact_identifiers_are_backed_by_source_fixtures_and_policy() -> None:
    contract = _json(CONTRACT)
    policy = _json(POLICY)
    namespaces = {
        alias
        for source in policy["sources"]
        if "MATERIALIZATION" in source["capabilities"]
        for namespace in source["namespaces"]
        for alias in [namespace["namespace"], *namespace.get("aliases", [])]
    }
    domains = {item["domain"]: item for item in contract["domains"]}

    sports = domains["SPORTS"]
    sports_source = _json(SOURCE_FIXTURES / sports["sourceFixture"])
    assert sports_source["id"] == sports["exactIdentifier"]["value"]
    assert sports["exactIdentifier"]["namespace"] in namespaces
    assert sports["canonicalEntityKind"] == "EVENT"
    assert sports["assetRelationKind"] == "BROADCAST_OF"

    animation = domains["ANIMATION"]
    animation_source = _json(SOURCE_FIXTURES / animation["sourceFixture"])
    assert animation_source["id"] == animation["exactIdentifier"]["value"]
    assert animation["exactIdentifier"]["namespace"] in namespaces
    assert animation["canonicalEntityKind"] == "WORK"
    assert animation["assetRelationKind"] == "PRESENTATION_OF"

    music = domains["MUSIC"]
    music_source = _json(SOURCE_FIXTURES / music["sourceFixture"])
    recording = next(
        item["payload"]
        for item in music_source["records"]
        if item["payload"]["entityType"] == "MUSIC_RECORDING"
    )
    assert music["exactIdentifier"]["value"] in recording["isrcs"]
    assert music["exactIdentifier"]["namespace"] in namespaces
    assert music["canonicalEntityKind"] == "MUSIC_RECORDING"
    assert music["assetRelationKind"] == "RECORDING_OF"
    label = next(
        item["payload"]
        for item in music_source["records"]
        if item["payload"]["entityType"] == "MUSIC_LABEL"
    )
    assert label["id"] == "df7d1c7f-ef95-425f-8eef-445b3d7bcbd9"
    assert "musicbrainz.label" in namespaces
    assert "musicbrainz-label" in namespaces

    assert set(contract["reviewOnlyCases"]) == {
        "PARENT_CONSTRAINED",
        "MULTIPLE_EXACT_CANDIDATES",
        "GOLD_CONFLICT",
        "EXISTING_CONFIRMED_LINK",
    }
