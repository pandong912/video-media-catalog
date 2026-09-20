from __future__ import annotations

from video_media_catalog.canonical import canonical_json
from video_media_catalog.gold_search_projection import (
    MAX_TITLES,
    project_gold_entity,
)


def _trace(assertion_id: str, *, source_path: str) -> str:
    return canonical_json(
        {
            "assertions": [
                {
                    "assertionId": assertion_id,
                    "sourceProductId": "tvmaze-public-api",
                    "sourceName": "TVmaze public API",
                    "sourceRecordId": "1",
                    "sourcePath": source_path,
                    "observedAt": "2026-09-19T00:00:00Z",
                    "citationKeys": ["sha256:" + ("9" * 64)],
                    "rights": {
                        "policyId": "tvmaze-api-cc-by-sa",
                        "policyZone": "open_sharealike",
                        "licenseId": "CC-BY-SA-version-unspecified",
                        "attributionText": "TV data provided by TVmaze.",
                        "sourceUrl": "https://www.tvmaze.com/api",
                        "shareAlike": True,
                    },
                }
            ]
        }
    )


def _lineage_fields(index: int, *, source_path: str) -> dict[str, str | None]:
    assertion_id = "sha256:" + f"{index:064x}"
    return {
        "selected_assertion_id": assertion_id,
        "assertion_ids_json": canonical_json([assertion_id]),
        "trace_json": _trace(assertion_id, source_path=source_path),
    }


def test_gold_projection_is_bounded_and_locale_aware() -> None:
    titles = [
        {
            "predicate": "title",
            "value_json": f'"Title {index}"',
            "qualifiers_json": ('{"language":"en","titleRole":"ALIAS"}'),
            "resolution_status": "SET",
            **_lineage_fields(index + 1, source_path=f"/titles/{index}"),
            "selected_assertion_id": None,
        }
        for index in range(MAX_TITLES + 2)
    ]
    titles.append(
        {
            "predicate": "title",
            "value_json": '"中文标题"',
            "qualifiers_json": ('{"language":"zh-hans","titleRole":"PRIMARY"}'),
            "resolution_status": "SELECTED",
            **_lineage_fields(1000, source_path="/name"),
        }
    )
    row = {
        "entity_key": "sha256:" + ("a" * 64),
        "entity_level": "SERIES",
        "entity_kind": "TV_SERIES",
        "status": "ACTIVE",
        "release_plan_id": "sha256:" + ("b" * 64),
        "source_node_count": 2,
        "fields": [
            *titles,
            {
                "predicate": "genre",
                "value_json": '"Drama"',
                "qualifiers_json": '{"vocabulary":"tvmaze"}',
                "resolution_status": "SET",
                **_lineage_fields(1001, source_path="/genres/0"),
                "selected_assertion_id": None,
            },
            {
                "predicate": "runtime_minutes",
                "value_json": "45",
                "qualifiers_json": "{}",
                "resolution_status": "SELECTED",
                **_lineage_fields(1002, source_path="/runtime"),
            },
        ],
        "identifiers": [
            {
                "namespace_id": "imdb-title",
                "value": "tt0000001",
                "issuer": "IMDb",
                "referent_kind": "SERIES",
                **_lineage_fields(1003, source_path="/externals/imdb"),
                "selected_assertion_id": None,
            }
        ],
        "relation_summary": [{"predicate": "episode_of", "count": 3}],
        "conflict_count": 1,
        "conflicts": [
            {
                "predicate": "status",
                "scope_hash": "sha256:" + ("8" * 64),
                "reason": "MULTIPLE_ELIGIBLE_VALUES",
                "assertion_ids_json": canonical_json(
                    ["sha256:" + f"{1004:064x}"]
                ),
                "candidate_values_json": '["Ended","Running"]',
                "trace_json": _trace(
                    "sha256:" + f"{1004:064x}",
                    source_path="/status",
                ),
            }
        ],
    }
    document = project_gold_entity(row)
    assert document["displayName"] == "中文标题"
    assert document["displayLanguage"] == "zh-hans"
    assert len(document["titles"]) == MAX_TITLES
    assert document["overflow"]["titles"] == 3
    assert document["attributes"]["genres"] == ["Drama"]
    assert document["attributes"]["runtimeMinutes"] == ["45"]
    assert document["externalIdentifiers"][0]["value"] == "tt0000001"
    assert document["conflictPredicates"] == ["status"]
    assert document["contextId"] == "personal-research"
    assert document["sourceBadges"][0]["sourceProductId"] == "tvmaze-public-api"
    assert document["winningAssertions"][0]["citationKeys"]
    assert document["rights"][0]["attributionText"].startswith("TV data")
    assert document["conflicts"][0]["candidateValuesJson"] == [
        '"Ended"',
        '"Running"',
    ]
