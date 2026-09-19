from __future__ import annotations

from video_media_catalog.gold_search_projection import (
    MAX_TITLES,
    project_gold_entity,
)


def test_gold_projection_is_bounded_and_locale_aware() -> None:
    titles = [
        {
            "predicate": "title",
            "value_json": f'"Title {index}"',
            "qualifiers_json": ('{"language":"en","titleRole":"ALIAS"}'),
            "resolution_status": "SET",
        }
        for index in range(MAX_TITLES + 2)
    ]
    titles.append(
        {
            "predicate": "title",
            "value_json": '"中文标题"',
            "qualifiers_json": ('{"language":"zh-hans","titleRole":"PRIMARY"}'),
            "resolution_status": "SELECTED",
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
            },
            {
                "predicate": "runtime_minutes",
                "value_json": "45",
                "qualifiers_json": "{}",
                "resolution_status": "SELECTED",
            },
        ],
        "identifiers": [
            {
                "namespace_id": "imdb-title",
                "value": "tt0000001",
                "issuer": "IMDb",
                "referent_kind": "SERIES",
            }
        ],
        "relation_summary": [{"predicate": "episode_of", "count": 3}],
        "conflict_count": 1,
        "conflicts": [{"predicate": "status"}],
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
