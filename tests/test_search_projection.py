from __future__ import annotations

import json

from video_media_catalog.search_projection import (
    normalize_attributes,
    project_entity,
    select_display_name,
)


def test_display_name_uses_fixed_language_fallback_before_name_type() -> None:
    names = [
        {"nameType": "PRIMARY", "language": "en", "value": "English"},
        {"nameType": "PRIMARY", "language": "zh", "value": "中文"},
        {"nameType": "TITLE", "language": "zh-Hans", "value": "简体标题"},
    ]

    assert select_display_name(names, fallback="Q1") == ("简体标题", "zh-hans")
    assert select_display_name([], fallback="Q1") == ("Q1", "und")


def test_display_name_other_languages_are_deterministic() -> None:
    names = [
        {"nameType": "PRIMARY", "language": "fr", "value": "Français"},
        {"nameType": "PRIMARY", "language": "de", "value": "Deutsch"},
    ]

    assert select_display_name(names, fallback="Q1") == ("Deutsch", "de")


def test_attributes_extract_descriptions_sitelinks_and_core_values() -> None:
    attributes = json.dumps(
        {
            "sources": [
                {
                    "source": "wikidata",
                    "source_record_id": "Q1",
                    "attributes_json": json.dumps(
                        {
                            "descriptions": {
                                "en": {"language": "en", "value": "A film"}
                            },
                            "sitelinks": {
                                "enwiki": {
                                    "title": "A Film",
                                    "badges": ["Q17437796"],
                                }
                            },
                            "releaseDates": [{"time": "+2020-01-01T00:00:00Z"}],
                            "genres": ["Q130232"],
                        }
                    ),
                },
                {
                    "source": "eidr",
                    "source_record_id": "10.5240/TEST",
                    "attributes_json": json.dumps(
                        {
                            "releaseDate": "2020-01-01",
                            "duration": "PT90M",
                        }
                    ),
                },
            ]
        }
    )

    descriptions, sitelinks, core = normalize_attributes(attributes)

    assert descriptions == [{"language": "en", "value": "A film"}]
    assert sitelinks[0]["site"] == "enwiki"
    assert core["releaseDates"] == [
        "2020-01-01",
        '{"time":"+2020-01-01T00:00:00Z"}',
    ]
    assert core["durations"] == ["PT90M"]


def test_project_entity_is_stable_and_includes_lineage_and_parents() -> None:
    row = {
        "entity_key": "sha256:" + "1" * 64,
        "entity_type": "TV_EPISODE",
        "canonical_source": "wikidata",
        "canonical_source_id": "Q1",
        "attributes_json": "{}",
        "names": [
            {
                "name_type": "PRIMARY",
                "language": "en",
                "value": "Episode",
                "source": "wikidata",
                "source_record_id": "Q1",
            }
        ],
        "external_identifiers": [
            {
                "scheme": "imdb",
                "value": "tt0000001",
                "source": "wikidata",
                "source_record_id": "Q1",
            }
        ],
        "relations": [
            {
                "relation_key": "sha256:" + "2" * 64,
                "relation_type": "PART_OF_SERIES",
                "object_entity_key": "sha256:" + "3" * 64,
                "ordinal": None,
                "source": "wikidata",
                "source_record_id": "Q1",
            }
        ],
        "source_records": [
            {
                "record_key": "sha256:" + "4" * 64,
                "source": "wikidata",
                "source_record_id": "Q1",
            }
        ],
    }

    projected = project_entity(row)

    assert projected["displayName"] == "Episode"
    assert projected["parentKeys"] == ["sha256:" + "3" * 64]
    assert projected["relationSummary"] == [
        {"relationType": "PART_OF_SERIES", "count": 1}
    ]
    assert projected["sourceRecordIds"] == ["Q1"]
    assert project_entity(row) == projected
