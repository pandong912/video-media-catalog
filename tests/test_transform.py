from __future__ import annotations

from pathlib import Path

from video_media_catalog.eidr import iter_eidr_records
from video_media_catalog.transform import build_curated_rows
from video_media_catalog.wikidata import iter_wikidata_records


def _records(fixture_dir: Path):
    return [
        *iter_wikidata_records(fixture_dir / "wikidata.json"),
        *iter_eidr_records(fixture_dir / "eidr.xml"),
    ]


def test_type_closure_credits_multilingual_names_and_external_ids(
    fixture_dir: Path,
) -> None:
    curated = build_curated_rows(_records(fixture_dir))
    entities = {
        (row["canonical_source"], row["canonical_source_id"]): row
        for row in curated.catalog_entity
    }

    assert entities[("wikidata", "Q1001")]["entity_type"] == "MOVIE"
    assert entities[("wikidata", "Q1002")]["entity_type"] == "TV_SERIES"
    assert entities[("wikidata", "Q1003")]["entity_type"] == "TV_SEASON"
    assert entities[("wikidata", "Q1004")]["entity_type"] == "TV_EPISODE"
    assert entities[("wikidata", "Q2001")]["entity_type"] == "PERSON"
    assert entities[("wikidata", "Q3001")]["entity_type"] == "ORGANIZATION"
    assert (
        entities[("eidr", "10.5240/1111-1111-1111-1111-1111-A")]["entity_type"]
        == "TV_SERIES"
    )
    assert (
        entities[("eidr", "10.5240/2222-2222-2222-2222-2222-A")]["entity_type"]
        == "TV_SEASON"
    )
    assert (
        entities[("eidr", "10.5240/3333-3333-3333-3333-3333-A")]["entity_type"]
        == "TV_EPISODE"
    )

    movie_key = entities[("wikidata", "Q1001")]["entity_key"]
    movie_names = {
        (row["name_type"], row["language"], row["value"], row["source"])
        for row in curated.catalog_name
        if row["entity_key"] == movie_key
    }
    assert ("PRIMARY", "zh", "示例电影", "wikidata") in movie_names
    assert ("ALIAS", "en", "The Example Film", "wikidata") in movie_names
    assert ("TITLE", "fr", "Film exemple", "wikidata") in movie_names
    assert ("PRIMARY", "en", "Example Movie EIDR", "eidr") in movie_names

    movie_identifiers = {
        (row["scheme"], row["value"], row["source"])
        for row in curated.catalog_external_identifier
        if row["entity_key"] == movie_key
    }
    assert ("wikidata", "Q1001", "wikidata") in movie_identifiers
    assert (
        "eidr",
        "10.5240/AAAA-BBBB-CCCC-DDDD-EEEE-C",
        "wikidata",
    ) in movie_identifiers
    assert ("imdb", "tt0000001", "eidr") in movie_identifiers
    assert ("douban", "1295644", "wikidata") in movie_identifiers

    credit_ordinals = {
        (row["relation_type"], row["ordinal"])
        for row in curated.catalog_relation
        if row["subject_entity_key"] == movie_key
    }
    assert ("DIRECTED_BY", "1") in credit_ordinals
    assert ("CAST_MEMBER", "2") in credit_ordinals


def test_exact_eidr_merge_conflict_and_parent_relations(fixture_dir: Path) -> None:
    curated = build_curated_rows(_records(fixture_dir))
    entities = {
        (row["canonical_source"], row["canonical_source_id"]): row
        for row in curated.catalog_entity
    }

    assert ("eidr", "10.5240/AAAA-BBBB-CCCC-DDDD-EEEE-C") not in entities
    assert ("eidr", "10.5240/FFFF-EEEE-DDDD-CCCC-BBBB-A") in entities
    assert any(
        row["error_code"] == "EXACT_IDENTIFIER_CONFLICT"
        and row["source_record_id"] == "10.5240/FFFF-EEEE-DDDD-CCCC-BBBB-A"
        for row in curated.catalog_ingest_error
    )
    relation_types = {row["relation_type"] for row in curated.catalog_relation}
    assert {"PART_OF_SERIES", "PART_OF_SEASON", "EDIT_OF"} <= relation_types


def test_transform_is_independent_of_input_order(fixture_dir: Path) -> None:
    records = _records(fixture_dir)
    forward = build_curated_rows(records).tables()
    reverse = build_curated_rows(list(reversed(records))).tables()
    assert forward == reverse
    assert all("tenant_id" not in row for rows in forward.values() for row in rows)
