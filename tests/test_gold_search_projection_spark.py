from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from video_media_catalog.canonical import canonical_json
from video_media_catalog.gold_search_projection import (
    build_gold_search_projection,
)
from video_media_catalog.gold_spark import create_gold_dataframes
from video_media_catalog.gold_tables import GOLD_DATA_COLUMNS


def _trace(assertion_id: str, source_path: str) -> str:
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
                    "rights": {
                        "policyId": "tvmaze-api-cc-by-sa",
                        "policyZone": "open_sharealike",
                        "licenseId": "CC-BY-SA",
                        "attributionText": "TV data provided by TVmaze.",
                        "sourceUrl": "https://www.tvmaze.com/api",
                    },
                }
            ]
        }
    )


def _wikidata_trace(assertion_id: str, source_path: str) -> str:
    return canonical_json(
        {
            "assertions": [
                {
                    "assertionId": assertion_id,
                    "sourceProductId": "wikidata-json-dump",
                    "sourceName": "Wikidata JSON entity dump",
                    "sourceRecordId": "Q135249048",
                    "sourcePath": source_path,
                    "observedAt": "2026-10-15T00:00:00Z",
                    "citationKeys": ["sha256:" + ("8" * 64)],
                    "rights": {
                        "policyId": "wikidata-structured-data-cc0",
                        "policyZone": "open_cc0",
                        "licenseId": "CC0-1.0",
                        "licenseUri": (
                            "https://creativecommons.org/publicdomain/zero/1.0/"
                        ),
                        "attributionText": "Wikidata structured data (CC0).",
                        "sourceUrl": "https://www.wikidata.org/",
                        "shareAlike": False,
                    },
                }
            ]
        }
    )


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("community-gold-search-projection-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.mark.spark
def test_distributed_gold_search_projection(spark: SparkSession) -> None:
    plan_id = "sha256:" + ("a" * 64)
    entity_key = "sha256:" + ("b" * 64)
    rows = {table: [] for table in GOLD_DATA_COLUMNS}
    rows["community_gold_entity"] = [
        {
            "row_key": "sha256:" + ("c" * 64),
            "release_plan_id": plan_id,
            "entity_key": entity_key,
            "entity_level": "SERIES",
            "entity_kind": "TV_SERIES",
            "status": "ACTIVE",
            "source_node_count": 1,
            "trace_json": "{}",
        }
    ]
    rows["community_gold_field"] = [
        {
            "resolution_key": "sha256:" + ("d" * 64),
            "release_plan_id": plan_id,
            "entity_key": entity_key,
            "predicate": "title",
            "scope_hash": "sha256:" + ("e" * 64),
            "value_type": "STRING",
            "value_json": '"Example"',
            "qualifiers_json": ('{"language":"en","titleRole":"PRIMARY"}'),
            "resolution_status": "SELECTED",
            "selected_assertion_id": "sha256:" + ("f" * 64),
            "assertion_ids_json": '["sha256:' + ("f" * 64) + '"]',
            "trace_json": _trace("sha256:" + ("f" * 64), "/name"),
        }
    ]
    rows["community_gold_identifier"] = [
        {
            "resolution_key": "sha256:" + ("1" * 64),
            "release_plan_id": plan_id,
            "entity_key": entity_key,
            "namespace_id": "tvmaze-show",
            "value": "1",
            "issuer": "TVmaze",
            "referent_kind": "SERIES",
            "assertion_ids_json": '["sha256:' + ("2" * 64) + '"]',
            "trace_json": _trace("sha256:" + ("2" * 64), "/id"),
        }
    ]
    frames = create_gold_dataframes(spark, rows)
    documents = build_gold_search_projection(
        spark,
        gold_tables=frames,
        release_plan_id=plan_id,
    ).collect()
    assert len(documents) == 1
    assert documents[0].entityKey == entity_key
    assert documents[0].displayName == "Example"
    assert documents[0].contextId == "research"
    assert "ownerSubject" not in documents[0].asDict()
    assert documents[0].sourceBadges[0].sourceProductId == "tvmaze"
    assert documents[0].externalIdentifiers[0].sourceProductId == "tvmaze"
    assert documents[0].externalIdentifiers[0].assertionId == ("sha256:" + ("2" * 64))

    affected = spark.createDataFrame([(entity_key,)], "entity_key string")
    filtered = build_gold_search_projection(
        spark,
        gold_tables=frames,
        release_plan_id=plan_id,
        affected_entity_keys=affected,
    ).collect()
    assert [document.entityKey for document in filtered] == [entity_key]

    unrelated = spark.createDataFrame(
        [("sha256:" + ("9" * 64),)],
        "entity_key string",
    )
    assert (
        build_gold_search_projection(
            spark,
            gold_tables=frames,
            release_plan_id=plan_id,
            affected_entity_keys=unrelated,
        ).count()
        == 0
    )


@pytest.mark.spark
def test_distributed_projection_derives_transitive_episode_hierarchy(
    spark: SparkSession,
) -> None:
    plan_id = "sha256:" + ("a" * 64)
    episode_key = "sha256:" + ("1" * 64)
    season_key = "sha256:" + ("2" * 64)
    series_key = "sha256:" + ("3" * 64)
    ambiguous_episode_key = "sha256:" + ("4" * 64)
    alternate_season_key = "sha256:" + ("5" * 64)
    rows = {table: [] for table in GOLD_DATA_COLUMNS}
    rows["community_gold_entity"] = [
        {
            "row_key": "sha256:" + f"{index + 10:064x}",
            "release_plan_id": plan_id,
            "entity_key": entity_key,
            "entity_level": entity_level,
            "entity_kind": entity_kind,
            "status": "ACTIVE",
            "source_node_count": 1,
            "trace_json": "{}",
        }
        for index, (entity_key, entity_level, entity_kind) in enumerate(
            (
                (episode_key, "EPISODE", "TV_EPISODE"),
                (season_key, "SEASON", "TV_SEASON"),
                (series_key, "SERIES", "TV_SERIES"),
                (ambiguous_episode_key, "EPISODE", "TV_EPISODE"),
                (alternate_season_key, "SEASON", "TV_SEASON"),
            )
        )
    ]
    rows["community_gold_relation"] = [
        {
            "resolution_key": "sha256:" + f"{index + 20:064x}",
            "release_plan_id": plan_id,
            "subject_entity_key": subject,
            "predicate": predicate,
            "object_entity_key": target,
            "qualifiers_json": "{}",
            "assertion_ids_json": '["sha256:' + f"{index + 30:064x}" + '"]',
            "trace_json": _trace(
                "sha256:" + f"{index + 30:064x}",
                f"/relations/{index}",
            ),
        }
        for index, (subject, predicate, target) in enumerate(
            (
                (episode_key, "part_of_season", season_key),
                (season_key, "part_of_series", series_key),
                (
                    ambiguous_episode_key,
                    "part_of_season",
                    season_key,
                ),
                (
                    ambiguous_episode_key,
                    "part_of_season",
                    alternate_season_key,
                ),
                (
                    alternate_season_key,
                    "part_of_series",
                    series_key,
                ),
            )
        )
    ]
    frames = create_gold_dataframes(spark, rows)
    documents = {
        row.entityKey: row
        for row in build_gold_search_projection(
            spark,
            gold_tables=frames,
            release_plan_id=plan_id,
        ).collect()
    }

    episode = documents[episode_key]
    assert episode.parentEntityKey == season_key
    assert episode.seasonEntityKey == season_key
    assert episode.seriesEntityKey == series_key
    season = documents[season_key]
    assert season.parentEntityKey == series_key
    assert season.seriesEntityKey == series_key
    assert season.seasonEntityKey is None
    ambiguous_episode = documents[ambiguous_episode_key]
    assert ambiguous_episode.parentEntityKey is None
    assert ambiguous_episode.seriesEntityKey is None
    assert ambiguous_episode.seasonEntityKey is None


@pytest.mark.spark
def test_distributed_news_reference_search_contribution(
    spark: SparkSession,
) -> None:
    plan_id = "sha256:" + ("a" * 64)
    entity_key = "sha256:" + ("b" * 64)
    title_id = "sha256:" + ("c" * 64)
    claim_id = "sha256:" + ("d" * 64)
    rows = {table: [] for table in GOLD_DATA_COLUMNS}
    rows["community_gold_entity"] = [
        {
            "row_key": "sha256:" + ("e" * 64),
            "release_plan_id": plan_id,
            "entity_key": entity_key,
            "entity_level": "NEWS_STORY",
            "entity_kind": "NEWS_STORY",
            "status": "ACTIVE",
            "source_node_count": 1,
            "trace_json": "{}",
        }
    ]
    rows["community_gold_field"] = [
        {
            "resolution_key": "sha256:" + f"{index + 10:064x}",
            "release_plan_id": plan_id,
            "entity_key": entity_key,
            "predicate": predicate,
            "scope_hash": "sha256:" + f"{index + 20:064x}",
            "value_type": "STRING",
            "value_json": canonical_json(text),
            "qualifiers_json": qualifiers,
            "resolution_status": "SELECTED",
            "selected_assertion_id": assertion_id,
            "assertion_ids_json": canonical_json([assertion_id]),
            "trace_json": _wikidata_trace(assertion_id, source_path),
        }
        for index, (
            predicate,
            text,
            qualifiers,
            assertion_id,
            source_path,
        ) in enumerate(
            (
                (
                    "title",
                    "A cited story",
                    '{"language":"en","titleRole":"PRIMARY"}',
                    title_id,
                    "/labels/en",
                ),
                (
                    "claim_text",
                    "A searchable reference claim.",
                    "{}",
                    claim_id,
                    "/claimText",
                ),
            )
        )
    ]

    document = build_gold_search_projection(
        spark,
        gold_tables=create_gold_dataframes(spark, rows),
        release_plan_id=plan_id,
    ).collect()[0]

    assert document.canonicalSubjectRoot == "WORK"
    assert document.referenceSlices == ["NEWS/JOURNALISM"]
    assert document.authorityScope == "GLOBAL_REFERENCE_ONLY"
    assert document.referenceCapabilities == ["DISPLAY", "IDENTITY", "RESEARCH"]
    assert {item.predicate: item.valueText for item in document.referenceCandidates}[
        "claim_text"
    ] == "A searchable reference claim."
