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


def _musicbrainz_trace(assertion_id: str, source_path: str) -> str:
    return canonical_json(
        {
            "assertions": [
                {
                    "assertionId": assertion_id,
                    "sourceProductId": "musicbrainz-core-dump",
                    "sourceName": "MusicBrainz core database dump",
                    "sourceRecordId": "df7d1c7f-ef95-425f-8eef-445b3d7bcbd9",
                    "sourcePath": source_path,
                    "observedAt": "2026-10-17T00:00:00Z",
                    "citationKeys": ["sha256:" + ("9" * 64)],
                    "rights": {
                        "policyId": "musicbrainz-core-data-cc0",
                        "policyZone": "open_cc0",
                        "licenseId": "CC0-1.0",
                        "licenseUri": (
                            "https://creativecommons.org/publicdomain/zero/1.0/"
                        ),
                        "attributionText": "MusicBrainz core database contributors",
                        "sourceUrl": "https://musicbrainz.org/",
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


@pytest.mark.spark
def test_distributed_music_label_search_contribution(
    spark: SparkSession,
) -> None:
    plan_id = "sha256:" + ("a" * 64)
    entity_key = "sha256:" + ("f" * 64)
    title_id = "sha256:" + ("6" * 64)
    agent_kind_id = "sha256:" + ("8" * 64)
    identifier_id = "sha256:" + ("7" * 64)
    rows = {table: [] for table in GOLD_DATA_COLUMNS}
    rows["community_gold_entity"] = [
        {
            "row_key": "sha256:" + ("5" * 64),
            "release_plan_id": plan_id,
            "entity_key": entity_key,
            "entity_level": "MUSIC_LABEL",
            "entity_kind": "MUSIC_LABEL",
            "status": "ACTIVE",
            "source_node_count": 1,
            "trace_json": "{}",
        }
    ]
    rows["community_gold_field"] = [
        {
            "resolution_key": "sha256:" + ("4" * 64),
            "release_plan_id": plan_id,
            "entity_key": entity_key,
            "predicate": "title",
            "scope_hash": "sha256:" + ("3" * 64),
            "value_type": "STRING",
            "value_json": canonical_json("Parlophone"),
            "qualifiers_json": '{"language":"en","titleRole":"PRIMARY"}',
            "resolution_status": "SELECTED",
            "selected_assertion_id": title_id,
            "assertion_ids_json": canonical_json([title_id]),
            "trace_json": _musicbrainz_trace(title_id, "/name"),
        },
        {
            "resolution_key": "sha256:" + ("9" * 64),
            "release_plan_id": plan_id,
            "entity_key": entity_key,
            "predicate": "music_agent_kind",
            "scope_hash": "sha256:" + ("0" * 64),
            "value_type": "STRING",
            "value_json": canonical_json("ORGANIZATION"),
            "qualifiers_json": '{"musicRole":"MUSIC_LABEL"}',
            "resolution_status": "SELECTED",
            "selected_assertion_id": agent_kind_id,
            "assertion_ids_json": canonical_json([agent_kind_id]),
            "trace_json": _musicbrainz_trace(
                agent_kind_id,
                "/entityType",
            ),
        },
    ]
    rows["community_gold_identifier"] = [
        {
            "resolution_key": "sha256:" + ("2" * 64),
            "release_plan_id": plan_id,
            "entity_key": entity_key,
            "namespace_id": "musicbrainz-label",
            "value": "df7d1c7f-ef95-425f-8eef-445b3d7bcbd9",
            "issuer": "MusicBrainz",
            "referent_kind": "MUSIC_LABEL",
            "assertion_ids_json": canonical_json([identifier_id]),
            "trace_json": _musicbrainz_trace(identifier_id, "/id"),
        }
    ]

    document = build_gold_search_projection(
        spark,
        gold_tables=create_gold_dataframes(spark, rows),
        release_plan_id=plan_id,
    ).collect()[0]

    assert document.canonicalSubjectRoot == "AGENT"
    assert document.referenceSlices == ["MUSIC"]
    assert document.domains == ["MUSIC"]
    assert document.musicAgentKind == "ORGANIZATION"
    assert document.typeConcepts[0].typeConceptId == "MUSIC_LABEL"
    assert document.externalIdentifiers[0].namespace == "musicbrainz-label"
    assert document.externalIdentifiers[0].url == (
        "https://musicbrainz.org/label/df7d1c7f-ef95-425f-8eef-445b3d7bcbd9"
    )
    assert "musicLabel" not in document.asDict()


@pytest.mark.spark
def test_distributed_music_recording_match_profile_fields(
    spark: SparkSession,
) -> None:
    plan_id = "sha256:" + ("a" * 64)
    exact_key = "sha256:" + ("1" * 64)
    variant_key = "sha256:" + ("2" * 64)
    work_key = "sha256:" + ("3" * 64)
    artist_key = "sha256:" + ("4" * 64)
    rows = {table: [] for table in GOLD_DATA_COLUMNS}
    rows["community_gold_entity"] = [
        {
            "row_key": "sha256:" + f"{index + 10:064x}",
            "release_plan_id": plan_id,
            "entity_key": entity_key,
            "entity_level": entity_level,
            "entity_kind": entity_kind,
            "status": "ACTIVE",
            "source_node_count": 2,
            "trace_json": "{}",
        }
        for index, (entity_key, entity_level, entity_kind) in enumerate(
            (
                (exact_key, "MUSIC_RECORDING", "MUSIC_RECORDING"),
                (variant_key, "MUSIC_RECORDING", "MUSIC_RECORDING"),
                (work_key, "MUSICAL_WORK", "MUSICAL_WORK"),
            )
        )
    ]
    title_rows = [
        (exact_key, "Exact recording", 20),
        (variant_key, "Reviewed remix", 21),
        (work_key, "Parent work", 22),
    ]
    rows["community_gold_field"] = [
        {
            "resolution_key": "sha256:" + f"{index + 30:064x}",
            "release_plan_id": plan_id,
            "entity_key": entity_key,
            "predicate": "title",
            "scope_hash": "sha256:" + f"{index + 40:064x}",
            "value_type": "STRING",
            "value_json": canonical_json(title),
            "qualifiers_json": '{"language":"en","titleRole":"PRIMARY"}',
            "resolution_status": "SELECTED",
            "selected_assertion_id": "sha256:" + f"{assertion:064x}",
            "assertion_ids_json": canonical_json(["sha256:" + f"{assertion:064x}"]),
            "trace_json": _musicbrainz_trace(
                "sha256:" + f"{assertion:064x}",
                "/name",
            ),
        }
        for index, (entity_key, title, assertion) in enumerate(title_rows)
    ]
    rows["community_gold_field"].extend(
        {
            "resolution_key": "sha256:" + f"{index + 120:064x}",
            "release_plan_id": plan_id,
            "entity_key": entity_key,
            "predicate": "media_object",
            "scope_hash": "sha256:" + f"{index + 130:064x}",
            "value_type": "STRING",
            "value_json": canonical_json(media_object),
            "qualifiers_json": '{"authorityProperty":"P51"}',
            "resolution_status": "SET",
            "selected_assertion_id": None,
            "assertion_ids_json": canonical_json(["sha256:" + f"{index + 140:064x}"]),
            "trace_json": _wikidata_trace(
                "sha256:" + f"{index + 140:064x}",
                f"/claims/P51/{index}",
            ),
        }
        for index, (entity_key, media_object) in enumerate(
            (
                (exact_key, "exact-recording.flac"),
                (variant_key, "reviewed-remix.mp3"),
            )
        )
    )
    rows["community_gold_field"].extend(
        {
            "resolution_key": "sha256:" + f"{index + 50:064x}",
            "release_plan_id": plan_id,
            "entity_key": variant_key,
            "predicate": "media_variant",
            "scope_hash": "sha256:" + f"{index + 60:064x}",
            "value_type": "STRING",
            "value_json": '"remix"',
            "qualifiers_json": canonical_json(
                {
                    "authorityRelation": "remix",
                    "direction": "backward",
                    "targetId": f"variant-{index}",
                }
            ),
            "resolution_status": "SET",
            "selected_assertion_id": None,
            "assertion_ids_json": canonical_json(["sha256:" + f"{index + 70:064x}"]),
            "trace_json": _musicbrainz_trace(
                "sha256:" + f"{index + 70:064x}",
                f"/recordingVariants/{index}",
            ),
        }
        for index in range(4)
    )
    rows["community_gold_identifier"] = [
        {
            "resolution_key": "sha256:" + f"{index + 80:064x}",
            "release_plan_id": plan_id,
            "entity_key": entity_key,
            "namespace_id": namespace,
            "value": value,
            "issuer": "MusicBrainz",
            "referent_kind": referent_kind,
            "assertion_ids_json": canonical_json(["sha256:" + f"{index + 90:064x}"]),
            "trace_json": _musicbrainz_trace(
                "sha256:" + f"{index + 90:064x}",
                "/id",
            ),
        }
        for index, (entity_key, namespace, value, referent_kind) in enumerate(
            (
                (
                    exact_key,
                    "musicbrainz-recording",
                    "9aa56ede-83be-44ea-aeb5-d61268972c53",
                    "MUSIC_RECORDING",
                ),
                (
                    variant_key,
                    "musicbrainz-recording",
                    "b70b3468-8f1a-4fa3-b853-122f57d98f92",
                    "MUSIC_RECORDING",
                ),
                (
                    work_key,
                    "musicbrainz-work",
                    "9dc29ea6-7a6f-3cc5-aa37-560b04afb0d7",
                    "MUSICAL_WORK",
                ),
            )
        )
    ]
    rows["community_gold_relation"] = [
        {
            "resolution_key": "sha256:" + f"{index + 100:064x}",
            "release_plan_id": plan_id,
            "subject_entity_key": subject,
            "predicate": predicate,
            "object_entity_key": target,
            "qualifiers_json": canonical_json(qualifiers),
            "assertion_ids_json": canonical_json(["sha256:" + f"{index + 110:064x}"]),
            "trace_json": _musicbrainz_trace(
                "sha256:" + f"{index + 110:064x}",
                f"/relations/{index}",
            ),
        }
        for index, (subject, predicate, target, qualifiers) in enumerate(
            (
                (exact_key, "recording_of", work_key, {}),
                (variant_key, "recording_of", work_key, {}),
                (
                    exact_key,
                    "credit",
                    artist_key,
                    {"role": "PERFORMER", "agentKind": "PERSON"},
                ),
            )
        )
    ]

    documents = {
        item.entityKey: item
        for item in build_gold_search_projection(
            spark,
            gold_tables=create_gold_dataframes(spark, rows),
            release_plan_id=plan_id,
        ).collect()
    }

    exact = documents[exact_key]
    assert exact.musicExactEligible is True
    assert exact.musicMediaObjects == ["exact-recording.flac"]
    assert exact.musicVariantSignals == []
    assert exact.musicWorkParentEntityKeys == [work_key]
    assert exact.musicCredits[0].entityKey == artist_key
    assert exact.musicCredits[0].agentKind == "PERSON"
    assert exact.externalIdentifiers[0].namespace == "musicbrainz-recording"
    variant = documents[variant_key]
    assert variant.musicExactEligible is False
    assert variant.musicMediaObjects == ["reviewed-remix.mp3"]
    assert len(variant.musicVariantSignals) == 4
    assert variant.musicWorkParentEntityKeys == [work_key]
    assert documents[work_key].musicExactEligible is False
