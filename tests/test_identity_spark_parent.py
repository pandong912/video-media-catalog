from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from video_media_catalog.identity_spark import (
    build_parent_constrained_work,
)
from video_media_catalog.identity_v2 import EntityLevel


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("identity-parent-constrained-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def _parent_work(
    spark: SparkSession,
):
    children = spark.createDataFrame(
        [
            ("imdb-title", f"tt-child-{case}", "EDITORIAL_WORK")
            for case in (
                "unique",
                "unresolved",
                "ambiguous",
                "type",
                "ordinal",
            )
        ]
        + [("imdb-title", "tt-child-unique-peer", "EDITORIAL_WORK")],
        (
            "subject_namespace_id STRING, subject_source_id STRING, "
            "subject_referent_kind STRING"
        ),
    )
    relationships = spark.createDataFrame(
        [
            (
                "sha256:" + f"{index:064x}",
                "imdb-title",
                f"tt-child-{case}",
                "EDITORIAL_WORK",
                "PART_OF_SERIES",
                "imdb-title",
                f"tt-parent-{case}",
                "EDITORIAL_WORK",
                ("{}" if case == "ordinal" else '{"episodeNumber":2,"seasonNumber":1}'),
            )
            for index, case in enumerate(
                (
                    "unique",
                    "unresolved",
                    "ambiguous",
                    "type",
                    "ordinal",
                ),
                start=1,
            )
        ]
        + [
            (
                "sha256:" + f"{6:064x}",
                "imdb-title",
                "tt-child-unique-peer",
                "EDITORIAL_WORK",
                "PART_OF_SERIES",
                "imdb-title",
                "tt-parent-unique",
                "EDITORIAL_WORK",
                '{"episodeNumber":2,"seasonNumber":1}',
            )
        ],
        (
            "assertion_id STRING, subject_namespace_id STRING, "
            "subject_source_id STRING, subject_referent_kind STRING, "
            "predicate STRING, object_namespace_id STRING, "
            "object_source_id STRING, object_referent_kind STRING, "
            "qualifiers_json STRING"
        ),
    )
    fields = spark.createDataFrame(
        [],
        (
            "assertion_id STRING, subject_namespace_id STRING, "
            "subject_source_id STRING, subject_referent_kind STRING, "
            "predicate STRING, value_json STRING"
        ),
    )
    type_groups = spark.createDataFrame(
        [
            (
                "imdb-title",
                f"tt-parent-{case}",
                "EDITORIAL_WORK",
                ["TV_EPISODE" if case == "type" else "TV_SERIES"],
            )
            for case in (
                "unique",
                "unresolved",
                "ambiguous",
                "type",
                "ordinal",
            )
        ],
        (
            "subject_namespace_id STRING, subject_source_id STRING, "
            "subject_referent_kind STRING, entity_types ARRAY<STRING>"
        ),
    )
    parent_memberships = spark.createDataFrame(
        [
            (
                "imdb-title",
                "tt-parent-unique",
                "EDITORIAL_WORK",
                "sha256:" + ("2" * 64),
                "sha256:" + ("3" * 64),
            ),
            (
                "imdb-title",
                "tt-parent-ambiguous",
                "EDITORIAL_WORK",
                "sha256:" + ("4" * 64),
                "sha256:" + ("5" * 64),
            ),
            (
                "imdb-title",
                "tt-parent-ambiguous",
                "EDITORIAL_WORK",
                "sha256:" + ("6" * 64),
                "sha256:" + ("7" * 64),
            ),
            (
                "imdb-title",
                "tt-parent-type",
                "EDITORIAL_WORK",
                "sha256:" + ("8" * 64),
                "sha256:" + ("9" * 64),
            ),
            (
                "imdb-title",
                "tt-parent-ordinal",
                "EDITORIAL_WORK",
                "sha256:" + ("a" * 64),
                "sha256:" + ("b" * 64),
            ),
        ],
        (
            "source_namespace_id STRING, source_id STRING, "
            "source_referent_kind STRING, entity_key STRING, "
            "membership_key STRING"
        ),
    )
    candidates = spark.createDataFrame(
        [],
        (
            "subject_namespace_id STRING, subject_source_id STRING, "
            "subject_referent_kind STRING, candidate_entity_key STRING"
        ),
    )
    return build_parent_constrained_work(
        children=children,
        relationship_assertions=relationships,
        field_assertions=fields,
        type_groups=type_groups,
        parent_memberships=parent_memberships,
        candidate_links=candidates,
        child_level=EntityLevel.EPISODE,
    )


@pytest.mark.spark
def test_parent_join_requires_one_compatible_membership(
    spark: SparkSession,
) -> None:
    rows = {row.subject_source_id: row for row in _parent_work(spark).collect()}
    unique = rows["tt-child-unique"]
    assert unique.resolution_mode == "BOOTSTRAP"
    assert unique.resolved_parent_membership_count == 1
    assert unique.season_number == "1"
    assert unique.episode_number == "2"
    unique_peer = rows["tt-child-unique-peer"]
    assert unique_peer.resolution_mode == "BOOTSTRAP"
    assert unique_peer.component_id == unique.component_id
    assert unique_peer.component_node_count == unique.component_node_count == 2
    assert unique_peer.allocation_anchor_id == unique.allocation_anchor_id
    assert unique.allocation_anchor_id == min(unique.node_id, unique_peer.node_id)

    unresolved = rows["tt-child-unresolved"]
    assert unresolved.resolution_mode == "CONFLICT_PARENT_UNRESOLVED"

    ambiguous = rows["tt-child-ambiguous"]
    assert ambiguous.resolution_mode == "CONFLICT_PARENT_AMBIGUOUS"
    assert ambiguous.resolved_parent_membership_count == 0
    assert ambiguous.max_parent_membership_candidate_count == 2

    assert rows["tt-child-type"].resolution_mode == "CONFLICT_PARENT_TYPE"
    assert rows["tt-child-ordinal"].resolution_mode == "CONFLICT_PARENT_ORDINAL"
