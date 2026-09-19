from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from video_media_catalog.canonical import canonical_json
from video_media_catalog.reference_selection import ReferenceSelectionConfig
from video_media_catalog.reference_selection_spark import (
    audit_reference_with_spark,
    select_reference_with_spark,
)
from video_media_catalog.wikidata_subset_spark import normalized_schema


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("reference-catalog-selection-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def _statement(target: str) -> dict:
    return {
        "rank": "normal",
        "mainsnak": {
            "snaktype": "value",
            "datavalue": {
                "type": "wikibase-entityid",
                "value": {"id": target, "entity-type": "item"},
            },
        },
    }


def _row(
    qid: str,
    entity_type: str,
    *,
    parent: str | None = None,
    person_credits: tuple[str, ...] = (),
) -> tuple[dict, tuple[str, str]]:
    claims = {
        "P577": [
            {
                "rank": "normal",
                "mainsnak": {
                    "snaktype": "value",
                    "datavalue": {
                        "value": {
                            "time": "+2020-01-01T00:00:00Z",
                            "precision": 11,
                        }
                    },
                },
            }
        ],
        "P2047": [
            {
                "rank": "normal",
                "mainsnak": {
                    "snaktype": "value",
                    "datavalue": {"value": {"amount": "+45"}},
                },
            }
        ],
        "P345": [
            {
                "rank": "normal",
                "mainsnak": {
                    "snaktype": "value",
                    "datavalue": {"value": f"tt{int(qid[1:]):07d}"},
                },
            }
        ],
    }
    relations = []
    if parent is not None:
        claims["P179"] = [_statement(parent)]
        relations.append(
            {
                "property_id": "P179",
                "target_qid": parent,
                "target_type_hint": "UNKNOWN",
            }
        )
    if person_credits:
        claims["P161"] = [_statement(target) for target in person_credits]
        relations.extend(
            {
                "property_id": "P161",
                "target_qid": target,
                "target_type_hint": "PERSON",
            }
            for target in person_credits
        )
    payload = {
        "id": qid,
        "labels": {"en": {"language": "en", "value": qid}},
        "sitelinks": {"enwiki": {"title": qid}},
        "claims": claims,
    }
    return (
        {
            "qid": qid,
            "qid_numeric": int(qid[1:]),
            "payload_json": canonical_json(payload),
            "sitelink_count": 1,
            "direct_types": [],
            "subclass_parents": [],
            "relations": relations,
        },
        (qid, entity_type),
    )


@pytest.mark.spark
def test_distributed_reference_selection_and_audit(
    spark: SparkSession,
) -> None:
    rows_and_types = [
        _row("Q1", "MOVIE"),
        _row("Q2", "TV_SERIES"),
        _row("Q3", "TV_SEASON", parent="Q2"),
        _row("Q4", "TV_EPISODE", parent="Q3"),
    ]
    normalized = spark.createDataFrame(
        [value[0] for value in rows_and_types],
        schema=normalized_schema(),
    )
    entity_types = spark.createDataFrame(
        [value[1] for value in rows_and_types],
        "qid STRING, entity_type STRING",
    )
    config = ReferenceSelectionConfig(
        content_quotas={
            "MOVIE": 1,
            "TV_SERIES": 1,
            "TV_SEASON": 1,
            "TV_EPISODE": 1,
        },
        agent_limits={"PERSON": 0, "ORGANIZATION": 0},
    )
    result = select_reference_with_spark(
        spark,
        normalized=normalized,
        entity_types=entity_types,
        config=config,
    )
    audit = audit_reference_with_spark(
        spark,
        normalized=normalized,
        entity_types=entity_types,
        result=result,
        config=config,
    )
    assert result.content_qids == ("Q1", "Q2", "Q3", "Q4")
    assert result.hierarchy_coverage == {
        "Q3": "COMPLETE",
        "Q4": "COMPLETE",
    }
    assert audit.content_count == 4
    assert audit.hierarchy_counts["PARTIAL"] == 0


@pytest.mark.spark
def test_agent_selection_does_not_reclassify_selected_content(
    spark: SparkSession,
) -> None:
    rows_and_types = [
        _row("Q1", "MOVIE", person_credits=("Q2", "Q5")),
        _row("Q2", "TV_SERIES"),
        _row("Q5", "PERSON"),
    ]
    normalized = spark.createDataFrame(
        [value[0] for value in rows_and_types],
        schema=normalized_schema(),
    )
    entity_types = spark.createDataFrame(
        [value[1] for value in rows_and_types],
        "qid STRING, entity_type STRING",
    )
    config = ReferenceSelectionConfig(
        content_quotas={
            "MOVIE": 1,
            "TV_SERIES": 1,
            "TV_SEASON": 0,
            "TV_EPISODE": 0,
        },
        agent_limits={"PERSON": 1, "ORGANIZATION": 0},
    )

    result = select_reference_with_spark(
        spark,
        normalized=normalized,
        entity_types=entity_types,
        config=config,
    )

    assert result.content_qids == ("Q1", "Q2")
    assert result.agent_qids == ("Q5",)
    assert set(result.content_qids).isdisjoint(result.agent_qids)
