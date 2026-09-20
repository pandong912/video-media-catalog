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
    assert documents[0].contextId == "personal-research"
    assert documents[0].sourceBadges[0].sourceProductId == "tvmaze-public-api"
