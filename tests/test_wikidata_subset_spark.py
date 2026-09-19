from __future__ import annotations

import bz2
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from video_media_catalog.constants import RELATION_PROPERTIES
from video_media_catalog.reference_selection import ReferenceSelectionConfig
from video_media_catalog.transform import _statement_value, build_curated_rows
from video_media_catalog.wikidata import (
    iter_wikidata_entities,
    iter_wikidata_records,
)
from video_media_catalog.wikidata_subset import SubsetSelectionConfig
from video_media_catalog.wikidata_subset_spark import (
    build_reference_subset,
    build_subset,
    normalize_dump,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("video-media-catalog-wikidata-subset-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.mark.spark
def test_small_spark_dump_builds_deterministic_budgeted_bzip2(
    spark: SparkSession,
    fixture_dir: Path,
    tmp_path: Path,
) -> None:
    dump = tmp_path / "wikidata-20260901-all.json.bz2"
    with bz2.open(dump, "wb") as handle:
        handle.write((fixture_dir / "wikidata.json").read_bytes())
    normalized = normalize_dump(spark, dump.as_uri())
    config = SubsetSelectionConfig(
        target_count=6,
        work_quotas={
            "MOVIE": 1,
            "TV_SERIES": 1,
            "TV_SEASON": 1,
            "TV_EPISODE": 1,
        },
    )

    built = build_subset(
        spark,
        normalized,
        config,
        max_closure_iterations=8,
    )
    output = tmp_path / "subset-output"
    built.lines.write.option("compression", "bzip2").text(str(output))
    parts = [
        path
        for path in output.iterdir()
        if path.name.startswith("part-") and path.suffix == ".bz2"
    ]

    assert len(parts) == 1
    assert built.selection.selected_qids == (
        "Q1001",
        "Q1002",
        "Q1003",
        "Q1004",
        "Q2001",
        "Q2002",
    )
    assert built.selection.selected_count == config.target_count
    assert built.dependency_rows == 4
    assert built.pruned_relation_statements == 1
    assert built.output_rows == 10

    entities = list(iter_wikidata_entities(parts[0]))
    assert len(entities) == built.output_rows
    selected = set(built.selection.selected_qids)
    for entity in entities:
        for property_id in RELATION_PROPERTIES:
            for statement in entity.get("claims", {}).get(property_id, []):
                assert _statement_value(statement) in selected

    curated = build_curated_rows(list(iter_wikidata_records(parts[0])))
    assert len(curated.catalog_entity) == config.target_count
    assert {row["canonical_source_id"] for row in curated.catalog_entity} == selected


@pytest.mark.spark
def test_small_spark_dump_builds_content_first_reference_subset(
    spark: SparkSession,
    fixture_dir: Path,
    tmp_path: Path,
) -> None:
    dump = tmp_path / "wikidata-reference.json.bz2"
    with bz2.open(dump, "wb") as handle:
        handle.write((fixture_dir / "wikidata.json").read_bytes())
    normalized = normalize_dump(spark, dump.as_uri())
    config = ReferenceSelectionConfig(
        content_quotas={
            "MOVIE": 1,
            "TV_SERIES": 1,
            "TV_SEASON": 1,
            "TV_EPISODE": 1,
        },
        agent_limits={"PERSON": 2, "ORGANIZATION": 1},
    )

    built = build_reference_subset(
        spark,
        normalized,
        config,
        max_closure_iterations=8,
    )

    assert built.selection.content_qids == (
        "Q1001",
        "Q1002",
        "Q1003",
        "Q1004",
    )
    assert built.selection.agent_qids == ("Q2001", "Q2002", "Q3001")
    assert built.selection.content_count == 4
    assert built.selection.agent_count == 3
    assert built.quality.status == "PASS"
    assert built.quality.hierarchy_counts == {"COMPLETE": 2, "PARTIAL": 0}
    assert (
        built.output_rows == len(built.selection.selected_qids) + built.dependency_rows
    )
