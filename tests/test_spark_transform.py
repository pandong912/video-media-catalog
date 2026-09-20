from __future__ import annotations

import inspect
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from video_media_catalog.eidr import iter_eidr_records
from video_media_catalog.spark_transform import transform_landing
from video_media_catalog.wikidata import iter_wikidata_records


def test_type_closure_truncates_iterative_spark_lineage() -> None:
    source = inspect.getsource(transform_landing)
    assert ".localCheckpoint(eager=True)" in source
    assert "delta.take(1)" not in source


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("video-media-catalog-unit-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.mark.spark
def test_distributed_transform_closure_exact_merge_and_conflict(
    spark: SparkSession, fixture_dir: Path
) -> None:
    records = [
        *iter_wikidata_records(fixture_dir / "wikidata.json"),
        *iter_eidr_records(fixture_dir / "eidr.xml"),
    ]
    landing = spark.createDataFrame(
        [record.model_dump(mode="python") for record in records],
        """
        record_key STRING,
        source STRING,
        source_record_id STRING,
        source_revision STRING,
        modified STRING,
        source_hash STRING,
        payload_json STRING
        """,
    )
    frames = transform_landing(spark, landing, max_closure_iterations=8)
    entities = {
        (row.canonical_source, row.canonical_source_id): row.entity_type
        for row in frames["catalog_entity"].collect()
    }
    errors = {
        (row.source_record_id, row.error_code)
        for row in frames["catalog_ingest_error"].collect()
    }

    assert entities[("wikidata", "Q1001")] == "MOVIE"
    assert entities[("wikidata", "Q1002")] == "TV_SERIES"
    assert entities[("wikidata", "Q1003")] == "TV_SEASON"
    assert entities[("wikidata", "Q1004")] == "TV_EPISODE"
    assert entities[("wikidata", "Q2001")] == "PERSON"
    assert entities[("wikidata", "Q3001")] == "ORGANIZATION"
    assert ("eidr", "10.5240/AAAA-BBBB-CCCC-DDDD-EEEE-C") not in entities
    assert (
        "10.5240/FFFF-EEEE-DDDD-CCCC-BBBB-A",
        "EXACT_IDENTIFIER_CONFLICT",
    ) in errors

    movie_key = next(
        row.entity_key
        for row in frames["catalog_entity"].collect()
        if row.canonical_source_id == "Q1001"
    )
    relations = {
        (row.relation_type, row.ordinal)
        for row in frames["catalog_relation"]
        .where(f"subject_entity_key = '{movie_key}'")
        .collect()
    }
    assert ("DIRECTED_BY", "1") in relations
    assert ("CAST_MEMBER", "2") in relations
