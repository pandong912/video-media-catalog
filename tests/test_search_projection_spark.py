from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from video_media_catalog.search_projection import build_projection


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("video-media-catalog-search-projection-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.mark.spark
def test_build_projection_keeps_one_document_per_entity(
    spark: SparkSession,
) -> None:
    entity_key = "sha256:" + "1" * 64
    frames = {
        "catalog_entity": spark.createDataFrame(
            [(entity_key, "MOVIE", "wikidata", "Q1", "{}")],
            """
            entity_key STRING,
            entity_type STRING,
            canonical_source STRING,
            canonical_source_id STRING,
            attributes_json STRING
            """,
        ),
        "catalog_name": spark.createDataFrame(
            [
                (
                    "sha256:" + "2" * 64,
                    entity_key,
                    "PRIMARY",
                    "en",
                    "Example",
                    "wikidata",
                    "Q1",
                )
            ],
            """
            name_key STRING,
            entity_key STRING,
            name_type STRING,
            language STRING,
            value STRING,
            source STRING,
            source_record_id STRING
            """,
        ),
        "catalog_external_identifier": spark.createDataFrame(
            [
                (
                    "sha256:" + "3" * 64,
                    entity_key,
                    "imdb",
                    "tt0000001",
                    "wikidata",
                    "Q1",
                )
            ],
            """
            identifier_key STRING,
            entity_key STRING,
            scheme STRING,
            value STRING,
            source STRING,
            source_record_id STRING
            """,
        ),
        "catalog_relation": spark.createDataFrame(
            [],
            """
            relation_key STRING,
            subject_entity_key STRING,
            relation_type STRING,
            object_entity_key STRING,
            ordinal STRING,
            source STRING,
            source_record_id STRING,
            attributes_json STRING
            """,
        ),
        "catalog_source_record": spark.createDataFrame(
            [
                (
                    "sha256:" + "4" * 64,
                    "wikidata",
                    "Q1",
                    None,
                    None,
                    "sha256:" + "5" * 64,
                    entity_key,
                    "{}",
                )
            ],
            """
            record_key STRING,
            source STRING,
            source_record_id STRING,
            source_revision STRING,
            modified STRING,
            source_hash STRING,
            entity_key STRING,
            payload_json STRING
            """,
        ),
        "catalog_ingest_error": spark.createDataFrame(
            [],
            """
            error_key STRING,
            source STRING,
            source_record_id STRING,
            error_code STRING,
            message STRING,
            details_json STRING
            """,
        ),
    }

    documents = build_projection(spark, frames).collect()

    assert len(documents) == 1
    assert documents[0].entityKey == entity_key
    assert documents[0].displayName == "Example"
    assert documents[0].externalIdentifiers[0].value == "tt0000001"
