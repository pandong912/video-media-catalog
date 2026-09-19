"""Snapshot-pinned migration of published v1 keys into the v2 identity ledger."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from video_media_catalog.canonical import deterministic_key
from video_media_catalog.community_ingest import (
    CommunityIngestRun,
    IngestRunKind,
    build_community_ingest_run,
)
from video_media_catalog.community_sources import (
    internal_key_continuity_profile,
)
from video_media_catalog.community_spark import (
    community_table_schema,
)
from video_media_catalog.community_tables import (
    DATA_TABLE_COLUMNS,
    TABLE_COLUMNS,
)
from video_media_catalog.constants import CURATED_TABLE_KEYS
from video_media_catalog.identity_v2 import EntityLevel
from video_media_catalog.models import SnapshotSet

V1_KEY_KINDS = {
    "catalog_source_record": ("record_key", "SOURCE_RECORD"),
    "catalog_entity": ("entity_key", "ENTITY"),
    "catalog_name": ("name_key", "NAME"),
    "catalog_external_identifier": ("identifier_key", "IDENTIFIER"),
    "catalog_relation": ("relation_key", "RELATION"),
    "catalog_ingest_error": ("error_key", "INGEST_ERROR"),
}

V1_ENTITY_LEVELS = {
    "MOVIE": EntityLevel.EDITORIAL_WORK,
    "TV_SERIES": EntityLevel.SERIES,
    "TV_SEASON": EntityLevel.SEASON,
    "TV_EPISODE": EntityLevel.EPISODE,
    "PERSON": EntityLevel.AGENT,
    "ORGANIZATION": EntityLevel.AGENT,
    "UNKNOWN": EntityLevel.UNKNOWN,
}


def v1_entity_level(entity_type: str) -> EntityLevel:
    return V1_ENTITY_LEVELS.get(entity_type, EntityLevel.UNKNOWN)


def build_v1_key_migration(
    spark: Any,
    *,
    snapshot_set: SnapshotSet,
    v1_tables: Mapping[str, Any],
) -> tuple[CommunityIngestRun, dict[str, Any]]:
    """Build scalable v2 rows without recomputing any published v1 key."""

    if set(v1_tables) != set(CURATED_TABLE_KEYS):
        raise ValueError("all six v1 snapshot-pinned tables are required")
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType

    for table, (key, _) in V1_KEY_KINDS.items():
        if key not in v1_tables[table].columns:
            raise ValueError(f"{table} is missing v1 key column {key}")

    level_udf = F.udf(
        lambda value: v1_entity_level(str(value)).value,
        StringType(),
    )
    policy = internal_key_continuity_profile()
    snapshot_identity = {
        "snapshotSetId": snapshot_set.snapshot_set_id,
        "tables": sorted(
            (
                {
                    "tableName": table.table_name,
                    "snapshotId": table.snapshot_id,
                }
                for table in snapshot_set.tables
            ),
            key=lambda item: item["tableName"],
        ),
    }
    input_id = deterministic_key("v1-snapshot-set-input", snapshot_identity)

    entities_base = (
        v1_tables["catalog_entity"]
        .select("entity_key", "entity_type")
        .dropDuplicates(["entity_key"])
        .persist()
    )
    legacy_parts = []
    for table, (key, kind) in V1_KEY_KINDS.items():
        legacy_parts.append(
            v1_tables[table]
            .select(F.col(key).alias("legacy_key"))
            .where(F.col("legacy_key").isNotNull())
            .dropDuplicates(["legacy_key"])
            .withColumn("legacy_kind", F.lit(kind))
        )
    legacy_base = legacy_parts[0]
    for part in legacy_parts[1:]:
        legacy_base = legacy_base.unionByName(part)
    conflicting_keys = (
        legacy_base.groupBy("legacy_key")
        .agg(F.countDistinct("legacy_kind").alias("kind_count"))
        .where(F.col("kind_count") > 1)
        .limit(1)
        .count()
    )
    if conflicting_keys:
        entities_base.unpersist()
        raise ValueError("one published v1 key appears in multiple key domains")
    legacy_base = legacy_base.dropDuplicates(["legacy_key"]).persist()

    try:
        entity_count = entities_base.count()
        legacy_count = legacy_base.count()
        expected_counts = {table: 0 for table in DATA_TABLE_COLUMNS}
        expected_counts["community_entity_ledger"] = entity_count
        expected_counts["community_legacy_key_map"] = legacy_count
        run = build_community_ingest_run(
            run_kind=IngestRunKind.V1_KEY_MIGRATION,
            source_product_id="media-catalog-v1",
            input_id=input_id,
            policy_id=policy.policy_id,
            policy_digest=policy.digest,
            image_digest=snapshot_set.image_digest,
            config_digest=snapshot_set.config_digest,
            started_at=snapshot_set.created_at,
            expected_counts=expected_counts,
            input_manifest=snapshot_identity,
        )

        entity_frame = (
            entities_base.withColumn("run_id", F.lit(run.run_id))
            .withColumn("allocation_id", F.lit(None).cast("string"))
            .withColumn("entity_level", level_udf("entity_type"))
            .withColumnRenamed("entity_type", "entity_kind")
            .withColumn("status", F.lit("ACTIVE"))
            .withColumn("created_at", F.lit(snapshot_set.created_at))
            .withColumn("first_release_id", F.lit(None).cast("string"))
            .withColumn("imported_v1", F.lit(True))
            .select(*TABLE_COLUMNS["community_entity_ledger"])
        )
        legacy_frame = (
            legacy_base.withColumn("run_id", F.lit(run.run_id))
            .withColumn("target_key", F.col("legacy_key"))
            .withColumn("imported_at", F.lit(snapshot_set.created_at))
            .withColumn(
                "source_snapshot_set_id",
                F.lit(snapshot_set.snapshot_set_id),
            )
            .select(*TABLE_COLUMNS["community_legacy_key_map"])
        )
        dataframes = {
            table: spark.createDataFrame(
                [],
                schema=community_table_schema(table),
            )
            for table in DATA_TABLE_COLUMNS
        }
        dataframes["community_entity_ledger"] = entity_frame
        dataframes["community_legacy_key_map"] = legacy_frame
        return run, dataframes
    finally:
        entities_base.unpersist()
        legacy_base.unpersist()
