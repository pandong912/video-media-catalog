"""Distributed exact identity resolution and source-node allocation."""

from __future__ import annotations

from typing import Any

from video_media_catalog.assertions import SourceNodeRef
from video_media_catalog.community_ingest import (
    CommunityIngestRun,
    IngestRunKind,
    build_community_ingest_run,
)
from video_media_catalog.community_rows import (
    entity_ledger_row,
    entity_membership_row,
    identity_decision_row,
    identity_evidence_row,
)
from video_media_catalog.community_sources import (
    internal_key_continuity_profile,
)
from video_media_catalog.community_spark import community_table_schema
from video_media_catalog.community_tables import DATA_TABLE_COLUMNS
from video_media_catalog.identity_resolution import (
    resolve_or_allocate_source_node,
)
from video_media_catalog.identity_v2 import EntityLevel
from video_media_catalog.v2_contracts import require_rfc3339, require_sha256

_EXTERNAL_NAMESPACE_MAP = {
    "imdb-title": "imdb",
}

_LEVEL_MAP = {
    "SERIES": (EntityLevel.SERIES, "TV_SERIES"),
    "TV_SERIES": (EntityLevel.SERIES, "TV_SERIES"),
    "EPISODE": (EntityLevel.EPISODE, "TV_EPISODE"),
    "TV_EPISODE": (EntityLevel.EPISODE, "TV_EPISODE"),
    "SEASON": (EntityLevel.SEASON, "TV_SEASON"),
    "TV_SEASON": (EntityLevel.SEASON, "TV_SEASON"),
    "MOVIE": (EntityLevel.EDITORIAL_WORK, "MOVIE"),
    "EDITORIAL_WORK": (EntityLevel.EDITORIAL_WORK, "EDITORIAL_WORK"),
    "PERSON": (EntityLevel.AGENT, "PERSON"),
    "ORGANIZATION": (EntityLevel.AGENT, "ORGANIZATION"),
}


def _compatible(source_type: str, v1_type: str) -> bool:
    source = _LEVEL_MAP.get(source_type.upper())
    if source is None:
        return False
    expected = source[1]
    return expected == v1_type or (expected == "EDITORIAL_WORK" and v1_type == "MOVIE")


def build_identity_resolution_dataframes(
    spark: Any,
    *,
    visible_silver: dict[str, Any],
    v1_external_identifiers: Any,
    v1_entities: Any,
    input_id: str,
    image_digest: str,
    config_digest: str,
    started_at: str,
) -> tuple[CommunityIngestRun, dict[str, Any]]:
    """Resolve unassigned source nodes; ambiguity is fail-closed for v1."""

    input_id = require_sha256(input_id, label="input_id")
    started_at = require_rfc3339(started_at, label="started_at")
    required = {
        "community_entity_type_assertion",
        "community_identifier_assertion",
        "community_entity_membership",
    }
    if not required.issubset(visible_silver):
        raise ValueError("identity resolution is missing Silver tables")

    from pyspark.sql import functions as F
    from pyspark.sql.types import BooleanType

    type_assertions = visible_silver["community_entity_type_assertion"].where(
        F.col("status") == "ACTIVE"
    )
    type_groups = (
        type_assertions.groupBy(
            "subject_namespace_id",
            "subject_source_id",
            "subject_referent_kind",
        )
        .agg(
            F.sort_array(F.collect_set("entity_type")).alias("entity_types"),
            F.sort_array(F.collect_set("assertion_id")).alias("type_assertion_ids"),
            F.min("observed_at").alias("observed_at"),
            F.min("policy_id").alias("policy_id"),
            F.min("policy_digest").alias("policy_digest"),
            F.countDistinct("policy_id").alias("policy_count"),
        )
        .persist()
    )
    invalid_type = type_groups.where(
        (F.size("entity_types") != 1) | (F.col("policy_count") != 1)
    ).limit(1)
    if invalid_type.count():
        type_groups.unpersist()
        raise ValueError("source node has conflicting type or policy assertions")

    as_of = F.to_timestamp(F.lit(started_at))
    memberships = visible_silver["community_entity_membership"].where(
        (F.to_timestamp("valid_from") <= as_of)
        & (F.col("valid_to").isNull() | (F.to_timestamp("valid_to") > as_of))
    )
    assigned_nodes = memberships.select(
        "source_namespace_id",
        "source_id",
        "source_referent_kind",
    ).dropDuplicates()
    unassigned = (
        type_groups.alias("t")
        .join(
            assigned_nodes.alias("m"),
            (
                (F.col("t.subject_namespace_id") == F.col("m.source_namespace_id"))
                & (F.col("t.subject_source_id") == F.col("m.source_id"))
                & (F.col("t.subject_referent_kind") == F.col("m.source_referent_kind"))
            ),
            "left_anti",
        )
        .persist()
    )

    namespace_map = spark.createDataFrame(
        sorted(_EXTERNAL_NAMESPACE_MAP.items()),
        "namespace_id STRING, scheme STRING",
    )
    identifiers = (
        visible_silver["community_identifier_assertion"]
        .where(F.col("status") == "ACTIVE")
        .join(namespace_map, "namespace_id", "inner")
    )
    candidate_rows = (
        identifiers.alias("i")
        .join(
            v1_external_identifiers.alias("x"),
            (F.col("i.scheme") == F.col("x.scheme"))
            & (F.col("i.value") == F.col("x.value")),
            "inner",
        )
        .join(
            v1_entities.select("entity_key", "entity_type").alias("e"),
            F.col("x.entity_key") == F.col("e.entity_key"),
            "inner",
        )
        .join(
            type_groups.alias("t"),
            (F.col("i.subject_namespace_id") == F.col("t.subject_namespace_id"))
            & (F.col("i.subject_source_id") == F.col("t.subject_source_id"))
            & (F.col("i.subject_referent_kind") == F.col("t.subject_referent_kind")),
            "inner",
        )
    )
    compatible_udf = F.udf(_compatible, BooleanType())
    candidates = (
        candidate_rows.where(
            compatible_udf(F.element_at("t.entity_types", 1), "e.entity_type")
        )
        .groupBy(
            "i.subject_namespace_id",
            "i.subject_source_id",
            "i.subject_referent_kind",
        )
        .agg(
            F.sort_array(F.collect_set("e.entity_key")).alias("candidate_entity_keys"),
            F.sort_array(F.collect_set("i.assertion_id")).alias(
                "identifier_assertion_ids"
            ),
        )
    )
    work = (
        unassigned.alias("u")
        .join(
            candidates.alias("c"),
            (F.col("u.subject_namespace_id") == F.col("c.subject_namespace_id"))
            & (F.col("u.subject_source_id") == F.col("c.subject_source_id"))
            & (F.col("u.subject_referent_kind") == F.col("c.subject_referent_kind")),
            "left",
        )
        .select(
            "u.*",
            F.coalesce(
                "c.candidate_entity_keys",
                F.from_json(F.lit("[]"), "array<string>"),
            ).alias("candidate_entity_keys"),
            F.coalesce(
                "c.identifier_assertion_ids",
                F.from_json(F.lit("[]"), "array<string>"),
            ).alias("identifier_assertion_ids"),
        )
    )

    def resolve(row):
        source_type = str(row["entity_types"][0]).upper()
        level, kind = _LEVEL_MAP.get(
            source_type,
            (EntityLevel.UNKNOWN, source_type),
        )
        assertion_keys = tuple(
            sorted(
                {
                    *row["type_assertion_ids"],
                    *row["identifier_assertion_ids"],
                }
            )
        )
        return resolve_or_allocate_source_node(
            source_node=SourceNodeRef(
                namespace_id=row["subject_namespace_id"],
                source_id=row["subject_source_id"],
                referent_kind=row["subject_referent_kind"],
            ),
            entity_level=level,
            entity_kind=kind,
            exact_candidate_entity_keys=tuple(row["candidate_entity_keys"]),
            assertion_keys=assertion_keys,
            observed_at=row["observed_at"],
            policy_id=row["policy_id"],
            policy_digest=row["policy_digest"],
            decision_policy_version="exact-identity-v1",
            decided_by="community-identity-spark-v1",
        )

    results = work.rdd.map(resolve).persist()
    try:
        conflict_count = results.map(lambda item: len(item.conflicts)).sum()
        if conflict_count:
            raise ValueError(f"identity conflicts {int(conflict_count)} require review")
        expected_counts = {table: 0 for table in DATA_TABLE_COLUMNS}
        expected_counts["community_entity_ledger"] = int(
            results.map(lambda item: len(item.entities)).sum()
        )
        expected_counts["community_identity_evidence"] = int(
            results.map(lambda item: len(item.evidence)).sum()
        )
        expected_counts["community_identity_decision"] = int(
            results.map(lambda item: len(item.decisions)).sum()
        )
        expected_counts["community_entity_membership"] = int(
            results.map(lambda item: len(item.memberships)).sum()
        )
        operation_policy = internal_key_continuity_profile()
        run = build_community_ingest_run(
            run_kind=IngestRunKind.IDENTITY_RESOLUTION,
            source_product_id="identity-resolution-v2",
            input_id=input_id,
            policy_id=operation_policy.policy_id,
            policy_digest=operation_policy.digest,
            image_digest=image_digest,
            config_digest=config_digest,
            started_at=started_at,
            expected_counts=expected_counts,
            input_manifest={
                "inputId": input_id,
                "resolver": "community-identity-spark-v1",
            },
        )
        row_rdds = {
            "community_entity_ledger": results.flatMap(
                lambda item: (
                    entity_ledger_row(run.run_id, value) for value in item.entities
                )
            ),
            "community_identity_evidence": results.flatMap(
                lambda item: (
                    identity_evidence_row(run.run_id, value) for value in item.evidence
                )
            ),
            "community_identity_decision": results.flatMap(
                lambda item: (
                    identity_decision_row(run.run_id, value) for value in item.decisions
                )
            ),
            "community_entity_membership": results.flatMap(
                lambda item: (
                    entity_membership_row(run.run_id, value)
                    for value in item.memberships
                )
            ),
        }
        dataframes = {}
        try:
            for table in DATA_TABLE_COLUMNS:
                frame = spark.createDataFrame(
                    row_rdds.get(table, spark.sparkContext.emptyRDD()),
                    schema=community_table_schema(table),
                ).persist()
                if frame.count() != expected_counts[table]:
                    frame.unpersist()
                    raise RuntimeError(f"{table} materialized count changed")
                dataframes[table] = frame
            return run, dataframes
        except Exception:
            for frame in dataframes.values():
                frame.unpersist()
            raise
    finally:
        results.unpersist()
        unassigned.unpersist()
        type_groups.unpersist()
