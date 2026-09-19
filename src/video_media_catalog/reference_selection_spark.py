"""Distributed content-first selection for the 100k reference catalog."""

from __future__ import annotations

import json
from typing import Any

from video_media_catalog.constants import (
    CREDIT_ORGANIZATION_PROPERTIES,
    CREDIT_PERSON_PROPERTIES,
)
from video_media_catalog.reference_selection import (
    AGENT_TYPES,
    CONTENT_TYPES,
    AssetDemandProfile,
    ReferenceQualityThresholds,
    ReferenceSelectionAudit,
    ReferenceSelectionConfig,
    ReferenceSelectionResult,
    reference_candidate,
)


def _score_payload(
    payload_json: str,
    entity_type: str,
    demand_profile: AssetDemandProfile | None,
) -> dict[str, Any]:
    candidate = reference_candidate(
        json.loads(payload_json),
        entity_type,
        demand_profile,
    )
    return {
        "completeness_score": candidate.completeness_score,
        "exact_identifier_count": candidate.exact_identifier_count,
        "demand_score": candidate.demand_score,
        "parent_qids": list(candidate.parent_qids),
        "has_title": candidate.has_title,
        "has_release": candidate.has_release,
        "has_runtime": candidate.has_runtime,
        "has_language": candidate.has_language,
        "has_country": candidate.has_country,
        "has_external_id": candidate.has_external_id,
    }


def _scored_candidates(
    normalized: Any,
    entity_types: Any,
    demand_profile: AssetDemandProfile | None,
) -> Any:
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        ArrayType,
        BooleanType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    score_schema = StructType(
        [
            StructField("completeness_score", IntegerType(), False),
            StructField("exact_identifier_count", IntegerType(), False),
            StructField("demand_score", IntegerType(), False),
            StructField(
                "parent_qids",
                ArrayType(StringType(), False),
                False,
            ),
            StructField("has_title", BooleanType(), False),
            StructField("has_release", BooleanType(), False),
            StructField("has_runtime", BooleanType(), False),
            StructField("has_language", BooleanType(), False),
            StructField("has_country", BooleanType(), False),
            StructField("has_external_id", BooleanType(), False),
        ]
    )
    score_udf = F.udf(
        lambda payload, entity_type: _score_payload(
            payload,
            entity_type,
            demand_profile,
        ),
        score_schema,
    )
    return (
        normalized.join(entity_types, "qid")
        .withColumn("_score", score_udf("payload_json", "entity_type"))
        .select(
            "qid",
            "qid_numeric",
            "entity_type",
            "sitelink_count",
            "relations",
            F.col("_score.completeness_score").alias("completeness_score"),
            F.col("_score.exact_identifier_count").alias("exact_identifier_count"),
            F.col("_score.demand_score").alias("demand_score"),
            F.col("_score.parent_qids").alias("parent_qids"),
            F.col("_score.has_title").alias("has_title"),
            F.col("_score.has_release").alias("has_release"),
            F.col("_score.has_runtime").alias("has_runtime"),
            F.col("_score.has_language").alias("has_language"),
            F.col("_score.has_country").alias("has_country"),
            F.col("_score.has_external_id").alias("has_external_id"),
        )
        .persist()
    )


def _collect_ranked(frame: Any, limit: int) -> list[Any]:
    from pyspark.sql import functions as F

    if limit <= 0:
        return []
    return (
        frame.orderBy(
            F.desc("demand_score"),
            F.desc("completeness_score"),
            F.desc("exact_identifier_count"),
            F.desc("sitelink_count"),
            F.asc("qid_numeric"),
        )
        .limit(limit)
        .select("qid", "entity_type")
        .collect()
    )


def _selected_frame(spark: Any, selected: dict[str, str]) -> Any:
    return spark.createDataFrame(
        sorted(selected.items()),
        "qid STRING NOT NULL, selected_type STRING NOT NULL",
    )


def _exclude_selected(frame: Any, selected: dict[str, str], spark: Any) -> Any:
    from pyspark.sql import functions as F

    if not selected:
        return frame
    return frame.join(
        F.broadcast(_selected_frame(spark, selected).select("qid")),
        "qid",
        "left_anti",
    )


def select_reference_with_spark(
    spark: Any,
    *,
    normalized: Any,
    entity_types: Any,
    config: ReferenceSelectionConfig,
    demand_profile: AssetDemandProfile | None = None,
) -> ReferenceSelectionResult:
    """Collect at most the configured content and agent budgets."""

    from pyspark.sql import functions as F

    if config.demand_profile_digest != (
        None if demand_profile is None else demand_profile.digest
    ):
        raise ValueError("selection config does not bind demand profile")
    candidates = _scored_candidates(
        normalized,
        entity_types,
        demand_profile,
    )
    selected: dict[str, str] = {}
    hierarchy: dict[str, str] = {}
    fallback_counts = {entity_type: 0 for entity_type in CONTENT_TYPES}

    def select_flat(entity_type: str) -> None:
        quota = config.content_quotas[entity_type]
        rows = _collect_ranked(
            candidates.where(F.col("entity_type") == entity_type),
            quota,
        )
        if len(rows) != quota:
            raise ValueError(f"not enough {entity_type} candidates")
        selected.update({row.qid: row.entity_type for row in rows})

    def select_hierarchy(entity_type: str, parents: set[str]) -> None:
        quota = config.content_quotas[entity_type]
        if quota == 0:
            return
        base = candidates.where(F.col("entity_type") == entity_type)
        preferred = (
            base.where(F.lit(False))
            if not parents
            else base.where(
                F.size(
                    F.array_intersect(
                        "parent_qids",
                        F.array(*[F.lit(value) for value in sorted(parents)]),
                    )
                )
                > 0
            )
        )
        preferred_rows = _collect_ranked(preferred, quota)
        selected_ids = {row.qid for row in preferred_rows}
        fallback_rows = []
        if len(preferred_rows) < quota:
            fallback_rows = _collect_ranked(
                base.where(~F.col("qid").isin(sorted(selected_ids))),
                quota - len(preferred_rows),
            )
        rows = [*preferred_rows, *fallback_rows]
        if len(rows) != quota:
            raise ValueError(f"not enough {entity_type} candidates")
        for row in preferred_rows:
            selected[row.qid] = row.entity_type
            hierarchy[row.qid] = "COMPLETE"
        for row in fallback_rows:
            selected[row.qid] = row.entity_type
            hierarchy[row.qid] = "PARTIAL"
        fallback_counts[entity_type] = len(fallback_rows)

    try:
        select_flat("MOVIE")
        select_flat("TV_SERIES")
        selected_series = {
            qid for qid, entity_type in selected.items() if entity_type == "TV_SERIES"
        }
        select_hierarchy("TV_SEASON", selected_series)
        selected_hierarchy = selected_series | {
            qid for qid, entity_type in selected.items() if entity_type == "TV_SEASON"
        }
        select_hierarchy("TV_EPISODE", selected_hierarchy)
        if len(selected) != config.content_target_count:
            raise RuntimeError("content selection count differs from target")

        selected_content = _selected_frame(spark, selected)
        relation_rows = (
            candidates.join(
                F.broadcast(selected_content.select("qid")),
                "qid",
                "inner",
            )
            .select(F.explode("relations").alias("relation"))
            .where(
                F.col("relation.property_id").isin(
                    sorted(
                        set(CREDIT_PERSON_PROPERTIES)
                        | set(CREDIT_ORGANIZATION_PROPERTIES)
                    )
                )
            )
            .select(
                F.col("relation.target_qid").alias("qid"),
                F.when(
                    F.col("relation.property_id").isin(
                        sorted(CREDIT_PERSON_PROPERTIES)
                    ),
                    F.lit("PERSON"),
                )
                .otherwise(F.lit("ORGANIZATION"))
                .alias("hint_type"),
            )
            .groupBy("qid")
            .agg(
                F.count(F.lit(1)).alias("reference_count"),
                F.collect_set("hint_type").alias("hint_types"),
            )
            .join(candidates, "qid", "inner")
            .withColumn(
                "resolved_type",
                F.when(
                    F.col("entity_type").isin(sorted(AGENT_TYPES)),
                    F.col("entity_type"),
                )
                .when(
                    F.array_contains("hint_types", "PERSON"),
                    F.lit("PERSON"),
                )
                .otherwise(F.lit("ORGANIZATION")),
            )
        )
        agents: dict[str, str] = {}
        for entity_type in ("PERSON", "ORGANIZATION"):
            rows = (
                relation_rows.where(F.col("resolved_type") == entity_type)
                .orderBy(
                    F.desc("reference_count"),
                    F.desc("demand_score"),
                    F.desc("completeness_score"),
                    F.desc("exact_identifier_count"),
                    F.desc("sitelink_count"),
                    F.asc("qid_numeric"),
                )
                .limit(config.agent_limits[entity_type])
                .select("qid")
                .collect()
            )
            agents.update({row.qid: entity_type for row in rows})

        return ReferenceSelectionResult(
            content_qids=tuple(sorted(selected, key=lambda value: int(value[1:]))),
            agent_qids=tuple(sorted(agents, key=lambda value: int(value[1:]))),
            entity_types={**selected, **agents},
            hierarchy_coverage=hierarchy,
            fallback_counts=fallback_counts,
        )
    finally:
        candidates.unpersist()


def audit_reference_with_spark(
    spark: Any,
    *,
    normalized: Any,
    entity_types: Any,
    result: ReferenceSelectionResult,
    config: ReferenceSelectionConfig,
    demand_profile: AssetDemandProfile | None = None,
    thresholds: ReferenceQualityThresholds | None = None,
) -> ReferenceSelectionAudit:
    from pyspark.sql import functions as F

    thresholds = thresholds or ReferenceQualityThresholds()
    candidates = _scored_candidates(
        normalized,
        entity_types,
        demand_profile,
    )
    selected = _selected_frame(
        spark,
        {qid: result.entity_types[qid] for qid in result.content_qids},
    )
    try:
        metrics = (
            candidates.join(selected, "qid", "inner")
            .groupBy("selected_type")
            .agg(
                F.count(F.lit(1)).alias("row_count"),
                *[
                    F.sum(F.col(field).cast("long")).alias(field)
                    for field in (
                        "has_title",
                        "has_release",
                        "has_runtime",
                        "has_language",
                        "has_country",
                        "has_external_id",
                    )
                ],
            )
            .collect()
        )
        by_type = {row["selected_type"]: row for row in metrics}

        def ratio(entity_type: str, field: str) -> float:
            row = by_type.get(entity_type)
            if row is None or int(row["row_count"]) == 0:
                return 1.0
            return int(row[field] or 0) / int(row["row_count"])

        field_coverage = {
            entity_type: {
                "title": ratio(entity_type, "has_title"),
                "release": ratio(entity_type, "has_release"),
                "runtime": ratio(entity_type, "has_runtime"),
                "language": ratio(entity_type, "has_language"),
                "country": ratio(entity_type, "has_country"),
                "externalId": ratio(entity_type, "has_external_id"),
            }
            for entity_type in sorted(CONTENT_TYPES)
        }
        complete = sum(
            value == "COMPLETE" for value in result.hierarchy_coverage.values()
        )
        partial = sum(
            value == "PARTIAL" for value in result.hierarchy_coverage.values()
        )
        episode_count = result.counts_by_type["TV_EPISODE"]
        complete_episodes = sum(
            result.entity_types[qid] == "TV_EPISODE" and value == "COMPLETE"
            for qid, value in result.hierarchy_coverage.items()
        )
        episode_parent = complete_episodes / episode_count if episode_count else 1.0
        overall_title = (
            sum(int(row["has_title"] or 0) for row in metrics) / result.content_count
            if result.content_count
            else 1.0
        )
        violations = []
        if overall_title < thresholds.minimum_title_coverage:
            violations.append(
                "TITLE_COVERAGE:"
                f"{overall_title:.12g}<"
                f"{thresholds.minimum_title_coverage:.12g}"
            )
        movie_release = field_coverage["MOVIE"]["release"]
        if movie_release < thresholds.minimum_movie_release_coverage:
            violations.append(
                "MOVIE_RELEASE_COVERAGE:"
                f"{movie_release:.12g}<"
                f"{thresholds.minimum_movie_release_coverage:.12g}"
            )
        if episode_parent < thresholds.minimum_episode_parent_coverage:
            violations.append(
                "EPISODE_PARENT_COVERAGE:"
                f"{episode_parent:.12g}<"
                f"{thresholds.minimum_episode_parent_coverage:.12g}"
            )
        return ReferenceSelectionAudit(
            config_digest=config.digest,
            demand_profile_digest=config.demand_profile_digest,
            content_count=result.content_count,
            agent_count=result.agent_count,
            counts_by_type=result.counts_by_type,
            fallback_counts=dict(sorted(result.fallback_counts.items())),
            hierarchy_counts={
                "COMPLETE": complete,
                "PARTIAL": partial,
            },
            field_coverage=field_coverage,
            violations=tuple(violations),
            status="FAILED" if violations else "PASS",
        )
    finally:
        candidates.unpersist()
