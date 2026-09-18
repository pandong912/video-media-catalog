"""Distributed normalization and deterministic Wikidata subset construction."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from video_media_catalog.canonical import canonical_json
from video_media_catalog.constants import (
    CREDIT_ORGANIZATION_PROPERTIES,
    CREDIT_PERSON_PROPERTIES,
    ENTITY_TYPE_SEEDS,
    MEDIA_ENTITY_TYPES,
    RELATION_PROPERTIES,
)
from video_media_catalog.transform import _qid_values, _statement_value, _statements
from video_media_catalog.wikidata_subset import (
    ENTITY_BUDGET_TYPES,
    HIERARCHY_PROPERTIES,
    SelectionResult,
    SubsetSelectionConfig,
    parse_and_normalize_dump_line,
    prepare_subset_payload,
    wikipedia_sitelink_count,
)

_TYPE_PRIORITY = {
    "TV_EPISODE": 0,
    "TV_SEASON": 1,
    "TV_SERIES": 2,
    "MOVIE": 3,
    "PERSON": 4,
    "ORGANIZATION": 5,
    "UNKNOWN": 99,
}


@dataclass(frozen=True)
class SparkSubsetBuild:
    lines: Any
    selection: SelectionResult
    dependency_rows: int
    pruned_relation_statements: int
    output_rows: int


def _normalized_dump_row(raw_line: str) -> dict[str, Any] | None:
    payload = parse_and_normalize_dump_line(raw_line)
    if payload is None:
        return None
    relations: list[dict[str, str]] = []
    for property_id in RELATION_PROPERTIES:
        for statement in _statements(payload, property_id):
            target = _statement_value(statement)
            if not isinstance(target, str) or not target.startswith("Q"):
                continue
            hint = (
                "PERSON"
                if property_id in CREDIT_PERSON_PROPERTIES
                else (
                    "ORGANIZATION"
                    if property_id in CREDIT_ORGANIZATION_PROPERTIES
                    else "UNKNOWN"
                )
            )
            relations.append(
                {
                    "property_id": property_id,
                    "target_qid": target,
                    "target_type_hint": hint,
                }
            )
    qid = str(payload["id"])
    return {
        "qid": qid,
        "qid_numeric": int(qid[1:]),
        "payload_json": canonical_json(payload),
        "sitelink_count": wikipedia_sitelink_count(payload),
        "direct_types": sorted(set(_qid_values(payload, "P31"))),
        "subclass_parents": sorted(set(_qid_values(payload, "P279"))),
        "relations": relations,
    }


def normalized_schema() -> Any:
    from pyspark.sql.types import (
        ArrayType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    relation = StructType(
        [
            StructField("property_id", StringType(), False),
            StructField("target_qid", StringType(), False),
            StructField("target_type_hint", StringType(), False),
        ]
    )
    return StructType(
        [
            StructField("qid", StringType(), False),
            StructField("qid_numeric", LongType(), False),
            StructField("payload_json", StringType(), False),
            StructField("sitelink_count", LongType(), False),
            StructField("direct_types", ArrayType(StringType(), False), False),
            StructField("subclass_parents", ArrayType(StringType(), False), False),
            StructField("relations", ArrayType(relation, False), False),
        ]
    )


def normalize_dump(spark: Any, dump_uri: str) -> Any:
    """Read official line JSON with Spark and normalize without driver collect."""

    parsed = (
        spark.read.text(dump_uri)
        .rdd.map(lambda row: _normalized_dump_row(row["value"]))
        .filter(lambda row: row is not None)
    )
    normalized = spark.createDataFrame(parsed, schema=normalized_schema()).persist()
    if not normalized.take(1):
        normalized.unpersist()
        raise ValueError("Wikidata dump contains no entities")
    duplicate = (
        normalized.groupBy("qid").count().where("count > 1").select("qid").take(1)
    )
    if duplicate:
        normalized.unpersist()
        raise ValueError(f"Wikidata dump contains duplicate entity {duplicate[0].qid}")
    return normalized


def write_normalized_staging(normalized: Any, destination_uri: str) -> int:
    """Write reusable normalized Parquet staging with no overwrite."""

    row_count = normalized.count()
    (
        normalized.repartitionByRange("qid_numeric")
        .sortWithinPartitions("qid_numeric")
        .write.mode("errorifexists")
        .parquet(destination_uri)
    )
    return row_count


def _choose_type_expression(column: Any) -> Any:
    from pyspark.sql import functions as F

    priority = F.create_map(
        *[
            item
            for entity_type, number in _TYPE_PRIORITY.items()
            for item in (F.lit(entity_type), F.lit(number))
        ]
    )
    return F.struct(
        priority[column].alias("priority"),
        column.alias("entity_type"),
    )


def classify_entities(
    spark: Any,
    normalized: Any,
    *,
    max_closure_iterations: int,
) -> tuple[Any, Any]:
    """Apply the existing P31/P279 seed and precedence semantics."""

    from pyspark.sql import functions as F

    closure = spark.createDataFrame(
        sorted(ENTITY_TYPE_SEEDS.items()), ["class_id", "entity_type"]
    ).persist()
    edges = (
        normalized.select(
            F.col("qid").alias("child"),
            F.explode("subclass_parents").alias("parent"),
        )
        .dropDuplicates()
        .persist()
    )
    for _ in range(max_closure_iterations):
        candidates = (
            edges.join(closure, edges.parent == closure.class_id, "inner")
            .select(F.col("child").alias("class_id"), "entity_type")
            .dropDuplicates()
        )
        delta = candidates.join(
            closure, ["class_id", "entity_type"], "left_anti"
        ).persist()
        if not delta.take(1):
            delta.unpersist()
            break
        previous = closure
        closure = previous.unionByName(delta).dropDuplicates().persist()
        closure.count()
        previous.unpersist()
        delta.unpersist()
    else:
        edges.unpersist()
        closure.unpersist()
        raise RuntimeError(
            "P31/P279 closure did not converge within "
            f"{max_closure_iterations} iterations"
        )

    direct = normalized.select(
        "qid",
        F.explode("direct_types").alias("class_id"),
    )
    classified = (
        direct.join(closure, "class_id", "inner")
        .select("qid", "entity_type")
        .dropDuplicates()
        .groupBy("qid")
        .agg(F.min(_choose_type_expression(F.col("entity_type"))).alias("chosen"))
        .select("qid", F.col("chosen.entity_type").alias("entity_type"))
    )
    entity_types = (
        normalized.select("qid")
        .join(classified, "qid", "left")
        .fillna({"entity_type": "UNKNOWN"})
        .persist()
    )
    return entity_types, closure


def _selected_frame(spark: Any, selected: dict[str, str]) -> Any:
    return spark.createDataFrame(
        [(qid, entity_type) for qid, entity_type in selected.items()],
        "qid STRING NOT NULL, selected_type STRING NOT NULL",
    )


def _exclude_selected(frame: Any, selected_frame: Any) -> Any:
    from pyspark.sql import functions as F

    return frame.join(
        F.broadcast(selected_frame.select("qid")),
        "qid",
        "left_anti",
    )


def _collect_ranked(frame: Any, limit: int, *leading_order: Any) -> list[Any]:
    from pyspark.sql import functions as F

    if limit <= 0:
        return []
    return (
        frame.orderBy(
            *leading_order,
            F.desc("sitelink_count"),
            F.asc("qid_numeric"),
        )
        .limit(limit)
        .select("qid", "entity_type")
        .collect()
    )


def select_with_spark(
    spark: Any,
    normalized: Any,
    entity_types: Any,
    config: SubsetSelectionConfig,
) -> SelectionResult:
    """Run distributed ranking while collecting at most target_count QIDs."""

    from pyspark.sql import functions as F

    candidate_base = (
        normalized.select("qid", "qid_numeric", "sitelink_count", "relations")
        .join(entity_types, "qid")
        .persist()
    )
    works = candidate_base.where(F.col("entity_type").isin(sorted(MEDIA_ENTITY_TYPES)))
    selected: dict[str, str] = {}
    for entity_type, quota in config.work_quotas.items():
        rows = _collect_ranked(
            works.where(F.col("entity_type") == entity_type),
            quota,
        )
        selected.update({row.qid: row.entity_type for row in rows})

    refill_needed = config.work_target_count - len(selected)
    if refill_needed > 0:
        rows = _collect_ranked(
            _exclude_selected(works, _selected_frame(spark, selected)),
            refill_needed,
        )
        selected.update({row.qid: row.entity_type for row in rows})
    work_count = len(selected)
    selected_work = _selected_frame(spark, selected)

    relation_rows = (
        candidate_base.join(
            F.broadcast(selected_work.select("qid")),
            "qid",
            "inner",
        )
        .select(
            F.col("qid").alias("subject_qid"),
            F.explode("relations").alias("relation"),
        )
        .persist()
    )
    hierarchy = (
        relation_rows.where(
            F.col("relation.property_id").isin(sorted(HIERARCHY_PROPERTIES))
        )
        .select(F.col("relation.target_qid").alias("qid"))
        .dropDuplicates()
        .join(candidate_base.drop("relations"), "qid", "inner")
        .where(F.col("entity_type").isin(sorted(ENTITY_BUDGET_TYPES)))
    )
    rows = _collect_ranked(
        _exclude_selected(hierarchy, _selected_frame(spark, selected)),
        config.target_count - len(selected),
    )
    selected.update({row.qid: row.entity_type for row in rows})
    hierarchy_count = len(rows)

    credit = (
        relation_rows.where(
            F.col("relation.property_id").isin(
                sorted(
                    set(CREDIT_PERSON_PROPERTIES) | set(CREDIT_ORGANIZATION_PROPERTIES)
                )
            )
        )
        .select(
            F.col("relation.target_qid").alias("qid"),
            F.when(
                F.col("relation.property_id").isin(sorted(CREDIT_PERSON_PROPERTIES)),
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
        .join(candidate_base.drop("relations"), "qid", "inner")
        .withColumn(
            "entity_type",
            F.when(
                F.col("entity_type") != "UNKNOWN",
                F.col("entity_type"),
            )
            .when(F.array_contains("hint_types", "PERSON"), F.lit("PERSON"))
            .otherwise(F.lit("ORGANIZATION")),
        )
        .where(F.col("entity_type").isin(sorted(ENTITY_BUDGET_TYPES)))
    )
    rows = _collect_ranked(
        _exclude_selected(credit, _selected_frame(spark, selected)),
        config.target_count - len(selected),
        F.desc("reference_count"),
    )
    selected.update({row.qid: row.entity_type for row in rows})
    credit_count = len(rows)

    fallback = candidate_base.where(
        F.col("entity_type").isin(sorted(ENTITY_BUDGET_TYPES))
    )
    rows = _collect_ranked(
        _exclude_selected(fallback, _selected_frame(spark, selected)),
        config.target_count - len(selected),
    )
    selected.update({row.qid: row.entity_type for row in rows})
    fallback_count = len(rows)

    candidate_base.unpersist()
    relation_rows.unpersist()
    return SelectionResult(
        selected_qids=tuple(sorted(selected, key=lambda qid: int(qid[1:]))),
        entity_types=selected,
        work_count=work_count,
        hierarchy_count=hierarchy_count,
        credit_count=credit_count,
        fallback_count=fallback_count,
    )


def dependency_rows(
    spark: Any,
    normalized: Any,
    selected: SelectionResult,
    *,
    max_closure_iterations: int,
) -> Any:
    """Find available P31/P279 class rows required by selected entities."""

    from pyspark.sql import functions as F

    selected_frame = _selected_frame(spark, dict(selected.entity_types))
    dependencies = (
        normalized.join(
            F.broadcast(selected_frame.select("qid")),
            "qid",
            "inner",
        )
        .select(F.explode("direct_types").alias("qid"))
        .dropDuplicates()
        .persist()
    )
    edges = normalized.select(
        F.col("qid").alias("child"),
        F.explode("subclass_parents").alias("parent"),
    ).dropDuplicates()
    for _ in range(max_closure_iterations):
        parents = (
            dependencies.join(edges, dependencies.qid == edges.child, "inner")
            .select(F.col("parent").alias("qid"))
            .dropDuplicates()
        )
        delta = parents.join(dependencies, "qid", "left_anti").persist()
        if not delta.take(1):
            delta.unpersist()
            break
        previous = dependencies
        dependencies = previous.unionByName(delta).dropDuplicates().persist()
        dependencies.count()
        previous.unpersist()
        delta.unpersist()
    else:
        dependencies.unpersist()
        raise RuntimeError(
            "classification dependency traversal did not converge within "
            f"{max_closure_iterations} iterations"
        )
    return (
        dependencies.join(normalized.select("qid"), "qid", "inner")
        .join(selected_frame.select("qid"), "qid", "left_anti")
        .dropDuplicates()
    )


def _pruned_row(row: Any, selected_qids: set[str]) -> dict[str, Any]:
    import json

    payload = json.loads(row["payload_json"])
    pruned, count = prepare_subset_payload(
        payload,
        selected_qids,
        dependency_row=not row["is_selected"],
    )
    return {
        "qid": row["qid"],
        "qid_numeric": row["qid_numeric"],
        "payload_json": canonical_json(pruned),
        "pruned_count": count,
        "is_selected": row["is_selected"],
    }


def _output_schema() -> Any:
    from pyspark.sql.types import (
        BooleanType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    return StructType(
        [
            StructField("qid", StringType(), False),
            StructField("qid_numeric", LongType(), False),
            StructField("payload_json", StringType(), False),
            StructField("pruned_count", LongType(), False),
            StructField("is_selected", BooleanType(), False),
        ]
    )


def build_subset(
    spark: Any,
    normalized: Any,
    config: SubsetSelectionConfig,
    *,
    max_closure_iterations: int = 64,
) -> SparkSubsetBuild:
    """Build ordered output rows and force metrics without full driver collect."""

    from pyspark.sql import functions as F

    if max_closure_iterations < 1:
        raise ValueError("max_closure_iterations must be positive")
    entity_types, closure = classify_entities(
        spark,
        normalized,
        max_closure_iterations=max_closure_iterations,
    )
    selection = select_with_spark(spark, normalized, entity_types, config)
    dependencies = dependency_rows(
        spark,
        normalized,
        selection,
        max_closure_iterations=max_closure_iterations,
    ).persist()
    dependency_count = dependencies.count()

    selected_frame = _selected_frame(spark, dict(selection.entity_types))
    selected_rows = normalized.join(
        F.broadcast(selected_frame.select("qid")),
        "qid",
        "inner",
    ).withColumn("is_selected", F.lit(True))
    dependency_payloads = normalized.join(dependencies, "qid", "inner").withColumn(
        "is_selected", F.lit(False)
    )
    source_rows = selected_rows.unionByName(dependency_payloads).select(
        "qid",
        "qid_numeric",
        "payload_json",
        "is_selected",
    )
    selected_qids = set(selection.selected_qids)
    broadcast_qids = spark.sparkContext.broadcast(selected_qids)
    output = spark.createDataFrame(
        source_rows.rdd.map(lambda row: _pruned_row(row, broadcast_qids.value)),
        schema=_output_schema(),
    ).persist()
    output_rows = output.count()
    selected_output = output.where(F.col("is_selected")).count()
    if selected_output != selection.selected_count:
        raise RuntimeError(
            "selected output row count differs from deterministic selection"
        )
    pruned_value = output.agg(F.sum("pruned_count").alias("count")).first()["count"]
    pruned_count = int(pruned_value or 0)
    lines = (
        output.repartition(1)
        .sortWithinPartitions("qid_numeric")
        .select(F.col("payload_json").alias("value"))
    )

    dependencies.unpersist()
    entity_types.unpersist()
    closure.unpersist()
    return SparkSubsetBuild(
        lines=lines,
        selection=selection,
        dependency_rows=dependency_count,
        pruned_relation_statements=pruned_count,
        output_rows=output_rows,
    )
