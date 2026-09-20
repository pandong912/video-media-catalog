"""Synthetic Spark benchmark for bounded exact identity resolution."""

from __future__ import annotations

import contextlib
import time
from typing import Any

from video_media_catalog.identity_spark import (
    DEFAULT_IDENTITY_RESOLUTION_CONFIG,
    IdentityResolutionConfig,
    _release_exact_blocking_labels,
    assign_exact_blocking_component_ids,
    build_exact_blocking_component_stats,
)


def _executed_plan_shuffle_metrics(frame: Any) -> dict[str, int | bool]:
    """Read bounded SQL-plan shuffle counters after an action."""

    totals = {
        "bytesWritten": 0,
        "recordsWritten": 0,
        "readBytes": 0,
        "recordsRead": 0,
    }
    available = False
    with contextlib.suppress(Exception):
        root = frame._jdf.queryExecution().executedPlan()
        pending = [root]
        visited: set[str] = set()
        while pending:
            plan = pending.pop()
            identity = str(plan.id())
            if identity in visited:
                continue
            visited.add(identity)
            if plan.nodeName() == "AdaptiveSparkPlan":
                pending.append(plan.executedPlan())
            metrics = plan.metrics().iterator()
            while metrics.hasNext():
                entry = metrics.next()
                metric = entry._2()
                name_option = metric.name()
                name = (
                    str(name_option.get()).lower()
                    if name_option.isDefined()
                    else ""
                )
                value = int(metric.value())
                if "shuffle bytes written" in name:
                    totals["bytesWritten"] += value
                    available = True
                elif "shuffle records written" in name:
                    totals["recordsWritten"] += value
                    available = True
                elif "remote bytes read" in name or "local bytes read" in name:
                    totals["readBytes"] += value
                    available = True
                elif "records read" in name and "shuffle" in name:
                    totals["recordsRead"] += value
                    available = True
            children = plan.children().iterator()
            while children.hasNext():
                pending.append(children.next())
    return {**totals, "available": available}


def _spark_job_shuffle_metrics(
    spark: Any,
    *,
    job_ids: set[int],
) -> dict[str, int | bool]:
    """Aggregate completed stage metrics for benchmark-created Spark jobs."""

    totals: dict[str, int | bool] = {
        "bytesWritten": 0,
        "recordsWritten": 0,
        "readBytes": 0,
        "recordsRead": 0,
        "memoryBytesSpilled": 0,
        "diskBytesSpilled": 0,
        "stageCount": 0,
        "jobCount": len(job_ids),
        "available": False,
    }
    with contextlib.suppress(Exception):
        context = spark.sparkContext
        tracker = context.statusTracker()
        store = context._jsc.sc().statusStore()
        empty_list = context._jvm.java.util.Collections.emptyList()
        quantiles = context._gateway.new_array(context._jvm.double, 0)
        stage_ids = {
            int(stage_id)
            for job_id in job_ids
            if (job_info := tracker.getJobInfo(job_id)) is not None
            for stage_id in job_info.stageIds
        }
        for stage_id in stage_ids:
            attempts = store.stageData(
                stage_id,
                False,
                empty_list,
                False,
                quantiles,
            ).iterator()
            completed = []
            while attempts.hasNext():
                attempt = attempts.next()
                if str(attempt.status()) == "COMPLETE":
                    completed.append(attempt)
            if not completed:
                continue
            stage = max(completed, key=lambda item: int(item.attemptId()))
            totals["bytesWritten"] += int(stage.shuffleWriteBytes())
            totals["recordsWritten"] += int(stage.shuffleWriteRecords())
            totals["readBytes"] += int(stage.shuffleReadBytes())
            totals["recordsRead"] += int(stage.shuffleReadRecords())
            totals["memoryBytesSpilled"] += int(stage.memoryBytesSpilled())
            totals["diskBytesSpilled"] += int(stage.diskBytesSpilled())
            totals["stageCount"] += 1
        totals["available"] = bool(totals["stageCount"])
    return totals


def run_identity_synthetic_benchmark(
    spark: Any,
    *,
    node_count: int = 10_000,
    component_size: int = 8,
    conflict_every_components: int = 20,
    partitions: int | None = None,
    resolution_config: IdentityResolutionConfig | None = None,
) -> dict[str, object]:
    """Run a deterministic, bounded exact-blocking workload."""

    if node_count < 1:
        raise ValueError("node_count must be positive")
    if component_size < 1:
        raise ValueError("component_size must be positive")
    if conflict_every_components < 1:
        raise ValueError("conflict_every_components must be positive")
    config = resolution_config or DEFAULT_IDENTITY_RESOLUTION_CONFIG

    from pyspark.sql import functions as F

    status_tracker = spark.sparkContext.statusTracker()
    jobs_before = set(status_tracker.getJobIdsForGroup())
    partition_count = partitions or max(
        2,
        min(512, spark.sparkContext.defaultParallelism),
    )
    numeric_nodes = spark.range(
        0,
        node_count,
        1,
        numPartitions=partition_count,
    ).withColumn("component_number", F.floor(F.col("id") / component_size))
    nodes = numeric_nodes.select(
        F.format_string("node-%012d", F.col("id")).alias("node_id")
    )
    blocking_edges = numeric_nodes.select(
        F.format_string("node-%012d", F.col("id")).alias("node_id"),
        F.format_string(
            "block-%012d",
            F.col("component_number"),
        ).alias("blocking_key"),
    )
    base_candidate = F.concat(
        F.lit("entity-"),
        F.format_string("%012d", F.col("component_number")),
    )
    conflict_candidate = F.concat(
        F.lit("entity-conflict-"),
        F.format_string("%012d", F.col("component_number")),
    )
    candidate_keys = F.when(
        (F.col("component_number") % conflict_every_components == 0)
        & (F.col("id") % component_size == 0),
        F.array(base_candidate, conflict_candidate),
    ).otherwise(F.array(base_candidate))
    synthetic_work = numeric_nodes.select(
        F.format_string("node-%012d", F.col("id")).alias("node_id"),
        candidate_keys.alias("candidate_entity_keys"),
    )

    started = time.perf_counter()
    labels = assign_exact_blocking_component_ids(
        nodes,
        blocking_edges,
        max_iterations=config.max_exact_blocking_label_iterations,
    )
    classified = None
    try:
        stats = build_exact_blocking_component_stats(
            synthetic_work.join(labels, "node_id"),
            config=config,
        )
        classified = stats.withColumn(
            "resolution_mode",
            F.when(
                F.col("component_node_count")
                > F.lit(config.max_exact_blocking_component_size),
                F.lit("EXACT_BLOCKING_COMPONENT_TOO_LARGE"),
            )
            .when(
                F.col("component_candidate_count")
                > F.lit(config.max_exact_blocking_component_candidate_keys),
                F.lit("EXACT_BLOCKING_COMPONENT_CANDIDATE_LIMIT_EXCEEDED"),
            )
            .when(
                F.size("component_candidate_keys") > 1,
                F.lit("MULTIPLE_EXACT_IDENTIFIER_CANDIDATES"),
            )
            .otherwise(F.lit("RESOLVED")),
        )
        summary_frame = classified.agg(
            F.count(F.lit(1)).alias("node_count"),
            F.countDistinct("component_id").alias("component_count"),
            F.sum(
                F.when(
                    F.col("resolution_mode")
                    == "EXACT_BLOCKING_COMPONENT_TOO_LARGE",
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).alias("oversized_conflicts"),
            F.sum(
                F.when(
                    F.col("resolution_mode")
                    == "EXACT_BLOCKING_COMPONENT_CANDIDATE_LIMIT_EXCEEDED",
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).alias("candidate_limit_conflicts"),
            F.sum(
                F.when(
                    F.col("resolution_mode")
                    == "MULTIPLE_EXACT_IDENTIFIER_CANDIDATES",
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).alias("multiple_candidate_conflicts"),
        )
        summary = summary_frame.first()
        runtime_seconds = time.perf_counter() - started
        benchmark_job_ids = set(status_tracker.getJobIdsForGroup()) - jobs_before
        shuffle_metrics = _spark_job_shuffle_metrics(
            spark,
            job_ids=benchmark_job_ids,
        )
        if not shuffle_metrics["available"]:
            shuffle_metrics = {
                **shuffle_metrics,
                **_executed_plan_shuffle_metrics(summary_frame),
            }
        conflict_counts = {
            "EXACT_BLOCKING_COMPONENT_TOO_LARGE": int(
                summary.oversized_conflicts or 0
            ),
            "EXACT_BLOCKING_COMPONENT_CANDIDATE_LIMIT_EXCEEDED": int(
                summary.candidate_limit_conflicts or 0
            ),
            "MULTIPLE_EXACT_IDENTIFIER_CANDIDATES": int(
                summary.multiple_candidate_conflicts or 0
            ),
        }
        return {
            "schemaVersion": "1.0",
            "nodeCount": int(summary.node_count),
            "edgeCount": node_count,
            "componentCount": int(summary.component_count),
            "runtimeSeconds": runtime_seconds,
            "shuffleMetrics": shuffle_metrics,
            "conflictCountsByReason": conflict_counts,
            "identityResolutionConfigDigest": config.digest,
            "identityResolutionConfig": config.model_dump(
                mode="json",
                by_alias=True,
            ),
            "sparkVersion": spark.version,
            "partitions": partition_count,
        }
    finally:
        if classified is not None:
            with contextlib.suppress(Exception):
                classified.unpersist()
        _release_exact_blocking_labels(labels)
