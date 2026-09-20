"""Shared Spark projections for as-of source record lifecycle."""

from __future__ import annotations

from typing import Any

from video_media_catalog.source_registry import SourceRegistrySnapshot

RECORD_IDENTITY_COLUMNS = (
    "source_system_id",
    "source_product_id",
    "source_namespace_id",
    "source_record_id",
)

COVERAGE_DOMAIN_COLUMNS = (
    "_batch_source_system_id",
    "_batch_source_product_id",
    "_coverage_scope_digest",
)

SOURCE_BATCH_METADATA_COLUMNS = (
    "_batch_id",
    "_batch_source_system_id",
    "_batch_source_product_id",
    "_batch_policy_id",
    "_batch_policy_digest",
    "_change_semantics",
    "_completeness",
    "_delete_coverage",
    "_coverage_scope_digest",
    "_batch_acquired_at",
    "_batch_record_count",
)


def assert_no_membership_closure_conflicts(memberships: Any) -> None:
    """Fail closed when one membership version has multiple closure times."""

    from pyspark.sql import functions as F

    conflicts = (
        memberships.where(F.col("valid_to").isNotNull())
        .groupBy(
            "source_namespace_id",
            "source_id",
            "source_referent_kind",
            "entity_key",
            "decision_id",
            "valid_from",
        )
        .agg(F.countDistinct("valid_to").alias("_closure_count"))
        .where(F.col("_closure_count") > 1)
        .limit(1)
    )
    if conflicts.count():
        raise ValueError("membership has conflicting closure times")


def select_effective_membership_versions(
    memberships: Any,
    *,
    as_of: str,
) -> Any:
    """Pick one version per membership key after closure conflict checks."""

    from pyspark.sql import Window
    from pyspark.sql import functions as F

    assert_no_membership_closure_conflicts(memberships)
    as_of_timestamp = F.to_timestamp(F.lit(as_of))
    membership_window = Window.partitionBy(
        "source_namespace_id",
        "source_id",
        "source_referent_kind",
        "entity_key",
        "decision_id",
        "valid_from",
    ).orderBy(
        F.when(F.col("valid_to").isNotNull(), F.lit(1)).otherwise(F.lit(0)).desc(),
        F.col("valid_to").asc_nulls_last(),
        F.col("membership_key").desc(),
    )
    versioned = memberships.withColumn(
        "_membership_version",
        F.row_number().over(membership_window),
    )
    return (
        versioned.where(F.col("_membership_version") == 1)
        .drop("_membership_version")
        .where(
            (F.to_timestamp("valid_from") <= as_of_timestamp)
            & (
                F.col("valid_to").isNull()
                | (F.to_timestamp("valid_to") > as_of_timestamp)
            )
        )
    )


def bind_committed_source_records(
    *,
    source_records: Any,
    ingest_runs: Any,
    committed_run_ids: tuple[str, ...],
    registry: SourceRegistrySnapshot,
) -> Any:
    """Bind committed source records to pinned ingest-run batch metadata."""

    from pyspark.sql import functions as F

    spark = source_records.sparkSession
    committed = spark.createDataFrame(
        [(run_id,) for run_id in committed_run_ids],
        "run_id STRING",
    )
    selected_records = source_records.join(committed, "run_id", "inner")
    batch_path = "$.inputManifest.batchManifest."
    metadata = (
        ingest_runs.where(F.col("run_kind") == "SOURCE_ASSERTIONS")
        .join(committed, "run_id", "inner")
        .select(
            "run_id",
            F.col("source_product_id").alias("_run_source_product_id"),
            F.get_json_object("manifest_json", "$.inputManifest.registryDigest").alias(
                "_registry_digest"
            ),
            F.get_json_object("manifest_json", batch_path + "batchId").alias(
                "_batch_id"
            ),
            F.get_json_object("manifest_json", batch_path + "sourceSystemId").alias(
                "_batch_source_system_id"
            ),
            F.get_json_object("manifest_json", batch_path + "sourceProductId").alias(
                "_batch_source_product_id"
            ),
            F.get_json_object("manifest_json", batch_path + "policyId").alias(
                "_batch_policy_id"
            ),
            F.get_json_object("manifest_json", batch_path + "policyDigest").alias(
                "_batch_policy_digest"
            ),
            F.get_json_object("manifest_json", batch_path + "changeSemantics").alias(
                "_change_semantics"
            ),
            F.get_json_object("manifest_json", batch_path + "completeness").alias(
                "_completeness"
            ),
            F.get_json_object("manifest_json", batch_path + "deleteCoverage").alias(
                "_delete_coverage"
            ),
            F.get_json_object(
                "manifest_json", batch_path + "coverageScopeDigest"
            ).alias("_coverage_scope_digest"),
            F.get_json_object("manifest_json", batch_path + "acquiredAt").alias(
                "_batch_acquired_at"
            ),
            F.get_json_object("manifest_json", batch_path + "recordCount")
            .cast("long")
            .alias("_batch_record_count"),
        )
        .persist()
    )
    required_metadata = (
        "_registry_digest",
        *SOURCE_BATCH_METADATA_COLUMNS,
    )
    invalid_metadata = F.lit(False)
    for column in required_metadata:
        invalid_metadata = invalid_metadata | F.col(column).isNull()
    if metadata.where(invalid_metadata).limit(1).count():
        metadata.unpersist()
        raise ValueError("source ingest run lacks pinned batch metadata")
    if (
        metadata.where(F.col("_registry_digest") != F.lit(registry.digest))
        .limit(1)
        .count()
    ):
        metadata.unpersist()
        raise ValueError("source ingest run used another registry snapshot")
    if (
        metadata.where(
            F.col("_run_source_product_id") != F.col("_batch_source_product_id")
        )
        .limit(1)
        .count()
    ):
        metadata.unpersist()
        raise ValueError("source ingest run and batch products differ")
    if metadata.groupBy("run_id").count().where(F.col("count") != 1).limit(1).count():
        metadata.unpersist()
        raise ValueError("source ingest run metadata is duplicated")

    actual_counts = selected_records.groupBy("run_id").count()
    if (
        metadata.join(actual_counts, "run_id", "left")
        .fillna(0, subset=["count"])
        .where(F.col("count") != F.col("_batch_record_count"))
        .limit(1)
        .count()
    ):
        metadata.unpersist()
        raise ValueError("committed source run does not contain its complete batch")

    bound = (
        selected_records.alias("r")
        .join(metadata.alias("m"), "run_id", "inner")
        .select(
            "r.*",
            *(
                F.col(f"m.{column}").alias(column)
                for column in required_metadata
                if column != "_registry_digest"
            ),
        )
        .persist()
    )
    if bound.count() != selected_records.count():
        metadata.unpersist()
        bound.unpersist()
        raise ValueError("source record has no committed ingest-run metadata")
    if (
        bound.where(
            (F.col("batch_id") != F.col("_batch_id"))
            | (F.col("source_system_id") != F.col("_batch_source_system_id"))
            | (F.col("source_product_id") != F.col("_batch_source_product_id"))
            | (F.col("policy_id") != F.col("_batch_policy_id"))
            | (F.col("policy_digest") != F.col("_batch_policy_digest"))
        )
        .limit(1)
        .count()
    ):
        metadata.unpersist()
        bound.unpersist()
        raise ValueError("source record does not bind its committed batch")
    metadata.unpersist()
    return bound


def build_source_lifecycle_events(
    bound: Any,
    *,
    as_of: str,
) -> Any:
    """Resolve committed source operations into lifecycle events."""

    from pyspark.sql import Window
    from pyspark.sql import functions as F

    as_of_timestamp = F.to_timestamp(F.lit(as_of))
    metadata = (
        bound.select(
            "run_id",
            "_batch_id",
            "_batch_source_system_id",
            "_batch_source_product_id",
            "_change_semantics",
            "_completeness",
            "_delete_coverage",
            "_coverage_scope_digest",
            "_batch_acquired_at",
        )
        .dropDuplicates(["run_id"])
        .where(F.to_timestamp("_batch_acquired_at") <= as_of_timestamp)
        .persist()
    )
    full_window = Window.partitionBy(*COVERAGE_DOMAIN_COLUMNS).orderBy(
        F.to_timestamp("_batch_acquired_at"),
        "_batch_id",
        "run_id",
    )
    full_snapshots = (
        metadata.where(
            (F.col("_change_semantics") == "FULL_SNAPSHOT")
            & (F.col("_completeness") == "COMPLETE")
            & (F.col("_delete_coverage") == "SNAPSHOT_DIFF")
        )
        .withColumn("_previous_run_id", F.lag("run_id").over(full_window))
        .withColumn("_previous_batch_id", F.lag("_batch_id").over(full_window))
        .withColumn(
            "_previous_acquired_at",
            F.lag("_batch_acquired_at").over(full_window),
        )
    )
    snapshot_diffs = full_snapshots.where(
        F.col("_delete_coverage") == "SNAPSHOT_DIFF"
    ).persist()
    try:
        baselines = snapshot_diffs.where(F.col("_previous_run_id").isNull()).select(
            "run_id",
            *COVERAGE_DOMAIN_COLUMNS,
            "_batch_acquired_at",
        )
        prior_same_coverage = baselines.alias("b").join(
            metadata.alias("p"),
            (
                (
                    F.col("b._batch_source_system_id")
                    == F.col("p._batch_source_system_id")
                )
                & (
                    F.col("b._batch_source_product_id")
                    == F.col("p._batch_source_product_id")
                )
                & (
                    F.col("b._coverage_scope_digest")
                    == F.col("p._coverage_scope_digest")
                )
                & (
                    F.to_timestamp("p._batch_acquired_at")
                    <= F.to_timestamp("b._batch_acquired_at")
                )
                & (F.col("p.run_id") != F.col("b.run_id"))
                & (F.col("p._change_semantics") == "FULL_SNAPSHOT")
                & (F.col("p._completeness") == "COMPLETE")
            ),
            "inner",
        )
        if prior_same_coverage.limit(1).count():
            raise ValueError(
                "snapshot-diff requires a prior complete snapshot for its coverage"
            )

        valid_inferred_runs = snapshot_diffs.where(
            F.col("_previous_run_id").isNotNull()
        ).select("run_id")
        if (
            bound.where(F.col("operation") == "INFERRED_ABSENCE")
            .select("run_id")
            .join(valid_inferred_runs, "run_id", "left_anti")
            .limit(1)
            .count()
        ):
            raise ValueError(
                "inferred absence requires a prior complete snapshot for coverage"
            )

        snapshot_pairs = snapshot_diffs.where(
            F.col("_previous_run_id").isNotNull()
        ).select(
            F.col("run_id").alias("_snapshot_run_id"),
            "_previous_run_id",
            "_batch_id",
            "_batch_acquired_at",
            "_previous_acquired_at",
            *COVERAGE_DOMAIN_COLUMNS,
        )
        coverage_match = (
            (F.col("p._batch_source_system_id") == F.col("r.source_system_id"))
            & (F.col("p._batch_source_product_id") == F.col("r.source_product_id"))
            & (F.col("p._coverage_scope_digest") == F.col("r._coverage_scope_digest"))
        )
        previous_ids = (
            snapshot_pairs.alias("p")
            .join(
                bound.alias("r"),
                coverage_match
                & (
                    F.to_timestamp("r._batch_acquired_at")
                    >= F.to_timestamp("p._previous_acquired_at")
                )
                & (
                    F.to_timestamp("r._batch_acquired_at")
                    < F.to_timestamp("p._batch_acquired_at")
                ),
                "inner",
            )
            .where(F.col("r.operation") == "UPSERT")
            .select(
                "p._snapshot_run_id",
                "p._batch_id",
                "p._batch_acquired_at",
                *(
                    F.col(f"r.{column}").alias(column)
                    for column in RECORD_IDENTITY_COLUMNS
                ),
            )
            .dropDuplicates(["_snapshot_run_id", *RECORD_IDENTITY_COLUMNS])
        )
        current_ids = (
            snapshot_pairs.alias("p")
            .join(
                bound.alias("r"),
                (F.col("p._snapshot_run_id") == F.col("r.run_id")) & coverage_match,
                "inner",
            )
            .where(F.col("r.operation") == "UPSERT")
            .select(
                "p._snapshot_run_id",
                *(
                    F.col(f"r.{column}").alias(column)
                    for column in RECORD_IDENTITY_COLUMNS
                ),
            )
            .dropDuplicates(["_snapshot_run_id", *RECORD_IDENTITY_COLUMNS])
        )
        missing = previous_ids.join(
            current_ids,
            ["_snapshot_run_id", *RECORD_IDENTITY_COLUMNS],
            "left_anti",
        )
        inferred_events = missing.select(
            F.lit(None).cast("string").alias("envelope_key"),
            *RECORD_IDENTITY_COLUMNS,
            F.lit("INFERRED_ABSENCE").alias("operation"),
            F.col("_batch_acquired_at").alias("observed_at"),
            F.col("_batch_acquired_at").alias("ingested_at"),
            F.lit(None).cast("string").alias("valid_from"),
            F.lit(None).cast("string").alias("valid_to"),
            F.lit(None).cast("string").alias("expires_at"),
            F.col("_batch_id").alias("batch_id"),
        )
        actual_events = bound.where(
            F.to_timestamp("_batch_acquired_at") <= as_of_timestamp
        ).select(
            "envelope_key",
            *RECORD_IDENTITY_COLUMNS,
            "operation",
            "observed_at",
            "ingested_at",
            "valid_from",
            "valid_to",
            "expires_at",
            "batch_id",
        )
        return actual_events.unionByName(inferred_events)
    finally:
        snapshot_diffs.unpersist()
        metadata.unpersist()


def latest_source_record_states(events: Any, *, as_of: str) -> Any:
    """Return the latest lifecycle state per source record identity."""

    from pyspark.sql import Window
    from pyspark.sql import functions as F

    as_of_timestamp = F.to_timestamp(F.lit(as_of))
    with_effective = events.withColumn(
        "_effective_at",
        F.coalesce(
            F.to_timestamp("valid_from"),
            F.to_timestamp("observed_at"),
        ),
    ).where(F.col("_effective_at") <= as_of_timestamp)
    event_window = Window.partitionBy(*RECORD_IDENTITY_COLUMNS).orderBy(
        F.col("_effective_at").desc(),
        F.to_timestamp("observed_at").desc(),
        F.to_timestamp("ingested_at").desc(),
        F.when(F.col("operation") == "UPSERT", F.lit(0)).otherwise(F.lit(1)).desc(),
        F.col("batch_id").desc(),
        F.col("envelope_key").desc_nulls_last(),
    )
    return (
        with_effective.withColumn("_record_version", F.row_number().over(event_window))
        .where(F.col("_record_version") == 1)
        .drop("_record_version")
    )


def current_upsert_envelope_keys(
    *,
    source_records: Any,
    ingest_runs: Any,
    committed_run_ids: tuple[str, ...],
    registry: SourceRegistrySnapshot,
    as_of: str,
) -> Any:
    """Resolve committed source records into active UPSERT envelope keys."""

    from pyspark.sql import functions as F

    bound = bind_committed_source_records(
        source_records=source_records,
        ingest_runs=ingest_runs,
        committed_run_ids=committed_run_ids,
        registry=registry,
    )
    try:
        events = build_source_lifecycle_events(bound, as_of=as_of)
        latest = latest_source_record_states(events, as_of=as_of)
        as_of_timestamp = F.to_timestamp(F.lit(as_of))
        current = (
            latest.where(F.col("operation") == "UPSERT")
            .where(
                F.col("valid_to").isNull()
                | (F.to_timestamp("valid_to") > as_of_timestamp)
            )
            .where(
                F.col("expires_at").isNull()
                | (F.to_timestamp("expires_at") > as_of_timestamp)
            )
            .select("envelope_key")
            .dropDuplicates(["envelope_key"])
            .persist()
        )
        current.count()
        return current
    finally:
        bound.unpersist()


def inactive_source_records(
    *,
    source_records: Any,
    ingest_runs: Any,
    committed_run_ids: tuple[str, ...],
    registry: SourceRegistrySnapshot,
    as_of: str,
) -> Any:
    """Return source records whose latest lifecycle state is not an active UPSERT."""

    from pyspark.sql import functions as F

    bound = bind_committed_source_records(
        source_records=source_records,
        ingest_runs=ingest_runs,
        committed_run_ids=committed_run_ids,
        registry=registry,
    )
    try:
        events = build_source_lifecycle_events(bound, as_of=as_of)
        latest = latest_source_record_states(events, as_of=as_of)
        as_of_timestamp = F.to_timestamp(F.lit(as_of))
        return latest.where(
            (F.col("operation") != "UPSERT")
            | (
                F.col("valid_to").isNotNull()
                & (F.to_timestamp("valid_to") <= as_of_timestamp)
            )
            | (
                F.col("expires_at").isNotNull()
                & (F.to_timestamp("expires_at") <= as_of_timestamp)
            )
        ).select(
            F.col("source_namespace_id").alias("subject_namespace_id"),
            F.col("source_record_id").alias("subject_source_id"),
            F.col("observed_at").alias("lifecycle_observed_at"),
        )
    finally:
        bound.unpersist()


def filter_assertions_for_current_envelopes(
    assertions: Any,
    *,
    current_envelope_keys: Any,
    as_of: str,
) -> Any:
    """Keep only assertions bound to active UPSERT envelopes and valid as-of."""

    from pyspark.sql import functions as F

    as_of_timestamp = F.to_timestamp(F.lit(as_of))
    active = assertions.where(F.col("status") == "ACTIVE").withColumn(
        "envelope_key",
        F.get_json_object("provenance_json", "$.envelopeKey"),
    )
    valid_to = F.to_timestamp(
        F.get_json_object("provenance_json", "$.validTo"),
    )
    return (
        active.join(current_envelope_keys, "envelope_key", "inner")
        .where(valid_to.isNull() | (valid_to > as_of_timestamp))
        .drop("envelope_key")
    )
