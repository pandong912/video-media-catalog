"""Shared Spark projections for as-of source record lifecycle."""

from __future__ import annotations

import uuid
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

SOURCE_LIFECYCLE_RECORD_COLUMNS = (
    "run_id",
    "envelope_key",
    "batch_id",
    *RECORD_IDENTITY_COLUMNS,
    "operation",
    "observed_at",
    "ingested_at",
    "valid_from",
    "valid_to",
    "expires_at",
    "policy_id",
    "policy_digest",
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


def _committed_run_frame(
    *,
    source_records: Any,
    committed_run_ids: tuple[str, ...] | None,
    committed_runs: Any | None,
) -> Any:
    if committed_runs is not None:
        if committed_run_ids:
            raise ValueError("provide committed_runs or committed_run_ids, not both")
        if "run_id" not in committed_runs.columns:
            raise ValueError("committed_runs dataframe requires run_id")
        return committed_runs.select("run_id").dropDuplicates(["run_id"])
    if not committed_run_ids:
        raise ValueError("source lifecycle requires committed runs")
    return source_records.sparkSession.createDataFrame(
        [(run_id,) for run_id in committed_run_ids],
        "run_id STRING",
    )


def bind_committed_source_records(
    *,
    source_records: Any,
    ingest_runs: Any,
    committed_run_ids: tuple[str, ...] | None = None,
    committed_runs: Any | None = None,
    registry: SourceRegistrySnapshot,
    repartition_count: int | None = None,
) -> Any:
    """Bind committed source records to pinned ingest-run batch metadata."""

    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    if repartition_count is not None and (
        isinstance(repartition_count, bool) or repartition_count < 1
    ):
        raise ValueError("source lifecycle repartition_count must be positive")
    committed = _committed_run_frame(
        source_records=source_records,
        committed_run_ids=committed_run_ids,
        committed_runs=committed_runs,
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
            F.col("started_at").alias("_run_started_at"),
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
            *(
                F.col(f"r.{column}").alias(column)
                for column in SOURCE_LIFECYCLE_RECORD_COLUMNS
            ),
            *(
                F.col(f"m.{column}").alias(column)
                for column in required_metadata
                if column != "_registry_digest"
            ),
            F.col("m._run_started_at").alias("_run_started_at"),
        )
    )
    if repartition_count is not None:
        bound = bound.repartition(repartition_count, "envelope_key")
    # The full IMDb lifecycle projection is larger than executor storage memory.
    # Keep it on disk and retain its source lineage so an executor loss can
    # recompute a missing partition instead of invalidating a local checkpoint.
    bound = bound.persist(StorageLevel.DISK_ONLY)
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
            F.col("_snapshot_run_id").alias("run_id"),
            *RECORD_IDENTITY_COLUMNS,
            F.lit("INFERRED_ABSENCE").alias("operation"),
            F.col("_batch_acquired_at").alias("observed_at"),
            F.col("_batch_acquired_at").alias("ingested_at"),
            F.lit(None).cast("string").alias("valid_from"),
            F.lit(None).cast("string").alias("valid_to"),
            F.lit(None).cast("string").alias("expires_at"),
            F.col("_batch_id").alias("batch_id"),
            F.lit(None).cast("string").alias("_run_started_at"),
        )
        actual_events = bound.where(
            F.to_timestamp("_batch_acquired_at") <= as_of_timestamp
        ).select(
            "envelope_key",
            "run_id",
            *RECORD_IDENTITY_COLUMNS,
            "operation",
            "observed_at",
            "ingested_at",
            "valid_from",
            "valid_to",
            "expires_at",
            "batch_id",
            "_run_started_at",
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
    order_by = [
        F.col("_effective_at").desc(),
        F.to_timestamp("observed_at").desc(),
        F.to_timestamp("ingested_at").desc(),
        F.when(F.col("operation") == "UPSERT", F.lit(0)).otherwise(F.lit(1)).desc(),
        F.col("batch_id").desc(),
    ]
    # Same envelope republished by a later mapper shares observed/ingested
    # timestamps. Prefer the later ingest, then a stable run id.
    if "_run_started_at" in events.columns:
        order_by.append(F.to_timestamp("_run_started_at").desc_nulls_last())
    if "run_id" in events.columns:
        order_by.append(F.col("run_id").desc_nulls_last())
    order_by.append(F.col("envelope_key").desc_nulls_last())
    event_window = Window.partitionBy(*RECORD_IDENTITY_COLUMNS).orderBy(*order_by)
    return (
        with_effective.withColumn("_record_version", F.row_number().over(event_window))
        .where(F.col("_record_version") == 1)
        .drop("_record_version")
    )


def _as_of_timestamp(as_of: str) -> Any:
    from pyspark.sql import functions as F

    return F.to_timestamp(F.lit(as_of))


def _is_active_upsert(latest: Any, *, as_of: str) -> Any:
    from pyspark.sql import functions as F

    as_of_timestamp = _as_of_timestamp(as_of)
    return (
        (F.col("operation") == "UPSERT")
        & (F.col("valid_to").isNull() | (F.to_timestamp("valid_to") > as_of_timestamp))
        & (
            F.col("expires_at").isNull()
            | (F.to_timestamp("expires_at") > as_of_timestamp)
        )
    )


def _is_inactive_record(latest: Any, *, as_of: str) -> Any:
    from pyspark.sql import functions as F

    as_of_timestamp = _as_of_timestamp(as_of)
    return (
        (F.col("operation") != "UPSERT")
        | (
            F.col("valid_to").isNotNull()
            & (F.to_timestamp("valid_to") <= as_of_timestamp)
        )
        | (
            F.col("expires_at").isNotNull()
            & (F.to_timestamp("expires_at") <= as_of_timestamp)
        )
    )


def _ensure_source_lifecycle_checkpoint_dir(frame: Any) -> None:
    """Configure durable lifecycle checkpoints for every Spark entry point."""

    spark = frame.sparkSession
    context = spark.sparkContext
    java_dir = context._jsc.sc().getCheckpointDir()
    if java_dir.isDefined():
        return
    warehouse = str(spark.conf.get("spark.sql.warehouse.dir", "/tmp"))
    if not warehouse.startswith(("s3://", "s3a://")):
        catalog_warehouses = sorted(
            {
                str(value)
                for key, value in context.getConf().getAll()
                if key.startswith("spark.sql.catalog.")
                and key.endswith(".warehouse")
                and str(value).startswith(("s3://", "s3a://"))
            }
        )
        if catalog_warehouses:
            warehouse = catalog_warehouses[0]
        elif not context.master.startswith("local"):
            raise RuntimeError(
                "distributed source lifecycle checkpoints require a remote warehouse"
            )
    checkpoint_run_id = f"{context.applicationId}-{uuid.uuid4().hex}"
    context.setCheckpointDir(
        f"{warehouse.rstrip('/')}/source-lifecycle-checkpoints/{checkpoint_run_id}"
    )


def persist_latest_source_record_states(
    *,
    source_records: Any,
    ingest_runs: Any,
    committed_run_ids: tuple[str, ...] | None = None,
    committed_runs: Any | None = None,
    registry: SourceRegistrySnapshot,
    as_of: str,
    repartition_count: int | None = None,
) -> tuple[Any, Any]:
    """Build durable shared as-of source lifecycle projections."""

    from pyspark import StorageLevel

    raw_bound = bind_committed_source_records(
        source_records=source_records,
        ingest_runs=ingest_runs,
        committed_run_ids=committed_run_ids,
        committed_runs=committed_runs,
        registry=registry,
        repartition_count=repartition_count,
    )
    _ensure_source_lifecycle_checkpoint_dir(raw_bound)
    bound = None
    latest = None
    try:
        bound = raw_bound.checkpoint(eager=True).persist(StorageLevel.DISK_ONLY)
        bound.count()
        raw_bound.unpersist()
        events = build_source_lifecycle_events(bound, as_of=as_of)
        # Durable checkpoints truncate the very large lifecycle plan without
        # depending on executor-local blocks. The disk cache then avoids
        # repeatedly reading checkpoint objects during Identity and Gold.
        latest = (
            latest_source_record_states(events, as_of=as_of)
            .checkpoint(eager=True)
            .persist(StorageLevel.DISK_ONLY)
        )
        latest.count()
        return bound, latest
    except Exception:
        raw_bound.unpersist()
        if bound is not None:
            bound.unpersist()
        if latest is not None:
            latest.unpersist()
        raise


def current_envelope_keys_from_latest(latest: Any, *, as_of: str) -> Any:
    """Derive active UPSERT envelope keys from a persisted latest-state projection."""

    columns = ["envelope_key"]
    if "run_id" in latest.columns:
        columns.append("run_id")
    return (
        latest.where(_is_active_upsert(latest, as_of=as_of))
        .select(*columns)
        .dropDuplicates(["envelope_key"])
    )


def inactive_source_records_from_latest(latest: Any, *, as_of: str) -> Any:
    """Derive inactive source records from a persisted latest-state projection."""

    from pyspark.sql import functions as F

    return latest.where(_is_inactive_record(latest, as_of=as_of)).select(
        *RECORD_IDENTITY_COLUMNS,
        "envelope_key",
        "operation",
        F.col("observed_at").alias("lifecycle_observed_at"),
    )


def _assertion_envelope_bindings(*assertion_frames: Any) -> Any:
    from pyspark.sql import functions as F

    bindings = None
    for frame in assertion_frames:
        selected = frame.select(
            "assertion_id",
            "subject_namespace_id",
            "subject_source_id",
            "subject_referent_kind",
            "policy_id",
            "policy_digest",
            "observed_at",
            F.get_json_object("provenance_json", "$.envelopeKey").alias("envelope_key"),
        ).where(F.col("envelope_key").isNotNull())
        bindings = (
            selected
            if bindings is None
            else bindings.unionByName(selected, allowMissingColumns=True)
        )
    if bindings is None:
        raise ValueError("assertion envelope bindings require at least one frame")
    return bindings


def build_inactive_membership_revocation_worklist(
    *,
    latest_states: Any,
    bound: Any,
    memberships: Any,
    type_assertions: Any,
    identifier_assertions: Any,
    as_of: str,
) -> tuple[Any, Any]:
    """Map inactive records to source nodes and join open memberships for revocation."""

    from pyspark.sql import functions as F

    inactive = inactive_source_records_from_latest(latest_states, as_of=as_of).select(
        *RECORD_IDENTITY_COLUMNS,
        F.col("envelope_key").alias("lifecycle_envelope_key"),
        "operation",
        "lifecycle_observed_at",
    )
    identity_envelopes = (
        bound.select(
            *RECORD_IDENTITY_COLUMNS,
            F.col("envelope_key").alias("historical_envelope_key"),
        )
        .where(F.col("historical_envelope_key").isNotNull())
        .dropDuplicates([*RECORD_IDENTITY_COLUMNS, "historical_envelope_key"])
    )
    assertion_bindings = _assertion_envelope_bindings(
        type_assertions,
        identifier_assertions,
    )
    historical_bindings = identity_envelopes.join(
        assertion_bindings,
        identity_envelopes.historical_envelope_key == assertion_bindings.envelope_key,
        "inner",
    )
    mapped = (
        inactive.join(historical_bindings, list(RECORD_IDENTITY_COLUMNS), "left")
        .groupBy(
            *RECORD_IDENTITY_COLUMNS,
            "lifecycle_envelope_key",
            "operation",
            "lifecycle_observed_at",
        )
        .agg(
            F.sort_array(
                F.collect_set(
                    F.when(
                        F.col("subject_namespace_id").isNotNull(),
                        F.struct(
                            F.col("subject_namespace_id").alias("namespace_id"),
                            F.col("subject_source_id").alias("source_id"),
                            F.col("subject_referent_kind").alias("referent_kind"),
                        ),
                    )
                )
            ).alias("source_nodes"),
            F.sort_array(F.collect_set("assertion_id")).alias("assertion_ids"),
            F.min("policy_id").alias("policy_id"),
            F.min("policy_digest").alias("policy_digest"),
            F.min("observed_at").alias("assertion_observed_at"),
        )
        .withColumn(
            "source_nodes",
            F.expr("filter(source_nodes, x -> x is not null)"),
        )
        .withColumn("source_node_count", F.size("source_nodes"))
        .withColumn(
            "revocation_disposition",
            F.when(F.col("source_node_count") == 0, F.lit("UNMAPPED"))
            .when(F.col("source_node_count") > 1, F.lit("AMBIGUOUS"))
            .otherwise(F.lit("REVOKABLE")),
        )
        .withColumn(
            "mapped_namespace_id",
            F.when(
                F.col("revocation_disposition") == F.lit("REVOKABLE"),
                F.col("source_nodes")[0]["namespace_id"],
            ),
        )
        .withColumn(
            "mapped_source_id",
            F.when(
                F.col("revocation_disposition") == F.lit("REVOKABLE"),
                F.col("source_nodes")[0]["source_id"],
            ),
        )
        .withColumn(
            "mapped_referent_kind",
            F.when(
                F.col("revocation_disposition") == F.lit("REVOKABLE"),
                F.col("source_nodes")[0]["referent_kind"],
            ),
        )
    )
    open_memberships = memberships.where(F.col("valid_to").isNull())
    revocations = (
        open_memberships.alias("m")
        .join(
            mapped.alias("i"),
            (
                (F.col("m.source_namespace_id") == F.col("i.mapped_namespace_id"))
                & (F.col("m.source_id") == F.col("i.mapped_source_id"))
                & (F.col("m.source_referent_kind") == F.col("i.mapped_referent_kind"))
            ),
            "inner",
        )
        .where(F.col("i.revocation_disposition") == F.lit("REVOKABLE"))
        .select(
            "m.membership_key",
            "m.source_namespace_id",
            "m.source_id",
            "m.source_referent_kind",
            "m.entity_key",
            "m.decision_id",
            "m.valid_from",
            "i.lifecycle_envelope_key",
            "i.operation",
            "i.lifecycle_observed_at",
            "i.assertion_ids",
            "i.policy_id",
            "i.policy_digest",
            "i.assertion_observed_at",
        )
    )
    mapping_conflicts = mapped.where(
        F.col("revocation_disposition").isin("UNMAPPED", "AMBIGUOUS")
    ).select(
        *RECORD_IDENTITY_COLUMNS,
        F.col("lifecycle_envelope_key").alias("envelope_key"),
        "operation",
        "lifecycle_observed_at",
        "revocation_disposition",
        "source_nodes",
        "assertion_ids",
        "policy_id",
        "policy_digest",
        "assertion_observed_at",
    )
    return revocations, mapping_conflicts


def current_upsert_envelope_keys(
    *,
    source_records: Any,
    ingest_runs: Any,
    committed_run_ids: tuple[str, ...] | None = None,
    committed_runs: Any | None = None,
    registry: SourceRegistrySnapshot,
    as_of: str,
) -> Any:
    """Resolve committed source records into active UPSERT envelope keys."""

    bound, latest = persist_latest_source_record_states(
        source_records=source_records,
        ingest_runs=ingest_runs,
        committed_run_ids=committed_run_ids,
        committed_runs=committed_runs,
        registry=registry,
        as_of=as_of,
    )
    try:
        current = current_envelope_keys_from_latest(latest, as_of=as_of).persist()
        current.count()
        return current
    finally:
        latest.unpersist()
        bound.unpersist()


def inactive_source_records(
    *,
    source_records: Any,
    ingest_runs: Any,
    committed_run_ids: tuple[str, ...] | None = None,
    committed_runs: Any | None = None,
    registry: SourceRegistrySnapshot,
    as_of: str,
) -> Any:
    """Return source records whose latest lifecycle state is not an active UPSERT."""

    from pyspark.sql import functions as F

    bound, latest = persist_latest_source_record_states(
        source_records=source_records,
        ingest_runs=ingest_runs,
        committed_run_ids=committed_run_ids,
        committed_runs=committed_runs,
        registry=registry,
        as_of=as_of,
    )
    try:
        return inactive_source_records_from_latest(latest, as_of=as_of).select(
            F.col("source_namespace_id").alias("subject_namespace_id"),
            F.col("source_record_id").alias("subject_source_id"),
            F.col("lifecycle_observed_at"),
        )
    finally:
        latest.unpersist()
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
    join_columns = ["envelope_key"]
    if "run_id" in current_envelope_keys.columns and "run_id" in active.columns:
        join_columns.append("run_id")
    return (
        active.join(current_envelope_keys, join_columns, "inner")
        .where(valid_to.isNull() | (valid_to > as_of_timestamp))
        .drop("envelope_key")
    )
