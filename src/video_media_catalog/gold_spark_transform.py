"""Distributed Silver-to-Gold policy resolution."""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from video_media_catalog.attribution import (
    AttributionEntry,
    AttributionManifest,
    build_attribution_manifest,
)
from video_media_catalog.canonical import canonical_json
from video_media_catalog.community_release import ReleasePolicyContext
from video_media_catalog.gold import (
    GoldAssertionLineage,
    GoldReleasePlan,
    GoldResolutionPolicy,
    GoldResolutionStatus,
    GoldRightsLineage,
    ResolutionOperator,
    build_gold_conflict,
    build_gold_entity,
    build_gold_field,
    build_gold_identifier,
    build_gold_relation,
    build_gold_release_plan,
    trace_with_assertion_lineage,
)
from video_media_catalog.gold_quality import (
    GoldQualityReport,
    build_gold_quality_report_from_metrics,
)
from video_media_catalog.gold_resolution import (
    ConflictDraft,
    FieldDraft,
    IdentifierDraft,
    RelationDraft,
)
from video_media_catalog.gold_rows import (
    gold_conflict_row,
    gold_entity_row,
    gold_field_row,
    gold_identifier_row,
    gold_relation_row,
)
from video_media_catalog.gold_spark import gold_table_schema
from video_media_catalog.gold_tables import GOLD_DATA_COLUMNS
from video_media_catalog.source_registry import SourceRegistrySnapshot
from video_media_catalog.v2_contracts import parse_rfc3339


@dataclass
class GoldSparkBuild:
    plan: GoldReleasePlan
    quality_report: GoldQualityReport
    attribution_manifest: AttributionManifest
    dataframes: dict[str, Any]

    def unpersist(self) -> None:
        for frame in self.dataframes.values():
            frame.unpersist()


def _resolved_memberships(
    *,
    silver: dict[str, Any],
    as_of: str,
    max_redirect_hops: int,
):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    if max_redirect_hops < 1:
        raise ValueError("max_redirect_hops must be positive")
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
    memberships = (
        silver["community_entity_membership"]
        .withColumn("_membership_version", F.row_number().over(membership_window))
        .where(F.col("_membership_version") == 1)
        .drop("_membership_version")
        .where(
            (F.to_timestamp("valid_from") <= as_of_timestamp)
            & (
                F.col("valid_to").isNull()
                | (F.to_timestamp("valid_to") > as_of_timestamp)
            )
        )
        .select(
            "source_namespace_id",
            "source_id",
            "source_referent_kind",
            F.col("entity_key").alias("resolved_entity_key"),
        )
        .persist()
    )
    duplicate_membership = (
        memberships.groupBy(
            "source_namespace_id",
            "source_id",
            "source_referent_kind",
        )
        .agg(F.countDistinct("resolved_entity_key").alias("entity_count"))
        .where(F.col("entity_count") > 1)
        .limit(1)
        .count()
    )
    if duplicate_membership:
        memberships.unpersist()
        raise ValueError("source node has multiple active memberships")

    redirects = (
        silver["community_entity_redirect"]
        .where(F.to_timestamp("effective_at") <= as_of_timestamp)
        .select(
            F.col("source_entity_key").alias("redirect_source"),
            F.col("target_entity_key").alias("redirect_target"),
        )
        .persist()
    )
    redirect_conflict = (
        redirects.groupBy("redirect_source")
        .agg(F.countDistinct("redirect_target").alias("target_count"))
        .where(F.col("target_count") > 1)
        .limit(1)
        .count()
    )
    if redirect_conflict:
        redirects.unpersist()
        memberships.unpersist()
        raise ValueError("entity key redirects to multiple targets")

    current = memberships
    try:
        for _ in range(max_redirect_hops):
            joined = current.alias("m").join(
                redirects.alias("r"),
                F.col("m.resolved_entity_key") == F.col("r.redirect_source"),
                "left",
            )
            changed = (
                joined.where(F.col("r.redirect_target").isNotNull()).limit(1).count()
            )
            next_frame = (
                joined.select(
                    "m.source_namespace_id",
                    "m.source_id",
                    "m.source_referent_kind",
                    F.coalesce(
                        "r.redirect_target",
                        "m.resolved_entity_key",
                    ).alias("resolved_entity_key"),
                )
                .dropDuplicates(
                    [
                        "source_namespace_id",
                        "source_id",
                        "source_referent_kind",
                    ]
                )
                .persist()
            )
            if current is not memberships:
                current.unpersist()
            current = next_frame
            if not changed:
                break
        else:
            raise ValueError("redirect chain exceeds configured hop limit")

        ledger = silver["community_entity_ledger"].select(
            "entity_key",
            "entity_level",
            "entity_kind",
            "status",
        )
        resolved = (
            current.join(
                ledger,
                current.resolved_entity_key == ledger.entity_key,
                "left",
            )
            .drop("entity_key")
            .persist()
        )
        if resolved.where(F.col("entity_level").isNull()).limit(1).count():
            resolved.unpersist()
            raise ValueError("identity membership resolves to missing entity")
        if resolved.where(F.col("status") == "TOMBSTONED").limit(1).count():
            resolved.unpersist()
            raise ValueError("identity membership resolves to tombstoned entity")
        return resolved
    except Exception:
        if current is not memberships:
            current.unpersist()
        raise
    finally:
        redirects.unpersist()
        memberships.unpersist()


def _rights_frame(
    spark: Any,
    *,
    registry: SourceRegistrySnapshot,
    context: ReleasePolicyContext,
    policy: GoldResolutionPolicy,
):
    profiles = {profile.policy_id: profile for profile in registry.rights_profiles}
    as_of = parse_rfc3339(context.as_of)
    rows = []
    for profile in profiles.values():
        statically_allowed = profile.zone in context.allowed_zones and all(
            profile.allows(
                action,
                at=as_of,
                audience=context.audience,
                purpose=context.purpose,
                territory=territory,
            )
            for action in policy.requested_actions
            for territory in context.territories
        )
        rows.append(
            (
                profile.policy_id,
                profile.digest,
                profile.zone.value,
                statically_allowed,
                profile.max_cache_age_days,
                profile.license_id,
                profile.license_uri,
                profile.attribution_text,
                profile.share_alike,
            )
        )
    return spark.createDataFrame(
        rows,
        """
        rights_policy_id STRING,
        rights_policy_digest STRING,
        rights_zone STRING,
        statically_allowed BOOLEAN,
        max_cache_age_days LONG,
        rights_license_id STRING,
        rights_license_uri STRING,
        rights_attribution_text STRING,
        rights_share_alike BOOLEAN
        """,
    )


def _source_products_frame(spark: Any, registry: SourceRegistrySnapshot):
    return spark.createDataFrame(
        [
            (
                product.source_product_id,
                product.policy_id,
                product.name,
                product.documentation_url,
            )
            for product in registry.source_products
        ],
        """
        source_product_id STRING,
        source_product_policy_id STRING,
        source_name STRING,
        source_documentation_url STRING
        """,
    )


def _validate_assertion_product_policies(
    frame: Any,
    *,
    source_records: Any,
    source_products: Any,
) -> None:
    """Validate assertion policy ownership even for non-published assertion kinds."""

    from pyspark.sql import functions as F

    active = frame.where(F.col("status") == "ACTIVE").withColumn(
        "envelope_key",
        F.get_json_object("provenance_json", "$.envelopeKey"),
    )
    source_metadata = (
        source_records.select(
            "envelope_key",
            "source_product_id",
            F.col("policy_id").alias("record_policy_id"),
            F.col("policy_digest").alias("record_policy_digest"),
        )
        .join(source_products, "source_product_id", "inner")
        .select(
            "envelope_key",
            "record_policy_id",
            "record_policy_digest",
            "source_product_policy_id",
        )
    )
    bound = active.alias("a").join(
        source_metadata.alias("s"),
        "envelope_key",
        "inner",
    )
    if bound.count() != active.count():
        raise ValueError("assertion provenance cannot resolve source record")
    if (
        bound.where(
            (F.col("a.policy_id") != F.col("s.record_policy_id"))
            | (F.col("a.policy_digest") != F.col("s.record_policy_digest"))
        )
        .limit(1)
        .count()
    ):
        raise ValueError("assertion and source record policies differ")
    if (
        bound.where(F.col("a.policy_id") != F.col("s.source_product_policy_id"))
        .limit(1)
        .count()
    ):
        raise ValueError("assertion does not bind its source product rights policy")


def _current_source_envelope_keys(
    *,
    source_records: Any,
    ingest_runs: Any,
    committed_run_ids: tuple[str, ...],
    registry: SourceRegistrySnapshot,
    as_of: str,
):
    """Resolve committed source-record operations into an as-of active set."""

    from pyspark.sql import Window
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
    bound = None
    required_metadata = (
        "_registry_digest",
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
    try:
        invalid_metadata = F.lit(False)
        for column in required_metadata:
            invalid_metadata = invalid_metadata | F.col(column).isNull()
        if metadata.where(invalid_metadata).limit(1).count():
            raise ValueError("source ingest run lacks pinned batch metadata")
        if (
            metadata.where(F.col("_registry_digest") != F.lit(registry.digest))
            .limit(1)
            .count()
        ):
            raise ValueError("source ingest run used another registry snapshot")
        if (
            metadata.where(
                F.col("_run_source_product_id") != F.col("_batch_source_product_id")
            )
            .limit(1)
            .count()
        ):
            raise ValueError("source ingest run and batch products differ")
        if (
            metadata.groupBy("run_id")
            .count()
            .where(F.col("count") != 1)
            .limit(1)
            .count()
        ):
            raise ValueError("source ingest run metadata is duplicated")

        actual_counts = selected_records.groupBy("run_id").count()
        if (
            metadata.join(actual_counts, "run_id", "left")
            .fillna(0, subset=["count"])
            .where(F.col("count") != F.col("_batch_record_count"))
            .limit(1)
            .count()
        ):
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
            raise ValueError("source record does not bind its committed batch")

        as_of_timestamp = F.to_timestamp(F.lit(as_of))
        metadata_as_of = metadata.where(
            F.to_timestamp("_batch_acquired_at") <= as_of_timestamp
        )
        full_window = Window.partitionBy(
            "_batch_source_system_id",
            "_batch_source_product_id",
            "_coverage_scope_digest",
        ).orderBy(
            F.to_timestamp("_batch_acquired_at"),
            "_batch_id",
            "run_id",
        )
        full_snapshots = (
            metadata_as_of.where(
                (F.col("_change_semantics") == "FULL_SNAPSHOT")
                & (F.col("_completeness") == "COMPLETE")
            )
            .withColumn("_previous_run_id", F.lag("run_id").over(full_window))
            .withColumn(
                "_previous_batch_id",
                F.lag("_batch_id").over(full_window),
            )
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
                "_batch_source_system_id",
                "_batch_source_product_id",
                "_coverage_scope_digest",
                "_batch_acquired_at",
            )
            prior_same_coverage = baselines.alias("b").join(
                metadata_as_of.alias("p"),
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

            record_identity = (
                "source_system_id",
                "source_product_id",
                "source_namespace_id",
                "source_record_id",
            )
            snapshot_pairs = snapshot_diffs.where(
                F.col("_previous_run_id").isNotNull()
            ).select(
                F.col("run_id").alias("_snapshot_run_id"),
                "_previous_run_id",
                "_batch_id",
                "_batch_acquired_at",
                "_batch_source_system_id",
                "_batch_source_product_id",
                "_previous_acquired_at",
            )
            previous_ids = (
                snapshot_pairs.alias("p")
                .join(
                    bound.alias("r"),
                    (
                        (
                            F.col("p._batch_source_system_id")
                            == F.col("r.source_system_id")
                        )
                        & (
                            F.col("p._batch_source_product_id")
                            == F.col("r.source_product_id")
                        )
                        & (
                            F.to_timestamp("r._batch_acquired_at")
                            >= F.to_timestamp("p._previous_acquired_at")
                        )
                        & (
                            F.to_timestamp("r._batch_acquired_at")
                            <= F.to_timestamp("p._batch_acquired_at")
                        )
                    ),
                    "inner",
                )
                .where(F.col("r.operation") == "UPSERT")
                .select(
                    "p._snapshot_run_id",
                    "p._batch_id",
                    "p._batch_acquired_at",
                    *(F.col(f"r.{column}").alias(column) for column in record_identity),
                )
                .dropDuplicates(["_snapshot_run_id", *record_identity])
            )
            current_ids = (
                snapshot_pairs.alias("p")
                .join(
                    bound.alias("r"),
                    F.col("p._snapshot_run_id") == F.col("r.run_id"),
                    "inner",
                )
                .where(F.col("r.operation") == "UPSERT")
                .select(
                    "p._snapshot_run_id",
                    *(F.col(f"r.{column}").alias(column) for column in record_identity),
                )
                .dropDuplicates(["_snapshot_run_id", *record_identity])
            )
            missing = previous_ids.join(
                current_ids,
                ["_snapshot_run_id", *record_identity],
                "left_anti",
            )
            inferred_events = missing.select(
                F.lit(None).cast("string").alias("envelope_key"),
                *record_identity,
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
                *record_identity,
                "operation",
                "observed_at",
                "ingested_at",
                "valid_from",
                "valid_to",
                "expires_at",
                "batch_id",
            )
            events = (
                actual_events.unionByName(inferred_events)
                .withColumn(
                    "_effective_at",
                    F.coalesce(
                        F.to_timestamp("valid_from"),
                        F.to_timestamp("observed_at"),
                    ),
                )
                .where(F.col("_effective_at") <= as_of_timestamp)
                .withColumn(
                    "_operation_priority",
                    F.when(F.col("operation") == "UPSERT", F.lit(0)).otherwise(
                        F.lit(1)
                    ),
                )
            )
            event_window = Window.partitionBy(*record_identity).orderBy(
                F.col("_effective_at").desc(),
                F.to_timestamp("observed_at").desc(),
                F.to_timestamp("ingested_at").desc(),
                F.col("_operation_priority").desc(),
                F.col("batch_id").desc(),
                F.col("envelope_key").desc_nulls_last(),
            )
            latest = (
                events.withColumn("_record_version", F.row_number().over(event_window))
                .where(F.col("_record_version") == 1)
                .drop("_record_version")
            )
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
            snapshot_diffs.unpersist()
    finally:
        if bound is not None:
            bound.unpersist()
        metadata.unpersist()


def _eligible_assertions(
    frame: Any,
    *,
    source_records: Any,
    current_source_envelope_keys: Any,
    source_products: Any,
    memberships: Any,
    rights: Any,
    context: ReleasePolicyContext,
):
    from pyspark.sql import functions as F

    source_metadata = source_records.select(
        "envelope_key",
        "source_product_id",
        "source_record_id",
        "citation_keys_json",
        F.col("policy_id").alias("record_policy_id"),
        F.col("policy_digest").alias("record_policy_digest"),
    ).join(source_products, "source_product_id", "inner")
    active = (
        frame.where(F.col("status") == "ACTIVE")
        .withColumn(
            "envelope_key",
            F.get_json_object("provenance_json", "$.envelopeKey"),
        )
        .persist()
    )
    bound = active.alias("a").join(
        source_metadata.select(
            "envelope_key",
            "source_product_id",
            "source_name",
            "source_documentation_url",
            "source_record_id",
            "citation_keys_json",
            "record_policy_id",
            "record_policy_digest",
            "source_product_policy_id",
        ).alias("s"),
        "envelope_key",
        "inner",
    )
    active_count = active.count()
    if bound.count() != active_count:
        active.unpersist()
        raise ValueError("assertion provenance cannot resolve source record")
    if (
        bound.where(
            (F.col("a.policy_id") != F.col("s.record_policy_id"))
            | (F.col("a.policy_digest") != F.col("s.record_policy_digest"))
        )
        .limit(1)
        .count()
    ):
        active.unpersist()
        raise ValueError("assertion and source record policies differ")
    if (
        bound.where(F.col("a.policy_id") != F.col("s.source_product_policy_id"))
        .limit(1)
        .count()
    ):
        active.unpersist()
        raise ValueError("assertion does not bind its source product rights policy")

    known = bound.alias("a").join(
        rights.alias("p"),
        (F.col("a.policy_id") == F.col("p.rights_policy_id"))
        & (F.col("a.policy_digest") == F.col("p.rights_policy_digest")),
        "left",
    )
    if known.where(F.col("p.rights_policy_id").isNull()).limit(1).count():
        active.unpersist()
        raise ValueError("assertion references unknown or changed rights policy")

    as_of_epoch = int(parse_rfc3339(context.as_of).timestamp())
    valid_from = F.to_timestamp(F.get_json_object("a.provenance_json", "$.validFrom"))
    valid_to = F.to_timestamp(F.get_json_object("a.provenance_json", "$.validTo"))
    observed_epoch = F.unix_timestamp("a.observed_at")
    rights_eligible = (
        known.where(F.col("p.statically_allowed"))
        .where(
            valid_from.isNull() | (valid_from <= F.to_timestamp(F.lit(context.as_of)))
        )
        .where(valid_to.isNull() | (valid_to > F.to_timestamp(F.lit(context.as_of))))
        .where(
            F.col("p.max_cache_age_days").isNull()
            | (
                observed_epoch + F.col("p.max_cache_age_days") * F.lit(86_400)
                > F.lit(as_of_epoch)
            )
        )
        .persist()
    )
    with_source = rights_eligible.join(
        current_source_envelope_keys,
        "envelope_key",
        "inner",
    )
    current_count = with_source.count()
    withheld = active_count - current_count

    resolved = (
        with_source.alias("a")
        .join(
            memberships.alias("m"),
            (F.col("a.subject_namespace_id") == F.col("m.source_namespace_id"))
            & (F.col("a.subject_source_id") == F.col("m.source_id"))
            & (F.col("a.subject_referent_kind") == F.col("m.source_referent_kind")),
            "inner",
        )
        .persist()
    )
    resolved_count = resolved.count()
    unresolved = current_count - resolved_count
    rights_eligible.unpersist()
    active.unpersist()
    return resolved, withheld, unresolved


def _assertion_lineage(row: Any) -> GoldAssertionLineage:
    provenance = json.loads(row["provenance_json"])
    citation_keys = tuple(provenance.get("citationKeys") or ())
    record_citation_keys = set(json.loads(row["citation_keys_json"]))
    if not set(citation_keys).issubset(record_citation_keys):
        raise ValueError("assertion citations are absent from the source record")
    source_name = str(row["source_name"])
    return GoldAssertionLineage(
        assertion_id=row["assertion_id"],
        source_product_id=row["source_product_id"],
        source_name=source_name,
        source_record_id=row["source_record_id"],
        source_path=provenance["sourcePath"],
        observed_at=row["observed_at"],
        citation_keys=citation_keys,
        rights=GoldRightsLineage(
            policy_id=row["rights_policy_id"],
            policy_zone=row["rights_zone"],
            license_id=row["rights_license_id"],
            license_uri=row["rights_license_uri"],
            attribution_text=(
                row["rights_attribution_text"] or f"Data from {source_name}."
            ),
            source_url=row["source_documentation_url"],
            share_alike=bool(row["rights_share_alike"]),
        ),
    )


def _lineage_json(row: Any) -> str:
    return canonical_json(
        _assertion_lineage(row).model_dump(
            mode="json",
            by_alias=True,
            exclude_none=True,
        )
    )


def _normalize_lineage(values: Any) -> tuple[GoldAssertionLineage, ...]:
    by_assertion = {}
    for item in values:
        value = GoldAssertionLineage.model_validate_json(item)
        existing = by_assertion.get(value.assertion_id)
        if existing is not None and existing != value:
            raise ValueError("assertion lineage changed during Gold resolution")
        by_assertion[value.assertion_id] = value
    return tuple(by_assertion[key] for key in sorted(by_assertion))


def _field_rule_udf(policy: GoldResolutionPolicy):
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructField, StructType

    rules = {
        rule.predicate: (
            rule.operator.value,
            rule.scope_qualifiers,
        )
        for rule in policy.rules
    }
    default = policy.default_operator.value

    def resolve_rule(predicate: str, qualifiers_json: str):
        operator, keys = rules.get(predicate, (default, ()))
        qualifiers = json.loads(qualifiers_json)
        scope = {key: qualifiers.get(key) for key in keys}
        return operator, canonical_json(scope)

    return F.udf(
        resolve_rule,
        StructType(
            [
                StructField("operator", StringType(), False),
                StructField("scope_json", StringType(), False),
            ]
        ),
    )


def _resolve_field_group(item):
    (entity_key, predicate, scope_json, operator), raw_values = item
    by_value: dict[tuple[str, str], list[tuple[str, str]]] = defaultdict(list)
    for value_type, value_json, assertion_id, lineage_json in raw_values:
        by_value[(value_type, value_json)].append((assertion_id, lineage_json))
    ordered = sorted(by_value)
    all_ids = tuple(
        sorted(
            assertion_id
            for assertions in by_value.values()
            for assertion_id, _ in assertions
        )
    )
    all_lineage = _normalize_lineage(
        lineage_json
        for assertions in by_value.values()
        for _, lineage_json in assertions
    )
    scope = json.loads(scope_json)
    trace = {"operator": operator}
    if operator == ResolutionOperator.SINGLE.value:
        if len(ordered) == 1:
            value_type, value_json = ordered[0]
            return [
                (
                    "field",
                    FieldDraft(
                        entity_key=entity_key,
                        predicate=predicate,
                        value_type=value_type,
                        value=json.loads(value_json),
                        qualifiers=scope,
                        status=GoldResolutionStatus.SELECTED,
                        assertion_ids=all_ids,
                        selected_assertion_id=min(all_ids),
                        trace=trace,
                        lineage=all_lineage,
                    ),
                )
            ]
        candidates = [json.loads(value_json) for _, value_json in ordered]
        return [
            (
                "field",
                FieldDraft(
                    entity_key=entity_key,
                    predicate=predicate,
                    value_type="CONFLICT",
                    value=None,
                    qualifiers=scope,
                    status=GoldResolutionStatus.CONFLICTED,
                    assertion_ids=all_ids,
                    selected_assertion_id=None,
                    trace=trace,
                    lineage=all_lineage,
                ),
            ),
            (
                "conflict",
                ConflictDraft(
                    entity_key=entity_key,
                    predicate=predicate,
                    qualifiers=scope,
                    reason="MULTIPLE_ELIGIBLE_VALUES",
                    assertion_ids=all_ids,
                    candidate_values=candidates,
                    trace=trace,
                    lineage=all_lineage,
                ),
            ),
        ]
    result = []
    for value_type, value_json in ordered:
        supporting = by_value[(value_type, value_json)]
        ids = tuple(sorted(assertion_id for assertion_id, _ in supporting))
        result.append(
            (
                "field",
                FieldDraft(
                    entity_key=entity_key,
                    predicate=predicate,
                    value_type=value_type,
                    value=json.loads(value_json),
                    qualifiers=scope,
                    status=GoldResolutionStatus.SET,
                    assertion_ids=ids,
                    selected_assertion_id=None,
                    trace=trace,
                    lineage=_normalize_lineage(
                        lineage_json for _, lineage_json in supporting
                    ),
                ),
            )
        )
    return result


def _identifier_draft(
    item: Any,
    *,
    policy_id: str,
    policy_version: str,
) -> IdentifierDraft:
    values = list(item[1])
    return IdentifierDraft(
        entity_key=item[0][0],
        namespace_id=item[0][1],
        value=item[0][2],
        issuer=item[0][3],
        referent_kind=item[0][4],
        assertion_ids=tuple(sorted({assertion_id for assertion_id, _ in values})),
        trace={
            "policyId": policy_id,
            "policyVersion": policy_version,
        },
        lineage=_normalize_lineage(lineage_json for _, lineage_json in values),
    )


def _relation_draft(
    item: Any,
    *,
    policy_id: str,
    policy_version: str,
) -> RelationDraft:
    values = list(item[1])
    return RelationDraft(
        subject_entity_key=item[0][0],
        predicate=item[0][1],
        object_entity_key=item[0][2],
        qualifiers=json.loads(item[0][3]),
        assertion_ids=tuple(sorted({assertion_id for assertion_id, _ in values})),
        trace={
            "policyId": policy_id,
            "policyVersion": policy_version,
        },
        lineage=_normalize_lineage(lineage_json for _, lineage_json in values),
    )


def build_distributed_gold(
    spark: Any,
    *,
    visible_silver: dict[str, Any],
    registry: SourceRegistrySnapshot,
    policy_context: ReleasePolicyContext,
    owner_subject: str,
    field_policy: GoldResolutionPolicy,
    committed_run_ids: tuple[str, ...],
    silver_snapshot_ids: dict[str, int | None],
    identity_snapshot_ids: dict[str, int | None],
    resolver_digest: str,
    image_digest: str,
    config_digest: str,
    planned_at: str,
    max_redirect_hops: int = 16,
) -> GoldSparkBuild:
    required = {
        "community_ingest_run",
        "community_source_record",
        "community_field_assertion",
        "community_identifier_assertion",
        "community_relationship_assertion",
        "community_entity_type_assertion",
        "community_entity_ledger",
        "community_entity_membership",
        "community_entity_redirect",
    }
    if not required.issubset(visible_silver):
        raise ValueError("Gold build is missing required Silver tables")
    from pyspark.sql import functions as F

    selected_runs = spark.createDataFrame(
        [(run_id,) for run_id in committed_run_ids],
        "run_id STRING",
    )
    committed_silver = {
        table: (
            frame.join(selected_runs, "run_id", "inner")
            if "run_id" in frame.columns
            else frame
        )
        for table, frame in visible_silver.items()
    }
    current_source_envelope_keys = _current_source_envelope_keys(
        source_records=committed_silver["community_source_record"],
        ingest_runs=committed_silver["community_ingest_run"],
        committed_run_ids=committed_run_ids,
        registry=registry,
        as_of=policy_context.as_of,
    )
    try:
        memberships = _resolved_memberships(
            silver=committed_silver,
            as_of=policy_context.as_of,
            max_redirect_hops=max_redirect_hops,
        )
    except Exception:
        current_source_envelope_keys.unpersist()
        raise
    try:
        rights = _rights_frame(
            spark,
            registry=registry,
            context=policy_context,
            policy=field_policy,
        )
        source_products = _source_products_frame(spark, registry)
    except Exception:
        memberships.unpersist()
        current_source_envelope_keys.unpersist()
        raise
    intermediates = [current_source_envelope_keys, memberships]
    field_drafts = None
    identifier_drafts = None
    relation_drafts = None
    try:
        _validate_assertion_product_policies(
            committed_silver["community_entity_type_assertion"],
            source_records=committed_silver["community_source_record"],
            source_products=source_products,
        )
        resolved_fields, field_withheld, field_unresolved = _eligible_assertions(
            committed_silver["community_field_assertion"],
            source_records=committed_silver["community_source_record"],
            current_source_envelope_keys=current_source_envelope_keys,
            source_products=source_products,
            memberships=memberships,
            rights=rights,
            context=policy_context,
        )
        resolved_identifiers, id_withheld, id_unresolved = _eligible_assertions(
            committed_silver["community_identifier_assertion"],
            source_records=committed_silver["community_source_record"],
            current_source_envelope_keys=current_source_envelope_keys,
            source_products=source_products,
            memberships=memberships,
            rights=rights,
            context=policy_context,
        )
        intermediates.extend((resolved_fields, resolved_identifiers))

        rule_udf = _field_rule_udf(field_policy)
        ruled_fields = (
            resolved_fields.withColumn(
                "_rule",
                rule_udf("predicate", "qualifiers_json"),
            )
            .withColumn("resolution_operator", F.col("_rule.operator"))
            .withColumn("scope_json", F.col("_rule.scope_json"))
            .drop("_rule")
            .persist()
        )
        intermediates.append(ruled_fields)
        never_count = ruled_fields.where(
            F.col("resolution_operator") == ResolutionOperator.NEVER_RESOLVE.value
        ).count()
        resolvable_fields = ruled_fields.where(
            F.col("resolution_operator") != ResolutionOperator.NEVER_RESOLVE.value
        )
        field_drafts = (
            resolvable_fields.rdd.map(
                lambda row: (
                    (
                        row["resolved_entity_key"],
                        row["predicate"],
                        row["scope_json"],
                        row["resolution_operator"],
                    ),
                    (
                        row["value_type"],
                        row["value_json"],
                        row["assertion_id"],
                        _lineage_json(row),
                    ),
                )
            )
            .groupByKey()
            .flatMap(_resolve_field_group)
            .persist()
        )

        collision = (
            resolved_identifiers.groupBy(
                "namespace_id",
                "value",
                "referent_kind",
            )
            .agg(F.countDistinct("resolved_entity_key").alias("entity_count"))
            .where(F.col("entity_count") > 1)
            .limit(1)
            .count()
        )
        if collision:
            raise ValueError("eligible identifier resolves to multiple Gold entities")
        identifier_drafts = (
            resolved_identifiers.rdd.map(
                lambda row: (
                    (
                        row["resolved_entity_key"],
                        row["namespace_id"],
                        row["value"],
                        row["issuer"],
                        row["referent_kind"],
                    ),
                    (row["assertion_id"], _lineage_json(row)),
                )
            )
            .groupByKey()
            .map(
                lambda item: _identifier_draft(
                    item,
                    policy_id=field_policy.policy_id,
                    policy_version=field_policy.policy_version,
                )
            )
            .persist()
        )

        resolved_relation_subjects, rel_withheld, rel_subject_unresolved = (
            _eligible_assertions(
                committed_silver["community_relationship_assertion"],
                source_records=committed_silver["community_source_record"],
                current_source_envelope_keys=current_source_envelope_keys,
                source_products=source_products,
                memberships=memberships,
                rights=rights,
                context=policy_context,
            )
        )
        intermediates.append(resolved_relation_subjects)
        resolved_relations = (
            resolved_relation_subjects.alias("r")
            .join(
                memberships.alias("o"),
                (F.col("r.object_namespace_id") == F.col("o.source_namespace_id"))
                & (F.col("r.object_source_id") == F.col("o.source_id"))
                & (F.col("r.object_referent_kind") == F.col("o.source_referent_kind")),
                "inner",
            )
            .select(
                "r.*",
                F.col("o.resolved_entity_key").alias("object_resolved_entity_key"),
            )
            .persist()
        )
        intermediates.append(resolved_relations)
        rel_object_unresolved = (
            resolved_relation_subjects.count() - resolved_relations.count()
        )
        relation_drafts = (
            resolved_relations.rdd.map(
                lambda row: (
                    (
                        row["resolved_entity_key"],
                        row["predicate"],
                        row["object_resolved_entity_key"],
                        row["qualifiers_json"],
                    ),
                    (row["assertion_id"], _lineage_json(row)),
                )
            )
            .groupByKey()
            .map(
                lambda item: _relation_draft(
                    item,
                    policy_id=field_policy.policy_id,
                    policy_version=field_policy.policy_version,
                )
            )
            .persist()
        )

        used_entity_keys = (
            field_drafts.map(lambda item: (item[1].entity_key,))
            .union(identifier_drafts.map(lambda item: (item.entity_key,)))
            .union(
                relation_drafts.flatMap(
                    lambda item: (
                        (item.subject_entity_key,),
                        (item.object_entity_key,),
                    )
                )
            )
            .distinct()
        )
        used_entities = spark.createDataFrame(
            used_entity_keys,
            "entity_key STRING",
        )
        entity_summary = (
            used_entities.join(
                memberships,
                used_entities.entity_key == memberships.resolved_entity_key,
                "inner",
            )
            .groupBy("entity_key")
            .agg(
                F.first("entity_level").alias("entity_level"),
                F.first("entity_kind").alias("entity_kind"),
                F.first("status").alias("status"),
                F.countDistinct(
                    "source_namespace_id",
                    "source_id",
                    "source_referent_kind",
                ).alias("source_node_count"),
                F.countDistinct("entity_level").alias("_level_count"),
                F.countDistinct("entity_kind").alias("_kind_count"),
            )
            .persist()
        )
        intermediates.append(entity_summary)
        if (
            entity_summary.where(
                (F.col("_level_count") != 1) | (F.col("_kind_count") != 1)
            )
            .limit(1)
            .count()
        ):
            raise ValueError("Gold entity has inconsistent ledger classification")

        policy_usage = (
            resolvable_fields.select(
                "source_product_id",
                "policy_id",
                "assertion_id",
            )
            .unionByName(
                resolved_identifiers.select(
                    "source_product_id",
                    "policy_id",
                    "assertion_id",
                )
            )
            .unionByName(
                resolved_relations.select(
                    "source_product_id",
                    "policy_id",
                    "assertion_id",
                )
            )
            .dropDuplicates(["assertion_id"])
            .groupBy("source_product_id", "policy_id")
            .count()
            .collect()
        )

        field_count = field_drafts.filter(lambda item: item[0] == "field").count()
        conflict_count = field_drafts.filter(lambda item: item[0] == "conflict").count()
        identifier_count = identifier_drafts.count()
        relation_count = relation_drafts.count()
        entity_count = entity_summary.count()
        table_counts = {
            "community_gold_entity": entity_count,
            "community_gold_field": field_count,
            "community_gold_identifier": identifier_count,
            "community_gold_relation": relation_count,
            "community_gold_conflict": conflict_count,
        }
        plan = build_gold_release_plan(
            owner_subject=owner_subject,
            policy_context=policy_context,
            committed_run_ids=committed_run_ids,
            silver_snapshot_ids=silver_snapshot_ids,
            identity_snapshot_ids=identity_snapshot_ids,
            rights_registry_digest=registry.digest,
            field_policy_digest=field_policy.digest,
            resolver_digest=resolver_digest,
            image_digest=image_digest,
            config_digest=config_digest,
            expected_counts=table_counts,
            planned_at=planned_at,
        )

        entity_rows = entity_summary.rdd.map(
            lambda row: gold_entity_row(
                build_gold_entity(
                    release_plan_id=plan.release_plan_id,
                    entity_key=row["entity_key"],
                    entity_level=row["entity_level"],
                    entity_kind=row["entity_kind"],
                    status=row["status"],
                    source_node_count=int(row["source_node_count"]),
                    trace={
                        "sourceNodeCount": int(row["source_node_count"]),
                        "resolver": "community-gold-spark-v2",
                    },
                )
            )
        )
        field_rows = field_drafts.filter(lambda item: item[0] == "field").map(
            lambda item: gold_field_row(
                build_gold_field(
                    release_plan_id=plan.release_plan_id,
                    entity_key=item[1].entity_key,
                    predicate=item[1].predicate,
                    value_type=item[1].value_type,
                    value=item[1].value,
                    qualifiers=item[1].qualifiers,
                    resolution_status=item[1].status,
                    assertion_ids=item[1].assertion_ids,
                    selected_assertion_id=item[1].selected_assertion_id,
                    trace=trace_with_assertion_lineage(
                        {
                            **item[1].trace,
                            "policyId": field_policy.policy_id,
                            "policyVersion": field_policy.policy_version,
                        },
                        item[1].lineage,
                    ),
                )
            )
        )
        conflict_rows = field_drafts.filter(lambda item: item[0] == "conflict").map(
            lambda item: gold_conflict_row(
                build_gold_conflict(
                    release_plan_id=plan.release_plan_id,
                    entity_key=item[1].entity_key,
                    predicate=item[1].predicate,
                    qualifiers=item[1].qualifiers,
                    reason=item[1].reason,
                    assertion_ids=item[1].assertion_ids,
                    candidate_values=item[1].candidate_values,
                    trace=trace_with_assertion_lineage(
                        {
                            **item[1].trace,
                            "policyId": field_policy.policy_id,
                            "policyVersion": field_policy.policy_version,
                        },
                        item[1].lineage,
                    ),
                )
            )
        )
        identifier_rows = identifier_drafts.map(
            lambda item: gold_identifier_row(
                build_gold_identifier(
                    release_plan_id=plan.release_plan_id,
                    entity_key=item.entity_key,
                    namespace_id=item.namespace_id,
                    value=item.value,
                    issuer=item.issuer,
                    referent_kind=item.referent_kind,
                    assertion_ids=item.assertion_ids,
                    trace=trace_with_assertion_lineage(
                        item.trace,
                        item.lineage,
                    ),
                )
            )
        )
        relation_rows = relation_drafts.map(
            lambda item: gold_relation_row(
                build_gold_relation(
                    release_plan_id=plan.release_plan_id,
                    subject_entity_key=item.subject_entity_key,
                    predicate=item.predicate,
                    object_entity_key=item.object_entity_key,
                    qualifiers=item.qualifiers,
                    assertion_ids=item.assertion_ids,
                    trace=trace_with_assertion_lineage(
                        item.trace,
                        item.lineage,
                    ),
                )
            )
        )
        row_rdds = {
            "community_gold_entity": entity_rows,
            "community_gold_field": field_rows,
            "community_gold_identifier": identifier_rows,
            "community_gold_relation": relation_rows,
            "community_gold_conflict": conflict_rows,
        }
        output_frames = {}
        try:
            for table in GOLD_DATA_COLUMNS:
                frame = spark.createDataFrame(
                    row_rdds[table],
                    schema=gold_table_schema(table),
                ).persist()
                if frame.count() != table_counts[table]:
                    frame.unpersist()
                    raise RuntimeError(f"{table} materialized count changed")
                output_frames[table] = frame
        except Exception:
            for frame in output_frames.values():
                frame.unpersist()
            raise

        eligible_policy_counts: defaultdict[str, int] = defaultdict(int)
        products = {
            product.source_product_id: product for product in registry.source_products
        }
        profiles = {profile.policy_id: profile for profile in registry.rights_profiles}
        attribution_entries = []
        for row in policy_usage:
            product_id = row["source_product_id"]
            policy_id = row["policy_id"]
            count = int(row["count"])
            product = products.get(product_id)
            profile = profiles.get(policy_id)
            if product is None or profile is None:
                for frame in output_frames.values():
                    frame.unpersist()
                raise ValueError("attribution source is absent from registry")
            eligible_policy_counts[policy_id] += count
            attribution_entries.append(
                AttributionEntry(
                    source_product_id=product_id,
                    policy_id=policy_id,
                    attribution_text=(
                        profile.attribution_text or f"Data from {product.name}."
                    ),
                    license_id=profile.license_id,
                    license_uri=profile.license_uri,
                    source_url=product.documentation_url,
                    share_alike=profile.share_alike,
                    claim_count=count,
                )
            )
        if not attribution_entries:
            for frame in output_frames.values():
                frame.unpersist()
            raise ValueError("Gold release has no attributable eligible assertions")
        attribution = build_attribution_manifest(
            release_id=plan.release_plan_id,
            entries=tuple(attribution_entries),
            created_at=planned_at,
        )
        quality = build_gold_quality_report_from_metrics(
            plan=plan,
            policy=field_policy,
            table_counts=table_counts,
            conflict_count=conflict_count,
            field_count=field_count,
            withheld_assertion_count=(
                field_withheld + id_withheld + rel_withheld + never_count
            ),
            unresolved_identity_count=(
                field_unresolved
                + id_unresolved
                + rel_subject_unresolved
                + rel_object_unresolved
            ),
            entity_count=entity_count,
            eligible_policy_counts=dict(eligible_policy_counts),
            created_at=planned_at,
        )
        return GoldSparkBuild(plan, quality, attribution, output_frames)
    finally:
        if relation_drafts is not None:
            relation_drafts.unpersist()
        if identifier_drafts is not None:
            identifier_drafts.unpersist()
        if field_drafts is not None:
            field_drafts.unpersist()
        for frame in reversed(intermediates):
            frame.unpersist()
