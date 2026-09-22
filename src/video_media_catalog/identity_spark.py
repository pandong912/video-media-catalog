"""Distributed registry-driven exact identity resolution."""

from __future__ import annotations

import contextlib
from collections.abc import Mapping
from typing import Any, Literal

from pydantic import Field, field_validator

from video_media_catalog.assertions import SourceNodeRef
from video_media_catalog.canonical import deterministic_key
from video_media_catalog.community_ingest import (
    CommunityIngestRun,
    IngestRunKind,
    build_community_ingest_run,
)
from video_media_catalog.community_rows import (
    entity_ledger_row,
    entity_membership_row,
    external_id_index_row,
    identity_conflict_row,
    identity_decision_row,
    identity_evidence_row,
)
from video_media_catalog.community_sources import (
    build_community_registry,
    internal_key_continuity_profile,
)
from video_media_catalog.community_spark import community_table_schema
from video_media_catalog.community_tables import DATA_TABLE_COLUMNS
from video_media_catalog.identity_resolution import (
    IdentityResolutionResult,
    SourceNodeResolutionInput,
    build_identity_conflict,
    build_lifecycle_revoke_evidence,
    build_oversized_blocking_component_conflict,
    canonical_referent_kind,
    referent_kinds_compatible,
    resolve_or_allocate_source_node,
    resolve_parent_constrained_source_node,
    resolve_shared_blocking_member,
    revoke_identity_membership,
)
from video_media_catalog.identity_v2 import (
    EntityLevel,
    ExternalIdIndexEntry,
    ParentConstraint,
    build_external_id_index_entry,
)
from video_media_catalog.source_lifecycle import (
    build_inactive_membership_revocation_worklist,
    current_envelope_keys_from_latest,
    filter_assertions_for_current_envelopes,
    persist_latest_source_record_states,
    select_effective_membership_versions,
)
from video_media_catalog.source_registry import (
    RegistryStatus,
    SourceRegistrySnapshot,
)
from video_media_catalog.v2_contracts import (
    V2ContractModel,
    digest_identity,
    require_rfc3339,
    require_sha256,
)

_LEVEL_MAP = {
    "WORK": (EntityLevel.EDITORIAL_WORK, "EDITORIAL_WORK", "EDITORIAL_WORK"),
    "MOVIE": (EntityLevel.EDITORIAL_WORK, "MOVIE", "EDITORIAL_WORK"),
    "EDITORIAL_WORK": (
        EntityLevel.EDITORIAL_WORK,
        "EDITORIAL_WORK",
        "EDITORIAL_WORK",
    ),
    "SERIES": (EntityLevel.SERIES, "TV_SERIES", "SERIES"),
    "TV_SERIES": (EntityLevel.SERIES, "TV_SERIES", "SERIES"),
    "SEASON": (EntityLevel.SEASON, "TV_SEASON", "SEASON"),
    "TV_SEASON": (EntityLevel.SEASON, "TV_SEASON", "SEASON"),
    "EPISODE": (EntityLevel.EPISODE, "TV_EPISODE", "EPISODE"),
    "TV_EPISODE": (EntityLevel.EPISODE, "TV_EPISODE", "EPISODE"),
    "EDIT": (EntityLevel.EDIT, "EDIT", "EDIT"),
    "MANIFESTATION": (
        EntityLevel.MANIFESTATION,
        "MANIFESTATION",
        "MANIFESTATION",
    ),
    "PERSON": (EntityLevel.AGENT, "PERSON", "AGENT"),
    "AGENT": (EntityLevel.AGENT, "AGENT", "AGENT"),
    "ORGANIZATION": (EntityLevel.AGENT, "ORGANIZATION", "ORGANIZATION"),
}

_V1_REFERENT_KIND = {
    "MOVIE": "EDITORIAL_WORK",
    "EDITORIAL_WORK": "EDITORIAL_WORK",
    "TV_SERIES": "SERIES",
    "SERIES": "SERIES",
    "TV_SEASON": "SEASON",
    "SEASON": "SEASON",
    "TV_EPISODE": "EPISODE",
    "EPISODE": "EPISODE",
    "EDIT": "EDIT",
    "MANIFESTATION": "MANIFESTATION",
    "PERSON": "AGENT",
    "AGENT": "AGENT",
    "ORGANIZATION": "ORGANIZATION",
}

_REFERENT_KIND_ALIASES = {
    **{kind: values[2] for kind, values in _LEVEL_MAP.items()},
    **_V1_REFERENT_KIND,
}


def _source_node_id(namespace_id: str, source_id: str, referent_kind: str) -> str:
    return f"{namespace_id}\x1f{source_id}\x1f{referent_kind}"


def _source_node_from_id(node_id: str) -> SourceNodeRef:
    namespace_id, source_id, referent_kind = node_id.split("\x1f", 2)
    return SourceNodeRef(
        namespace_id=namespace_id,
        source_id=source_id,
        referent_kind=referent_kind,
    )


MAX_EXACT_BLOCKING_LABEL_ITERATIONS = 64
MAX_EXACT_BLOCKING_RESOLUTION_COMPONENT_SIZE = 256
MAX_EXACT_BLOCKING_NODE_CANDIDATE_KEYS = 256
MAX_EXACT_BLOCKING_COMPONENT_CANDIDATE_KEYS = 256
SOURCE_LIFECYCLE_BYPASS_REPARTITIONS = 128


class IdentityResolutionConfig(V2ContractModel):
    """Versioned, bounded controls for distributed identity resolution."""

    schema_version: Literal["1.0"] = "1.0"
    max_exact_blocking_label_iterations: int = Field(
        default=MAX_EXACT_BLOCKING_LABEL_ITERATIONS,
        ge=1,
        le=1024,
    )
    max_exact_blocking_component_size: int = Field(
        default=MAX_EXACT_BLOCKING_RESOLUTION_COMPONENT_SIZE,
        ge=1,
        le=1_000_000,
    )
    max_exact_blocking_node_candidate_keys: int = Field(
        default=MAX_EXACT_BLOCKING_NODE_CANDIDATE_KEYS,
        ge=1,
        le=1_000_000,
    )
    max_exact_blocking_component_candidate_keys: int = Field(
        default=MAX_EXACT_BLOCKING_COMPONENT_CANDIDATE_KEYS,
        ge=1,
        le=1_000_000,
    )

    @field_validator(
        "max_exact_blocking_label_iterations",
        "max_exact_blocking_component_size",
        "max_exact_blocking_node_candidate_keys",
        "max_exact_blocking_component_candidate_keys",
        mode="before",
    )
    @classmethod
    def reject_boolean_limits(cls, value: object) -> object:
        if isinstance(value, bool):
            raise ValueError("identity resolution limits must be integers")
        return value

    @property
    def digest(self) -> str:
        return digest_identity(
            self.model_dump(mode="json", by_alias=True),
        )

    def bind_runtime_config(self, runtime_config_digest: str) -> str:
        """Bind caller runtime config and resolver controls into one digest."""

        return digest_identity(
            {
                "runtimeConfigDigest": require_sha256(
                    runtime_config_digest,
                    label="runtime_config_digest",
                ),
                "identityResolutionConfigDigest": self.digest,
                "identityResolutionConfig": self.model_dump(
                    mode="json",
                    by_alias=True,
                ),
            }
        )


DEFAULT_IDENTITY_RESOLUTION_CONFIG = IdentityResolutionConfig()

_SOURCE_NODE_COLUMNS = (
    "subject_namespace_id",
    "subject_source_id",
    "subject_referent_kind",
)


def _ensure_identity_checkpoint_dir(spark: Any) -> None:
    """Give localCheckpoint a writable directory before truncating lineage."""

    sc = spark.sparkContext
    java_dir = sc._jsc.sc().getCheckpointDir()
    if java_dir.isDefined():
        return
    warehouse = str(spark.conf.get("spark.sql.warehouse.dir", "/tmp")).rstrip("/")
    sc.setCheckpointDir(f"{warehouse}/identity-checkpoints")


def _materialize_exact_blocking_labels(
    frame: Any,
    *,
    label: str = "label",
) -> Any:
    """Truncate label propagation lineage; fail closed on checkpoint loss."""

    from pyspark import StorageLevel

    spark = frame.sparkSession
    _ensure_identity_checkpoint_dir(spark)
    persisted = frame.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        persisted.count()
        materialized = persisted.localCheckpoint(eager=True)
    except Exception as exc:
        with contextlib.suppress(Exception):
            persisted.unpersist()
        raise RuntimeError(
            f"exact blocking {label} localCheckpoint failed; refusing incomplete merge"
        ) from exc
    try:
        materialized.take(1)
    except Exception as exc:
        with contextlib.suppress(Exception):
            persisted.unpersist()
            materialized.unpersist()
        raise RuntimeError(
            f"exact blocking {label} checkpoint is unreadable; "
            "refusing incomplete merge"
        ) from exc
    if persisted is not materialized:
        with contextlib.suppress(Exception):
            persisted.unpersist()
    return materialized


def _release_exact_blocking_labels(frame: Any) -> None:
    with contextlib.suppress(Exception):
        frame.unpersist()


def assign_exact_blocking_component_ids(
    nodes: Any,
    blocking_edges: Any,
    *,
    max_iterations: int = MAX_EXACT_BLOCKING_LABEL_ITERATIONS,
) -> Any:
    """Assign component ids via deterministic min-label propagation on Spark."""

    from pyspark.sql import functions as F

    stable_nodes = _materialize_exact_blocking_labels(
        nodes.select("node_id"),
        label="node input",
    )
    stable_edges = None
    labels = None
    try:
        # Both frames commonly originate from the same large identity plan. Spark
        # can otherwise retain conflicting expression IDs when the edge frame is
        # joined to an aggregate derived from itself during label propagation.
        stable_edges = _materialize_exact_blocking_labels(
            blocking_edges.select("node_id", "blocking_key"),
            label="edge input",
        )
        labels = _materialize_exact_blocking_labels(
            stable_nodes.select("node_id", F.col("node_id").alias("label"))
        )
        for _ in range(max_iterations):
            blocking_labels = (
                stable_edges.join(labels, "node_id")
                .groupBy("blocking_key")
                .agg(F.min("label").alias("blocking_label"))
            )
            propagated = (
                stable_edges.join(blocking_labels, "blocking_key")
                .groupBy("node_id")
                .agg(F.min("blocking_label").alias("propagated_label"))
            )
            next_labels = (
                stable_nodes.join(labels, "node_id")
                .join(propagated, "node_id", "left")
                .select(
                    F.col("node_id"),
                    F.least(
                        F.col("label"),
                        F.coalesce(F.col("propagated_label"), F.col("label")),
                    ).alias("label"),
                )
            )
            label_changed = (
                next_labels.alias("current")
                .join(labels.alias("previous"), "node_id")
                .where(F.col("current.label") != F.col("previous.label"))
                .limit(1)
                .take(1)
            )
            previous = labels
            try:
                labels = _materialize_exact_blocking_labels(next_labels)
            finally:
                _release_exact_blocking_labels(previous)
            if not label_changed:
                break
        else:
            _release_exact_blocking_labels(labels)
            labels = None
            raise RuntimeError(
                "exact blocking component labels did not converge within "
                f"{max_iterations} iterations"
            )
        return labels.withColumnRenamed("label", "component_id")
    except Exception:
        if labels is not None:
            _release_exact_blocking_labels(labels)
        raise
    finally:
        if stable_edges is not None:
            _release_exact_blocking_labels(stable_edges)
        _release_exact_blocking_labels(stable_nodes)


def build_exact_blocking_component_stats(
    labeled_work: Any,
    *,
    config: IdentityResolutionConfig | None = None,
) -> Any:
    """Compute bounded component/candidate statistics entirely in Spark SQL."""

    from pyspark.sql import functions as F

    config = config or DEFAULT_IDENTITY_RESOLUTION_CONFIG
    work_columns = labeled_work.columns
    component_counts = labeled_work.groupBy("component_id").agg(
        F.count(F.lit(1)).alias("component_node_count")
    )
    eligible_components = component_counts.where(
        F.col("component_node_count") <= F.lit(config.max_exact_blocking_component_size)
    ).select(F.col("component_id").alias("eligible_component_id"))
    distinct_candidates = (
        labeled_work.select(
            "component_id",
            F.explode("candidate_entity_keys").alias("candidate_entity_key"),
        )
        .where(F.col("candidate_entity_key").isNotNull())
        .distinct()
        .join(
            eligible_components,
            F.col("component_id") == F.col("eligible_component_id"),
            "inner",
        )
        .drop("eligible_component_id")
    )
    candidate_counts = distinct_candidates.groupBy("component_id").agg(
        F.count(F.lit(1)).alias("component_candidate_count")
    )
    bounded_candidate_components = candidate_counts.where(
        F.col("component_candidate_count")
        <= F.lit(config.max_exact_blocking_component_candidate_keys)
    ).select(F.col("component_id").alias("bounded_component_id"))
    candidate_keys = (
        distinct_candidates.join(
            bounded_candidate_components,
            F.col("component_id") == F.col("bounded_component_id"),
            "inner",
        )
        .drop("bounded_component_id")
        .groupBy("component_id")
        .agg(
            F.sort_array(F.collect_set("candidate_entity_key")).alias(
                "component_candidate_keys"
            )
        )
    )
    return (
        labeled_work.alias("work")
        .join(
            component_counts.select(
                F.col("component_id").alias("count_component_id"),
                "component_node_count",
            ).alias("counts"),
            F.col("work.component_id") == F.col("counts.count_component_id"),
            "left",
        )
        .join(
            candidate_counts.select(
                F.col("component_id").alias("candidate_count_component_id"),
                "component_candidate_count",
            ).alias("candidate_counts"),
            F.col("work.component_id")
            == F.col("candidate_counts.candidate_count_component_id"),
            "left",
        )
        .join(
            candidate_keys.select(
                F.col("component_id").alias("candidate_keys_component_id"),
                "component_candidate_keys",
            ).alias("candidate_keys"),
            F.col("work.component_id")
            == F.col("candidate_keys.candidate_keys_component_id"),
            "left",
        )
        .select(
            *[F.col(f"work.{name}") for name in work_columns],
            F.coalesce(F.col("counts.component_node_count"), F.lit(0)).alias(
                "component_node_count"
            ),
            F.coalesce(
                F.col("candidate_counts.component_candidate_count"),
                F.lit(0),
            ).alias("component_candidate_count"),
            F.coalesce(
                F.col("candidate_keys.component_candidate_keys"),
                F.array().cast("array<string>"),
            ).alias("component_candidate_keys"),
        )
    )


def build_parent_constrained_work(
    *,
    children: Any,
    relationship_assertions: Any,
    field_assertions: Any,
    type_groups: Any,
    parent_memberships: Any,
    candidate_links: Any,
    child_level: EntityLevel,
    config: IdentityResolutionConfig | None = None,
) -> Any:
    """Build parent-constrained child work with vectorized Spark joins."""

    from pyspark.sql import Window
    from pyspark.sql import functions as F

    config = config or DEFAULT_IDENTITY_RESOLUTION_CONFIG
    source_columns = list(_SOURCE_NODE_COLUMNS)
    empty_strings = F.array().cast("array<string>")
    node_id = F.concat_ws(
        "\x1f",
        F.col("subject_namespace_id"),
        F.col("subject_source_id"),
        F.col("subject_referent_kind"),
    )

    ordinal_fields = (
        field_assertions.where(
            F.lower(F.col("predicate")).isin("season_number", "episode_number")
        )
        .select(
            *source_columns,
            "assertion_id",
            F.lower(F.col("predicate")).alias("ordinal_predicate"),
            F.regexp_replace(
                F.trim(F.get_json_object("value_json", "$")),
                '^"|"$',
                "",
            ).alias("ordinal_value"),
        )
        .where(F.length("ordinal_value") > 0)
    )
    ordinal_groups = ordinal_fields.groupBy(*source_columns).agg(
        F.countDistinct(
            F.when(
                F.col("ordinal_predicate") == "season_number",
                F.col("ordinal_value"),
            )
        ).alias("field_season_value_count"),
        F.countDistinct(
            F.when(
                F.col("ordinal_predicate") == "episode_number",
                F.col("ordinal_value"),
            )
        ).alias("field_episode_value_count"),
        F.min(
            F.when(
                F.col("ordinal_predicate") == "season_number",
                F.col("ordinal_value"),
            )
        ).alias("field_season_number"),
        F.min(
            F.when(
                F.col("ordinal_predicate") == "episode_number",
                F.col("ordinal_value"),
            )
        ).alias("field_episode_number"),
        F.min(
            F.when(
                F.col("ordinal_predicate") == "season_number",
                F.col("assertion_id"),
            )
        ).alias("season_assertion_id"),
        F.min(
            F.when(
                F.col("ordinal_predicate") == "episode_number",
                F.col("assertion_id"),
            )
        ).alias("episode_assertion_id"),
    )

    parent_types = type_groups.select(
        F.col("subject_namespace_id").alias("parent_namespace_id"),
        F.col("subject_source_id").alias("parent_source_id"),
        F.col("subject_referent_kind").alias("parent_referent_kind"),
        F.upper(F.element_at("entity_types", 1)).alias("parent_entity_type"),
    ).withColumn(
        "parent_level",
        F.when(
            F.col("parent_entity_type").isin(
                "SERIES",
                "TV_SERIES",
            ),
            F.lit(EntityLevel.SERIES.value),
        )
        .when(
            F.col("parent_entity_type").isin(
                "SEASON",
                "TV_SEASON",
            ),
            F.lit(EntityLevel.SEASON.value),
        )
        .otherwise(F.lit(None).cast("string")),
    )
    raw_membership_candidates = parent_memberships.select(
        F.col("source_namespace_id").alias("parent_namespace_id"),
        F.col("source_id").alias("parent_source_id"),
        F.col("source_referent_kind").alias("parent_referent_kind"),
        F.col("entity_key").alias("parent_entity_key"),
        F.col("membership_key").alias("parent_membership_key"),
    ).dropDuplicates()
    parent_source_columns = [
        "parent_namespace_id",
        "parent_source_id",
        "parent_referent_kind",
    ]
    parent_membership_counts = raw_membership_candidates.groupBy(
        *parent_source_columns
    ).agg(
        F.countDistinct("parent_membership_key").alias(
            "parent_membership_candidate_count"
        )
    )
    unique_parent_memberships = (
        raw_membership_candidates.join(
            parent_membership_counts.where(
                F.col("parent_membership_candidate_count") == 1
            ).select(*parent_source_columns),
            parent_source_columns,
            "inner",
        )
        .groupBy(*parent_source_columns)
        .agg(
            F.min("parent_entity_key").alias("parent_entity_key"),
            F.min("parent_membership_key").alias("parent_membership_key"),
        )
    )
    membership_candidates = parent_membership_counts.join(
        unique_parent_memberships,
        parent_source_columns,
        "left",
    )
    relationships = relationship_assertions.select(
        *source_columns,
        F.col("assertion_id").alias("relationship_assertion_id"),
        F.lower(F.col("predicate")).alias("relationship_predicate"),
        F.col("object_namespace_id").alias("parent_namespace_id"),
        F.col("object_source_id").alias("parent_source_id"),
        F.col("object_referent_kind").alias("parent_referent_kind"),
        "qualifiers_json",
    )
    relation_rows = (
        children.alias("child")
        .join(relationships.alias("relationship"), source_columns, "left")
        .join(
            parent_types.alias("parent_type"),
            [
                "parent_namespace_id",
                "parent_source_id",
                "parent_referent_kind",
            ],
            "left",
        )
        .join(
            membership_candidates.alias("membership"),
            [
                "parent_namespace_id",
                "parent_source_id",
                "parent_referent_kind",
            ],
            "left",
        )
        .join(ordinal_groups.alias("ordinals"), source_columns, "left")
        .withColumn(
            "field_season_value_count",
            F.coalesce(F.col("field_season_value_count"), F.lit(0)),
        )
        .withColumn(
            "field_episode_value_count",
            F.coalesce(F.col("field_episode_value_count"), F.lit(0)),
        )
        .withColumn(
            "qualifier_season_number",
            F.coalesce(
                F.get_json_object("qualifiers_json", "$.seasonNumber"),
                F.when(
                    F.lit(child_level.value) == EntityLevel.SEASON.value,
                    F.get_json_object("qualifiers_json", "$.ordinal"),
                ),
            ),
        )
        .withColumn(
            "qualifier_episode_number",
            F.coalesce(
                F.get_json_object("qualifiers_json", "$.episodeNumber"),
                F.when(
                    F.col("parent_level") == EntityLevel.SEASON.value,
                    F.get_json_object("qualifiers_json", "$.ordinal"),
                ),
            ),
        )
        .withColumn(
            "season_number",
            F.coalesce("qualifier_season_number", "field_season_number"),
        )
        .withColumn(
            "episode_number",
            F.coalesce("qualifier_episode_number", "field_episode_number"),
        )
    )
    if child_level == EntityLevel.SEASON:
        type_compatible = (F.col("parent_level") == EntityLevel.SERIES.value) & F.col(
            "relationship_predicate"
        ).isin(
            "part_of_series",
            "season_of",
        )
        ordinal_compatible = (
            F.col("season_number").isNotNull()
            & (F.col("field_season_value_count") <= 1)
            & (
                F.col("qualifier_season_number").isNull()
                | F.col("field_season_number").isNull()
                | (F.col("qualifier_season_number") == F.col("field_season_number"))
            )
        )
        relevant_field_assertions = F.when(
            F.col("season_assertion_id").isNotNull(),
            F.array("season_assertion_id"),
        ).otherwise(empty_strings)
    elif child_level == EntityLevel.EPISODE:
        type_compatible = (
            (F.col("parent_level") == EntityLevel.SERIES.value)
            & F.col("relationship_predicate").isin(
                "part_of_series",
                "episode_of_series",
            )
        ) | (
            (F.col("parent_level") == EntityLevel.SEASON.value)
            & F.col("relationship_predicate").isin(
                "part_of_season",
                "episode_of_season",
            )
        )
        episode_number_consistent = (F.col("field_episode_value_count") <= 1) & (
            F.col("qualifier_episode_number").isNull()
            | F.col("field_episode_number").isNull()
            | (F.col("qualifier_episode_number") == F.col("field_episode_number"))
        )
        season_number_consistent = (F.col("field_season_value_count") <= 1) & (
            F.col("qualifier_season_number").isNull()
            | F.col("field_season_number").isNull()
            | (F.col("qualifier_season_number") == F.col("field_season_number"))
        )
        ordinal_compatible = (
            F.col("episode_number").isNotNull()
            & episode_number_consistent
            & (
                (F.col("parent_level") == EntityLevel.SEASON.value)
                | (F.col("season_number").isNotNull() & season_number_consistent)
            )
        )
        relevant_field_assertions = F.array_union(
            F.when(
                F.col("season_assertion_id").isNotNull(),
                F.array("season_assertion_id"),
            ).otherwise(empty_strings),
            F.when(
                F.col("episode_assertion_id").isNotNull(),
                F.array("episode_assertion_id"),
            ).otherwise(empty_strings),
        )
    else:
        raise ValueError("parent-constrained work supports only season or episode")

    relation_rows = (
        relation_rows.withColumn("type_compatible", type_compatible)
        .withColumn("ordinal_compatible", ordinal_compatible)
        .withColumn(
            "ordinal_assertion_ids",
            F.array_sort(
                F.array_distinct(
                    F.array_union(
                        relevant_field_assertions,
                        F.when(
                            F.col("qualifier_season_number").isNotNull()
                            | F.col("qualifier_episode_number").isNotNull(),
                            F.array("relationship_assertion_id"),
                        ).otherwise(empty_strings),
                    )
                )
            ),
        )
    )
    valid_binding = (
        F.col("type_compatible")
        & F.col("ordinal_compatible")
        & F.col("parent_membership_key").isNotNull()
    )
    binding_identity = F.struct(
        "parent_namespace_id",
        "parent_source_id",
        "parent_referent_kind",
        "parent_entity_key",
        "parent_membership_key",
        "parent_level",
        "season_number",
        "episode_number",
    )
    binding_choice = F.struct(
        "parent_namespace_id",
        "parent_source_id",
        "parent_referent_kind",
        "parent_entity_key",
        "parent_membership_key",
        "parent_level",
        F.col("relationship_assertion_id").alias("relationship_assertion_key"),
        "ordinal_assertion_ids",
        "season_number",
        "episode_number",
    )
    binding_order = F.concat_ws(
        "\x1f",
        "parent_namespace_id",
        "parent_source_id",
        "parent_referent_kind",
        "parent_entity_key",
        "parent_membership_key",
        "parent_level",
        F.coalesce("season_number", F.lit("")),
        F.coalesce("episode_number", F.lit("")),
        "relationship_assertion_id",
    )
    relation_summary = relation_rows.groupBy(*source_columns).agg(
        F.countDistinct("relationship_assertion_id").alias("relationship_count"),
        F.countDistinct(
            F.when(
                F.col("type_compatible"),
                F.col("relationship_assertion_id"),
            )
        ).alias("type_compatible_relationship_count"),
        F.countDistinct(
            F.when(
                F.col("type_compatible") & F.col("ordinal_compatible"),
                F.col("relationship_assertion_id"),
            )
        ).alias("ordinal_compatible_relationship_count"),
        F.countDistinct(
            F.when(
                valid_binding,
                F.col("parent_membership_key"),
            )
        ).alias("resolved_parent_membership_count"),
        F.countDistinct(F.when(valid_binding, binding_identity)).alias(
            "parent_choice_count"
        ),
        F.min_by(
            F.when(valid_binding, binding_choice),
            F.when(valid_binding, binding_order),
        ).alias("parent_choice"),
        F.max(
            F.when(
                F.col("type_compatible") & F.col("ordinal_compatible"),
                F.coalesce(
                    F.col("parent_membership_candidate_count"),
                    F.lit(0),
                ),
            ).otherwise(F.lit(0))
        ).alias("max_parent_membership_candidate_count"),
        F.min("relationship_assertion_id").alias("first_relationship_assertion_id"),
    )
    prepared = (
        children.join(relation_summary, source_columns, "left")
        .withColumn(
            "parent_choice_count",
            F.coalesce(F.col("parent_choice_count"), F.lit(0)),
        )
        .withColumn(
            "parent_resolution_mode",
            F.when(
                F.coalesce(F.col("relationship_count"), F.lit(0)) == 0,
                F.lit("CONFLICT_PARENT_UNRESOLVED"),
            )
            .when(
                F.coalesce(
                    F.col("type_compatible_relationship_count"),
                    F.lit(0),
                )
                == 0,
                F.lit("CONFLICT_PARENT_TYPE"),
            )
            .when(
                F.coalesce(
                    F.col("ordinal_compatible_relationship_count"),
                    F.lit(0),
                )
                == 0,
                F.lit("CONFLICT_PARENT_ORDINAL"),
            )
            .when(
                F.coalesce(
                    F.col("max_parent_membership_candidate_count"),
                    F.lit(0),
                )
                > 1,
                F.lit("CONFLICT_PARENT_AMBIGUOUS"),
            )
            .when(
                F.coalesce(
                    F.col("resolved_parent_membership_count"),
                    F.lit(0),
                )
                == 0,
                F.lit("CONFLICT_PARENT_UNRESOLVED"),
            )
            .when(
                F.col("parent_choice_count") != 1,
                F.lit("CONFLICT_PARENT_AMBIGUOUS"),
            )
            .otherwise(F.lit("READY")),
        )
        .select(
            *children.columns,
            F.coalesce(F.col("relationship_count"), F.lit(0)).alias(
                "relationship_count"
            ),
            F.coalesce(
                F.col("type_compatible_relationship_count"),
                F.lit(0),
            ).alias("type_compatible_relationship_count"),
            F.coalesce(
                F.col("ordinal_compatible_relationship_count"),
                F.lit(0),
            ).alias("ordinal_compatible_relationship_count"),
            F.coalesce(
                F.col("resolved_parent_membership_count"),
                F.lit(0),
            ).alias("resolved_parent_membership_count"),
            F.coalesce(
                F.col("max_parent_membership_candidate_count"),
                F.lit(0),
            ).alias("max_parent_membership_candidate_count"),
            F.coalesce(
                F.when(
                    F.col("first_relationship_assertion_id").isNotNull(),
                    F.array("first_relationship_assertion_id"),
                ),
                empty_strings,
            ).alias("relationship_assertion_ids"),
            "parent_choice_count",
            F.col("parent_choice.parent_namespace_id").alias("parent_namespace_id"),
            F.col("parent_choice.parent_source_id").alias("parent_source_id"),
            F.col("parent_choice.parent_referent_kind").alias("parent_referent_kind"),
            F.col("parent_choice.parent_entity_key").alias("parent_entity_key"),
            F.col("parent_choice.parent_membership_key").alias("parent_membership_key"),
            F.col("parent_choice.parent_level").alias("parent_level"),
            F.col("parent_choice.relationship_assertion_key").alias(
                "relationship_assertion_key"
            ),
            F.coalesce(
                F.col("parent_choice.ordinal_assertion_ids"),
                empty_strings,
            ).alias("ordinal_assertion_ids"),
            F.col("parent_choice.season_number").alias("season_number"),
            F.col("parent_choice.episode_number").alias("episode_number"),
            "parent_resolution_mode",
        )
        .withColumn("node_id", node_id)
    )

    node_candidate_counts = candidate_links.groupBy(*source_columns).agg(
        F.count(F.lit(1)).alias("node_candidate_count")
    )
    bounded_node_candidates = (
        candidate_links.join(
            node_candidate_counts.where(
                F.col("node_candidate_count")
                <= F.lit(config.max_exact_blocking_node_candidate_keys)
            ),
            source_columns,
            "inner",
        )
        .groupBy(*source_columns)
        .agg(
            F.sort_array(F.collect_set("candidate_entity_key")).alias(
                "candidate_entity_keys"
            )
        )
    )
    prepared = (
        prepared.join(node_candidate_counts, source_columns, "left")
        .join(bounded_node_candidates, source_columns, "left")
        .withColumn(
            "node_candidate_count",
            F.coalesce(F.col("node_candidate_count"), F.lit(0)),
        )
        .withColumn(
            "candidate_entity_keys",
            F.coalesce(F.col("candidate_entity_keys"), empty_strings),
        )
        .withColumn(
            "parent_resolution_mode",
            F.when(
                F.col("node_candidate_count")
                > F.lit(config.max_exact_blocking_node_candidate_keys),
                F.lit("CONFLICT_NODE_CANDIDATES"),
            ).otherwise(F.col("parent_resolution_mode")),
        )
    )
    ready = (
        prepared.where(F.col("parent_resolution_mode") == "READY")
        .withColumn(
            "component_id",
            F.sha2(
                F.concat_ws(
                    "\x1f",
                    F.lit("parent-constrained-v1"),
                    F.lit(child_level.value),
                    "parent_entity_key",
                    "parent_level",
                    F.coalesce("season_number", F.lit("")),
                    F.coalesce("episode_number", F.lit("")),
                ),
                256,
            ),
        )
        .withColumn(
            "allocation_anchor_id",
            F.min("node_id").over(Window.partitionBy("component_id")),
        )
    )
    ready_stats = build_exact_blocking_component_stats(ready, config=config).withColumn(
        "resolution_mode",
        F.when(
            F.col("component_node_count")
            > F.lit(config.max_exact_blocking_component_size),
            F.lit("CONFLICT_OVERSIZED"),
        )
        .when(
            F.col("component_candidate_count")
            > F.lit(config.max_exact_blocking_component_candidate_keys),
            F.lit("CONFLICT_COMPONENT_CANDIDATES"),
        )
        .when(
            F.size("component_candidate_keys") > 1,
            F.lit("CONFLICT_MULTI"),
        )
        .when(
            F.size("component_candidate_keys") == 1,
            F.lit("ACCEPT"),
        )
        .otherwise(F.lit("BOOTSTRAP")),
    )
    not_ready = (
        prepared.where(F.col("parent_resolution_mode") != "READY")
        .withColumn("component_id", F.col("node_id"))
        .withColumn("component_node_count", F.lit(1).cast("long"))
        .withColumn("component_candidate_count", F.lit(0).cast("long"))
        .withColumn("component_candidate_keys", empty_strings)
        .withColumn("allocation_anchor_id", F.col("node_id"))
        .withColumn("resolution_mode", F.col("parent_resolution_mode"))
    )
    output_columns = [
        *prepared.columns,
        "component_id",
        "component_node_count",
        "component_candidate_count",
        "component_candidate_keys",
        "allocation_anchor_id",
        "resolution_mode",
    ]
    return not_ready.select(*output_columns).unionByName(
        ready_stats.select(*output_columns)
    )


def exact_id_namespace_rows(
    registry: SourceRegistrySnapshot,
) -> tuple[dict[str, object], ...]:
    """Project active registry namespaces into Spark join metadata."""

    active_systems = {
        system.source_system_id
        for system in registry.source_systems
        if system.status == RegistryStatus.ACTIVE
    }
    active_products = {
        product.source_product_id
        for product in registry.source_products
        if product.status == RegistryStatus.ACTIVE
        and product.source_system_id in active_systems
    }
    rows: dict[tuple[str, str, str], dict[str, object]] = {}
    for namespace in registry.source_namespaces:
        if namespace.source_product_id not in active_products:
            continue
        pattern = namespace.identifier_pattern
        if pattern is not None:
            pattern = (
                f"^(?:{pattern})$"
                if namespace.case_sensitive
                else f"(?i)^(?:{pattern})$"
            )
        for scheme in namespace.matching_schemes:
            for referent_kind in namespace.referent_kinds:
                canonical_kind = canonical_referent_kind(referent_kind)
                key = (namespace.namespace_id, scheme, canonical_kind)
                rows[key] = {
                    "namespace_id": namespace.namespace_id,
                    "scheme": scheme,
                    "referent_kind": canonical_kind,
                    "case_sensitive": namespace.case_sensitive,
                    "match_pattern": pattern,
                }
    return tuple(rows[key] for key in sorted(rows))


def _empty_known_index(spark: Any) -> Any:
    return spark.createDataFrame(
        [],
        (
            "namespace_id STRING, normalized_value STRING, "
            "referent_kind STRING, entity_key STRING"
        ),
    )


def _build_seed_index_entry(
    row: Any,
    *,
    materialization_id: str,
    observed_at: str,
    policy_id: str,
    policy_digest: str,
) -> ExternalIdIndexEntry:
    source_key = deterministic_key(
        "v1-external-id-index-source-v2",
        {
            "namespaceId": row["namespace_id"],
            "normalizedValue": row["normalized_value"],
            "referentKind": row["referent_kind"],
            "entityKey": row["entity_key"],
        },
    )
    return build_external_id_index_entry(
        materialization_id=materialization_id,
        namespace_id=row["namespace_id"],
        normalized_value=row["normalized_value"],
        referent_kind=row["referent_kind"],
        entity_key=row["entity_key"],
        assertion_keys=(source_key,),
        observed_at=observed_at,
        policy_id=policy_id,
        policy_digest=policy_digest,
    )


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
    registry: SourceRegistrySnapshot | None = None,
    pinned_inputs: Mapping[str, Any] | None = None,
    source_records: Any | None = None,
    ingest_runs: Any | None = None,
    committed_source_run_ids: tuple[str, ...] | None = None,
    resolution_config: IdentityResolutionConfig | None = None,
) -> tuple[CommunityIngestRun, dict[str, Any]]:
    """Resolve source nodes through registry namespaces and quarantine ambiguity."""

    input_id = require_sha256(input_id, label="input_id")
    image_digest = require_sha256(image_digest, label="image_digest")
    runtime_config_digest = require_sha256(config_digest, label="config_digest")
    resolution_config = resolution_config or DEFAULT_IDENTITY_RESOLUTION_CONFIG
    config_digest = resolution_config.bind_runtime_config(runtime_config_digest)
    started_at = require_rfc3339(started_at, label="started_at")
    registry = registry or build_community_registry()
    materialization_id = deterministic_key(
        "external-id-index-materialization-v2",
        {
            "inputId": input_id,
            "imageDigest": image_digest,
            "configDigest": config_digest,
            "startedAt": started_at,
            "registryDigest": registry.digest,
        },
    )
    required = {
        "community_field_assertion",
        "community_entity_type_assertion",
        "community_identifier_assertion",
        "community_relationship_assertion",
        "community_entity_membership",
    }
    if not required.issubset(visible_silver):
        raise ValueError("identity resolution is missing Silver tables")
    required_external_columns = {"entity_key", "scheme", "value"}
    required_entity_columns = {"entity_key", "entity_type"}
    if not required_external_columns.issubset(v1_external_identifiers.columns):
        raise ValueError("v1 external identifiers are missing required columns")
    if not required_entity_columns.issubset(v1_entities.columns):
        raise ValueError("v1 entities are missing required columns")

    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    namespace_rows = exact_id_namespace_rows(registry)
    namespace_schemes = spark.createDataFrame(
        [
            (
                row["namespace_id"],
                row["scheme"],
                row["referent_kind"],
                row["case_sensitive"],
                row["match_pattern"],
            )
            for row in namespace_rows
        ],
        (
            "namespace_id STRING, scheme STRING, referent_kind STRING, "
            "case_sensitive BOOLEAN, match_pattern STRING"
        ),
    )

    type_configs = spark.createDataFrame(
        [
            (
                source_type,
                level.value,
                entity_kind,
                referent_kind,
            )
            for source_type, (level, entity_kind, referent_kind) in sorted(
                _LEVEL_MAP.items()
            )
        ],
        (
            "source_entity_type STRING, entity_level STRING, "
            "entity_kind STRING, referent_kind STRING"
        ),
    )
    v1_type_configs = spark.createDataFrame(
        sorted(_V1_REFERENT_KIND.items()),
        "v1_entity_type STRING, referent_kind STRING",
    )
    if (
        source_records is None
        or ingest_runs is None
        or committed_source_run_ids is None
    ):
        raise ValueError(
            "identity resolution requires source lifecycle inputs "
            "(source_records, ingest_runs, committed_source_run_ids)"
        )
    bound_source_records, latest_source_record_states = (
        persist_latest_source_record_states(
            source_records=source_records,
            ingest_runs=ingest_runs,
            committed_run_ids=committed_source_run_ids,
            registry=registry,
            as_of=started_at,
            repartition_count=min(
                SOURCE_LIFECYCLE_BYPASS_REPARTITIONS,
                int(spark.conf.get("spark.sql.shuffle.partitions", "200")),
            ),
        )
    )
    current_envelope_keys = current_envelope_keys_from_latest(
        latest_source_record_states,
        as_of=started_at,
    ).persist()
    current_envelope_keys.count()
    try:
        type_assertions = filter_assertions_for_current_envelopes(
            visible_silver["community_entity_type_assertion"],
            current_envelope_keys=current_envelope_keys,
            as_of=started_at,
        )
        identifier_assertions = filter_assertions_for_current_envelopes(
            visible_silver["community_identifier_assertion"],
            current_envelope_keys=current_envelope_keys,
            as_of=started_at,
        )
        relationship_assertions = filter_assertions_for_current_envelopes(
            visible_silver["community_relationship_assertion"],
            current_envelope_keys=current_envelope_keys,
            as_of=started_at,
        )
        field_assertions = filter_assertions_for_current_envelopes(
            visible_silver["community_field_assertion"],
            current_envelope_keys=current_envelope_keys,
            as_of=started_at,
        )
    finally:
        current_envelope_keys.unpersist()
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
        .localCheckpoint(eager=True)
    )
    invalid_type = type_groups.where(
        (F.size("entity_types") != 1) | (F.col("policy_count") != 1)
    ).limit(1)
    if invalid_type.count():
        type_groups.unpersist()
        raise ValueError("source node has conflicting type or policy assertions")

    as_of = F.to_timestamp(F.lit(started_at))
    memberships = select_effective_membership_versions(
        visible_silver["community_entity_membership"],
        as_of=started_at,
    )
    assigned_nodes = memberships.select(
        "source_namespace_id",
        "source_id",
        "source_referent_kind",
    ).dropDuplicates()
    all_unassigned = (
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
        .localCheckpoint(eager=True)
    )
    source_entity_type = F.upper(F.element_at(F.col("entity_types"), 1))
    hierarchy_types = ("SEASON", "TV_SEASON", "EPISODE", "TV_EPISODE")
    unassigned = all_unassigned.where(
        ~source_entity_type.isin(*hierarchy_types)
    ).localCheckpoint(eager=True)
    hierarchy_unassigned = all_unassigned.where(
        source_entity_type.isin(*hierarchy_types)
    ).localCheckpoint(eager=True)

    from pyspark.sql.types import BooleanType

    compatible_referent_kind = F.udf(referent_kinds_compatible, BooleanType())

    registered_identifiers = (
        identifier_assertions.alias("i")
        .join(
            type_groups.alias("t"),
            (
                (F.col("i.subject_namespace_id") == F.col("t.subject_namespace_id"))
                & (F.col("i.subject_source_id") == F.col("t.subject_source_id"))
                & (F.col("i.subject_referent_kind") == F.col("t.subject_referent_kind"))
            ),
            "inner",
        )
        .join(
            type_configs.alias("tc"),
            F.upper(F.element_at(F.col("t.entity_types"), 1))
            == F.col("tc.source_entity_type"),
            "inner",
        )
        .join(
            namespace_schemes.alias("n"),
            (
                (F.lower(F.trim(F.col("i.namespace_id"))) == F.col("n.scheme"))
                & (F.col("tc.referent_kind") == F.col("n.referent_kind"))
            ),
            "inner",
        )
        .where(
            compatible_referent_kind(F.col("i.referent_kind"), F.col("n.referent_kind"))
        )
        .where(
            F.col("n.match_pattern").isNull()
            | F.expr("trim(i.value) RLIKE n.match_pattern")
        )
        .select(
            F.col("i.subject_namespace_id").alias("subject_namespace_id"),
            F.col("i.subject_source_id").alias("subject_source_id"),
            F.col("i.subject_referent_kind").alias("subject_referent_kind"),
            F.col("i.assertion_id").alias("assertion_id"),
            F.col("i.policy_id").alias("identifier_policy_id"),
            F.col("i.policy_digest").alias("identifier_policy_digest"),
            F.col("n.namespace_id").alias("namespace_id"),
            F.when(
                F.col("n.case_sensitive"),
                F.trim(F.col("i.value")),
            )
            .otherwise(F.upper(F.trim(F.col("i.value"))))
            .alias("normalized_value"),
            F.col("n.referent_kind").alias("referent_kind"),
        )
        .dropDuplicates()
        .localCheckpoint(eager=True)
    )

    assigned_identifier_index = (
        registered_identifiers.alias("i")
        .join(
            memberships.alias("m"),
            (
                (F.col("i.subject_namespace_id") == F.col("m.source_namespace_id"))
                & (F.col("i.subject_source_id") == F.col("m.source_id"))
                & (F.col("i.subject_referent_kind") == F.col("m.source_referent_kind"))
            ),
            "inner",
        )
        .select(
            "i.namespace_id",
            "i.normalized_value",
            "i.referent_kind",
            "m.entity_key",
        )
        .dropDuplicates()
    )

    v1_index = (
        v1_external_identifiers.alias("x")
        .join(
            v1_entities.select("entity_key", "entity_type").alias("e"),
            F.col("x.entity_key") == F.col("e.entity_key"),
            "inner",
        )
        .join(
            v1_type_configs.alias("vt"),
            F.upper(F.trim(F.col("e.entity_type"))) == F.col("vt.v1_entity_type"),
            "inner",
        )
        .join(
            namespace_schemes.alias("n"),
            (
                (F.lower(F.trim(F.col("x.scheme"))) == F.col("n.scheme"))
                & (F.col("vt.referent_kind") == F.col("n.referent_kind"))
            ),
            "inner",
        )
        .where(
            F.col("n.match_pattern").isNull()
            | F.expr("trim(x.value) RLIKE n.match_pattern")
        )
        .select(
            F.col("n.namespace_id").alias("namespace_id"),
            F.when(
                F.col("n.case_sensitive"),
                F.trim(F.col("x.value")),
            )
            .otherwise(F.upper(F.trim(F.col("x.value"))))
            .alias("normalized_value"),
            F.col("n.referent_kind").alias("referent_kind"),
            F.col("e.entity_key").alias("entity_key"),
        )
        .dropDuplicates()
    )

    existing_index = visible_silver.get("community_external_id_index")
    existing_known_raw = (
        _empty_known_index(spark)
        if existing_index is None
        else existing_index.where(F.to_timestamp("observed_at") <= as_of).select(
            "namespace_id",
            "normalized_value",
            "referent_kind",
            "entity_key",
        )
    )
    existing_known_aliases = (
        existing_known_raw.alias("x")
        .join(
            namespace_schemes.alias("n"),
            (
                (F.lower(F.trim(F.col("x.namespace_id"))) == F.col("n.scheme"))
                & (F.col("x.referent_kind") == F.col("n.referent_kind"))
            ),
            "inner",
        )
        .where(
            F.col("n.match_pattern").isNull()
            | F.expr("trim(x.normalized_value) RLIKE n.match_pattern")
        )
        .select(
            F.col("n.namespace_id").alias("namespace_id"),
            F.when(
                F.col("n.case_sensitive"),
                F.trim(F.col("x.normalized_value")),
            )
            .otherwise(F.upper(F.trim(F.col("x.normalized_value"))))
            .alias("normalized_value"),
            F.col("n.referent_kind").alias("referent_kind"),
            F.col("x.entity_key").alias("entity_key"),
        )
        .dropDuplicates()
    )
    existing_known = existing_known_raw.unionByName(
        existing_known_aliases
    ).dropDuplicates()
    known_index = (
        existing_known.unionByName(assigned_identifier_index)
        .unionByName(v1_index)
        .dropDuplicates(
            [
                "namespace_id",
                "normalized_value",
                "referent_kind",
                "entity_key",
            ]
        )
    )

    candidate_links = _materialize_exact_blocking_labels(
        (
            registered_identifiers.alias("i")
            .join(
                known_index.alias("k"),
                (
                    (F.col("i.namespace_id") == F.col("k.namespace_id"))
                    & (F.col("i.normalized_value") == F.col("k.normalized_value"))
                    & (F.col("i.referent_kind") == F.col("k.referent_kind"))
                ),
                "inner",
            )
            .select(
                F.col("i.subject_namespace_id").alias("subject_namespace_id"),
                F.col("i.subject_source_id").alias("subject_source_id"),
                F.col("i.subject_referent_kind").alias("subject_referent_kind"),
                F.col("k.entity_key").alias("candidate_entity_key"),
            )
            .distinct()
        ),
        label="exact candidate links",
    )

    def _resolution_input(row: Any) -> SourceNodeResolutionInput:
        source_type = str(row["entity_types"][0]).upper()
        level, kind, _ = _LEVEL_MAP.get(
            source_type,
            (EntityLevel.UNKNOWN, source_type, canonical_referent_kind(source_type)),
        )
        assertion_keys = tuple(
            sorted(
                {
                    *row["type_assertion_ids"],
                    *row["identifier_assertion_ids"],
                }
            )
        )
        return SourceNodeResolutionInput(
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
        )

    def _index_entries(
        row: Any,
        entity_key: str,
    ) -> tuple[ExternalIdIndexEntry, ...]:
        return tuple(
            build_external_id_index_entry(
                materialization_id=materialization_id,
                namespace_id=spec["namespace_id"],
                normalized_value=spec["normalized_value"],
                referent_kind=spec["referent_kind"],
                entity_key=entity_key,
                assertion_keys=(spec["assertion_id"],),
                observed_at=row["observed_at"],
                policy_id=spec["identifier_policy_id"],
                policy_digest=spec["identifier_policy_digest"],
            )
            for spec in row["identifier_specs"]
        )

    candidate_count_rows = candidate_links.groupBy(
        "subject_namespace_id",
        "subject_source_id",
        "subject_referent_kind",
    ).agg(F.countDistinct("candidate_entity_key").alias("node_candidate_count"))
    node_candidate_counts = (
        unassigned.select(*_SOURCE_NODE_COLUMNS)
        .distinct()
        .join(candidate_count_rows, list(_SOURCE_NODE_COLUMNS), "left")
        .fillna({"node_candidate_count": 0})
        .persist()
    )
    identifier_groups = registered_identifiers.groupBy(
        "subject_namespace_id",
        "subject_source_id",
        "subject_referent_kind",
    ).agg(
        F.sort_array(F.collect_set("assertion_id")).alias("identifier_assertion_ids"),
        F.sort_array(
            F.collect_set(
                F.struct(
                    "namespace_id",
                    "normalized_value",
                    "referent_kind",
                    "assertion_id",
                    "identifier_policy_id",
                    "identifier_policy_digest",
                )
            )
        ).alias("identifier_specs"),
    )
    source_columns = list(_SOURCE_NODE_COLUMNS)
    empty_candidate_keys = F.from_json(F.lit("[]"), "array<string>")
    empty_identifier_assertion_ids = F.from_json(F.lit("[]"), "array<string>")
    empty_identifier_specs = F.from_json(
        F.lit("[]"),
        (
            "array<struct<namespace_id:string,normalized_value:string,"
            "referent_kind:string,assertion_id:string,"
            "identifier_policy_id:string,identifier_policy_digest:string>>"
        ),
    )
    node_id_expr = F.concat_ws(
        "\x1f",
        F.col("subject_namespace_id"),
        F.col("subject_source_id"),
        F.col("subject_referent_kind"),
    )
    blocking_key_expr = F.concat_ws(
        "\x1f",
        F.col("namespace_id"),
        F.col("normalized_value"),
        F.col("referent_kind"),
    )

    def _resolution_work_columns(base: Any) -> Any:
        return base.join(identifier_groups, source_columns, "left").select(
            *unassigned.columns,
            F.coalesce("candidate_entity_keys", empty_candidate_keys).alias(
                "candidate_entity_keys"
            ),
            F.coalesce(F.col("node_candidate_count"), F.lit(0)).alias(
                "node_candidate_count"
            ),
            F.coalesce(
                "identifier_assertion_ids", empty_identifier_assertion_ids
            ).alias("identifier_assertion_ids"),
            F.coalesce("identifier_specs", empty_identifier_specs).alias(
                "identifier_specs"
            ),
        )

    try:
        bounded_node_keys = node_candidate_counts.where(
            F.col("node_candidate_count")
            <= F.lit(resolution_config.max_exact_blocking_node_candidate_keys)
        )
        oversized_node_keys = node_candidate_counts.where(
            F.col("node_candidate_count")
            > F.lit(resolution_config.max_exact_blocking_node_candidate_keys)
        )
        oversized_work = _resolution_work_columns(
            unassigned.join(oversized_node_keys, source_columns, "inner").select(
                *unassigned.columns,
                empty_candidate_keys.alias("candidate_entity_keys"),
                F.col("node_candidate_count"),
            )
        )
        oversized_work = (
            oversized_work.withColumn("node_id", node_id_expr)
            .withColumn("component_id", node_id_expr)
            .withColumn("component_node_count", F.lit(1).cast("long"))
            .withColumn("component_candidate_count", F.lit(0).cast("long"))
            .withColumn("component_candidate_keys", empty_candidate_keys)
            .withColumn("resolution_mode", F.lit("CONFLICT_NODE_CANDIDATES"))
        )
        candidates = (
            candidate_links.join(
                bounded_node_keys,
                source_columns,
                "inner",
            )
            .groupBy(
                "subject_namespace_id",
                "subject_source_id",
                "subject_referent_kind",
            )
            .agg(
                F.sort_array(F.collect_set("candidate_entity_key")).alias(
                    "candidate_entity_keys"
                )
            )
            .select(
                F.col("subject_namespace_id"),
                F.col("subject_source_id"),
                F.col("subject_referent_kind"),
                F.col("candidate_entity_keys"),
            )
        )
        bounded_work = _resolution_work_columns(
            unassigned.join(bounded_node_keys, source_columns, "inner").join(
                candidates, source_columns, "left"
            )
        )
        bounded_work = bounded_work.withColumn("node_id", node_id_expr)
        dag_nodes = bounded_work.select("node_id").distinct()
        dag_source_nodes = bounded_work.select(*source_columns).distinct()
        blocking_edges = (
            registered_identifiers.join(dag_source_nodes, source_columns, "inner")
            .select(
                node_id_expr.alias("node_id"),
                blocking_key_expr.alias("blocking_key"),
            )
            .distinct()
        )
        component_labels = assign_exact_blocking_component_ids(
            dag_nodes,
            blocking_edges,
            max_iterations=resolution_config.max_exact_blocking_label_iterations,
        )
        # Truncate shared lineage and avoid Analyzer-conflicting self-joins
        # on component_id (Spark INTERNAL_ERROR on join attribute resolution).
        labeled_work = _materialize_exact_blocking_labels(
            bounded_work.join(component_labels, "node_id"),
            label="component labeled work",
        )
        _release_exact_blocking_labels(component_labels)
        try:
            bounded_work = build_exact_blocking_component_stats(
                labeled_work,
                config=resolution_config,
            )
            bounded_work = _materialize_exact_blocking_labels(
                bounded_work.withColumn(
                    "resolution_mode",
                    F.when(
                        F.col("component_node_count")
                        > F.lit(resolution_config.max_exact_blocking_component_size),
                        F.lit("CONFLICT_OVERSIZED"),
                    )
                    .when(
                        F.col("component_candidate_count")
                        > F.lit(
                            resolution_config.max_exact_blocking_component_candidate_keys
                        ),
                        F.lit("CONFLICT_COMPONENT_CANDIDATES"),
                    )
                    .when(
                        F.size("component_candidate_keys") > 1,
                        F.lit("CONFLICT_MULTI"),
                    )
                    .when(
                        F.size("component_candidate_keys") == 1,
                        F.lit("ACCEPT"),
                    )
                    .otherwise(F.lit("BOOTSTRAP")),
                ),
                label="component resolved work",
            )
        finally:
            _release_exact_blocking_labels(labeled_work)
        work = oversized_work.unionByName(bounded_work)
    finally:
        node_candidate_counts.unpersist()
    anchors = work.where(
        (F.col("resolution_mode") == F.lit("BOOTSTRAP"))
        & (F.col("node_id") == F.col("component_id"))
    )

    def _allocate_anchor(
        row: Any,
    ) -> tuple[str, str]:
        result = resolve_or_allocate_source_node(
            source_node=SourceNodeRef(
                namespace_id=row["subject_namespace_id"],
                source_id=row["subject_source_id"],
                referent_kind=row["subject_referent_kind"],
            ),
            entity_level=_resolution_input(row).entity_level,
            entity_kind=_resolution_input(row).entity_kind,
            exact_candidate_entity_keys=(),
            assertion_keys=_resolution_input(row).assertion_keys,
            observed_at=row["observed_at"],
            policy_id=row["policy_id"],
            policy_digest=row["policy_digest"],
            decision_policy_version="exact-identity-v2",
            decided_by="community-identity-spark-v2",
            materialization_id=materialization_id,
        )
        if result.conflicts:
            raise ValueError("bootstrap anchor unexpectedly conflicted")
        return row["component_id"], result.memberships[0].entity_key

    if anchors.rdd.isEmpty():
        anchor_entity_keys = spark.createDataFrame(
            [],
            "component_id STRING, anchor_entity_key STRING",
        )
    else:
        anchor_entity_keys = anchors.rdd.map(_allocate_anchor).toDF(
            ["component_id", "anchor_entity_key"]
        )
    work = work.join(anchor_entity_keys, "component_id", "left")

    def resolve_row(
        row: Any,
    ) -> tuple[IdentityResolutionResult, tuple[ExternalIdIndexEntry, ...]]:
        resolution_input = _resolution_input(row)
        source_node = resolution_input.source_node
        mode = row["resolution_mode"]
        if mode == "CONFLICT_OVERSIZED":
            result = build_oversized_blocking_component_conflict(
                source_node=source_node,
                component_id=row["component_id"],
                component_node_count=int(row["component_node_count"]),
                assertion_keys=resolution_input.assertion_keys,
                observed_at=row["observed_at"],
                policy_id=row["policy_id"],
                policy_digest=row["policy_digest"],
                materialization_id=materialization_id,
                max_component_size=(
                    resolution_config.max_exact_blocking_component_size
                ),
            )
            return result, ()
        if mode == "CONFLICT_NODE_CANDIDATES":
            result = IdentityResolutionResult(
                conflicts=(
                    build_identity_conflict(
                        materialization_id=materialization_id,
                        source_node=source_node,
                        candidate_entity_keys=(
                            deterministic_key(
                                "oversized-exact-blocking-node-candidates-v2",
                                {
                                    "namespaceId": row["subject_namespace_id"],
                                    "sourceId": row["subject_source_id"],
                                    "referentKind": row["subject_referent_kind"],
                                },
                            ),
                        ),
                        assertion_keys=resolution_input.assertion_keys,
                        reason="EXACT_BLOCKING_NODE_CANDIDATE_LIMIT_EXCEEDED",
                        observed_at=row["observed_at"],
                        policy_id=row["policy_id"],
                        policy_digest=row["policy_digest"],
                        details={
                            "nodeCandidateCount": int(row["node_candidate_count"]),
                            "maxNodeCandidateKeys": (
                                resolution_config.max_exact_blocking_node_candidate_keys
                            ),
                        },
                    ),
                )
            ).require_consistent()
            return result, ()
        if mode == "CONFLICT_COMPONENT_CANDIDATES":
            result = IdentityResolutionResult(
                conflicts=(
                    build_identity_conflict(
                        materialization_id=materialization_id,
                        source_node=source_node,
                        candidate_entity_keys=(
                            deterministic_key(
                                "oversized-exact-blocking-component-candidates-v2",
                                {"componentId": row["component_id"]},
                            ),
                        ),
                        assertion_keys=resolution_input.assertion_keys,
                        reason="EXACT_BLOCKING_COMPONENT_CANDIDATE_LIMIT_EXCEEDED",
                        observed_at=row["observed_at"],
                        policy_id=row["policy_id"],
                        policy_digest=row["policy_digest"],
                        details={
                            "componentCandidateCount": int(
                                row["component_candidate_count"]
                            ),
                            "maxComponentCandidateKeys": (
                                resolution_config.max_exact_blocking_component_candidate_keys
                            ),
                        },
                    ),
                )
            ).require_consistent()
            return result, ()
        if mode == "CONFLICT_MULTI":
            result = IdentityResolutionResult(
                conflicts=(
                    build_identity_conflict(
                        materialization_id=materialization_id,
                        source_node=source_node,
                        candidate_entity_keys=tuple(row["component_candidate_keys"]),
                        assertion_keys=resolution_input.assertion_keys,
                        reason="MULTIPLE_EXACT_IDENTIFIER_CANDIDATES",
                        observed_at=row["observed_at"],
                        policy_id=row["policy_id"],
                        policy_digest=row["policy_digest"],
                    ),
                )
            ).require_consistent()
            return result, ()
        if mode == "ACCEPT":
            result = resolve_or_allocate_source_node(
                source_node=source_node,
                entity_level=resolution_input.entity_level,
                entity_kind=resolution_input.entity_kind,
                exact_candidate_entity_keys=(row["component_candidate_keys"][0],),
                assertion_keys=resolution_input.assertion_keys,
                observed_at=row["observed_at"],
                policy_id=row["policy_id"],
                policy_digest=row["policy_digest"],
                decision_policy_version="exact-identity-v2",
                decided_by="community-identity-spark-v2",
                materialization_id=materialization_id,
            )
        elif row["node_id"] == row["component_id"]:
            result = resolve_or_allocate_source_node(
                source_node=source_node,
                entity_level=resolution_input.entity_level,
                entity_kind=resolution_input.entity_kind,
                exact_candidate_entity_keys=(),
                assertion_keys=resolution_input.assertion_keys,
                observed_at=row["observed_at"],
                policy_id=row["policy_id"],
                policy_digest=row["policy_digest"],
                decision_policy_version="exact-identity-v2",
                decided_by="community-identity-spark-v2",
                materialization_id=materialization_id,
            )
        else:
            result = resolve_shared_blocking_member(
                source_node=source_node,
                entity_key=row["anchor_entity_key"],
                anchor_source_node=_source_node_from_id(row["component_id"]),
                assertion_keys=resolution_input.assertion_keys,
                observed_at=row["observed_at"],
                policy_id=row["policy_id"],
                policy_digest=row["policy_digest"],
                decision_policy_version="exact-identity-v2",
                decided_by="community-identity-spark-v2",
            )
        if result.conflicts:
            return result, ()
        entity_key = result.memberships[0].entity_key
        return result, _index_entries(row, entity_key)

    revocations, mapping_conflicts = build_inactive_membership_revocation_worklist(
        latest_states=latest_source_record_states,
        bound=bound_source_records,
        memberships=memberships,
        type_assertions=visible_silver["community_entity_type_assertion"],
        identifier_assertions=visible_silver["community_identifier_assertion"],
        as_of=started_at,
    )

    def _revoke_inactive_membership(row: Any) -> IdentityResolutionResult:
        from video_media_catalog.identity_v2 import build_entity_membership

        source_node = SourceNodeRef(
            namespace_id=row["source_namespace_id"],
            source_id=row["source_id"],
            referent_kind=row["source_referent_kind"],
        )
        membership = build_entity_membership(
            source_node=source_node,
            entity_key=row["entity_key"],
            decision_id=row["decision_id"],
            valid_from=row["valid_from"],
        )
        assertion_keys = tuple(
            item for item in row["assertion_ids"] if item is not None
        )
        evidence = build_lifecycle_revoke_evidence(
            source_node=source_node,
            candidate_entity_key=row["entity_key"],
            assertion_keys=assertion_keys,
            observed_at=row["assertion_observed_at"],
            policy_id=row["policy_id"],
            policy_digest=row["policy_digest"],
            envelope_key=row["lifecycle_envelope_key"],
            operation=row["operation"],
            lifecycle_observed_at=row["lifecycle_observed_at"],
        )
        return revoke_identity_membership(
            membership=membership,
            evidence_keys=(evidence.evidence_key,),
            policy_version="exact-identity-v2",
            decided_by="community-identity-spark-v2",
            decided_at=row["lifecycle_observed_at"],
            reason="SOURCE_RECORD_INACTIVE",
            evidence=(evidence,),
        )

    def _inactive_mapping_conflict(row: Any) -> IdentityResolutionResult:
        operation_policy = internal_key_continuity_profile()
        reason = (
            "INACTIVE_SOURCE_NODE_AMBIGUOUS"
            if row["revocation_disposition"] == "AMBIGUOUS"
            else "INACTIVE_SOURCE_NODE_UNMAPPED"
        )
        candidate_entity_keys = tuple(
            deterministic_key(
                "inactive-source-node-mapping-v2",
                {
                    "sourceSystemId": row["source_system_id"],
                    "sourceProductId": row["source_product_id"],
                    "sourceNamespaceId": row["source_namespace_id"],
                    "sourceRecordId": row["source_record_id"],
                    "envelopeKey": row["envelope_key"],
                    "operation": row["operation"],
                },
            ),
        )
        assertion_keys = tuple(row["assertion_ids"] or [])
        if not assertion_keys:
            assertion_keys = candidate_entity_keys
        source_nodes = row["source_nodes"] or []
        if source_nodes:
            anchor = source_nodes[0]
            conflict_node = SourceNodeRef(
                namespace_id=anchor["namespace_id"],
                source_id=anchor["source_id"],
                referent_kind=anchor["referent_kind"],
            )
        else:
            conflict_node = SourceNodeRef(
                namespace_id=row["source_namespace_id"],
                source_id=row["source_record_id"],
                referent_kind="SOURCE_RECORD",
            )
        return IdentityResolutionResult(
            conflicts=(
                build_identity_conflict(
                    materialization_id=materialization_id,
                    source_node=conflict_node,
                    candidate_entity_keys=candidate_entity_keys,
                    assertion_keys=assertion_keys,
                    reason=reason,
                    observed_at=row["lifecycle_observed_at"],
                    policy_id=row["policy_id"] or operation_policy.policy_id,
                    policy_digest=row["policy_digest"] or operation_policy.digest,
                    details={
                        "envelopeKey": row["envelope_key"],
                        "operation": row["operation"],
                        "sourceNodes": source_nodes,
                    },
                ),
            )
        ).require_consistent()

    membership_projection_schema = (
        "source_namespace_id STRING, source_id STRING, "
        "source_referent_kind STRING, entity_key STRING, membership_key STRING"
    )

    def _project_active_memberships(item: Any) -> list[tuple[str, ...]]:
        result = item[0] if isinstance(item, tuple) else item
        return [
            (
                membership.source_node.namespace_id,
                membership.source_node.source_id,
                membership.source_node.referent_kind,
                membership.entity_key,
                membership.membership_key,
            )
            for membership in result.memberships
            if membership.valid_to is None
        ]

    def _hierarchy_work_columns(children: Any) -> Any:
        return children.join(identifier_groups, source_columns, "left").select(
            *children.columns,
            F.coalesce(
                "identifier_assertion_ids",
                empty_identifier_assertion_ids,
            ).alias("identifier_assertion_ids"),
            F.coalesce("identifier_specs", empty_identifier_specs).alias(
                "identifier_specs"
            ),
        )

    def _attach_parent_anchor_keys(parent_work: Any) -> Any:
        parent_anchors = parent_work.where(
            (F.col("resolution_mode") == "BOOTSTRAP")
            & (F.col("node_id") == F.col("allocation_anchor_id"))
        )

        def allocate_parent_anchor(row: Any) -> tuple[str, str]:
            resolution_input = _resolution_input(row)
            constraint = ParentConstraint(
                child_level=resolution_input.entity_level,
                parent_level=EntityLevel(row["parent_level"]),
                parent_source_node=SourceNodeRef(
                    namespace_id=row["parent_namespace_id"],
                    source_id=row["parent_source_id"],
                    referent_kind=row["parent_referent_kind"],
                ),
                parent_entity_key=row["parent_entity_key"],
                parent_membership_key=row["parent_membership_key"],
                relationship_assertion_key=row["relationship_assertion_key"],
                ordinal_assertion_keys=tuple(row["ordinal_assertion_ids"]),
                season_number=row["season_number"],
                episode_number=row["episode_number"],
            )
            result = resolve_parent_constrained_source_node(
                source_node=resolution_input.source_node,
                entity_level=resolution_input.entity_level,
                entity_kind=resolution_input.entity_kind,
                parent_constraint=constraint,
                assertion_keys=resolution_input.assertion_keys,
                observed_at=row["observed_at"],
                policy_id=row["policy_id"],
                policy_digest=row["policy_digest"],
                decision_policy_version="parent-constrained-identity-v1",
                decided_by="community-identity-spark-v2",
            )
            return row["component_id"], result.memberships[0].entity_key

        if parent_anchors.rdd.isEmpty():
            anchor_keys = spark.createDataFrame(
                [],
                "component_id STRING, parent_anchor_entity_key STRING",
            )
        else:
            anchor_keys = parent_anchors.rdd.map(allocate_parent_anchor).toDF(
                ["component_id", "parent_anchor_entity_key"]
            )
        return parent_work.join(anchor_keys, "component_id", "left")

    parent_conflict_reasons = {
        "CONFLICT_PARENT_UNRESOLVED": "PARENT_MEMBERSHIP_UNRESOLVED",
        "CONFLICT_PARENT_TYPE": "PARENT_TYPE_INCOMPATIBLE",
        "CONFLICT_PARENT_ORDINAL": "PARENT_ORDINAL_UNRESOLVED",
        "CONFLICT_PARENT_AMBIGUOUS": "PARENT_MEMBERSHIP_AMBIGUOUS",
        "CONFLICT_NODE_CANDIDATES": ("EXACT_BLOCKING_NODE_CANDIDATE_LIMIT_EXCEEDED"),
        "CONFLICT_COMPONENT_CANDIDATES": (
            "EXACT_BLOCKING_COMPONENT_CANDIDATE_LIMIT_EXCEEDED"
        ),
        "CONFLICT_MULTI": "MULTIPLE_EXACT_IDENTIFIER_CANDIDATES",
    }

    def resolve_parent_row(
        row: Any,
    ) -> tuple[IdentityResolutionResult, tuple[ExternalIdIndexEntry, ...]]:
        resolution_input = _resolution_input(row)
        source_node = resolution_input.source_node
        mode = row["resolution_mode"]
        if mode == "CONFLICT_OVERSIZED":
            return (
                build_oversized_blocking_component_conflict(
                    source_node=source_node,
                    component_id=row["component_id"],
                    component_node_count=int(row["component_node_count"]),
                    assertion_keys=resolution_input.assertion_keys,
                    observed_at=row["observed_at"],
                    policy_id=row["policy_id"],
                    policy_digest=row["policy_digest"],
                    materialization_id=materialization_id,
                    max_component_size=(
                        resolution_config.max_exact_blocking_component_size
                    ),
                ),
                (),
            )
        if mode in parent_conflict_reasons:
            candidate_keys = tuple(
                row["component_candidate_keys"] or row["candidate_entity_keys"] or []
            )
            if not candidate_keys:
                candidate_keys = (
                    deterministic_key(
                        "parent-constrained-conflict-candidate-v2",
                        {
                            "sourceNode": {
                                "namespaceId": row["subject_namespace_id"],
                                "sourceId": row["subject_source_id"],
                                "referentKind": row["subject_referent_kind"],
                            },
                            "mode": mode,
                        },
                    ),
                )
            assertion_keys = tuple(
                sorted(
                    {
                        *resolution_input.assertion_keys,
                        *(row["relationship_assertion_ids"] or []),
                        *(row["ordinal_assertion_ids"] or []),
                    }
                )
            )
            result = IdentityResolutionResult(
                conflicts=(
                    build_identity_conflict(
                        materialization_id=materialization_id,
                        source_node=source_node,
                        candidate_entity_keys=candidate_keys,
                        assertion_keys=assertion_keys,
                        reason=parent_conflict_reasons[mode],
                        observed_at=row["observed_at"],
                        policy_id=row["policy_id"],
                        policy_digest=row["policy_digest"],
                        details={
                            "parentConstrained": True,
                            "relationshipCount": int(row["relationship_count"]),
                            "typeCompatibleRelationshipCount": int(
                                row["type_compatible_relationship_count"]
                            ),
                            "ordinalCompatibleRelationshipCount": int(
                                row["ordinal_compatible_relationship_count"]
                            ),
                            "resolvedParentMembershipCount": int(
                                row["resolved_parent_membership_count"]
                            ),
                            "maxParentMembershipCandidateCount": int(
                                row["max_parent_membership_candidate_count"]
                            ),
                            "parentChoiceCount": int(row["parent_choice_count"]),
                            "nodeCandidateCount": int(row["node_candidate_count"]),
                            "componentCandidateCount": int(
                                row["component_candidate_count"]
                            ),
                        },
                    ),
                )
            ).require_consistent()
            return result, ()
        constraint = ParentConstraint(
            child_level=resolution_input.entity_level,
            parent_level=EntityLevel(row["parent_level"]),
            parent_source_node=SourceNodeRef(
                namespace_id=row["parent_namespace_id"],
                source_id=row["parent_source_id"],
                referent_kind=row["parent_referent_kind"],
            ),
            parent_entity_key=row["parent_entity_key"],
            parent_membership_key=row["parent_membership_key"],
            relationship_assertion_key=row["relationship_assertion_key"],
            ordinal_assertion_keys=tuple(row["ordinal_assertion_ids"]),
            season_number=row["season_number"],
            episode_number=row["episode_number"],
        )
        entity_key = (
            row["component_candidate_keys"][0]
            if mode == "ACCEPT"
            else row["parent_anchor_entity_key"]
        )
        allocate_entity = (
            mode == "BOOTSTRAP" and row["node_id"] == row["allocation_anchor_id"]
        )
        result = resolve_parent_constrained_source_node(
            source_node=source_node,
            entity_level=resolution_input.entity_level,
            entity_kind=resolution_input.entity_kind,
            parent_constraint=constraint,
            assertion_keys=resolution_input.assertion_keys,
            observed_at=row["observed_at"],
            policy_id=row["policy_id"],
            policy_digest=row["policy_digest"],
            decision_policy_version="parent-constrained-identity-v1",
            decided_by="community-identity-spark-v2",
            entity_key=None if allocate_entity else entity_key,
        )
        resolved_key = result.memberships[0].entity_key
        return result, _index_entries(row, resolved_key)

    resolution_results = work.rdd.map(resolve_row).persist(StorageLevel.MEMORY_AND_DISK)
    hierarchy_source_type = F.upper(F.element_at(F.col("entity_types"), 1))
    season_source_nodes = hierarchy_unassigned.where(
        hierarchy_source_type.isin("SEASON", "TV_SEASON")
    )
    episode_source_nodes = hierarchy_unassigned.where(
        hierarchy_source_type.isin("EPISODE", "TV_EPISODE")
    )
    has_season_children = bool(season_source_nodes.limit(1).take(1))
    has_episode_children = bool(episode_source_nodes.limit(1).take(1))
    parent_memberships = None
    episode_parent_memberships = None
    materialized_season_work = None
    materialized_episode_work = None
    season_results = spark.sparkContext.emptyRDD()
    episode_results = spark.sparkContext.emptyRDD()
    if has_season_children or has_episode_children:
        relationship_assertions = _materialize_exact_blocking_labels(
            relationship_assertions,
            label="current relationship assertions",
        )
        field_assertions = _materialize_exact_blocking_labels(
            field_assertions,
            label="current field assertions",
        )
        exact_memberships = spark.createDataFrame(
            resolution_results.flatMap(_project_active_memberships),
            schema=membership_projection_schema,
        )
        parent_memberships = _materialize_exact_blocking_labels(
            (
                memberships.select(
                    "source_namespace_id",
                    "source_id",
                    "source_referent_kind",
                    "entity_key",
                    "membership_key",
                )
                .unionByName(exact_memberships)
                .dropDuplicates()
            ),
            label="resolved primary memberships",
        )
        _release_exact_blocking_labels(bounded_work)
    if has_season_children:
        season_children = _hierarchy_work_columns(season_source_nodes)
        materialized_season_work = _materialize_exact_blocking_labels(
            build_parent_constrained_work(
                children=season_children,
                relationship_assertions=relationship_assertions,
                field_assertions=field_assertions,
                type_groups=type_groups,
                parent_memberships=parent_memberships,
                candidate_links=candidate_links,
                child_level=EntityLevel.SEASON,
                config=resolution_config,
            ),
            label="season parent-constrained work",
        )
        season_work = _attach_parent_anchor_keys(materialized_season_work)
        season_results = season_work.rdd.map(resolve_parent_row).persist(
            StorageLevel.MEMORY_AND_DISK
        )
    if has_episode_children:
        if has_season_children:
            season_memberships = spark.createDataFrame(
                season_results.flatMap(_project_active_memberships),
                schema=membership_projection_schema,
            )
            episode_parent_memberships = _materialize_exact_blocking_labels(
                parent_memberships.unionByName(season_memberships).dropDuplicates(),
                label="resolved season memberships",
            )
            _release_exact_blocking_labels(materialized_season_work)
        else:
            episode_parent_memberships = parent_memberships
        episode_children = _hierarchy_work_columns(episode_source_nodes)
        materialized_episode_work = _materialize_exact_blocking_labels(
            build_parent_constrained_work(
                children=episode_children,
                relationship_assertions=relationship_assertions,
                field_assertions=field_assertions,
                type_groups=type_groups,
                parent_memberships=episode_parent_memberships,
                candidate_links=candidate_links,
                child_level=EntityLevel.EPISODE,
                config=resolution_config,
            ),
            label="episode parent-constrained work",
        )
        episode_work = _attach_parent_anchor_keys(materialized_episode_work)
        episode_results = episode_work.rdd.map(resolve_parent_row)
    if parent_memberships is not episode_parent_memberships:
        _release_exact_blocking_labels(parent_memberships)
    _release_exact_blocking_labels(episode_parent_memberships)
    _release_exact_blocking_labels(relationship_assertions)
    _release_exact_blocking_labels(field_assertions)
    hierarchy_unassigned.unpersist()
    unassigned.unpersist()
    all_unassigned.unpersist()

    active_membership_counts = memberships.groupBy(
        "source_namespace_id",
        "source_id",
        "source_referent_kind",
    ).agg(
        F.countDistinct("membership_key").alias("existing_membership_count"),
    )
    bounded_active_membership_keys = (
        memberships.join(
            active_membership_counts.where(
                F.col("existing_membership_count")
                <= F.lit(resolution_config.max_exact_blocking_node_candidate_keys)
            ),
            [
                "source_namespace_id",
                "source_id",
                "source_referent_kind",
            ],
            "inner",
        )
        .groupBy(
            "source_namespace_id",
            "source_id",
            "source_referent_kind",
        )
        .agg(
            F.sort_array(F.collect_set("entity_key")).alias(
                "existing_membership_entity_keys"
            )
        )
    )
    active_membership_groups = active_membership_counts.join(
        bounded_active_membership_keys,
        [
            "source_namespace_id",
            "source_id",
            "source_referent_kind",
        ],
        "left",
    ).withColumn(
        "existing_membership_entity_keys",
        F.coalesce(
            F.col("existing_membership_entity_keys"),
            empty_candidate_keys,
        ),
    )
    recheck_candidate_counts = candidate_links.groupBy(*source_columns).agg(
        F.count(F.lit(1)).alias("node_candidate_count")
    )
    recheck_candidate_keys = (
        candidate_links.join(
            recheck_candidate_counts.where(
                F.col("node_candidate_count")
                <= F.lit(resolution_config.max_exact_blocking_node_candidate_keys)
            ),
            source_columns,
            "inner",
        )
        .groupBy(*source_columns)
        .agg(
            F.sort_array(F.collect_set("candidate_entity_key")).alias(
                "candidate_entity_keys"
            )
        )
    )
    assigned_rechecks = (
        type_groups.alias("types")
        .join(
            active_membership_groups.alias("memberships"),
            (
                (
                    F.col("types.subject_namespace_id")
                    == F.col("memberships.source_namespace_id")
                )
                & (F.col("types.subject_source_id") == F.col("memberships.source_id"))
                & (
                    F.col("types.subject_referent_kind")
                    == F.col("memberships.source_referent_kind")
                )
            ),
            "inner",
        )
        .select("types.*", "memberships.*")
        .join(recheck_candidate_counts, source_columns, "left")
        .join(recheck_candidate_keys, source_columns, "left")
        .join(identifier_groups, source_columns, "left")
        .withColumn(
            "node_candidate_count",
            F.coalesce(F.col("node_candidate_count"), F.lit(0)),
        )
        .withColumn(
            "candidate_entity_keys",
            F.coalesce(F.col("candidate_entity_keys"), empty_candidate_keys),
        )
        .withColumn(
            "new_candidate_entity_keys",
            F.array_except(
                F.col("candidate_entity_keys"),
                F.col("existing_membership_entity_keys"),
            ),
        )
        .where(
            (F.col("existing_membership_count") != 1)
            | (
                F.col("node_candidate_count")
                > F.lit(resolution_config.max_exact_blocking_node_candidate_keys)
            )
            | (F.size("new_candidate_entity_keys") > 0)
        )
    )

    def resolve_assigned_recheck(row: Any) -> IdentityResolutionResult:
        resolution_input = _resolution_input(row)
        existing_keys = tuple(row["existing_membership_entity_keys"])
        candidate_keys = tuple(row["candidate_entity_keys"] or [])
        if int(row["existing_membership_count"]) != 1:
            reason = "EXISTING_MEMBERSHIP_AMBIGUOUS"
        elif (
            int(row["node_candidate_count"])
            > resolution_config.max_exact_blocking_node_candidate_keys
        ):
            reason = "EXACT_BLOCKING_NODE_CANDIDATE_LIMIT_EXCEEDED"
            candidate_keys = (
                *existing_keys,
                deterministic_key(
                    "oversized-existing-membership-candidates-v2",
                    {
                        "namespaceId": row["subject_namespace_id"],
                        "sourceId": row["subject_source_id"],
                        "referentKind": row["subject_referent_kind"],
                    },
                ),
            )
        else:
            reason = "EXISTING_MEMBERSHIP_EXACT_ID_CONFLICT"
        all_candidates = tuple(sorted({*existing_keys, *candidate_keys}))
        if not all_candidates:
            all_candidates = (
                deterministic_key(
                    "ambiguous-existing-membership-candidates-v2",
                    {
                        "namespaceId": row["subject_namespace_id"],
                        "sourceId": row["subject_source_id"],
                        "referentKind": row["subject_referent_kind"],
                    },
                ),
            )
        return IdentityResolutionResult(
            conflicts=(
                build_identity_conflict(
                    materialization_id=materialization_id,
                    source_node=resolution_input.source_node,
                    candidate_entity_keys=all_candidates,
                    assertion_keys=resolution_input.assertion_keys,
                    reason=reason,
                    observed_at=row["observed_at"],
                    policy_id=row["policy_id"],
                    policy_digest=row["policy_digest"],
                    details={
                        "incrementalReevaluation": True,
                        "existingMembershipCount": int(
                            row["existing_membership_count"]
                        ),
                        "nodeCandidateCount": int(row["node_candidate_count"]),
                        "membershipRewriteSuppressed": True,
                    },
                ),
            )
        ).require_consistent()

    assigned_recheck_results = assigned_rechecks.rdd.map(resolve_assigned_recheck)
    if revocations.rdd.isEmpty():
        revocation_results = spark.sparkContext.emptyRDD()
    else:
        revocation_results = revocations.rdd.map(_revoke_inactive_membership)
    if mapping_conflicts.rdd.isEmpty():
        mapping_conflict_results = spark.sparkContext.emptyRDD()
    else:
        mapping_conflict_results = mapping_conflicts.rdd.map(_inactive_mapping_conflict)
    revocation_results = revocation_results.union(mapping_conflict_results)
    results = (
        resolution_results.union(season_results)
        .union(episode_results)
        .union(assigned_recheck_results.map(lambda item: (item, ())))
        .union(revocation_results.map(lambda item: (item, ())))
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    operation_policy = internal_key_continuity_profile()
    seed_entries = v1_index.rdd.map(
        lambda row: _build_seed_index_entry(
            row,
            materialization_id=materialization_id,
            observed_at=started_at,
            policy_id=operation_policy.policy_id,
            policy_digest=operation_policy.digest,
        )
    )
    index_entries = (
        seed_entries.union(results.flatMap(lambda item: item[1]))
        .keyBy(lambda item: item.index_entry_key)
        .reduceByKey(lambda left, _right: left)
        .values()
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    try:
        expected_counts = {table: 0 for table in DATA_TABLE_COLUMNS}
        expected_counts["community_external_id_index"] = int(index_entries.count())
        expected_counts["community_entity_ledger"] = int(
            results.map(lambda item: len(item[0].entities)).sum()
        )
        expected_counts["community_identity_evidence"] = int(
            results.map(lambda item: len(item[0].evidence)).sum()
        )
        expected_counts["community_identity_conflict"] = int(
            results.map(lambda item: len(item[0].conflicts)).sum()
        )
        expected_counts["community_identity_decision"] = int(
            results.map(lambda item: len(item[0].decisions)).sum()
        )
        expected_counts["community_entity_membership"] = int(
            results.map(lambda item: len(item[0].memberships)).sum()
        )

        def count_conflict_reasons(
            counts: dict[str, int],
            item: tuple[
                IdentityResolutionResult,
                tuple[ExternalIdIndexEntry, ...],
            ],
        ) -> dict[str, int]:
            updated = dict(counts)
            for conflict in item[0].conflicts:
                updated[conflict.reason] = updated.get(conflict.reason, 0) + 1
            return updated

        def merge_conflict_reason_counts(
            left: dict[str, int],
            right: dict[str, int],
        ) -> dict[str, int]:
            merged = dict(left)
            for reason, count in right.items():
                merged[reason] = merged.get(reason, 0) + count
            return merged

        conflict_counts_by_reason = dict(
            sorted(
                results.aggregate(
                    {},
                    count_conflict_reasons,
                    merge_conflict_reason_counts,
                ).items()
            )
        )
        if (
            sum(conflict_counts_by_reason.values())
            != expected_counts["community_identity_conflict"]
        ):
            raise RuntimeError("identity conflict reason counts changed")
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
                "registryDigest": registry.digest,
                "resolver": "community-identity-spark-v2",
                "runtimeConfigDigest": runtime_config_digest,
                "identityResolutionConfigDigest": resolution_config.digest,
                "identityResolutionConfig": resolution_config.model_dump(
                    mode="json",
                    by_alias=True,
                ),
                "conflictCountsByReason": conflict_counts_by_reason,
                **(
                    {"pinnedInputs": dict(pinned_inputs)}
                    if pinned_inputs is not None
                    else {}
                ),
            },
        )
        row_rdds = {
            "community_external_id_index": index_entries.map(
                lambda value: external_id_index_row(run.run_id, value)
            ),
            "community_entity_ledger": results.flatMap(
                lambda item: (
                    entity_ledger_row(run.run_id, value) for value in item[0].entities
                )
            ),
            "community_identity_evidence": results.flatMap(
                lambda item: (
                    identity_evidence_row(run.run_id, value)
                    for value in item[0].evidence
                )
            ),
            "community_identity_conflict": results.flatMap(
                lambda item: (
                    identity_conflict_row(run.run_id, value)
                    for value in item[0].conflicts
                )
            ),
            "community_identity_decision": results.flatMap(
                lambda item: (
                    identity_decision_row(run.run_id, value)
                    for value in item[0].decisions
                )
            ),
            "community_entity_membership": results.flatMap(
                lambda item: (
                    entity_membership_row(run.run_id, value)
                    for value in item[0].memberships
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
        index_entries.unpersist()
        results.unpersist()
        resolution_results.unpersist()
        season_results.unpersist()
        _release_exact_blocking_labels(work)
        _release_exact_blocking_labels(bounded_work)
        _release_exact_blocking_labels(materialized_season_work)
        _release_exact_blocking_labels(materialized_episode_work)
        _release_exact_blocking_labels(parent_memberships)
        _release_exact_blocking_labels(episode_parent_memberships)
        _release_exact_blocking_labels(candidate_links)
        _release_exact_blocking_labels(relationship_assertions)
        _release_exact_blocking_labels(field_assertions)
        unassigned.unpersist()
        hierarchy_unassigned.unpersist()
        all_unassigned.unpersist()
        registered_identifiers.unpersist()
        type_groups.unpersist()
        latest_source_record_states.unpersist()
        bound_source_records.unpersist()
