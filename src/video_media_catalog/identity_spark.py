"""Distributed registry-driven exact identity resolution."""

from __future__ import annotations

import contextlib
from collections.abc import Mapping
from typing import Any

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
    resolve_shared_blocking_member,
    revoke_identity_membership,
)
from video_media_catalog.identity_v2 import (
    EntityLevel,
    ExternalIdIndexEntry,
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
from video_media_catalog.v2_contracts import require_rfc3339, require_sha256

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

_SOURCE_NODE_COLUMNS = (
    "subject_namespace_id",
    "subject_source_id",
    "subject_referent_kind",
)


def _materialize_exact_blocking_labels(
    frame: Any,
    *,
    label: str = "label",
) -> Any:
    """Truncate label propagation lineage; fail closed on checkpoint loss."""

    try:
        materialized = frame.localCheckpoint(eager=True)
    except Exception as exc:
        raise RuntimeError(
            f"exact blocking {label} localCheckpoint failed; refusing incomplete merge"
        ) from exc
    try:
        materialized.take(1)
    except Exception as exc:
        raise RuntimeError(
            f"exact blocking {label} checkpoint is unreadable; "
            "refusing incomplete merge"
        ) from exc
    return materialized


def _release_exact_blocking_labels(frame: Any) -> None:
    with contextlib.suppress(Exception):
        frame.unpersist()


def assign_exact_blocking_component_ids(
    nodes: Any,
    blocking_edges: Any,
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
        for _ in range(MAX_EXACT_BLOCKING_LABEL_ITERATIONS):
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
                f"{MAX_EXACT_BLOCKING_LABEL_ITERATIONS} iterations"
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
) -> tuple[CommunityIngestRun, dict[str, Any]]:
    """Resolve source nodes through registry namespaces and quarantine ambiguity."""

    input_id = require_sha256(input_id, label="input_id")
    image_digest = require_sha256(image_digest, label="image_digest")
    config_digest = require_sha256(config_digest, label="config_digest")
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
        "community_entity_type_assertion",
        "community_identifier_assertion",
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
    namespace_configs = namespace_schemes.select(
        "namespace_id",
        "referent_kind",
        "case_sensitive",
        "match_pattern",
    ).dropDuplicates()

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
        .localCheckpoint(eager=True)
    )

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
            namespace_configs.alias("n"),
            (
                (F.col("i.namespace_id") == F.col("n.namespace_id"))
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
    existing_known = (
        _empty_known_index(spark)
        if existing_index is None
        else existing_index.where(F.to_timestamp("observed_at") <= as_of).select(
            "namespace_id",
            "normalized_value",
            "referent_kind",
            "entity_key",
        )
    )
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

    candidate_rows = registered_identifiers.alias("i").join(
        known_index.alias("k"),
        (
            (F.col("i.namespace_id") == F.col("k.namespace_id"))
            & (F.col("i.normalized_value") == F.col("k.normalized_value"))
            & (F.col("i.referent_kind") == F.col("k.referent_kind"))
        ),
        "inner",
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

    candidate_count_rows = candidate_rows.groupBy(
        F.col("i.subject_namespace_id").alias("subject_namespace_id"),
        F.col("i.subject_source_id").alias("subject_source_id"),
        F.col("i.subject_referent_kind").alias("subject_referent_kind"),
    ).agg(F.countDistinct("k.entity_key").alias("node_candidate_count"))
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
            <= F.lit(MAX_EXACT_BLOCKING_NODE_CANDIDATE_KEYS)
        )
        oversized_node_keys = node_candidate_counts.where(
            F.col("node_candidate_count")
            > F.lit(MAX_EXACT_BLOCKING_NODE_CANDIDATE_KEYS)
        )
        work_parts: list[Any] = []
        if oversized_node_keys.take(1):
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
                .withColumn("component_node_count", F.lit(1))
                .withColumn("component_candidate_count", F.lit(0))
                .withColumn("component_candidate_keys", F.array())
                .withColumn("resolution_mode", F.lit("CONFLICT_NODE_CANDIDATES"))
            )
            work_parts.append(oversized_work)
        if bounded_node_keys.take(1):
            candidates = (
                candidate_rows.join(
                    bounded_node_keys,
                    [
                        candidate_rows["i.subject_namespace_id"]
                        == bounded_node_keys["subject_namespace_id"],
                        candidate_rows["i.subject_source_id"]
                        == bounded_node_keys["subject_source_id"],
                        candidate_rows["i.subject_referent_kind"]
                        == bounded_node_keys["subject_referent_kind"],
                    ],
                    "inner",
                )
                .groupBy(
                    "i.subject_namespace_id",
                    "i.subject_source_id",
                    "i.subject_referent_kind",
                )
                .agg(
                    F.sort_array(F.collect_set("k.entity_key")).alias(
                        "candidate_entity_keys"
                    ),
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
            )
            bounded_work = bounded_work.join(component_labels, "node_id")
            component_counts = bounded_work.groupBy("component_id").agg(
                F.count("*").alias("component_node_count"),
            )
            bounded_work = bounded_work.join(component_counts, "component_id")
            component_eligible = bounded_work.where(
                F.col("component_node_count")
                <= F.lit(MAX_EXACT_BLOCKING_RESOLUTION_COMPONENT_SIZE)
            )
            component_distinct_candidates = (
                component_eligible.select(
                    "component_id",
                    F.explode("candidate_entity_keys").alias("candidate_entity_key"),
                )
                .where(F.col("candidate_entity_key").isNotNull())
                .distinct()
            )
            component_candidate_counts = (
                component_distinct_candidates.groupBy("component_id").agg(
                    F.count("candidate_entity_key").alias("component_candidate_count")
                )
            ).persist()
            try:
                bounded_component_candidates = component_candidate_counts.where(
                    F.col("component_candidate_count")
                    <= F.lit(MAX_EXACT_BLOCKING_COMPONENT_CANDIDATE_KEYS)
                )
                candidate_stats = (
                    component_distinct_candidates.join(
                        bounded_component_candidates,
                        "component_id",
                        "inner",
                    )
                    .groupBy("component_id")
                    .agg(
                        F.sort_array(F.collect_set("candidate_entity_key")).alias(
                            "component_candidate_keys"
                        ),
                    )
                )
                bounded_work = (
                    bounded_work.join(
                        component_candidate_counts, "component_id", "left"
                    )
                    .join(candidate_stats, "component_id", "left")
                    .withColumn(
                        "component_candidate_count",
                        F.coalesce(F.col("component_candidate_count"), F.lit(0)),
                    )
                    .withColumn(
                        "component_candidate_keys",
                        F.coalesce("component_candidate_keys", F.array()),
                    )
                )
            finally:
                component_candidate_counts.unpersist()
            bounded_work = bounded_work.withColumn(
                "resolution_mode",
                F.when(
                    F.col("component_node_count")
                    > F.lit(MAX_EXACT_BLOCKING_RESOLUTION_COMPONENT_SIZE),
                    F.lit("CONFLICT_OVERSIZED"),
                )
                .when(
                    F.col("component_candidate_count")
                    > F.lit(MAX_EXACT_BLOCKING_COMPONENT_CANDIDATE_KEYS),
                    F.lit("CONFLICT_COMPONENT_CANDIDATES"),
                )
                .when(F.size("component_candidate_keys") > 1, F.lit("CONFLICT_MULTI"))
                .when(F.size("component_candidate_keys") == 1, F.lit("ACCEPT"))
                .otherwise(F.lit("BOOTSTRAP")),
            )
            work_parts.append(bounded_work)
        if not work_parts:
            work = _resolution_work_columns(
                unassigned.limit(0).select(
                    *unassigned.columns,
                    empty_candidate_keys.alias("candidate_entity_keys"),
                    F.lit(0).cast("long").alias("node_candidate_count"),
                )
            )
            work = (
                work.withColumn("node_id", node_id_expr)
                .withColumn("component_id", node_id_expr)
                .withColumn("component_node_count", F.lit(0).cast("long"))
                .withColumn("component_candidate_count", F.lit(0).cast("long"))
                .withColumn("component_candidate_keys", empty_candidate_keys)
                .withColumn("resolution_mode", F.lit("BOOTSTRAP"))
            )
        elif len(work_parts) == 1:
            work = work_parts[0]
        else:
            work = work_parts[0].unionByName(work_parts[1])
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
                max_component_size=MAX_EXACT_BLOCKING_RESOLUTION_COMPONENT_SIZE,
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
                                MAX_EXACT_BLOCKING_NODE_CANDIDATE_KEYS
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
                                MAX_EXACT_BLOCKING_COMPONENT_CANDIDATE_KEYS
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

    resolution_results = work.rdd.map(resolve_row)
    if revocations.rdd.isEmpty():
        revocation_results = spark.sparkContext.emptyRDD()
    else:
        revocation_results = revocations.rdd.map(_revoke_inactive_membership)
    if mapping_conflicts.rdd.isEmpty():
        mapping_conflict_results = spark.sparkContext.emptyRDD()
    else:
        mapping_conflict_results = mapping_conflicts.rdd.map(_inactive_mapping_conflict)
    revocation_results = revocation_results.union(mapping_conflict_results)
    results = resolution_results.union(
        revocation_results.map(lambda item: (item, ()))
    ).persist()
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
        .persist()
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
        unassigned.unpersist()
        registered_identifiers.unpersist()
        type_groups.unpersist()
        latest_source_record_states.unpersist()
        bound_source_records.unpersist()
