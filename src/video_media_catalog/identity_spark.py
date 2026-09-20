"""Distributed registry-driven exact identity resolution."""

from __future__ import annotations

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
    resolve_or_allocate_source_node,
)
from video_media_catalog.identity_v2 import (
    EntityLevel,
    ExternalIdIndexEntry,
    build_external_id_index_entry,
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


def canonical_referent_kind(value: str) -> str:
    """Normalize source-specific kinds to exact-ID blocking domains."""

    normalized = value.strip().upper()
    return _REFERENT_KIND_ALIASES.get(normalized, normalized)


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

    from pyspark.sql import Window
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
    referent_aliases = spark.createDataFrame(
        sorted(
            {(alias, canonical) for alias, canonical in _REFERENT_KIND_ALIASES.items()}
        ),
        "referent_kind_alias STRING, referent_kind STRING",
    )

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
    )
    membership_versions = (
        visible_silver["community_entity_membership"]
        .withColumn("_membership_version", F.row_number().over(membership_window))
        .where(F.col("_membership_version") == 1)
        .drop("_membership_version")
    )
    memberships = membership_versions.where(
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

    identifier_assertions = visible_silver["community_identifier_assertion"].where(
        F.col("status") == "ACTIVE"
    )
    registered_identifiers = (
        identifier_assertions.alias("i")
        .join(
            referent_aliases.alias("rk"),
            F.upper(F.trim(F.col("i.referent_kind")))
            == F.col("rk.referent_kind_alias"),
            "inner",
        )
        .join(
            namespace_configs.alias("n"),
            (
                (F.col("i.namespace_id") == F.col("n.namespace_id"))
                & (F.col("rk.referent_kind") == F.col("n.referent_kind"))
            ),
            "inner",
        )
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
            (
                (
                    F.upper(F.element_at(F.col("t.entity_types"), 1))
                    == F.col("tc.source_entity_type")
                )
                & (F.col("tc.referent_kind") == F.col("n.referent_kind"))
            ),
            "inner",
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
    candidates = (
        candidate_rows.groupBy(
            "i.subject_namespace_id",
            "i.subject_source_id",
            "i.subject_referent_kind",
        )
        .agg(
            F.sort_array(F.collect_set("k.entity_key")).alias("candidate_entity_keys"),
        )
        .select(
            F.col("subject_namespace_id"),
            F.col("subject_source_id"),
            F.col("subject_referent_kind"),
            F.col("candidate_entity_keys"),
        )
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
    source_columns = [
        "subject_namespace_id",
        "subject_source_id",
        "subject_referent_kind",
    ]
    work = (
        unassigned.join(candidates, source_columns, "left")
        .join(identifier_groups, source_columns, "left")
        .select(
            *unassigned.columns,
            F.coalesce(
                "candidate_entity_keys",
                F.from_json(F.lit("[]"), "array<string>"),
            ).alias("candidate_entity_keys"),
            F.coalesce(
                "identifier_assertion_ids",
                F.from_json(F.lit("[]"), "array<string>"),
            ).alias("identifier_assertion_ids"),
            F.coalesce(
                "identifier_specs",
                F.from_json(
                    F.lit("[]"),
                    (
                        "array<struct<namespace_id:string,normalized_value:string,"
                        "referent_kind:string,assertion_id:string,"
                        "identifier_policy_id:string,"
                        "identifier_policy_digest:string>>"
                    ),
                ),
            ).alias("identifier_specs"),
        )
    )

    def resolve(
        row: Any,
    ) -> tuple[IdentityResolutionResult, tuple[ExternalIdIndexEntry, ...]]:
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
        result = resolve_or_allocate_source_node(
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
            decision_policy_version="exact-identity-v2",
            decided_by="community-identity-spark-v2",
            materialization_id=materialization_id,
        )
        if result.conflicts:
            return result, ()
        entity_key = result.memberships[0].entity_key
        entries = tuple(
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
        return result, entries

    results = work.rdd.map(resolve).persist()
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
        type_groups.unpersist()
