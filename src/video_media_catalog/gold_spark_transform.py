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
    GoldReleasePlan,
    GoldResolutionPolicy,
    GoldResolutionStatus,
    ResolutionOperator,
    build_gold_conflict,
    build_gold_entity,
    build_gold_field,
    build_gold_identifier,
    build_gold_relation,
    build_gold_release_plan,
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
    from pyspark.sql import functions as F

    if max_redirect_hops < 1:
        raise ValueError("max_redirect_hops must be positive")
    as_of_timestamp = F.to_timestamp(F.lit(as_of))
    memberships = (
        silver["community_entity_membership"]
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
            )
        )
    return spark.createDataFrame(
        rows,
        """
        rights_policy_id STRING,
        rights_policy_digest STRING,
        rights_zone STRING,
        statically_allowed BOOLEAN,
        max_cache_age_days LONG
        """,
    )


def _eligible_assertions(
    frame: Any,
    *,
    source_records: Any,
    memberships: Any,
    rights: Any,
    context: ReleasePolicyContext,
):
    from pyspark.sql import functions as F

    active = frame.where(F.col("status") == "ACTIVE").persist()
    known = active.alias("a").join(
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
        .withColumn(
            "envelope_key",
            F.get_json_object("a.provenance_json", "$.envelopeKey"),
        )
        .persist()
    )
    active_count = active.count()
    rights_count = rights_eligible.count()
    withheld = active_count - rights_count

    with_source = rights_eligible.alias("a").join(
        source_records.select(
            "envelope_key",
            "source_product_id",
            F.col("policy_id").alias("record_policy_id"),
            F.col("policy_digest").alias("record_policy_digest"),
        ).alias("s"),
        "envelope_key",
        "inner",
    )
    if with_source.count() != rights_count:
        rights_eligible.unpersist()
        active.unpersist()
        raise ValueError("assertion provenance cannot resolve source record")
    if (
        with_source.where(
            (F.col("a.policy_id") != F.col("s.record_policy_id"))
            | (F.col("a.policy_digest") != F.col("s.record_policy_digest"))
        )
        .limit(1)
        .count()
    ):
        rights_eligible.unpersist()
        active.unpersist()
        raise ValueError("assertion and source record policies differ")

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
    unresolved = rights_count - resolved_count
    rights_eligible.unpersist()
    active.unpersist()
    return resolved, withheld, unresolved


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
    by_value: dict[tuple[str, str], list[str]] = defaultdict(list)
    for value_type, value_json, assertion_id in raw_values:
        by_value[(value_type, value_json)].append(assertion_id)
    ordered = sorted(by_value)
    all_ids = tuple(
        sorted(
            assertion_id
            for assertions in by_value.values()
            for assertion_id in assertions
        )
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
                ),
            ),
        ]
    result = []
    for value_type, value_json in ordered:
        ids = tuple(sorted(by_value[(value_type, value_json)]))
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
                ),
            )
        )
    return result


def build_distributed_gold(
    spark: Any,
    *,
    visible_silver: dict[str, Any],
    registry: SourceRegistrySnapshot,
    policy_context: ReleasePolicyContext,
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
        "community_source_record",
        "community_field_assertion",
        "community_identifier_assertion",
        "community_relationship_assertion",
        "community_entity_ledger",
        "community_entity_membership",
        "community_entity_redirect",
    }
    if not required.issubset(visible_silver):
        raise ValueError("Gold build is missing required Silver tables")
    from pyspark.sql import functions as F

    memberships = _resolved_memberships(
        silver=visible_silver,
        as_of=policy_context.as_of,
        max_redirect_hops=max_redirect_hops,
    )
    rights = _rights_frame(
        spark,
        registry=registry,
        context=policy_context,
        policy=field_policy,
    )
    intermediates = [memberships]
    field_drafts = None
    identifier_drafts = None
    relation_drafts = None
    try:
        resolved_fields, field_withheld, field_unresolved = _eligible_assertions(
            visible_silver["community_field_assertion"],
            source_records=visible_silver["community_source_record"],
            memberships=memberships,
            rights=rights,
            context=policy_context,
        )
        resolved_identifiers, id_withheld, id_unresolved = _eligible_assertions(
            visible_silver["community_identifier_assertion"],
            source_records=visible_silver["community_source_record"],
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
                    row["assertion_id"],
                )
            )
            .groupByKey()
            .map(
                lambda item: IdentifierDraft(
                    entity_key=item[0][0],
                    namespace_id=item[0][1],
                    value=item[0][2],
                    issuer=item[0][3],
                    referent_kind=item[0][4],
                    assertion_ids=tuple(sorted(set(item[1]))),
                    trace={
                        "policyId": field_policy.policy_id,
                        "policyVersion": field_policy.policy_version,
                    },
                )
            )
            .persist()
        )

        resolved_relation_subjects, rel_withheld, rel_subject_unresolved = (
            _eligible_assertions(
                visible_silver["community_relationship_assertion"],
                source_records=visible_silver["community_source_record"],
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
                    row["assertion_id"],
                )
            )
            .groupByKey()
            .map(
                lambda item: RelationDraft(
                    subject_entity_key=item[0][0],
                    predicate=item[0][1],
                    object_entity_key=item[0][2],
                    qualifiers=json.loads(item[0][3]),
                    assertion_ids=tuple(sorted(set(item[1]))),
                    trace={
                        "policyId": field_policy.policy_id,
                        "policyVersion": field_policy.policy_version,
                    },
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
                        "resolver": "community-gold-spark-v1",
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
                    trace={
                        **item[1].trace,
                        "policyId": field_policy.policy_id,
                        "policyVersion": field_policy.policy_version,
                    },
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
                    trace={
                        **item[1].trace,
                        "policyId": field_policy.policy_id,
                        "policyVersion": field_policy.policy_version,
                    },
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
                    trace=item.trace,
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
                    trace=item.trace,
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
