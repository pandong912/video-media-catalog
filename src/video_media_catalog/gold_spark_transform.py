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
from video_media_catalog.community_ingest import CommunityIngestRun
from video_media_catalog.community_release import ReleasePolicyContext
from video_media_catalog.gold import (
    GoldAssertionLineage,
    GoldReleasePlan,
    GoldResolutionPolicy,
    GoldResolutionStatus,
    GoldRightsLineage,
    PredicateKind,
    ResolutionOperator,
    build_gold_conflict,
    build_gold_entity,
    build_gold_field,
    build_gold_identifier,
    build_gold_relation,
    build_gold_release_plan,
    trace_with_assertion_lineage,
)
from video_media_catalog.gold_freshness import (
    ReleaseFreshnessPolicy,
    build_release_freshness_matrix,
    disabled_release_freshness_policy,
)
from video_media_catalog.gold_quality import (
    GoldBuildMode,
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
from video_media_catalog.rights import RightsTerminationFence
from video_media_catalog.source_lifecycle import (
    current_upsert_envelope_keys,
    select_effective_membership_versions,
)
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
        select_effective_membership_versions(
            silver["community_entity_membership"],
            as_of=as_of,
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
            "run_id",
            "source_product_id",
            F.col("policy_id").alias("record_policy_id"),
            F.col("policy_digest").alias("record_policy_digest"),
        )
        .join(source_products, "source_product_id", "inner")
        .select(
            "envelope_key",
            "run_id",
            "record_policy_id",
            "record_policy_digest",
            "source_product_policy_id",
        )
    )
    bound = active.alias("a").join(
        source_metadata.alias("s"),
        ["envelope_key", "run_id"],
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


def _eligible_assertions(
    frame: Any,
    *,
    source_records: Any,
    current_source_envelope_keys: Any,
    source_products: Any,
    memberships: Any,
    rights: Any,
    context: ReleasePolicyContext,
    requested_actions: tuple[Any, ...] = (),
    termination_fences: tuple[RightsTerminationFence, ...] = (),
):
    from pyspark.sql import functions as F

    source_metadata = source_records.select(
        "envelope_key",
        "run_id",
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
            "run_id",
            "source_product_id",
            "source_name",
            "source_documentation_url",
            "source_record_id",
            "citation_keys_json",
            "record_policy_id",
            "record_policy_digest",
            "source_product_policy_id",
        ).alias("s"),
        ["envelope_key", "run_id"],
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

    active_fences = tuple(
        fence
        for fence in termination_fences
        if parse_rfc3339(fence.effective_at) <= parse_rfc3339(context.as_of)
        and set(fence.blocked_actions) & set(requested_actions)
    )
    fenced = F.lit(False)
    for fence in active_fences:
        source_match = (F.col("a.source_product_id") == fence.source_product_id) & (
            F.col("a.policy_id") == fence.policy_id
        )
        if (
            bound.alias("a")
            .where(
                source_match & (F.col("a.record_policy_digest") != fence.policy_digest)
            )
            .limit(1)
            .count()
        ):
            active.unpersist()
            raise ValueError("termination fence policy digest differs from registry")
        fenced = fenced | source_match
    rights_candidates = bound.alias("a").where(~fenced)

    known = rights_candidates.alias("a").join(
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
    current_join = ["envelope_key"]
    if "run_id" in current_source_envelope_keys.columns:
        current_join.append("run_id")
    with_source = rights_eligible.join(
        current_source_envelope_keys,
        current_join,
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


def _field_rule_udf(
    policy: GoldResolutionPolicy,
    assertion_kind: PredicateKind = PredicateKind.FIELD,
):
    from pyspark.sql import functions as F
    from pyspark.sql.types import BooleanType, StringType, StructField, StructType

    rules = {
        rule.predicate: (
            rule.operator.value,
            rule.scope_qualifiers,
            rule.source_priority,
            rule.rights_first,
        )
        for rule in policy.rules
        if rule.assertion_kind == assertion_kind
    }
    default = policy.default_operator.value

    def resolve_rule(predicate: str, qualifiers_json: str):
        operator, keys, source_priority, rights_first = rules.get(
            predicate,
            (default, (), (), True),
        )
        qualifiers = json.loads(qualifiers_json)
        scope = {key: qualifiers.get(key) for key in keys}
        return (
            operator,
            canonical_json(scope),
            canonical_json(source_priority),
            rights_first,
        )

    return F.udf(
        resolve_rule,
        StructType(
            [
                StructField("operator", StringType(), False),
                StructField("scope_json", StringType(), False),
                StructField("source_priority_json", StringType(), False),
                StructField("rights_first", BooleanType(), False),
            ]
        ),
    )


def _resolve_field_group(item):
    (
        (
            entity_key,
            predicate,
            scope_json,
            operator,
            source_priority_json,
        ),
        raw_values,
    ) = item
    by_value: dict[tuple[str, str], list[tuple[str, str, str]]] = defaultdict(list)
    values = list(raw_values)
    for value_type, value_json, assertion_id, lineage_json, source_product_id in values:
        by_value[(value_type, value_json)].append(
            (assertion_id, lineage_json, source_product_id)
        )
    ordered = sorted(by_value)
    all_ids = tuple(
        sorted(
            assertion_id
            for assertions in by_value.values()
            for assertion_id, _, _ in assertions
        )
    )
    all_lineage = _normalize_lineage(
        lineage_json
        for assertions in by_value.values()
        for _, lineage_json, _ in assertions
    )
    scope = json.loads(scope_json)
    source_priority = tuple(json.loads(source_priority_json))
    trace = {
        "operator": operator,
        "rightsFirst": True,
        "sourcePriority": source_priority,
    }
    if operator == ResolutionOperator.SINGLE.value:
        ranks = {source: rank for rank, source in enumerate(source_priority)}
        selected_rank = min(
            (
                ranks.get(source_product_id, len(source_priority))
                for *_, source_product_id in values
            ),
            default=len(source_priority),
        )
        selected_values = [
            value
            for value, assertions in by_value.items()
            if any(
                ranks.get(source_product_id, len(source_priority)) == selected_rank
                for _, _, source_product_id in assertions
            )
        ]
        trace["selectedSourceRank"] = selected_rank
        if len(selected_values) == 1:
            value_type, value_json = selected_values[0]
            selected_ids = tuple(
                sorted(
                    assertion_id
                    for assertion_id, _, source_product_id in by_value[
                        selected_values[0]
                    ]
                    if ranks.get(source_product_id, len(source_priority))
                    == selected_rank
                )
            )
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
                        selected_assertion_id=min(selected_ids),
                        trace=trace,
                        lineage=all_lineage,
                    ),
                )
            ]
        candidates = [
            json.loads(value_json) for _, value_json in sorted(selected_values)
        ]
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
        ids = tuple(sorted(assertion_id for assertion_id, _, _ in supporting))
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
                        lineage_json for _, lineage_json, _ in supporting
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


def _resolve_relation_group(item: Any):
    (
        (
            subject,
            predicate,
            scope_json,
            operator,
            source_priority_json,
        ),
        raw_values,
    ) = item
    values = list(raw_values)
    scope = json.loads(scope_json)
    source_priority = tuple(json.loads(source_priority_json))
    ranks = {source: rank for rank, source in enumerate(source_priority)}
    all_ids = tuple(sorted({value[1] for value in values}))
    all_lineage = _normalize_lineage(value[2] for value in values)
    trace = {
        "operator": operator,
        "rightsFirst": True,
        "sourcePriority": source_priority,
    }
    if operator == ResolutionOperator.SINGLE.value:
        selected_rank = min(
            (
                ranks.get(source_product_id, len(source_priority))
                for _, _, _, source_product_id in values
            ),
            default=len(source_priority),
        )
        preferred = [
            value
            for value in values
            if ranks.get(value[3], len(source_priority)) == selected_rank
        ]
        object_keys = sorted({value[0] for value in preferred})
        trace["selectedSourceRank"] = selected_rank
        if len(object_keys) == 1:
            return [
                (
                    "relation",
                    RelationDraft(
                        subject_entity_key=subject,
                        predicate=predicate,
                        object_entity_key=object_keys[0],
                        qualifiers=scope,
                        assertion_ids=all_ids,
                        trace=trace,
                        lineage=all_lineage,
                    ),
                )
            ]
        return [
            (
                "conflict",
                ConflictDraft(
                    entity_key=subject,
                    predicate=predicate,
                    qualifiers=scope,
                    reason="MULTIPLE_ELIGIBLE_RELATION_TARGETS",
                    assertion_ids=all_ids,
                    candidate_values=object_keys,
                    trace=trace,
                    lineage=all_lineage,
                ),
            )
        ]

    by_object: dict[str, list[tuple[str, str, str]]] = defaultdict(list)
    for object_key, assertion_id, lineage_json, source_product_id in values:
        by_object[object_key].append((assertion_id, lineage_json, source_product_id))
    return [
        (
            "relation",
            RelationDraft(
                subject_entity_key=subject,
                predicate=predicate,
                object_entity_key=object_key,
                qualifiers=scope,
                assertion_ids=tuple(
                    sorted(assertion_id for assertion_id, _, _ in supporting)
                ),
                trace=trace,
                lineage=_normalize_lineage(
                    lineage_json for _, lineage_json, _ in supporting
                ),
            ),
        )
        for object_key, supporting in sorted(by_object.items())
    ]


def build_distributed_gold(
    spark: Any,
    *,
    visible_silver: dict[str, Any],
    registry: SourceRegistrySnapshot,
    policy_context: ReleasePolicyContext,
    field_policy: GoldResolutionPolicy,
    committed_run_ids: tuple[str, ...] = (),
    committed_runs: Any | None = None,
    silver_epoch_id: str | None = None,
    committed_run_count: int | None = None,
    committed_run_digest: str | None = None,
    silver_snapshot_ids: dict[str, int | None],
    identity_snapshot_ids: dict[str, int | None],
    resolver_digest: str,
    image_digest: str,
    config_digest: str,
    planned_at: str,
    max_redirect_hops: int = 16,
    freshness_policy: ReleaseFreshnessPolicy | None = None,
    build_mode: GoldBuildMode = GoldBuildMode.RELEASE,
    termination_fences: tuple[RightsTerminationFence, ...] = (),
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
    if len({fence.fence_id for fence in termination_fences}) != len(termination_fences):
        raise ValueError("Gold build contains duplicate termination fences")
    source_products_by_id = {
        product.source_product_id: product for product in registry.source_products
    }
    rights_profiles_by_id = {
        profile.policy_id: profile for profile in registry.rights_profiles
    }
    for fence in termination_fences:
        product = source_products_by_id.get(fence.source_product_id)
        profile = rights_profiles_by_id.get(fence.policy_id)
        if (
            product is None
            or profile is None
            or product.policy_id != fence.policy_id
            or profile.digest != fence.policy_digest
        ):
            raise ValueError("termination fence is not bound to the source registry")
    from pyspark.sql import functions as F

    if committed_runs is None:
        if not committed_run_ids:
            raise ValueError("Gold build requires committed runs")
        if any(
            value is not None
            for value in (
                silver_epoch_id,
                committed_run_count,
                committed_run_digest,
            )
        ):
            raise ValueError("legacy Gold input cannot declare an epoch summary")
        selected_runs = spark.createDataFrame(
            [(run_id,) for run_id in committed_run_ids],
            "run_id STRING",
        )
    else:
        if committed_run_ids:
            raise ValueError("Gold build cannot mix run IDs and a run dataframe")
        if "run_id" not in committed_runs.columns:
            raise ValueError("committed_runs dataframe requires run_id")
        if any(
            value is None
            for value in (
                silver_epoch_id,
                committed_run_count,
                committed_run_digest,
            )
        ):
            raise ValueError("distributed committed runs require an epoch summary")
        selected_runs = committed_runs.select("run_id").dropDuplicates(["run_id"])
    committed_silver = {
        table: (
            frame.join(selected_runs, "run_id", "inner")
            if "run_id" in frame.columns
            else frame
        )
        for table, frame in visible_silver.items()
    }
    current_source_envelope_keys = current_upsert_envelope_keys(
        source_records=committed_silver["community_source_record"],
        ingest_runs=committed_silver["community_ingest_run"],
        committed_runs=selected_runs,
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
            requested_actions=field_policy.requested_actions,
            termination_fences=termination_fences,
        )
        resolved_identifiers, id_withheld, id_unresolved = _eligible_assertions(
            committed_silver["community_identifier_assertion"],
            source_records=committed_silver["community_source_record"],
            current_source_envelope_keys=current_source_envelope_keys,
            source_products=source_products,
            memberships=memberships,
            rights=rights,
            context=policy_context,
            requested_actions=field_policy.requested_actions,
            termination_fences=termination_fences,
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
            .withColumn(
                "source_priority_json",
                F.col("_rule.source_priority_json"),
            )
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
                        row["source_priority_json"],
                    ),
                    (
                        row["value_type"],
                        row["value_json"],
                        row["assertion_id"],
                        _lineage_json(row),
                        row["source_product_id"],
                    ),
                )
            )
            .groupByKey()
            .flatMap(_resolve_field_group)
            .persist()
        )

        identifier_rule_udf = _field_rule_udf(
            field_policy,
            PredicateKind.IDENTIFIER,
        )
        ruled_identifiers = (
            resolved_identifiers.withColumn(
                "_rule",
                identifier_rule_udf("namespace_id", F.lit("{}")),
            )
            .withColumn("resolution_operator", F.col("_rule.operator"))
            .drop("_rule")
            .persist()
        )
        intermediates.append(ruled_identifiers)
        identifier_never_count = ruled_identifiers.where(
            F.col("resolution_operator") == ResolutionOperator.NEVER_RESOLVE.value
        ).count()
        resolvable_identifiers = ruled_identifiers.where(
            F.col("resolution_operator") != ResolutionOperator.NEVER_RESOLVE.value
        )
        duplicate_external_id_count = (
            resolvable_identifiers.groupBy(
                "namespace_id",
                "value",
                "referent_kind",
            )
            .agg(F.countDistinct("resolved_entity_key").alias("entity_count"))
            .where(F.col("entity_count") > 1)
            .count()
        )
        identifier_drafts = (
            resolvable_identifiers.rdd.map(
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
                requested_actions=field_policy.requested_actions,
                termination_fences=termination_fences,
            )
        )
        intermediates.append(resolved_relation_subjects)
        relation_rule_udf = _field_rule_udf(
            field_policy,
            PredicateKind.RELATIONSHIP,
        )
        ruled_relation_subjects = (
            resolved_relation_subjects.withColumn(
                "_rule",
                relation_rule_udf("predicate", "qualifiers_json"),
            )
            .withColumn("resolution_operator", F.col("_rule.operator"))
            .withColumn("scope_json", F.col("_rule.scope_json"))
            .withColumn(
                "source_priority_json",
                F.col("_rule.source_priority_json"),
            )
            .drop("_rule")
            .persist()
        )
        intermediates.append(ruled_relation_subjects)
        relation_never_count = ruled_relation_subjects.where(
            F.col("resolution_operator") == ResolutionOperator.NEVER_RESOLVE.value
        ).count()
        resolvable_relation_subjects = ruled_relation_subjects.where(
            F.col("resolution_operator") != ResolutionOperator.NEVER_RESOLVE.value
        )
        resolved_relations = (
            resolvable_relation_subjects.alias("r")
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
            resolvable_relation_subjects.count() - resolved_relations.count()
        )
        relation_drafts = (
            resolved_relations.rdd.map(
                lambda row: (
                    (
                        row["resolved_entity_key"],
                        row["predicate"],
                        row["scope_json"],
                        row["resolution_operator"],
                        row["source_priority_json"],
                    ),
                    (
                        row["object_resolved_entity_key"],
                        row["assertion_id"],
                        _lineage_json(row),
                        row["source_product_id"],
                    ),
                )
            )
            .groupByKey()
            .flatMap(_resolve_relation_group)
            .persist()
        )

        used_entity_keys = (
            field_drafts.map(lambda item: (item[1].entity_key,))
            .union(identifier_drafts.map(lambda item: (item.entity_key,)))
            .union(
                relation_drafts.filter(lambda item: item[0] == "relation").flatMap(
                    lambda item: (
                        (item[1].subject_entity_key,),
                        (item[1].object_entity_key,),
                    )
                )
            )
            .union(
                relation_drafts.filter(lambda item: item[0] == "conflict").map(
                    lambda item: (item[1].entity_key,)
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

        parent_relations = spark.createDataFrame(
            relation_drafts.filter(lambda item: item[0] == "relation").map(
                lambda item: (
                    item[1].subject_entity_key,
                    item[1].predicate,
                )
            ),
            "entity_key STRING, predicate STRING",
        ).dropDuplicates(["entity_key", "predicate"])
        orphan_episode_count = (
            entity_summary.where(F.col("entity_level") == "EPISODE")
            .join(
                parent_relations.where(
                    F.col("predicate").isin(
                        "part_of",
                        "part_of_season",
                        "part_of_series",
                        "season",
                    )
                ).select("entity_key"),
                "entity_key",
                "left_anti",
            )
            .count()
        )
        orphan_season_count = (
            entity_summary.where(F.col("entity_level") == "SEASON")
            .join(
                parent_relations.where(
                    F.col("predicate").isin("part_of", "part_of_series")
                ).select("entity_key"),
                "entity_key",
                "left_anti",
            )
            .count()
        )

        policy_usage = (
            resolvable_fields.select(
                "source_product_id",
                "policy_id",
                "assertion_id",
            )
            .unionByName(
                resolvable_identifiers.select(
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
        field_conflict_count = field_drafts.filter(
            lambda item: item[0] == "conflict"
        ).count()
        relation_conflict_count = relation_drafts.filter(
            lambda item: item[0] == "conflict"
        ).count()
        conflict_count = field_conflict_count + relation_conflict_count
        identifier_count = identifier_drafts.count()
        relation_count = relation_drafts.filter(
            lambda item: item[0] == "relation"
        ).count()
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
            silver_epoch_id=silver_epoch_id,
            committed_run_count=committed_run_count,
            committed_run_digest=committed_run_digest,
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
                        "resolver": "community-gold-spark-v3",
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
        conflict_rows = (
            field_drafts.filter(lambda item: item[0] == "conflict")
            .map(
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
            .union(
                relation_drafts.filter(lambda item: item[0] == "conflict").map(
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
        relation_rows = relation_drafts.filter(lambda item: item[0] == "relation").map(
            lambda item: gold_relation_row(
                build_gold_relation(
                    release_plan_id=plan.release_plan_id,
                    subject_entity_key=item[1].subject_entity_key,
                    predicate=item[1].predicate,
                    object_entity_key=item[1].object_entity_key,
                    qualifiers=item[1].qualifiers,
                    assertion_ids=item[1].assertion_ids,
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
        selected_ingest_runs = tuple(
            CommunityIngestRun.model_validate_json(row["manifest_json"])
            for row in committed_silver["community_ingest_run"]
            .select("manifest_json")
            .collect()
        )
        release_freshness = build_release_freshness_matrix(
            ingest_runs=selected_ingest_runs,
            policy=(
                freshness_policy
                if freshness_policy is not None
                else disabled_release_freshness_policy()
            ),
            as_of=policy_context.as_of,
        )
        quality = build_gold_quality_report_from_metrics(
            plan=plan,
            policy=field_policy,
            table_counts=table_counts,
            conflict_count=conflict_count,
            field_count=field_count,
            withheld_assertion_count=(
                field_withheld
                + id_withheld
                + rel_withheld
                + never_count
                + identifier_never_count
                + relation_never_count
            ),
            unresolved_identity_count=(
                field_unresolved
                + id_unresolved
                + rel_subject_unresolved
                + rel_object_unresolved
            ),
            entity_count=entity_count,
            eligible_policy_counts=dict(eligible_policy_counts),
            attribution_counts=dict(eligible_policy_counts),
            orphan_episode_count=orphan_episode_count,
            orphan_season_count=orphan_season_count,
            duplicate_external_id_count=duplicate_external_id_count,
            release_freshness=release_freshness,
            build_mode=build_mode,
            resolution_count=(field_count + relation_count + relation_conflict_count),
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
