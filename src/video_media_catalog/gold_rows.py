"""Row projections for policy-specific Gold v2 tables."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from video_media_catalog.canonical import canonical_json
from video_media_catalog.gold import (
    GoldConflict,
    GoldEntity,
    GoldField,
    GoldIdentifier,
    GoldRelation,
    GoldReleasePlan,
)
from video_media_catalog.gold_ingest import GoldReleaseCommit
from video_media_catalog.gold_tables import (
    GOLD_DATA_COLUMNS,
    GOLD_TABLE_KEYS,
)


def _model_json(value) -> str:
    return canonical_json(
        value.model_dump(mode="json", by_alias=True, exclude_none=True)
    )


def gold_release_plan_row(plan: GoldReleasePlan) -> dict[str, Any]:
    return {
        "release_plan_id": plan.release_plan_id,
        "context_id": plan.policy_context.context_id,
        "as_of": plan.policy_context.as_of,
        "rights_registry_digest": plan.rights_registry_digest,
        "field_policy_digest": plan.field_policy_digest,
        "resolver_digest": plan.resolver_digest,
        "image_digest": plan.image_digest,
        "config_digest": plan.config_digest,
        "expected_counts_json": canonical_json(plan.expected_counts),
        "plan_json": _model_json(plan),
    }


def gold_release_commit_row(commit: GoldReleaseCommit) -> dict[str, Any]:
    return {
        "commit_key": commit.commit_key,
        "release_plan_id": commit.release_plan_id,
        "committed_at": commit.committed_at,
        "table_counts_json": canonical_json(commit.table_counts),
        "table_snapshot_ids_json": canonical_json(commit.table_snapshot_ids),
        "quality_report_json": _model_json(commit.quality_report),
        "attribution_manifest_json": _model_json(commit.attribution_manifest),
        "commit_json": _model_json(commit),
    }


def gold_entity_row(value: GoldEntity) -> dict[str, Any]:
    return {
        "row_key": value.row_key,
        "release_plan_id": value.release_plan_id,
        "entity_key": value.entity_key,
        "entity_level": value.entity_level,
        "entity_kind": value.entity_kind,
        "status": value.status,
        "source_node_count": value.source_node_count,
        "trace_json": value.trace_json,
    }


def gold_field_row(value: GoldField) -> dict[str, Any]:
    return {
        "resolution_key": value.resolution_key,
        "release_plan_id": value.release_plan_id,
        "entity_key": value.entity_key,
        "predicate": value.predicate,
        "scope_hash": value.scope_hash,
        "value_type": value.value_type,
        "value_json": value.value_json,
        "qualifiers_json": value.qualifiers_json,
        "resolution_status": value.resolution_status.value,
        "selected_assertion_id": value.selected_assertion_id,
        "assertion_ids_json": canonical_json(value.assertion_ids),
        "trace_json": value.trace_json,
    }


def gold_identifier_row(value: GoldIdentifier) -> dict[str, Any]:
    return {
        "resolution_key": value.resolution_key,
        "release_plan_id": value.release_plan_id,
        "entity_key": value.entity_key,
        "namespace_id": value.namespace_id,
        "value": value.value,
        "issuer": value.issuer,
        "referent_kind": value.referent_kind,
        "assertion_ids_json": canonical_json(value.assertion_ids),
        "trace_json": value.trace_json,
    }


def gold_relation_row(value: GoldRelation) -> dict[str, Any]:
    return {
        "resolution_key": value.resolution_key,
        "release_plan_id": value.release_plan_id,
        "subject_entity_key": value.subject_entity_key,
        "predicate": value.predicate,
        "object_entity_key": value.object_entity_key,
        "qualifiers_json": value.qualifiers_json,
        "assertion_ids_json": canonical_json(value.assertion_ids),
        "trace_json": value.trace_json,
    }


def gold_conflict_row(value: GoldConflict) -> dict[str, Any]:
    return {
        "conflict_key": value.conflict_key,
        "release_plan_id": value.release_plan_id,
        "entity_key": value.entity_key,
        "predicate": value.predicate,
        "scope_hash": value.scope_hash,
        "reason": value.reason,
        "assertion_ids_json": canonical_json(value.assertion_ids),
        "candidate_values_json": value.candidate_values_json,
        "trace_json": value.trace_json,
    }


def gold_rows(
    *,
    entities: Iterable[GoldEntity],
    fields: Iterable[GoldField],
    identifiers: Iterable[GoldIdentifier],
    relations: Iterable[GoldRelation],
    conflicts: Iterable[GoldConflict],
) -> dict[str, list[dict[str, Any]]]:
    return {
        "community_gold_entity": [gold_entity_row(value) for value in entities],
        "community_gold_field": [gold_field_row(value) for value in fields],
        "community_gold_identifier": [
            gold_identifier_row(value) for value in identifiers
        ],
        "community_gold_relation": [gold_relation_row(value) for value in relations],
        "community_gold_conflict": [gold_conflict_row(value) for value in conflicts],
    }


def validate_gold_rows(
    plan: GoldReleasePlan,
    rows: Mapping[str, Iterable[dict[str, Any]]],
) -> dict[str, list[dict[str, Any]]]:
    if set(rows) != set(GOLD_DATA_COLUMNS):
        raise ValueError("Gold rows must contain every Gold data table")
    normalized = {table: list(table_rows) for table, table_rows in rows.items()}
    for table, table_rows in normalized.items():
        columns = set(GOLD_DATA_COLUMNS[table])
        key = GOLD_TABLE_KEYS[table]
        seen = set()
        for row in table_rows:
            if set(row) != columns:
                raise ValueError(f"{table} row columns do not match contract")
            if row["release_plan_id"] != plan.release_plan_id:
                raise ValueError(f"{table} row belongs to another release plan")
            if row[key] in seen:
                raise ValueError(f"{table} contains duplicate {key}")
            seen.add(row[key])
        if len(table_rows) != plan.expected_counts[table]:
            raise ValueError(f"{table} count differs from Gold release plan")
    return normalized
