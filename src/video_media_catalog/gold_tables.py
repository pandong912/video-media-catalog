"""Logical Gold v2 table schemas."""

from __future__ import annotations

GOLD_CONTROL_COLUMNS: dict[str, tuple[str, ...]] = {
    "community_gold_release_plan": (
        "release_plan_id",
        "context_id",
        "as_of",
        "rights_registry_digest",
        "field_policy_digest",
        "resolver_digest",
        "image_digest",
        "config_digest",
        "expected_counts_json",
        "plan_json",
    ),
    "community_gold_release_commit": (
        "commit_key",
        "release_plan_id",
        "committed_at",
        "table_counts_json",
        "table_snapshot_ids_json",
        "quality_report_json",
        "attribution_manifest_json",
        "commit_json",
    ),
}

GOLD_DATA_COLUMNS: dict[str, tuple[str, ...]] = {
    "community_gold_entity": (
        "row_key",
        "release_plan_id",
        "entity_key",
        "entity_level",
        "entity_kind",
        "status",
        "source_node_count",
        "trace_json",
    ),
    "community_gold_field": (
        "resolution_key",
        "release_plan_id",
        "entity_key",
        "predicate",
        "scope_hash",
        "value_type",
        "value_json",
        "qualifiers_json",
        "resolution_status",
        "selected_assertion_id",
        "assertion_ids_json",
        "trace_json",
    ),
    "community_gold_identifier": (
        "resolution_key",
        "release_plan_id",
        "entity_key",
        "namespace_id",
        "value",
        "issuer",
        "referent_kind",
        "assertion_ids_json",
        "trace_json",
    ),
    "community_gold_relation": (
        "resolution_key",
        "release_plan_id",
        "subject_entity_key",
        "predicate",
        "object_entity_key",
        "qualifiers_json",
        "assertion_ids_json",
        "trace_json",
    ),
    "community_gold_conflict": (
        "conflict_key",
        "release_plan_id",
        "entity_key",
        "predicate",
        "scope_hash",
        "reason",
        "assertion_ids_json",
        "candidate_values_json",
        "trace_json",
    ),
}

GOLD_TABLE_COLUMNS = {**GOLD_CONTROL_COLUMNS, **GOLD_DATA_COLUMNS}

GOLD_TABLE_KEYS = {
    "community_gold_release_plan": "release_plan_id",
    "community_gold_release_commit": "release_plan_id",
    "community_gold_entity": "row_key",
    "community_gold_field": "resolution_key",
    "community_gold_identifier": "resolution_key",
    "community_gold_relation": "resolution_key",
    "community_gold_conflict": "conflict_key",
}

GOLD_NULLABLE_COLUMNS = {
    table: frozenset(
        {"value_json", "selected_assertion_id"}
        if table == "community_gold_field"
        else set()
    )
    for table in GOLD_TABLE_COLUMNS
}
