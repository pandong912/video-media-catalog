"""Logical Silver v2 table names, physical mapping, keys, and nullability."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping

from video_media_catalog.v2_contracts import require_slug

RUN_TABLE_COLUMNS: dict[str, tuple[str, ...]] = {
    "community_ingest_run": (
        "run_id",
        "run_kind",
        "source_product_id",
        "input_id",
        "policy_id",
        "policy_digest",
        "image_digest",
        "config_digest",
        "started_at",
        "expected_counts_json",
        "manifest_json",
    ),
    "community_ingest_commit": (
        "commit_key",
        "run_id",
        "committed_at",
        "table_counts_json",
        "table_snapshot_ids_json",
        "commit_json",
    ),
}

DATA_TABLE_COLUMNS: dict[str, tuple[str, ...]] = {
    "community_source_record": (
        "envelope_key",
        "run_id",
        "batch_id",
        "source_system_id",
        "source_product_id",
        "source_namespace_id",
        "source_record_id",
        "source_revision",
        "operation",
        "source_modified_at",
        "observed_at",
        "ingested_at",
        "valid_from",
        "valid_to",
        "expires_at",
        "payload_schema",
        "source_hash",
        "payload_json",
        "payload_object_json",
        "raw_object_json",
        "source_location",
        "policy_id",
        "policy_digest",
        "citation_keys_json",
    ),
    "community_field_assertion": (
        "assertion_id",
        "run_id",
        "subject_namespace_id",
        "subject_source_id",
        "subject_referent_kind",
        "predicate",
        "value_type",
        "value_json",
        "qualifiers_json",
        "status",
        "provenance_json",
        "policy_id",
        "policy_digest",
        "observed_at",
    ),
    "community_identifier_assertion": (
        "assertion_id",
        "run_id",
        "subject_namespace_id",
        "subject_source_id",
        "subject_referent_kind",
        "namespace_id",
        "value",
        "issuer",
        "referent_kind",
        "status",
        "provenance_json",
        "policy_id",
        "policy_digest",
        "observed_at",
    ),
    "community_relationship_assertion": (
        "assertion_id",
        "run_id",
        "subject_namespace_id",
        "subject_source_id",
        "subject_referent_kind",
        "predicate",
        "object_namespace_id",
        "object_source_id",
        "object_referent_kind",
        "qualifiers_json",
        "status",
        "provenance_json",
        "policy_id",
        "policy_digest",
        "observed_at",
    ),
    "community_entity_type_assertion": (
        "assertion_id",
        "run_id",
        "subject_namespace_id",
        "subject_source_id",
        "subject_referent_kind",
        "entity_type",
        "status",
        "provenance_json",
        "policy_id",
        "policy_digest",
        "observed_at",
    ),
    "community_external_id_index": (
        "index_entry_key",
        "run_id",
        "blocking_key",
        "materialization_id",
        "namespace_id",
        "normalized_value",
        "referent_kind",
        "entity_key",
        "assertion_keys_json",
        "observed_at",
        "policy_id",
        "policy_digest",
        "index_json",
    ),
    "community_entity_ledger": (
        "entity_key",
        "run_id",
        "allocation_id",
        "entity_level",
        "entity_kind",
        "status",
        "created_at",
        "first_release_id",
        "imported_v1",
    ),
    "community_legacy_key_map": (
        "legacy_key",
        "run_id",
        "legacy_kind",
        "target_key",
        "imported_at",
        "source_snapshot_set_id",
    ),
    "community_identity_evidence": (
        "evidence_key",
        "run_id",
        "kind",
        "source_namespace_id",
        "source_id",
        "source_referent_kind",
        "candidate_entity_key",
        "assertion_keys_json",
        "observed_at",
        "policy_id",
        "policy_digest",
        "confidence",
        "details_json",
        "evidence_json",
    ),
    "community_identity_conflict": (
        "conflict_key",
        "run_id",
        "materialization_id",
        "source_namespace_id",
        "source_id",
        "source_referent_kind",
        "candidate_entity_keys_json",
        "assertion_keys_json",
        "reason",
        "observed_at",
        "policy_id",
        "policy_digest",
        "details_json",
        "conflict_json",
    ),
    "community_identity_decision": (
        "decision_id",
        "run_id",
        "status",
        "source_namespace_id",
        "source_id",
        "source_referent_kind",
        "entity_key",
        "evidence_keys_json",
        "policy_version",
        "decided_by",
        "decided_at",
        "reason",
        "decision_json",
    ),
    "community_entity_membership": (
        "membership_key",
        "run_id",
        "source_namespace_id",
        "source_id",
        "source_referent_kind",
        "entity_key",
        "decision_id",
        "valid_from",
        "valid_to",
    ),
    "community_entity_redirect": (
        "redirect_key",
        "run_id",
        "source_entity_key",
        "target_entity_key",
        "effective_at",
        "decision_id",
    ),
    "community_entity_merge_event": (
        "merge_event_key",
        "run_id",
        "entity_keys_json",
        "survivor_entity_key",
        "redirect_keys_json",
        "decision_id",
        "effective_at",
        "merged_by",
        "reason",
        "event_json",
    ),
    "community_entity_split_event": (
        "split_event_key",
        "run_id",
        "source_entity_key",
        "target_entity_keys_json",
        "assignments_json",
        "effective_at",
        "split_by",
        "reason",
        "event_json",
    ),
}

TABLE_COLUMNS = {**RUN_TABLE_COLUMNS, **DATA_TABLE_COLUMNS}

CONTROL_TABLES = frozenset(RUN_TABLE_COLUMNS)
SOURCE_TABLES = frozenset(
    {
        "community_source_record",
        "community_field_assertion",
        "community_identifier_assertion",
        "community_relationship_assertion",
        "community_entity_type_assertion",
    }
)
IDENTITY_TABLES = frozenset(DATA_TABLE_COLUMNS) - SOURCE_TABLES

if CONTROL_TABLES & SOURCE_TABLES or CONTROL_TABLES & IDENTITY_TABLES:
    raise RuntimeError("community table groups must be disjoint")
if frozenset(TABLE_COLUMNS) != CONTROL_TABLES | SOURCE_TABLES | IDENTITY_TABLES:
    raise RuntimeError("community table groups must cover every logical table")

MAX_IDENTITY_GENERATION_ID_LENGTH = 64
_PHYSICAL_TABLE_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")


def require_identity_generation_id(value: str) -> str:
    """Return one bounded generation token suitable for deterministic mapping."""

    if not isinstance(value, str):
        raise ValueError("identity_generation_id must be a stable lowercase slug")
    return require_slug(
        value,
        label="identity_generation_id",
        max_length=MAX_IDENTITY_GENERATION_ID_LENGTH,
    )


def identity_physical_table_name(table: str, generation_id: str) -> str:
    """Resolve one logical Identity table to a short collision-safe Glue name."""

    if table not in IDENTITY_TABLES:
        raise KeyError(f"not an Identity table: {table}")
    generation = require_identity_generation_id(generation_id)
    readable = re.sub(r"[^a-z0-9]+", "_", generation).strip("_")[:32]
    digest = hashlib.sha256(generation.encode("utf-8")).hexdigest()[:24]
    physical = f"{table}__g_{readable}_{digest}"
    if len(physical) > 127 or _PHYSICAL_TABLE_IDENTIFIER.fullmatch(physical) is None:
        raise ValueError("resolved Identity physical table name is unsafe")
    return physical


def build_community_table_mapping(
    identity_generation_id: str | None = None,
) -> dict[str, str]:
    """Build the complete logical-to-physical table mapping."""

    generation = (
        None
        if identity_generation_id is None
        else require_identity_generation_id(identity_generation_id)
    )
    return {
        table: (
            identity_physical_table_name(table, generation)
            if generation is not None and table in IDENTITY_TABLES
            else table
        )
        for table in TABLE_COLUMNS
    }


def validate_community_table_mapping(
    mapping: Mapping[str, str],
    *,
    identity_generation_id: str | None,
) -> dict[str, str]:
    """Reject incomplete, injected, or cross-generation physical mappings."""

    expected = build_community_table_mapping(identity_generation_id)
    if set(mapping) != set(expected):
        raise ValueError("table mapping must contain every community logical table")
    normalized = dict(mapping)
    if any(
        not isinstance(physical, str)
        or len(physical) > 127
        or _PHYSICAL_TABLE_IDENTIFIER.fullmatch(physical) is None
        for physical in normalized.values()
    ):
        raise ValueError("table mapping contains an unsafe physical table name")
    if len(set(normalized.values())) != len(normalized):
        raise ValueError("table mapping physical names must be unique")
    mismatches = sorted(
        table for table, physical in normalized.items() if physical != expected[table]
    )
    if mismatches:
        raise ValueError(
            "table mapping does not match the declared Identity generation: "
            + ", ".join(mismatches)
        )
    return {table: normalized[table] for table in TABLE_COLUMNS}


TABLE_KEYS: dict[str, str] = {
    "community_ingest_run": "run_id",
    "community_ingest_commit": "run_id",
    "community_source_record": "envelope_key",
    "community_field_assertion": "assertion_id",
    "community_identifier_assertion": "assertion_id",
    "community_relationship_assertion": "assertion_id",
    "community_entity_type_assertion": "assertion_id",
    "community_external_id_index": "index_entry_key",
    "community_entity_ledger": "entity_key",
    "community_legacy_key_map": "legacy_key",
    "community_identity_evidence": "evidence_key",
    "community_identity_conflict": "conflict_key",
    "community_identity_decision": "decision_id",
    "community_entity_membership": "membership_key",
    "community_entity_redirect": "redirect_key",
    "community_entity_merge_event": "merge_event_key",
    "community_entity_split_event": "split_event_key",
}

# Source envelopes are immutable, but a corrected mapper republishes the same
# envelope under a new run. Match run_id so that republish inserts its own rows
# instead of no-op merging into the previous run.
TABLE_MERGE_KEYS: dict[str, tuple[str, ...]] = {
    table: (key,) for table, key in TABLE_KEYS.items()
}
TABLE_MERGE_KEYS["community_source_record"] = ("envelope_key", "run_id")

NULLABLE_COLUMNS: dict[str, frozenset[str]] = {
    "community_ingest_run": frozenset(),
    "community_ingest_commit": frozenset(),
    "community_source_record": frozenset(
        {
            "source_revision",
            "source_modified_at",
            "valid_from",
            "valid_to",
            "expires_at",
            "payload_json",
            "payload_object_json",
        }
    ),
    "community_field_assertion": frozenset(),
    "community_identifier_assertion": frozenset(),
    "community_relationship_assertion": frozenset(),
    "community_entity_type_assertion": frozenset(),
    "community_external_id_index": frozenset(),
    "community_entity_ledger": frozenset({"allocation_id", "first_release_id"}),
    "community_legacy_key_map": frozenset(),
    "community_identity_evidence": frozenset({"confidence"}),
    "community_identity_conflict": frozenset(),
    "community_identity_decision": frozenset(),
    "community_entity_membership": frozenset({"valid_to"}),
    "community_entity_redirect": frozenset(),
    "community_entity_merge_event": frozenset(),
    "community_entity_split_event": frozenset(),
}

_ENTITY_KEY_PARTITIONED_TABLES = {
    "community_entity_ledger",
    "community_legacy_key_map",
    "community_entity_redirect",
}

TABLE_PARTITION_COLUMNS: dict[str, str] = {
    table: (
        "blocking_key"
        if table == "community_external_id_index"
        else TABLE_KEYS[table]
        if table in _ENTITY_KEY_PARTITIONED_TABLES
        else "run_id"
    )
    for table in TABLE_COLUMNS
}
