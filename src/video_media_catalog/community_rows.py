"""Deterministic row projections from v2 contract objects to Silver tables."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from video_media_catalog.assertions import (
    EntityTypeAssertion,
    FieldAssertion,
    IdentifierAssertion,
    RelationshipAssertion,
)
from video_media_catalog.canonical import canonical_json
from video_media_catalog.community_ingest import (
    CommunityIngestCommit,
    CommunityIngestRun,
)
from video_media_catalog.community_tables import (
    DATA_TABLE_COLUMNS,
    RUN_TABLE_COLUMNS,
    TABLE_KEYS,
)
from video_media_catalog.connector import ConnectorRecordEnvelope
from video_media_catalog.identity_v2 import (
    EntityLedgerEntry,
    EntityMembership,
    EntityMergeEvent,
    EntityRedirect,
    EntitySplitEvent,
    ExternalIdIndexEntry,
    IdentityConflict,
    IdentityDecision,
    IdentityEvidence,
    LegacyKeyMap,
)
from video_media_catalog.v2_contracts import require_sha256


def _model_json(value) -> str:
    return canonical_json(
        value.model_dump(mode="json", by_alias=True, exclude_none=True)
    )


def ingest_run_row(run: CommunityIngestRun) -> dict[str, Any]:
    return {
        "run_id": run.run_id,
        "run_kind": run.run_kind.value,
        "source_product_id": run.source_product_id,
        "input_id": run.input_id,
        "policy_id": run.policy_id,
        "policy_digest": run.policy_digest,
        "image_digest": run.image_digest,
        "config_digest": run.config_digest,
        "started_at": run.started_at,
        "expected_counts_json": canonical_json(run.expected_counts),
        "manifest_json": _model_json(run),
    }


def ingest_commit_row(commit: CommunityIngestCommit) -> dict[str, Any]:
    return {
        "commit_key": commit.commit_key,
        "run_id": commit.run_id,
        "committed_at": commit.committed_at,
        "table_counts_json": canonical_json(commit.table_counts),
        "table_snapshot_ids_json": canonical_json(commit.table_snapshot_ids),
        "commit_json": _model_json(commit),
    }


def source_record_row(
    run_id: str,
    envelope: ConnectorRecordEnvelope,
) -> dict[str, Any]:
    return {
        "envelope_key": envelope.envelope_key,
        "run_id": require_sha256(run_id, label="run_id"),
        "batch_id": envelope.batch_id,
        "source_system_id": envelope.source_system_id,
        "source_product_id": envelope.source_product_id,
        "source_namespace_id": envelope.source_namespace_id,
        "source_record_id": envelope.source_record_id,
        "source_revision": envelope.source_revision,
        "operation": envelope.operation.value,
        "source_modified_at": envelope.source_modified_at,
        "observed_at": envelope.observed_at,
        "ingested_at": envelope.ingested_at,
        "valid_from": envelope.valid_from,
        "valid_to": envelope.valid_to,
        "expires_at": envelope.expires_at,
        "payload_schema": envelope.payload_schema,
        "source_hash": envelope.source_hash,
        "payload_json": envelope.payload_json,
        "payload_object_json": (
            None
            if envelope.payload_object is None
            else _model_json(envelope.payload_object)
        ),
        "raw_object_json": _model_json(envelope.raw_object),
        "source_location": envelope.source_location,
        "policy_id": envelope.policy_id,
        "policy_digest": envelope.policy_digest,
        "citation_keys_json": canonical_json(envelope.citation_keys),
    }


def _subject_columns(assertion) -> dict[str, str]:
    return {
        "subject_namespace_id": assertion.subject.namespace_id,
        "subject_source_id": assertion.subject.source_id,
        "subject_referent_kind": assertion.subject.referent_kind,
    }


def _assertion_provenance_columns(assertion) -> dict[str, str]:
    provenance = assertion.provenance
    return {
        "provenance_json": _model_json(provenance),
        "policy_id": provenance.policy_id,
        "policy_digest": provenance.policy_digest,
        "observed_at": provenance.observed_at,
    }


def field_assertion_row(
    run_id: str,
    assertion: FieldAssertion,
) -> dict[str, Any]:
    return {
        "assertion_id": assertion.assertion_id,
        "run_id": require_sha256(run_id, label="run_id"),
        **_subject_columns(assertion),
        "predicate": assertion.predicate,
        "value_type": assertion.value_type.value,
        "value_json": assertion.value_json,
        "qualifiers_json": canonical_json(assertion.qualifiers),
        "status": assertion.status.value,
        **_assertion_provenance_columns(assertion),
    }


def identifier_assertion_row(
    run_id: str,
    assertion: IdentifierAssertion,
) -> dict[str, Any]:
    return {
        "assertion_id": assertion.assertion_id,
        "run_id": require_sha256(run_id, label="run_id"),
        **_subject_columns(assertion),
        "namespace_id": assertion.namespace_id,
        "value": assertion.value,
        "issuer": assertion.issuer,
        "referent_kind": assertion.referent_kind,
        "status": assertion.status.value,
        **_assertion_provenance_columns(assertion),
    }


def relationship_assertion_row(
    run_id: str,
    assertion: RelationshipAssertion,
) -> dict[str, Any]:
    return {
        "assertion_id": assertion.assertion_id,
        "run_id": require_sha256(run_id, label="run_id"),
        **_subject_columns(assertion),
        "predicate": assertion.predicate,
        "object_namespace_id": assertion.object.namespace_id,
        "object_source_id": assertion.object.source_id,
        "object_referent_kind": assertion.object.referent_kind,
        "qualifiers_json": canonical_json(assertion.qualifiers),
        "status": assertion.status.value,
        **_assertion_provenance_columns(assertion),
    }


def entity_type_assertion_row(
    run_id: str,
    assertion: EntityTypeAssertion,
) -> dict[str, Any]:
    return {
        "assertion_id": assertion.assertion_id,
        "run_id": require_sha256(run_id, label="run_id"),
        **_subject_columns(assertion),
        "entity_type": assertion.entity_type,
        "status": assertion.status.value,
        **_assertion_provenance_columns(assertion),
    }


def entity_ledger_row(
    run_id: str,
    entity: EntityLedgerEntry,
) -> dict[str, Any]:
    return {
        "entity_key": entity.entity_key,
        "run_id": require_sha256(run_id, label="run_id"),
        "allocation_id": entity.allocation_id,
        "entity_level": entity.entity_level.value,
        "entity_kind": entity.entity_kind,
        "status": entity.status.value,
        "created_at": entity.created_at,
        "first_release_id": entity.first_release_id,
        "imported_v1": entity.imported_v1,
    }


def external_id_index_row(
    run_id: str,
    entry: ExternalIdIndexEntry,
) -> dict[str, Any]:
    return {
        "index_entry_key": entry.index_entry_key,
        "run_id": require_sha256(run_id, label="run_id"),
        "blocking_key": entry.blocking_key,
        "materialization_id": entry.materialization_id,
        "namespace_id": entry.namespace_id,
        "normalized_value": entry.normalized_value,
        "referent_kind": entry.referent_kind,
        "entity_key": entry.entity_key,
        "assertion_keys_json": canonical_json(entry.assertion_keys),
        "observed_at": entry.observed_at,
        "policy_id": entry.policy_id,
        "policy_digest": entry.policy_digest,
        "index_json": _model_json(entry),
    }


def legacy_key_map_row(
    run_id: str,
    mapping: LegacyKeyMap,
) -> dict[str, Any]:
    return {
        "legacy_key": mapping.legacy_key,
        "run_id": require_sha256(run_id, label="run_id"),
        "legacy_kind": mapping.legacy_kind,
        "target_key": mapping.target_key,
        "imported_at": mapping.imported_at,
        "source_snapshot_set_id": mapping.source_snapshot_set_id,
    }


def identity_evidence_row(
    run_id: str,
    evidence: IdentityEvidence,
) -> dict[str, Any]:
    return {
        "evidence_key": evidence.evidence_key,
        "run_id": require_sha256(run_id, label="run_id"),
        "kind": evidence.kind.value,
        "source_namespace_id": evidence.source_node.namespace_id,
        "source_id": evidence.source_node.source_id,
        "source_referent_kind": evidence.source_node.referent_kind,
        "candidate_entity_key": evidence.candidate_entity_key,
        "assertion_keys_json": canonical_json(evidence.assertion_keys),
        "observed_at": evidence.observed_at,
        "policy_id": evidence.policy_id,
        "policy_digest": evidence.policy_digest,
        "confidence": evidence.confidence,
        "details_json": canonical_json(evidence.details),
        "evidence_json": _model_json(evidence),
    }


def identity_conflict_row(
    run_id: str,
    conflict: IdentityConflict,
) -> dict[str, Any]:
    return {
        "conflict_key": conflict.conflict_key,
        "run_id": require_sha256(run_id, label="run_id"),
        "materialization_id": conflict.materialization_id,
        "source_namespace_id": conflict.source_node.namespace_id,
        "source_id": conflict.source_node.source_id,
        "source_referent_kind": conflict.source_node.referent_kind,
        "candidate_entity_keys_json": canonical_json(conflict.candidate_entity_keys),
        "assertion_keys_json": canonical_json(conflict.assertion_keys),
        "reason": conflict.reason,
        "observed_at": conflict.observed_at,
        "policy_id": conflict.policy_id,
        "policy_digest": conflict.policy_digest,
        "details_json": canonical_json(conflict.details),
        "conflict_json": _model_json(conflict),
    }


def identity_decision_row(
    run_id: str,
    decision: IdentityDecision,
) -> dict[str, Any]:
    return {
        "decision_id": decision.decision_id,
        "run_id": require_sha256(run_id, label="run_id"),
        "status": decision.status.value,
        "source_namespace_id": decision.source_node.namespace_id,
        "source_id": decision.source_node.source_id,
        "source_referent_kind": decision.source_node.referent_kind,
        "entity_key": decision.entity_key,
        "evidence_keys_json": canonical_json(decision.evidence_keys),
        "policy_version": decision.policy_version,
        "decided_by": decision.decided_by,
        "decided_at": decision.decided_at,
        "reason": decision.reason,
        "decision_json": _model_json(decision),
    }


def entity_membership_row(
    run_id: str,
    membership: EntityMembership,
) -> dict[str, Any]:
    return {
        "membership_key": membership.membership_key,
        "run_id": require_sha256(run_id, label="run_id"),
        "source_namespace_id": membership.source_node.namespace_id,
        "source_id": membership.source_node.source_id,
        "source_referent_kind": membership.source_node.referent_kind,
        "entity_key": membership.entity_key,
        "decision_id": membership.decision_id,
        "valid_from": membership.valid_from,
        "valid_to": membership.valid_to,
    }


def entity_redirect_row(
    run_id: str,
    redirect: EntityRedirect,
) -> dict[str, Any]:
    return {
        "redirect_key": redirect.redirect_key,
        "run_id": require_sha256(run_id, label="run_id"),
        "source_entity_key": redirect.source_entity_key,
        "target_entity_key": redirect.target_entity_key,
        "effective_at": redirect.effective_at,
        "decision_id": redirect.decision_id,
    }


def entity_merge_event_row(
    run_id: str,
    event: EntityMergeEvent,
) -> dict[str, Any]:
    return {
        "merge_event_key": event.merge_event_key,
        "run_id": require_sha256(run_id, label="run_id"),
        "entity_keys_json": canonical_json(event.entity_keys),
        "survivor_entity_key": event.survivor_entity_key,
        "redirect_keys_json": canonical_json(event.redirect_keys),
        "decision_id": event.decision_id,
        "effective_at": event.effective_at,
        "merged_by": event.merged_by,
        "reason": event.reason,
        "event_json": _model_json(event),
    }


def entity_split_event_row(
    run_id: str,
    event: EntitySplitEvent,
) -> dict[str, Any]:
    return {
        "split_event_key": event.split_event_key,
        "run_id": require_sha256(run_id, label="run_id"),
        "source_entity_key": event.source_entity_key,
        "target_entity_keys_json": canonical_json(event.target_entity_keys),
        "assignments_json": canonical_json(
            tuple(
                assignment.model_dump(mode="json", by_alias=True)
                for assignment in event.assignments
            )
        ),
        "effective_at": event.effective_at,
        "split_by": event.split_by,
        "reason": event.reason,
        "event_json": _model_json(event),
    }


def empty_data_rows() -> dict[str, list[dict[str, Any]]]:
    return {table: [] for table in DATA_TABLE_COLUMNS}


def validate_data_rows(
    run: CommunityIngestRun,
    rows: Mapping[str, Iterable[dict[str, Any]]],
) -> dict[str, list[dict[str, Any]]]:
    if set(rows) != set(DATA_TABLE_COLUMNS):
        raise ValueError("row set must contain every v2 data table")
    normalized = {table: list(table_rows) for table, table_rows in rows.items()}
    for table, table_rows in normalized.items():
        columns = set(DATA_TABLE_COLUMNS[table])
        key = TABLE_KEYS[table]
        seen: set[Any] = set()
        for row in table_rows:
            if set(row) != columns:
                raise ValueError(f"{table} row columns do not match contract")
            if row["run_id"] != run.run_id:
                raise ValueError(f"{table} row belongs to another run")
            if row[key] in seen:
                raise ValueError(f"{table} contains duplicate {key}")
            seen.add(row[key])
        expected = run.expected_counts[table]
        if len(table_rows) != expected:
            raise ValueError(
                f"{table} count {len(table_rows)} does not match expected {expected}"
            )
    return normalized


def validate_run_row(row: dict[str, Any]) -> None:
    if set(row) != set(RUN_TABLE_COLUMNS["community_ingest_run"]):
        raise ValueError("community_ingest_run row columns do not match contract")


def validate_commit_row(row: dict[str, Any]) -> None:
    if set(row) != set(RUN_TABLE_COLUMNS["community_ingest_commit"]):
        raise ValueError("community_ingest_commit row columns do not match contract")
