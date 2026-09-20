"""Auditable, fail-closed Iceberg maintenance planning."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from datetime import timedelta
from enum import StrEnum
from typing import Any, Literal, Self

from pydantic import Field, ValidationInfo, field_validator, model_validator

from video_media_catalog.canonical import deterministic_key
from video_media_catalog.community_tables import TABLE_COLUMNS
from video_media_catalog.gold_tables import GOLD_TABLE_COLUMNS
from video_media_catalog.v2_contracts import (
    V2ContractModel,
    parse_rfc3339,
    require_rfc3339,
    require_sha256,
)

MIN_RETENTION_DAYS = 7
MIN_RETAIN_LAST = 2
DEFAULT_RETENTION_DAYS = 30
DEFAULT_RETAIN_LAST = 5
ALLOWED_MAINTENANCE_TABLES = frozenset(TABLE_COLUMNS) | frozenset(GOLD_TABLE_COLUMNS)

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_ZERO_DIGEST = "sha256:" + ("0" * 64)


class MaintenanceOperation(StrEnum):
    REWRITE_DATA_FILES = "rewrite-data-files"
    REWRITE_MANIFESTS = "rewrite-manifests"
    EXPIRE_SNAPSHOTS = "expire-snapshots"
    REMOVE_ORPHAN_FILES = "remove-orphan-files"

    @property
    def destructive(self) -> bool:
        return self in {
            MaintenanceOperation.EXPIRE_SNAPSHOTS,
            MaintenanceOperation.REMOVE_ORPHAN_FILES,
        }


_OPERATION_ORDER = {
    MaintenanceOperation.REWRITE_DATA_FILES: 0,
    MaintenanceOperation.REWRITE_MANIFESTS: 1,
    MaintenanceOperation.EXPIRE_SNAPSHOTS: 2,
    MaintenanceOperation.REMOVE_ORPHAN_FILES: 3,
}


class IcebergSnapshotState(V2ContractModel):
    snapshot_id: int = Field(gt=0)
    committed_at: str
    is_current: bool = False

    @field_validator("committed_at")
    @classmethod
    def validate_committed_at(cls, value: str) -> str:
        return require_rfc3339(value)


class IcebergMaintenanceAction(V2ContractModel):
    table: str
    operation: MaintenanceOperation
    sql: str
    destructive: bool
    current_snapshot_id: int | None = Field(default=None, gt=0)
    protected_snapshot_ids: tuple[int, ...] = ()
    candidate_snapshot_ids: tuple[int, ...] = ()

    @field_validator("table")
    @classmethod
    def validate_table(cls, value: str) -> str:
        if value not in ALLOWED_MAINTENANCE_TABLES:
            raise ValueError(f"table is not maintenance-allowlisted: {value}")
        return value

    @field_validator("protected_snapshot_ids", "candidate_snapshot_ids")
    @classmethod
    def normalize_snapshot_ids(cls, value: tuple[int, ...]) -> tuple[int, ...]:
        if any(isinstance(item, bool) or item <= 0 for item in value):
            raise ValueError("maintenance snapshot IDs must be positive")
        if len(value) != len(set(value)):
            raise ValueError("maintenance snapshot IDs must be unique")
        return tuple(sorted(value))

    @model_validator(mode="after")
    def validate_safety(self) -> Self:
        if self.destructive != self.operation.destructive:
            raise ValueError("maintenance destructive flag does not match operation")
        protected = set(self.protected_snapshot_ids)
        if self.current_snapshot_id is not None:
            protected.add(self.current_snapshot_id)
        if protected.intersection(self.candidate_snapshot_ids):
            raise ValueError("maintenance candidates include a protected snapshot")
        return self


class IcebergMaintenancePlan(V2ContractModel):
    schema_version: Literal["1.0"] = "1.0"
    plan_id: str
    catalog_name: str
    namespace: str
    dry_run: bool = True
    planned_at: str
    retention_days: int = Field(ge=MIN_RETENTION_DAYS)
    orphan_retention_days: int = Field(ge=MIN_RETENTION_DAYS)
    retain_last: int = Field(ge=MIN_RETAIN_LAST)
    references_reviewed: bool = False
    actions: tuple[IcebergMaintenanceAction, ...]

    @field_validator("plan_id")
    @classmethod
    def validate_plan_id(cls, value: str) -> str:
        return require_sha256(value, label="maintenance plan_id")

    @field_validator("catalog_name", "namespace")
    @classmethod
    def validate_identifier(cls, value: str) -> str:
        if _IDENTIFIER.fullmatch(value) is None:
            raise ValueError("catalog and namespace must be safe identifiers")
        return value

    @field_validator("planned_at")
    @classmethod
    def validate_planned_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @model_validator(mode="after")
    def validate_plan(self, info: ValidationInfo) -> Self:
        if not self.actions:
            raise ValueError("maintenance plan requires at least one action")
        if (
            not self.dry_run
            and any(action.destructive for action in self.actions)
            and not self.references_reviewed
        ):
            raise ValueError(
                "executing destructive maintenance requires reviewed references"
            )
        if not (info.context or {}).get("skip_identity"):
            expected = deterministic_key(
                "iceberg-maintenance-plan-v1",
                _plan_identity(self),
            )
            if self.plan_id != expected:
                raise ValueError("maintenance plan_id does not match plan contents")
        return self


def _safe_identifier(value: str, *, label: str) -> str:
    if _IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f"{label} must be a safe SQL identifier")
    return value


def _table_argument(namespace: str, table: str) -> str:
    _safe_identifier(namespace, label="namespace")
    if table not in ALLOWED_MAINTENANCE_TABLES:
        raise ValueError(f"table is not maintenance-allowlisted: {table}")
    return f"{namespace}.{table}"


def _timestamp_literal(value: str) -> str:
    parsed = parse_rfc3339(value)
    return parsed.strftime("%Y-%m-%d %H:%M:%S") + " UTC"


def rewrite_data_files_sql(
    *,
    catalog_name: str,
    namespace: str,
    table: str,
) -> str:
    catalog = _safe_identifier(catalog_name, label="catalog_name")
    target = _table_argument(namespace, table)
    return (
        f"CALL `{catalog}`.system.rewrite_data_files("
        f"table => '{target}', "
        "options => map('min-input-files', '5'))"
    )


def rewrite_manifests_sql(
    *,
    catalog_name: str,
    namespace: str,
    table: str,
) -> str:
    catalog = _safe_identifier(catalog_name, label="catalog_name")
    target = _table_argument(namespace, table)
    return f"CALL `{catalog}`.system.rewrite_manifests(table => '{target}')"


def expire_snapshots_sql(
    *,
    catalog_name: str,
    namespace: str,
    table: str,
    older_than: str,
    retain_last: int,
) -> str:
    if retain_last < MIN_RETAIN_LAST:
        raise ValueError(f"retain_last must be at least {MIN_RETAIN_LAST}")
    catalog = _safe_identifier(catalog_name, label="catalog_name")
    target = _table_argument(namespace, table)
    timestamp = _timestamp_literal(older_than)
    return (
        f"CALL `{catalog}`.system.expire_snapshots("
        f"table => '{target}', "
        f"older_than => TIMESTAMP '{timestamp}', "
        f"retain_last => {retain_last})"
    )


def remove_orphan_files_sql(
    *,
    catalog_name: str,
    namespace: str,
    table: str,
    older_than: str,
) -> str:
    catalog = _safe_identifier(catalog_name, label="catalog_name")
    target = _table_argument(namespace, table)
    timestamp = _timestamp_literal(older_than)
    return (
        f"CALL `{catalog}`.system.remove_orphan_files("
        f"table => '{target}', "
        f"older_than => TIMESTAMP '{timestamp}')"
    )


def _plan_identity(plan: IcebergMaintenancePlan) -> dict[str, Any]:
    return {
        "schemaVersion": plan.schema_version,
        "catalogName": plan.catalog_name,
        "namespace": plan.namespace,
        "dryRun": plan.dry_run,
        "plannedAt": plan.planned_at,
        "retentionDays": plan.retention_days,
        "orphanRetentionDays": plan.orphan_retention_days,
        "retainLast": plan.retain_last,
        "referencesReviewed": plan.references_reviewed,
        "actions": [
            action.model_dump(mode="json", by_alias=True, exclude_none=True)
            for action in plan.actions
        ],
    }


def build_iceberg_maintenance_plan(
    *,
    catalog_name: str,
    namespace: str,
    tables: Sequence[str],
    operations: Sequence[MaintenanceOperation | str],
    snapshots: Mapping[str, Sequence[IcebergSnapshotState]],
    protected_snapshot_ids: Mapping[str, Sequence[int]] | None = None,
    planned_at: str,
    retention_days: int = DEFAULT_RETENTION_DAYS,
    orphan_retention_days: int = DEFAULT_RETENTION_DAYS,
    retain_last: int = DEFAULT_RETAIN_LAST,
    dry_run: bool = True,
    references_reviewed: bool = False,
) -> IcebergMaintenancePlan:
    """Build a deterministic plan and reject unsafe expiry before execution."""

    catalog = _safe_identifier(catalog_name, label="catalog_name")
    current_namespace = _safe_identifier(namespace, label="namespace")
    selected_tables = tuple(sorted(set(tables)))
    if not selected_tables:
        raise ValueError("maintenance requires at least one table")
    unknown = sorted(set(selected_tables) - ALLOWED_MAINTENANCE_TABLES)
    if unknown:
        raise ValueError(
            "tables are not maintenance-allowlisted: " + ", ".join(unknown)
        )
    selected_operations = tuple(
        sorted(
            {MaintenanceOperation(item) for item in operations},
            key=_OPERATION_ORDER.__getitem__,
        )
    )
    if not selected_operations:
        raise ValueError("maintenance requires at least one operation")
    if retention_days < MIN_RETENTION_DAYS:
        raise ValueError(f"retention_days must be at least {MIN_RETENTION_DAYS}")
    if orphan_retention_days < MIN_RETENTION_DAYS:
        raise ValueError(f"orphan_retention_days must be at least {MIN_RETENTION_DAYS}")
    if retain_last < MIN_RETAIN_LAST:
        raise ValueError(f"retain_last must be at least {MIN_RETAIN_LAST}")
    normalized_planned_at = require_rfc3339(planned_at)
    planned_datetime = parse_rfc3339(normalized_planned_at)
    expiry_cutoff = planned_datetime - timedelta(days=retention_days)
    orphan_cutoff = planned_datetime - timedelta(days=orphan_retention_days)
    expiry_cutoff_text = expiry_cutoff.isoformat().replace("+00:00", "Z")
    orphan_cutoff_text = orphan_cutoff.isoformat().replace("+00:00", "Z")
    protected_input = protected_snapshot_ids or {}
    unknown_protected_tables = sorted(set(protected_input) - set(selected_tables))
    if unknown_protected_tables:
        raise ValueError(
            "protected snapshot tables were not selected: "
            + ", ".join(unknown_protected_tables)
        )

    actions: list[IcebergMaintenanceAction] = []
    for table in selected_tables:
        states = tuple(snapshots.get(table, ()))
        state_ids = [state.snapshot_id for state in states]
        if len(state_ids) != len(set(state_ids)):
            raise ValueError(f"{table} snapshot inventory contains duplicates")
        current_ids = [state.snapshot_id for state in states if state.is_current]
        if states and len(current_ids) != 1:
            raise ValueError(
                f"{table} non-empty snapshot inventory requires one current entry"
            )
        current_id = current_ids[0] if current_ids else None
        external_protected = tuple(sorted(set(protected_input.get(table, ()))))
        missing_protected = sorted(set(external_protected) - set(state_ids))
        if missing_protected:
            raise ValueError(
                f"{table} protected snapshots are absent from inventory: "
                + ", ".join(str(item) for item in missing_protected)
            )
        procedure_retained: set[int] = set()
        if current_id is not None:
            procedure_retained.add(current_id)
        retained_last = sorted(
            states,
            key=lambda state: (
                parse_rfc3339(state.committed_at),
                state.snapshot_id,
            ),
            reverse=True,
        )[:retain_last]
        procedure_retained.update(state.snapshot_id for state in retained_last)
        all_protected = set(external_protected) | procedure_retained
        old_states = tuple(
            state
            for state in states
            if parse_rfc3339(state.committed_at) < expiry_cutoff
        )
        unsafe_references = sorted(
            state.snapshot_id
            for state in old_states
            if state.snapshot_id in external_protected
            and state.snapshot_id not in procedure_retained
        )
        if (
            MaintenanceOperation.EXPIRE_SNAPSHOTS in selected_operations
            and unsafe_references
        ):
            raise ValueError(
                f"{table} retention window would expire referenced snapshots: "
                + ", ".join(str(item) for item in unsafe_references)
            )
        candidates = tuple(
            state.snapshot_id
            for state in old_states
            if state.snapshot_id not in all_protected
        )

        for operation in selected_operations:
            if operation == MaintenanceOperation.REWRITE_DATA_FILES:
                sql = rewrite_data_files_sql(
                    catalog_name=catalog,
                    namespace=current_namespace,
                    table=table,
                )
                action_candidates: tuple[int, ...] = ()
            elif operation == MaintenanceOperation.REWRITE_MANIFESTS:
                sql = rewrite_manifests_sql(
                    catalog_name=catalog,
                    namespace=current_namespace,
                    table=table,
                )
                action_candidates = ()
            elif operation == MaintenanceOperation.EXPIRE_SNAPSHOTS:
                sql = expire_snapshots_sql(
                    catalog_name=catalog,
                    namespace=current_namespace,
                    table=table,
                    older_than=expiry_cutoff_text,
                    retain_last=retain_last,
                )
                action_candidates = candidates
            else:
                sql = remove_orphan_files_sql(
                    catalog_name=catalog,
                    namespace=current_namespace,
                    table=table,
                    older_than=orphan_cutoff_text,
                )
                action_candidates = ()
            actions.append(
                IcebergMaintenanceAction(
                    table=table,
                    operation=operation,
                    sql=sql,
                    destructive=operation.destructive,
                    current_snapshot_id=current_id,
                    protected_snapshot_ids=external_protected,
                    candidate_snapshot_ids=action_candidates,
                )
            )

    provisional = IcebergMaintenancePlan.model_validate(
        {
            "plan_id": _ZERO_DIGEST,
            "catalog_name": catalog,
            "namespace": current_namespace,
            "dry_run": dry_run,
            "planned_at": normalized_planned_at,
            "retention_days": retention_days,
            "orphan_retention_days": orphan_retention_days,
            "retain_last": retain_last,
            "references_reviewed": references_reviewed,
            "actions": tuple(actions),
        },
        context={"skip_identity": True},
    )
    normalized = provisional.model_dump(mode="python")
    normalized["plan_id"] = deterministic_key(
        "iceberg-maintenance-plan-v1",
        _plan_identity(provisional),
    )
    return IcebergMaintenancePlan.model_validate(normalized)


def execute_iceberg_maintenance_plan(
    spark: Any,
    plan: IcebergMaintenancePlan,
) -> tuple[dict[str, Any], ...]:
    """Execute an already-audited non-dry-run plan in declared order."""

    if plan.dry_run:
        raise ValueError("dry-run maintenance plans cannot be executed")
    results = []
    for action in plan.actions:
        row_count = spark.sql(action.sql).count()
        results.append(
            {
                "table": action.table,
                "operation": action.operation.value,
                "rowCount": row_count,
            }
        )
    return tuple(results)
