"""Auditable, fail-closed Iceberg maintenance planning."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from datetime import timedelta
from enum import StrEnum
from typing import Any, Literal, Protocol, Self

from pydantic import Field, ValidationInfo, field_validator, model_validator

from video_media_catalog.canonical import deterministic_key
from video_media_catalog.community_tables import (
    IDENTITY_TABLES,
    TABLE_COLUMNS,
    require_identity_generation_id,
    validate_community_table_mapping,
)
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
FAILED_RUN_ROLLBACK_TABLES = IDENTITY_TABLES | frozenset({"community_ingest_run"})

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


class FailedRunSnapshotState(V2ContractModel):
    snapshot_id: int = Field(gt=0)
    sequence_number: int = Field(default=0, ge=0)
    parent_snapshot_id: int | None = Field(default=None, gt=0)
    journal_parent_snapshot_id: int | None = Field(default=None, gt=0)
    parent_journal_recorded: bool = False
    committed_at: str
    owner_run_id: str | None = None
    rollback_run_id: str | None = None
    is_current: bool = False
    is_current_ancestor: bool = False

    @field_validator("committed_at")
    @classmethod
    def validate_committed_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @field_validator("owner_run_id", "rollback_run_id")
    @classmethod
    def validate_optional_run_id(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return require_sha256(value, label="rollback run_id")


class FailedRunTableState(V2ContractModel):
    logical_table: str
    physical_table: str
    snapshots: tuple[FailedRunSnapshotState, ...]
    protected_refs: dict[str, int] = Field(default_factory=dict)

    @field_validator("logical_table")
    @classmethod
    def validate_logical_table(cls, value: str) -> str:
        if value not in FAILED_RUN_ROLLBACK_TABLES:
            raise ValueError(f"table is not failed-run rollback eligible: {value}")
        return value

    @field_validator("physical_table")
    @classmethod
    def validate_physical_table(cls, value: str) -> str:
        return _safe_identifier(value, label="physical_table")

    @field_validator("protected_refs")
    @classmethod
    def validate_protected_refs(cls, value: dict[str, int]) -> dict[str, int]:
        normalized: dict[str, int] = {}
        for name, snapshot_id in value.items():
            reference = name.strip()
            if not reference or len(reference) > 255:
                raise ValueError("protected Iceberg ref name must be bounded")
            if isinstance(snapshot_id, bool) or snapshot_id <= 0:
                raise ValueError("protected Iceberg ref snapshot must be positive")
            normalized[reference] = snapshot_id
        return dict(sorted(normalized.items()))

    @model_validator(mode="after")
    def validate_inventory(self) -> Self:
        ids = [snapshot.snapshot_id for snapshot in self.snapshots]
        if len(ids) != len(set(ids)):
            raise ValueError(f"{self.logical_table} snapshot inventory has duplicates")
        current = [snapshot for snapshot in self.snapshots if snapshot.is_current]
        if self.snapshots and len(current) != 1:
            raise ValueError(
                f"{self.logical_table} snapshot inventory requires one current head"
            )
        if current and not current[0].is_current_ancestor:
            raise ValueError(
                f"{self.logical_table} current snapshot must be a current ancestor"
            )
        missing_refs = sorted(set(self.protected_refs.values()) - set(ids))
        if missing_refs:
            raise ValueError(
                f"{self.logical_table} protected refs are absent from inventory"
            )
        return self


class FailedRunRollbackAction(V2ContractModel):
    logical_table: str
    physical_table: str
    expected_head_snapshot_id: int = Field(gt=0)
    target_snapshot_id: int | None = Field(default=None, gt=0)
    owned_snapshot_ids: tuple[int, ...]
    latest_owned_at: str
    protected_refs: dict[str, int] = Field(default_factory=dict)
    already_rolled_back: bool = False
    sql: str

    @field_validator("logical_table")
    @classmethod
    def validate_logical_table(cls, value: str) -> str:
        if value not in FAILED_RUN_ROLLBACK_TABLES:
            raise ValueError(f"rollback action is not failed-run eligible: {value}")
        return value

    @field_validator("physical_table")
    @classmethod
    def validate_physical_table(cls, value: str) -> str:
        return _safe_identifier(value, label="physical_table")

    @field_validator("owned_snapshot_ids")
    @classmethod
    def validate_owned_snapshot_ids(cls, value: tuple[int, ...]) -> tuple[int, ...]:
        if not value or any(isinstance(item, bool) or item <= 0 for item in value):
            raise ValueError("rollback action requires positive owned snapshots")
        if len(value) != len(set(value)):
            raise ValueError("rollback owned snapshots must be unique")
        return value

    @field_validator("latest_owned_at")
    @classmethod
    def validate_latest_owned_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @model_validator(mode="after")
    def validate_action(self) -> Self:
        if self.expected_head_snapshot_id != self.owned_snapshot_ids[-1]:
            raise ValueError("rollback expected head must be the last owned snapshot")
        if set(self.protected_refs.values()).intersection(self.owned_snapshot_ids):
            raise ValueError("rollback cannot move an externally protected snapshot")
        return self


class FailedRunRollbackPlan(V2ContractModel):
    schema_version: Literal["1.0"] = "1.0"
    plan_id: str
    run_id: str
    catalog_name: str
    namespace: str
    identity_generation_id: str | None = None
    table_mapping: dict[str, str]
    dry_run: bool = True
    planned_at: str
    no_commit_verified: bool = True
    allow_legacy_parent_inference: bool = False
    references_reviewed: bool = False
    actions: tuple[FailedRunRollbackAction, ...]

    @field_validator("plan_id", "run_id")
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("catalog_name", "namespace")
    @classmethod
    def validate_identifier(cls, value: str) -> str:
        return _safe_identifier(value, label="rollback identifier")

    @field_validator("identity_generation_id")
    @classmethod
    def validate_generation(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return require_identity_generation_id(value)

    @field_validator("planned_at")
    @classmethod
    def validate_planned_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @model_validator(mode="after")
    def validate_plan(self, info: ValidationInfo) -> Self:
        validate_community_table_mapping(
            self.table_mapping,
            identity_generation_id=self.identity_generation_id,
        )
        if (
            self.allow_legacy_parent_inference
            and self.identity_generation_id is not None
        ):
            raise ValueError(
                "parent inference is allowed only for legacy fixed-table runs"
            )
        if not self.no_commit_verified:
            raise ValueError("failed-run rollback requires proof that no commit exists")
        if not self.actions:
            raise ValueError("failed-run rollback found no run-owned snapshots")
        if not self.dry_run and not self.references_reviewed:
            raise ValueError(
                "executing failed-run rollback requires reviewed protected refs"
            )
        if not (info.context or {}).get("skip_identity"):
            expected = deterministic_key(
                "iceberg-failed-run-rollback-plan-v1",
                _failed_run_plan_identity(self),
            )
            if self.plan_id != expected:
                raise ValueError("failed-run rollback plan_id does not match contents")
        return self


class FailedRunRollbackExecutor(Protocol):
    def commit_exists(self, run_id: str) -> bool: ...

    def current_state(
        self,
        action: FailedRunRollbackAction,
    ) -> tuple[int | None, str | None]: ...

    def protected_refs(
        self,
        action: FailedRunRollbackAction,
    ) -> Mapping[str, int]: ...

    def rollback(
        self,
        action: FailedRunRollbackAction,
        *,
        run_id: str,
    ) -> None: ...


def rollback_to_snapshot_sql(
    *,
    catalog_name: str,
    namespace: str,
    physical_table: str,
    snapshot_id: int,
) -> str:
    if isinstance(snapshot_id, bool) or snapshot_id <= 0:
        raise ValueError("rollback target snapshot must be positive")
    catalog = _safe_identifier(catalog_name, label="catalog_name")
    current_namespace = _safe_identifier(namespace, label="namespace")
    physical = _safe_identifier(physical_table, label="physical_table")
    return (
        f"CALL `{catalog}`.system.rollback_to_snapshot("
        f"table => '{current_namespace}.{physical}', "
        f"snapshot_id => {snapshot_id})"
    )


def reset_failed_run_rows_sql(
    *,
    catalog_name: str,
    namespace: str,
    physical_table: str,
    run_id: str,
) -> str:
    catalog = _safe_identifier(catalog_name, label="catalog_name")
    current_namespace = _safe_identifier(namespace, label="namespace")
    physical = _safe_identifier(physical_table, label="physical_table")
    failed_run = require_sha256(run_id, label="run_id")
    return (
        f"DELETE FROM `{catalog}`.`{current_namespace}`.`{physical}` "
        f"WHERE run_id = '{failed_run}'"
    )


def _failed_run_plan_identity(plan: FailedRunRollbackPlan) -> dict[str, Any]:
    return {
        "schemaVersion": plan.schema_version,
        "runId": plan.run_id,
        "catalogName": plan.catalog_name,
        "namespace": plan.namespace,
        "identityGenerationId": plan.identity_generation_id,
        "tableMapping": plan.table_mapping,
        "dryRun": plan.dry_run,
        "plannedAt": plan.planned_at,
        "noCommitVerified": plan.no_commit_verified,
        "allowLegacyParentInference": plan.allow_legacy_parent_inference,
        "referencesReviewed": plan.references_reviewed,
        "actions": [
            action.model_dump(mode="json", by_alias=True, exclude_none=True)
            for action in plan.actions
        ],
    }


def _owned_snapshot_chain(
    state: FailedRunTableState,
    *,
    run_id: str,
    allow_legacy_parent_inference: bool,
) -> tuple[tuple[FailedRunSnapshotState, ...], int | None, bool] | None:
    current = next(
        (snapshot for snapshot in state.snapshots if snapshot.is_current),
        None,
    )
    current_id = None if current is None else current.snapshot_id
    all_owned = {
        snapshot.snapshot_id: snapshot
        for snapshot in state.snapshots
        if snapshot.owner_run_id == run_id
    }
    children_by_parent: dict[int, list[int]] = {}
    for snapshot in all_owned.values():
        if snapshot.parent_snapshot_id is not None:
            children_by_parent.setdefault(
                snapshot.parent_snapshot_id,
                [],
            ).append(snapshot.snapshot_id)

    def component_from(
        root: FailedRunSnapshotState,
    ) -> dict[int, FailedRunSnapshotState]:
        component_ids = {root.snapshot_id}
        pending = [root.snapshot_id]
        while pending:
            parent_id = pending.pop()
            for child_id in children_by_parent.get(parent_id, ()):
                if child_id not in component_ids:
                    component_ids.add(child_id)
                    pending.append(child_id)
        return {
            snapshot_id: snapshot
            for snapshot_id, snapshot in all_owned.items()
            if snapshot_id in component_ids
        }

    rollback_markers = sorted(
        (
            snapshot
            for snapshot in state.snapshots
            if snapshot.rollback_run_id == run_id
        ),
        key=lambda snapshot: (
            snapshot.sequence_number,
            parse_rfc3339(snapshot.committed_at),
            snapshot.snapshot_id,
        ),
    )
    active_cutoff = -1 if not rollback_markers else rollback_markers[-1].sequence_number
    active_owned = {
        snapshot_id: snapshot
        for snapshot_id, snapshot in all_owned.items()
        if snapshot.is_current_ancestor and snapshot.sequence_number > active_cutoff
    }
    if active_owned:
        owned = active_owned
    elif roots_at_current := [
        snapshot
        for snapshot in all_owned.values()
        if snapshot.parent_snapshot_id == current_id
        and snapshot.sequence_number > active_cutoff
    ]:
        owned = component_from(
            max(
                roots_at_current,
                key=lambda snapshot: (
                    snapshot.sequence_number,
                    parse_rfc3339(snapshot.committed_at),
                    snapshot.snapshot_id,
                ),
            )
        )
    elif rollback_markers:
        previous_cutoff = (
            -1 if len(rollback_markers) == 1 else rollback_markers[-2].sequence_number
        )
        owned = {
            snapshot_id: snapshot
            for snapshot_id, snapshot in all_owned.items()
            if previous_cutoff
            < snapshot.sequence_number
            < rollback_markers[-1].sequence_number
        }
    else:
        current_ancestor_owned = {
            snapshot_id: snapshot
            for snapshot_id, snapshot in all_owned.items()
            if snapshot.is_current_ancestor
        }
        if current_ancestor_owned:
            owned = current_ancestor_owned
        else:
            roots_at_current = [
                snapshot
                for snapshot in all_owned.values()
                if snapshot.parent_snapshot_id == current_id
            ]
            if not roots_at_current:
                owned = all_owned
            else:
                root = max(
                    roots_at_current,
                    key=lambda snapshot: (
                        snapshot.sequence_number,
                        parse_rfc3339(snapshot.committed_at),
                        snapshot.snapshot_id,
                    ),
                )
                owned = component_from(root)
    if not owned:
        return None
    for snapshot in owned.values():
        if (
            not snapshot.parent_journal_recorded
            or snapshot.journal_parent_snapshot_id != snapshot.parent_snapshot_id
        ) and not allow_legacy_parent_inference:
            raise ValueError(
                f"{state.logical_table} run-owned snapshot parent journal is invalid"
            )
    children = {
        snapshot.parent_snapshot_id
        for snapshot in owned.values()
        if snapshot.parent_snapshot_id in owned
    }
    tips = sorted(set(owned) - children)
    roots = sorted(
        snapshot.snapshot_id
        for snapshot in owned.values()
        if snapshot.parent_snapshot_id not in owned
    )
    if len(tips) != 1 or len(roots) != 1:
        raise ValueError(
            f"{state.logical_table} run-owned snapshots are not one linear chain"
        )
    reverse_chain: list[FailedRunSnapshotState] = []
    cursor_id: int | None = tips[0]
    while cursor_id in owned:
        snapshot = owned[cursor_id]
        reverse_chain.append(snapshot)
        cursor_id = snapshot.parent_snapshot_id
    chain = tuple(reversed(reverse_chain))
    if len(chain) != len(owned) or chain[0].snapshot_id != roots[0]:
        raise ValueError(
            f"{state.logical_table} run-owned snapshots are not contiguous"
        )
    target = chain[0].parent_snapshot_id
    already_rolled_back = current_id == target or (
        target is None and current is not None and current.rollback_run_id == run_id
    )
    if current_id != chain[-1].snapshot_id and not already_rolled_back:
        raise ValueError(
            f"{state.logical_table} current head changed after the failed run"
        )
    return chain, target, already_rolled_back


def build_failed_run_rollback_plan(
    *,
    catalog_name: str,
    namespace: str,
    run_id: str,
    identity_generation_id: str | None,
    table_mapping: Mapping[str, str],
    table_states: Sequence[FailedRunTableState],
    commit_exists: bool,
    planned_at: str,
    dry_run: bool = True,
    allow_legacy_parent_inference: bool = False,
    references_reviewed: bool = False,
) -> FailedRunRollbackPlan:
    """Plan a reverse-order rollback after proving every safety guard."""

    failed_run = require_sha256(run_id, label="run_id")
    generation = (
        None
        if identity_generation_id is None
        else require_identity_generation_id(identity_generation_id)
    )
    mapping = validate_community_table_mapping(
        table_mapping,
        identity_generation_id=generation,
    )
    if commit_exists:
        raise ValueError("committed runs must never be rolled back")
    if len({state.logical_table for state in table_states}) != len(table_states):
        raise ValueError("failed-run rollback table states contain duplicates")

    actions: list[FailedRunRollbackAction] = []
    for state in table_states:
        if state.physical_table != mapping[state.logical_table]:
            raise ValueError(
                f"{state.logical_table} physical table differs from generation mapping"
            )
        resolved = _owned_snapshot_chain(
            state,
            run_id=failed_run,
            allow_legacy_parent_inference=allow_legacy_parent_inference,
        )
        if resolved is None:
            continue
        chain, target, already_rolled_back = resolved
        owned_ids = tuple(snapshot.snapshot_id for snapshot in chain)
        protected_owned = sorted(
            set(state.protected_refs.values()).intersection(owned_ids)
        )
        if protected_owned:
            raise ValueError(
                f"{state.logical_table} run-owned snapshots are protected by refs: "
                + ", ".join(str(item) for item in protected_owned)
            )
        sql = (
            rollback_to_snapshot_sql(
                catalog_name=catalog_name,
                namespace=namespace,
                physical_table=state.physical_table,
                snapshot_id=target,
            )
            if target is not None
            else reset_failed_run_rows_sql(
                catalog_name=catalog_name,
                namespace=namespace,
                physical_table=state.physical_table,
                run_id=failed_run,
            )
        )
        actions.append(
            FailedRunRollbackAction(
                logical_table=state.logical_table,
                physical_table=state.physical_table,
                expected_head_snapshot_id=chain[-1].snapshot_id,
                target_snapshot_id=target,
                owned_snapshot_ids=owned_ids,
                latest_owned_at=chain[-1].committed_at,
                protected_refs=state.protected_refs,
                already_rolled_back=already_rolled_back,
                sql=sql,
            )
        )
    actions.sort(
        key=lambda action: (
            parse_rfc3339(action.latest_owned_at),
            action.expected_head_snapshot_id,
            action.logical_table,
        ),
        reverse=True,
    )
    provisional = FailedRunRollbackPlan.model_validate(
        {
            "plan_id": _ZERO_DIGEST,
            "run_id": failed_run,
            "catalog_name": catalog_name,
            "namespace": namespace,
            "identity_generation_id": generation,
            "table_mapping": mapping,
            "dry_run": dry_run,
            "planned_at": require_rfc3339(planned_at),
            "no_commit_verified": True,
            "allow_legacy_parent_inference": allow_legacy_parent_inference,
            "references_reviewed": references_reviewed,
            "actions": tuple(actions),
        },
        context={"skip_identity": True},
    )
    normalized = provisional.model_dump(mode="python")
    normalized["plan_id"] = deterministic_key(
        "iceberg-failed-run-rollback-plan-v1",
        _failed_run_plan_identity(provisional),
    )
    return FailedRunRollbackPlan.model_validate(normalized)


def execute_failed_run_rollback_plan(
    plan: FailedRunRollbackPlan,
    executor: FailedRunRollbackExecutor,
) -> tuple[dict[str, Any], ...]:
    """Recheck mutable guards and execute one audited reverse-order plan."""

    if plan.dry_run:
        raise ValueError("dry-run failed-run rollback plans cannot be executed")
    if not plan.references_reviewed:
        raise ValueError("failed-run rollback references were not reviewed")
    if executor.commit_exists(plan.run_id):
        raise RuntimeError("run acquired a commit marker after rollback planning")

    audit: list[dict[str, Any]] = []
    for action in plan.actions:
        if executor.commit_exists(plan.run_id):
            raise RuntimeError("run acquired a commit marker during rollback")
        current_id, rollback_run_id = executor.current_state(action)
        already_rolled_back = current_id == action.target_snapshot_id or (
            action.target_snapshot_id is None and rollback_run_id == plan.run_id
        )
        current_refs = dict(sorted(executor.protected_refs(action).items()))
        if current_refs != action.protected_refs:
            raise RuntimeError(
                f"{action.logical_table} protected refs changed after planning"
            )
        if set(current_refs.values()).intersection(action.owned_snapshot_ids):
            raise RuntimeError(
                f"{action.logical_table} acquired a protected run-owned snapshot"
            )
        if already_rolled_back:
            audit.append(
                {
                    "logicalTable": action.logical_table,
                    "physicalTable": action.physical_table,
                    "status": "ALREADY_ROLLED_BACK",
                    "targetSnapshotId": action.target_snapshot_id,
                }
            )
            continue
        if current_id != action.expected_head_snapshot_id:
            raise RuntimeError(
                f"{action.logical_table} current head changed after planning"
            )
        if executor.current_state(action) != (current_id, rollback_run_id):
            raise RuntimeError(
                f"{action.logical_table} current head changed during guard checks"
            )
        executor.rollback(action, run_id=plan.run_id)
        resulting_id, resulting_rollback_run_id = executor.current_state(action)
        reached_target = resulting_id == action.target_snapshot_id or (
            action.target_snapshot_id is None
            and resulting_rollback_run_id == plan.run_id
        )
        if not reached_target:
            raise RuntimeError(
                f"{action.logical_table} rollback target could not be verified"
            )
        audit.append(
            {
                "logicalTable": action.logical_table,
                "physicalTable": action.physical_table,
                "status": "ROLLED_BACK",
                "fromSnapshotId": action.expected_head_snapshot_id,
                "targetSnapshotId": action.target_snapshot_id,
            }
        )
    return tuple(audit)
