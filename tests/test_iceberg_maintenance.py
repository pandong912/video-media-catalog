from __future__ import annotations

import pytest

from video_media_catalog.community_tables import build_community_table_mapping
from video_media_catalog.iceberg_maintenance import (
    MIN_RETENTION_DAYS,
    FailedRunSnapshotState,
    FailedRunTableState,
    IcebergSnapshotState,
    MaintenanceOperation,
    build_failed_run_rollback_plan,
    build_iceberg_maintenance_plan,
    execute_failed_run_rollback_plan,
    execute_iceberg_maintenance_plan,
    expire_snapshots_sql,
    remove_orphan_files_sql,
    rewrite_data_files_sql,
    rewrite_manifests_sql,
)
from video_media_catalog.iceberg_maintenance_cli import build_parser

TABLE = "community_source_record"
FAILED_RUN = "sha256:" + ("f" * 64)
GENERATION = "catalog-2026-09"


def _snapshots() -> dict[str, tuple[IcebergSnapshotState, ...]]:
    return {
        TABLE: (
            IcebergSnapshotState(
                snapshot_id=10,
                committed_at="2026-08-01T00:00:00Z",
            ),
            IcebergSnapshotState(
                snapshot_id=20,
                committed_at="2026-09-18T00:00:00Z",
            ),
            IcebergSnapshotState(
                snapshot_id=30,
                committed_at="2026-09-19T00:00:00Z",
                is_current=True,
            ),
        )
    }


def test_sql_generators_are_allowlisted_and_auditable() -> None:
    assert "rewrite_data_files" in rewrite_data_files_sql(
        catalog_name="media",
        namespace="video_media_catalog",
        table=TABLE,
    )
    assert "rewrite_manifests" in rewrite_manifests_sql(
        catalog_name="media",
        namespace="video_media_catalog",
        table=TABLE,
    )
    expiry = expire_snapshots_sql(
        catalog_name="media",
        namespace="video_media_catalog",
        table=TABLE,
        older_than="2026-09-01T00:00:00Z",
        retain_last=5,
    )
    assert "expire_snapshots" in expiry
    assert "retain_last => 5" in expiry
    orphan = remove_orphan_files_sql(
        catalog_name="media",
        namespace="video_media_catalog",
        table=TABLE,
        older_than="2026-09-01T00:00:00Z",
    )
    assert "remove_orphan_files" in orphan
    with pytest.raises(ValueError, match="allowlisted"):
        rewrite_data_files_sql(
            catalog_name="media",
            namespace="video_media_catalog",
            table="not_owned",
        )


def test_maintenance_plan_is_dry_run_deterministic_and_protects_current() -> None:
    values = {
        "catalog_name": "media",
        "namespace": "video_media_catalog",
        "tables": (TABLE,),
        "operations": tuple(MaintenanceOperation),
        "snapshots": _snapshots(),
        "planned_at": "2026-09-20T00:00:00Z",
        "retain_last": 2,
    }
    first = build_iceberg_maintenance_plan(**values)
    second = build_iceberg_maintenance_plan(**values)
    assert first == second
    assert first.dry_run
    assert tuple(action.operation for action in first.actions) == (
        MaintenanceOperation.REWRITE_DATA_FILES,
        MaintenanceOperation.REWRITE_MANIFESTS,
        MaintenanceOperation.EXPIRE_SNAPSHOTS,
        MaintenanceOperation.REMOVE_ORPHAN_FILES,
    )
    expiry = next(
        action
        for action in first.actions
        if action.operation == MaintenanceOperation.EXPIRE_SNAPSHOTS
    )
    assert expiry.current_snapshot_id == 30
    assert expiry.candidate_snapshot_ids == (10,)
    assert 30 not in expiry.candidate_snapshot_ids
    assert all(
        action.sql.startswith("CALL `media`.system.") for action in first.actions
    )


def test_expiry_fails_closed_when_retention_reaches_referenced_snapshot() -> None:
    with pytest.raises(ValueError, match="would expire referenced"):
        build_iceberg_maintenance_plan(
            catalog_name="media",
            namespace="video_media_catalog",
            tables=(TABLE,),
            operations=(MaintenanceOperation.EXPIRE_SNAPSHOTS,),
            snapshots=_snapshots(),
            protected_snapshot_ids={TABLE: (10,)},
            planned_at="2026-09-20T00:00:00Z",
            retain_last=2,
        )


def test_destructive_execution_requires_reference_review_and_retention_floor() -> None:
    with pytest.raises(ValueError, match="retention_days"):
        build_iceberg_maintenance_plan(
            catalog_name="media",
            namespace="video_media_catalog",
            tables=(TABLE,),
            operations=(MaintenanceOperation.REWRITE_DATA_FILES,),
            snapshots=_snapshots(),
            planned_at="2026-09-20T00:00:00Z",
            retention_days=MIN_RETENTION_DAYS - 1,
        )
    with pytest.raises(ValueError, match="reviewed references"):
        build_iceberg_maintenance_plan(
            catalog_name="media",
            namespace="video_media_catalog",
            tables=(TABLE,),
            operations=(MaintenanceOperation.REMOVE_ORPHAN_FILES,),
            snapshots=_snapshots(),
            planned_at="2026-09-20T00:00:00Z",
            dry_run=False,
        )


def test_non_empty_inventory_requires_exactly_one_current_snapshot() -> None:
    states = {
        TABLE: (
            IcebergSnapshotState(
                snapshot_id=10,
                committed_at="2026-08-01T00:00:00Z",
            ),
        )
    }
    with pytest.raises(ValueError, match="requires one current"):
        build_iceberg_maintenance_plan(
            catalog_name="media",
            namespace="video_media_catalog",
            tables=(TABLE,),
            operations=(MaintenanceOperation.EXPIRE_SNAPSHOTS,),
            snapshots=states,
            planned_at="2026-09-20T00:00:00Z",
        )


def test_dry_run_plan_cannot_be_executed() -> None:
    plan = build_iceberg_maintenance_plan(
        catalog_name="media",
        namespace="video_media_catalog",
        tables=(TABLE,),
        operations=(MaintenanceOperation.REWRITE_MANIFESTS,),
        snapshots=_snapshots(),
        planned_at="2026-09-20T00:00:00Z",
    )
    with pytest.raises(ValueError, match="dry-run"):
        execute_iceberg_maintenance_plan(object(), plan)


def test_maintenance_cli_defaults_to_dry_run() -> None:
    parsed = build_parser().parse_args(
        [
            "--table",
            TABLE,
            "--operation",
            "rewrite-data-files",
            "--planned-at",
            "2026-09-20T00:00:00Z",
            "--warehouse",
            "file:///tmp/warehouse",
        ]
    )
    assert not parsed.execute
    assert not parsed.references_reviewed


def _failed_table_state(
    logical_table: str,
    *,
    owned_snapshot_id: int,
    committed_at: str,
    current_snapshot_id: int | None = None,
    protected: bool = False,
) -> FailedRunTableState:
    mapping = build_community_table_mapping(GENERATION)
    parent_id = owned_snapshot_id - 1
    current_id = (
        owned_snapshot_id if current_snapshot_id is None else current_snapshot_id
    )
    snapshots = [
        FailedRunSnapshotState(
            snapshot_id=parent_id,
            committed_at="2026-09-20T00:00:00Z",
            is_current=current_id == parent_id,
            is_current_ancestor=True,
        ),
        FailedRunSnapshotState(
            snapshot_id=owned_snapshot_id,
            parent_snapshot_id=parent_id,
            journal_parent_snapshot_id=parent_id,
            parent_journal_recorded=True,
            committed_at=committed_at,
            owner_run_id=FAILED_RUN,
            is_current=current_id == owned_snapshot_id,
            is_current_ancestor=current_id != parent_id,
        ),
    ]
    if current_id not in {parent_id, owned_snapshot_id}:
        snapshots.append(
            FailedRunSnapshotState(
                snapshot_id=current_id,
                parent_snapshot_id=owned_snapshot_id,
                committed_at="2026-09-20T00:03:00Z",
                owner_run_id="sha256:" + ("e" * 64),
                is_current=True,
                is_current_ancestor=True,
            )
        )
    return FailedRunTableState(
        logical_table=logical_table,
        physical_table=mapping[logical_table],
        snapshots=tuple(snapshots),
        protected_refs=({"tag:published": owned_snapshot_id} if protected else {}),
    )


def _failed_plan(**overrides):
    values = {
        "catalog_name": "media",
        "namespace": "video_media_catalog",
        "run_id": FAILED_RUN,
        "identity_generation_id": GENERATION,
        "table_mapping": build_community_table_mapping(GENERATION),
        "table_states": (
            _failed_table_state(
                "community_entity_ledger",
                owned_snapshot_id=20,
                committed_at="2026-09-20T00:01:00Z",
            ),
            _failed_table_state(
                "community_identity_evidence",
                owned_snapshot_id=40,
                committed_at="2026-09-20T00:02:00Z",
            ),
        ),
        "commit_exists": False,
        "planned_at": "2026-09-20T00:04:00Z",
    }
    values.update(overrides)
    return build_failed_run_rollback_plan(**values)


def test_failed_run_rollback_dry_run_is_reverse_order_and_auditable() -> None:
    plan = _failed_plan()

    assert plan.dry_run
    assert [action.logical_table for action in plan.actions] == [
        "community_identity_evidence",
        "community_entity_ledger",
    ]
    assert [action.target_snapshot_id for action in plan.actions] == [39, 19]
    assert all("rollback_to_snapshot" in action.sql for action in plan.actions)


def test_failed_run_first_snapshot_uses_audited_empty_reset() -> None:
    mapping = build_community_table_mapping(GENERATION)
    state = FailedRunTableState(
        logical_table="community_entity_ledger",
        physical_table=mapping["community_entity_ledger"],
        snapshots=(
            FailedRunSnapshotState(
                snapshot_id=20,
                parent_journal_recorded=True,
                committed_at="2026-09-20T00:01:00Z",
                owner_run_id=FAILED_RUN,
                is_current=True,
                is_current_ancestor=True,
            ),
        ),
    )
    plan = _failed_plan(table_states=(state,))

    assert plan.actions[0].target_snapshot_id is None
    assert plan.actions[0].sql.startswith("DELETE FROM")
    assert f"run_id = '{FAILED_RUN}'" in plan.actions[0].sql


def test_legacy_rollback_infers_parents_and_cleans_manifest_last() -> None:
    mapping = build_community_table_mapping()

    def legacy_state(
        logical_table: str,
        *,
        parent_snapshot_id: int,
        owned_snapshot_id: int,
        committed_at: str,
    ) -> FailedRunTableState:
        return FailedRunTableState(
            logical_table=logical_table,
            physical_table=mapping[logical_table],
            snapshots=(
                FailedRunSnapshotState(
                    snapshot_id=parent_snapshot_id,
                    committed_at="2026-09-20T00:00:00Z",
                    is_current_ancestor=True,
                ),
                FailedRunSnapshotState(
                    snapshot_id=owned_snapshot_id,
                    parent_snapshot_id=parent_snapshot_id,
                    committed_at=committed_at,
                    owner_run_id=FAILED_RUN,
                    is_current=True,
                    is_current_ancestor=True,
                ),
            ),
        )

    states = (
        legacy_state(
            "community_ingest_run",
            parent_snapshot_id=10,
            owned_snapshot_id=11,
            committed_at="2026-09-20T00:01:00Z",
        ),
        legacy_state(
            "community_external_id_index",
            parent_snapshot_id=20,
            owned_snapshot_id=21,
            committed_at="2026-09-20T00:02:00Z",
        ),
        legacy_state(
            "community_entity_ledger",
            parent_snapshot_id=30,
            owned_snapshot_id=31,
            committed_at="2026-09-20T00:03:00Z",
        ),
    )
    values = {
        "catalog_name": "media",
        "namespace": "video_media_catalog",
        "run_id": FAILED_RUN,
        "identity_generation_id": None,
        "table_mapping": mapping,
        "table_states": states,
        "commit_exists": False,
        "planned_at": "2026-09-20T00:04:00Z",
    }

    with pytest.raises(ValueError, match="parent journal is invalid"):
        build_failed_run_rollback_plan(**values)

    plan = build_failed_run_rollback_plan(
        **values,
        allow_legacy_parent_inference=True,
    )
    assert plan.identity_generation_id is None
    assert plan.allow_legacy_parent_inference
    assert [action.logical_table for action in plan.actions] == [
        "community_entity_ledger",
        "community_external_id_index",
        "community_ingest_run",
    ]
    assert [action.target_snapshot_id for action in plan.actions] == [30, 20, 10]


def test_rollback_marker_excludes_an_earlier_cleaned_attempt() -> None:
    mapping = build_community_table_mapping(GENERATION)
    state = FailedRunTableState(
        logical_table="community_entity_ledger",
        physical_table=mapping["community_entity_ledger"],
        snapshots=(
            FailedRunSnapshotState(
                snapshot_id=20,
                sequence_number=1,
                parent_journal_recorded=True,
                committed_at="2026-09-20T00:01:00Z",
                owner_run_id=FAILED_RUN,
                is_current_ancestor=True,
            ),
            FailedRunSnapshotState(
                snapshot_id=25,
                sequence_number=2,
                parent_snapshot_id=20,
                committed_at="2026-09-20T00:02:00Z",
                rollback_run_id=FAILED_RUN,
                is_current_ancestor=True,
            ),
            FailedRunSnapshotState(
                snapshot_id=30,
                sequence_number=3,
                parent_snapshot_id=25,
                journal_parent_snapshot_id=25,
                parent_journal_recorded=True,
                committed_at="2026-09-20T00:03:00Z",
                owner_run_id=FAILED_RUN,
                is_current=True,
                is_current_ancestor=True,
            ),
        ),
    )
    plan = _failed_plan(table_states=(state,))

    assert plan.actions[0].owned_snapshot_ids == (30,)
    assert plan.actions[0].target_snapshot_id == 25


def test_failed_run_rollback_rejects_commit_protected_and_changed_head() -> None:
    with pytest.raises(ValueError, match="committed runs"):
        _failed_plan(commit_exists=True)

    with pytest.raises(ValueError, match="protected by refs"):
        _failed_plan(
            table_states=(
                _failed_table_state(
                    "community_entity_ledger",
                    owned_snapshot_id=20,
                    committed_at="2026-09-20T00:01:00Z",
                    protected=True,
                ),
            )
        )

    with pytest.raises(ValueError, match="current head changed"):
        _failed_plan(
            table_states=(
                _failed_table_state(
                    "community_entity_ledger",
                    owned_snapshot_id=20,
                    committed_at="2026-09-20T00:01:00Z",
                    current_snapshot_id=30,
                ),
            )
        )


class _RollbackExecutor:
    def __init__(self, plan, *, committed: bool = False) -> None:
        self.committed = committed
        self.states = {
            action.physical_table: (
                action.expected_head_snapshot_id,
                None,
            )
            for action in plan.actions
        }
        self.executed: list[str] = []

    def commit_exists(self, _run_id: str) -> bool:
        return self.committed

    def current_state(self, action):
        return self.states[action.physical_table]

    def protected_refs(self, action):
        return action.protected_refs

    def rollback(self, action, *, run_id: str) -> None:
        self.executed.append(action.logical_table)
        self.states[action.physical_table] = (
            action.target_snapshot_id,
            run_id if action.target_snapshot_id is None else None,
        )


def test_failed_run_rollback_execution_rechecks_commit_and_is_idempotent() -> None:
    plan = _failed_plan(dry_run=False, references_reviewed=True)
    committed = _RollbackExecutor(plan, committed=True)
    with pytest.raises(RuntimeError, match="commit marker"):
        execute_failed_run_rollback_plan(plan, committed)

    executor = _RollbackExecutor(plan)
    first = execute_failed_run_rollback_plan(plan, executor)
    second = execute_failed_run_rollback_plan(plan, executor)

    assert [item["status"] for item in first] == ["ROLLED_BACK", "ROLLED_BACK"]
    assert [item["status"] for item in second] == [
        "ALREADY_ROLLED_BACK",
        "ALREADY_ROLLED_BACK",
    ]
    assert executor.executed == [
        "community_identity_evidence",
        "community_entity_ledger",
    ]


def test_rollback_cli_exposes_failed_run_two_phase_command() -> None:
    parsed = build_parser().parse_args(
        [
            "rollback-failed-run",
            "--run-id",
            FAILED_RUN,
            "--identity-generation-id",
            GENERATION,
            "--planned-at",
            "2026-09-20T00:04:00Z",
            "--warehouse",
            "file:///tmp/warehouse",
        ]
    )
    assert parsed.command == "rollback-failed-run"
    assert not parsed.execute

    legacy = build_parser().parse_args(
        [
            "rollback-failed-run",
            "--run-id",
            FAILED_RUN,
            "--allow-legacy-parent-inference",
            "--planned-at",
            "2026-09-20T00:04:00Z",
            "--warehouse",
            "file:///tmp/warehouse",
        ]
    )
    assert legacy.identity_generation_id is None
    assert legacy.allow_legacy_parent_inference
