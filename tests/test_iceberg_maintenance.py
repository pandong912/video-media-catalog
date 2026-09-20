from __future__ import annotations

import pytest

from video_media_catalog.iceberg_maintenance import (
    MIN_RETENTION_DAYS,
    IcebergSnapshotState,
    MaintenanceOperation,
    build_iceberg_maintenance_plan,
    execute_iceberg_maintenance_plan,
    expire_snapshots_sql,
    remove_orphan_files_sql,
    rewrite_data_files_sql,
    rewrite_manifests_sql,
)
from video_media_catalog.iceberg_maintenance_cli import build_parser

TABLE = "community_source_record"


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
