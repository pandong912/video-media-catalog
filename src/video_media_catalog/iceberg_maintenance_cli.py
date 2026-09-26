"""CLI for dry-run-first Iceberg maintenance."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

from video_media_catalog.canonical import canonical_json
from video_media_catalog.community_iceberg import (
    RUN_PARENT_SNAPSHOT_PROPERTY,
    RUN_SNAPSHOT_PROPERTY,
    CommunityCatalogTables,
)
from video_media_catalog.community_tables import (
    build_community_table_mapping,
    require_identity_generation_id,
)
from video_media_catalog.iceberg import (
    FAILED_RUN_ROLLBACK_PROPERTY,
    CatalogConfig,
    execute_iceberg_sql,
)
from video_media_catalog.iceberg_maintenance import (
    ALLOWED_MAINTENANCE_TABLES,
    DEFAULT_RETAIN_LAST,
    DEFAULT_RETENTION_DAYS,
    FAILED_RUN_ROLLBACK_TABLES,
    FailedRunRollbackAction,
    FailedRunSnapshotState,
    FailedRunTableState,
    IcebergSnapshotState,
    MaintenanceOperation,
    build_failed_run_rollback_plan,
    build_iceberg_maintenance_plan,
    execute_failed_run_rollback_plan,
    execute_iceberg_maintenance_plan,
)
from video_media_catalog.v2_contracts import require_rfc3339, require_sha256


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="video-media-catalog-iceberg-maintenance",
        description=(
            "Build an auditable Iceberg maintenance plan; execution is opt-in."
        ),
    )
    parser.add_argument(
        "command",
        nargs="?",
        choices=("maintenance", "rollback-failed-run"),
        default="maintenance",
    )
    parser.add_argument(
        "--table",
        dest="tables",
        action="append",
        default=[],
        choices=sorted(ALLOWED_MAINTENANCE_TABLES),
    )
    parser.add_argument(
        "--operation",
        dest="operations",
        action="append",
        default=[],
        choices=[item.value for item in MaintenanceOperation],
    )
    parser.add_argument("--planned-at")
    parser.add_argument("--retention-days", type=int, default=DEFAULT_RETENTION_DAYS)
    parser.add_argument(
        "--orphan-retention-days",
        type=int,
        default=DEFAULT_RETENTION_DAYS,
    )
    parser.add_argument("--retain-last", type=int, default=DEFAULT_RETAIN_LAST)
    parser.add_argument(
        "--protected-snapshot",
        dest="protected_snapshots",
        action="append",
        default=[],
        metavar="TABLE=SNAPSHOT_ID",
    )
    parser.add_argument(
        "--references-reviewed",
        action="store_true",
        help="confirm the external epoch snapshot reference inventory is complete",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="execute the generated plan; omitted means dry-run",
    )
    parser.add_argument("--run-id")
    parser.add_argument("--identity-generation-id")
    parser.add_argument(
        "--allow-legacy-parent-inference",
        action="store_true",
        help=(
            "allow Iceberg parent lineage to replace missing snapshot journals "
            "only for a legacy fixed-table run"
        ),
    )
    parser.add_argument("--catalog-name", default="media")
    parser.add_argument("--namespace", default="video_media_catalog")
    parser.add_argument(
        "--catalog-type",
        choices=("hadoop", "glue"),
        default="glue",
    )
    parser.add_argument("--warehouse")
    parser.add_argument("--aws-region")
    parser.add_argument("--s3-endpoint")
    parser.add_argument("--s3-path-style-access", action="store_true")
    parser.add_argument(
        "--s3-credentials-provider",
        choices=("web-identity", "default"),
        default="web-identity",
    )
    parser.add_argument("--master")
    parser.add_argument("--app-name", default="media-catalog-iceberg-maintenance")
    parser.add_argument("--spark-packages")
    return parser


def _parse_protected_snapshots(values: Sequence[str]) -> dict[str, tuple[int, ...]]:
    parsed: dict[str, set[int]] = {}
    for value in values:
        table, separator, raw_snapshot_id = value.partition("=")
        if not separator or table not in ALLOWED_MAINTENANCE_TABLES:
            raise ValueError("protected snapshot must use ALLOWLISTED_TABLE=ID")
        try:
            snapshot_id = int(raw_snapshot_id)
        except ValueError as exc:
            raise ValueError("protected snapshot ID must be an integer") from exc
        if snapshot_id <= 0:
            raise ValueError("protected snapshot ID must be positive")
        if snapshot_id in parsed.setdefault(table, set()):
            raise ValueError(f"duplicate protected snapshot: {table}={snapshot_id}")
        parsed[table].add(snapshot_id)
    return {table: tuple(sorted(items)) for table, items in sorted(parsed.items())}


def _rfc3339(value: Any) -> str:
    if isinstance(value, datetime):
        normalized = (
            value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
        )
        return normalized.isoformat().replace("+00:00", "Z")
    return require_rfc3339(str(value))


def _snapshot_inventory(
    spark: Any,
    *,
    config: CatalogConfig,
    table: str,
) -> tuple[IcebergSnapshotState, ...]:
    identifier = f"`{config.catalog_name}`.`{config.namespace}`.`{table}`"
    current_rows = spark.sql(
        f"""
        SELECT snapshot_id
        FROM {identifier}.history
        ORDER BY made_current_at DESC
        LIMIT 1
        """
    ).collect()
    current_id = None if not current_rows else int(current_rows[0]["snapshot_id"])
    rows = spark.sql(
        f"""
        SELECT snapshot_id, committed_at
        FROM {identifier}.snapshots
        ORDER BY committed_at, snapshot_id
        """
    ).collect()
    return tuple(
        IcebergSnapshotState(
            snapshot_id=int(row["snapshot_id"]),
            committed_at=_rfc3339(row["committed_at"]),
            is_current=int(row["snapshot_id"]) == current_id,
        )
        for row in rows
    )


def _iceberg_referenced_snapshot_ids(
    spark: Any,
    *,
    config: CatalogConfig,
    table: str,
) -> tuple[int, ...]:
    identifier = f"`{config.catalog_name}`.`{config.namespace}`.`{table}`"
    rows = spark.sql(
        f"""
        SELECT snapshot_id
        FROM {identifier}.refs
        """
    ).collect()
    return tuple(sorted({int(row["snapshot_id"]) for row in rows}))


def _rollback_protected_refs(
    spark: Any,
    *,
    config: CatalogConfig,
    physical_table: str,
    declared_snapshot_ids: Sequence[int] = (),
) -> dict[str, int]:
    identifier = f"`{config.catalog_name}`.`{config.namespace}`.`{physical_table}`"
    rows = spark.sql(
        f"""
        SELECT name, type, snapshot_id
        FROM {identifier}.refs
        """
    ).collect()
    protected = {
        f"{str(row['type']).lower()}:{row['name']}": int(row["snapshot_id"])
        for row in rows
        if str(row["name"]) != "main"
    }
    for snapshot_id in declared_snapshot_ids:
        protected[f"declared:{snapshot_id}"] = snapshot_id
    return dict(sorted(protected.items()))


def _rollback_table_state(
    spark: Any,
    *,
    config: CatalogConfig,
    logical_table: str,
    physical_table: str,
    run_id: str,
    allow_legacy_control_inference: bool,
    declared_snapshot_ids: Sequence[int] = (),
) -> FailedRunTableState:
    identifier = f"`{config.catalog_name}`.`{config.namespace}`.`{physical_table}`"
    current_rows = spark.sql(
        f"""
        SELECT snapshot_id
        FROM {identifier}.history
        ORDER BY made_current_at DESC
        LIMIT 1
        """
    ).collect()
    current_id = None if not current_rows else int(current_rows[0]["snapshot_id"])
    ancestor_rows = spark.sql(
        f"""
        SELECT snapshot_id
        FROM {identifier}.history
        WHERE is_current_ancestor
        """
    ).collect()
    current_ancestor_ids = {int(row["snapshot_id"]) for row in ancestor_rows}
    rows = spark.sql(
        f"""
        SELECT
            snapshot_id,
            parent_id,
            committed_at,
            summary['{RUN_SNAPSHOT_PROPERTY}'] AS owner_run_id,
            summary['{RUN_PARENT_SNAPSHOT_PROPERTY}'] AS journal_parent_id,
            summary['{FAILED_RUN_ROLLBACK_PROPERTY}'] AS rollback_run_id
        FROM {identifier}.snapshots
        ORDER BY committed_at, snapshot_id
        """
    ).collect()
    run_presence: dict[int, bool] = {}

    def snapshot_contains_run(snapshot_id: int | None) -> bool:
        if snapshot_id is None:
            return False
        if snapshot_id not in run_presence:
            run_presence[snapshot_id] = bool(
                spark.read.format("iceberg")
                .option("snapshot-id", str(snapshot_id))
                .load(f"{config.catalog_name}.{config.namespace}.{physical_table}")
                .where(f"run_id = '{run_id}'")
                .limit(1)
                .count()
            )
        return run_presence[snapshot_id]

    snapshots = []
    for sequence_number, row in enumerate(rows, start=1):
        snapshot_id = int(row["snapshot_id"])
        parent_snapshot_id = None if row["parent_id"] is None else int(row["parent_id"])
        raw_journal_parent = row["journal_parent_id"]
        journal_recorded = raw_journal_parent is not None
        journal_parent_id = (
            None if raw_journal_parent in {None, "none"} else int(raw_journal_parent)
        )
        owner_run_id = None if row["owner_run_id"] is None else str(row["owner_run_id"])
        if (
            allow_legacy_control_inference
            and logical_table == "community_ingest_run"
            and owner_run_id is None
            and snapshot_contains_run(snapshot_id)
            and not snapshot_contains_run(parent_snapshot_id)
        ):
            owner_run_id = run_id
        snapshots.append(
            FailedRunSnapshotState(
                snapshot_id=snapshot_id,
                sequence_number=sequence_number,
                parent_snapshot_id=parent_snapshot_id,
                journal_parent_snapshot_id=journal_parent_id,
                parent_journal_recorded=journal_recorded,
                committed_at=_rfc3339(row["committed_at"]),
                owner_run_id=owner_run_id,
                rollback_run_id=(
                    None
                    if row["rollback_run_id"] is None
                    else str(row["rollback_run_id"])
                ),
                is_current=int(row["snapshot_id"]) == current_id,
                is_current_ancestor=(int(row["snapshot_id"]) in current_ancestor_ids),
            )
        )
    return FailedRunTableState(
        logical_table=logical_table,
        physical_table=physical_table,
        snapshots=tuple(snapshots),
        protected_refs=_rollback_protected_refs(
            spark,
            config=config,
            physical_table=physical_table,
            declared_snapshot_ids=declared_snapshot_ids,
        ),
    )


class _SparkFailedRunRollbackExecutor:
    def __init__(
        self,
        spark: Any,
        *,
        config: CatalogConfig,
        tables: CommunityCatalogTables,
        declared_snapshots: dict[str, tuple[int, ...]],
        run_id: str,
        allow_legacy_parent_inference: bool,
    ) -> None:
        self.spark = spark
        self.config = config
        self.tables = tables
        self.declared_snapshots = declared_snapshots
        self.run_id = run_id
        self.allow_legacy_parent_inference = allow_legacy_parent_inference

    def commit_exists(self, run_id: str) -> bool:
        return self.tables.read_commit(run_id) is not None

    def _state(self, action: FailedRunRollbackAction) -> FailedRunTableState:
        return _rollback_table_state(
            self.spark,
            config=self.config,
            logical_table=action.logical_table,
            physical_table=action.physical_table,
            run_id=self.run_id,
            allow_legacy_control_inference=self.allow_legacy_parent_inference,
            declared_snapshot_ids=self.declared_snapshots.get(
                action.logical_table,
                (),
            ),
        )

    def current_state(
        self,
        action: FailedRunRollbackAction,
    ) -> tuple[int | None, str | None]:
        current = next(
            (
                snapshot
                for snapshot in self._state(action).snapshots
                if snapshot.is_current
            ),
            None,
        )
        if current is None:
            return None, None
        return current.snapshot_id, current.rollback_run_id

    def protected_refs(
        self,
        action: FailedRunRollbackAction,
    ) -> dict[str, int]:
        return self._state(action).protected_refs

    def rollback(
        self,
        action: FailedRunRollbackAction,
        *,
        run_id: str,
    ) -> None:
        if action.target_snapshot_id is not None:
            self.spark.sql(action.sql).collect()
            return
        execute_iceberg_sql(
            self.spark,
            action.sql,
            snapshot_properties={FAILED_RUN_ROLLBACK_PROPERTY: run_id},
        )


def _spark_session(parsed: argparse.Namespace, config: CatalogConfig) -> Any:
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(parsed.app_name).config(
        "spark.sql.session.timeZone",
        "UTC",
    )
    if parsed.master:
        builder = builder.master(parsed.master)
    builder = config.configure_builder(builder)
    if parsed.spark_packages:
        builder = builder.config("spark.jars.packages", parsed.spark_packages)
    return builder.getOrCreate()


def _catalog_config(parsed: argparse.Namespace) -> CatalogConfig:
    if not parsed.warehouse:
        raise ValueError("--warehouse is required")
    return CatalogConfig(
        catalog_name=parsed.catalog_name,
        namespace=parsed.namespace,
        warehouse=parsed.warehouse,
        catalog_type=parsed.catalog_type,
        aws_region=parsed.aws_region,
        s3_endpoint=parsed.s3_endpoint,
        s3_path_style_access=parsed.s3_path_style_access,
        s3_credentials_provider=parsed.s3_credentials_provider,
    )


def _run_maintenance(parsed: argparse.Namespace) -> dict[str, Any]:
    if not parsed.tables:
        raise ValueError("maintenance requires at least one --table")
    if not parsed.operations:
        raise ValueError("maintenance requires at least one --operation")
    if not parsed.planned_at:
        raise ValueError("maintenance requires --planned-at")
    if (
        parsed.run_id is not None
        or parsed.identity_generation_id is not None
        or parsed.allow_legacy_parent_inference
    ):
        raise ValueError("failed-run arguments require rollback-failed-run")
    config = _catalog_config(parsed)
    declared_protected = _parse_protected_snapshots(parsed.protected_snapshots)
    unselected_protected = sorted(set(declared_protected) - set(parsed.tables))
    if unselected_protected:
        raise ValueError(
            "protected snapshot tables were not selected: "
            + ", ".join(unselected_protected)
        )
    spark = _spark_session(parsed, config)
    try:
        snapshots = {
            table: _snapshot_inventory(spark, config=config, table=table)
            for table in sorted(set(parsed.tables))
        }
        protected = {
            table: tuple(
                sorted(
                    set(declared_protected.get(table, ()))
                    | set(
                        _iceberg_referenced_snapshot_ids(
                            spark,
                            config=config,
                            table=table,
                        )
                    )
                )
            )
            for table in sorted(set(parsed.tables))
        }
        plan = build_iceberg_maintenance_plan(
            catalog_name=config.catalog_name,
            namespace=config.namespace,
            tables=parsed.tables,
            operations=parsed.operations,
            snapshots=snapshots,
            protected_snapshot_ids=protected,
            planned_at=parsed.planned_at,
            retention_days=parsed.retention_days,
            orphan_retention_days=parsed.orphan_retention_days,
            retain_last=parsed.retain_last,
            dry_run=not parsed.execute,
            references_reviewed=parsed.references_reviewed,
        )
        executed = (
            execute_iceberg_maintenance_plan(spark, plan) if parsed.execute else ()
        )
        return {
            "plan": plan.model_dump(
                mode="json",
                by_alias=True,
                exclude_none=True,
            ),
            "executed": executed,
        }
    finally:
        spark.stop()


def _run_failed_run_rollback(parsed: argparse.Namespace) -> dict[str, Any]:
    if parsed.tables or parsed.operations:
        raise ValueError("rollback-failed-run does not accept maintenance operations")
    if not parsed.run_id:
        raise ValueError("rollback-failed-run requires --run-id")
    if not parsed.planned_at:
        raise ValueError("rollback-failed-run requires --planned-at")
    failed_run = require_sha256(parsed.run_id, label="run_id")
    planned_at = require_rfc3339(parsed.planned_at, label="planned_at")
    generation = (
        None
        if parsed.identity_generation_id is None
        else require_identity_generation_id(parsed.identity_generation_id)
    )
    if generation is None and not parsed.allow_legacy_parent_inference:
        raise ValueError(
            "legacy fixed-table rollback requires --allow-legacy-parent-inference"
        )
    if generation is not None and parsed.allow_legacy_parent_inference:
        raise ValueError(
            "--allow-legacy-parent-inference is valid only without "
            "--identity-generation-id"
        )
    mapping = build_community_table_mapping(generation)
    declared_protected = _parse_protected_snapshots(parsed.protected_snapshots)
    unknown_protected = sorted(set(declared_protected) - FAILED_RUN_ROLLBACK_TABLES)
    if unknown_protected:
        raise ValueError(
            "rollback protected snapshots must be Identity tables: "
            + ", ".join(unknown_protected)
        )
    config = _catalog_config(parsed)
    spark = _spark_session(parsed, config)
    try:
        tables = CommunityCatalogTables(
            spark,
            config,
            identity_generation_id=generation,
            table_mapping=mapping,
        )
        run = tables.read_run(failed_run)
        if run is None:
            raise ValueError("failed-run rollback requires an existing run manifest")
        if run.input_manifest.get("identityGenerationId") != generation:
            raise ValueError("run manifest belongs to another Identity generation")
        table_states = tuple(
            _rollback_table_state(
                spark,
                config=config,
                logical_table=table,
                physical_table=mapping[table],
                run_id=failed_run,
                allow_legacy_control_inference=(parsed.allow_legacy_parent_inference),
                declared_snapshot_ids=declared_protected.get(table, ()),
            )
            for table in sorted(FAILED_RUN_ROLLBACK_TABLES)
        )
        plan = build_failed_run_rollback_plan(
            catalog_name=config.catalog_name,
            namespace=config.namespace,
            run_id=failed_run,
            identity_generation_id=generation,
            table_mapping=mapping,
            table_states=table_states,
            commit_exists=tables.read_commit(failed_run) is not None,
            planned_at=planned_at,
            dry_run=not parsed.execute,
            allow_legacy_parent_inference=(parsed.allow_legacy_parent_inference),
            references_reviewed=parsed.references_reviewed,
        )
        executor = _SparkFailedRunRollbackExecutor(
            spark,
            config=config,
            tables=tables,
            declared_snapshots=declared_protected,
            run_id=failed_run,
            allow_legacy_parent_inference=parsed.allow_legacy_parent_inference,
        )
        audit = (
            execute_failed_run_rollback_plan(plan, executor) if parsed.execute else ()
        )
        return {
            "operation": "rollback-failed-run",
            "plan": plan.model_dump(
                mode="json",
                by_alias=True,
            ),
            "audit": audit,
        }
    finally:
        spark.stop()


def run(parsed: argparse.Namespace) -> dict[str, Any]:
    if parsed.command == "rollback-failed-run":
        return _run_failed_run_rollback(parsed)
    return _run_maintenance(parsed)


def main(argv: Sequence[str] | None = None) -> int:
    print(canonical_json(run(build_parser().parse_args(argv))))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
