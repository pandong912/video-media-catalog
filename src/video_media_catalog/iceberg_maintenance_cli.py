"""CLI for dry-run-first Iceberg maintenance."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

from video_media_catalog.canonical import canonical_json
from video_media_catalog.iceberg import CatalogConfig
from video_media_catalog.iceberg_maintenance import (
    ALLOWED_MAINTENANCE_TABLES,
    DEFAULT_RETAIN_LAST,
    DEFAULT_RETENTION_DAYS,
    IcebergSnapshotState,
    MaintenanceOperation,
    build_iceberg_maintenance_plan,
    execute_iceberg_maintenance_plan,
)
from video_media_catalog.v2_contracts import require_rfc3339


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="video-media-catalog-iceberg-maintenance",
        description=(
            "Build an auditable Iceberg maintenance plan; execution is opt-in."
        ),
    )
    parser.add_argument(
        "--table",
        dest="tables",
        action="append",
        required=True,
        choices=sorted(ALLOWED_MAINTENANCE_TABLES),
    )
    parser.add_argument(
        "--operation",
        dest="operations",
        action="append",
        required=True,
        choices=[item.value for item in MaintenanceOperation],
    )
    parser.add_argument("--planned-at", required=True)
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
    parser.add_argument("--catalog-name", default="media")
    parser.add_argument("--namespace", default="video_media_catalog")
    parser.add_argument(
        "--catalog-type",
        choices=("hadoop", "glue"),
        default="glue",
    )
    parser.add_argument("--warehouse", required=True)
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


def run(parsed: argparse.Namespace) -> dict[str, Any]:
    config = CatalogConfig(
        catalog_name=parsed.catalog_name,
        namespace=parsed.namespace,
        warehouse=parsed.warehouse,
        catalog_type=parsed.catalog_type,
        aws_region=parsed.aws_region,
        s3_endpoint=parsed.s3_endpoint,
        s3_path_style_access=parsed.s3_path_style_access,
        s3_credentials_provider=parsed.s3_credentials_provider,
    )
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


def main(argv: Sequence[str] | None = None) -> int:
    print(canonical_json(run(build_parser().parse_args(argv))))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
