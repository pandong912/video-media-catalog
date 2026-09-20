"""Iceberg persistence and commit-last visibility for community Silver v2."""

from __future__ import annotations

import uuid
from typing import Any

from video_media_catalog.community_ingest import (
    CommunityIngestCommit,
    CommunityIngestRun,
    build_community_ingest_commit,
)
from video_media_catalog.community_rows import (
    ingest_commit_row,
    ingest_run_row,
)
from video_media_catalog.community_tables import (
    DATA_TABLE_COLUMNS,
    NULLABLE_COLUMNS,
    TABLE_COLUMNS,
    TABLE_KEYS,
    TABLE_PARTITION_COLUMNS,
)
from video_media_catalog.iceberg import (
    CatalogConfig,
    execute_iceberg_sql,
    find_owned_snapshot_id,
)
from video_media_catalog.v2_contracts import (
    require_rfc3339,
    require_sha256,
)

_RUN_SNAPSHOT_PROPERTY = "video-media-catalog.run-id"

_TYPE_OVERRIDES = {
    ("community_entity_ledger", "imported_v1"): "BOOLEAN",
    ("community_identity_evidence", "confidence"): "DOUBLE",
}


def _column_definition(table: str, column: str) -> str:
    data_type = _TYPE_OVERRIDES.get((table, column), "STRING")
    nullability = "" if column in NULLABLE_COLUMNS[table] else " NOT NULL"
    return f"`{column}` {data_type}{nullability}"


class CommunityCatalogTables:
    """Own append-only v2 tables whose visibility is fenced by run commits."""

    def __init__(self, spark: Any, config: CatalogConfig) -> None:
        self.spark = spark
        self.config = config

    @property
    def namespace_identifier(self) -> str:
        return f"`{self.config.catalog_name}`.`{self.config.namespace}`"

    def table_identifier(self, table: str) -> str:
        if table not in TABLE_COLUMNS:
            raise KeyError(f"unknown community catalog table: {table}")
        return f"{self.namespace_identifier}.`{table}`"

    def table_name(self, table: str) -> str:
        if table not in TABLE_COLUMNS:
            raise KeyError(f"unknown community catalog table: {table}")
        return f"{self.config.catalog_name}.{self.config.namespace}.{table}"

    def create_tables(self) -> None:
        self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {self.namespace_identifier}")
        for table, columns in TABLE_COLUMNS.items():
            definition = ",\n".join(
                _column_definition(table, column) for column in columns
            )
            partition_column = TABLE_PARTITION_COLUMNS[table]
            self.spark.sql(
                f"""
                CREATE TABLE IF NOT EXISTS {self.table_identifier(table)} (
                    {definition}
                )
                USING iceberg
                PARTITIONED BY (bucket(128, `{partition_column}`))
                TBLPROPERTIES (
                    'format-version' = '2',
                    'write.parquet.compression-codec' = 'zstd'
                )
                """
            )

    def merge_insert_only(
        self,
        table: str,
        dataframe: Any,
        *,
        snapshot_properties: dict[str, str] | None = None,
    ) -> int:
        """Insert immutable logical keys and never mutate source history."""

        if table not in TABLE_COLUMNS:
            raise KeyError(f"unknown community catalog table: {table}")
        columns = TABLE_COLUMNS[table]
        key = TABLE_KEYS[table]
        missing = sorted(set(columns) - set(dataframe.columns))
        extra = sorted(set(dataframe.columns) - set(columns))
        if missing or extra:
            raise ValueError(
                f"{table} dataframe columns differ: missing={missing}, extra={extra}"
            )
        staged = dataframe.select(*columns).dropDuplicates([key]).persist()
        try:
            row_count = staged.count()
            if row_count == 0:
                return 0
            required = [
                column for column in columns if column not in NULLABLE_COLUMNS[table]
            ]
            null_predicate = " OR ".join(f"`{column}` IS NULL" for column in required)
            if staged.where(null_predicate).limit(1).count():
                raise ValueError(f"{table} contains null in a required column")
            view = f"_community_catalog_{table}_{uuid.uuid4().hex}"
            staged.createOrReplaceTempView(view)
            quoted = ", ".join(f"`{column}`" for column in columns)
            source = ", ".join(f"s.`{column}`" for column in columns)
            nullability_config = "spark.sql.iceberg.check-nullability"
            previous = self.spark.conf.get(nullability_config, "true")
            self.spark.conf.set(nullability_config, "false")
            try:
                execute_iceberg_sql(
                    self.spark,
                    f"""
                    MERGE INTO {self.table_identifier(table)} t
                    USING `{view}` s
                    ON t.`{key}` = s.`{key}`
                    WHEN NOT MATCHED THEN INSERT ({quoted})
                    VALUES ({source})
                    """,
                    snapshot_properties=snapshot_properties,
                )
            finally:
                self.spark.conf.set(nullability_config, previous)
                self.spark.catalog.dropTempView(view)
            return row_count
        finally:
            staged.unpersist()

    def stage_and_commit(
        self,
        *,
        run: CommunityIngestRun,
        dataframes: dict[str, Any],
        committed_at: str,
    ) -> CommunityIngestCommit:
        """Stage all rows and publish one immutable run commit last."""

        committed = require_rfc3339(committed_at, label="committed_at")
        if set(dataframes) != set(DATA_TABLE_COLUMNS):
            raise ValueError("all community Silver dataframes are required")
        self.create_tables()
        existing = self.read_commit(run.run_id)
        if existing is not None:
            if existing.table_counts != run.expected_counts:
                raise RuntimeError("existing run commit conflicts with expected counts")
            self._verify_run_manifest(run)
            return existing

        run_frame = self.spark.createDataFrame([ingest_run_row(run)])
        self.merge_insert_only("community_ingest_run", run_frame)
        self._verify_run_manifest(run)

        for table in DATA_TABLE_COLUMNS:
            staged_count = dataframes[table].count()
            if staged_count != run.expected_counts[table]:
                raise ValueError(
                    f"{table} staged count {staged_count} differs from "
                    f"expected {run.expected_counts[table]}"
                )
            if (
                dataframes[table]
                .where(f"`run_id` IS NULL OR `run_id` <> '{run.run_id}'")
                .limit(1)
                .count()
            ):
                raise ValueError(f"{table} contains rows for another run")
            self.merge_insert_only(
                table,
                dataframes[table],
                snapshot_properties={_RUN_SNAPSHOT_PROPERTY: run.run_id},
            )

        actual_counts = {
            table: self._run_row_count(table, run.run_id)
            for table in DATA_TABLE_COLUMNS
        }
        if actual_counts != run.expected_counts:
            raise RuntimeError(
                "persisted per-run counts do not match immutable run manifest"
            )
        snapshot_ids = {
            table: self._run_snapshot_id(
                table,
                run.run_id,
                expected_row_count=actual_counts[table],
            )
            for table in DATA_TABLE_COLUMNS
        }
        commit = build_community_ingest_commit(
            run_id=run.run_id,
            committed_at=committed,
            table_counts=actual_counts,
            table_snapshot_ids=snapshot_ids,
        )
        commit_frame = self.spark.createDataFrame([ingest_commit_row(commit)])
        self.merge_insert_only("community_ingest_commit", commit_frame)
        published = self.read_commit(run.run_id)
        if published != commit:
            raise RuntimeError("community ingest commit could not be verified")
        return published

    def read_commit(self, run_id: str) -> CommunityIngestCommit | None:
        run_id = require_sha256(run_id, label="run_id")
        rows = self.spark.sql(
            f"""
            SELECT commit_json
            FROM {self.table_identifier("community_ingest_commit")}
            WHERE run_id = '{run_id}'
            LIMIT 2
            """
        ).collect()
        if not rows:
            return None
        if len(rows) != 1:
            raise RuntimeError("one community run has multiple commit markers")
        return CommunityIngestCommit.model_validate_json(rows[0]["commit_json"])

    def visible_dataframes(
        self,
        *,
        data_snapshot_ids: dict[str, int | None],
        commit_snapshot_id: int,
    ) -> dict[str, Any]:
        """Read exact snapshots and filter every row through committed runs."""

        if set(data_snapshot_ids) != set(DATA_TABLE_COLUMNS):
            raise ValueError("all community data snapshot IDs are required")
        if commit_snapshot_id <= 0:
            raise ValueError("commit_snapshot_id must be positive")
        commits = (
            self.spark.read.format("iceberg")
            .option("snapshot-id", str(commit_snapshot_id))
            .load(self.table_name("community_ingest_commit"))
            .select("run_id")
            .dropDuplicates(["run_id"])
        )
        visible = {}
        for table, snapshot_id in data_snapshot_ids.items():
            frame = (
                self.spark.table(self.table_name(table)).limit(0)
                if snapshot_id is None
                else self.spark.read.format("iceberg")
                .option("snapshot-id", str(snapshot_id))
                .load(self.table_name(table))
            )
            visible[table] = frame.join(commits, "run_id", "inner")
        return visible

    def _verify_run_manifest(self, run: CommunityIngestRun) -> None:
        rows = self.spark.sql(
            f"""
            SELECT manifest_json
            FROM {self.table_identifier("community_ingest_run")}
            WHERE run_id = '{run.run_id}'
            LIMIT 2
            """
        ).collect()
        if len(rows) != 1:
            raise RuntimeError("community ingest run manifest is missing or duplicated")
        stored = CommunityIngestRun.model_validate_json(rows[0]["manifest_json"])
        if stored != run:
            raise RuntimeError("community ingest run manifest conflicts")

    def _run_row_count(self, table: str, run_id: str) -> int:
        run_id = require_sha256(run_id, label="run_id")
        rows = self.spark.sql(
            f"""
            SELECT COUNT(*) AS row_count
            FROM {self.table_identifier(table)}
            WHERE run_id = '{run_id}'
            """
        ).collect()
        if len(rows) != 1:
            raise RuntimeError(f"could not count persisted {table} rows")
        return int(rows[0]["row_count"])

    def _run_snapshot_id(
        self,
        table: str,
        run_id: str,
        *,
        expected_row_count: int,
    ) -> int | None:
        run_id = require_sha256(run_id, label="run_id")
        return find_owned_snapshot_id(
            self.spark,
            table_identifier=self.table_identifier(table),
            table_name=self.table_name(table),
            primary_key=TABLE_KEYS[table],
            identity_column="run_id",
            identity_value=run_id,
            snapshot_property=_RUN_SNAPSHOT_PROPERTY,
            expected_row_count=expected_row_count,
        )

    def _latest_snapshot_id(self, table: str) -> int | None:
        rows = self.spark.sql(
            f"""
            SELECT snapshot_id
            FROM {self.table_identifier(table)}.snapshots
            ORDER BY committed_at DESC
            LIMIT 1
            """
        ).collect()
        return None if not rows else int(rows[0]["snapshot_id"])
