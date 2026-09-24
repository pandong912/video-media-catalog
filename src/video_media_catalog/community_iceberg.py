"""Iceberg persistence and commit-last visibility for community Silver v2."""

from __future__ import annotations

import uuid
from collections.abc import Callable, Mapping, Sequence
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
from video_media_catalog.community_snapshot import (
    CommunitySilverEpochManifest,
    build_committed_run_digest,
)
from video_media_catalog.community_tables import (
    DATA_TABLE_COLUMNS,
    NULLABLE_COLUMNS,
    TABLE_COLUMNS,
    TABLE_KEYS,
    TABLE_MERGE_KEYS,
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

    def latest_snapshot_id(self, table: str) -> int | None:
        """Capture the current Iceberg snapshot for an existing v2 table."""

        if table not in TABLE_COLUMNS:
            raise KeyError(f"unknown community catalog table: {table}")
        return self._latest_snapshot_id(table)

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
        keys = TABLE_MERGE_KEYS[table]
        missing = sorted(set(columns) - set(dataframe.columns))
        extra = sorted(set(dataframe.columns) - set(columns))
        if missing or extra:
            raise ValueError(
                f"{table} dataframe columns differ: missing={missing}, extra={extra}"
            )
        staged = dataframe.select(*columns).dropDuplicates(list(keys)).persist()
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
            match = " AND ".join(f"t.`{column}` = s.`{column}`" for column in keys)
            nullability_config = "spark.sql.iceberg.check-nullability"
            previous = self.spark.conf.get(nullability_config, "true")
            self.spark.conf.set(nullability_config, "false")
            try:
                execute_iceberg_sql(
                    self.spark,
                    f"""
                    MERGE INTO {self.table_identifier(table)} t
                    USING `{view}` s
                    ON {match}
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
        dataframes: Mapping[str, Any],
        committed_at: str,
        dataframe_factory: Callable[[str], Any] | None = None,
    ) -> CommunityIngestCommit:
        """Stage all rows and publish one immutable run commit last."""

        committed = require_rfc3339(committed_at, label="committed_at")
        unknown_tables = sorted(set(dataframes) - set(DATA_TABLE_COLUMNS))
        if unknown_tables:
            raise ValueError(f"unknown community Silver dataframes: {unknown_tables}")
        if dataframe_factory is not None and not callable(dataframe_factory):
            raise TypeError("dataframe_factory must be callable")
        self.create_tables()
        existing = self.read_commit(run.run_id)
        if existing is not None:
            return self._reuse_existing_commit(run, existing)

        self._ensure_run_manifest(run)

        for table in DATA_TABLE_COLUMNS:
            self._stage_data_table(
                table,
                run,
                dataframes=dataframes,
                dataframe_factory=dataframe_factory,
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
        try:
            self.merge_insert_only("community_ingest_commit", commit_frame)
        except Exception:
            published = self.read_commit(run.run_id)
            if published is None:
                raise
            return self._reuse_existing_commit(run, published)
        published = self.read_commit(run.run_id)
        if published != commit:
            if published is None:
                raise RuntimeError("community ingest commit could not be verified")
            return self._reuse_existing_commit(run, published)
        return published

    def _stage_data_table(
        self,
        table: str,
        run: CommunityIngestRun,
        *,
        dataframes: Mapping[str, Any],
        dataframe_factory: Callable[[str], Any] | None,
    ) -> None:
        """Write one missing run table or verify an already complete write."""

        expected = run.expected_counts[table]
        current = self._run_row_count(table, run.run_id)
        if self._verify_completed_table(
            table,
            run.run_id,
            current_row_count=current,
            expected_row_count=expected,
        ):
            return
        if self._verify_no_snapshot_or_concurrent_winner(
            table,
            run.run_id,
            expected_row_count=expected,
        ):
            return

        frame = dataframes.get(table)
        if frame is None and expected > 0:
            if dataframe_factory is None:
                raise ValueError(f"{table} dataframe is required for missing rows")
            frame = dataframe_factory(table)
            if frame is None:
                raise ValueError(f"{table} dataframe factory returned no dataframe")
        if frame is None:
            return

        self._validate_run_frame(
            table,
            frame,
            run_id=run.run_id,
            expected_row_count=expected,
        )

        # Counting/validating a large frame can take long enough for another
        # exact submitter to win. Re-read state immediately before MERGE so the
        # loser does not intentionally create a second run-tagged snapshot.
        current = self._run_row_count(table, run.run_id)
        if self._verify_completed_table(
            table,
            run.run_id,
            current_row_count=current,
            expected_row_count=expected,
        ):
            return
        if self._verify_no_snapshot_or_concurrent_winner(
            table,
            run.run_id,
            expected_row_count=expected,
        ):
            return

        if expected == 0:
            merged_count = self.merge_insert_only(
                table,
                frame,
                snapshot_properties={_RUN_SNAPSHOT_PROPERTY: run.run_id},
            )
            if merged_count != 0:
                raise RuntimeError(f"{table} wrote rows for an empty run table")
            if self._run_row_count(table, run.run_id) != 0:
                raise RuntimeError(f"{table} has rows despite zero expected rows")
            self._run_snapshot_id(table, run.run_id, expected_row_count=0)
            return

        try:
            self.merge_insert_only(
                table,
                frame,
                snapshot_properties={_RUN_SNAPSHOT_PROPERTY: run.run_id},
            )
        except Exception:
            current = self._run_row_count(table, run.run_id)
            if self._verify_completed_table(
                table,
                run.run_id,
                current_row_count=current,
                expected_row_count=expected,
            ):
                return
            # Preserve the MERGE failure only when it left no ambiguous tagged
            # snapshot. Any tagged/partial state is a stronger fail-closed error.
            self._run_snapshot_id(table, run.run_id, expected_row_count=0)
            raise

        current = self._run_row_count(table, run.run_id)
        if not self._verify_completed_table(
            table,
            run.run_id,
            current_row_count=current,
            expected_row_count=expected,
        ):
            raise RuntimeError(f"{table} MERGE committed no rows for this run")

    def _verify_completed_table(
        self,
        table: str,
        run_id: str,
        *,
        current_row_count: int,
        expected_row_count: int,
    ) -> bool:
        if current_row_count == 0:
            return False
        if current_row_count != expected_row_count:
            relation = (
                "partial"
                if current_row_count < expected_row_count
                else "more than expected"
            )
            raise RuntimeError(
                f"{table} has {relation} persisted rows for this run: "
                f"{current_row_count} of {expected_row_count}"
            )
        self._run_snapshot_id(
            table,
            run_id,
            expected_row_count=expected_row_count,
        )
        return True

    def _verify_no_snapshot_or_concurrent_winner(
        self,
        table: str,
        run_id: str,
        *,
        expected_row_count: int,
    ) -> bool:
        try:
            self._run_snapshot_id(table, run_id, expected_row_count=0)
        except RuntimeError:
            current = self._run_row_count(table, run_id)
            if expected_row_count > 0 and current == expected_row_count:
                self._run_snapshot_id(
                    table,
                    run_id,
                    expected_row_count=expected_row_count,
                )
                return True
            raise
        return False

    @staticmethod
    def _validate_run_frame(
        table: str,
        dataframe: Any,
        *,
        run_id: str,
        expected_row_count: int,
    ) -> None:
        staged_count = dataframe.count()
        if staged_count != expected_row_count:
            raise ValueError(
                f"{table} staged count {staged_count} differs from "
                f"expected {expected_row_count}"
            )
        if (
            dataframe.where(f"`run_id` IS NULL OR `run_id` <> '{run_id}'")
            .limit(1)
            .count()
        ):
            raise ValueError(f"{table} contains rows for another run")

    def _ensure_run_manifest(self, run: CommunityIngestRun) -> None:
        stored = self._read_run_manifest(run.run_id)
        if stored is not None:
            if stored != run:
                raise RuntimeError("community ingest run manifest conflicts")
            return
        run_frame = self.spark.createDataFrame([ingest_run_row(run)])
        try:
            self.merge_insert_only("community_ingest_run", run_frame)
        except Exception:
            stored = self._read_run_manifest(run.run_id)
            if stored is None:
                raise
            if stored != run:
                raise RuntimeError("community ingest run manifest conflicts") from None
            return
        self._verify_run_manifest(run)

    def _reuse_existing_commit(
        self,
        run: CommunityIngestRun,
        existing: CommunityIngestCommit,
    ) -> CommunityIngestCommit:
        """Return the winner of an exact retry or concurrent duplicate submit."""

        if existing.run_id != run.run_id:
            raise RuntimeError("existing run commit belongs to another run")
        if existing.table_counts != run.expected_counts:
            raise RuntimeError("existing run commit conflicts with expected counts")
        self._verify_run_manifest(run)
        return existing

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

    def committed_runs_dataframe(self, commit_snapshot_id: int) -> Any:
        """Return committed run IDs from one exact Iceberg snapshot."""

        if isinstance(commit_snapshot_id, bool) or commit_snapshot_id <= 0:
            raise ValueError("commit_snapshot_id must be positive")
        return (
            self.spark.read.format("iceberg")
            .option("snapshot-id", str(commit_snapshot_id))
            .load(self.table_name("community_ingest_commit"))
            .select("run_id")
        )

    def committed_run_summary(self, committed_runs: Any) -> tuple[int, str]:
        """Summarize an unbounded run set without collecting IDs to the driver."""

        from pyspark.sql import functions as F

        if "run_id" not in committed_runs.columns:
            raise ValueError("committed runs dataframe requires run_id")
        runs = committed_runs.select("run_id").persist()
        try:
            if (
                runs.where(
                    F.col("run_id").isNull()
                    | ~F.col("run_id").rlike(r"^sha256:[0-9a-f]{64}$")
                )
                .limit(1)
                .count()
            ):
                raise RuntimeError("commit snapshot contains an invalid run ID")
            if (
                runs.groupBy("run_id")
                .count()
                .where(F.col("count") != 1)
                .limit(1)
                .count()
            ):
                raise RuntimeError("commit snapshot contains duplicate run IDs")
            run_count = runs.count()
            bucket_rows = (
                runs.withColumn("_bucket", F.substring("run_id", 8, 2))
                .groupBy("_bucket")
                .agg(
                    F.count("*").alias("run_count"),
                    F.sha2(
                        F.concat_ws(
                            "\n",
                            F.sort_array(F.collect_list("run_id")),
                        ),
                        256,
                    ).alias("run_digest"),
                )
                .orderBy("_bucket")
                .collect()
            )
            # Only the fixed 256 bucket summaries cross the driver boundary.
            buckets = tuple(
                (
                    str(row["_bucket"]),
                    int(row["run_count"]),
                    "sha256:" + str(row["run_digest"]),
                )
                for row in bucket_rows
            )
            return run_count, build_committed_run_digest(
                run_count=run_count,
                buckets=buckets,
            )
        finally:
            runs.unpersist()

    def validate_epoch_committed_runs(
        self,
        epoch: CommunitySilverEpochManifest,
        committed_runs: Any,
    ) -> None:
        count, digest = self.committed_run_summary(committed_runs)
        if count != epoch.committed_run_count:
            raise ValueError("Silver epoch committed run count does not match snapshot")
        if digest != epoch.committed_run_digest:
            raise ValueError(
                "Silver epoch committed run digest does not match snapshot"
            )

    def visible_run_dataframe(
        self,
        *,
        run_snapshot_id: int,
        committed_runs: Any,
    ) -> Any:
        """Read exact run metadata and fence it through distributed commits."""

        from pyspark.sql import functions as F

        if isinstance(run_snapshot_id, bool) or run_snapshot_id <= 0:
            raise ValueError("run_snapshot_id must be positive")
        runs = (
            self.spark.read.format("iceberg")
            .option("snapshot-id", str(run_snapshot_id))
            .load(self.table_name("community_ingest_run"))
        )
        selected = committed_runs.select("run_id").dropDuplicates(["run_id"])
        missing = selected.join(runs.select("run_id"), "run_id", "left_anti")
        if missing.limit(1).count():
            raise ValueError("run snapshot is missing a committed run manifest")
        visible = runs.join(selected, "run_id", "inner")
        if (
            visible.groupBy("run_id")
            .count()
            .where(F.col("count") != 1)
            .limit(1)
            .count()
        ):
            raise ValueError("run snapshot contains duplicate committed manifests")
        return visible

    def visible_dataframes(
        self,
        *,
        data_snapshot_ids: dict[str, int | None],
        commit_snapshot_id: int,
        committed_runs: Any | None = None,
        run_id_filters: Mapping[str, Sequence[str]] | None = None,
    ) -> dict[str, Any]:
        """Read exact snapshots and filter every row through committed runs.

        Bounded run filters are pushed into Iceberg before a broadcast
        left-semi commit fence. This avoids shuffling an entire large run by
        its constant run_id merely to prove that the run is committed.
        """

        from pyspark.sql import functions as F

        if set(data_snapshot_ids) != set(DATA_TABLE_COLUMNS):
            raise ValueError("all community data snapshot IDs are required")
        if commit_snapshot_id <= 0:
            raise ValueError("commit_snapshot_id must be positive")
        raw_filters = dict(run_id_filters or {})
        unknown_filter_tables = sorted(set(raw_filters) - set(DATA_TABLE_COLUMNS))
        if unknown_filter_tables:
            raise ValueError(
                "run ID filters contain unknown community tables: "
                + ", ".join(unknown_filter_tables)
            )
        normalized_filters: dict[str, tuple[str, ...]] = {}
        for table, run_ids in raw_filters.items():
            if isinstance(run_ids, (str, bytes)):
                raise TypeError(f"{table} run ID filter must be a sequence")
            normalized = tuple(
                dict.fromkeys(
                    require_sha256(run_id, label=f"{table} run_id")
                    for run_id in run_ids
                )
            )
            if not normalized:
                raise ValueError(f"{table} run ID filter must be non-empty")
            normalized_filters[table] = normalized
        commits = (
            self.committed_runs_dataframe(commit_snapshot_id)
            if committed_runs is None
            else committed_runs.select("run_id")
        )
        commits = commits.dropDuplicates(["run_id"])
        visible = {}
        for table, snapshot_id in data_snapshot_ids.items():
            frame = (
                self.spark.table(self.table_name(table)).limit(0)
                if snapshot_id is None
                else self.spark.read.format("iceberg")
                .option("snapshot-id", str(snapshot_id))
                .load(self.table_name(table))
            )
            run_ids = normalized_filters.get(table)
            if run_ids is None:
                visible[table] = frame.join(commits, "run_id", "inner")
                continue
            predicate = (
                F.col("run_id") == F.lit(run_ids[0])
                if len(run_ids) == 1
                else F.col("run_id").isin(*run_ids)
            )
            selected_rows = frame.where(predicate)
            selected_commits = commits.where(predicate)
            visible[table] = selected_rows.join(
                F.broadcast(selected_commits),
                "run_id",
                "left_semi",
            )
        return visible

    def _read_run_manifest(self, run_id: str) -> CommunityIngestRun | None:
        run_id = require_sha256(run_id, label="run_id")
        rows = self.spark.sql(
            f"""
            SELECT manifest_json
            FROM {self.table_identifier("community_ingest_run")}
            WHERE run_id = '{run_id}'
            LIMIT 2
            """
        ).collect()
        if not rows:
            return None
        if len(rows) != 1:
            raise RuntimeError("community ingest run manifest is duplicated")
        return CommunityIngestRun.model_validate_json(rows[0]["manifest_json"])

    def _verify_run_manifest(self, run: CommunityIngestRun) -> None:
        stored = self._read_run_manifest(run.run_id)
        if stored is None:
            raise RuntimeError("community ingest run manifest is missing")
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
