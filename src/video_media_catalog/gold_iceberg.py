"""Iceberg persistence and commit-last publication for Gold v2."""

from __future__ import annotations

import hashlib
import uuid
from typing import Any

from video_media_catalog.attribution import AttributionManifest
from video_media_catalog.gold import GoldReleasePlan
from video_media_catalog.gold_ingest import (
    GoldReleaseCommit,
    build_gold_release_commit,
)
from video_media_catalog.gold_quality import (
    GoldQualityReport,
    GoldQualityStatus,
)
from video_media_catalog.gold_rows import (
    gold_release_commit_row,
    gold_release_plan_row,
)
from video_media_catalog.gold_tables import (
    GOLD_DATA_COLUMNS,
    GOLD_NULLABLE_COLUMNS,
    GOLD_TABLE_COLUMNS,
    GOLD_TABLE_KEYS,
)
from video_media_catalog.iceberg import (
    CatalogConfig,
    execute_iceberg_sql,
    find_owned_snapshot_id,
)
from video_media_catalog.models import ObjectRef
from video_media_catalog.v2_contracts import (
    require_rfc3339,
    require_sha256,
)

_RELEASE_SNAPSHOT_PROPERTY = "video-media-catalog.release-plan-id"

_TYPE_OVERRIDES = {
    ("community_gold_entity", "source_node_count"): "BIGINT",
}


def _column_definition(table: str, column: str) -> str:
    data_type = _TYPE_OVERRIDES.get((table, column), "STRING")
    nullable = "" if column in GOLD_NULLABLE_COLUMNS[table] else " NOT NULL"
    return f"`{column}` {data_type}{nullable}"


class CommunityGoldTables:
    def __init__(self, spark: Any, config: CatalogConfig) -> None:
        self.spark = spark
        self.config = config

    @property
    def namespace_identifier(self) -> str:
        return f"`{self.config.catalog_name}`.`{self.config.namespace}`"

    def table_identifier(self, table: str) -> str:
        if table not in GOLD_TABLE_COLUMNS:
            raise KeyError(f"unknown Gold table: {table}")
        return f"{self.namespace_identifier}.`{table}`"

    def table_name(self, table: str) -> str:
        if table not in GOLD_TABLE_COLUMNS:
            raise KeyError(f"unknown Gold table: {table}")
        return f"{self.config.catalog_name}.{self.config.namespace}.{table}"

    def create_tables(self) -> None:
        self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {self.namespace_identifier}")
        for table, columns in GOLD_TABLE_COLUMNS.items():
            definition = ",\n".join(
                _column_definition(table, column) for column in columns
            )
            self.spark.sql(
                f"""
                CREATE TABLE IF NOT EXISTS {self.table_identifier(table)} (
                    {definition}
                )
                USING iceberg
                PARTITIONED BY (bucket(128, `release_plan_id`))
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
        if table not in GOLD_TABLE_COLUMNS:
            raise KeyError(f"unknown Gold table: {table}")
        columns = GOLD_TABLE_COLUMNS[table]
        key = GOLD_TABLE_KEYS[table]
        missing = sorted(set(columns) - set(dataframe.columns))
        extra = sorted(set(dataframe.columns) - set(columns))
        if missing or extra:
            raise ValueError(
                f"{table} dataframe columns differ: missing={missing}, extra={extra}"
            )
        staged = dataframe.select(*columns).dropDuplicates([key]).persist()
        try:
            count = staged.count()
            if count == 0:
                return 0
            required = [
                column
                for column in columns
                if column not in GOLD_NULLABLE_COLUMNS[table]
            ]
            predicate = " OR ".join(f"`{column}` IS NULL" for column in required)
            if staged.where(predicate).limit(1).count():
                raise ValueError(f"{table} contains null in required column")
            view = f"_community_gold_{table}_{uuid.uuid4().hex}"
            staged.createOrReplaceTempView(view)
            quoted = ", ".join(f"`{column}`" for column in columns)
            source = ", ".join(f"s.`{column}`" for column in columns)
            setting = "spark.sql.iceberg.check-nullability"
            previous = self.spark.conf.get(setting, "true")
            self.spark.conf.set(setting, "false")
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
                self.spark.conf.set(setting, previous)
                self.spark.catalog.dropTempView(view)
            return count
        finally:
            staged.unpersist()

    def stage_and_commit(
        self,
        *,
        plan: GoldReleasePlan,
        dataframes: dict[str, Any],
        quality_report: GoldQualityReport,
        quality_report_ref: ObjectRef,
        attribution_manifest: AttributionManifest,
        attribution_manifest_ref: ObjectRef,
        committed_at: str,
    ) -> GoldReleaseCommit:
        committed = require_rfc3339(committed_at, label="committed_at")
        if set(dataframes) != set(GOLD_DATA_COLUMNS):
            raise ValueError("all Gold dataframes are required")
        if (
            quality_report.release_plan_id != plan.release_plan_id
            or quality_report.field_policy_digest != plan.field_policy_digest
            or quality_report.config_digest != plan.config_digest
            or quality_report.release_freshness.as_of != plan.policy_context.as_of
            or quality_report.table_counts != plan.expected_counts
            or quality_report.status != GoldQualityStatus.PASS
        ):
            raise ValueError("Gold quality report does not approve this plan")
        if attribution_manifest.release_id != plan.release_plan_id:
            raise ValueError("attribution manifest does not bind this plan")
        attribution_coverage = attribution_manifest.claim_counts_by_policy
        if (
            attribution_coverage != quality_report.eligible_policy_counts
            or attribution_coverage != quality_report.attribution_counts
        ):
            raise ValueError(
                "attribution manifest does not cover every eligible assertion"
            )
        _verify_model_ref(quality_report.json_bytes(), quality_report_ref)
        _verify_model_ref(
            attribution_manifest.json_bytes(),
            attribution_manifest_ref,
        )

        self.create_tables()
        existing = self.read_commit(plan.release_plan_id)
        if existing is not None:
            if existing.table_counts != plan.expected_counts:
                raise RuntimeError("existing Gold commit conflicts with plan")
            self._verify_plan(plan)
            return existing

        self.merge_insert_only(
            "community_gold_release_plan",
            self.spark.createDataFrame([gold_release_plan_row(plan)]),
        )
        self._verify_plan(plan)
        for table in GOLD_DATA_COLUMNS:
            frame = dataframes[table]
            count = frame.count()
            if count != plan.expected_counts[table]:
                raise ValueError(f"{table} count differs from Gold plan")
            if (
                frame.where(
                    "`release_plan_id` IS NULL OR "
                    f"`release_plan_id` <> '{plan.release_plan_id}'"
                )
                .limit(1)
                .count()
            ):
                raise ValueError(f"{table} contains another release plan")
            self.merge_insert_only(
                table,
                frame,
                snapshot_properties={_RELEASE_SNAPSHOT_PROPERTY: plan.release_plan_id},
            )

        counts = {
            table: self._plan_row_count(table, plan.release_plan_id)
            for table in GOLD_DATA_COLUMNS
        }
        if counts != plan.expected_counts:
            raise RuntimeError("persisted Gold counts differ from release plan")
        snapshots = {
            table: self._release_snapshot_id(
                table,
                plan.release_plan_id,
                expected_row_count=counts[table],
            )
            for table in GOLD_DATA_COLUMNS
        }
        commit = build_gold_release_commit(
            release_plan_id=plan.release_plan_id,
            context_id=plan.policy_context.context_id,
            committed_at=committed,
            table_counts=counts,
            table_snapshot_ids=snapshots,
            quality_report=quality_report_ref,
            attribution_manifest=attribution_manifest_ref,
        )
        self.merge_insert_only(
            "community_gold_release_commit",
            self.spark.createDataFrame([gold_release_commit_row(commit)]),
        )
        published = self.read_commit(plan.release_plan_id)
        if published != commit:
            raise RuntimeError("Gold release commit could not be verified")
        return published

    def read_commit(self, release_plan_id: str) -> GoldReleaseCommit | None:
        plan_id = require_sha256(release_plan_id, label="release_plan_id")
        rows = self.spark.sql(
            f"""
            SELECT commit_json
            FROM {self.table_identifier("community_gold_release_commit")}
            WHERE release_plan_id = '{plan_id}'
            LIMIT 2
            """
        ).collect()
        if not rows:
            return None
        if len(rows) != 1:
            raise RuntimeError("Gold release has multiple commit markers")
        return GoldReleaseCommit.model_validate_json(rows[0]["commit_json"])

    def visible_dataframes(
        self,
        *,
        data_snapshot_ids: dict[str, int | None],
        commit_snapshot_id: int,
    ) -> dict[str, Any]:
        if set(data_snapshot_ids) != set(GOLD_DATA_COLUMNS):
            raise ValueError("all Gold data snapshot IDs are required")
        if commit_snapshot_id <= 0:
            raise ValueError("Gold commit snapshot ID must be positive")
        committed = (
            self.spark.read.format("iceberg")
            .option("snapshot-id", str(commit_snapshot_id))
            .load(self.table_name("community_gold_release_commit"))
            .select("release_plan_id")
            .dropDuplicates(["release_plan_id"])
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
            visible[table] = frame.join(committed, "release_plan_id", "inner")
        return visible

    def _verify_plan(self, plan: GoldReleasePlan) -> None:
        rows = self.spark.sql(
            f"""
            SELECT plan_json
            FROM {self.table_identifier("community_gold_release_plan")}
            WHERE release_plan_id = '{plan.release_plan_id}'
            LIMIT 2
            """
        ).collect()
        if len(rows) != 1:
            raise RuntimeError("Gold release plan is missing or duplicated")
        stored = GoldReleasePlan.model_validate_json(rows[0]["plan_json"])
        if stored != plan:
            raise RuntimeError("Gold release plan conflicts")

    def _plan_row_count(self, table: str, release_plan_id: str) -> int:
        plan_id = require_sha256(release_plan_id, label="release_plan_id")
        rows = self.spark.sql(
            f"""
            SELECT COUNT(*) AS row_count
            FROM {self.table_identifier(table)}
            WHERE release_plan_id = '{plan_id}'
            """
        ).collect()
        if len(rows) != 1:
            raise RuntimeError(f"could not count persisted {table} rows")
        return int(rows[0]["row_count"])

    def _release_snapshot_id(
        self,
        table: str,
        release_plan_id: str,
        *,
        expected_row_count: int,
    ) -> int | None:
        plan_id = require_sha256(release_plan_id, label="release_plan_id")
        return find_owned_snapshot_id(
            self.spark,
            table_identifier=self.table_identifier(table),
            table_name=self.table_name(table),
            primary_key=GOLD_TABLE_KEYS[table],
            identity_column="release_plan_id",
            identity_value=plan_id,
            snapshot_property=_RELEASE_SNAPSHOT_PROPERTY,
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


def _verify_model_ref(payload: bytes, reference: ObjectRef) -> None:
    if (
        reference.size_bytes != len(payload)
        or reference.checksum.value != hashlib.sha256(payload).hexdigest()
    ):
        raise ValueError("Gold control ObjectRef does not bind its payload")
