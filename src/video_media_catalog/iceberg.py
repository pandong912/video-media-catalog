"""Iceberg table ownership and insert-only idempotent MERGE operations."""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, ClassVar, Literal

from video_media_catalog.constants import CURATED_TABLE_KEYS
from video_media_catalog.models import SnapshotTable

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

TABLE_COLUMNS: dict[str, tuple[str, ...]] = {
    "catalog_source_record": (
        "record_key",
        "source",
        "source_record_id",
        "source_revision",
        "modified",
        "source_hash",
        "entity_key",
        "payload_json",
    ),
    "catalog_entity": (
        "entity_key",
        "entity_type",
        "canonical_source",
        "canonical_source_id",
        "attributes_json",
    ),
    "catalog_name": (
        "name_key",
        "entity_key",
        "name_type",
        "language",
        "value",
        "source",
        "source_record_id",
    ),
    "catalog_external_identifier": (
        "identifier_key",
        "entity_key",
        "scheme",
        "value",
        "source",
        "source_record_id",
    ),
    "catalog_relation": (
        "relation_key",
        "subject_entity_key",
        "relation_type",
        "object_entity_key",
        "ordinal",
        "source",
        "source_record_id",
        "attributes_json",
    ),
    "catalog_ingest_error": (
        "error_key",
        "source",
        "source_record_id",
        "error_code",
        "message",
        "details_json",
    ),
}
NULLABLE_COLUMNS = {
    "catalog_source_record": frozenset({"source_revision", "modified"}),
    "catalog_entity": frozenset(),
    "catalog_name": frozenset(),
    "catalog_external_identifier": frozenset(),
    "catalog_relation": frozenset({"ordinal"}),
    "catalog_ingest_error": frozenset(),
}


class _SparkSqlCallable:
    class Java:
        implements: ClassVar[list[str]] = ["java.util.concurrent.Callable"]

    def __init__(self, spark: Any, statement: str) -> None:
        self._spark = spark
        self._statement = statement

    def call(self) -> int:
        self._spark.sql(self._statement).collect()
        return 0


def execute_iceberg_sql(
    spark: Any,
    statement: str,
    *,
    snapshot_properties: dict[str, str] | None = None,
) -> None:
    """Execute SQL with properties atomically attached to its Iceberg commit."""

    if not snapshot_properties:
        spark.sql(statement)
        return
    if any(not key or not value for key, value in snapshot_properties.items()):
        raise ValueError("Iceberg snapshot properties must be non-empty strings")
    try:
        jvm = spark._jvm
    except AttributeError as exc:
        raise RuntimeError(
            "Spark JVM is required to attach Iceberg snapshot identity"
        ) from exc
    properties = jvm.java.util.HashMap()
    for key, value in sorted(snapshot_properties.items()):
        properties.put(key, value)
    jvm.org.apache.iceberg.spark.CommitMetadata.withCommitProperties(
        properties,
        _SparkSqlCallable(spark, statement),
        jvm.java.lang.RuntimeException._java_lang_class,
    )


def find_owned_snapshot_id(
    spark: Any,
    *,
    table_identifier: str,
    table_name: str,
    primary_key: str,
    identity_column: str,
    identity_value: str,
    snapshot_property: str,
    expected_row_count: int,
) -> int | None:
    """Find one identity-tagged snapshot and verify its exact row additions."""

    rows = spark.sql(
        f"""
        SELECT snapshot_id, parent_id
        FROM {table_identifier}.snapshots
        WHERE summary['{snapshot_property}'] = '{identity_value}'
        """
    ).collect()
    if expected_row_count == 0:
        if rows:
            raise RuntimeError(
                f"{table_name} has an identity snapshot despite zero expected rows"
            )
        return None
    if len(rows) != 1:
        raise RuntimeError(
            f"{table_name} must have exactly one identity snapshot; found {len(rows)}"
        )

    snapshot_id = int(rows[0]["snapshot_id"])
    parent_id = rows[0]["parent_id"]
    candidate = (
        spark.read.format("iceberg")
        .option("snapshot-id", str(snapshot_id))
        .load(table_name)
        .select(primary_key, identity_column)
    )
    identity_predicate = f"`{identity_column}` = '{identity_value}'"
    candidate_identity_count = candidate.where(identity_predicate).count()
    if candidate_identity_count != expected_row_count:
        raise RuntimeError(
            f"{table_name} identity snapshot contains "
            f"{candidate_identity_count} rows for this write; "
            f"expected {expected_row_count}"
        )

    if parent_id is None:
        additions = candidate
    else:
        parent = (
            spark.read.format("iceberg")
            .option("snapshot-id", str(int(parent_id)))
            .load(table_name)
            .select(primary_key, identity_column)
        )
        if parent.where(identity_predicate).limit(1).count():
            raise RuntimeError(
                f"{table_name} identity rows existed before its tagged snapshot"
            )
        parent_keys = parent.select(primary_key).dropDuplicates([primary_key])
        additions = candidate.join(parent_keys, primary_key, "left_anti")

    additions = additions.persist()
    try:
        addition_count = additions.count()
        if addition_count != expected_row_count:
            raise RuntimeError(
                f"{table_name} tagged snapshot added {addition_count} rows; "
                f"expected {expected_row_count}"
            )
        foreign_predicate = (
            f"`{identity_column}` IS NULL OR `{identity_column}` <> '{identity_value}'"
        )
        if additions.where(foreign_predicate).limit(1).count():
            raise RuntimeError(
                f"{table_name} tagged snapshot added rows for another identity"
            )
    finally:
        additions.unpersist()
    return snapshot_id


@dataclass(frozen=True)
class CatalogConfig:
    catalog_name: str
    namespace: str
    warehouse: str
    catalog_type: Literal["hadoop", "glue"] = "hadoop"
    aws_region: str | None = None
    s3_endpoint: str | None = None
    s3_path_style_access: bool = False
    s3_credentials_provider: Literal["web-identity", "default"] = "web-identity"

    def __post_init__(self) -> None:
        for label, value in (
            ("catalog_name", self.catalog_name),
            ("namespace", self.namespace),
        ):
            if _IDENTIFIER.fullmatch(value) is None:
                raise ValueError(f"{label} is not a safe Spark identifier: {value!r}")
        if self.catalog_type == "glue" and not self.warehouse.startswith("s3"):
            raise ValueError("Glue catalog warehouse must be an s3:// or s3a:// URI")
        if self.s3_credentials_provider not in {"web-identity", "default"}:
            raise ValueError("unsupported S3 credentials provider")

    def spark_configs(self) -> dict[str, str]:
        prefix = f"spark.sql.catalog.{self.catalog_name}"
        configs = {
            "spark.sql.extensions": (
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
            ),
            prefix: "org.apache.iceberg.spark.SparkCatalog",
            f"{prefix}.warehouse": self.warehouse,
        }
        if self.catalog_type == "hadoop":
            configs[f"{prefix}.type"] = "hadoop"
        else:
            configs[f"{prefix}.catalog-impl"] = (
                "org.apache.iceberg.aws.glue.GlueCatalog"
            )
            configs[f"{prefix}.io-impl"] = "org.apache.iceberg.aws.s3.S3FileIO"
            # Hadoop S3A uses AWS SDK v1. EKS requires explicit web identity,
            # while EMR Serverless injects credentials through the default
            # provider chain. Iceberg S3FileIO uses SDK v2 independently.
            providers = {
                "web-identity": (
                    "com.amazonaws.auth.WebIdentityTokenCredentialsProvider"
                ),
                "default": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
            }
            configs["spark.hadoop.fs.s3a.aws.credentials.provider"] = providers[
                self.s3_credentials_provider
            ]
            if self.aws_region:
                configs[f"{prefix}.client.region"] = self.aws_region
            if self.s3_endpoint:
                configs[f"{prefix}.s3.endpoint"] = self.s3_endpoint
            if self.s3_path_style_access:
                configs[f"{prefix}.s3.path-style-access"] = "true"
        if self.aws_region:
            configs["spark.hadoop.fs.s3a.endpoint.region"] = self.aws_region
        if self.s3_endpoint:
            configs["spark.hadoop.fs.s3a.endpoint"] = self.s3_endpoint
        if self.s3_path_style_access:
            configs["spark.hadoop.fs.s3a.path.style.access"] = "true"
        return configs

    def configure_builder(self, builder: Any) -> Any:
        for key, value in self.spark_configs().items():
            builder = builder.config(key, value)
        return builder


class MediaCatalogTables:
    """Own the media-catalog namespace and its six curated Iceberg tables."""

    def __init__(self, spark: Any, config: CatalogConfig) -> None:
        self.spark = spark
        self.config = config

    @property
    def namespace_identifier(self) -> str:
        return f"`{self.config.catalog_name}`.`{self.config.namespace}`"

    def table_identifier(self, table: str) -> str:
        if table not in TABLE_COLUMNS:
            raise KeyError(f"unknown media catalog table: {table}")
        return f"{self.namespace_identifier}.`{table}`"

    def table_name(self, table: str) -> str:
        if table not in TABLE_COLUMNS:
            raise KeyError(f"unknown media catalog table: {table}")
        return f"{self.config.catalog_name}.{self.config.namespace}.{table}"

    def create_tables(self) -> None:
        self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {self.namespace_identifier}")
        definitions = {
            "catalog_source_record": """
                record_key STRING NOT NULL,
                source STRING NOT NULL,
                source_record_id STRING NOT NULL,
                source_revision STRING,
                modified STRING,
                source_hash STRING NOT NULL,
                entity_key STRING NOT NULL,
                payload_json STRING NOT NULL
            """,
            "catalog_entity": """
                entity_key STRING NOT NULL,
                entity_type STRING NOT NULL,
                canonical_source STRING NOT NULL,
                canonical_source_id STRING NOT NULL,
                attributes_json STRING NOT NULL
            """,
            "catalog_name": """
                name_key STRING NOT NULL,
                entity_key STRING NOT NULL,
                name_type STRING NOT NULL,
                language STRING NOT NULL,
                value STRING NOT NULL,
                source STRING NOT NULL,
                source_record_id STRING NOT NULL
            """,
            "catalog_external_identifier": """
                identifier_key STRING NOT NULL,
                entity_key STRING NOT NULL,
                scheme STRING NOT NULL,
                value STRING NOT NULL,
                source STRING NOT NULL,
                source_record_id STRING NOT NULL
            """,
            "catalog_relation": """
                relation_key STRING NOT NULL,
                subject_entity_key STRING NOT NULL,
                relation_type STRING NOT NULL,
                object_entity_key STRING NOT NULL,
                ordinal STRING,
                source STRING NOT NULL,
                source_record_id STRING NOT NULL,
                attributes_json STRING NOT NULL
            """,
            "catalog_ingest_error": """
                error_key STRING NOT NULL,
                source STRING NOT NULL,
                source_record_id STRING NOT NULL,
                error_code STRING NOT NULL,
                message STRING NOT NULL,
                details_json STRING NOT NULL
            """,
        }
        partitions = {
            table: f"PARTITIONED BY (bucket(128, {key}))"
            for table, key in CURATED_TABLE_KEYS.items()
        }
        for table, ddl in definitions.items():
            self.spark.sql(
                f"""
                CREATE TABLE IF NOT EXISTS {self.table_identifier(table)} (
                    {ddl}
                )
                USING iceberg
                {partitions[table]}
                TBLPROPERTIES (
                    'format-version' = '2',
                    'write.parquet.compression-codec' = 'zstd'
                )
                """
            )

    def merge_insert_only(self, table: str, dataframe: Any) -> int:
        """Insert unseen deterministic keys and never mutate existing facts."""

        columns = TABLE_COLUMNS[table]
        key = CURATED_TABLE_KEYS[table]
        missing = sorted(set(columns) - set(dataframe.columns))
        if missing:
            raise ValueError(f"{table} dataframe is missing columns: {missing}")
        staged = dataframe.select(*columns).dropDuplicates([key]).persist()
        try:
            row_count = staged.count()
            if row_count == 0:
                return 0
            required_columns = [
                column for column in columns if column not in NULLABLE_COLUMNS[table]
            ]
            null_predicate = " OR ".join(
                f"`{column}` IS NULL" for column in required_columns
            )
            if staged.where(null_predicate).limit(1).count():
                raise ValueError(f"{table} contains null in a required column")
            view = f"_media_catalog_{table}_{uuid.uuid4().hex}"
            staged.createOrReplaceTempView(view)
            quoted_columns = ", ".join(f"`{column}`" for column in columns)
            source_columns = ", ".join(f"s.`{column}`" for column in columns)
            nullability_config = "spark.sql.iceberg.check-nullability"
            previous_check = self.spark.conf.get(nullability_config, "true")
            self.spark.conf.set(nullability_config, "false")
            try:
                self.spark.sql(
                    f"""
                    MERGE INTO {self.table_identifier(table)} t
                    USING `{view}` s
                    ON t.`{key}` = s.`{key}`
                    WHEN NOT MATCHED THEN INSERT ({quoted_columns})
                    VALUES ({source_columns})
                    """
                )
            finally:
                self.spark.conf.set(nullability_config, previous_check)
                self.spark.catalog.dropTempView(view)
            return row_count
        finally:
            staged.unpersist()

    def merge_all(self, dataframes: dict[str, Any]) -> dict[str, int]:
        if set(dataframes) != set(TABLE_COLUMNS):
            raise ValueError("all six curated dataframes must be provided")
        return {
            table: self.merge_insert_only(table, dataframes[table])
            for table in TABLE_COLUMNS
        }

    def capture_snapshots(
        self,
        row_counts: dict[str, int],
        *,
        empty_committed_at: str,
    ) -> list[SnapshotTable]:
        if set(row_counts) != set(TABLE_COLUMNS):
            raise ValueError("all six table counts are required")
        snapshots: list[SnapshotTable] = []
        for table in TABLE_COLUMNS:
            identifier = self.table_identifier(table)
            rows = self.spark.sql(
                f"""
                SELECT snapshot_id, parent_id, committed_at, operation
                FROM {identifier}.snapshots
                ORDER BY committed_at DESC
                LIMIT 1
                """
            ).collect()
            if not rows:
                if row_counts[table] != 0:
                    raise RuntimeError(
                        f"{table} wrote rows but has no Iceberg snapshot"
                    )
                snapshots.append(
                    SnapshotTable(
                        table_name=self.table_name(table),
                        committed_at=empty_committed_at,
                        operation="empty",
                        record_count=0,
                    )
                )
                continue
            row = rows[0]
            committed = row["committed_at"]
            if isinstance(committed, datetime):
                if committed.tzinfo is None:
                    committed = committed.replace(tzinfo=UTC)
                committed_at = (
                    committed.astimezone(UTC).isoformat().replace("+00:00", "Z")
                )
            else:
                committed_at = str(committed)
            snapshots.append(
                SnapshotTable(
                    table_name=self.table_name(table),
                    snapshot_id=int(row["snapshot_id"]),
                    parent_snapshot_id=(
                        None if row["parent_id"] is None else int(row["parent_id"])
                    ),
                    committed_at=committed_at,
                    operation=str(row["operation"]),
                    record_count=row_counts[table],
                )
            )
        return snapshots
