"""Shared Iceberg catalog configuration and commit helpers."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, ClassVar, Literal

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


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
    identity_predicate = f"`{identity_column}` = '{identity_value}'"
    owned_snapshots = []
    for row in rows:
        snapshot_id = int(row["snapshot_id"])
        candidate = (
            spark.read.format("iceberg")
            .option("snapshot-id", str(snapshot_id))
            .load(table_name)
            .select(primary_key, identity_column)
        )
        candidate_identity_count = candidate.where(identity_predicate).count()
        if candidate_identity_count:
            owned_snapshots.append((row, candidate, candidate_identity_count))

    if expected_row_count == 0:
        if owned_snapshots:
            raise RuntimeError(
                f"{table_name} has an identity snapshot despite zero expected rows"
            )
        return None
    if len(owned_snapshots) != 1:
        raise RuntimeError(
            f"{table_name} must have exactly one identity snapshot; "
            f"found {len(owned_snapshots)}"
        )

    row, candidate, candidate_identity_count = owned_snapshots[0]
    snapshot_id = int(row["snapshot_id"])
    parent_id = row["parent_id"]
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
        addition_key = [primary_key, identity_column]
        parent_keys = parent.select(*addition_key).dropDuplicates(addition_key)
        additions = candidate.join(parent_keys, addition_key, "left_anti")

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
            configs[f"{prefix}.http-client.type"] = "urlconnection"
            configs[f"{prefix}.http-client.urlconnection.connection-timeout-ms"] = (
                "60000"
            )
            configs[f"{prefix}.http-client.urlconnection.socket-timeout-ms"] = "120000"
            configs[f"{prefix}.s3.retry.num-retries"] = "32"
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
