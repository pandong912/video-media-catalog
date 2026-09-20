from __future__ import annotations

from pathlib import Path

import pytest

from video_media_catalog.iceberg import (
    TABLE_COLUMNS,
    CatalogConfig,
    MediaCatalogTables,
)


class Builder:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    def config(self, key: str, value: str):
        self.values[key] = value
        return self


class FakeCatalog:
    def __init__(self) -> None:
        self.dropped: list[str] = []

    def dropTempView(self, name: str) -> None:
        self.dropped.append(name)


class FakeResult:
    def __init__(self, rows=None) -> None:
        self.rows = rows or []

    def collect(self):
        return self.rows


class FakeConf:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    def get(self, key: str, default: str) -> str:
        return self.values.get(key, default)

    def set(self, key: str, value: str) -> None:
        self.values[key] = value


class FakeSpark:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.catalog = FakeCatalog()
        self.conf = FakeConf()

    def sql(self, statement: str):
        self.statements.append(statement)
        return FakeResult()


class SnapshotSpark(FakeSpark):
    def sql(self, statement: str):
        self.statements.append(statement)
        if "catalog_entity`.snapshots" in statement:
            return FakeResult(
                [
                    {
                        "snapshot_id": 123,
                        "parent_id": 122,
                        "committed_at": "2026-09-18T06:00:00Z",
                        "operation": "append",
                    }
                ]
            )
        return FakeResult()


class FakeFrame:
    def __init__(self, count: int) -> None:
        self.columns = list(TABLE_COLUMNS["catalog_entity"])
        self._count = count
        self.view: str | None = None

    def select(self, *columns: str):
        assert columns == TABLE_COLUMNS["catalog_entity"]
        return self

    def dropDuplicates(self, keys: list[str]):
        assert keys == ["entity_key"]
        return self

    def persist(self):
        return self

    def unpersist(self) -> None:
        return None

    def count(self) -> int:
        return self._count

    def where(self, predicate: str):
        assert "IS NULL" in predicate
        return self

    def limit(self, count: int):
        assert count == 1
        self._count = 0
        return self

    def createOrReplaceTempView(self, view: str) -> None:
        self.view = view


def test_hadoop_and_glue_catalog_configuration(tmp_path: Path) -> None:
    hadoop = CatalogConfig(
        catalog_name="media",
        namespace="catalog_v1",
        warehouse=(tmp_path / "warehouse").as_uri(),
    )
    builder = hadoop.configure_builder(Builder())
    assert builder.values["spark.sql.catalog.media.type"] == "hadoop"

    glue = CatalogConfig(
        catalog_name="media",
        namespace="catalog_v1",
        warehouse="s3://example-bucket/catalog",
        catalog_type="glue",
        aws_region="us-east-1",
        s3_endpoint="https://s3.us-east-1.amazonaws.com",
    )
    values = glue.spark_configs()
    assert values["spark.sql.catalog.media.catalog-impl"].endswith("GlueCatalog")
    assert values["spark.sql.catalog.media.io-impl"].endswith("S3FileIO")
    assert values["spark.sql.catalog.media.client.region"] == "us-east-1"
    assert values["spark.hadoop.fs.s3a.aws.credentials.provider"] == (
        "com.amazonaws.auth.WebIdentityTokenCredentialsProvider"
    )
    emr = CatalogConfig(
        catalog_name="media",
        namespace="catalog_v1",
        warehouse="s3://example-bucket/catalog",
        catalog_type="glue",
        s3_credentials_provider="default",
    )
    assert emr.spark_configs()["spark.hadoop.fs.s3a.aws.credentials.provider"] == (
        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    )


def test_catalog_identifiers_are_validated() -> None:
    with pytest.raises(ValueError, match="safe Spark identifier"):
        CatalogConfig(
            catalog_name="media; DROP TABLE x",
            namespace="catalog",
            warehouse="file:///tmp/catalog",
        )
    with pytest.raises(ValueError, match="credentials provider"):
        CatalogConfig(
            catalog_name="media",
            namespace="catalog",
            warehouse="s3://example-bucket/catalog",
            catalog_type="glue",
            s3_credentials_provider="unknown",  # type: ignore[arg-type]
        )


def test_creates_six_tables_and_uses_insert_only_merge(tmp_path: Path) -> None:
    spark = FakeSpark()
    tables = MediaCatalogTables(
        spark,
        CatalogConfig(
            catalog_name="media",
            namespace="catalog_v1",
            warehouse=(tmp_path / "warehouse").as_uri(),
        ),
    )
    tables.create_tables()
    assert sum("CREATE TABLE IF NOT EXISTS" in sql for sql in spark.statements) == 6

    frame = FakeFrame(2)
    assert tables.merge_insert_only("catalog_entity", frame) == 2
    merge = spark.statements[-1]
    assert "WHEN NOT MATCHED THEN INSERT" in merge
    assert "WHEN MATCHED" not in merge
    assert frame.view is not None
    assert spark.catalog.dropped == [frame.view]


def test_empty_merge_does_not_create_snapshot(tmp_path: Path) -> None:
    spark = FakeSpark()
    tables = MediaCatalogTables(
        spark,
        CatalogConfig(
            catalog_name="media",
            namespace="catalog_v1",
            warehouse=(tmp_path / "warehouse").as_uri(),
        ),
    )
    frame = FakeFrame(0)
    assert tables.merge_insert_only("catalog_entity", frame) == 0
    assert not spark.statements


def test_captures_real_snapshot_metadata_and_omits_empty_snapshot_id(
    tmp_path: Path,
) -> None:
    spark = SnapshotSpark()
    tables = MediaCatalogTables(
        spark,
        CatalogConfig(
            catalog_name="governance",
            namespace="media_catalog",
            warehouse=(tmp_path / "warehouse").as_uri(),
        ),
    )
    counts = {table: 0 for table in TABLE_COLUMNS}
    counts["catalog_entity"] = 2
    snapshots = tables.capture_snapshots(
        counts,
        empty_committed_at="2026-09-18T06:00:00Z",
    )
    entity = next(
        item for item in snapshots if item.table_name.endswith(".catalog_entity")
    )
    errors = next(
        item for item in snapshots if item.table_name.endswith(".catalog_ingest_error")
    )
    assert entity.snapshot_id == 123
    assert entity.parent_snapshot_id == 122
    assert entity.record_count == 2
    assert errors.snapshot_id is None
    assert errors.operation == "empty"
    assert errors.record_count == 0
