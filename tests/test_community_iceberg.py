from __future__ import annotations

from pathlib import Path

from video_media_catalog.community_iceberg import CommunityCatalogTables
from video_media_catalog.community_ingest import (
    CommunityIngestCommit,
    IngestRunKind,
    build_community_ingest_run,
)
from video_media_catalog.community_tables import DATA_TABLE_COLUMNS, TABLE_COLUMNS
from video_media_catalog.iceberg import CatalogConfig


class FakeCatalog:
    def __init__(self) -> None:
        self.dropped: list[str] = []

    def dropTempView(self, name: str) -> None:
        self.dropped.append(name)


class FakeConf:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    def get(self, key: str, default: str) -> str:
        return self.values.get(key, default)

    def set(self, key: str, value: str) -> None:
        self.values[key] = value


class FakeResult:
    def collect(self):
        return []


class FakeSpark:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.catalog = FakeCatalog()
        self.conf = FakeConf()

    def sql(self, statement: str):
        self.statements.append(statement)
        return FakeResult()


class FakeFrame:
    def __init__(self, table: str, count: int) -> None:
        self.columns = list(TABLE_COLUMNS[table])
        self._count = count
        self.view: str | None = None

    def select(self, *columns: str):
        assert columns == tuple(self.columns)
        return self

    def dropDuplicates(self, keys: list[str]):
        assert len(keys) == 1
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


def test_creates_v2_tables_with_policy_aware_types(tmp_path: Path) -> None:
    spark = FakeSpark()
    tables = CommunityCatalogTables(
        spark,
        CatalogConfig(
            catalog_name="media",
            namespace="community_v2",
            warehouse=(tmp_path / "warehouse").as_uri(),
        ),
    )
    tables.create_tables()
    creates = [
        statement
        for statement in spark.statements
        if "CREATE TABLE IF NOT EXISTS" in statement
    ]
    assert len(creates) == len(TABLE_COLUMNS)
    assert any("`imported_v1` BOOLEAN NOT NULL" in sql for sql in creates)
    assert any("`confidence` DOUBLE" in sql for sql in creates)
    assert all("format-version" in sql for sql in creates)


def test_v2_merge_is_insert_only_and_commit_is_unique_by_run(
    tmp_path: Path,
) -> None:
    spark = FakeSpark()
    tables = CommunityCatalogTables(
        spark,
        CatalogConfig(
            catalog_name="media",
            namespace="community_v2",
            warehouse=(tmp_path / "warehouse").as_uri(),
        ),
    )
    frame = FakeFrame("community_ingest_commit", 1)
    assert tables.merge_insert_only("community_ingest_commit", frame) == 1
    merge = spark.statements[-1]
    assert "ON t.`run_id` = s.`run_id`" in merge
    assert "WHEN NOT MATCHED THEN INSERT" in merge
    assert "WHEN MATCHED" not in merge


def test_empty_v2_merge_does_not_write(tmp_path: Path) -> None:
    spark = FakeSpark()
    tables = CommunityCatalogTables(
        spark,
        CatalogConfig(
            catalog_name="media",
            namespace="community_v2",
            warehouse=(tmp_path / "warehouse").as_uri(),
        ),
    )
    assert (
        tables.merge_insert_only(
            "community_source_record",
            FakeFrame("community_source_record", 0),
        )
        == 0
    )
    assert not spark.statements


class StageFrame:
    def __init__(self, count: int, run_id: str) -> None:
        self._count = count
        self.run_id = run_id

    def count(self) -> int:
        return self._count

    def where(self, predicate: str):
        expected = f"`run_id` <> '{self.run_id}'"
        return StageFrame(0 if expected in predicate else 1, self.run_id)

    def limit(self, count: int):
        assert count == 1
        return self


class StageSpark:
    def createDataFrame(self, rows):
        return rows


class RecordingTables(CommunityCatalogTables):
    def __init__(self, run_counts: dict[str, int]) -> None:
        super().__init__(
            StageSpark(),
            CatalogConfig(
                catalog_name="media",
                namespace="community_v2",
                warehouse="file:///tmp/community-v2-test",
            ),
        )
        self.run_counts = run_counts
        self.events: list[str] = []
        self.commit: CommunityIngestCommit | None = None

    def create_tables(self) -> None:
        self.events.append("create")

    def read_commit(self, run_id: str):
        return self.commit

    def merge_insert_only(self, table: str, dataframe) -> int:
        self.events.append(table)
        if table == "community_ingest_commit":
            self.commit = CommunityIngestCommit.model_validate_json(
                dataframe[0]["commit_json"]
            )
        return len(dataframe) if isinstance(dataframe, list) else dataframe.count()

    def _verify_run_manifest(self, run) -> None:
        self.events.append("verify-run")

    def _run_row_count(self, table: str, run_id: str) -> int:
        return self.run_counts[table]

    def _latest_snapshot_id(self, table: str) -> int | None:
        return 100 if self.run_counts[table] else None


def test_run_commit_is_published_last_and_reused() -> None:
    counts = {table: 0 for table in DATA_TABLE_COLUMNS}
    counts["community_source_record"] = 1
    run = build_community_ingest_run(
        run_kind=IngestRunKind.SOURCE_ASSERTIONS,
        source_product_id="tvmaze-public-api",
        input_id="sha256:" + ("a" * 64),
        policy_id="tvmaze-api-cc-by-sa",
        policy_digest="sha256:" + ("b" * 64),
        image_digest="sha256:" + ("c" * 64),
        config_digest="sha256:" + ("d" * 64),
        started_at="2026-09-19T00:00:00Z",
        expected_counts=counts,
        input_manifest={"recordSetId": "sha256:" + ("a" * 64)},
    )
    frames = {table: StageFrame(count, run.run_id) for table, count in counts.items()}
    tables = RecordingTables(counts)
    commit = tables.stage_and_commit(
        run=run,
        dataframes=frames,
        committed_at="2026-09-19T00:01:00Z",
    )
    assert commit.table_counts == counts
    assert tables.events[-1] == "community_ingest_commit"
    event_count = len(tables.events)
    assert (
        tables.stage_and_commit(
            run=run,
            dataframes=frames,
            committed_at="2026-09-19T00:01:00Z",
        )
        == commit
    )
    assert len(tables.events) == event_count + 2
