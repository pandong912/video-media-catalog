from __future__ import annotations

from pathlib import Path

import pytest

from video_media_catalog.community_iceberg import CommunityCatalogTables
from video_media_catalog.community_ingest import (
    CommunityIngestCommit,
    CommunityIngestRun,
    IngestRunKind,
    build_community_ingest_commit,
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
        assert keys
        assert set(keys).issubset(self.columns)
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
    external_index = next(
        sql for sql in creates if "`community_external_id_index`" in sql
    )
    assert "bucket(128, `blocking_key`)" in external_index
    assert any("`community_identity_conflict`" in sql for sql in creates)
    assert any("`community_entity_merge_event`" in sql for sql in creates)
    assert any("`community_entity_split_event`" in sql for sql in creates)
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


def test_source_record_merge_matches_envelope_and_run(tmp_path: Path) -> None:
    spark = FakeSpark()
    tables = CommunityCatalogTables(
        spark,
        CatalogConfig(
            catalog_name="media",
            namespace="community_v2",
            warehouse=(tmp_path / "warehouse").as_uri(),
        ),
    )
    frame = FakeFrame("community_source_record", 2)
    assert tables.merge_insert_only("community_source_record", frame) == 2
    merge = spark.statements[-1]
    assert "ON t.`envelope_key` = s.`envelope_key` AND t.`run_id` = s.`run_id`" in merge
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
        self.count_calls = 0

    def count(self) -> int:
        self.count_calls += 1
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
    def __init__(
        self,
        run_counts: dict[str, int] | None = None,
        owned_snapshots: dict[str, list[int]] | None = None,
    ) -> None:
        super().__init__(
            StageSpark(),
            CatalogConfig(
                catalog_name="media",
                namespace="community_v2",
                warehouse="file:///tmp/community-v2-test",
            ),
        )
        self.run_counts = {table: 0 for table in DATA_TABLE_COLUMNS} | dict(
            run_counts or {}
        )
        self.owned_snapshots = {table: [] for table in DATA_TABLE_COLUMNS}
        for table, snapshots in (owned_snapshots or {}).items():
            self.owned_snapshots[table] = list(snapshots)
        self.events: list[str] = []
        self.merge_events: list[str] = []
        self.commit: CommunityIngestCommit | None = None
        self.manifest: CommunityIngestRun | None = None
        self.snapshot_properties: dict[str, list[dict[str, str] | None]] = {}
        self.concurrent_data_winners: set[str] = set()
        self.concurrent_commit_winner = False
        self.raise_after_commit_winner = False
        self._next_snapshot_id = 100

    def create_tables(self) -> None:
        self.events.append("create")

    def read_commit(self, run_id: str):
        self.events.append("read-commit")
        return self.commit

    def _read_run_manifest(self, run_id: str):
        self.events.append("read-run")
        return self.manifest

    def merge_insert_only(
        self,
        table: str,
        dataframe,
        *,
        snapshot_properties=None,
    ) -> int:
        self.events.append(f"merge:{table}")
        self.merge_events.append(table)
        self.snapshot_properties.setdefault(table, []).append(snapshot_properties)
        if table == "community_ingest_run":
            proposed = CommunityIngestRun.model_validate_json(
                dataframe[0]["manifest_json"]
            )
            if self.manifest is None:
                self.manifest = proposed
            return 1
        if table == "community_ingest_commit":
            proposed = CommunityIngestCommit.model_validate_json(
                dataframe[0]["commit_json"]
            )
            if self.concurrent_commit_winner:
                self.commit = build_community_ingest_commit(
                    run_id=proposed.run_id,
                    committed_at="2026-09-20T00:00:30Z",
                    table_counts=proposed.table_counts,
                    table_snapshot_ids=proposed.table_snapshot_ids,
                )
                if self.raise_after_commit_winner:
                    raise RuntimeError("concurrent commit winner")
            elif self.commit is None:
                self.commit = proposed
            return 1

        count = len(dataframe) if isinstance(dataframe, list) else dataframe.count()
        if table in DATA_TABLE_COLUMNS and count:
            self._next_snapshot_id += 1
            if table in self.concurrent_data_winners:
                self.run_counts[table] = count
                self.owned_snapshots[table] = [self._next_snapshot_id]
                raise RuntimeError("concurrent MERGE winner")
            self.run_counts[table] += count
            self.owned_snapshots[table].append(self._next_snapshot_id)
        return count

    def _run_row_count(self, table: str, run_id: str) -> int:
        self.events.append(f"count:{table}")
        return self.run_counts[table]

    def _run_snapshot_id(
        self,
        table: str,
        run_id: str,
        *,
        expected_row_count: int,
    ) -> int | None:
        self.events.append(f"snapshot:{table}:{expected_row_count}")
        snapshots = self.owned_snapshots[table]
        if expected_row_count == 0:
            if snapshots:
                raise RuntimeError(
                    f"{table} has an identity snapshot despite zero expected rows"
                )
            return None
        if len(snapshots) != 1:
            raise RuntimeError(
                f"{table} must have exactly one identity snapshot; "
                f"found {len(snapshots)}"
            )
        if self.run_counts[table] != expected_row_count:
            raise RuntimeError(f"{table} owned snapshot row count differs")
        return snapshots[0]

    def _latest_snapshot_id(self, table: str) -> int | None:
        raise AssertionError(
            "stage_and_commit must not read the global latest snapshot"
        )


def _run_with_counts(**count_overrides: int) -> CommunityIngestRun:
    counts = {table: 0 for table in DATA_TABLE_COLUMNS}
    counts.update(count_overrides)
    return build_community_ingest_run(
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


def _frames_for(run: CommunityIngestRun) -> dict[str, StageFrame]:
    return {
        table: StageFrame(count, run.run_id)
        for table, count in run.expected_counts.items()
    }


def test_complete_table_skips_frame_and_commit_stays_last() -> None:
    run = _run_with_counts(
        community_source_record=1,
        community_field_assertion=1,
    )
    frames = _frames_for(run)
    tables = RecordingTables(
        run_counts={"community_source_record": 1},
        owned_snapshots={"community_source_record": [41]},
    )

    commit = tables.stage_and_commit(
        run=run,
        dataframes=frames,
        committed_at="2026-09-19T00:01:00Z",
    )

    assert frames["community_source_record"].count_calls == 0
    assert "community_source_record" not in tables.merge_events
    assert "community_field_assertion" in tables.merge_events
    assert commit.table_snapshot_ids["community_source_record"] == 41
    assert tables.merge_events[-1] == "community_ingest_commit"


def test_zero_persisted_rows_validate_and_merge_frame() -> None:
    run = _run_with_counts(community_source_record=1)
    frames = _frames_for(run)
    tables = RecordingTables()

    commit = tables.stage_and_commit(
        run=run,
        dataframes=frames,
        committed_at="2026-09-19T00:01:00Z",
    )

    assert commit.table_counts == run.expected_counts
    assert frames["community_source_record"].count_calls >= 2
    assert tables.snapshot_properties["community_source_record"] == [
        {"video-media-catalog.run-id": run.run_id}
    ]
    assert len(tables.owned_snapshots["community_source_record"]) == 1


def test_lazy_factory_loads_only_the_missing_nonempty_table() -> None:
    run = _run_with_counts(
        community_source_record=1,
        community_field_assertion=1,
    )
    tables = RecordingTables(
        run_counts={"community_source_record": 1},
        owned_snapshots={"community_source_record": [41]},
    )
    requested: list[str] = []

    def dataframe_factory(table: str) -> StageFrame:
        requested.append(table)
        return StageFrame(run.expected_counts[table], run.run_id)

    tables.stage_and_commit(
        run=run,
        dataframes={},
        dataframe_factory=dataframe_factory,
        committed_at="2026-09-19T00:01:00Z",
    )

    assert requested == ["community_field_assertion"]


@pytest.mark.parametrize(
    ("current", "message"),
    [(1, "partial persisted rows"), (3, "more than expected persisted rows")],
)
def test_invalid_persisted_count_fails_without_restaging(
    current: int,
    message: str,
) -> None:
    run = _run_with_counts(community_source_record=2)
    frames = _frames_for(run)
    tables = RecordingTables(
        run_counts={"community_source_record": current},
        owned_snapshots={"community_source_record": [41]},
    )

    with pytest.raises(RuntimeError, match=message):
        tables.stage_and_commit(
            run=run,
            dataframes=frames,
            committed_at="2026-09-19T00:01:00Z",
        )

    assert frames["community_source_record"].count_calls == 0
    assert "community_source_record" not in tables.merge_events
    assert "community_ingest_commit" not in tables.merge_events


@pytest.mark.parametrize(
    ("expected", "current", "snapshots", "message"),
    [
        (1, 1, [], "found 0"),
        (1, 1, [41, 42], "found 2"),
        (0, 0, [41], "despite zero expected rows"),
    ],
)
def test_snapshot_anomalies_fail_closed(
    expected: int,
    current: int,
    snapshots: list[int],
    message: str,
) -> None:
    run = _run_with_counts(community_source_record=expected)
    tables = RecordingTables(
        run_counts={"community_source_record": current},
        owned_snapshots={"community_source_record": snapshots},
    )

    with pytest.raises(RuntimeError, match=message):
        tables.stage_and_commit(
            run=run,
            dataframes=_frames_for(run),
            committed_at="2026-09-19T00:01:00Z",
        )

    assert "community_ingest_commit" not in tables.merge_events


def test_exact_run_manifest_is_reused_and_conflict_fails() -> None:
    run = _run_with_counts(community_source_record=1)
    exact = RecordingTables()
    exact.manifest = run
    exact.stage_and_commit(
        run=run,
        dataframes=_frames_for(run),
        committed_at="2026-09-19T00:01:00Z",
    )
    assert "community_ingest_run" not in exact.merge_events

    payload = run.model_dump(mode="python")
    payload["started_at"] = "2026-09-19T00:00:01Z"
    conflicting = CommunityIngestRun.model_validate(
        payload,
        context={"skip_identity": True},
    )
    conflict = RecordingTables()
    conflict.manifest = conflicting
    with pytest.raises(RuntimeError, match="manifest conflicts"):
        conflict.stage_and_commit(
            run=run,
            dataframes={},
            committed_at="2026-09-19T00:01:00Z",
        )
    assert not conflict.merge_events


def test_concurrent_merge_winner_is_verified_without_second_snapshot() -> None:
    run = _run_with_counts(community_source_record=1)
    tables = RecordingTables()
    tables.concurrent_data_winners.add("community_source_record")

    commit = tables.stage_and_commit(
        run=run,
        dataframes=_frames_for(run),
        committed_at="2026-09-19T00:01:00Z",
    )

    assert commit.table_counts["community_source_record"] == 1
    assert len(tables.owned_snapshots["community_source_record"]) == 1
    assert tables.merge_events[-1] == "community_ingest_commit"


def test_concurrent_commit_winner_is_returned_after_merge_conflict() -> None:
    run = _run_with_counts()
    tables = RecordingTables()
    tables.concurrent_commit_winner = True
    tables.raise_after_commit_winner = True

    commit = tables.stage_and_commit(
        run=run,
        dataframes=_frames_for(run),
        committed_at="2026-09-20T00:01:00Z",
    )

    assert commit.committed_at == "2026-09-20T00:00:30Z"
    assert tables.merge_events[-1] == "community_ingest_commit"


def test_exact_retry_reuses_commit_without_frames_or_merges() -> None:
    run = _run_with_counts(community_source_record=1)
    frames = _frames_for(run)
    tables = RecordingTables()
    first = tables.stage_and_commit(
        run=run,
        dataframes=frames,
        committed_at="2026-09-19T00:01:00Z",
    )
    merge_count = len(tables.merge_events)
    frame_counts = {table: frame.count_calls for table, frame in frames.items()}

    replay = tables.stage_and_commit(
        run=run,
        dataframes={},
        committed_at="2026-09-19T00:02:00Z",
    )

    assert replay == first
    assert len(tables.merge_events) == merge_count
    assert {table: frame.count_calls for table, frame in frames.items()} == frame_counts
