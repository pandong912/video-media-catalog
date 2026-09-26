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
from video_media_catalog.community_tables import (
    DATA_TABLE_COLUMNS,
    IDENTITY_TABLES,
    SOURCE_TABLES,
    TABLE_COLUMNS,
    build_community_table_mapping,
    identity_physical_table_name,
)
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
    def __init__(self, rows=None) -> None:
        self.rows = [] if rows is None else rows

    def collect(self):
        return self.rows


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


def test_generation_mapping_is_safe_stable_and_keeps_source_fixed() -> None:
    generation = "catalog.release-2026-09"
    first = build_community_table_mapping(generation)
    second = build_community_table_mapping(generation)

    assert first == second
    assert all(first[table] == table for table in SOURCE_TABLES)
    assert all(first[table] != table for table in IDENTITY_TABLES)
    assert all(len(first[table]) <= 127 for table in IDENTITY_TABLES)
    assert all(first[table].replace("_", "").isalnum() for table in IDENTITY_TABLES)
    assert (
        identity_physical_table_name(
            "community_entity_ledger",
            generation,
        )
        == first["community_entity_ledger"]
    )
    with pytest.raises(ValueError, match="lowercase slug"):
        build_community_table_mapping("bad generation;drop table")


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


def test_generation_create_and_merge_resolve_identity_physical_table(
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
        identity_generation_id="catalog-2026-09",
    )
    tables.create_tables()
    physical = tables.table_mapping["community_entity_ledger"]
    assert physical != "community_entity_ledger"
    assert any(
        f".`{physical}`" in statement
        for statement in spark.statements
        if "CREATE TABLE IF NOT EXISTS" in statement
    )
    assert any(
        ".`community_source_record`" in statement
        for statement in spark.statements
        if "CREATE TABLE IF NOT EXISTS" in statement
    )


def test_same_entity_key_merges_into_new_generation_not_legacy_table(
    tmp_path: Path,
) -> None:
    spark = FakeSpark()
    config = CatalogConfig(
        catalog_name="media",
        namespace="community_v2",
        warehouse=(tmp_path / "warehouse").as_uri(),
    )
    legacy = CommunityCatalogTables(spark, config)
    generated = CommunityCatalogTables(
        spark,
        config,
        identity_generation_id="catalog-2026-09",
    )

    assert (
        legacy.merge_insert_only(
            "community_entity_ledger",
            FakeFrame("community_entity_ledger", 1),
        )
        == 1
    )
    assert (
        generated.merge_insert_only(
            "community_entity_ledger",
            FakeFrame("community_entity_ledger", 1),
        )
        == 1
    )
    merges = [statement for statement in spark.statements if "MERGE INTO" in statement]
    assert ".`community_entity_ledger` t" in merges[0]
    assert f".`{generated.table_mapping['community_entity_ledger']}` t" in merges[1]


def test_full_generation_rejects_any_nonempty_identity_table(
    tmp_path: Path,
) -> None:
    class NonEmptyIdentitySpark(FakeSpark):
        def sql(self, statement: str):
            self.statements.append(statement)
            if (
                "SELECT 1 AS present" in statement
                and "community_entity_ledger__g_" in statement
            ):
                return FakeResult([{"present": 1}])
            return FakeResult()

    tables = CommunityCatalogTables(
        NonEmptyIdentitySpark(),
        CatalogConfig(
            catalog_name="media",
            namespace="community_v2",
            warehouse=(tmp_path / "warehouse").as_uri(),
        ),
        identity_generation_id="catalog-2026-09",
    )
    with pytest.raises(RuntimeError, match="requires empty physical tables"):
        tables.assert_identity_tables_empty()


def test_incremental_generation_requires_pinned_identity_heads() -> None:
    tables = RecordingTables(identity_generation_id="catalog-2026-09")
    snapshots = {table: None for table in DATA_TABLE_COLUMNS}
    snapshots["community_entity_ledger"] = 41
    tables.owned_snapshots["community_entity_ledger"] = [41]
    tables.assert_identity_snapshot_heads(snapshots)

    tables.owned_snapshots["community_entity_ledger"] = [42]
    with pytest.raises(RuntimeError, match="snapshot is stale"):
        tables.assert_identity_snapshot_heads(snapshots)


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
    def __init__(self, table: str, count: int, run_id: str) -> None:
        self.table = table
        self.columns = list(TABLE_COLUMNS[table])
        self._count = count
        self.run_id = run_id
        self.count_calls = 0

    def count(self) -> int:
        self.count_calls += 1
        return self._count

    def select(self, *columns: str):
        assert set(columns).issubset(self.columns)
        return self

    def dropDuplicates(self, keys: list[str]):
        assert set(keys).issubset(self.columns)
        return self

    def where(self, predicate: str):
        return StageFrame(self.table, 0, self.run_id)

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
        identity_generation_id: str | None = None,
    ) -> None:
        super().__init__(
            StageSpark(),
            CatalogConfig(
                catalog_name="media",
                namespace="community_v2",
                warehouse="file:///tmp/community-v2-test",
            ),
            identity_generation_id=identity_generation_id,
        )
        self.run_counts = {table: 0 for table in DATA_TABLE_COLUMNS} | dict(
            run_counts or {}
        )
        self.owned_snapshots = {table: [] for table in TABLE_COLUMNS}
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
        snapshots = self.owned_snapshots[table]
        return snapshots[-1] if snapshots else None

    def _current_snapshot_run_id(self, table: str) -> str | None:
        return None


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


def _identity_run_with_counts(
    *,
    generation: str = "catalog-2026-09",
    mode: str = "incremental",
    **count_overrides: int,
) -> CommunityIngestRun:
    counts = {table: 0 for table in DATA_TABLE_COLUMNS}
    counts.update(count_overrides)
    return build_community_ingest_run(
        run_kind=IngestRunKind.IDENTITY_RESOLUTION,
        source_product_id="identity-resolution-v2",
        input_id="sha256:" + ("a" * 64),
        policy_id="internal-key-continuity",
        policy_digest="sha256:" + ("b" * 64),
        image_digest="sha256:" + ("c" * 64),
        config_digest="sha256:" + ("d" * 64),
        started_at="2026-09-19T00:00:00Z",
        expected_counts=counts,
        input_manifest={
            "identityGenerationId": generation,
            "identityMode": mode,
            "tableMapping": build_community_table_mapping(generation),
        },
    )


def _frames_for(run: CommunityIngestRun) -> dict[str, StageFrame]:
    return {
        table: StageFrame(table, count, run.run_id)
        for table, count in run.expected_counts.items()
    }


def test_legacy_partial_run_resumes_for_source_ingestion_compatibility() -> None:
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

    assert "community_source_record" not in tables.merge_events
    assert commit.table_snapshot_ids["community_source_record"] == 41
    assert tables.merge_events[-1] == "community_ingest_commit"


def test_generation_partial_run_must_be_rolled_back_before_retry() -> None:
    run = _identity_run_with_counts(community_entity_ledger=1)
    frames = _frames_for(run)
    tables = RecordingTables(
        run_counts={"community_entity_ledger": 1},
        owned_snapshots={"community_entity_ledger": [41]},
        identity_generation_id="catalog-2026-09",
    )
    snapshots = {table: None for table in DATA_TABLE_COLUMNS}
    snapshots["community_entity_ledger"] = 41

    with pytest.raises(RuntimeError, match="rollback-failed-run"):
        tables.stage_and_commit(
            run=run,
            dataframes=frames,
            committed_at="2026-09-19T00:01:00Z",
            identity_mode="incremental",
            expected_identity_snapshot_ids=snapshots,
        )
    assert frames["community_entity_ledger"].count_calls == 0
    assert tables.merge_events == []


def test_generation_resolution_requires_explicit_mode() -> None:
    run = _identity_run_with_counts()
    tables = RecordingTables(identity_generation_id="catalog-2026-09")

    with pytest.raises(ValueError, match="requires a mode"):
        tables.stage_and_commit(
            run=run,
            dataframes=_frames_for(run),
            committed_at="2026-09-19T00:01:00Z",
        )

    assert tables.events == []


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
    assert tables.snapshot_properties["community_ingest_run"] == [
        {
            "video-media-catalog.run-id": run.run_id,
            "video-media-catalog.parent-snapshot-id": "none",
        }
    ]
    assert tables.snapshot_properties["community_source_record"] == [
        {
            "video-media-catalog.run-id": run.run_id,
            "video-media-catalog.parent-snapshot-id": "none",
        }
    ]
    assert len(tables.owned_snapshots["community_source_record"]) == 1


def test_lazy_factory_loads_all_nonempty_tables_before_writes() -> None:
    run = _run_with_counts(
        community_source_record=1,
        community_field_assertion=1,
    )
    tables = RecordingTables()
    requested: list[str] = []
    created: dict[str, StageFrame] = {}

    def dataframe_factory(table: str) -> StageFrame:
        requested.append(table)
        frame = StageFrame(table, run.expected_counts[table], run.run_id)
        created[table] = frame
        return frame

    tables.stage_and_commit(
        run=run,
        dataframes={},
        dataframe_factory=dataframe_factory,
        committed_at="2026-09-19T00:01:00Z",
    )

    assert requested == [
        "community_source_record",
        "community_field_assertion",
    ]
    assert all(frame.count_calls >= 2 for frame in created.values())


def test_legacy_lazy_factory_skips_already_complete_table() -> None:
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
        return StageFrame(table, run.expected_counts[table], run.run_id)

    tables.stage_and_commit(
        run=run,
        dataframes={},
        dataframe_factory=dataframe_factory,
        committed_at="2026-09-19T00:01:00Z",
    )

    assert requested == ["community_field_assertion"]


def test_all_frames_are_validated_before_any_write() -> None:
    run = _run_with_counts(
        community_source_record=1,
        community_field_assertion=1,
    )
    frames = _frames_for(run)
    frames["community_field_assertion"]._count = 0
    tables = RecordingTables()

    with pytest.raises(ValueError, match="staged count"):
        tables.stage_and_commit(
            run=run,
            dataframes=frames,
            committed_at="2026-09-19T00:01:00Z",
        )

    assert tables.merge_events == []
    assert tables.manifest is None


def test_identity_existing_new_key_collision_fails_preflight() -> None:
    class CollisionFrame:
        def select(self, *_columns):
            return self

        def dropDuplicates(self, _keys):
            return self

        def join(self, _other, _key, _kind):
            return self

        def limit(self, _count):
            return self

        def count(self):
            return 1

    class CollisionSpark(StageSpark):
        def table(self, _name):
            return CollisionFrame()

    run = _run_with_counts(community_entity_ledger=1)
    tables = RecordingTables()
    tables.spark = CollisionSpark()

    with pytest.raises(RuntimeError, match="existing/new key collision"):
        tables._preflight_identity_keys(
            run,
            dataframes={"community_entity_ledger": CollisionFrame()},
            identity_mode="full",
        )


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

    assert frames["community_source_record"].count_calls >= 2
    assert "community_source_record" not in tables.merge_events
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
