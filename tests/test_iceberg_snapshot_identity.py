from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from video_media_catalog.iceberg import (
    execute_iceberg_sql,
    find_owned_snapshot_id,
)

RUN_ID = "sha256:" + ("a" * 64)
OTHER_RUN_ID = "sha256:" + ("b" * 64)


class FakeFrame:
    def __init__(self, rows: list[dict[str, str]]) -> None:
        self.rows = rows

    def select(self, *columns: str):
        return FakeFrame(
            [{column: row[column] for column in columns} for row in self.rows]
        )

    def where(self, predicate: str):
        column = predicate.split("`", 2)[1]
        value = predicate.rsplit("'", 2)[1]
        if " IS NULL OR " in predicate:
            rows = [
                row
                for row in self.rows
                if row.get(column) is None or row.get(column) != value
            ]
        else:
            rows = [row for row in self.rows if row.get(column) == value]
        return FakeFrame(rows)

    def limit(self, count: int):
        return FakeFrame(self.rows[:count])

    def count(self) -> int:
        return len(self.rows)

    def dropDuplicates(self, keys: list[str]):
        seen = set()
        rows = []
        for row in self.rows:
            identity = tuple(row[key] for key in keys)
            if identity not in seen:
                seen.add(identity)
                rows.append(row)
        return FakeFrame(rows)

    def join(self, other, key: str | list[str], how: str):
        assert how == "left_anti"
        keys = (key,) if isinstance(key, str) else tuple(key)
        other_keys = {tuple(row[item] for item in keys) for row in other.rows}
        return FakeFrame(
            [
                row
                for row in self.rows
                if tuple(row[item] for item in keys) not in other_keys
            ]
        )

    def persist(self):
        return self

    def unpersist(self) -> None:
        return None


class FakeRead:
    def __init__(self, snapshots: dict[int, list[dict[str, str]]]) -> None:
        self.snapshots = snapshots
        self.snapshot_id: int | None = None
        self.loaded_snapshot_ids: list[int] = []

    def format(self, value: str):
        assert value == "iceberg"
        return self

    def option(self, key: str, value: str):
        assert key == "snapshot-id"
        self.snapshot_id = int(value)
        return self

    def load(self, table_name: str):
        assert table_name == "media.community.records"
        assert self.snapshot_id is not None
        self.loaded_snapshot_ids.append(self.snapshot_id)
        return FakeFrame(self.snapshots[self.snapshot_id])


@dataclass
class FakeSnapshotResult:
    rows: list[dict[str, int | None]]

    def collect(self):
        return self.rows


class SnapshotSpark:
    def __init__(
        self,
        snapshot_rows: list[dict[str, int | None]],
        snapshots: dict[int, list[dict[str, str]]],
    ) -> None:
        self.snapshot_rows = snapshot_rows
        self.read = FakeRead(snapshots)
        self.statements: list[str] = []

    def sql(self, statement: str):
        self.statements.append(statement)
        assert "summary['video-media-catalog.run-id']" in statement
        assert RUN_ID in statement
        return FakeSnapshotResult(self.snapshot_rows)


def test_owned_snapshot_ignores_later_writer_and_verifies_added_rows() -> None:
    parent = [{"row_key": "old", "run_id": OTHER_RUN_ID}]
    own = [
        *parent,
        {"row_key": "own-1", "run_id": RUN_ID},
        {"row_key": "own-2", "run_id": RUN_ID},
    ]
    later = [*own, {"row_key": "later", "run_id": OTHER_RUN_ID}]
    spark = SnapshotSpark(
        [{"snapshot_id": 101, "parent_id": 100}],
        {100: parent, 101: own, 999: later},
    )

    assert (
        find_owned_snapshot_id(
            spark,
            table_identifier="`media`.`community`.`records`",
            table_name="media.community.records",
            primary_key="row_key",
            identity_column="run_id",
            identity_value=RUN_ID,
            snapshot_property="video-media-catalog.run-id",
            expected_row_count=2,
        )
        == 101
    )
    assert spark.read.loaded_snapshot_ids == [101, 100]


@pytest.mark.parametrize(
    ("snapshot_rows", "expected"),
    [
        ([], "found 0"),
        (
            [
                {"snapshot_id": 101, "parent_id": 100},
                {"snapshot_id": 102, "parent_id": 101},
            ],
            "found 2",
        ),
    ],
)
def test_owned_snapshot_rejects_zero_or_multiple_marked_snapshots(
    snapshot_rows: list[dict[str, int | None]],
    expected: str,
) -> None:
    snapshots = {
        int(row["snapshot_id"]): [
            {"row_key": f"own-{row['snapshot_id']}", "run_id": RUN_ID}
        ]
        for row in snapshot_rows
    }
    spark = SnapshotSpark(snapshot_rows, snapshots)
    with pytest.raises(RuntimeError, match=expected):
        find_owned_snapshot_id(
            spark,
            table_identifier="`media`.`community`.`records`",
            table_name="media.community.records",
            primary_key="row_key",
            identity_column="run_id",
            identity_value=RUN_ID,
            snapshot_property="video-media-catalog.run-id",
            expected_row_count=1,
        )


def test_owned_snapshot_ignores_orphan_tagged_attempt() -> None:
    parent = [{"row_key": "old", "run_id": OTHER_RUN_ID}]
    own = [*parent, {"row_key": "own", "run_id": RUN_ID}]
    spark = SnapshotSpark(
        [
            {
                "snapshot_id": 101,
                "parent_id": 100,
                "added_records": "214127556",
            },
            {"snapshot_id": 102, "parent_id": 101, "added_records": "1"},
        ],
        {100: parent, 101: parent, 102: own},
    )
    assert (
        find_owned_snapshot_id(
            spark,
            table_identifier="`media`.`community`.`records`",
            table_name="media.community.records",
            primary_key="row_key",
            identity_column="run_id",
            identity_value=RUN_ID,
            snapshot_property="video-media-catalog.run-id",
            expected_row_count=1,
        )
        == 102
    )


def test_orphan_tagged_attempt_does_not_block_zero_row_probe() -> None:
    foreign = [{"row_key": "old", "run_id": OTHER_RUN_ID}]
    spark = SnapshotSpark(
        [
            {
                "snapshot_id": 101,
                "parent_id": 100,
                "added_records": "214127556",
            }
        ],
        {101: foreign},
    )
    assert (
        find_owned_snapshot_id(
            spark,
            table_identifier="`media`.`community`.`records`",
            table_name="media.community.records",
            primary_key="row_key",
            identity_column="run_id",
            identity_value=RUN_ID,
            snapshot_property="video-media-catalog.run-id",
            expected_row_count=0,
        )
        is None
    )


def test_owned_snapshot_counts_republished_key_as_addition() -> None:
    parent = [{"row_key": "same", "run_id": OTHER_RUN_ID}]
    own = [*parent, {"row_key": "same", "run_id": RUN_ID}]
    spark = SnapshotSpark(
        [{"snapshot_id": 102, "parent_id": 101, "added_records": "1"}],
        {101: parent, 102: own},
    )
    assert (
        find_owned_snapshot_id(
            spark,
            table_identifier="`media`.`community`.`records`",
            table_name="media.community.records",
            primary_key="row_key",
            identity_column="run_id",
            identity_value=RUN_ID,
            snapshot_property="video-media-catalog.run-id",
            expected_row_count=1,
        )
        == 102
    )


def test_owned_snapshot_rejects_foreign_additions() -> None:
    spark = SnapshotSpark(
        [{"snapshot_id": 101, "parent_id": None}],
        {
            101: [
                {"row_key": "own", "run_id": RUN_ID},
                {"row_key": "foreign", "run_id": OTHER_RUN_ID},
            ]
        },
    )
    with pytest.raises(RuntimeError, match="tagged snapshot added 2 rows"):
        find_owned_snapshot_id(
            spark,
            table_identifier="`media`.`community`.`records`",
            table_name="media.community.records",
            primary_key="row_key",
            identity_column="run_id",
            identity_value=RUN_ID,
            snapshot_property="video-media-catalog.run-id",
            expected_row_count=1,
        )


class FakeJavaMap(dict):
    def put(self, key: str, value: str) -> None:
        self[key] = value


class FakeCommitMetadata:
    properties: dict[str, str] | None = None

    @classmethod
    def withCommitProperties(cls, properties, callable_object, exception_class):
        cls.properties = dict(properties)
        assert exception_class is RuntimeError
        return callable_object.call()


class SqlSpark:
    def __init__(self) -> None:
        self.statements: list[str] = []
        runtime_exception = SimpleNamespace(_java_lang_class=RuntimeError)
        self._jvm = SimpleNamespace(
            java=SimpleNamespace(
                util=SimpleNamespace(HashMap=FakeJavaMap),
                lang=SimpleNamespace(RuntimeException=runtime_exception),
            ),
            org=SimpleNamespace(
                apache=SimpleNamespace(
                    iceberg=SimpleNamespace(
                        spark=SimpleNamespace(CommitMetadata=FakeCommitMetadata)
                    )
                )
            ),
        )

    def sql(self, statement: str):
        self.statements.append(statement)
        return FakeSnapshotResult([])


def test_sql_commit_properties_wrap_the_merge_atomically() -> None:
    spark = SqlSpark()
    execute_iceberg_sql(
        spark,
        "MERGE INTO target USING staged",
        snapshot_properties={"video-media-catalog.run-id": RUN_ID},
    )
    assert spark.statements == ["MERGE INTO target USING staged"]
    assert FakeCommitMetadata.properties == {"video-media-catalog.run-id": RUN_ID}
