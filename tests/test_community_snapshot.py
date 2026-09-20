from __future__ import annotations

import pytest

from video_media_catalog.community_snapshot import (
    build_community_silver_snapshot_set,
)
from video_media_catalog.community_tables import DATA_TABLE_COLUMNS


def test_silver_snapshot_set_is_deterministic() -> None:
    values = {
        "committed_run_ids": (
            "sha256:" + ("b" * 64),
            "sha256:" + ("a" * 64),
        ),
        "run_snapshot_id": 99,
        "commit_snapshot_id": 100,
        "data_snapshot_ids": {
            table: (200 if table == "community_source_record" else None)
            for table in DATA_TABLE_COLUMNS
        },
        "created_at": "2026-09-19T00:00:00Z",
    }
    first = build_community_silver_snapshot_set(**values)
    second = build_community_silver_snapshot_set(
        **{
            **values,
            "committed_run_ids": tuple(reversed(values["committed_run_ids"])),
        }
    )
    assert first == second
    assert first == type(first).model_validate_json(first.json_bytes())


def test_silver_snapshot_set_requires_all_data_tables() -> None:
    with pytest.raises(ValueError, match="every data table"):
        build_community_silver_snapshot_set(
            committed_run_ids=("sha256:" + ("a" * 64),),
            run_snapshot_id=99,
            commit_snapshot_id=100,
            data_snapshot_ids={},
            created_at="2026-09-19T00:00:00Z",
        )
