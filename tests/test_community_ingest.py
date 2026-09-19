from __future__ import annotations

import pytest

from video_media_catalog.community_ingest import (
    IngestRunKind,
    build_community_ingest_commit,
    build_community_ingest_run,
)
from video_media_catalog.community_tables import DATA_TABLE_COLUMNS


def _counts(**overrides) -> dict[str, int]:
    values = {table: 0 for table in DATA_TABLE_COLUMNS}
    values.update(overrides)
    return values


def test_ingest_run_and_commit_have_deterministic_identity() -> None:
    run = build_community_ingest_run(
        run_kind=IngestRunKind.SOURCE_ASSERTIONS,
        source_product_id="tvmaze-public-api",
        input_id="sha256:" + ("a" * 64),
        policy_id="tvmaze-api-cc-by-sa",
        policy_digest="sha256:" + ("b" * 64),
        image_digest="sha256:" + ("c" * 64),
        config_digest="sha256:" + ("d" * 64),
        started_at="2026-09-19T00:00:00Z",
        expected_counts=_counts(community_source_record=1),
        input_manifest={"recordSetId": "sha256:" + ("a" * 64)},
    )
    repeated = type(run).model_validate_json(run.json_bytes())
    assert run == repeated

    commit = build_community_ingest_commit(
        run_id=run.run_id,
        committed_at="2026-09-19T00:01:00Z",
        table_counts=run.expected_counts,
        table_snapshot_ids={
            table: (100 if table == "community_source_record" else None)
            for table in DATA_TABLE_COLUMNS
        },
    )
    assert commit == type(commit).model_validate_json(commit.json_bytes())


def test_ingest_run_requires_counts_for_every_table() -> None:
    with pytest.raises(ValueError, match="every v2 data table"):
        build_community_ingest_run(
            run_kind=IngestRunKind.SOURCE_ASSERTIONS,
            source_product_id="tvmaze-public-api",
            input_id="sha256:" + ("a" * 64),
            policy_id="tvmaze-api-cc-by-sa",
            policy_digest="sha256:" + ("b" * 64),
            image_digest="sha256:" + ("c" * 64),
            config_digest="sha256:" + ("d" * 64),
            started_at="2026-09-19T00:00:00Z",
            expected_counts={},
            input_manifest={"recordSetId": "sha256:" + ("a" * 64)},
        )


def test_commit_requires_snapshot_for_positive_run_count() -> None:
    with pytest.raises(ValueError, match="no containing snapshot"):
        build_community_ingest_commit(
            run_id="sha256:" + ("a" * 64),
            committed_at="2026-09-19T00:01:00Z",
            table_counts=_counts(community_source_record=1),
            table_snapshot_ids={table: None for table in DATA_TABLE_COLUMNS},
        )
