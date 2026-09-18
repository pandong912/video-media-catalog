from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from video_media_catalog.commit import LocalControlPublisher, publish_commit
from video_media_catalog.constants import (
    ALGORITHM_DIGEST,
    ALGORITHM_SPEC_ID,
    CURATED_TABLE_KEYS,
    PRODUCER,
    STAGE,
)
from video_media_catalog.identity import is_canonical_uuid7
from video_media_catalog.models import OutputCommit, SnapshotSet, SnapshotTable
from video_media_catalog.runtime_args import RuntimeArguments

RUN_ID = "01a081e8-6420-7000-8000-000000000202"
JOB_SPEC_ID = "01a081e8-6420-7000-8000-000000000203"
TENANT_ID = "01a081e8-6420-7000-8000-000000000204"
STAGE_TIME = "2026-09-18T06:09:59.000001Z"


def _runtime(tmp_path: Path) -> RuntimeArguments:
    return RuntimeArguments(
        manifest_uri="s3://input/source-manifest.parquet",
        manifest_hash="sha256:hex:" + "a" * 64,
        manifest_version="version-1",
        manifest_etag="etag-1",
        manifest_size=4096,
        run_id=RUN_ID,
        job_spec_id=JOB_SPEC_ID,
        tenant_id=TENANT_ID,
        attempt=1,
        output_prefix=(tmp_path / "run").as_uri(),
        executor_image="registry.example/catalog@sha256:" + "c" * 64,
    )


def _row_counts() -> dict[str, int]:
    return {
        "catalog_source_record": 2,
        "catalog_entity": 7,
        "catalog_name": 4,
        "catalog_external_identifier": 2,
        "catalog_relation": 3,
        "catalog_ingest_error": 0,
    }


def _tables(*, changed_entity_snapshot: bool = False) -> list[SnapshotTable]:
    result = []
    for index, table in enumerate(CURATED_TABLE_KEYS, start=101):
        snapshot_id = index
        if table == "catalog_entity" and changed_entity_snapshot:
            snapshot_id = 999
        result.append(
            SnapshotTable(
                table_name=f"governance.media_catalog.{table}",
                snapshot_id=(None if table == "catalog_ingest_error" else snapshot_id),
                parent_snapshot_id=None,
                committed_at=STAGE_TIME,
                operation=("empty" if table == "catalog_ingest_error" else "append"),
                record_count=_row_counts()[table],
            )
        )
    return result


def _publish(
    tmp_path: Path,
    *,
    tables: list[SnapshotTable] | None = None,
):
    return publish_commit(
        publisher=LocalControlPublisher(tmp_path / "control"),
        runtime=_runtime(tmp_path),
        tables=tables or _tables(),
        row_counts=_row_counts(),
        config_digest="sha256:" + "f" * 64,
        stage_time=STAGE_TIME,
    )


def test_fixed_algorithm_constants() -> None:
    assert STAGE == "media-catalog-commit"
    assert PRODUCER == "video-media-catalog-spark/1.0.0"
    assert ALGORITHM_SPEC_ID == "media-catalog-wikidata-eidr-v1"
    assert (
        "sha256:" + hashlib.sha256(ALGORITHM_SPEC_ID.encode()).hexdigest()
    ) == ALGORITHM_DIGEST


def test_snapshot_and_output_commit_match_control_protojson(
    tmp_path: Path,
) -> None:
    snapshot, commit = _publish(tmp_path)
    snapshot_path = tmp_path / "control" / "snapshot-set.json"
    commit_path = tmp_path / "control" / "output.commit.json"
    snapshot_json = json.loads(snapshot_path.read_bytes())
    commit_json = json.loads(commit_path.read_bytes())

    assert is_canonical_uuid7(snapshot.snapshot_set_id)
    assert is_canonical_uuid7(commit.commit_id)
    assert snapshot.input_manifest.format == "OBJECT_FORMAT_PARQUET"
    assert snapshot.input_manifest.media_type == "application/vnd.apache.parquet"
    assert snapshot.input_manifest.object_version == "version-1"
    assert snapshot.input_manifest.etag == "etag-1"
    assert "attributes" not in snapshot_json["inputManifest"]
    assert snapshot_json["outputCount"] == "7"
    empty = next(
        table
        for table in snapshot_json["tables"]
        if table["tableName"].endswith(".catalog_ingest_error")
    )
    assert "snapshotId" not in empty
    assert empty["operation"] == "empty"
    assert empty["recordCount"] == "0"
    assert commit_json["labels"]["input_manifest_digest"] == ("sha256:hex:" + "a" * 64)
    for table, count in _row_counts().items():
        key = f"{table}_count"
        assert snapshot.metrics[key] == str(count)
        assert commit.labels[key] == str(count)
    assert commit.output_manifest.uri == snapshot_path.as_uri()
    assert SnapshotSet.model_validate_json(snapshot_path.read_bytes()) == snapshot
    assert OutputCommit.model_validate_json(commit_path.read_bytes()) == commit


def test_commit_rerun_reuses_fully_compatible_objects(tmp_path: Path) -> None:
    first = _publish(tmp_path)
    first_bytes = (tmp_path / "control" / "output.commit.json").read_bytes()
    second = _publish(tmp_path)
    assert first == second
    assert (tmp_path / "control" / "output.commit.json").read_bytes() == first_bytes


def test_commit_rerun_rejects_table_metadata_conflict(tmp_path: Path) -> None:
    _publish(tmp_path)
    with pytest.raises(RuntimeError, match="SnapshotSet"):
        _publish(tmp_path, tables=_tables(changed_entity_snapshot=True))
