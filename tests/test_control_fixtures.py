from __future__ import annotations

import json
from pathlib import Path

from video_media_catalog.constants import ALGORITHM_DIGEST, ALGORITHM_SPEC_ID
from video_media_catalog.identity import is_canonical_uuid7
from video_media_catalog.models import JobSpec, OutputCommit, SnapshotSet


def test_control_repository_snapshot_and_commit_fixtures_parse(
    fixture_dir: Path,
) -> None:
    control = fixture_dir / "control"
    job_spec = JobSpec.model_validate_json(
        (control / "media_catalog_job_spec.v1.json").read_bytes()
    )
    snapshot = SnapshotSet.model_validate_json(
        (control / "media_catalog_snapshot_set.v1.json").read_bytes()
    )
    commit = OutputCommit.model_validate_json(
        (control / "media_catalog_output_commit.v1.json").read_bytes()
    )

    assert is_canonical_uuid7(snapshot.snapshot_set_id)
    assert is_canonical_uuid7(commit.commit_id)
    assert snapshot.job_spec_id == job_spec.job_spec_id
    assert snapshot.tenant_id == job_spec.tenant_id
    assert snapshot.input_manifest == job_spec.input_manifest
    assert snapshot.input_manifest.format == "OBJECT_FORMAT_PARQUET"
    assert snapshot.input_manifest.media_type == "application/vnd.apache.parquet"
    assert snapshot.metrics["algorithm_spec_id"] == ALGORITHM_SPEC_ID
    assert snapshot.metrics["algorithm_digest"] == ALGORITHM_DIGEST
    assert commit.labels["input_manifest_digest"] == (
        "sha256:hex:" + snapshot.input_manifest.checksum.value
    )
    assert {table.table_name.rsplit(".", 1)[0] for table in snapshot.tables} == {
        "governance.media_catalog"
    }


def test_copied_control_fixtures_match_sibling_repository(
    fixture_dir: Path,
) -> None:
    upstream = (
        Path(__file__).resolve().parents[2]
        / "video-governance-control-media-catalog"
        / "contracts"
        / "fixtures"
    )
    if not upstream.exists():
        return
    local = fixture_dir / "control"
    for name in (
        "media_catalog_job_spec.v1.json",
        "media_catalog_snapshot_set.v1.json",
        "media_catalog_output_commit.v1.json",
    ):
        assert json.loads((local / name).read_text()) == json.loads(
            (upstream / name).read_text()
        )
