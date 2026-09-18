from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from video_media_catalog.canonical import sha256_digest
from video_media_catalog.commit import LocalControlPublisher
from video_media_catalog.constants import CURATED_TABLE_KEYS
from video_media_catalog.landing import build_landing_manifest
from video_media_catalog.models import LandingSummary
from video_media_catalog.quality import (
    QualityConfig,
    QualityMetrics,
    build_quality_report,
    publish_quality_report,
    quality_violations,
    read_quality_gate,
)
from video_media_catalog.runtime_args import RuntimeArguments
from video_media_catalog.spark_input import LandingInput
from video_media_catalog.validate_cli import main as validate_main

RUN_ID = "01a081e8-6420-7000-8000-000000000202"
JOB_SPEC_ID = "01a081e8-6420-7000-8000-000000000203"
TENANT_ID = "01a081e8-6420-7000-8000-000000000204"


def _runtime(tmp_path: Path, *, tenant_id: str = TENANT_ID) -> RuntimeArguments:
    return RuntimeArguments(
        manifest_uri="s3://input/source-manifest.parquet",
        manifest_hash="sha256:hex:" + "a" * 64,
        manifest_version="version-1",
        manifest_etag="etag-1",
        manifest_size=4096,
        run_id=RUN_ID,
        job_spec_id=JOB_SPEC_ID,
        tenant_id=tenant_id,
        attempt=1,
        output_prefix=(tmp_path / "run").as_uri(),
        executor_image="registry.example/catalog@sha256:" + "c" * 64,
    )


def _landing() -> LandingInput:
    manifest = build_landing_manifest(
        sources=[],
        shards=[],
        source_counts={},
        input_manifest_digest="sha256:hex:" + "a" * 64,
    )
    manifest_uri = "file:///tmp/landing-manifest.json"
    payload = manifest.json_bytes()
    summary = LandingSummary(
        manifest_id=manifest.manifest_id,
        manifest_uri=manifest_uri,
        manifest_checksum=sha256_digest(payload),
        record_count=0,
        shard_count=0,
    )
    return LandingInput(
        manifest_uri=manifest_uri,
        manifest_digest=sha256_digest(payload),
        manifest_size=len(payload),
        manifest=manifest,
        summary=summary,
    )


def _metrics(entity_count: int) -> QualityMetrics:
    counts = {table: 1 for table in CURATED_TABLE_KEYS}
    counts["catalog_entity"] = entity_count
    counts["catalog_name"] = entity_count
    counts["catalog_ingest_error"] = 0
    zeros = {table: 0 for table in CURATED_TABLE_KEYS}
    return QualityMetrics(
        table_counts=counts,
        null_primary_keys=zeros,
        duplicate_primary_keys=zeros,
        dangling_relation_subjects=0,
        dangling_relation_objects=0,
        ingest_error_count=0,
        named_entity_count=entity_count,
        name_coverage=1.0,
    )


@pytest.mark.parametrize("entity_count", [95, 100, 105])
def test_five_percent_entity_count_boundaries_pass(entity_count: int) -> None:
    config = QualityConfig(
        expected_entity_count=100,
        entity_count_tolerance_percent=5,
    )
    assert config.minimum_entity_count == 95
    assert config.maximum_entity_count == 105
    assert quality_violations(_metrics(entity_count), config) == []


@pytest.mark.parametrize("entity_count", [94, 106])
def test_entity_count_outside_boundary_fails(entity_count: int) -> None:
    violations = quality_violations(
        _metrics(entity_count),
        QualityConfig(
            expected_entity_count=100,
            entity_count_tolerance_percent=5,
        ),
    )
    assert any(value.startswith("ENTITY_COUNT_OUT_OF_RANGE") for value in violations)


def test_failed_report_is_published_before_commit_last_summary(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    landing = _landing()
    config = QualityConfig(expected_entity_count=100)
    report = build_quality_report(
        runtime=runtime,
        landing_input=landing,
        config=config,
        metrics=_metrics(1),
        started_at="2026-09-18T06:00:00Z",
        completed_at="2026-09-18T06:00:01Z",
    )
    publisher = LocalControlPublisher(tmp_path / "quality")

    publication = publish_quality_report(
        publisher=publisher,
        runtime=runtime,
        landing_input=landing,
        report=report,
    )

    assert publication.report.status == "FAILED"
    assert (tmp_path / "quality" / "quality-report.json").exists()
    assert (tmp_path / "quality" / "quality-summary.json").exists()
    report_json = json.loads(
        (tmp_path / "quality" / "quality-report.json").read_bytes()
    )
    assert report_json["attempt"] == "1"
    assert report_json["expectedEntityCount"] == "100"
    assert report_json["tableCounts"]["catalog_entity"] == "1"
    summary_json = json.loads(
        (tmp_path / "quality" / "quality-summary.json").read_bytes()
    )
    assert summary_json["status"] == "FAILED"
    assert summary_json["report"]["checksum"]["value"] == sha256_digest(
        (tmp_path / "quality" / "quality-report.json").read_bytes()
    ).removeprefix("sha256:")
    with pytest.raises(ValueError, match="did not pass"):
        read_quality_gate(
            publisher=publisher,
            runtime=runtime,
            landing_input=landing,
        )


def test_summary_report_binding_tamper_is_rejected(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    landing = _landing()
    report = build_quality_report(
        runtime=runtime,
        landing_input=landing,
        config=QualityConfig(),
        metrics=_metrics(1),
        started_at="2026-09-18T06:00:00Z",
        completed_at="2026-09-18T06:00:01Z",
    )
    publisher = LocalControlPublisher(tmp_path / "quality")
    publish_quality_report(
        publisher=publisher,
        runtime=runtime,
        landing_input=landing,
        report=report,
    )
    report_path = tmp_path / "quality" / "quality-report.json"
    report_path.write_bytes(report_path.read_bytes() + b" ")

    with pytest.raises(ValueError, match="ObjectRef"):
        read_quality_gate(
            publisher=publisher,
            runtime=runtime,
            landing_input=landing,
        )


def test_wrong_runtime_identity_is_rejected(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    other = _runtime(
        tmp_path,
        tenant_id="01a081e8-6420-7000-8000-000000000205",
    )
    landing = _landing()
    report = build_quality_report(
        runtime=other,
        landing_input=landing,
        config=QualityConfig(),
        metrics=_metrics(1),
        started_at="2026-09-18T06:00:00Z",
        completed_at="2026-09-18T06:00:01Z",
    )
    publisher = LocalControlPublisher(tmp_path / "quality")
    publish_quality_report(
        publisher=publisher,
        runtime=other,
        landing_input=landing,
        report=report,
    )

    with pytest.raises(ValueError, match="input identity"):
        read_quality_gate(
            publisher=publisher,
            runtime=runtime,
            landing_input=landing,
        )


def test_validate_cli_returns_nonzero_for_published_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report = build_quality_report(
        runtime=_runtime(tmp_path),
        landing_input=_landing(),
        config=QualityConfig(expected_entity_count=100),
        metrics=_metrics(1),
        started_at="2026-09-18T06:00:00Z",
        completed_at="2026-09-18T06:00:01Z",
    )
    monkeypatch.setattr(
        "video_media_catalog.validate_cli.run",
        lambda _parsed: report,
    )
    arguments = [
        "--manifest-uri",
        "s3://bucket/manifest",
        "--manifest-hash",
        "a" * 64,
        "--manifest-version",
        "v1",
        "--manifest-etag",
        "e1",
        "--manifest-size",
        "1",
        "--run-id",
        RUN_ID,
        "--job-spec-id",
        JOB_SPEC_ID,
        "--tenant-id",
        TENANT_ID,
        "--attempt",
        "1",
        "--output-prefix",
        (tmp_path / "run").as_uri(),
        "--executor-image",
        "registry/image@sha256:" + "c" * 64,
        "--stage",
        "media-catalog-validate",
    ]
    assert validate_main(arguments) == 2


def test_quality_report_rejects_unknown_fields(tmp_path: Path) -> None:
    report = build_quality_report(
        runtime=_runtime(tmp_path),
        landing_input=_landing(),
        config=QualityConfig(),
        metrics=_metrics(1),
        started_at="2026-09-18T06:00:00Z",
        completed_at="2026-09-18T06:00:01Z",
    )
    with pytest.raises(ValidationError, match="Extra inputs"):
        report.__class__.model_validate({**report.model_dump(), "unexpected": True})
