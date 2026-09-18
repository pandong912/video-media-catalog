from __future__ import annotations

from pathlib import Path

import pytest

from video_media_catalog.commit import LocalControlPublisher
from video_media_catalog.constants import CURATED_TABLE_KEYS
from video_media_catalog.landing import extract_landing
from video_media_catalog.quality import (
    VALIDATE_STAGE,
    QualityConfig,
    QualityMetrics,
    build_quality_report,
    publish_quality_report,
)
from video_media_catalog.spark_cli import build_parser, run
from video_media_catalog.spark_input import load_landing_input, runtime_arguments
from video_media_catalog.storage import local_path


def _arguments(tmp_path: Path, fixture_dir: Path):
    landing_root = tmp_path / "landing"
    extract_landing(
        output_uri=str(landing_root),
        wikidata_uri=str(fixture_dir / "wikidata.json"),
        eidr_xml_uri=str(fixture_dir / "eidr.xml"),
        shard_records=5,
    )
    return build_parser().parse_args(
        [
            "--landing-manifest-uri",
            (landing_root / "landing-manifest.json").as_uri(),
            "--catalog-type",
            "hadoop",
            "--catalog-name",
            "quality_gate_test",
            "--namespace",
            "catalog_v1",
            "--warehouse",
            (tmp_path / "warehouse").as_uri(),
            "--manifest-uri",
            "s3://catalog-input/source-manifest.parquet",
            "--manifest-hash",
            "sha256:hex:" + "a" * 64,
            "--manifest-version",
            "manifest-v1",
            "--manifest-etag",
            "manifest-etag",
            "--manifest-size",
            "4096",
            "--run-id",
            "01a081e8-6420-7000-8000-000000000202",
            "--job-spec-id",
            "01a081e8-6420-7000-8000-000000000203",
            "--tenant-id",
            "01a081e8-6420-7000-8000-000000000204",
            "--attempt",
            "1",
            "--output-prefix",
            (tmp_path / "run").as_uri(),
            "--executor-image",
            "registry.example/catalog@sha256:" + "c" * 64,
            "--stage",
            "media-catalog-commit",
        ]
    )


def _metrics(*, failed: bool) -> QualityMetrics:
    counts = {table: 1 for table in CURATED_TABLE_KEYS}
    counts["catalog_entity"] = 1
    counts["catalog_name"] = 1
    counts["catalog_ingest_error"] = 1 if failed else 0
    zeros = {table: 0 for table in CURATED_TABLE_KEYS}
    return QualityMetrics(
        table_counts=counts,
        null_primary_keys=zeros,
        duplicate_primary_keys=zeros,
        dangling_relation_subjects=0,
        dangling_relation_objects=0,
        ingest_error_count=counts["catalog_ingest_error"],
        named_entity_count=1,
        name_coverage=1,
    )


@pytest.mark.parametrize(
    ("scenario", "message"),
    [
        ("missing", "summary is missing"),
        ("failed", "did not pass"),
        ("wrong_identity", "input identity"),
        ("wrong_config", "expected quality config"),
    ],
)
def test_commit_quality_failures_happen_before_any_iceberg_call(
    tmp_path: Path,
    fixture_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    scenario: str,
    message: str,
) -> None:
    parsed = _arguments(tmp_path, fixture_dir)
    runtime = runtime_arguments(parsed)
    landing = load_landing_input(
        runtime,
        landing_manifest_uri=parsed.landing_manifest_uri,
        aws_region=None,
        s3_endpoint=None,
        s3_path_style_access=False,
        max_landing_shard_bytes=parsed.max_landing_shard_bytes,
    )
    publisher = LocalControlPublisher(local_path(runtime.stage_prefix(VALIDATE_STAGE)))
    if scenario != "missing":
        report_runtime = (
            runtime.model_copy(
                update={"tenant_id": "01a081e8-6420-7000-8000-000000000205"}
            )
            if scenario == "wrong_identity"
            else runtime
        )
        report = build_quality_report(
            runtime=report_runtime,
            landing_input=landing,
            config=QualityConfig(
                max_closure_iterations=(8 if scenario == "wrong_config" else 64)
            ),
            metrics=_metrics(failed=scenario == "failed"),
            started_at="2026-09-18T06:00:00Z",
            completed_at="2026-09-18T06:00:01Z",
        )
        publish_quality_report(
            publisher=publisher,
            runtime=report_runtime,
            landing_input=landing,
            report=report,
        )

    iceberg_called = False

    class ForbiddenIceberg:
        def __init__(self, *_args, **_kwargs) -> None:
            nonlocal iceberg_called
            iceberg_called = True

    monkeypatch.setattr(
        "video_media_catalog.spark_cli.MediaCatalogTables",
        ForbiddenIceberg,
    )
    with pytest.raises(ValueError, match=message):
        run(parsed)
    assert not iceberg_called


def test_commit_quality_gate_can_only_be_disabled_explicitly() -> None:
    parser = build_parser()
    assert parser.get_default("quality_report_required") is True
    parsed = parser.parse_args(
        [
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
            "01a081e8-6420-7000-8000-000000000202",
            "--job-spec-id",
            "01a081e8-6420-7000-8000-000000000203",
            "--tenant-id",
            "01a081e8-6420-7000-8000-000000000204",
            "--attempt",
            "1",
            "--output-prefix",
            "file:///tmp/run",
            "--executor-image",
            "registry/image@sha256:" + "c" * 64,
            "--stage",
            "media-catalog-commit",
            "--warehouse",
            "file:///tmp/warehouse",
            "--no-quality-report-required",
        ]
    )
    assert parsed.quality_report_required is False
