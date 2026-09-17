from __future__ import annotations

import uuid

import pytest
from pydantic import ValidationError

from video_media_catalog.cli import build_parser as build_extract_parser
from video_media_catalog.identity import is_canonical_uuid7, stable_uuid7
from video_media_catalog.runtime_args import RuntimeArguments
from video_media_catalog.spark_cli import build_parser as build_spark_parser
from video_media_catalog.spark_cli import run as run_spark

RUN_ID = "01a081e8-6420-7000-8000-000000000202"
JOB_SPEC_ID = "01a081e8-6420-7000-8000-000000000203"
TENANT_ID = "01a081e8-6420-7000-8000-000000000204"


def _standard_arguments() -> list[str]:
    return [
        "--manifest-uri",
        "s3://bucket/source-manifest.parquet",
        "--manifest-hash",
        "sha256:hex:" + "a" * 64,
        "--manifest-version",
        "version-1",
        "--manifest-etag",
        "etag-1",
        "--manifest-size",
        "4096",
        "--run-id",
        RUN_ID,
        "--job-spec-id",
        JOB_SPEC_ID,
        "--tenant-id",
        TENANT_ID,
        "--attempt",
        "1",
        "--output-prefix",
        "s3://bucket/runs/" + RUN_ID,
        "--executor-image",
        "registry.example/catalog@sha256:" + "c" * 64,
    ]


def test_extract_runtime_parser_accepts_argo_arguments_without_subcommand() -> None:
    parsed = build_extract_parser().parse_args(_standard_arguments())
    runtime = RuntimeArguments.model_validate(
        {field: getattr(parsed, field) for field in RuntimeArguments.model_fields}
    )
    assert runtime.input_manifest.format == "OBJECT_FORMAT_PARQUET"
    assert runtime.image_digest == "sha256:" + "c" * 64
    assert runtime.stage_prefix("media-catalog-extract") == (
        f"s3://bucket/runs/{RUN_ID}/attempt=1/stage=media-catalog-extract"
    )


def test_spark_parser_accepts_standard_arguments_and_stage() -> None:
    parsed = build_spark_parser().parse_args(
        [*_standard_arguments(), "--stage", "media-catalog-commit"]
    )
    assert parsed.stage == "media-catalog-commit"
    assert parsed.landing_manifest_uri is None


def test_spark_parser_reads_gitops_catalog_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MEDIA_CATALOG_CATALOG_TYPE", "glue")
    monkeypatch.setenv("MEDIA_CATALOG_CATALOG_NAME", "media")
    monkeypatch.setenv("MEDIA_CATALOG_NAMESPACE", "video_media_catalog")
    monkeypatch.setenv(
        "MEDIA_CATALOG_WAREHOUSE_URI",
        "s3://media-catalog/warehouse/",
    )

    parsed = build_spark_parser().parse_args(
        [*_standard_arguments(), "--stage", "media-catalog-commit"]
    )

    assert parsed.catalog_type == "glue"
    assert parsed.catalog_name == "media"
    assert parsed.namespace == "video_media_catalog"
    assert parsed.warehouse == "s3://media-catalog/warehouse/"


def test_spark_runtime_rejects_wrong_stage_before_starting_spark() -> None:
    parsed = build_spark_parser().parse_args(
        [
            *_standard_arguments(),
            "--stage",
            "wrong-stage",
            "--warehouse",
            "file:///tmp/warehouse",
        ]
    )
    with pytest.raises(ValueError, match="media-catalog-commit"):
        run_spark(parsed)


@pytest.mark.parametrize("field", ["run_id", "job_spec_id", "tenant_id"])
def test_runtime_rejects_non_uuid7_identities(field: str) -> None:
    values = {
        "manifest_uri": "s3://bucket/source-manifest.parquet",
        "manifest_hash": "sha256:" + "a" * 64,
        "manifest_version": "v1",
        "manifest_etag": "etag",
        "manifest_size": 1,
        "run_id": RUN_ID,
        "job_spec_id": JOB_SPEC_ID,
        "tenant_id": TENANT_ID,
        "attempt": 1,
        "output_prefix": "s3://bucket/run",
        "executor_image": "registry.example/catalog@sha256:" + "c" * 64,
    }
    values[field] = "not-a-uuid"
    with pytest.raises(ValidationError, match="UUIDv7"):
        RuntimeArguments.model_validate(values)


def test_stable_uuid7_uses_run_timestamp_and_identity() -> None:
    first = stable_uuid7(kind="snapshot", run_id=RUN_ID, identity={"x": 1})
    second = stable_uuid7(kind="snapshot", run_id=RUN_ID, identity={"x": 1})
    different = stable_uuid7(kind="snapshot", run_id=RUN_ID, identity={"x": 2})
    assert first == second
    assert first != different
    assert is_canonical_uuid7(first)
    assert uuid.UUID(first).int >> 80 == uuid.UUID(RUN_ID).int >> 80
