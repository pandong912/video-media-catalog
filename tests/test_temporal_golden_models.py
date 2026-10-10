from __future__ import annotations

from dataclasses import asdict, replace

import pytest

from video_media_catalog.temporal.activities.golden import (
    _validate_preflight_documents,
)
from video_media_catalog.temporal.errors import NonRetryableGoldenBuildError
from video_media_catalog.temporal.golden_models import (
    FreshnessOverrideInput,
    GoldenBuildEnv,
    GoldenBuildInput,
)
from video_media_catalog.temporal.models import ObjectRefPayload


def _ref(uri: str) -> ObjectRefPayload:
    return ObjectRefPayload(
        uri=uri,
        format="OBJECT_FORMAT_JSON",
        media_type="application/json",
        checksum_algorithm="CHECKSUM_ALGORITHM_SHA256",
        checksum_value="a" * 64,
        size_bytes=100,
        etag="etag",
        object_version="v1",
    )


def _run(index: int) -> str:
    return "sha256:" + f"{index:064x}"


def _input() -> GoldenBuildInput:
    return GoldenBuildInput(
        build_id="tmdb-multisource-20261009",
        identity_generation_id="pure-source-2026-09-r1",
        baseline_snapshot=_ref("s3://bucket/baseline.json"),
        tmdb_pipeline_summary=_ref("s3://bucket/tmdb-summary.json"),
        baseline_source_run_ids=(_run(1), _run(2), _run(3)),
        baseline_identity_run_id=_run(4),
        tmdb_source_run_ids=tuple(_run(index) for index in range(10, 25)),
        source_watermarks={
            "imdb-non-commercial-datasets": "2026-09-20T22:03:01Z",
            "tmdb-research": "2026-10-07",
            "tvmaze-public-api": "2026-09-25T18:45:56Z",
            "wikidata-json-dump": "20260914",
        },
        freshness_override=FreshnessOverrideInput(
            reason="One-time release over already verified source baselines.",
            imdb_acquired_at="2026-09-20T22:03:01Z",
            tvmaze_acquired_at="2026-09-25T18:45:56Z",
            max_slo_hours=720,
        ),
        env=GoldenBuildEnv(
            aws_region="us-east-1",
            warehouse_uri="s3://bucket/warehouse",
            control_prefix="s3://bucket/warehouse/research/control",
            gold_output_prefix="s3://bucket/warehouse/research/gold",
            pipeline_summary_prefix="s3://bucket/landing/summaries/golden",
            emr_application_name="app",
            emr_execution_role_arn="arn:aws:iam::123456789012:role/emr",
            emr_log_uri="s3://bucket/logs/",
            research_silver_entry_point="local:///opt/research_silver_cli.py",
            gold_entry_point="local:///opt/gold_cli.py",
            image_digest="sha256:" + ("f" * 64),
            gold_emr_application_name="gold-app",
            gold_emr_log_uri="s3://bucket/gold-logs/",
        ),
    )


def _documents(
    input_data: GoldenBuildInput,
) -> tuple[dict[str, object], dict[str, object]]:
    baseline_runs = [
        *input_data.baseline_source_run_ids,
        input_data.baseline_identity_run_id,
    ]
    baseline = {
        "schemaVersion": "2.0",
        "identityGenerationId": input_data.identity_generation_id,
        "committedRunIds": baseline_runs,
        "tableMapping": {"community_ingest_run": "community_ingest_run"},
    }
    tmdb = {
        "mode": "bootstrap",
        "captureCount": 15,
        "captures": [{} for _ in range(15)],
        "sourceSilver": [
            {"runId": run_id} for run_id in input_data.tmdb_source_run_ids
        ],
    }
    return baseline, tmdb


def test_preflight_pins_nineteen_runs_and_minimum_freshness_slos() -> None:
    input_data = _input()
    baseline, tmdb = _documents(input_data)

    result = _validate_preflight_documents(
        input_data,
        baseline=baseline,
        tmdb_summary=tmdb,
        planned_at="2026-10-09T00:00:00Z",
    )

    assert len(result.all_snapshot_run_ids) == 19
    assert result.imdb_slo_hours == 434
    assert result.tvmaze_slo_hours == 318
    assert result.imdb_age_hours <= result.imdb_slo_hours
    assert result.tvmaze_age_hours <= result.tvmaze_slo_hours


def test_preflight_rejects_a_different_tmdb_run_set() -> None:
    input_data = _input()
    baseline, tmdb = _documents(input_data)
    tmdb["sourceSilver"] = [
        *tmdb["sourceSilver"][:-1],
        {"runId": _run(99)},
    ]

    with pytest.raises(
        NonRetryableGoldenBuildError,
        match="does not match",
    ):
        _validate_preflight_documents(
            input_data,
            baseline=baseline,
            tmdb_summary=tmdb,
            planned_at="2026-10-09T00:00:00Z",
        )


def test_preflight_uses_next_hour_when_source_age_is_exact() -> None:
    input_data = _input()
    baseline, tmdb = _documents(input_data)
    override = input_data.freshness_override
    input_data = replace(
        input_data,
        freshness_override=FreshnessOverrideInput(
            reason=override.reason,
            imdb_acquired_at="2026-10-08T23:00:00Z",
            tvmaze_acquired_at="2026-10-08T22:00:00Z",
            max_slo_hours=720,
        ),
    )

    result = _validate_preflight_documents(
        input_data,
        baseline=baseline,
        tmdb_summary=tmdb,
        planned_at="2026-10-09T00:00:00Z",
    )

    assert result.imdb_slo_hours == 2
    assert result.tvmaze_slo_hours == 3


def test_golden_input_is_temporal_serializable() -> None:
    payload = asdict(_input())
    assert payload["identity_generation_id"] == "pure-source-2026-09-r1"
    assert len(payload["tmdb_source_run_ids"]) == 15
