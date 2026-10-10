from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace
from typing import Any

import pytest
from test_temporal_golden_models import _input, _ref, _run

from video_media_catalog.community_snapshot import SILVER_EPOCH_MEDIA_TYPE
from video_media_catalog.temporal.activities import golden as golden_mod
from video_media_catalog.temporal.golden_models import (
    GoldenEpochResult,
    GoldenIdentityResult,
    GoldenPreflightResult,
)
from video_media_catalog.temporal.models import ObjectRefPayload


def _mapping(reference: ObjectRefPayload) -> dict[str, Any]:
    return {
        "uri": reference.uri,
        "format": reference.format,
        "mediaType": reference.media_type,
        "checksum": {
            "algorithm": reference.checksum_algorithm,
            "value": reference.checksum_value,
        },
        "sizeBytes": reference.size_bytes,
        "etag": reference.etag,
        "objectVersion": reference.object_version,
    }


def test_golden_emr_profile_is_single_attempt_and_scaled() -> None:
    input_data = _input()

    parsed = golden_mod._emr_namespace(
        input_data=input_data,
        stage="identity",
        entry_point=input_data.env.research_silver_entry_point,
        entry_args=["resolve-identity"],
        workflow_run_id="temporal-run-1",
        control_profile=False,
    )

    assert parsed.max_attempts == 1
    assert parsed.executor_instances == 8
    assert parsed.shuffle_partitions == 432
    assert parsed.executor_cores == 8
    assert parsed.executor_memory == "48g"
    assert parsed.executor_disk == "400G"
    assert parsed.driver_memory == "16g"
    assert parsed.driver_disk == "50G"
    assert parsed.enable_speculation is False
    assert parsed.execution_timeout_minutes == 900
    assert parsed.client_token == golden_mod._stage_token(
        build_id=input_data.build_id,
        stage="identity",
        workflow_run_id="temporal-run-1",
    )


def test_golden_release_uses_dedicated_capacity_with_replacement_headroom() -> None:
    base = _input()
    input_data = replace(
        base,
        env=replace(
            base.env,
            gold_executor_memory_overhead="6g",
            gold_executor_disk="185G",
        ),
    )

    parsed = golden_mod._emr_namespace(
        input_data=input_data,
        stage="release",
        entry_point=input_data.env.gold_entry_point,
        entry_args=["--silver-snapshot-uri", "s3://bucket/snapshot.json"],
        workflow_run_id="temporal-run-1",
        control_profile=False,
    )

    assert parsed.application_name == "gold-app"
    assert parsed.log_uri == "s3://bucket/gold-logs/"
    assert parsed.executor_instances == 20
    assert parsed.executor_cores == 4
    assert parsed.executor_memory == "24g"
    assert parsed.executor_memory_overhead == "6g"
    assert parsed.executor_disk == "185G"
    assert parsed.shuffle_partitions == 1152
    assert parsed.driver_memory_overhead == "4g"
    assert parsed.enable_speculation is True


def test_golden_release_rejects_capacity_without_replacement_headroom() -> None:
    input_data = _input()
    oversized_env = replace(
        input_data.env,
        gold_executor_instances=23,
        gold_executor_disk="170G",
    )

    with pytest.raises(
        golden_mod.NonRetryableGoldenBuildError,
        match="including 1 replacement executor",
    ):
        golden_mod._emr_namespace(
            input_data=replace(input_data, env=oversized_env),
            stage="release",
            entry_point=input_data.env.gold_entry_point,
            entry_args=["--silver-snapshot-uri", "s3://bucket/snapshot.json"],
            workflow_run_id="temporal-run-1",
            control_profile=False,
        )


def test_golden_release_passes_pinned_temporary_quality_thresholds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = _input()
    input_data = replace(
        base,
        env=replace(
            base.env,
            gold_tmdb_freshness_slo_hours=72,
            gold_max_unresolved_identity_ratio=0.12,
            gold_quality_override_reason="One-time r15 publication authorization.",
        ),
    )
    preflight = GoldenPreflightResult(
        build_spec=_ref("s3://bucket/build-spec.json"),
        planned_at="2026-10-09T00:00:00Z",
        all_snapshot_run_ids=(),
        tmdb_source_run_ids=input_data.tmdb_source_run_ids,
        imdb_age_hours=1.0,
        tvmaze_age_hours=1.0,
        imdb_slo_hours=2,
        tvmaze_slo_hours=2,
        freshness_policy_digest=_run(80),
        identity_config_digest=_run(81),
        identity_resolution_config_digest=_run(82),
        identity_bound_config_digest=_run(83),
    )
    epoch = GoldenEpochResult(
        epoch_id=_run(84),
        epoch=replace(
            _ref("s3://bucket/epoch.json"),
            media_type=SILVER_EPOCH_MEDIA_TYPE,
        ),
        committed_run_count=20,
        committed_run_digest=_run(85),
        data_snapshot_ids={"community_ingest_run": 1},
        job_run_id="epoch-job",
    )
    observed_args: list[str] = []

    def fake_stage(**kwargs: Any) -> tuple[dict[str, Any], str]:
        observed_args.extend(kwargs["entry_args"])
        raise golden_mod.NonRetryableGoldenBuildError("stop after capturing args")

    monkeypatch.setattr(golden_mod, "_run_emr_stage", fake_stage)
    monkeypatch.setattr(
        golden_mod.activity,
        "info",
        lambda: SimpleNamespace(workflow_run_id="temporal-run-1"),
    )
    monkeypatch.setattr(
        golden_mod,
        "classify_golden_build_exception",
        lambda exc: exc,
    )

    with pytest.raises(
        golden_mod.NonRetryableGoldenBuildError,
        match="stop after capturing args",
    ):
        golden_mod.submit_golden_release(
            input_data,
            preflight,
            epoch,
            "2026-10-09T00:00:00Z",
        )

    assert (
        golden_mod._single_argument(
            observed_args,
            "--tmdb-freshness-slo-hours",
        )
        == "72"
    )
    assert (
        golden_mod._single_argument(
            observed_args,
            "--max-unresolved-identity-ratio",
        )
        == "0.12"
    )


def test_golden_emr_stage_resumes_job_from_matching_heartbeat(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_data = _input()
    observed: dict[str, Any] = {}

    def fake_submit(_parsed: Any, **kwargs: Any) -> dict[str, Any]:
        observed.update(kwargs)
        return {
            "applicationId": "00fakerefapp",
            "jobRunId": "00existingjob",
            "state": "SUCCESS",
        }

    monkeypatch.setattr(golden_mod, "run_emr_submit", fake_submit)
    monkeypatch.setattr(golden_mod, "_s3_client", lambda _region: object())
    monkeypatch.setattr(
        golden_mod,
        "read_driver_json_summary",
        lambda **_kwargs: {"snapshot": "complete"},
    )
    monkeypatch.setattr(
        golden_mod.activity,
        "info",
        lambda: SimpleNamespace(
            heartbeat_details=(
                {
                    "phase": "golden-snapshot",
                    "buildId": input_data.build_id,
                    "jobRunId": "00existingjob",
                },
            )
        ),
    )
    monkeypatch.setattr(golden_mod.activity, "heartbeat", lambda _details: None)

    summary, job_run_id = golden_mod._run_emr_stage(
        input_data=input_data,
        stage="snapshot",
        entry_point=input_data.env.research_silver_entry_point,
        entry_args=["publish-snapshot"],
        required_keys=("snapshot",),
        workflow_run_id="temporal-run-1",
        control_profile=True,
    )

    assert observed["resume_job_run_id"] == "00existingjob"
    assert summary == {"snapshot": "complete"}
    assert job_run_id == "00existingjob"


def test_golden_preflight_reuses_verified_identity_job(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_data = _input()
    source_build_id = "tmdb-multisource-20261009-r3"
    source_runtime, source_resolution, source_bound = (
        golden_mod._identity_config_digests(source_build_id)
    )
    preflight = GoldenPreflightResult(
        build_spec=_ref("s3://bucket/build-spec.json"),
        planned_at="2026-10-09T00:00:00Z",
        all_snapshot_run_ids=tuple(
            sorted(
                (
                    *input_data.baseline_source_run_ids,
                    input_data.baseline_identity_run_id,
                    *input_data.tmdb_source_run_ids,
                )
            )
        ),
        tmdb_source_run_ids=tuple(sorted(input_data.tmdb_source_run_ids)),
        imdb_age_hours=1.0,
        tvmaze_age_hours=1.0,
        imdb_slo_hours=2,
        tvmaze_slo_hours=2,
        freshness_policy_digest=_run(90),
        identity_config_digest=_run(91),
        identity_resolution_config_digest=source_resolution,
        identity_bound_config_digest=_run(92),
    )
    image_digest = _run(93)
    arguments = ["resolve-identity"]
    for run_id in preflight.tmdb_source_run_ids:
        arguments.extend(["--source-run-id", run_id])
    arguments.extend(
        [
            "--identity-generation-id",
            input_data.identity_generation_id,
            "--identity-mode",
            "incremental",
            "--image-digest",
            image_digest,
            "--config-digest",
            source_runtime,
        ]
    )
    job_run_id = "00verifiedidentity"
    fake_emr = SimpleNamespace(
        get_job_run=lambda **_kwargs: {
            "jobRun": {
                "applicationId": "00verifiedapplication",
                "jobRunId": job_run_id,
                "name": f"gold-identity-{source_build_id}",
                "state": "SUCCESS",
                "executionRole": input_data.env.emr_execution_role_arn,
                "jobDriver": {
                    "sparkSubmit": {
                        "entryPoint": input_data.env.research_silver_entry_point,
                        "entryPointArguments": arguments,
                    }
                },
            }
        }
    )
    monkeypatch.setattr(golden_mod, "_emr_client", lambda _region: fake_emr)
    payload = {
        "schemaVersion": "1.0",
        "sourceBuildId": source_build_id,
        "applicationId": "00verifiedapplication",
        "jobRunId": job_run_id,
        "imageDigest": image_digest,
        "identitySummary": {
            "context": "research",
            "stage": "identity-resolution",
            "runId": _run(94),
            "commitKey": _run(95),
            "configDigest": source_bound,
            "identityResolutionConfigDigest": source_resolution,
            "registryDigest": golden_mod.build_community_registry().digest,
            "identityGenerationId": input_data.identity_generation_id,
            "identityMode": "incremental",
            "sourceRunIds": list(preflight.tmdb_source_run_ids),
            "tableCounts": {"community_entity_ledger": 1},
            "tableSnapshotIds": {"community_entity_ledger": 7},
        },
    }

    result = golden_mod._validated_reused_identity(
        input_data,
        preflight,
        payload,
    )

    assert result.run_id == _run(94)
    assert result.job_run_id == job_run_id
    assert result.source_run_ids == preflight.tmdb_source_run_ids


def test_epoch_requires_exact_pinned_runs_plus_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_data = _input()
    preflight = GoldenPreflightResult(
        build_spec=_ref("s3://bucket/build-spec.json"),
        planned_at="2026-10-09T00:00:00Z",
        all_snapshot_run_ids=tuple(
            sorted(
                (
                    *input_data.baseline_source_run_ids,
                    input_data.baseline_identity_run_id,
                    *input_data.tmdb_source_run_ids,
                )
            )
        ),
        tmdb_source_run_ids=tuple(sorted(input_data.tmdb_source_run_ids)),
        imdb_age_hours=433.95,
        tvmaze_age_hours=317.24,
        imdb_slo_hours=434,
        tvmaze_slo_hours=318,
        freshness_policy_digest="sha256:" + ("f" * 64),
        identity_config_digest="sha256:" + ("e" * 64),
        identity_resolution_config_digest="sha256:" + ("d" * 64),
        identity_bound_config_digest="sha256:" + ("c" * 64),
    )
    identity = GoldenIdentityResult(
        run_id=_run(100),
        commit_key=_run(101),
        source_run_ids=preflight.tmdb_source_run_ids,
        table_counts={"community_entity_ledger": 1},
        table_snapshot_ids={"community_entity_ledger": 7},
        job_run_id="identity-job",
    )
    expected = tuple(sorted((*preflight.all_snapshot_run_ids, identity.run_id)))
    epoch_ref = ObjectRefPayload(
        uri="s3://bucket/epoch.json",
        format="OBJECT_FORMAT_JSON",
        media_type=SILVER_EPOCH_MEDIA_TYPE,
        checksum_algorithm="CHECKSUM_ALGORITHM_SHA256",
        checksum_value="b" * 64,
        size_bytes=200,
        etag="epoch-etag",
        object_version="epoch-v1",
    )
    observed_args: list[str] = []

    def fake_stage(**kwargs: Any) -> tuple[dict[str, Any], str]:
        observed_args.extend(kwargs["entry_args"])
        return (
            {
                "epochId": _run(102),
                "identityGenerationId": input_data.identity_generation_id,
                "silverEpoch": _mapping(epoch_ref),
                "deltaRunIds": expected,
                "committedRunCount": len(expected),
                "committedRunDigest": _run(103),
                "dataSnapshotIds": {"community_ingest_run": 99},
            },
            "epoch-job",
        )

    monkeypatch.setattr(golden_mod, "_run_emr_stage", fake_stage)
    monkeypatch.setattr(
        golden_mod.activity,
        "info",
        lambda: SimpleNamespace(workflow_run_id="temporal-run-1"),
    )

    result = golden_mod.publish_golden_epoch(
        input_data,
        preflight,
        identity,
        "2026-10-09T00:00:00Z",
    )

    delta_values = [
        observed_args[index + 1]
        for index, value in enumerate(observed_args)
        if value == "--delta-run-id"
    ]
    assert tuple(delta_values) == expected
    assert result.committed_run_count == 20
