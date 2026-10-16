from __future__ import annotations

import hashlib
from dataclasses import replace
from types import SimpleNamespace
from typing import Any

import pytest
from test_temporal_golden_models import _input, _ref, _run

from video_media_catalog.community_snapshot import (
    SILVER_EPOCH_MEDIA_TYPE,
    SILVER_SNAPSHOT_MEDIA_TYPE,
)
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


def _fixture_incremental_identity_runs(input_data: Any) -> tuple[str, ...]:
    snapshot_source_runs = {
        *input_data.baseline_source_run_ids,
        *input_data.tmdb_source_run_ids,
    }
    return tuple(
        sorted(snapshot_source_runs - set(input_data.baseline_identity_source_run_ids))
    )


def _reuse_snapshot_ref() -> ObjectRefPayload:
    return replace(
        _ref("s3://bucket/reused-pre-identity-snapshot.json"),
        media_type=golden_mod.SILVER_SNAPSHOT_MEDIA_TYPE,
    )


def _reuse_snapshot_payload(
    input_data: Any,
    preflight: GoldenPreflightResult,
) -> dict[str, Any]:
    return {
        "schemaVersion": "2.0",
        "identityGenerationId": input_data.identity_generation_id,
        "committedRunIds": list(preflight.all_snapshot_run_ids),
        "tableMapping": {"community_ingest_run": "community_ingest_run"},
        "dataSnapshotIds": {"community_ingest_run": 7},
    }


def _reuse_identity_arguments(reference: ObjectRefPayload) -> list[str]:
    return [
        "resolve-identity",
        *golden_mod._object_args("silver-snapshot", reference),
        "--silver-snapshot-media-type",
        golden_mod.SILVER_SNAPSHOT_MEDIA_TYPE,
    ]


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
    assert parsed.executor_instances == 7
    assert parsed.shuffle_partitions == 432
    assert parsed.executor_cores == 8
    assert parsed.executor_memory == "36g"
    assert parsed.executor_memory_overhead == "20g"
    assert parsed.executor_disk == "400G"
    assert parsed.driver_memory == "16g"
    assert parsed.driver_memory_overhead == "4g"
    assert parsed.driver_disk == "50G"
    assert parsed.enable_speculation is False
    assert parsed.failure_policy == "fail-fast"
    assert parsed.execution_timeout_minutes == 900
    assert parsed.client_token == golden_mod._stage_token(
        build_id=input_data.build_id,
        stage="identity",
        workflow_run_id="temporal-run-1",
    )


def test_golden_identity_rejects_insufficient_python_memory_overhead() -> None:
    input_data = _input()

    with pytest.raises(
        golden_mod.NonRetryableGoldenBuildError,
        match=r"memory overhead must provide at least 2\.5 GiB per core",
    ):
        golden_mod._emr_namespace(
            input_data=replace(
                input_data,
                env=replace(
                    input_data.env,
                    build_executor_memory_overhead="8g",
                ),
            ),
            stage="identity",
            entry_point=input_data.env.research_silver_entry_point,
            entry_args=["resolve-identity"],
            workflow_run_id="temporal-run-1",
            control_profile=False,
        )


def test_golden_control_profile_uses_r8l_snapshot_memory_shape() -> None:
    """Snapshot/epoch use 48g heap and omit overhead (r8l-proven shape)."""

    input_data = _input()
    parsed = golden_mod._emr_namespace(
        input_data=input_data,
        stage="snapshot",
        entry_point=input_data.env.research_silver_entry_point,
        entry_args=["publish-snapshot"],
        workflow_run_id="temporal-run-1",
        control_profile=True,
    )
    assert parsed.executor_instances == 4
    assert parsed.executor_memory == "48g"
    assert parsed.executor_memory_overhead is None
    assert parsed.driver_memory_overhead is None
    assert parsed.failure_policy == "resilient"
    assert parsed.executor_disk == "200G"


def test_golden_identity_rejects_emr_serverless_worker_memory_overflow() -> None:
    """8 vCPU workers max at 60 GiB; 48g+20g must fail before EMR submit."""

    input_data = _input()

    with pytest.raises(
        golden_mod.NonRetryableGoldenBuildError,
        match=r"Identity executor total memory.*got 68 GiB",
    ):
        golden_mod._emr_namespace(
            input_data=replace(
                input_data,
                env=replace(
                    input_data.env,
                    build_executor_memory="48g",
                    build_executor_memory_overhead="20g",
                ),
            ),
            stage="identity",
            entry_point=input_data.env.research_silver_entry_point,
            entry_args=["resolve-identity"],
            workflow_run_id="temporal-run-1",
            control_profile=False,
        )


def test_golden_identity_rejects_application_capacity_overflow() -> None:
    input_data = _input()

    with pytest.raises(
        golden_mod.NonRetryableGoldenBuildError,
        match=r"Identity EMR request exceeds.*vCPU 68>64",
    ):
        golden_mod._emr_namespace(
            input_data=replace(
                input_data,
                env=replace(
                    input_data.env,
                    build_executor_instances=8,
                ),
            ),
            stage="identity",
            entry_point=input_data.env.research_silver_entry_point,
            entry_args=["resolve-identity"],
            workflow_run_id="temporal-run-1",
            control_profile=False,
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
    assert parsed.enable_speculation is False
    assert parsed.failure_policy == "fail-fast"
    assert parsed.execution_timeout_minutes == 720


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
    source_policy_digest = golden_mod.load_catalog_source_policy().digest
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
        source_policy_digest=source_policy_digest,
    )
    identity = GoldenIdentityResult(
        run_id=_run(86),
        commit_key=_run(87),
        source_run_ids=input_data.tmdb_source_run_ids,
        table_counts={"community_entity_ledger": 1},
        table_snapshot_ids={"community_entity_ledger": 1},
        job_run_id="identity-job",
        source_policy_digest=source_policy_digest,
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
            identity,
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


def test_golden_release_refuses_mismatched_identity_source_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_policy_digest = golden_mod.load_catalog_source_policy().digest
    input_data = _input()
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
        source_policy_digest=source_policy_digest,
    )
    identity = GoldenIdentityResult(
        run_id=_run(86),
        commit_key=_run(87),
        source_run_ids=input_data.tmdb_source_run_ids,
        table_counts={"community_entity_ledger": 1},
        table_snapshot_ids={"community_entity_ledger": 1},
        job_run_id="identity-job",
        source_policy_digest="sha256:" + ("a" * 64),
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
    called = {"emr": False}

    def fake_stage(**_kwargs: Any) -> tuple[dict[str, Any], str]:
        called["emr"] = True
        raise AssertionError("Gold EMR must not start")

    monkeypatch.setattr(golden_mod, "_run_emr_stage", fake_stage)
    monkeypatch.setattr(
        golden_mod,
        "classify_golden_build_exception",
        lambda exc: exc,
    )

    with pytest.raises(
        golden_mod.NonRetryableGoldenBuildError,
        match="refusing to start Gold EMR",
    ):
        golden_mod.submit_golden_release(
            input_data,
            preflight,
            identity,
            epoch,
            "2026-10-09T00:00:00Z",
        )

    assert called["emr"] is False


def test_golden_release_refuses_mismatched_catalog_source_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_data = _input()
    stale_policy = "sha256:" + ("b" * 64)
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
        source_policy_digest=stale_policy,
    )
    identity = GoldenIdentityResult(
        run_id=_run(86),
        commit_key=_run(87),
        source_run_ids=input_data.tmdb_source_run_ids,
        table_counts={"community_entity_ledger": 1},
        table_snapshot_ids={"community_entity_ledger": 1},
        job_run_id="identity-job",
        source_policy_digest=stale_policy,
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
    called = {"emr": False}

    def fake_stage(**_kwargs: Any) -> tuple[dict[str, Any], str]:
        called["emr"] = True
        raise AssertionError("Gold EMR must not start")

    monkeypatch.setattr(golden_mod, "_run_emr_stage", fake_stage)
    monkeypatch.setattr(
        golden_mod,
        "classify_golden_build_exception",
        lambda exc: exc,
    )

    with pytest.raises(
        golden_mod.NonRetryableGoldenBuildError,
        match="refusing to start Gold EMR",
    ):
        golden_mod.submit_golden_release(
            input_data,
            preflight,
            identity,
            epoch,
            "2026-10-09T00:00:00Z",
        )

    assert called["emr"] is False


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


@pytest.mark.parametrize(
    ("variant", "expected_error"),
    [
        ("valid", None),
        ("receipt-object-mismatch", "EMR Silver ObjectRef differs from the receipt"),
        ("job-object-mismatch", "EMR Silver ObjectRef differs from the receipt"),
        ("snapshot-content-mismatch", "snapshot differs from the planned snapshot"),
    ],
)
def test_golden_preflight_reuses_verified_identity_job(
    monkeypatch: pytest.MonkeyPatch,
    variant: str,
    expected_error: str | None,
) -> None:
    input_data = _input()
    identity_source_run_ids = _fixture_incremental_identity_runs(input_data)
    source_build_id = "tmdb-multisource-20261009-r3"
    source_policy_digest = golden_mod.load_catalog_source_policy().digest
    source_runtime, source_resolution, source_bound = (
        golden_mod._identity_config_digests(
            source_build_id,
            source_policy_digest=source_policy_digest,
        )
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
        source_policy_digest=source_policy_digest,
    )
    image_digest = _run(93)
    snapshot_ref = _reuse_snapshot_ref()
    arguments = _reuse_identity_arguments(snapshot_ref)
    for run_id in identity_source_run_ids:
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
            "--source-policy-digest",
            source_policy_digest,
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
    monkeypatch.setattr(
        golden_mod,
        "read_driver_json_summary",
        lambda **_kwargs: {
            "runId": _run(94),
            "commitKey": _run(95),
            "sourceRunIds": list(identity_source_run_ids),
            "identityGenerationId": input_data.identity_generation_id,
            "identityMode": "incremental",
            "tableCounts": {"community_entity_ledger": 1},
            "tableSnapshotIds": {"community_entity_ledger": 7},
        },
    )
    payload = {
        "schemaVersion": "1.0",
        "sourceBuildId": source_build_id,
        "applicationId": "00verifiedapplication",
        "jobRunId": job_run_id,
        "imageDigest": image_digest,
        "silverSnapshot": _mapping(snapshot_ref),
        "identitySummary": {
            "context": "research",
            "stage": "identity-resolution",
            "runId": _run(94),
            "commitKey": _run(95),
            "configDigest": source_bound,
            "identityResolutionConfigDigest": source_resolution,
            "registryDigest": golden_mod.build_community_registry().digest,
            "sourcePolicyDigest": source_policy_digest,
            "identityGenerationId": input_data.identity_generation_id,
            "identityMode": "incremental",
            "sourceRunIds": list(identity_source_run_ids),
            "tableCounts": {"community_entity_ledger": 1},
            "tableSnapshotIds": {"community_entity_ledger": 7},
        },
    }
    reuse_snapshot = _reuse_snapshot_payload(input_data, preflight)
    if variant == "receipt-object-mismatch":
        payload["silverSnapshot"]["uri"] = "s3://bucket/other-snapshot.json"
    elif variant == "job-object-mismatch":
        uri_index = arguments.index("--silver-snapshot-uri") + 1
        arguments[uri_index] = "s3://bucket/other-snapshot.json"
    elif variant == "snapshot-content-mismatch":
        reuse_snapshot["committedRunIds"] = reuse_snapshot["committedRunIds"][:-1]

    if expected_error is not None:
        with pytest.raises(
            golden_mod.NonRetryableGoldenBuildError,
            match=expected_error,
        ):
            golden_mod._validated_reused_identity(
                input_data,
                preflight,
                payload,
                reuse_snapshot=reuse_snapshot,
            )
        return

    result = golden_mod._validated_reused_identity(
        input_data,
        preflight,
        payload,
        reuse_snapshot=reuse_snapshot,
    )

    assert result.run_id == _run(94)
    assert result.job_run_id == job_run_id
    assert result.source_run_ids == identity_source_run_ids
    assert result.source_policy_digest == source_policy_digest


def test_golden_preflight_rejects_identity_reuse_across_source_policy_digest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_data = _input()
    identity_source_run_ids = _fixture_incremental_identity_runs(input_data)
    source_build_id = "catalog-source-policy-v8-20261014-r2"
    current_policy = golden_mod.load_catalog_source_policy().digest
    historical_policy = "sha256:" + ("b" * 64)
    source_runtime, source_resolution, source_bound = (
        golden_mod._identity_config_digests(
            source_build_id,
            source_policy_digest=historical_policy,
        )
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
        source_policy_digest=current_policy,
        source_run_states=(),
    )
    image_digest = _run(93)
    snapshot_ref = _reuse_snapshot_ref()
    arguments = _reuse_identity_arguments(snapshot_ref)
    for run_id in identity_source_run_ids:
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
            "--source-policy-digest",
            historical_policy,
        ]
    )
    job_run_id = "00historicalidentity"
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
    monkeypatch.setattr(
        golden_mod,
        "read_driver_json_summary",
        lambda **_kwargs: {
            "runId": _run(94),
            "commitKey": _run(95),
            "sourceRunIds": list(identity_source_run_ids),
            "identityGenerationId": input_data.identity_generation_id,
            "identityMode": "incremental",
            "tableCounts": {"community_entity_ledger": 1},
            "tableSnapshotIds": {"community_entity_ledger": 7},
        },
    )
    payload = {
        "schemaVersion": "1.0",
        "sourceBuildId": source_build_id,
        "applicationId": "00verifiedapplication",
        "jobRunId": job_run_id,
        "imageDigest": image_digest,
        "silverSnapshot": _mapping(snapshot_ref),
        "identitySummary": {
            "context": "research",
            "stage": "identity-resolution",
            "runId": _run(94),
            "commitKey": _run(95),
            "configDigest": source_bound,
            "identityResolutionConfigDigest": source_resolution,
            "registryDigest": golden_mod.build_community_registry().digest,
            "sourcePolicyDigest": historical_policy,
            "identityGenerationId": input_data.identity_generation_id,
            "identityMode": "incremental",
            "sourceRunIds": list(identity_source_run_ids),
            "tableCounts": {"community_entity_ledger": 1},
            "tableSnapshotIds": {"community_entity_ledger": 7},
        },
    }

    assert current_policy != historical_policy
    with pytest.raises(
        golden_mod.NonRetryableGoldenBuildError,
        match="sourcePolicyDigest differs from the current Catalog source policy",
    ):
        golden_mod._validated_reused_identity(
            input_data,
            preflight,
            payload,
            reuse_snapshot=_reuse_snapshot_payload(input_data, preflight),
        )


def test_golden_preflight_reuses_identity_across_registry_digest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_data = _input()
    identity_source_run_ids = _fixture_incremental_identity_runs(input_data)
    source_build_id = "catalog-source-policy-v8-20261014-r2"
    source_policy_digest = golden_mod.load_catalog_source_policy().digest
    source_runtime, source_resolution, source_bound = (
        golden_mod._identity_config_digests(
            source_build_id,
            source_policy_digest=source_policy_digest,
        )
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
        source_policy_digest=source_policy_digest,
        source_run_states=(),
    )
    image_digest = _run(93)
    snapshot_ref = _reuse_snapshot_ref()
    arguments = _reuse_identity_arguments(snapshot_ref)
    for run_id in identity_source_run_ids:
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
            "--source-policy-digest",
            source_policy_digest,
        ]
    )
    job_run_id = "00historicalregistry"
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
    monkeypatch.setattr(
        golden_mod,
        "read_driver_json_summary",
        lambda **_kwargs: {
            "runId": _run(94),
            "commitKey": _run(95),
            "sourceRunIds": list(identity_source_run_ids),
            "identityGenerationId": input_data.identity_generation_id,
            "identityMode": "incremental",
            "tableCounts": {"community_entity_ledger": 1},
            "tableSnapshotIds": {"community_entity_ledger": 7},
        },
    )
    historical_registry = "sha256:" + ("a" * 64)
    assert historical_registry != golden_mod.build_community_registry().digest
    payload = {
        "schemaVersion": "1.0",
        "sourceBuildId": source_build_id,
        "applicationId": "00verifiedapplication",
        "jobRunId": job_run_id,
        "imageDigest": image_digest,
        "silverSnapshot": _mapping(snapshot_ref),
        "identitySummary": {
            "context": "research",
            "stage": "identity-resolution",
            "runId": _run(94),
            "commitKey": _run(95),
            "configDigest": source_bound,
            "identityResolutionConfigDigest": source_resolution,
            "registryDigest": historical_registry,
            "sourcePolicyDigest": source_policy_digest,
            "identityGenerationId": input_data.identity_generation_id,
            "identityMode": "incremental",
            "sourceRunIds": list(identity_source_run_ids),
            "tableCounts": {"community_entity_ledger": 1},
            "tableSnapshotIds": {"community_entity_ledger": 7},
        },
    }

    result = golden_mod._validated_reused_identity(
        input_data,
        preflight,
        payload,
        reuse_snapshot=_reuse_snapshot_payload(input_data, preflight),
    )

    assert result.run_id == _run(94)
    assert result.source_policy_digest == source_policy_digest


def test_golden_preflight_rejects_pre_policy_identity_job(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_data = _input()
    identity_source_run_ids = _fixture_incremental_identity_runs(input_data)
    source_build_id = "tmdb-multisource-20261009-r3"
    _source_runtime, source_resolution, _source_bound = (
        golden_mod._identity_config_digests(
            source_build_id,
            source_policy_digest="sha256:" + ("c" * 64),
        )
    )
    # Reconstruct the historical runtime digest without a source-policy pin by
    # binding whatever digest the EMR job recorded.
    historical_runtime = "sha256:" + ("d" * 64)
    historical_bound = golden_mod.IdentityResolutionConfig().bind_runtime_config(
        historical_runtime
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
        source_policy_digest=golden_mod.load_catalog_source_policy().digest,
    )
    image_digest = _run(93)
    snapshot_ref = _reuse_snapshot_ref()
    arguments = _reuse_identity_arguments(snapshot_ref)
    for run_id in identity_source_run_ids:
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
            historical_runtime,
        ]
    )
    job_run_id = "00prepolicyidentity"
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
    monkeypatch.setattr(
        golden_mod,
        "read_driver_json_summary",
        lambda **_kwargs: {
            "runId": _run(94),
            "commitKey": _run(95),
            "sourceRunIds": list(identity_source_run_ids),
            "identityGenerationId": input_data.identity_generation_id,
            "identityMode": "incremental",
            "tableCounts": {"community_entity_ledger": 1},
            "tableSnapshotIds": {"community_entity_ledger": 7},
        },
    )
    payload = {
        "schemaVersion": "1.0",
        "sourceBuildId": source_build_id,
        "applicationId": "00verifiedapplication",
        "jobRunId": job_run_id,
        "imageDigest": image_digest,
        "silverSnapshot": _mapping(snapshot_ref),
        "identitySummary": {
            "context": "research",
            "stage": "identity-resolution",
            "runId": _run(94),
            "commitKey": _run(95),
            "configDigest": historical_bound,
            "identityResolutionConfigDigest": source_resolution,
            "registryDigest": golden_mod.build_community_registry().digest,
            "identityGenerationId": input_data.identity_generation_id,
            "identityMode": "incremental",
            "sourceRunIds": list(identity_source_run_ids),
            "tableCounts": {"community_entity_ledger": 1},
            "tableSnapshotIds": {"community_entity_ledger": 7},
        },
    }

    with pytest.raises(
        golden_mod.NonRetryableGoldenBuildError,
        match="sourcePolicyDigest differs from the current Catalog source policy",
    ):
        golden_mod._validated_reused_identity(
            input_data,
            preflight,
            payload,
            reuse_snapshot=_reuse_snapshot_payload(input_data, preflight),
        )


def test_golden_identity_rejects_mismatched_source_policy_digest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_data = _input()
    source_policy_digest = golden_mod.load_catalog_source_policy().digest
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
        identity_resolution_config_digest=_run(92),
        identity_bound_config_digest=_run(93),
        source_policy_digest=source_policy_digest,
    )
    required_keys: tuple[str, ...] = ()

    def fake_stage(**kwargs: Any) -> tuple[dict[str, Any], str]:
        nonlocal required_keys
        required_keys = kwargs["required_keys"]
        return (
            {
                "runId": _run(94),
                "commitKey": _run(95),
                "configDigest": preflight.identity_bound_config_digest,
                "identityResolutionConfigDigest": (
                    preflight.identity_resolution_config_digest
                ),
                "sourcePolicyDigest": _run(96),
                "identityGenerationId": input_data.identity_generation_id,
                "identityMode": "incremental",
                "sourceRunIds": list(preflight.tmdb_source_run_ids),
                "tableCounts": {"community_entity_ledger": 1},
                "tableSnapshotIds": {"community_entity_ledger": 7},
            },
            "identity-job",
        )

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
        match="Identity summary differs",
    ):
        golden_mod.resolve_golden_identity(
            input_data,
            preflight,
            SimpleNamespace(snapshot=_ref("s3://bucket/snapshot.json")),
            "2026-10-09T00:00:00Z",
            "2026-10-09T00:00:00Z",
        )

    assert "sourcePolicyDigest" in required_keys


@pytest.mark.parametrize("reuse_identity", [False, True])
def test_epoch_accepts_verified_identity_lineage_closure(
    monkeypatch: pytest.MonkeyPatch,
    reuse_identity: bool,
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
        run_id=(input_data.baseline_identity_run_id if reuse_identity else _run(100)),
        commit_key=_run(101),
        source_run_ids=preflight.tmdb_source_run_ids,
        table_counts={"community_entity_ledger": 1},
        table_snapshot_ids={"community_entity_ledger": 7},
        job_run_id="identity-job",
        source_policy_digest=golden_mod.load_catalog_source_policy().digest,
    )
    expected = tuple(sorted({*preflight.all_snapshot_run_ids, identity.run_id}))
    lineage = (_run(104),)
    committed = tuple(sorted({*expected, *lineage}))
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
                "requestedDeltaRunIds": expected,
                "identityLineageRunIds": lineage,
                "deltaRunIds": committed,
                "committedRunCount": len(committed),
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
    assert len(delta_values) == len(set(delta_values))
    assert result.committed_run_count == len(committed)


def test_normalized_object_ref_payload_ignores_checksum_encoding() -> None:
    left = ObjectRefPayload(
        uri="s3://bucket/silver.json",
        format="OBJECT_FORMAT_JSON",
        media_type=SILVER_SNAPSHOT_MEDIA_TYPE,
        checksum_algorithm="sha256",
        checksum_value="sha256:" + ("a" * 64),
        size_bytes=12,
        etag='"etag-1"',
        object_version="v1",
    )
    right = ObjectRefPayload(
        uri="s3://bucket/silver.json",
        format="OBJECT_FORMAT_JSON",
        media_type=SILVER_SNAPSHOT_MEDIA_TYPE,
        checksum_algorithm="CHECKSUM_ALGORITHM_SHA256",
        checksum_value="a" * 64,
        size_bytes=12,
        etag="etag-1",
        object_version="v1",
    )
    assert golden_mod._normalized_object_ref_payload(
        left
    ) == golden_mod._normalized_object_ref_payload(right)


def test_baseline_identity_parent_silver_binds_downloaded_content(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_data = _input()
    from video_media_catalog.community_snapshot import (
        DATA_TABLE_COLUMNS,
        build_community_silver_snapshot_set,
    )
    from video_media_catalog.community_tables import build_community_table_mapping

    table_mapping = build_community_table_mapping(input_data.identity_generation_id)
    snapshot = build_community_silver_snapshot_set(
        committed_run_ids=tuple(
            sorted(
                (
                    *input_data.baseline_source_run_ids,
                    input_data.baseline_identity_run_id,
                )
            )
        ),
        run_snapshot_id=1,
        commit_snapshot_id=2,
        data_snapshot_ids={table: 1 for table in DATA_TABLE_COLUMNS},
        identity_generation_id=input_data.identity_generation_id,
        table_mapping=table_mapping,
        created_at="2026-10-08T00:00:00Z",
    )
    body = snapshot.json_bytes()
    silver_ref = ObjectRefPayload(
        uri="s3://bucket/parent-silver.json",
        format="OBJECT_FORMAT_JSON",
        media_type=SILVER_SNAPSHOT_MEDIA_TYPE,
        checksum_algorithm="CHECKSUM_ALGORITHM_SHA256",
        checksum_value=hashlib.sha256(body).hexdigest(),
        size_bytes=len(body),
        etag="parent-etag",
        object_version="parent-version",
    )
    baseline = {
        "schemaVersion": "2.0",
        "identityGenerationId": input_data.identity_generation_id,
        "tableMapping": table_mapping,
        "committedRunIds": list(snapshot.committed_run_ids),
    }

    def fake_read(_client, reference, *, label, max_bytes=0):
        assert reference.uri == silver_ref.uri
        return body

    monkeypatch.setattr(golden_mod, "_read_ref_bytes", fake_read)
    golden_mod._validate_baseline_identity_parent_silver(
        object(),
        silver_ref,
        expected_manifest_id=snapshot.snapshot_set_id,
        baseline=baseline,
        identity_generation_id=input_data.identity_generation_id,
    )


def test_baseline_identity_rejects_epoch_binding_without_epoch_id() -> None:
    input_data = _input()
    from test_temporal_golden_models import _baseline_identity_manifest

    epoch_ref = replace(
        _ref("s3://bucket/parent-epoch.json"),
        media_type=SILVER_EPOCH_MEDIA_TYPE,
    )
    manifest = _baseline_identity_manifest(
        identity_generation_id=input_data.identity_generation_id,
        source_run_ids=input_data.baseline_identity_source_run_ids,
        silver_ref=epoch_ref,
    )
    # Keep snapshotSetId while advertising epoch media type — must fail closed.
    baseline = {
        "schemaVersion": "2.0",
        "identityGenerationId": input_data.identity_generation_id,
        "tableMapping": {"community_ingest_run": "community_ingest_run"},
    }
    with pytest.raises(
        golden_mod.NonRetryableGoldenBuildError,
        match="epoch Silver binding requires epochId",
    ):
        golden_mod._validate_baseline_identity_manifest(
            input_data,
            baseline=baseline,
            payload=manifest,
            identity_run_id=str(manifest["runId"]),
            identity_source_run_ids=input_data.baseline_identity_source_run_ids,
            client=None,
        )
