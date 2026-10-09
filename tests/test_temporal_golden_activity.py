from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from test_temporal_golden_models import _input, _ref, _run

from video_media_catalog.community_snapshot import SILVER_EPOCH_MEDIA_TYPE
from video_media_catalog.temporal.activities import golden as golden_mod
from video_media_catalog.temporal.golden_models import (
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
    assert parsed.executor_instances == 12
    assert parsed.shuffle_partitions == 288
    assert parsed.executor_cores == 4
    assert parsed.client_token == golden_mod._stage_token(
        build_id=input_data.build_id,
        stage="identity",
        workflow_run_id="temporal-run-1",
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
