from __future__ import annotations

from dataclasses import asdict, replace

import pytest

from video_media_catalog.catalog_source_policy import load_catalog_source_policy
from video_media_catalog.community_ingest import (
    IngestRunKind,
    build_community_ingest_run,
)
from video_media_catalog.community_snapshot import SILVER_SNAPSHOT_MEDIA_TYPE
from video_media_catalog.community_tables import DATA_TABLE_COLUMNS
from video_media_catalog.gold_freshness import (
    research_release_freshness_policy,
    scope_release_freshness_policy,
)
from video_media_catalog.temporal.activities.golden import (
    _identity_source_run_ids,
    _incremental_identity_source_run_ids,
)
from video_media_catalog.temporal.activities.golden import (
    _validate_preflight_documents as _validate_preflight_documents_impl,
)
from video_media_catalog.temporal.errors import NonRetryableGoldenBuildError
from video_media_catalog.temporal.golden_admin_cli import _state_bool
from video_media_catalog.temporal.golden_models import (
    FreshnessOverrideInput,
    GoldenBuildEnv,
    GoldenBuildInput,
    GoldenIdentityMode,
    GoldenSourceFreshnessStatus,
    GoldenSourceRunState,
    GoldenSourceRunStatus,
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


def _ref_payload(reference: ObjectRefPayload) -> dict[str, object]:
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


def _baseline_identity_manifest(
    *,
    identity_generation_id: str,
    source_run_ids: tuple[str, ...],
    silver_ref: ObjectRefPayload | None = None,
) -> dict[str, object]:
    table_mapping = {"community_ingest_run": "community_ingest_run"}
    identity_silver_ref = silver_ref or replace(
        _ref("s3://bucket/baseline-identity-input.json"),
        media_type=SILVER_SNAPSHOT_MEDIA_TYPE,
    )
    source_policy_digest = _run(703)
    input_id = _run(704)
    pinned_inputs = {
        "silverSnapshot": {
            "object": _ref_payload(identity_silver_ref),
            "snapshotSetId": _run(705),
        },
        "sourceRunIds": list(source_run_ids),
        "identityGenerationId": identity_generation_id,
        "identityMode": "incremental",
        "tableMapping": table_mapping,
        "sourcePolicyDigest": source_policy_digest,
    }
    run = build_community_ingest_run(
        run_kind=IngestRunKind.IDENTITY_RESOLUTION,
        source_product_id="identity-resolution-v2",
        input_id=input_id,
        policy_id="internal-key-continuity",
        policy_digest=_run(706),
        image_digest=_run(707),
        config_digest=_run(708),
        started_at="2026-10-08T00:00:00Z",
        expected_counts={table: 0 for table in DATA_TABLE_COLUMNS},
        input_manifest={
            "inputId": input_id,
            "identityGenerationId": identity_generation_id,
            "identityMode": "incremental",
            "tableMapping": table_mapping,
            "sourcePolicyDigest": source_policy_digest,
            "pinnedInputs": pinned_inputs,
        },
    )
    return run.model_dump(mode="json", by_alias=True)


def _validate_preflight_documents(
    input_data: GoldenBuildInput,
    *,
    baseline: dict[str, object],
    baseline_identity_manifest: dict[str, object] | None = None,
    tmdb_summary: dict[str, object],
    planned_at: str,
):
    return _validate_preflight_documents_impl(
        input_data,
        baseline=baseline,
        baseline_identity_manifest=(
            baseline_identity_manifest
            or _baseline_identity_manifest(
                identity_generation_id=input_data.identity_generation_id,
                source_run_ids=input_data.baseline_identity_source_run_ids,
            )
        ),
        tmdb_summary=tmdb_summary,
        planned_at=planned_at,
    )


def test_source_state_boolean_config_is_strict() -> None:
    assert not _state_bool({"identitySpine": False}, "identitySpine", default=True)
    assert not _state_bool({}, "identitySpine", default=False)
    with pytest.raises(ValueError, match="must be a boolean"):
        _state_bool({"identitySpine": "false"}, "identitySpine", default=True)


def _input() -> GoldenBuildInput:
    manifest = _baseline_identity_manifest(
        identity_generation_id="pure-source-2026-09-r1",
        source_run_ids=(_run(3),),
    )
    return GoldenBuildInput(
        build_id="tmdb-multisource-20261009",
        identity_generation_id="pure-source-2026-09-r1",
        baseline_snapshot=_ref("s3://bucket/baseline.json"),
        tmdb_pipeline_summary=_ref("s3://bucket/tmdb-summary.json"),
        baseline_source_run_ids=(_run(1), _run(2), _run(3)),
        baseline_identity_run_id=str(manifest["runId"]),
        baseline_identity_manifest=replace(
            _ref("s3://bucket/baseline-identity-manifest.json"),
            media_type=(
                "application/vnd.video-media-catalog.community-ingest-run.v2+json"
            ),
        ),
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
        baseline_identity_source_run_ids=(_run(3),),
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


def _with_source_states(input_data: GoldenBuildInput) -> GoldenBuildInput:
    return replace(
        input_data,
        source_run_states=(
            GoldenSourceRunState(
                source_product_id="imdb-non-commercial-datasets",
                run_ids=(input_data.baseline_source_run_ids[0],),
                run_status=GoldenSourceRunStatus.COMMITTED,
                freshness_status=GoldenSourceFreshnessStatus.OVERRIDDEN,
                watermark=input_data.source_watermarks["imdb-non-commercial-datasets"],
                acquired_at=input_data.freshness_override.imdb_acquired_at,
            ),
            GoldenSourceRunState(
                source_product_id="tvmaze-public-api",
                run_ids=(input_data.baseline_source_run_ids[1],),
                run_status=GoldenSourceRunStatus.COMMITTED,
                freshness_status=GoldenSourceFreshnessStatus.OVERRIDDEN,
                watermark=input_data.source_watermarks["tvmaze-public-api"],
                acquired_at=input_data.freshness_override.tvmaze_acquired_at,
            ),
            GoldenSourceRunState(
                source_product_id="wikidata-json-dump",
                run_ids=(input_data.baseline_source_run_ids[2],),
                run_status=GoldenSourceRunStatus.COMMITTED,
                freshness_status=GoldenSourceFreshnessStatus.PASS,
                watermark=input_data.source_watermarks["wikidata-json-dump"],
                identity_spine=False,
                materialization=False,
                gold_contribution=True,
            ),
            GoldenSourceRunState(
                source_product_id="tmdb-research",
                run_ids=input_data.tmdb_source_run_ids,
                run_status=GoldenSourceRunStatus.COMMITTED,
                freshness_status=GoldenSourceFreshnessStatus.PASS,
                watermark=input_data.source_watermarks["tmdb-research"],
                identity_spine=True,
                materialization=True,
            ),
        ),
    )


def test_preflight_rejects_legacy_source_routing_without_explicit_states() -> None:
    input_data = _input()
    baseline, tmdb = _documents(input_data)

    with pytest.raises(
        NonRetryableGoldenBuildError,
        match="requires explicit sourceRunStates",
    ):
        _validate_preflight_documents(
            input_data,
            baseline=baseline,
            tmdb_summary=tmdb,
            planned_at="2026-10-09T00:00:00Z",
        )


def test_preflight_pins_explicit_source_run_and_freshness_states() -> None:
    input_data = _with_source_states(_input())
    baseline, tmdb = _documents(input_data)

    result = _validate_preflight_documents(
        input_data,
        baseline=baseline,
        tmdb_summary=tmdb,
        planned_at="2026-10-09T00:00:00Z",
    )

    assert result.source_policy_digest == load_catalog_source_policy().digest
    assert {
        item.source_product_id: item.freshness_status
        for item in result.source_run_states
    } == {
        "imdb-non-commercial-datasets": GoldenSourceFreshnessStatus.OVERRIDDEN,
        "tmdb-research": GoldenSourceFreshnessStatus.PASS,
        "tvmaze-public-api": GoldenSourceFreshnessStatus.OVERRIDDEN,
        "wikidata-json-dump": GoldenSourceFreshnessStatus.PASS,
    }
    assert result.all_snapshot_run_ids == tuple(
        sorted(
            (
                input_data.baseline_source_run_ids[2],
                input_data.baseline_identity_run_id,
                *input_data.tmdb_source_run_ids,
            )
        )
    )
    assert (
        _incremental_identity_source_run_ids(
            input_data,
            result,
        )
        == input_data.tmdb_source_run_ids
    )


def test_preflight_accepts_tmdb_runs_closed_in_the_incremental_baseline() -> None:
    base = _with_source_states(_input())
    music_run = _run(30)
    baseline_identity_source_run_ids = tuple(
        sorted(
            (
                *base.baseline_identity_source_run_ids,
                *base.tmdb_source_run_ids,
            )
        )
    )
    identity_manifest = _baseline_identity_manifest(
        identity_generation_id=base.identity_generation_id,
        source_run_ids=baseline_identity_source_run_ids,
    )
    input_data = replace(
        base,
        baseline_source_run_ids=tuple(
            sorted((*base.baseline_source_run_ids, *base.tmdb_source_run_ids))
        ),
        baseline_identity_run_id=str(identity_manifest["runId"]),
        baseline_identity_source_run_ids=baseline_identity_source_run_ids,
        supplemental_source_run_ids=(music_run,),
        source_watermarks={
            **base.source_watermarks,
            "musicbrainz-core-dump": "music-pilot-r1",
        },
        source_run_states=(
            *base.source_run_states,
            GoldenSourceRunState(
                source_product_id="musicbrainz-core-dump",
                run_ids=(music_run,),
                run_status=GoldenSourceRunStatus.COMMITTED,
                freshness_status=GoldenSourceFreshnessStatus.PASS,
                watermark="music-pilot-r1",
                identity_spine=True,
                materialization=False,
                gold_contribution=True,
            ),
        ),
    )
    baseline, tmdb = _documents(input_data)

    result = _validate_preflight_documents(
        input_data,
        baseline=baseline,
        baseline_identity_manifest=identity_manifest,
        tmdb_summary=tmdb,
        planned_at="2026-10-09T00:00:00Z",
    )

    assert result.tmdb_source_run_ids == input_data.tmdb_source_run_ids
    assert set(input_data.tmdb_source_run_ids) < set(result.all_snapshot_run_ids)
    assert _incremental_identity_source_run_ids(input_data, result) == (music_run,)


def test_preflight_rejects_a_partially_baseline_pinned_tmdb_cohort() -> None:
    base = _with_source_states(_input())
    input_data = replace(
        base,
        baseline_source_run_ids=(
            *base.baseline_source_run_ids,
            base.tmdb_source_run_ids[0],
        ),
    )
    baseline, tmdb = _documents(input_data)

    with pytest.raises(
        NonRetryableGoldenBuildError,
        match="entirely baseline-pinned or entirely delta-pinned",
    ):
        _validate_preflight_documents(
            input_data,
            baseline=baseline,
            tmdb_summary=tmdb,
            planned_at="2026-10-09T00:00:00Z",
        )


def test_preflight_accepts_enabled_source_identity_intent() -> None:
    base = _with_source_states(_input())
    input_data = replace(
        base,
        source_run_states=tuple(
            replace(
                state,
                identity_spine=True,
                materialization=True,
                gold_contribution=True,
            )
            if state.source_product_id == "imdb-non-commercial-datasets"
            else state
            for state in base.source_run_states
        ),
    )
    baseline, tmdb = _documents(input_data)

    result = _validate_preflight_documents(
        input_data,
        baseline=baseline,
        tmdb_summary=tmdb,
        planned_at="2026-10-09T00:00:00Z",
    )

    assert result.all_snapshot_run_ids == tuple(
        sorted(
            (
                _run(1),
                _run(3),
                input_data.baseline_identity_run_id,
                *input_data.tmdb_source_run_ids,
            )
        )
    )
    assert _incremental_identity_source_run_ids(input_data, result) == tuple(
        sorted((_run(1), *input_data.tmdb_source_run_ids))
    )


def test_preflight_full_identity_uses_fresh_generation_and_all_spine_sources() -> None:
    baseline_input = _with_source_states(_input())
    input_data = replace(
        baseline_input,
        identity_generation_id="pure-source-2026-09-r2",
        identity_mode=GoldenIdentityMode.FULL,
        source_run_states=tuple(
            replace(
                state,
                identity_spine=True,
                materialization=True,
                gold_contribution=True,
            )
            if state.source_product_id
            in {
                "imdb-non-commercial-datasets",
                "tvmaze-public-api",
            }
            else state
            for state in baseline_input.source_run_states
        ),
    )
    baseline, tmdb = _documents(input_data)
    baseline["identityGenerationId"] = baseline_input.identity_generation_id
    baseline_manifest = _baseline_identity_manifest(
        identity_generation_id=baseline_input.identity_generation_id,
        source_run_ids=input_data.baseline_identity_source_run_ids,
    )

    result = _validate_preflight_documents(
        input_data,
        baseline=baseline,
        baseline_identity_manifest=baseline_manifest,
        tmdb_summary=tmdb,
        planned_at="2026-10-09T00:00:00Z",
    )

    expected_identity_runs = tuple(
        sorted(
            (
                input_data.baseline_source_run_ids[0],
                input_data.baseline_source_run_ids[1],
                *input_data.tmdb_source_run_ids,
            )
        )
    )
    assert input_data.baseline_identity_run_id not in result.all_snapshot_run_ids
    assert set(result.all_snapshot_run_ids) == {
        *input_data.baseline_source_run_ids,
        *input_data.tmdb_source_run_ids,
    }
    assert _identity_source_run_ids(input_data, result) == expected_identity_runs


def test_preflight_full_identity_rejects_reusing_populated_generation() -> None:
    input_data = replace(
        _with_source_states(_input()),
        identity_mode=GoldenIdentityMode.FULL,
    )
    baseline, tmdb = _documents(input_data)

    with pytest.raises(
        NonRetryableGoldenBuildError,
        match="requires a fresh Identity generation",
    ):
        _validate_preflight_documents(
            input_data,
            baseline=baseline,
            tmdb_summary=tmdb,
            planned_at="2026-10-09T00:00:00Z",
        )


def test_preflight_accepts_reference_only_musicbrainz_source_state() -> None:
    base = _with_source_states(_input())
    music_run = _run(90)
    input_data = replace(
        base,
        supplemental_source_run_ids=(music_run,),
        source_watermarks={
            **base.source_watermarks,
            "musicbrainz-core-dump": "musicbrainz-core-2026-10-17",
        },
        source_run_states=(
            *base.source_run_states,
            GoldenSourceRunState(
                source_product_id="musicbrainz-core-dump",
                run_ids=(music_run,),
                run_status=GoldenSourceRunStatus.COMMITTED,
                freshness_status=GoldenSourceFreshnessStatus.PASS,
                watermark="musicbrainz-core-2026-10-17",
                identity_spine=True,
                materialization=False,
                gold_contribution=True,
            ),
        ),
    )
    baseline, tmdb = _documents(input_data)

    result = _validate_preflight_documents(
        input_data,
        baseline=baseline,
        tmdb_summary=tmdb,
        planned_at="2026-10-09T00:00:00Z",
    )

    assert music_run in result.all_snapshot_run_ids
    assert music_run in _incremental_identity_source_run_ids(input_data, result)
    music = next(
        state
        for state in result.source_run_states
        if state.source_product_id == "musicbrainz-core-dump"
    )
    assert music.identity_spine and music.gold_contribution
    assert music.materialization is False


def test_preflight_does_not_reprocess_baseline_identity_sources() -> None:
    base = _with_source_states(_input())
    input_data = replace(
        base,
        source_run_states=tuple(
            replace(state, identity_spine=True)
            if state.source_product_id == "wikidata-json-dump"
            else state
            for state in base.source_run_states
        ),
    )
    baseline, tmdb = _documents(input_data)

    result = _validate_preflight_documents(
        input_data,
        baseline=baseline,
        tmdb_summary=tmdb,
        planned_at="2026-10-09T00:00:00Z",
    )

    assert input_data.baseline_identity_source_run_ids[
        0
    ] not in _incremental_identity_source_run_ids(input_data, result)


def test_preflight_requires_baseline_identity_source_pin() -> None:
    input_data = replace(
        _with_source_states(_input()),
        baseline_identity_source_run_ids=(),
    )
    baseline, tmdb = _documents(input_data)

    with pytest.raises(
        NonRetryableGoldenBuildError,
        match="baseline Identity source run IDs must be pinned",
    ):
        _validate_preflight_documents(
            input_data,
            baseline=baseline,
            tmdb_summary=tmdb,
            planned_at="2026-10-09T00:00:00Z",
        )


def test_preflight_rejects_baseline_identity_manifest_coverage_mismatch() -> None:
    manifest = _baseline_identity_manifest(
        identity_generation_id="pure-source-2026-09-r1",
        source_run_ids=(_run(2),),
    )
    input_data = _with_source_states(
        replace(
            _input(),
            baseline_identity_run_id=str(manifest["runId"]),
        )
    )
    baseline, tmdb = _documents(input_data)

    with pytest.raises(
        NonRetryableGoldenBuildError,
        match="source runs differ from the pinned coverage",
    ):
        _validate_preflight_documents(
            input_data,
            baseline=baseline,
            baseline_identity_manifest=manifest,
            tmdb_summary=tmdb,
            planned_at="2026-10-09T00:00:00Z",
        )


def test_preflight_rejects_incomplete_baseline_identity_silver_reference() -> None:
    manifest = _baseline_identity_manifest(
        identity_generation_id="pure-source-2026-09-r1",
        source_run_ids=(_run(3),),
        silver_ref=replace(
            _ref("s3://bucket/baseline-identity-input.json"),
            media_type=SILVER_SNAPSHOT_MEDIA_TYPE,
            size_bytes=0,
        ),
    )
    input_data = _with_source_states(
        replace(
            _input(),
            baseline_identity_run_id=str(manifest["runId"]),
        )
    )
    baseline, tmdb = _documents(input_data)

    with pytest.raises(
        NonRetryableGoldenBuildError,
        match="Silver ObjectRef is incomplete",
    ):
        _validate_preflight_documents(
            input_data,
            baseline=baseline,
            baseline_identity_manifest=manifest,
            tmdb_summary=tmdb,
            planned_at="2026-10-09T00:00:00Z",
        )


def test_preflight_rejects_noncontributing_baseline_identity_coverage() -> None:
    base = _with_source_states(_input())
    input_data = replace(
        base,
        source_run_states=tuple(
            replace(state, gold_contribution=False)
            if state.source_product_id == "wikidata-json-dump"
            else state
            for state in base.source_run_states
        ),
    )
    baseline, tmdb = _documents(input_data)

    with pytest.raises(
        NonRetryableGoldenBuildError,
        match="baseline Identity coverage contains source runs excluded from Gold",
    ):
        _validate_preflight_documents(
            input_data,
            baseline=baseline,
            tmdb_summary=tmdb,
            planned_at="2026-10-09T00:00:00Z",
        )


def test_preflight_projects_sources_already_covered_by_baseline_identity() -> None:
    base = _with_source_states(_input())
    input_data = replace(
        base,
        source_run_states=tuple(
            replace(state, identity_spine=False, gold_contribution=True)
            if state.source_product_id == "wikidata-json-dump"
            else replace(state, identity_spine=True, gold_contribution=True)
            for state in base.source_run_states
        ),
    )
    baseline, tmdb = _documents(input_data)

    result = _validate_preflight_documents(
        input_data,
        baseline=baseline,
        tmdb_summary=tmdb,
        planned_at="2026-10-09T00:00:00Z",
    )

    assert set(result.all_snapshot_run_ids) == {
        *input_data.baseline_source_run_ids,
        input_data.baseline_identity_run_id,
        *input_data.tmdb_source_run_ids,
    }
    assert _incremental_identity_source_run_ids(input_data, result) == tuple(
        sorted(
            (
                input_data.baseline_source_run_ids[0],
                input_data.baseline_source_run_ids[1],
                *input_data.tmdb_source_run_ids,
            )
        )
    )


def test_preflight_rejects_gold_only_nonbaseline_runs() -> None:
    base = _with_source_states(_input())
    input_data = replace(
        base,
        source_run_states=tuple(
            replace(
                state,
                identity_spine=False,
                gold_contribution=True,
            )
            if state.source_product_id == "tmdb-research"
            else state
            for state in base.source_run_states
        ),
    )
    baseline, tmdb = _documents(input_data)

    with pytest.raises(
        NonRetryableGoldenBuildError,
        match=r"Gold-contributing.*baseline Identity",
    ):
        _validate_preflight_documents(
            input_data,
            baseline=baseline,
            tmdb_summary=tmdb,
            planned_at="2026-10-09T00:00:00Z",
        )


def test_preflight_allows_reused_identity_tmdb_materialization_without_spine() -> None:
    base = _with_source_states(_input())
    input_data = replace(
        base,
        identity_reuse=_ref("s3://bucket/identity-reuse.json"),
        source_run_states=tuple(
            replace(
                state,
                identity_spine=False,
                materialization=True,
                gold_contribution=True,
            )
            if state.source_product_id == "tmdb-research"
            else replace(state, identity_spine=True, gold_contribution=True)
            for state in base.source_run_states
        ),
    )
    baseline, tmdb = _documents(input_data)

    result = _validate_preflight_documents(
        input_data,
        baseline=baseline,
        tmdb_summary=tmdb,
        planned_at="2026-10-09T00:00:00Z",
    )

    assert set(result.all_snapshot_run_ids) == {
        *input_data.baseline_source_run_ids,
        input_data.baseline_identity_run_id,
        *input_data.tmdb_source_run_ids,
    }
    assert "tmdb-research" not in {
        state.source_product_id
        for state in result.source_run_states
        if state.identity_spine
    }
    assert _incremental_identity_source_run_ids(input_data, result) == tuple(
        sorted(
            (
                input_data.baseline_source_run_ids[0],
                input_data.baseline_source_run_ids[1],
                *input_data.tmdb_source_run_ids,
            )
        )
    )


def test_preflight_rejects_pending_source_gold_contribution() -> None:
    base = _with_source_states(_input())
    eidr_run = _run(90)
    input_data = replace(
        base,
        supplemental_source_run_ids=(eidr_run,),
        source_run_states=(
            *base.source_run_states,
            GoldenSourceRunState(
                source_product_id="eidr-public-registry",
                run_ids=(eidr_run,),
                run_status=GoldenSourceRunStatus.PENDING,
                freshness_status=GoldenSourceFreshnessStatus.UNKNOWN,
                identity_spine=True,
                materialization=False,
                gold_contribution=True,
            ),
        ),
    )
    baseline, tmdb = _documents(input_data)

    with pytest.raises(
        NonRetryableGoldenBuildError,
        match="pending, failed, or excluded",
    ):
        _validate_preflight_documents(
            input_data,
            baseline=baseline,
            tmdb_summary=tmdb,
            planned_at="2026-10-09T00:00:00Z",
        )


def test_preflight_rejects_stale_optional_source_gold_contribution() -> None:
    base = _with_source_states(_input())
    eidr_run = _run(90)
    input_data = replace(
        base,
        supplemental_source_run_ids=(eidr_run,),
        source_run_states=(
            *base.source_run_states,
            GoldenSourceRunState(
                source_product_id="eidr-public-registry",
                run_ids=(eidr_run,),
                run_status=GoldenSourceRunStatus.COMMITTED,
                freshness_status=GoldenSourceFreshnessStatus.STALE,
                identity_spine=True,
                materialization=False,
                gold_contribution=True,
            ),
        ),
    )
    baseline, tmdb = _documents(input_data)

    with pytest.raises(
        NonRetryableGoldenBuildError,
        match="not fresh enough for Gold contribution",
    ):
        _validate_preflight_documents(
            input_data,
            baseline=baseline,
            tmdb_summary=tmdb,
            planned_at="2026-10-09T00:00:00Z",
        )


def test_preflight_rejects_source_state_that_disagrees_with_override() -> None:
    base = _with_source_states(_input())
    input_data = replace(
        base,
        source_run_states=tuple(
            replace(
                state,
                freshness_status=GoldenSourceFreshnessStatus.PASS,
            )
            if state.source_product_id == "imdb-non-commercial-datasets"
            else state
            for state in base.source_run_states
        ),
    )
    baseline, tmdb = _documents(input_data)

    with pytest.raises(
        NonRetryableGoldenBuildError,
        match="override states must match IMDb and TVmaze",
    ):
        _validate_preflight_documents(
            input_data,
            baseline=baseline,
            tmdb_summary=tmdb,
            planned_at="2026-10-09T00:00:00Z",
        )


def test_preflight_includes_supplemental_runs_in_snapshot_and_identity() -> None:
    base = _with_source_states(_input())
    eidr_run = _run(30)
    input_data = replace(
        base,
        supplemental_source_run_ids=(eidr_run,),
        source_run_states=(
            *base.source_run_states,
            GoldenSourceRunState(
                source_product_id="eidr-public-registry",
                run_ids=(eidr_run,),
                run_status=GoldenSourceRunStatus.COMMITTED,
                freshness_status=GoldenSourceFreshnessStatus.NOT_REQUIRED,
                identity_spine=True,
                materialization=True,
            ),
        ),
    )
    baseline, tmdb = _documents(input_data)

    result = _validate_preflight_documents(
        input_data,
        baseline=baseline,
        tmdb_summary=tmdb,
        planned_at="2026-10-09T00:00:00Z",
    )

    assert eidr_run in result.all_snapshot_run_ids
    assert _incremental_identity_source_run_ids(input_data, result) == tuple(
        sorted((*input_data.tmdb_source_run_ids, eidr_run))
    )


def test_incremental_identity_run_set_normalizes_sha256_case() -> None:
    input_data = _with_source_states(_input())
    baseline, tmdb = _documents(input_data)
    preflight = _validate_preflight_documents(
        input_data,
        baseline=baseline,
        tmdb_summary=tmdb,
        planned_at="2026-10-09T00:00:00Z",
    )
    mixed_case_input = replace(
        input_data,
        baseline_identity_run_id=input_data.baseline_identity_run_id.upper(),
        baseline_identity_source_run_ids=tuple(
            run_id.upper() for run_id in input_data.baseline_identity_source_run_ids
        ),
    )

    assert (
        _incremental_identity_source_run_ids(
            mixed_case_input,
            preflight,
        )
        == input_data.tmdb_source_run_ids
    )


def test_preflight_applies_audited_temporary_quality_thresholds() -> None:
    base = _with_source_states(_input())
    input_data = replace(
        base,
        env=replace(
            base.env,
            gold_tmdb_freshness_slo_hours=72,
            gold_max_unresolved_identity_ratio=0.12,
            gold_quality_override_reason="One-time r15 publication authorization.",
        ),
    )
    baseline, tmdb = _documents(input_data)

    result = _validate_preflight_documents(
        input_data,
        baseline=baseline,
        tmdb_summary=tmdb,
        planned_at="2026-10-09T00:00:00Z",
    )

    assert (
        result.freshness_policy_digest
        == scope_release_freshness_policy(
            research_release_freshness_policy(
                tmdb_slo_hours=72,
                tvmaze_slo_hours=result.tvmaze_slo_hours,
                imdb_slo_hours=result.imdb_slo_hours,
                wikidata_slo_hours=45 * 24,
            ),
            selected_source_product_ids={
                "tmdb-research",
                "wikidata-json-dump",
            },
        ).digest
    )


def test_preflight_requires_audit_reason_for_non_default_quality_thresholds() -> None:
    base = _input()
    input_data = replace(
        base,
        env=replace(
            base.env,
            gold_max_unresolved_identity_ratio=0.12,
        ),
    )
    baseline, tmdb = _documents(input_data)

    with pytest.raises(
        NonRetryableGoldenBuildError,
        match="requires an audit reason",
    ):
        _validate_preflight_documents(
            input_data,
            baseline=baseline,
            tmdb_summary=tmdb,
            planned_at="2026-10-09T00:00:00Z",
        )


def test_preflight_allows_observe_only_quality_with_audit_reason() -> None:
    base = _with_source_states(_input())
    input_data = replace(
        base,
        env=replace(
            base.env,
            gold_quality_gate_enabled=False,
            gold_quality_override_reason=(
                "Temporary research-only observe mode while quality is remediated."
            ),
        ),
    )
    baseline, tmdb = _documents(input_data)

    result = _validate_preflight_documents(
        input_data,
        baseline=baseline,
        tmdb_summary=tmdb,
        planned_at="2026-10-09T00:00:00Z",
    )

    assert result.tmdb_source_run_ids == input_data.tmdb_source_run_ids


def test_preflight_requires_audit_reason_when_quality_gate_is_disabled() -> None:
    base = _input()
    input_data = replace(
        base,
        env=replace(
            base.env,
            gold_quality_gate_enabled=False,
        ),
    )
    baseline, tmdb = _documents(input_data)

    with pytest.raises(
        NonRetryableGoldenBuildError,
        match="requires an audit reason",
    ):
        _validate_preflight_documents(
            input_data,
            baseline=baseline,
            tmdb_summary=tmdb,
            planned_at="2026-10-09T00:00:00Z",
        )


def test_preflight_rejects_identity_profile_without_python_memory_room() -> None:
    base = _with_source_states(_input())
    input_data = replace(
        base,
        env=replace(
            base.env,
            build_executor_memory_overhead="8g",
        ),
    )
    baseline, tmdb = _documents(input_data)

    with pytest.raises(
        NonRetryableGoldenBuildError,
        match=r"memory overhead must provide at least 2\.5 GiB per core",
    ):
        _validate_preflight_documents(
            input_data,
            baseline=baseline,
            tmdb_summary=tmdb,
            planned_at="2026-10-09T00:00:00Z",
        )


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
    base = _input()
    baseline, tmdb = _documents(base)
    override = base.freshness_override
    input_data = _with_source_states(
        replace(
            base,
            freshness_override=FreshnessOverrideInput(
                reason=override.reason,
                imdb_acquired_at="2026-10-08T23:00:00Z",
                tvmaze_acquired_at="2026-10-08T22:00:00Z",
                max_slo_hours=720,
            ),
        )
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
    assert payload["supplemental_source_run_ids"] == ()
    assert payload["env"]["gold_tmdb_freshness_slo_hours"] == 36
    assert payload["env"]["gold_max_unresolved_identity_ratio"] == 0.05
    assert payload["env"]["gold_quality_gate_enabled"] is True
    assert payload["env"]["gold_quality_override_reason"] == ""
