from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from typing import Any, cast

import pytest
from jsonschema import Draft202012Validator
from temporalio import activity, workflow
from temporalio.client import Client, ScheduleAlreadyRunningError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker
from test_temporal_golden_models import _input, _ref, _run

from video_media_catalog.reference_fixture import load_reference_fixture
from video_media_catalog.temporal.activities.research_domain import (
    RESEARCH_CAPTURE_MANIFEST_SCHEMA,
    load_pinned_research_capture,
)
from video_media_catalog.temporal.golden_models import (
    GoldenBuildInput,
    GoldenBuildResult,
    GoldenEpochResult,
    GoldenIdentityResult,
    GoldenReleaseResult,
    GoldenSnapshotResult,
    GoldenSourceFreshnessStatus,
    GoldenSourceRunState,
    GoldenSourceRunStatus,
)
from video_media_catalog.temporal.models import (
    CaptureResult,
    ObjectRefPayload,
    PipelineEnv,
    SilverResult,
)
from video_media_catalog.temporal.names import (
    GOLDEN_WORKFLOW_TASK_QUEUE,
    SILVER_TASK_QUEUE,
)
from video_media_catalog.temporal.research_domain_admin_cli import (
    _SCHEDULE_COORDINATES,
    RESEARCH_DOMAIN_PROFILES_SCHEMA,
    _load_schedules,
    _upsert_schedule,
)
from video_media_catalog.temporal.research_domain_models import (
    ResearchCaptureResult,
    ResearchDomainInput,
    ResearchSourceProfile,
    ResearchSourceResult,
)
from video_media_catalog.temporal.workflows.research_domain import (
    ResearchCaptureWorkflow,
    ResearchDomainWorkflow,
    ResearchSilverWorkflow,
    _golden_input,
)


@workflow.defn(name="GoldenBuildWorkflow", sandboxed=False)
class StubGoldenBuildWorkflow:
    @workflow.run
    async def run(self, input_data: GoldenBuildInput) -> GoldenBuildResult:
        return _golden_result(input_data)


def _env() -> PipelineEnv:
    return PipelineEnv(
        destination_prefix="s3://bucket/research/capture",
        image_digest="sha256:" + ("1" * 64),
        user_agent="video-media-catalog/research-test",
        aws_region="us-east-1",
        warehouse_uri="s3://bucket/warehouse",
        record_staging_prefix="s3://bucket/warehouse/records",
        silver_checkpoint_prefix="s3://bucket/warehouse/checkpoints",
        pipeline_summary_prefix="s3://bucket/research/summaries",
        emr_application_name="research",
        emr_execution_role_arn="arn:aws:iam::1:role/research",
        emr_log_uri="s3://bucket/logs",
        emr_entry_point="local:///opt/video-media-catalog/community_cli.py",
    )


def _object_mapping(uri: str, marker: str) -> dict[str, Any]:
    return {
        "uri": uri,
        "format": "OBJECT_FORMAT_JSON",
        "mediaType": "application/json",
        "checksum": {"algorithm": "sha256", "value": marker * 64},
        "sizeBytes": 12,
        "etag": f"{marker}-etag",
        "objectVersion": f"{marker}-version",
    }


def _profile(
    source_product_id: str,
    marker: str,
    *,
    capture_manifest: ObjectRefPayload | None = None,
) -> ResearchSourceProfile:
    return ResearchSourceProfile(
        profile_id=f"sports-{source_product_id}",
        domain="SPORTS",
        source_product_id=source_product_id,
        capture_manifest=capture_manifest
        or ObjectRefPayload.from_mapping(
            _object_mapping(
                f"s3://bucket/captures/{source_product_id}.json",
                marker,
            )
        ),
        env=_env(),
        watermark=f"commit-{marker}",
        materialization=True,
    )


def test_news_and_learning_profiles_are_reference_gold_contributors() -> None:
    values = {
        "profile_id": "news-wikidata-reference",
        "domain": "NEWS",
        "source_product_id": "wikidata-json-dump",
        "capture_manifest": ObjectRefPayload.from_mapping(
            _object_mapping("s3://bucket/captures/news.json", "f")
        ),
        "env": _env(),
        "watermark": "wikidata-news-2026-10",
    }
    profile = ResearchSourceProfile(
        **values,
        identity_spine=True,
        gold_contribution=True,
    )

    assert profile.materialization is False
    assert profile.gold_contribution is True
    with pytest.raises(ValueError, match="identity and Gold contribution"):
        ResearchSourceProfile(**values)
    with pytest.raises(ValueError, match="identity and Gold contribution"):
        ResearchSourceProfile(
            **values,
            identity_spine=True,
            gold_contribution=True,
            materialization=True,
        )
    with pytest.raises(ValueError, match="not registered"):
        ResearchSourceProfile(
            **{
                **values,
                "source_product_id": "openfootball-git",
            },
            identity_spine=True,
            gold_contribution=True,
        )


def test_music_profiles_feed_two_reference_only_golden_source_states() -> None:
    profiles = tuple(
        ResearchSourceProfile(
            profile_id=f"music-{source_product_id}",
            domain="MUSIC",
            source_product_id=source_product_id,
            capture_manifest=ObjectRefPayload.from_mapping(
                _object_mapping(
                    f"s3://bucket/captures/{source_product_id}.json",
                    marker,
                )
            ),
            env=_env(),
            watermark=f"music-{marker}",
            identity_spine=True,
            materialization=False,
            gold_contribution=True,
        )
        for source_product_id, marker in (
            ("musicbrainz-core-dump", "a"),
            ("wikidata-music-reference", "b"),
        )
    )
    fixtures = (
        load_reference_fixture(
            Path("tests/fixtures/research/musicbrainz-music-reference.v1.json")
        ),
        load_reference_fixture(
            Path("tests/fixtures/research/wikidata-music-reference.v1.json")
        ),
    )
    assert tuple(item.source_product_id for item in fixtures) == tuple(
        profile.source_product_id for profile in profiles
    )
    assert any(
        record.payload.get("entityType") == "MUSIC_LABEL"
        for record in fixtures[0].records
    )
    assert any(
        record.payload.get("entityTypeHint") == "MUSIC_LABEL"
        and "P966" in record.payload.get("claims", {})
        for record in fixtures[1].records
    )
    completed = tuple(
        ResearchSourceResult(
            profile_id=profile.profile_id,
            domain=profile.domain,
            source_product_id=profile.source_product_id,
            watermark=profile.watermark,
            acquired_at="2026-10-17T00:00:00Z",
            capture_manifest=profile.capture_manifest,
            captures=(),
            silvers=(
                SilverResult(
                    batch_id="sha256:" + (marker * 64),
                    run_id="sha256:" + (marker * 64),
                    commit_key="sha256:" + (marker.upper() * 64),
                    job_run_id=f"music-{marker}",
                    table_counts={"community_assertion": 1},
                    table_snapshot_ids={"community_assertion": 1},
                ),
            ),
        )
        for profile, marker in zip(profiles, ("a", "b"), strict=True)
    )
    baseline = _input()
    baseline = replace(
        baseline,
        source_run_states=(
            GoldenSourceRunState(
                source_product_id="wikidata-json-dump",
                run_ids=(baseline.baseline_source_run_ids[2],),
                run_status=GoldenSourceRunStatus.COMMITTED,
                freshness_status=GoldenSourceFreshnessStatus.PASS,
                watermark=baseline.source_watermarks["wikidata-json-dump"],
                identity_spine=False,
                materialization=True,
                gold_contribution=True,
            ),
        ),
    )
    golden = _golden_input(
        ResearchDomainInput(
            domain="MUSIC",
            sources=profiles,
            golden_build=baseline,
            next_source_index=2,
            completed_sources=completed,
        )
    )

    states = {state.source_product_id: state for state in golden.source_run_states}
    assert set(states) == {
        "musicbrainz-core-dump",
        "wikidata-json-dump",
        "wikidata-music-reference",
    }
    assert set(golden.supplemental_source_run_ids) == {
        "sha256:" + ("a" * 64),
        "sha256:" + ("b" * 64),
    }
    assert states["wikidata-json-dump"].run_ids == (
        baseline.baseline_source_run_ids[2],
    )
    assert not states["wikidata-json-dump"].identity_spine
    assert states["wikidata-json-dump"].materialization
    assert states["wikidata-json-dump"].gold_contribution
    assert states["wikidata-json-dump"].capture_reference is None
    assert states["wikidata-music-reference"].run_ids == ("sha256:" + ("b" * 64),)
    assert all(
        state.identity_spine
        and state.gold_contribution
        and not state.materialization
        and state.capture_reference is not None
        for source_product_id, state in states.items()
        if source_product_id in {"musicbrainz-core-dump", "wikidata-music-reference"}
    )
    with pytest.raises(ValueError, match="requires ordered MusicBrainz"):
        ResearchDomainInput(
            domain="MUSIC",
            sources=profiles[:1],
            golden_build=baseline,
        )


def _capture(profile: ResearchSourceProfile, marker: str) -> ResearchCaptureResult:
    return ResearchCaptureResult(
        profile_id=profile.profile_id,
        domain=profile.domain,
        source_product_id=profile.source_product_id,
        watermark=profile.watermark,
        acquired_at="2026-10-15T00:00:00Z",
        captures=(
            CaptureResult(
                kind="snapshot",
                batch_id="sha256:" + (marker * 64),
                record_set_id="sha256:" + ({"a": "c", "b": "d"}[marker] * 64),
                record_count=1,
                batch_manifest=ObjectRefPayload.from_mapping(
                    _object_mapping(
                        f"s3://bucket/batches/{marker}.json",
                        marker,
                    )
                ),
                record_set_manifest=ObjectRefPayload.from_mapping(
                    _object_mapping(
                        f"s3://bucket/record-sets/{marker}.json",
                        marker,
                    )
                ),
            ),
        ),
    )


def test_loads_only_manifest_bound_to_the_source_profile(tmp_path) -> None:
    payload = {
        "schemaVersion": RESEARCH_CAPTURE_MANIFEST_SCHEMA,
        "profileId": "sports-openfootball-git",
        "domain": "SPORTS",
        "sourceProductId": "openfootball-git",
        "watermark": "commit-a",
        "acquiredAt": "2026-10-15T00:00:00Z",
        "captures": [
            {
                "kind": "snapshot",
                "sourceProductId": "openfootball-git",
                "batchId": "sha256:" + ("a" * 64),
                "recordSetId": "sha256:" + ("b" * 64),
                "recordCount": 3,
                "batchManifest": _object_mapping(
                    "s3://bucket/research/capture/batch.json",
                    "c",
                ),
                "recordSetManifest": _object_mapping(
                    "s3://bucket/research/capture/records.json",
                    "d",
                ),
            }
        ],
    }
    body = json.dumps(payload, sort_keys=True).encode()
    path = tmp_path / "capture.json"
    path.write_bytes(body)
    reference = ObjectRefPayload(
        uri=path.as_uri(),
        format="OBJECT_FORMAT_JSON",
        media_type="application/json",
        checksum_algorithm="sha256",
        checksum_value=hashlib.sha256(body).hexdigest(),
        size_bytes=len(body),
    )

    result = load_pinned_research_capture(
        _profile("openfootball-git", "a", capture_manifest=reference)
    )

    assert result.source_product_id == "openfootball-git"
    assert result.captures[0].record_count == 3
    assert result.captures[0].kind == "snapshot"


def test_rejects_source_registered_for_another_domain() -> None:
    with pytest.raises(ValueError, match="not registered"):
        ResearchSourceProfile(
            profile_id="bad",
            domain="ANIMATION",
            source_product_id="openfootball-git",
            capture_manifest=ObjectRefPayload.from_mapping(
                _object_mapping("s3://bucket/capture.json", "a")
            ),
            env=_env(),
            watermark="commit-a",
        )


def test_domain_workflow_continues_as_new_and_commits_gold() -> None:
    asyncio.run(_domain_workflow_case())


async def _domain_workflow_case() -> None:
    calls: list[str] = []
    profiles = (
        _profile("openfootball-git", "a"),
        _profile("statsbomb-open-data", "b"),
    )

    @activity.defn(name="LoadPinnedResearchCapture")
    async def load(profile: ResearchSourceProfile) -> ResearchCaptureResult:
        calls.append(f"capture:{profile.source_product_id}")
        marker = "a" if profile.source_product_id == "openfootball-git" else "b"
        return _capture(profile, marker)

    @activity.defn(name="SubmitSourceSilverBundle")
    async def silver(
        captures: tuple[CaptureResult, ...],
        _env: PipelineEnv,
        _committed_at: str,
    ) -> list[SilverResult]:
        marker = captures[0].batch_id[-1]
        calls.append(f"silver:{marker}")
        return [
            SilverResult(
                batch_id=captures[0].batch_id,
                run_id="sha256:" + (marker * 64),
                commit_key="sha256:" + (marker.upper() * 64),
                job_run_id=f"job-{marker}",
                table_counts={"community_assertion": 1},
                table_snapshot_ids={"community_assertion": 10},
            )
        ]

    async with (
        await WorkflowEnvironment.start_time_skipping() as temporal,
        Worker(
            temporal.client,
            task_queue="research-domain-test",
            workflows=[
                ResearchDomainWorkflow,
                ResearchCaptureWorkflow,
                ResearchSilverWorkflow,
            ],
            activities=[load],
        ),
        Worker(
            temporal.client,
            task_queue=SILVER_TASK_QUEUE,
            activities=[silver],
        ),
        Worker(
            temporal.client,
            task_queue=GOLDEN_WORKFLOW_TASK_QUEUE,
            workflows=[StubGoldenBuildWorkflow],
        ),
    ):
        result = await temporal.client.execute_workflow(
            ResearchDomainWorkflow.run,
            ResearchDomainInput(
                domain="SPORTS",
                sources=profiles,
                golden_build=_input(),
                continue_as_new_after_sources=1,
            ),
            id="research-sports-test",
            task_queue="research-domain-test",
            execution_timeout=timedelta(minutes=5),
        )

    assert [item.source_product_id for item in result.sources] == [
        "openfootball-git",
        "statsbomb-open-data",
    ]
    assert result.golden.release.release_plan_id == _run(56)
    assert calls == [
        "capture:openfootball-git",
        "silver:a",
        "capture:statsbomb-open-data",
        "silver:b",
    ]
    assert result.golden.identity.source_run_ids == (
        "sha256:" + ("a" * 64),
        "sha256:" + ("b" * 64),
    )


def _golden_result(input_data: GoldenBuildInput) -> GoldenBuildResult:
    return GoldenBuildResult(
        build_id=input_data.build_id,
        planned_at="2026-10-15T00:00:00Z",
        committed_at="2026-10-15T00:00:00Z",
        build_spec=_ref("s3://bucket/build-spec.json"),
        snapshot=GoldenSnapshotResult(
            snapshot_set_id=_run(50),
            snapshot=_ref("s3://bucket/snapshot.json"),
            job_run_id="snapshot-job",
        ),
        identity=GoldenIdentityResult(
            run_id=_run(51),
            commit_key=_run(52),
            source_run_ids=input_data.supplemental_source_run_ids,
            table_counts={"community_entity_ledger": 2},
            table_snapshot_ids={"community_entity_ledger": 11},
            job_run_id="identity-job",
            source_policy_digest=_run(55),
        ),
        epoch=GoldenEpochResult(
            epoch_id=_run(53),
            epoch=_ref("s3://bucket/epoch.json"),
            committed_run_count=2,
            committed_run_digest=_run(54),
            data_snapshot_ids={"community_ingest_run": 12},
            job_run_id="epoch-job",
        ),
        release=GoldenReleaseResult(
            release_plan_id=_run(56),
            commit_key=_run(57),
            release_plan=_ref("s3://bucket/release-plan.json"),
            quality_report=_ref("s3://bucket/quality.json"),
            attribution_manifest=_ref("s3://bucket/attribution.json"),
            release_commit=_ref("s3://bucket/release.json"),
            table_counts={"community_gold_entity": 2},
            table_snapshot_ids={"community_gold_entity": 13},
            job_run_id="release-job",
        ),
    )


class _ScheduleHandle:
    def __init__(self) -> None:
        self.updated = False

    async def update(self, updater: Any) -> None:
        update = await updater(None)
        assert update.schedule.state.paused is False
        assert "publication remains disabled" in update.schedule.state.note
        self.updated = True


class _ExistingScheduleClient:
    def __init__(self) -> None:
        self.handle = _ScheduleHandle()

    def get_schedule_handle(self, _schedule_id: str) -> _ScheduleHandle:
        return self.handle

    async def create_schedule(
        self,
        _schedule_id: str,
        _schedule: Any,
    ) -> None:
        raise ScheduleAlreadyRunningError()


def test_research_schedule_is_unpaused_and_updated_idempotently() -> None:
    client = _ExistingScheduleClient()
    schedule_id = "research-openfootball-daily-v1"
    _domain, _source, spec = _SCHEDULE_COORDINATES[schedule_id]
    asyncio.run(
        _upsert_schedule(
            cast(Client, client),
            schedule_id=schedule_id,
            spec=spec,
            input_data=ResearchDomainInput(
                domain="SPORTS",
                sources=(_profile("openfootball-git", "a"),),
                golden_build=_input(),
            ),
            task_queue="research-test",
        )
    )
    assert client.handle.updated is True
    assert len(_SCHEDULE_COORDINATES) == 8


def test_research_domain_profile_schema_includes_music_schedule() -> None:
    schema = json.loads(
        Path("contracts/temporal/research-domain-profiles.v1.schema.json").read_text(
            encoding="utf-8"
        )
    )

    Draft202012Validator.check_schema(schema)
    schedules = schema["properties"]["schedules"]
    assert "research-music-monthly-v1" in schedules["required"]
    assert schedules["properties"]["research-music-monthly-v1"] == {
        "$ref": "#/$defs/music"
    }
    assert schema["properties"]["publicationGates"]["properties"][
        "musicPublicationAllowed"
    ] == {"const": False}


def test_schedule_config_rejects_any_publication_or_cutover_gate(
    tmp_path,
) -> None:
    path = tmp_path / "profiles.json"
    path.write_text(
        json.dumps(
            {
                "schemaVersion": RESEARCH_DOMAIN_PROFILES_SCHEMA,
                "publicationGates": {
                    "sportsPublicationAllowed": True,
                    "animationPublicationAllowed": False,
                    "musicPublicationAllowed": False,
                    "openSearchAliasActivationAllowed": False,
                    "cutoverAllowed": False,
                    "liveCutoverClaimed": False,
                    "reverseDualWriteAllowed": False,
                    "referenceWorldMaterializationAllowed": False,
                    "referenceWorldPublicationAllowed": False,
                    "tenantCanonicalWriteAllowed": False,
                },
                "schedules": {},
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="publication and cutover gates"):
        _load_schedules(str(path))
