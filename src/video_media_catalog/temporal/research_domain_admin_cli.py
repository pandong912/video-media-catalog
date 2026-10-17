"""Idempotent schedule management for global-reference research domains."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import timedelta
from typing import Any
from urllib.parse import urlsplit

from temporalio.client import (
    Client,
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleAlreadyRunningError,
    ScheduleCalendarSpec,
    ScheduleOverlapPolicy,
    SchedulePolicy,
    ScheduleRange,
    ScheduleSpec,
    ScheduleState,
    ScheduleUpdate,
    ScheduleUpdateInput,
)
from temporalio.service import RPCError, RPCStatusCode

from video_media_catalog.temporal.admin_cli import _env_from_mapping
from video_media_catalog.temporal.golden_admin_cli import _input_from_mapping
from video_media_catalog.temporal.models import ObjectRefPayload
from video_media_catalog.temporal.names import (
    GOLDEN_WORKFLOW_TASK_QUEUE,
    SCHEDULE_MADB_WEEKLY,
    SCHEDULE_MUSIC_MONTHLY,
    SCHEDULE_OPENFOOTBALL_DAILY,
    SCHEDULE_STATSBOMB_WEEKLY,
    SCHEDULE_WIKIDATA_ANIMATION_MONTHLY,
    SCHEDULE_WIKIDATA_LEARNING_MONTHLY,
    SCHEDULE_WIKIDATA_NEWS_MONTHLY,
    SCHEDULE_WIKIDATA_SPORTS_MONTHLY,
    TEMPORAL_NAMESPACE,
    WORKFLOW_TASK_QUEUE,
)
from video_media_catalog.temporal.research_domain_models import (
    ResearchDomainInput,
    ResearchSourceProfile,
)
from video_media_catalog.temporal.workflows.research_domain import (
    ResearchDomainWorkflow,
)
from video_media_catalog.v2_contracts import require_sha256

RESEARCH_DOMAIN_PROFILES_SCHEMA = "video.media-catalog.research-domain-profiles.v1"
_REQUIRED_DISABLED_GATES = frozenset(
    {
        "sportsPublicationAllowed",
        "animationPublicationAllowed",
        "musicPublicationAllowed",
        "openSearchAliasActivationAllowed",
        "cutoverAllowed",
        "liveCutoverClaimed",
        "reverseDualWriteAllowed",
        "referenceWorldMaterializationAllowed",
        "referenceWorldPublicationAllowed",
        "tenantCanonicalWriteAllowed",
    }
)
_CATALOG_ROOT = "s3://ai-video-platform-dev-media-catalog-209479308066"
_CAPTURE_PREFIX = f"{_CATALOG_ROOT}/landing/research/capture"
_WAREHOUSE = f"{_CATALOG_ROOT}/warehouse"
_EMR_RUNTIME_ROLE = (
    "arn:aws:iam::209479308066:role/ai-video-platform-dev-media-catalog-emr-runtime"
)

_SCHEDULE_COORDINATES = {
    SCHEDULE_WIKIDATA_SPORTS_MONTHLY: (
        "SPORTS",
        "wikidata-json-dump",
        ScheduleSpec(
            calendars=[
                ScheduleCalendarSpec(
                    day_of_month=(ScheduleRange(3),),
                    hour=(ScheduleRange(1),),
                    minute=(ScheduleRange(10),),
                )
            ]
        ),
    ),
    SCHEDULE_WIKIDATA_ANIMATION_MONTHLY: (
        "ANIMATION",
        "wikidata-json-dump",
        ScheduleSpec(
            calendars=[
                ScheduleCalendarSpec(
                    day_of_month=(ScheduleRange(3),),
                    hour=(ScheduleRange(3),),
                    minute=(ScheduleRange(10),),
                )
            ]
        ),
    ),
    SCHEDULE_WIKIDATA_NEWS_MONTHLY: (
        "NEWS",
        "wikidata-json-dump",
        ScheduleSpec(
            calendars=[
                ScheduleCalendarSpec(
                    day_of_month=(ScheduleRange(4),),
                    hour=(ScheduleRange(1),),
                    minute=(ScheduleRange(10),),
                )
            ]
        ),
    ),
    SCHEDULE_WIKIDATA_LEARNING_MONTHLY: (
        "LEARNING",
        "wikidata-json-dump",
        ScheduleSpec(
            calendars=[
                ScheduleCalendarSpec(
                    day_of_month=(ScheduleRange(4),),
                    hour=(ScheduleRange(3),),
                    minute=(ScheduleRange(10),),
                )
            ]
        ),
    ),
    SCHEDULE_MUSIC_MONTHLY: (
        "MUSIC",
        ("musicbrainz-core-dump", "wikidata-json-dump"),
        ScheduleSpec(
            calendars=[
                ScheduleCalendarSpec(
                    day_of_month=(ScheduleRange(5),),
                    hour=(ScheduleRange(1),),
                    minute=(ScheduleRange(10),),
                )
            ]
        ),
    ),
    SCHEDULE_OPENFOOTBALL_DAILY: (
        "SPORTS",
        "openfootball-git",
        ScheduleSpec(
            calendars=[
                ScheduleCalendarSpec(
                    hour=(ScheduleRange(2),),
                    minute=(ScheduleRange(15),),
                )
            ]
        ),
    ),
    SCHEDULE_STATSBOMB_WEEKLY: (
        "SPORTS",
        "statsbomb-open-data",
        ScheduleSpec(
            calendars=[
                ScheduleCalendarSpec(
                    day_of_week=(ScheduleRange(0),),
                    hour=(ScheduleRange(4),),
                    minute=(ScheduleRange(20),),
                )
            ]
        ),
    ),
    SCHEDULE_MADB_WEEKLY: (
        "ANIMATION",
        "madb-public-data",
        ScheduleSpec(
            calendars=[
                ScheduleCalendarSpec(
                    day_of_week=(ScheduleRange(3),),
                    hour=(ScheduleRange(5),),
                    minute=(ScheduleRange(25),),
                )
            ]
        ),
    ),
}


def _boolean(values: dict[str, Any], name: str, *, default: bool) -> bool:
    value = values.get(name, default)
    if not isinstance(value, bool):
        raise ValueError(f"{name} must be a boolean")
    return value


def _source_from_mapping(values: dict[str, Any]) -> ResearchSourceProfile:
    materialization = _boolean(values, "materialization", default=False)
    profile = ResearchSourceProfile(
        profile_id=str(values["profileId"]),
        domain=str(values["domain"]),  # type: ignore[arg-type]
        source_product_id=str(values["sourceProductId"]),
        capture_manifest=ObjectRefPayload.from_mapping(values["captureManifest"]),
        env=_env_from_mapping(values["env"]),
        watermark=str(values["watermark"]),
        identity_spine=_boolean(values, "identitySpine", default=False),
        materialization=materialization,
        gold_contribution=_boolean(
            values,
            "goldContribution",
            default=materialization,
        ),
        maximum_attempts=int(values.get("maximumAttempts", 3)),
        initial_retry_seconds=int(values.get("initialRetrySeconds", 30)),
        maximum_retry_seconds=int(values.get("maximumRetrySeconds", 600)),
        capture_timeout_minutes=int(values.get("captureTimeoutMinutes", 30)),
        silver_timeout_minutes=int(values.get("silverTimeoutMinutes", 720)),
        max_manifest_bytes=int(values.get("maxManifestBytes", 4 * 1024 * 1024)),
    )
    _validate_source_coordinates(profile)
    return profile


def _is_under(uri: str, prefix: str) -> bool:
    candidate = urlsplit(uri)
    boundary = urlsplit(prefix)
    return (
        candidate.scheme == "s3"
        and candidate.netloc == boundary.netloc
        and candidate.path.startswith(boundary.path.rstrip("/") + "/")
    )


def _validate_pinned_object(
    reference: ObjectRefPayload,
    *,
    prefixes: tuple[str, ...],
    label: str,
) -> None:
    if not any(_is_under(reference.uri, prefix) for prefix in prefixes):
        raise ValueError(f"{label} is outside its reviewed S3 prefixes")
    if not reference.etag or not reference.object_version or reference.size_bytes <= 0:
        raise ValueError(f"{label} requires size, ETag and ObjectVersion")
    require_sha256(reference.checksum_digest(), label=f"{label} checksum")


def _validate_source_coordinates(profile: ResearchSourceProfile) -> None:
    env = profile.env
    expected = {
        "destination_prefix": _CAPTURE_PREFIX,
        "aws_region": "us-east-1",
        "warehouse_uri": _WAREHOUSE,
        "record_staging_prefix": (f"{_WAREHOUSE}/research/control/record-shards"),
        "silver_checkpoint_prefix": (
            f"{_WAREHOUSE}/research/control/source-silver-checkpoints"
        ),
        "pipeline_summary_prefix": (
            f"{_CATALOG_ROOT}/landing/research/pipeline-summaries"
        ),
        "emr_application_name": (
            "ai-video-platform-dev-media-catalog-wikidata-reference"
        ),
        "emr_execution_role_arn": _EMR_RUNTIME_ROLE,
        "emr_log_uri": (f"{_CATALOG_ROOT}/raw/wikidata/emr-serverless-logs/"),
        "emr_entry_point": (
            "local:///opt/video-media-catalog/src/video_media_catalog/community_cli.py"
        ),
        "catalog_type": "glue",
        "catalog_name": "media",
        "glue_namespace": "video_media_catalog",
    }
    drift = [name for name, value in expected.items() if getattr(env, name) != value]
    if drift:
        raise ValueError(
            "research source environment drifted: " + ", ".join(sorted(drift))
        )
    if (
        env.s3_endpoint is not None
        or env.s3_path_style_access
        or not env.user_agent.startswith("video-media-catalog/")
    ):
        raise ValueError("research source environment uses an unsafe S3/client mode")
    if profile.maximum_attempts > 3:
        raise ValueError("research source retries exceed the production bound")
    _validate_pinned_object(
        profile.capture_manifest,
        prefixes=(_CAPTURE_PREFIX,),
        label=f"{profile.profile_id} capture manifest",
    )


def _validate_golden_coordinates(input_data: ResearchDomainInput) -> None:
    golden = input_data.golden_build
    env = golden.env
    expected = {
        "aws_region": "us-east-1",
        "warehouse_uri": _WAREHOUSE,
        "control_prefix": f"{_WAREHOUSE}/research/control",
        "gold_output_prefix": f"{_WAREHOUSE}/research/gold",
        "pipeline_summary_prefix": (
            f"{_WAREHOUSE}/research/control/golden-builds/summaries"
        ),
        "emr_application_name": ("ai-video-platform-dev-media-catalog-golden-release"),
        "gold_emr_application_name": (
            "ai-video-platform-dev-media-catalog-golden-release"
        ),
        "emr_execution_role_arn": _EMR_RUNTIME_ROLE,
        "emr_log_uri": (f"{_CATALOG_ROOT}/raw/wikidata/emr-serverless-logs/"),
        "gold_emr_log_uri": (f"{_CATALOG_ROOT}/raw/golden/emr-serverless-logs/"),
        "research_silver_entry_point": (
            "local:///opt/video-media-catalog/src/"
            "video_media_catalog/research_silver_cli.py"
        ),
        "gold_entry_point": (
            "local:///opt/video-media-catalog/src/video_media_catalog/gold_cli.py"
        ),
        "catalog_type": "glue",
        "catalog_name": "media",
        "glue_namespace": "video_media_catalog",
    }
    drift = [name for name, value in expected.items() if getattr(env, name) != value]
    if drift:
        raise ValueError(
            "research Golden environment drifted: " + ", ".join(sorted(drift))
        )
    _validate_pinned_object(
        golden.baseline_snapshot,
        prefixes=(f"{_WAREHOUSE}/research/control/golden-builds/baselines",),
        label="Golden baseline snapshot",
    )
    _validate_pinned_object(
        golden.tmdb_pipeline_summary,
        prefixes=(f"{_CATALOG_ROOT}/landing/research/pipeline-summaries/tmdb",),
        label="TMDB pipeline summary",
    )
    if golden.identity_reuse is not None:
        _validate_pinned_object(
            golden.identity_reuse,
            prefixes=(f"{_WAREHOUSE}/research/control",),
            label="Golden identity reuse",
        )
    for state in golden.source_run_states:
        if state.capture_reference is not None:
            _validate_pinned_object(
                state.capture_reference,
                prefixes=(_CAPTURE_PREFIX,),
                label=f"{state.source_product_id} capture reference",
            )


def _domain_input_from_mapping(values: dict[str, Any]) -> ResearchDomainInput:
    sources = tuple(_source_from_mapping(value) for value in values["sources"])
    input_data = ResearchDomainInput(
        domain=str(values["domain"]),  # type: ignore[arg-type]
        sources=sources,
        golden_build=_input_from_mapping(values["goldenBuild"]),
        golden_task_queue=str(values["goldenTaskQueue"]),
        continue_as_new_after_sources=int(values.get("continueAsNewAfterSources", 2)),
        failure_budget=int(values.get("failureBudget", 1)),
    )
    _validate_golden_coordinates(input_data)
    return input_data


def _bind_emr_image_digest(value: Any, digest: str) -> Any:
    if isinstance(value, dict):
        return {
            key: _bind_emr_image_digest(item, digest) for key, item in value.items()
        }
    if isinstance(value, list):
        return [_bind_emr_image_digest(item, digest) for item in value]
    if value == "${EMR_IMAGE_DIGEST}":
        if not digest:
            raise ValueError("EMR image digest binding is required")
        return digest
    return value


def _load_schedules(
    path: str,
    *,
    emr_image_digest: str = "",
) -> dict[str, ResearchDomainInput]:
    with open(path, encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError("research domain config must contain an object")
    if payload.get("schemaVersion") != RESEARCH_DOMAIN_PROFILES_SCHEMA:
        raise ValueError("research domain config schemaVersion is unsupported")
    gates = payload.get("publicationGates")
    if (
        not isinstance(gates, dict)
        or set(gates) != _REQUIRED_DISABLED_GATES
        or any(value is not False for value in gates.values())
    ):
        raise ValueError("all research publication and cutover gates must be false")
    if not isinstance(payload.get("schedules"), dict):
        raise ValueError("research domain config must contain a schedules object")
    if emr_image_digest:
        emr_image_digest = require_sha256(
            emr_image_digest,
            label="research EMR image digest",
        )
        payload = _bind_emr_image_digest(payload, emr_image_digest)
    schedules = {
        str(schedule_id): _domain_input_from_mapping(value)
        for schedule_id, value in payload["schedules"].items()
    }
    if set(schedules) != set(_SCHEDULE_COORDINATES):
        raise ValueError("research domain config must define all eight schedules")
    for schedule_id, input_data in schedules.items():
        expected_domain, raw_expected_sources, _spec = _SCHEDULE_COORDINATES[
            schedule_id
        ]
        expected_sources = (
            raw_expected_sources
            if isinstance(raw_expected_sources, tuple)
            else (raw_expected_sources,)
        )
        actual_sources = tuple(
            source.source_product_id for source in input_data.sources
        )
        if input_data.domain != expected_domain or actual_sources != expected_sources:
            raise ValueError(f"{schedule_id} source product mismatch")
        if expected_domain in {"NEWS", "LEARNING", "MUSIC"} and any(
            not source.identity_spine
            or source.materialization
            or not source.gold_contribution
            for source in input_data.sources
        ):
            raise ValueError(
                f"{schedule_id} must contribute reference-only Gold candidates"
            )
        if "${EMR_IMAGE_DIGEST}" in {
            *(source.env.image_digest for source in input_data.sources),
            input_data.golden_build.env.image_digest,
        }:
            raise ValueError("research profile contains an unbound EMR image digest")
        if emr_image_digest and (
            any(
                source.env.image_digest != emr_image_digest
                for source in input_data.sources
            )
            or input_data.golden_build.env.image_digest != emr_image_digest
        ):
            raise ValueError(f"{schedule_id} does not use the GitOps-pinned EMR image")
    return schedules


async def _upsert_schedule(
    client: Client,
    *,
    schedule_id: str,
    spec: ScheduleSpec,
    input_data: ResearchDomainInput,
    task_queue: str,
) -> None:
    schedule = Schedule(
        action=ScheduleActionStartWorkflow(
            ResearchDomainWorkflow.run,
            input_data,
            id=f"{schedule_id}-scheduled",
            task_queue=task_queue,
            execution_timeout=timedelta(days=7),
        ),
        spec=spec,
        policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP),
        state=ScheduleState(
            note=(
                f"{input_data.domain} research Capture/Silver/Gold; "
                "World domain publication remains disabled"
            ),
            paused=False,
        ),
    )
    handle = client.get_schedule_handle(schedule_id)
    try:
        await client.create_schedule(schedule_id, schedule)
        return
    except ScheduleAlreadyRunningError:
        pass
    except RPCError as exc:
        if exc.status != RPCStatusCode.ALREADY_EXISTS:
            raise

    async def updater(_input: ScheduleUpdateInput) -> ScheduleUpdate:
        return ScheduleUpdate(schedule=schedule)

    await handle.update(updater)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="video-media-catalog-research-temporal-admin")
    parser.add_argument(
        "--temporal-address",
        default=os.environ.get(
            "TEMPORAL_ADDRESS",
            "temporal-frontend.temporal.svc.cluster.local:7233",
        ),
    )
    parser.add_argument(
        "--temporal-namespace",
        default=os.environ.get("TEMPORAL_NAMESPACE", TEMPORAL_NAMESPACE),
    )
    parser.add_argument(
        "--task-queue",
        default=os.environ.get("TEMPORAL_TASK_QUEUE", WORKFLOW_TASK_QUEUE),
    )
    parser.add_argument(
        "--golden-task-queue",
        default=os.environ.get(
            "TEMPORAL_GOLDEN_TASK_QUEUE",
            GOLDEN_WORKFLOW_TASK_QUEUE,
        ),
    )
    parser.add_argument(
        "--emr-image-digest",
        default=os.environ.get("MEDIA_CATALOG_EMR_IMAGE_DIGEST", ""),
    )
    parser.add_argument(
        "--profiles-file",
        default=os.environ.get(
            "RESEARCH_DOMAIN_PROFILES_FILE",
            "/config/research-domain-profiles.json",
        ),
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser(
        "ensure-runtime",
        help="Create or update all production research schedules",
    )
    commands.add_parser(
        "validate-profiles",
        help="Validate immutable profile coordinates without contacting Temporal",
    )
    return parser


async def ensure_runtime(parsed: argparse.Namespace) -> dict[str, Any]:
    schedules = _load_schedules(
        parsed.profiles_file,
        emr_image_digest=parsed.emr_image_digest,
    )
    if any(
        input_data.golden_task_queue != parsed.golden_task_queue
        for input_data in schedules.values()
    ):
        raise ValueError("research profile Golden task queue does not match runtime")
    client = await Client.connect(
        parsed.temporal_address,
        namespace=parsed.temporal_namespace,
    )
    for schedule_id in sorted(schedules):
        _domain, _source, spec = _SCHEDULE_COORDINATES[schedule_id]
        await _upsert_schedule(
            client,
            schedule_id=schedule_id,
            spec=spec,
            input_data=schedules[schedule_id],
            task_queue=parsed.task_queue,
        )
    return {"schedules": sorted(schedules), "paused": False}


def main(argv: list[str] | None = None) -> int:
    parsed = build_parser().parse_args(argv)
    if parsed.command == "ensure-runtime":
        result = asyncio.run(ensure_runtime(parsed))
    elif parsed.command == "validate-profiles":
        schedules = _load_schedules(
            parsed.profiles_file,
            emr_image_digest=parsed.emr_image_digest,
        )
        if any(
            input_data.golden_task_queue != parsed.golden_task_queue
            for input_data in schedules.values()
        ):
            raise ValueError(
                "research profile Golden task queue does not match runtime"
            )
        result = {
            "schemaVersion": RESEARCH_DOMAIN_PROFILES_SCHEMA,
            "schedules": sorted(schedules),
            "publicationGates": "DISABLED",
        }
    else:
        raise SystemExit(f"unsupported command: {parsed.command}")
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
