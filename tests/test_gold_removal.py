from __future__ import annotations

import pytest

from video_media_catalog.community_release import (
    PurgeTargetKind,
    RemovalAction,
    RemovalReceiptStatus,
    SourceRemovalImpact,
    SourceRemovalReceipt,
    build_restricted_purge_target,
    build_source_removal_plan,
)
from video_media_catalog.gold_removal import execute_source_removal
from video_media_catalog.gold_removal_cli import build_parser
from video_media_catalog.rights import build_rights_termination_fence
from video_media_catalog.tmdb import tmdb_rights_profile

TIMESTAMP = "2026-09-20T12:00:00Z"


def _fence():
    return build_rights_termination_fence(
        profile=tmdb_rights_profile(),
        source_product_id="tmdb-research",
        effective_at=TIMESTAMP,
        reason="Terms terminated",
        created_at=TIMESTAMP,
    )


def _impact() -> SourceRemovalImpact:
    return SourceRemovalImpact(
        assertion_counts={"field": 10, "identifier": 2},
        affected_entity_count=4,
        affected_release_plan_ids=("sha256:" + ("1" * 64),),
        affected_indexes=("media-catalog-research-old",),
    )


def _targets():
    return (
        build_restricted_purge_target(
            source_product_id="tmdb-research",
            kind=PurgeTargetKind.RAW,
            uri="s3://catalog-restricted/tmdb/raw/batch.json",
        ),
        build_restricted_purge_target(
            source_product_id="tmdb-research",
            kind=PurgeTargetKind.DERIVED,
            uri="s3://catalog-restricted/tmdb/derived/assertions.parquet",
        ),
    )


def test_removal_planner_defaults_to_dry_run_with_full_impact() -> None:
    plan = build_source_removal_plan(
        owner_subject="owner-123",
        rights_fence=_fence(),
        impact=_impact(),
        purge_targets=_targets(),
        planned_at=TIMESTAMP,
    )
    assert plan.dry_run
    assert set(plan.actions) == {
        RemovalAction.RIGHTS_FENCE,
        RemovalAction.RE_GOLD,
        RemovalAction.RE_INDEX,
        RemovalAction.PURGE_RESTRICTED,
    }
    assert plan.impact.affected_assertion_count == 12
    assert plan == type(plan).model_validate_json(plan.json_bytes())


def test_executable_removal_requires_confirmation_and_component_allowlist() -> None:
    with pytest.raises(ValueError, match="canonical"):
        build_restricted_purge_target(
            source_product_id="tmdb-research",
            kind=PurgeTargetKind.RAW,
            uri="s3://catalog-restricted/tmdb/%2e%2e/other.json",
        )
    with pytest.raises(ValueError, match="exact source product"):
        build_source_removal_plan(
            owner_subject="owner-123",
            rights_fence=_fence(),
            impact=_impact(),
            purge_targets=_targets(),
            dry_run=False,
            allowlist_prefixes=("s3://catalog-restricted/tmdb",),
            planned_at=TIMESTAMP,
        )
    with pytest.raises(ValueError, match="outside"):
        build_source_removal_plan(
            owner_subject="owner-123",
            rights_fence=_fence(),
            impact=_impact(),
            purge_targets=_targets(),
            dry_run=False,
            confirm_source_product_id="tmdb-research",
            allowlist_prefixes=("s3://catalog-restricted/tm",),
            planned_at=TIMESTAMP,
        )


def test_removal_execution_returns_immutable_receipt() -> None:
    plan = build_source_removal_plan(
        owner_subject="owner-123",
        rights_fence=_fence(),
        impact=_impact(),
        purge_targets=_targets(),
        dry_run=False,
        confirm_source_product_id="tmdb-research",
        allowlist_prefixes=("s3://catalog-restricted/tmdb",),
        planned_at=TIMESTAMP,
    )
    events = []
    receipt = execute_source_removal(
        plan,
        confirm_source_product_id="tmdb-research",
        install_rights_fence=lambda fence: events.append(("fence", fence.fence_id)),
        purge_target=lambda target: events.append(("purge", target.target_id)),
        rebuild_gold=lambda removal_plan: (
            events.append(("gold", removal_plan.plan_id)) or "sha256:" + ("a" * 64)
        ),
        rebuild_index=lambda removal_plan, release_id: (
            events.append(("index", removal_plan.plan_id, release_id)) or ("b" * 64)
        ),
        executed_at=TIMESTAMP,
    )
    assert receipt.status == RemovalReceiptStatus.COMPLETED
    assert receipt.rights_fence_installed
    assert len(receipt.purged_target_ids) == 2
    assert events[0][0] == "fence"
    assert events[-1][0] == "index"
    assert receipt == SourceRemovalReceipt.model_validate_json(receipt.json_bytes())


def test_removal_cli_is_dry_run_by_default() -> None:
    parsed = build_parser().parse_args(
        [
            "--rights-profile-json",
            "/tmp/profile.json",
            "--source-product-id",
            "tmdb-research",
            "--owner-subject",
            "owner-123",
            "--effective-at",
            TIMESTAMP,
            "--planned-at",
            TIMESTAMP,
            "--reason",
            "terminated",
        ]
    )
    assert not parsed.execute_plan
    assert parsed.confirm_source_product_id is None
