"""Dry-run-first CLI for planning source termination and removal."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from video_media_catalog.canonical import canonical_json
from video_media_catalog.community_release import (
    PurgeTargetKind,
    SourceRemovalImpact,
    build_restricted_purge_target,
    build_source_removal_plan,
)
from video_media_catalog.rights import (
    RightsProfile,
    build_rights_termination_fence,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="video-media-catalog-gold-removal",
        description="Plan a rights-fenced Gold source removal; defaults to dry-run.",
    )
    parser.add_argument("--rights-profile-json", type=Path, required=True)
    parser.add_argument("--source-product-id", required=True)
    parser.add_argument("--owner-subject", required=True)
    parser.add_argument("--effective-at", required=True)
    parser.add_argument("--planned-at", required=True)
    parser.add_argument("--reason", required=True)
    parser.add_argument(
        "--assertion-count",
        action="append",
        default=[],
        metavar="KIND=COUNT",
    )
    parser.add_argument("--affected-entity-count", type=int, default=0)
    parser.add_argument(
        "--affected-release-plan-id",
        action="append",
        default=[],
    )
    parser.add_argument("--affected-index", action="append", default=[])
    parser.add_argument("--raw-target", action="append", default=[])
    parser.add_argument("--derived-target", action="append", default=[])
    parser.add_argument(
        "--execute-plan",
        action="store_true",
        help="Authorize execution; this planner never deletes targets itself.",
    )
    parser.add_argument("--confirm-source-product-id")
    parser.add_argument("--allow-prefix", action="append", default=[])
    return parser


def _assertion_counts(values: list[str]) -> dict[str, int]:
    result: dict[str, int] = {}
    for item in values:
        kind, separator, raw_count = item.partition("=")
        kind = kind.strip()
        if not separator or not kind:
            raise ValueError("assertion count must use KIND=COUNT")
        try:
            count = int(raw_count)
        except ValueError as exc:
            raise ValueError("assertion count must be an integer") from exc
        if count < 0 or kind in result:
            raise ValueError("assertion counts must be unique and non-negative")
        result[kind] = count
    return result


def run(parsed: argparse.Namespace) -> dict[str, Any]:
    profile = RightsProfile.model_validate_json(parsed.rights_profile_json.read_bytes())
    fence = build_rights_termination_fence(
        profile=profile,
        source_product_id=parsed.source_product_id,
        effective_at=parsed.effective_at,
        reason=parsed.reason,
        created_at=parsed.planned_at,
    )
    impact = SourceRemovalImpact(
        assertion_counts=_assertion_counts(parsed.assertion_count),
        affected_entity_count=parsed.affected_entity_count,
        affected_release_plan_ids=tuple(parsed.affected_release_plan_id),
        affected_indexes=tuple(parsed.affected_index),
    )
    targets = tuple(
        [
            build_restricted_purge_target(
                source_product_id=parsed.source_product_id,
                kind=PurgeTargetKind.RAW,
                uri=uri,
            )
            for uri in parsed.raw_target
        ]
        + [
            build_restricted_purge_target(
                source_product_id=parsed.source_product_id,
                kind=PurgeTargetKind.DERIVED,
                uri=uri,
            )
            for uri in parsed.derived_target
        ]
    )
    plan = build_source_removal_plan(
        owner_subject=parsed.owner_subject,
        rights_fence=fence,
        impact=impact,
        purge_targets=targets,
        dry_run=not parsed.execute_plan,
        confirm_source_product_id=parsed.confirm_source_product_id,
        allowlist_prefixes=tuple(parsed.allow_prefix),
        planned_at=parsed.planned_at,
    )
    return plan.model_dump(mode="json", by_alias=True, exclude_none=True)


def main(argv: Sequence[str] | None = None) -> int:
    print(canonical_json(run(build_parser().parse_args(argv))))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
