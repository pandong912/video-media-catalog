from __future__ import annotations

import pytest

from video_media_catalog.gold import (
    build_gold_release_plan,
    research_context,
    research_policy,
)
from video_media_catalog.gold_quality import (
    GoldQualityStatus,
    build_gold_quality_report_from_metrics,
)
from video_media_catalog.gold_tables import GOLD_DATA_COLUMNS


def _counts(**overrides: int) -> dict[str, int]:
    values = {table: 0 for table in GOLD_DATA_COLUMNS}
    values.update(overrides)
    return values


def _plan(counts: dict[str, int]):
    policy = research_policy()
    return build_gold_release_plan(
        policy_context=research_context(as_of="2026-09-20T00:00:00Z"),
        committed_run_ids=("sha256:" + ("a" * 64),),
        silver_snapshot_ids={"community_field_assertion": 10},
        identity_snapshot_ids={"community_entity_membership": 11},
        rights_registry_digest="sha256:" + ("b" * 64),
        field_policy_digest=policy.digest,
        resolver_digest="sha256:" + ("c" * 64),
        image_digest="sha256:" + ("d" * 64),
        config_digest="sha256:" + ("e" * 64),
        expected_counts=counts,
        planned_at="2026-09-20T00:00:00Z",
    )


def test_unresolved_ratio_uses_identity_lookup_endpoints_not_entity_count() -> None:
    counts = _counts(
        community_gold_entity=134_048,
        community_gold_field=3_179_138,
        community_gold_identifier=329_526,
        community_gold_relation=765_327,
        community_gold_conflict=108_681,
    )
    policy = research_policy()
    report = build_gold_quality_report_from_metrics(
        plan=_plan(counts),
        policy=policy,
        table_counts=counts,
        conflict_count=108_681,
        field_count=3_179_138,
        withheld_assertion_count=2_304_456,
        unresolved_identity_count=208_118,
        entity_count=134_048,
        eligible_policy_counts={"wikidata-structured-data-cc0": 4_478_309},
        created_at="2026-09-20T00:00:00Z",
    )

    expected = 208_118 / (3_179_138 + 329_526 + 2 * 765_327 + 208_118)
    assert report.unresolved_identity_ratio == pytest.approx(expected)
    assert report.unresolved_identity_ratio < policy.max_unresolved_identity_ratio
    assert report.status == GoldQualityStatus.PASS
    assert report.violations == ()


def test_quality_report_rejects_forged_unresolved_ratio() -> None:
    counts = _counts(community_gold_entity=1, community_gold_field=1)
    report = build_gold_quality_report_from_metrics(
        plan=_plan(counts),
        policy=research_policy(),
        table_counts=counts,
        conflict_count=0,
        field_count=1,
        withheld_assertion_count=0,
        unresolved_identity_count=0,
        entity_count=1,
        eligible_policy_counts={"wikidata-structured-data-cc0": 1},
        created_at="2026-09-20T00:00:00Z",
    )
    payload = report.model_dump(mode="python")
    payload["unresolved_identity_ratio"] = 1.0

    with pytest.raises(ValueError, match="does not match Gold identity lookups"):
        type(report).model_validate(payload)
