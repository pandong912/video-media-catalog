from __future__ import annotations

import pytest

from video_media_catalog.community_ingest import (
    IngestRunKind,
    build_community_ingest_run,
)
from video_media_catalog.community_tables import DATA_TABLE_COLUMNS
from video_media_catalog.connector import ChangeSemantics, Completeness
from video_media_catalog.gold import (
    FieldPolicyRule,
    PredicateKind,
    ResolutionOperator,
    build_gold_release_plan,
    research_context,
    research_policy,
)
from video_media_catalog.gold_freshness import (
    ReleaseFreshnessPolicy,
    SourceFreshnessRequirement,
    build_release_freshness_matrix,
    research_release_freshness_policy,
    scope_release_freshness_policy,
)
from video_media_catalog.gold_quality import (
    GoldBuildMode,
    GoldQualityStatus,
    build_gold_quality_report_from_metrics,
)
from video_media_catalog.gold_resolution import GoldResolutionDraft
from video_media_catalog.gold_tables import GOLD_DATA_COLUMNS

TIMESTAMP = "2026-09-20T12:00:00Z"


def _source_run(
    *,
    source_product_id: str,
    acquired_at: str,
    semantics: ChangeSemantics,
    completeness: Completeness,
    watermark: str | None = None,
):
    counts = {table: 0 for table in DATA_TABLE_COLUMNS}
    return build_community_ingest_run(
        run_kind=IngestRunKind.SOURCE_ASSERTIONS,
        source_product_id=source_product_id,
        input_id="sha256:" + ("1" * 64),
        policy_id="example-policy",
        policy_digest="sha256:" + ("2" * 64),
        image_digest="sha256:" + ("3" * 64),
        config_digest="sha256:" + ("4" * 64),
        started_at=acquired_at,
        expected_counts=counts,
        input_manifest={
            "batchManifest": {
                "sourceProductId": source_product_id,
                "changeSemantics": semantics.value,
                "completeness": completeness.value,
                "coverageScopeDigest": "sha256:" + ("5" * 64),
                "watermarkAfter": watermark,
                "acquiredAt": acquired_at,
            }
        },
    )


def _gold_counts(**overrides: int) -> dict[str, int]:
    counts = {table: 0 for table in GOLD_DATA_COLUMNS}
    counts.update(overrides)
    return counts


def test_research_predicate_matrix_is_rights_first_and_fail_closed() -> None:
    policy = research_policy()
    title = policy.rule_for("title")
    assert title.operator == ResolutionOperator.SINGLE
    assert title.rights_first
    assert title.source_priority[0] == "imdb-non-commercial-datasets"
    assert policy.rule_for("genre").operator == ResolutionOperator.SET_UNION
    assert (
        policy.rule_for("cast_member", PredicateKind.RELATIONSHIP).operator
        == ResolutionOperator.SET_UNION
    )
    assert (
        policy.rule_for("part_of_series", PredicateKind.RELATIONSHIP).operator
        == ResolutionOperator.SINGLE
    )
    assert (
        policy.rule_for("imdb-title", PredicateKind.IDENTIFIER).operator
        == ResolutionOperator.SET_UNION
    )
    for kind in PredicateKind:
        assert (
            policy.rule_for("not_registered", kind).operator
            == ResolutionOperator.NEVER_RESOLVE
        )
    with pytest.raises(ValueError):
        FieldPolicyRule(
            predicate="unsafe",
            operator=ResolutionOperator.SINGLE,
            rights_first=False,
        )


def test_release_freshness_tracks_complete_delta_partial_and_slos() -> None:
    policy = ReleaseFreshnessPolicy(
        requirements=(
            SourceFreshnessRequirement(
                source_product_id="tmdb-research",
                required=True,
                slo_hours=36,
            ),
            SourceFreshnessRequirement(
                source_product_id="eidr-public-registry",
                required=False,
                feed_freshness_required=False,
                require_complete_baseline=True,
            ),
            SourceFreshnessRequirement(
                source_product_id="douban-id-only",
                required=False,
                feed_freshness_required=False,
                require_complete_baseline=False,
            ),
        )
    )
    complete = _source_run(
        source_product_id="tmdb-research",
        acquired_at="2026-09-19T00:00:00Z",
        semantics=ChangeSemantics.FULL_SNAPSHOT,
        completeness=Completeness.COMPLETE,
    )
    delta = _source_run(
        source_product_id="tmdb-research",
        acquired_at="2026-09-20T10:00:00Z",
        semantics=ChangeSemantics.DELTA,
        completeness=Completeness.COMPLETE,
        watermark="2026-09-20T09:00:00Z",
    )
    eidr_partial = _source_run(
        source_product_id="eidr-public-registry",
        acquired_at="2026-09-20T11:00:00Z",
        semantics=ChangeSemantics.FULL_SNAPSHOT,
        completeness=Completeness.PARTIAL,
    )
    matrix = build_release_freshness_matrix(
        ingest_runs=(complete, delta, eidr_partial),
        policy=policy,
        as_of=TIMESTAMP,
    )
    by_source = {item.source_product_id: item for item in matrix.sources}
    assert by_source["tmdb-research"].latest_complete is not None
    assert by_source["tmdb-research"].latest_delta is not None
    assert by_source["tmdb-research"].effective_age_hours == 3
    assert by_source["eidr-public-registry"].latest_complete is None
    assert by_source["eidr-public-registry"].latest_partial is not None
    assert not by_source["douban-id-only"].feed_freshness_required
    assert not matrix.blocking_violations

    defaults = {
        item.source_product_id: item
        for item in research_release_freshness_policy().requirements
    }
    assert defaults["tmdb-research"].slo_hours == 36
    assert defaults["tvmaze-public-api"].slo_hours == 36
    assert defaults["imdb-non-commercial-datasets"].slo_hours == 10 * 24
    assert defaults["wikidata-json-dump"].slo_hours == 45 * 24


def test_bounded_freshness_scope_only_blocks_selected_sources() -> None:
    scoped = scope_release_freshness_policy(
        research_release_freshness_policy(),
        selected_source_product_ids={"imdb-non-commercial-datasets"},
    )
    requirements = {item.source_product_id: item for item in scoped.requirements}
    assert requirements["imdb-non-commercial-datasets"].required
    assert not requirements["tmdb-research"].required
    assert requirements["tmdb-research"].feed_freshness_required


def test_quality_report_blocks_governance_failures_in_backfill_mode() -> None:
    policy = research_policy()
    counts = _gold_counts(
        community_gold_entity=1,
        community_gold_field=1,
        community_gold_conflict=1,
    )
    plan = build_gold_release_plan(
        policy_context=research_context(as_of=TIMESTAMP),
        committed_run_ids=("sha256:" + ("a" * 64),),
        silver_snapshot_ids={"community_field_assertion": 10},
        identity_snapshot_ids={"community_entity_membership": 11},
        rights_registry_digest="sha256:" + ("b" * 64),
        field_policy_digest=policy.digest,
        resolver_digest="sha256:" + ("c" * 64),
        image_digest="sha256:" + ("d" * 64),
        config_digest="sha256:" + ("e" * 64),
        expected_counts=counts,
        planned_at=TIMESTAMP,
    )
    report = build_gold_quality_report_from_metrics(
        plan=plan,
        policy=policy,
        table_counts=counts,
        conflict_count=1,
        field_count=1,
        resolution_count=1,
        withheld_assertion_count=0,
        unresolved_identity_count=1,
        entity_count=1,
        eligible_policy_counts={"example-policy": 2},
        attribution_counts={"example-policy": 1},
        orphan_episode_count=1,
        orphan_season_count=1,
        duplicate_external_id_count=1,
        build_mode=GoldBuildMode.CANDIDATE_BACKFILL,
        created_at=TIMESTAMP,
    )
    assert report.status == GoldQualityStatus.FAILED
    assert report.build_mode == GoldBuildMode.CANDIDATE_BACKFILL
    assert report.config_digest == plan.config_digest
    assert any(item.startswith("ORPHAN_EPISODE_COUNT") for item in report.violations)
    assert "ATTRIBUTION_COUNTS_MISMATCH" in report.violations


def test_draft_quality_uses_identity_lookup_endpoints() -> None:
    draft = GoldResolutionDraft(
        entity_keys=tuple(f"entity-{index}" for index in range(100)),
        source_node_counts={},
        fields=(),
        identifiers=(),
        relations=(),
        conflicts=(),
        eligible_policy_counts={},
        withheld_assertion_count=0,
        unresolved_identity_count=1,
        attribution_counts={},
    )
    with pytest.raises(ValueError, match="unresolved identity ratio"):
        draft.validate_quality(research_policy())
