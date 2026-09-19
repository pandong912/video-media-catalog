from __future__ import annotations

from video_media_catalog.asset_match_evaluation import (
    GoldenMatchCase,
    GoldenMatchSet,
    MatchEvaluationGates,
    evaluate_asset_match_manifests,
)
from video_media_catalog.asset_matching import (
    AssetMatchRequest,
    CatalogMatchRecord,
    MatchContentType,
    MatchExternalIdentifier,
    generate_asset_match_manifest,
)
from video_media_catalog.canonical import sha256_digest
from video_media_catalog.identity import stable_uuid7

RUN_ID = "01a081e8-6420-7000-8000-000000000202"
RELEASE_PLAN_ID = "sha256:" + ("a" * 64)


def _asset_id(index: int) -> str:
    return stable_uuid7(
        kind="asset-match-case",
        run_id=RUN_ID,
        identity={"index": index},
    )


def _case(index: int) -> tuple[GoldenMatchCase, object]:
    entity_key = sha256_digest(f"entity-{index}")
    if index < 100:
        content_type = MatchContentType.MOVIE
    elif index < 140:
        content_type = MatchContentType.TV_SERIES
    elif index < 180:
        content_type = MatchContentType.TV_SEASON
    else:
        content_type = MatchContentType.TV_EPISODE
    identifier = MatchExternalIdentifier(
        namespace="imdb-title",
        value=f"tt{index:07d}",
    )
    request = AssetMatchRequest(
        asset_version_id=_asset_id(index),
        content_type=content_type,
        title=f"Movie {index}",
        release_year=2000 + (index % 20),
        season_number=(1 if content_type == MatchContentType.TV_SEASON else None),
        episode_number=(index if content_type == MatchContentType.TV_EPISODE else None),
        external_identifiers=(identifier,),
    )
    record = CatalogMatchRecord(
        entity_key=entity_key,
        release_plan_id=RELEASE_PLAN_ID,
        content_type=content_type,
        titles=(f"Movie {index}",),
        release_year=2000 + (index % 20),
        season_number=(1 if content_type == MatchContentType.TV_SEASON else None),
        episode_number=(index if content_type == MatchContentType.TV_EPISODE else None),
        external_identifiers=(identifier,),
    )
    manifest = generate_asset_match_manifest(
        request=request,
        records=[record],
        release_plan_id=RELEASE_PLAN_ID,
        concrete_index="community-gold-v2-20260919",
        generated_at="2026-09-19T04:00:00Z",
    )
    return (
        GoldenMatchCase(
            request=request,
            expected_entity_key=entity_key,
            cohorts=("multilingual",) if index % 2 else ("ambiguous-title",),
        ),
        manifest,
    )


def test_three_hundred_case_golden_set_passes_default_gates() -> None:
    pairs = [_case(index) for index in range(300)]
    golden_set = GoldenMatchSet(cases=tuple(case for case, _ in pairs))
    report = evaluate_asset_match_manifests(
        golden_set=golden_set,
        manifests=[manifest for _, manifest in pairs],
        release_plan_id=RELEASE_PLAN_ID,
        generated_at="2026-09-19T05:00:00Z",
    )

    assert report.status == "PASS"
    assert report.overall.case_count == 300
    assert report.overall.top1_accuracy == 1
    assert report.overall.recall_at_5 == 1
    assert report.cohorts["ambiguous-title"].case_count == 150
    assert report.cohorts["type.movie"].case_count == 100
    assert report.cohorts["type.tv_episode"].case_count == 120
    assert report.json_bytes().endswith(b"\n")


def test_false_positive_gate_blocks_proposed_acceptance() -> None:
    case, manifest = _case(1)
    wrong_case = case.model_copy(
        update={"expected_entity_key": sha256_digest("different-entity")}
    )
    report = evaluate_asset_match_manifests(
        golden_set=GoldenMatchSet(cases=(wrong_case,)),
        manifests=[manifest],
        release_plan_id=RELEASE_PLAN_ID,
        generated_at="2026-09-19T05:00:00Z",
        gates=MatchEvaluationGates(
            minimum_cases=1,
            minimum_cases_by_type={
                "MOVIE": 0,
                "TV_SERIES": 0,
                "TV_SEASON": 0,
                "TV_EPISODE": 0,
            },
            minimum_top1_accuracy=0,
            minimum_recall_at_5=0,
            minimum_exact_id_top1_accuracy=0,
            maximum_false_positive_rate=0,
        ),
    )

    assert report.status == "FAILED"
    assert report.overall.false_positive_rate == 1
    assert report.violations == ("FALSE_POSITIVE_RATE:1>0",)
