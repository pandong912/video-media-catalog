from __future__ import annotations

import pytest

from video_media_catalog.asset_matching import (
    AssetMatchConfig,
    AssetMatchRequest,
    CatalogMatchRecord,
    MatchConfidenceTier,
    MatchContentType,
    MatchDisposition,
    MatchExternalIdentifier,
    catalog_match_record_from_gold,
    confirmed_reference_link_proposal,
    generate_asset_match_manifest,
    normalize_match_title,
)
from video_media_catalog.gold_api_models import (
    GoldAttributes,
    GoldCatalogEntity,
    GoldExternalIdentifier,
    GoldOverflow,
    GoldTitle,
)

ASSET_VERSION_ID = "01994c64-70dd-7b70-8000-000000000001"
RELEASE_PLAN_ID = "sha256:" + ("a" * 64)


def _identifier(value: str) -> MatchExternalIdentifier:
    return MatchExternalIdentifier(namespace="imdb-title", value=value)


def _record(
    key: str,
    *,
    title: str,
    imdb: str | None = None,
    year: int = 2002,
    duration: int = 6000,
    content_type: MatchContentType = MatchContentType.MOVIE,
    season: int | None = None,
    episode: int | None = None,
) -> CatalogMatchRecord:
    return CatalogMatchRecord(
        entity_key="sha256:" + (key * 64),
        release_plan_id=RELEASE_PLAN_ID,
        content_type=content_type,
        titles=(title,),
        release_year=year,
        duration_seconds=duration,
        season_number=season,
        episode_number=episode,
        languages=("en",),
        external_identifiers=() if imdb is None else (_identifier(imdb),),
    )


def test_exact_identifier_outranks_title_only_and_requires_review() -> None:
    request = AssetMatchRequest(
        asset_version_id=ASSET_VERSION_ID,
        content_type=MatchContentType.MOVIE,
        title="Hero",
        release_year=2002,
        duration_seconds=6000,
        languages=("en",),
        external_identifiers=(_identifier("tt0299977"),),
    )
    exact = _record("b", title="英雄", imdb="tt0299977")
    title_only = _record("c", title="Hero")
    manifest = generate_asset_match_manifest(
        request=request,
        records=[title_only, exact],
        release_plan_id=RELEASE_PLAN_ID,
        concrete_index="community-gold-v2-20260919",
        generated_at="2026-09-19T04:00:00Z",
    )

    assert manifest.disposition == MatchDisposition.REVIEW_REQUIRED
    assert manifest.candidates[0].entity_key == exact.entity_key
    assert manifest.candidates[0].tier == MatchConfidenceTier.EXACT
    assert manifest.candidates[0].evidence.exact_identifier_matches == (
        "imdb-title:tt0299977",
    )
    assert manifest.request_digest == request.digest
    assert manifest.json_bytes().endswith(b"\n")

    proposal = confirmed_reference_link_proposal(
        manifest=manifest,
        candidate_key=manifest.candidates[0].candidate_key,
    )
    assert proposal.entity_key == exact.entity_key
    assert proposal.method == "CATALOG_MATCH_CONFIRMED"
    with pytest.raises(ValueError, match="not present"):
        confirmed_reference_link_proposal(
            manifest=manifest,
            candidate_key="sha256:" + ("f" * 64),
        )


def test_episode_number_conflict_penalizes_ambiguous_title() -> None:
    request = AssetMatchRequest(
        asset_version_id=ASSET_VERSION_ID,
        content_type=MatchContentType.TV_EPISODE,
        title="Pilot",
        season_number=1,
        episode_number=1,
    )
    correct = _record(
        "b",
        title="Pilot",
        content_type=MatchContentType.TV_EPISODE,
        season=1,
        episode=1,
    )
    wrong = _record(
        "c",
        title="Pilot",
        content_type=MatchContentType.TV_EPISODE,
        season=1,
        episode=2,
    )
    manifest = generate_asset_match_manifest(
        request=request,
        records=[wrong, correct],
        release_plan_id=RELEASE_PLAN_ID,
        concrete_index="community-gold-v2-20260919",
        generated_at="2026-09-19T04:00:00Z",
    )

    assert manifest.candidates[0].entity_key == correct.entity_key
    assert manifest.candidates[0].evidence.episode_match is True
    assert all(
        candidate.entity_key != wrong.entity_key for candidate in manifest.candidates
    )


def test_matcher_rejects_unbounded_or_cross_release_retrieval() -> None:
    request = AssetMatchRequest(
        asset_version_id=ASSET_VERSION_ID,
        title="Example",
    )
    config = AssetMatchConfig(max_retrieval_records=1)
    records = [
        _record("b", title="Example"),
        _record("c", title="Example"),
    ]
    with pytest.raises(ValueError, match="exceeds configured bound"):
        generate_asset_match_manifest(
            request=request,
            records=records,
            release_plan_id=RELEASE_PLAN_ID,
            concrete_index="community-gold-v2-20260919",
            generated_at="2026-09-19T04:00:00Z",
            config=config,
        )

    cross_release = records[0].model_copy(
        update={"release_plan_id": "sha256:" + ("d" * 64)}
    )
    with pytest.raises(ValueError, match="cross Gold release"):
        generate_asset_match_manifest(
            request=request,
            records=[cross_release],
            release_plan_id=RELEASE_PLAN_ID,
            concrete_index="community-gold-v2-20260919",
            generated_at="2026-09-19T04:00:00Z",
        )


def test_title_normalization_is_unicode_and_punctuation_stable() -> None:
    assert normalize_match_title("  英雄\uff1aHero\uff01 ") == "英雄 hero"
    assert normalize_match_title("\uff28\uff25\uff32\uff2f") == "hero"


def test_gold_api_entity_adapts_to_bounded_match_record() -> None:
    entity = GoldCatalogEntity(
        entity_key="sha256:" + ("b" * 64),
        entity_level="WORK",
        entity_kind="MOVIE",
        status="ACTIVE",
        release_plan_id=RELEASE_PLAN_ID,
        display_name="Hero",
        display_language="en",
        titles=[
            GoldTitle(value="Hero", language="en", title_role="PRIMARY"),
            GoldTitle(value="Hero", language="zh", title_role="ALIAS"),
        ],
        attributes=GoldAttributes(
            formats=[],
            languages=["English", "en"],
            statuses=[],
            premiered=["2002-12-19"],
            ended=[],
            runtime_minutes=["100"],
            average_runtime_minutes=[],
            genres=[],
        ),
        external_identifiers=[
            GoldExternalIdentifier(
                namespace="imdb-title",
                value="tt0299977",
                issuer="IMDb",
                referent_kind="MOVIE",
            )
        ],
        relation_summary=[],
        conflict_count=0,
        conflict_predicates=[],
        source_node_count=1,
        overflow=GoldOverflow(
            titles=0,
            external_identifiers=0,
            relation_types=0,
            formats=0,
            languages=0,
            statuses=0,
            premiered=0,
            ended=0,
            runtime_minutes=0,
            average_runtime_minutes=0,
            genres=0,
        ),
    )

    record = catalog_match_record_from_gold(entity)
    assert record.content_type == MatchContentType.MOVIE
    assert record.titles == ("Hero",)
    assert record.release_year == 2002
    assert record.duration_seconds == 6000
    assert record.languages == ("en", "zh")
