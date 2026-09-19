from __future__ import annotations

import random

from video_media_catalog.reference_selection import (
    AssetDemandProfile,
    ReferenceCandidate,
    ReferenceQualityThresholds,
    ReferenceSelectionConfig,
    build_reference_selection_audit,
    reference_candidate,
    select_reference_candidates,
)
from video_media_catalog.wikidata_subset import RelationReference


def _candidate(
    qid: str,
    entity_type: str,
    *,
    sites: int = 0,
    completeness: int = 0,
    exact_ids: int = 0,
    demand: int = 0,
    parents: tuple[str, ...] = (),
    relations: tuple[tuple[str, str], ...] = (),
    complete_fields: bool = True,
) -> ReferenceCandidate:
    return ReferenceCandidate(
        qid=qid,
        entity_type=entity_type,
        sitelink_count=sites,
        completeness_score=completeness,
        exact_identifier_count=exact_ids,
        demand_score=demand,
        has_title=complete_fields,
        has_release=complete_fields,
        has_runtime=complete_fields,
        has_language=complete_fields,
        has_country=complete_fields,
        has_external_id=bool(exact_ids),
        parent_qids=parents,
        relations=tuple(
            RelationReference(property_id, target) for property_id, target in relations
        ),
    )


def _config() -> ReferenceSelectionConfig:
    return ReferenceSelectionConfig(
        content_quotas={
            "MOVIE": 2,
            "TV_SERIES": 1,
            "TV_SEASON": 2,
            "TV_EPISODE": 3,
        },
        agent_limits={"PERSON": 1, "ORGANIZATION": 1},
    )


def test_content_budget_excludes_agents_and_prefers_hierarchy_closure() -> None:
    candidates = [
        _candidate(
            "Q1",
            "MOVIE",
            completeness=100,
            exact_ids=1,
            relations=(("P57", "Q100"), ("P272", "Q200")),
        ),
        _candidate("Q2", "MOVIE", completeness=90),
        _candidate("Q3", "MOVIE", sites=100, completeness=10),
        _candidate("Q10", "TV_SERIES", completeness=100),
        _candidate("Q11", "TV_SEASON", parents=("Q10",), completeness=80),
        _candidate("Q12", "TV_SEASON", parents=("Q10",), completeness=70),
        _candidate("Q13", "TV_SEASON", sites=100, completeness=100),
        _candidate("Q21", "TV_EPISODE", parents=("Q11",), completeness=80),
        _candidate("Q22", "TV_EPISODE", parents=("Q12",), completeness=70),
        _candidate("Q23", "TV_EPISODE", parents=("Q10",), completeness=60),
        _candidate("Q24", "TV_EPISODE", sites=100, completeness=100),
        _candidate("Q100", "PERSON", completeness=50),
        _candidate("Q200", "ORGANIZATION", completeness=50),
    ]
    result = select_reference_candidates(candidates, _config())

    assert result.content_count == 8
    assert result.agent_count == 2
    assert "Q100" not in result.content_qids
    assert result.hierarchy_coverage["Q11"] == "COMPLETE"
    assert result.hierarchy_coverage["Q23"] == "COMPLETE"
    assert "Q13" not in result.content_qids
    assert "Q24" not in result.content_qids
    assert result.content_qids[:2] == ("Q1", "Q2")


def test_reference_selection_is_independent_of_input_order() -> None:
    candidates = [
        _candidate("Q1", "MOVIE", completeness=100),
        _candidate("Q2", "MOVIE", completeness=90),
        _candidate("Q10", "TV_SERIES", completeness=100),
        _candidate("Q11", "TV_SEASON", parents=("Q10",)),
        _candidate("Q12", "TV_SEASON", parents=("Q10",)),
        _candidate("Q21", "TV_EPISODE", parents=("Q11",)),
        _candidate("Q22", "TV_EPISODE", parents=("Q12",)),
        _candidate("Q23", "TV_EPISODE", parents=("Q10",)),
    ]
    expected = select_reference_candidates(candidates, _config())
    for seed in range(5):
        shuffled = list(candidates)
        random.Random(seed).shuffle(shuffled)
        assert select_reference_candidates(shuffled, _config()) == expected


def test_reference_candidate_uses_demand_and_exact_ids() -> None:
    payload = {
        "id": "Q1",
        "labels": {"en": {"language": "en", "value": "Example"}},
        "sitelinks": {"enwiki": {"title": "Example"}},
        "claims": {
            "P345": [
                {
                    "rank": "normal",
                    "mainsnak": {
                        "snaktype": "value",
                        "datavalue": {"value": "tt0000001"},
                    },
                }
            ],
            "P364": [
                {
                    "rank": "normal",
                    "mainsnak": {
                        "snaktype": "value",
                        "datavalue": {
                            "value": {
                                "id": "Q1860",
                                "entity-type": "item",
                            }
                        },
                    },
                }
            ],
        },
    }
    profile = AssetDemandProfile(
        sample_count=100,
        source_manifest_digest="sha256:" + ("a" * 64),
        content_type_weights={"MOVIE": 0.5},
        language_weights={"Q1860": 0.8},
        created_at="2026-09-19T00:00:00Z",
    )
    candidate = reference_candidate(payload, "MOVIE", profile)
    assert candidate.has_title
    assert candidate.has_external_id
    assert candidate.exact_identifier_count == 1
    assert candidate.demand_score == 900


def test_reference_audit_blocks_missing_required_fields() -> None:
    candidates = [
        _candidate("Q1", "MOVIE", complete_fields=False),
        _candidate("Q10", "TV_SERIES"),
        _candidate("Q11", "TV_SEASON", parents=("Q10",)),
        _candidate("Q21", "TV_EPISODE", parents=("Q11",)),
    ]
    config = ReferenceSelectionConfig(
        content_quotas={
            "MOVIE": 1,
            "TV_SERIES": 1,
            "TV_SEASON": 1,
            "TV_EPISODE": 1,
        },
        agent_limits={"PERSON": 0, "ORGANIZATION": 0},
    )
    result = select_reference_candidates(candidates, config)
    audit = build_reference_selection_audit(
        result=result,
        candidates=candidates,
        config=config,
        thresholds=ReferenceQualityThresholds(),
    )
    assert audit.status == "FAILED"
    assert any("TITLE_COVERAGE" in value for value in audit.violations)
