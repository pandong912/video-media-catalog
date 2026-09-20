from __future__ import annotations

import pytest

from video_media_catalog.gold import (
    GoldResolutionStatus,
    build_gold_field,
    build_gold_release_plan,
    personal_research_context,
    personal_research_policy,
)
from video_media_catalog.gold_tables import GOLD_DATA_COLUMNS


def _counts(**overrides) -> dict[str, int]:
    values = {table: 0 for table in GOLD_DATA_COLUMNS}
    values.update(overrides)
    return values


def test_gold_policy_and_plan_are_deterministic() -> None:
    policy = personal_research_policy()
    plan = build_gold_release_plan(
        owner_subject="owner-123",
        policy_context=personal_research_context(
            as_of="2026-09-19T00:00:00Z",
        ),
        committed_run_ids=("sha256:" + ("a" * 64),),
        silver_snapshot_ids={"community_field_assertion": 10},
        identity_snapshot_ids={"community_entity_membership": 11},
        rights_registry_digest="sha256:" + ("b" * 64),
        field_policy_digest=policy.digest,
        resolver_digest="sha256:" + ("c" * 64),
        image_digest="sha256:" + ("d" * 64),
        config_digest="sha256:" + ("e" * 64),
        expected_counts=_counts(community_gold_entity=1),
        planned_at="2026-09-19T00:00:00Z",
    )
    assert plan == type(plan).model_validate_json(plan.json_bytes())
    assert policy.rule_for("genre").operator.value == "SET_UNION"
    assert policy.rule_for("unknown").operator.value == "NEVER_RESOLVE"
    assert {action.value for action in policy.requested_actions} == {
        "display",
        "search",
        "store",
        "transform",
    }
    assert "research_private" in {
        zone.value for zone in plan.policy_context.allowed_zones
    }
    assert plan.policy_context.context_id == "personal-research"
    assert plan.owner_subject == "owner-123"
    invalid = plan.model_dump(mode="python")
    invalid["policy_context"] = plan.policy_context.model_copy(
        update={"context_id": "public-sharealike"}
    )
    with pytest.raises(ValueError, match="single personal-research"):
        type(plan).model_validate(invalid)


def test_gold_field_binds_scope_status_and_lineage() -> None:
    field = build_gold_field(
        release_plan_id="sha256:" + ("a" * 64),
        entity_key="sha256:" + ("b" * 64),
        predicate="title",
        value_type="STRING",
        value="Example",
        qualifiers={"language": "en", "titleRole": "PRIMARY"},
        resolution_status=GoldResolutionStatus.SELECTED,
        assertion_ids=("sha256:" + ("c" * 64),),
        selected_assertion_id="sha256:" + ("c" * 64),
        trace={"operator": "SINGLE"},
    )
    assert field.value_json == '"Example"'
    assert field.selected_assertion_id in field.assertion_ids
