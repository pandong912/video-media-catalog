from __future__ import annotations

import pytest

from video_media_catalog.gold import (
    GoldResolutionStatus,
    build_gold_field,
    build_gold_release_plan,
    research_context,
    research_policy,
)
from video_media_catalog.gold_tables import GOLD_DATA_COLUMNS


def _counts(**overrides) -> dict[str, int]:
    values = {table: 0 for table in GOLD_DATA_COLUMNS}
    values.update(overrides)
    return values


def test_gold_policy_and_plan_are_deterministic() -> None:
    policy = research_policy()
    plan = build_gold_release_plan(
        policy_context=research_context(
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
    assert plan.policy_context.context_id == "research"
    assert plan.schema_version == "2.1"
    assert "ownerSubject" not in plan.model_dump(mode="json", by_alias=True)
    with pytest.raises(ValueError):
        type(plan).model_validate(
            {**plan.model_dump(mode="python"), "owner_subject": "legacy-owner"}
        )
    invalid = plan.model_dump(mode="python")
    invalid["policy_context"] = plan.policy_context.model_copy(
        update={"context_id": "public-sharealike"}
    )
    with pytest.raises(ValueError, match="single research"):
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


def test_gold_plan_binds_epoch_summary_without_historical_run_list() -> None:
    policy = research_policy()
    plan = build_gold_release_plan(
        policy_context=research_context(
            as_of="2026-09-19T00:00:00Z",
        ),
        silver_epoch_id="sha256:" + ("a" * 64),
        committed_run_count=50_000,
        committed_run_digest="sha256:" + ("b" * 64),
        silver_snapshot_ids={"community_field_assertion": 10},
        identity_snapshot_ids={"community_entity_membership": 11},
        rights_registry_digest="sha256:" + ("c" * 64),
        field_policy_digest=policy.digest,
        resolver_digest="sha256:" + ("d" * 64),
        image_digest="sha256:" + ("e" * 64),
        config_digest="sha256:" + ("f" * 64),
        expected_counts=_counts(),
        planned_at="2026-09-19T00:00:00Z",
    )
    assert plan.committed_run_ids == ()
    assert plan.committed_run_count == 50_000
    assert plan == type(plan).model_validate_json(plan.json_bytes())

    invalid = plan.model_dump(mode="python")
    invalid["committed_run_ids"] = ("sha256:" + ("9" * 64),)
    with pytest.raises(ValueError, match="either legacy run IDs"):
        type(plan).model_validate(invalid)
