from __future__ import annotations

import hashlib
import json

from video_media_catalog.canonical import canonical_json
from video_media_catalog.gold import (
    GoldAssertionLineage,
    GoldResolutionStatus,
    GoldRightsLineage,
    ResolutionOperator,
)
from video_media_catalog.gold_spark_transform import (
    _resolve_field_from_grouped_values,
    _resolve_field_group,
    _resolve_relation_from_grouped_objects,
    _resolve_relation_group,
    _resolve_single_field_values,
    _resolve_single_fields_partition,
    _resolve_single_relation_objects,
    _resolve_single_relations_partition,
)
from video_media_catalog.rights import PolicyZone


def _priority(*sources: str) -> str:
    return json.dumps(list(sources))


def _aid(label: str) -> str:
    return "sha256:" + hashlib.sha256(label.encode()).hexdigest()


def _lineage(assertion_id: str, source_product_id: str) -> str:
    return canonical_json(
        GoldAssertionLineage(
            assertion_id=assertion_id,
            source_product_id=source_product_id,
            source_name=source_product_id,
            source_record_id=f"rec-{assertion_id[-8:]}",
            source_path="$.title",
            observed_at="2026-10-01T00:00:00Z",
            rights=GoldRightsLineage(
                policy_id="policy",
                policy_zone=PolicyZone.RESEARCH_PRIVATE,
                license_id="license",
                attribution_text="attr",
                source_url="https://example.test",
            ),
        ).model_dump(mode="json", by_alias=True, exclude_none=True)
    )


def test_resolve_field_group_matches_grouped_helper_for_single() -> None:
    a1, a2, a3 = _aid("1"), _aid("2"), _aid("3")
    key = (
        "entity-a",
        "title",
        "{}",
        ResolutionOperator.SINGLE.value,
        _priority("src-a", "src-b"),
    )
    values = [
        ("STRING", '"Alpha"', a2, _lineage(a2, "src-b"), "src-b"),
        ("STRING", '"Alpha"', a1, _lineage(a1, "src-a"), "src-a"),
        ("STRING", '"Beta"', a3, _lineage(a3, "src-b"), "src-b"),
    ]
    from_group = _resolve_field_group((key, values))
    by_value: dict[tuple[str, str], list[tuple[str, str, str]]] = {}
    for value_type, value_json, assertion_id, lineage_json, source_product_id in values:
        by_value.setdefault((value_type, value_json), []).append(
            (assertion_id, lineage_json, source_product_id)
        )
    from_helper = _resolve_field_from_grouped_values(key, by_value)
    assert [(kind, draft.status, draft.value) for kind, draft in from_group] == [
        (kind, draft.status, draft.value) for kind, draft in from_helper
    ]
    assert from_helper[0][1].status == GoldResolutionStatus.SELECTED
    assert from_helper[0][1].value == "Alpha"
    assert from_helper[0][1].selected_assertion_id == a1


def test_resolve_field_set_union_emits_one_draft_per_value() -> None:
    a1, a2 = _aid("one"), _aid("two")
    key = (
        "entity-a",
        "aka",
        "{}",
        ResolutionOperator.SET_UNION.value,
        _priority("src-a"),
    )
    by_value = {
        ("STRING", '"One"'): [(a1, _lineage(a1, "src-a"), "src-a")],
        ("STRING", '"Two"'): [(a2, _lineage(a2, "src-a"), "src-a")],
    }
    drafts = _resolve_field_from_grouped_values(key, by_value)
    assert {draft.value for _, draft in drafts} == {"One", "Two"}
    assert all(draft.status == GoldResolutionStatus.SET for _, draft in drafts)


def test_single_field_value_groups_merge_duplicate_slots() -> None:
    a1, a2 = _aid("first"), _aid("second")
    key = (
        "entity-a",
        "title",
        "{}",
        ResolutionOperator.SINGLE.value,
        _priority("src-a"),
    )
    drafts = _resolve_single_field_values(
        (
            key,
            [
                (
                    "STRING",
                    '"Alpha"',
                    [(a2, _lineage(a2, "src-a"), "src-a")],
                ),
                (
                    "STRING",
                    '"Alpha"',
                    [(a1, _lineage(a1, "src-a"), "src-a")],
                ),
            ],
        )
    )

    assert len(drafts) == 1
    _, draft = drafts[0]
    assert draft.status == GoldResolutionStatus.SELECTED
    assert draft.value == "Alpha"
    assert draft.assertion_ids == tuple(sorted((a1, a2)))
    assert draft.selected_assertion_id == min(a1, a2)
    assert tuple(item.assertion_id for item in draft.lineage) == tuple(sorted((a1, a2)))


def test_streaming_field_partition_matches_grouped_resolution() -> None:
    a1, a2, a3 = _aid("stream-1"), _aid("stream-2"), _aid("stream-3")
    key = (
        "entity-a",
        "title",
        "{}",
        ResolutionOperator.SINGLE.value,
        _priority("src-a", "src-b"),
    )
    groups = [
        (
            key,
            (
                "STRING",
                '"Alpha"',
                [(a1, _lineage(a1, "src-a"), "src-a")],
            ),
        ),
        (
            key,
            (
                "STRING",
                '"Alpha"',
                [(a2, _lineage(a2, "src-b"), "src-b")],
            ),
        ),
        (
            key,
            (
                "STRING",
                '"Beta"',
                [(a3, _lineage(a3, "src-b"), "src-b")],
            ),
        ),
    ]

    streamed = list(_resolve_single_fields_partition(iter(groups)))
    grouped = _resolve_single_field_values((key, [value for _, value in groups]))
    assert streamed == grouped
    assert streamed[0][1].value == "Alpha"
    assert streamed[0][1].assertion_ids == tuple(sorted((a1, a2, a3)))


def test_resolve_relation_group_matches_grouped_helper() -> None:
    r1, r2 = _aid("r1"), _aid("r2")
    key = (
        "subject-a",
        "based_on",
        "{}",
        ResolutionOperator.SINGLE.value,
        _priority("src-a", "src-b"),
    )
    values = [
        ("object-a", r1, _lineage(r1, "src-a"), "src-a"),
        ("object-b", r2, _lineage(r2, "src-b"), "src-b"),
    ]
    from_group = _resolve_relation_group((key, values))
    by_object = {
        "object-a": [(r1, _lineage(r1, "src-a"), "src-a")],
        "object-b": [(r2, _lineage(r2, "src-b"), "src-b")],
    }
    from_helper = _resolve_relation_from_grouped_objects(key, by_object)
    assert [(kind, type(draft).__name__) for kind, draft in from_group] == [
        (kind, type(draft).__name__) for kind, draft in from_helper
    ]
    assert from_helper[0][0] == "relation"
    assert from_helper[0][1].object_entity_key == "object-a"


def test_streaming_relation_partition_merges_duplicate_objects() -> None:
    r1, r2 = _aid("stream-r1"), _aid("stream-r2")
    key = (
        "subject-a",
        "based_on",
        "{}",
        ResolutionOperator.SINGLE.value,
        _priority("src-a"),
    )
    groups = [
        (
            key,
            ("object-a", [(r1, _lineage(r1, "src-a"), "src-a")]),
        ),
        (
            key,
            ("object-a", [(r2, _lineage(r2, "src-a"), "src-a")]),
        ),
    ]

    streamed = list(_resolve_single_relations_partition(iter(groups)))
    grouped = _resolve_single_relation_objects((key, [value for _, value in groups]))
    assert streamed == grouped
    assert streamed[0][1].assertion_ids == tuple(sorted((r1, r2)))
