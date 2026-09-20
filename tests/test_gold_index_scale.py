from __future__ import annotations

import pytest

from video_media_catalog.gold_index_scale import (
    plan_gold_index_sizing,
    synthetic_gold_document,
    synthetic_gold_sizing_plan,
)


def test_synthetic_million_scale_plans_do_not_materialize_target_volume() -> None:
    one_million = synthetic_gold_sizing_plan(
        target_document_count=1_000_000,
        sample_size=4,
        bulk_partitions=2,
        bulk_workers=2,
    )
    five_million = synthetic_gold_sizing_plan(
        target_document_count=5_000_000,
        sample_size=4,
        bulk_partitions=2,
        bulk_workers=2,
    )

    assert one_million.sample_document_count == 4
    assert five_million.sample_document_count == 4
    assert one_million.recommended_primary_shards >= 1
    assert five_million.recommended_primary_shards >= 5
    assert (
        abs(
            five_million.estimated_bulk_duration_seconds
            - (one_million.estimated_bulk_duration_seconds * 5)
        )
        <= 5
    )


def test_sizing_planner_uses_action_and_byte_bounds() -> None:
    plan = plan_gold_index_sizing(
        [synthetic_gold_document(1), synthetic_gold_document(2)],
        target_document_count=10,
        bulk_partitions=1,
        bulk_workers=1,
        bulk_chunk_size=3,
        bulk_max_chunk_bytes=10_000,
        target_primary_shard_bytes=10_000_000,
        max_documents_per_primary_shard=100,
        documents_per_second_per_worker=2,
        bytes_per_second_per_worker=10_000_000,
    )

    assert plan.estimated_documents_per_bulk_request == 3
    assert plan.estimated_bulk_request_count == 4
    assert plan.estimated_bulk_duration_seconds == 5
    assert plan.average_bulk_action_bytes > plan.average_document_bytes
    assert plan.assumptions_digest.startswith("sha256:")


def test_sizing_planner_rejects_action_larger_than_bulk_limit() -> None:
    with pytest.raises(ValueError, match="exceeds bulk-max-chunk-bytes"):
        plan_gold_index_sizing(
            [synthetic_gold_document(1)],
            target_document_count=10,
            bulk_max_chunk_bytes=100,
        )
