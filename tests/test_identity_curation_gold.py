from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from video_media_catalog.assertions import SourceNodeRef
from video_media_catalog.community_rows import (
    entity_ledger_row,
    entity_membership_row,
    identity_conflict_row,
)
from video_media_catalog.community_snapshot import SILVER_SNAPSHOT_MEDIA_TYPE
from video_media_catalog.community_spark import create_community_dataframes
from video_media_catalog.community_tables import DATA_TABLE_COLUMNS
from video_media_catalog.gold_spark_transform import _resolved_memberships
from video_media_catalog.identity_curation import (
    IDENTITY_CURATION_MANIFEST_MEDIA_TYPE,
    CurationEntityAllocation,
    CurationSplitAssignment,
    IdentityCurationAction,
    IdentityCurationOperation,
    PinnedSilverSnapshot,
    build_identity_curation_dataframes,
    build_identity_curation_manifest,
    build_identity_curation_manifest_ref,
)
from video_media_catalog.identity_v2 import (
    EntityLevel,
    allocate_entity,
    build_entity_membership,
    build_identity_conflict,
)
from video_media_catalog.models import Checksum, ObjectRef

BASE_RUN_ID = "sha256:" + ("a" * 64)
TIMESTAMP = "2026-09-20T00:00:00Z"
EARLIER = "2026-09-19T00:00:00Z"
UUID7_A = "01a081e8-6420-7000-8000-000000000202"
UUID7_B = "01a081e8-6420-7000-8000-000000000203"
UUID7_C = "01a081e8-6420-7000-8000-000000000204"


def _digest(character: str) -> str:
    return "sha256:" + (character * 64)


def _node(source_id: str) -> SourceNodeRef:
    return SourceNodeRef(
        namespace_id="tvmaze-show",
        source_id=source_id,
        referent_kind="SERIES",
    )


def _entity(allocation_id: str, created_at: str = EARLIER):
    return allocate_entity(
        allocation_id=allocation_id,
        entity_level=EntityLevel.SERIES,
        entity_kind="TV_SERIES",
        created_at=created_at,
    )


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("identity-curation-gold-e2e")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def _pinned() -> PinnedSilverSnapshot:
    return PinnedSilverSnapshot(
        object=ObjectRef(
            uri="file:///tmp/curation-gold-silver.json",
            format="OBJECT_FORMAT_JSON",
            media_type=SILVER_SNAPSHOT_MEDIA_TYPE,
            checksum=Checksum(value="b" * 64),
            size_bytes=100,
        ),
        snapshot_set_id=_digest("c"),
    )


def _manifest(operation: IdentityCurationOperation):
    return build_identity_curation_manifest(
        pinned_silver_snapshot=_pinned(),
        operations=(operation,),
        operator_subject="owner-1",
        reason="curated for Gold",
        operated_at=TIMESTAMP,
        config_digest=_digest("d"),
        image_digest=_digest("e"),
    )


def _manifest_ref(manifest):
    return build_identity_curation_manifest_ref(
        manifest,
        ObjectRef(
            uri="file:///tmp/curation-gold-manifest.json",
            format="OBJECT_FORMAT_JSON",
            media_type=IDENTITY_CURATION_MANIFEST_MEDIA_TYPE,
            checksum=Checksum(value="f" * 64),
            size_bytes=100,
        ),
    )


def _visible(spark, *, entities, memberships, conflict):
    rows = {table: [] for table in DATA_TABLE_COLUMNS}
    rows["community_entity_ledger"] = [
        entity_ledger_row(BASE_RUN_ID, entity) for entity in entities
    ]
    rows["community_entity_membership"] = [
        entity_membership_row(BASE_RUN_ID, membership) for membership in memberships
    ]
    rows["community_identity_conflict"] = [identity_conflict_row(BASE_RUN_ID, conflict)]
    return create_community_dataframes(spark, rows)


def _gold_memberships(visible, curated):
    return {
        "community_entity_ledger": visible["community_entity_ledger"].unionByName(
            curated["community_entity_ledger"]
        ),
        "community_entity_membership": visible[
            "community_entity_membership"
        ].unionByName(curated["community_entity_membership"]),
        "community_entity_redirect": visible["community_entity_redirect"].unionByName(
            curated["community_entity_redirect"]
        ),
    }


@pytest.mark.spark
def test_merge_redirects_are_applied_by_gold(
    spark: SparkSession,
) -> None:
    survivor = _entity(UUID7_A, "2026-09-18T00:00:00Z")
    retired = _entity(UUID7_B)
    existing_node = _node("existing")
    conflict_node = _node("unresolved")
    membership = build_entity_membership(
        source_node=existing_node,
        entity_key=retired.entity_key,
        decision_id=_digest("1"),
        valid_from=EARLIER,
    )
    conflict = build_identity_conflict(
        materialization_id=_digest("2"),
        source_node=conflict_node,
        candidate_entity_keys=(survivor.entity_key, retired.entity_key),
        assertion_keys=(_digest("3"),),
        reason="MULTIPLE_EXACT_IDENTIFIER_CANDIDATES",
        observed_at=EARLIER,
        policy_id="internal-key-continuity",
        policy_digest=_digest("4"),
    )
    operation = IdentityCurationOperation(
        action=IdentityCurationAction.MERGE,
        conflict_key=conflict.conflict_key,
        source_node=conflict.source_node,
        assertion_keys=conflict.assertion_keys,
        entity_keys=conflict.candidate_entity_keys,
        expected_survivor_entity_key=survivor.entity_key,
    )
    manifest = _manifest(operation)
    visible = _visible(
        spark,
        entities=(survivor, retired),
        memberships=(membership,),
        conflict=conflict,
    )
    _, curated = build_identity_curation_dataframes(
        spark,
        visible_silver=visible,
        manifest=manifest,
        manifest_ref=_manifest_ref(manifest),
    )
    resolved = _resolved_memberships(
        silver=_gold_memberships(visible, curated),
        as_of=TIMESTAMP,
        max_redirect_hops=4,
    )
    try:
        mapping = {
            row["source_id"]: row["resolved_entity_key"] for row in resolved.collect()
        }
        assert mapping == {
            "existing": survivor.entity_key,
            "unresolved": survivor.entity_key,
        }
        assert curated["community_entity_merge_event"].count() == 1
        assert curated["community_entity_redirect"].count() == 1
    finally:
        resolved.unpersist()
        for frame in curated.values():
            frame.unpersist()


@pytest.mark.spark
def test_explicit_redirect_is_applied_by_gold(
    spark: SparkSession,
) -> None:
    source = _entity(UUID7_A)
    target = _entity(UUID7_B)
    existing_node = _node("redirected")
    conflict_node = _node("reviewed")
    membership = build_entity_membership(
        source_node=existing_node,
        entity_key=source.entity_key,
        decision_id=_digest("a"),
        valid_from=EARLIER,
    )
    conflict = build_identity_conflict(
        materialization_id=_digest("b"),
        source_node=conflict_node,
        candidate_entity_keys=(source.entity_key, target.entity_key),
        assertion_keys=(_digest("c"),),
        reason="ENTITY_REDIRECT_REQUIRED",
        observed_at=EARLIER,
        policy_id="internal-key-continuity",
        policy_digest=_digest("d"),
    )
    operation = IdentityCurationOperation(
        action=IdentityCurationAction.REDIRECT,
        conflict_key=conflict.conflict_key,
        source_node=conflict.source_node,
        assertion_keys=conflict.assertion_keys,
        source_entity_key=source.entity_key,
        target_entity_key=target.entity_key,
    )
    manifest = _manifest(operation)
    visible = _visible(
        spark,
        entities=(source, target),
        memberships=(membership,),
        conflict=conflict,
    )
    _, curated = build_identity_curation_dataframes(
        spark,
        visible_silver=visible,
        manifest=manifest,
        manifest_ref=_manifest_ref(manifest),
    )
    resolved = _resolved_memberships(
        silver=_gold_memberships(visible, curated),
        as_of=TIMESTAMP,
        max_redirect_hops=4,
    )
    try:
        mapping = {
            row["source_id"]: row["resolved_entity_key"] for row in resolved.collect()
        }
        assert mapping == {
            "redirected": target.entity_key,
            "reviewed": target.entity_key,
        }
        assert curated["community_entity_redirect"].count() == 1
        assert curated["community_entity_merge_event"].count() == 0
    finally:
        resolved.unpersist()
        for frame in curated.values():
            frame.unpersist()


@pytest.mark.spark
def test_split_membership_reassignment_is_applied_by_gold(
    spark: SparkSession,
) -> None:
    source = _entity(UUID7_A)
    new_target = _entity(UUID7_C, TIMESTAMP)
    first_node = _node("1")
    second_node = _node("2")
    conflict_node = _node("3")
    memberships = (
        build_entity_membership(
            source_node=first_node,
            entity_key=source.entity_key,
            decision_id=_digest("5"),
            valid_from=EARLIER,
        ),
        build_entity_membership(
            source_node=second_node,
            entity_key=source.entity_key,
            decision_id=_digest("6"),
            valid_from=EARLIER,
        ),
    )
    conflict = build_identity_conflict(
        materialization_id=_digest("7"),
        source_node=conflict_node,
        candidate_entity_keys=(source.entity_key,),
        assertion_keys=(_digest("8"),),
        reason="SOURCE_ENTITY_REQUIRES_SPLIT",
        observed_at=EARLIER,
        policy_id="internal-key-continuity",
        policy_digest=_digest("9"),
    )
    operation = IdentityCurationOperation(
        action=IdentityCurationAction.SPLIT,
        conflict_key=conflict.conflict_key,
        source_node=conflict.source_node,
        assertion_keys=conflict.assertion_keys,
        source_entity_key=source.entity_key,
        split_assignments=(
            CurationSplitAssignment(
                source_node=first_node,
                target_entity_key=source.entity_key,
            ),
            CurationSplitAssignment(
                source_node=second_node,
                target_entity_key=new_target.entity_key,
            ),
            CurationSplitAssignment(
                source_node=conflict_node,
                target_entity_key=new_target.entity_key,
            ),
        ),
        new_entities=(
            CurationEntityAllocation(
                entity_key=new_target.entity_key,
                allocation_id=UUID7_C,
                entity_level=EntityLevel.SERIES,
                entity_kind="TV_SERIES",
            ),
        ),
    )
    manifest = _manifest(operation)
    visible = _visible(
        spark,
        entities=(source,),
        memberships=memberships,
        conflict=conflict,
    )
    _, curated = build_identity_curation_dataframes(
        spark,
        visible_silver=visible,
        manifest=manifest,
        manifest_ref=_manifest_ref(manifest),
    )
    resolved = _resolved_memberships(
        silver=_gold_memberships(visible, curated),
        as_of="2026-09-20T00:00:01Z",
        max_redirect_hops=4,
    )
    try:
        mapping = {
            row["source_id"]: row["resolved_entity_key"] for row in resolved.collect()
        }
        assert mapping == {
            "1": source.entity_key,
            "2": new_target.entity_key,
            "3": new_target.entity_key,
        }
        assert curated["community_entity_split_event"].count() == 1
        assert curated["community_entity_membership"].count() == 5
    finally:
        resolved.unpersist()
        for frame in curated.values():
            frame.unpersist()
