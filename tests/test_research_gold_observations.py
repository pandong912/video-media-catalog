from __future__ import annotations

import json

from video_media_catalog.assertions import SourceNodeRef, ValueType
from video_media_catalog.community_sources import openfootball_rights_profile
from video_media_catalog.connector import (
    RecordOperation,
    build_connector_record_envelope,
)
from video_media_catalog.gold import (
    build_gold_release_plan,
    research_context,
    research_policy,
)
from video_media_catalog.gold_ingest import (
    ATTRIBUTION_MEDIA_TYPE,
    GOLD_QUALITY_MEDIA_TYPE,
    build_gold_release_commit,
)
from video_media_catalog.gold_quality import (
    GoldQualityStatus,
    build_gold_quality_report,
)
from video_media_catalog.gold_resolution import resolve_gold_draft
from video_media_catalog.identity_resolution import (
    build_identity_index,
    resolve_or_allocate_source_node,
)
from video_media_catalog.identity_v2 import EntityLevel
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.source_mapper import AssertionBuilder
from video_media_catalog.v2_contracts import digest_identity

TIMESTAMP = "2026-10-15T00:00:00Z"


def _raw() -> ObjectRef:
    return ObjectRef(
        uri="file:///tmp/research-gold-observation.json",
        format="OBJECT_FORMAT_JSON",
        media_type="application/json",
        checksum=Checksum(value="a" * 64),
        size_bytes=128,
    )


def _envelope(revision: str, citation: str):
    policy = openfootball_rights_profile()
    return build_connector_record_envelope(
        payload={"id": "eng.1", "score": revision},
        batch_id="sha256:" + ("b" * 64),
        source_system_id="openfootball",
        source_product_id="openfootball-git",
        source_namespace_id="openfootball-competition",
        source_record_id="eng.1",
        source_revision=revision,
        operation=RecordOperation.UPSERT,
        observed_at=f"2026-10-{revision.zfill(2)}T00:00:00Z",
        ingested_at=f"2026-10-{revision.zfill(2)}T00:00:00Z",
        payload_schema="openfootball-event",
        raw_object=_raw(),
        source_location=f"/revisions/{revision}",
        policy_id=policy.policy_id,
        policy_digest=policy.digest,
        citation_keys=(citation,),
    )


def _report_ref(media_type: str, name: str) -> ObjectRef:
    return ObjectRef(
        uri=f"file:///tmp/{name}.json",
        format="OBJECT_FORMAT_JSON",
        media_type=media_type,
        checksum=Checksum(value="c" * 64),
        size_bytes=256,
    )


def test_temporal_gold_keeps_history_and_selects_superseding_value() -> None:
    node = SourceNodeRef(
        namespace_id="openfootball-competition",
        source_id="eng.1",
        referent_kind="SPORTS_COMPETITION",
    )
    policy = openfootball_rights_profile()
    citation = "sha256:" + ("d" * 64)
    first_builder = AssertionBuilder(
        envelope=_envelope("1", citation),
        source_node=node,
        mapper_id="openfootball-source-mapper",
        mapper_version="1.0.0",
    )
    first_builder.add_field(
        "score",
        ValueType.JSON,
        {"home": 1, "away": 0},
        "/score",
    )
    first = first_builder.fields[0]
    second_builder = AssertionBuilder(
        envelope=_envelope("2", citation),
        source_node=node,
        mapper_id="openfootball-source-mapper",
        mapper_version="1.0.0",
    )
    second_builder.add_field(
        "score",
        ValueType.JSON,
        {"home": 1, "away": 1},
        "/score",
        supersedes_assertion_id=first.assertion_id,
    )
    second = second_builder.fields[0]
    resolved_identity = resolve_or_allocate_source_node(
        source_node=node,
        entity_level=EntityLevel.SPORTS_COMPETITION,
        entity_kind="SPORTS_COMPETITION",
        exact_candidate_entity_keys=(),
        assertion_keys=(first.assertion_id,),
        observed_at=TIMESTAMP,
        policy_id=policy.policy_id,
        policy_digest=policy.digest,
        decision_policy_version="research-identity-v1",
        decided_by="research-identity-resolver",
    )
    identity_index = build_identity_index(
        entities=resolved_identity.entities,
        memberships=resolved_identity.memberships,
        redirects=(),
        as_of="2026-10-31T00:00:00Z",
    )
    field_policy = research_policy()
    context = research_context(as_of="2026-10-31T00:00:00Z")
    draft = resolve_gold_draft(
        identity_index=identity_index,
        field_assertions=(first, second),
        identifier_assertions=(),
        relationship_assertions=(),
        rights_profiles=(policy,),
        policy_context=context,
        field_policy=field_policy,
    )
    plan_values = {
        "policy_context": context,
        "committed_run_ids": ("sha256:" + ("1" * 64),),
        "silver_snapshot_ids": {"community_field_assertion": 1},
        "identity_snapshot_ids": {"community_entity_membership": 1},
        "rights_registry_digest": digest_identity(
            [policy.model_dump(mode="json", by_alias=True)]
        ),
        "source_policy_digest": "sha256:" + ("2" * 64),
        "field_policy_digest": field_policy.digest,
        "resolver_digest": "sha256:" + ("3" * 64),
        "image_digest": "sha256:" + ("4" * 64),
        "config_digest": "sha256:" + ("5" * 64),
        "expected_counts": draft.expected_counts,
        "planned_at": TIMESTAMP,
        "domain_coverage": {"SPORTS": 1},
        "source_snapshot_digests": {"openfootball-git": "sha256:" + ("6" * 64)},
    }
    plan = build_gold_release_plan(**plan_values)
    replayed_plan = build_gold_release_plan(**plan_values)
    materialized = draft.materialize(plan, identity_index)
    score = draft.fields[0]

    assert plan == replayed_plan
    assert score.value == {"home": 1, "away": 1}
    assert len(draft.observations) == 2
    assert [item.is_winning for item in draft.observations] == [False, True]
    assert len(materialized["community_gold_observation"]) == 2
    assert (
        materialized["community_gold_observation"][1].supersedes_assertion_id
        == first.assertion_id
    )

    quality = build_gold_quality_report(
        plan=plan,
        draft=draft,
        policy=field_policy,
        created_at=TIMESTAMP,
    )
    assert quality.status == GoldQualityStatus.PASS
    assert quality.domain_coverage == {"SPORTS": 1}
    snapshots = {
        table: index + 1 if count else None
        for index, (table, count) in enumerate(sorted(draft.expected_counts.items()))
    }
    commit_values = {
        "release_plan_id": plan.release_plan_id,
        "committed_at": TIMESTAMP,
        "table_counts": draft.expected_counts,
        "table_snapshot_ids": snapshots,
        "quality_report": _report_ref(GOLD_QUALITY_MEDIA_TYPE, "quality"),
        "attribution_manifest": _report_ref(
            ATTRIBUTION_MEDIA_TYPE,
            "attribution",
        ),
        "source_policy_digest": plan.source_policy_digest,
    }
    first_commit = build_gold_release_commit(**commit_values)
    second_commit = build_gold_release_commit(**commit_values)

    assert first_commit == second_commit
    assert first_commit.commit_key.startswith("sha256:")
    assert json.loads(materialized["community_gold_observation"][1].value_json) == {
        "away": 1,
        "home": 1,
    }
