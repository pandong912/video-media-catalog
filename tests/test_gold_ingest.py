from __future__ import annotations

import hashlib

import pytest

from video_media_catalog.attribution import (
    AttributionEntry,
    build_attribution_manifest,
)
from video_media_catalog.gold import (
    GoldResolutionStatus,
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
from video_media_catalog.gold_resolution import (
    ConflictDraft,
    FieldDraft,
    GoldResolutionDraft,
)
from video_media_catalog.gold_tables import GOLD_DATA_COLUMNS
from video_media_catalog.models import Checksum, ObjectRef

TIMESTAMP = "2026-09-19T00:00:00Z"


def _context():
    return research_context(
        as_of=TIMESTAMP,
    )


def _draft(*, conflict: bool = False) -> GoldResolutionDraft:
    conflicts = (
        (
            ConflictDraft(
                entity_key="sha256:" + ("1" * 64),
                predicate="title",
                qualifiers={"language": "en"},
                reason="MULTIPLE_ELIGIBLE_VALUES",
                assertion_ids=(
                    "sha256:" + ("2" * 64),
                    "sha256:" + ("3" * 64),
                ),
                candidate_values=["A", "B"],
                trace={"operator": "SINGLE"},
            ),
        )
        if conflict
        else ()
    )
    fields = (
        (
            FieldDraft(
                entity_key="sha256:" + ("1" * 64),
                predicate="title",
                value_type="CONFLICT",
                value=None,
                qualifiers={"language": "en"},
                status=GoldResolutionStatus.CONFLICTED,
                assertion_ids=(
                    "sha256:" + ("2" * 64),
                    "sha256:" + ("3" * 64),
                ),
                selected_assertion_id=None,
                trace={"operator": "SINGLE"},
            ),
        )
        if conflict
        else ()
    )
    return GoldResolutionDraft(
        entity_keys=("sha256:" + ("1" * 64),),
        source_node_counts={"sha256:" + ("1" * 64): 1},
        fields=fields,
        identifiers=(),
        relations=(),
        conflicts=conflicts,
        eligible_policy_counts={"tvmaze-api-cc-by-sa": 1},
        withheld_assertion_count=0,
        unresolved_identity_count=0,
    )


def _plan(draft: GoldResolutionDraft):
    policy = research_policy()
    return build_gold_release_plan(
        owner_subject="owner-123",
        policy_context=_context(),
        committed_run_ids=("sha256:" + ("a" * 64),),
        silver_snapshot_ids={"community_field_assertion": 10},
        identity_snapshot_ids={"community_entity_membership": 11},
        rights_registry_digest="sha256:" + ("b" * 64),
        field_policy_digest=policy.digest,
        resolver_digest="sha256:" + ("c" * 64),
        image_digest="sha256:" + ("d" * 64),
        config_digest="sha256:" + ("e" * 64),
        expected_counts=draft.expected_counts,
        planned_at=TIMESTAMP,
    )


def _ref(payload: bytes, media_type: str, name: str) -> ObjectRef:
    return ObjectRef(
        uri=f"file:///tmp/{name}",
        format="OBJECT_FORMAT_JSON",
        media_type=media_type,
        checksum=Checksum(value=hashlib.sha256(payload).hexdigest()),
        size_bytes=len(payload),
    )


def test_gold_quality_and_release_commit_are_bound() -> None:
    draft = _draft()
    plan = _plan(draft)
    policy = research_policy()
    quality = build_gold_quality_report(
        plan=plan,
        draft=draft,
        policy=policy,
        created_at=TIMESTAMP,
    )
    assert quality.status == GoldQualityStatus.PASS
    attribution = build_attribution_manifest(
        release_id=plan.release_plan_id,
        entries=(
            AttributionEntry(
                source_product_id="tvmaze-public-api",
                policy_id="tvmaze-api-cc-by-sa",
                attribution_text="TV data provided by TVmaze.",
                license_id="CC-BY-SA",
                source_url="https://www.tvmaze.com/",
                share_alike=True,
                claim_count=1,
            ),
        ),
        created_at=TIMESTAMP,
    )
    snapshots = {
        table: (100 if plan.expected_counts[table] else None)
        for table in GOLD_DATA_COLUMNS
    }
    commit = build_gold_release_commit(
        release_plan_id=plan.release_plan_id,
        owner_subject=plan.owner_subject,
        context_id=plan.policy_context.context_id,
        committed_at=TIMESTAMP,
        table_counts=plan.expected_counts,
        table_snapshot_ids=snapshots,
        quality_report=_ref(
            quality.json_bytes(),
            GOLD_QUALITY_MEDIA_TYPE,
            "quality.json",
        ),
        attribution_manifest=_ref(
            attribution.json_bytes(),
            ATTRIBUTION_MEDIA_TYPE,
            "attribution.json",
        ),
    )
    assert commit == type(commit).model_validate_json(commit.json_bytes())


def test_gold_quality_reports_policy_failure() -> None:
    draft = _draft(conflict=True)
    plan = _plan(draft)
    report = build_gold_quality_report(
        plan=plan,
        draft=draft,
        policy=research_policy(),
        created_at=TIMESTAMP,
    )
    assert report.status == GoldQualityStatus.FAILED
    assert report.violations


def test_gold_commit_requires_snapshot_for_nonempty_table() -> None:
    counts = {table: 0 for table in GOLD_DATA_COLUMNS}
    counts["community_gold_entity"] = 1
    payload = b"{}\n"
    with pytest.raises(ValueError, match="no snapshot"):
        build_gold_release_commit(
            release_plan_id="sha256:" + ("a" * 64),
            owner_subject="owner-123",
            context_id="research",
            committed_at=TIMESTAMP,
            table_counts=counts,
            table_snapshot_ids={table: None for table in GOLD_DATA_COLUMNS},
            quality_report=_ref(
                payload,
                GOLD_QUALITY_MEDIA_TYPE,
                "quality.json",
            ),
            attribution_manifest=_ref(
                payload,
                ATTRIBUTION_MEDIA_TYPE,
                "attribution.json",
            ),
        )
