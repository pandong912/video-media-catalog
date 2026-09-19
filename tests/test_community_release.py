from __future__ import annotations

import pytest

from video_media_catalog.community_release import (
    GoldTableSnapshot,
    ReleaseInput,
    ReleasePolicyContext,
    build_catalog_release_manifest,
)
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.rights import PolicyZone


def _quality_ref() -> ObjectRef:
    return ObjectRef(
        uri="s3://catalog-control/quality/report.json",
        format="OBJECT_FORMAT_JSON",
        media_type="application/json",
        checksum=Checksum(value="a" * 64),
        size_bytes=100,
        etag="etag",
        object_version="version",
    )


def test_policy_specific_release_is_deterministic() -> None:
    values = {
        "previous_release_id": None,
        "contract_id": "community-gold",
        "contract_version": "2.0",
        "contract_digest": "sha256:" + ("b" * 64),
        "policy_context": ReleasePolicyContext(
            context_id="public-cc0",
            audience="public",
            purpose="catalog",
            territories=("*",),
            as_of="2026-09-19T00:00:00Z",
            allowed_zones=(PolicyZone.OPEN_CC0,),
        ),
        "inputs": (
            ReleaseInput(
                source_product_id="wikidata-json-dump",
                batch_id="sha256:" + ("c" * 64),
                ingest_run_id="sha256:" + ("d" * 64),
                silver_snapshot_id=10,
            ),
        ),
        "identity_policy_digest": "sha256:" + ("e" * 64),
        "field_policy_digest": "sha256:" + ("f" * 64),
        "rights_registry_digest": "sha256:" + ("1" * 64),
        "tables": (
            GoldTableSnapshot(
                table_name="media.community_gold.catalog_entity",
                snapshot_id=20,
                committed_at="2026-09-19T00:00:00Z",
                operation="append",
                affected_record_count=5,
                total_record_count=100,
                schema_id=1,
            ),
        ),
        "quality_reports": (_quality_ref(),),
        "created_at": "2026-09-19T00:00:00Z",
    }
    first = build_catalog_release_manifest(**values)
    second = build_catalog_release_manifest(**values)
    assert first == second
    assert first.json_bytes() == second.json_bytes()
    assert first.release_id.startswith("sha256:")


def test_release_context_cannot_publish_quarantine() -> None:
    with pytest.raises(ValueError, match="quarantine"):
        ReleasePolicyContext(
            context_id="invalid",
            audience="public",
            purpose="catalog",
            as_of="2026-09-19T00:00:00Z",
            allowed_zones=(PolicyZone.QUARANTINE,),
        )


def test_gold_table_counts_distinguish_affected_and_total() -> None:
    with pytest.raises(ValueError, match="exceeds"):
        GoldTableSnapshot(
            table_name="media.community_gold.catalog_entity",
            snapshot_id=20,
            committed_at="2026-09-19T00:00:00Z",
            operation="append",
            affected_record_count=101,
            total_record_count=100,
            schema_id=1,
        )
