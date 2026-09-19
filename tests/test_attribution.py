from __future__ import annotations

import pytest

from video_media_catalog.attribution import (
    AttributionEntry,
    build_attribution_manifest,
)


def _entry(source: str, claims: int) -> AttributionEntry:
    return AttributionEntry(
        source_product_id=source,
        policy_id=f"{source}-policy",
        attribution_text=f"Data from {source}.",
        license_id="CC-BY-SA",
        source_url=f"https://example.com/{source}",
        share_alike=True,
        claim_count=claims,
    )


def test_attribution_manifest_is_sorted_and_deterministic() -> None:
    values = {
        "release_id": "sha256:" + ("a" * 64),
        "entries": (_entry("tvmaze", 10), _entry("bangumi", 20)),
        "created_at": "2026-09-19T00:00:00Z",
    }
    first = build_attribution_manifest(**values)
    second = build_attribution_manifest(
        **{**values, "entries": tuple(reversed(values["entries"]))}
    )
    assert first == second
    assert [entry.source_product_id for entry in first.entries] == [
        "bangumi",
        "tvmaze",
    ]


def test_empty_attribution_entry_is_rejected() -> None:
    with pytest.raises(ValueError, match="claims or assets"):
        _entry("tvmaze", 0)
