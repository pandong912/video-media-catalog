from __future__ import annotations

from datetime import UTC, datetime

import pytest

from video_media_catalog.rights import (
    PolicyZone,
    RightsProfile,
    UsageAction,
    evaluate_rights,
)


def _profile(**overrides) -> RightsProfile:
    values = {
        "policy_id": "example-open",
        "policy_version": "1",
        "zone": PolicyZone.OPEN_ATTRIBUTED,
        "license_id": "CC-BY-4.0",
        "license_uri": "https://creativecommons.org/licenses/by/4.0/",
        "terms_url": "https://example.com/terms",
        "permissions": (
            UsageAction.STORE,
            UsageAction.TRANSFORM,
            UsageAction.DISPLAY,
            UsageAction.SEARCH,
        ),
        "audiences": ("public", "internal"),
        "territories": ("*",),
        "attribution_text": "Data from Example.",
    }
    values.update(overrides)
    return RightsProfile(**values)


def test_rights_profile_digest_is_deterministic() -> None:
    first = _profile(
        permissions=(UsageAction.SEARCH, UsageAction.STORE, UsageAction.DISPLAY),
        audiences=("internal", "public"),
    )
    second = _profile(
        permissions=(UsageAction.DISPLAY, UsageAction.SEARCH, UsageAction.STORE),
        audiences=("public", "internal"),
    )
    assert first.permissions == second.permissions
    assert first.audiences == second.audiences
    assert first.digest == second.digest
    assert first.json_bytes() == second.json_bytes()


def test_attributed_and_sharealike_profiles_fail_closed() -> None:
    with pytest.raises(ValueError, match="attribution"):
        _profile(attribution_text=None)
    with pytest.raises(ValueError, match="share_alike"):
        _profile(
            zone=PolicyZone.OPEN_SHAREALIKE,
            share_alike=False,
        )
    with pytest.raises(ValueError, match="max_cache_age_days"):
        _profile(
            zone=PolicyZone.FEDERATED_EPHEMERAL,
            attribution_text=None,
        )


def test_rights_evaluation_intersects_all_sources() -> None:
    open_profile = _profile()
    restricted = _profile(
        policy_id="display-only",
        zone=PolicyZone.RESEARCH_PRIVATE,
        permissions=(UsageAction.STORE, UsageAction.DISPLAY),
        audiences=("personal",),
        attribution_text=None,
        expires_at="2026-10-01T00:00:00Z",
        purge_on_termination=True,
    )
    evaluation = evaluate_rights(
        (open_profile, restricted),
        (UsageAction.DISPLAY, UsageAction.SEARCH),
        audience="personal",
        at=datetime(2026, 9, 20, tzinfo=UTC),
    )
    assert not evaluation.allowed
    assert evaluation.denied_by == ("display-only", "example-open")
    assert evaluation.earliest_expiry == "2026-10-01T00:00:00Z"


def test_expired_profile_denies_an_otherwise_permitted_action() -> None:
    profile = _profile(expires_at="2026-09-20T00:00:00Z")
    assert not profile.allows(
        UsageAction.DISPLAY,
        audience="public",
        at=datetime(2026, 9, 20, tzinfo=UTC),
    )
