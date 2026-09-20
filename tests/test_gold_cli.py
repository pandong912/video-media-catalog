from __future__ import annotations

import pytest

from video_media_catalog.gold_cli import _snapshot_ref, build_parser


def _arguments() -> list[str]:
    return [
        "--silver-snapshot-uri",
        "file:///tmp/silver.json",
        "--silver-snapshot-hash",
        "a" * 64,
        "--silver-snapshot-size",
        "100",
        "--output-prefix",
        "file:///tmp/gold",
        "--planned-at",
        "2026-09-19T00:00:00Z",
        "--committed-at",
        "2026-09-19T00:01:00Z",
        "--catalog-type",
        "hadoop",
        "--warehouse",
        "file:///tmp/warehouse",
        "--image-digest",
        "sha256:" + ("b" * 64),
        "--owner-subject",
        "owner-123",
    ]


def test_gold_cli_builds_local_snapshot_reference() -> None:
    parsed = build_parser().parse_args(_arguments())
    reference = _snapshot_ref(parsed)
    assert reference.uri == "file:///tmp/silver.json"
    assert reference.object_version is None
    assert parsed.owner_subject == "owner-123"
    assert parsed.silver_namespace == "video_media_catalog"
    assert parsed.gold_namespace == "video_media_catalog"
    assert parsed.app_name == "media-catalog-research-gold"
    assert parsed.s3_credentials_provider == "web-identity"
    assert not hasattr(parsed, "context_id")
    assert not hasattr(parsed, "allowed_zones")

    emr = build_parser().parse_args(
        [*_arguments(), "--s3-credentials-provider", "default"]
    )
    assert emr.s3_credentials_provider == "default"


def test_gold_cli_requires_immutable_s3_snapshot() -> None:
    arguments = _arguments()
    arguments[1] = "s3://bucket/silver.json"
    with pytest.raises(ValueError, match="version and ETag"):
        _snapshot_ref(build_parser().parse_args(arguments))
