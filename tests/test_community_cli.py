from __future__ import annotations

import pytest

from video_media_catalog.community_cli import _object_ref, build_parser


def _arguments() -> list[str]:
    return [
        "--batch-manifest-uri",
        "file:///tmp/batch.json",
        "--batch-manifest-hash",
        "a" * 64,
        "--batch-manifest-size",
        "100",
        "--record-set-manifest-uri",
        "file:///tmp/records.json",
        "--record-set-manifest-hash",
        "b" * 64,
        "--record-set-manifest-size",
        "100",
        "--committed-at",
        "2026-09-19T00:00:00Z",
        "--catalog-type",
        "hadoop",
        "--warehouse",
        "file:///tmp/warehouse",
    ]


def test_community_cli_builds_bounded_control_object_ref() -> None:
    parsed = build_parser().parse_args(_arguments())
    reference = _object_ref(
        parsed,
        "batch_manifest",
        media_type="application/vnd.example+json",
    )
    assert reference.uri == "file:///tmp/batch.json"
    assert reference.checksum.value == "a" * 64
    assert reference.object_version is None


def test_community_cli_requires_s3_immutability_fields() -> None:
    arguments = _arguments()
    arguments[1] = "s3://bucket/batch.json"
    parsed = build_parser().parse_args(arguments)
    with pytest.raises(ValueError, match="version and ETag"):
        _object_ref(
            parsed,
            "batch_manifest",
            media_type="application/vnd.example+json",
        )
