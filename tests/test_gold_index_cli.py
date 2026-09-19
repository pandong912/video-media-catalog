from __future__ import annotations

import pytest

from video_media_catalog.gold_index_cli import _release_ref, build_parser


def _arguments() -> list[str]:
    return [
        "--release-commit-uri",
        "file:///tmp/release.json",
        "--release-commit-hash",
        "a" * 64,
        "--release-commit-size",
        "100",
        "--manifest-prefix",
        "file:///tmp/index-builds",
        "--completed-at",
        "2026-09-19T00:00:00Z",
        "--image-digest",
        "sha256:" + ("b" * 64),
        "--catalog-type",
        "hadoop",
        "--warehouse",
        "file:///tmp/warehouse",
        "--opensearch-endpoint",
        "http://localhost:9200",
        "--allow-insecure-opensearch",
    ]


def test_gold_index_cli_builds_local_release_reference() -> None:
    reference = _release_ref(build_parser().parse_args(_arguments()))
    assert reference.uri == "file:///tmp/release.json"
    assert reference.object_version is None


def test_gold_index_cli_requires_immutable_s3_release() -> None:
    arguments = _arguments()
    arguments[1] = "s3://bucket/release.json"
    with pytest.raises(ValueError, match="version and ETag"):
        _release_ref(build_parser().parse_args(arguments))
