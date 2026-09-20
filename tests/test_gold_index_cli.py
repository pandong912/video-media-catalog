from __future__ import annotations

import pytest

from video_media_catalog.gold_index_cli import (
    _affected_ref,
    _release_ref,
    _sizing_result,
    build_parser,
    build_sizing_parser,
)


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
    parsed = build_parser().parse_args(_arguments())
    reference = _release_ref(parsed)
    assert reference.uri == "file:///tmp/release.json"
    assert reference.object_version is None
    assert parsed.read_alias == "media-catalog-research-read"
    assert parsed.index_prefix == "media-catalog-research"
    assert parsed.namespace == "video_media_catalog"
    assert parsed.app_name == "media-catalog-research-index"
    assert parsed.bulk_partitions == 32
    assert parsed.bulk_workers == 2
    assert parsed.incremental_task_timeout_seconds == 6 * 60 * 60
    assert parsed.enable_incremental is False


def test_gold_index_cli_requires_immutable_s3_release() -> None:
    arguments = _arguments()
    arguments[1] = "s3://bucket/release.json"
    with pytest.raises(ValueError, match="version and ETag"):
        _release_ref(build_parser().parse_args(arguments))


def test_gold_index_cli_keeps_incremental_mode_explicitly_disabled() -> None:
    arguments = [
        *_arguments(),
        "--affected-entity-manifest-uri",
        "file:///tmp/affected.json",
    ]
    parsed = build_parser().parse_args(arguments)
    with pytest.raises(ValueError, match="require --enable-incremental"):
        _affected_ref(parsed)


def test_gold_index_cli_builds_immutable_affected_manifest_reference() -> None:
    parsed = build_parser().parse_args(
        [
            *_arguments(),
            "--enable-incremental",
            "--affected-entity-manifest-uri",
            "file:///tmp/affected.json",
            "--affected-entity-manifest-hash",
            "c" * 64,
            "--affected-entity-manifest-size",
            "200",
        ]
    )
    reference = _affected_ref(parsed)
    assert reference is not None
    assert reference.uri == "file:///tmp/affected.json"
    assert reference.checksum.value == "c" * 64


def test_gold_index_sizing_cli_plans_small_offline_sample() -> None:
    parsed = build_sizing_parser().parse_args(
        [
            "--scale",
            "1m",
            "--sample-size",
            "3",
            "--bulk-partitions",
            "2",
            "--bulk-workers",
            "1",
        ]
    )
    result = _sizing_result(parsed)
    assert len(result["plans"]) == 1
    assert result["plans"][0]["targetDocumentCount"] == 1_000_000
    assert result["plans"][0]["sampleDocumentCount"] == 3
