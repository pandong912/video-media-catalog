from __future__ import annotations

import pytest

from video_media_catalog.gold_index_cli import (
    _affected_ref,
    _release_ref,
    _sizing_result,
    build_parser,
    build_sizing_parser,
    cleanup_failed_gold_index,
    validate_opensearch_capacity,
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
    assert parsed.min_free_storage_gib == 100
    assert parsed.max_jvm_pressure_percent == 75


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


class _Cluster:
    def health(self):
        return {"status": "green", "number_of_pending_tasks": 1}


class _Nodes:
    def __init__(self, *, free_gib: int = 200, jvm: int = 42):
        self.free_gib = free_gib
        self.jvm = jvm

    def stats(self, *, metric: str):
        assert metric == "jvm,fs"
        return {
            "nodes": {
                "node-1": {
                    "fs": {
                        "total": {
                            "available_in_bytes": self.free_gib * 1024**3,
                        }
                    },
                    "jvm": {"mem": {"heap_used_percent": self.jvm}},
                }
            }
        }


class _CapacityClient:
    def __init__(self, *, free_gib: int = 200, jvm: int = 42):
        self.cluster = _Cluster()
        self.nodes = _Nodes(free_gib=free_gib, jvm=jvm)


class _CleanupIndices:
    def __init__(self, *, active: bool) -> None:
        self.active = active
        self.exists_value = True
        self.delete_calls = 0

    def get_alias(self, *, name: str):
        assert name == "media-catalog-research-read"
        return {"media-catalog-research-failed": {}} if self.active else {}

    def exists(self, *, index: str) -> bool:
        assert index == "media-catalog-research-failed"
        return self.exists_value

    def delete(self, *, index: str) -> None:
        assert index == "media-catalog-research-failed"
        self.delete_calls += 1
        self.exists_value = False


class _CleanupClient:
    def __init__(self, *, active: bool) -> None:
        self.indices = _CleanupIndices(active=active)


def test_failed_build_cleanup_deletes_only_new_unreferenced_shadow() -> None:
    client = _CleanupClient(active=False)
    assert cleanup_failed_gold_index(
        client,
        index_name="media-catalog-research-failed",
        index_created=True,
    )
    assert client.indices.delete_calls == 1

    existing = _CleanupClient(active=False)
    assert not cleanup_failed_gold_index(
        existing,
        index_name="media-catalog-research-failed",
        index_created=False,
    )
    assert existing.indices.delete_calls == 0

    active = _CleanupClient(active=True)
    assert not cleanup_failed_gold_index(
        active,
        index_name="media-catalog-research-failed",
        index_created=True,
    )
    assert active.indices.delete_calls == 0


def test_gold_index_capacity_preflight_records_bounded_cluster_stats() -> None:
    result = validate_opensearch_capacity(
        _CapacityClient(),
        min_free_storage_gib=100,
        max_jvm_pressure_percent=75,
        max_pending_tasks=10,
    )
    assert result["clusterStatus"] == "green"
    assert result["minimumFreeStorageBytes"] == 200 * 1024**3
    assert result["maximumJvmPressurePercent"] == 42


@pytest.mark.parametrize(
    ("free_gib", "jvm", "message"),
    [(50, 42, "free storage"), (200, 90, "JVM pressure")],
)
def test_gold_index_capacity_preflight_fails_closed(
    free_gib: int,
    jvm: int,
    message: str,
) -> None:
    with pytest.raises(RuntimeError, match=message):
        validate_opensearch_capacity(
            _CapacityClient(free_gib=free_gib, jvm=jvm),
            min_free_storage_gib=100,
            max_jvm_pressure_percent=75,
            max_pending_tasks=10,
        )
