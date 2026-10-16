from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
import yaml
from test_temporal_golden_models import _baseline_identity_manifest

from video_media_catalog.catalog_source_policy import CATALOG_SOURCE_POLICY_DIGEST


def _load_preflight() -> ModuleType:
    script = (
        Path(__file__).resolve().parents[1] / "scripts" / "golden_release_preflight.py"
    )
    spec = importlib.util.spec_from_file_location("golden_release_preflight", script)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


preflight = _load_preflight()


def _run(index: int) -> str:
    return "sha256:" + f"{index:064x}"


def _object_ref(
    uri: str,
    *,
    media_type: str = "application/json",
) -> dict[str, Any]:
    return {
        "uri": uri,
        "format": "OBJECT_FORMAT_JSON",
        "mediaType": media_type,
        "checksum": {
            "algorithm": "CHECKSUM_ALGORITHM_SHA256",
            "value": "a" * 64,
        },
        "sizeBytes": 100,
        "etag": "etag",
        "objectVersion": "v1",
    }


def _base_values(**overrides: Any) -> dict[str, Any]:
    values: dict[str, Any] = {
        "aws": {"region": "us-east-1"},
        "image": {"digest": "sha256:" + ("1" * 64)},
        "temporal": {"taskQueue": "vw-media-catalog-golden-r8i-v1"},
        "build": {
            "buildId": "catalog-five-source-v8-20261016-r8i",
            "env": {
                "imageDigest": "sha256:" + ("2" * 64),
                "goldExecutorDisk": "185G",
            },
        },
    }
    for key, value in overrides.items():
        if key == "build":
            values["build"].update(value)
        elif key == "env":
            values["build"]["env"].update(value)
        else:
            values[key] = value
    return values


def _runtime_values(*, tmdb_identity_spine: bool) -> dict[str, Any]:
    values = _base_values()
    baseline_run_ids = (_run(1), _run(2), _run(3))
    tmdb_run_ids = tuple(_run(index) for index in range(10, 25))
    identity_manifest = _baseline_identity_manifest(
        identity_generation_id="pure-source-2026-09-r1",
        source_run_ids=(baseline_run_ids[2],),
    )
    identity_run_id = str(identity_manifest["runId"])
    values["build"].update(
        {
            "identityGenerationId": "pure-source-2026-09-r1",
            "baselineSnapshot": _object_ref("s3://bucket/baseline.json"),
            "tmdbPipelineSummary": _object_ref("s3://bucket/tmdb-summary.json"),
            "baselineSourceRunIds": list(baseline_run_ids),
            "baselineIdentityRunId": identity_run_id,
            "baselineIdentityManifest": _object_ref(
                "s3://bucket/baseline-identity-manifest.json",
                media_type=(
                    "application/vnd.video-media-catalog.community-ingest-run.v2+json"
                ),
            ),
            "baselineIdentitySourceRunIds": [baseline_run_ids[2]],
            "tmdbSourceRunIds": list(tmdb_run_ids),
            "supplementalSourceRunIds": [],
            "sourceWatermarks": {
                "imdb-non-commercial-datasets": "2026-09-20T22:03:01Z",
                "tmdb-research": "2026-10-07",
                "tvmaze-public-api": "2026-09-25T18:45:56Z",
                "wikidata-json-dump": "20260914",
            },
            "freshnessOverride": {
                "reason": "One-time release over verified source baselines.",
                "imdbAcquiredAt": "2026-09-20T22:03:01Z",
                "tvmazeAcquiredAt": "2026-09-25T18:45:56Z",
                "maxSloHours": 720,
            },
            "sourceRunStates": [
                {
                    "sourceProductId": "imdb-non-commercial-datasets",
                    "runIds": [baseline_run_ids[0]],
                    "runStatus": "COMMITTED",
                    "freshnessStatus": "OVERRIDDEN",
                    "watermark": "2026-09-20T22:03:01Z",
                    "acquiredAt": "2026-09-20T22:03:01Z",
                    "identitySpine": True,
                    "materialization": True,
                },
                {
                    "sourceProductId": "tvmaze-public-api",
                    "runIds": [baseline_run_ids[1]],
                    "runStatus": "COMMITTED",
                    "freshnessStatus": "OVERRIDDEN",
                    "watermark": "2026-09-25T18:45:56Z",
                    "acquiredAt": "2026-09-25T18:45:56Z",
                    "identitySpine": True,
                    "materialization": True,
                },
                {
                    "sourceProductId": "wikidata-json-dump",
                    "runIds": [baseline_run_ids[2]],
                    "runStatus": "COMMITTED",
                    "freshnessStatus": "PASS",
                    "watermark": "20260914",
                    "identitySpine": False,
                    "materialization": True,
                },
                {
                    "sourceProductId": "tmdb-research",
                    "runIds": list(tmdb_run_ids),
                    "runStatus": "COMMITTED",
                    "freshnessStatus": "PASS",
                    "watermark": "2026-10-07",
                    "identitySpine": tmdb_identity_spine,
                    "materialization": True,
                },
            ],
        }
    )
    values["build"]["env"].update(
        {
            "awsRegion": "us-east-1",
            "warehouseUri": "s3://bucket/warehouse",
            "controlPrefix": "s3://bucket/warehouse/research/control",
            "goldOutputPrefix": "s3://bucket/warehouse/research/gold",
            "pipelineSummaryPrefix": "s3://bucket/landing/summaries/golden",
            "emrApplicationName": "snapshot-app",
            "goldEmrApplicationName": "gold-app",
            "emrExecutionRoleArn": "arn:aws:iam::123456789012:role/emr",
            "emrLogUri": "s3://bucket/logs/",
            "goldEmrLogUri": "s3://bucket/gold-logs/",
            "researchSilverEntryPoint": "local:///opt/research_silver_cli.py",
            "goldEntryPoint": "local:///opt/gold_cli.py",
        }
    )
    baseline = {
        "schemaVersion": "2.0",
        "identityGenerationId": "pure-source-2026-09-r1",
        "committedRunIds": [*baseline_run_ids, identity_run_id],
        "tableMapping": {"community_ingest_run": "community_ingest_run"},
    }
    tmdb_summary = {
        "mode": "bootstrap",
        "captureCount": 15,
        "captures": [{} for _ in range(15)],
        "sourceSilver": [{"runId": run_id} for run_id in tmdb_run_ids],
    }
    values["_testDocuments"] = (baseline, identity_manifest, tmdb_summary)
    return values


def test_preflight_passes_without_identity_reuse() -> None:
    results = preflight.evaluate_values(_base_values())
    assert all(item.passed for item in results)
    assert any(item.name == "build.identityReuse" for item in results)


def test_preflight_blocks_r8h_pre_gate_image_digests() -> None:
    unsafe = next(iter(preflight._UNSAFE_PRE_GATE_DIGESTS))
    results = preflight.evaluate_values(
        _base_values(
            image={"digest": unsafe},
            env={"imageDigest": unsafe},
        )
    )
    by_name = {item.name: item for item in results}
    assert by_name["image.digest"].passed is False
    assert by_name["build.env.imageDigest"].passed is False
    assert "r8h digests are blocked" in by_name["image.digest"].detail


def test_preflight_fails_wrong_disk() -> None:
    results = preflight.evaluate_values(_base_values(env={"goldExecutorDisk": "170G"}))
    disk = next(item for item in results if item.name == "build.env.goldExecutorDisk")
    assert disk.passed is False


def test_preflight_fails_empty_digests_and_ids() -> None:
    results = preflight.evaluate_values(
        _base_values(
            image={"digest": ""},
            temporal={"taskQueue": ""},
            build={"buildId": ""},
            env={"imageDigest": "not-a-digest"},
        )
    )
    by_name = {item.name: item for item in results}
    assert by_name["image.digest"].passed is False
    assert by_name["build.env.imageDigest"].passed is False
    assert by_name["temporal.taskQueue"].passed is False
    assert by_name["build.buildId"].passed is False


def test_preflight_checks_identity_reuse_receipt_digest() -> None:
    reference = {
        "uri": "s3://bucket/identity-reuse.json",
        "checksum": {"algorithm": "CHECKSUM_ALGORITHM_SHA256", "value": "a" * 64},
        "sizeBytes": 12,
    }

    def download(ref: dict[str, Any], *, region: str, s3_client: Any = None) -> dict:
        assert ref["uri"] == reference["uri"]
        assert region == "us-east-1"
        return {
            "identitySummary": {
                "sourcePolicyDigest": CATALOG_SOURCE_POLICY_DIGEST,
            }
        }

    results = preflight.evaluate_values(
        _base_values(build={"identityReuse": reference}),
        download_receipt=download,
    )
    assert all(item.passed for item in results)
    reuse = next(
        item for item in results if item.name == "identityReuse.sourcePolicyDigest"
    )
    assert reuse.detail == CATALOG_SOURCE_POLICY_DIGEST


def test_preflight_fails_stale_identity_reuse_digest() -> None:
    def download(_ref: dict[str, Any], *, region: str, s3_client: Any = None) -> dict:
        return {
            "identitySummary": {
                "sourcePolicyDigest": "sha256:" + ("4" * 64),
            }
        }

    results = preflight.evaluate_values(
        _base_values(
            build={
                "identityReuse": {
                    "uri": "s3://bucket/identity-reuse.json",
                    "checksum": {
                        "algorithm": "CHECKSUM_ALGORITHM_SHA256",
                        "value": "a" * 64,
                    },
                }
            }
        ),
        download_receipt=download,
    )
    reuse = next(
        item for item in results if item.name == "identityReuse.sourcePolicyDigest"
    )
    assert reuse.passed is False
    assert "CATALOG_SOURCE_POLICY_DIGEST" in reuse.detail


def test_runtime_preflight_accepts_fresh_identity_with_tmdb_spine() -> None:
    values = _runtime_values(tmdb_identity_spine=True)
    documents = values.pop("_testDocuments")

    result = preflight.evaluate_runtime_input(
        values,
        documents=documents,
        planned_at="2026-10-09T00:00:00Z",
    )

    assert result.passed is True
    assert "tmdb-research" in result.detail
    assert "incrementalIdentityRuns=17" in result.detail
    assert "identityExecutorProfile=7x8c/48g+20g/400G" in result.detail


def test_runtime_preflight_rejects_fresh_identity_without_tmdb_spine() -> None:
    values = _runtime_values(tmdb_identity_spine=False)
    documents = values.pop("_testDocuments")

    result = preflight.evaluate_runtime_input(
        values,
        documents=documents,
        planned_at="2026-10-09T00:00:00Z",
    )

    assert result.passed is False
    assert "without incremental Identity" in result.detail


def test_runtime_preflight_rejects_r8j_baseline_delta_inversion() -> None:
    values = _runtime_values(tmdb_identity_spine=True)
    documents = values.pop("_testDocuments")
    for state in values["build"]["sourceRunStates"]:
        source = state["sourceProductId"]
        if source in {
            "imdb-non-commercial-datasets",
            "tvmaze-public-api",
        }:
            state["identitySpine"] = False
        elif source == "wikidata-json-dump":
            state["identitySpine"] = True

    result = preflight.evaluate_runtime_input(
        values,
        documents=documents,
        planned_at="2026-10-09T00:00:00Z",
    )

    assert result.passed is False
    assert "unprocessed Silver delta" in result.detail


def test_preflight_cli_exit_codes(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        preflight,
        "evaluate_runtime_input",
        lambda _values: preflight.CheckResult(
            name="build.runtimePreflight",
            passed=True,
            detail="stubbed for CLI exit-code coverage",
        ),
    )
    pass_path = tmp_path / "pass.yaml"
    fail_path = tmp_path / "fail.yaml"
    pass_path.write_text(yaml.safe_dump(_base_values()), encoding="utf-8")
    fail_path.write_text(
        yaml.safe_dump(_base_values(env={"goldExecutorDisk": "60G"})),
        encoding="utf-8",
    )

    assert preflight.main([str(pass_path)]) == 0
    assert "PASS" in capsys.readouterr().out

    assert preflight.main([str(fail_path)]) == 2
    captured = capsys.readouterr()
    assert "[FAIL] build.env.goldExecutorDisk" in captured.out
