from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
import yaml

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


def test_preflight_cli_exit_codes(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
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
