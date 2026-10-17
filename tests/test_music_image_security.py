from __future__ import annotations

import json
from pathlib import Path

import pytest

from video_media_catalog.music_image_security import (
    ImageSecurityEvidenceError,
    build_grype_baseline_evidence,
    main,
    validate_ecr_scan,
)

IMAGE_DIGEST = "sha256:" + ("a" * 64)
BASE_IMAGE = "example.invalid/catalog@sha256:" + ("b" * 64)
IMAGE = f"example.invalid/catalog@{IMAGE_DIGEST}"


def _ecr_scan(
    *,
    status: str = "COMPLETE",
    critical: int = 0,
    high: int = 0,
) -> dict[str, object]:
    return {
        "imageId": {"imageDigest": IMAGE_DIGEST},
        "imageScanStatus": {
            "status": status,
            "description": "scanner result",
        },
        "imageScanFindings": {
            "findingSeverityCounts": {
                "CRITICAL": critical,
                "HIGH": high,
            }
        },
    }


def _match(
    vulnerability_id: str,
    severity: str,
    package: str,
) -> dict[str, object]:
    return {
        "vulnerability": {
            "id": vulnerability_id,
            "namespace": "nvd:cpe",
            "severity": severity,
        },
        "artifact": {
            "name": package,
            "version": "1.0",
            "type": "python",
            "purl": f"pkg:pypi/{package}@1.0",
        },
    }


def _grype_report(*matches: dict[str, object]) -> dict[str, object]:
    return {
        "descriptor": {
            "name": "grype",
            "version": "0.120.1",
            "db": {
                "status": {
                    "schemaVersion": "v6.1.10",
                    "built": "2026-10-17T06:27:09Z",
                    "from": "https://grype.example/db?checksum=sha256:test",
                }
            },
        },
        "matches": list(matches),
    }


def test_ecr_scan_must_complete_and_have_zero_blocking_findings() -> None:
    assert validate_ecr_scan(
        _ecr_scan(),
        expected_image_digest=IMAGE_DIGEST,
    ) == {"critical": 0, "high": 0}

    with pytest.raises(ImageSecurityEvidenceError, match="not COMPLETE"):
        validate_ecr_scan(
            _ecr_scan(status="FAILED"),
            expected_image_digest=IMAGE_DIGEST,
        )
    with pytest.raises(ImageSecurityEvidenceError, match="HIGH or CRITICAL"):
        validate_ecr_scan(
            _ecr_scan(high=1),
            expected_image_digest=IMAGE_DIGEST,
        )
    with pytest.raises(ImageSecurityEvidenceError, match="does not match"):
        validate_ecr_scan(
            _ecr_scan(),
            expected_image_digest="sha256:" + ("c" * 64),
        )


def test_grype_baseline_allows_only_inherited_blocking_findings() -> None:
    inherited = _match("CVE-2026-0001", "Critical", "vendor-runtime")
    baseline = _grype_report(inherited)
    image = _grype_report(
        inherited,
        _match("CVE-2026-0002", "Medium", "application-package"),
    )

    evidence = build_grype_baseline_evidence(
        baseline_report=baseline,
        image_report=image,
        baseline_report_digest="sha256:" + ("d" * 64),
        image_report_digest="sha256:" + ("e" * 64),
        baseline_image=BASE_IMAGE,
        image=IMAGE,
    )

    assert evidence["status"] == "APPROVED"
    assert evidence["introducedHighOrCritical"] == []
    assert evidence["baseline"]["findingCounts"] == {"Critical": 1}
    assert evidence["image"]["findingCounts"] == {"Critical": 1, "Medium": 1}


def test_grype_baseline_rejects_new_high_finding() -> None:
    evidence = build_grype_baseline_evidence(
        baseline_report=_grype_report(),
        image_report=_grype_report(
            _match("CVE-2026-9999", "High", "application-package")
        ),
        baseline_report_digest="sha256:" + ("d" * 64),
        image_report_digest="sha256:" + ("e" * 64),
        baseline_image=BASE_IMAGE,
        image=IMAGE,
    )

    assert evidence["status"] == "REJECTED"
    assert [
        finding["vulnerabilityId"] for finding in evidence["introducedHighOrCritical"]
    ] == ["CVE-2026-9999"]


def test_compare_grype_cli_writes_rejection_before_failing(
    tmp_path: Path,
) -> None:
    baseline = tmp_path / "baseline.json"
    image = tmp_path / "image.json"
    output = tmp_path / "evidence.json"
    baseline.write_text(json.dumps(_grype_report()), encoding="utf-8")
    image.write_text(
        json.dumps(_grype_report(_match("CVE-2026-9999", "High", "new"))),
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as error:
        main(
            [
                "compare-grype",
                "--baseline-report",
                str(baseline),
                "--image-report",
                str(image),
                "--baseline-image",
                BASE_IMAGE,
                "--image",
                IMAGE,
                "--output",
                str(output),
            ]
        )

    assert error.value.code == 2
    assert json.loads(output.read_text(encoding="utf-8"))["status"] == "REJECTED"
