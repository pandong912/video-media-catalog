"""Fail-closed validation for Music release image security evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from video_media_catalog.canonical import canonical_json_bytes

ECR_COMPLETE = "COMPLETE"
BLOCKING_SEVERITIES = frozenset({"Critical", "High"})
EVIDENCE_SCHEMA = "video.media-catalog.music-image-scan.v1"


class ImageSecurityEvidenceError(ValueError):
    """Raised when scanner output cannot approve an image."""


def _mapping(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ImageSecurityEvidenceError(f"{field} must be an object")
    return value


def _sequence(value: Any, field: str) -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ImageSecurityEvidenceError(f"{field} must be an array")
    return value


def _sha256(path: Path) -> str:
    return f"sha256:{hashlib.sha256(path.read_bytes()).hexdigest()}"


def validate_ecr_scan(
    scan: Mapping[str, Any],
    *,
    expected_image_digest: str,
) -> dict[str, int]:
    """Require a completed ECR scan with no HIGH or CRITICAL findings."""

    image_id = _mapping(scan.get("imageId"), "imageId")
    actual_digest = image_id.get("imageDigest")
    if actual_digest != expected_image_digest:
        raise ImageSecurityEvidenceError(
            "ECR scan image digest does not match the released image"
        )
    status = _mapping(scan.get("imageScanStatus"), "imageScanStatus")
    if status.get("status") != ECR_COMPLETE:
        description = status.get("description") or "no description"
        raise ImageSecurityEvidenceError(
            f"ECR scan is not COMPLETE: {status.get('status')!r}: {description}"
        )
    findings = _mapping(scan.get("imageScanFindings"), "imageScanFindings")
    raw_counts = findings.get("findingSeverityCounts") or {}
    counts = _mapping(raw_counts, "imageScanFindings.findingSeverityCounts")
    result: dict[str, int] = {}
    for severity in ("CRITICAL", "HIGH"):
        value = counts.get(severity, 0)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise ImageSecurityEvidenceError(
                f"ECR {severity} finding count must be a non-negative integer"
            )
        result[severity.lower()] = value
    if any(result.values()):
        raise ImageSecurityEvidenceError("ECR scan contains HIGH or CRITICAL findings")
    return result


def _database_identity(report: Mapping[str, Any]) -> dict[str, Any]:
    descriptor = _mapping(report.get("descriptor"), "descriptor")
    database = _mapping(descriptor.get("db"), "descriptor.db")
    status = _mapping(database.get("status"), "descriptor.db.status")
    identity = {
        "schemaVersion": status.get("schemaVersion"),
        "built": status.get("built"),
        "source": status.get("from"),
    }
    if not all(isinstance(value, str) and value for value in identity.values()):
        raise ImageSecurityEvidenceError("Grype database identity is incomplete")
    return identity


def _scanner_identity(report: Mapping[str, Any]) -> dict[str, str]:
    descriptor = _mapping(report.get("descriptor"), "descriptor")
    name = descriptor.get("name")
    version = descriptor.get("version")
    if not isinstance(name, str) or not name:
        raise ImageSecurityEvidenceError("Grype scanner name is missing")
    if not isinstance(version, str) or not version:
        raise ImageSecurityEvidenceError("Grype scanner version is missing")
    return {"name": name, "version": version}


def _finding(match: Any) -> dict[str, Any]:
    item = _mapping(match, "matches[]")
    vulnerability = _mapping(item.get("vulnerability"), "matches[].vulnerability")
    artifact = _mapping(item.get("artifact"), "matches[].artifact")
    finding = {
        "vulnerabilityId": vulnerability.get("id"),
        "namespace": vulnerability.get("namespace"),
        "severity": vulnerability.get("severity"),
        "package": {
            "name": artifact.get("name"),
            "version": artifact.get("version"),
            "type": artifact.get("type"),
            "purl": artifact.get("purl"),
        },
    }
    if not isinstance(finding["vulnerabilityId"], str):
        raise ImageSecurityEvidenceError("Grype vulnerability id is missing")
    if not isinstance(finding["severity"], str):
        raise ImageSecurityEvidenceError("Grype vulnerability severity is missing")
    package = finding["package"]
    if not isinstance(package["name"], str) or not isinstance(package["version"], str):
        raise ImageSecurityEvidenceError("Grype package identity is incomplete")
    return finding


def _findings(report: Mapping[str, Any]) -> list[dict[str, Any]]:
    matches = _sequence(report.get("matches"), "matches")
    unique = {
        canonical_json_bytes(finding): finding
        for finding in (_finding(match) for match in matches)
    }
    return [unique[key] for key in sorted(unique)]


def _severity_counts(findings: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts = Counter(str(finding["severity"]) for finding in findings)
    return dict(sorted(counts.items()))


def build_grype_baseline_evidence(
    *,
    baseline_report: Mapping[str, Any],
    image_report: Mapping[str, Any],
    baseline_report_digest: str,
    image_report_digest: str,
    baseline_image: str,
    image: str,
) -> dict[str, Any]:
    """Approve only when the image adds no HIGH/CRITICAL finding to its base."""

    baseline_database = _database_identity(baseline_report)
    image_database = _database_identity(image_report)
    if baseline_database != image_database:
        raise ImageSecurityEvidenceError(
            "baseline and image were not scanned with the same Grype database"
        )
    baseline_scanner = _scanner_identity(baseline_report)
    image_scanner = _scanner_identity(image_report)
    if baseline_scanner != image_scanner:
        raise ImageSecurityEvidenceError(
            "baseline and image were not scanned with the same Grype version"
        )

    baseline_findings = _findings(baseline_report)
    image_findings = _findings(image_report)
    baseline_blocking = {
        canonical_json_bytes(finding): finding
        for finding in baseline_findings
        if finding["severity"] in BLOCKING_SEVERITIES
    }
    image_blocking = {
        canonical_json_bytes(finding): finding
        for finding in image_findings
        if finding["severity"] in BLOCKING_SEVERITIES
    }
    introduced_keys = sorted(image_blocking.keys() - baseline_blocking.keys())
    resolved_keys = sorted(baseline_blocking.keys() - image_blocking.keys())
    introduced = [image_blocking[key] for key in introduced_keys]
    resolved = [baseline_blocking[key] for key in resolved_keys]

    evidence = {
        "schemaVersion": EVIDENCE_SCHEMA,
        "status": "APPROVED" if not introduced else "REJECTED",
        "scanner": baseline_scanner,
        "database": baseline_database,
        "policy": {
            "id": "NO_NEW_HIGH_OR_CRITICAL_VS_PINNED_BASE",
            "blockingSeverities": sorted(BLOCKING_SEVERITIES),
        },
        "baseline": {
            "image": baseline_image,
            "reportDigest": baseline_report_digest,
            "findingCounts": _severity_counts(baseline_findings),
        },
        "image": {
            "image": image,
            "reportDigest": image_report_digest,
            "findingCounts": _severity_counts(image_findings),
        },
        "introducedHighOrCritical": introduced,
        "resolvedHighOrCritical": resolved,
    }
    return evidence


def _load(path: Path) -> Mapping[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ImageSecurityEvidenceError(f"cannot read {path}: {exc}") from exc
    return _mapping(value, str(path))


def _write(path: Path, value: Mapping[str, Any]) -> None:
    path.write_bytes(canonical_json_bytes(value) + b"\n")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    ecr = subparsers.add_parser("validate-ecr")
    ecr.add_argument("--scan", type=Path, required=True)
    ecr.add_argument("--expected-image-digest", required=True)

    grype = subparsers.add_parser("compare-grype")
    grype.add_argument("--baseline-report", type=Path, required=True)
    grype.add_argument("--image-report", type=Path, required=True)
    grype.add_argument("--baseline-image", required=True)
    grype.add_argument("--image", required=True)
    grype.add_argument("--output", type=Path, required=True)

    args = parser.parse_args(argv)
    try:
        if args.command == "validate-ecr":
            validate_ecr_scan(
                _load(args.scan),
                expected_image_digest=args.expected_image_digest,
            )
            return 0
        evidence = build_grype_baseline_evidence(
            baseline_report=_load(args.baseline_report),
            image_report=_load(args.image_report),
            baseline_report_digest=_sha256(args.baseline_report),
            image_report_digest=_sha256(args.image_report),
            baseline_image=args.baseline_image,
            image=args.image,
        )
        _write(args.output, evidence)
        if evidence["status"] != "APPROVED":
            raise ImageSecurityEvidenceError(
                "image introduced HIGH or CRITICAL findings"
            )
        return 0
    except ImageSecurityEvidenceError as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    raise SystemExit(main())
