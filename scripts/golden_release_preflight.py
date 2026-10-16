#!/usr/bin/env python3
"""Hard-fail GitOps preflight before an expensive Golden Gold EMR release.

Usage:
  uv run python scripts/golden_release_preflight.py path/to/values-dev.yaml
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import yaml

from video_media_catalog.catalog_source_policy import CATALOG_SOURCE_POLICY_DIGEST

_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
_REQUIRED_DISK = "185G"
# Digests pinned for r8h: worker/EMR images lack the release-start Identity
# policy gate and the early Gold provenance check. Starting Gold on these
# burns hours before fail-closing on source-policy mismatch.
_UNSAFE_PRE_GATE_DIGESTS = frozenset(
    {
        "sha256:91bb7c3a480187dea363bed5c05c0f1a59cee5c83e3ffc8eab238bc0866cc04b",
        "sha256:98536ff0ac95d8ca38b172f108f03181975dc374578b9ce00fd03857fc7940d3",
    }
)


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    detail: str

    def line(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        return f"[{status}] {self.name}: {self.detail}"


def _require_mapping(value: Any, *, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be a mapping")
    return value


def _digest(body: bytes) -> str:
    return "sha256:" + hashlib.sha256(body).hexdigest()


def _is_sha256(value: Any) -> bool:
    return isinstance(value, str) and bool(_SHA256.fullmatch(value.lower()))


def _split_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.lstrip("/"):
        raise ValueError(f"expected an S3 object URI, got {uri}")
    return parsed.netloc, parsed.path.lstrip("/")


def _download_identity_reuse_receipt(
    reference: dict[str, Any],
    *,
    region: str,
    s3_client: Any | None = None,
) -> dict[str, Any]:
    import boto3

    client = s3_client or boto3.client("s3", region_name=region)
    uri = str(reference["uri"])
    bucket, key = _split_s3_uri(uri)
    request: dict[str, Any] = {"Bucket": bucket, "Key": key}
    object_version = reference.get("objectVersion")
    if object_version:
        request["VersionId"] = str(object_version)
    response = client.get_object(**request)
    body = response["Body"].read()
    checksum = _require_mapping(
        reference.get("checksum"),
        label="identityReuse.checksum",
    )
    expected_hex = str(checksum.get("value") or "").removeprefix("sha256:")
    if _digest(body) != f"sha256:{expected_hex.lower()}":
        raise ValueError("identityReuse checksum does not match downloaded body")
    size_bytes = reference.get("sizeBytes")
    if size_bytes is not None and len(body) != int(size_bytes):
        raise ValueError("identityReuse sizeBytes does not match downloaded body")
    payload = json.loads(body)
    if not isinstance(payload, dict):
        raise ValueError("identityReuse receipt must be a JSON object")
    return payload


def evaluate_values(
    values: dict[str, Any],
    *,
    s3_client: Any | None = None,
    download_receipt: Any | None = None,
) -> list[CheckResult]:
    checks: list[CheckResult] = []
    build = _require_mapping(values.get("build"), label="build")
    env = _require_mapping(build.get("env"), label="build.env")
    image = _require_mapping(values.get("image"), label="image")
    temporal = _require_mapping(values.get("temporal"), label="temporal")
    aws = _require_mapping(values.get("aws"), label="aws")
    region = str(aws.get("region") or "")

    build_id = str(build.get("buildId") or "").strip()
    checks.append(
        CheckResult(
            name="build.buildId",
            passed=bool(build_id),
            detail=build_id or "empty",
        )
    )

    task_queue = str(temporal.get("taskQueue") or "").strip()
    checks.append(
        CheckResult(
            name="temporal.taskQueue",
            passed=bool(task_queue),
            detail=task_queue or "empty",
        )
    )

    worker_digest = str(image.get("digest") or "")
    worker_ok = _is_sha256(worker_digest) and (
        worker_digest.lower() not in _UNSAFE_PRE_GATE_DIGESTS
    )
    checks.append(
        CheckResult(
            name="image.digest",
            passed=worker_ok,
            detail=(
                worker_digest
                if worker_ok
                else (
                    f"{worker_digest or 'empty'} "
                    "(must be sha256 and post-policy-gate; "
                    "r8h digests are blocked)"
                )
            ),
        )
    )

    image_digest = str(env.get("imageDigest") or "")
    emr_ok = _is_sha256(image_digest) and (
        image_digest.lower() not in _UNSAFE_PRE_GATE_DIGESTS
    )
    checks.append(
        CheckResult(
            name="build.env.imageDigest",
            passed=emr_ok,
            detail=(
                image_digest
                if emr_ok
                else (
                    f"{image_digest or 'empty'} "
                    "(must be sha256 and post-policy-gate; "
                    "r8h digests are blocked)"
                )
            ),
        )
    )

    disk = str(env.get("goldExecutorDisk") or "")
    checks.append(
        CheckResult(
            name="build.env.goldExecutorDisk",
            passed=disk == _REQUIRED_DISK,
            detail=f"{disk or 'empty'} (required {_REQUIRED_DISK})",
        )
    )

    identity_reuse = build.get("identityReuse")
    if identity_reuse is None:
        checks.append(
            CheckResult(
                name="build.identityReuse",
                passed=True,
                detail="absent (fresh Identity required)",
            )
        )
        return checks

    try:
        reference = _require_mapping(identity_reuse, label="build.identityReuse")
        downloader = download_receipt or _download_identity_reuse_receipt
        receipt = downloader(reference, region=region, s3_client=s3_client)
        summary = receipt.get("identitySummary")
        if not isinstance(summary, dict):
            raise ValueError("identityReuse receipt lacks identitySummary object")
        receipt_digest = str(summary.get("sourcePolicyDigest") or "")
        if not _is_sha256(receipt_digest):
            raise ValueError(
                "identitySummary.sourcePolicyDigest must be a sha256 digest"
            )
        if receipt_digest.lower() != CATALOG_SOURCE_POLICY_DIGEST.lower():
            raise ValueError(
                "identitySummary.sourcePolicyDigest differs from "
                f"CATALOG_SOURCE_POLICY_DIGEST ({CATALOG_SOURCE_POLICY_DIGEST})"
            )
        checks.append(
            CheckResult(
                name="identityReuse.sourcePolicyDigest",
                passed=True,
                detail=receipt_digest,
            )
        )
    except Exception as exc:
        checks.append(
            CheckResult(
                name="identityReuse.sourcePolicyDigest",
                passed=False,
                detail=str(exc),
            )
        )
    return checks


def load_values(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    return _require_mapping(payload, label=str(path))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="golden_release_preflight",
        description=(
            "Hard-check GitOps values-dev.yaml before starting Gold EMR release"
        ),
    )
    parser.add_argument("values_file", type=Path)
    parsed = parser.parse_args(argv)
    values = load_values(parsed.values_file)
    results = evaluate_values(values)
    for item in results:
        print(item.line())
    failed = [item for item in results if not item.passed]
    if failed:
        print(
            f"FAIL: {len(failed)} check(s) failed; refusing Gold EMR start",
            file=sys.stderr,
        )
        return 2
    print("PASS: golden release preflight")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
