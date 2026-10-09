"""Bounded EMR Serverless driver stdout readers for Source Silver verification."""

from __future__ import annotations

import gzip
import json
from typing import Any
from urllib.parse import urlparse


def driver_stdout_prefix(
    *,
    log_uri: str,
    application_id: str,
    job_run_id: str,
) -> str:
    base = log_uri.rstrip("/")
    return f"{base}/applications/{application_id}/jobs/{job_run_id}/SPARK_DRIVER/stdout"


def _split_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path:
        raise ValueError(f"expected s3 URI, got {uri}")
    return parsed.netloc, parsed.path.lstrip("/")


def list_candidate_stdout_keys(
    s3_client: Any, *, bucket: str, prefix: str
) -> list[str]:
    keys: list[str] = []
    token: str | None = None
    while True:
        request: dict[str, Any] = {
            "Bucket": bucket,
            "Prefix": prefix,
            "MaxKeys": 100,
        }
        if token:
            request["ContinuationToken"] = token
        response = s3_client.list_objects_v2(**request)
        for item in response.get("Contents", []):
            key = str(item["Key"])
            if key.endswith("/") or item.get("Size", 0) == 0:
                continue
            keys.append(key)
        if not response.get("IsTruncated"):
            return keys
        token = response.get("NextContinuationToken")


def _decode_body(body: bytes, *, key: str) -> str:
    if key.endswith(".gz") or body[:2] == b"\x1f\x8b":
        return gzip.decompress(body).decode("utf-8")
    return body.decode("utf-8")


def parse_driver_json_summary(
    stdout_text: str,
    *,
    required_keys: tuple[str, ...],
    label: str,
) -> dict[str, Any]:
    """Return the last driver JSON object containing every required key."""
    candidates: list[dict[str, Any]] = []
    for line in stdout_text.splitlines():
        text = line.strip()
        if not text.startswith("{"):
            continue
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict) and all(key in payload for key in required_keys):
            candidates.append(payload)
    if not candidates:
        raise ValueError(f"EMR driver stdout did not contain a {label} summary")
    return candidates[-1]


def parse_source_silver_summary(stdout_text: str) -> dict[str, Any]:
    """Return the last JSON object that looks like a Source Silver summary."""
    return parse_driver_json_summary(
        stdout_text,
        required_keys=("runId", "commitKey"),
        label="Source Silver",
    )


def read_driver_json_summary(
    *,
    s3_client: Any,
    log_uri: str,
    application_id: str,
    job_run_id: str,
    required_keys: tuple[str, ...],
    label: str,
    max_bytes: int = 8 * 1024 * 1024,
) -> dict[str, Any]:
    prefix_uri = driver_stdout_prefix(
        log_uri=log_uri,
        application_id=application_id,
        job_run_id=job_run_id,
    )
    bucket, prefix = _split_s3_uri(prefix_uri)
    keys = list_candidate_stdout_keys(s3_client, bucket=bucket, prefix=prefix)
    if not keys:
        # Some EMR layouts append .gz without listing the bare prefix object.
        keys = list_candidate_stdout_keys(
            s3_client, bucket=bucket, prefix=f"{prefix}.gz"
        )
    if not keys:
        raise ValueError(f"no EMR driver stdout objects under {prefix_uri}")

    # Prefer the lexicographically last object (final attempt / rolled part).
    key = sorted(keys)[-1]
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read(max_bytes + 1)
    if len(body) > max_bytes:
        raise ValueError(f"EMR driver stdout exceeds {max_bytes} bytes")
    return parse_driver_json_summary(
        _decode_body(body, key=key),
        required_keys=required_keys,
        label=label,
    )


def read_source_silver_summary(
    *,
    s3_client: Any,
    log_uri: str,
    application_id: str,
    job_run_id: str,
    max_bytes: int = 8 * 1024 * 1024,
) -> dict[str, Any]:
    return read_driver_json_summary(
        s3_client=s3_client,
        log_uri=log_uri,
        application_id=application_id,
        job_run_id=job_run_id,
        required_keys=("runId", "commitKey"),
        label="Source Silver",
        max_bytes=max_bytes,
    )
