"""Activities that admit immutable Sports and Animation capture manifests."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from temporalio import activity
from temporalio.exceptions import ApplicationError

from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.object_store import BoundedObjectStore
from video_media_catalog.temporal.models import CaptureResult, ObjectRefPayload
from video_media_catalog.temporal.research_domain_models import (
    ResearchCaptureResult,
    ResearchSourceProfile,
)
from video_media_catalog.v2_contracts import require_rfc3339, require_sha256

RESEARCH_CAPTURE_MANIFEST_SCHEMA = (
    "video.media-catalog.research-capture-manifest.v1"
)


def _object_ref(reference: ObjectRefPayload) -> ObjectRef:
    return ObjectRef(
        uri=reference.uri,
        format=reference.format,
        media_type=reference.media_type,
        checksum=Checksum(value=reference.checksum_digest()),
        size_bytes=reference.size_bytes,
        etag=reference.etag,
        object_version=reference.object_version,
    )


def _required_string(value: Any, *, label: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{label} must be non-empty")
    return normalized


def _is_within_prefix(uri: str, prefix: str) -> bool:
    candidate = urlsplit(uri)
    boundary = urlsplit(prefix)
    if candidate.scheme == "file":
        return True
    if candidate.scheme != "s3" or boundary.scheme != "s3":
        return False
    prefix_path = boundary.path.rstrip("/") + "/"
    return (
        candidate.netloc == boundary.netloc
        and candidate.path.startswith(prefix_path)
    )


def _capture_result(
    value: Any,
    *,
    source_product_id: str,
    destination_prefix: str,
) -> CaptureResult:
    if not isinstance(value, dict):
        raise ValueError("research capture entry must be an object")
    if value.get("kind") != "snapshot":
        raise ValueError("research sources must use snapshot capture semantics")
    batch_id = require_sha256(
        _required_string(value.get("batchId"), label="capture batchId")
    )
    record_set_id = require_sha256(
        _required_string(value.get("recordSetId"), label="capture recordSetId")
    )
    record_count = value.get("recordCount")
    if isinstance(record_count, bool) or not isinstance(record_count, int):
        raise ValueError("research capture recordCount must be an integer")
    if record_count < 0:
        raise ValueError("research capture recordCount cannot be negative")
    if value.get("sourceProductId") != source_product_id:
        raise ValueError("research capture sourceProductId does not match its profile")
    batch_manifest = ObjectRefPayload.from_mapping(value["batchManifest"])
    record_set_manifest = ObjectRefPayload.from_mapping(
        value["recordSetManifest"]
    )
    if not all(
        _is_within_prefix(reference.uri, destination_prefix)
        for reference in (batch_manifest, record_set_manifest)
    ):
        raise ValueError("research capture objects must stay in destinationPrefix")
    return CaptureResult(
        kind="snapshot",
        batch_id=batch_id,
        record_set_id=record_set_id,
        record_count=record_count,
        batch_manifest=batch_manifest,
        record_set_manifest=record_set_manifest,
        retry_count=int(value.get("retryCount", 0)),
        rate_limit_count=int(value.get("rateLimitCount", 0)),
    )


def _parse_manifest(
    payload: Any,
    *,
    profile: ResearchSourceProfile,
) -> ResearchCaptureResult:
    if not isinstance(payload, dict):
        raise ValueError("research capture manifest must contain an object")
    expected = {
        "schemaVersion": RESEARCH_CAPTURE_MANIFEST_SCHEMA,
        "profileId": profile.profile_id,
        "domain": profile.domain,
        "sourceProductId": profile.source_product_id,
        "watermark": profile.watermark,
    }
    for name, value in expected.items():
        if payload.get(name) != value:
            raise ValueError(f"research capture manifest {name} mismatch")
    acquired_at = require_rfc3339(
        _required_string(payload.get("acquiredAt"), label="capture acquiredAt")
    )
    raw_captures = payload.get("captures")
    if not isinstance(raw_captures, list) or not raw_captures:
        raise ValueError("research capture manifest requires captures")
    captures = tuple(
        _capture_result(
            item,
            source_product_id=profile.source_product_id,
            destination_prefix=profile.env.destination_prefix,
        )
        for item in raw_captures
    )
    batch_ids = tuple(item.batch_id for item in captures)
    if len(batch_ids) != len(set(batch_ids)):
        raise ValueError("research capture manifest contains duplicate batches")
    return ResearchCaptureResult(
        profile_id=profile.profile_id,
        domain=profile.domain,
        source_product_id=profile.source_product_id,
        watermark=profile.watermark,
        acquired_at=acquired_at,
        captures=captures,
    )


@activity.defn(name="LoadPinnedResearchCapture")
def load_pinned_research_capture(
    profile: ResearchSourceProfile,
) -> ResearchCaptureResult:
    """Read one checksum/version-pinned connector result without mutable fallback."""

    reference = _object_ref(profile.capture_manifest)
    if not _is_within_prefix(reference.uri, profile.env.destination_prefix):
        raise ApplicationError(
            "research capture manifest must stay in destinationPrefix",
            non_retryable=True,
        )
    if urlsplit(reference.uri).scheme == "s3" and (
        not reference.etag or not reference.object_version
    ):
        raise ApplicationError(
            "research S3 capture manifests require ETag and ObjectVersion",
            non_retryable=True,
        )
    store = (
        BoundedObjectStore(client=object())
        if urlsplit(reference.uri).scheme == "file"
        else BoundedObjectStore(
            region=profile.env.aws_region,
            endpoint_url=profile.env.s3_endpoint,
            path_style_access=profile.env.s3_path_style_access,
        )
    )
    try:
        store.verify(reference, max_bytes=profile.max_manifest_bytes)
        if activity.in_activity():
            activity.heartbeat(
                {
                    "phase": "research-capture-manifest-verified",
                    "profileId": profile.profile_id,
                }
            )
        with tempfile.TemporaryDirectory(
            prefix="research-capture-manifest-"
        ) as directory:
            materialized = store.download(
                reference,
                Path(directory) / "capture.json",
                max_bytes=profile.max_manifest_bytes,
            )
            payload = json.loads(materialized.path.read_bytes())
        return _parse_manifest(payload, profile=profile)
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ApplicationError(str(exc), non_retryable=True) from exc
