"""Serializable contracts for one-shot EIDR public-research ingestion."""

from __future__ import annotations

import hashlib
import math
from dataclasses import asdict, dataclass, replace
from typing import Literal
from urllib.parse import urlsplit

from video_media_catalog.canonical import canonical_json_bytes
from video_media_catalog.community_snapshot import (
    CONTROL_MAX_BYTES,
    SILVER_SNAPSHOT_MEDIA_TYPE,
)
from video_media_catalog.eidr_backfill import (
    BACKFILL_WATERMARK_MEDIA_TYPE,
    DEFAULT_DISCOVERED_PAGE_IDS,
    MAX_DISCOVERED_PAGE_IDS,
)
from video_media_catalog.eidr_backfill_cli import (
    EIDR_AUTHORIZATION_EVIDENCE_MEDIA_TYPE,
    PUBLIC_RESEARCH_DEFAULT_BATCH_IDS,
    PUBLIC_RESEARCH_DEFAULT_MAX_BATCHES,
    PUBLIC_RESEARCH_DEFAULT_MAX_DURATION_SECONDS,
    PUBLIC_RESEARCH_DEFAULT_MAX_IDS,
    PUBLIC_RESEARCH_MAX_BATCH_IDS,
    PUBLIC_RESEARCH_MAX_BATCHES,
    PUBLIC_RESEARCH_MAX_DURATION_SECONDS,
    PUBLIC_RESEARCH_MAX_IDS,
    PUBLIC_RESEARCH_MINIMUM_REQUEST_INTERVAL_SECONDS,
)
from video_media_catalog.eidr_public_provider import (
    MAX_MINIMUM_REQUEST_INTERVAL_SECONDS,
)
from video_media_catalog.temporal.models import (
    CaptureResult,
    ObjectRefPayload,
    PipelineEnv,
    SilverResult,
)
from video_media_catalog.v2_contracts import (
    parse_rfc3339,
    require_rfc3339,
    require_sha256,
)


def _require_s3_prefix(value: str, *, label: str) -> str:
    normalized = value.strip().rstrip("/")
    parsed = urlsplit(normalized)
    if (
        parsed.scheme != "s3"
        or not parsed.netloc
        or not parsed.path.lstrip("/")
        or parsed.query
        or parsed.fragment
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise ValueError(f"{label} must be an s3:// prefix")
    return normalized


def require_pinned_s3_object(
    reference: ObjectRefPayload,
    *,
    label: str,
    media_type: str,
) -> None:
    """Require all immutable identity fields used by the one-shot workflow."""

    parsed = urlsplit(reference.uri)
    if (
        parsed.scheme != "s3"
        or not parsed.netloc
        or not parsed.path.lstrip("/")
        or parsed.query
        or parsed.fragment
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise ValueError(f"{label} must be an immutable s3:// object")
    if reference.format != "OBJECT_FORMAT_JSON":
        raise ValueError(f"{label} must be a JSON ObjectRef")
    if reference.media_type != media_type:
        raise ValueError(f"{label} has an unexpected media type")
    if not 0 < reference.size_bytes <= CONTROL_MAX_BYTES:
        raise ValueError(f"{label} size is outside the control-object bound")
    if not str(reference.etag or "").strip().strip('"'):
        raise ValueError(f"{label} requires an ETag")
    if not str(reference.object_version or "").strip():
        raise ValueError(f"{label} requires an object version")
    digest = reference.checksum_digest().lower()
    require_sha256(digest, label=f"{label} checksum")


def normalize_pinned_s3_object(
    reference: ObjectRefPayload,
    *,
    label: str,
    media_type: str,
) -> ObjectRefPayload:
    require_pinned_s3_object(
        reference,
        label=label,
        media_type=media_type,
    )
    return replace(
        reference,
        checksum_algorithm="CHECKSUM_ALGORITHM_SHA256",
        checksum_value=reference.checksum_digest().removeprefix("sha256:").lower(),
        etag=str(reference.etag).strip().strip('"'),
        object_version=str(reference.object_version).strip(),
    )


def _bounded_integer(value: int, *, label: str, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{label} must be an integer")
    if not 0 < value <= maximum:
        raise ValueError(f"{label} must be between 1 and {maximum}")
    return value


def _bounded_float(
    value: float,
    *,
    label: str,
    minimum: float,
    maximum: float,
) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"{label} must be numeric")
    normalized = float(value)
    if not math.isfinite(normalized) or not minimum <= normalized <= maximum:
        raise ValueError(f"{label} must be between {minimum} and {maximum}")
    return normalized


@dataclass(frozen=True)
class EidrPublicResearchInput:
    """Pinned, bounded inputs for one explicit EIDR public-research run."""

    env: PipelineEnv
    silver_snapshot: ObjectRefPayload
    source_release_id: str
    authorization_evidence: ObjectRefPayload
    authorization_issued_at: str
    acquired_at: str
    emr_image_digest: str
    max_ids: int = PUBLIC_RESEARCH_DEFAULT_MAX_IDS
    max_batches: int = PUBLIC_RESEARCH_DEFAULT_MAX_BATCHES
    batch_size: int = PUBLIC_RESEARCH_DEFAULT_BATCH_IDS
    max_duration_seconds: float = PUBLIC_RESEARCH_DEFAULT_MAX_DURATION_SECONDS
    minimum_request_interval_seconds: float = (
        PUBLIC_RESEARCH_MINIMUM_REQUEST_INTERVAL_SECONDS
    )
    discovery_page_size: int = DEFAULT_DISCOVERED_PAGE_IDS
    lookup_watermark: ObjectRefPayload | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "silver_snapshot",
            normalize_pinned_s3_object(
                self.silver_snapshot,
                label="silver_snapshot",
                media_type=SILVER_SNAPSHOT_MEDIA_TYPE,
            ),
        )
        object.__setattr__(
            self,
            "authorization_evidence",
            normalize_pinned_s3_object(
                self.authorization_evidence,
                label="authorization_evidence",
                media_type=EIDR_AUTHORIZATION_EVIDENCE_MEDIA_TYPE,
            ),
        )
        if self.lookup_watermark is not None:
            object.__setattr__(
                self,
                "lookup_watermark",
                normalize_pinned_s3_object(
                    self.lookup_watermark,
                    label="lookup_watermark",
                    media_type=BACKFILL_WATERMARK_MEDIA_TYPE,
                ),
            )
        object.__setattr__(
            self,
            "source_release_id",
            require_sha256(self.source_release_id, label="source_release_id"),
        )
        object.__setattr__(
            self,
            "authorization_issued_at",
            require_rfc3339(
                self.authorization_issued_at,
                label="authorization_issued_at",
            ),
        )
        object.__setattr__(
            self,
            "acquired_at",
            require_rfc3339(self.acquired_at, label="acquired_at"),
        )
        if parse_rfc3339(
            self.authorization_issued_at,
            label="authorization_issued_at",
        ) > parse_rfc3339(self.acquired_at, label="acquired_at"):
            raise ValueError(
                "authorization evidence cannot be issued after acquired_at"
            )
        object.__setattr__(
            self,
            "emr_image_digest",
            require_sha256(self.emr_image_digest, label="emr_image_digest"),
        )
        image_digest = require_sha256(
            self.env.image_digest,
            label="env.image_digest",
        )
        if not self.env.user_agent.strip():
            raise ValueError("env.user_agent must be non-empty")
        if not self.env.aws_region.strip():
            raise ValueError("env.aws_region must be non-empty")
        if self.env.catalog_type != "glue":
            raise ValueError("EIDR discovery requires catalog_type=glue")
        if not self.env.catalog_name.strip() or not self.env.glue_namespace.strip():
            raise ValueError("Glue catalog name and namespace must be non-empty")
        if not self.env.emr_application_name.strip():
            raise ValueError("env.emr_application_name must be non-empty")
        if not self.env.emr_execution_role_arn.strip():
            raise ValueError("env.emr_execution_role_arn must be non-empty")
        if not self.env.emr_entry_point.strip():
            raise ValueError("env.emr_entry_point must be non-empty")
        normalized_prefixes = {
            "destination_prefix": _require_s3_prefix(
                self.env.destination_prefix,
                label="env.destination_prefix",
            )
        }
        for field_name in (
            "warehouse_uri",
            "record_staging_prefix",
            "silver_checkpoint_prefix",
            "pipeline_summary_prefix",
            "emr_log_uri",
        ):
            normalized_prefixes[field_name] = _require_s3_prefix(
                str(getattr(self.env, field_name)),
                label=f"env.{field_name}",
            )
        object.__setattr__(
            self,
            "env",
            replace(
                self.env,
                image_digest=image_digest,
                **normalized_prefixes,
            ),
        )
        _bounded_integer(
            self.env.emr_executor_instances,
            label="env.emr_executor_instances",
            maximum=10_000,
        )
        _bounded_integer(
            self.env.emr_shuffle_partitions,
            label="env.emr_shuffle_partitions",
            maximum=100_000,
        )
        _bounded_integer(
            self.max_ids,
            label="max_ids",
            maximum=PUBLIC_RESEARCH_MAX_IDS,
        )
        _bounded_integer(
            self.max_batches,
            label="max_batches",
            maximum=PUBLIC_RESEARCH_MAX_BATCHES,
        )
        _bounded_integer(
            self.batch_size,
            label="batch_size",
            maximum=PUBLIC_RESEARCH_MAX_BATCH_IDS,
        )
        object.__setattr__(
            self,
            "max_duration_seconds",
            _bounded_float(
                self.max_duration_seconds,
                label="max_duration_seconds",
                minimum=0.001,
                maximum=PUBLIC_RESEARCH_MAX_DURATION_SECONDS,
            ),
        )
        object.__setattr__(
            self,
            "minimum_request_interval_seconds",
            _bounded_float(
                self.minimum_request_interval_seconds,
                label="minimum_request_interval_seconds",
                minimum=PUBLIC_RESEARCH_MINIMUM_REQUEST_INTERVAL_SECONDS,
                maximum=MAX_MINIMUM_REQUEST_INTERVAL_SECONDS,
            ),
        )
        _bounded_integer(
            self.discovery_page_size,
            label="discovery_page_size",
            maximum=MAX_DISCOVERED_PAGE_IDS,
        )


def eidr_public_research_workflow_id(input_data: EidrPublicResearchInput) -> str:
    """Return the stable reject-duplicate Temporal workflow ID."""

    digest = hashlib.sha256(canonical_json_bytes(asdict(input_data))).hexdigest()
    return f"eidr-public-research-v1-{digest[:32]}"


@dataclass(frozen=True)
class EidrDiscoveryResult:
    manifest_id: str
    manifest: ObjectRefPayload
    id_count: int
    page_count: int
    job_run_id: str


@dataclass(frozen=True)
class EidrLookupResult:
    run_manifest_id: str
    run_manifest: ObjectRefPayload
    completed: bool
    stop_reason: str
    batch_count: int
    attempted_id_count: int
    found_count: int
    not_found_count: int
    attempt_count: int
    retry_count: int
    rate_limit_count: int
    source_silver_inputs: tuple[CaptureResult, ...]
    source_completeness: Literal["PARTIAL"] = "PARTIAL"
    complete_feed_allowed: Literal[False] = False


@dataclass(frozen=True)
class EidrPublicResearchResult:
    discovery: EidrDiscoveryResult
    lookup: EidrLookupResult
    silvers: tuple[SilverResult, ...]
    supplemental_source_run_ids: tuple[str, ...]
    summary: ObjectRefPayload
