"""Strict control-plane arguments shared by extract and Spark stages."""

from __future__ import annotations

import re
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator

from video_media_catalog.identity import require_canonical_uuid7
from video_media_catalog.models import Checksum, ObjectRef

_PINNED_IMAGE = re.compile(r"^[^\s@]+(?:/[^@\s]+)*@sha256:(?P<digest>[0-9a-f]{64})$")


def join_uri(prefix: str, *parts: str) -> str:
    normalized: list[str] = []
    for part in parts:
        item = part.strip("/")
        if (
            not item
            or item in {".", ".."}
            or any(segment in {".", ".."} for segment in item.split("/"))
        ):
            raise ValueError("URI path components must not contain dot segments")
        normalized.append(item)
    return "/".join([prefix.rstrip("/"), *normalized])


class RuntimeArguments(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    manifest_uri: str
    manifest_hash: str
    manifest_version: str
    manifest_etag: str
    manifest_size: int = Field(ge=0)
    run_id: str
    job_spec_id: str
    tenant_id: str
    attempt: int = Field(gt=0)
    output_prefix: str
    executor_image: str

    @field_validator("manifest_hash")
    @classmethod
    def normalize_hash(cls, value: str) -> str:
        match = re.fullmatch(
            r"(?:sha256:hex:|sha256:)([0-9a-fA-F]{64})",
            value,
        )
        if match is None:
            raise ValueError("manifest hash must use sha256:hex:<hex> or sha256:<hex>")
        return match.group(1).lower()

    @field_validator("run_id", "job_spec_id", "tenant_id")
    @classmethod
    def validate_identity(cls, value: str) -> str:
        return require_canonical_uuid7(value)

    @field_validator("manifest_uri")
    @classmethod
    def validate_manifest_uri(cls, value: str) -> str:
        parsed = urlsplit(value)
        if (
            parsed.scheme != "s3"
            or not parsed.netloc
            or not parsed.path.lstrip("/")
            or parsed.query
            or parsed.fragment
            or parsed.username is not None
            or parsed.password is not None
        ):
            raise ValueError("runtime JobSpec manifest must be an s3://bucket/key URI")
        return value

    @field_validator(
        "manifest_version",
        "manifest_etag",
    )
    @classmethod
    def require_nonempty(cls, value: str) -> str:
        if not value:
            raise ValueError("immutable manifest version and ETag are required")
        return value.strip('"')

    @field_validator("output_prefix")
    @classmethod
    def validate_output_prefix(cls, value: str) -> str:
        parsed = urlsplit(value)
        if parsed.scheme not in {"s3", "file"}:
            raise ValueError("output prefix must use s3:// or file://")
        if parsed.query or parsed.fragment or parsed.username or parsed.password:
            raise ValueError(
                "output prefix must not contain credentials/query/fragment"
            )
        if parsed.scheme == "s3" and (not parsed.netloc or not parsed.path.lstrip("/")):
            raise ValueError("S3 output prefix must include bucket and run key")
        return value.rstrip("/")

    @field_validator("executor_image")
    @classmethod
    def validate_executor_image(cls, value: str) -> str:
        if _PINNED_IMAGE.fullmatch(value) is None:
            raise ValueError("executor image must end with @sha256:<64 lowercase hex>")
        return value

    @property
    def image_digest(self) -> str:
        match = _PINNED_IMAGE.fullmatch(self.executor_image)
        assert match is not None
        return f"sha256:{match.group('digest')}"

    @property
    def input_manifest(self) -> ObjectRef:
        return ObjectRef(
            uri=self.manifest_uri,
            format="OBJECT_FORMAT_PARQUET",
            media_type="application/vnd.apache.parquet",
            checksum=Checksum(value=self.manifest_hash),
            size_bytes=self.manifest_size,
            etag=self.manifest_etag,
            object_version=self.manifest_version,
        )

    def stage_prefix(self, stage: str) -> str:
        return join_uri(
            self.output_prefix,
            f"attempt={self.attempt}",
            f"stage={stage}",
        )
