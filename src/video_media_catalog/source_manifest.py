"""Immutable Parquet source-manifest contract."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel, ConfigDict, Field, field_validator

SOURCE_MANIFEST_MEDIA_TYPE = "application/vnd.apache.parquet"
SOURCE_MANIFEST_MAX_ROWS = 16
SOURCE_MANIFEST_SCHEMA = pa.schema(
    [
        pa.field("source", pa.string(), nullable=False),
        pa.field("uri", pa.string(), nullable=False),
        pa.field("sha256", pa.string(), nullable=False),
        pa.field("size_bytes", pa.int64(), nullable=False),
        pa.field("compression", pa.string(), nullable=False),
        pa.field("object_version", pa.string(), nullable=True),
        pa.field("etag", pa.string(), nullable=True),
        pa.field("license", pa.string(), nullable=False),
    ],
    metadata={
        b"contract": b"video-media-catalog.source-manifest",
        b"schema_version": b"1.0",
    },
)


class SourceManifestEntry(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    source: Literal["wikidata", "eidr"]
    uri: str
    sha256: str
    size_bytes: int = Field(gt=0)
    compression: Literal["plain", "gzip", "bzip2"]
    object_version: str | None = None
    etag: str | None = None
    license: str

    @field_validator("sha256")
    @classmethod
    def normalize_sha256(cls, value: str) -> str:
        match = re.fullmatch(
            r"(?:sha256:hex:|sha256:)?([0-9a-fA-F]{64})",
            value,
        )
        if match is None:
            raise ValueError("source sha256 must contain exactly 64 hex digits")
        return match.group(1).lower()

    @field_validator("uri")
    @classmethod
    def validate_uri(cls, value: str) -> str:
        parsed = urlsplit(value)
        if parsed.scheme not in {"s3", "file"}:
            raise ValueError("source URI must use s3:// or file://")
        if parsed.query or parsed.fragment or parsed.username or parsed.password:
            raise ValueError("source URI must not contain credentials/query/fragment")
        if parsed.scheme == "s3" and (not parsed.netloc or not parsed.path.lstrip("/")):
            raise ValueError("S3 source URI must include bucket and key")
        return value

    @field_validator("object_version", "etag")
    @classmethod
    def normalize_optional(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        return normalized or None

    @field_validator("license")
    @classmethod
    def require_license(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("source license must be explicitly declared")
        return value.strip()


def load_source_manifest(path: Path) -> list[SourceManifestEntry]:
    parquet = pq.ParquetFile(path)
    if parquet.metadata.num_rows > SOURCE_MANIFEST_MAX_ROWS:
        raise ValueError(f"source manifest exceeds {SOURCE_MANIFEST_MAX_ROWS} rows")
    actual = parquet.schema_arrow
    if actual.names != SOURCE_MANIFEST_SCHEMA.names:
        raise ValueError("source manifest columns must exactly match the v1 contract")
    for expected_field, actual_field in zip(
        SOURCE_MANIFEST_SCHEMA, actual, strict=True
    ):
        if expected_field.type != actual_field.type:
            raise ValueError(
                f"source manifest field {expected_field.name} must be "
                f"{expected_field.type}"
            )
    entries = [
        SourceManifestEntry.model_validate(row) for row in parquet.read().to_pylist()
    ]
    if not entries:
        raise ValueError("source manifest must contain at least one source")
    sources = [entry.source for entry in entries]
    if len(sources) != len(set(sources)):
        raise ValueError("source manifest contains duplicate source rows")
    for entry in entries:
        if entry.source == "eidr" and entry.compression != "plain":
            raise ValueError("EIDR XML source must use plain compression")
    return entries


def write_source_manifest(path: Path, entries: list[SourceManifestEntry]) -> None:
    """Write deterministic fixtures/control manifests."""

    validated = [SourceManifestEntry.model_validate(entry) for entry in entries]
    table = pa.Table.from_pylist(
        [entry.model_dump(mode="python") for entry in validated],
        schema=SOURCE_MANIFEST_SCHEMA,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        table,
        path,
        compression="zstd",
        compression_level=9,
        use_dictionary=False,
        write_statistics=True,
        version="2.6",
        data_page_version="1.0",
    )
