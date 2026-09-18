"""Data contracts used at the landing and control-plane boundaries."""

from __future__ import annotations

import re
from typing import Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_serializer,
    field_validator,
    model_validator,
)

from video_media_catalog.canonical import canonical_json_bytes
from video_media_catalog.constants import (
    ALGORITHM_DIGEST,
    ALGORITHM_SPEC_ID,
    CONTROL_SCHEMA_VERSION,
    CURATED_TABLE_KEYS,
    LANDING_SCHEMA_VERSION,
    PRODUCER,
    STAGE,
)
from video_media_catalog.identity import require_canonical_uuid7


def _to_camel(value: str) -> str:
    first, *rest = value.split("_")
    return first + "".join(part.capitalize() for part in rest)


def _validate_prefixed_sha256(value: str) -> str:
    if re.fullmatch(r"sha256:[0-9a-f]{64}", value) is None:
        raise ValueError("expected a sha256:<64 lowercase hex> digest")
    return value


class ContractModel(BaseModel):
    """Accept additive fields and emit lower-camel ProtoJSON names."""

    model_config = ConfigDict(
        alias_generator=_to_camel,
        extra="allow",
        populate_by_name=True,
    )

    def json_bytes(self) -> bytes:
        return canonical_json_bytes(
            self.model_dump(mode="json", by_alias=True, exclude_none=True),
            newline=True,
        )


class LandingRecord(BaseModel):
    """One immutable source record in a normalized Parquet envelope."""

    model_config = ConfigDict(extra="forbid")

    record_key: str
    source: Literal["wikidata", "eidr"]
    source_record_id: str
    source_revision: str | None = None
    modified: str | None = None
    source_hash: str
    payload_json: str

    @field_validator("record_key", "source_hash")
    @classmethod
    def validate_sha256(cls, value: str) -> str:
        return _validate_prefixed_sha256(value)


class SourceObject(ContractModel):
    source: Literal["wikidata", "eidr"]
    uri: str
    checksum: str
    size_bytes: int = Field(ge=0)
    compression: Literal["plain", "gzip", "bzip2"]
    object_version: str | None = None
    etag: str | None = None
    license: str | None = None

    @field_validator("checksum")
    @classmethod
    def validate_checksum(cls, value: str) -> str:
        return _validate_prefixed_sha256(value)

    @field_serializer("size_bytes", when_used="json")
    def serialize_size(self, value: int) -> str:
        return str(value)


class LandingShard(ContractModel):
    uri: str
    checksum: str
    size_bytes: int = Field(ge=0)
    record_count: int = Field(ge=0)
    first_record_key: str
    last_record_key: str
    etag: str | None = None
    object_version: str | None = None

    @field_validator("checksum", "first_record_key", "last_record_key")
    @classmethod
    def validate_digests(cls, value: str) -> str:
        return _validate_prefixed_sha256(value)

    @field_serializer("size_bytes", "record_count", when_used="json")
    def serialize_int64(self, value: int) -> str:
        return str(value)


class LandingManifest(ContractModel):
    manifest_id: str
    schema_version: Literal["1.0"] = LANDING_SCHEMA_VERSION
    algorithm_spec_id: str = ALGORITHM_SPEC_ID
    input_manifest_digest: str | None = None
    sources: list[SourceObject]
    shards: list[LandingShard]
    record_count: int = Field(ge=0)
    source_counts: dict[str, int]

    @field_validator("manifest_id")
    @classmethod
    def validate_manifest_id(cls, value: str) -> str:
        return _validate_prefixed_sha256(value)

    @field_validator("input_manifest_digest")
    @classmethod
    def validate_input_manifest_digest(
        cls,
        value: str | None,
    ) -> str | None:
        if (
            value is not None
            and re.fullmatch(
                r"sha256:hex:[0-9a-f]{64}",
                value,
            )
            is None
        ):
            raise ValueError(
                "input_manifest_digest must use sha256:hex:<64 lowercase hex>"
            )
        return value

    @field_serializer("record_count", when_used="json")
    def serialize_record_count(self, value: int) -> str:
        return str(value)


class LandingSummary(ContractModel):
    schema_version: Literal["1.0"] = LANDING_SCHEMA_VERSION
    status: Literal["COMPLETE"] = "COMPLETE"
    manifest_id: str
    manifest_uri: str
    manifest_checksum: str
    record_count: int = Field(ge=0)
    shard_count: int = Field(ge=0)

    @field_validator("manifest_id", "manifest_checksum")
    @classmethod
    def validate_digests(cls, value: str) -> str:
        return _validate_prefixed_sha256(value)

    @field_serializer("record_count", "shard_count", when_used="json")
    def serialize_int64(self, value: int) -> str:
        return str(value)


class Checksum(ContractModel):
    algorithm: Literal["CHECKSUM_ALGORITHM_SHA256"] = "CHECKSUM_ALGORITHM_SHA256"
    encoding: Literal["CHECKSUM_ENCODING_HEX"] = "CHECKSUM_ENCODING_HEX"
    value: str

    @field_validator("value")
    @classmethod
    def validate_value(cls, value: str) -> str:
        normalized = value.removeprefix("sha256:").lower()
        if re.fullmatch(r"[0-9a-f]{64}", normalized) is None:
            raise ValueError("checksum must be a SHA-256 hex digest")
        return normalized


class ObjectRef(ContractModel):
    uri: str
    format: Literal[
        "OBJECT_FORMAT_JSON",
        "OBJECT_FORMAT_PARQUET",
        "OBJECT_FORMAT_OTHER",
    ]
    media_type: str
    checksum: Checksum
    size_bytes: int = Field(ge=0)
    etag: str | None = None
    object_version: str | None = None
    created_at: str | None = None
    attributes: dict[str, str] = Field(
        default_factory=dict,
        exclude_if=lambda value: not value,
    )

    @field_serializer("size_bytes", when_used="json")
    def serialize_size(self, value: int) -> str:
        return str(value)


class JobSpec(ContractModel):
    job_spec_id: str
    tenant_id: str
    job_kind: Literal["video.media-catalog.v1"]
    input_manifest: ObjectRef
    output_uri_prefix: str
    executor_image: str
    created_at: str
    resources: dict[str, object] = Field(default_factory=dict)
    retry_policy: dict[str, object] = Field(default_factory=dict)
    timeout_us: int = Field(default=0, ge=0)
    labels: dict[str, str] = Field(default_factory=dict)

    @field_validator("job_spec_id", "tenant_id")
    @classmethod
    def validate_uuid7(cls, value: str) -> str:
        return require_canonical_uuid7(value)

    @model_validator(mode="after")
    def validate_media_catalog_input(self) -> JobSpec:
        if (
            self.input_manifest.format != "OBJECT_FORMAT_PARQUET"
            or self.input_manifest.media_type != "application/vnd.apache.parquet"
            or self.input_manifest.etag is None
            or self.input_manifest.object_version is None
        ):
            raise ValueError("media catalog JobSpec requires immutable Parquet input")
        if (
            re.fullmatch(
                r"^[^\s@]+(?:/[^@\s]+)*@sha256:[0-9a-f]{64}$",
                self.executor_image,
            )
            is None
        ):
            raise ValueError("JobSpec executor image must be digest pinned")
        return self

    @field_serializer("timeout_us", when_used="json")
    def serialize_timeout(self, value: int) -> str:
        return str(value)


class SnapshotTable(ContractModel):
    table_name: str
    snapshot_id: int | None = Field(default=None, gt=0)
    parent_snapshot_id: int | None = Field(default=None, gt=0)
    committed_at: str
    operation: str
    record_count: int | None = Field(default=None, ge=0)

    @field_serializer(
        "snapshot_id",
        "parent_snapshot_id",
        "record_count",
        when_used="json",
    )
    def serialize_optional_int64(self, value: int | None) -> str | None:
        return None if value is None else str(value)

    @model_validator(mode="after")
    def validate_empty_snapshot(self) -> SnapshotTable:
        if self.snapshot_id is None and (
            self.parent_snapshot_id is not None
            or self.operation != "empty"
            or self.record_count != 0
        ):
            raise ValueError(
                "a table without snapshotId must be an empty zero-row table"
            )
        return self


class SnapshotSet(ContractModel):
    snapshot_set_id: str
    compute_run_id: str
    job_spec_id: str
    tenant_id: str
    attempt: int = Field(gt=0)
    stage: Literal["media-catalog-commit"] = STAGE
    input_manifest: ObjectRef
    tables: list[SnapshotTable]
    created_at: str
    schema_version: Literal["1.0"] = CONTROL_SCHEMA_VERSION
    output_count: int = Field(ge=0)
    producer: Literal["video-media-catalog-spark/1.0.0"] = PRODUCER
    algorithm_digest: Literal[
        "sha256:b0fe12dbe3670909f5a54c416a247b503eb49515a9da7d6d22754017bbb57c89"
    ] = ALGORITHM_DIGEST
    image_digest: str
    config_digest: str
    metrics: dict[str, str] = Field(default_factory=dict)

    @field_validator("algorithm_digest", "image_digest", "config_digest")
    @classmethod
    def validate_digests(cls, value: str) -> str:
        return _validate_prefixed_sha256(value)

    @field_validator(
        "snapshot_set_id",
        "compute_run_id",
        "job_spec_id",
        "tenant_id",
    )
    @classmethod
    def validate_uuid7(cls, value: str) -> str:
        return require_canonical_uuid7(value)

    @model_validator(mode="after")
    def validate_table_set(self) -> SnapshotSet:
        if len(self.tables) != len(CURATED_TABLE_KEYS):
            raise ValueError("SnapshotSet must contain exactly six catalog tables")
        logical_names = {table.table_name.rsplit(".", 1)[-1] for table in self.tables}
        if logical_names != set(CURATED_TABLE_KEYS):
            raise ValueError("SnapshotSet table names do not match the catalog")
        prefixes = {
            table.table_name.rsplit(".", 1)[0]
            for table in self.tables
            if "." in table.table_name
        }
        if len(prefixes) != 1 or "" in prefixes:
            raise ValueError(
                "SnapshotSet tables must share one non-empty namespace prefix"
            )
        if self.metrics.get("algorithm_spec_id") != ALGORITHM_SPEC_ID:
            raise ValueError("SnapshotSet metrics must bind algorithm_spec_id")
        if self.metrics.get("algorithm_digest") != ALGORITHM_DIGEST:
            raise ValueError("SnapshotSet metrics must bind algorithm_digest")
        for table in self.tables:
            logical_name = table.table_name.rsplit(".", 1)[-1]
            if self.metrics.get(f"{logical_name}_count") != str(table.record_count):
                raise ValueError(
                    "SnapshotSet metrics must bind every table recordCount"
                )
        entity = next(
            table
            for table in self.tables
            if table.table_name.endswith(".catalog_entity")
        )
        if entity.record_count != self.output_count:
            raise ValueError("SnapshotSet outputCount must equal entity count")
        return self

    @field_serializer("output_count", when_used="json")
    def serialize_output_count(self, value: int) -> str:
        return str(value)


class OutputCommit(ContractModel):
    commit_id: str
    compute_run_id: str
    job_spec_id: str
    tenant_id: str
    output_manifest: ObjectRef
    committed_at: str
    schema_version: Literal["1.0"] = CONTROL_SCHEMA_VERSION
    output_count: int = Field(ge=0)
    total_duration_us: int = Field(ge=0)
    producer: Literal["video-media-catalog-spark/1.0.0"] = PRODUCER
    labels: dict[str, str] = Field(default_factory=dict)

    @field_validator("commit_id", "compute_run_id", "job_spec_id", "tenant_id")
    @classmethod
    def validate_uuid7(cls, value: str) -> str:
        return require_canonical_uuid7(value)

    @model_validator(mode="after")
    def validate_media_catalog_labels(self) -> OutputCommit:
        if (
            self.output_manifest.format != "OBJECT_FORMAT_JSON"
            or self.output_manifest.media_type
            != "application/vnd.video-governance.snapshot-set+json"
        ):
            raise ValueError("OutputCommit must reference a JSON SnapshotSet")
        expected = {
            "stage": STAGE,
            "algorithm_spec_id": ALGORITHM_SPEC_ID,
            "algorithm_digest": ALGORITHM_DIGEST,
        }
        if any(self.labels.get(key) != value for key, value in expected.items()):
            raise ValueError("OutputCommit labels do not bind fixed stage identity")
        if (
            self.labels.get("image_digest") is None
            or self.labels.get("config_digest") is None
        ):
            raise ValueError("OutputCommit labels must bind image/config digests")
        if (
            re.fullmatch(
                r"sha256:hex:[0-9a-f]{64}",
                self.labels.get("input_manifest_digest", ""),
            )
            is None
        ):
            raise ValueError("input_manifest_digest must use sha256:hex:<hex>")
        snapshot_id = self.labels.get("snapshot_set_id", "")
        require_canonical_uuid7(snapshot_id)
        for table in CURATED_TABLE_KEYS:
            value = self.labels.get(f"{table}_count", "")
            if not value.isdigit():
                raise ValueError("OutputCommit labels must bind all six table counts")
        return self

    @field_serializer("output_count", "total_duration_us", when_used="json")
    def serialize_int64(self, value: int) -> str:
        return str(value)
