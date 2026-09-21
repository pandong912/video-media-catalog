"""Durable, content-bound checkpoints for Source Silver Spark mapping."""

from __future__ import annotations

import base64
import hashlib
import logging
import re
import threading
import uuid
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, Self
from urllib.parse import urlsplit

from pydantic import Field, field_validator, model_validator

from video_media_catalog.canonical import (
    canonical_json_bytes,
    deterministic_key,
    sha256_digest,
)
from video_media_catalog.community_rows import (
    entity_type_assertion_row,
    field_assertion_row,
    identifier_assertion_row,
    relationship_assertion_row,
    source_record_row,
)
from video_media_catalog.community_tables import (
    DATA_TABLE_COLUMNS,
    NULLABLE_COLUMNS,
)
from video_media_catalog.connector import (
    ConnectorBatchManifest,
    ConnectorRecordSetManifest,
)
from video_media_catalog.models import ObjectRef
from video_media_catalog.object_store import (
    BoundedObjectStore,
    ObjectStoreError,
    S3Location,
    conditional_publish_bytes,
)
from video_media_catalog.record_shard_materialization import MaterializedRecordShard
from video_media_catalog.runtime_args import join_uri
from video_media_catalog.spark_input import spark_uri
from video_media_catalog.storage import (
    ImmutableObjectConflictError,
    digest_file,
    local_path,
)
from video_media_catalog.v2_contracts import (
    V2ContractModel,
    digest_identity,
    require_sha256,
    require_slug,
)

DEFAULT_SOURCE_SILVER_CHECKPOINT_GROUP_SIZE = 64
MAX_SOURCE_SILVER_CHECKPOINT_GROUP_SIZE = 1024
MAX_SOURCE_SILVER_CHECKPOINT_RECEIPT_BYTES = 16 * 1024 * 1024
SOURCE_SILVER_CHECKPOINT_RECEIPT_MEDIA_TYPE = (
    "application/vnd.video-media-catalog.source-silver-checkpoint.v1+json"
)
SOURCE_SILVER_CHECKPOINT_TABLES = (
    "community_source_record",
    "community_field_assertion",
    "community_identifier_assertion",
    "community_relationship_assertion",
    "community_entity_type_assertion",
)
_ZERO_RUN_ID = "sha256:" + ("0" * 64)
_HEX_32 = re.compile(r"^[0-9a-f]{32}$")
_LOGGER = logging.getLogger(__name__)


class SourceSilverMapperIdentity(V2ContractModel):
    """Versioned mapper declaration bound into checkpoint identity."""

    mapper_id: str
    mapper_version: str

    @field_validator("mapper_id")
    @classmethod
    def validate_mapper_id(cls, value: str) -> str:
        return require_slug(value, label="mapper_id")

    @field_validator("mapper_version")
    @classmethod
    def validate_mapper_version(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized or len(normalized) > 128:
            raise ValueError("mapper_version must be non-empty and bounded")
        return normalized


_DEFAULT_MAPPER_IDENTITIES = {
    "eidr-public-registry": SourceSilverMapperIdentity(
        mapper_id="eidr-v2-mapper",
        mapper_version="1.0.0",
    ),
    "imdb-non-commercial-datasets": SourceSilverMapperIdentity(
        mapper_id="imdb-official-tsv-mapper",
        mapper_version="1.0.1",
    ),
    "tmdb-research": SourceSilverMapperIdentity(
        mapper_id="tmdb-research-mapper",
        mapper_version="1.0.0",
    ),
    "tvmaze-public-api": SourceSilverMapperIdentity(
        mapper_id="tvmaze-show-mapper",
        mapper_version="1.0.0",
    ),
    "wikidata-json-dump": SourceSilverMapperIdentity(
        mapper_id="wikidata-v2-mapper",
        mapper_version="1.1.0",
    ),
}


def mapper_identity_for_product(source_product_id: str) -> SourceSilverMapperIdentity:
    """Return the reviewed mapper identity used by ``mapper_for_product``."""

    try:
        return _DEFAULT_MAPPER_IDENTITIES[source_product_id]
    except KeyError as exc:
        raise ValueError(
            f"unsupported Source Silver mapper identity: {source_product_id}"
        ) from exc


def _checkpoint_schema_payload() -> dict[str, Any]:
    return {
        "format": "PARQUET",
        "projectionVersion": "1.0",
        "tables": {
            table: [
                {
                    "name": column,
                    "nullable": column in NULLABLE_COLUMNS[table],
                    "type": "STRING",
                }
                for column in DATA_TABLE_COLUMNS[table]
                if column != "run_id"
            ]
            for table in SOURCE_SILVER_CHECKPOINT_TABLES
        },
    }


SOURCE_SILVER_CHECKPOINT_SCHEMA_DIGEST = digest_identity(_checkpoint_schema_payload())


class SourceSilverSchemaIdentity(V2ContractModel):
    """Schema declaration for the five run-id-free checkpoint projections."""

    schema_id: Literal["source-silver-mapped-checkpoint"] = (
        "source-silver-mapped-checkpoint"
    )
    schema_version: str = "1.0"
    schema_digest: str = SOURCE_SILVER_CHECKPOINT_SCHEMA_DIGEST

    @field_validator("schema_version")
    @classmethod
    def validate_schema_version(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized or len(normalized) > 128:
            raise ValueError("schema_version must be non-empty and bounded")
        return normalized

    @field_validator("schema_digest")
    @classmethod
    def validate_schema_digest(cls, value: str) -> str:
        return require_sha256(value, label="schema_digest")


class SourceSilverCheckpointIdentity(V2ContractModel):
    """All immutable inputs that may affect a mapped Source Silver row."""

    schema_version: Literal["1.0"] = "1.0"
    record_set_id: str
    source_product_id: str
    source_objects: tuple[ObjectRef, ...]
    mapper_identity: SourceSilverMapperIdentity
    projection_schema: SourceSilverSchemaIdentity
    registry_digest: str
    policy_id: str
    policy_digest: str
    image_digest: str
    config_digest: str
    group_size: int = Field(
        ge=1,
        le=MAX_SOURCE_SILVER_CHECKPOINT_GROUP_SIZE,
    )

    @field_validator(
        "record_set_id",
        "registry_digest",
        "policy_digest",
        "image_digest",
        "config_digest",
    )
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("source_product_id", "policy_id")
    @classmethod
    def validate_reference(cls, value: str) -> str:
        return require_slug(value, label="checkpoint reference")

    @property
    def checkpoint_id(self) -> str:
        return deterministic_key(
            "source-silver-mapped-checkpoint-v1",
            self.model_dump(mode="json", by_alias=True, exclude_none=True),
        )


class SourceSilverCheckpointGroupIdentity(V2ContractModel):
    """One deterministic consecutive source-shard group."""

    checkpoint_id: str
    group_index: int = Field(ge=0)
    first_shard_index: int = Field(ge=0)
    source_objects: tuple[ObjectRef, ...] = Field(min_length=1)

    @field_validator("checkpoint_id")
    @classmethod
    def validate_checkpoint_id(cls, value: str) -> str:
        return require_sha256(value, label="checkpoint_id")

    @property
    def group_id(self) -> str:
        return deterministic_key(
            "source-silver-mapped-checkpoint-group-v1",
            self.model_dump(mode="json", by_alias=True, exclude_none=True),
        )


class SourceSilverCheckpointObject(V2ContractModel):
    """Pinned Parquet or success-marker object emitted by Spark."""

    uri: str
    size_bytes: int = Field(ge=0)
    checksum: str | None = None
    etag: str | None = None
    object_version: str | None = None

    @field_validator("checksum")
    @classmethod
    def validate_checksum(cls, value: str | None) -> str | None:
        return None if value is None else require_sha256(value, label="checksum")

    @field_validator("etag", "object_version")
    @classmethod
    def validate_optional_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip().strip('"')
        if not normalized or len(normalized) > 1024:
            raise ValueError("checkpoint object metadata must be non-empty and bounded")
        return normalized

    @model_validator(mode="after")
    def validate_storage_identity(self) -> Self:
        scheme = urlsplit(self.uri).scheme
        if scheme == "file":
            if (
                self.checksum is None
                or self.etag is not None
                or self.object_version is not None
            ):
                raise ValueError(
                    "file checkpoint objects require checksum and no S3 metadata"
                )
        elif scheme == "s3":
            if self.etag is None or self.object_version is None:
                raise ValueError(
                    "S3 checkpoint objects require ETag and object version"
                )
        else:
            raise ValueError("checkpoint object URI must use file:// or s3://")
        return self


def _validate_checkpoint_prefix(prefix: str) -> str:
    normalized = prefix.strip().rstrip("/")
    parsed = urlsplit(normalized)
    if parsed.scheme not in {"file", "s3"}:
        raise ValueError("checkpoint prefix must use file:// or s3://")
    if parsed.query or parsed.fragment or parsed.username or parsed.password:
        raise ValueError(
            "checkpoint prefix must not contain credentials, query, or fragment"
        )
    if parsed.scheme == "file":
        path = local_path(normalized)
        if not path.is_absolute():
            raise ValueError("file checkpoint prefix must be absolute")
    elif not parsed.netloc or not parsed.path.lstrip("/"):
        raise ValueError("S3 checkpoint prefix must include bucket and key prefix")
    return normalized


def resolve_source_silver_checkpoint_prefix(
    prefix: str | None,
    *,
    warehouse: str,
) -> str | None:
    """Confine optional checkpoint output to the catalog control namespace."""

    if prefix is None:
        return None
    normalized = _validate_checkpoint_prefix(prefix)
    warehouse_parsed = urlsplit(warehouse.rstrip("/"))
    if warehouse_parsed.scheme not in {"file", "s3", "s3a"}:
        raise ValueError("warehouse must use file://, s3://, or s3a://")
    if (
        warehouse_parsed.query
        or warehouse_parsed.fragment
        or warehouse_parsed.username
        or warehouse_parsed.password
    ):
        raise ValueError("warehouse must not contain credentials, query, or fragment")
    if warehouse_parsed.scheme == "file":
        warehouse_root = local_path(warehouse).resolve()
        expected = (
            warehouse_root / "research/control/source-silver-checkpoints"
        ).as_uri()
    else:
        if not warehouse_parsed.netloc or not warehouse_parsed.path.lstrip("/"):
            raise ValueError("warehouse must include bucket and key prefix")
        expected = join_uri(
            (f"s3://{warehouse_parsed.netloc}/{warehouse_parsed.path.lstrip('/')}"),
            "research/control/source-silver-checkpoints",
        )
    if normalized != expected.rstrip("/"):
        raise ValueError(
            "checkpoint prefix must equal the warehouse "
            "research/control/source-silver-checkpoints path"
        )
    return normalized


def _uri_is_within(uri: str, prefix: str) -> bool:
    scheme = urlsplit(prefix).scheme
    if urlsplit(uri).scheme != scheme:
        return False
    if scheme == "file":
        try:
            local_path(uri).resolve().relative_to(local_path(prefix).resolve())
        except ValueError:
            return False
        return True
    location = S3Location.parse(uri)
    root = S3Location.parse(prefix)
    return bool(
        location.bucket == root.bucket
        and location.key.startswith(root.key.rstrip("/") + "/")
    )


def _uris_equal(left: str, right: str) -> bool:
    scheme = urlsplit(left).scheme
    if urlsplit(right).scheme != scheme:
        return False
    if scheme == "file":
        return local_path(left).resolve() == local_path(right).resolve()
    return S3Location.parse(left) == S3Location.parse(right)


class SourceSilverCheckpointTableReceipt(V2ContractModel):
    """Counts, logical digest, and physical files for one table projection."""

    table_name: str
    row_count: int = Field(ge=0)
    row_digest: str
    output_prefix: str
    data_objects: tuple[SourceSilverCheckpointObject, ...] = Field(min_length=1)
    success_object: SourceSilverCheckpointObject

    @field_validator("table_name")
    @classmethod
    def validate_table_name(cls, value: str) -> str:
        if value not in SOURCE_SILVER_CHECKPOINT_TABLES:
            raise ValueError("unsupported Source Silver checkpoint table")
        return value

    @field_validator("row_digest")
    @classmethod
    def validate_row_digest(cls, value: str) -> str:
        return require_sha256(value, label="row_digest")

    @field_validator("output_prefix")
    @classmethod
    def validate_output_prefix(cls, value: str) -> str:
        return _validate_checkpoint_prefix(value)

    @field_validator("data_objects")
    @classmethod
    def validate_data_objects(
        cls,
        value: tuple[SourceSilverCheckpointObject, ...],
    ) -> tuple[SourceSilverCheckpointObject, ...]:
        uris = [item.uri for item in value]
        if uris != sorted(set(uris)):
            raise ValueError("checkpoint data objects must be unique and URI-sorted")
        if any(not item.uri.endswith(".parquet") for item in value):
            raise ValueError("checkpoint data objects must be Parquet files")
        return value

    @model_validator(mode="after")
    def validate_output_membership(self) -> Self:
        if any(
            not _uri_is_within(item.uri, self.output_prefix)
            for item in self.data_objects
        ):
            raise ValueError("checkpoint data object is outside its output prefix")
        if not _uris_equal(
            self.success_object.uri,
            join_uri(self.output_prefix, "_SUCCESS"),
        ):
            raise ValueError("checkpoint success marker does not match output prefix")
        return self


class SourceSilverCheckpointGroupReceipt(V2ContractModel):
    """Commit-last marker for one complete mapped shard group."""

    schema_version: Literal["1.0"] = "1.0"
    status: Literal["COMPLETED"] = "COMPLETED"
    checkpoint_identity: SourceSilverCheckpointIdentity
    group_identity: SourceSilverCheckpointGroupIdentity
    attempt_id: str
    tables: tuple[SourceSilverCheckpointTableReceipt, ...]

    @field_validator("attempt_id")
    @classmethod
    def validate_attempt_id(cls, value: str) -> str:
        if _HEX_32.fullmatch(value) is None:
            raise ValueError("checkpoint attempt_id must be 32 lowercase hex digits")
        return value

    @field_validator("tables")
    @classmethod
    def validate_tables(
        cls,
        value: tuple[SourceSilverCheckpointTableReceipt, ...],
    ) -> tuple[SourceSilverCheckpointTableReceipt, ...]:
        names = tuple(item.table_name for item in value)
        if names != SOURCE_SILVER_CHECKPOINT_TABLES:
            raise ValueError(
                "checkpoint receipt must contain all five tables in stable order"
            )
        return value

    @model_validator(mode="after")
    def validate_group_binding(self) -> Self:
        if self.group_identity.checkpoint_id != self.checkpoint_identity.checkpoint_id:
            raise ValueError("checkpoint receipt group belongs to another checkpoint")
        expected_sources = self.checkpoint_identity.source_objects[
            self.group_identity.first_shard_index : (
                self.group_identity.first_shard_index
                + len(self.group_identity.source_objects)
            )
        ]
        if expected_sources != self.group_identity.source_objects:
            raise ValueError("checkpoint receipt group does not bind ordered sources")
        return self

    @property
    def counts(self) -> dict[str, int]:
        return {item.table_name: item.row_count for item in self.tables}


@dataclass(frozen=True)
class SourceSilverCheckpointConfig:
    """Connection and projection identity options for durable checkpoints."""

    aws_region: str | None = None
    s3_endpoint: str | None = None
    s3_path_style_access: bool = False
    mapper_identity: SourceSilverMapperIdentity | None = None
    schema_identity: SourceSilverSchemaIdentity | None = None
    s3_client: Any | None = field(default=None, repr=False, compare=False)


@dataclass(frozen=True)
class SourceSilverCheckpointProgress:
    """One driver-side durable group progress event."""

    checkpoint_id: str
    group_id: str
    group_index: int
    completed_groups: int
    total_groups: int
    status: Literal["REUSED", "MATERIALIZED"]


def build_source_silver_checkpoint_identity(
    *,
    registry_digest: str,
    batch: ConnectorBatchManifest,
    record_set: ConnectorRecordSetManifest,
    group_size: int = DEFAULT_SOURCE_SILVER_CHECKPOINT_GROUP_SIZE,
    mapper_identity: SourceSilverMapperIdentity | None = None,
    schema_identity: SourceSilverSchemaIdentity | None = None,
) -> SourceSilverCheckpointIdentity:
    """Build the run-id-independent identity for a mapped checkpoint."""

    return SourceSilverCheckpointIdentity(
        record_set_id=record_set.record_set_id,
        source_product_id=batch.source_product_id,
        source_objects=record_set.record_objects,
        mapper_identity=mapper_identity
        or mapper_identity_for_product(batch.source_product_id),
        projection_schema=schema_identity or SourceSilverSchemaIdentity(),
        registry_digest=registry_digest,
        policy_id=batch.policy_id,
        policy_digest=batch.policy_digest,
        image_digest=batch.image_digest,
        config_digest=batch.config_digest,
        group_size=group_size,
    )


def source_silver_checkpoint_groups(
    identity: SourceSilverCheckpointIdentity,
) -> tuple[SourceSilverCheckpointGroupIdentity, ...]:
    """Split ordered source objects into stable consecutive groups."""

    return tuple(
        SourceSilverCheckpointGroupIdentity(
            checkpoint_id=identity.checkpoint_id,
            group_index=start // identity.group_size,
            first_shard_index=start,
            source_objects=identity.source_objects[start : start + identity.group_size],
        )
        for start in range(0, len(identity.source_objects), identity.group_size)
    )


def source_silver_checkpoint_root_uri(
    prefix: str,
    identity: SourceSilverCheckpointIdentity,
) -> str:
    return join_uri(
        _validate_checkpoint_prefix(prefix),
        "source-silver",
        f"checkpoint={identity.checkpoint_id.removeprefix('sha256:')}",
    )


def source_silver_checkpoint_group_root_uri(
    prefix: str,
    identity: SourceSilverCheckpointIdentity,
    group: SourceSilverCheckpointGroupIdentity,
) -> str:
    return join_uri(
        source_silver_checkpoint_root_uri(prefix, identity),
        (
            f"group={group.group_index:06d}-"
            f"{group.group_id.removeprefix('sha256:')[:16]}"
        ),
    )


def source_silver_checkpoint_group_receipt_uri(
    prefix: str,
    identity: SourceSilverCheckpointIdentity,
    group: SourceSilverCheckpointGroupIdentity,
) -> str:
    return join_uri(
        source_silver_checkpoint_group_root_uri(prefix, identity, group),
        "receipt.json",
    )


def checkpoint_table_schema(table: str) -> Any:
    """Spark schema for one run-id-free mapped checkpoint table."""

    if table not in SOURCE_SILVER_CHECKPOINT_TABLES:
        raise KeyError(f"unsupported Source Silver checkpoint table: {table}")
    from pyspark.sql.types import StringType, StructField, StructType

    return StructType(
        [
            StructField(
                column,
                StringType(),
                column in NULLABLE_COLUMNS[table],
            )
            for column in DATA_TABLE_COLUMNS[table]
            if column != "run_id"
        ]
    )


def _checkpoint_columns(table: str) -> tuple[str, ...]:
    return tuple(column for column in DATA_TABLE_COLUMNS[table] if column != "run_id")


def _checkpoint_row(row: dict[str, Any]) -> dict[str, Any]:
    row.pop("run_id")
    return row


def _source_row(pair: tuple[Any, Any]) -> dict[str, Any]:
    return _checkpoint_row(source_record_row(_ZERO_RUN_ID, pair[0]))


def _field_rows(pair: tuple[Any, Any]) -> Iterator[dict[str, Any]]:
    for assertion in pair[1].field_assertions:
        yield _checkpoint_row(field_assertion_row(_ZERO_RUN_ID, assertion))


def _identifier_rows(pair: tuple[Any, Any]) -> Iterator[dict[str, Any]]:
    for assertion in pair[1].identifier_assertions:
        yield _checkpoint_row(identifier_assertion_row(_ZERO_RUN_ID, assertion))


def _relationship_rows(pair: tuple[Any, Any]) -> Iterator[dict[str, Any]]:
    for assertion in pair[1].relationship_assertions:
        yield _checkpoint_row(relationship_assertion_row(_ZERO_RUN_ID, assertion))


def _entity_type_rows(pair: tuple[Any, Any]) -> Iterator[dict[str, Any]]:
    for assertion in pair[1].entity_type_assertions:
        yield _checkpoint_row(entity_type_assertion_row(_ZERO_RUN_ID, assertion))


def _checkpoint_row_rdd(mapped: Any, table: str) -> Any:
    if table == "community_source_record":
        return mapped.map(_source_row)
    if table == "community_field_assertion":
        return mapped.flatMap(_field_rows)
    if table == "community_identifier_assertion":
        return mapped.flatMap(_identifier_rows)
    if table == "community_relationship_assertion":
        return mapped.flatMap(_relationship_rows)
    if table == "community_entity_type_assertion":
        return mapped.flatMap(_entity_type_rows)
    raise KeyError(f"unsupported Source Silver checkpoint table: {table}")


def _partition_row_summary(
    rows: Iterable[Any],
    columns: tuple[str, ...],
) -> Iterator[tuple[int, int, int]]:
    count = 0
    xor_value = 0
    sum_value = 0
    modulus = 1 << 256
    for row in rows:
        if hasattr(row, "asDict"):
            values = row.asDict(recursive=True)
        elif isinstance(row, Mapping):
            values = dict(row)
        else:
            raise TypeError(
                f"unsupported Source Silver checkpoint row: {type(row).__name__}"
            )
        identity = {column: values.get(column) for column in columns}
        value = int.from_bytes(
            hashlib.sha256(canonical_json_bytes(identity)).digest(),
            "big",
        )
        count += 1
        xor_value ^= value
        sum_value = (sum_value + value) % modulus
    yield count, xor_value, sum_value


def summarize_checkpoint_frame(frame: Any, table: str) -> tuple[int, str]:
    """Return count and an order-independent multiset digest."""

    columns = _checkpoint_columns(table)
    if tuple(frame.columns) != columns:
        raise RuntimeError(f"{table} checkpoint columns changed")
    summaries = frame.rdd.mapPartitions(
        lambda rows: _partition_row_summary(rows, columns)
    ).collect()
    count = sum(item[0] for item in summaries)
    xor_value = 0
    sum_value = 0
    modulus = 1 << 256
    for _, partition_xor, partition_sum in summaries:
        xor_value ^= partition_xor
        sum_value = (sum_value + partition_sum) % modulus
    digest = sha256_digest(
        canonical_json_bytes(
            {
                "algorithm": "sha256-multiset-xor-sum-v1",
                "count": count,
                "sum": f"{sum_value:064x}",
                "table": table,
                "xor": f"{xor_value:064x}",
            }
        )
    )
    return count, digest


def _not_found(exc: BaseException) -> bool:
    response = getattr(exc, "response", {})
    code = str(response.get("Error", {}).get("Code", ""))
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return code in {"404", "NoSuchKey", "NotFound"} or status == 404


def _normalized_etag(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip().strip('"')
    return normalized or None


def _s3_checksum(response: Mapping[str, Any]) -> str | None:
    native = response.get("ChecksumSHA256")
    metadata = response.get("Metadata") or {}
    metadata_value = metadata.get("sha256") if isinstance(metadata, Mapping) else None
    native_value: str | None = None
    if isinstance(native, str):
        try:
            decoded = base64.b64decode(native, validate=True)
        except ValueError as exc:
            raise RuntimeError("checkpoint object has invalid native checksum") from exc
        if len(decoded) != 32:
            raise RuntimeError("checkpoint object has invalid native checksum")
        native_value = "sha256:" + decoded.hex()
    metadata_digest: str | None = None
    if isinstance(metadata_value, str):
        metadata_digest = require_sha256(
            "sha256:" + metadata_value.removeprefix("sha256:").lower(),
            label="checkpoint object metadata checksum",
        )
    if (
        native_value is not None
        and metadata_digest is not None
        and native_value != metadata_digest
    ):
        raise RuntimeError("checkpoint object checksum metadata disagrees")
    return native_value or metadata_digest


class SourceSilverCheckpointStore:
    """Read, verify, and commit checkpoint receipts and Spark outputs."""

    def __init__(
        self,
        prefix: str,
        *,
        config: SourceSilverCheckpointConfig,
    ) -> None:
        self.prefix = _validate_checkpoint_prefix(prefix)
        client = config.s3_client
        if client is None and urlsplit(self.prefix).scheme == "file":
            client = object()
        self.store = BoundedObjectStore(
            region=config.aws_region,
            endpoint_url=config.s3_endpoint,
            path_style_access=config.s3_path_style_access,
            client=client,
        )

    @property
    def client(self) -> Any:
        return self.store.client

    def _read_receipt_payload(self, uri: str) -> bytes | None:
        if urlsplit(uri).scheme == "file":
            path = local_path(uri).resolve()
            if not path.exists():
                return None
            if not path.is_file():
                raise RuntimeError("checkpoint receipt path is not a file")
            size = path.stat().st_size
            if size > MAX_SOURCE_SILVER_CHECKPOINT_RECEIPT_BYTES:
                raise ValueError("Source Silver checkpoint receipt is too large")
            return path.read_bytes()

        location = S3Location.parse(uri)
        try:
            response = self.client.get_object(
                Bucket=location.bucket,
                Key=location.key,
                ChecksumMode="ENABLED",
            )
        except Exception as exc:
            if _not_found(exc):
                return None
            raise
        body = response.get("Body")
        if body is None or not hasattr(body, "read"):
            raise RuntimeError("checkpoint receipt has no readable body")
        declared = response.get("ContentLength")
        if (
            not isinstance(declared, int)
            or declared < 0
            or declared > MAX_SOURCE_SILVER_CHECKPOINT_RECEIPT_BYTES
        ):
            body.close()
            raise ValueError("Source Silver checkpoint receipt has invalid size")
        try:
            payload = body.read(MAX_SOURCE_SILVER_CHECKPOINT_RECEIPT_BYTES + 1)
        finally:
            body.close()
        if not isinstance(payload, bytes) or len(payload) != declared:
            raise RuntimeError("checkpoint receipt body length changed")
        checksum = _s3_checksum(response)
        if checksum is None:
            raise RuntimeError("checkpoint receipt checksum is unavailable")
        if sha256_digest(payload) != checksum:
            raise RuntimeError("checkpoint receipt checksum mismatch")
        return payload

    def read_optional(
        self,
        identity: SourceSilverCheckpointIdentity,
        group: SourceSilverCheckpointGroupIdentity,
    ) -> SourceSilverCheckpointGroupReceipt | None:
        uri = source_silver_checkpoint_group_receipt_uri(
            self.prefix,
            identity,
            group,
        )
        payload = self._read_receipt_payload(uri)
        if payload is None:
            return None
        try:
            receipt = SourceSilverCheckpointGroupReceipt.model_validate_json(payload)
        except Exception as exc:
            raise RuntimeError(
                f"invalid Source Silver checkpoint receipt: {uri}"
            ) from exc
        self.validate_receipt_identity(receipt, identity=identity, group=group)
        return receipt

    @staticmethod
    def validate_receipt_identity(
        receipt: SourceSilverCheckpointGroupReceipt,
        *,
        identity: SourceSilverCheckpointIdentity,
        group: SourceSilverCheckpointGroupIdentity,
    ) -> None:
        if receipt.checkpoint_identity != identity or receipt.group_identity != group:
            raise RuntimeError(
                "Source Silver checkpoint receipt identity does not match"
            )

    def publish(
        self,
        receipt: SourceSilverCheckpointGroupReceipt,
    ) -> None:
        uri = source_silver_checkpoint_group_receipt_uri(
            self.prefix,
            receipt.checkpoint_identity,
            receipt.group_identity,
        )
        conditional_publish_bytes(
            self.store,
            receipt.json_bytes(),
            uri,
            media_type=SOURCE_SILVER_CHECKPOINT_RECEIPT_MEDIA_TYPE,
            object_format="OBJECT_FORMAT_JSON",
            max_bytes=MAX_SOURCE_SILVER_CHECKPOINT_RECEIPT_BYTES,
        )

    def _describe_file_object(self, path: Path) -> SourceSilverCheckpointObject:
        digest, size = digest_file(path)
        return SourceSilverCheckpointObject(
            uri=path.resolve().as_uri(),
            size_bytes=size,
            checksum=digest,
        )

    def _describe_s3_object(
        self,
        location: S3Location,
    ) -> SourceSilverCheckpointObject:
        response = self.client.head_object(
            Bucket=location.bucket,
            Key=location.key,
            ChecksumMode="ENABLED",
        )
        size = response.get("ContentLength")
        etag = _normalized_etag(response.get("ETag"))
        version = response.get("VersionId")
        if (
            isinstance(size, bool)
            or not isinstance(size, int)
            or size < 0
            or etag is None
            or not isinstance(version, str)
            or not version
            or version == "null"
        ):
            raise RuntimeError(
                "S3 checkpoint output requires size, ETag, and bucket versioning"
            )
        return SourceSilverCheckpointObject(
            uri=location.uri,
            size_bytes=size,
            checksum=_s3_checksum(response),
            etag=etag,
            object_version=version,
        )

    def _list_s3_keys(self, output_prefix: str) -> tuple[S3Location, ...]:
        location = S3Location.parse(output_prefix)
        prefix = location.key.rstrip("/") + "/"
        request: dict[str, Any] = {
            "Bucket": location.bucket,
            "Prefix": prefix,
        }
        keys: list[S3Location] = []
        while True:
            response = self.client.list_objects_v2(**request)
            for item in response.get("Contents") or ():
                key = item.get("Key")
                if isinstance(key, str):
                    keys.append(S3Location(location.bucket, key))
            if not response.get("IsTruncated"):
                break
            token = response.get("NextContinuationToken")
            if not isinstance(token, str) or not token:
                raise RuntimeError("S3 checkpoint listing omitted continuation token")
            request["ContinuationToken"] = token
        return tuple(sorted(keys, key=lambda item: item.key))

    def describe_table_output(
        self,
        output_prefix: str,
    ) -> tuple[
        tuple[SourceSilverCheckpointObject, ...],
        SourceSilverCheckpointObject,
    ]:
        if urlsplit(output_prefix).scheme == "file":
            root = local_path(output_prefix).resolve()
            success = root / "_SUCCESS"
            if not success.is_file():
                raise RuntimeError("checkpoint output has no _SUCCESS marker")
            data_paths = sorted(
                path for path in root.glob("*.parquet") if path.is_file()
            )
            if not data_paths:
                raise RuntimeError("checkpoint output has no Parquet data objects")
            return (
                tuple(self._describe_file_object(path) for path in data_paths),
                self._describe_file_object(success),
            )

        locations = self._list_s3_keys(output_prefix)
        success_key = S3Location.parse(output_prefix).key.rstrip("/") + "/_SUCCESS"
        success_location = next(
            (item for item in locations if item.key == success_key),
            None,
        )
        if success_location is None:
            raise RuntimeError("checkpoint output has no _SUCCESS marker")
        data_locations = tuple(
            item for item in locations if item.key.endswith(".parquet")
        )
        if not data_locations:
            raise RuntimeError("checkpoint output has no Parquet data objects")
        return (
            tuple(self._describe_s3_object(item) for item in data_locations),
            self._describe_s3_object(success_location),
        )

    def validate_table_output(
        self,
        receipt: SourceSilverCheckpointTableReceipt,
    ) -> None:
        data_objects, success_object = self.describe_table_output(receipt.output_prefix)
        if (
            data_objects != receipt.data_objects
            or success_object != receipt.success_object
        ):
            raise RuntimeError(
                f"{receipt.table_name} checkpoint output metadata changed"
            )


def _checkpoint_spark_uri(uri: str) -> str:
    if urlsplit(uri).scheme == "file":
        return str(local_path(uri))
    return spark_uri(uri)


def _logical_receipt_matches(
    left: SourceSilverCheckpointGroupReceipt,
    right: SourceSilverCheckpointGroupReceipt,
) -> bool:
    return bool(
        left.checkpoint_identity == right.checkpoint_identity
        and left.group_identity == right.group_identity
        and tuple(
            (item.table_name, item.row_count, item.row_digest) for item in left.tables
        )
        == tuple(
            (item.table_name, item.row_count, item.row_digest) for item in right.tables
        )
    )


def _validate_receipt_outputs(
    store: SourceSilverCheckpointStore,
    receipt: SourceSilverCheckpointGroupReceipt,
) -> None:
    for table_receipt in receipt.tables:
        store.validate_table_output(table_receipt)


class SourceSilverCheckpointFrames(Mapping[str, Any]):
    """Lazy, cached DataFrame mapping backed by validated group receipts."""

    def __init__(
        self,
        *,
        spark: Any,
        receipts: Sequence[SourceSilverCheckpointGroupReceipt],
        expected_counts: Mapping[str, int],
        run_id: str,
    ) -> None:
        self._spark = spark
        self._receipts = tuple(receipts)
        self._expected_counts = dict(expected_counts)
        self._run_id = require_sha256(run_id, label="run_id")
        self._keys = tuple(
            table for table in DATA_TABLE_COLUMNS if self._expected_counts[table] > 0
        )
        self._loaded: dict[str, Any] = {}
        self._closed = False
        self._lock = threading.RLock()

    def __iter__(self) -> Iterator[str]:
        return iter(self._keys)

    def __len__(self) -> int:
        return len(self._keys)

    def __getitem__(self, table: str) -> Any:
        if table not in self._expected_counts or self._expected_counts[table] == 0:
            raise KeyError(table)
        if table not in SOURCE_SILVER_CHECKPOINT_TABLES:
            raise RuntimeError(f"no Source Silver checkpoint projection for {table}")
        with self._lock:
            if self._closed:
                raise RuntimeError("Source Silver checkpoint frames are released")
            existing = self._loaded.get(table)
            if existing is not None:
                return existing
            frame = self._load(table)
            self._loaded[table] = frame
            return frame

    def _load(self, table: str) -> Any:
        from pyspark import StorageLevel
        from pyspark.sql import functions as F

        data_uris = [
            _checkpoint_spark_uri(item.uri)
            for receipt in self._receipts
            for table_receipt in receipt.tables
            if table_receipt.table_name == table
            for item in table_receipt.data_objects
        ]
        if not data_uris:
            raise RuntimeError(f"{table} checkpoint has no data objects")
        return (
            self._spark.read.schema(checkpoint_table_schema(table))
            .parquet(*data_uris)
            .withColumn("run_id", F.lit(self._run_id))
            .select(*DATA_TABLE_COLUMNS[table])
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

    @property
    def loaded_tables(self) -> frozenset[str]:
        with self._lock:
            return frozenset(self._loaded)

    def unpersist_loaded(self) -> None:
        with self._lock:
            frames = tuple(self._loaded.values())
            self._loaded.clear()
            self._closed = True
        for frame in frames:
            frame.unpersist()


@dataclass(frozen=True)
class SourceSilverCheckpointBuild:
    """Validated receipts plus counts, before the stable run ID is known."""

    checkpoint_id: str
    expected_counts: dict[str, int]
    spark: Any = field(repr=False, compare=False)
    receipts: tuple[SourceSilverCheckpointGroupReceipt, ...] = field(
        repr=False,
        compare=False,
    )

    def frames_for_run(self, run_id: str) -> SourceSilverCheckpointFrames:
        return SourceSilverCheckpointFrames(
            spark=self.spark,
            receipts=self.receipts,
            expected_counts=self.expected_counts,
            run_id=run_id,
        )


def _materialize_checkpoint_group(
    spark: Any,
    *,
    store: SourceSilverCheckpointStore,
    identity: SourceSilverCheckpointIdentity,
    group: SourceSilverCheckpointGroupIdentity,
    shards: Sequence[MaterializedRecordShard],
    parse_record: Callable[[Any], Any],
    mapper: Callable[[Any], Any],
) -> SourceSilverCheckpointGroupReceipt:
    from pyspark import StorageLevel

    attempt_id = uuid.uuid4().hex
    attempt_root = join_uri(
        source_silver_checkpoint_group_root_uri(
            store.prefix,
            identity,
            group,
        ),
        "attempts",
        f"attempt={attempt_id}",
    )
    envelopes = (
        spark.read.text([shard.spark_uri for shard in shards])
        .rdd.map(parse_record)
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    mapped = envelopes.map(lambda envelope: (envelope, mapper(envelope))).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    try:
        record_count = envelopes.count()
        duplicate = (
            envelopes.map(lambda item: (item.envelope_key, 1))
            .reduceByKey(lambda left, right: left + right)
            .filter(lambda pair: pair[1] > 1)
            .take(1)
        )
        if duplicate:
            raise ValueError("checkpoint group contains duplicate envelope keys")

        table_receipts: list[SourceSilverCheckpointTableReceipt] = []
        for table in SOURCE_SILVER_CHECKPOINT_TABLES:
            frame = spark.createDataFrame(
                _checkpoint_row_rdd(mapped, table),
                schema=checkpoint_table_schema(table),
            ).persist(StorageLevel.MEMORY_AND_DISK)
            try:
                count, digest = summarize_checkpoint_frame(frame, table)
                if table == "community_source_record" and count != record_count:
                    raise RuntimeError(
                        "checkpoint source-record projection count changed"
                    )
                output_prefix = join_uri(attempt_root, f"table={table}")
                (
                    frame.write.mode("errorifexists")
                    .option("compression", "snappy")
                    .parquet(_checkpoint_spark_uri(output_prefix))
                )
                data_objects, success_object = store.describe_table_output(
                    output_prefix
                )
                table_receipts.append(
                    SourceSilverCheckpointTableReceipt(
                        table_name=table,
                        row_count=count,
                        row_digest=digest,
                        output_prefix=output_prefix,
                        data_objects=data_objects,
                        success_object=success_object,
                    )
                )
            finally:
                frame.unpersist()
        return SourceSilverCheckpointGroupReceipt(
            checkpoint_identity=identity,
            group_identity=group,
            attempt_id=attempt_id,
            tables=tuple(table_receipts),
        )
    finally:
        mapped.unpersist()
        envelopes.unpersist()


def _resolve_checkpoint_group(
    spark: Any,
    *,
    store: SourceSilverCheckpointStore,
    identity: SourceSilverCheckpointIdentity,
    group: SourceSilverCheckpointGroupIdentity,
    shards: Sequence[MaterializedRecordShard],
    parse_record: Callable[[Any], Any],
    mapper: Callable[[Any], Any],
) -> tuple[
    SourceSilverCheckpointGroupReceipt,
    Literal["REUSED", "MATERIALIZED"],
]:
    existing = store.read_optional(identity, group)
    if existing is not None:
        _validate_receipt_outputs(store, existing)
        return existing, "REUSED"

    candidate = _materialize_checkpoint_group(
        spark,
        store=store,
        identity=identity,
        group=group,
        shards=shards,
        parse_record=parse_record,
        mapper=mapper,
    )
    try:
        store.publish(candidate)
    except (ImmutableObjectConflictError, ObjectStoreError) as exc:
        if isinstance(exc, ObjectStoreError) and (
            exc.code != "IMMUTABLE_OBJECT_CONFLICT"
        ):
            raise
        winner = store.read_optional(identity, group)
        if winner is None or not _logical_receipt_matches(winner, candidate):
            raise RuntimeError(
                "concurrent Source Silver checkpoint receipt conflicts"
            ) from exc
        _validate_receipt_outputs(store, winner)
        return winner, "MATERIALIZED"
    published = store.read_optional(identity, group)
    if published != candidate:
        raise RuntimeError("published Source Silver checkpoint receipt changed")
    return candidate, "MATERIALIZED"


def build_source_silver_checkpoint_frames(
    spark: Any,
    *,
    registry_digest: str,
    batch: ConnectorBatchManifest,
    record_set: ConnectorRecordSetManifest,
    materialized_shards: Sequence[MaterializedRecordShard],
    checkpoint_prefix: str,
    checkpoint_group_size: int,
    checkpoint_config: SourceSilverCheckpointConfig,
    parse_record: Callable[[Any], Any],
    mapper: Callable[[Any], Any],
    progress_callback: (Callable[[SourceSilverCheckpointProgress], None] | None) = None,
) -> SourceSilverCheckpointBuild:
    """Validate group receipts and materialize only missing mapped groups."""

    identity = build_source_silver_checkpoint_identity(
        registry_digest=registry_digest,
        batch=batch,
        record_set=record_set,
        group_size=checkpoint_group_size,
        mapper_identity=checkpoint_config.mapper_identity,
        schema_identity=checkpoint_config.schema_identity,
    )
    groups = source_silver_checkpoint_groups(identity)
    if len(groups) != (
        (len(materialized_shards) + checkpoint_group_size - 1) // checkpoint_group_size
    ):
        raise RuntimeError("checkpoint group plan does not match materialized shards")
    store = SourceSilverCheckpointStore(
        checkpoint_prefix,
        config=checkpoint_config,
    )
    receipts: list[SourceSilverCheckpointGroupReceipt] = []
    total_groups = len(groups)
    for completed_groups, group in enumerate(groups, start=1):
        start = group.first_shard_index
        shards = materialized_shards[start : start + len(group.source_objects)]
        receipt, status = _resolve_checkpoint_group(
            spark,
            store=store,
            identity=identity,
            group=group,
            shards=shards,
            parse_record=parse_record,
            mapper=mapper,
        )
        receipts.append(receipt)
        progress = SourceSilverCheckpointProgress(
            checkpoint_id=identity.checkpoint_id,
            group_id=group.group_id,
            group_index=group.group_index,
            completed_groups=completed_groups,
            total_groups=total_groups,
            status=status,
        )
        _LOGGER.info(
            "Source Silver checkpoint group %d/%d %s: %s",
            completed_groups,
            total_groups,
            status.lower(),
            group.group_id,
        )
        if progress_callback is not None:
            progress_callback(progress)
    if not groups:
        _LOGGER.info("Source Silver checkpoint has no groups: 0/0 complete")

    expected_counts = {table: 0 for table in DATA_TABLE_COLUMNS}
    for receipt in receipts:
        for table, count in receipt.counts.items():
            expected_counts[table] += count
    if expected_counts["community_source_record"] != record_set.record_count:
        raise ValueError("checkpoint records do not match record-set count")
    return SourceSilverCheckpointBuild(
        checkpoint_id=identity.checkpoint_id,
        expected_counts=expected_counts,
        spark=spark,
        receipts=tuple(receipts),
    )
