"""Versioned OpenSearch index lifecycle and immutable build manifests."""

from __future__ import annotations

import base64
import copy
import hashlib
import re
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal, Protocol
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from video_media_catalog.canonical import canonical_json_bytes
from video_media_catalog.constants import CURATED_TABLE_KEYS
from video_media_catalog.models import ObjectRef, SnapshotSet
from video_media_catalog.opensearch_client import (
    OpenSearchConnection,
    create_opensearch_client,
)

READ_ALIAS = "media-catalog-entities-read"
INDEX_PREFIX = "media-catalog-entities-v1"
PROJECTION_VERSION = "1"
MAX_MANIFEST_BYTES = 1024 * 1024

_SAFE_INDEX_NAME = re.compile(r"^[a-z0-9][a-z0-9_-]{0,254}$")

_MAPPINGS: dict[str, Any] = {
    "dynamic": "strict",
    "properties": {
        "entityKey": {"type": "keyword"},
        "entityType": {"type": "keyword"},
        "canonicalSource": {"type": "keyword"},
        "canonicalSourceId": {"type": "keyword"},
        "displayName": {
            "type": "text",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 1024}},
        },
        "displayLanguage": {"type": "keyword"},
        "names": {
            "type": "nested",
            "dynamic": "strict",
            "properties": {
                "nameType": {"type": "keyword"},
                "language": {"type": "keyword"},
                "value": {
                    "type": "text",
                    "fields": {"keyword": {"type": "keyword", "ignore_above": 1024}},
                },
                "source": {"type": "keyword"},
                "sourceRecordId": {"type": "keyword"},
            },
        },
        "descriptions": {
            "type": "nested",
            "dynamic": "strict",
            "properties": {
                "language": {"type": "keyword"},
                "value": {"type": "text"},
            },
        },
        "sitelinks": {
            "type": "nested",
            "dynamic": "strict",
            "properties": {
                "site": {"type": "keyword"},
                "title": {
                    "type": "text",
                    "fields": {"keyword": {"type": "keyword", "ignore_above": 1024}},
                },
                "url": {"type": "keyword", "ignore_above": 2048},
                "badges": {"type": "keyword"},
            },
        },
        "attributes": {
            "type": "object",
            "dynamic": "strict",
            "properties": {
                "releaseDates": {"type": "keyword"},
                "durations": {"type": "keyword"},
                "languages": {"type": "keyword"},
                "countries": {"type": "keyword"},
                "genres": {"type": "keyword"},
                "episodeCounts": {"type": "keyword"},
                "seasonCounts": {"type": "keyword"},
                "modified": {"type": "keyword"},
            },
        },
        "externalIdentifiers": {
            "type": "nested",
            "dynamic": "strict",
            "properties": {
                "scheme": {"type": "keyword"},
                "value": {"type": "keyword", "ignore_above": 1024},
                "source": {"type": "keyword"},
                "sourceRecordId": {"type": "keyword"},
            },
        },
        "relations": {
            "type": "nested",
            "dynamic": "strict",
            "properties": {
                "relationKey": {"type": "keyword"},
                "relationType": {"type": "keyword"},
                "objectEntityKey": {"type": "keyword"},
                "ordinal": {"type": "keyword"},
                "source": {"type": "keyword"},
                "sourceRecordId": {"type": "keyword"},
            },
        },
        "relationSummary": {
            "type": "nested",
            "dynamic": "strict",
            "properties": {
                "relationType": {"type": "keyword"},
                "count": {"type": "integer"},
            },
        },
        "parentKeys": {"type": "keyword"},
        "sourceRecordIds": {"type": "keyword"},
        "sourceRecords": {
            "type": "nested",
            "dynamic": "strict",
            "properties": {
                "recordKey": {"type": "keyword"},
                "source": {"type": "keyword"},
                "sourceRecordId": {"type": "keyword"},
            },
        },
    },
}

MAPPING_DIGEST = "sha256:" + hashlib.sha256(canonical_json_bytes(_MAPPINGS)).hexdigest()
INDEX_MAPPINGS = {
    **_MAPPINGS,
    "_meta": {
        "mappingVersion": PROJECTION_VERSION,
        "mappingDigest": MAPPING_DIGEST,
    },
}


def _now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _validate_index_name(value: str, *, label: str) -> str:
    if _SAFE_INDEX_NAME.fullmatch(value) is None:
        raise ValueError(f"{label} is not a safe lowercase OpenSearch name")
    return value


def table_snapshot_identity(snapshot_set: SnapshotSet) -> list[dict[str, Any]]:
    snapshots = [
        {
            "tableName": table.table_name,
            "snapshotId": table.snapshot_id,
        }
        for table in snapshot_set.tables
    ]
    if {item["tableName"].rsplit(".", 1)[-1] for item in snapshots} != set(
        CURATED_TABLE_KEYS
    ):
        raise ValueError("snapshot set does not identify all six curated tables")
    return sorted(snapshots, key=lambda item: item["tableName"])


def index_config_digest(
    *,
    read_alias: str,
    index_prefix: str,
    shards: int,
    replicas: int,
    bulk_chunk_size: int,
) -> str:
    if shards < 1 or replicas < 0 or bulk_chunk_size < 1:
        raise ValueError("invalid OpenSearch index build configuration")
    _validate_index_name(read_alias, label="read alias")
    _validate_index_name(index_prefix, label="index prefix")
    payload = {
        "projectionVersion": PROJECTION_VERSION,
        "mappingDigest": MAPPING_DIGEST,
        "readAlias": read_alias,
        "indexPrefix": index_prefix,
        "shards": shards,
        "replicas": replicas,
        "bulkChunkSize": bulk_chunk_size,
    }
    return "sha256:" + hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def derive_build_id(
    *,
    snapshot_set: SnapshotSet,
    config_digest: str,
) -> str:
    payload = {
        "tables": table_snapshot_identity(snapshot_set),
        "mappingDigest": MAPPING_DIGEST,
        "configDigest": config_digest,
    }
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def versioned_index_name(index_prefix: str, build_id: str) -> str:
    _validate_index_name(index_prefix, label="index prefix")
    if re.fullmatch(r"[0-9a-f]{64}", build_id) is None:
        raise ValueError("build id must be a lowercase SHA-256 hex digest")
    return _validate_index_name(
        f"{index_prefix}-{build_id[:24]}",
        label="versioned index",
    )


def index_definition(*, shards: int, replicas: int) -> dict[str, Any]:
    if shards < 1 or replicas < 0:
        raise ValueError("invalid index shard or replica count")
    return {
        "settings": {
            "index": {
                "number_of_shards": shards,
                "number_of_replicas": replicas,
            }
        },
        "mappings": copy.deepcopy(INDEX_MAPPINGS),
    }


def _status_code(exc: Exception) -> int | None:
    status = getattr(exc, "status_code", None)
    if isinstance(status, int):
        return status
    info = getattr(exc, "info", None)
    if isinstance(info, Mapping):
        raw = info.get("status")
        return raw if isinstance(raw, int) else None
    return None


def _is_not_found(exc: Exception) -> bool:
    return _status_code(exc) == 404


def _existing_mapping_digest(client: Any, index_name: str) -> str | None:
    response = client.indices.get_mapping(index=index_name)
    index_mapping = response.get(index_name)
    if index_mapping is None and len(response) == 1:
        index_mapping = next(iter(response.values()))
    if not isinstance(index_mapping, Mapping):
        return None
    mappings = index_mapping.get("mappings")
    if not isinstance(mappings, Mapping):
        return None
    metadata = mappings.get("_meta")
    return metadata.get("mappingDigest") if isinstance(metadata, Mapping) else None


def ensure_versioned_index(
    client: Any,
    *,
    index_name: str,
    shards: int,
    replicas: int,
) -> bool:
    """Create a target index once and reject incompatible existing mappings."""

    _validate_index_name(index_name, label="versioned index")
    if client.indices.exists(index=index_name):
        if _existing_mapping_digest(client, index_name) != MAPPING_DIGEST:
            raise RuntimeError("existing versioned index has an incompatible mapping")
        return False
    try:
        client.indices.create(
            index=index_name,
            body=index_definition(shards=shards, replicas=replicas),
        )
        return True
    except Exception as exc:
        if not client.indices.exists(index=index_name):
            raise
        if _existing_mapping_digest(client, index_name) != MAPPING_DIGEST:
            raise RuntimeError(
                "concurrently created index has an incompatible mapping"
            ) from exc
        return False


@dataclass
class BulkResult:
    document_count: int = 0
    error_count: int = 0
    errors: list[dict[str, Any]] = field(default_factory=list)

    def merge(self, other: BulkResult) -> None:
        self.document_count += other.document_count
        self.error_count += other.error_count
        remaining = max(0, 20 - len(self.errors))
        self.errors.extend(other.errors[:remaining])

    def as_dict(self) -> dict[str, Any]:
        return {
            "documentCount": self.document_count,
            "errorCount": self.error_count,
            "errors": self.errors,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> BulkResult:
        return cls(
            document_count=int(value["documentCount"]),
            error_count=int(value["errorCount"]),
            errors=list(value.get("errors") or []),
        )


def _document(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if hasattr(value, "asDict"):
        return dict(value.asDict(recursive=True))
    raise TypeError(f"unsupported search document row: {type(value).__name__}")


def bulk_partition(
    rows: Iterable[Any],
    *,
    client: Any,
    index_name: str,
    chunk_size: int,
    request_timeout: float,
    streaming_bulk: Callable[..., Iterable[tuple[bool, dict[str, Any]]]] | None = None,
) -> BulkResult:
    """Index one iterator incrementally and report every partial bulk failure."""

    if streaming_bulk is None:
        from opensearchpy.helpers import streaming_bulk

    def actions() -> Iterable[dict[str, Any]]:
        for row in rows:
            document = _document(row)
            entity_key = document.get("entityKey")
            if not isinstance(entity_key, str) or not entity_key:
                raise ValueError("projected document has no entityKey")
            yield {
                "_op_type": "index",
                "_index": index_name,
                "_id": entity_key,
                "_source": document,
            }

    result = BulkResult()
    for succeeded, item in streaming_bulk(
        client,
        actions(),
        chunk_size=chunk_size,
        max_retries=3,
        initial_backoff=1,
        max_backoff=8,
        raise_on_error=False,
        raise_on_exception=False,
        yield_ok=True,
        request_timeout=request_timeout,
    ):
        operation = next(iter(item.values())) if item else {}
        if succeeded:
            result.document_count += 1
            continue
        result.error_count += 1
        if len(result.errors) < 20:
            result.errors.append(
                {
                    "id": (
                        operation.get("_id") if isinstance(operation, Mapping) else None
                    ),
                    "status": (
                        operation.get("status")
                        if isinstance(operation, Mapping)
                        else None
                    ),
                    "error": (
                        operation.get("error")
                        if isinstance(operation, Mapping)
                        else operation
                    ),
                }
            )
    return result


def _index_spark_partition(
    rows: Iterable[Any],
    *,
    connection: OpenSearchConnection,
    index_name: str,
    chunk_size: int,
) -> Iterable[dict[str, Any]]:
    client = create_opensearch_client(connection)
    try:
        yield bulk_partition(
            rows,
            client=client,
            index_name=index_name,
            chunk_size=chunk_size,
            request_timeout=connection.timeout_seconds,
        ).as_dict()
    finally:
        transport = getattr(client, "transport", None)
        if transport is not None and hasattr(transport, "close"):
            transport.close()


def distributed_bulk_index(
    documents: Any,
    *,
    connection: OpenSearchConnection,
    index_name: str,
    chunk_size: int,
    partitions: int | None = None,
) -> BulkResult:
    """Bulk index per Spark partition; only partition summaries reach the driver."""

    if partitions is not None:
        if partitions < 1:
            raise ValueError("partitions must be positive")
        documents = documents.repartition(partitions, "entityKey")
    summaries = documents.rdd.mapPartitions(
        lambda rows: _index_spark_partition(
            rows,
            connection=connection,
            index_name=index_name,
            chunk_size=chunk_size,
        )
    ).collect()
    result = BulkResult()
    for summary in summaries:
        result.merge(BulkResult.from_dict(summary))
    return result


def index_document_count(client: Any, *, index_name: str) -> int:
    client.indices.refresh(index=index_name)
    response = client.count(index=index_name)
    count = response.get("count")
    if not isinstance(count, int) or count < 0:
        raise RuntimeError("OpenSearch count response is invalid")
    return count


def current_alias_indices(client: Any, *, alias: str) -> list[str]:
    _validate_index_name(alias, label="read alias")
    try:
        response = client.indices.get_alias(name=alias)
    except Exception as exc:
        if _is_not_found(exc):
            return []
        raise
    if not isinstance(response, Mapping):
        raise RuntimeError("OpenSearch alias response is invalid")
    return sorted(str(index) for index in response)


def switch_read_alias(client: Any, *, alias: str, target_index: str) -> bool:
    """Atomically remove all old targets and add exactly one read target."""

    _validate_index_name(alias, label="read alias")
    _validate_index_name(target_index, label="target index")
    current = current_alias_indices(client, alias=alias)
    if current == [target_index]:
        return False
    actions = [
        {"remove": {"index": index, "alias": alias}}
        for index in current
        if index != target_index
    ]
    actions.append(
        {
            "add": {
                "index": target_index,
                "alias": alias,
                "is_write_index": False,
            }
        }
    )
    client.indices.update_aliases(body={"actions": actions})
    return True


def _camel(value: str) -> str:
    first, *rest = value.split("_")
    return first + "".join(item.capitalize() for item in rest)


class ManifestTableSnapshot(BaseModel):
    model_config = ConfigDict(alias_generator=_camel, populate_by_name=True)

    table_name: str
    snapshot_id: int | None = None


class IndexBuildManifest(BaseModel):
    model_config = ConfigDict(
        alias_generator=_camel,
        populate_by_name=True,
        extra="forbid",
    )

    schema_version: Literal["1.0"] = "1.0"
    status: Literal["COMPLETED", "FAILED"]
    build_id: str
    source_snapshot_set_id: str
    source_snapshot_set_uri: str
    source_snapshot_set: ObjectRef
    table_snapshots: list[ManifestTableSnapshot]
    mapping_digest: str
    config_digest: str
    document_count: int = Field(ge=0)
    error_count: int = Field(ge=0)
    index: str
    alias: str
    started_at: str
    completed_at: str

    @field_validator("build_id")
    @classmethod
    def validate_build_id(cls, value: str) -> str:
        if re.fullmatch(r"[0-9a-f]{64}", value) is None:
            raise ValueError("buildId must be a lowercase SHA-256 hex digest")
        return value

    @field_validator("mapping_digest", "config_digest")
    @classmethod
    def validate_digest(cls, value: str) -> str:
        if re.fullmatch(r"sha256:[0-9a-f]{64}", value) is None:
            raise ValueError("expected a sha256:<64 lowercase hex> digest")
        return value

    @model_validator(mode="after")
    def validate_source_snapshot_set(self) -> IndexBuildManifest:
        reference = self.source_snapshot_set
        if self.source_snapshot_set_uri != reference.uri:
            raise ValueError("sourceSnapshotSetUri must match sourceSnapshotSet.uri")
        if (
            reference.format != "OBJECT_FORMAT_JSON"
            or reference.media_type
            != "application/vnd.video-governance.snapshot-set+json"
            or reference.etag is None
            or reference.object_version is None
            or not 0 < reference.size_bytes <= 16 * 1024 * 1024
        ):
            raise ValueError(
                "sourceSnapshotSet must be a complete bounded JSON ObjectRef"
            )
        return self

    def json_bytes(self) -> bytes:
        return canonical_json_bytes(
            self.model_dump(mode="json", by_alias=True),
            newline=True,
        )

    def immutable_result(self) -> dict[str, Any]:
        return self.model_dump(
            mode="json",
            by_alias=True,
            exclude={"started_at", "completed_at"},
        )


class IndexManifestPublisher(Protocol):
    def read_optional(self) -> IndexBuildManifest | None: ...

    def publish(self, manifest: IndexBuildManifest) -> IndexBuildManifest: ...


class S3IndexManifestPublisher:
    """Publish one immutable build manifest with an S3 conditional PUT."""

    def __init__(
        self,
        uri: str,
        *,
        aws_region: str | None = None,
        endpoint_url: str | None = None,
        path_style_access: bool = False,
        client: Any | None = None,
    ) -> None:
        parsed = urlsplit(uri)
        if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.strip("/"):
            raise ValueError("index manifest must be a full s3:// object URI")
        self.uri = uri
        self.bucket = parsed.netloc
        self.key = parsed.path.lstrip("/")
        if client is None:
            import boto3
            from botocore.config import Config

            client = boto3.client(
                "s3",
                region_name=aws_region,
                endpoint_url=endpoint_url,
                config=Config(
                    signature_version="s3v4",
                    retries={"max_attempts": 5, "mode": "standard"},
                    s3={
                        "addressing_style": ("path" if path_style_access else "virtual")
                    },
                ),
            )
        self.client = client

    def read_optional(self) -> IndexBuildManifest | None:
        try:
            response = self.client.get_object(
                Bucket=self.bucket,
                Key=self.key,
                ChecksumMode="ENABLED",
            )
        except Exception as exc:
            metadata = getattr(exc, "response", {})
            code = str(metadata.get("Error", {}).get("Code", ""))
            if code in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise
        declared = response.get("ContentLength")
        body = response.get("Body")
        if body is None or not hasattr(body, "read"):
            raise RuntimeError("S3 manifest object has no readable body")
        if isinstance(declared, int) and declared > MAX_MANIFEST_BYTES:
            body.close()
            raise ValueError("index manifest exceeds maximum size")
        try:
            payload = body.read(MAX_MANIFEST_BYTES + 1)
        finally:
            body.close()
        if len(payload) > MAX_MANIFEST_BYTES:
            raise ValueError("index manifest exceeds maximum size")
        checksum = response.get("ChecksumSHA256")
        if checksum is not None:
            actual = base64.b64encode(hashlib.sha256(payload).digest()).decode()
            if checksum != actual:
                raise RuntimeError("index manifest checksum mismatch")
        return IndexBuildManifest.model_validate_json(payload)

    def publish(self, manifest: IndexBuildManifest) -> IndexBuildManifest:
        payload = manifest.json_bytes()
        if len(payload) > MAX_MANIFEST_BYTES:
            raise ValueError("index manifest exceeds maximum size")
        native_checksum = base64.b64encode(hashlib.sha256(payload).digest()).decode()
        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=self.key,
                Body=payload,
                ContentLength=len(payload),
                ContentType="application/json",
                ChecksumSHA256=native_checksum,
                Metadata={"sha256": hashlib.sha256(payload).hexdigest()},
                IfNoneMatch="*",
            )
        except Exception as exc:
            metadata = getattr(exc, "response", {})
            code = str(metadata.get("Error", {}).get("Code", ""))
            status = metadata.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if code not in {
                "PreconditionFailed",
                "ConditionalRequestConflict",
            } and status not in {409, 412}:
                raise
            existing = self.read_optional()
            if (
                existing is None
                or existing.immutable_result() != manifest.immutable_result()
            ):
                raise RuntimeError(
                    f"immutable index manifest conflicts: {self.uri}"
                ) from exc
            return existing
        published = self.read_optional()
        if (
            published is None
            or published.immutable_result() != manifest.immutable_result()
        ):
            raise RuntimeError("published index manifest could not be verified")
        return published


def completed_manifest(
    *,
    snapshot_set: SnapshotSet,
    snapshot_set_object_ref: ObjectRef,
    build_id: str,
    config_digest: str,
    document_count: int,
    index_name: str,
    alias: str,
    started_at: str,
) -> IndexBuildManifest:
    return IndexBuildManifest(
        status="COMPLETED",
        build_id=build_id,
        source_snapshot_set_id=snapshot_set.snapshot_set_id,
        source_snapshot_set_uri=snapshot_set_object_ref.uri,
        source_snapshot_set=snapshot_set_object_ref,
        table_snapshots=table_snapshot_identity(snapshot_set),
        mapping_digest=MAPPING_DIGEST,
        config_digest=config_digest,
        document_count=document_count,
        error_count=0,
        index=index_name,
        alias=alias,
        started_at=started_at,
        completed_at=_now(),
    )


def failed_manifest(
    *,
    snapshot_set: SnapshotSet,
    snapshot_set_object_ref: ObjectRef,
    build_id: str,
    config_digest: str,
    document_count: int,
    error_count: int,
    index_name: str,
    alias: str,
    started_at: str,
) -> IndexBuildManifest:
    return IndexBuildManifest(
        status="FAILED",
        build_id=build_id,
        source_snapshot_set_id=snapshot_set.snapshot_set_id,
        source_snapshot_set_uri=snapshot_set_object_ref.uri,
        source_snapshot_set=snapshot_set_object_ref,
        table_snapshots=table_snapshot_identity(snapshot_set),
        mapping_digest=MAPPING_DIGEST,
        config_digest=config_digest,
        document_count=document_count,
        error_count=error_count,
        index=index_name,
        alias=alias,
        started_at=started_at,
        completed_at=_now(),
    )
