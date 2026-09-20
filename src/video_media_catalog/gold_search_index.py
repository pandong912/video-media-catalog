"""Strict shadow OpenSearch index contract for Gold v2 documents."""

from __future__ import annotations

import base64
import copy
import hashlib
import json
import queue
import re
import threading
import time
from collections.abc import Callable, Iterable, Mapping
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any, Literal, Self
from urllib.parse import urlsplit

from pydantic import Field, field_validator, model_validator

from video_media_catalog.canonical import canonical_json_bytes
from video_media_catalog.gold import RESEARCH_CONTEXT_ID
from video_media_catalog.gold_ingest import (
    GOLD_RELEASE_COMMIT_MEDIA_TYPE,
    GoldReleaseCommit,
)
from video_media_catalog.gold_tables import GOLD_DATA_COLUMNS
from video_media_catalog.models import ObjectRef
from video_media_catalog.object_store import (
    BoundedObjectStore,
    ObjectStoreError,
    S3Location,
)
from video_media_catalog.opensearch_client import (
    OpenSearchConnection,
    create_opensearch_client,
)
from video_media_catalog.runtime_args import join_uri
from video_media_catalog.search_index import BulkResult, current_alias_indices
from video_media_catalog.storage import ImmutableObjectConflictError, local_path
from video_media_catalog.v2_contracts import (
    V2ContractModel,
    require_oidc_subject,
    require_rfc3339,
    require_sha256,
)

RESEARCH_READ_ALIAS = "media-catalog-research-read"
RESEARCH_INDEX_PREFIX = "media-catalog-research"
PROJECTION_VERSION = "5"
DEFAULT_GOLD_BULK_PARTITIONS = 32
DEFAULT_GOLD_BULK_WORKERS = 2
MAX_GOLD_BULK_PARTITIONS = 4096
MAX_GOLD_BULK_WORKERS = 32
MAX_GOLD_BULK_CHUNK_ACTIONS = 10_000
MAX_GOLD_BULK_CHUNK_BYTES = 100 * 1024 * 1024
MAX_GOLD_INCREMENTAL_TASK_SECONDS = 24 * 60 * 60
MAX_GOLD_CONTROL_BYTES = 16 * 1024 * 1024
GOLD_PARTITION_RECEIPT_MEDIA_TYPE = (
    "application/vnd.video-media-catalog.gold-index-partition-receipt.v2+json"
)
GOLD_AFFECTED_ENTITY_MANIFEST_MEDIA_TYPE = (
    "application/vnd.video-media-catalog.gold-affected-entities.v2+json"
)

_SAFE_NAME = re.compile(r"^[a-z0-9][a-z0-9_-]{0,254}$")

_MAPPINGS: dict[str, Any] = {
    "dynamic": "strict",
    "properties": {
        "entityKey": {"type": "keyword"},
        "entityLevel": {"type": "keyword"},
        "entityKind": {"type": "keyword"},
        "status": {"type": "keyword"},
        "releasePlanId": {"type": "keyword"},
        "contextId": {"type": "keyword"},
        "ownerSubject": {"type": "keyword"},
        "displayName": {
            "type": "text",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 1024}},
        },
        "displayLanguage": {"type": "keyword"},
        "titles": {
            "type": "nested",
            "dynamic": "strict",
            "properties": {
                "value": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 1024,
                        }
                    },
                },
                "language": {"type": "keyword"},
                "titleRole": {"type": "keyword"},
            },
        },
        "attributes": {
            "type": "object",
            "dynamic": "strict",
            "properties": {
                "formats": {"type": "keyword"},
                "languages": {"type": "keyword"},
                "statuses": {"type": "keyword"},
                "premiered": {"type": "date", "format": "strict_date"},
                "ended": {"type": "date", "format": "strict_date"},
                "runtimeMinutes": {"type": "integer"},
                "averageRuntimeMinutes": {"type": "integer"},
                "genres": {"type": "keyword"},
            },
        },
        "externalIdentifiers": {
            "type": "nested",
            "dynamic": "strict",
            "properties": {
                "namespace": {"type": "keyword"},
                "value": {"type": "keyword", "ignore_above": 1024},
                "issuer": {"type": "keyword"},
                "referentKind": {"type": "keyword"},
                "url": {"type": "keyword", "index": False},
            },
        },
        "relationSummary": {
            "type": "nested",
            "dynamic": "strict",
            "properties": {
                "predicate": {"type": "keyword"},
                "count": {"type": "long"},
            },
        },
        "sourceBadges": {
            "type": "nested",
            "dynamic": "strict",
            "properties": {
                "sourceProductId": {"type": "keyword"},
                "displayName": {"type": "keyword", "ignore_above": 512},
                "sourceUrl": {"type": "keyword", "ignore_above": 2048},
                "policyZones": {"type": "keyword"},
                "assertionCount": {"type": "long"},
                "winningAssertionCount": {"type": "long"},
            },
        },
        "winningAssertions": {
            "type": "nested",
            "dynamic": "strict",
            "properties": {
                "kind": {"type": "keyword"},
                "assertionId": {"type": "keyword"},
                "predicate": {"type": "keyword"},
                "valueJson": {"type": "keyword", "ignore_above": 4096},
                "qualifiersJson": {"type": "keyword", "ignore_above": 4096},
                "resolutionStatus": {"type": "keyword"},
                "sourceProductId": {"type": "keyword"},
                "sourceRecordId": {"type": "keyword", "ignore_above": 2048},
                "sourcePath": {"type": "keyword", "ignore_above": 2048},
                "observedAt": {"type": "date", "format": "strict_date_time"},
                "citationKeys": {"type": "keyword"},
                "citationOverflow": {"type": "integer"},
            },
        },
        "rights": {
            "type": "nested",
            "dynamic": "strict",
            "properties": {
                "sourceProductId": {"type": "keyword"},
                "policyId": {"type": "keyword"},
                "policyZone": {"type": "keyword"},
                "licenseId": {"type": "keyword"},
                "licenseUri": {"type": "keyword", "ignore_above": 2048},
                "attributionText": {
                    "type": "keyword",
                    "ignore_above": 2048,
                },
                "sourceUrl": {"type": "keyword", "ignore_above": 2048},
                "shareAlike": {"type": "boolean"},
            },
        },
        "conflictCount": {"type": "long"},
        "conflictPredicates": {"type": "keyword"},
        "conflicts": {
            "type": "nested",
            "dynamic": "strict",
            "properties": {
                "predicate": {"type": "keyword"},
                "scopeHash": {"type": "keyword"},
                "reason": {"type": "keyword"},
                "assertionIds": {"type": "keyword"},
                "assertionOverflow": {"type": "integer"},
                "candidateValuesJson": {
                    "type": "keyword",
                    "ignore_above": 4096,
                },
                "candidateValueOverflow": {"type": "integer"},
                "sourceProductIds": {"type": "keyword"},
            },
        },
        "sourceNodeCount": {"type": "long"},
        "overflow": {
            "type": "object",
            "dynamic": "strict",
            "properties": {
                "titles": {"type": "integer"},
                "externalIdentifiers": {"type": "integer"},
                "relationTypes": {"type": "integer"},
                "sourceBadges": {"type": "integer"},
                "winningAssertions": {"type": "integer"},
                "citationKeys": {"type": "integer"},
                "rights": {"type": "integer"},
                "conflicts": {"type": "integer"},
                "formats": {"type": "integer"},
                "languages": {"type": "integer"},
                "statuses": {"type": "integer"},
                "premiered": {"type": "integer"},
                "ended": {"type": "integer"},
                "runtimeMinutes": {"type": "integer"},
                "averageRuntimeMinutes": {"type": "integer"},
                "genres": {"type": "integer"},
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


def _safe_name(value: str, *, label: str) -> str:
    if _SAFE_NAME.fullmatch(value) is None:
        raise ValueError(f"{label} is not a safe OpenSearch name")
    return value


class GoldIndexConfigIdentity(V2ContractModel):
    projection_version: Literal["5"] = PROJECTION_VERSION
    mapping_digest: str = MAPPING_DIGEST
    context_id: Literal["research"] = RESEARCH_CONTEXT_ID
    owner_subject: str
    read_alias: str
    index_prefix: str
    shards: int = Field(ge=1)
    replicas: int = Field(ge=0)
    bulk_chunk_size: int = Field(ge=1, le=MAX_GOLD_BULK_CHUNK_ACTIONS)
    bulk_max_chunk_bytes: int = Field(ge=1, le=MAX_GOLD_BULK_CHUNK_BYTES)
    image_digest: str

    @field_validator("mapping_digest", "image_digest")
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("owner_subject")
    @classmethod
    def validate_owner_subject(cls, value: str) -> str:
        return require_oidc_subject(value)

    @field_validator("read_alias", "index_prefix")
    @classmethod
    def validate_names(cls, value: str) -> str:
        return _safe_name(value, label="OpenSearch name")

    @model_validator(mode="after")
    def validate_research_identity(self) -> Self:
        if (
            self.read_alias != RESEARCH_READ_ALIAS
            or self.index_prefix != RESEARCH_INDEX_PREFIX
        ):
            raise ValueError("research index and alias names are fixed")
        if self.mapping_digest != MAPPING_DIGEST:
            raise ValueError("Gold index config must use the research index contract")
        return self

    @property
    def digest(self) -> str:
        payload = self.model_dump(mode="json", by_alias=True, exclude_none=True)
        return "sha256:" + hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def gold_index_config_identity(
    *,
    read_alias: str,
    index_prefix: str,
    owner_subject: str,
    shards: int,
    replicas: int,
    bulk_chunk_size: int,
    bulk_max_chunk_bytes: int,
    image_digest: str,
) -> GoldIndexConfigIdentity:
    return GoldIndexConfigIdentity(
        owner_subject=owner_subject,
        read_alias=read_alias,
        index_prefix=index_prefix,
        shards=shards,
        replicas=replicas,
        bulk_chunk_size=bulk_chunk_size,
        bulk_max_chunk_bytes=bulk_max_chunk_bytes,
        image_digest=image_digest,
    )


def gold_index_config_digest(
    *,
    read_alias: str,
    index_prefix: str,
    owner_subject: str,
    shards: int,
    replicas: int,
    bulk_chunk_size: int,
    bulk_max_chunk_bytes: int,
    image_digest: str,
) -> str:
    return gold_index_config_identity(
        read_alias=read_alias,
        index_prefix=index_prefix,
        owner_subject=owner_subject,
        shards=shards,
        replicas=replicas,
        bulk_chunk_size=bulk_chunk_size,
        bulk_max_chunk_bytes=bulk_max_chunk_bytes,
        image_digest=image_digest,
    ).digest


def _validate_immutable_control_ref(
    reference: ObjectRef,
    *,
    media_type: str,
    label: str,
) -> None:
    if (
        reference.format != "OBJECT_FORMAT_JSON"
        or reference.media_type != media_type
        or not 0 < reference.size_bytes <= MAX_GOLD_CONTROL_BYTES
    ):
        raise ValueError(f"{label} must be a bounded JSON object")
    if reference.uri.startswith("s3://") and (
        reference.etag is None or reference.object_version is None
    ):
        raise ValueError(f"S3 {label} must be immutable")


class GoldAffectedEntityOperation(V2ContractModel):
    entity_key: str
    operation: Literal["UPSERT", "DELETE"]

    @field_validator("entity_key")
    @classmethod
    def validate_entity_key(cls, value: str) -> str:
        return require_sha256(value, label="entity_key")


class GoldAffectedEntityManifest(V2ContractModel):
    """Immutable affected-entity set for the opt-in incremental build path."""

    schema_version: Literal["2.0"] = "2.0"
    release_plan_id: str
    owner_subject: str
    context_id: Literal["research"] = RESEARCH_CONTEXT_ID
    release_commit: ObjectRef
    base_index: str
    base_release_plan_id: str
    base_document_count: int = Field(ge=0)
    operations: list[GoldAffectedEntityOperation]
    generated_at: str

    @field_validator("release_plan_id", "base_release_plan_id")
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("owner_subject")
    @classmethod
    def validate_owner_subject(cls, value: str) -> str:
        return require_oidc_subject(value)

    @field_validator("base_index")
    @classmethod
    def validate_base_index(cls, value: str) -> str:
        value = _safe_name(value, label="base index")
        if not value.startswith(RESEARCH_INDEX_PREFIX + "-"):
            raise ValueError("base index must belong to the research index family")
        return value

    @field_validator("operations")
    @classmethod
    def validate_operations(
        cls,
        value: list[GoldAffectedEntityOperation],
    ) -> list[GoldAffectedEntityOperation]:
        keys = [item.entity_key for item in value]
        if keys != sorted(keys) or len(keys) != len(set(keys)):
            raise ValueError(
                "affected entity operations must be unique and sorted by entity key"
            )
        return value

    @field_validator("generated_at")
    @classmethod
    def validate_generated_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @model_validator(mode="after")
    def validate_release_reference(self) -> Self:
        _validate_immutable_control_ref(
            self.release_commit,
            media_type=GOLD_RELEASE_COMMIT_MEDIA_TYPE,
            label="release commit",
        )
        return self

    @property
    def digest(self) -> str:
        return "sha256:" + hashlib.sha256(self.json_bytes()).hexdigest()

    @property
    def upsert_entity_keys(self) -> tuple[str, ...]:
        return tuple(
            item.entity_key for item in self.operations if item.operation == "UPSERT"
        )

    @property
    def delete_entity_keys(self) -> tuple[str, ...]:
        return tuple(
            item.entity_key for item in self.operations if item.operation == "DELETE"
        )


class GoldIndexPartitionReceipt(V2ContractModel):
    """One immutable, content-bound receipt for a deterministic Spark partition."""

    schema_version: Literal["2.0"] = "2.0"
    status: Literal["COMPLETED"] = "COMPLETED"
    build_id: str
    partition_id: int = Field(ge=0)
    partition_count: int = Field(ge=1)
    operation: Literal["FULL", "UPSERT", "DELETE", "BASELINE"]
    release_commit: ObjectRef
    affected_entity_manifest: ObjectRef | None = None
    mapping_digest: str
    config_identity: GoldIndexConfigIdentity
    config_digest: str
    image_digest: str
    index: str
    input_document_count: int = Field(ge=0)
    successful_document_count: int = Field(ge=0)
    failed_document_count: Literal[0] = 0
    input_digest: str

    @field_validator("build_id")
    @classmethod
    def validate_build_id(cls, value: str) -> str:
        if re.fullmatch(r"[0-9a-f]{64}", value) is None:
            raise ValueError("build_id must be lowercase SHA-256 hex")
        return value

    @field_validator(
        "mapping_digest",
        "config_digest",
        "image_digest",
        "input_digest",
    )
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("index")
    @classmethod
    def validate_index(cls, value: str) -> str:
        value = _safe_name(value, label="index")
        if not value.startswith(RESEARCH_INDEX_PREFIX + "-"):
            raise ValueError("partition receipt index must be in the research family")
        return value

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        if self.partition_id >= self.partition_count:
            raise ValueError("partition_id must be smaller than partition_count")
        _validate_immutable_control_ref(
            self.release_commit,
            media_type=GOLD_RELEASE_COMMIT_MEDIA_TYPE,
            label="release commit",
        )
        incremental = self.operation != "FULL"
        if incremental != (self.affected_entity_manifest is not None):
            raise ValueError(
                "incremental receipts must bind exactly one affected-entity manifest"
            )
        if self.affected_entity_manifest is not None:
            _validate_immutable_control_ref(
                self.affected_entity_manifest,
                media_type=GOLD_AFFECTED_ENTITY_MANIFEST_MEDIA_TYPE,
                label="affected-entity manifest",
            )
        if (
            self.mapping_digest != MAPPING_DIGEST
            or self.config_identity.mapping_digest != self.mapping_digest
            or self.config_digest != self.config_identity.digest
            or self.image_digest != self.config_identity.image_digest
            or self.successful_document_count != self.input_document_count
        ):
            raise ValueError("partition receipt build identity or counts do not match")
        return self


def derive_gold_build_id(
    *,
    commit: GoldReleaseCommit,
    config_digest: str,
) -> str:
    require_sha256(config_digest, label="config_digest")
    return hashlib.sha256(
        canonical_json_bytes(
            {
                "commitKey": commit.commit_key,
                "releasePlanId": commit.release_plan_id,
                "ownerSubject": commit.owner_subject,
                "contextId": commit.context_id,
                "tableSnapshotIds": commit.table_snapshot_ids,
                "mappingDigest": MAPPING_DIGEST,
                "configDigest": config_digest,
            }
        )
    ).hexdigest()


def gold_index_name(prefix: str, build_id: str) -> str:
    _safe_name(prefix, label="index prefix")
    if re.fullmatch(r"[0-9a-f]{64}", build_id) is None:
        raise ValueError("build_id must be lowercase SHA-256 hex")
    return _safe_name(f"{prefix}-{build_id[:24]}", label="index name")


def gold_index_definition(
    *,
    owner_subject: str,
    shards: int,
    replicas: int,
) -> dict[str, Any]:
    if shards < 1 or replicas < 0:
        raise ValueError("invalid shard or replica count")
    mappings = copy.deepcopy(INDEX_MAPPINGS)
    mappings["_meta"]["ownerSubject"] = require_oidc_subject(owner_subject)
    return {
        "settings": {
            "index": {
                "number_of_shards": shards,
                "number_of_replicas": replicas,
            }
        },
        "mappings": mappings,
    }


def _status_code(exc: Exception) -> int | None:
    status = getattr(exc, "status_code", None)
    if isinstance(status, int):
        return status
    info = getattr(exc, "info", None)
    if isinstance(info, Mapping) and isinstance(info.get("status"), int):
        return int(info["status"])
    return None


def _existing_mapping_metadata(
    client: Any,
    index_name: str,
) -> Mapping[str, Any] | None:
    response = client.indices.get_mapping(index=index_name)
    if not isinstance(response, Mapping):
        return None
    value = response.get(index_name)
    if value is None and len(response) == 1:
        value = next(iter(response.values()))
    if not isinstance(value, Mapping):
        return None
    mappings = value.get("mappings")
    metadata = mappings.get("_meta") if isinstance(mappings, Mapping) else None
    return metadata if isinstance(metadata, Mapping) else None


def _has_compatible_mapping(
    client: Any,
    *,
    index_name: str,
    owner_subject: str,
) -> bool:
    metadata = _existing_mapping_metadata(client, index_name)
    return bool(
        metadata is not None
        and metadata.get("mappingDigest") == MAPPING_DIGEST
        and metadata.get("ownerSubject") == owner_subject
    )


def ensure_gold_index(
    client: Any,
    *,
    index_name: str,
    owner_subject: str,
    shards: int,
    replicas: int,
) -> bool:
    owner = require_oidc_subject(owner_subject)
    if client.indices.exists(index=index_name):
        if not _has_compatible_mapping(
            client,
            index_name=index_name,
            owner_subject=owner,
        ):
            raise RuntimeError("existing Gold index mapping or owner is incompatible")
        return False
    try:
        client.indices.create(
            index=index_name,
            body=gold_index_definition(
                owner_subject=owner,
                shards=shards,
                replicas=replicas,
            ),
        )
        return True
    except Exception as exc:
        if _status_code(exc) not in {400, 409} or not client.indices.exists(
            index=index_name
        ):
            raise
        if not _has_compatible_mapping(
            client,
            index_name=index_name,
            owner_subject=owner,
        ):
            raise RuntimeError(
                "concurrently created Gold index mapping or owner is incompatible"
            ) from exc
        return False


def gold_partition_receipt_uri(
    prefix: str,
    *,
    build_id: str,
    operation: str,
    partition_id: int,
) -> str:
    if re.fullmatch(r"[0-9a-f]{64}", build_id) is None:
        raise ValueError("build_id must be lowercase SHA-256 hex")
    normalized_operation = operation.lower()
    if normalized_operation not in {"full", "upsert", "delete", "baseline"}:
        raise ValueError("unsupported Gold partition operation")
    if partition_id < 0:
        raise ValueError("partition_id must be non-negative")
    return join_uri(
        prefix,
        "checkpoints",
        build_id,
        normalized_operation,
        f"part-{partition_id:06d}.json",
    )


class GoldPartitionReceiptPublisher:
    """Read and conditionally publish one bounded immutable partition receipt."""

    def __init__(
        self,
        uri: str,
        *,
        aws_region: str | None = None,
        endpoint_url: str | None = None,
        path_style_access: bool = False,
        client: Any | None = None,
    ) -> None:
        scheme = urlsplit(uri).scheme
        if scheme not in {"file", "s3"}:
            raise ValueError("partition receipt URI must use file:// or s3://")
        self.uri = uri
        if client is None and scheme == "file":
            client = object()
        self.store = BoundedObjectStore(
            region=aws_region,
            endpoint_url=endpoint_url,
            path_style_access=path_style_access,
            client=client,
        )

    def _read_payload(self) -> bytes | None:
        if urlsplit(self.uri).scheme == "file":
            path = local_path(self.uri).resolve()
            if not path.exists():
                return None
            if path.stat().st_size > MAX_GOLD_CONTROL_BYTES:
                raise ValueError("Gold partition receipt exceeds maximum size")
            return path.read_bytes()
        location = S3Location.parse(self.uri)
        try:
            response = self.store.client.get_object(
                Bucket=location.bucket,
                Key=location.key,
                ChecksumMode="ENABLED",
            )
        except Exception as exc:
            metadata = getattr(exc, "response", {})
            code = str(metadata.get("Error", {}).get("Code", ""))
            status = metadata.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if code in {"404", "NoSuchKey", "NotFound"} or status == 404:
                return None
            raise
        body = response.get("Body")
        if body is None or not hasattr(body, "read"):
            raise RuntimeError("Gold partition receipt has no readable body")
        declared = response.get("ContentLength")
        if isinstance(declared, int) and declared > MAX_GOLD_CONTROL_BYTES:
            body.close()
            raise ValueError("Gold partition receipt exceeds maximum size")
        try:
            payload = body.read(MAX_GOLD_CONTROL_BYTES + 1)
        finally:
            body.close()
        if not isinstance(payload, bytes) or len(payload) > MAX_GOLD_CONTROL_BYTES:
            raise ValueError("Gold partition receipt exceeds maximum size")
        digest = hashlib.sha256(payload).hexdigest()
        native = response.get("ChecksumSHA256")
        metadata_digest = (response.get("Metadata") or {}).get("sha256")
        if native is None and metadata_digest is None:
            raise RuntimeError("Gold partition receipt checksum is unavailable")
        if native is not None and native != base64.b64encode(
            bytes.fromhex(digest)
        ).decode("ascii"):
            raise RuntimeError("Gold partition receipt checksum mismatch")
        if metadata_digest is not None and metadata_digest.lower() != digest:
            raise RuntimeError("Gold partition receipt checksum mismatch")
        return payload

    def read_optional(self) -> GoldIndexPartitionReceipt | None:
        payload = self._read_payload()
        if payload is None:
            return None
        return GoldIndexPartitionReceipt.model_validate_json(payload)

    def publish(
        self,
        receipt: GoldIndexPartitionReceipt,
    ) -> GoldIndexPartitionReceipt:
        payload = receipt.json_bytes()
        try:
            self.store.upload_bytes(
                payload,
                self.uri,
                media_type=GOLD_PARTITION_RECEIPT_MEDIA_TYPE,
                object_format="OBJECT_FORMAT_JSON",
                max_bytes=MAX_GOLD_CONTROL_BYTES,
            )
        except (ImmutableObjectConflictError, ObjectStoreError) as exc:
            if not isinstance(exc, ImmutableObjectConflictError) and (
                exc.code != "IMMUTABLE_OBJECT_CONFLICT"
            ):
                raise
            existing = self.read_optional()
            if existing != receipt:
                raise RuntimeError(
                    f"immutable Gold partition receipt conflicts: {self.uri}"
                ) from exc
            return existing
        existing = self.read_optional()
        if existing != receipt:
            raise RuntimeError("published Gold partition receipt could not be verified")
        return existing


class _CommutativeActionDigest:
    def __init__(self) -> None:
        self.count = 0
        self.xor = 0
        self.total = 0

    def update(self, identity: Mapping[str, Any]) -> None:
        value = int.from_bytes(
            hashlib.sha256(canonical_json_bytes(identity)).digest(),
            "big",
        )
        self.count += 1
        self.xor ^= value
        self.total = (self.total + value) % (1 << 256)

    @property
    def digest(self) -> str:
        payload = {
            "count": self.count,
            "sum": f"{self.total:064x}",
            "xor": f"{self.xor:064x}",
        }
        return "sha256:" + hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def _gold_document(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if hasattr(value, "asDict"):
        return dict(value.asDict(recursive=True))
    raise TypeError(f"unsupported Gold search document row: {type(value).__name__}")


def _gold_bulk_action(
    value: Any,
    *,
    index_name: str,
    operation: Literal["FULL", "UPSERT", "DELETE"],
) -> tuple[dict[str, Any], dict[str, Any]]:
    document = _gold_document(value)
    entity_key = document.get("entityKey")
    if not isinstance(entity_key, str) or not entity_key:
        raise ValueError("projected Gold document has no entityKey")
    if operation == "DELETE":
        action = {
            "_op_type": "delete",
            "_index": index_name,
            "_id": entity_key,
        }
        identity = {"entityKey": entity_key, "operation": operation}
        return action, identity
    action = {
        "_op_type": "index",
        "_index": index_name,
        "_id": entity_key,
        "_source": document,
    }
    identity = {
        "document": document,
        "entityKey": entity_key,
        "operation": operation,
    }
    return action, identity


def _bulk_wire_bytes(action: Mapping[str, Any]) -> int:
    operation = str(action["_op_type"])
    metadata = {
        operation: {
            "_id": action["_id"],
            "_index": action["_index"],
        }
    }
    size = (
        len(
            json.dumps(
                metadata,
                ensure_ascii=True,
                separators=(",", ":"),
            ).encode("utf-8")
        )
        + 1
    )
    if operation != "delete":
        size += (
            len(
                json.dumps(
                    action["_source"],
                    ensure_ascii=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            )
            + 1
        )
    return size


def _bulk_actions(
    actions: Iterable[dict[str, Any]],
    *,
    client: Any,
    chunk_size: int,
    max_chunk_bytes: int,
    request_timeout: float,
    streaming_bulk: Callable[..., Iterable[tuple[bool, dict[str, Any]]]] | None = None,
) -> BulkResult:
    if streaming_bulk is None:
        from opensearchpy.helpers import streaming_bulk

    result = BulkResult()
    for succeeded, item in streaming_bulk(
        client,
        actions,
        chunk_size=chunk_size,
        max_chunk_bytes=max_chunk_bytes,
        max_retries=3,
        initial_backoff=1,
        max_backoff=8,
        raise_on_error=False,
        raise_on_exception=False,
        yield_ok=True,
        request_timeout=request_timeout,
    ):
        operation_name = next(iter(item), "")
        operation = item.get(operation_name, {}) if item else {}
        delete_not_found = (
            operation_name == "delete"
            and isinstance(operation, Mapping)
            and operation.get("status") == 404
        )
        if succeeded or delete_not_found:
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


def _close_client(client: Any) -> None:
    transport = getattr(client, "transport", None)
    if transport is not None and hasattr(transport, "close"):
        transport.close()


@dataclass(frozen=True)
class GoldPartitionBulkResult:
    input_document_count: int
    input_digest: str
    bulk_result: BulkResult


def _prepared_actions(
    rows: Iterable[Any],
    *,
    index_name: str,
    operation: Literal["FULL", "UPSERT", "DELETE"],
    max_chunk_bytes: int,
    accumulator: _CommutativeActionDigest,
) -> Iterable[dict[str, Any]]:
    for row in rows:
        action, identity = _gold_bulk_action(
            row,
            index_name=index_name,
            operation=operation,
        )
        if _bulk_wire_bytes(action) > max_chunk_bytes:
            raise ValueError(
                f"Gold document {action['_id']} exceeds bulk-max-chunk-bytes"
            )
        accumulator.update(identity)
        yield action


def summarize_gold_partition(
    rows: Iterable[Any],
    *,
    index_name: str,
    operation: Literal["FULL", "UPSERT", "DELETE"],
    max_chunk_bytes: int,
) -> tuple[int, str]:
    accumulator = _CommutativeActionDigest()
    for _ in _prepared_actions(
        rows,
        index_name=index_name,
        operation=operation,
        max_chunk_bytes=max_chunk_bytes,
        accumulator=accumulator,
    ):
        pass
    return accumulator.count, accumulator.digest


def gold_bulk_partition(
    rows: Iterable[Any],
    *,
    connection: OpenSearchConnection,
    index_name: str,
    operation: Literal["FULL", "UPSERT", "DELETE"],
    chunk_size: int,
    max_chunk_bytes: int,
    workers: int,
    client_factory: Callable[[OpenSearchConnection], Any] = create_opensearch_client,
    streaming_bulk: Callable[..., Iterable[tuple[bool, dict[str, Any]]]] | None = None,
) -> GoldPartitionBulkResult:
    """Stream one Spark partition through bounded retrying worker queues."""

    if not 1 <= chunk_size <= MAX_GOLD_BULK_CHUNK_ACTIONS:
        raise ValueError(
            f"Gold bulk chunk size must be between 1 and {MAX_GOLD_BULK_CHUNK_ACTIONS}"
        )
    if not 1 <= max_chunk_bytes <= MAX_GOLD_BULK_CHUNK_BYTES:
        raise ValueError(
            f"Gold bulk chunk bytes must be between 1 and {MAX_GOLD_BULK_CHUNK_BYTES}"
        )
    if not 1 <= workers <= MAX_GOLD_BULK_WORKERS:
        raise ValueError(
            f"Gold bulk workers must be between 1 and {MAX_GOLD_BULK_WORKERS}"
        )
    accumulator = _CommutativeActionDigest()
    actions = _prepared_actions(
        rows,
        index_name=index_name,
        operation=operation,
        max_chunk_bytes=max_chunk_bytes,
        accumulator=accumulator,
    )
    if workers == 1:
        client = client_factory(connection)
        try:
            bulk_result = _bulk_actions(
                actions,
                client=client,
                chunk_size=chunk_size,
                max_chunk_bytes=max_chunk_bytes,
                request_timeout=connection.timeout_seconds,
                streaming_bulk=streaming_bulk,
            )
        finally:
            _close_client(client)
        return GoldPartitionBulkResult(
            input_document_count=accumulator.count,
            input_digest=accumulator.digest,
            bulk_result=bulk_result,
        )

    work_queue: queue.Queue[object] = queue.Queue(
        maxsize=max(workers, workers * min(chunk_size, 128))
    )
    sentinel = object()
    stopped = threading.Event()

    def queued_actions() -> Iterable[dict[str, Any]]:
        while not stopped.is_set():
            try:
                item = work_queue.get(timeout=0.1)
            except queue.Empty:
                continue
            if item is sentinel:
                return
            if not isinstance(item, dict):
                raise TypeError("Gold bulk worker received an invalid action")
            yield item

    def run_worker() -> BulkResult:
        client = client_factory(connection)
        try:
            return _bulk_actions(
                queued_actions(),
                client=client,
                chunk_size=chunk_size,
                max_chunk_bytes=max_chunk_bytes,
                request_timeout=connection.timeout_seconds,
                streaming_bulk=streaming_bulk,
            )
        finally:
            _close_client(client)

    executor = ThreadPoolExecutor(
        max_workers=workers,
        thread_name_prefix="gold-bulk",
    )
    futures: list[Future[BulkResult]] = [
        executor.submit(run_worker) for _ in range(workers)
    ]

    def put(item: object) -> None:
        while True:
            for future in futures:
                exception = future.exception() if future.done() else None
                if exception is not None:
                    raise exception
            try:
                work_queue.put(item, timeout=0.1)
                return
            except queue.Full:
                continue

    try:
        for action in actions:
            put(action)
        for _ in futures:
            put(sentinel)
        bulk_result = BulkResult()
        for future in futures:
            bulk_result.merge(future.result())
    except BaseException:
        stopped.set()
        executor.shutdown(wait=True, cancel_futures=True)
        raise
    else:
        executor.shutdown(wait=True)
    return GoldPartitionBulkResult(
        input_document_count=accumulator.count,
        input_digest=accumulator.digest,
        bulk_result=bulk_result,
    )


@dataclass
class GoldDistributedBulkResult:
    input_document_count: int = 0
    document_count: int = 0
    error_count: int = 0
    errors: list[dict[str, Any]] = field(default_factory=list)
    partition_receipts: list[GoldIndexPartitionReceipt] = field(default_factory=list)
    reused_partition_count: int = 0

    @property
    def receipt_set_digest(self) -> str:
        return gold_partition_receipt_set_digest(self.partition_receipts)

    def merge_summary(self, value: Mapping[str, Any]) -> None:
        self.input_document_count += int(value["inputDocumentCount"])
        self.document_count += int(value["documentCount"])
        self.error_count += int(value["errorCount"])
        remaining = max(0, 20 - len(self.errors))
        self.errors.extend(list(value.get("errors") or [])[:remaining])
        receipt = value.get("receipt")
        if receipt is not None:
            self.partition_receipts.append(
                GoldIndexPartitionReceipt.model_validate(receipt)
            )
        self.reused_partition_count += int(bool(value.get("reused")))


def gold_partition_receipt_set_digest(
    receipts: Iterable[GoldIndexPartitionReceipt],
) -> str:
    values = [
        receipt.model_dump(mode="json", by_alias=True, exclude_none=True)
        for receipt in sorted(
            receipts,
            key=lambda value: (value.operation, value.partition_id),
        )
    ]
    return "sha256:" + hashlib.sha256(canonical_json_bytes(values)).hexdigest()


def _receipt_identity_matches(
    receipt: GoldIndexPartitionReceipt,
    *,
    build_id: str,
    partition_id: int,
    partition_count: int,
    operation: Literal["FULL", "UPSERT", "DELETE"],
    release_commit: ObjectRef,
    affected_entity_manifest: ObjectRef | None,
    config_identity: GoldIndexConfigIdentity,
    index_name: str,
) -> bool:
    return bool(
        receipt.build_id == build_id
        and receipt.partition_id == partition_id
        and receipt.partition_count == partition_count
        and receipt.operation == operation
        and receipt.release_commit == release_commit
        and receipt.affected_entity_manifest == affected_entity_manifest
        and receipt.mapping_digest == MAPPING_DIGEST
        and receipt.config_identity == config_identity
        and receipt.config_digest == config_identity.digest
        and receipt.image_digest == config_identity.image_digest
        and receipt.index == index_name
    )


def _index_gold_spark_partition(
    partition_id: int,
    rows: Iterable[Any],
    *,
    connection: OpenSearchConnection,
    index_name: str,
    build_id: str,
    operation: Literal["FULL", "UPSERT", "DELETE"],
    release_commit: ObjectRef,
    affected_entity_manifest: ObjectRef | None,
    config_identity: GoldIndexConfigIdentity,
    receipt_prefix: str,
    partition_count: int,
    workers: int,
    aws_region: str | None,
    s3_endpoint: str | None,
    s3_path_style_access: bool,
) -> Iterable[dict[str, Any]]:
    uri = gold_partition_receipt_uri(
        receipt_prefix,
        build_id=build_id,
        operation=operation,
        partition_id=partition_id,
    )
    publisher = GoldPartitionReceiptPublisher(
        uri,
        aws_region=aws_region,
        endpoint_url=s3_endpoint,
        path_style_access=s3_path_style_access,
    )
    existing = publisher.read_optional()
    if existing is not None:
        if not _receipt_identity_matches(
            existing,
            build_id=build_id,
            partition_id=partition_id,
            partition_count=partition_count,
            operation=operation,
            release_commit=release_commit,
            affected_entity_manifest=affected_entity_manifest,
            config_identity=config_identity,
            index_name=index_name,
        ):
            raise RuntimeError(f"immutable Gold partition receipt conflicts: {uri}")
        input_count, input_digest = summarize_gold_partition(
            rows,
            index_name=index_name,
            operation=operation,
            max_chunk_bytes=config_identity.bulk_max_chunk_bytes,
        )
        if (
            existing.input_document_count != input_count
            or existing.successful_document_count != input_count
            or existing.input_digest != input_digest
        ):
            raise RuntimeError(f"immutable Gold partition receipt conflicts: {uri}")
        yield {
            "inputDocumentCount": input_count,
            "documentCount": input_count,
            "errorCount": 0,
            "errors": [],
            "receipt": existing.model_dump(
                mode="json",
                by_alias=True,
                exclude_none=True,
            ),
            "reused": True,
        }
        return

    partition_result = gold_bulk_partition(
        rows,
        connection=connection,
        index_name=index_name,
        operation=operation,
        chunk_size=config_identity.bulk_chunk_size,
        max_chunk_bytes=config_identity.bulk_max_chunk_bytes,
        workers=workers,
    )
    bulk_result = partition_result.bulk_result
    receipt = None
    if (
        bulk_result.error_count == 0
        and bulk_result.document_count == partition_result.input_document_count
    ):
        receipt = publisher.publish(
            GoldIndexPartitionReceipt(
                build_id=build_id,
                partition_id=partition_id,
                partition_count=partition_count,
                operation=operation,
                release_commit=release_commit,
                affected_entity_manifest=affected_entity_manifest,
                mapping_digest=MAPPING_DIGEST,
                config_identity=config_identity,
                config_digest=config_identity.digest,
                image_digest=config_identity.image_digest,
                index=index_name,
                input_document_count=partition_result.input_document_count,
                successful_document_count=bulk_result.document_count,
                input_digest=partition_result.input_digest,
            )
        )
    yield {
        "inputDocumentCount": partition_result.input_document_count,
        "documentCount": bulk_result.document_count,
        "errorCount": bulk_result.error_count,
        "errors": bulk_result.errors,
        "receipt": (
            None
            if receipt is None
            else receipt.model_dump(mode="json", by_alias=True, exclude_none=True)
        ),
        "reused": False,
    }


def distributed_gold_bulk_index(
    documents: Any,
    *,
    connection: OpenSearchConnection,
    index_name: str,
    build_id: str,
    operation: Literal["FULL", "UPSERT", "DELETE"],
    release_commit: ObjectRef,
    affected_entity_manifest: ObjectRef | None,
    config_identity: GoldIndexConfigIdentity,
    receipt_prefix: str,
    partitions: int,
    workers: int,
    aws_region: str | None = None,
    s3_endpoint: str | None = None,
    s3_path_style_access: bool = False,
) -> GoldDistributedBulkResult:
    """Index deterministic Spark partitions and resume from immutable receipts."""

    if not 1 <= partitions <= MAX_GOLD_BULK_PARTITIONS:
        raise ValueError(
            f"Gold bulk partitions must be between 1 and {MAX_GOLD_BULK_PARTITIONS}"
        )
    if not 1 <= workers <= MAX_GOLD_BULK_WORKERS:
        raise ValueError(
            f"Gold bulk workers must be between 1 and {MAX_GOLD_BULK_WORKERS}"
        )
    if (operation == "FULL") != (affected_entity_manifest is None):
        raise ValueError(
            "affected-entity manifest is required only for incremental operations"
        )
    partitioned = documents.repartition(partitions, "entityKey")
    summaries = partitioned.rdd.mapPartitionsWithIndex(
        lambda partition_id, rows: _index_gold_spark_partition(
            partition_id,
            rows,
            connection=connection,
            index_name=index_name,
            build_id=build_id,
            operation=operation,
            release_commit=release_commit,
            affected_entity_manifest=affected_entity_manifest,
            config_identity=config_identity,
            receipt_prefix=receipt_prefix,
            partition_count=partitions,
            workers=workers,
            aws_region=aws_region,
            s3_endpoint=s3_endpoint,
            s3_path_style_access=s3_path_style_access,
        )
    ).collect()
    result = GoldDistributedBulkResult()
    for summary in summaries:
        result.merge_summary(summary)
    return result


def validate_gold_affected_entity_manifest(
    manifest: GoldAffectedEntityManifest,
    *,
    manifest_reference: ObjectRef,
    release_commit: GoldReleaseCommit,
    release_commit_reference: ObjectRef,
    owner_subject: str,
) -> None:
    _validate_immutable_control_ref(
        manifest_reference,
        media_type=GOLD_AFFECTED_ENTITY_MANIFEST_MEDIA_TYPE,
        label="affected-entity manifest",
    )
    owner = require_oidc_subject(owner_subject)
    if (
        manifest.release_commit != release_commit_reference
        or manifest.release_plan_id != release_commit.release_plan_id
        or manifest.owner_subject != owner
        or manifest.owner_subject != release_commit.owner_subject
        or manifest.context_id != release_commit.context_id
    ):
        raise ValueError(
            "affected-entity manifest does not match the target Gold release"
        )


def _validate_by_query_response(
    response: Any,
    *,
    expected_count: int,
    operation: str,
) -> None:
    if not isinstance(response, Mapping):
        raise RuntimeError(f"OpenSearch {operation} response is invalid")
    failures = response.get("failures")
    total = response.get("total")
    version_conflicts = response.get("version_conflicts", 0)
    if (
        response.get("timed_out")
        or failures
        or isinstance(total, bool)
        or not isinstance(total, int)
        or total != expected_count
        or isinstance(version_conflicts, bool)
        or not isinstance(version_conflicts, int)
        or version_conflicts != 0
    ):
        raise RuntimeError(
            f"OpenSearch {operation} failed closed: "
            f"total={total}, expected={expected_count}, failures={failures}"
        )
    completion_fields = (
        ("created", "updated") if operation == "reindex" else ("updated", "noops")
    )
    completion_values = [response.get(field, 0) for field in completion_fields]
    if any(
        isinstance(value, bool) or not isinstance(value, int) or value < 0
        for value in completion_values
    ):
        raise RuntimeError(f"OpenSearch {operation} response is invalid")
    completed = sum(completion_values)
    if completed != expected_count:
        raise RuntimeError(
            f"OpenSearch {operation} completion count mismatch: "
            f"completed={completed}, expected={expected_count}"
        )


def _await_opensearch_task(
    client: Any,
    submission: Any,
    *,
    operation: str,
    request_timeout: float,
    task_timeout_seconds: int,
) -> Mapping[str, Any]:
    if isinstance(submission, Mapping) and "total" in submission:
        return submission
    task_id = submission.get("task") if isinstance(submission, Mapping) else None
    if not isinstance(task_id, str) or not task_id:
        raise RuntimeError(f"OpenSearch {operation} task response is invalid")
    deadline = time.monotonic() + task_timeout_seconds
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            with suppress(Exception):
                client.tasks.cancel(task_id=task_id)
            raise RuntimeError(f"OpenSearch {operation} task timed out")
        wait_seconds = max(1, min(int(remaining), int(request_timeout)))
        task = client.tasks.get(
            task_id=task_id,
            wait_for_completion=True,
            timeout=f"{wait_seconds}s",
            request_timeout=request_timeout,
        )
        if not isinstance(task, Mapping):
            raise RuntimeError(f"OpenSearch {operation} task response is invalid")
        if not task.get("completed"):
            time.sleep(min(1.0, remaining))
            continue
        if task.get("error"):
            raise RuntimeError(f"OpenSearch {operation} task failed: {task['error']}")
        response = task.get("response")
        if not isinstance(response, Mapping):
            raise RuntimeError(f"OpenSearch {operation} task response is invalid")
        return response


def ensure_incremental_gold_baseline(
    client: Any,
    *,
    alias: str,
    index_name: str,
    build_id: str,
    release_commit: GoldReleaseCommit,
    release_commit_reference: ObjectRef,
    affected_manifest: GoldAffectedEntityManifest,
    affected_manifest_reference: ObjectRef,
    config_identity: GoldIndexConfigIdentity,
    receipt_prefix: str,
    request_timeout: float,
    task_timeout_seconds: int = 6 * 60 * 60,
    aws_region: str | None = None,
    s3_endpoint: str | None = None,
    s3_path_style_access: bool = False,
) -> tuple[GoldIndexPartitionReceipt, bool]:
    """Copy the immutable base index without mutating active cursor targets."""

    if (
        isinstance(task_timeout_seconds, bool)
        or not isinstance(task_timeout_seconds, int)
        or not 1 <= task_timeout_seconds <= MAX_GOLD_INCREMENTAL_TASK_SECONDS
    ):
        raise ValueError(
            "incremental task timeout must be between 1 second and 24 hours"
        )
    current = current_alias_indices(client, alias=alias)
    if current not in ([affected_manifest.base_index], [index_name]):
        raise RuntimeError(
            "incremental Gold build alias is not bound to its base or target index"
        )
    if affected_manifest.base_index == index_name:
        raise RuntimeError("incremental Gold build must create a new concrete index")
    input_digest = (
        "sha256:"
        + hashlib.sha256(
            canonical_json_bytes(
                {
                    "affectedEntityManifest": affected_manifest_reference,
                    "baseDocumentCount": affected_manifest.base_document_count,
                    "baseIndex": affected_manifest.base_index,
                    "baseReleasePlanId": affected_manifest.base_release_plan_id,
                    "targetIndex": index_name,
                    "targetReleasePlanId": release_commit.release_plan_id,
                }
            )
        ).hexdigest()
    )
    receipt = GoldIndexPartitionReceipt(
        build_id=build_id,
        partition_id=0,
        partition_count=1,
        operation="BASELINE",
        release_commit=release_commit_reference,
        affected_entity_manifest=affected_manifest_reference,
        mapping_digest=MAPPING_DIGEST,
        config_identity=config_identity,
        config_digest=config_identity.digest,
        image_digest=config_identity.image_digest,
        index=index_name,
        input_document_count=affected_manifest.base_document_count,
        successful_document_count=affected_manifest.base_document_count,
        input_digest=input_digest,
    )
    uri = gold_partition_receipt_uri(
        receipt_prefix,
        build_id=build_id,
        operation="BASELINE",
        partition_id=0,
    )
    publisher = GoldPartitionReceiptPublisher(
        uri,
        aws_region=aws_region,
        endpoint_url=s3_endpoint,
        path_style_access=s3_path_style_access,
    )
    existing = publisher.read_optional()
    if existing is not None:
        if existing != receipt:
            raise RuntimeError(f"immutable Gold partition receipt conflicts: {uri}")
        if current == [affected_manifest.base_index]:
            validate_gold_index_contents(
                client,
                index_name=affected_manifest.base_index,
                owner_subject=affected_manifest.owner_subject,
                release_plan_id=affected_manifest.base_release_plan_id,
                expected_document_count=affected_manifest.base_document_count,
            )
        return existing, True
    if current != [affected_manifest.base_index]:
        raise RuntimeError(
            "incremental Gold target alias has no immutable baseline receipt"
        )
    validate_gold_index_contents(
        client,
        index_name=affected_manifest.base_index,
        owner_subject=affected_manifest.owner_subject,
        release_plan_id=affected_manifest.base_release_plan_id,
        expected_document_count=affected_manifest.base_document_count,
    )

    reindex_submission = client.reindex(
        body={
            "conflicts": "abort",
            "source": {
                "index": affected_manifest.base_index,
                "query": {
                    "bool": {
                        "filter": [
                            {"term": {"ownerSubject": affected_manifest.owner_subject}},
                            {
                                "term": {
                                    "releasePlanId": (
                                        affected_manifest.base_release_plan_id
                                    )
                                }
                            },
                        ]
                    }
                },
            },
            "dest": {"index": index_name, "op_type": "index"},
        },
        refresh=True,
        wait_for_completion=False,
        request_timeout=request_timeout,
    )
    reindex_response = _await_opensearch_task(
        client,
        reindex_submission,
        operation="reindex",
        request_timeout=request_timeout,
        task_timeout_seconds=task_timeout_seconds,
    )
    _validate_by_query_response(
        reindex_response,
        expected_count=affected_manifest.base_document_count,
        operation="reindex",
    )
    update_submission = client.update_by_query(
        index=index_name,
        body={
            "query": {
                "term": {"ownerSubject": affected_manifest.owner_subject},
            },
            "script": {
                "lang": "painless",
                "source": (
                    "if (ctx._source.releasePlanId == params.releasePlanId) "
                    "{ ctx.op = 'noop' } else "
                    "{ ctx._source.releasePlanId = params.releasePlanId }"
                ),
                "params": {"releasePlanId": release_commit.release_plan_id},
            },
        },
        conflicts="abort",
        refresh=True,
        wait_for_completion=False,
        request_timeout=request_timeout,
    )
    update_response = _await_opensearch_task(
        client,
        update_submission,
        operation="update_by_query",
        request_timeout=request_timeout,
        task_timeout_seconds=task_timeout_seconds,
    )
    _validate_by_query_response(
        update_response,
        expected_count=affected_manifest.base_document_count,
        operation="update_by_query",
    )
    validate_gold_index_contents(
        client,
        index_name=index_name,
        owner_subject=affected_manifest.owner_subject,
        release_plan_id=release_commit.release_plan_id,
        expected_document_count=affected_manifest.base_document_count,
    )
    return publisher.publish(receipt), False


def validate_affected_entity_results(
    client: Any,
    *,
    index_name: str,
    upsert_entity_keys: Iterable[str],
    delete_entity_keys: Iterable[str],
    chunk_size: int = 1000,
) -> None:
    if chunk_size < 1:
        raise ValueError("mget chunk size must be positive")
    expected = [
        *((entity_key, True) for entity_key in upsert_entity_keys),
        *((entity_key, False) for entity_key in delete_entity_keys),
    ]
    for offset in range(0, len(expected), chunk_size):
        chunk = expected[offset : offset + chunk_size]
        response = client.mget(
            index=index_name,
            body={"ids": [entity_key for entity_key, _ in chunk]},
        )
        documents = response.get("docs") if isinstance(response, Mapping) else None
        if not isinstance(documents, list) or len(documents) != len(chunk):
            raise RuntimeError("OpenSearch affected-entity mget response is invalid")
        by_id = {
            item.get("_id"): item
            for item in documents
            if isinstance(item, Mapping) and isinstance(item.get("_id"), str)
        }
        for entity_key, should_exist in chunk:
            item = by_id.get(entity_key)
            if (
                item is None
                or not isinstance(item.get("found"), bool)
                or item["found"] != should_exist
            ):
                raise RuntimeError(
                    "Gold incremental affected-entity verification failed"
                )


def validate_gold_index_owner(
    client: Any,
    *,
    index_name: str,
    owner_subject: str,
    expected_document_count: int,
) -> int:
    owner = require_oidc_subject(owner_subject)
    if (
        isinstance(expected_document_count, bool)
        or not isinstance(expected_document_count, int)
        or expected_document_count < 0
    ):
        raise ValueError("expected document count must be non-negative")
    if not _has_compatible_mapping(
        client,
        index_name=index_name,
        owner_subject=owner,
    ):
        raise RuntimeError("Gold shadow index owner metadata mismatch")
    client.indices.refresh(index=index_name)
    total_response = client.count(index=index_name)
    owner_response = client.count(
        index=index_name,
        body={"query": {"term": {"ownerSubject": owner}}},
    )
    if not isinstance(total_response, Mapping) or not isinstance(
        owner_response, Mapping
    ):
        raise RuntimeError("OpenSearch count response is invalid")
    total_count = total_response.get("count")
    owner_count = owner_response.get("count")
    if (
        not isinstance(total_count, int)
        or isinstance(total_count, bool)
        or not isinstance(owner_count, int)
        or isinstance(owner_count, bool)
        or total_count < 0
        or owner_count < 0
    ):
        raise RuntimeError("OpenSearch count response is invalid")
    if total_count != expected_document_count or owner_count != total_count:
        raise RuntimeError(
            "Gold shadow index contains missing or mismatched document owners"
        )
    return total_count


def validate_gold_index_contents(
    client: Any,
    *,
    index_name: str,
    owner_subject: str,
    release_plan_id: str,
    expected_document_count: int,
) -> int:
    """Require one concrete index to contain only the expected owner/release."""

    owner = require_oidc_subject(owner_subject)
    release_plan = require_sha256(release_plan_id, label="release_plan_id")
    total_count = validate_gold_index_owner(
        client,
        index_name=index_name,
        owner_subject=owner,
        expected_document_count=expected_document_count,
    )
    release_response = client.count(
        index=index_name,
        body={
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"ownerSubject": owner}},
                        {"term": {"releasePlanId": release_plan}},
                    ]
                }
            }
        },
    )
    if not isinstance(release_response, Mapping):
        raise RuntimeError("OpenSearch count response is invalid")
    release_count = release_response.get("count")
    if (
        not isinstance(release_count, int)
        or isinstance(release_count, bool)
        or release_count < 0
    ):
        raise RuntimeError("OpenSearch count response is invalid")
    if release_count != total_count:
        raise RuntimeError(
            "Gold shadow index contains documents from another release plan"
        )
    return total_count


@dataclass(frozen=True)
class GoldBuildCountReconciliation:
    gold_entity_count: int
    projected_document_count: int
    successful_document_count: int
    failed_document_count: int
    concrete_index_document_count: int


def reconcile_full_gold_index_counts(
    *,
    gold_entity_count: int,
    projected_document_count: int,
    successful_document_count: int,
    failed_document_count: int,
    concrete_index_document_count: int,
) -> GoldBuildCountReconciliation:
    values = {
        "Gold entity": gold_entity_count,
        "projected document": projected_document_count,
        "successful document": successful_document_count,
        "failed document": failed_document_count,
        "concrete index": concrete_index_document_count,
    }
    if any(
        isinstance(value, bool) or not isinstance(value, int) or value < 0
        for value in values.values()
    ):
        raise ValueError("Gold build counts must be non-negative integers")
    if (
        failed_document_count != 0
        or len(
            {
                gold_entity_count,
                projected_document_count,
                successful_document_count,
                concrete_index_document_count,
            }
        )
        != 1
    ):
        rendered = ", ".join(f"{name}={value}" for name, value in values.items())
        raise RuntimeError(f"Gold index count reconciliation failed: {rendered}")
    return GoldBuildCountReconciliation(
        gold_entity_count=gold_entity_count,
        projected_document_count=projected_document_count,
        successful_document_count=successful_document_count,
        failed_document_count=failed_document_count,
        concrete_index_document_count=concrete_index_document_count,
    )


class GoldIndexBuildManifest(V2ContractModel):
    schema_version: Literal["2.0"] = "2.0"
    status: Literal["COMPLETED"] = "COMPLETED"
    build_mode: Literal["FULL", "INCREMENTAL"] = "FULL"
    build_id: str
    release_plan_id: str
    owner_subject: str
    context_id: Literal["research"] = RESEARCH_CONTEXT_ID
    release_commit: ObjectRef
    affected_entity_manifest: ObjectRef | None = None
    table_snapshot_ids: dict[str, int | None]
    mapping_digest: str
    config_identity: GoldIndexConfigIdentity
    config_digest: str
    document_count: int = Field(ge=0)
    gold_entity_count: int | None = Field(default=None, ge=0)
    successful_document_count: int | None = Field(default=None, ge=0)
    failed_document_count: int = Field(default=0, ge=0)
    concrete_index_document_count: int | None = Field(default=None, ge=0)
    partition_receipt_count: int = Field(default=0, ge=0)
    partition_receipt_digest: str | None = None
    checkpoint_prefix: str | None = None
    index: str
    alias: str
    completed_at: str

    @field_validator("build_id")
    @classmethod
    def validate_build_id(cls, value: str) -> str:
        if re.fullmatch(r"[0-9a-f]{64}", value) is None:
            raise ValueError("build_id must be lowercase SHA-256 hex")
        return value

    @field_validator(
        "release_plan_id",
        "mapping_digest",
        "config_digest",
        "partition_receipt_digest",
    )
    @classmethod
    def validate_digest(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return require_sha256(value)

    @field_validator("owner_subject")
    @classmethod
    def validate_owner_subject(cls, value: str) -> str:
        return require_oidc_subject(value)

    @field_validator("index", "alias")
    @classmethod
    def validate_names(cls, value: str) -> str:
        return _safe_name(value, label="OpenSearch name")

    @field_validator("checkpoint_prefix")
    @classmethod
    def validate_checkpoint_prefix(cls, value: str | None) -> str | None:
        if value is None:
            return None
        parsed = urlsplit(value)
        if (
            parsed.scheme not in {"file", "s3"}
            or parsed.query
            or parsed.fragment
            or parsed.username is not None
            or parsed.password is not None
        ):
            raise ValueError("checkpoint_prefix must use file:// or s3://")
        if parsed.scheme == "s3" and (not parsed.netloc or not parsed.path.lstrip("/")):
            raise ValueError("S3 checkpoint_prefix must include bucket and key")
        return value.rstrip("/")

    @field_validator("table_snapshot_ids")
    @classmethod
    def validate_snapshot_ids(
        cls, value: dict[str, int | None]
    ) -> dict[str, int | None]:
        if set(value) != set(GOLD_DATA_COLUMNS):
            raise ValueError("index manifest must contain every Gold data table")
        if any(
            snapshot is not None and (isinstance(snapshot, bool) or snapshot <= 0)
            for snapshot in value.values()
        ):
            raise ValueError("table snapshot IDs must be positive when present")
        return dict(sorted(value.items()))

    @field_validator("completed_at")
    @classmethod
    def validate_completed_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @model_validator(mode="after")
    def validate_release_commit(self) -> Self:
        if self.alias != RESEARCH_READ_ALIAS or not self.index.startswith(
            RESEARCH_INDEX_PREFIX + "-"
        ):
            raise ValueError("index manifest must use the research index family")
        reference = self.release_commit
        if (
            reference.format != "OBJECT_FORMAT_JSON"
            or reference.media_type != GOLD_RELEASE_COMMIT_MEDIA_TYPE
            or reference.size_bytes <= 0
        ):
            raise ValueError("release_commit must be a non-empty Gold commit")
        if reference.uri.startswith("s3://") and (
            reference.etag is None or reference.object_version is None
        ):
            raise ValueError("S3 release_commit must be immutable")
        if (self.build_mode == "INCREMENTAL") != (
            self.affected_entity_manifest is not None
        ):
            raise ValueError(
                "incremental build manifests must bind an affected-entity manifest"
            )
        if self.affected_entity_manifest is not None:
            _validate_immutable_control_ref(
                self.affected_entity_manifest,
                media_type=GOLD_AFFECTED_ENTITY_MANIFEST_MEDIA_TYPE,
                label="affected-entity manifest",
            )
        if (
            self.mapping_digest != MAPPING_DIGEST
            or self.config_identity.mapping_digest != self.mapping_digest
            or self.config_identity.owner_subject != self.owner_subject
            or self.config_identity.context_id != self.context_id
            or self.config_identity.read_alias != self.alias
            or self.config_digest != self.config_identity.digest
        ):
            raise ValueError(
                "index manifest config identity does not match its owner or contract"
            )
        if (
            (
                self.gold_entity_count is not None
                and self.gold_entity_count != self.document_count
            )
            or (
                self.concrete_index_document_count is not None
                and self.concrete_index_document_count != self.document_count
            )
            or self.failed_document_count != 0
            or (
                self.build_mode == "FULL"
                and self.successful_document_count is not None
                and self.successful_document_count != self.document_count
            )
        ):
            raise ValueError("index manifest count reconciliation is inconsistent")
        if (self.partition_receipt_count > 0) != (
            self.partition_receipt_digest is not None
        ):
            raise ValueError(
                "partition receipt count and digest must be present together"
            )
        return self
