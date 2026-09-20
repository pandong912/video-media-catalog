"""Strict shadow OpenSearch index contract for Gold v2 documents."""

from __future__ import annotations

import copy
import hashlib
import re
from collections.abc import Mapping
from typing import Any, Literal, Self

from pydantic import Field, field_validator, model_validator

from video_media_catalog.canonical import canonical_json_bytes
from video_media_catalog.gold import PERSONAL_RESEARCH_CONTEXT_ID
from video_media_catalog.gold_ingest import (
    GOLD_RELEASE_COMMIT_MEDIA_TYPE,
    GoldReleaseCommit,
)
from video_media_catalog.gold_tables import GOLD_DATA_COLUMNS
from video_media_catalog.models import ObjectRef
from video_media_catalog.v2_contracts import (
    V2ContractModel,
    require_oidc_subject,
    require_rfc3339,
    require_sha256,
)

RESEARCH_READ_ALIAS = "media-catalog-research-read"
RESEARCH_INDEX_PREFIX = "media-catalog-research"
PROJECTION_VERSION = "3"

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
    if shards < 1 or replicas < 0:
        raise ValueError("invalid shard or replica count")
    if bulk_chunk_size < 1 or bulk_max_chunk_bytes < 1:
        raise ValueError("invalid bulk configuration")
    if read_alias != RESEARCH_READ_ALIAS or index_prefix != RESEARCH_INDEX_PREFIX:
        raise ValueError("research index and alias names are fixed")
    require_sha256(image_digest, label="image_digest")
    payload = {
        "projectionVersion": PROJECTION_VERSION,
        "mappingDigest": MAPPING_DIGEST,
        "contextId": PERSONAL_RESEARCH_CONTEXT_ID,
        "ownerSubject": require_oidc_subject(owner_subject),
        "readAlias": _safe_name(read_alias, label="read alias"),
        "indexPrefix": _safe_name(index_prefix, label="index prefix"),
        "shards": shards,
        "replicas": replicas,
        "bulkChunkSize": bulk_chunk_size,
        "bulkMaxChunkBytes": bulk_max_chunk_bytes,
        "imageDigest": image_digest,
    }
    return "sha256:" + hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


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


def gold_index_definition(*, shards: int, replicas: int) -> dict[str, Any]:
    if shards < 1 or replicas < 0:
        raise ValueError("invalid shard or replica count")
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
    if isinstance(info, Mapping) and isinstance(info.get("status"), int):
        return int(info["status"])
    return None


def _existing_mapping_digest(client: Any, index_name: str) -> str | None:
    response = client.indices.get_mapping(index=index_name)
    value = response.get(index_name)
    if value is None and len(response) == 1:
        value = next(iter(response.values()))
    if not isinstance(value, Mapping):
        return None
    mappings = value.get("mappings")
    metadata = mappings.get("_meta") if isinstance(mappings, Mapping) else None
    return metadata.get("mappingDigest") if isinstance(metadata, Mapping) else None


def ensure_gold_index(
    client: Any,
    *,
    index_name: str,
    shards: int,
    replicas: int,
) -> bool:
    if client.indices.exists(index=index_name):
        if _existing_mapping_digest(client, index_name) != MAPPING_DIGEST:
            raise RuntimeError("existing Gold index mapping is incompatible")
        return False
    try:
        client.indices.create(
            index=index_name,
            body=gold_index_definition(shards=shards, replicas=replicas),
        )
        return True
    except Exception as exc:
        if _status_code(exc) not in {400, 409} or not client.indices.exists(
            index=index_name
        ):
            raise
        if _existing_mapping_digest(client, index_name) != MAPPING_DIGEST:
            raise RuntimeError(
                "concurrently created Gold index mapping is incompatible"
            ) from exc
        return False


class GoldIndexBuildManifest(V2ContractModel):
    schema_version: Literal["2.0"] = "2.0"
    status: Literal["COMPLETED"] = "COMPLETED"
    build_id: str
    release_plan_id: str
    owner_subject: str
    context_id: Literal["personal-research"] = PERSONAL_RESEARCH_CONTEXT_ID
    release_commit: ObjectRef
    table_snapshot_ids: dict[str, int | None]
    mapping_digest: str
    config_digest: str
    document_count: int = Field(ge=0)
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
    )
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("owner_subject")
    @classmethod
    def validate_owner_subject(cls, value: str) -> str:
        return require_oidc_subject(value)

    @field_validator("index", "alias")
    @classmethod
    def validate_names(cls, value: str) -> str:
        return _safe_name(value, label="OpenSearch name")

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
        return self
