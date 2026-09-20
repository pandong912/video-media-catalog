"""Offline synthetic sizing plans for million-scale Gold indexes."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Iterable, Mapping
from typing import Any

from pydantic import Field, field_validator

from video_media_catalog.canonical import canonical_json_bytes
from video_media_catalog.gold_search_index import (
    DEFAULT_GOLD_BULK_PARTITIONS,
    DEFAULT_GOLD_BULK_WORKERS,
    RESEARCH_INDEX_PREFIX,
)
from video_media_catalog.search_index import DEFAULT_MAX_BULK_BYTES
from video_media_catalog.v2_contracts import V2ContractModel, require_sha256

SYNTHETIC_SCALE_DOCUMENTS = {
    "1m": 1_000_000,
    "5m": 5_000_000,
}
DEFAULT_TARGET_PRIMARY_SHARD_BYTES = 30 * 1024 * 1024 * 1024
DEFAULT_MAX_DOCUMENTS_PER_PRIMARY_SHARD = 1_000_000
DEFAULT_DOCUMENTS_PER_SECOND_PER_WORKER = 750
DEFAULT_BYTES_PER_SECOND_PER_WORKER = 10 * 1024 * 1024
INDEX_STORAGE_OVERHEAD_RATIO = 1.30


class GoldIndexSizingPlan(V2ContractModel):
    target_document_count: int = Field(ge=1)
    sample_document_count: int = Field(ge=1)
    average_document_bytes: int = Field(ge=1)
    average_bulk_action_bytes: int = Field(ge=1)
    estimated_source_bytes: int = Field(ge=1)
    estimated_primary_store_bytes: int = Field(ge=1)
    recommended_primary_shards: int = Field(ge=1)
    bulk_partitions: int = Field(ge=1)
    bulk_workers_per_partition: int = Field(ge=1)
    bulk_chunk_size: int = Field(ge=1)
    bulk_max_chunk_bytes: int = Field(ge=1)
    estimated_documents_per_bulk_request: int = Field(ge=1)
    estimated_bulk_request_count: int = Field(ge=1)
    estimated_bulk_duration_seconds: int = Field(ge=1)
    assumptions_digest: str

    @field_validator("assumptions_digest")
    @classmethod
    def validate_assumptions_digest(cls, value: str) -> str:
        return require_sha256(value, label="assumptions_digest")


def synthetic_gold_document(sequence: int) -> dict[str, Any]:
    if sequence < 0:
        raise ValueError("synthetic document sequence must be non-negative")
    key = "sha256:" + hashlib.sha256(f"gold-{sequence}".encode()).hexdigest()
    title = f"Synthetic Film or Series {sequence:08d}"
    return {
        "entityKey": key,
        "entityLevel": "WORK",
        "entityKind": "MOVIE" if sequence % 2 == 0 else "TV_SERIES",
        "status": "ACTIVE",
        "releasePlanId": "sha256:" + ("a" * 64),
        "contextId": "research",
        "displayName": title,
        "displayLanguage": "en",
        "titles": [
            {"value": title, "language": "en", "titleRole": "PRIMARY"},
            {
                "value": f"合成影视条目 {sequence:08d}",
                "language": "zh-hans",
                "titleRole": "TITLE",
            },
        ],
        "attributes": {
            "formats": ["DIGITAL"],
            "languages": ["en", "zh"],
            "statuses": ["RELEASED"],
            "premiered": ["2026-01-01"],
            "ended": [],
            "runtimeMinutes": ["120"],
            "averageRuntimeMinutes": [],
            "genres": ["Drama", "Science Fiction"],
        },
        "externalIdentifiers": [
            {
                "namespace": "imdb-title",
                "value": f"tt{sequence % 100_000_000:08d}",
                "issuer": "IMDb",
                "referentKind": "WORK",
            }
        ],
        "relationSummary": [{"predicate": "HAS_CONTRIBUTOR", "count": 8}],
        "sourceBadges": [
            {
                "sourceProductId": "synthetic-public-catalog",
                "displayName": "Synthetic public catalog",
                "sourceUrl": "https://example.invalid/catalog",
                "policyZones": ["open"],
                "assertionCount": 12,
                "winningAssertionCount": 8,
            }
        ],
        "winningAssertions": [],
        "rights": [],
        "conflictCount": 0,
        "conflictPredicates": [],
        "conflicts": [],
        "sourceNodeCount": 2,
        "overflow": {
            "titles": 0,
            "externalIdentifiers": 0,
            "relationTypes": 0,
            "sourceBadges": 0,
            "winningAssertions": 0,
            "citationKeys": 0,
            "rights": 0,
            "conflicts": 0,
            "formats": 0,
            "languages": 0,
            "statuses": 0,
            "premiered": 0,
            "ended": 0,
            "runtimeMinutes": 0,
            "averageRuntimeMinutes": 0,
            "genres": 0,
        },
    }


def _bulk_action_bytes(document: Mapping[str, Any]) -> int:
    entity_key = document.get("entityKey")
    if not isinstance(entity_key, str) or not entity_key:
        raise ValueError("sizing sample document has no entityKey")
    metadata = {
        "index": {
            "_id": entity_key,
            "_index": f"{RESEARCH_INDEX_PREFIX}-synthetic",
        }
    }
    return (
        len(
            json.dumps(
                metadata,
                ensure_ascii=True,
                separators=(",", ":"),
            ).encode("utf-8")
        )
        + 1
        + len(
            json.dumps(
                document,
                ensure_ascii=True,
                separators=(",", ":"),
            ).encode("utf-8")
        )
        + 1
    )


def plan_gold_index_sizing(
    sample_documents: Iterable[Mapping[str, Any]],
    *,
    target_document_count: int,
    bulk_partitions: int = DEFAULT_GOLD_BULK_PARTITIONS,
    bulk_workers: int = DEFAULT_GOLD_BULK_WORKERS,
    bulk_chunk_size: int = 500,
    bulk_max_chunk_bytes: int = DEFAULT_MAX_BULK_BYTES,
    target_primary_shard_bytes: int = DEFAULT_TARGET_PRIMARY_SHARD_BYTES,
    max_documents_per_primary_shard: int = (DEFAULT_MAX_DOCUMENTS_PER_PRIMARY_SHARD),
    documents_per_second_per_worker: int = (DEFAULT_DOCUMENTS_PER_SECOND_PER_WORKER),
    bytes_per_second_per_worker: int = DEFAULT_BYTES_PER_SECOND_PER_WORKER,
) -> GoldIndexSizingPlan:
    inputs = {
        "target_document_count": target_document_count,
        "bulk_partitions": bulk_partitions,
        "bulk_workers": bulk_workers,
        "bulk_chunk_size": bulk_chunk_size,
        "bulk_max_chunk_bytes": bulk_max_chunk_bytes,
        "target_primary_shard_bytes": target_primary_shard_bytes,
        "max_documents_per_primary_shard": max_documents_per_primary_shard,
        "documents_per_second_per_worker": documents_per_second_per_worker,
        "bytes_per_second_per_worker": bytes_per_second_per_worker,
    }
    if any(
        isinstance(value, bool) or not isinstance(value, int) or value < 1
        for value in inputs.values()
    ):
        raise ValueError("Gold sizing inputs must be positive integers")
    documents = [dict(document) for document in sample_documents]
    if not documents:
        raise ValueError("Gold sizing requires at least one sample document")
    document_sizes = [len(canonical_json_bytes(document)) for document in documents]
    action_sizes = [_bulk_action_bytes(document) for document in documents]
    average_document_bytes = math.ceil(sum(document_sizes) / len(document_sizes))
    average_action_bytes = math.ceil(sum(action_sizes) / len(action_sizes))
    if average_action_bytes > bulk_max_chunk_bytes:
        raise ValueError("average Gold action exceeds bulk-max-chunk-bytes")
    estimated_source_bytes = average_document_bytes * target_document_count
    estimated_primary_store_bytes = math.ceil(
        estimated_source_bytes * INDEX_STORAGE_OVERHEAD_RATIO
    )
    recommended_shards = max(
        1,
        math.ceil(estimated_primary_store_bytes / target_primary_shard_bytes),
        math.ceil(target_document_count / max_documents_per_primary_shard),
    )
    documents_per_request = max(
        1,
        min(
            bulk_chunk_size,
            bulk_max_chunk_bytes // average_action_bytes,
        ),
    )
    request_count = math.ceil(target_document_count / documents_per_request)
    parallel_workers = bulk_partitions * bulk_workers
    duration_by_documents = target_document_count / (
        parallel_workers * documents_per_second_per_worker
    )
    duration_by_bytes = (average_action_bytes * target_document_count) / (
        parallel_workers * bytes_per_second_per_worker
    )
    assumptions = {
        **inputs,
        "averageBulkActionBytes": average_action_bytes,
        "averageDocumentBytes": average_document_bytes,
        "indexStorageOverheadRatio": INDEX_STORAGE_OVERHEAD_RATIO,
        "sampleDocumentCount": len(documents),
    }
    assumptions_digest = (
        "sha256:" + hashlib.sha256(canonical_json_bytes(assumptions)).hexdigest()
    )
    return GoldIndexSizingPlan(
        target_document_count=target_document_count,
        sample_document_count=len(documents),
        average_document_bytes=average_document_bytes,
        average_bulk_action_bytes=average_action_bytes,
        estimated_source_bytes=estimated_source_bytes,
        estimated_primary_store_bytes=estimated_primary_store_bytes,
        recommended_primary_shards=recommended_shards,
        bulk_partitions=bulk_partitions,
        bulk_workers_per_partition=bulk_workers,
        bulk_chunk_size=bulk_chunk_size,
        bulk_max_chunk_bytes=bulk_max_chunk_bytes,
        estimated_documents_per_bulk_request=documents_per_request,
        estimated_bulk_request_count=request_count,
        estimated_bulk_duration_seconds=max(
            1,
            math.ceil(max(duration_by_documents, duration_by_bytes)),
        ),
        assumptions_digest=assumptions_digest,
    )


def synthetic_gold_sizing_plan(
    *,
    target_document_count: int,
    sample_size: int = 1000,
    **kwargs: Any,
) -> GoldIndexSizingPlan:
    if not 1 <= sample_size <= 10_000:
        raise ValueError("synthetic sample size must be between 1 and 10,000")
    return plan_gold_index_sizing(
        (synthetic_gold_document(sequence) for sequence in range(sample_size)),
        target_document_count=target_document_count,
        **kwargs,
    )
