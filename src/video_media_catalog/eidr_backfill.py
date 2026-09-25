"""Snapshot-pinned EIDR discovery and authorized exact-lookup backfills."""

from __future__ import annotations

import hashlib
import math
import re
import tempfile
import threading
import time
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from enum import StrEnum
from itertools import islice
from pathlib import Path
from typing import Any, Literal, Protocol, Self
from urllib.parse import urlsplit

from pydantic import Field, ValidationInfo, field_validator, model_validator

from video_media_catalog.canonical import deterministic_key, sha256_digest
from video_media_catalog.community_snapshot import (
    CONTROL_MAX_BYTES,
    SILVER_SNAPSHOT_MEDIA_TYPE,
    CommunitySilverSnapshotSet,
)
from video_media_catalog.community_sources import (
    EIDR_EXACT_LOOKUP_CONNECTOR_ID,
    eidr_rights_profile,
)
from video_media_catalog.connector import (
    ChangeSemantics,
    Completeness,
    DeleteCoverage,
    RecordOperation,
    Serialization,
    TransportKind,
    build_connector_batch_manifest,
    build_connector_record_envelope,
)
from video_media_catalog.connector_publish import (
    DEFAULT_RECORD_SHARD_BYTES,
    PublishedConnectorCapture,
    publish_connector_capture,
)
from video_media_catalog.eidr import iter_eidr_payloads, normalize_eidr_id
from video_media_catalog.models import ObjectRef
from video_media_catalog.object_store import (
    MaterializedObject,
    RuntimeObjectStore,
    UploadResult,
)
from video_media_catalog.storage import join_uri
from video_media_catalog.v2_contracts import (
    V2ContractModel,
    digest_identity,
    require_rfc3339,
    require_sha256,
    require_slug,
)

EIDR_SOURCE_SYSTEM_ID = "eidr"
EIDR_SOURCE_PRODUCT_ID = "eidr-public-registry"
EIDR_NAMESPACE_ID = "eidr-content"

DISCOVERED_ID_PAGE_MEDIA_TYPE = (
    "application/vnd.video-media-catalog.eidr-discovered-id-page.v1+json"
)
DISCOVERED_ID_MANIFEST_MEDIA_TYPE = (
    "application/vnd.video-media-catalog.eidr-discovered-id-manifest.v1+json"
)
LOOKUP_BATCH_MEDIA_TYPE = (
    "application/vnd.video-media-catalog.eidr-exact-lookup-batch.v1+json"
)
LOOKUP_RECEIPT_MEDIA_TYPE = (
    "application/vnd.video-media-catalog.eidr-exact-lookup-receipt.v1+json"
)
BACKFILL_WATERMARK_MEDIA_TYPE = (
    "application/vnd.video-media-catalog.eidr-backfill-watermark.v1+json"
)
BACKFILL_RUN_MANIFEST_MEDIA_TYPE = (
    "application/vnd.video-media-catalog.eidr-backfill-run-manifest.v1+json"
)

DEFAULT_DISCOVERED_PAGE_IDS = 4_096
MAX_DISCOVERED_PAGE_IDS = 16_384
MAX_DISCOVERED_PAGES = 16_384
DISCOVERED_PAGE_MAX_BYTES = 4 * 1024 * 1024
DEFAULT_LOOKUP_BATCH_IDS = 100
MAX_LOOKUP_BATCH_IDS = 1_000
DEFAULT_MAX_XML_BYTES = 8 * 1024 * 1024
MAX_MAX_XML_BYTES = 64 * 1024 * 1024
MAX_RECORD_SHARD_BYTES = 128 * 1024 * 1024
MAX_AUTHORIZATION_OBJECT_BYTES = CONTROL_MAX_BYTES
EIDR_SOURCE_SEMAPHORE_PERMITS = 1
DEFAULT_BACKFILL_MAX_BATCHES = 1_000
MAX_BACKFILL_MAX_BATCHES = 4_096
DEFAULT_BACKFILL_MAX_DURATION_SECONDS = 6 * 60 * 60
MAX_BACKFILL_MAX_DURATION_SECONDS = 7 * 24 * 60 * 60
DEFAULT_BACKFILL_MAX_IDS = 100_000
MAX_BACKFILL_MAX_IDS = MAX_BACKFILL_MAX_BATCHES * MAX_LOOKUP_BATCH_IDS
_ZERO_DIGEST = "sha256:" + ("0" * 64)
_SHARD_PATTERN = re.compile(r"^[0-9a-f]{2}$")
_SOURCE_SEMAPHORE = threading.BoundedSemaphore(EIDR_SOURCE_SEMAPHORE_PERMITS)


class EidrBackfillError(RuntimeError):
    """Base error for a fail-closed EIDR backfill."""


class EidrProviderNotAuthorizedError(EidrBackfillError):
    """Raised when an exact-lookup provider lacks explicit authorization."""


def _object_identity(reference: ObjectRef) -> dict[str, Any]:
    return reference.model_dump(mode="json", by_alias=True, exclude_none=True)


def _validate_immutable_ref(
    reference: ObjectRef,
    *,
    label: str,
    max_bytes: int,
    media_type: str | None = None,
) -> None:
    parsed = urlsplit(reference.uri)
    if (
        parsed.scheme not in {"file", "s3"}
        or parsed.query
        or parsed.fragment
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise ValueError(f"{label} must be an immutable file:// or s3:// object")
    if parsed.scheme == "s3" and (
        reference.etag is None or reference.object_version is None
    ):
        raise ValueError(f"{label} S3 object requires ETag and object version")
    if parsed.scheme == "file" and (
        reference.etag is not None or reference.object_version is not None
    ):
        raise ValueError(f"{label} file object cannot declare S3 metadata")
    if not 0 < reference.size_bytes <= max_bytes:
        raise ValueError(f"{label} exceeds its bounded object size")
    if media_type is not None and reference.media_type != media_type:
        raise ValueError(f"{label} has an unexpected media type")


def _validate_json_control_ref(
    reference: ObjectRef,
    *,
    label: str,
    media_type: str,
    max_bytes: int = CONTROL_MAX_BYTES,
) -> None:
    _validate_immutable_ref(
        reference,
        label=label,
        max_bytes=max_bytes,
        media_type=media_type,
    )
    if reference.format != "OBJECT_FORMAT_JSON":
        raise ValueError(f"{label} must be a JSON ObjectRef")


def _require_model_reference(
    model: V2ContractModel,
    reference: ObjectRef,
    *,
    label: str,
    media_type: str,
    max_bytes: int = CONTROL_MAX_BYTES,
) -> None:
    _validate_json_control_ref(
        reference,
        label=label,
        media_type=media_type,
        max_bytes=max_bytes,
    )
    payload = model.json_bytes()
    if (
        len(payload) != reference.size_bytes
        or sha256_digest(payload).removeprefix("sha256:") != reference.checksum.value
    ):
        raise ValueError(f"{label} does not bind the supplied contract")


def _timestamped_ref(reference: ObjectRef, timestamp: str) -> ObjectRef:
    return reference.model_copy(update={"created_at": timestamp})


class _TimestampedStore:
    """Make output ObjectRefs replay-stable at a caller-pinned timestamp."""

    def __init__(self, delegate: RuntimeObjectStore, timestamp: str) -> None:
        self.delegate = delegate
        self.timestamp = timestamp

    def verify(self, object_ref: ObjectRef, *, max_bytes: int) -> None:
        self.delegate.verify(object_ref, max_bytes=max_bytes)

    def download(
        self,
        object_ref: ObjectRef,
        destination: Path,
        *,
        max_bytes: int,
    ) -> MaterializedObject:
        return self.delegate.download(
            object_ref,
            destination,
            max_bytes=max_bytes,
        )

    def upload_file(
        self,
        source: Path,
        destination_uri: str,
        *,
        media_type: str,
        object_format: str,
        max_bytes: int,
    ) -> UploadResult:
        result = self.delegate.upload_file(
            source,
            destination_uri,
            media_type=media_type,
            object_format=object_format,
            max_bytes=max_bytes,
        )
        return UploadResult(
            object_ref=_timestamped_ref(result.object_ref, self.timestamp),
            reused=result.reused,
        )

    def upload_bytes(
        self,
        payload: bytes,
        destination_uri: str,
        *,
        media_type: str,
        object_format: str,
        max_bytes: int,
    ) -> UploadResult:
        result = self.delegate.upload_bytes(
            payload,
            destination_uri,
            media_type=media_type,
            object_format=object_format,
            max_bytes=max_bytes,
        )
        return UploadResult(
            object_ref=_timestamped_ref(result.object_ref, self.timestamp),
            reused=result.reused,
        )


class EidrDiscoverySourceBinding(V2ContractModel):
    binding_id: str
    source_release_id: str
    silver_snapshot_set_id: str
    silver_snapshot_object: ObjectRef
    identifier_assertion_snapshot_id: int = Field(gt=0)
    committed_run_ids: tuple[str, ...]

    @field_validator(
        "binding_id",
        "source_release_id",
        "silver_snapshot_set_id",
    )
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("committed_run_ids")
    @classmethod
    def normalize_runs(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(
            sorted({require_sha256(item, label="run_id") for item in value})
        )
        if not normalized:
            raise ValueError("source binding requires committed Silver runs")
        return normalized

    @model_validator(mode="after")
    def validate_binding(self, info: ValidationInfo) -> Self:
        _validate_json_control_ref(
            self.silver_snapshot_object,
            label="Silver snapshot",
            media_type=SILVER_SNAPSHOT_MEDIA_TYPE,
        )
        if not (info.context or {}).get("skip_identity"):
            expected = deterministic_key(
                "eidr-discovery-source-binding-v1",
                _source_binding_identity(self),
            )
            if self.binding_id != expected:
                raise ValueError(
                    "binding_id does not match its source release/snapshot"
                )
        return self


def _source_binding_identity(binding: EidrDiscoverySourceBinding) -> dict[str, Any]:
    return {
        "sourceReleaseId": binding.source_release_id,
        "silverSnapshotSetId": binding.silver_snapshot_set_id,
        "silverSnapshotObject": _object_identity(binding.silver_snapshot_object),
        "identifierAssertionSnapshotId": binding.identifier_assertion_snapshot_id,
        "committedRunIds": binding.committed_run_ids,
    }


def build_eidr_discovery_source_binding(
    *,
    source_release_id: str,
    silver_snapshot: CommunitySilverSnapshotSet,
    silver_snapshot_object: ObjectRef,
) -> EidrDiscoverySourceBinding:
    _require_model_reference(
        silver_snapshot,
        silver_snapshot_object,
        label="Silver snapshot",
        media_type=SILVER_SNAPSHOT_MEDIA_TYPE,
    )
    assertion_snapshot = silver_snapshot.data_snapshot_ids[
        "community_identifier_assertion"
    ]
    if assertion_snapshot is None:
        raise ValueError("pinned Silver epoch has no IdentifierAssertion snapshot")
    values = {
        "source_release_id": require_sha256(
            source_release_id,
            label="source_release_id",
        ),
        "silver_snapshot_set_id": silver_snapshot.snapshot_set_id,
        "silver_snapshot_object": silver_snapshot_object,
        "identifier_assertion_snapshot_id": assertion_snapshot,
        "committed_run_ids": silver_snapshot.committed_run_ids,
    }
    provisional = EidrDiscoverySourceBinding.model_validate(
        {"binding_id": _ZERO_DIGEST, **values},
        context={"skip_identity": True},
    )
    return EidrDiscoverySourceBinding(
        binding_id=deterministic_key(
            "eidr-discovery-source-binding-v1",
            _source_binding_identity(provisional),
        ),
        **values,
    )


class DiscoveredEidrId(V2ContractModel):
    discovered_id: str
    eidr_id: str

    @field_validator("discovered_id")
    @classmethod
    def validate_discovered_id(cls, value: str) -> str:
        return require_sha256(value, label="discovered_id")

    @field_validator("eidr_id")
    @classmethod
    def normalize_identifier(cls, value: str) -> str:
        return normalize_eidr_id(value)

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        if self.discovered_id != discovered_eidr_id(self.eidr_id):
            raise ValueError("discovered_id does not match the EIDR ID")
        return self


def discovered_eidr_id(eidr_id: str) -> str:
    return deterministic_key(
        "eidr-discovered-id-v1",
        {"eidrId": normalize_eidr_id(eidr_id)},
    )


class DiscoveredEidrIdPage(V2ContractModel):
    schema_version: str = "1.0"
    page_id: str
    source_binding_id: str
    shard_key: str
    page_index: int = Field(ge=0)
    start_ordinal: int = Field(ge=0)
    items: tuple[DiscoveredEidrId, ...] = Field(
        min_length=1,
        max_length=MAX_DISCOVERED_PAGE_IDS,
    )

    @field_validator("page_id", "source_binding_id")
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("shard_key")
    @classmethod
    def validate_shard(cls, value: str) -> str:
        normalized = value.strip().lower()
        if _SHARD_PATTERN.fullmatch(normalized) is None:
            raise ValueError("discovered-ID shard key must be two lowercase hex digits")
        return normalized

    @model_validator(mode="after")
    def validate_page(self, info: ValidationInfo) -> Self:
        identifiers = [item.eidr_id for item in self.items]
        if identifiers != sorted(set(identifiers)):
            raise ValueError("discovered-ID page items must be sorted and unique")
        if any(_eidr_shard(item) != self.shard_key for item in identifiers):
            raise ValueError("discovered-ID page contains an item in the wrong shard")
        if not (info.context or {}).get("skip_identity"):
            expected = deterministic_key(
                "eidr-discovered-id-page-v1",
                _discovered_page_identity(self),
            )
            if self.page_id != expected:
                raise ValueError("page_id does not match discovered-ID page contents")
        return self


def _discovered_page_identity(page: DiscoveredEidrIdPage) -> dict[str, Any]:
    return {
        "schemaVersion": page.schema_version,
        "sourceBindingId": page.source_binding_id,
        "shardKey": page.shard_key,
        "pageIndex": page.page_index,
        "startOrdinal": page.start_ordinal,
        "items": [item.model_dump(mode="json", by_alias=True) for item in page.items],
    }


def build_discovered_eidr_id_page(**values: Any) -> DiscoveredEidrIdPage:
    provisional = DiscoveredEidrIdPage.model_validate(
        {"page_id": _ZERO_DIGEST, **values},
        context={"skip_identity": True},
    )
    normalized = provisional.model_dump(mode="python")
    normalized["page_id"] = deterministic_key(
        "eidr-discovered-id-page-v1",
        _discovered_page_identity(provisional),
    )
    return DiscoveredEidrIdPage.model_validate(normalized)


class DiscoveredEidrIdPageRef(V2ContractModel):
    page_id: str
    shard_key: str
    page_index: int = Field(ge=0)
    start_ordinal: int = Field(ge=0)
    id_count: int = Field(gt=0, le=MAX_DISCOVERED_PAGE_IDS)
    first_eidr_id: str
    last_eidr_id: str
    page_object: ObjectRef

    @field_validator("page_id")
    @classmethod
    def validate_page_id(cls, value: str) -> str:
        return require_sha256(value, label="page_id")

    @field_validator("shard_key")
    @classmethod
    def validate_shard(cls, value: str) -> str:
        normalized = value.strip().lower()
        if _SHARD_PATTERN.fullmatch(normalized) is None:
            raise ValueError("page reference shard key must be two hex digits")
        return normalized

    @field_validator("first_eidr_id", "last_eidr_id")
    @classmethod
    def normalize_identifier(cls, value: str) -> str:
        return normalize_eidr_id(value)

    @model_validator(mode="after")
    def validate_reference(self) -> Self:
        _validate_json_control_ref(
            self.page_object,
            label="discovered-ID page",
            media_type=DISCOVERED_ID_PAGE_MEDIA_TYPE,
            max_bytes=DISCOVERED_PAGE_MAX_BYTES,
        )
        if self.first_eidr_id > self.last_eidr_id:
            raise ValueError("discovered-ID page key bounds are reversed")
        if (
            _eidr_shard(self.first_eidr_id) != self.shard_key
            or _eidr_shard(self.last_eidr_id) != self.shard_key
        ):
            raise ValueError("discovered-ID page bounds do not match their shard")
        return self


class DiscoveredEidrIdManifest(V2ContractModel):
    schema_version: str = "1.0"
    manifest_id: str
    source: EidrDiscoverySourceBinding
    page_size: int = Field(gt=0, le=MAX_DISCOVERED_PAGE_IDS)
    pages: tuple[DiscoveredEidrIdPageRef, ...] = Field(max_length=MAX_DISCOVERED_PAGES)
    id_count: int = Field(ge=0)
    page_count: int = Field(ge=0, le=MAX_DISCOVERED_PAGES)
    created_at: str

    @field_validator("manifest_id")
    @classmethod
    def validate_manifest_id(cls, value: str) -> str:
        return require_sha256(value, label="manifest_id")

    @field_validator("created_at")
    @classmethod
    def validate_created_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @model_validator(mode="after")
    def validate_manifest(self, info: ValidationInfo) -> Self:
        if self.page_count != len(self.pages):
            raise ValueError("discovered-ID page_count does not match pages")
        expected_ordinal = 0
        previous_key: tuple[str, int] | None = None
        previous_last_by_shard: dict[str, str] = {}
        for page in self.pages:
            key = (page.shard_key, page.page_index)
            if previous_key is not None and key <= previous_key:
                raise ValueError("discovered-ID page references must be ordered")
            if page.start_ordinal != expected_ordinal:
                raise ValueError("discovered-ID page ordinals must be contiguous")
            previous_last = previous_last_by_shard.get(page.shard_key)
            if previous_last is not None and page.first_eidr_id <= previous_last:
                raise ValueError("discovered-ID page ranges overlap")
            previous_last_by_shard[page.shard_key] = page.last_eidr_id
            expected_ordinal += page.id_count
            previous_key = key
        if expected_ordinal != self.id_count:
            raise ValueError("discovered-ID id_count does not match page references")
        if not (info.context or {}).get("skip_identity"):
            expected = deterministic_key(
                "eidr-discovered-id-manifest-v1",
                _discovered_manifest_identity(self),
            )
            if self.manifest_id != expected:
                raise ValueError("manifest_id does not match discovered-ID manifest")
        return self


def _discovered_manifest_identity(
    manifest: DiscoveredEidrIdManifest,
) -> dict[str, Any]:
    return {
        "schemaVersion": manifest.schema_version,
        "source": manifest.source.model_dump(
            mode="json",
            by_alias=True,
            exclude_none=True,
        ),
        "pageSize": manifest.page_size,
        "pages": [
            page.model_dump(mode="json", by_alias=True, exclude_none=True)
            for page in manifest.pages
        ],
        "idCount": manifest.id_count,
        "pageCount": manifest.page_count,
        "createdAt": manifest.created_at,
    }


def build_discovered_eidr_id_manifest(**values: Any) -> DiscoveredEidrIdManifest:
    provisional = DiscoveredEidrIdManifest.model_validate(
        {"manifest_id": _ZERO_DIGEST, **values},
        context={"skip_identity": True},
    )
    normalized = provisional.model_dump(mode="python")
    normalized["manifest_id"] = deterministic_key(
        "eidr-discovered-id-manifest-v1",
        _discovered_manifest_identity(provisional),
    )
    return DiscoveredEidrIdManifest.model_validate(normalized)


class PublishedDiscoveredEidrIds(V2ContractModel):
    manifest: DiscoveredEidrIdManifest
    manifest_object: ObjectRef

    @model_validator(mode="after")
    def validate_publication(self) -> Self:
        _require_model_reference(
            self.manifest,
            self.manifest_object,
            label="discovered-ID manifest",
            media_type=DISCOVERED_ID_MANIFEST_MEDIA_TYPE,
        )
        return self


@dataclass(frozen=True, slots=True)
class EidrDiscoveryRow:
    shard_key: str
    page_index: int
    eidr_id: str


def _eidr_shard(eidr_id: str) -> str:
    normalized = normalize_eidr_id(eidr_id)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:2]


def iter_discovered_eidr_rows(
    eidr_ids: Iterable[str],
    *,
    page_size: int = DEFAULT_DISCOVERED_PAGE_IDS,
) -> Iterator[EidrDiscoveryRow]:
    """Build deterministic local rows for tests and bounded local integrations."""

    if not 0 < page_size <= MAX_DISCOVERED_PAGE_IDS:
        raise ValueError("page_size is outside the supported bound")
    values = (
        {normalize_eidr_id(eidr_ids)}
        if isinstance(eidr_ids, str)
        else {normalize_eidr_id(item) for item in eidr_ids}
    )
    normalized = sorted(values, key=lambda item: (_eidr_shard(item), item))
    shard_position: dict[str, int] = {}
    for eidr_id in normalized:
        shard = _eidr_shard(eidr_id)
        position = shard_position.get(shard, 0)
        yield EidrDiscoveryRow(
            shard_key=shard,
            page_index=position // page_size,
            eidr_id=eidr_id,
        )
        shard_position[shard] = position + 1


def _coerce_discovery_row(value: Any) -> EidrDiscoveryRow:
    if isinstance(value, EidrDiscoveryRow):
        row = value
    elif isinstance(value, dict):
        row = EidrDiscoveryRow(
            shard_key=str(value["shard_key"]),
            page_index=int(value["page_index"]),
            eidr_id=str(value["eidr_id"]),
        )
    else:
        row = EidrDiscoveryRow(
            shard_key=str(value.shard_key),
            page_index=int(value.page_index),
            eidr_id=str(value.eidr_id),
        )
    normalized = normalize_eidr_id(row.eidr_id)
    shard = row.shard_key.strip().lower()
    if _SHARD_PATTERN.fullmatch(shard) is None or shard != _eidr_shard(normalized):
        raise ValueError("discovered-ID row has an invalid deterministic shard")
    if row.page_index < 0:
        raise ValueError("discovered-ID page index must be non-negative")
    return EidrDiscoveryRow(shard, row.page_index, normalized)


def publish_discovered_eidr_id_manifest(
    *,
    rows: Iterable[Any],
    source: EidrDiscoverySourceBinding,
    destination_prefix: str,
    created_at: str,
    store: RuntimeObjectStore,
    page_size: int = DEFAULT_DISCOVERED_PAGE_IDS,
) -> PublishedDiscoveredEidrIds:
    """Publish content-addressed pages and commit the bounded summary last."""

    created = require_rfc3339(created_at, label="created_at")
    if not 0 < page_size <= MAX_DISCOVERED_PAGE_IDS:
        raise ValueError("page_size is outside the supported bound")
    timestamped_store = _TimestampedStore(store, created)
    pointers: list[DiscoveredEidrIdPageRef] = []
    current_key: tuple[str, int] | None = None
    current_items: list[DiscoveredEidrId] = []
    expected_position_by_shard: dict[str, int] = {}
    previous_identifier_by_shard: dict[str, str] = {}
    total = 0

    def flush() -> None:
        nonlocal current_items
        if current_key is None or not current_items:
            return
        if len(pointers) >= MAX_DISCOVERED_PAGES:
            raise ValueError("discovered-ID manifest exceeds its page bound")
        shard_key, page_index = current_key
        page = build_discovered_eidr_id_page(
            source_binding_id=source.binding_id,
            shard_key=shard_key,
            page_index=page_index,
            start_ordinal=total - len(current_items),
            items=tuple(current_items),
        )
        payload = page.json_bytes()
        if len(payload) > DISCOVERED_PAGE_MAX_BYTES:
            raise ValueError("discovered-ID page exceeds its byte bound")
        page_object = timestamped_store.upload_bytes(
            payload,
            join_uri(
                destination_prefix,
                "eidr",
                "discovered-id-pages",
                f"source={source.binding_id.removeprefix('sha256:')}",
                f"shard={shard_key}",
                f"page={page_index:05d}",
                f"{page.page_id.removeprefix('sha256:')}.json",
            ),
            media_type=DISCOVERED_ID_PAGE_MEDIA_TYPE,
            object_format="OBJECT_FORMAT_JSON",
            max_bytes=DISCOVERED_PAGE_MAX_BYTES,
        ).object_ref
        pointers.append(
            DiscoveredEidrIdPageRef(
                page_id=page.page_id,
                shard_key=page.shard_key,
                page_index=page.page_index,
                start_ordinal=page.start_ordinal,
                id_count=len(page.items),
                first_eidr_id=page.items[0].eidr_id,
                last_eidr_id=page.items[-1].eidr_id,
                page_object=page_object,
            )
        )
        current_items = []

    previous_key: tuple[str, int, str] | None = None
    for raw_row in rows:
        row = _coerce_discovery_row(raw_row)
        position = expected_position_by_shard.get(row.shard_key, 0)
        expected_page = position // page_size
        if row.page_index != expected_page:
            raise ValueError("discovered-ID rows are not in deterministic pages")
        previous_identifier = previous_identifier_by_shard.get(row.shard_key)
        if previous_identifier is not None and row.eidr_id <= previous_identifier:
            raise ValueError("discovered-ID rows must be sorted and deduplicated")
        order_key = (row.shard_key, row.page_index, row.eidr_id)
        if previous_key is not None and order_key <= previous_key:
            raise ValueError("discovered-ID rows must use stable shard/page ordering")
        key = (row.shard_key, row.page_index)
        if current_key is not None and key != current_key:
            flush()
        current_key = key
        current_items.append(
            DiscoveredEidrId(
                discovered_id=discovered_eidr_id(row.eidr_id),
                eidr_id=row.eidr_id,
            )
        )
        total += 1
        expected_position_by_shard[row.shard_key] = position + 1
        previous_identifier_by_shard[row.shard_key] = row.eidr_id
        previous_key = order_key
    flush()

    manifest = build_discovered_eidr_id_manifest(
        source=source,
        page_size=page_size,
        pages=tuple(pointers),
        id_count=total,
        page_count=len(pointers),
        created_at=created,
    )
    payload = manifest.json_bytes()
    if len(payload) > CONTROL_MAX_BYTES:
        raise ValueError("discovered-ID summary exceeds the control object bound")
    manifest_object = timestamped_store.upload_bytes(
        payload,
        join_uri(
            destination_prefix,
            "eidr",
            "discovered-id-manifests",
            manifest.manifest_id.removeprefix("sha256:"),
            "manifest.json",
        ),
        media_type=DISCOVERED_ID_MANIFEST_MEDIA_TYPE,
        object_format="OBJECT_FORMAT_JSON",
        max_bytes=CONTROL_MAX_BYTES,
    ).object_ref
    return PublishedDiscoveredEidrIds(
        manifest=manifest,
        manifest_object=manifest_object,
    )


def build_discovered_eidr_id_frame(
    identifier_assertions: Any,
    *,
    silver_snapshot: CommunitySilverSnapshotSet,
    page_size: int = DEFAULT_DISCOVERED_PAGE_IDS,
) -> Any:
    """Distributed normalize/dedupe/page transform over a pinned assertion epoch."""

    if not 0 < page_size <= MAX_DISCOVERED_PAGE_IDS:
        raise ValueError("page_size is outside the supported bound")
    if silver_snapshot.data_snapshot_ids["community_identifier_assertion"] is None:
        raise ValueError("pinned Silver epoch has no IdentifierAssertion snapshot")
    required = {"run_id", "namespace_id", "value", "status"}
    missing = sorted(required - set(identifier_assertions.columns))
    if missing:
        raise ValueError(f"IdentifierAssertion frame is missing columns: {missing}")

    from pyspark.sql import Window
    from pyspark.sql import functions as F

    spark = identifier_assertions.sparkSession
    selected_runs = F.broadcast(
        spark.createDataFrame(
            [(run_id,) for run_id in silver_snapshot.committed_run_ids],
            "run_id STRING",
        )
    )
    candidates = (
        identifier_assertions.join(selected_runs, "run_id", "inner")
        .where(
            (F.col("namespace_id") == EIDR_NAMESPACE_ID) & (F.col("status") == "ACTIVE")
        )
        .select(
            F.upper(
                F.regexp_replace(
                    F.trim(F.col("value")),
                    r"(?i)^(?:https?://doi\.org/|doi:)",
                    "",
                )
            ).alias("eidr_id")
        )
    )
    eidr_pattern = r"^10\.5240/(?:[0-9A-Z]{4}-){5}[0-9A-Z]$"
    if candidates.where(~F.col("eidr_id").rlike(eidr_pattern)).limit(1).count():
        raise ValueError("pinned IdentifierAssertion contains an invalid EIDR ID")
    deduplicated = candidates.dropDuplicates(["eidr_id"]).withColumn(
        "shard_key",
        F.substring(F.sha2(F.col("eidr_id"), 256), 1, 2),
    )
    within_shard = Window.partitionBy("shard_key").orderBy("eidr_id")
    return (
        deduplicated.withColumn("_position", F.row_number().over(within_shard) - 1)
        .withColumn(
            "page_index",
            F.floor(F.col("_position") / F.lit(page_size)).cast("long"),
        )
        .select("shard_key", "page_index", "eidr_id")
    )


def extract_and_publish_discovered_eidr_ids(
    *,
    identifier_assertions: Any,
    silver_snapshot: CommunitySilverSnapshotSet,
    silver_snapshot_object: ObjectRef,
    source_release_id: str,
    destination_prefix: str,
    created_at: str,
    store: RuntimeObjectStore,
    page_size: int = DEFAULT_DISCOVERED_PAGE_IDS,
) -> PublishedDiscoveredEidrIds:
    """Extract on Spark, then stream bounded pages to immutable storage."""

    source = build_eidr_discovery_source_binding(
        source_release_id=source_release_id,
        silver_snapshot=silver_snapshot,
        silver_snapshot_object=silver_snapshot_object,
    )
    frame = build_discovered_eidr_id_frame(
        identifier_assertions,
        silver_snapshot=silver_snapshot,
        page_size=page_size,
    )
    ordered = frame.orderBy("shard_key", "page_index", "eidr_id")
    return publish_discovered_eidr_id_manifest(
        rows=ordered.toLocalIterator(),
        source=source,
        destination_prefix=destination_prefix,
        created_at=created_at,
        store=store,
        page_size=page_size,
    )


def read_discovered_eidr_id_manifest(
    *,
    reference: ObjectRef,
    store: RuntimeObjectStore,
) -> DiscoveredEidrIdManifest:
    _validate_json_control_ref(
        reference,
        label="discovered-ID manifest",
        media_type=DISCOVERED_ID_MANIFEST_MEDIA_TYPE,
    )
    store.verify(reference, max_bytes=CONTROL_MAX_BYTES)
    with tempfile.TemporaryDirectory(prefix="eidr-discovered-manifest-") as directory:
        materialized = store.download(
            reference,
            Path(directory) / "manifest.json",
            max_bytes=CONTROL_MAX_BYTES,
        )
        manifest = DiscoveredEidrIdManifest.model_validate_json(
            materialized.path.read_bytes()
        )
    _require_model_reference(
        manifest,
        reference,
        label="discovered-ID manifest",
        media_type=DISCOVERED_ID_MANIFEST_MEDIA_TYPE,
    )
    return manifest


class EidrLookupStatus(StrEnum):
    FOUND = "FOUND"
    NOT_FOUND = "NOT_FOUND"


@dataclass(frozen=True, slots=True)
class EidrExactLookupResult:
    eidr_id: str
    status: EidrLookupStatus
    xml: bytes | None = None
    attempt_count: int = 1
    retry_count: int = 0
    rate_limit_count: int = 0

    def __post_init__(self) -> None:
        object.__setattr__(self, "eidr_id", normalize_eidr_id(self.eidr_id))
        if not isinstance(self.status, EidrLookupStatus):
            object.__setattr__(self, "status", EidrLookupStatus(self.status))
        if self.attempt_count < 1:
            raise ValueError("provider attempt_count must be positive")
        if not 0 <= self.retry_count < self.attempt_count:
            raise ValueError("provider retry_count must be below attempt_count")
        if not 0 <= self.rate_limit_count <= self.retry_count:
            raise ValueError("provider rate_limit_count must be covered by retries")
        if self.status == EidrLookupStatus.FOUND:
            if not isinstance(self.xml, bytes) or not self.xml:
                raise ValueError("FOUND exact lookup requires non-empty XML bytes")
        elif self.xml is not None:
            raise ValueError("NOT_FOUND exact lookup cannot carry XML bytes")


class EidrProviderAuthorization(V2ContractModel):
    authorization_id: str
    provider_id: str
    authorization_object: ObjectRef
    policy_id: str
    policy_digest: str
    exact_lookup_allowed: Literal[True] = True
    complete_feed_allowed: bool = False
    issued_at: str

    @field_validator("authorization_id", "policy_digest")
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("provider_id", "policy_id")
    @classmethod
    def validate_slug(cls, value: str) -> str:
        return require_slug(value, label="provider authorization reference")

    @field_validator("issued_at")
    @classmethod
    def validate_issued_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @model_validator(mode="after")
    def validate_authorization(self, info: ValidationInfo) -> Self:
        _validate_immutable_ref(
            self.authorization_object,
            label="provider authorization",
            max_bytes=MAX_AUTHORIZATION_OBJECT_BYTES,
        )
        if not (info.context or {}).get("skip_identity"):
            expected = deterministic_key(
                "eidr-provider-authorization-v1",
                _provider_authorization_identity(self),
            )
            if self.authorization_id != expected:
                raise ValueError("authorization_id does not match authorization")
        return self


def _provider_authorization_identity(
    authorization: EidrProviderAuthorization,
) -> dict[str, Any]:
    return {
        "providerId": authorization.provider_id,
        "authorizationObject": _object_identity(authorization.authorization_object),
        "policyId": authorization.policy_id,
        "policyDigest": authorization.policy_digest,
        "exactLookupAllowed": authorization.exact_lookup_allowed,
        "completeFeedAllowed": authorization.complete_feed_allowed,
        "issuedAt": authorization.issued_at,
    }


def build_eidr_provider_authorization(**values: Any) -> EidrProviderAuthorization:
    provisional = EidrProviderAuthorization.model_validate(
        {"authorization_id": _ZERO_DIGEST, **values},
        context={"skip_identity": True},
    )
    normalized = provisional.model_dump(mode="python")
    normalized["authorization_id"] = deterministic_key(
        "eidr-provider-authorization-v1",
        _provider_authorization_identity(provisional),
    )
    return EidrProviderAuthorization.model_validate(normalized)


class EidrProvider(Protocol):
    """Injected provider exposing only bounded exact-ID lookup."""

    authorization: EidrProviderAuthorization

    def lookup_exact(
        self,
        *,
        eidr_ids: tuple[str, ...],
        request_id: str,
    ) -> Iterable[EidrExactLookupResult]: ...


class EidrCompleteFeedProof(V2ContractModel):
    proof_id: str
    authorization_id: str
    manifest_id: str
    start_ordinal: int = Field(ge=0)
    end_ordinal: int = Field(gt=0)
    coverage_id: str
    evidence_object: ObjectRef
    issued_at: str

    @field_validator("proof_id", "authorization_id", "manifest_id")
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("coverage_id")
    @classmethod
    def validate_coverage_id(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized or len(normalized) > 256:
            raise ValueError("complete-feed coverage_id must be non-empty and bounded")
        return normalized

    @field_validator("issued_at")
    @classmethod
    def validate_issued_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @model_validator(mode="after")
    def validate_proof(self, info: ValidationInfo) -> Self:
        if self.end_ordinal <= self.start_ordinal:
            raise ValueError("complete-feed proof range must be non-empty")
        _validate_immutable_ref(
            self.evidence_object,
            label="complete-feed evidence",
            max_bytes=CONTROL_MAX_BYTES,
        )
        if not (info.context or {}).get("skip_identity"):
            expected = deterministic_key(
                "eidr-complete-feed-proof-v1",
                _complete_feed_proof_identity(self),
            )
            if self.proof_id != expected:
                raise ValueError("proof_id does not match complete-feed proof")
        return self


def _complete_feed_proof_identity(proof: EidrCompleteFeedProof) -> dict[str, Any]:
    return {
        "authorizationId": proof.authorization_id,
        "manifestId": proof.manifest_id,
        "startOrdinal": proof.start_ordinal,
        "endOrdinal": proof.end_ordinal,
        "coverageId": proof.coverage_id,
        "evidenceObject": _object_identity(proof.evidence_object),
        "issuedAt": proof.issued_at,
    }


def build_eidr_complete_feed_proof(**values: Any) -> EidrCompleteFeedProof:
    provisional = EidrCompleteFeedProof.model_validate(
        {"proof_id": _ZERO_DIGEST, **values},
        context={"skip_identity": True},
    )
    normalized = provisional.model_dump(mode="python")
    normalized["proof_id"] = deterministic_key(
        "eidr-complete-feed-proof-v1",
        _complete_feed_proof_identity(provisional),
    )
    return EidrCompleteFeedProof.model_validate(normalized)


class EidrLookupBatch(V2ContractModel):
    schema_version: str = "1.0"
    batch_id: str
    manifest_id: str
    manifest_object: ObjectRef
    source_binding_id: str
    authorization_id: str
    watermark_before_id: str | None = None
    watermark_before_object: ObjectRef | None = None
    start_ordinal: int = Field(ge=0)
    end_ordinal: int = Field(gt=0)
    items: tuple[DiscoveredEidrId, ...] = Field(
        min_length=1,
        max_length=MAX_LOOKUP_BATCH_IDS,
    )
    complete_feed_proof_id: str | None = None
    requested_at: str

    @field_validator(
        "batch_id",
        "manifest_id",
        "source_binding_id",
        "authorization_id",
        "watermark_before_id",
        "complete_feed_proof_id",
    )
    @classmethod
    def validate_digest(cls, value: str | None) -> str | None:
        return None if value is None else require_sha256(value)

    @field_validator("requested_at")
    @classmethod
    def validate_requested_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @model_validator(mode="after")
    def validate_batch(self, info: ValidationInfo) -> Self:
        _validate_json_control_ref(
            self.manifest_object,
            label="lookup discovered-ID manifest",
            media_type=DISCOVERED_ID_MANIFEST_MEDIA_TYPE,
        )
        if (self.watermark_before_id is None) != (self.watermark_before_object is None):
            raise ValueError("lookup batch watermark ID/ObjectRef must be paired")
        if self.watermark_before_object is not None:
            _validate_json_control_ref(
                self.watermark_before_object,
                label="lookup input watermark",
                media_type=BACKFILL_WATERMARK_MEDIA_TYPE,
            )
        if self.end_ordinal - self.start_ordinal != len(self.items):
            raise ValueError("lookup batch range does not match its items")
        discovered = [item.discovered_id for item in self.items]
        if len(discovered) != len(set(discovered)):
            raise ValueError("lookup batch contains duplicate discovered IDs")
        if not (info.context or {}).get("skip_identity"):
            expected = deterministic_key(
                "eidr-exact-lookup-batch-v1",
                _lookup_batch_identity(self),
            )
            if self.batch_id != expected:
                raise ValueError("batch_id does not match exact-lookup batch")
        return self


def _lookup_batch_identity(batch: EidrLookupBatch) -> dict[str, Any]:
    return {
        "schemaVersion": batch.schema_version,
        "manifestId": batch.manifest_id,
        "manifestObject": _object_identity(batch.manifest_object),
        "sourceBindingId": batch.source_binding_id,
        "authorizationId": batch.authorization_id,
        "watermarkBeforeId": batch.watermark_before_id,
        "watermarkBeforeObject": (
            None
            if batch.watermark_before_object is None
            else _object_identity(batch.watermark_before_object)
        ),
        "startOrdinal": batch.start_ordinal,
        "endOrdinal": batch.end_ordinal,
        "items": [item.model_dump(mode="json", by_alias=True) for item in batch.items],
        "completeFeedProofId": batch.complete_feed_proof_id,
        "requestedAt": batch.requested_at,
    }


def build_eidr_lookup_batch(**values: Any) -> EidrLookupBatch:
    provisional = EidrLookupBatch.model_validate(
        {"batch_id": _ZERO_DIGEST, **values},
        context={"skip_identity": True},
    )
    normalized = provisional.model_dump(mode="python")
    normalized["batch_id"] = deterministic_key(
        "eidr-exact-lookup-batch-v1",
        _lookup_batch_identity(provisional),
    )
    return EidrLookupBatch.model_validate(normalized)


class EidrLookupItemReceipt(V2ContractModel):
    discovered_id: str
    eidr_id: str
    status: EidrLookupStatus
    attempt_count: int = Field(gt=0)
    retry_count: int = Field(ge=0)
    rate_limit_count: int = Field(ge=0)
    raw_object: ObjectRef | None = None

    @field_validator("discovered_id")
    @classmethod
    def validate_discovered_id(cls, value: str) -> str:
        return require_sha256(value, label="discovered_id")

    @field_validator("eidr_id")
    @classmethod
    def normalize_identifier(cls, value: str) -> str:
        return normalize_eidr_id(value)

    @model_validator(mode="after")
    def validate_item(self) -> Self:
        if self.discovered_id != discovered_eidr_id(self.eidr_id):
            raise ValueError("receipt discovered_id does not bind its EIDR ID")
        if self.retry_count >= self.attempt_count:
            raise ValueError("receipt retries must be below attempt count")
        if self.rate_limit_count > self.retry_count:
            raise ValueError("receipt rate limits must be covered by retries")
        if self.status == EidrLookupStatus.FOUND:
            if self.raw_object is None:
                raise ValueError("FOUND receipt requires a raw XML ObjectRef")
            _validate_immutable_ref(
                self.raw_object,
                label="provider XML",
                max_bytes=MAX_MAX_XML_BYTES,
                media_type="application/xml",
            )
        elif self.raw_object is not None:
            raise ValueError("NOT_FOUND receipt cannot reference raw XML")
        return self


class EidrLookupReceipt(V2ContractModel):
    """Commit-last window receipt for one exact-lookup ordinal slice."""

    schema_version: str = "1.0"
    receipt_id: str
    status: Literal["COMPLETED"] = "COMPLETED"
    lookup_batch_id: str
    lookup_batch_object: ObjectRef
    manifest_id: str
    source_binding_id: str
    start_ordinal: int = Field(ge=0)
    next_ordinal: int = Field(gt=0)
    items: tuple[EidrLookupItemReceipt, ...] = Field(
        min_length=1,
        max_length=MAX_LOOKUP_BATCH_IDS,
    )
    retry_count: int = Field(ge=0)
    rate_limit_count: int = Field(ge=0)
    connector_batch_id: str | None = None
    connector_batch_object: ObjectRef | None = None
    record_set_id: str | None = None
    record_set_object: ObjectRef | None = None
    completed_at: str

    @field_validator(
        "receipt_id",
        "lookup_batch_id",
        "manifest_id",
        "source_binding_id",
        "connector_batch_id",
        "record_set_id",
    )
    @classmethod
    def validate_digest(cls, value: str | None) -> str | None:
        return None if value is None else require_sha256(value)

    @field_validator("completed_at")
    @classmethod
    def validate_completed_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @model_validator(mode="after")
    def validate_receipt(self, info: ValidationInfo) -> Self:
        _validate_json_control_ref(
            self.lookup_batch_object,
            label="lookup batch",
            media_type=LOOKUP_BATCH_MEDIA_TYPE,
        )
        if self.next_ordinal - self.start_ordinal != len(self.items):
            raise ValueError("receipt ordinal range does not match its items")
        if self.retry_count != sum(item.retry_count for item in self.items):
            raise ValueError("receipt retry_count does not match item receipts")
        if self.rate_limit_count != sum(item.rate_limit_count for item in self.items):
            raise ValueError("receipt rate_limit_count does not match item receipts")
        capture_values = (
            self.connector_batch_id,
            self.connector_batch_object,
            self.record_set_id,
            self.record_set_object,
        )
        if any(value is not None for value in capture_values) != all(
            value is not None for value in capture_values
        ):
            raise ValueError("receipt Connector capture references must be complete")
        found = any(item.status == EidrLookupStatus.FOUND for item in self.items)
        if found != (self.connector_batch_id is not None):
            raise ValueError("receipt capture presence must match FOUND results")
        if self.connector_batch_object is not None:
            _validate_json_control_ref(
                self.connector_batch_object,
                label="Connector batch manifest",
                media_type=(
                    "application/vnd.video-media-catalog.connector-batch.v2+json"
                ),
            )
        if self.record_set_object is not None:
            _validate_json_control_ref(
                self.record_set_object,
                label="Connector record set",
                media_type=("application/vnd.video-media-catalog.record-set.v2+json"),
            )
        if not (info.context or {}).get("skip_identity"):
            expected = deterministic_key(
                "eidr-exact-lookup-receipt-v1",
                _lookup_receipt_identity(self),
            )
            if self.receipt_id != expected:
                raise ValueError("receipt_id does not match lookup receipt")
        return self


def _lookup_receipt_identity(receipt: EidrLookupReceipt) -> dict[str, Any]:
    return {
        "schemaVersion": receipt.schema_version,
        "status": receipt.status,
        "lookupBatchId": receipt.lookup_batch_id,
        "lookupBatchObject": _object_identity(receipt.lookup_batch_object),
        "manifestId": receipt.manifest_id,
        "sourceBindingId": receipt.source_binding_id,
        "startOrdinal": receipt.start_ordinal,
        "nextOrdinal": receipt.next_ordinal,
        "items": [
            item.model_dump(mode="json", by_alias=True, exclude_none=True)
            for item in receipt.items
        ],
        "retryCount": receipt.retry_count,
        "rateLimitCount": receipt.rate_limit_count,
        "connectorBatchId": receipt.connector_batch_id,
        "connectorBatchObject": (
            None
            if receipt.connector_batch_object is None
            else _object_identity(receipt.connector_batch_object)
        ),
        "recordSetId": receipt.record_set_id,
        "recordSetObject": (
            None
            if receipt.record_set_object is None
            else _object_identity(receipt.record_set_object)
        ),
        "completedAt": receipt.completed_at,
    }


def build_eidr_lookup_receipt(**values: Any) -> EidrLookupReceipt:
    provisional = EidrLookupReceipt.model_validate(
        {"receipt_id": _ZERO_DIGEST, **values},
        context={"skip_identity": True},
    )
    normalized = provisional.model_dump(mode="python")
    normalized["receipt_id"] = deterministic_key(
        "eidr-exact-lookup-receipt-v1",
        _lookup_receipt_identity(provisional),
    )
    return EidrLookupReceipt.model_validate(normalized)


class EidrBackfillWatermark(V2ContractModel):
    """Append-only discovered-ID ordinal watermark; never a mutable latest."""

    schema_version: str = "1.0"
    watermark_id: str
    manifest_id: str
    source_binding_id: str
    total_id_count: int = Field(ge=0)
    next_ordinal: int = Field(ge=0)
    previous_watermark_id: str | None = None
    receipt_id: str
    receipt_object: ObjectRef
    updated_at: str

    @field_validator(
        "watermark_id",
        "manifest_id",
        "source_binding_id",
        "previous_watermark_id",
        "receipt_id",
    )
    @classmethod
    def validate_digest(cls, value: str | None) -> str | None:
        return None if value is None else require_sha256(value)

    @field_validator("updated_at")
    @classmethod
    def validate_updated_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @model_validator(mode="after")
    def validate_watermark(self, info: ValidationInfo) -> Self:
        if self.next_ordinal > self.total_id_count:
            raise ValueError("watermark exceeds discovered-ID manifest")
        _validate_json_control_ref(
            self.receipt_object,
            label="lookup receipt",
            media_type=LOOKUP_RECEIPT_MEDIA_TYPE,
        )
        if not (info.context or {}).get("skip_identity"):
            expected = deterministic_key(
                "eidr-backfill-watermark-v1",
                _watermark_identity(self),
            )
            if self.watermark_id != expected:
                raise ValueError("watermark_id does not match watermark")
        return self


def _watermark_identity(watermark: EidrBackfillWatermark) -> dict[str, Any]:
    return {
        "schemaVersion": watermark.schema_version,
        "manifestId": watermark.manifest_id,
        "sourceBindingId": watermark.source_binding_id,
        "totalIdCount": watermark.total_id_count,
        "nextOrdinal": watermark.next_ordinal,
        "previousWatermarkId": watermark.previous_watermark_id,
        "receiptId": watermark.receipt_id,
        "receiptObject": _object_identity(watermark.receipt_object),
        "updatedAt": watermark.updated_at,
    }


def build_eidr_backfill_watermark(**values: Any) -> EidrBackfillWatermark:
    provisional = EidrBackfillWatermark.model_validate(
        {"watermark_id": _ZERO_DIGEST, **values},
        context={"skip_identity": True},
    )
    normalized = provisional.model_dump(mode="python")
    normalized["watermark_id"] = deterministic_key(
        "eidr-backfill-watermark-v1",
        _watermark_identity(provisional),
    )
    return EidrBackfillWatermark.model_validate(normalized)


def read_eidr_backfill_watermark(
    *,
    reference: ObjectRef,
    store: RuntimeObjectStore,
) -> EidrBackfillWatermark:
    _validate_json_control_ref(
        reference,
        label="EIDR backfill watermark",
        media_type=BACKFILL_WATERMARK_MEDIA_TYPE,
    )
    store.verify(reference, max_bytes=CONTROL_MAX_BYTES)
    with tempfile.TemporaryDirectory(prefix="eidr-backfill-watermark-") as directory:
        materialized = store.download(
            reference,
            Path(directory) / "watermark.json",
            max_bytes=CONTROL_MAX_BYTES,
        )
        watermark = EidrBackfillWatermark.model_validate_json(
            materialized.path.read_bytes()
        )
    _require_model_reference(
        watermark,
        reference,
        label="EIDR backfill watermark",
        media_type=BACKFILL_WATERMARK_MEDIA_TYPE,
    )
    return watermark


@dataclass(frozen=True)
class EidrBackfillBatchResult:
    completed: bool
    lookup_batch: EidrLookupBatch | None = None
    lookup_batch_object: ObjectRef | None = None
    capture: PublishedConnectorCapture | None = None
    receipt: EidrLookupReceipt | None = None
    receipt_object: ObjectRef | None = None
    watermark: EidrBackfillWatermark | None = None
    watermark_object: ObjectRef | None = None


class EidrBackfillStopReason(StrEnum):
    MANIFEST_EXHAUSTED = "MANIFEST_EXHAUSTED"
    MAX_BATCHES = "MAX_BATCHES"
    MAX_DURATION = "MAX_DURATION"
    MAX_IDS = "MAX_IDS"


class EidrBackfillRunBatch(V2ContractModel):
    """One preserved single-batch commit inside a bounded manifest run."""

    batch_index: int = Field(ge=0)
    start_ordinal: int = Field(ge=0)
    next_ordinal: int = Field(gt=0)
    lookup_batch_id: str
    lookup_batch_object: ObjectRef
    receipt_id: str
    receipt_object: ObjectRef
    watermark_id: str
    watermark_object: ObjectRef
    connector_batch_id: str | None = None
    connector_batch_object: ObjectRef | None = None
    record_set_id: str | None = None
    record_set_object: ObjectRef | None = None
    found_count: int = Field(ge=0)
    not_found_count: int = Field(ge=0)
    attempt_count: int = Field(ge=1)
    retry_count: int = Field(ge=0)
    rate_limit_count: int = Field(ge=0)

    @field_validator(
        "lookup_batch_id",
        "receipt_id",
        "watermark_id",
        "connector_batch_id",
        "record_set_id",
    )
    @classmethod
    def validate_digest(cls, value: str | None) -> str | None:
        return None if value is None else require_sha256(value)

    @model_validator(mode="after")
    def validate_batch(self) -> Self:
        item_count = self.next_ordinal - self.start_ordinal
        if item_count <= 0:
            raise ValueError("backfill run batch ordinal range must be non-empty")
        if self.found_count + self.not_found_count != item_count:
            raise ValueError("backfill run batch counts do not match its range")
        if self.attempt_count < item_count:
            raise ValueError("backfill run batch attempts are below its item count")
        if self.retry_count != self.attempt_count - item_count:
            raise ValueError("backfill run batch retry count does not match attempts")
        if self.rate_limit_count > self.retry_count:
            raise ValueError("backfill run batch rate limits exceed retries")
        _validate_json_control_ref(
            self.lookup_batch_object,
            label="run lookup batch",
            media_type=LOOKUP_BATCH_MEDIA_TYPE,
        )
        _validate_json_control_ref(
            self.receipt_object,
            label="run lookup receipt",
            media_type=LOOKUP_RECEIPT_MEDIA_TYPE,
        )
        _validate_json_control_ref(
            self.watermark_object,
            label="run backfill watermark",
            media_type=BACKFILL_WATERMARK_MEDIA_TYPE,
        )
        capture_values = (
            self.connector_batch_id,
            self.connector_batch_object,
            self.record_set_id,
            self.record_set_object,
        )
        if any(value is not None for value in capture_values) != all(
            value is not None for value in capture_values
        ):
            raise ValueError("backfill run Connector capture references must be paired")
        if (self.found_count > 0) != (self.connector_batch_id is not None):
            raise ValueError("backfill run capture presence must match found records")
        if self.connector_batch_object is not None:
            _validate_json_control_ref(
                self.connector_batch_object,
                label="run Connector batch",
                media_type=(
                    "application/vnd.video-media-catalog.connector-batch.v2+json"
                ),
            )
        if self.record_set_object is not None:
            _validate_json_control_ref(
                self.record_set_object,
                label="run Connector record set",
                media_type=("application/vnd.video-media-catalog.record-set.v2+json"),
            )
        return self


class EidrBackfillRunManifest(V2ContractModel):
    """Bounded index of independent partial captures for Source Silver fan-out."""

    schema_version: str = "1.0"
    run_manifest_id: str
    discovered_manifest_id: str
    discovered_manifest_object: ObjectRef
    source_binding_id: str
    authorization_id: str
    start_ordinal: int = Field(ge=0)
    next_ordinal: int = Field(ge=0)
    total_id_count: int = Field(ge=0)
    watermark_before_id: str | None = None
    watermark_before_object: ObjectRef | None = None
    watermark_after_id: str | None = None
    watermark_after_object: ObjectRef | None = None
    batches: tuple[EidrBackfillRunBatch, ...] = Field(
        max_length=MAX_BACKFILL_MAX_BATCHES
    )
    batch_count: int = Field(ge=0, le=MAX_BACKFILL_MAX_BATCHES)
    attempted_id_count: int = Field(ge=0, le=MAX_BACKFILL_MAX_IDS)
    found_count: int = Field(ge=0)
    not_found_count: int = Field(ge=0)
    attempt_count: int = Field(ge=0)
    retry_count: int = Field(ge=0)
    rate_limit_count: int = Field(ge=0)
    completed: bool
    stop_reason: EidrBackfillStopReason
    source_completeness: Literal["PARTIAL"] = "PARTIAL"
    complete_feed_allowed: Literal[False] = False
    created_at: str

    @field_validator(
        "run_manifest_id",
        "discovered_manifest_id",
        "source_binding_id",
        "authorization_id",
        "watermark_before_id",
        "watermark_after_id",
    )
    @classmethod
    def validate_digest(cls, value: str | None) -> str | None:
        return None if value is None else require_sha256(value)

    @field_validator("created_at")
    @classmethod
    def validate_created_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @model_validator(mode="after")
    def validate_manifest(self, info: ValidationInfo) -> Self:
        _validate_json_control_ref(
            self.discovered_manifest_object,
            label="run discovered-ID manifest",
            media_type=DISCOVERED_ID_MANIFEST_MEDIA_TYPE,
        )
        if (self.watermark_before_id is None) != (self.watermark_before_object is None):
            raise ValueError("run input watermark ID/ObjectRef must be paired")
        if (self.watermark_after_id is None) != (self.watermark_after_object is None):
            raise ValueError("run output watermark ID/ObjectRef must be paired")
        for label, reference in (
            ("run input watermark", self.watermark_before_object),
            ("run output watermark", self.watermark_after_object),
        ):
            if reference is not None:
                _validate_json_control_ref(
                    reference,
                    label=label,
                    media_type=BACKFILL_WATERMARK_MEDIA_TYPE,
                )
        if not (self.start_ordinal <= self.next_ordinal <= self.total_id_count):
            raise ValueError("backfill run ordinals exceed the discovered manifest")
        if self.batch_count != len(self.batches):
            raise ValueError("backfill run batch_count does not match batches")
        if [item.batch_index for item in self.batches] != list(
            range(len(self.batches))
        ):
            raise ValueError("backfill run batch indexes must be contiguous")
        for field_name in (
            "lookup_batch_id",
            "receipt_id",
            "watermark_id",
            "connector_batch_id",
            "record_set_id",
        ):
            values = [
                value
                for batch in self.batches
                if (value := getattr(batch, field_name)) is not None
            ]
            if len(values) != len(set(values)):
                raise ValueError(f"backfill run contains duplicate {field_name} values")
        expected_ordinal = self.start_ordinal
        for batch in self.batches:
            if batch.start_ordinal != expected_ordinal:
                raise ValueError("backfill run batch ordinals must be contiguous")
            expected_ordinal = batch.next_ordinal
        if expected_ordinal != self.next_ordinal:
            raise ValueError("backfill run next ordinal does not match batches")
        if self.attempted_id_count != self.next_ordinal - self.start_ordinal:
            raise ValueError("backfill run attempted IDs do not match its range")
        for field_name in (
            "found_count",
            "not_found_count",
            "attempt_count",
            "retry_count",
            "rate_limit_count",
        ):
            if getattr(self, field_name) != sum(
                getattr(batch, field_name) for batch in self.batches
            ):
                raise ValueError(
                    f"backfill run {field_name} does not match its batches"
                )
        if self.found_count + self.not_found_count != self.attempted_id_count:
            raise ValueError("backfill run result counts do not match attempted IDs")
        if self.batches:
            if (
                self.watermark_after_id != self.batches[-1].watermark_id
                or self.watermark_after_object != self.batches[-1].watermark_object
            ):
                raise ValueError("backfill run output watermark does not match batches")
        elif (
            self.watermark_after_id != self.watermark_before_id
            or self.watermark_after_object != self.watermark_before_object
        ):
            raise ValueError("empty backfill run must preserve its input watermark")
        exhausted = self.next_ordinal == self.total_id_count
        if self.completed != exhausted:
            raise ValueError("backfill run completed flag does not match its ordinal")
        if exhausted != (self.stop_reason == EidrBackfillStopReason.MANIFEST_EXHAUSTED):
            raise ValueError("backfill run stop reason does not match completion")
        if not (info.context or {}).get("skip_identity"):
            expected = deterministic_key(
                "eidr-backfill-run-manifest-v1",
                _backfill_run_manifest_identity(self),
            )
            if self.run_manifest_id != expected:
                raise ValueError("run_manifest_id does not match backfill run manifest")
        return self


def _backfill_run_manifest_identity(
    manifest: EidrBackfillRunManifest,
) -> dict[str, Any]:
    return {
        "schemaVersion": manifest.schema_version,
        "discoveredManifestId": manifest.discovered_manifest_id,
        "discoveredManifestObject": _object_identity(
            manifest.discovered_manifest_object
        ),
        "sourceBindingId": manifest.source_binding_id,
        "authorizationId": manifest.authorization_id,
        "startOrdinal": manifest.start_ordinal,
        "nextOrdinal": manifest.next_ordinal,
        "totalIdCount": manifest.total_id_count,
        "watermarkBeforeId": manifest.watermark_before_id,
        "watermarkBeforeObject": (
            None
            if manifest.watermark_before_object is None
            else _object_identity(manifest.watermark_before_object)
        ),
        "watermarkAfterId": manifest.watermark_after_id,
        "watermarkAfterObject": (
            None
            if manifest.watermark_after_object is None
            else _object_identity(manifest.watermark_after_object)
        ),
        "batches": [
            item.model_dump(mode="json", by_alias=True, exclude_none=True)
            for item in manifest.batches
        ],
        "batchCount": manifest.batch_count,
        "attemptedIdCount": manifest.attempted_id_count,
        "foundCount": manifest.found_count,
        "notFoundCount": manifest.not_found_count,
        "attemptCount": manifest.attempt_count,
        "retryCount": manifest.retry_count,
        "rateLimitCount": manifest.rate_limit_count,
        "completed": manifest.completed,
        "stopReason": manifest.stop_reason,
        "sourceCompleteness": manifest.source_completeness,
        "completeFeedAllowed": manifest.complete_feed_allowed,
        "createdAt": manifest.created_at,
    }


def build_eidr_backfill_run_manifest(**values: Any) -> EidrBackfillRunManifest:
    provisional = EidrBackfillRunManifest.model_validate(
        {"run_manifest_id": _ZERO_DIGEST, **values},
        context={"skip_identity": True},
    )
    normalized = provisional.model_dump(mode="python")
    normalized["run_manifest_id"] = deterministic_key(
        "eidr-backfill-run-manifest-v1",
        _backfill_run_manifest_identity(provisional),
    )
    return EidrBackfillRunManifest.model_validate(normalized)


@dataclass(frozen=True)
class EidrBackfillRunResult:
    manifest: EidrBackfillRunManifest
    manifest_object: ObjectRef
    watermark: EidrBackfillWatermark | None
    watermark_object: ObjectRef | None


def _read_page(
    *,
    pointer: DiscoveredEidrIdPageRef,
    source_binding_id: str,
    store: RuntimeObjectStore,
) -> DiscoveredEidrIdPage:
    store.verify(pointer.page_object, max_bytes=DISCOVERED_PAGE_MAX_BYTES)
    with tempfile.TemporaryDirectory(prefix="eidr-discovered-page-") as directory:
        materialized = store.download(
            pointer.page_object,
            Path(directory) / "page.json",
            max_bytes=DISCOVERED_PAGE_MAX_BYTES,
        )
        page = DiscoveredEidrIdPage.model_validate_json(materialized.path.read_bytes())
    _require_model_reference(
        page,
        pointer.page_object,
        label="discovered-ID page",
        media_type=DISCOVERED_ID_PAGE_MEDIA_TYPE,
        max_bytes=DISCOVERED_PAGE_MAX_BYTES,
    )
    if (
        page.page_id != pointer.page_id
        or page.source_binding_id != source_binding_id
        or page.shard_key != pointer.shard_key
        or page.page_index != pointer.page_index
        or page.start_ordinal != pointer.start_ordinal
        or len(page.items) != pointer.id_count
        or page.items[0].eidr_id != pointer.first_eidr_id
        or page.items[-1].eidr_id != pointer.last_eidr_id
    ):
        raise ValueError("discovered-ID page does not match manifest pointer")
    return page


def _select_lookup_items(
    *,
    manifest: DiscoveredEidrIdManifest,
    start_ordinal: int,
    limit: int,
    store: RuntimeObjectStore,
) -> tuple[DiscoveredEidrId, ...]:
    selected: list[DiscoveredEidrId] = []
    end_ordinal = min(manifest.id_count, start_ordinal + limit)
    for pointer in manifest.pages:
        page_end = pointer.start_ordinal + pointer.id_count
        if page_end <= start_ordinal:
            continue
        if pointer.start_ordinal >= end_ordinal:
            break
        page = _read_page(
            pointer=pointer,
            source_binding_id=manifest.source.binding_id,
            store=store,
        )
        slice_start = max(0, start_ordinal - pointer.start_ordinal)
        slice_end = min(pointer.id_count, end_ordinal - pointer.start_ordinal)
        selected.extend(page.items[slice_start:slice_end])
        if len(selected) >= limit:
            break
    expected = end_ordinal - start_ordinal
    if len(selected) != expected:
        raise ValueError("discovered-ID pages do not cover the requested range")
    return tuple(selected)


def _validate_provider(
    provider: EidrProvider | None,
    *,
    store: RuntimeObjectStore,
) -> EidrProviderAuthorization:
    if provider is None:
        raise EidrProviderNotAuthorizedError(
            "EIDR exact lookup requires an explicitly injected authorized provider"
        )
    authorization = getattr(provider, "authorization", None)
    if not isinstance(authorization, EidrProviderAuthorization):
        raise EidrProviderNotAuthorizedError(
            "injected EIDR provider lacks a validated authorization contract"
        )
    policy = eidr_rights_profile()
    if (
        authorization.policy_id != policy.policy_id
        or authorization.policy_digest != policy.digest
        or not authorization.exact_lookup_allowed
    ):
        raise EidrProviderNotAuthorizedError(
            "EIDR provider authorization does not bind the active source policy"
        )
    store.verify(
        authorization.authorization_object,
        max_bytes=MAX_AUTHORIZATION_OBJECT_BYTES,
    )
    return authorization


def _validate_complete_feed_proof(
    proof: EidrCompleteFeedProof | None,
    *,
    authorization: EidrProviderAuthorization,
    manifest: DiscoveredEidrIdManifest,
    start_ordinal: int,
    end_ordinal: int,
    store: RuntimeObjectStore,
) -> None:
    if proof is None:
        return
    if not authorization.complete_feed_allowed:
        raise EidrProviderNotAuthorizedError(
            "provider authorization does not permit complete-feed semantics"
        )
    if (
        proof.authorization_id != authorization.authorization_id
        or proof.manifest_id != manifest.manifest_id
        or proof.start_ordinal != start_ordinal
        or proof.end_ordinal != end_ordinal
    ):
        raise EidrProviderNotAuthorizedError(
            "complete-feed proof does not bind this exact lookup range"
        )
    store.verify(proof.evidence_object, max_bytes=CONTROL_MAX_BYTES)


def _provider_results(
    provider: EidrProvider,
    *,
    batch: EidrLookupBatch,
    max_xml_bytes: int,
) -> tuple[EidrExactLookupResult, ...]:
    raw = tuple(
        islice(
            provider.lookup_exact(
                eidr_ids=tuple(item.eidr_id for item in batch.items),
                request_id=batch.batch_id,
            ),
            len(batch.items) + 1,
        )
    )
    if len(raw) != len(batch.items):
        raise EidrBackfillError(
            "provider must return exactly one result per exact EIDR ID"
        )
    if any(not isinstance(item, EidrExactLookupResult) for item in raw):
        raise EidrBackfillError("provider returned an invalid exact-lookup result")
    by_id: dict[str, EidrExactLookupResult] = {}
    for result in raw:
        if result.eidr_id in by_id:
            raise EidrBackfillError("provider returned a duplicate EIDR result")
        if result.xml is not None and len(result.xml) > max_xml_bytes:
            raise EidrBackfillError("provider XML exceeds the configured byte bound")
        by_id[result.eidr_id] = result
    requested = {item.eidr_id for item in batch.items}
    if set(by_id) != requested:
        raise EidrBackfillError("provider results do not match exact requested IDs")
    return tuple(by_id[item.eidr_id] for item in batch.items)


def _parse_exact_xml(
    *,
    result: EidrExactLookupResult,
    path: Path,
) -> dict[str, object]:
    assert result.xml is not None
    path.write_bytes(result.xml)
    payloads = tuple(islice(iter_eidr_payloads(path), 2))
    if len(payloads) != 1 or payloads[0].get("id") != result.eidr_id:
        raise EidrBackfillError(
            "provider XML must contain exactly the requested EIDR record"
        )
    return payloads[0]


def _watermark_value(manifest_id: str, ordinal: int) -> str:
    return f"{manifest_id}:ordinal:{ordinal}"


def run_eidr_exact_lookup_batch(
    *,
    manifest: DiscoveredEidrIdManifest,
    manifest_object: ObjectRef,
    destination_prefix: str,
    acquired_at: str,
    image_digest: str,
    store: RuntimeObjectStore,
    provider: EidrProvider | None,
    watermark: EidrBackfillWatermark | None = None,
    watermark_object: ObjectRef | None = None,
    batch_size: int = DEFAULT_LOOKUP_BATCH_IDS,
    max_xml_bytes: int = DEFAULT_MAX_XML_BYTES,
    record_shard_bytes: int = DEFAULT_RECORD_SHARD_BYTES,
    complete_feed_proof: EidrCompleteFeedProof | None = None,
) -> EidrBackfillBatchResult:
    """Run one bounded exact-ID batch and publish its watermark last."""

    if not _SOURCE_SEMAPHORE.acquire(blocking=False):
        raise EidrBackfillError(
            "EIDR exact lookup source semaphore permits only one in-flight batch"
        )
    try:
        return _run_eidr_exact_lookup_batch(
            manifest=manifest,
            manifest_object=manifest_object,
            destination_prefix=destination_prefix,
            acquired_at=acquired_at,
            image_digest=image_digest,
            store=store,
            provider=provider,
            watermark=watermark,
            watermark_object=watermark_object,
            batch_size=batch_size,
            max_xml_bytes=max_xml_bytes,
            record_shard_bytes=record_shard_bytes,
            complete_feed_proof=complete_feed_proof,
        )
    finally:
        _SOURCE_SEMAPHORE.release()


def _run_eidr_exact_lookup_batch(
    *,
    manifest: DiscoveredEidrIdManifest,
    manifest_object: ObjectRef,
    destination_prefix: str,
    acquired_at: str,
    image_digest: str,
    store: RuntimeObjectStore,
    provider: EidrProvider | None,
    watermark: EidrBackfillWatermark | None = None,
    watermark_object: ObjectRef | None = None,
    batch_size: int = DEFAULT_LOOKUP_BATCH_IDS,
    max_xml_bytes: int = DEFAULT_MAX_XML_BYTES,
    record_shard_bytes: int = DEFAULT_RECORD_SHARD_BYTES,
    complete_feed_proof: EidrCompleteFeedProof | None = None,
) -> EidrBackfillBatchResult:
    authorization = _validate_provider(provider, store=store)
    assert provider is not None
    acquired = require_rfc3339(acquired_at, label="acquired_at")
    image = require_sha256(image_digest, label="image_digest")
    if not 0 < batch_size <= MAX_LOOKUP_BATCH_IDS:
        raise ValueError("batch_size is outside the supported bound")
    if not 0 < max_xml_bytes <= MAX_MAX_XML_BYTES:
        raise ValueError("max_xml_bytes is outside the supported bound")
    if not 0 < record_shard_bytes <= MAX_RECORD_SHARD_BYTES:
        raise ValueError("record_shard_bytes is outside the supported bound")
    _require_model_reference(
        manifest,
        manifest_object,
        label="discovered-ID manifest",
        media_type=DISCOVERED_ID_MANIFEST_MEDIA_TYPE,
    )
    store.verify(manifest_object, max_bytes=CONTROL_MAX_BYTES)
    if (watermark is None) != (watermark_object is None):
        raise ValueError("watermark and watermark ObjectRef must be supplied together")
    start_ordinal = 0
    if watermark is not None and watermark_object is not None:
        _require_model_reference(
            watermark,
            watermark_object,
            label="EIDR backfill watermark",
            media_type=BACKFILL_WATERMARK_MEDIA_TYPE,
        )
        store.verify(watermark_object, max_bytes=CONTROL_MAX_BYTES)
        if (
            watermark.manifest_id != manifest.manifest_id
            or watermark.source_binding_id != manifest.source.binding_id
            or watermark.total_id_count != manifest.id_count
        ):
            raise ValueError("watermark does not bind this discovered-ID manifest")
        start_ordinal = watermark.next_ordinal
    if start_ordinal == manifest.id_count:
        return EidrBackfillBatchResult(
            completed=True,
            watermark=watermark,
            watermark_object=watermark_object,
        )

    items = _select_lookup_items(
        manifest=manifest,
        start_ordinal=start_ordinal,
        limit=batch_size,
        store=store,
    )
    end_ordinal = start_ordinal + len(items)
    _validate_complete_feed_proof(
        complete_feed_proof,
        authorization=authorization,
        manifest=manifest,
        start_ordinal=start_ordinal,
        end_ordinal=end_ordinal,
        store=store,
    )
    batch = build_eidr_lookup_batch(
        manifest_id=manifest.manifest_id,
        manifest_object=manifest_object,
        source_binding_id=manifest.source.binding_id,
        authorization_id=authorization.authorization_id,
        watermark_before_id=(None if watermark is None else watermark.watermark_id),
        watermark_before_object=watermark_object,
        start_ordinal=start_ordinal,
        end_ordinal=end_ordinal,
        items=items,
        complete_feed_proof_id=(
            None if complete_feed_proof is None else complete_feed_proof.proof_id
        ),
        requested_at=acquired,
    )
    timestamped_store = _TimestampedStore(store, acquired)
    batch_object = timestamped_store.upload_bytes(
        batch.json_bytes(),
        join_uri(
            destination_prefix,
            "eidr",
            "exact-lookup-batches",
            batch.batch_id.removeprefix("sha256:"),
            "batch.json",
        ),
        media_type=LOOKUP_BATCH_MEDIA_TYPE,
        object_format="OBJECT_FORMAT_JSON",
        max_bytes=CONTROL_MAX_BYTES,
    ).object_ref

    results = _provider_results(
        provider,
        batch=batch,
        max_xml_bytes=max_xml_bytes,
    )
    if complete_feed_proof is not None and any(
        result.status != EidrLookupStatus.FOUND for result in results
    ):
        raise EidrBackfillError(
            "authorized complete-feed batch returned a missing exact ID"
        )

    parsed: list[tuple[EidrExactLookupResult, dict[str, object], ObjectRef]] = []
    raw_by_id: dict[str, ObjectRef] = {}
    with tempfile.TemporaryDirectory(prefix="eidr-exact-lookup-") as directory:
        root = Path(directory)
        for index, result in enumerate(results):
            if result.status == EidrLookupStatus.NOT_FOUND:
                continue
            payload = _parse_exact_xml(
                result=result,
                path=root / f"response-{index:04d}.xml",
            )
            assert result.xml is not None
            digest = sha256_digest(result.xml)
            raw_object = timestamped_store.upload_bytes(
                result.xml,
                join_uri(
                    destination_prefix,
                    "eidr",
                    "exact-lookup-raw",
                    f"id={discovered_eidr_id(result.eidr_id).removeprefix('sha256:')}",
                    f"{digest.removeprefix('sha256:')}.xml",
                ),
                media_type="application/xml",
                object_format="OBJECT_FORMAT_OTHER",
                max_bytes=max_xml_bytes,
            ).object_ref
            parsed.append((result, payload, raw_object))
            raw_by_id[result.eidr_id] = raw_object

    capture: PublishedConnectorCapture | None = None
    if parsed:
        policy = eidr_rights_profile()
        complete = complete_feed_proof is not None
        config_digest = digest_identity(
            {
                "runner": EIDR_EXACT_LOOKUP_CONNECTOR_ID,
                "version": "1.0.0",
                "lookupBatchId": batch.batch_id,
                "authorizationId": authorization.authorization_id,
                "completeFeedProofId": (
                    None
                    if complete_feed_proof is None
                    else complete_feed_proof.proof_id
                ),
            }
        )
        connector_batch = build_connector_batch_manifest(
            source_system_id=EIDR_SOURCE_SYSTEM_ID,
            source_product_id=EIDR_SOURCE_PRODUCT_ID,
            connector_id=EIDR_EXACT_LOOKUP_CONNECTOR_ID,
            connector_version="1.0.0",
            image_digest=image,
            config_digest=config_digest,
            policy_id=policy.policy_id,
            policy_digest=policy.digest,
            transport_kind=TransportKind.API,
            serialization=Serialization.XML,
            change_semantics=(
                ChangeSemantics.FULL_SNAPSHOT if complete else ChangeSemantics.DELTA
            ),
            completeness=(Completeness.COMPLETE if complete else Completeness.PARTIAL),
            delete_coverage=(
                DeleteCoverage.SNAPSHOT_DIFF if complete else DeleteCoverage.NONE
            ),
            coverage_scope={
                "mode": "EXACT_ID_LOOKUP",
                "lookupBatchId": batch.batch_id,
                "manifestId": manifest.manifest_id,
                "sourceBindingId": manifest.source.binding_id,
                "startOrdinal": start_ordinal,
                "endOrdinal": end_ordinal,
                "completeFeedProofId": (
                    None
                    if complete_feed_proof is None
                    else complete_feed_proof.proof_id
                ),
                "coverageId": (
                    None
                    if complete_feed_proof is None
                    else complete_feed_proof.coverage_id
                ),
            },
            watermark_before=_watermark_value(
                manifest.manifest_id,
                start_ordinal,
            ),
            watermark_after=_watermark_value(
                manifest.manifest_id,
                end_ordinal,
            ),
            raw_objects=tuple(item[2] for item in parsed),
            acquired_at=acquired,
            record_count=len(parsed),
            error_count=0,
            retry_count=sum(result.retry_count for result in results),
            rate_limit_count=sum(result.rate_limit_count for result in results),
        )

        def envelopes() -> Iterator[Any]:
            for result, payload, raw_object in parsed:
                modified = payload.get("modified")
                yield build_connector_record_envelope(
                    payload=payload,
                    batch_id=connector_batch.batch_id,
                    source_system_id=EIDR_SOURCE_SYSTEM_ID,
                    source_product_id=EIDR_SOURCE_PRODUCT_ID,
                    source_namespace_id=EIDR_NAMESPACE_ID,
                    source_record_id=result.eidr_id,
                    source_revision=(None if modified is None else str(modified)),
                    operation=RecordOperation.UPSERT,
                    source_modified_at=(None if modified is None else str(modified)),
                    observed_at=acquired,
                    ingested_at=acquired,
                    payload_schema="eidr-record-exact-lookup-v1",
                    raw_object=raw_object,
                    source_location=(
                        f"/exact-lookups/{discovered_eidr_id(result.eidr_id)}"
                    ),
                    policy_id=connector_batch.policy_id,
                    policy_digest=connector_batch.policy_digest,
                )

        capture = publish_connector_capture(
            destination_prefix=destination_prefix,
            batch=connector_batch,
            envelopes=envelopes(),
            store=timestamped_store,
            record_shard_bytes=record_shard_bytes,
        )

    item_receipts = tuple(
        EidrLookupItemReceipt(
            discovered_id=discovered_eidr_id(result.eidr_id),
            eidr_id=result.eidr_id,
            status=result.status,
            attempt_count=result.attempt_count,
            retry_count=result.retry_count,
            rate_limit_count=result.rate_limit_count,
            raw_object=raw_by_id.get(result.eidr_id),
        )
        for result in results
    )
    receipt = build_eidr_lookup_receipt(
        lookup_batch_id=batch.batch_id,
        lookup_batch_object=batch_object,
        manifest_id=manifest.manifest_id,
        source_binding_id=manifest.source.binding_id,
        start_ordinal=start_ordinal,
        next_ordinal=end_ordinal,
        items=item_receipts,
        retry_count=sum(item.retry_count for item in item_receipts),
        rate_limit_count=sum(item.rate_limit_count for item in item_receipts),
        connector_batch_id=(
            None if capture is None else capture.batch_manifest.batch_id
        ),
        connector_batch_object=(
            None if capture is None else capture.batch_manifest_object
        ),
        record_set_id=(
            None if capture is None else capture.record_set_manifest.record_set_id
        ),
        record_set_object=(
            None if capture is None else capture.record_set_manifest_object
        ),
        completed_at=acquired,
    )
    receipt_object = timestamped_store.upload_bytes(
        receipt.json_bytes(),
        join_uri(
            destination_prefix,
            "eidr",
            "exact-lookup-receipts",
            receipt.receipt_id.removeprefix("sha256:"),
            "receipt.json",
        ),
        media_type=LOOKUP_RECEIPT_MEDIA_TYPE,
        object_format="OBJECT_FORMAT_JSON",
        max_bytes=CONTROL_MAX_BYTES,
    ).object_ref
    next_watermark = build_eidr_backfill_watermark(
        manifest_id=manifest.manifest_id,
        source_binding_id=manifest.source.binding_id,
        total_id_count=manifest.id_count,
        next_ordinal=end_ordinal,
        previous_watermark_id=(None if watermark is None else watermark.watermark_id),
        receipt_id=receipt.receipt_id,
        receipt_object=receipt_object,
        updated_at=acquired,
    )
    next_watermark_object = timestamped_store.upload_bytes(
        next_watermark.json_bytes(),
        join_uri(
            destination_prefix,
            "eidr",
            "backfill-watermarks",
            next_watermark.watermark_id.removeprefix("sha256:"),
            "watermark.json",
        ),
        media_type=BACKFILL_WATERMARK_MEDIA_TYPE,
        object_format="OBJECT_FORMAT_JSON",
        max_bytes=CONTROL_MAX_BYTES,
    ).object_ref
    return EidrBackfillBatchResult(
        completed=end_ordinal == manifest.id_count,
        lookup_batch=batch,
        lookup_batch_object=batch_object,
        capture=capture,
        receipt=receipt,
        receipt_object=receipt_object,
        watermark=next_watermark,
        watermark_object=next_watermark_object,
    )


def read_eidr_backfill_run_manifest(
    *,
    reference: ObjectRef,
    store: RuntimeObjectStore,
) -> EidrBackfillRunManifest:
    """Read and content-bind one aggregate backfill run manifest."""

    _validate_json_control_ref(
        reference,
        label="EIDR backfill run manifest",
        media_type=BACKFILL_RUN_MANIFEST_MEDIA_TYPE,
    )
    store.verify(reference, max_bytes=CONTROL_MAX_BYTES)
    with tempfile.TemporaryDirectory(prefix="eidr-backfill-run-") as directory:
        materialized = store.download(
            reference,
            Path(directory) / "manifest.json",
            max_bytes=CONTROL_MAX_BYTES,
        )
        manifest = EidrBackfillRunManifest.model_validate_json(
            materialized.path.read_bytes()
        )
    _require_model_reference(
        manifest,
        reference,
        label="EIDR backfill run manifest",
        media_type=BACKFILL_RUN_MANIFEST_MEDIA_TYPE,
    )
    return manifest


def verify_eidr_backfill_run_manifest_objects(
    *,
    manifest: EidrBackfillRunManifest,
    store: RuntimeObjectStore,
) -> None:
    """Verify every immutable control ObjectRef nested in a run manifest."""

    references: list[ObjectRef] = [manifest.discovered_manifest_object]
    references.extend(
        reference
        for reference in (
            manifest.watermark_before_object,
            manifest.watermark_after_object,
        )
        if reference is not None
    )
    for batch in manifest.batches:
        references.extend(
            (
                batch.lookup_batch_object,
                batch.receipt_object,
                batch.watermark_object,
            )
        )
        references.extend(
            reference
            for reference in (
                batch.connector_batch_object,
                batch.record_set_object,
            )
            if reference is not None
        )
    verified: set[tuple[str, str, int, str | None, str | None]] = set()
    for reference in references:
        identity = (
            reference.uri,
            reference.checksum.value,
            reference.size_bytes,
            reference.etag,
            reference.object_version,
        )
        if identity in verified:
            continue
        store.verify(reference, max_bytes=CONTROL_MAX_BYTES)
        verified.add(identity)


def expand_eidr_source_silver_inputs(
    manifest: EidrBackfillRunManifest,
) -> tuple[tuple[ObjectRef, ObjectRef], ...]:
    """Return existing Connector batch/record-set pairs for Source Silver fan-out."""

    inputs: list[tuple[ObjectRef, ObjectRef]] = []
    for batch in manifest.batches:
        if batch.connector_batch_object is None:
            continue
        assert batch.record_set_object is not None
        inputs.append((batch.connector_batch_object, batch.record_set_object))
    return tuple(inputs)


def run_eidr_exact_lookup_manifest(
    *,
    manifest: DiscoveredEidrIdManifest,
    manifest_object: ObjectRef,
    destination_prefix: str,
    acquired_at: str,
    image_digest: str,
    store: RuntimeObjectStore,
    provider: EidrProvider | None,
    watermark: EidrBackfillWatermark | None = None,
    watermark_object: ObjectRef | None = None,
    batch_size: int = DEFAULT_LOOKUP_BATCH_IDS,
    max_xml_bytes: int = DEFAULT_MAX_XML_BYTES,
    record_shard_bytes: int = DEFAULT_RECORD_SHARD_BYTES,
    max_batches: int = DEFAULT_BACKFILL_MAX_BATCHES,
    max_duration_seconds: float = DEFAULT_BACKFILL_MAX_DURATION_SECONDS,
    max_ids: int = DEFAULT_BACKFILL_MAX_IDS,
    clock=time.monotonic,
) -> EidrBackfillRunResult:
    """Reuse the single-batch primitive until one reviewed run bound is reached."""

    if (
        isinstance(max_batches, bool)
        or not isinstance(max_batches, int)
        or not 0 < max_batches <= MAX_BACKFILL_MAX_BATCHES
    ):
        raise ValueError("max_batches is outside the supported bound")
    duration = float(max_duration_seconds)
    if (
        not math.isfinite(duration)
        or not 0 < duration <= MAX_BACKFILL_MAX_DURATION_SECONDS
    ):
        raise ValueError("max_duration_seconds is outside the supported bound")
    if (
        isinstance(max_ids, bool)
        or not isinstance(max_ids, int)
        or not 0 < max_ids <= MAX_BACKFILL_MAX_IDS
    ):
        raise ValueError("max_ids is outside the supported bound")
    if not 0 < batch_size <= MAX_LOOKUP_BATCH_IDS:
        raise ValueError("batch_size is outside the supported bound")
    if not 0 < max_xml_bytes <= MAX_MAX_XML_BYTES:
        raise ValueError("max_xml_bytes is outside the supported bound")
    if not 0 < record_shard_bytes <= MAX_RECORD_SHARD_BYTES:
        raise ValueError("record_shard_bytes is outside the supported bound")
    acquired = require_rfc3339(acquired_at, label="acquired_at")
    image = require_sha256(image_digest, label="image_digest")
    authorization = _validate_provider(provider, store=store)
    _require_model_reference(
        manifest,
        manifest_object,
        label="discovered-ID manifest",
        media_type=DISCOVERED_ID_MANIFEST_MEDIA_TYPE,
    )
    store.verify(manifest_object, max_bytes=CONTROL_MAX_BYTES)
    if (watermark is None) != (watermark_object is None):
        raise ValueError("watermark and watermark ObjectRef must be supplied together")
    if watermark is not None and watermark_object is not None:
        _require_model_reference(
            watermark,
            watermark_object,
            label="EIDR backfill watermark",
            media_type=BACKFILL_WATERMARK_MEDIA_TYPE,
        )
        store.verify(watermark_object, max_bytes=CONTROL_MAX_BYTES)
        if (
            watermark.manifest_id != manifest.manifest_id
            or watermark.source_binding_id != manifest.source.binding_id
            or watermark.total_id_count != manifest.id_count
        ):
            raise ValueError("watermark does not bind this discovered-ID manifest")

    start_ordinal = 0 if watermark is None else watermark.next_ordinal
    current_watermark = watermark
    current_watermark_object = watermark_object
    entries: list[EidrBackfillRunBatch] = []
    started_at = float(clock())
    if not math.isfinite(started_at):
        raise ValueError("runner clock returned a non-finite value")

    while True:
        current_ordinal = (
            start_ordinal
            if current_watermark is None
            else current_watermark.next_ordinal
        )
        attempted = current_ordinal - start_ordinal
        if current_ordinal == manifest.id_count:
            stop_reason = EidrBackfillStopReason.MANIFEST_EXHAUSTED
            break
        if attempted >= max_ids:
            stop_reason = EidrBackfillStopReason.MAX_IDS
            break
        if len(entries) >= max_batches:
            stop_reason = EidrBackfillStopReason.MAX_BATCHES
            break
        now = float(clock())
        if not math.isfinite(now) or now < started_at:
            raise ValueError("runner clock must be finite and monotonic")
        if now - started_at >= duration:
            stop_reason = EidrBackfillStopReason.MAX_DURATION
            break

        effective_batch_size = min(
            batch_size,
            max_ids - attempted,
            manifest.id_count - current_ordinal,
        )
        result = run_eidr_exact_lookup_batch(
            manifest=manifest,
            manifest_object=manifest_object,
            destination_prefix=destination_prefix,
            acquired_at=acquired,
            image_digest=image,
            store=store,
            provider=provider,
            watermark=current_watermark,
            watermark_object=current_watermark_object,
            batch_size=effective_batch_size,
            max_xml_bytes=max_xml_bytes,
            record_shard_bytes=record_shard_bytes,
            complete_feed_proof=None,
        )
        if (
            result.lookup_batch is None
            or result.lookup_batch_object is None
            or result.receipt is None
            or result.receipt_object is None
            or result.watermark is None
            or result.watermark_object is None
        ):
            raise RuntimeError("non-empty EIDR batch omitted commit artifacts")
        found_count = sum(
            item.status == EidrLookupStatus.FOUND for item in result.receipt.items
        )
        not_found_count = len(result.receipt.items) - found_count
        attempt_count = sum(item.attempt_count for item in result.receipt.items)
        entries.append(
            EidrBackfillRunBatch(
                batch_index=len(entries),
                start_ordinal=result.receipt.start_ordinal,
                next_ordinal=result.receipt.next_ordinal,
                lookup_batch_id=result.lookup_batch.batch_id,
                lookup_batch_object=result.lookup_batch_object,
                receipt_id=result.receipt.receipt_id,
                receipt_object=result.receipt_object,
                watermark_id=result.watermark.watermark_id,
                watermark_object=result.watermark_object,
                connector_batch_id=result.receipt.connector_batch_id,
                connector_batch_object=result.receipt.connector_batch_object,
                record_set_id=result.receipt.record_set_id,
                record_set_object=result.receipt.record_set_object,
                found_count=found_count,
                not_found_count=not_found_count,
                attempt_count=attempt_count,
                retry_count=result.receipt.retry_count,
                rate_limit_count=result.receipt.rate_limit_count,
            )
        )
        current_watermark = result.watermark
        current_watermark_object = result.watermark_object

    next_ordinal = (
        start_ordinal if current_watermark is None else current_watermark.next_ordinal
    )
    run_manifest = build_eidr_backfill_run_manifest(
        discovered_manifest_id=manifest.manifest_id,
        discovered_manifest_object=manifest_object,
        source_binding_id=manifest.source.binding_id,
        authorization_id=authorization.authorization_id,
        start_ordinal=start_ordinal,
        next_ordinal=next_ordinal,
        total_id_count=manifest.id_count,
        watermark_before_id=(None if watermark is None else watermark.watermark_id),
        watermark_before_object=watermark_object,
        watermark_after_id=(
            None if current_watermark is None else current_watermark.watermark_id
        ),
        watermark_after_object=current_watermark_object,
        batches=tuple(entries),
        batch_count=len(entries),
        attempted_id_count=next_ordinal - start_ordinal,
        found_count=sum(item.found_count for item in entries),
        not_found_count=sum(item.not_found_count for item in entries),
        attempt_count=sum(item.attempt_count for item in entries),
        retry_count=sum(item.retry_count for item in entries),
        rate_limit_count=sum(item.rate_limit_count for item in entries),
        completed=next_ordinal == manifest.id_count,
        stop_reason=stop_reason,
        source_completeness="PARTIAL",
        complete_feed_allowed=False,
        created_at=acquired,
    )
    timestamped_store = _TimestampedStore(store, acquired)
    run_manifest_object = timestamped_store.upload_bytes(
        run_manifest.json_bytes(),
        join_uri(
            destination_prefix,
            "eidr",
            "backfill-run-manifests",
            run_manifest.run_manifest_id.removeprefix("sha256:"),
            "manifest.json",
        ),
        media_type=BACKFILL_RUN_MANIFEST_MEDIA_TYPE,
        object_format="OBJECT_FORMAT_JSON",
        max_bytes=CONTROL_MAX_BYTES,
    ).object_ref
    _require_model_reference(
        run_manifest,
        run_manifest_object,
        label="EIDR backfill run manifest",
        media_type=BACKFILL_RUN_MANIFEST_MEDIA_TYPE,
    )
    return EidrBackfillRunResult(
        manifest=run_manifest,
        manifest_object=run_manifest_object,
        watermark=current_watermark,
        watermark_object=current_watermark_object,
    )
