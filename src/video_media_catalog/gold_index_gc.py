"""Fail-closed snapshot, deletion, and restore-drill runtime for Research Gold."""

from __future__ import annotations

import base64
import hashlib
import re
import tempfile
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, Field, field_validator, model_validator

from video_media_catalog.canonical import canonical_json_bytes
from video_media_catalog.gold_search_index import (
    GOLD_INDEX_ACTIVATION_RECEIPT_MEDIA_TYPE,
    RESEARCH_INDEX_PREFIX,
    RESEARCH_READ_ALIAS,
    GoldIndexActivationReceipt,
    plan_gold_index_retention,
)
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.object_store import (
    BoundedObjectStore,
    S3Location,
    conditional_publish_bytes,
)
from video_media_catalog.opensearch_index import (
    current_alias_indices,
    index_document_count,
)
from video_media_catalog.storage import digest_file, local_path
from video_media_catalog.v2_contracts import (
    V2ContractModel,
    require_rfc3339,
    require_sha256,
)

MAX_GC_CONTROL_BYTES = 16 * 1024 * 1024
GOLD_INDEX_ARCHIVE_RECEIPT_MEDIA_TYPE = (
    "application/vnd.video-media-catalog.gold-index-archive.v2+json"
)
GOLD_INDEX_GC_RESULT_MEDIA_TYPE = (
    "application/vnd.video-media-catalog.gold-index-gc-result.v2+json"
)
GOLD_INDEX_RESTORE_RECEIPT_MEDIA_TYPE = (
    "application/vnd.video-media-catalog.gold-index-restore.v2+json"
)

_SAFE_NAME = re.compile(r"^[a-z0-9][a-z0-9_-]{0,254}$")
_SAFE_OPERATION_ID = re.compile(r"^[a-z0-9][a-z0-9-]{0,62}$")
_S3_BUCKET = re.compile(
    r"^(?!\d+\.\d+\.\d+\.\d+$)(?!.*\.\.)[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$"
)
_ROLE_ARN = re.compile(r"^arn:[^:]+:iam::\d{12}:role/[A-Za-z0-9+=,.@_/-]+$")
_VOLATILE_INDEX_SETTINGS = frozenset(
    {
        "creation_date",
        "creation_date_string",
        "history_uuid",
        "provided_name",
        "uuid",
        "verified_before_close",
        "version",
    }
)


def _safe_name(value: str, *, label: str) -> str:
    if _SAFE_NAME.fullmatch(value) is None:
        raise ValueError(f"{label} is not a safe OpenSearch name")
    return value


def _research_index(value: str, *, label: str) -> str:
    value = _safe_name(value, label=label)
    if not value.startswith(RESEARCH_INDEX_PREFIX + "-"):
        raise ValueError(f"{label} is outside the Research index family")
    return value


def _operation_id(value: str) -> str:
    if _SAFE_OPERATION_ID.fullmatch(value) is None:
        raise ValueError("operation_id must be a safe lowercase identifier")
    return value


def _status_code(exc: BaseException) -> int | None:
    status = getattr(exc, "status_code", None)
    if isinstance(status, int):
        return status
    info = getattr(exc, "info", None)
    if isinstance(info, Mapping) and isinstance(info.get("status"), int):
        return int(info["status"])
    response = getattr(exc, "response", None)
    if isinstance(response, Mapping):
        metadata = response.get("ResponseMetadata")
        if isinstance(metadata, Mapping):
            value = metadata.get("HTTPStatusCode")
            if isinstance(value, int):
                return value
    return None


def _validate_control_ref(
    reference: ObjectRef,
    *,
    media_type: str,
    label: str,
) -> None:
    if (
        reference.format != "OBJECT_FORMAT_JSON"
        or reference.media_type != media_type
        or not 0 < reference.size_bytes <= MAX_GC_CONTROL_BYTES
    ):
        raise ValueError(f"{label} must be a bounded JSON object")
    if reference.uri.startswith("s3://") and (
        not reference.etag or not reference.object_version
    ):
        raise ValueError(f"S3 {label} must be version and ETag pinned")


class GoldIndexArchiveReceipt(V2ContractModel):
    """On-site evidence captured before deleting a legacy Research index."""

    schema_version: Literal["2.1"] = "2.1"
    status: Literal["ARCHIVED"] = "ARCHIVED"
    operation_id: str
    activation_receipt: ObjectRef
    repository: str
    snapshot: str
    snapshot_uuid: str = Field(min_length=1, max_length=512)
    old_index: str
    active_index: str
    read_alias: Literal["media-catalog-research-read"] = RESEARCH_READ_ALIAS
    mapping_digest: str
    settings_digest: str
    document_count: int = Field(ge=0)
    snapshot_indices: tuple[str, ...]
    failed_shards: Literal[0] = 0
    archived_at: str

    @field_validator("operation_id")
    @classmethod
    def validate_operation_id(cls, value: str) -> str:
        return _operation_id(value)

    @field_validator("repository", "snapshot")
    @classmethod
    def validate_repository_name(cls, value: str) -> str:
        return _safe_name(value, label="snapshot repository/name")

    @field_validator("old_index", "active_index")
    @classmethod
    def validate_index(cls, value: str) -> str:
        return _research_index(value, label="Research index")

    @field_validator("mapping_digest", "settings_digest")
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("snapshot_indices")
    @classmethod
    def validate_snapshot_indices(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        for index_name in value:
            _research_index(index_name, label="snapshot Research index")
        return value

    @field_validator("archived_at")
    @classmethod
    def validate_archived_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @model_validator(mode="after")
    def validate_contract(self) -> GoldIndexArchiveReceipt:
        _validate_control_ref(
            self.activation_receipt,
            media_type=GOLD_INDEX_ACTIVATION_RECEIPT_MEDIA_TYPE,
            label="activation receipt",
        )
        if self.old_index == self.active_index:
            raise ValueError("old and active indices must differ")
        if (
            self.snapshot_indices != tuple(sorted(set(self.snapshot_indices)))
            or self.old_index not in self.snapshot_indices
            or self.active_index in self.snapshot_indices
        ):
            raise ValueError(
                "archive snapshot indices must be sorted, unique, include old, "
                "and exclude active"
            )
        return self


class GoldIndexGcResultReceipt(V2ContractModel):
    """Immutable evidence that one archived index was safely deleted."""

    schema_version: Literal["2.1"] = "2.1"
    status: Literal["DELETED"] = "DELETED"
    operation_id: str
    activation_receipt: ObjectRef
    archive_receipt: ObjectRef
    repository: str
    snapshot: str
    old_index: str
    active_index: str
    read_alias: Literal["media-catalog-research-read"] = RESEARCH_READ_ALIAS
    mapping_digest: str
    settings_digest: str
    document_count: int = Field(ge=0)
    remaining_research_indices: tuple[str, ...]
    deleted_at: str

    @field_validator("operation_id")
    @classmethod
    def validate_operation_id(cls, value: str) -> str:
        return _operation_id(value)

    @field_validator("repository", "snapshot")
    @classmethod
    def validate_repository_name(cls, value: str) -> str:
        return _safe_name(value, label="snapshot repository/name")

    @field_validator("old_index", "active_index")
    @classmethod
    def validate_index(cls, value: str) -> str:
        return _research_index(value, label="Research index")

    @field_validator("mapping_digest", "settings_digest")
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("deleted_at")
    @classmethod
    def validate_deleted_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @model_validator(mode="after")
    def validate_contract(self) -> GoldIndexGcResultReceipt:
        _validate_control_ref(
            self.activation_receipt,
            media_type=GOLD_INDEX_ACTIVATION_RECEIPT_MEDIA_TYPE,
            label="activation receipt",
        )
        _validate_control_ref(
            self.archive_receipt,
            media_type=GOLD_INDEX_ARCHIVE_RECEIPT_MEDIA_TYPE,
            label="archive receipt",
        )
        if self.old_index == self.active_index:
            raise ValueError("old and active indices must differ")
        if self.remaining_research_indices != (self.active_index,):
            raise ValueError("GC result must leave only the active Research index")
        return self


class GoldIndexRestoreReceipt(V2ContractModel):
    """Immutable evidence for an original-name, alias-neutral restore drill."""

    schema_version: Literal["2.1"] = "2.1"
    status: Literal["RESTORED"] = "RESTORED"
    operation_id: str
    archive_receipt: ObjectRef
    repository: str
    snapshot: str
    restored_index: str
    active_index: str
    read_alias: Literal["media-catalog-research-read"] = RESEARCH_READ_ALIAS
    mapping_digest: str
    settings_digest: str
    document_count: int = Field(ge=0)
    alias_indices_before: tuple[str, ...]
    alias_indices_after: tuple[str, ...]
    restored_at: str

    @field_validator("operation_id")
    @classmethod
    def validate_operation_id(cls, value: str) -> str:
        return _operation_id(value)

    @field_validator("repository", "snapshot")
    @classmethod
    def validate_repository_name(cls, value: str) -> str:
        return _safe_name(value, label="snapshot repository/name")

    @field_validator("restored_index", "active_index")
    @classmethod
    def validate_index(cls, value: str) -> str:
        return _research_index(value, label="Research index")

    @field_validator("mapping_digest", "settings_digest")
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("restored_at")
    @classmethod
    def validate_restored_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @model_validator(mode="after")
    def validate_contract(self) -> GoldIndexRestoreReceipt:
        _validate_control_ref(
            self.archive_receipt,
            media_type=GOLD_INDEX_ARCHIVE_RECEIPT_MEDIA_TYPE,
            label="archive receipt",
        )
        expected = (self.active_index,)
        if (
            self.restored_index == self.active_index
            or self.alias_indices_before != expected
            or self.alias_indices_after != expected
        ):
            raise ValueError("restore drill must preserve the active-only read alias")
        return self


@dataclass(frozen=True)
class IndexEvidence:
    mapping_digest: str
    settings_digest: str
    document_count: int


def _canonical_digest(value: Any) -> str:
    return "sha256:" + hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _extract_index_value(
    response: Any,
    *,
    index_name: str,
    field: str,
) -> Mapping[str, Any]:
    if not isinstance(response, Mapping):
        raise RuntimeError(f"OpenSearch {field} response is invalid")
    value = response.get(index_name)
    if value is None and len(response) == 1:
        value = next(iter(response.values()))
    if not isinstance(value, Mapping):
        raise RuntimeError(f"OpenSearch {field} response omitted {index_name}")
    nested = value.get(field)
    if not isinstance(nested, Mapping):
        raise RuntimeError(f"OpenSearch {field} response is invalid")
    return nested


def _stable_settings(settings: Mapping[str, Any]) -> dict[str, Any]:
    """Remove settings OpenSearch regenerates when restoring an index."""

    normalized = {
        str(key): value
        for key, value in settings.items()
        if str(key) not in _VOLATILE_INDEX_SETTINGS
    }
    index_settings = normalized.get("index")
    if isinstance(index_settings, Mapping):
        normalized["index"] = {
            str(key): value
            for key, value in index_settings.items()
            if str(key) not in _VOLATILE_INDEX_SETTINGS
        }
    for key in tuple(normalized):
        if (
            key.startswith("index.")
            and key.removeprefix("index.").split(".", 1)[0] in _VOLATILE_INDEX_SETTINGS
        ):
            normalized.pop(key)
    return normalized


def collect_index_evidence(client: Any, *, index_name: str) -> IndexEvidence:
    """Capture restorable mapping/settings digests and a refreshed count."""

    _research_index(index_name, label="Research index")
    mappings = _extract_index_value(
        client.indices.get_mapping(index=index_name),
        index_name=index_name,
        field="mappings",
    )
    settings = _extract_index_value(
        client.indices.get_settings(index=index_name, flat_settings=False),
        index_name=index_name,
        field="settings",
    )
    return IndexEvidence(
        mapping_digest=_canonical_digest(mappings),
        settings_digest=_canonical_digest(_stable_settings(settings)),
        document_count=index_document_count(client, index_name=index_name),
    )


def _index_exists(client: Any, index_name: str) -> bool:
    value = client.indices.exists(index=index_name)
    if not isinstance(value, bool):
        raise RuntimeError("OpenSearch index existence response is invalid")
    return value


def research_indices(client: Any) -> tuple[str, ...]:
    pattern = RESEARCH_INDEX_PREFIX + "-*"
    try:
        response = client.indices.get(
            index=pattern,
            expand_wildcards="all",
            allow_no_indices=True,
            ignore_unavailable=True,
        )
    except TypeError:
        response = client.indices.get(index=pattern)
    except Exception as exc:
        if _status_code(exc) == 404:
            return ()
        raise
    if not isinstance(response, Mapping):
        raise RuntimeError("OpenSearch index listing response is invalid")
    result = tuple(
        sorted(
            str(index_name)
            for index_name in response
            if str(index_name).startswith(RESEARCH_INDEX_PREFIX + "-")
        )
    )
    for index_name in result:
        _research_index(index_name, label="Research index")
    return result


def _creation_epoch_millis(client: Any, index_name: str) -> int:
    settings = _extract_index_value(
        client.indices.get_settings(index=index_name, flat_settings=False),
        index_name=index_name,
        field="settings",
    )
    index_settings = settings.get("index")
    value = (
        index_settings.get("creation_date")
        if isinstance(index_settings, Mapping)
        else settings.get("index.creation_date")
    )
    try:
        created_at = int(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            f"OpenSearch index {index_name} has no valid creation date"
        ) from exc
    if created_at < 0:
        raise RuntimeError(f"OpenSearch index {index_name} has invalid creation date")
    return created_at


def _assert_alias_is_active_only(
    client: Any,
    *,
    old_index: str,
    active_index: str,
) -> tuple[str, ...]:
    alias_indices = tuple(current_alias_indices(client, alias=RESEARCH_READ_ALIAS))
    if old_index in alias_indices:
        raise RuntimeError("old Research index is still attached to the read alias")
    if alias_indices != (active_index,):
        raise RuntimeError(
            "Research read alias compare-and-swap failed: "
            f"expected {(active_index,)!r}, found {alias_indices!r}"
        )
    return alias_indices


def _assert_pre_delete_state(
    client: Any,
    *,
    old_index: str,
    active_index: str,
    archive_indices: tuple[str, ...],
) -> None:
    missing = tuple(
        index_name
        for index_name in archive_indices
        if not _index_exists(client, index_name)
    )
    if missing:
        raise RuntimeError(f"archive target Research indices do not exist: {missing!r}")
    if not _index_exists(client, active_index):
        raise RuntimeError("active Research index does not exist")
    _assert_alias_is_active_only(
        client,
        old_index=old_index,
        active_index=active_index,
    )
    indices = research_indices(client)
    expected = tuple(sorted((active_index, *archive_indices)))
    if indices != expected:
        raise RuntimeError(
            "archive-delete requires exactly the active and declared archive "
            f"indices; expected {expected!r}, found {indices!r}"
        )


def _assert_post_delete_state(
    client: Any,
    *,
    old_index: str,
    active_index: str,
    archive_indices: tuple[str, ...],
) -> None:
    _assert_alias_is_active_only(
        client,
        old_index=old_index,
        active_index=active_index,
    )
    remaining_targets = tuple(
        index_name
        for index_name in archive_indices
        if _index_exists(client, index_name)
    )
    if remaining_targets:
        raise RuntimeError(
            "archived Research indices still exist after deletion: "
            f"{remaining_targets!r}"
        )
    if not _index_exists(client, active_index):
        raise RuntimeError("active Research index disappeared during GC")
    indices = research_indices(client)
    if indices != (active_index,):
        raise RuntimeError(
            "Research index postcondition failed: "
            f"expected {(active_index,)!r}, found {indices!r}"
        )


def _assert_evidence_matches(
    expected: GoldIndexArchiveReceipt,
    actual: IndexEvidence,
) -> None:
    if (
        expected.mapping_digest != actual.mapping_digest
        or expected.settings_digest != actual.settings_digest
        or expected.document_count != actual.document_count
    ):
        raise RuntimeError("Research index differs from its immutable archive receipt")


def _snapshot_record(
    client: Any,
    *,
    repository: str,
    snapshot: str,
) -> Mapping[str, Any] | None:
    try:
        response = client.snapshot.get(
            repository=repository,
            snapshot=snapshot,
        )
    except Exception as exc:
        if _status_code(exc) == 404:
            return None
        raise
    if not isinstance(response, Mapping):
        raise RuntimeError("OpenSearch snapshot response is invalid")
    snapshots = response.get("snapshots")
    if not isinstance(snapshots, list):
        raise RuntimeError("OpenSearch snapshot response is invalid")
    if not snapshots:
        return None
    if len(snapshots) != 1 or not isinstance(snapshots[0], Mapping):
        raise RuntimeError("OpenSearch returned an ambiguous snapshot response")
    return snapshots[0]


def _snapshot_failed_shards(record: Mapping[str, Any]) -> int:
    shards = record.get("shards")
    if not isinstance(shards, Mapping):
        raise RuntimeError("OpenSearch snapshot omitted shard results")
    failed = shards.get("failed")
    if isinstance(failed, bool) or not isinstance(failed, int) or failed < 0:
        raise RuntimeError("OpenSearch snapshot failed-shard count is invalid")
    failures = record.get("failures", [])
    if not isinstance(failures, list):
        raise RuntimeError("OpenSearch snapshot failures are invalid")
    return max(failed, len(failures))


def assert_snapshot_success(
    client: Any,
    *,
    repository: str,
    snapshot: str,
    snapshot_indices: tuple[str, ...],
    expected_uuid: str | None = None,
) -> Mapping[str, Any]:
    record = _snapshot_record(
        client,
        repository=repository,
        snapshot=snapshot,
    )
    if record is None:
        raise RuntimeError("OpenSearch snapshot does not exist")
    indices = record.get("indices")
    if (
        not isinstance(indices, list)
        or tuple(sorted(map(str, indices))) != snapshot_indices
    ):
        raise RuntimeError(
            "OpenSearch snapshot does not contain exactly the declared archive indices"
        )
    if record.get("include_global_state") is not False:
        raise RuntimeError("OpenSearch snapshot includes global cluster state")
    if expected_uuid is not None and record.get("uuid") != expected_uuid:
        raise RuntimeError("OpenSearch snapshot UUID differs from archive receipt")
    if str(record.get("state", "")) != "SUCCESS":
        raise RuntimeError("OpenSearch snapshot is not successful")
    if _snapshot_failed_shards(record) != 0:
        raise RuntimeError("OpenSearch snapshot contains failed shards")
    return record


def _ensure_snapshot(
    client: Any,
    *,
    repository: str,
    snapshot: str,
    snapshot_indices: tuple[str, ...],
    metadata: Mapping[str, Any],
    timeout_seconds: float,
    poll_seconds: float,
    monotonic: Callable[[], float],
    sleep: Callable[[float], None],
) -> Mapping[str, Any]:
    record = _snapshot_record(
        client,
        repository=repository,
        snapshot=snapshot,
    )
    if record is None:
        client.snapshot.create(
            repository=repository,
            snapshot=snapshot,
            body={
                "indices": list(snapshot_indices),
                "include_global_state": False,
                "partial": False,
                "metadata": dict(metadata),
            },
            wait_for_completion=False,
        )
    deadline = monotonic() + timeout_seconds
    while True:
        record = _snapshot_record(
            client,
            repository=repository,
            snapshot=snapshot,
        )
        if record is None:
            raise RuntimeError("OpenSearch lost the requested manual snapshot")
        indices = record.get("indices")
        if (
            not isinstance(indices, list)
            or tuple(sorted(map(str, indices))) != snapshot_indices
        ):
            raise RuntimeError("existing OpenSearch snapshot has a different index set")
        if record.get("include_global_state") is not False:
            raise RuntimeError("OpenSearch snapshot includes global cluster state")
        if record.get("metadata") != metadata:
            raise RuntimeError(
                "existing OpenSearch snapshot has conflicting operation metadata"
            )
        state = str(record.get("state", ""))
        if state == "SUCCESS":
            if _snapshot_failed_shards(record) != 0:
                raise RuntimeError("OpenSearch snapshot contains failed shards")
            return record
        if state in {"FAILED", "PARTIAL", "INCOMPATIBLE"}:
            raise RuntimeError(f"OpenSearch snapshot ended in state {state}")
        if monotonic() >= deadline:
            raise TimeoutError("OpenSearch snapshot did not finish before timeout")
        sleep(poll_seconds)


def _native_sha256(value: str) -> str:
    return base64.b64decode(value).hex()


def discover_control_ref(
    store: BoundedObjectStore,
    *,
    uri: str,
    media_type: str,
    required: bool = True,
) -> ObjectRef | None:
    """Bind an existing control object to checksum, ETag, and S3 version."""

    if urlsplit(uri).scheme == "file":
        path = local_path(uri)
        if not path.exists():
            if required:
                raise FileNotFoundError(path)
            return None
        digest, size = digest_file(path)
        return ObjectRef(
            uri=uri,
            format="OBJECT_FORMAT_JSON",
            media_type=media_type,
            checksum=Checksum(value=digest),
            size_bytes=size,
        )
    location = S3Location.parse(uri)
    try:
        head = store.client.head_object(
            Bucket=location.bucket,
            Key=location.key,
            ChecksumMode="ENABLED",
        )
    except Exception as exc:
        if not required and _status_code(exc) == 404:
            return None
        raise
    metadata = head.get("Metadata") or {}
    checksum = metadata.get("sha256")
    if checksum is None and head.get("ChecksumSHA256"):
        checksum = _native_sha256(str(head["ChecksumSHA256"]))
    etag = str(head.get("ETag") or "").strip('"') or None
    version = head.get("VersionId")
    if not checksum or not etag or not version:
        raise RuntimeError(
            "S3 control object must expose checksum, ETag, and object version"
        )
    reference = ObjectRef(
        uri=uri,
        format="OBJECT_FORMAT_JSON",
        media_type=media_type,
        checksum=Checksum(value=str(checksum)),
        size_bytes=int(head.get("ContentLength", -1)),
        etag=etag,
        object_version=str(version),
    )
    return store.head(
        reference,
        max_bytes=MAX_GC_CONTROL_BYTES,
        require_immutable_metadata=True,
    )


def read_control_model[ReceiptT: BaseModel](
    store: BoundedObjectStore,
    reference: ObjectRef,
    *,
    media_type: str,
    model: type[ReceiptT],
) -> ReceiptT:
    _validate_control_ref(
        reference,
        media_type=media_type,
        label="control object",
    )
    store.head(
        reference,
        max_bytes=MAX_GC_CONTROL_BYTES,
        require_immutable_metadata=reference.uri.startswith("s3://"),
    )
    with tempfile.TemporaryDirectory(prefix="research-index-gc-") as directory:
        materialized = store.download(
            reference,
            Path(directory) / "control.json",
            max_bytes=MAX_GC_CONTROL_BYTES,
        )
        return model.model_validate_json(materialized.path.read_bytes())


def _publish_model(
    store: BoundedObjectStore,
    *,
    model: V2ContractModel,
    destination_uri: str,
    media_type: str,
) -> ObjectRef:
    return conditional_publish_bytes(
        store,
        model.json_bytes(),
        destination_uri,
        media_type=media_type,
        object_format="OBJECT_FORMAT_JSON",
        max_bytes=MAX_GC_CONTROL_BYTES,
    ).object_ref


def _validate_archive_identity(
    receipt: GoldIndexArchiveReceipt,
    *,
    activation_receipt: ObjectRef,
    repository: str,
    snapshot: str,
    old_index: str,
    active_index: str,
) -> None:
    if (
        receipt.activation_receipt != activation_receipt
        or receipt.repository != repository
        or receipt.snapshot != snapshot
        or receipt.old_index != old_index
        or receipt.active_index != active_index
        or receipt.read_alias != RESEARCH_READ_ALIAS
    ):
        raise RuntimeError("archive receipt does not match this GC operation")


def _validate_gc_identity(
    receipt: GoldIndexGcResultReceipt,
    *,
    operation_id: str,
    activation_receipt: ObjectRef,
    repository: str,
    snapshot: str,
    old_index: str,
    active_index: str,
) -> None:
    if (
        receipt.operation_id != operation_id
        or receipt.activation_receipt != activation_receipt
        or receipt.repository != repository
        or receipt.snapshot != snapshot
        or receipt.old_index != old_index
        or receipt.active_index != active_index
    ):
        raise RuntimeError("existing GC receipt conflicts with this operation")


def _retention_candidates(
    client: Any,
    *,
    alias_indices: tuple[str, ...],
    referenced_indices: tuple[str, ...],
    delete_before_epoch_millis: int,
) -> tuple[str, ...]:
    indices = research_indices(client)
    creation_times = {
        index_name: _creation_epoch_millis(client, index_name) for index_name in indices
    }
    return plan_gold_index_retention(
        creation_epoch_millis=creation_times,
        active_indices=alias_indices,
        referenced_indices=referenced_indices,
        delete_before_epoch_millis=delete_before_epoch_millis,
    )


def _receipt_dump(reference: ObjectRef) -> dict[str, Any]:
    return reference.model_dump(mode="json", by_alias=True, exclude_none=True)


def _validate_activation_receipt(
    store: BoundedObjectStore,
    reference: ObjectRef,
    *,
    active_index: str,
    old_index: str,
) -> GoldIndexActivationReceipt:
    activation = read_control_model(
        store,
        reference,
        media_type=GOLD_INDEX_ACTIVATION_RECEIPT_MEDIA_TYPE,
        model=GoldIndexActivationReceipt,
    )
    if (
        activation.status != "ACTIVATED"
        or activation.alias != RESEARCH_READ_ALIAS
        or activation.index != active_index
        or activation.expected_previous_indices != (old_index,)
        or not activation.changed
    ):
        raise RuntimeError(
            "activation receipt does not authorize this active/old index pair"
        )
    return activation


def archive_delete(
    client: Any,
    store: BoundedObjectStore,
    *,
    operation_id: str,
    repository: str,
    snapshot: str,
    old_index: str,
    active_index: str,
    activation_receipt: ObjectRef,
    archive_receipt_uri: str,
    gc_result_uri: str,
    completed_at: str,
    delete_before_epoch_millis: int,
    referenced_indices: tuple[str, ...] = (),
    additional_archive_indices: tuple[str, ...] = (),
    execute: bool = False,
    reuse_archive_receipt: ObjectRef | None = None,
    snapshot_timeout_seconds: float = 7200,
    snapshot_poll_seconds: float = 15,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """Archive and optionally delete an explicit inactive Research index set."""

    operation_id = _operation_id(operation_id)
    repository = _safe_name(repository, label="snapshot repository")
    snapshot = _safe_name(snapshot, label="snapshot name")
    old_index = _research_index(old_index, label="old Research index")
    active_index = _research_index(active_index, label="active Research index")
    completed_at = require_rfc3339(completed_at)
    if old_index == active_index:
        raise ValueError("old and active indices must differ")
    if snapshot_timeout_seconds <= 0 or snapshot_poll_seconds <= 0:
        raise ValueError("snapshot timeout and poll interval must be positive")
    referenced = tuple(sorted(set(referenced_indices)))
    if len(referenced) != len(referenced_indices):
        raise ValueError("referenced Research indices must be unique")
    for index_name in referenced:
        _research_index(index_name, label="referenced Research index")
    additional = tuple(sorted(set(additional_archive_indices)))
    if len(additional) != len(additional_archive_indices):
        raise ValueError("additional archive indices must be unique")
    for index_name in additional:
        _research_index(index_name, label="additional archive Research index")
    if {old_index, active_index} & set(additional) or set(referenced) & set(additional):
        raise ValueError(
            "additional archive indices must exclude active, old, "
            "and referenced indices"
        )
    archive_indices = tuple(sorted((old_index, *additional)))
    _validate_activation_receipt(
        store,
        activation_receipt,
        active_index=active_index,
        old_index=old_index,
    )

    existing_gc_ref = discover_control_ref(
        store,
        uri=gc_result_uri,
        media_type=GOLD_INDEX_GC_RESULT_MEDIA_TYPE,
        required=False,
    )
    if existing_gc_ref is not None:
        existing_gc = read_control_model(
            store,
            existing_gc_ref,
            media_type=GOLD_INDEX_GC_RESULT_MEDIA_TYPE,
            model=GoldIndexGcResultReceipt,
        )
        _validate_gc_identity(
            existing_gc,
            operation_id=operation_id,
            activation_receipt=activation_receipt,
            repository=repository,
            snapshot=snapshot,
            old_index=old_index,
            active_index=active_index,
        )
        if (
            reuse_archive_receipt is not None
            and reuse_archive_receipt != existing_gc.archive_receipt
        ):
            raise RuntimeError(
                "supplied archive receipt conflicts with existing GC result"
            )
        validate_gc_result(client, store, existing_gc)
        return {
            "status": "DELETED",
            "reused": True,
            "gcResult": _receipt_dump(existing_gc_ref),
            "archiveReceipt": _receipt_dump(existing_gc.archive_receipt),
            "activationReceipt": _receipt_dump(activation_receipt),
        }

    discovered_archive_ref = discover_control_ref(
        store,
        uri=archive_receipt_uri,
        media_type=GOLD_INDEX_ARCHIVE_RECEIPT_MEDIA_TYPE,
        required=False,
    )
    if (
        reuse_archive_receipt is not None
        and discovered_archive_ref is not None
        and (
            reuse_archive_receipt.uri != discovered_archive_ref.uri
            or reuse_archive_receipt.checksum != discovered_archive_ref.checksum
            or reuse_archive_receipt.size_bytes != discovered_archive_ref.size_bytes
            or reuse_archive_receipt.etag != discovered_archive_ref.etag
            or reuse_archive_receipt.object_version
            != discovered_archive_ref.object_version
        )
    ):
        raise RuntimeError(
            "archive receipt destination conflicts with supplied receipt"
        )
    archive_ref = reuse_archive_receipt or discovered_archive_ref
    archive: GoldIndexArchiveReceipt | None = None
    if archive_ref is not None:
        archive = read_control_model(
            store,
            archive_ref,
            media_type=GOLD_INDEX_ARCHIVE_RECEIPT_MEDIA_TYPE,
            model=GoldIndexArchiveReceipt,
        )
        _validate_archive_identity(
            archive,
            activation_receipt=activation_receipt,
            repository=repository,
            snapshot=snapshot,
            old_index=old_index,
            active_index=active_index,
        )
        if not set(archive_indices).issubset(archive.snapshot_indices):
            raise RuntimeError(
                "archive receipt does not cover every requested archive index"
            )
        assert_snapshot_success(
            client,
            repository=repository,
            snapshot=snapshot,
            snapshot_indices=archive.snapshot_indices,
            expected_uuid=archive.snapshot_uuid,
        )

    existing_archive_indices = tuple(
        index_name
        for index_name in archive_indices
        if _index_exists(client, index_name)
    )
    if not existing_archive_indices:
        if not execute or archive is None or archive_ref is None:
            raise RuntimeError("archive target Research indices do not exist")
        _assert_post_delete_state(
            client,
            old_index=old_index,
            active_index=active_index,
            archive_indices=archive_indices,
        )
        gc_receipt = GoldIndexGcResultReceipt(
            operation_id=operation_id,
            activation_receipt=activation_receipt,
            archive_receipt=archive_ref,
            repository=repository,
            snapshot=snapshot,
            old_index=old_index,
            active_index=active_index,
            mapping_digest=archive.mapping_digest,
            settings_digest=archive.settings_digest,
            document_count=archive.document_count,
            remaining_research_indices=(active_index,),
            deleted_at=completed_at,
        )
        gc_ref = _publish_model(
            store,
            model=gc_receipt,
            destination_uri=gc_result_uri,
            media_type=GOLD_INDEX_GC_RESULT_MEDIA_TYPE,
        )
        return {
            "status": "DELETED",
            "reused": True,
            "gcResult": _receipt_dump(gc_ref),
            "archiveReceipt": _receipt_dump(archive_ref),
            "activationReceipt": _receipt_dump(activation_receipt),
        }

    if archive is None and existing_archive_indices != archive_indices:
        raise RuntimeError("one or more archive target Research indices do not exist")
    _assert_pre_delete_state(
        client,
        old_index=old_index,
        active_index=active_index,
        archive_indices=existing_archive_indices,
    )
    if old_index in existing_archive_indices:
        evidence = collect_index_evidence(client, index_name=old_index)
    elif archive is not None:
        evidence = IndexEvidence(
            mapping_digest=archive.mapping_digest,
            settings_digest=archive.settings_digest,
            document_count=archive.document_count,
        )
    else:
        raise RuntimeError("old Research index disappeared before it was archived")
    alias_indices = (active_index,)
    candidates: tuple[str, ...] = ()
    if archive is None:
        candidates = _retention_candidates(
            client,
            alias_indices=alias_indices,
            referenced_indices=referenced,
            delete_before_epoch_millis=delete_before_epoch_millis,
        )
        ineligible = tuple(
            index_name for index_name in archive_indices if index_name not in candidates
        )
        if ineligible:
            raise RuntimeError(
                f"Research indices are not eligible for retention GC: {ineligible!r}"
            )
    elif old_index in existing_archive_indices:
        _assert_evidence_matches(archive, evidence)

    if not execute:
        return {
            "status": "DRY_RUN",
            "execute": False,
            "oldIndex": old_index,
            "activeIndex": active_index,
            "archiveIndices": list(archive_indices),
            "aliasIndices": list(alias_indices),
            "mappingDigest": evidence.mapping_digest,
            "settingsDigest": evidence.settings_digest,
            "documentCount": evidence.document_count,
            "retentionCandidates": list(candidates),
            "snapshotReusable": archive is not None,
            "activationReceipt": _receipt_dump(activation_receipt),
        }

    if archive is None:
        snapshot_metadata = {
            "videoMediaCatalogOperationId": operation_id,
            "archiveIndices": ",".join(archive_indices),
            "mappingDigest": evidence.mapping_digest,
            "settingsDigest": evidence.settings_digest,
            "documentCount": str(evidence.document_count),
            "activationReceiptDigest": _canonical_digest(
                activation_receipt.model_dump(
                    mode="json",
                    by_alias=True,
                    exclude_none=True,
                )
            ),
        }
        snapshot_record = _ensure_snapshot(
            client,
            repository=repository,
            snapshot=snapshot,
            snapshot_indices=archive_indices,
            metadata=snapshot_metadata,
            timeout_seconds=snapshot_timeout_seconds,
            poll_seconds=snapshot_poll_seconds,
            monotonic=monotonic,
            sleep=sleep,
        )
        snapshot_uuid = snapshot_record.get("uuid")
        if not isinstance(snapshot_uuid, str) or not snapshot_uuid:
            raise RuntimeError("OpenSearch snapshot omitted its immutable UUID")
        archive = GoldIndexArchiveReceipt(
            operation_id=operation_id,
            activation_receipt=activation_receipt,
            repository=repository,
            snapshot=snapshot,
            snapshot_uuid=snapshot_uuid,
            old_index=old_index,
            active_index=active_index,
            mapping_digest=evidence.mapping_digest,
            settings_digest=evidence.settings_digest,
            document_count=evidence.document_count,
            snapshot_indices=archive_indices,
            failed_shards=0,
            archived_at=completed_at,
        )
        archive_ref = _publish_model(
            store,
            model=archive,
            destination_uri=archive_receipt_uri,
            media_type=GOLD_INDEX_ARCHIVE_RECEIPT_MEDIA_TYPE,
        )
    if archive_ref is None:
        raise RuntimeError(
            "archive receipt publication returned no immutable reference"
        )

    # Final fail-closed CAS and evidence check immediately precede deletion.
    current_archive_indices = tuple(
        index_name
        for index_name in archive_indices
        if _index_exists(client, index_name)
    )
    if current_archive_indices != existing_archive_indices:
        raise RuntimeError("Research archive index existence changed after snapshot")
    _assert_pre_delete_state(
        client,
        old_index=old_index,
        active_index=active_index,
        archive_indices=current_archive_indices,
    )
    if old_index in current_archive_indices:
        _assert_evidence_matches(
            archive,
            collect_index_evidence(client, index_name=old_index),
        )
    for index_name in current_archive_indices:
        client.indices.delete(index=index_name)
    _assert_post_delete_state(
        client,
        old_index=old_index,
        active_index=active_index,
        archive_indices=archive_indices,
    )

    gc_receipt = GoldIndexGcResultReceipt(
        operation_id=operation_id,
        activation_receipt=activation_receipt,
        archive_receipt=archive_ref,
        repository=repository,
        snapshot=snapshot,
        old_index=old_index,
        active_index=active_index,
        mapping_digest=archive.mapping_digest,
        settings_digest=archive.settings_digest,
        document_count=archive.document_count,
        remaining_research_indices=(active_index,),
        deleted_at=completed_at,
    )
    gc_ref = _publish_model(
        store,
        model=gc_receipt,
        destination_uri=gc_result_uri,
        media_type=GOLD_INDEX_GC_RESULT_MEDIA_TYPE,
    )
    return {
        "status": "DELETED",
        "reused": False,
        "gcResult": _receipt_dump(gc_ref),
        "archiveReceipt": _receipt_dump(archive_ref),
        "activationReceipt": _receipt_dump(activation_receipt),
    }


def validate_gc_result(
    client: Any,
    store: BoundedObjectStore,
    receipt: GoldIndexGcResultReceipt,
) -> None:
    """Independently recheck a GC result and its pinned successful snapshot."""

    archive = read_control_model(
        store,
        receipt.archive_receipt,
        media_type=GOLD_INDEX_ARCHIVE_RECEIPT_MEDIA_TYPE,
        model=GoldIndexArchiveReceipt,
    )
    _validate_archive_identity(
        archive,
        activation_receipt=receipt.activation_receipt,
        repository=receipt.repository,
        snapshot=receipt.snapshot,
        old_index=receipt.old_index,
        active_index=receipt.active_index,
    )
    if (
        archive.activation_receipt != receipt.activation_receipt
        or archive.mapping_digest != receipt.mapping_digest
        or archive.settings_digest != receipt.settings_digest
        or archive.document_count != receipt.document_count
    ):
        raise RuntimeError("GC result does not match its archive receipt")
    _validate_activation_receipt(
        store,
        receipt.activation_receipt,
        active_index=receipt.active_index,
        old_index=receipt.old_index,
    )
    assert_snapshot_success(
        client,
        repository=receipt.repository,
        snapshot=receipt.snapshot,
        snapshot_indices=archive.snapshot_indices,
        expected_uuid=archive.snapshot_uuid,
    )
    _assert_post_delete_state(
        client,
        old_index=receipt.old_index,
        active_index=receipt.active_index,
        archive_indices=archive.snapshot_indices,
    )


def _wait_for_restored_index(
    client: Any,
    *,
    index_name: str,
    timeout_seconds: float,
    poll_seconds: float,
    monotonic: Callable[[], float],
    sleep: Callable[[float], None],
) -> None:
    deadline = monotonic() + timeout_seconds
    while True:
        if _index_exists(client, index_name):
            health = client.cluster.health(index=index_name)
            if not isinstance(health, Mapping):
                raise RuntimeError("restored Research index health response is invalid")
            if not bool(health.get("timed_out")) and str(health.get("status", "")) in {
                "yellow",
                "green",
            }:
                return
        if monotonic() >= deadline:
            raise TimeoutError(
                "restored Research index did not reach yellow health before timeout"
            )
        sleep(poll_seconds)


def _validate_restore_identity(
    receipt: GoldIndexRestoreReceipt,
    *,
    operation_id: str,
    archive: GoldIndexArchiveReceipt,
) -> None:
    if (
        receipt.operation_id != operation_id
        or receipt.repository != archive.repository
        or receipt.snapshot != archive.snapshot
        or receipt.restored_index != archive.old_index
        or receipt.active_index != archive.active_index
        or receipt.mapping_digest != archive.mapping_digest
        or receipt.settings_digest != archive.settings_digest
        or receipt.document_count != archive.document_count
    ):
        raise RuntimeError("existing restore receipt conflicts with this operation")


def restore_drill(
    client: Any,
    store: BoundedObjectStore,
    *,
    operation_id: str,
    archive_receipt: ObjectRef,
    restore_receipt_uri: str,
    completed_at: str,
    restore_timeout_seconds: float = 7200,
    restore_poll_seconds: float = 15,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """Restore a snapshot under its original index name without touching aliases."""

    operation_id = _operation_id(operation_id)
    completed_at = require_rfc3339(completed_at)
    if restore_timeout_seconds <= 0 or restore_poll_seconds <= 0:
        raise ValueError("restore timeout and poll interval must be positive")
    archive = read_control_model(
        store,
        archive_receipt,
        media_type=GOLD_INDEX_ARCHIVE_RECEIPT_MEDIA_TYPE,
        model=GoldIndexArchiveReceipt,
    )
    _validate_activation_receipt(
        store,
        archive.activation_receipt,
        active_index=archive.active_index,
        old_index=archive.old_index,
    )
    assert_snapshot_success(
        client,
        repository=archive.repository,
        snapshot=archive.snapshot,
        snapshot_indices=archive.snapshot_indices,
        expected_uuid=archive.snapshot_uuid,
    )
    before = _assert_alias_is_active_only(
        client,
        old_index=archive.old_index,
        active_index=archive.active_index,
    )
    if not _index_exists(client, archive.active_index):
        raise RuntimeError("active Research index does not exist")
    indices_before = research_indices(client)
    allowed_before = {
        (archive.active_index,),
        tuple(sorted((archive.active_index, archive.old_index))),
    }
    if indices_before not in allowed_before:
        raise RuntimeError("restore drill found unexpected Research indices")

    existing_restore_ref = discover_control_ref(
        store,
        uri=restore_receipt_uri,
        media_type=GOLD_INDEX_RESTORE_RECEIPT_MEDIA_TYPE,
        required=False,
    )
    existing_restore: GoldIndexRestoreReceipt | None = None
    if existing_restore_ref is not None:
        existing_restore = read_control_model(
            store,
            existing_restore_ref,
            media_type=GOLD_INDEX_RESTORE_RECEIPT_MEDIA_TYPE,
            model=GoldIndexRestoreReceipt,
        )
        _validate_restore_identity(
            existing_restore,
            operation_id=operation_id,
            archive=archive,
        )
        if existing_restore.archive_receipt != archive_receipt:
            raise RuntimeError(
                "existing restore receipt binds a different archive object"
            )

    restored_already = _index_exists(client, archive.old_index)
    if restored_already:
        _wait_for_restored_index(
            client,
            index_name=archive.old_index,
            timeout_seconds=restore_timeout_seconds,
            poll_seconds=restore_poll_seconds,
            monotonic=monotonic,
            sleep=sleep,
        )
        _assert_evidence_matches(
            archive,
            collect_index_evidence(client, index_name=archive.old_index),
        )
        if existing_restore is not None and existing_restore_ref is not None:
            after = _assert_alias_is_active_only(
                client,
                old_index=archive.old_index,
                active_index=archive.active_index,
            )
            expected_indices = tuple(sorted((archive.active_index, archive.old_index)))
            if (
                before != after
                or after != existing_restore.alias_indices_after
                or research_indices(client) != expected_indices
            ):
                raise RuntimeError(
                    "existing restore receipt no longer matches cluster state"
                )
            return {
                "status": "RESTORED",
                "reused": True,
                "restoreReceipt": _receipt_dump(existing_restore_ref),
                "archiveReceipt": _receipt_dump(archive_receipt),
                "activationReceipt": _receipt_dump(archive.activation_receipt),
            }
    else:
        if indices_before != (archive.active_index,):
            raise RuntimeError("restore drill precondition is invalid")
        client.snapshot.restore(
            repository=archive.repository,
            snapshot=archive.snapshot,
            body={
                "indices": [archive.old_index],
                "include_global_state": False,
                "include_aliases": False,
            },
            wait_for_completion=False,
        )
        _wait_for_restored_index(
            client,
            index_name=archive.old_index,
            timeout_seconds=restore_timeout_seconds,
            poll_seconds=restore_poll_seconds,
            monotonic=monotonic,
            sleep=sleep,
        )
        _assert_evidence_matches(
            archive,
            collect_index_evidence(client, index_name=archive.old_index),
        )

    after = _assert_alias_is_active_only(
        client,
        old_index=archive.old_index,
        active_index=archive.active_index,
    )
    if before != after:
        raise RuntimeError("restore drill changed the Research read alias")
    expected_indices = tuple(sorted((archive.active_index, archive.old_index)))
    if research_indices(client) != expected_indices:
        raise RuntimeError("restore drill produced an unexpected Research index set")

    receipt = GoldIndexRestoreReceipt(
        operation_id=operation_id,
        archive_receipt=archive_receipt,
        repository=archive.repository,
        snapshot=archive.snapshot,
        restored_index=archive.old_index,
        active_index=archive.active_index,
        mapping_digest=archive.mapping_digest,
        settings_digest=archive.settings_digest,
        document_count=archive.document_count,
        alias_indices_before=before,
        alias_indices_after=after,
        restored_at=completed_at,
    )
    restore_ref = _publish_model(
        store,
        model=receipt,
        destination_uri=restore_receipt_uri,
        media_type=GOLD_INDEX_RESTORE_RECEIPT_MEDIA_TYPE,
    )
    return {
        "status": "RESTORED",
        "reused": restored_already and existing_restore is not None,
        "restoreReceipt": _receipt_dump(restore_ref),
        "archiveReceipt": _receipt_dump(archive_receipt),
        "activationReceipt": _receipt_dump(archive.activation_receipt),
    }


def validate_restore_result(
    client: Any,
    store: BoundedObjectStore,
    receipt: GoldIndexRestoreReceipt,
) -> None:
    """Independently recheck original-name restore evidence and alias neutrality."""

    archive = read_control_model(
        store,
        receipt.archive_receipt,
        media_type=GOLD_INDEX_ARCHIVE_RECEIPT_MEDIA_TYPE,
        model=GoldIndexArchiveReceipt,
    )
    _validate_activation_receipt(
        store,
        archive.activation_receipt,
        active_index=archive.active_index,
        old_index=archive.old_index,
    )
    _validate_restore_identity(
        receipt,
        operation_id=receipt.operation_id,
        archive=archive,
    )
    assert_snapshot_success(
        client,
        repository=receipt.repository,
        snapshot=receipt.snapshot,
        snapshot_indices=archive.snapshot_indices,
        expected_uuid=archive.snapshot_uuid,
    )
    if not _index_exists(client, receipt.restored_index) or not _index_exists(
        client, receipt.active_index
    ):
        raise RuntimeError("restore validation is missing a required Research index")
    _assert_evidence_matches(
        archive,
        collect_index_evidence(client, index_name=receipt.restored_index),
    )
    aliases = _assert_alias_is_active_only(
        client,
        old_index=receipt.restored_index,
        active_index=receipt.active_index,
    )
    if aliases != receipt.alias_indices_after:
        raise RuntimeError("restore receipt alias evidence no longer matches")


def _repository_body(
    *,
    bucket: str,
    base_path: str,
    region: str,
    role_arn: str,
    readonly: bool,
) -> dict[str, Any]:
    if _S3_BUCKET.fullmatch(bucket) is None:
        raise ValueError("snapshot bucket is invalid")
    normalized_path = base_path.strip("/")
    if not normalized_path or any(
        part in {"", ".", ".."} for part in normalized_path.split("/")
    ):
        raise ValueError("snapshot base path must be a non-empty normalized S3 key")
    if not region or len(region) > 64:
        raise ValueError("snapshot repository region is invalid")
    if _ROLE_ARN.fullmatch(role_arn) is None:
        raise ValueError("snapshot repository role ARN is invalid")
    return {
        "type": "s3",
        "settings": {
            "bucket": bucket,
            "base_path": normalized_path,
            "region": region,
            "role_arn": role_arn,
            "server_side_encryption": True,
            "readonly": readonly,
        },
    }


def _normalized_repository(value: Mapping[str, Any]) -> dict[str, Any]:
    settings = value.get("settings")
    if not isinstance(settings, Mapping):
        raise RuntimeError("OpenSearch snapshot repository settings are invalid")

    def normalize(item: Any) -> Any:
        if isinstance(item, str) and item.lower() in {"true", "false"}:
            return item.lower() == "true"
        return item

    return {
        "type": str(value.get("type", "")),
        "settings": {
            str(key): normalize(item)
            for key, item in sorted(settings.items(), key=lambda pair: str(pair[0]))
        },
    }


def register_snapshot_repository(
    client: Any,
    *,
    repository: str,
    bucket: str,
    base_path: str,
    region: str,
    role_arn: str,
    readonly: bool = False,
) -> dict[str, Any]:
    """Idempotently register one exact S3 snapshot repository."""

    repository = _safe_name(repository, label="snapshot repository")
    expected = _repository_body(
        bucket=bucket,
        base_path=base_path,
        region=region,
        role_arn=role_arn,
        readonly=readonly,
    )
    try:
        response = client.snapshot.get_repository(repository=repository)
    except Exception as exc:
        if _status_code(exc) != 404:
            raise
        response = None
    if response is not None:
        if (
            not isinstance(response, Mapping)
            or repository not in response
            or not isinstance(response[repository], Mapping)
        ):
            raise RuntimeError("OpenSearch repository response is invalid")
        if _normalized_repository(response[repository]) != _normalized_repository(
            expected
        ):
            raise RuntimeError(
                "existing snapshot repository has conflicting immutable settings"
            )
        return {"status": "REGISTERED", "repository": repository, "reused": True}

    client.snapshot.create_repository(
        repository=repository,
        body=expected,
        verify=True,
    )
    created = client.snapshot.get_repository(repository=repository)
    if (
        not isinstance(created, Mapping)
        or repository not in created
        or not isinstance(created[repository], Mapping)
        or _normalized_repository(created[repository])
        != _normalized_repository(expected)
    ):
        raise RuntimeError("OpenSearch repository verification failed")
    return {"status": "REGISTERED", "repository": repository, "reused": False}
