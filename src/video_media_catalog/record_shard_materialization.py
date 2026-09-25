"""Version-safe record shard materialization for Spark ingestion."""

from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock
from typing import Literal
from urllib.parse import unquote, urlsplit

from video_media_catalog.connector import (
    ConnectorBatchManifest,
    ConnectorRecordSetManifest,
)
from video_media_catalog.models import ObjectRef
from video_media_catalog.object_store import RuntimeObjectStore
from video_media_catalog.spark_config import spark_uri
from video_media_catalog.storage import (
    digest_file,
    join_uri,
    local_path,
    publish_file_immutable,
)
from video_media_catalog.v2_contracts import (
    parse_rfc3339,
    require_rfc3339,
    require_sha256,
)

MAX_RECORD_SHARD_COUNT = 8192
MAX_MATERIALIZE_WORKERS = 32
MAX_ENVELOPE_BOUNDARY_BYTES = 256 * 1024
LANDING_RECORD_STAGING_ROOT = "landing/research/materialized-record-shards"


def _spark_input_uri(uri: str) -> str:
    return str(local_path(uri)) if urlsplit(uri).scheme == "file" else uri


@dataclass(frozen=True)
class MaterializedRecordShard:
    """Immutable Spark input bound to a verified source ObjectRef."""

    source: ObjectRef
    spark_uri: str
    checksum: str
    size_bytes: int
    first_envelope_key: str
    last_envelope_key: str
    prevalidated_reuse: bool = False


@dataclass(frozen=True)
class RecordShardMaterializationProgress:
    """One driver-side shard materialization progress event."""

    shard_index: int
    completed_shards: int
    total_shards: int
    status: Literal["FULL_VERIFY", "PREVALIDATED_REUSE"]


@dataclass(frozen=True)
class PrevalidatedRecordSetGrant:
    """Narrow authorization to reuse one exact record set's staged shards."""

    record_set_id: str
    batch_id: str
    record_count: int
    shard_count: int
    size_bytes: int
    expires_at: str
    batch_manifest_ref: ObjectRef
    record_set_manifest_ref: ObjectRef
    staging_prefix: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "record_set_id",
            require_sha256(self.record_set_id, label="prevalidated record_set_id"),
        )
        object.__setattr__(
            self,
            "batch_id",
            require_sha256(self.batch_id, label="prevalidated batch_id"),
        )
        if min(self.record_count, self.shard_count, self.size_bytes) < 1:
            raise ValueError(
                "prevalidated record count, shard count, and size must be positive"
            )
        object.__setattr__(
            self,
            "expires_at",
            require_rfc3339(
                self.expires_at,
                label="prevalidated grant expires_at",
            ),
        )
        for label, reference in (
            ("batch manifest", self.batch_manifest_ref),
            ("record-set manifest", self.record_set_manifest_ref),
        ):
            if (
                urlsplit(reference.uri).scheme != "s3"
                or reference.etag is None
                or reference.object_version is None
                or reference.size_bytes < 1
            ):
                raise ValueError(
                    f"prevalidated {label} must be an immutable S3 ObjectRef"
                )
        normalized_prefix = self.staging_prefix.rstrip("/")
        if urlsplit(normalized_prefix).scheme != "s3":
            raise ValueError("prevalidated staging prefix must use s3://")
        _staging_bucket_and_key(normalized_prefix)
        object.__setattr__(self, "staging_prefix", normalized_prefix)


def materialized_shard_from_reference(reference: ObjectRef) -> MaterializedRecordShard:
    """Build a local Spark input descriptor after checksum verification."""

    if urlsplit(reference.uri).scheme != "file":
        raise ValueError("unmaterialized Spark input requires file:// record shards")
    path = local_path(reference.uri).resolve()
    digest, size = digest_file(path)
    checksum = digest.removeprefix("sha256:")
    if size != reference.size_bytes or checksum != reference.checksum.value:
        raise ValueError("local record shard differs from immutable declaration")
    first_key, last_key = _envelope_key_bounds(path)
    return MaterializedRecordShard(
        source=reference,
        spark_uri=_spark_input_uri(reference.uri),
        checksum=reference.checksum.value,
        size_bytes=reference.size_bytes,
        first_envelope_key=first_key,
        last_envelope_key=last_key,
    )


def _warehouse_bucket_and_key(warehouse: str) -> tuple[str, str]:
    parsed = urlsplit(warehouse.rstrip("/"))
    if parsed.scheme not in {"file", "s3", "s3a"}:
        raise ValueError("warehouse must use file://, s3://, or s3a://")
    if parsed.query or parsed.fragment or parsed.username or parsed.password:
        raise ValueError("warehouse must not contain credentials/query/fragment")
    if parsed.scheme == "file":
        return "", unquote(parsed.path.lstrip("/"))
    if not parsed.netloc or not parsed.path.lstrip("/"):
        raise ValueError("warehouse must include bucket and key prefix")
    return parsed.netloc, unquote(parsed.path.lstrip("/"))


def _staging_bucket_and_key(staging_prefix: str) -> tuple[str, str]:
    parsed = urlsplit(staging_prefix.rstrip("/"))
    if parsed.scheme not in {"file", "s3"}:
        raise ValueError("record staging prefix must use file:// or s3://")
    if parsed.query or parsed.fragment or parsed.username or parsed.password:
        raise ValueError(
            "record staging prefix must not contain credentials/query/fragment"
        )
    if parsed.scheme == "file":
        key = unquote(parsed.path.lstrip("/"))
        if not key:
            raise ValueError("file record staging prefix must include a directory path")
        return "", key
    if not parsed.netloc or not parsed.path.lstrip("/"):
        raise ValueError("S3 record staging prefix must include bucket and key prefix")
    return parsed.netloc, unquote(parsed.path.lstrip("/"))


def _allowed_record_staging_roots(warehouse_key: str) -> tuple[str, ...]:
    control_root = "/".join(
        part for part in (warehouse_key.rstrip("/"), "research", "control") if part
    )
    return (control_root, LANDING_RECORD_STAGING_ROOT)


def validate_record_staging_prefix(
    staging_prefix: str,
    *,
    warehouse: str,
    require_s3: bool,
) -> str:
    """Ensure staging stays within catalog-bucket writable research paths."""

    normalized = staging_prefix.rstrip("/")
    staging_bucket, staging_key = _staging_bucket_and_key(normalized)
    warehouse_bucket, warehouse_key = _warehouse_bucket_and_key(warehouse)
    scheme = urlsplit(normalized).scheme
    if require_s3:
        if scheme != "s3":
            raise ValueError("S3 record shards require s3:// record staging prefix")
        if urlsplit(warehouse).scheme not in {"s3", "s3a"}:
            raise ValueError("S3 record staging requires S3 warehouse")
        if staging_bucket != warehouse_bucket:
            raise ValueError("record staging prefix must use catalog warehouse bucket")
        if not any(
            staging_key == root or staging_key.startswith(f"{root}/")
            for root in _allowed_record_staging_roots(warehouse_key)
        ):
            raise ValueError(
                "record staging prefix is not under an allowed catalog write path"
            )
        return normalized
    if scheme == "s3" and warehouse_bucket and staging_bucket != warehouse_bucket:
        raise ValueError("record staging prefix must use catalog warehouse bucket")
    return normalized


def resolve_record_staging_prefix(
    staging_prefix: str | None,
    *,
    warehouse: str,
    references: Sequence[ObjectRef],
) -> str | None:
    requires_s3 = any(
        urlsplit(reference.uri).scheme == "s3" for reference in references
    )
    if requires_s3:
        if not staging_prefix:
            raise ValueError(
                "S3 record shard materialization requires --record-staging-prefix"
            )
        return validate_record_staging_prefix(
            staging_prefix,
            warehouse=warehouse,
            require_s3=True,
        )
    if staging_prefix:
        return validate_record_staging_prefix(
            staging_prefix,
            warehouse=warehouse,
            require_s3=False,
        )
    return None


def _envelope_key_bounds(path: Path) -> tuple[str, str]:
    first: str | None = None
    last: str | None = None
    with path.open("rb") as handle:
        for line in handle:
            if not line.strip():
                continue
            envelope_key = json.loads(line)["envelopeKey"]
            if not isinstance(envelope_key, str):
                raise ValueError("record shard line missing envelopeKey")
            if first is None:
                first = envelope_key
            last = envelope_key
    if first is None or last is None:
        raise ValueError("record shard contains no envelopes")
    return first, last


def prevalidated_grant_applies(
    grant: PrevalidatedRecordSetGrant,
    *,
    batch: ConnectorBatchManifest,
    record_set: ConnectorRecordSetManifest,
    batch_manifest_ref: ObjectRef,
    record_set_manifest_ref: ObjectRef,
    staging_prefix: str,
    now: datetime | None = None,
) -> bool:
    """Return false only when a valid grant is stale or targets another input."""

    current = datetime.now(UTC) if now is None else now.astimezone(UTC)
    if (
        parse_rfc3339(
            grant.expires_at,
            label="prevalidated grant expires_at",
        )
        <= current
    ):
        return False
    if (
        batch.batch_id != grant.batch_id
        or record_set.record_set_id != grant.record_set_id
    ):
        return False
    if batch_manifest_ref != grant.batch_manifest_ref:
        raise ValueError("prevalidated grant batch manifest binding drifted")
    if record_set_manifest_ref != grant.record_set_manifest_ref:
        raise ValueError("prevalidated grant record-set manifest binding drifted")
    if staging_prefix.rstrip("/") != grant.staging_prefix:
        raise ValueError("prevalidated grant staging prefix binding drifted")
    if record_set.batch_id != batch.batch_id:
        raise ValueError("prevalidated record set does not bind its batch")
    if (
        batch.record_count != grant.record_count
        or record_set.record_count != grant.record_count
    ):
        raise ValueError("prevalidated grant record count binding drifted")
    if len(record_set.record_objects) != grant.shard_count:
        raise ValueError("prevalidated grant shard count binding drifted")
    if sum(item.size_bytes for item in record_set.record_objects) != grant.size_bytes:
        raise ValueError("prevalidated grant byte size binding drifted")
    return True


def record_staging_uri(staging_prefix: str, checksum: str) -> str:
    return join_uri(
        staging_prefix,
        f"sha256={checksum}",
        f"sha256:{checksum}.ndjson",
    )


def _materialize_file_shard(
    reference: ObjectRef,
    *,
    scratch_dir: Path,
    max_bytes: int,
) -> MaterializedRecordShard:
    source = local_path(reference.uri).resolve()
    digest, size = digest_file(source)
    checksum = digest.removeprefix("sha256:")
    if size != reference.size_bytes or checksum != reference.checksum.value:
        raise ValueError("local record shard differs from immutable declaration")
    if size > max_bytes:
        raise ValueError("local record shard exceeds configured limit")
    destination = scratch_dir / f"sha256={checksum}" / f"sha256:{checksum}.ndjson"
    destination.parent.mkdir(parents=True, exist_ok=True)
    if not publish_file_immutable(source, destination):
        actual_digest, actual_size = digest_file(destination)
        actual_checksum = actual_digest.removeprefix("sha256:")
        if (
            actual_size != reference.size_bytes
            or actual_checksum != reference.checksum.value
        ):
            raise ValueError("local record shard staging conflict")
    first_key, last_key = _envelope_key_bounds(destination)
    return MaterializedRecordShard(
        source=reference,
        spark_uri=_spark_input_uri(destination.as_uri()),
        checksum=reference.checksum.value,
        size_bytes=reference.size_bytes,
        first_envelope_key=first_key,
        last_envelope_key=last_key,
    )


def _materialize_s3_shard(
    store: RuntimeObjectStore,
    reference: ObjectRef,
    *,
    shard_index: int,
    staging_prefix: str,
    scratch_dir: Path,
    max_bytes: int,
) -> MaterializedRecordShard:
    if reference.object_version is None or reference.etag is None:
        raise ValueError("S3 record shard requires VersionId and ETag")
    checksum = reference.checksum.value
    destination_uri = record_staging_uri(staging_prefix, checksum)
    download_path = (
        scratch_dir
        / f"shard={shard_index:05d}-sha256={checksum[:16]}"
        / "source.ndjson"
    )
    download_path.parent.mkdir(parents=True, exist_ok=True)
    downloaded = store.download(
        reference,
        download_path,
        max_bytes=max_bytes,
    )
    try:
        if (
            downloaded.sha256 != checksum
            or downloaded.size_bytes != reference.size_bytes
        ):
            raise ValueError(
                "downloaded record shard differs from immutable declaration"
            )
        first_key, last_key = _envelope_key_bounds(downloaded.path)
        uploaded = store.upload_file(
            downloaded.path,
            destination_uri,
            media_type=reference.media_type,
            object_format=reference.format,
            max_bytes=max_bytes,
        )
        staged = uploaded.object_ref
        if (
            staged.checksum.value != checksum
            or staged.size_bytes != reference.size_bytes
        ):
            raise ValueError("staged record shard differs from immutable declaration")
        return MaterializedRecordShard(
            source=reference,
            spark_uri=spark_uri(staged.uri),
            checksum=checksum,
            size_bytes=reference.size_bytes,
            first_envelope_key=first_key,
            last_envelope_key=last_key,
        )
    finally:
        downloaded.path.unlink(missing_ok=True)
        with suppress(OSError):
            downloaded.path.parent.rmdir()


def _envelope_key_from_line(line: bytes) -> str:
    try:
        parsed = json.loads(line)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("staged record shard boundary is not valid JSON") from exc
    envelope_key = parsed.get("envelopeKey") if isinstance(parsed, dict) else None
    if not isinstance(envelope_key, str):
        raise ValueError("staged record shard line missing envelopeKey")
    return envelope_key


def _first_envelope_key(payload: bytes, *, reaches_eof: bool) -> str:
    parts = payload.split(b"\n")
    for index, line in enumerate(parts):
        if not line.strip():
            continue
        complete = index < len(parts) - 1 or reaches_eof
        if not complete:
            raise ValueError("staged record shard first line exceeds the bounded range")
        return _envelope_key_from_line(line)
    raise ValueError("staged record shard contains no envelope in its first range")


def _last_envelope_key(payload: bytes) -> str:
    for line in reversed(payload.split(b"\n")):
        if line.strip():
            return _envelope_key_from_line(line)
    raise ValueError("staged record shard contains no envelope in its last range")


def _staged_envelope_key_bounds(
    store: RuntimeObjectStore,
    staged: ObjectRef,
) -> tuple[str, str]:
    first_length = min(staged.size_bytes, MAX_ENVELOPE_BOUNDARY_BYTES)
    first_payload = store.read_range(
        staged,
        offset=0,
        length=first_length,
        max_bytes=MAX_ENVELOPE_BOUNDARY_BYTES,
    )
    first_key = _first_envelope_key(
        first_payload,
        reaches_eof=first_length == staged.size_bytes,
    )
    if first_length == staged.size_bytes:
        return first_key, _last_envelope_key(first_payload)

    tail_length = min(
        staged.size_bytes,
        MAX_ENVELOPE_BOUNDARY_BYTES + 1,
    )
    tail_offset = staged.size_bytes - tail_length
    tail_payload = store.read_range(
        staged,
        offset=tail_offset,
        length=tail_length,
        max_bytes=MAX_ENVELOPE_BOUNDARY_BYTES + 1,
    )
    if tail_offset > 0:
        if tail_payload[:1] == b"\n":
            tail_payload = tail_payload[1:]
        else:
            separator = tail_payload.find(b"\n")
            if separator < 0:
                raise ValueError(
                    "staged record shard last line exceeds the bounded range"
                )
            tail_payload = tail_payload[separator + 1 :]
    return first_key, _last_envelope_key(tail_payload)


def _reuse_prevalidated_s3_shard(
    store: RuntimeObjectStore,
    reference: ObjectRef,
    *,
    staging_prefix: str,
    max_bytes: int,
) -> MaterializedRecordShard:
    if (
        urlsplit(reference.uri).scheme != "s3"
        or reference.object_version is None
        or reference.etag is None
    ):
        raise ValueError(
            "prevalidated record reuse requires immutable S3 source shards"
        )
    store.head(
        reference,
        max_bytes=max_bytes,
        require_immutable_metadata=True,
    )
    checksum = reference.checksum.value
    staged = store.head(
        ObjectRef(
            uri=record_staging_uri(staging_prefix, checksum),
            format=reference.format,
            media_type=reference.media_type,
            checksum=reference.checksum,
            size_bytes=reference.size_bytes,
        ),
        max_bytes=max_bytes,
        require_immutable_metadata=True,
    )
    first_key, last_key = _staged_envelope_key_bounds(store, staged)
    return MaterializedRecordShard(
        source=reference,
        spark_uri=spark_uri(staged.uri),
        checksum=checksum,
        size_bytes=reference.size_bytes,
        first_envelope_key=first_key,
        last_envelope_key=last_key,
        prevalidated_reuse=True,
    )


def materialize_record_shards(
    store: RuntimeObjectStore,
    references: Sequence[ObjectRef],
    *,
    staging_prefix: str | None = None,
    scratch_dir: Path,
    max_bytes: int,
    max_shards: int = MAX_RECORD_SHARD_COUNT,
    workers: int = 1,
    prevalidated_grant: PrevalidatedRecordSetGrant | None = None,
    batch: ConnectorBatchManifest | None = None,
    record_set: ConnectorRecordSetManifest | None = None,
    batch_manifest_ref: ObjectRef | None = None,
    record_set_manifest_ref: ObjectRef | None = None,
    progress_callback: (
        Callable[[RecordShardMaterializationProgress], None] | None
    ) = None,
) -> tuple[MaterializedRecordShard, ...]:
    """Materialize versioned record shards into immutable Spark inputs."""

    if max_bytes < 1:
        raise ValueError("max_bytes must be positive")
    if max_shards < 1:
        raise ValueError("max_shards must be positive")
    if not 1 <= workers <= MAX_MATERIALIZE_WORKERS:
        raise ValueError(f"workers must be between 1 and {MAX_MATERIALIZE_WORKERS}")
    if len(references) > max_shards:
        raise ValueError("record shard count exceeds configured limit")
    if not references:
        return ()
    scratch_dir.mkdir(parents=True, exist_ok=True)
    s3_refs = [
        reference for reference in references if urlsplit(reference.uri).scheme == "s3"
    ]
    if s3_refs and staging_prefix is None:
        raise ValueError("S3 record shard materialization requires staging prefix")

    use_prevalidated = False
    if prevalidated_grant is not None:
        if (
            batch is None
            or record_set is None
            or batch_manifest_ref is None
            or record_set_manifest_ref is None
            or staging_prefix is None
        ):
            raise ValueError(
                "prevalidated record reuse requires complete manifest context"
            )
        if tuple(references) != record_set.record_objects:
            raise ValueError(
                "prevalidated materialization references differ from record set"
            )
        use_prevalidated = prevalidated_grant_applies(
            prevalidated_grant,
            batch=batch,
            record_set=record_set,
            batch_manifest_ref=batch_manifest_ref,
            record_set_manifest_ref=record_set_manifest_ref,
            staging_prefix=staging_prefix,
        )

    completed = 0
    progress_lock = Lock()

    def completed_result(
        shard_index: int,
        result: MaterializedRecordShard,
    ) -> MaterializedRecordShard:
        nonlocal completed
        with progress_lock:
            completed += 1
            progress = RecordShardMaterializationProgress(
                shard_index=shard_index,
                completed_shards=completed,
                total_shards=len(references),
                status=(
                    "PREVALIDATED_REUSE" if result.prevalidated_reuse else "FULL_VERIFY"
                ),
            )
        if progress_callback is not None:
            progress_callback(progress)
        return result

    def materialize_one(
        indexed_reference: tuple[int, ObjectRef],
    ) -> MaterializedRecordShard:
        shard_index, reference = indexed_reference
        if use_prevalidated:
            assert staging_prefix is not None
            return completed_result(
                shard_index,
                _reuse_prevalidated_s3_shard(
                    store,
                    reference,
                    staging_prefix=staging_prefix,
                    max_bytes=max_bytes,
                ),
            )
        scheme = urlsplit(reference.uri).scheme
        if scheme == "file":
            return completed_result(
                shard_index,
                _materialize_file_shard(
                    reference,
                    scratch_dir=scratch_dir,
                    max_bytes=max_bytes,
                ),
            )
        if scheme != "s3":
            raise ValueError("record shard URI must use file:// or s3://")
        assert staging_prefix is not None
        return completed_result(
            shard_index,
            _materialize_s3_shard(
                store,
                reference,
                shard_index=shard_index,
                staging_prefix=staging_prefix,
                scratch_dir=scratch_dir,
                max_bytes=max_bytes,
            ),
        )

    indexed = tuple(enumerate(references))
    if workers == 1:
        return tuple(materialize_one(item) for item in indexed)
    with ThreadPoolExecutor(
        max_workers=min(workers, len(indexed)),
        thread_name_prefix="record-shard-materialize",
    ) as executor:
        return tuple(executor.map(materialize_one, indexed))


def validate_stream_envelope_key_bounds(
    shards: Sequence[MaterializedRecordShard],
    record_set: ConnectorRecordSetManifest,
) -> None:
    if record_set.record_count == 0:
        return
    if not shards:
        raise ValueError("record set requires materialized shards")
    if shards[0].first_envelope_key != record_set.first_envelope_key:
        raise ValueError("first envelope key does not match record set")
    if shards[-1].last_envelope_key != record_set.last_envelope_key:
        raise ValueError("last envelope key does not match record set")


def bind_materialized_shards(
    shards: Sequence[MaterializedRecordShard],
    references: Sequence[ObjectRef],
) -> None:
    """Fail closed when materialized inputs drift from declared ObjectRefs."""

    if len(shards) != len(references):
        raise ValueError("materialized shard count differs from record set")
    for shard, reference in zip(shards, references, strict=True):
        if shard.source != reference:
            raise ValueError("materialized shard source binding mismatch")
        if (
            shard.checksum != reference.checksum.value
            or shard.size_bytes != reference.size_bytes
        ):
            raise ValueError("materialized shard checksum binding mismatch")
