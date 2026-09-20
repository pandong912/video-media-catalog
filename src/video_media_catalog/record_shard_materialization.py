"""Version-safe record shard materialization for Spark ingestion."""

from __future__ import annotations

import json
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

from video_media_catalog.connector import ConnectorRecordSetManifest
from video_media_catalog.models import ObjectRef
from video_media_catalog.object_store import RuntimeObjectStore
from video_media_catalog.runtime_args import join_uri
from video_media_catalog.spark_input import spark_uri
from video_media_catalog.storage import digest_file, local_path, publish_file_immutable

MAX_RECORD_SHARD_COUNT = 4096
RECORD_STAGING_SEGMENT = "_staging/record-shards"


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


def default_record_staging_prefix(*uris: str) -> str:
    """Derive a sibling staging prefix from versioned record object URIs."""

    if not uris:
        raise ValueError("record staging prefix requires at least one object URI")
    parsed = urlsplit(uris[0])
    if parsed.scheme == "file":
        parent = local_path(uris[0]).resolve().parent
        return join_uri(parent.as_uri(), RECORD_STAGING_SEGMENT)
    if parsed.scheme != "s3":
        raise ValueError("record staging prefix requires file:// or s3:// URIs")
    parent = parsed.path.rsplit("/", 1)[0]
    return f"s3://{parsed.netloc}{parent}/{RECORD_STAGING_SEGMENT}"


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
    staging_prefix: str,
    scratch_dir: Path,
    max_bytes: int,
) -> MaterializedRecordShard:
    if reference.object_version is None or reference.etag is None:
        raise ValueError("S3 record shard requires VersionId and ETag")
    checksum = reference.checksum.value
    destination_uri = record_staging_uri(staging_prefix, checksum)
    with tempfile.TemporaryDirectory(
        prefix="record-shard-download-",
        dir=scratch_dir,
    ) as directory:
        downloaded = store.download(
            reference,
            Path(directory) / "source.ndjson",
            max_bytes=max_bytes,
        )
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
    if staged.checksum.value != checksum or staged.size_bytes != reference.size_bytes:
        raise ValueError("staged record shard differs from immutable declaration")
    return MaterializedRecordShard(
        source=reference,
        spark_uri=spark_uri(staged.uri),
        checksum=checksum,
        size_bytes=reference.size_bytes,
        first_envelope_key=first_key,
        last_envelope_key=last_key,
    )


def materialize_record_shards(
    store: RuntimeObjectStore,
    references: Sequence[ObjectRef],
    *,
    staging_prefix: str | None = None,
    scratch_dir: Path | None = None,
    max_bytes: int,
    max_shards: int = MAX_RECORD_SHARD_COUNT,
) -> tuple[MaterializedRecordShard, ...]:
    """Materialize versioned record shards into immutable Spark inputs."""

    if max_bytes < 1:
        raise ValueError("max_bytes must be positive")
    if max_shards < 1:
        raise ValueError("max_shards must be positive")
    if len(references) > max_shards:
        raise ValueError("record shard count exceeds configured limit")
    if not references:
        return ()
    resolved_scratch = scratch_dir or Path(
        tempfile.mkdtemp(prefix="record-shard-materialize-")
    )
    resolved_scratch.mkdir(parents=True, exist_ok=True)
    s3_refs = [
        reference for reference in references if urlsplit(reference.uri).scheme == "s3"
    ]
    resolved_staging = staging_prefix or (
        default_record_staging_prefix(*(item.uri for item in s3_refs))
        if s3_refs
        else None
    )
    materialized: list[MaterializedRecordShard] = []
    for reference in references:
        scheme = urlsplit(reference.uri).scheme
        if scheme == "file":
            materialized.append(
                _materialize_file_shard(
                    reference,
                    scratch_dir=resolved_scratch,
                    max_bytes=max_bytes,
                )
            )
            continue
        if scheme != "s3":
            raise ValueError("record shard URI must use file:// or s3://")
        if resolved_staging is None:
            raise ValueError("S3 record shard materialization requires staging prefix")
        materialized.append(
            _materialize_s3_shard(
                store,
                reference,
                staging_prefix=resolved_staging,
                scratch_dir=resolved_scratch,
                max_bytes=max_bytes,
            )
        )
    return tuple(materialized)


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
