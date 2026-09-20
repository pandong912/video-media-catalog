"""Bounded finalization for distributed Wikidata full-media staging."""

from __future__ import annotations

import hashlib
import json
import tempfile
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from video_media_catalog.connector import (
    ConnectorRecordSetEpochManifest,
    ConnectorRecordSetPartitionManifest,
    ConnectorRecordShard,
    build_connector_record_set_epoch_manifest,
    build_connector_record_set_partition_manifest,
    build_connector_sharded_record_set_manifest,
)
from video_media_catalog.connector_publish import publish_capture_window_commit
from video_media_catalog.object_store import (
    ObjectStoreError,
    RuntimeObjectStore,
    S3Location,
)
from video_media_catalog.runtime_args import join_uri
from video_media_catalog.storage import local_path
from video_media_catalog.wikidata_full_backfill import (
    FullMediaBackfillConfig,
    WikidataFullMediaBackfillCommit,
    build_full_media_capture_receipt,
    build_wikidata_full_media_commit,
)
from video_media_catalog.wikidata_full_backfill_spark import SparkFullMediaBuild

CONTROL_OBJECT_MAX_BYTES = 16 * 1024 * 1024
_STREAM_BYTES = 1024 * 1024


def full_media_output_root(
    output_prefix: str,
    *,
    dump_date: str,
    build_digest: str,
) -> str:
    return join_uri(
        output_prefix,
        "wikidata",
        "full-media",
        f"date={dump_date}",
        f"build-sha256={build_digest.removeprefix('sha256:')}",
    )


def _is_missing(exc: BaseException) -> bool:
    response = getattr(exc, "response", {})
    code = str(response.get("Error", {}).get("Code", ""))
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return code in {"404", "NoSuchKey", "NotFound"} or status == 404


def _read_control_uri(
    uri: str,
    *,
    s3: Any,
) -> bytes | None:
    parsed = urlsplit(uri)
    if parsed.scheme == "file":
        path = local_path(uri)
        if not path.exists():
            return None
        if path.stat().st_size > CONTROL_OBJECT_MAX_BYTES:
            raise ValueError("existing control object exceeds size limit")
        return path.read_bytes()
    location = S3Location.parse(uri)
    try:
        response = s3.get_object(
            Bucket=location.bucket,
            Key=location.key,
            ChecksumMode="ENABLED",
        )
    except Exception as exc:
        if _is_missing(exc):
            return None
        raise
    body = response.get("Body")
    if body is None or not hasattr(body, "read"):
        raise RuntimeError("S3 control object has no readable body")
    try:
        declared = int(response.get("ContentLength", -1))
        if declared < 1 or declared > CONTROL_OBJECT_MAX_BYTES:
            raise ValueError("existing control object exceeds size limit")
        payload = body.read(CONTROL_OBJECT_MAX_BYTES + 1)
    finally:
        body.close()
    if len(payload) != declared or len(payload) > CONTROL_OBJECT_MAX_BYTES:
        raise ValueError("existing control object size changed while reading")
    return payload


def load_existing_full_media_commit(
    *,
    output_root: str,
    expected_build_digest: str,
    store: RuntimeObjectStore,
    s3: Any,
) -> WikidataFullMediaBackfillCommit | None:
    payload = _read_control_uri(
        join_uri(output_root, "backfill-commit.json"),
        s3=s3,
    )
    if payload is None:
        return None
    commit = WikidataFullMediaBackfillCommit.model_validate_json(payload)
    if commit.build_digest != expected_build_digest:
        raise ObjectStoreError(
            "IMMUTABLE_OBJECT_CONFLICT",
            "existing backfill commit binds different content",
        )
    store.verify(commit.dump, max_bytes=commit.dump.size_bytes)
    store.verify(commit.batch_manifest, max_bytes=CONTROL_OBJECT_MAX_BYTES)
    store.verify(commit.record_set_manifest, max_bytes=CONTROL_OBJECT_MAX_BYTES)
    store.verify(commit.source_watermark, max_bytes=CONTROL_OBJECT_MAX_BYTES)
    store.verify(commit.window_receipt, max_bytes=CONTROL_OBJECT_MAX_BYTES)
    return commit


def _local_staged_part(record_staging_uri: str, shard_index: int) -> Path:
    directory = local_path(join_uri(record_staging_uri, f"shard_index={shard_index}"))
    candidates = sorted(
        path
        for path in directory.iterdir()
        if path.is_file()
        and path.name.startswith("part-")
        and not path.name.endswith(".crc")
    )
    if len(candidates) != 1:
        raise RuntimeError("staged shard directory must contain exactly one part")
    return candidates[0]


def _download_s3_staged_part(
    *,
    record_staging_uri: str,
    shard_index: int,
    destination: Path,
    max_bytes: int,
    s3: Any,
) -> Path:
    prefix = S3Location.parse(
        join_uri(record_staging_uri, f"shard_index={shard_index}", "placeholder")
    )
    key_prefix = prefix.key.rsplit("/", 1)[0] + "/"
    listed = s3.list_objects_v2(
        Bucket=prefix.bucket,
        Prefix=key_prefix,
        MaxKeys=4,
    )
    if listed.get("IsTruncated"):
        raise RuntimeError("staged shard directory contains too many objects")
    candidates = [
        str(item["Key"])
        for item in listed.get("Contents") or []
        if str(item.get("Key", "")).rsplit("/", 1)[-1].startswith("part-")
    ]
    if len(candidates) != 1:
        raise RuntimeError("staged shard directory must contain exactly one part")
    key = candidates[0]
    head = s3.head_object(Bucket=prefix.bucket, Key=key)
    version = head.get("VersionId")
    request: dict[str, Any] = {
        "Bucket": prefix.bucket,
        "Key": key,
        "ChecksumMode": "ENABLED",
    }
    if isinstance(version, str) and version and version != "null":
        request["VersionId"] = version
    response = s3.get_object(**request)
    body = response.get("Body")
    if body is None or not hasattr(body, "read"):
        raise RuntimeError("staged S3 shard has no readable body")
    size = 0
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        with destination.open("wb") as handle:
            while chunk := body.read(_STREAM_BYTES):
                size += len(chunk)
                if size > max_bytes:
                    raise ValueError("staged record shard exceeds configured limit")
                handle.write(chunk)
    finally:
        body.close()
    declared = int(response.get("ContentLength", -1))
    if declared != size:
        raise ValueError("staged record shard size changed while downloading")
    return destination


def _inspect_shard(path: Path, *, max_bytes: int) -> dict[str, Any]:
    digest = hashlib.sha256()
    size = 0
    count = 0
    first_key: str | None = None
    last_key: str | None = None
    with path.open("rb") as handle:
        for line in handle:
            size += len(line)
            if size > max_bytes:
                raise ValueError("record shard exceeds configured limit")
            digest.update(line)
            if not line.strip():
                continue
            value = json.loads(line)
            envelope_key = value.get("envelopeKey")
            if not isinstance(envelope_key, str):
                raise ValueError("record shard line has no envelopeKey")
            first_key = first_key or envelope_key
            last_key = envelope_key
            count += 1
    if count < 1 or first_key is None or last_key is None:
        raise ValueError("record shard contains no envelopes")
    return {
        "sha256": digest.hexdigest(),
        "size_bytes": size,
        "record_count": count,
        "first_envelope_key": first_key,
        "last_envelope_key": last_key,
    }


def _summary_value(row: Any, name: str) -> Any:
    try:
        return row[name]
    except TypeError:
        return getattr(row, name)


def _materialize_staged_part(
    *,
    record_staging_uri: str,
    shard_index: int,
    scratch_dir: Path,
    max_bytes: int,
    s3: Any,
) -> Path:
    if urlsplit(record_staging_uri).scheme == "file":
        return _local_staged_part(record_staging_uri, shard_index)
    return _download_s3_staged_part(
        record_staging_uri=record_staging_uri,
        shard_index=shard_index,
        destination=scratch_dir / f"staged-shard-{shard_index:08d}.ndjson",
        max_bytes=max_bytes,
        s3=s3,
    )


def _publish_shard(
    *,
    summary: Any,
    record_staging_uri: str,
    output_root: str,
    config: FullMediaBackfillConfig,
    store: RuntimeObjectStore,
    s3: Any,
    scratch_dir: Path,
) -> ConnectorRecordShard:
    shard_index = int(_summary_value(summary, "shard_index"))
    epoch_index = int(_summary_value(summary, "epoch_index"))
    partition_index = int(_summary_value(summary, "partition_index"))
    source = _materialize_staged_part(
        record_staging_uri=record_staging_uri,
        shard_index=shard_index,
        scratch_dir=scratch_dir,
        max_bytes=config.max_shard_bytes,
        s3=s3,
    )
    inspected = _inspect_shard(source, max_bytes=config.max_shard_bytes)
    expected = {
        "size_bytes": int(_summary_value(summary, "size_bytes")),
        "record_count": int(_summary_value(summary, "record_count")),
        "first_envelope_key": str(_summary_value(summary, "first_envelope_key")),
        "last_envelope_key": str(_summary_value(summary, "last_envelope_key")),
    }
    for name, value in expected.items():
        if inspected[name] != value:
            raise ValueError(f"staged shard {name} differs from Spark summary")
    destination = join_uri(
        output_root,
        "records",
        f"epoch={epoch_index:05d}",
        f"partition={partition_index:05d}",
        f"shard={shard_index:08d}",
        f"{inspected['sha256']}.ndjson",
    )
    reference = store.upload_file(
        source,
        destination,
        media_type=(
            "application/vnd.video-media-catalog.connector-record-envelope.v2+ndjson"
        ),
        object_format="OBJECT_FORMAT_OTHER",
        max_bytes=config.max_shard_bytes,
    ).object_ref
    if (
        reference.checksum.value != inspected["sha256"]
        or reference.size_bytes != inspected["size_bytes"]
    ):
        raise RuntimeError("published record shard differs from staged bytes")
    return ConnectorRecordShard(
        shard_index=shard_index,
        object_ref=reference,
        record_count=inspected["record_count"],
        first_envelope_key=inspected["first_envelope_key"],
        last_envelope_key=inspected["last_envelope_key"],
    )


def _publish_control(
    *,
    payload: bytes,
    uri: str,
    media_type: str,
    store: RuntimeObjectStore,
) -> Any:
    return store.upload_bytes(
        payload,
        uri,
        media_type=media_type,
        object_format="OBJECT_FORMAT_JSON",
        max_bytes=CONTROL_OBJECT_MAX_BYTES,
    ).object_ref


def _bounded_partition_summaries(
    summaries: Any,
    *,
    global_partition_index: int,
    limit: int,
) -> list[Any]:
    rows = (
        summaries.where(summaries.global_partition_index == global_partition_index)
        .orderBy("shard_index")
        .take(limit + 1)
    )
    if len(rows) > limit:
        raise ValueError("partition summary exceeds configured shard bound")
    return rows


def _partition_manifest(
    *,
    build: SparkFullMediaBuild,
    shards: list[ConnectorRecordShard],
    epoch_index: int,
    partition_index: int,
) -> ConnectorRecordSetPartitionManifest:
    return build_connector_record_set_partition_manifest(
        batch_id=build.batch.batch_id,
        source_product_id=build.batch.source_product_id,
        policy_id=build.batch.policy_id,
        policy_digest=build.batch.policy_digest,
        epoch_index=epoch_index,
        partition_index=partition_index,
        shards=tuple(shards),
        shard_count=len(shards),
        record_count=sum(item.record_count for item in shards),
        size_bytes=sum(item.object_ref.size_bytes for item in shards),
        first_envelope_key=shards[0].first_envelope_key,
        last_envelope_key=shards[-1].last_envelope_key,
        created_at=build.batch.acquired_at,
    )


def _epoch_manifest(
    *,
    build: SparkFullMediaBuild,
    epoch_index: int,
    partitions: list[tuple[ConnectorRecordSetPartitionManifest, Any]],
) -> ConnectorRecordSetEpochManifest:
    return build_connector_record_set_epoch_manifest(
        batch_id=build.batch.batch_id,
        source_product_id=build.batch.source_product_id,
        policy_id=build.batch.policy_id,
        policy_digest=build.batch.policy_digest,
        epoch_index=epoch_index,
        partition_objects=tuple(reference for _, reference in partitions),
        partition_count=len(partitions),
        shard_count=sum(item.shard_count for item, _ in partitions),
        record_count=sum(item.record_count for item, _ in partitions),
        size_bytes=sum(item.size_bytes for item, _ in partitions),
        first_envelope_key=partitions[0][0].first_envelope_key,
        last_envelope_key=partitions[-1][0].last_envelope_key,
        created_at=build.batch.acquired_at,
    )


def publish_full_media_backfill(
    *,
    build: SparkFullMediaBuild,
    summaries: Any,
    config: FullMediaBackfillConfig,
    record_staging_uri: str,
    output_root: str,
    control_prefix: str,
    dump_date: str,
    store: RuntimeObjectStore,
    s3: Any,
) -> WikidataFullMediaBackfillCommit:
    """Finalize bounded shards, hierarchical manifests, then one commit marker."""

    epoch_entries: list[tuple[ConnectorRecordSetEpochManifest, Any]] = []
    with tempfile.TemporaryDirectory(
        prefix="wikidata-full-media-finalizer-"
    ) as directory:
        scratch_dir = Path(directory)
        for epoch_index in range(build.profile.estimated_epochs):
            partition_entries: list[
                tuple[ConnectorRecordSetPartitionManifest, Any]
            ] = []
            for partition_index in range(config.max_partitions_per_epoch):
                global_partition_index = (
                    epoch_index * config.max_partitions_per_epoch + partition_index
                )
                if global_partition_index >= build.profile.estimated_partitions:
                    break
                rows = _bounded_partition_summaries(
                    summaries,
                    global_partition_index=global_partition_index,
                    limit=config.max_shards_per_partition,
                )
                if not rows:
                    continue
                shards = [
                    _publish_shard(
                        summary=row,
                        record_staging_uri=record_staging_uri,
                        output_root=output_root,
                        config=config,
                        store=store,
                        s3=s3,
                        scratch_dir=scratch_dir,
                    )
                    for row in rows
                ]
                partition = _partition_manifest(
                    build=build,
                    shards=shards,
                    epoch_index=epoch_index,
                    partition_index=partition_index,
                )
                partition_ref = _publish_control(
                    payload=partition.json_bytes(),
                    uri=join_uri(
                        output_root,
                        "record-set",
                        f"epoch={epoch_index:05d}",
                        f"partition={partition_index:05d}.json",
                    ),
                    media_type=(
                        "application/vnd.video-media-catalog."
                        "record-set-partition.v2.1+json"
                    ),
                    store=store,
                )
                partition_entries.append((partition, partition_ref))
                for shard in shards:
                    staged = scratch_dir / (
                        f"staged-shard-{shard.shard_index:08d}.ndjson"
                    )
                    staged.unlink(missing_ok=True)
            if not partition_entries:
                continue
            epoch = _epoch_manifest(
                build=build,
                epoch_index=epoch_index,
                partitions=partition_entries,
            )
            epoch_ref = _publish_control(
                payload=epoch.json_bytes(),
                uri=join_uri(
                    output_root,
                    "record-set",
                    f"epoch={epoch_index:05d}.json",
                ),
                media_type=(
                    "application/vnd.video-media-catalog.record-set-epoch.v2.1+json"
                ),
                store=store,
            )
            epoch_entries.append((epoch, epoch_ref))

    if not epoch_entries:
        raise RuntimeError("full-media finalizer produced no epochs")
    record_count = sum(item.record_count for item, _ in epoch_entries)
    size_bytes = sum(item.size_bytes for item, _ in epoch_entries)
    shard_count = sum(item.shard_count for item, _ in epoch_entries)
    partition_count = sum(item.partition_count for item, _ in epoch_entries)
    if (
        record_count != build.profile.record_count
        or size_bytes != build.profile.envelope_bytes
        or shard_count > build.profile.estimated_shards
        or partition_count > build.profile.estimated_partitions
    ):
        raise RuntimeError("finalized record-set totals differ from Spark profile")

    batch_ref = _publish_control(
        payload=build.batch.json_bytes(),
        uri=join_uri(output_root, "batch-manifest.json"),
        media_type="application/vnd.video-media-catalog.connector-batch.v2+json",
        store=store,
    )
    record_set = build_connector_sharded_record_set_manifest(
        batch_id=build.batch.batch_id,
        source_product_id=build.batch.source_product_id,
        policy_id=build.batch.policy_id,
        policy_digest=build.batch.policy_digest,
        epoch_objects=tuple(reference for _, reference in epoch_entries),
        epoch_count=len(epoch_entries),
        partition_count=partition_count,
        shard_count=shard_count,
        record_count=record_count,
        size_bytes=size_bytes,
        first_envelope_key=epoch_entries[0][0].first_envelope_key,
        last_envelope_key=epoch_entries[-1][0].last_envelope_key,
        created_at=build.batch.acquired_at,
    )
    record_set_ref = _publish_control(
        payload=record_set.json_bytes(),
        uri=join_uri(output_root, "record-set.json"),
        media_type=("application/vnd.video-media-catalog.sharded-record-set.v2.1+json"),
        store=store,
    )
    receipt = build_full_media_capture_receipt(
        batch_object=batch_ref,
        build_digest=build.profile.build_digest,
        config_digest=build.profile.config_digest,
        image_digest=build.profile.image_digest,
        policy_digest=build.batch.policy_digest,
        dump_date=dump_date,
        record_count=record_count,
    )
    window_control = publish_capture_window_commit(
        destination_prefix=control_prefix,
        receipt=receipt,
        store=store,
    )
    commit = build_wikidata_full_media_commit(
        build_digest=build.profile.build_digest,
        config_digest=build.profile.config_digest,
        image_digest=build.profile.image_digest,
        dump=build.profile.dump,
        batch_manifest=batch_ref,
        record_set_manifest=record_set_ref,
        source_watermark=window_control.source_watermark_object,
        window_receipt=window_control.receipt_object,
        profile=build.profile,
        created_at=build.batch.acquired_at,
    )
    _publish_control(
        payload=commit.json_bytes(),
        uri=join_uri(output_root, "backfill-commit.json"),
        media_type=(
            "application/vnd.video-media-catalog.wikidata-full-media-backfill.v1+json"
        ),
        store=store,
    )
    return commit
