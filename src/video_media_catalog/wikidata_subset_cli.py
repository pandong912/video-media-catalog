"""Spark 3.5.5 CLI for deterministic, budgeted Wikidata subset generation."""

from __future__ import annotations

import argparse
import base64
import hashlib
import os
import re
import tempfile
import uuid
from collections.abc import Mapping, Sequence
from contextlib import suppress
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from video_media_catalog.canonical import canonical_json, sha256_digest
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.object_store import (
    BoundedObjectStore,
    ObjectStoreError,
    S3Location,
    _etag,
    _timestamp,
)
from video_media_catalog.source_manifest import (
    SOURCE_MANIFEST_MEDIA_TYPE,
    write_source_manifest,
)
from video_media_catalog.spark_input import configure_s3a_builder
from video_media_catalog.wikidata_subset import (
    DEFAULT_TARGET_COUNT,
    DEFAULT_WORK_QUOTAS,
    NORMALIZATION_ALGORITHM_ID,
    SUBSET_ALGORITHM_ID,
    NormalizedStagingManifest,
    SubsetAuditManifest,
    SubsetSelectionConfig,
    audit_json_bytes,
    subset_source_manifest_entry,
)
from video_media_catalog.wikidata_subset_spark import (
    build_subset,
    configure_bfs_materialize_dir,
    item_entity_rows,
    normalize_dump,
    write_normalized_staging,
)
from video_media_catalog.wikidata_sync import (
    DEFAULT_MAX_DUMP_BYTES,
    validate_official_dump_url,
)

MAX_SUBSET_BYTES = 4 * 1024**3
MAX_CONTROL_BYTES = 16 * 1024**2
S3_STREAM_CHUNK_BYTES = 1024 * 1024
MAX_TEMPORARY_KEYS = 32
_DUMP_NAME = re.compile(r"^wikidata-([0-9]{8})-all\.json\.bz2$")
_SHA1 = re.compile(r"^[0-9a-f]{40}$")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="video-media-catalog-wikidata-subset",
        description=(
            "Build a deterministic budgeted subset from one immutable official "
            "Wikidata JSON bzip2 S3 object."
        ),
    )
    parser.add_argument("--dump-uri", required=True)
    parser.add_argument("--dump-sha256", required=True)
    parser.add_argument("--dump-size", required=True, type=int)
    parser.add_argument("--dump-version", required=True)
    parser.add_argument("--dump-etag", required=True)
    parser.add_argument("--output-prefix", required=True)
    parser.add_argument("--staging-prefix", required=True)
    parser.add_argument("--target-count", type=int, default=DEFAULT_TARGET_COUNT)
    parser.add_argument(
        "--movie-count",
        type=int,
        default=DEFAULT_WORK_QUOTAS["MOVIE"],
    )
    parser.add_argument(
        "--tv-series-count",
        type=int,
        default=DEFAULT_WORK_QUOTAS["TV_SERIES"],
    )
    parser.add_argument(
        "--tv-season-count",
        type=int,
        default=DEFAULT_WORK_QUOTAS["TV_SEASON"],
    )
    parser.add_argument(
        "--tv-episode-count",
        type=int,
        default=DEFAULT_WORK_QUOTAS["TV_EPISODE"],
    )
    parser.add_argument(
        "--max-dump-bytes",
        type=int,
        default=DEFAULT_MAX_DUMP_BYTES,
    )
    parser.add_argument(
        "--max-output-bytes",
        type=int,
        default=MAX_SUBSET_BYTES,
    )
    parser.add_argument("--max-closure-iterations", type=int, default=64)
    parser.add_argument("--shuffle-partitions", type=int)
    parser.add_argument("--master", help="optional Spark master, e.g. local[2]")
    parser.add_argument(
        "--app-name",
        default="video-media-catalog-wikidata-subset",
    )
    parser.add_argument("--spark-packages")
    parser.add_argument("--aws-region", default=os.environ.get("AWS_REGION"))
    parser.add_argument("--s3-endpoint", default=os.environ.get("S3_ENDPOINT"))
    parser.add_argument(
        "--s3-path-style-access",
        action="store_true",
        default=os.environ.get("S3_PATH_STYLE", "").lower() in {"1", "true", "yes"},
    )
    return parser


def _s3_client(parsed: argparse.Namespace) -> Any:
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        region_name=parsed.aws_region,
        endpoint_url=parsed.s3_endpoint,
        config=Config(
            signature_version="s3v4",
            retries={"max_attempts": 5, "mode": "standard"},
            s3={
                "addressing_style": (
                    "path" if parsed.s3_path_style_access else "virtual"
                )
            },
        ),
    )


def _is_missing(exc: BaseException) -> bool:
    response = getattr(exc, "response", {})
    code = str(response.get("Error", {}).get("Code", ""))
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return code in {"404", "NoSuchKey", "NotFound"} or status == 404


def _is_precondition_failure(exc: BaseException) -> bool:
    response = getattr(exc, "response", {})
    code = str(response.get("Error", {}).get("Code", ""))
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return code in {"PreconditionFailed", "ConditionalRequestConflict"} or status in {
        409,
        412,
    }


def _prefix(uri: str) -> S3Location:
    parsed = urlsplit(uri)
    if (
        parsed.scheme != "s3"
        or not parsed.netloc
        or not parsed.path.strip("/")
        or parsed.query
        or parsed.fragment
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise ValueError("prefix must be s3://bucket/non-empty-key")
    return S3Location.parse(uri.rstrip("/") + "/placeholder")


def _join(prefix: S3Location, *parts: str) -> S3Location:
    base = prefix.key.rsplit("/", 1)[0]
    key = "/".join(
        [
            *(segment for segment in [base] if segment),
            *(part.strip("/") for part in parts if part.strip("/")),
        ]
    )
    return S3Location(prefix.bucket, key)


def _spark_uri(uri: str) -> str:
    return "s3a://" + uri[len("s3://") :] if uri.startswith("s3://") else uri


def _read_small_json(s3: Any, location: S3Location) -> bytes | None:
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
        if declared < 0 or declared > MAX_CONTROL_BYTES:
            raise ValueError("S3 control object exceeds its size limit")
        payload = body.read(MAX_CONTROL_BYTES + 1)
    finally:
        body.close()
    if len(payload) > MAX_CONTROL_BYTES:
        raise ValueError("S3 control object exceeds its size limit")
    return payload


def _dump_reference(
    parsed: argparse.Namespace,
    *,
    s3: Any,
    store: BoundedObjectStore,
) -> tuple[ObjectRef, str]:
    location = S3Location.parse(parsed.dump_uri)
    filename = location.key.rsplit("/", 1)[-1]
    match = _DUMP_NAME.fullmatch(filename)
    if match is None:
        raise ValueError("dump URI must end with wikidata-YYYYMMDD-all.json.bz2")
    dump_date = match.group(1)
    dump_etag = _etag(parsed.dump_etag)
    dump_version = str(parsed.dump_version).strip()
    if dump_etag is None or not dump_version or dump_version == "null":
        raise ValueError("dump ETag and VersionId must be non-empty")
    reference = ObjectRef(
        uri=location.uri,
        format="OBJECT_FORMAT_JSON",
        media_type="application/x-bzip2",
        checksum=Checksum(value=parsed.dump_sha256),
        size_bytes=parsed.dump_size,
        etag=dump_etag,
        object_version=dump_version,
        attributes={"compression": "bzip2"},
    )
    store.verify(reference, max_bytes=parsed.max_dump_bytes)
    head = s3.head_object(
        Bucket=location.bucket,
        Key=location.key,
        VersionId=dump_version,
        ChecksumMode="ENABLED",
    )
    metadata = {
        str(key).lower(): str(value)
        for key, value in (head.get("Metadata") or {}).items()
    }
    source_url = metadata.get("source-url", "")
    upstream_sha1 = metadata.get("upstream-sha1", "").lower()
    official = validate_official_dump_url(source_url)
    if (
        official.date != dump_date
        or official.filename != filename
        or _SHA1.fullmatch(upstream_sha1) is None
        or metadata.get("dump-date") != dump_date
        or f"date={dump_date}/sha1={upstream_sha1}/" not in location.key
    ):
        raise ValueError("dump object metadata/key do not bind an official sync result")
    reference = reference.model_copy(
        update={
            "created_at": _timestamp(head.get("LastModified")),
            "attributes": {
                "compression": "bzip2",
                "dumpDate": dump_date,
                "sourceUrl": source_url,
                "upstreamSha1": upstream_sha1,
            },
        }
    )
    return reference, dump_date


def _normalization_locations(
    staging_prefix: S3Location,
    dump: ObjectRef,
) -> tuple[S3Location, S3Location, str]:
    identity = sha256_digest(
        canonical_json(
            {
                "algorithmId": NORMALIZATION_ALGORITHM_ID,
                "dump": dump.model_dump(
                    mode="json",
                    by_alias=True,
                    exclude_none=True,
                ),
            }
        )
    ).removeprefix("sha256:")
    root = _join(
        staging_prefix,
        f"dump-sha256={dump.checksum.value}",
        f"normalization-sha256={identity}",
    )
    data = _join(root, "data")
    marker = _join(root, "_normalized-manifest.json")
    return data, marker, root.uri


def _load_or_build_staging(
    *,
    spark: Any,
    s3: Any,
    store: BoundedObjectStore,
    dump: ObjectRef,
    data: S3Location,
    marker: S3Location,
) -> Any:
    existing = _read_small_json(s3, marker)
    if existing is not None:
        marker_head = s3.head_object(
            Bucket=marker.bucket,
            Key=marker.key,
            ChecksumMode="ENABLED",
        )
        if not marker_head.get("VersionId"):
            raise ValueError("normalized staging marker requires S3 VersionId")
        manifest = NormalizedStagingManifest.model_validate_json(existing)
        if manifest.dump != dump or manifest.data_uri != data.uri:
            raise ValueError("normalized staging marker conflicts with current dump")
        normalized = spark.read.parquet(_spark_uri(data.uri)).persist()
        if normalized.count() != manifest.row_count:
            normalized.unpersist()
            raise ValueError("normalized staging row count differs from commit marker")
        # Older staging may still contain dump property entities (P*).
        return item_entity_rows(normalized)

    normalized = normalize_dump(spark, _spark_uri(dump.uri))
    row_count = write_normalized_staging(normalized, _spark_uri(data.uri))
    manifest = NormalizedStagingManifest(
        dump=dump,
        data_uri=data.uri,
        row_count=row_count,
    )
    marker_ref = store.upload_bytes(
        manifest.json_bytes(),
        marker.uri,
        media_type="application/json",
        object_format="OBJECT_FORMAT_JSON",
        max_bytes=MAX_CONTROL_BYTES,
    ).object_ref
    _require_versioned(marker_ref, "normalized staging marker")
    return item_entity_rows(normalized)


def _hash_s3_object(
    s3: Any,
    location: S3Location,
    *,
    version: str | None,
    max_bytes: int,
) -> tuple[str, int]:
    request: dict[str, Any] = {
        "Bucket": location.bucket,
        "Key": location.key,
        "ChecksumMode": "ENABLED",
    }
    if version:
        request["VersionId"] = version
    response = s3.get_object(**request)
    body = response.get("Body")
    if body is None or not hasattr(body, "read"):
        raise RuntimeError("S3 data object has no readable body")
    digest = hashlib.sha256()
    size = 0
    prefix = bytearray()
    try:
        declared = int(response.get("ContentLength", -1))
        if declared < 1 or declared > max_bytes:
            raise ObjectStoreError(
                "OBJECT_TOO_LARGE", "subset output exceeds configured size limit"
            )
        while chunk := body.read(S3_STREAM_CHUNK_BYTES):
            size += len(chunk)
            if size > max_bytes:
                raise ObjectStoreError(
                    "OBJECT_TOO_LARGE", "subset output exceeds configured size limit"
                )
            if len(prefix) < 3:
                prefix.extend(chunk[: 3 - len(prefix)])
            digest.update(chunk)
    finally:
        body.close()
    if size != declared:
        raise ValueError("S3 object size differs from Content-Length")
    if bytes(prefix) != b"BZh":
        raise ValueError("subset output is not a bzip2 object")
    return digest.hexdigest(), size


def _output_ref(
    location: S3Location,
    head: Mapping[str, Any],
    *,
    sha256: str,
    size: int,
    algorithm_id: str = SUBSET_ALGORITHM_ID,
) -> ObjectRef:
    etag = _etag(head.get("ETag"))
    version = head.get("VersionId")
    if etag is None or not isinstance(version, str) or not version or version == "null":
        raise ValueError("subset bucket must return complete ETag and VersionId")
    return ObjectRef(
        uri=location.uri,
        format="OBJECT_FORMAT_JSON",
        media_type="application/x-bzip2",
        checksum=Checksum(value=sha256),
        size_bytes=size,
        etag=etag,
        object_version=version,
        created_at=_timestamp(head.get("LastModified")),
        attributes={
            "algorithmId": algorithm_id,
            "compression": "bzip2",
        },
    )


def _verify_existing_output(
    s3: Any,
    location: S3Location,
    *,
    sha256: str,
    size: int,
    config_digest: str,
    dump_sha256: str,
    max_bytes: int,
    algorithm_id: str = SUBSET_ALGORITHM_ID,
) -> ObjectRef | None:
    try:
        head = s3.head_object(
            Bucket=location.bucket,
            Key=location.key,
            ChecksumMode="ENABLED",
        )
    except Exception as exc:
        if _is_missing(exc):
            return None
        raise
    metadata = {
        str(key).lower(): str(value)
        for key, value in (head.get("Metadata") or {}).items()
    }
    if (
        int(head.get("ContentLength", -1)) != size
        or metadata.get("sha256") != sha256
        or metadata.get("config-digest") != config_digest
        or metadata.get("dump-sha256") != dump_sha256
        or metadata.get("algorithm-id") != algorithm_id
    ):
        raise ObjectStoreError(
            "IMMUTABLE_OBJECT_CONFLICT",
            "subset destination exists with conflicting metadata",
        )
    actual_sha256, actual_size = _hash_s3_object(
        s3,
        location,
        version=head.get("VersionId"),
        max_bytes=max_bytes,
    )
    if actual_sha256 != sha256 or actual_size != size:
        raise ObjectStoreError(
            "IMMUTABLE_OBJECT_CONFLICT",
            "subset destination exists with conflicting content",
        )
    return _output_ref(
        location,
        head,
        sha256=sha256,
        size=size,
        algorithm_id=algorithm_id,
    )


def _publish_s3_part(
    *,
    s3: Any,
    source: S3Location,
    source_version: str | None,
    destination: S3Location,
    config_digest: str,
    dump_sha256: str,
    max_bytes: int,
    algorithm_id: str = SUBSET_ALGORITHM_ID,
) -> ObjectRef:
    sha256, size = _hash_s3_object(
        s3,
        source,
        version=source_version,
        max_bytes=max_bytes,
    )
    existing = _verify_existing_output(
        s3,
        destination,
        sha256=sha256,
        size=size,
        config_digest=config_digest,
        dump_sha256=dump_sha256,
        max_bytes=max_bytes,
        algorithm_id=algorithm_id,
    )
    if existing is not None:
        return existing

    request: dict[str, Any] = {
        "Bucket": source.bucket,
        "Key": source.key,
    }
    if source_version:
        request["VersionId"] = source_version
    response = s3.get_object(**request)
    body = response.get("Body")
    if body is None or not hasattr(body, "read"):
        raise RuntimeError("temporary subset part has no readable body")
    published_version: str | None = None
    try:
        try:
            published = s3.put_object(
                Bucket=destination.bucket,
                Key=destination.key,
                Body=body,
                ContentLength=size,
                ContentType="application/x-bzip2",
                Metadata={
                    "sha256": sha256,
                    "config-digest": config_digest,
                    "dump-sha256": dump_sha256,
                    "algorithm-id": algorithm_id,
                },
                ChecksumSHA256=base64.b64encode(bytes.fromhex(sha256)).decode("ascii"),
                IfNoneMatch="*",
            )
            published_version = published.get("VersionId")
        except Exception as exc:
            if not _is_precondition_failure(exc):
                raise
            existing = _verify_existing_output(
                s3,
                destination,
                sha256=sha256,
                size=size,
                config_digest=config_digest,
                dump_sha256=dump_sha256,
                max_bytes=max_bytes,
                algorithm_id=algorithm_id,
            )
            if existing is None:
                raise
            return existing
    finally:
        body.close()
    if (
        not isinstance(published_version, str)
        or not published_version
        or published_version == "null"
    ):
        raise ValueError("subset bucket did not return a VersionId")
    head = s3.head_object(
        Bucket=destination.bucket,
        Key=destination.key,
        VersionId=published_version,
        ChecksumMode="ENABLED",
    )
    metadata = {
        str(key).lower(): str(value)
        for key, value in (head.get("Metadata") or {}).items()
    }
    if int(head.get("ContentLength", -1)) != size or metadata.get("sha256") != sha256:
        raise RuntimeError("published subset failed metadata verification")
    return _output_ref(
        destination,
        head,
        sha256=sha256,
        size=size,
        algorithm_id=algorithm_id,
    )


def _temporary_part(
    s3: Any,
    temporary_prefix: S3Location,
) -> tuple[S3Location, str | None]:
    response = s3.list_objects_v2(
        Bucket=temporary_prefix.bucket,
        Prefix=temporary_prefix.key.rstrip("/") + "/",
        MaxKeys=MAX_TEMPORARY_KEYS + 1,
    )
    if response.get("IsTruncated"):
        raise RuntimeError("Spark temporary output contains too many objects")
    objects = response.get("Contents") or []
    if len(objects) > MAX_TEMPORARY_KEYS:
        raise RuntimeError("Spark temporary output contains too many objects")
    candidates = [
        str(item["Key"])
        for item in objects
        if str(item.get("Key", "")).rsplit("/", 1)[-1].startswith("part-")
        and str(item.get("Key", "")).endswith(".bz2")
    ]
    if len(candidates) != 1:
        raise RuntimeError("Spark must produce exactly one bzip2 part object")
    part = S3Location(temporary_prefix.bucket, candidates[0])
    head = s3.head_object(Bucket=part.bucket, Key=part.key)
    return part, head.get("VersionId")


def _delete_temporary_prefix(s3: Any, prefix: S3Location) -> None:
    continuation: str | None = None
    while True:
        request: dict[str, Any] = {
            "Bucket": prefix.bucket,
            "Prefix": prefix.key.rstrip("/") + "/",
            "MaxKeys": 1000,
        }
        if continuation:
            request["ContinuationToken"] = continuation
        response = s3.list_objects_v2(**request)
        objects: list[dict[str, str]] = []
        for item in response.get("Contents") or []:
            key = str(item["Key"])
            head = s3.head_object(Bucket=prefix.bucket, Key=key)
            entry = {"Key": key}
            if head.get("VersionId"):
                entry["VersionId"] = str(head["VersionId"])
            objects.append(entry)
        if objects:
            s3.delete_objects(
                Bucket=prefix.bucket,
                Delete={"Objects": objects, "Quiet": True},
            )
        if not response.get("IsTruncated"):
            break
        continuation = response.get("NextContinuationToken")
        if not continuation:
            raise RuntimeError("S3 temporary listing omitted continuation token")


def _require_versioned(reference: ObjectRef, name: str) -> None:
    if (
        reference.etag is None
        or reference.object_version is None
        or reference.object_version == "null"
    ):
        raise ValueError(f"{name} publication requires S3 ETag and VersionId")


def _existing_audit(
    s3: Any,
    location: S3Location,
    *,
    dump: ObjectRef,
    config: SubsetSelectionConfig,
) -> SubsetAuditManifest | None:
    payload = _read_small_json(s3, location)
    if payload is None:
        return None
    audit = SubsetAuditManifest.model_validate_json(payload)
    if (
        audit.dump != dump
        or audit.config_digest != config.digest
        or audit.target_count != config.target_count
        or audit.work_quotas != config.work_quotas
    ):
        raise ValueError("existing audit manifest conflicts with requested build")
    return audit


def _spark_session(parsed: argparse.Namespace) -> Any:
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(parsed.app_name)
    if parsed.master:
        builder = builder.master(parsed.master)
    if parsed.shuffle_partitions is not None:
        builder = builder.config(
            "spark.sql.shuffle.partitions",
            str(parsed.shuffle_partitions),
        )
    if parsed.spark_packages:
        builder = builder.config("spark.jars.packages", parsed.spark_packages)
    builder = configure_s3a_builder(
        builder,
        aws_region=parsed.aws_region,
        s3_endpoint=parsed.s3_endpoint,
        s3_path_style_access=parsed.s3_path_style_access,
    )
    return builder.getOrCreate()


def run(
    parsed: argparse.Namespace,
    *,
    s3: Any | None = None,
    spark: Any | None = None,
) -> SubsetAuditManifest:
    if parsed.max_dump_bytes < 1:
        raise ValueError("max-dump-bytes must be positive")
    if parsed.max_output_bytes < 1 or parsed.max_output_bytes > MAX_SUBSET_BYTES:
        raise ValueError("max-output-bytes must be between 1 and 4 GiB")
    if parsed.max_closure_iterations < 1:
        raise ValueError("max-closure-iterations must be positive")
    if parsed.shuffle_partitions is not None and parsed.shuffle_partitions < 1:
        raise ValueError("shuffle-partitions must be positive")
    config = SubsetSelectionConfig(
        target_count=parsed.target_count,
        work_quotas={
            "MOVIE": parsed.movie_count,
            "TV_SERIES": parsed.tv_series_count,
            "TV_SEASON": parsed.tv_season_count,
            "TV_EPISODE": parsed.tv_episode_count,
        },
    )
    client = s3 or _s3_client(parsed)
    store = BoundedObjectStore(
        region=parsed.aws_region,
        endpoint_url=parsed.s3_endpoint,
        path_style_access=parsed.s3_path_style_access,
        client=client,
    )
    dump, dump_date = _dump_reference(parsed, s3=client, store=store)
    output_prefix = _prefix(parsed.output_prefix)
    staging_prefix = _prefix(parsed.staging_prefix)
    build_root = _join(
        output_prefix,
        f"date={dump_date}",
        f"dump-sha256={dump.checksum.value}",
        f"config-sha256={config.digest.removeprefix('sha256:')}",
    )
    audit_location = _join(build_root, "audit-manifest.json")
    existing = _existing_audit(
        client,
        audit_location,
        dump=dump,
        config=config,
    )
    if existing is not None:
        store.verify(existing.subset, max_bytes=parsed.max_output_bytes)
        store.verify(existing.source_manifest, max_bytes=MAX_CONTROL_BYTES)
        return existing

    data_location, marker_location, staging_uri = _normalization_locations(
        staging_prefix,
        dump,
    )
    owns_spark = spark is None
    session = spark or _spark_session(parsed)
    temporary = _join(
        output_prefix,
        "_temporary",
        f"date={dump_date}",
        f"config-sha256={config.digest.removeprefix('sha256:')}",
        f"spark-output-{uuid.uuid4().hex}",
    )
    configure_bfs_materialize_dir(
        session,
        _spark_uri(f"{temporary.uri.rstrip('/')}/bfs-materialize"),
    )
    try:
        normalized = _load_or_build_staging(
            spark=session,
            s3=client,
            store=store,
            dump=dump,
            data=data_location,
            marker=marker_location,
        )
        built = build_subset(
            session,
            normalized,
            config,
            max_closure_iterations=parsed.max_closure_iterations,
        )
        (
            built.lines.write.mode("errorifexists")
            .option("compression", "bzip2")
            .text(_spark_uri(temporary.uri))
        )
        store.verify(dump, max_bytes=parsed.max_dump_bytes)
        part, part_version = _temporary_part(client, temporary)
        subset_location = _join(
            build_root,
            f"wikidata-{dump_date}-subset.json.bz2",
        )
        subset = _publish_s3_part(
            s3=client,
            source=part,
            source_version=part_version,
            destination=subset_location,
            config_digest=config.digest,
            dump_sha256=dump.checksum.value,
            max_bytes=parsed.max_output_bytes,
        )
        _require_versioned(subset, "subset")

        manifest_location = _join(build_root, "source-manifest.parquet")
        with tempfile.TemporaryDirectory(
            prefix="media-catalog-source-manifest-"
        ) as directory:
            manifest_path = Path(directory) / "source-manifest.parquet"
            write_source_manifest(
                manifest_path,
                [subset_source_manifest_entry(subset)],
            )
            source_manifest = store.upload_file(
                manifest_path,
                manifest_location.uri,
                media_type=SOURCE_MANIFEST_MEDIA_TYPE,
                object_format="OBJECT_FORMAT_PARQUET",
                max_bytes=MAX_CONTROL_BYTES,
            ).object_ref
        _require_versioned(source_manifest, "source manifest")

        audit = SubsetAuditManifest(
            config_digest=config.digest,
            dump=dump,
            subset=subset,
            source_manifest=source_manifest,
            target_count=config.target_count,
            work_quotas=config.work_quotas,
            selected_count=built.selection.selected_count,
            selected_counts=built.selection.counts_by_type,
            dependency_rows=built.dependency_rows,
            pruned_relation_statements=built.pruned_relation_statements,
            normalization_staging_uri=staging_uri,
        )
        audit_ref = store.upload_bytes(
            audit_json_bytes(audit),
            audit_location.uri,
            media_type="application/json",
            object_format="OBJECT_FORMAT_JSON",
            max_bytes=MAX_CONTROL_BYTES,
        ).object_ref
        _require_versioned(audit_ref, "audit manifest")
        return audit
    finally:
        with suppress(Exception):
            _delete_temporary_prefix(client, temporary)
        if owns_spark:
            session.stop()


def main(argv: Sequence[str] | None = None) -> int:
    audit = run(build_parser().parse_args(argv))
    print(
        canonical_json(audit.model_dump(mode="json", by_alias=True, exclude_none=True))
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
