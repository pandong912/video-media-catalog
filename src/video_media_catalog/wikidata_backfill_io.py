"""S3 and Spark I/O shared by Wikidata full backfills."""

from __future__ import annotations

import argparse
import re
from typing import Any
from urllib.parse import urlsplit

from video_media_catalog.canonical import canonical_json, sha256_digest
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.object_store import (
    BoundedObjectStore,
    S3Location,
    _etag,
    _timestamp,
)
from video_media_catalog.spark_config import configure_s3a_builder
from video_media_catalog.wikidata_spark import (
    NORMALIZATION_ALGORITHM_ID,
    NormalizedStagingManifest,
    item_entity_rows,
    normalize_dump,
    write_normalized_staging,
)
from video_media_catalog.wikidata_sync import validate_official_dump_url

MAX_CONTROL_BYTES = 16 * 1024**2
_DUMP_NAME = re.compile(r"^wikidata-([0-9]{8})-all\.json\.bz2$")
_SHA1 = re.compile(r"^[0-9a-f]{40}$")


def s3_client(parsed: argparse.Namespace) -> Any:
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


def parse_s3_prefix(uri: str) -> S3Location:
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


def join_s3(prefix: S3Location, *parts: str) -> S3Location:
    base = prefix.key.rsplit("/", 1)[0]
    key = "/".join(
        [
            *(segment for segment in [base] if segment),
            *(part.strip("/") for part in parts if part.strip("/")),
        ]
    )
    return S3Location(prefix.bucket, key)


def spark_uri(uri: str) -> str:
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


def dump_reference(
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
    return (
        reference.model_copy(
            update={
                "created_at": _timestamp(head.get("LastModified")),
                "attributes": {
                    "compression": "bzip2",
                    "dumpDate": dump_date,
                    "sourceUrl": source_url,
                    "upstreamSha1": upstream_sha1,
                },
            }
        ),
        dump_date,
    )


def normalization_locations(
    staging_prefix: S3Location,
    dump: ObjectRef,
) -> tuple[S3Location, S3Location]:
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
    root = join_s3(
        staging_prefix,
        f"dump-sha256={dump.checksum.value}",
        f"normalization-sha256={identity}",
    )
    return join_s3(root, "data"), join_s3(root, "_normalized-manifest.json")


def _require_versioned(reference: ObjectRef, name: str) -> None:
    if (
        reference.etag is None
        or reference.object_version is None
        or reference.object_version == "null"
    ):
        raise ValueError(f"{name} publication requires S3 ETag and VersionId")


def load_or_build_normalized_staging(
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
        normalized = spark.read.parquet(spark_uri(data.uri)).persist()
        if normalized.count() != manifest.row_count:
            normalized.unpersist()
            raise ValueError("normalized staging row count differs from commit marker")
        return item_entity_rows(normalized)

    normalized = normalize_dump(spark, spark_uri(dump.uri))
    row_count = write_normalized_staging(normalized, spark_uri(data.uri))
    marker_ref = store.upload_bytes(
        NormalizedStagingManifest(
            dump=dump,
            data_uri=data.uri,
            row_count=row_count,
        ).json_bytes(),
        marker.uri,
        media_type="application/json",
        object_format="OBJECT_FORMAT_JSON",
        max_bytes=MAX_CONTROL_BYTES,
    ).object_ref
    _require_versioned(marker_ref, "normalized staging marker")
    return item_entity_rows(normalized)


def spark_session(parsed: argparse.Namespace) -> Any:
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
        credentials_provider=parsed.s3_credentials_provider,
    )
    return builder.getOrCreate()
