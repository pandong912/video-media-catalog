"""Shared landing control validation and Spark loading for batch stages."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

from video_media_catalog.canonical import sha256_digest
from video_media_catalog.commit import MAX_CONTROL_BYTES
from video_media_catalog.landing import validate_landing_manifest
from video_media_catalog.models import (
    Checksum,
    LandingManifest,
    LandingSummary,
    ObjectRef,
)
from video_media_catalog.object_store import BoundedObjectStore
from video_media_catalog.runtime_args import RuntimeArguments, join_uri
from video_media_catalog.storage import digest_file, local_path


@dataclass(frozen=True)
class LandingInput:
    manifest_uri: str
    manifest_digest: str
    manifest_size: int
    manifest: LandingManifest
    summary: LandingSummary

    @property
    def summary_uri(self) -> str:
        return join_uri(self.manifest_uri.rsplit("/", 1)[0], "landing-summary.json")

    @property
    def shard_uris(self) -> list[str]:
        return [spark_uri(shard.uri) for shard in self.manifest.shards]


def runtime_arguments(parsed: Any) -> RuntimeArguments:
    return RuntimeArguments.model_validate(
        {field: getattr(parsed, field) for field in RuntimeArguments.model_fields}
    )


def _read_s3_bytes(
    uri: str,
    *,
    region: str | None,
    endpoint_url: str | None,
    path_style_access: bool,
) -> bytes:
    import boto3
    from botocore.config import Config

    parsed = urlparse(uri)
    response = boto3.client(
        "s3",
        region_name=region,
        endpoint_url=endpoint_url,
        config=Config(
            signature_version="s3v4",
            retries={"max_attempts": 5, "mode": "standard"},
            s3={"addressing_style": ("path" if path_style_access else "virtual")},
        ),
    ).get_object(
        Bucket=parsed.netloc,
        Key=parsed.path.lstrip("/"),
        ChecksumMode="ENABLED",
    )
    body = response.get("Body")
    if body is None or not hasattr(body, "read"):
        raise ValueError("landing control object has no readable body")
    length = int(response.get("ContentLength", -1))
    if length < 0 or length > MAX_CONTROL_BYTES:
        body.close()
        raise ValueError("landing control object exceeds maximum size")
    try:
        payload = body.read(MAX_CONTROL_BYTES + 1)
    finally:
        body.close()
    if len(payload) > MAX_CONTROL_BYTES or len(payload) != length:
        raise ValueError("landing control object has invalid bounded size")
    return payload


def _read_control_bytes(
    uri: str,
    *,
    region: str | None,
    endpoint_url: str | None,
    path_style_access: bool,
) -> bytes:
    if urlparse(uri).scheme == "s3":
        return _read_s3_bytes(
            uri,
            region=region,
            endpoint_url=endpoint_url,
            path_style_access=path_style_access,
        )
    path = local_path(uri).resolve()
    if path.stat().st_size > MAX_CONTROL_BYTES:
        raise ValueError("landing control object exceeds maximum size")
    return path.read_bytes()


def load_landing_input(
    runtime: RuntimeArguments,
    *,
    landing_manifest_uri: str | None,
    aws_region: str | None,
    s3_endpoint: str | None,
    s3_path_style_access: bool,
    max_landing_shard_bytes: int,
) -> LandingInput:
    """Validate complete landing manifest, summary, and every shard ObjectRef."""

    if max_landing_shard_bytes < 1:
        raise ValueError("max-landing-shard-bytes must be positive")
    manifest_uri = landing_manifest_uri or join_uri(
        runtime.stage_prefix("media-catalog-extract"),
        "landing-manifest.json",
    )
    manifest_payload = _read_control_bytes(
        manifest_uri,
        region=aws_region,
        endpoint_url=s3_endpoint,
        path_style_access=s3_path_style_access,
    )
    manifest_digest = sha256_digest(manifest_payload)
    manifest = LandingManifest.model_validate_json(manifest_payload)
    validate_landing_manifest(manifest)
    summary_uri = join_uri(
        manifest_uri.rsplit("/", 1)[0],
        "landing-summary.json",
    )
    summary = LandingSummary.model_validate_json(
        _read_control_bytes(
            summary_uri,
            region=aws_region,
            endpoint_url=s3_endpoint,
            path_style_access=s3_path_style_access,
        )
    )
    if (
        summary.manifest_uri != manifest_uri
        or summary.manifest_checksum != manifest_digest
        or summary.manifest_id != manifest.manifest_id
        or summary.record_count != manifest.record_count
        or summary.shard_count != len(manifest.shards)
    ):
        raise ValueError("landing summary does not bind the landing manifest")
    expected_input_digest = f"sha256:hex:{runtime.manifest_hash}"
    if manifest.input_manifest_digest != expected_input_digest and (
        landing_manifest_uri is None or urlparse(manifest_uri).scheme == "s3"
    ):
        raise ValueError("landing manifest does not bind the JobSpec input manifest")

    object_store: BoundedObjectStore | None = None
    for shard in manifest.shards:
        reference = ObjectRef(
            uri=shard.uri,
            format="OBJECT_FORMAT_PARQUET",
            media_type="application/vnd.apache.parquet",
            checksum=Checksum(value=shard.checksum),
            size_bytes=shard.size_bytes,
            etag=shard.etag,
            object_version=shard.object_version,
        )
        if urlparse(shard.uri).scheme == "file":
            digest, size = digest_file(local_path(shard.uri))
            if (
                size > max_landing_shard_bytes
                or size != shard.size_bytes
                or digest != shard.checksum
            ):
                raise ValueError(
                    "local landing shard differs from its immutable declaration"
                )
        else:
            if object_store is None:
                object_store = BoundedObjectStore(
                    region=aws_region,
                    endpoint_url=s3_endpoint,
                    path_style_access=s3_path_style_access,
                )
            object_store.verify(reference, max_bytes=max_landing_shard_bytes)
    return LandingInput(
        manifest_uri=manifest_uri,
        manifest_digest=manifest_digest,
        manifest_size=len(manifest_payload),
        manifest=manifest,
        summary=summary,
    )


def spark_uri(uri: str) -> str:
    return "s3a://" + uri[len("s3://") :] if uri.startswith("s3://") else uri


def _empty_landing(spark: Any) -> Any:
    return spark.createDataFrame(
        [],
        """
        record_key STRING NOT NULL,
        source STRING NOT NULL,
        source_record_id STRING NOT NULL,
        source_revision STRING,
        modified STRING,
        source_hash STRING NOT NULL,
        payload_json STRING NOT NULL
        """,
    )


def load_landing_frame(spark: Any, landing_input: LandingInput) -> Any:
    landing = (
        spark.read.parquet(*landing_input.shard_uris)
        if landing_input.shard_uris
        else _empty_landing(spark)
    ).persist()
    actual_record_count = landing.count()
    if actual_record_count != landing_input.manifest.record_count:
        landing.unpersist()
        raise ValueError(
            "landing record count mismatch: "
            f"manifest={landing_input.manifest.record_count}, "
            f"actual={actual_record_count}"
        )
    return landing


def configure_s3a_builder(
    builder: Any,
    *,
    aws_region: str | None,
    s3_endpoint: str | None,
    s3_path_style_access: bool,
    credentials_provider: str = "web-identity",
) -> Any:
    providers = {
        "default": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        "web-identity": "com.amazonaws.auth.WebIdentityTokenCredentialsProvider",
    }
    try:
        provider = providers[credentials_provider]
    except KeyError as exc:
        raise ValueError(
            "credentials_provider must be 'default' or 'web-identity'"
        ) from exc
    builder = builder.config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        provider,
    )
    if aws_region:
        builder = builder.config(
            "spark.hadoop.fs.s3a.endpoint.region",
            aws_region,
        )
    if s3_endpoint:
        builder = builder.config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
    if s3_path_style_access:
        builder = builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
    return builder
