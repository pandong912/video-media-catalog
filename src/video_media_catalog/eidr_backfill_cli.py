"""CLI boundaries for snapshot discovery and injected EIDR exact lookup."""

from __future__ import annotations

import argparse
import re
import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from video_media_catalog.canonical import canonical_json
from video_media_catalog.community_iceberg import CommunityCatalogTables
from video_media_catalog.community_snapshot import (
    CONTROL_MAX_BYTES,
    SILVER_SNAPSHOT_MEDIA_TYPE,
    CommunitySilverSnapshotSet,
)
from video_media_catalog.eidr_backfill import (
    BACKFILL_WATERMARK_MEDIA_TYPE,
    DEFAULT_DISCOVERED_PAGE_IDS,
    DEFAULT_LOOKUP_BATCH_IDS,
    DEFAULT_MAX_XML_BYTES,
    DISCOVERED_ID_MANIFEST_MEDIA_TYPE,
    EIDR_SOURCE_SEMAPHORE_PERMITS,
    MAX_DISCOVERED_PAGE_IDS,
    MAX_LOOKUP_BATCH_IDS,
    MAX_MAX_XML_BYTES,
    EidrCompleteFeedProof,
    EidrProvider,
    EidrProviderNotAuthorizedError,
    extract_and_publish_discovered_eidr_ids,
    read_discovered_eidr_id_manifest,
    read_eidr_backfill_watermark,
    run_eidr_exact_lookup_batch,
)
from video_media_catalog.iceberg import CatalogConfig
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.object_store import BoundedObjectStore, RuntimeObjectStore
from video_media_catalog.v2_contracts import require_sha256

DEFAULT_RECORD_SHARD_BYTES = 8 * 1024 * 1024
MAX_URI_LENGTH = 2_048


def _add_object_ref_args(
    parser: argparse.ArgumentParser,
    prefix: str,
    *,
    required: bool,
) -> None:
    dashed = prefix.replace("_", "-")
    parser.add_argument(f"--{dashed}-uri", required=required, default=None)
    parser.add_argument(f"--{dashed}-hash", required=required, default=None)
    parser.add_argument(
        f"--{dashed}-size",
        required=required,
        type=int,
        default=None,
    )
    parser.add_argument(f"--{dashed}-version", default="")
    parser.add_argument(f"--{dashed}-etag", default="")


def _add_store_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--aws-region")
    parser.add_argument("--s3-endpoint")
    parser.add_argument("--s3-path-style-access", action="store_true")


def _add_catalog_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--catalog-name", default="media")
    parser.add_argument("--namespace", default="video_media_catalog")
    parser.add_argument("--catalog-type", choices=("hadoop", "glue"), default="glue")
    parser.add_argument("--warehouse", required=True)
    parser.add_argument(
        "--s3-credentials-provider",
        choices=("web-identity", "default"),
        default="web-identity",
    )
    parser.add_argument("--master")
    parser.add_argument("--app-name", default="media-catalog-eidr-discovery")
    parser.add_argument("--shuffle-partitions", type=int)
    parser.add_argument("--spark-packages")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="video-media-catalog-eidr-backfill",
        description=(
            "Extract pinned EIDR identifiers or run one authorized exact-ID batch. "
            "No search, crawl, or default network provider is included. "
            f"Exact lookup is source-semaphore={EIDR_SOURCE_SEMAPHORE_PERMITS}."
        ),
    )
    commands = parser.add_subparsers(dest="command", required=True)

    extraction = commands.add_parser(
        "extract-ids",
        help="publish a discovered-ID manifest from a pinned Silver epoch",
    )
    _add_object_ref_args(extraction, "silver_snapshot", required=True)
    extraction.add_argument("--source-release-id", required=True)
    extraction.add_argument("--destination-prefix", required=True)
    extraction.add_argument("--created-at", required=True)
    extraction.add_argument(
        "--page-size",
        type=int,
        default=DEFAULT_DISCOVERED_PAGE_IDS,
    )
    _add_store_args(extraction)
    _add_catalog_args(extraction)

    lookup = commands.add_parser(
        "lookup-batch",
        help=(
            "run one bounded exact-ID batch with an injected authorized provider; "
            f"source semaphore={EIDR_SOURCE_SEMAPHORE_PERMITS}"
        ),
    )
    _add_object_ref_args(lookup, "manifest", required=True)
    _add_object_ref_args(lookup, "watermark", required=False)
    lookup.add_argument("--destination-prefix", required=True)
    lookup.add_argument("--acquired-at", required=True)
    lookup.add_argument("--image-digest", required=True)
    lookup.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_LOOKUP_BATCH_IDS,
    )
    lookup.add_argument(
        "--max-xml-bytes",
        type=int,
        default=DEFAULT_MAX_XML_BYTES,
    )
    lookup.add_argument(
        "--record-shard-bytes",
        type=int,
        default=DEFAULT_RECORD_SHARD_BYTES,
    )
    _add_store_args(lookup)
    return parser


def _validate_uri(value: str, *, label: str) -> str:
    normalized = value.strip()
    parsed = urlsplit(normalized)
    if (
        len(normalized) > MAX_URI_LENGTH
        or parsed.scheme not in {"file", "s3"}
        or parsed.query
        or parsed.fragment
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise ValueError(f"{label} must be a bounded file:// or s3:// URI")
    if parsed.scheme == "s3" and (not parsed.netloc or not parsed.path.lstrip("/")):
        raise ValueError(f"{label} must identify an S3 object")
    if parsed.scheme == "file" and not parsed.path:
        raise ValueError(f"{label} must identify a local object")
    return normalized


def _object_ref(
    parsed: argparse.Namespace,
    prefix: str,
    *,
    media_type: str,
    required: bool = True,
) -> ObjectRef | None:
    uri_value = getattr(parsed, f"{prefix}_uri")
    digest_value = getattr(parsed, f"{prefix}_hash")
    size_value = getattr(parsed, f"{prefix}_size")
    version_value = str(getattr(parsed, f"{prefix}_version") or "").strip()
    etag_value = str(getattr(parsed, f"{prefix}_etag") or "").strip().strip('"')
    supplied = [
        uri_value is not None,
        digest_value is not None,
        size_value is not None,
        bool(version_value),
        bool(etag_value),
    ]
    if not any(supplied):
        if required:
            raise ValueError(f"{prefix} ObjectRef is required")
        return None
    if uri_value is None or digest_value is None or size_value is None:
        raise ValueError(f"{prefix} URI, hash, and size must be supplied together")
    uri = _validate_uri(str(uri_value), label=f"{prefix} URI")
    match = re.fullmatch(
        r"(?:sha256:hex:|sha256:)?([0-9a-fA-F]{64})",
        str(digest_value).strip(),
    )
    if match is None:
        raise ValueError(f"{prefix} hash must contain 64 SHA-256 hex digits")
    size = int(size_value)
    if not 0 < size <= CONTROL_MAX_BYTES:
        raise ValueError(f"{prefix} size must be between 1 byte and 16 MiB")
    scheme = urlsplit(uri).scheme
    if scheme == "s3" and (not version_value or not etag_value):
        raise ValueError(f"S3 {prefix} requires version and ETag")
    if scheme == "file" and (version_value or etag_value):
        raise ValueError(f"file {prefix} cannot declare S3 metadata")
    return ObjectRef(
        uri=uri,
        format="OBJECT_FORMAT_JSON",
        media_type=media_type,
        checksum=Checksum(value=match.group(1).lower()),
        size_bytes=size,
        etag=etag_value or None,
        object_version=version_value or None,
    )


def _output_prefix(value: str) -> str:
    normalized = _validate_uri(value, label="destination prefix")
    return normalized.rstrip("/")


def _runtime_store(
    parsed: argparse.Namespace,
    *,
    input_refs: Sequence[ObjectRef],
    destination_prefix: str,
) -> RuntimeObjectStore:
    local = urlsplit(destination_prefix).scheme == "file" and all(
        urlsplit(reference.uri).scheme == "file" for reference in input_refs
    )
    return BoundedObjectStore(
        region=parsed.aws_region,
        endpoint_url=parsed.s3_endpoint,
        path_style_access=parsed.s3_path_style_access,
        client=object() if local else None,
    )


def _read_silver_snapshot(
    reference: ObjectRef,
    *,
    store: RuntimeObjectStore,
) -> CommunitySilverSnapshotSet:
    store.verify(reference, max_bytes=CONTROL_MAX_BYTES)
    with tempfile.TemporaryDirectory(prefix="eidr-silver-snapshot-") as directory:
        materialized = store.download(
            reference,
            Path(directory) / "snapshot.json",
            max_bytes=CONTROL_MAX_BYTES,
        )
        return CommunitySilverSnapshotSet.model_validate_json(
            materialized.path.read_bytes()
        )


def _catalog_config(parsed: argparse.Namespace) -> CatalogConfig:
    return CatalogConfig(
        catalog_name=parsed.catalog_name,
        namespace=parsed.namespace,
        warehouse=parsed.warehouse,
        catalog_type=parsed.catalog_type,
        aws_region=parsed.aws_region,
        s3_endpoint=parsed.s3_endpoint,
        s3_path_style_access=parsed.s3_path_style_access,
        s3_credentials_provider=parsed.s3_credentials_provider,
    )


def _spark_session(parsed: argparse.Namespace, config: CatalogConfig) -> Any:
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(parsed.app_name)
    if parsed.master:
        builder = builder.master(parsed.master)
    builder = config.configure_builder(builder)
    if parsed.shuffle_partitions is not None:
        if not 0 < parsed.shuffle_partitions <= 100_000:
            raise ValueError("shuffle-partitions must be between 1 and 100000")
        builder = builder.config(
            "spark.sql.shuffle.partitions",
            str(parsed.shuffle_partitions),
        )
    if parsed.spark_packages:
        builder = builder.config("spark.jars.packages", parsed.spark_packages)
    return builder.getOrCreate()


def _run_extract(
    parsed: argparse.Namespace,
    *,
    store: RuntimeObjectStore | None,
    spark: Any | None,
) -> dict[str, Any]:
    if not 0 < parsed.page_size <= MAX_DISCOVERED_PAGE_IDS:
        raise ValueError("page-size is outside the supported bound")
    source_release_id = require_sha256(
        parsed.source_release_id,
        label="source_release_id",
    )
    destination = _output_prefix(parsed.destination_prefix)
    reference = _object_ref(
        parsed,
        "silver_snapshot",
        media_type=SILVER_SNAPSHOT_MEDIA_TYPE,
    )
    assert reference is not None
    runtime_store = store or _runtime_store(
        parsed,
        input_refs=(reference,),
        destination_prefix=destination,
    )
    snapshot = _read_silver_snapshot(reference, store=runtime_store)
    snapshot_id = snapshot.data_snapshot_ids["community_identifier_assertion"]
    if snapshot_id is None:
        raise ValueError("pinned Silver epoch has no IdentifierAssertion snapshot")
    config = _catalog_config(parsed)
    session = spark or _spark_session(parsed, config)
    owns_session = spark is None
    try:
        tables = CommunityCatalogTables(session, config)
        assertions = (
            session.read.format("iceberg")
            .option("snapshot-id", str(snapshot_id))
            .load(tables.table_name("community_identifier_assertion"))
        )
        published = extract_and_publish_discovered_eidr_ids(
            identifier_assertions=assertions,
            silver_snapshot=snapshot,
            silver_snapshot_object=reference,
            source_release_id=source_release_id,
            destination_prefix=destination,
            created_at=parsed.created_at,
            store=runtime_store,
            page_size=parsed.page_size,
        )
        return {
            "manifestId": published.manifest.manifest_id,
            "manifest": published.manifest_object.model_dump(
                mode="json",
                by_alias=True,
                exclude_none=True,
            ),
            "idCount": published.manifest.id_count,
            "pageCount": published.manifest.page_count,
            "sourceBindingId": published.manifest.source.binding_id,
        }
    finally:
        if owns_session:
            session.stop()


def _run_lookup(
    parsed: argparse.Namespace,
    *,
    provider: EidrProvider | None,
    complete_feed_proof: EidrCompleteFeedProof | None,
    store: RuntimeObjectStore | None,
) -> dict[str, Any]:
    if provider is None:
        raise EidrProviderNotAuthorizedError(
            "lookup-batch requires an injected authorized EIDR provider"
        )
    if not 0 < parsed.batch_size <= MAX_LOOKUP_BATCH_IDS:
        raise ValueError("batch-size is outside the supported bound")
    if not 0 < parsed.max_xml_bytes <= MAX_MAX_XML_BYTES:
        raise ValueError("max-xml-bytes is outside the supported bound")
    if parsed.record_shard_bytes < 1:
        raise ValueError("record-shard-bytes must be positive")
    destination = _output_prefix(parsed.destination_prefix)
    manifest_ref = _object_ref(
        parsed,
        "manifest",
        media_type=DISCOVERED_ID_MANIFEST_MEDIA_TYPE,
    )
    assert manifest_ref is not None
    watermark_ref = _object_ref(
        parsed,
        "watermark",
        media_type=BACKFILL_WATERMARK_MEDIA_TYPE,
        required=False,
    )
    refs = (manifest_ref,) if watermark_ref is None else (manifest_ref, watermark_ref)
    runtime_store = store or _runtime_store(
        parsed,
        input_refs=refs,
        destination_prefix=destination,
    )
    manifest = read_discovered_eidr_id_manifest(
        reference=manifest_ref,
        store=runtime_store,
    )
    watermark = (
        None
        if watermark_ref is None
        else read_eidr_backfill_watermark(
            reference=watermark_ref,
            store=runtime_store,
        )
    )
    result = run_eidr_exact_lookup_batch(
        manifest=manifest,
        manifest_object=manifest_ref,
        destination_prefix=destination,
        acquired_at=parsed.acquired_at,
        image_digest=parsed.image_digest,
        store=runtime_store,
        provider=provider,
        watermark=watermark,
        watermark_object=watermark_ref,
        batch_size=parsed.batch_size,
        max_xml_bytes=parsed.max_xml_bytes,
        record_shard_bytes=parsed.record_shard_bytes,
        complete_feed_proof=complete_feed_proof,
    )
    response: dict[str, Any] = {"completed": result.completed}
    if result.lookup_batch is not None and result.lookup_batch_object is not None:
        response["lookupBatchId"] = result.lookup_batch.batch_id
        response["lookupBatch"] = result.lookup_batch_object.model_dump(
            mode="json",
            by_alias=True,
            exclude_none=True,
        )
    if result.receipt is not None and result.receipt_object is not None:
        response["receiptId"] = result.receipt.receipt_id
        response["receipt"] = result.receipt_object.model_dump(
            mode="json",
            by_alias=True,
            exclude_none=True,
        )
        response["retryCount"] = result.receipt.retry_count
        response["rateLimitCount"] = result.receipt.rate_limit_count
    if result.watermark is not None and result.watermark_object is not None:
        response["watermarkId"] = result.watermark.watermark_id
        response["watermark"] = result.watermark_object.model_dump(
            mode="json",
            by_alias=True,
            exclude_none=True,
        )
        response["nextOrdinal"] = result.watermark.next_ordinal
    if result.capture is not None:
        response["connectorBatchId"] = result.capture.batch_manifest.batch_id
        response["recordSetId"] = result.capture.record_set_manifest.record_set_id
    return response


def run(
    parsed: argparse.Namespace,
    *,
    provider: EidrProvider | None = None,
    complete_feed_proof: EidrCompleteFeedProof | None = None,
    store: RuntimeObjectStore | None = None,
    spark: Any | None = None,
) -> dict[str, Any]:
    if parsed.command == "extract-ids":
        if provider is not None or complete_feed_proof is not None:
            raise ValueError("extract-ids does not accept provider injection")
        return _run_extract(parsed, store=store, spark=spark)
    if spark is not None:
        raise ValueError("lookup-batch does not accept a Spark session")
    return _run_lookup(
        parsed,
        provider=provider,
        complete_feed_proof=complete_feed_proof,
        store=store,
    )


def main(
    argv: Sequence[str] | None = None,
    *,
    provider: EidrProvider | None = None,
    complete_feed_proof: EidrCompleteFeedProof | None = None,
) -> int:
    parsed = build_parser().parse_args(argv)
    print(
        canonical_json(
            run(
                parsed,
                provider=provider,
                complete_feed_proof=complete_feed_proof,
            )
        )
    )
    return 0


if __name__ == "__main__":
    main()
