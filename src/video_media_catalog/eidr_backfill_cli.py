"""CLI boundaries for EIDR discovery, exact lookup, and Silver fan-out."""

from __future__ import annotations

import argparse
import re
import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from video_media_catalog.canonical import canonical_json, canonical_json_bytes
from video_media_catalog.community_iceberg import CommunityCatalogTables
from video_media_catalog.community_snapshot import (
    CONTROL_MAX_BYTES,
    SILVER_SNAPSHOT_MEDIA_TYPE,
    CommunitySilverSnapshotSet,
)
from video_media_catalog.eidr_backfill import (
    BACKFILL_RUN_MANIFEST_MEDIA_TYPE,
    BACKFILL_WATERMARK_MEDIA_TYPE,
    DEFAULT_BACKFILL_MAX_BATCHES,
    DEFAULT_BACKFILL_MAX_DURATION_SECONDS,
    DEFAULT_BACKFILL_MAX_IDS,
    DEFAULT_DISCOVERED_PAGE_IDS,
    DEFAULT_LOOKUP_BATCH_IDS,
    DEFAULT_MAX_XML_BYTES,
    DISCOVERED_ID_MANIFEST_MEDIA_TYPE,
    EIDR_SOURCE_SEMAPHORE_PERMITS,
    MAX_BACKFILL_MAX_BATCHES,
    MAX_BACKFILL_MAX_DURATION_SECONDS,
    MAX_BACKFILL_MAX_IDS,
    MAX_DISCOVERED_PAGE_IDS,
    MAX_LOOKUP_BATCH_IDS,
    MAX_MAX_XML_BYTES,
    MAX_RECORD_SHARD_BYTES,
    EidrCompleteFeedProof,
    EidrProvider,
    EidrProviderNotAuthorizedError,
    expand_eidr_source_silver_inputs,
    extract_and_publish_discovered_eidr_ids,
    read_discovered_eidr_id_manifest,
    read_eidr_backfill_run_manifest,
    read_eidr_backfill_watermark,
    run_eidr_exact_lookup_batch,
    run_eidr_exact_lookup_manifest,
    verify_eidr_backfill_run_manifest_objects,
)
from video_media_catalog.eidr_public_provider import (
    DEFAULT_EIDR_PUBLIC_USER_AGENT,
    DEFAULT_MAX_ATTEMPTS,
    DEFAULT_MAX_RETRY_AFTER_SECONDS,
    DEFAULT_MINIMUM_REQUEST_INTERVAL_SECONDS,
    DEFAULT_REQUEST_TIMEOUT_SECONDS,
    DEFAULT_RETRY_INITIAL_BACKOFF_SECONDS,
    DEFAULT_RETRY_MAX_BACKOFF_SECONDS,
    EidrPublicProvider,
)
from video_media_catalog.iceberg import CatalogConfig
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.object_store import BoundedObjectStore, RuntimeObjectStore
from video_media_catalog.v2_contracts import require_sha256

DEFAULT_RECORD_SHARD_BYTES = 8 * 1024 * 1024
MAX_URI_LENGTH = 2_048
EIDR_AUTHORIZATION_EVIDENCE_MEDIA_TYPE = "application/json"
DEFAULT_MAX_SOURCE_SILVER_INPUTS = DEFAULT_BACKFILL_MAX_BATCHES
MAX_SOURCE_SILVER_INPUTS = MAX_BACKFILL_MAX_BATCHES
DEFAULT_MAX_SOURCE_SILVER_OUTPUT_BYTES = 1024 * 1024
MAX_SOURCE_SILVER_OUTPUT_BYTES = CONTROL_MAX_BYTES


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


def _add_public_provider_args(parser: argparse.ArgumentParser) -> None:
    _add_object_ref_args(parser, "authorization_evidence", required=False)
    parser.add_argument("--authorization-issued-at")
    parser.add_argument("--user-agent", default=DEFAULT_EIDR_PUBLIC_USER_AGENT)
    parser.add_argument(
        "--request-timeout-seconds",
        type=float,
        default=DEFAULT_REQUEST_TIMEOUT_SECONDS,
    )
    parser.add_argument(
        "--minimum-request-interval-seconds",
        type=float,
        default=DEFAULT_MINIMUM_REQUEST_INTERVAL_SECONDS,
    )
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=DEFAULT_MAX_ATTEMPTS,
    )
    parser.add_argument(
        "--retry-initial-backoff-seconds",
        type=float,
        default=DEFAULT_RETRY_INITIAL_BACKOFF_SECONDS,
    )
    parser.add_argument(
        "--retry-max-backoff-seconds",
        type=float,
        default=DEFAULT_RETRY_MAX_BACKOFF_SECONDS,
    )
    parser.add_argument(
        "--max-retry-after-seconds",
        type=float,
        default=DEFAULT_MAX_RETRY_AFTER_SECONDS,
    )


def _add_lookup_args(parser: argparse.ArgumentParser) -> None:
    _add_object_ref_args(parser, "manifest", required=True)
    _add_object_ref_args(parser, "watermark", required=False)
    parser.add_argument("--destination-prefix", required=True)
    parser.add_argument("--acquired-at", required=True)
    parser.add_argument("--image-digest", required=True)
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_LOOKUP_BATCH_IDS,
    )
    parser.add_argument(
        "--max-xml-bytes",
        type=int,
        default=DEFAULT_MAX_XML_BYTES,
    )
    parser.add_argument(
        "--record-shard-bytes",
        type=int,
        default=DEFAULT_RECORD_SHARD_BYTES,
    )
    _add_public_provider_args(parser)
    _add_store_args(parser)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="video-media-catalog-eidr-backfill",
        description=(
            "Extract pinned EIDR identifiers or run bounded anonymous exact-ID "
            "resolution. No title search, crawl, credentials, or complete-feed "
            "semantics are provided. "
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
            "run one bounded exact-ID batch with a public or injected provider; "
            f"source semaphore={EIDR_SOURCE_SEMAPHORE_PERMITS}"
        ),
    )
    _add_lookup_args(lookup)

    manifest_lookup = commands.add_parser(
        "lookup-manifest",
        help=(
            "run repeated partial exact-ID batches and publish one aggregate "
            "Source Silver fan-out manifest"
        ),
    )
    _add_lookup_args(manifest_lookup)
    manifest_lookup.add_argument(
        "--max-batches",
        type=int,
        default=DEFAULT_BACKFILL_MAX_BATCHES,
    )
    manifest_lookup.add_argument(
        "--max-duration-seconds",
        type=float,
        default=DEFAULT_BACKFILL_MAX_DURATION_SECONDS,
    )
    manifest_lookup.add_argument(
        "--max-ids",
        type=int,
        default=DEFAULT_BACKFILL_MAX_IDS,
    )

    expansion = commands.add_parser(
        "expand-source-silver-inputs",
        help=(
            "verify a pinned EIDR run manifest and emit bounded Connector "
            "batch/record-set ObjectRef pairs for Argo withParam"
        ),
    )
    _add_object_ref_args(expansion, "run_manifest", required=True)
    expansion.add_argument(
        "--allow-partial-run",
        action="store_true",
        help="explicitly permit an incomplete run; disabled by default",
    )
    expansion.add_argument(
        "--max-inputs",
        type=int,
        default=DEFAULT_MAX_SOURCE_SILVER_INPUTS,
    )
    expansion.add_argument(
        "--max-output-bytes",
        type=int,
        default=DEFAULT_MAX_SOURCE_SILVER_OUTPUT_BYTES,
    )
    _add_store_args(expansion)
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


def _resolve_lookup_provider(
    parsed: argparse.Namespace,
    *,
    injected: EidrProvider | None,
) -> EidrProvider:
    evidence_supplied = any(
        (
            parsed.authorization_evidence_uri is not None,
            parsed.authorization_evidence_hash is not None,
            parsed.authorization_evidence_size is not None,
            bool(str(parsed.authorization_evidence_version or "").strip()),
            bool(str(parsed.authorization_evidence_etag or "").strip()),
            bool(str(parsed.authorization_issued_at or "").strip()),
        )
    )
    if injected is not None:
        if evidence_supplied:
            raise ValueError(
                "injected provider cannot be combined with public provider evidence"
            )
        return injected
    if not evidence_supplied:
        raise EidrProviderNotAuthorizedError(
            "lookup command requires an injected provider or pinned public "
            "authorization evidence"
        )
    evidence = _object_ref(
        parsed,
        "authorization_evidence",
        media_type=EIDR_AUTHORIZATION_EVIDENCE_MEDIA_TYPE,
        required=True,
    )
    assert evidence is not None
    issued_at = str(parsed.authorization_issued_at or "").strip()
    if not issued_at:
        raise ValueError("public provider requires --authorization-issued-at")
    return EidrPublicProvider(
        authorization_object=evidence,
        authorization_issued_at=issued_at,
        user_agent=parsed.user_agent,
        request_timeout_seconds=parsed.request_timeout_seconds,
        minimum_request_interval_seconds=(parsed.minimum_request_interval_seconds),
        max_attempts=parsed.max_attempts,
        max_xml_bytes=parsed.max_xml_bytes,
        retry_initial_backoff_seconds=parsed.retry_initial_backoff_seconds,
        retry_max_backoff_seconds=parsed.retry_max_backoff_seconds,
        max_retry_after_seconds=parsed.max_retry_after_seconds,
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


def _input_store(
    parsed: argparse.Namespace,
    *,
    input_refs: Sequence[ObjectRef],
) -> RuntimeObjectStore:
    local = all(urlsplit(reference.uri).scheme == "file" for reference in input_refs)
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


def _run_expand_source_silver_inputs(
    parsed: argparse.Namespace,
    *,
    store: RuntimeObjectStore | None,
) -> list[dict[str, Any]]:
    if not 0 < parsed.max_inputs <= MAX_SOURCE_SILVER_INPUTS:
        raise ValueError("max-inputs is outside the supported bound")
    if not 0 < parsed.max_output_bytes <= MAX_SOURCE_SILVER_OUTPUT_BYTES:
        raise ValueError("max-output-bytes is outside the supported bound")
    reference = _object_ref(
        parsed,
        "run_manifest",
        media_type=BACKFILL_RUN_MANIFEST_MEDIA_TYPE,
    )
    assert reference is not None
    runtime_store = store or _input_store(parsed, input_refs=(reference,))
    manifest = read_eidr_backfill_run_manifest(
        reference=reference,
        store=runtime_store,
    )
    if not manifest.completed and not parsed.allow_partial_run:
        raise ValueError("incomplete EIDR run manifest requires --allow-partial-run")
    inputs = expand_eidr_source_silver_inputs(manifest)
    if len(inputs) > parsed.max_inputs:
        raise ValueError("Source Silver input count exceeds --max-inputs")
    output = [
        {
            "batchManifest": batch_manifest.model_dump(
                mode="json",
                by_alias=True,
                exclude_none=True,
            ),
            "recordSetManifest": record_set_manifest.model_dump(
                mode="json",
                by_alias=True,
                exclude_none=True,
            ),
        }
        for batch_manifest, record_set_manifest in inputs
    ]
    output_bytes = len(canonical_json_bytes(output)) + 1
    if output_bytes > parsed.max_output_bytes:
        raise ValueError("Source Silver input JSON exceeds --max-output-bytes")
    verify_eidr_backfill_run_manifest_objects(
        manifest=manifest,
        store=runtime_store,
    )
    return output


def _run_lookup(
    parsed: argparse.Namespace,
    *,
    provider: EidrProvider | None,
    complete_feed_proof: EidrCompleteFeedProof | None,
    store: RuntimeObjectStore | None,
) -> dict[str, Any]:
    if not 0 < parsed.batch_size <= MAX_LOOKUP_BATCH_IDS:
        raise ValueError("batch-size is outside the supported bound")
    if not 0 < parsed.max_xml_bytes <= MAX_MAX_XML_BYTES:
        raise ValueError("max-xml-bytes is outside the supported bound")
    if not 0 < parsed.record_shard_bytes <= MAX_RECORD_SHARD_BYTES:
        raise ValueError("record-shard-bytes is outside the supported bound")
    if parsed.command == "lookup-manifest":
        if complete_feed_proof is not None:
            raise ValueError(
                "lookup-manifest is always partial and rejects complete-feed proof"
            )
        if not 0 < parsed.max_batches <= MAX_BACKFILL_MAX_BATCHES:
            raise ValueError("max-batches is outside the supported bound")
        if not 0 < parsed.max_duration_seconds <= (MAX_BACKFILL_MAX_DURATION_SECONDS):
            raise ValueError("max-duration-seconds is outside the supported bound")
        if not 0 < parsed.max_ids <= MAX_BACKFILL_MAX_IDS:
            raise ValueError("max-ids is outside the supported bound")
    destination = _output_prefix(parsed.destination_prefix)
    resolved_provider = _resolve_lookup_provider(parsed, injected=provider)
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
    refs = (
        manifest_ref,
        resolved_provider.authorization.authorization_object,
        *((watermark_ref,) if watermark_ref is not None else ()),
    )
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
    if parsed.command == "lookup-manifest":
        run_result = run_eidr_exact_lookup_manifest(
            manifest=manifest,
            manifest_object=manifest_ref,
            destination_prefix=destination,
            acquired_at=parsed.acquired_at,
            image_digest=parsed.image_digest,
            store=runtime_store,
            provider=resolved_provider,
            watermark=watermark,
            watermark_object=watermark_ref,
            batch_size=parsed.batch_size,
            max_xml_bytes=parsed.max_xml_bytes,
            record_shard_bytes=parsed.record_shard_bytes,
            max_batches=parsed.max_batches,
            max_duration_seconds=parsed.max_duration_seconds,
            max_ids=parsed.max_ids,
        )
        run_manifest = run_result.manifest
        response = {
            "completed": run_manifest.completed,
            "stopReason": run_manifest.stop_reason,
            "sourceCompleteness": run_manifest.source_completeness,
            "completeFeedAllowed": run_manifest.complete_feed_allowed,
            "runManifestId": run_manifest.run_manifest_id,
            "runManifest": run_result.manifest_object.model_dump(
                mode="json",
                by_alias=True,
                exclude_none=True,
            ),
            "batchCount": run_manifest.batch_count,
            "attemptedIdCount": run_manifest.attempted_id_count,
            "foundCount": run_manifest.found_count,
            "notFoundCount": run_manifest.not_found_count,
            "attemptCount": run_manifest.attempt_count,
            "retryCount": run_manifest.retry_count,
            "rateLimitCount": run_manifest.rate_limit_count,
            "sourceSilverInputCount": sum(
                item.connector_batch_object is not None for item in run_manifest.batches
            ),
        }
        if run_result.watermark is not None and run_result.watermark_object is not None:
            response["watermarkId"] = run_result.watermark.watermark_id
            response["watermark"] = run_result.watermark_object.model_dump(
                mode="json",
                by_alias=True,
                exclude_none=True,
            )
            response["nextOrdinal"] = run_result.watermark.next_ordinal
        return response

    result = run_eidr_exact_lookup_batch(
        manifest=manifest,
        manifest_object=manifest_ref,
        destination_prefix=destination,
        acquired_at=parsed.acquired_at,
        image_digest=parsed.image_digest,
        store=runtime_store,
        provider=resolved_provider,
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
) -> dict[str, Any] | list[dict[str, Any]]:
    if parsed.command == "expand-source-silver-inputs":
        if provider is not None or complete_feed_proof is not None or spark is not None:
            raise ValueError(
                "expand-source-silver-inputs accepts only immutable object inputs"
            )
        return _run_expand_source_silver_inputs(parsed, store=store)
    if parsed.command == "extract-ids":
        if provider is not None or complete_feed_proof is not None:
            raise ValueError("extract-ids does not accept provider injection")
        return _run_extract(parsed, store=store, spark=spark)
    if spark is not None:
        raise ValueError("lookup commands do not accept a Spark session")
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
