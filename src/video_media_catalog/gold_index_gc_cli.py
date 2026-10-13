"""CLI for Research Gold snapshot repository, GC, and restore drills."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from typing import Any
from urllib.parse import urlsplit

from video_media_catalog.canonical import canonical_json
from video_media_catalog.gold_index_gc import (
    GOLD_INDEX_ARCHIVE_RECEIPT_MEDIA_TYPE,
    archive_delete,
    register_snapshot_repository,
    restore_drill,
)
from video_media_catalog.gold_search_index import (
    GOLD_INDEX_ACTIVATION_RECEIPT_MEDIA_TYPE,
)
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.object_store import BoundedObjectStore
from video_media_catalog.opensearch_client import (
    OpenSearchConnection,
    create_opensearch_client,
)


def _add_opensearch_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--aws-region")
    parser.add_argument("--opensearch-endpoint", required=True)
    parser.add_argument("--opensearch-service", choices=("es", "aoss"), default="es")
    parser.add_argument("--request-timeout-seconds", type=float, default=30)
    parser.add_argument("--allow-insecure-opensearch", action="store_true")


def _add_object_store_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--s3-endpoint")
    parser.add_argument("--s3-path-style-access", action="store_true")


def _add_archive_ref_arguments(
    parser: argparse.ArgumentParser,
    *,
    prefix: str,
    required: bool,
) -> None:
    parser.add_argument(f"--{prefix}-uri", required=required)
    parser.add_argument(f"--{prefix}-hash", required=required)
    parser.add_argument(f"--{prefix}-size", type=int, required=required)
    parser.add_argument(f"--{prefix}-version", default="")
    parser.add_argument(f"--{prefix}-etag", default="")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="video-media-catalog-gold-index-gc",
        description=(
            "Archive and delete inactive Research indices or restore one "
            "original-name drill index without changing aliases."
        ),
    )
    commands = parser.add_subparsers(dest="command", required=True)

    repository = commands.add_parser(
        "register-repository",
        help="Idempotently register one exact S3 snapshot repository",
    )
    _add_opensearch_arguments(repository)
    repository.add_argument("--repository", required=True)
    repository.add_argument("--bucket", required=True)
    repository.add_argument("--base-path", required=True)
    repository.add_argument("--repository-region", required=True)
    repository.add_argument("--role-arn", required=True)
    repository.add_argument("--readonly", action="store_true")

    archive = commands.add_parser(
        "archive-delete",
        help="Plan by default; pass --execute to snapshot, receipt, and delete",
    )
    _add_opensearch_arguments(archive)
    _add_object_store_arguments(archive)
    archive.add_argument("--operation-id", required=True)
    archive.add_argument("--repository", required=True)
    archive.add_argument("--snapshot", required=True)
    archive.add_argument("--old-index", required=True)
    archive.add_argument("--active-index", required=True)
    archive.add_argument("--archive-receipt-uri", required=True)
    archive.add_argument("--gc-result-uri", required=True)
    _add_archive_ref_arguments(
        archive,
        prefix="activation-receipt",
        required=True,
    )
    archive.add_argument("--completed-at", required=True)
    archive.add_argument("--delete-before-epoch-millis", type=int, required=True)
    archive.add_argument("--referenced-index", action="append", default=[])
    archive.add_argument("--snapshot-timeout-seconds", type=float, default=7200)
    archive.add_argument("--snapshot-poll-seconds", type=float, default=15)
    archive.add_argument(
        "--execute",
        action="store_true",
        help="Perform snapshot and deletion; omitted means immutable dry-run",
    )
    _add_archive_ref_arguments(
        archive,
        prefix="reuse-archive-receipt",
        required=False,
    )

    restore = commands.add_parser(
        "restore-drill",
        help="Restore the archived index under its original name without aliases",
    )
    _add_opensearch_arguments(restore)
    _add_object_store_arguments(restore)
    restore.add_argument("--operation-id", required=True)
    _add_archive_ref_arguments(
        restore,
        prefix="archive-receipt",
        required=True,
    )
    restore.add_argument("--restore-receipt-uri", required=True)
    restore.add_argument("--completed-at", required=True)
    restore.add_argument("--restore-timeout-seconds", type=float, default=7200)
    restore.add_argument("--restore-poll-seconds", type=float, default=15)
    return parser


def _reference(
    parsed: argparse.Namespace,
    *,
    prefix: str,
    media_type: str,
    required: bool,
) -> ObjectRef | None:
    attribute = prefix.replace("-", "_")
    uri = getattr(parsed, f"{attribute}_uri")
    checksum = getattr(parsed, f"{attribute}_hash")
    size = getattr(parsed, f"{attribute}_size")
    version = getattr(parsed, f"{attribute}_version")
    etag = getattr(parsed, f"{attribute}_etag")
    supplied = [
        uri is not None,
        checksum is not None,
        size is not None,
        bool(version),
        bool(etag),
    ]
    if not any(supplied):
        if required:
            raise ValueError(f"{prefix} immutable reference is required")
        return None
    if uri is None or checksum is None or size is None:
        raise ValueError(
            f"{prefix} URI, hash, and size must be supplied together"
        )
    reference = ObjectRef(
        uri=str(uri),
        format="OBJECT_FORMAT_JSON",
        media_type=media_type,
        checksum=Checksum(value=str(checksum)),
        size_bytes=int(size),
        object_version=str(version) or None,
        etag=str(etag) or None,
    )
    if reference.size_bytes < 1:
        raise ValueError(f"{prefix} must not be empty")
    if reference.uri.startswith("s3://") and (
        reference.object_version is None or reference.etag is None
    ):
        raise ValueError(f"S3 {prefix} requires version and ETag")
    return reference


def _close(client: Any) -> None:
    transport = getattr(client, "transport", None)
    close = getattr(transport, "close", None)
    if callable(close):
        close()


def _object_store(
    parsed: argparse.Namespace,
    *,
    client: Any | None = None,
) -> BoundedObjectStore:
    if client is not None:
        return BoundedObjectStore(client=client)
    uris = [
        str(value)
        for name, value in vars(parsed).items()
        if name.endswith("_uri") and value
    ]
    local_only = bool(uris) and all(
        urlsplit(uri).scheme == "file" for uri in uris
    )
    return BoundedObjectStore(
        region=parsed.aws_region,
        endpoint_url=parsed.s3_endpoint,
        path_style_access=parsed.s3_path_style_access,
        client=object() if local_only else None,
    )


def run(
    parsed: argparse.Namespace,
    *,
    store: BoundedObjectStore | None = None,
    client: Any | None = None,
) -> dict[str, Any]:
    owns_client = client is None
    opensearch = client or create_opensearch_client(
        OpenSearchConnection(
            endpoint=parsed.opensearch_endpoint,
            aws_region=parsed.aws_region,
            service=parsed.opensearch_service,
            timeout_seconds=parsed.request_timeout_seconds,
            allow_insecure=parsed.allow_insecure_opensearch,
        )
    )
    try:
        if parsed.command == "register-repository":
            return register_snapshot_repository(
                opensearch,
                repository=parsed.repository,
                bucket=parsed.bucket,
                base_path=parsed.base_path,
                region=parsed.repository_region,
                role_arn=parsed.role_arn,
                readonly=parsed.readonly,
            )
        object_store = store or _object_store(parsed)
        if parsed.command == "archive-delete":
            activation = _reference(
                parsed,
                prefix="activation-receipt",
                media_type=GOLD_INDEX_ACTIVATION_RECEIPT_MEDIA_TYPE,
                required=True,
            )
            if activation is None:
                raise RuntimeError("activation receipt reference is unavailable")
            reuse = _reference(
                parsed,
                prefix="reuse-archive-receipt",
                media_type=GOLD_INDEX_ARCHIVE_RECEIPT_MEDIA_TYPE,
                required=False,
            )
            return archive_delete(
                opensearch,
                object_store,
                operation_id=parsed.operation_id,
                repository=parsed.repository,
                snapshot=parsed.snapshot,
                old_index=parsed.old_index,
                active_index=parsed.active_index,
                activation_receipt=activation,
                archive_receipt_uri=parsed.archive_receipt_uri,
                gc_result_uri=parsed.gc_result_uri,
                completed_at=parsed.completed_at,
                delete_before_epoch_millis=parsed.delete_before_epoch_millis,
                referenced_indices=tuple(parsed.referenced_index),
                execute=parsed.execute,
                reuse_archive_receipt=reuse,
                snapshot_timeout_seconds=parsed.snapshot_timeout_seconds,
                snapshot_poll_seconds=parsed.snapshot_poll_seconds,
            )
        archive_receipt = _reference(
            parsed,
            prefix="archive-receipt",
            media_type=GOLD_INDEX_ARCHIVE_RECEIPT_MEDIA_TYPE,
            required=True,
        )
        if archive_receipt is None:
            raise RuntimeError("archive receipt reference is unavailable")
        return restore_drill(
            opensearch,
            object_store,
            operation_id=parsed.operation_id,
            archive_receipt=archive_receipt,
            restore_receipt_uri=parsed.restore_receipt_uri,
            completed_at=parsed.completed_at,
            restore_timeout_seconds=parsed.restore_timeout_seconds,
            restore_poll_seconds=parsed.restore_poll_seconds,
        )
    finally:
        if owns_client:
            _close(opensearch)


def main(argv: Sequence[str] | None = None) -> int:
    result = run(build_parser().parse_args(argv))
    print(canonical_json(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
