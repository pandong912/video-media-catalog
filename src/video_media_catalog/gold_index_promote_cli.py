"""Explicitly activate one validated Research Gold concrete index."""

from __future__ import annotations

import argparse
import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from video_media_catalog.canonical import canonical_json
from video_media_catalog.gold_search_index import (
    GOLD_INDEX_ACTIVATION_RECEIPT_MEDIA_TYPE,
    GOLD_INDEX_BUILD_MANIFEST_MEDIA_TYPE,
    GoldIndexActivationReceipt,
    GoldIndexBuildManifest,
    validate_gold_index_contents,
    validate_gold_index_mapping,
)
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.object_store import BoundedObjectStore
from video_media_catalog.opensearch_client import (
    OpenSearchConnection,
    create_opensearch_client,
)
from video_media_catalog.opensearch_index import switch_read_alias
from video_media_catalog.storage import join_uri

CONTROL_MAX_BYTES = 16 * 1024 * 1024


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="video-media-catalog-gold-index-promote",
        description=(
            "Activate one immutable Research Gold index build using an "
            "expected-old alias precondition."
        ),
    )
    parser.add_argument("--build-manifest-uri", required=True)
    parser.add_argument("--build-manifest-hash", required=True)
    parser.add_argument("--build-manifest-size", type=int, required=True)
    parser.add_argument("--build-manifest-version", default="")
    parser.add_argument("--build-manifest-etag", default="")
    parser.add_argument("--expected-build-id", required=True)
    parser.add_argument("--expected-release-plan-id", required=True)
    parser.add_argument(
        "--expected-old-index",
        action="append",
        default=[],
        help="Repeat for every concrete index expected behind the alias",
    )
    parser.add_argument("--activation-prefix", required=True)
    parser.add_argument("--activated-at", required=True)
    parser.add_argument("--authorized-by", required=True)
    parser.add_argument("--authorization-reason", required=True)
    parser.add_argument("--aws-region")
    parser.add_argument("--s3-endpoint")
    parser.add_argument("--s3-path-style-access", action="store_true")
    parser.add_argument("--opensearch-endpoint", required=True)
    parser.add_argument("--opensearch-service", choices=("es", "aoss"), default="es")
    parser.add_argument("--request-timeout-seconds", type=float, default=30)
    parser.add_argument("--allow-insecure-opensearch", action="store_true")
    return parser


def _manifest_ref(parsed: argparse.Namespace) -> ObjectRef:
    reference = ObjectRef(
        uri=parsed.build_manifest_uri,
        format="OBJECT_FORMAT_JSON",
        media_type=GOLD_INDEX_BUILD_MANIFEST_MEDIA_TYPE,
        checksum=Checksum(value=parsed.build_manifest_hash),
        size_bytes=parsed.build_manifest_size,
        object_version=parsed.build_manifest_version or None,
        etag=parsed.build_manifest_etag or None,
    )
    if reference.size_bytes < 1:
        raise ValueError("build manifest must not be empty")
    if reference.uri.startswith("s3://") and (
        reference.object_version is None or reference.etag is None
    ):
        raise ValueError("S3 build manifest requires version and ETag")
    return reference


def _read_manifest(
    store: BoundedObjectStore,
    reference: ObjectRef,
) -> GoldIndexBuildManifest:
    store.verify(reference, max_bytes=CONTROL_MAX_BYTES)
    with tempfile.TemporaryDirectory(prefix="research-index-promote-") as directory:
        materialized = store.download(
            reference,
            Path(directory) / "build-manifest.json",
            max_bytes=CONTROL_MAX_BYTES,
        )
        return GoldIndexBuildManifest.model_validate_json(
            materialized.path.read_bytes()
        )


def _close(client: Any) -> None:
    transport = getattr(client, "transport", None)
    close = getattr(transport, "close", None)
    if callable(close):
        close()


def run(
    parsed: argparse.Namespace,
    *,
    store: BoundedObjectStore | None = None,
    client: Any | None = None,
) -> dict[str, Any]:
    reference = _manifest_ref(parsed)
    local = urlsplit(reference.uri).scheme == "file"
    object_store = store or BoundedObjectStore(
        region=parsed.aws_region,
        endpoint_url=parsed.s3_endpoint,
        path_style_access=parsed.s3_path_style_access,
        client=object() if local else None,
    )
    manifest = _read_manifest(object_store, reference)
    if manifest.build_id != parsed.expected_build_id:
        raise ValueError("build manifest does not match expected build ID")
    if manifest.release_plan_id != parsed.expected_release_plan_id:
        raise ValueError("build manifest does not match expected release plan")

    expected_previous = tuple(sorted(set(parsed.expected_old_index)))
    if len(expected_previous) != len(parsed.expected_old_index):
        raise ValueError("expected old indices must be unique")

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
        validate_gold_index_mapping(opensearch, index_name=manifest.index)
        validate_gold_index_contents(
            opensearch,
            index_name=manifest.index,
            release_plan_id=manifest.release_plan_id,
            expected_document_count=manifest.document_count,
        )
        switch_read_alias(
            opensearch,
            alias=manifest.alias,
            target_index=manifest.index,
            expected_current=expected_previous,
        )
        receipt = GoldIndexActivationReceipt(
            build_manifest=reference,
            build_id=manifest.build_id,
            release_plan_id=manifest.release_plan_id,
            mapping_digest=manifest.mapping_digest,
            config_digest=manifest.config_digest,
            index=manifest.index,
            alias=manifest.alias,
            expected_previous_indices=expected_previous,
            changed=expected_previous != (manifest.index,),
            authorized_by=parsed.authorized_by,
            authorization_reason=parsed.authorization_reason,
            activated_at=parsed.activated_at,
        )
        receipt_ref = object_store.upload_bytes(
            receipt.json_bytes(),
            join_uri(
                parsed.activation_prefix,
                f"gold-index-activation-{manifest.build_id}.json",
            ),
            media_type=GOLD_INDEX_ACTIVATION_RECEIPT_MEDIA_TYPE,
            object_format="OBJECT_FORMAT_JSON",
            max_bytes=CONTROL_MAX_BYTES,
        ).object_ref
        return {
            "status": receipt.status,
            "buildId": manifest.build_id,
            "releasePlanId": manifest.release_plan_id,
            "index": manifest.index,
            "alias": manifest.alias,
            "activationReceipt": receipt_ref.model_dump(
                mode="json", by_alias=True, exclude_none=True
            ),
        }
    finally:
        if owns_client:
            _close(opensearch)


def main(argv: Sequence[str] | None = None) -> int:
    print(canonical_json(run(build_parser().parse_args(argv))))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
