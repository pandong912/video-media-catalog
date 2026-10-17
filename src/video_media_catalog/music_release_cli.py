"""CLI for publishing and resolving Music Catalog release requests."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

from video_media_catalog.canonical import canonical_json, canonical_json_bytes
from video_media_catalog.music_release import (
    DEFAULT_MATCH_PROFILE,
    DEFAULT_MUSICBRAINZ_FIXTURE,
    DEFAULT_WIKIDATA_FIXTURE,
    object_ref_mapping,
    publish_local_music_release_bundle,
    publish_reviewed_music_release_bundle,
    resolve_music_release_request,
)
from video_media_catalog.object_store import BoundedObjectStore

DEFAULT_BUCKET = "ai-video-platform-dev-media-catalog-209479308066"
DEFAULT_CAPTURE_PREFIX = "landing/research/capture/music-release/v1"
DEFAULT_REQUEST_PREFIX = "landing/research/music/release-requests"
_AWS_ACCOUNT = re.compile(r"^[0-9]{12}$")


class _ProtectedS3Client:
    """Inject expected-owner and KMS controls into bounded S3 operations."""

    def __init__(
        self,
        client: Any,
        *,
        kms_key_id: str,
        expected_bucket_owner: str,
    ) -> None:
        self._client = client
        self._kms_key_id = kms_key_id
        self._expected_bucket_owner = expected_bucket_owner

    def _verify_encryption(self, response: dict[str, Any]) -> dict[str, Any]:
        if response.get("ServerSideEncryption") != "aws:kms" or not response.get(
            "SSEKMSKeyId"
        ):
            raise ValueError("published Music release object is not KMS encrypted")
        actual_key = str(response["SSEKMSKeyId"])
        if self._kms_key_id.startswith("arn:") and actual_key != self._kms_key_id:
            raise ValueError("published Music release object uses a different KMS key")
        return response

    def put_object(self, **values: Any) -> dict[str, Any]:
        return self._client.put_object(
            **values,
            ExpectedBucketOwner=self._expected_bucket_owner,
            ServerSideEncryption="aws:kms",
            SSEKMSKeyId=self._kms_key_id,
        )

    def head_object(self, **values: Any) -> dict[str, Any]:
        response = self._client.head_object(
            **values,
            ExpectedBucketOwner=self._expected_bucket_owner,
        )
        return self._verify_encryption(response)

    def get_object(self, **values: Any) -> dict[str, Any]:
        response = self._client.get_object(
            **values,
            ExpectedBucketOwner=self._expected_bucket_owner,
        )
        return self._verify_encryption(response)


def _json_object(path: Path, *, label: str) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{label} must contain a JSON object")
    return value


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes(value, newline=True))


def _add_fixture_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--musicbrainz-fixture",
        type=Path,
        default=DEFAULT_MUSICBRAINZ_FIXTURE,
    )
    parser.add_argument(
        "--wikidata-fixture",
        type=Path,
        default=DEFAULT_WIKIDATA_FIXTURE,
    )
    parser.add_argument(
        "--match-profile",
        type=Path,
        default=DEFAULT_MATCH_PROFILE,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="video-media-catalog-music-release")
    commands = parser.add_subparsers(dest="command", required=True)

    publish = commands.add_parser(
        "publish-reviewed",
        help="Publish immutable curated captures and a REVIEWED request",
    )
    publish.add_argument("--source-revision", required=True)
    publish.add_argument("--golden-build-json", required=True, type=Path)
    publish.add_argument("--bucket", default=DEFAULT_BUCKET)
    publish.add_argument(
        "--capture-prefix",
        "--prefix",
        dest="capture_prefix",
        default=DEFAULT_CAPTURE_PREFIX,
    )
    publish.add_argument("--request-prefix", default=DEFAULT_REQUEST_PREFIX)
    publish.add_argument("--region", default="us-east-1")
    publish.add_argument("--kms-key-id")
    publish.add_argument(
        "--expected-bucket-owner",
        "--owner",
        dest="expected_bucket_owner",
    )
    publish.add_argument("--request-output", type=Path)
    publish.add_argument("--dry-run", action="store_true")
    publish.add_argument(
        "--bundle-directory",
        type=Path,
        default=Path("music-release-bundle"),
    )
    _add_fixture_arguments(publish)

    resolve = commands.add_parser(
        "resolve",
        help="Bind the post-build image digest without changing other values",
    )
    resolve.add_argument("--request", required=True, type=Path)
    resolve.add_argument(
        "--reviewed-request-ref",
        required=True,
        type=Path,
    )
    resolve.add_argument(
        "--emr-image-digest",
        "--image-digest",
        dest="emr_image_digest",
        required=True,
        help="Digest of the EMR execution image (legacy --image-digest is accepted)",
    )
    resolve.add_argument("--output", type=Path)
    return parser


def _publish_reviewed(parsed: argparse.Namespace) -> dict[str, Any]:
    golden_build = _json_object(
        parsed.golden_build_json,
        label="Golden Build input",
    )
    fixture_arguments = {
        "musicbrainz_fixture_path": parsed.musicbrainz_fixture,
        "wikidata_fixture_path": parsed.wikidata_fixture,
        "match_profile_path": parsed.match_profile,
    }
    if parsed.dry_run:
        bundle = publish_local_music_release_bundle(
            source_revision=parsed.source_revision,
            golden_build=golden_build,
            destination_directory=parsed.bundle_directory,
            store=BoundedObjectStore(client=object()),
            **fixture_arguments,
        )
        if parsed.request_output is not None:
            _write_json(parsed.request_output, bundle.request)
        return {
            "schemaVersion": "video.media-catalog.music-release-dry-run-bundle.v1",
            "dryRun": True,
            "publishable": False,
            "releaseId": bundle.release_id,
            "bundleDirectory": str(parsed.bundle_directory.resolve()),
            "requestTemplate": object_ref_mapping(bundle.request_object),
        }

    if not parsed.kms_key_id:
        raise ValueError("--kms-key-id is required outside --dry-run")
    if (
        not parsed.expected_bucket_owner
        or _AWS_ACCOUNT.fullmatch(parsed.expected_bucket_owner) is None
    ):
        raise ValueError("--expected-bucket-owner must be a 12-digit AWS account ID")
    if "/" in parsed.bucket or not parsed.bucket.strip():
        raise ValueError("--bucket must be an S3 bucket name")
    capture_prefix = parsed.capture_prefix.strip("/")
    request_prefix = parsed.request_prefix.strip("/")
    if not capture_prefix or not request_prefix:
        raise ValueError("capture and request prefixes must be non-empty")

    import boto3

    client = _ProtectedS3Client(
        boto3.client("s3", region_name=parsed.region),
        kms_key_id=parsed.kms_key_id,
        expected_bucket_owner=parsed.expected_bucket_owner,
    )
    bundle = publish_reviewed_music_release_bundle(
        source_revision=parsed.source_revision,
        golden_build=golden_build,
        destination_prefix=f"s3://{parsed.bucket}/{capture_prefix}",
        request_destination_prefix=f"s3://{parsed.bucket}/{request_prefix}",
        store=BoundedObjectStore(client=client),
        **fixture_arguments,
    )
    if parsed.request_output is not None:
        _write_json(parsed.request_output, bundle.request)
    return object_ref_mapping(bundle.request_object)


def _resolve(parsed: argparse.Namespace) -> dict[str, Any]:
    request = _json_object(parsed.request, label="Music release request")
    reviewed_reference = _json_object(
        parsed.reviewed_request_ref,
        label="reviewed request ObjectRef",
    )
    resolved = resolve_music_release_request(
        request,
        image_digest=parsed.emr_image_digest,
        reviewed_request=reviewed_reference,
    )
    if parsed.output is not None:
        _write_json(parsed.output, resolved)
    return resolved


def main(argv: list[str] | None = None) -> int:
    parsed = build_parser().parse_args(argv)
    if parsed.command == "publish-reviewed":
        result = _publish_reviewed(parsed)
    elif parsed.command == "resolve":
        result = _resolve(parsed)
    else:
        raise SystemExit(f"unsupported command: {parsed.command}")
    print(canonical_json(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
