from __future__ import annotations

import pytest

from video_media_catalog.community_cli import _object_ref, build_parser
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.record_shard_materialization import (
    resolve_record_staging_prefix,
)


def _arguments() -> list[str]:
    return [
        "--batch-manifest-uri",
        "file:///tmp/batch.json",
        "--batch-manifest-hash",
        "a" * 64,
        "--batch-manifest-size",
        "100",
        "--record-set-manifest-uri",
        "file:///tmp/records.json",
        "--record-set-manifest-hash",
        "b" * 64,
        "--record-set-manifest-size",
        "100",
        "--committed-at",
        "2026-09-19T00:00:00Z",
        "--catalog-type",
        "hadoop",
        "--warehouse",
        "file:///tmp/warehouse",
    ]


def test_community_cli_builds_bounded_control_object_ref() -> None:
    parsed = build_parser().parse_args(_arguments())
    reference = _object_ref(
        parsed,
        "batch_manifest",
        media_type="application/vnd.example+json",
    )
    assert reference.uri == "file:///tmp/batch.json"
    assert reference.checksum.value == "a" * 64
    assert reference.object_version is None
    assert parsed.namespace == "video_media_catalog"
    assert parsed.s3_credentials_provider == "web-identity"

    emr = build_parser().parse_args(
        [*_arguments(), "--s3-credentials-provider", "default"]
    )
    assert emr.s3_credentials_provider == "default"


def test_community_cli_requires_s3_immutability_fields() -> None:
    arguments = _arguments()
    arguments[1] = "s3://bucket/batch.json"
    parsed = build_parser().parse_args(arguments)
    with pytest.raises(ValueError, match="version and ETag"):
        _object_ref(
            parsed,
            "batch_manifest",
            media_type="application/vnd.example+json",
        )


def test_community_cli_rejects_missing_record_staging_prefix_for_s3_shards() -> None:
    reference = ObjectRef(
        uri="s3://bucket/captures/records.ndjson",
        format="OBJECT_FORMAT_OTHER",
        media_type="application/json",
        checksum=Checksum(value="c" * 64),
        size_bytes=1,
        etag="etag",
        object_version="version",
    )
    with pytest.raises(ValueError, match="requires --record-staging-prefix"):
        resolve_record_staging_prefix(
            None,
            warehouse="s3://bucket/community-warehouse",
            references=(reference,),
        )


def test_community_cli_rejects_capture_sibling_record_staging_prefix() -> None:
    reference = ObjectRef(
        uri="s3://bucket/captures/records.ndjson",
        format="OBJECT_FORMAT_OTHER",
        media_type="application/json",
        checksum=Checksum(value="c" * 64),
        size_bytes=1,
        etag="etag",
        object_version="version",
    )
    with pytest.raises(ValueError, match="allowed catalog write path"):
        resolve_record_staging_prefix(
            "s3://bucket/captures/_staging/record-shards",
            warehouse="s3://bucket/community-warehouse",
            references=(reference,),
        )
