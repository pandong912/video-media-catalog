from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from video_media_catalog.assertions import SourceNodeRef
from video_media_catalog.community_snapshot import SILVER_SNAPSHOT_MEDIA_TYPE
from video_media_catalog.identity_curation import (
    IDENTITY_CURATION_MANIFEST_MEDIA_TYPE,
    IdentityCurationAction,
    IdentityCurationManifest,
    IdentityCurationOperation,
    PinnedSilverSnapshot,
    build_identity_curation_manifest,
)
from video_media_catalog.identity_curation_cli import build_parser, run
from video_media_catalog.identity_v2 import build_identity_conflict
from video_media_catalog.models import Checksum, ObjectRef


def _digest(character: str) -> str:
    return "sha256:" + (character * 64)


def _manifest():
    source_node = SourceNodeRef(
        namespace_id="tvmaze-show",
        source_id="1",
        referent_kind="SERIES",
    )
    conflict = build_identity_conflict(
        materialization_id=_digest("1"),
        source_node=source_node,
        candidate_entity_keys=(_digest("2"),),
        assertion_keys=(_digest("3"),),
        reason="MULTIPLE_EXACT_IDENTIFIER_CANDIDATES",
        observed_at="2026-09-19T00:00:00Z",
        policy_id="internal-key-continuity",
        policy_digest=_digest("4"),
    )
    return build_identity_curation_manifest(
        pinned_silver_snapshot=PinnedSilverSnapshot(
            object=ObjectRef(
                uri="file:///tmp/silver.json",
                format="OBJECT_FORMAT_JSON",
                media_type=SILVER_SNAPSHOT_MEDIA_TYPE,
                checksum=Checksum(value="5" * 64),
                size_bytes=100,
            ),
            snapshot_set_id=_digest("6"),
        ),
        operations=(
            IdentityCurationOperation(
                action=IdentityCurationAction.ACCEPT,
                conflict_key=conflict.conflict_key,
                source_node=source_node,
                assertion_keys=conflict.assertion_keys,
                candidate_entity_key=_digest("2"),
            ),
        ),
        operator_subject="owner-1",
        reason="manual review",
        operated_at="2026-09-20T00:00:00Z",
        config_digest=_digest("7"),
        image_digest=_digest("8"),
    )


def _apply_arguments() -> list[str]:
    return [
        "apply",
        "--review-manifest-uri",
        "file:///tmp/curation.json",
        "--review-manifest-hash",
        "9" * 64,
        "--review-manifest-size",
        "100",
        "--operator-subject",
        "owner-1",
        "--image-digest",
        _digest("8"),
        "--config-digest",
        _digest("7"),
        "--committed-at",
        "2026-09-20T00:01:00Z",
        "--catalog-type",
        "hadoop",
        "--warehouse",
        "file:///tmp/warehouse",
    ]


def test_parser_exposes_publish_and_apply_stages() -> None:
    applied = build_parser().parse_args(_apply_arguments())
    assert applied.command == "apply"
    assert applied.namespace == "video_media_catalog"
    assert applied.app_name == "media-catalog-identity-curation"

    published = build_parser().parse_args(
        [
            "publish",
            "--manifest-file",
            "/tmp/curation.json",
            "--destination-uri",
            "file:///tmp/published-curation.json",
        ]
    )
    assert published.command == "publish"


def test_publish_validates_and_returns_immutable_manifest_ref(
    tmp_path: Path,
) -> None:
    manifest = _manifest()
    source = tmp_path / "curation-source.json"
    source.write_bytes(manifest.json_bytes())
    destination = tmp_path / "published" / "curation.json"
    parsed = build_parser().parse_args(
        [
            "publish",
            "--manifest-file",
            str(source),
            "--destination-uri",
            destination.as_uri(),
        ]
    )

    result = run(parsed)

    assert result["requestId"] == manifest.manifest_id
    assert result["status"] == "SUBMITTED"
    assert result["manifest"]["object"]["mediaType"] == (
        IDENTITY_CURATION_MANIFEST_MEDIA_TYPE
    )
    assert (
        IdentityCurationManifest.model_validate_json(destination.read_bytes())
        == manifest
    )


def test_apply_requires_versioned_s3_manifest_before_spark() -> None:
    arguments = _apply_arguments()
    arguments[2] = "s3://bucket/curation.json"
    parsed = build_parser().parse_args(arguments)

    with pytest.raises(ValueError, match="requires version and ETag"):
        run(parsed)


def test_apply_binds_runtime_image_and_config_digests(tmp_path: Path) -> None:
    payload = _manifest().json_bytes()
    source = tmp_path / "curation.json"
    source.write_bytes(payload)
    arguments = _apply_arguments()
    arguments[2] = source.as_uri()
    arguments[4] = hashlib.sha256(payload).hexdigest()
    arguments[6] = str(len(payload))
    arguments[10] = _digest("0")
    parsed = build_parser().parse_args(arguments)

    with pytest.raises(ValueError, match="runtime image/config"):
        run(parsed)


def test_publish_rejects_non_contract_json(tmp_path: Path) -> None:
    source = tmp_path / "invalid.json"
    source.write_text(json.dumps({"action": "ACCEPT"}))
    parsed = build_parser().parse_args(
        [
            "publish",
            "--manifest-file",
            str(source),
            "--destination-uri",
            (tmp_path / "published.json").as_uri(),
        ]
    )

    with pytest.raises(ValueError):
        run(parsed)
