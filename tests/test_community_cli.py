from __future__ import annotations

import hashlib

import pytest

from video_media_catalog.community_cli import (
    SHARDED_RECORD_SET_MEDIA_TYPE,
    _has_prevalidated_record_set_grant,
    _object_ref,
    _prevalidated_record_set_grant,
    _read_record_set,
    build_parser,
    run,
)
from video_media_catalog.connector import (
    ConnectorRecordShard,
    build_connector_record_set_epoch_manifest,
    build_connector_record_set_partition_manifest,
    build_connector_sharded_record_set_manifest,
)
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.object_store import BoundedObjectStore
from video_media_catalog.record_shard_materialization import (
    resolve_record_staging_prefix,
)
from video_media_catalog.source_silver_checkpoint import (
    DEFAULT_SOURCE_SILVER_CHECKPOINT_GROUP_SIZE,
    resolve_source_silver_checkpoint_prefix,
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


def _control_ref(uri: str, checksum: str) -> ObjectRef:
    return ObjectRef(
        uri=uri,
        format="OBJECT_FORMAT_JSON",
        media_type="application/json",
        checksum=Checksum(value=checksum),
        size_bytes=100,
        etag="etag",
        object_version="version",
    )


def _grant_arguments() -> list[str]:
    return [
        "--prevalidated-record-set-id",
        "sha256:94c790bfb69131c9c9728c5aca190dfe5fa6acce3e257d7c2225e2cd479b3d03",
        "--prevalidated-batch-id",
        "sha256:eecbfa4fbadb18a91574bb02763c19cf94ee1e32b58cb3643dadd936f1b70d66",
        "--prevalidated-record-count",
        "214127556",
        "--prevalidated-shard-count",
        "2638",
        "--prevalidated-size-bytes",
        "353664211724",
        "--prevalidated-expires-at",
        "2099-01-01T00:00:00Z",
    ]


def _write_object(
    path,
    payload: bytes,
    *,
    media_type: str,
    object_format: str = "OBJECT_FORMAT_JSON",
) -> ObjectRef:
    path.write_bytes(payload)
    return ObjectRef(
        uri=path.as_uri(),
        format=object_format,
        media_type=media_type,
        checksum=Checksum(value=hashlib.sha256(payload).hexdigest()),
        size_bytes=len(payload),
    )


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
    assert parsed.materialize_workers == 1
    assert parsed.checkpoint_group_size == DEFAULT_SOURCE_SILVER_CHECKPOINT_GROUP_SIZE

    emr = build_parser().parse_args(
        [*_arguments(), "--s3-credentials-provider", "default"]
    )
    assert emr.s3_credentials_provider == "default"


def test_community_cli_builds_exact_prevalidated_grant() -> None:
    parsed = build_parser().parse_args(
        [*_arguments(), *_grant_arguments(), "--materialize-workers", "8"]
    )
    batch_ref = _control_ref("s3://bucket/control/batch.json", "a" * 64)
    record_set_ref = _control_ref(
        "s3://bucket/control/record-set.json",
        "b" * 64,
    )

    grant = _prevalidated_record_set_grant(
        parsed,
        batch_manifest_ref=batch_ref,
        record_set_manifest_ref=record_set_ref,
        staging_prefix="s3://bucket/staging/",
    )

    assert grant is not None
    assert grant.record_set_id == (
        "sha256:94c790bfb69131c9c9728c5aca190dfe5fa6acce3e257d7c2225e2cd479b3d03"
    )
    assert grant.batch_id == (
        "sha256:eecbfa4fbadb18a91574bb02763c19cf94ee1e32b58cb3643dadd936f1b70d66"
    )
    assert grant.record_count == 214127556
    assert grant.shard_count == 2638
    assert grant.size_bytes == 353664211724
    assert grant.batch_manifest_ref == batch_ref
    assert grant.record_set_manifest_ref == record_set_ref
    assert grant.staging_prefix == "s3://bucket/staging"
    assert parsed.materialize_workers == 8


def test_community_cli_requires_all_prevalidated_grant_arguments() -> None:
    parsed = build_parser().parse_args(
        [
            *_arguments(),
            "--prevalidated-record-set-id",
            "sha256:" + ("c" * 64),
        ]
    )
    with pytest.raises(ValueError, match="provided together"):
        _has_prevalidated_record_set_grant(parsed)


def test_community_cli_rejects_invalid_prevalidated_grant_values() -> None:
    arguments = _grant_arguments()
    arguments[-1] = "not-a-timestamp"
    with pytest.raises(SystemExit):
        build_parser().parse_args([*_arguments(), *arguments])

    arguments = _grant_arguments()
    count_index = arguments.index("--prevalidated-record-count") + 1
    arguments[count_index] = "0"
    with pytest.raises(SystemExit):
        build_parser().parse_args([*_arguments(), *arguments])

    with pytest.raises(SystemExit):
        build_parser().parse_args(
            [
                *_arguments(),
                "--materialize-workers",
                "33",
            ]
        )
    parsed = build_parser().parse_args(
        [*_arguments(), "--checkpoint-group-size", "1025"]
    )
    with pytest.raises(ValueError, match="checkpoint-group-size"):
        run(parsed)


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


def test_community_cli_resolves_hierarchical_record_set(tmp_path) -> None:
    first_key = "sha256:" + ("1" * 64)
    last_key = "sha256:" + ("2" * 64)
    shard_payload = (
        b'{"envelopeKey":"' + first_key.encode() + b'"}\n'
        b'{"envelopeKey":"' + last_key.encode() + b'"}\n'
    )
    shard_ref = _write_object(
        tmp_path / "records.ndjson",
        shard_payload,
        media_type="application/x-ndjson",
        object_format="OBJECT_FORMAT_OTHER",
    )
    shard = ConnectorRecordShard(
        shard_index=0,
        object_ref=shard_ref,
        record_count=2,
        first_envelope_key=first_key,
        last_envelope_key=last_key,
    )
    batch_id = "sha256:" + ("3" * 64)
    policy_digest = "sha256:" + ("4" * 64)
    created_at = "2026-09-20T00:00:00Z"
    partition = build_connector_record_set_partition_manifest(
        batch_id=batch_id,
        source_product_id="wikidata-json-dump",
        policy_id="wikidata-structured-data-cc0",
        policy_digest=policy_digest,
        epoch_index=0,
        partition_index=0,
        shards=(shard,),
        shard_count=1,
        record_count=2,
        size_bytes=shard_ref.size_bytes,
        first_envelope_key=first_key,
        last_envelope_key=last_key,
        created_at=created_at,
    )
    partition_ref = _write_object(
        tmp_path / "partition.json",
        partition.json_bytes(),
        media_type=(
            "application/vnd.video-media-catalog.record-set-partition.v2.1+json"
        ),
    )
    epoch = build_connector_record_set_epoch_manifest(
        batch_id=batch_id,
        source_product_id="wikidata-json-dump",
        policy_id="wikidata-structured-data-cc0",
        policy_digest=policy_digest,
        epoch_index=0,
        partition_objects=(partition_ref,),
        partition_count=1,
        shard_count=1,
        record_count=2,
        size_bytes=shard_ref.size_bytes,
        first_envelope_key=first_key,
        last_envelope_key=last_key,
        created_at=created_at,
    )
    epoch_ref = _write_object(
        tmp_path / "epoch.json",
        epoch.json_bytes(),
        media_type=("application/vnd.video-media-catalog.record-set-epoch.v2.1+json"),
    )
    root = build_connector_sharded_record_set_manifest(
        batch_id=batch_id,
        source_product_id="wikidata-json-dump",
        policy_id="wikidata-structured-data-cc0",
        policy_digest=policy_digest,
        epoch_objects=(epoch_ref,),
        epoch_count=1,
        partition_count=1,
        shard_count=1,
        record_count=2,
        size_bytes=shard_ref.size_bytes,
        first_envelope_key=first_key,
        last_envelope_key=last_key,
        created_at=created_at,
    )
    root_ref = _write_object(
        tmp_path / "record-set.json",
        root.json_bytes(),
        media_type=SHARDED_RECORD_SET_MEDIA_TYPE,
    )

    resolved_ref, resolved = _read_record_set(
        BoundedObjectStore(client=object()),
        root_ref.model_copy(
            update={
                "media_type": ("application/vnd.video-media-catalog.record-set.v2+json")
            }
        ),
    )

    assert resolved_ref.media_type == SHARDED_RECORD_SET_MEDIA_TYPE
    assert resolved.record_set_id == root.record_set_id
    assert resolved.record_objects == (shard_ref,)
    assert resolved.record_count == 2
    assert resolved.first_envelope_key == first_key
    assert resolved.last_envelope_key == last_key


def test_source_silver_checkpoint_prefix_is_confined_to_warehouse() -> None:
    expected = (
        "s3://bucket/community-warehouse/research/control/source-silver-checkpoints"
    )
    assert (
        resolve_source_silver_checkpoint_prefix(
            expected,
            warehouse="s3://bucket/community-warehouse/",
        )
        == expected
    )
    with pytest.raises(ValueError, match="must equal"):
        resolve_source_silver_checkpoint_prefix(
            "s3://bucket/other/source-silver-checkpoints",
            warehouse="s3://bucket/community-warehouse/",
        )
