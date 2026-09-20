from __future__ import annotations

import base64
import hashlib
import io
import json
import tempfile
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import ClassVar
from urllib.parse import urlparse, urlunparse

import pytest

from video_media_catalog.community_sources import build_community_registry
from video_media_catalog.connector import (
    ChangeSemantics,
    Completeness,
    DeleteCoverage,
    RecordOperation,
    Serialization,
    TransportKind,
    build_connector_batch_manifest,
    build_connector_record_envelope,
    build_connector_record_set_manifest,
)
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.object_store import BoundedObjectStore, ObjectStoreError
from video_media_catalog.record_shard_materialization import (
    MaterializedRecordShard,
    bind_materialized_shards,
    materialize_record_shards,
    record_staging_uri,
    resolve_record_staging_prefix,
    validate_record_staging_prefix,
)
from video_media_catalog.source_silver import build_source_silver_rows
from video_media_catalog.tvmaze import (
    TVMAZE_CONNECTOR_ID,
    TVMAZE_POLICY_ID,
    TVMAZE_SOURCE_PRODUCT_ID,
    TVMAZE_SOURCE_SYSTEM_ID,
    tvmaze_rights_profile,
)

WAREHOUSE = "s3://bucket/community-warehouse"
CONTROL_STAGING_PREFIX = (
    "s3://bucket/community-warehouse/research/control/record-shards"
)
LANDING_STAGING_PREFIX = "s3://bucket/landing/research/materialized-record-shards/run-1"


class PreconditionFailure(Exception):
    response: ClassVar[dict] = {
        "Error": {"Code": "PreconditionFailed"},
        "ResponseMetadata": {"HTTPStatusCode": 412},
    }


class VersionedRaceClient:
    """Serve versioned bytes while latest key content can diverge."""

    def __init__(
        self,
        *,
        versioned_payload: bytes,
        latest_payload: bytes,
        version_id: str = "version-1",
    ) -> None:
        self.versioned_payload = versioned_payload
        self.latest_payload = latest_payload
        self.version_id = version_id
        self.staged: dict[str, bytes] = {}
        self.get_requests: list[dict] = []

    def _metadata(self, payload: bytes, *, version_id: str) -> dict:
        digest = hashlib.sha256(payload).digest()
        return {
            "ContentLength": len(payload),
            "ETag": '"etag-1"',
            "VersionId": version_id,
            "ChecksumSHA256": base64.b64encode(digest).decode(),
        }

    def head_object(self, **request):
        payload = (
            self.versioned_payload
            if request.get("VersionId") == self.version_id
            else self.latest_payload
        )
        return self._metadata(payload, version_id=request.get("VersionId", "latest"))

    def get_object(self, **request):
        self.get_requests.append(request)
        payload = (
            self.versioned_payload
            if request.get("VersionId") == self.version_id
            else self.latest_payload
        )
        version = request.get("VersionId", "latest")
        metadata = self._metadata(payload, version_id=version)
        return {**metadata, "Body": io.BytesIO(payload)}

    def put_object(self, **request):
        key = f"{request['Bucket']}/{request['Key']}"
        body = request["Body"]
        payload = body.read() if hasattr(body, "read") else body
        if key in self.staged and self.staged[key] != payload:
            raise PreconditionFailure
        self.staged[key] = payload
        return self._metadata(payload, version_id="staged-version")

    def head_object_after_put(self, **request):
        key = f"{request['Bucket']}/{request['Key']}"
        payload = self.staged[key]
        return self._metadata(payload, version_id="staged-version")


def _sample_envelope_bytes() -> bytes:
    policy = tvmaze_rights_profile()
    raw_object = ObjectRef(
        uri="file:///tmp/raw.json",
        format="OBJECT_FORMAT_JSON",
        media_type="application/json",
        checksum=Checksum(value="a" * 64),
        size_bytes=1,
        created_at="2026-09-20T00:00:00Z",
    )
    batch = build_connector_batch_manifest(
        source_system_id=TVMAZE_SOURCE_SYSTEM_ID,
        source_product_id=TVMAZE_SOURCE_PRODUCT_ID,
        connector_id=TVMAZE_CONNECTOR_ID,
        connector_version="1.0.0",
        image_digest="sha256:" + ("a" * 64),
        config_digest="sha256:" + ("b" * 64),
        policy_id=TVMAZE_POLICY_ID,
        policy_digest=policy.digest,
        transport_kind=TransportKind.API,
        serialization=Serialization.JSON,
        change_semantics=ChangeSemantics.FULL_SNAPSHOT,
        completeness=Completeness.COMPLETE,
        delete_coverage=DeleteCoverage.SNAPSHOT_DIFF,
        coverage_scope={"endpoint": "/shows"},
        raw_objects=(raw_object,),
        acquired_at="2026-09-20T00:00:00Z",
        record_count=1,
        error_count=0,
    )
    envelope = build_connector_record_envelope(
        payload={
            "id": 1,
            "name": "Example",
            "type": "Scripted",
            "language": "English",
            "updated": 1_700_000_000,
            "genres": ["Drama"],
            "externals": {"imdb": "tt0000001"},
        },
        batch_id=batch.batch_id,
        source_system_id=batch.source_system_id,
        source_product_id=batch.source_product_id,
        source_namespace_id="tvmaze-show",
        source_record_id="1",
        source_revision="1700000000",
        operation=RecordOperation.UPSERT,
        source_modified_at="2023-11-14T22:13:20Z",
        observed_at=batch.acquired_at,
        ingested_at=batch.acquired_at,
        payload_schema="tvmaze-show-v1",
        raw_object=raw_object,
        source_location="/page/0/item/0",
        policy_id=batch.policy_id,
        policy_digest=batch.policy_digest,
    )
    return envelope.json_bytes()


def _s3_ref(
    payload: bytes,
    *,
    uri: str = "s3://bucket/captures/records.ndjson",
) -> ObjectRef:
    return ObjectRef(
        uri=uri,
        format="OBJECT_FORMAT_OTHER",
        media_type=(
            "application/vnd.video-media-catalog.connector-record-envelope.v2+ndjson"
        ),
        checksum=Checksum(value=hashlib.sha256(payload).hexdigest()),
        size_bytes=len(payload),
        etag="etag-1",
        object_version="version-1",
    )


def _capture_record(payload_line: bytes) -> tuple[object, object, object]:
    policy = tvmaze_rights_profile()
    raw_object = ObjectRef(
        uri="file:///tmp/raw.json",
        format="OBJECT_FORMAT_JSON",
        media_type="application/json",
        checksum=Checksum(value="a" * 64),
        size_bytes=1,
        created_at="2026-09-20T00:00:00Z",
    )
    batch = build_connector_batch_manifest(
        source_system_id=TVMAZE_SOURCE_SYSTEM_ID,
        source_product_id=TVMAZE_SOURCE_PRODUCT_ID,
        connector_id=TVMAZE_CONNECTOR_ID,
        connector_version="1.0.0",
        image_digest="sha256:" + ("a" * 64),
        config_digest="sha256:" + ("b" * 64),
        policy_id=TVMAZE_POLICY_ID,
        policy_digest=policy.digest,
        transport_kind=TransportKind.API,
        serialization=Serialization.JSON,
        change_semantics=ChangeSemantics.FULL_SNAPSHOT,
        completeness=Completeness.COMPLETE,
        delete_coverage=DeleteCoverage.SNAPSHOT_DIFF,
        coverage_scope={"endpoint": "/shows"},
        raw_objects=(raw_object,),
        acquired_at="2026-09-20T00:00:00Z",
        record_count=1,
        error_count=0,
    )
    envelope = build_connector_record_envelope(
        payload=json.loads(payload_line),
        batch_id=batch.batch_id,
        source_system_id=batch.source_system_id,
        source_product_id=batch.source_product_id,
        source_namespace_id="tvmaze-show",
        source_record_id="1",
        source_revision="1700000000",
        operation=RecordOperation.UPSERT,
        source_modified_at="2023-11-14T22:13:20Z",
        observed_at=batch.acquired_at,
        ingested_at=batch.acquired_at,
        payload_schema="tvmaze-show-v1",
        raw_object=raw_object,
        source_location="/page/0/item/0",
        policy_id=batch.policy_id,
        policy_digest=batch.policy_digest,
    )
    record_set = build_connector_record_set_manifest(
        batch_id=batch.batch_id,
        source_product_id=batch.source_product_id,
        policy_id=batch.policy_id,
        policy_digest=batch.policy_digest,
        record_objects=(_s3_ref(payload_line),),
        record_count=1,
        first_envelope_key=envelope.envelope_key,
        last_envelope_key=envelope.envelope_key,
        created_at=batch.acquired_at,
    )
    return batch, record_set, envelope


def test_record_staging_uri_is_checksum_addressed() -> None:
    checksum = "a" * 64
    uri = record_staging_uri("s3://bucket/staging", checksum)
    assert uri == (
        "s3://bucket/staging/sha256=" + checksum + f"/sha256:{checksum}.ndjson"
    )


def test_validate_record_staging_prefix_accepts_control_root() -> None:
    validated = validate_record_staging_prefix(
        CONTROL_STAGING_PREFIX,
        warehouse=WAREHOUSE,
        require_s3=True,
    )
    assert validated == CONTROL_STAGING_PREFIX


def test_validate_record_staging_prefix_accepts_landing_root() -> None:
    validated = validate_record_staging_prefix(
        LANDING_STAGING_PREFIX,
        warehouse=WAREHOUSE,
        require_s3=True,
    )
    assert validated == LANDING_STAGING_PREFIX


def test_validate_record_staging_prefix_rejects_cross_bucket() -> None:
    with pytest.raises(ValueError, match="catalog warehouse bucket"):
        validate_record_staging_prefix(
            "s3://other-bucket/community-warehouse/research/control",
            warehouse=WAREHOUSE,
            require_s3=True,
        )


def test_validate_record_staging_prefix_rejects_disallowed_prefix() -> None:
    with pytest.raises(ValueError, match="allowed catalog write path"):
        validate_record_staging_prefix(
            "s3://bucket/captures/b1/_staging/record-shards",
            warehouse=WAREHOUSE,
            require_s3=True,
        )


def test_resolve_record_staging_prefix_requires_explicit_s3_prefix() -> None:
    reference = _s3_ref(_sample_envelope_bytes())
    with pytest.raises(ValueError, match="requires --record-staging-prefix"):
        resolve_record_staging_prefix(
            None,
            warehouse=WAREHOUSE,
            references=(reference,),
        )


def test_materialize_rejects_missing_s3_staging_prefix(tmp_path: Path) -> None:
    payload = _sample_envelope_bytes()
    reference = _s3_ref(payload)
    store = BoundedObjectStore(
        client=VersionedRaceClient(
            versioned_payload=payload,
            latest_payload=payload,
        )
    )
    with pytest.raises(ValueError, match="requires staging prefix"):
        materialize_record_shards(
            store,
            (reference,),
            scratch_dir=tmp_path,
            max_bytes=len(payload),
        )


def test_materialize_s3_shard_uses_version_id_after_latest_overwrite(
    tmp_path: Path,
) -> None:
    original = _sample_envelope_bytes()
    overwritten = _sample_envelope_bytes().replace(b"Example", b"Tampered")
    reference = _s3_ref(original)

    class UploadClient(VersionedRaceClient):
        def head_object(self, **request):
            key = f"{request['Bucket']}/{request['Key']}"
            if key in self.staged:
                payload = self.staged[key]
                return self._metadata(payload, version_id="staged-version")
            return super().head_object(**request)

    upload_client = UploadClient(
        versioned_payload=original,
        latest_payload=overwritten,
    )
    store = BoundedObjectStore(client=upload_client)
    store.verify(reference, max_bytes=len(original))

    materialized = materialize_record_shards(
        store,
        (reference,),
        staging_prefix=CONTROL_STAGING_PREFIX,
        scratch_dir=tmp_path,
        max_bytes=len(original),
    )[0]

    assert upload_client.get_requests
    assert all(
        request.get("VersionId") == "version-1"
        for request in upload_client.get_requests
    )
    assert materialized.checksum == reference.checksum.value
    assert materialized.size_bytes == reference.size_bytes
    assert materialized.spark_uri.startswith("s3a://")
    staged_key = materialized.spark_uri.removeprefix("s3a://")
    staged_payload = upload_client.staged[staged_key]
    assert staged_payload == original
    assert staged_payload != overwritten


def test_materialize_reuses_existing_staging_with_matching_checksum(
    tmp_path: Path,
) -> None:
    payload = _sample_envelope_bytes()
    reference = _s3_ref(payload)
    client = VersionedRaceClient(versioned_payload=payload, latest_payload=payload)
    store = BoundedObjectStore(client=client)
    first = materialize_record_shards(
        store,
        (reference,),
        staging_prefix=CONTROL_STAGING_PREFIX,
        scratch_dir=tmp_path / "one",
        max_bytes=len(payload),
    )[0]
    second = materialize_record_shards(
        store,
        (reference,),
        staging_prefix=CONTROL_STAGING_PREFIX,
        scratch_dir=tmp_path / "two",
        max_bytes=len(payload),
    )[0]
    assert first.spark_uri == second.spark_uri
    assert first.checksum == second.checksum == reference.checksum.value


def test_materialize_rejects_checksum_mismatch(tmp_path: Path) -> None:
    payload = _sample_envelope_bytes()
    reference = _s3_ref(payload)
    reference = reference.model_copy(
        update={"checksum": Checksum(value="0" * 64)},
    )
    client = VersionedRaceClient(versioned_payload=payload, latest_payload=payload)
    store = BoundedObjectStore(client=client)
    with pytest.raises(ObjectStoreError, match="checksum"):
        store.verify(reference, max_bytes=len(payload))


def test_materialize_rejects_size_mismatch(tmp_path: Path) -> None:
    payload = _sample_envelope_bytes()
    reference = _s3_ref(payload)
    reference = reference.model_copy(update={"size_bytes": len(payload) + 1})
    client = VersionedRaceClient(versioned_payload=payload, latest_payload=payload)
    store = BoundedObjectStore(client=client)
    with pytest.raises(ObjectStoreError, match="size"):
        store.verify(reference, max_bytes=len(payload) + 1)


def test_materialize_file_shard_preserves_percent_encoded_paths(tmp_path: Path) -> None:
    path = tmp_path / "sha256=abc" / "sha256:deadbeef.ndjson"
    path.parent.mkdir(parents=True)
    payload = _sample_envelope_bytes()
    path.write_bytes(payload)
    parsed = urlparse(path.as_uri())
    encoded_path = parsed.path.replace("=", "%3D").replace(":", "%3A")
    encoded = urlunparse((parsed.scheme, parsed.netloc, encoded_path, "", "", ""))
    reference = ObjectRef(
        uri=encoded,
        format="OBJECT_FORMAT_OTHER",
        media_type=(
            "application/vnd.video-media-catalog.connector-record-envelope.v2+ndjson"
        ),
        checksum=Checksum(value=hashlib.sha256(payload).hexdigest()),
        size_bytes=len(payload),
        created_at="2026-09-20T00:00:00Z",
    )
    materialized = materialize_record_shards(
        BoundedObjectStore(client=object()),
        (reference,),
        scratch_dir=tmp_path / "scratch",
        max_bytes=len(payload),
    )[0]
    assert Path(materialized.spark_uri).read_bytes() == payload


def test_scratch_lifecycle_cleans_after_file_materialize_success(
    tmp_path: Path,
) -> None:
    path = tmp_path / "records.ndjson"
    payload = _sample_envelope_bytes()
    path.write_bytes(payload)
    reference = ObjectRef(
        uri=path.as_uri(),
        format="OBJECT_FORMAT_OTHER",
        media_type=(
            "application/vnd.video-media-catalog.connector-record-envelope.v2+ndjson"
        ),
        checksum=Checksum(value=hashlib.sha256(payload).hexdigest()),
        size_bytes=len(payload),
        created_at="2026-09-20T00:00:00Z",
    )
    with TemporaryDirectory(prefix="record-shard-scratch-") as scratch_name:
        scratch = Path(scratch_name)
        materialize_record_shards(
            BoundedObjectStore(client=object()),
            (reference,),
            scratch_dir=scratch,
            max_bytes=len(payload),
        )
        assert any(scratch.iterdir())
        scratch_path = Path(scratch_name)
    assert not scratch_path.exists()


def test_scratch_lifecycle_cleans_after_s3_materialize_failure(
    tmp_path: Path,
) -> None:
    payload = _sample_envelope_bytes()
    reference = _s3_ref(payload).model_copy(
        update={"checksum": Checksum(value="0" * 64)},
    )
    store = BoundedObjectStore(
        client=VersionedRaceClient(
            versioned_payload=payload,
            latest_payload=payload,
        )
    )
    with TemporaryDirectory(prefix="record-shard-scratch-") as scratch_name:
        scratch = Path(scratch_name)
        with pytest.raises(ObjectStoreError, match="checksum"):
            store.verify(reference, max_bytes=len(payload))
            materialize_record_shards(
                store,
                (reference,),
                staging_prefix=CONTROL_STAGING_PREFIX,
                scratch_dir=scratch,
                max_bytes=len(payload),
            )
        scratch_path = Path(scratch_name)
    assert not scratch_path.exists()


def test_materialize_does_not_create_orphan_temp_dirs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mkdtemp_calls: list[str] = []
    original_mkdtemp = tempfile.mkdtemp

    def tracked_mkdtemp(*args: object, **kwargs: object) -> str:
        mkdtemp_calls.append(str(args))
        return original_mkdtemp(*args, **kwargs)

    monkeypatch.setattr(tempfile, "mkdtemp", tracked_mkdtemp)
    payload = _sample_envelope_bytes()
    reference = _s3_ref(payload)
    store = BoundedObjectStore(
        client=VersionedRaceClient(
            versioned_payload=payload,
            latest_payload=payload,
        )
    )
    materialize_record_shards(
        store,
        (reference,),
        staging_prefix=CONTROL_STAGING_PREFIX,
        scratch_dir=tmp_path,
        max_bytes=len(payload),
    )
    assert mkdtemp_calls == []


def test_bind_materialized_shards_rejects_checksum_drift() -> None:
    reference = _s3_ref(_sample_envelope_bytes())
    shard = MaterializedRecordShard(
        source=reference,
        spark_uri="s3a://bucket/staged",
        checksum="0" * 64,
        size_bytes=reference.size_bytes,
        first_envelope_key="sha256:" + ("1" * 64),
        last_envelope_key="sha256:" + ("1" * 64),
    )
    with pytest.raises(ValueError, match="checksum binding mismatch"):
        bind_materialized_shards((shard,), (reference,))


def test_source_silver_rejects_envelope_key_bounds(tmp_path: Path) -> None:
    payload = {
        "id": 1,
        "name": "Example",
        "type": "Scripted",
        "language": "English",
        "updated": 1_700_000_000,
        "genres": ["Drama"],
        "externals": {"imdb": "tt0000001"},
    }
    batch, record_set, _envelope = _capture_record(json.dumps(payload).encode())
    record_set = record_set.model_copy(
        update={"first_envelope_key": "sha256:" + ("0" * 64)},
    )
    with pytest.raises(ValueError, match="first envelope key"):
        build_source_silver_rows(
            registry=build_community_registry(),
            batch=batch,
            record_set=record_set,
            envelopes=[
                build_connector_record_envelope(
                    payload=payload,
                    batch_id=batch.batch_id,
                    source_system_id=batch.source_system_id,
                    source_product_id=batch.source_product_id,
                    source_namespace_id="tvmaze-show",
                    source_record_id="1",
                    source_revision="1700000000",
                    operation=RecordOperation.UPSERT,
                    source_modified_at="2023-11-14T22:13:20Z",
                    observed_at=batch.acquired_at,
                    ingested_at=batch.acquired_at,
                    payload_schema="tvmaze-show-v1",
                    raw_object=batch.raw_objects[0],
                    source_location="/page/0/item/0",
                    policy_id=batch.policy_id,
                    policy_digest=batch.policy_digest,
                )
            ],
        )
