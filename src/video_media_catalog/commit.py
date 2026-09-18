"""ProtoJSON SnapshotSet/OutputCommit publication with commit-last semantics."""

from __future__ import annotations

import base64
import hashlib
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlparse

from video_media_catalog.constants import (
    ALGORITHM_DIGEST,
    ALGORITHM_SPEC_ID,
    CURATED_TABLE_KEYS,
    PRODUCER,
    STAGE,
)
from video_media_catalog.identity import stable_uuid7
from video_media_catalog.models import (
    Checksum,
    ObjectRef,
    OutputCommit,
    SnapshotSet,
    SnapshotTable,
)
from video_media_catalog.runtime_args import RuntimeArguments
from video_media_catalog.storage import atomic_write_bytes, file_uri, local_path

SNAPSHOT_SET_MEDIA_TYPE = "application/vnd.video-governance.snapshot-set+json"
OUTPUT_COMMIT_MEDIA_TYPE = "application/vnd.video-governance.output-commit+json"
MAX_CONTROL_BYTES = 16 * 1024 * 1024


def _now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _etag(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip().strip('"')
    return normalized or None


def _timestamp(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    return None


def _native_sha256(value: str) -> str:
    return base64.b64encode(bytes.fromhex(value)).decode("ascii")


@dataclass(frozen=True)
class PublishedObject:
    uri: str
    payload: bytes
    etag: str | None = None
    object_version: str | None = None
    created_at: str | None = None

    def object_ref(self, media_type: str) -> ObjectRef:
        digest = hashlib.sha256(self.payload).hexdigest()
        return ObjectRef(
            uri=self.uri,
            format="OBJECT_FORMAT_JSON",
            media_type=media_type,
            checksum=Checksum(value=digest),
            size_bytes=len(self.payload),
            etag=self.etag,
            object_version=self.object_version,
            created_at=self.created_at,
        )


class ControlPublisher(Protocol):
    def read_optional(self, name: str) -> PublishedObject | None: ...

    def publish_immutable(self, name: str, payload: bytes) -> PublishedObject: ...


class LocalControlPublisher:
    def __init__(self, root: Path) -> None:
        self.root = root.resolve()

    def read_optional(self, name: str) -> PublishedObject | None:
        path = self.root / name
        if not path.exists():
            return None
        size = path.stat().st_size
        if size > MAX_CONTROL_BYTES:
            raise ValueError(f"control object is too large: {path}")
        payload = path.read_bytes()
        if len(payload) > MAX_CONTROL_BYTES:
            raise ValueError(f"control object is too large: {path}")
        created_at = (
            datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
            .isoformat()
            .replace("+00:00", "Z")
        )
        return PublishedObject(file_uri(path), payload, created_at=created_at)

    def publish_immutable(self, name: str, payload: bytes) -> PublishedObject:
        if len(payload) > MAX_CONTROL_BYTES:
            raise ValueError("control object exceeds maximum size")
        path = self.root / name
        atomic_write_bytes(path, payload)
        published = self.read_optional(name)
        assert published is not None
        return published


class S3ControlPublisher:
    def __init__(
        self,
        root_uri: str,
        *,
        region: str | None = None,
        endpoint_url: str | None = None,
        path_style_access: bool = False,
        client: Any | None = None,
    ) -> None:
        parsed = urlparse(root_uri)
        if parsed.scheme != "s3" or not parsed.netloc:
            raise ValueError("S3 output root must be an s3:// URI")
        self.bucket = parsed.netloc
        self.prefix = parsed.path.strip("/")
        if client is None:
            import boto3
            from botocore.config import Config

            client = boto3.client(
                "s3",
                region_name=region,
                endpoint_url=endpoint_url,
                config=Config(
                    signature_version="s3v4",
                    retries={"max_attempts": 5, "mode": "standard"},
                    s3={
                        "addressing_style": ("path" if path_style_access else "virtual")
                    },
                ),
            )
        self.client = client

    def _key(self, name: str) -> str:
        return "/".join(part for part in (self.prefix, name) if part)

    def _uri(self, name: str) -> str:
        return f"s3://{self.bucket}/{self._key(name)}"

    def read_optional(self, name: str) -> PublishedObject | None:
        try:
            response = self.client.get_object(
                Bucket=self.bucket,
                Key=self._key(name),
                ChecksumMode="ENABLED",
            )
        except Exception as exc:
            metadata = getattr(exc, "response", {})
            code = str(metadata.get("Error", {}).get("Code", ""))
            if code in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise
        body = response.get("Body")
        if body is None or not hasattr(body, "read"):
            raise RuntimeError("S3 control object has no readable body")
        declared = response.get("ContentLength")
        if isinstance(declared, int) and declared > MAX_CONTROL_BYTES:
            body.close()
            raise ValueError(f"control object is too large: {self._uri(name)}")
        try:
            payload = body.read(MAX_CONTROL_BYTES + 1)
        finally:
            body.close()
        if len(payload) > MAX_CONTROL_BYTES:
            raise ValueError(f"control object is too large: {self._uri(name)}")
        digest = hashlib.sha256(payload).hexdigest()
        native = response.get("ChecksumSHA256")
        if native is not None and native != _native_sha256(digest):
            raise RuntimeError("S3 control object native checksum mismatch")
        return PublishedObject(
            self._uri(name),
            payload,
            etag=_etag(response.get("ETag")),
            object_version=response.get("VersionId"),
            created_at=_timestamp(response.get("LastModified")),
        )

    def publish_immutable(self, name: str, payload: bytes) -> PublishedObject:
        if len(payload) > MAX_CONTROL_BYTES:
            raise ValueError("control object exceeds maximum size")
        digest = hashlib.sha256(payload).hexdigest()
        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=self._key(name),
                Body=payload,
                ContentLength=len(payload),
                ContentType="application/json",
                Metadata={"sha256": digest},
                ChecksumSHA256=_native_sha256(digest),
                IfNoneMatch="*",
            )
        except Exception as exc:
            metadata = getattr(exc, "response", {})
            code = str(metadata.get("Error", {}).get("Code", ""))
            status = metadata.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if code not in {
                "PreconditionFailed",
                "ConditionalRequestConflict",
            } and status not in {409, 412}:
                raise
            existing = self.read_optional(name)
            if existing is None or existing.payload != payload:
                raise RuntimeError(
                    f"immutable control object conflicts: {self._uri(name)}"
                ) from exc
            return existing
        published = self.read_optional(name)
        if published is None or published.payload != payload:
            raise RuntimeError("published S3 control object could not be verified")
        return published


def control_publisher(
    output_uri: str,
    *,
    aws_region: str | None = None,
    s3_endpoint: str | None = None,
    s3_path_style_access: bool = False,
) -> ControlPublisher:
    if urlparse(output_uri).scheme == "s3":
        return S3ControlPublisher(
            output_uri,
            region=aws_region,
            endpoint_url=s3_endpoint,
            path_style_access=s3_path_style_access,
        )
    return LocalControlPublisher(local_path(output_uri))


def _metrics(row_counts: dict[str, int]) -> dict[str, str]:
    if set(row_counts) != set(CURATED_TABLE_KEYS):
        raise ValueError("all six curated table counts are required")
    return {
        "algorithm_spec_id": ALGORITHM_SPEC_ID,
        "algorithm_digest": ALGORITHM_DIGEST,
        **{f"{table}_count": str(row_counts[table]) for table in CURATED_TABLE_KEYS},
    }


def _identity(
    runtime: RuntimeArguments,
    *,
    tables: list[SnapshotTable],
    config_digest: str,
) -> dict[str, Any]:
    return {
        "runId": runtime.run_id,
        "jobSpecId": runtime.job_spec_id,
        "tenantId": runtime.tenant_id,
        "attempt": runtime.attempt,
        "inputManifest": runtime.input_manifest.model_dump(
            mode="json", by_alias=True, exclude_none=True
        ),
        "tables": [
            table.model_dump(mode="json", by_alias=True, exclude_none=True)
            for table in tables
        ],
        "algorithmDigest": ALGORITHM_DIGEST,
        "imageDigest": runtime.image_digest,
        "configDigest": config_digest,
    }


def _labels(
    runtime: RuntimeArguments,
    *,
    snapshot_set_id: str,
    config_digest: str,
    metrics: dict[str, str],
) -> dict[str, str]:
    return {
        "stage": STAGE,
        "attempt": str(runtime.attempt),
        "input_manifest_digest": (f"sha256:hex:{runtime.manifest_hash}"),
        "snapshot_set_id": snapshot_set_id,
        "algorithm_spec_id": ALGORITHM_SPEC_ID,
        "algorithm_digest": ALGORITHM_DIGEST,
        "image_digest": runtime.image_digest,
        "config_digest": config_digest,
        **metrics,
    }


def publish_commit(
    *,
    publisher: ControlPublisher,
    runtime: RuntimeArguments,
    tables: list[SnapshotTable],
    row_counts: dict[str, int],
    config_digest: str,
    stage_time: str,
    started_ns: int | None = None,
) -> tuple[SnapshotSet, OutputCommit]:
    """Publish a verified SnapshotSet and then its OutputCommit."""

    ordered_tables = sorted(tables, key=lambda table: table.table_name)
    if {table.table_name.rsplit(".", 1)[-1] for table in ordered_tables} != set(
        row_counts
    ):
        raise ValueError("snapshot tables and row counts do not match")
    for table in ordered_tables:
        expected_count = row_counts[table.table_name.rsplit(".", 1)[-1]]
        if table.record_count != expected_count:
            raise ValueError("snapshot table record count does not match stage count")
    metrics = _metrics(row_counts)
    identity = _identity(
        runtime,
        tables=ordered_tables,
        config_digest=config_digest,
    )
    snapshot_set_id = stable_uuid7(
        kind="snapshot",
        run_id=runtime.run_id,
        identity=identity,
    )
    commit_id = stable_uuid7(
        kind="commit",
        run_id=runtime.run_id,
        identity={**identity, "snapshotSetId": snapshot_set_id},
    )
    expected_labels = _labels(
        runtime,
        snapshot_set_id=snapshot_set_id,
        config_digest=config_digest,
        metrics=metrics,
    )

    existing_snapshot_object = publisher.read_optional("snapshot-set.json")
    existing_commit_object = publisher.read_optional("output.commit.json")
    if existing_commit_object is not None and existing_snapshot_object is None:
        raise RuntimeError("OutputCommit exists without its SnapshotSet")
    snapshot: SnapshotSet
    if existing_snapshot_object is not None:
        existing = SnapshotSet.model_validate_json(existing_snapshot_object.payload)
        expected = SnapshotSet(
            snapshot_set_id=snapshot_set_id,
            compute_run_id=runtime.run_id,
            job_spec_id=runtime.job_spec_id,
            tenant_id=runtime.tenant_id,
            attempt=runtime.attempt,
            input_manifest=runtime.input_manifest,
            tables=ordered_tables,
            created_at=existing.created_at,
            output_count=row_counts["catalog_entity"],
            image_digest=runtime.image_digest,
            config_digest=config_digest,
            metrics=metrics,
        )
        if existing != expected:
            raise RuntimeError("existing SnapshotSet conflicts with this commit")
        snapshot = existing
        snapshot_object = existing_snapshot_object
    else:
        snapshot = SnapshotSet(
            snapshot_set_id=snapshot_set_id,
            compute_run_id=runtime.run_id,
            job_spec_id=runtime.job_spec_id,
            tenant_id=runtime.tenant_id,
            attempt=runtime.attempt,
            input_manifest=runtime.input_manifest,
            tables=ordered_tables,
            created_at=stage_time,
            output_count=row_counts["catalog_entity"],
            image_digest=runtime.image_digest,
            config_digest=config_digest,
            metrics=metrics,
        )
        snapshot_object = publisher.publish_immutable(
            "snapshot-set.json", snapshot.json_bytes()
        )
    snapshot_ref = snapshot_object.object_ref(SNAPSHOT_SET_MEDIA_TYPE)

    if existing_commit_object is not None:
        existing_commit = OutputCommit.model_validate_json(
            existing_commit_object.payload
        )
        expected_commit = OutputCommit(
            commit_id=commit_id,
            compute_run_id=runtime.run_id,
            job_spec_id=runtime.job_spec_id,
            tenant_id=runtime.tenant_id,
            output_manifest=snapshot_ref,
            committed_at=existing_commit.committed_at,
            output_count=snapshot.output_count,
            total_duration_us=existing_commit.total_duration_us,
            producer=PRODUCER,
            labels=expected_labels,
        )
        if existing_commit != expected_commit:
            raise RuntimeError("existing OutputCommit conflicts with this commit")
        return snapshot, existing_commit

    elapsed_us = (
        max(0, (time.monotonic_ns() - started_ns) // 1_000)
        if started_ns is not None
        else 0
    )
    commit = OutputCommit(
        commit_id=commit_id,
        compute_run_id=runtime.run_id,
        job_spec_id=runtime.job_spec_id,
        tenant_id=runtime.tenant_id,
        output_manifest=snapshot_ref,
        committed_at=_now(),
        output_count=snapshot.output_count,
        total_duration_us=elapsed_us,
        producer=PRODUCER,
        labels=expected_labels,
    )
    publisher.publish_immutable("output.commit.json", commit.json_bytes())
    return snapshot, commit
