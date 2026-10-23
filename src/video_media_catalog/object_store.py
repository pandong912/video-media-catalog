"""Bounded local/S3 object transfer for runtime extraction."""

from __future__ import annotations

import base64
import hashlib
import os
import re
import tempfile
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import quote, unquote, urlsplit

from video_media_catalog.execution_contracts import ObjectProvider, ObjectRefV2
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.storage import (
    ImmutableObjectConflictError,
    digest_file,
    local_path,
    publish_file_immutable,
)

TRANSFER_CHUNK_BYTES = 1024 * 1024
MAX_SINGLE_PUT_BYTES = 5 * 1024 * 1024 * 1024


class ObjectStoreError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class MaterializedObject:
    path: Path
    size_bytes: int
    sha256: str


@dataclass(frozen=True)
class UploadResult:
    object_ref: ObjectRef
    reused: bool


@dataclass(frozen=True)
class ObjectRefV2WriteTarget:
    object_ref_id: str
    tenant_id: str
    project_id: str
    data_plane_binding_id: str
    data_plane_id: str
    provider: ObjectProvider
    uri: str
    object_key: str
    format: str
    media_type: str
    schema_digest: str | None
    encryption_context_digest: str
    created_at: str


@dataclass(frozen=True)
class ObjectRefV2UploadResult:
    object_ref: ObjectRefV2
    reused: bool


class RuntimeObjectStore(Protocol):
    def head(
        self,
        object_ref: ObjectRef,
        *,
        max_bytes: int,
        require_immutable_metadata: bool = False,
    ) -> ObjectRef: ...

    def verify(
        self,
        object_ref: ObjectRef,
        *,
        max_bytes: int,
    ) -> None: ...

    def read_range(
        self,
        object_ref: ObjectRef,
        *,
        offset: int,
        length: int,
        max_bytes: int,
    ) -> bytes: ...

    def download(
        self,
        object_ref: ObjectRef,
        destination: Path,
        *,
        max_bytes: int,
    ) -> MaterializedObject: ...

    def upload_file(
        self,
        source: Path,
        destination_uri: str,
        *,
        media_type: str,
        object_format: str,
        max_bytes: int,
    ) -> UploadResult: ...

    def upload_bytes(
        self,
        payload: bytes,
        destination_uri: str,
        *,
        media_type: str,
        object_format: str,
        max_bytes: int,
    ) -> UploadResult: ...

    def publish_file_conditional(
        self,
        source: Path,
        destination_uri: str,
        *,
        media_type: str,
        object_format: str,
        max_bytes: int,
    ) -> UploadResult: ...

    def publish_bytes_conditional(
        self,
        payload: bytes,
        destination_uri: str,
        *,
        media_type: str,
        object_format: str,
        max_bytes: int,
    ) -> UploadResult: ...


def conditional_publish_bytes(
    store: RuntimeObjectStore,
    payload: bytes,
    destination_uri: str,
    *,
    media_type: str,
    object_format: str,
    max_bytes: int,
) -> UploadResult:
    """Conditionally publish bytes and verify the returned immutable reference."""

    publisher = getattr(store, "publish_bytes_conditional", None)
    if callable(publisher):
        result = publisher(
            payload,
            destination_uri,
            media_type=media_type,
            object_format=object_format,
            max_bytes=max_bytes,
        )
    else:
        result = store.upload_bytes(
            payload,
            destination_uri,
            media_type=media_type,
            object_format=object_format,
            max_bytes=max_bytes,
        )
    expected_sha256 = hashlib.sha256(payload).hexdigest()
    reference = result.object_ref
    destination_scheme = urlsplit(destination_uri).scheme
    if destination_scheme == "s3":
        same_destination = S3Location.parse(reference.uri) == S3Location.parse(
            destination_uri
        )
    else:
        same_destination = (
            local_path(reference.uri).resolve() == local_path(destination_uri).resolve()
        )
    if (
        not same_destination
        or reference.size_bytes != len(payload)
        or reference.checksum.value != expected_sha256
        or reference.media_type != media_type
        or reference.format != object_format
    ):
        raise ObjectStoreError(
            "PUBLISH_RESULT_MISMATCH",
            "object store returned a reference that does not bind published bytes",
        )
    if destination_scheme == "s3" and (
        not reference.etag or not reference.object_version
    ):
        raise ObjectStoreError(
            "IMMUTABLE_METADATA_REQUIRED",
            "conditional S3 publication requires ETag and object version",
        )
    return result


def _etag(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip().strip('"')
    return normalized or None


def _timestamp(value: Any) -> str | None:
    if isinstance(value, datetime):
        current = value.astimezone(UTC)
        return current.isoformat().replace("+00:00", "Z")
    return None


def _native_sha256(value: str) -> str:
    return base64.b64encode(bytes.fromhex(value)).decode("ascii")


@dataclass(frozen=True)
class S3Location:
    bucket: str
    key: str

    @classmethod
    def parse(cls, uri: str) -> S3Location:
        parsed = urlsplit(uri)
        if (
            parsed.scheme != "s3"
            or not parsed.netloc
            or not parsed.path.lstrip("/")
            or parsed.query
            or parsed.fragment
            or parsed.username is not None
            or parsed.password is not None
        ):
            raise ObjectStoreError(
                "INVALID_S3_URI",
                "S3 URI must be s3://bucket/key without credentials or query",
            )
        return cls(parsed.netloc, unquote(parsed.path.lstrip("/")))

    @property
    def uri(self) -> str:
        return f"s3://{self.bucket}/{quote(self.key, safe='/=:@!$&()*+,;~-._')}"


@dataclass(frozen=True)
class OssLocation:
    bucket: str
    key: str

    @classmethod
    def parse(cls, uri: str) -> OssLocation:
        parsed = urlsplit(uri)
        if (
            parsed.scheme != "oss"
            or not parsed.netloc
            or not parsed.path.lstrip("/")
            or parsed.query
            or parsed.fragment
            or parsed.username is not None
            or parsed.password is not None
            or "%" in parsed.path
        ):
            raise ObjectStoreError(
                "INVALID_OSS_URI",
                "OSS URI must be oss://bucket/key without credentials or query",
            )
        return cls(parsed.netloc, parsed.path.lstrip("/"))

    @property
    def uri(self) -> str:
        return f"oss://{self.bucket}/{self.key}"


class BoundedObjectStore:
    """Uses boto3's default credential chain and bounded disk materialization."""

    def __init__(
        self,
        *,
        region: str | None = None,
        endpoint_url: str | None = None,
        path_style_access: bool = False,
        client: Any | None = None,
    ) -> None:
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

    def verify(
        self,
        object_ref: ObjectRef,
        *,
        max_bytes: int,
    ) -> None:
        """Verify immutable object metadata without materializing S3 bytes."""

        self.head(object_ref, max_bytes=max_bytes)

    def head(
        self,
        object_ref: ObjectRef,
        *,
        max_bytes: int,
        require_immutable_metadata: bool = False,
    ) -> ObjectRef:
        """Verify object metadata and return the exact immutable S3 identity."""

        if max_bytes < 1:
            raise ValueError("max_bytes must be positive")
        if urlsplit(object_ref.uri).scheme == "file":
            if require_immutable_metadata:
                raise ObjectStoreError(
                    "IMMUTABLE_METADATA_REQUIRED",
                    "immutable S3 ETag and object version are required",
                )
            if object_ref.etag is not None or object_ref.object_version is not None:
                raise ObjectStoreError(
                    "UNVERIFIABLE_LOCAL_METADATA",
                    "file:// inputs cannot verify ETag or object version",
                )
            path = local_path(object_ref.uri).resolve()
            digest, size = digest_file(path)
            if size > max_bytes:
                raise ObjectStoreError(
                    "OBJECT_TOO_LARGE",
                    "local object exceeds configured limit",
                )
            if (
                size != object_ref.size_bytes
                or digest.removeprefix("sha256:") != object_ref.checksum.value
            ):
                raise ObjectStoreError(
                    "OBJECT_CHECKSUM_MISMATCH",
                    "local object differs from immutable declaration",
                )
            return object_ref

        location = S3Location.parse(object_ref.uri)
        request: dict[str, Any] = {
            "Bucket": location.bucket,
            "Key": location.key,
            "ChecksumMode": "ENABLED",
        }
        if object_ref.object_version:
            request["VersionId"] = object_ref.object_version
        response = self.client.head_object(**request)
        size = int(response.get("ContentLength", -1))
        if size < 0 or size != object_ref.size_bytes:
            raise ObjectStoreError(
                "OBJECT_SIZE_MISMATCH",
                "S3 object size differs from immutable declaration",
            )
        if size > max_bytes:
            raise ObjectStoreError(
                "OBJECT_TOO_LARGE",
                "S3 object exceeds configured limit",
            )
        expected_sha256 = object_ref.checksum.value
        native = response.get("ChecksumSHA256")
        metadata_sha256 = (response.get("Metadata") or {}).get("sha256")
        if native is not None:
            checksum_matches = native == _native_sha256(expected_sha256)
        elif metadata_sha256 is not None:
            checksum_matches = metadata_sha256.lower() == expected_sha256
        else:
            raise ObjectStoreError(
                "OBJECT_CHECKSUM_UNAVAILABLE",
                "S3 object exposes no verifiable SHA-256 metadata",
            )
        if not checksum_matches:
            raise ObjectStoreError(
                "OBJECT_CHECKSUM_MISMATCH",
                "S3 object checksum differs from immutable declaration",
            )
        if object_ref.etag is not None and _etag(response.get("ETag")) != _etag(
            object_ref.etag
        ):
            raise ObjectStoreError(
                "OBJECT_ETAG_MISMATCH",
                "S3 ETag differs from immutable declaration",
            )
        if (
            object_ref.object_version is not None
            and response.get("VersionId") != object_ref.object_version
        ):
            raise ObjectStoreError(
                "OBJECT_VERSION_MISMATCH",
                "S3 version differs from immutable declaration",
            )
        actual_etag = _etag(response.get("ETag"))
        actual_version = response.get("VersionId")
        if require_immutable_metadata and (actual_etag is None or not actual_version):
            raise ObjectStoreError(
                "IMMUTABLE_METADATA_REQUIRED",
                "S3 object must expose an ETag and object version",
            )
        return object_ref.model_copy(
            update={
                "etag": actual_etag,
                "object_version": actual_version,
                "created_at": _timestamp(response.get("LastModified"))
                or object_ref.created_at,
            }
        )

    def read_range(
        self,
        object_ref: ObjectRef,
        *,
        offset: int,
        length: int,
        max_bytes: int,
    ) -> bytes:
        """Read one exact, strictly bounded range from an immutable S3 object."""

        if max_bytes < 1:
            raise ValueError("max_bytes must be positive")
        if offset < 0:
            raise ValueError("offset must not be negative")
        if length < 1:
            raise ValueError("length must be positive")
        if length > max_bytes:
            raise ObjectStoreError(
                "RANGE_TOO_LARGE",
                "requested S3 range exceeds configured limit",
            )
        if offset + length > object_ref.size_bytes:
            raise ObjectStoreError(
                "INVALID_OBJECT_RANGE",
                "requested S3 range exceeds declared object size",
            )
        if object_ref.etag is None or object_ref.object_version is None:
            raise ObjectStoreError(
                "IMMUTABLE_METADATA_REQUIRED",
                "bounded S3 range reads require ETag and object version",
            )
        location = S3Location.parse(object_ref.uri)
        end = offset + length - 1
        response = self.client.get_object(
            Bucket=location.bucket,
            Key=location.key,
            VersionId=object_ref.object_version,
            IfMatch=f'"{_etag(object_ref.etag)}"',
            Range=f"bytes={offset}-{end}",
            ChecksumMode="ENABLED",
        )
        body = response.get("Body")
        if body is None or not hasattr(body, "read"):
            raise ObjectStoreError(
                "OBJECT_RANGE_FAILED",
                "S3 range response has no readable body",
            )
        try:
            if response.get("ContentLength") != length:
                raise ObjectStoreError(
                    "OBJECT_RANGE_SIZE_MISMATCH",
                    "S3 range response length differs from request",
                )
            content_range = re.fullmatch(
                r"bytes (\d+)-(\d+)/(\d+)",
                str(response.get("ContentRange", "")),
            )
            if content_range is None or tuple(
                int(value) for value in content_range.groups()
            ) != (offset, end, object_ref.size_bytes):
                raise ObjectStoreError(
                    "OBJECT_RANGE_MISMATCH",
                    "S3 Content-Range differs from immutable declaration",
                )
            if _etag(response.get("ETag")) != _etag(object_ref.etag):
                raise ObjectStoreError(
                    "OBJECT_ETAG_MISMATCH",
                    "S3 range ETag differs from immutable declaration",
                )
            if response.get("VersionId") != object_ref.object_version:
                raise ObjectStoreError(
                    "OBJECT_VERSION_MISMATCH",
                    "S3 range version differs from immutable declaration",
                )
            payload = bytearray()
            while len(payload) < length:
                chunk = body.read(min(TRANSFER_CHUNK_BYTES, length - len(payload)))
                if not chunk:
                    break
                payload.extend(chunk)
            if len(payload) != length or body.read(1):
                raise ObjectStoreError(
                    "OBJECT_RANGE_SIZE_MISMATCH",
                    "S3 range body differs from declared response length",
                )
            return bytes(payload)
        finally:
            body.close()

    def download(
        self,
        object_ref: ObjectRef,
        destination: Path,
        *,
        max_bytes: int,
    ) -> MaterializedObject:
        if max_bytes < 1:
            raise ValueError("max_bytes must be positive")
        scheme = urlsplit(object_ref.uri).scheme
        if scheme == "file":
            return self._download_file(object_ref, destination, max_bytes=max_bytes)
        location = S3Location.parse(object_ref.uri)
        request: dict[str, Any] = {
            "Bucket": location.bucket,
            "Key": location.key,
            "ChecksumMode": "ENABLED",
        }
        if object_ref.object_version:
            request["VersionId"] = object_ref.object_version
        response = self.client.get_object(**request)
        body = response.get("Body")
        if body is None or not hasattr(body, "read"):
            raise ObjectStoreError(
                "OBJECT_DOWNLOAD_FAILED", "S3 response has no readable body"
            )
        declared = response.get("ContentLength")
        if isinstance(declared, int) and declared > max_bytes:
            body.close()
            raise ObjectStoreError(
                "OBJECT_TOO_LARGE", "S3 object exceeds configured limit"
            )
        temporary: Path | None = None
        digest = hashlib.sha256()
        size = 0
        try:
            destination.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(
                mode="wb",
                dir=destination.parent,
                prefix=f".{destination.name}.",
                suffix=".download",
                delete=False,
            ) as handle:
                temporary = Path(handle.name)
                while chunk := body.read(TRANSFER_CHUNK_BYTES):
                    size += len(chunk)
                    if size > max_bytes:
                        raise ObjectStoreError(
                            "OBJECT_TOO_LARGE",
                            "S3 object exceeds configured limit",
                        )
                    handle.write(chunk)
                    digest.update(chunk)
                handle.flush()
                os.fsync(handle.fileno())
            actual = digest.hexdigest()
            self._verify_download(object_ref, response, size, actual)
            os.replace(temporary, destination)
            temporary = None
            return MaterializedObject(destination, size, actual)
        finally:
            body.close()
            if temporary is not None:
                with suppress(OSError):
                    temporary.unlink(missing_ok=True)

    def _download_file(
        self,
        object_ref: ObjectRef,
        destination: Path,
        *,
        max_bytes: int,
    ) -> MaterializedObject:
        if object_ref.etag is not None or object_ref.object_version is not None:
            raise ObjectStoreError(
                "UNVERIFIABLE_LOCAL_METADATA",
                "file:// inputs cannot verify ETag or object version",
            )
        source = local_path(object_ref.uri).resolve()
        if source.stat().st_size > max_bytes:
            raise ObjectStoreError(
                "OBJECT_TOO_LARGE", "local object exceeds configured limit"
            )
        destination.parent.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha256()
        size = 0
        temporary: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb",
                dir=destination.parent,
                prefix=f".{destination.name}.",
                suffix=".download",
                delete=False,
            ) as writer:
                temporary = Path(writer.name)
                with source.open("rb") as reader:
                    while chunk := reader.read(TRANSFER_CHUNK_BYTES):
                        size += len(chunk)
                        if size > max_bytes:
                            raise ObjectStoreError(
                                "OBJECT_TOO_LARGE",
                                "local object exceeds configured limit",
                            )
                        digest.update(chunk)
                        writer.write(chunk)
                writer.flush()
                os.fsync(writer.fileno())
            actual = digest.hexdigest()
            self._verify_download(object_ref, {}, size, actual)
            os.replace(temporary, destination)
            temporary = None
            return MaterializedObject(destination, size, actual)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    @staticmethod
    def _verify_download(
        expected: ObjectRef,
        response: dict[str, Any],
        size: int,
        sha256: str,
    ) -> None:
        if expected.size_bytes != size:
            raise ObjectStoreError(
                "OBJECT_SIZE_MISMATCH",
                "downloaded size differs from immutable declaration",
            )
        if expected.checksum.value != sha256:
            raise ObjectStoreError(
                "OBJECT_CHECKSUM_MISMATCH",
                "downloaded SHA-256 differs from immutable declaration",
            )
        native = response.get("ChecksumSHA256")
        if native is not None and native != _native_sha256(sha256):
            raise ObjectStoreError(
                "OBJECT_CHECKSUM_MISMATCH",
                "S3 native SHA-256 differs from immutable declaration",
            )
        if expected.etag is not None and _etag(response.get("ETag")) != _etag(
            expected.etag
        ):
            raise ObjectStoreError(
                "OBJECT_ETAG_MISMATCH",
                "S3 ETag differs from immutable declaration",
            )
        if (
            expected.object_version is not None
            and response.get("VersionId") != expected.object_version
        ):
            raise ObjectStoreError(
                "OBJECT_VERSION_MISMATCH",
                "S3 version differs from immutable declaration",
            )

    def upload_bytes(
        self,
        payload: bytes,
        destination_uri: str,
        *,
        media_type: str,
        object_format: str,
        max_bytes: int,
    ) -> UploadResult:
        return self.publish_bytes_conditional(
            payload,
            destination_uri,
            media_type=media_type,
            object_format=object_format,
            max_bytes=max_bytes,
        )

    def publish_bytes_conditional(
        self,
        payload: bytes,
        destination_uri: str,
        *,
        media_type: str,
        object_format: str,
        max_bytes: int,
    ) -> UploadResult:
        if len(payload) > max_bytes:
            raise ObjectStoreError(
                "OBJECT_TOO_LARGE", "output exceeds configured limit"
            )
        with tempfile.TemporaryDirectory(prefix="media-catalog-upload-") as directory:
            path = Path(directory) / "object"
            path.write_bytes(payload)
            return self.publish_file_conditional(
                path,
                destination_uri,
                media_type=media_type,
                object_format=object_format,
                max_bytes=max_bytes,
            )

    def upload_file(
        self,
        source: Path,
        destination_uri: str,
        *,
        media_type: str,
        object_format: str,
        max_bytes: int,
    ) -> UploadResult:
        return self.publish_file_conditional(
            source,
            destination_uri,
            media_type=media_type,
            object_format=object_format,
            max_bytes=max_bytes,
        )

    def publish_file_conditional(
        self,
        source: Path,
        destination_uri: str,
        *,
        media_type: str,
        object_format: str,
        max_bytes: int,
    ) -> UploadResult:
        sha256_prefixed, size = digest_file(source)
        sha256 = sha256_prefixed.removeprefix("sha256:")
        if size > min(max_bytes, MAX_SINGLE_PUT_BYTES):
            raise ObjectStoreError(
                "OBJECT_TOO_LARGE",
                "immutable single-part output exceeds configured limit",
            )
        if urlsplit(destination_uri).scheme != "s3":
            return self._upload_file_local(
                source,
                destination_uri,
                media_type=media_type,
                object_format=object_format,
                sha256=sha256,
                size=size,
            )
        location = S3Location.parse(destination_uri)
        with source.open("rb") as body:
            try:
                response = self.client.put_object(
                    Bucket=location.bucket,
                    Key=location.key,
                    Body=body,
                    ContentLength=size,
                    ContentType=media_type,
                    Metadata={"sha256": sha256},
                    ChecksumSHA256=_native_sha256(sha256),
                    IfNoneMatch="*",
                )
            except Exception as exc:
                if not self._is_precondition_failure(exc):
                    raise
                existing = self._verify_existing_content(
                    location,
                    expected_size=size,
                    expected_sha256=sha256,
                    max_bytes=max_bytes,
                )
                return UploadResult(
                    self._object_ref(
                        location,
                        media_type,
                        object_format,
                        size,
                        sha256,
                        existing,
                    ),
                    True,
                )
        head_request: dict[str, Any] = {
            "Bucket": location.bucket,
            "Key": location.key,
            "ChecksumMode": "ENABLED",
        }
        if response.get("VersionId"):
            head_request["VersionId"] = response["VersionId"]
        metadata = self.client.head_object(**head_request)
        metadata = {**response, **metadata}
        return UploadResult(
            self._object_ref(
                location,
                media_type,
                object_format,
                size,
                sha256,
                metadata,
            ),
            False,
        )

    def _upload_file_local(
        self,
        source: Path,
        destination_uri: str,
        *,
        media_type: str,
        object_format: str,
        sha256: str,
        size: int,
    ) -> UploadResult:
        destination = local_path(destination_uri).resolve()
        destination.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            dir=destination.parent,
            delete=False,
        ) as handle:
            temporary = Path(handle.name)
        try:
            with source.open("rb") as reader, temporary.open("wb") as writer:
                while chunk := reader.read(TRANSFER_CHUNK_BYTES):
                    writer.write(chunk)
            try:
                reused = not publish_file_immutable(temporary, destination)
            except ImmutableObjectConflictError as exc:
                raise ObjectStoreError(
                    "IMMUTABLE_OBJECT_CONFLICT",
                    "destination exists with different immutable content",
                ) from exc
        finally:
            temporary.unlink(missing_ok=True)
        created = (
            datetime.fromtimestamp(destination.stat().st_mtime, tz=UTC)
            .isoformat()
            .replace("+00:00", "Z")
        )
        return UploadResult(
            ObjectRef(
                uri=destination.as_uri(),
                format=object_format,
                media_type=media_type,
                checksum=Checksum(value=sha256),
                size_bytes=size,
                created_at=created,
            ),
            reused,
        )

    def _verify_existing_content(
        self,
        location: S3Location,
        *,
        expected_size: int,
        expected_sha256: str,
        max_bytes: int,
    ) -> dict[str, Any]:
        response = self.client.get_object(
            Bucket=location.bucket,
            Key=location.key,
            ChecksumMode="ENABLED",
        )
        body = response.get("Body")
        if body is None or not hasattr(body, "read"):
            raise ObjectStoreError(
                "IMMUTABLE_OBJECT_CONFLICT",
                "existing object cannot be verified",
            )
        declared_size = response.get("ContentLength")
        if isinstance(declared_size, int) and declared_size != expected_size:
            body.close()
            raise ObjectStoreError(
                "IMMUTABLE_OBJECT_CONFLICT",
                "destination exists with different immutable content",
            )
        digest = hashlib.sha256()
        size = 0
        try:
            while chunk := body.read(TRANSFER_CHUNK_BYTES):
                size += len(chunk)
                if size > max_bytes:
                    raise ObjectStoreError(
                        "IMMUTABLE_OBJECT_CONFLICT",
                        "destination exists with different immutable content",
                    )
                digest.update(chunk)
        finally:
            body.close()
        if size != expected_size or digest.hexdigest() != expected_sha256:
            raise ObjectStoreError(
                "IMMUTABLE_OBJECT_CONFLICT",
                "destination exists with different immutable content",
            )
        return response

    @staticmethod
    def _is_precondition_failure(exc: BaseException) -> bool:
        response = getattr(exc, "response", {})
        code = str(response.get("Error", {}).get("Code", ""))
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        return code in {
            "PreconditionFailed",
            "ConditionalRequestConflict",
        } or status in {409, 412}

    @staticmethod
    def _object_ref(
        location: S3Location,
        media_type: str,
        object_format: str,
        size: int,
        sha256: str,
        response: dict[str, Any],
    ) -> ObjectRef:
        return ObjectRef(
            uri=location.uri,
            format=object_format,
            media_type=media_type,
            checksum=Checksum(value=sha256),
            size_bytes=size,
            etag=_etag(response.get("ETag")),
            object_version=response.get("VersionId"),
            created_at=_timestamp(response.get("LastModified")),
        )


class ExactObjectStoreV2:
    """Exact-version S3/OSS operations for ObjectRef v2."""

    def __init__(
        self,
        *,
        s3_client: Any | None = None,
        oss_client: Any | None = None,
        aws_region: str | None = None,
    ) -> None:
        if s3_client is None and aws_region is not None:
            import boto3

            s3_client = boto3.client("s3", region_name=aws_region)
        self.s3_client = s3_client
        self.oss_client = oss_client

    def verify(
        self,
        object_ref: ObjectRefV2,
        *,
        max_bytes: int,
    ) -> ObjectRefV2:
        if max_bytes < 1:
            raise ValueError("max_bytes must be positive")
        response = self._head(object_ref)
        self._verify_metadata(object_ref, response, max_bytes=max_bytes)
        return object_ref

    def download(
        self,
        object_ref: ObjectRefV2,
        destination: Path,
        *,
        max_bytes: int,
    ) -> MaterializedObject:
        if max_bytes < 1:
            raise ValueError("max_bytes must be positive")
        response = self._get(object_ref)
        self._verify_metadata(object_ref, response, max_bytes=max_bytes)
        body = response.get("Body") or response.get("body")
        if body is None or not hasattr(body, "read"):
            raise ObjectStoreError(
                "OBJECT_DOWNLOAD_FAILED",
                "provider response has no readable body",
            )
        temporary: Path | None = None
        digest = hashlib.sha256()
        size = 0
        try:
            destination.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(
                mode="wb",
                dir=destination.parent,
                prefix=f".{destination.name}.",
                suffix=".download",
                delete=False,
            ) as writer:
                temporary = Path(writer.name)
                while chunk := body.read(TRANSFER_CHUNK_BYTES):
                    size += len(chunk)
                    if size > max_bytes:
                        raise ObjectStoreError(
                            "OBJECT_TOO_LARGE",
                            "provider object exceeds configured limit",
                        )
                    digest.update(chunk)
                    writer.write(chunk)
                writer.flush()
                os.fsync(writer.fileno())
            actual = digest.hexdigest()
            if size != object_ref.size_bytes or actual != object_ref.checksum:
                raise ObjectStoreError(
                    "OBJECT_CHECKSUM_MISMATCH",
                    "downloaded bytes differ from exact ObjectRef v2",
                )
            os.replace(temporary, destination)
            temporary = None
            return MaterializedObject(destination, size, actual)
        finally:
            body.close()
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    def publish_bytes(
        self,
        payload: bytes,
        target: ObjectRefV2WriteTarget,
        *,
        max_bytes: int,
    ) -> ObjectRefV2UploadResult:
        if max_bytes < 1:
            raise ValueError("max_bytes must be positive")
        if not payload or len(payload) > max_bytes:
            raise ObjectStoreError(
                "OBJECT_TOO_LARGE",
                "ObjectRef v2 payload must be non-empty and within its limit",
            )
        checksum = hashlib.sha256(payload).hexdigest()
        reused = False
        try:
            response = self._put(payload, target, checksum=checksum)
        except Exception as exc:
            if not BoundedObjectStore._is_precondition_failure(exc):
                raise
            reused = True
            response = self._head_target(target)
        version_token = self._version(response)
        etag = _etag(response.get("ETag") or response.get("etag"))
        if not version_token or not etag:
            raise ObjectStoreError(
                "IMMUTABLE_METADATA_REQUIRED",
                "ObjectRef v2 publication requires exact version and ETag",
            )
        reference = ObjectRefV2.create(
            object_ref_id=target.object_ref_id,
            tenant_id=target.tenant_id,
            project_id=target.project_id,
            data_plane_binding_id=target.data_plane_binding_id,
            data_plane_id=target.data_plane_id,
            provider=target.provider,
            uri=target.uri,
            object_key=target.object_key,
            format=target.format,  # type: ignore[arg-type]
            media_type=target.media_type,
            checksum=checksum,
            size_bytes=len(payload),
            etag=etag,
            version_token=version_token,
            schema_digest=target.schema_digest,
            encryption_context_digest=target.encryption_context_digest,
            created_at=target.created_at,
        )
        verified = self._head(reference)
        self._verify_metadata(reference, verified, max_bytes=max_bytes)
        return ObjectRefV2UploadResult(reference, reused)

    def _head(self, object_ref: ObjectRefV2) -> dict[str, Any]:
        if object_ref.provider == "AWS_S3":
            if self.s3_client is None:
                raise ObjectStoreError(
                    "PROVIDER_CLIENT_UNAVAILABLE",
                    "AWS S3 client is not configured",
                )
            location = S3Location.parse(object_ref.uri)
            return dict(
                self.s3_client.head_object(
                    Bucket=location.bucket,
                    Key=location.key,
                    VersionId=object_ref.version_token,
                    ChecksumMode="ENABLED",
                )
            )
        if self.oss_client is None:
            raise ObjectStoreError(
                "PROVIDER_CLIENT_UNAVAILABLE",
                "Alibaba OSS client is not configured",
            )
        location = OssLocation.parse(object_ref.uri)
        return dict(
            self.oss_client.head_object(
                bucket=location.bucket,
                key=location.key,
                version_id=object_ref.version_token,
            )
        )

    def _get(self, object_ref: ObjectRefV2) -> dict[str, Any]:
        if object_ref.provider == "AWS_S3":
            if self.s3_client is None:
                raise ObjectStoreError(
                    "PROVIDER_CLIENT_UNAVAILABLE",
                    "AWS S3 client is not configured",
                )
            location = S3Location.parse(object_ref.uri)
            return dict(
                self.s3_client.get_object(
                    Bucket=location.bucket,
                    Key=location.key,
                    VersionId=object_ref.version_token,
                    ChecksumMode="ENABLED",
                    IfMatch=f'"{_etag(object_ref.etag)}"',
                )
            )
        if self.oss_client is None:
            raise ObjectStoreError(
                "PROVIDER_CLIENT_UNAVAILABLE",
                "Alibaba OSS client is not configured",
            )
        location = OssLocation.parse(object_ref.uri)
        return dict(
            self.oss_client.get_object(
                bucket=location.bucket,
                key=location.key,
                version_id=object_ref.version_token,
                if_match=object_ref.etag,
            )
        )

    def _put(
        self,
        payload: bytes,
        target: ObjectRefV2WriteTarget,
        *,
        checksum: str,
    ) -> dict[str, Any]:
        if target.provider == "AWS_S3":
            if self.s3_client is None:
                raise ObjectStoreError(
                    "PROVIDER_CLIENT_UNAVAILABLE",
                    "AWS S3 client is not configured",
                )
            location = S3Location.parse(target.uri)
            if location.key != target.object_key:
                raise ObjectStoreError(
                    "OBJECT_KEY_MISMATCH",
                    "target URI does not bind objectKey",
                )
            response = dict(
                self.s3_client.put_object(
                    Bucket=location.bucket,
                    Key=location.key,
                    Body=payload,
                    ContentLength=len(payload),
                    ContentType=target.media_type,
                    Metadata={"sha256": checksum},
                    ChecksumSHA256=_native_sha256(checksum),
                    IfNoneMatch="*",
                )
            )
            head_request: dict[str, Any] = {
                "Bucket": location.bucket,
                "Key": location.key,
                "ChecksumMode": "ENABLED",
            }
            if response.get("VersionId"):
                head_request["VersionId"] = response["VersionId"]
            return {**response, **dict(self.s3_client.head_object(**head_request))}
        if self.oss_client is None:
            raise ObjectStoreError(
                "PROVIDER_CLIENT_UNAVAILABLE",
                "Alibaba OSS client is not configured",
            )
        location = OssLocation.parse(target.uri)
        if location.key != target.object_key:
            raise ObjectStoreError(
                "OBJECT_KEY_MISMATCH",
                "target URI does not bind objectKey",
            )
        response = dict(
            self.oss_client.put_object(
                bucket=location.bucket,
                key=location.key,
                data=payload,
                content_type=target.media_type,
                metadata={"sha256": checksum},
                forbid_overwrite=True,
            )
        )
        version = self._version(response)
        return {
            **response,
            **dict(
                self.oss_client.head_object(
                    bucket=location.bucket,
                    key=location.key,
                    version_id=version,
                )
            ),
        }

    def _head_target(self, target: ObjectRefV2WriteTarget) -> dict[str, Any]:
        if target.provider == "AWS_S3":
            if self.s3_client is None:
                raise ObjectStoreError(
                    "PROVIDER_CLIENT_UNAVAILABLE",
                    "AWS S3 client is not configured",
                )
            location = S3Location.parse(target.uri)
            return dict(
                self.s3_client.head_object(
                    Bucket=location.bucket,
                    Key=location.key,
                    ChecksumMode="ENABLED",
                )
            )
        if self.oss_client is None:
            raise ObjectStoreError(
                "PROVIDER_CLIENT_UNAVAILABLE",
                "Alibaba OSS client is not configured",
            )
        location = OssLocation.parse(target.uri)
        return dict(
            self.oss_client.head_object(
                bucket=location.bucket,
                key=location.key,
                version_id=None,
            )
        )

    @staticmethod
    def _version(response: dict[str, Any]) -> str:
        return str(
            response.get("VersionId")
            or response.get("versionId")
            or response.get("version_id")
            or ""
        )

    @classmethod
    def _verify_metadata(
        cls,
        expected: ObjectRefV2,
        response: dict[str, Any],
        *,
        max_bytes: int,
    ) -> None:
        size = int(response.get("ContentLength", response.get("content_length", -1)))
        if size != expected.size_bytes:
            raise ObjectStoreError(
                "OBJECT_SIZE_MISMATCH",
                "provider size differs from exact ObjectRef v2",
            )
        if size > max_bytes:
            raise ObjectStoreError(
                "OBJECT_TOO_LARGE",
                "provider object exceeds configured limit",
            )
        if cls._version(response) != expected.version_token:
            raise ObjectStoreError(
                "OBJECT_VERSION_MISMATCH",
                "provider version differs from exact ObjectRef v2",
            )
        if _etag(response.get("ETag") or response.get("etag")) != _etag(expected.etag):
            raise ObjectStoreError(
                "OBJECT_ETAG_MISMATCH",
                "provider ETag differs from exact ObjectRef v2",
            )
        metadata = response.get("Metadata") or response.get("metadata") or {}
        native = response.get("ChecksumSHA256") or response.get("checksum_sha256")
        if native and expected.provider == "AWS_S3":
            matches = str(native) == _native_sha256(expected.checksum)
        else:
            actual = (
                metadata.get("sha256")
                or metadata.get("x-oss-meta-sha256")
                or response.get("sha256")
            )
            matches = str(actual or "").lower() == expected.checksum
        if not matches:
            raise ObjectStoreError(
                "OBJECT_CHECKSUM_MISMATCH",
                "provider checksum differs from exact ObjectRef v2",
            )
