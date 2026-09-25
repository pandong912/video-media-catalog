"""Small local object-store abstraction with immutable atomic publication."""

from __future__ import annotations

import hashlib
import os
import shutil
import tempfile
from errno import EXDEV
from pathlib import Path
from urllib.parse import unquote, urlparse

from video_media_catalog.canonical import sha256_digest


class UnsupportedUriError(ValueError):
    """Raised when extraction receives a URI unsupported by the local backend."""


class ImmutableObjectConflictError(RuntimeError):
    """Raised when an immutable output already exists with different bytes."""


def join_uri(prefix: str, *parts: str) -> str:
    """Join safe URI path components without permitting dot segments."""

    normalized: list[str] = []
    for part in parts:
        item = part.strip("/")
        if (
            not item
            or item in {".", ".."}
            or any(segment in {".", ".."} for segment in item.split("/"))
        ):
            raise ValueError("URI path components must not contain dot segments")
        normalized.append(item)
    return "/".join([prefix.rstrip("/"), *normalized])


def local_path(uri: str | Path) -> Path:
    if isinstance(uri, Path):
        return uri
    parsed = urlparse(uri)
    if parsed.scheme == "":
        return Path(uri)
    if parsed.scheme != "file":
        raise UnsupportedUriError(
            f"unsupported extraction URI scheme {parsed.scheme!r}; "
            "this release accepts local paths and file:// URIs"
        )
    if parsed.netloc not in {"", "localhost"}:
        raise UnsupportedUriError("file URI authority must be empty or localhost")
    return Path(unquote(parsed.path))


def file_uri(path: Path) -> str:
    return path.resolve().as_uri()


def digest_file(path: Path, *, chunk_size: int = 1024 * 1024) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
            size += len(chunk)
    return "sha256:" + digest.hexdigest(), size


def atomic_write_bytes(path: Path, payload: bytes) -> bool:
    """Publish immutable bytes, returning False when identical bytes exist."""

    path.parent.mkdir(parents=True, exist_ok=True)
    expected = sha256_digest(payload)
    if path.exists():
        actual, _ = digest_file(path)
        if actual == expected:
            return False
        raise ImmutableObjectConflictError(
            f"immutable object already exists with different content: {path}"
        )
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            actual, _ = digest_file(path)
            if actual != expected:
                raise ImmutableObjectConflictError(
                    "immutable object was concurrently published with "
                    f"different content: {path}"
                ) from None
            return False
    finally:
        temporary.unlink(missing_ok=True)
    return True


def publish_file_immutable(source: Path, destination: Path) -> bool:
    """Atomically move a completed file unless identical output already exists."""

    destination.parent.mkdir(parents=True, exist_ok=True)
    source_digest, _ = digest_file(source)
    if destination.exists():
        destination_digest, _ = digest_file(destination)
        source.unlink(missing_ok=True)
        if source_digest == destination_digest:
            return False
        raise ImmutableObjectConflictError(
            f"immutable object already exists with different content: {destination}"
        )
    try:
        os.link(source, destination)
    except FileExistsError:
        destination_digest, _ = digest_file(destination)
        source.unlink(missing_ok=True)
        if source_digest != destination_digest:
            raise ImmutableObjectConflictError(
                "immutable object was concurrently published with "
                f"different content: {destination}"
            ) from None
        return False
    except OSError as exc:
        if exc.errno != EXDEV:
            raise
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{destination.name}.",
            suffix=".tmp",
            dir=destination.parent,
        )
        os.close(descriptor)
        temporary = Path(temporary_name)
        try:
            shutil.copyfile(source, temporary)
            try:
                os.link(temporary, destination)
            except FileExistsError:
                destination_digest, _ = digest_file(destination)
                if source_digest != destination_digest:
                    raise ImmutableObjectConflictError(
                        "immutable object was concurrently published with "
                        f"different content: {destination}"
                    ) from None
                return False
        finally:
            temporary.unlink(missing_ok=True)
            source.unlink(missing_ok=True)
    else:
        source.unlink(missing_ok=True)
    return True
