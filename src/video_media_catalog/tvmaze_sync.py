"""Bounded, replayable capture of the public TVmaze show index."""

from __future__ import annotations

import time
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from pydantic import Field

from video_media_catalog.canonical import deterministic_key, sha256_digest
from video_media_catalog.connector import (
    ChangeSemantics,
    Completeness,
    ConnectorBatchManifest,
    ConnectorRecordSetManifest,
    DeleteCoverage,
    Serialization,
    TransportKind,
    build_connector_batch_manifest,
    build_connector_record_set_manifest,
)
from video_media_catalog.models import ObjectRef
from video_media_catalog.object_store import BoundedObjectStore, RuntimeObjectStore
from video_media_catalog.runtime_args import join_uri
from video_media_catalog.tvmaze import (
    TVMAZE_CONNECTOR_ID,
    TVMAZE_POLICY_ID,
    TVMAZE_SOURCE_PRODUCT_ID,
    TVMAZE_SOURCE_SYSTEM_ID,
    TVMazeShowConnector,
    _decode_page,
    tvmaze_rights_profile,
)
from video_media_catalog.v2_contracts import (
    V2ContractModel,
    require_rfc3339,
    require_sha256,
)

TVMAZE_API_ORIGIN = "https://api.tvmaze.com"
DEFAULT_MAX_PAGE_BYTES = 16 * 1024 * 1024
DEFAULT_RECORD_SHARD_BYTES = 8 * 1024 * 1024
DEFAULT_MAX_PAGES = 100_000
CONTROL_OBJECT_MAX_BYTES = 16 * 1024 * 1024


@dataclass(frozen=True)
class TVMazePageFetch:
    status: int
    body: bytes
    retry_count: int = 0
    rate_limit_count: int = 0


class TVMazePageFetcher(Protocol):
    def fetch_page(self, page: int) -> TVMazePageFetch: ...


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class TVMazeHttpFetcher:
    """Small fixed-origin client with respectful throttling and retry."""

    def __init__(
        self,
        *,
        user_agent: str,
        timeout_seconds: float = 30,
        minimum_interval_seconds: float = 0.55,
        max_attempts: int = 5,
        max_page_bytes: int = DEFAULT_MAX_PAGE_BYTES,
        opener: Any | None = None,
        clock=time.monotonic,
        sleeper=time.sleep,
    ) -> None:
        if not user_agent.strip() or len(user_agent) > 256:
            raise ValueError("a bounded identifying User-Agent is required")
        if timeout_seconds <= 0 or minimum_interval_seconds < 0:
            raise ValueError("HTTP timing limits are invalid")
        if max_attempts < 1 or max_page_bytes < 1:
            raise ValueError("HTTP attempt and page limits must be positive")
        self.user_agent = user_agent.strip()
        self.timeout_seconds = timeout_seconds
        self.minimum_interval_seconds = minimum_interval_seconds
        self.max_attempts = max_attempts
        self.max_page_bytes = max_page_bytes
        self.opener = opener or build_opener(_NoRedirect)
        self.clock = clock
        self.sleeper = sleeper
        self._last_request_at: float | None = None

    def fetch_page(self, page: int) -> TVMazePageFetch:
        if page < 0:
            raise ValueError("TVmaze page must be non-negative")
        retries = 0
        rate_limits = 0
        url = f"{TVMAZE_API_ORIGIN}/shows?{urlencode({'page': page})}"
        for attempt in range(1, self.max_attempts + 1):
            self._throttle()
            request = Request(
                url,
                headers={
                    "Accept": "application/json",
                    "Accept-Encoding": "identity",
                    "User-Agent": self.user_agent,
                },
            )
            response = None
            try:
                response = self.opener.open(
                    request,
                    timeout=self.timeout_seconds,
                )
                self._validate_final_url(response.geturl(), page=page)
                body = response.read(self.max_page_bytes + 1)
                if len(body) > self.max_page_bytes:
                    raise ValueError("TVmaze page exceeds configured byte limit")
                return TVMazePageFetch(
                    status=int(response.status),
                    body=body,
                    retry_count=retries,
                    rate_limit_count=rate_limits,
                )
            except HTTPError as exc:
                response = exc
                status = int(exc.code)
                self._validate_final_url(exc.geturl(), page=page)
                if status == 404:
                    return TVMazePageFetch(
                        status=404,
                        body=b"",
                        retry_count=retries,
                        rate_limit_count=rate_limits,
                    )
                retryable = status in {408, 429, 500, 502, 503, 504}
                if not retryable or attempt == self.max_attempts:
                    raise RuntimeError(
                        f"TVmaze request failed with HTTP {status}"
                    ) from exc
                if status == 429:
                    rate_limits += 1
                retries += 1
                self.sleeper(_retry_delay(exc.headers, attempt))
            except URLError as exc:
                if attempt == self.max_attempts:
                    raise RuntimeError("TVmaze request failed") from exc
                retries += 1
                self.sleeper(min(30.0, float(2 ** (attempt - 1))))
            finally:
                if response is not None:
                    with suppress(Exception):
                        response.close()
        raise AssertionError("TVmaze retry loop did not terminate")

    def _throttle(self) -> None:
        now = self.clock()
        if self._last_request_at is not None:
            remaining = self.minimum_interval_seconds - (now - self._last_request_at)
            if remaining > 0:
                self.sleeper(remaining)
        self._last_request_at = self.clock()

    @staticmethod
    def _validate_final_url(url: str, *, page: int) -> None:
        parsed = urlsplit(url)
        if (
            parsed.scheme != "https"
            or parsed.hostname != "api.tvmaze.com"
            or parsed.port not in {None, 443}
            or parsed.path != "/shows"
            or parse_qs(parsed.query) != {"page": [str(page)]}
        ):
            raise ValueError("TVmaze redirected outside the approved endpoint")


def _retry_delay(headers: Any, attempt: int) -> float:
    value = headers.get("Retry-After") if headers is not None else None
    if value is not None:
        try:
            return min(60.0, max(0.0, float(value)))
        except ValueError:
            pass
    return min(30.0, float(2 ** (attempt - 1)))


class TVMazeSyncResult(V2ContractModel):
    batch_manifest: ConnectorBatchManifest
    batch_manifest_object: ObjectRef
    record_set_manifest: ConnectorRecordSetManifest
    record_set_manifest_object: ObjectRef
    page_count: int = Field(gt=0)
    record_count: int = Field(ge=0)


def capture_tvmaze_show_index(
    *,
    destination_prefix: str,
    acquired_at: str,
    image_digest: str,
    config_digest: str,
    fetcher: TVMazePageFetcher,
    store: RuntimeObjectStore | None = None,
    max_pages: int = DEFAULT_MAX_PAGES,
    max_page_bytes: int = DEFAULT_MAX_PAGE_BYTES,
    record_shard_bytes: int = DEFAULT_RECORD_SHARD_BYTES,
) -> TVMazeSyncResult:
    """Capture raw pages, then publish normalized records and commit markers."""

    acquired = require_rfc3339(acquired_at, label="acquired_at")
    image = require_sha256(image_digest, label="image_digest")
    config = require_sha256(config_digest, label="config_digest")
    destination = urlsplit(destination_prefix)
    if destination.scheme not in {"file", "s3"}:
        raise ValueError("destination_prefix must use file:// or s3://")
    if max_pages < 1 or max_page_bytes < 1 or record_shard_bytes < 1:
        raise ValueError("TVmaze capture limits must be positive")
    if store is None:
        store = (
            BoundedObjectStore(client=object())
            if urlsplit(destination_prefix).scheme == "file"
            else BoundedObjectStore()
        )
    policy = tvmaze_rights_profile()
    capture_id = deterministic_key(
        "tvmaze-show-index-capture-v1",
        {
            "acquiredAt": acquired,
            "imageDigest": image,
            "configDigest": config,
            "policyDigest": policy.digest,
        },
    ).removeprefix("sha256:")

    with TemporaryDirectory(prefix="tvmaze-capture-") as directory:
        root = Path(directory)
        raw_objects: list[ObjectRef] = []
        page_paths: list[Path] = []
        record_count = 0
        retry_count = 0
        rate_limit_count = 0
        reached_end = False

        for page in range(max_pages):
            result = fetcher.fetch_page(page)
            retry_count += result.retry_count
            rate_limit_count += result.rate_limit_count
            if result.status == 404:
                reached_end = True
                break
            if result.status != 200:
                raise RuntimeError(
                    f"TVmaze page {page} returned unexpected HTTP {result.status}"
                )
            if not result.body or len(result.body) > max_page_bytes:
                raise ValueError("TVmaze page is empty or exceeds byte limit")
            decoded = _decode_page(result.body)
            record_count += len(decoded)
            digest = sha256_digest(result.body)
            page_path = root / f"page-{page:06d}.json"
            page_path.write_bytes(result.body)
            page_paths.append(page_path)
            raw_uri = join_uri(
                destination_prefix,
                "tvmaze",
                "captures",
                capture_id,
                f"page={page:06d}",
                f"{digest}.json",
            )
            raw_objects.append(
                store.upload_file(
                    page_path,
                    raw_uri,
                    media_type="application/json",
                    object_format="OBJECT_FORMAT_JSON",
                    max_bytes=max_page_bytes,
                ).object_ref
            )

        if not reached_end:
            raise RuntimeError("TVmaze page limit reached before terminal 404")
        if not raw_objects:
            raise RuntimeError("TVmaze show index returned no pages")
        if record_count == 0:
            raise RuntimeError("TVmaze show index returned no records")

        batch = build_connector_batch_manifest(
            source_system_id=TVMAZE_SOURCE_SYSTEM_ID,
            source_product_id=TVMAZE_SOURCE_PRODUCT_ID,
            connector_id=TVMAZE_CONNECTOR_ID,
            connector_version="1.0.0",
            image_digest=image,
            config_digest=config,
            policy_id=TVMAZE_POLICY_ID,
            policy_digest=policy.digest,
            transport_kind=TransportKind.API,
            serialization=Serialization.JSON,
            change_semantics=ChangeSemantics.FULL_SNAPSHOT,
            completeness=Completeness.COMPLETE,
            delete_coverage=DeleteCoverage.SNAPSHOT_DIFF,
            coverage_scope={
                "endpoint": "/shows",
                "entityType": "show",
                "firstPage": 0,
                "lastPageInclusive": len(raw_objects) - 1,
                "terminalStatus": 404,
            },
            raw_objects=tuple(raw_objects),
            acquired_at=acquired,
            record_count=record_count,
            error_count=0,
            retry_count=retry_count,
            rate_limit_count=rate_limit_count,
        )
        batch_prefix = join_uri(
            destination_prefix,
            "tvmaze",
            "batches",
            batch.batch_id.removeprefix("sha256:"),
        )
        batch_manifest_object = store.upload_bytes(
            batch.json_bytes(),
            join_uri(batch_prefix, "batch-manifest.json"),
            media_type="application/vnd.video-media-catalog.connector-batch.v2+json",
            object_format="OBJECT_FORMAT_JSON",
            max_bytes=CONTROL_OBJECT_MAX_BYTES,
        ).object_ref

        connector = TVMazeShowConnector()
        shard = bytearray()
        shard_index = 0
        record_objects: list[ObjectRef] = []
        first_key: str | None = None
        last_key: str | None = None
        decoded_count = 0
        seen_source_ids: set[str] = set()

        def flush_shard() -> None:
            nonlocal shard, shard_index
            if not shard:
                return
            payload = bytes(shard)
            digest = sha256_digest(payload)
            uri = join_uri(
                batch_prefix,
                "records",
                f"shard={shard_index:05d}",
                f"{digest}.ndjson",
            )
            record_objects.append(
                store.upload_bytes(
                    payload,
                    uri,
                    media_type=(
                        "application/vnd.video-media-catalog."
                        "connector-record-envelope.v2+ndjson"
                    ),
                    object_format="OBJECT_FORMAT_OTHER",
                    max_bytes=record_shard_bytes,
                ).object_ref
            )
            shard = bytearray()
            shard_index += 1

        for page_number, (path, raw_object) in enumerate(
            zip(page_paths, raw_objects, strict=True)
        ):
            for envelope in connector.decode_page(
                batch,
                path.read_bytes(),
                raw_object=raw_object,
                page_number=page_number,
            ):
                if envelope.source_record_id in seen_source_ids:
                    raise ValueError(
                        f"duplicate TVmaze show id: {envelope.source_record_id}"
                    )
                seen_source_ids.add(envelope.source_record_id)
                line = envelope.json_bytes()
                if len(line) > record_shard_bytes:
                    raise ValueError("one TVmaze record exceeds shard byte limit")
                if shard and len(shard) + len(line) > record_shard_bytes:
                    flush_shard()
                shard.extend(line)
                first_key = first_key or envelope.envelope_key
                last_key = envelope.envelope_key
                decoded_count += 1
        flush_shard()
        if decoded_count != record_count:
            raise RuntimeError("TVmaze decoded count differs from captured pages")

        record_set = build_connector_record_set_manifest(
            batch_id=batch.batch_id,
            source_product_id=batch.source_product_id,
            policy_id=batch.policy_id,
            policy_digest=batch.policy_digest,
            record_objects=tuple(record_objects),
            record_count=decoded_count,
            first_envelope_key=first_key,
            last_envelope_key=last_key,
            created_at=acquired,
        )
        record_set_manifest_object = store.upload_bytes(
            record_set.json_bytes(),
            join_uri(batch_prefix, "record-set.json"),
            media_type="application/vnd.video-media-catalog.record-set.v2+json",
            object_format="OBJECT_FORMAT_JSON",
            max_bytes=CONTROL_OBJECT_MAX_BYTES,
        ).object_ref
        return TVMazeSyncResult(
            batch_manifest=batch,
            batch_manifest_object=batch_manifest_object,
            record_set_manifest=record_set,
            record_set_manifest_object=record_set_manifest_object,
            page_count=len(raw_objects),
            record_count=decoded_count,
        )
