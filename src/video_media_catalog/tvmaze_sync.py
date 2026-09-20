"""Bounded, replayable capture of the public TVmaze show index."""

from __future__ import annotations

import json
import time
from collections.abc import Iterator
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from pydantic import Field

from video_media_catalog.canonical import (
    canonical_json_bytes,
    deterministic_key,
    sha256_digest,
)
from video_media_catalog.connector import (
    ChangeSemantics,
    Completeness,
    ConnectorBatchManifest,
    ConnectorRecordEnvelope,
    ConnectorRecordSetManifest,
    DeleteCoverage,
    RecordOperation,
    Serialization,
    TransportKind,
    build_connector_batch_manifest,
    build_connector_record_envelope,
    build_connector_record_set_manifest,
)
from video_media_catalog.connector_publish import (
    PublishedConnectorCapture,
    publish_connector_capture,
)
from video_media_catalog.models import ObjectRef
from video_media_catalog.object_store import BoundedObjectStore, RuntimeObjectStore
from video_media_catalog.runtime_args import join_uri
from video_media_catalog.tvmaze import (
    TVMAZE_CONNECTOR_ID,
    TVMAZE_DELTA_CONNECTOR_ID,
    TVMAZE_POLICY_ID,
    TVMAZE_SHOW_NAMESPACE_ID,
    TVMAZE_SOURCE_PRODUCT_ID,
    TVMAZE_SOURCE_SYSTEM_ID,
    TVMazeShowConnector,
    _decode_page,
    _show_id,
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
DEFAULT_MAX_UPDATES = 20_000
CONTROL_OBJECT_MAX_BYTES = 16 * 1024 * 1024


@dataclass(frozen=True)
class TVMazePageFetch:
    status: int
    body: bytes
    retry_count: int = 0
    rate_limit_count: int = 0


class TVMazePageFetcher(Protocol):
    def fetch_page(self, page: int) -> TVMazePageFetch: ...


class TVMazeDeltaFetcher(Protocol):
    def fetch_updates(self, since: str) -> TVMazePageFetch: ...

    def fetch_show(self, show_id: int) -> TVMazePageFetch: ...


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
        return self._fetch("/shows", {"page": str(page)})

    def fetch_updates(self, since: str) -> TVMazePageFetch:
        if since not in {"day", "week", "month"}:
            raise ValueError("TVmaze update window must be day, week, or month")
        return self._fetch("/updates/shows", {"since": since})

    def fetch_show(self, show_id: int) -> TVMazePageFetch:
        if isinstance(show_id, bool) or not isinstance(show_id, int) or show_id <= 0:
            raise ValueError("TVmaze show id must be a positive integer")
        return self._fetch(f"/shows/{show_id}", {})

    def _fetch(
        self,
        path: str,
        query: dict[str, str],
    ) -> TVMazePageFetch:
        retries = 0
        rate_limits = 0
        suffix = f"?{urlencode(query)}" if query else ""
        url = f"{TVMAZE_API_ORIGIN}{path}{suffix}"
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
                self._validate_final_url(
                    response.geturl(),
                    path=path,
                    query=query,
                )
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
                self._validate_final_url(
                    exc.geturl(),
                    path=path,
                    query=query,
                )
                if status == 404:
                    body = exc.read(self.max_page_bytes + 1)
                    if len(body) > self.max_page_bytes:
                        raise ValueError(
                            "TVmaze error response exceeds configured byte limit"
                        ) from exc
                    return TVMazePageFetch(
                        status=404,
                        body=body,
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
    def _validate_final_url(
        url: str,
        *,
        path: str,
        query: dict[str, str],
    ) -> None:
        parsed = urlsplit(url)
        if (
            parsed.scheme != "https"
            or parsed.hostname != "api.tvmaze.com"
            or parsed.port not in {None, 443}
            or parsed.path != path
            or parse_qs(parsed.query) != {key: [value] for key, value in query.items()}
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
                "pagination": "contiguous-until-404",
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


def capture_tvmaze_show_delta(
    *,
    destination_prefix: str,
    acquired_at: str,
    image_digest: str,
    config_digest: str,
    since: str,
    fetcher: TVMazeDeltaFetcher,
    store: RuntimeObjectStore,
    max_page_bytes: int = DEFAULT_MAX_PAGE_BYTES,
    max_updates: int = DEFAULT_MAX_UPDATES,
    record_shard_bytes: int = DEFAULT_RECORD_SHARD_BYTES,
) -> PublishedConnectorCapture:
    """Capture the official update index and current show details as one delta."""

    acquired = require_rfc3339(acquired_at, label="acquired_at")
    image = require_sha256(image_digest, label="image_digest")
    config = require_sha256(config_digest, label="config_digest")
    if since not in {"day", "week", "month"}:
        raise ValueError("TVmaze update window must be day, week, or month")
    if min(max_page_bytes, max_updates, record_shard_bytes) < 1:
        raise ValueError("TVmaze delta byte limits must be positive")
    policy = tvmaze_rights_profile()
    capture_id = deterministic_key(
        "tvmaze-show-updates-capture-v1",
        {
            "acquiredAt": acquired,
            "imageDigest": image,
            "configDigest": config,
            "policyDigest": policy.digest,
            "since": since,
        },
    ).removeprefix("sha256:")
    payload_spool = TemporaryDirectory(prefix="tvmaze-delta-payloads-")
    payload_root = Path(payload_spool.name)
    update_result = fetcher.fetch_updates(since)
    if update_result.status != 200:
        raise RuntimeError(f"TVmaze show updates returned HTTP {update_result.status}")
    if not update_result.body or len(update_result.body) > max_page_bytes:
        raise ValueError("TVmaze update index is empty or exceeds its byte limit")
    try:
        update_payload = json.loads(update_result.body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("TVmaze update index must be UTF-8 JSON") from exc
    if not isinstance(update_payload, dict):
        raise ValueError("TVmaze update index must be a JSON object")
    updates: dict[int, int] = {}
    for raw_id, raw_timestamp in update_payload.items():
        try:
            show_id = int(raw_id)
        except (TypeError, ValueError) as exc:
            raise ValueError("TVmaze update index contains an invalid show id") from exc
        if (
            show_id <= 0
            or isinstance(raw_timestamp, bool)
            or not isinstance(raw_timestamp, int)
            or raw_timestamp <= 0
        ):
            raise ValueError("TVmaze update index contains an invalid timestamp")
        updates[show_id] = raw_timestamp
        if len(updates) > max_updates:
            raise RuntimeError("TVmaze update count exceeds configured limit")

    def upload_raw(body: bytes, *parts: str) -> ObjectRef:
        digest = sha256_digest(body)
        return store.upload_bytes(
            body,
            join_uri(
                destination_prefix,
                "tvmaze",
                "updates",
                capture_id,
                *parts,
                f"{digest}.json",
            ),
            media_type="application/json",
            object_format="OBJECT_FORMAT_JSON",
            max_bytes=max_page_bytes,
        ).object_ref

    raw_objects = [
        upload_raw(
            update_result.body,
            "index",
            f"since={since}",
        )
    ]
    detail_records: list[tuple[int, int, int, Path | None, ObjectRef]] = []
    retry_count = update_result.retry_count
    rate_limit_count = update_result.rate_limit_count
    for show_id, updated in sorted(updates.items()):
        response = fetcher.fetch_show(show_id)
        retry_count += response.retry_count
        rate_limit_count += response.rate_limit_count
        if response.status not in {200, 404}:
            raise RuntimeError(f"TVmaze show {show_id} returned HTTP {response.status}")
        body = response.body or canonical_json_bytes(
            {
                "connectorObservation": "not-found",
                "id": show_id,
                "status": response.status,
            },
            newline=True,
        )
        if len(body) > max_page_bytes:
            raise ValueError("TVmaze show detail exceeds its byte limit")
        payload_path = None
        if response.status == 200:
            try:
                value = json.loads(body)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ValueError("TVmaze show detail must be UTF-8 JSON") from exc
            if not isinstance(value, dict) or _show_id(value) != show_id:
                raise ValueError("TVmaze show detail identity does not match request")
            payload_path = payload_root / f"{show_id}.json"
            payload_path.write_bytes(canonical_json_bytes(value))
        raw_object = upload_raw(body, "shows", f"id={show_id}")
        raw_objects.append(raw_object)
        detail_records.append(
            (show_id, updated, response.status, payload_path, raw_object)
        )
    batch = build_connector_batch_manifest(
        source_system_id=TVMAZE_SOURCE_SYSTEM_ID,
        source_product_id=TVMAZE_SOURCE_PRODUCT_ID,
        connector_id=TVMAZE_DELTA_CONNECTOR_ID,
        connector_version="1.0.0",
        image_digest=image,
        config_digest=config,
        policy_id=TVMAZE_POLICY_ID,
        policy_digest=policy.digest,
        transport_kind=TransportKind.API,
        serialization=Serialization.JSON,
        change_semantics=ChangeSemantics.DELTA,
        completeness=Completeness.COMPLETE,
        delete_coverage=DeleteCoverage.EXPLICIT,
        coverage_scope={
            "endpoint": "/updates/shows",
            "detailEndpoint": "/shows/{id}",
            "entityType": "show",
            "since": since,
        },
        watermark_before=f"since:{since}",
        watermark_after=(
            datetime.fromtimestamp(max(updates.values()), tz=UTC)
            .isoformat()
            .replace("+00:00", "Z")
            if updates
            else acquired
        ),
        raw_objects=tuple(raw_objects),
        acquired_at=acquired,
        record_count=len(detail_records),
        error_count=0,
        retry_count=retry_count,
        rate_limit_count=rate_limit_count,
    )

    def envelopes() -> Iterator[ConnectorRecordEnvelope]:
        for show_id, updated, status, payload_path, raw_object in detail_records:
            modified_at = (
                datetime.fromtimestamp(updated, tz=UTC)
                .isoformat()
                .replace("+00:00", "Z")
            )
            common = {
                "batch_id": batch.batch_id,
                "source_system_id": batch.source_system_id,
                "source_product_id": batch.source_product_id,
                "source_namespace_id": TVMAZE_SHOW_NAMESPACE_ID,
                "source_record_id": str(show_id),
                "source_revision": str(updated),
                "source_modified_at": modified_at,
                "observed_at": batch.acquired_at,
                "ingested_at": batch.acquired_at,
                "payload_schema": "tvmaze-show-v1",
                "raw_object": raw_object,
                "source_location": f"/shows/{show_id}",
                "policy_id": batch.policy_id,
                "policy_digest": batch.policy_digest,
            }
            if status == 404:
                yield build_connector_record_envelope(
                    operation=RecordOperation.DELETE,
                    **common,
                )
            else:
                if payload_path is None:
                    raise RuntimeError("TVmaze UPSERT payload spool is missing")
                yield build_connector_record_envelope(
                    payload=json.loads(payload_path.read_bytes()),
                    operation=RecordOperation.UPSERT,
                    **common,
                )

    try:
        return publish_connector_capture(
            destination_prefix=destination_prefix,
            batch=batch,
            envelopes=envelopes(),
            store=store,
            record_shard_bytes=record_shard_bytes,
        )
    finally:
        payload_spool.cleanup()
