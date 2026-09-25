"""Replayable TMDB daily-export and changes/detail API acquisition."""

from __future__ import annotations

import gzip
import hashlib
import json
import re
import time
from collections.abc import Iterable, Iterator, Mapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from video_media_catalog.canonical import canonical_json_bytes, deterministic_key
from video_media_catalog.connector import (
    CaptureWindowPlan,
    ChangeSemantics,
    Completeness,
    ConnectorRecordEnvelope,
    DeleteCoverage,
    RecordOperation,
    Serialization,
    SourceWindow,
    TransportKind,
    build_connector_batch_manifest,
    build_connector_record_envelope,
    plan_bounded_capture_windows,
    select_capture_window,
)
from video_media_catalog.connector_publish import (
    DEFAULT_RECORD_SHARD_BYTES,
    PublishedConnectorCapture,
    publish_connector_capture,
)
from video_media_catalog.models import ObjectRef
from video_media_catalog.object_store import RuntimeObjectStore
from video_media_catalog.storage import join_uri
from video_media_catalog.tmdb import (
    TMDB_API_ORIGIN,
    TMDB_CHANGES_CONNECTOR_ID,
    TMDB_DAILY_CONNECTOR_ID,
    TMDB_ENTITY_KINDS,
    TMDB_FILES_ORIGIN,
    TMDB_MOVIE_NAMESPACE_ID,
    TMDB_PERSON_NAMESPACE_ID,
    TMDB_POLICY_ID,
    TMDB_SOURCE_PRODUCT_ID,
    TMDB_SOURCE_SYSTEM_ID,
    TMDB_TV_NAMESPACE_ID,
    tmdb_rights_profile,
)
from video_media_catalog.v2_contracts import require_rfc3339, require_sha256

DEFAULT_MAX_EXPORT_BYTES = 4 * 1024**3
DEFAULT_MAX_API_BYTES = 32 * 1024 * 1024
DEFAULT_MAX_CHANGE_PAGES = 10_000
DEFAULT_MAX_CHANGED_IDS = 20_000

_EXPORT_PREFIX = {
    "movie": "movie_ids",
    "tv": "tv_series_ids",
    "person": "person_ids",
}
_NAMESPACE_BY_KIND = {
    "movie": TMDB_MOVIE_NAMESPACE_ID,
    "tv": TMDB_TV_NAMESPACE_ID,
    "person": TMDB_PERSON_NAMESPACE_ID,
}
_API_PATH = re.compile(r"/3/(?:movie|tv|person)/(?:changes|[1-9][0-9]*)")


def tmdb_export_filename(kind: str, export_date: date) -> str:
    try:
        prefix = _EXPORT_PREFIX[kind]
    except KeyError as exc:
        raise ValueError(f"unsupported TMDB export kind: {kind}") from exc
    return f"{prefix}_{export_date:%m_%d_%Y}.json.gz"


def iter_tmdb_export(path: Path) -> Iterator[dict[str, Any]]:
    with gzip.open(path, mode="rt", encoding="utf-8", newline="") as handle:
        for line_number, raw in enumerate(handle, start=1):
            try:
                value = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"{path}:{line_number}: invalid TMDB export JSON"
                ) from exc
            if (
                not isinstance(value, dict)
                or isinstance(value.get("id"), bool)
                or not isinstance(value.get("id"), int)
                or value["id"] <= 0
            ):
                raise ValueError(
                    f"{path}:{line_number}: TMDB export row requires a positive id"
                )
            yield value


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def capture_tmdb_daily_exports(
    *,
    export_paths: Mapping[str, Path],
    export_date: date,
    destination_prefix: str,
    acquired_at: str,
    image_digest: str,
    config_digest: str,
    store: RuntimeObjectStore,
    retry_count: int = 0,
    rate_limit_count: int = 0,
    max_export_bytes: int = DEFAULT_MAX_EXPORT_BYTES,
    record_shard_bytes: int = DEFAULT_RECORD_SHARD_BYTES,
) -> PublishedConnectorCapture:
    if set(export_paths) != set(TMDB_ENTITY_KINDS):
        raise ValueError("TMDB daily capture requires movie, tv, and person exports")
    acquired = require_rfc3339(acquired_at, label="acquired_at")
    image = require_sha256(image_digest, label="image_digest")
    config = require_sha256(config_digest, label="config_digest")
    if max_export_bytes < 1 or retry_count < 0 or rate_limit_count < 0:
        raise ValueError("TMDB daily capture limits and counters are invalid")
    policy = tmdb_rights_profile()
    capture_id = deterministic_key(
        "tmdb-daily-id-export-capture-v1",
        {
            "exportDate": export_date.isoformat(),
            "acquiredAt": acquired,
            "imageDigest": image,
            "configDigest": config,
            "policyDigest": policy.digest,
        },
    ).removeprefix("sha256:")
    raw_objects: list[ObjectRef] = []
    counts: dict[str, int] = {}
    for kind in TMDB_ENTITY_KINDS:
        path = export_paths[kind]
        count = sum(1 for _ in iter_tmdb_export(path))
        if count < 1:
            raise ValueError(f"TMDB {kind} daily export contains no records")
        counts[kind] = count
        digest = _file_sha256(path)
        uploaded = store.upload_file(
            path,
            join_uri(
                destination_prefix,
                "tmdb",
                "daily-exports",
                capture_id,
                kind,
                f"{digest}.json.gz",
            ),
            media_type="application/gzip",
            object_format="OBJECT_FORMAT_OTHER",
            max_bytes=max_export_bytes,
        ).object_ref
        if uploaded.checksum.value != digest.removeprefix("sha256:"):
            raise RuntimeError(
                f"TMDB {kind} export changed while its capture was being published"
            )
        raw_objects.append(uploaded)
    batch = build_connector_batch_manifest(
        source_system_id=TMDB_SOURCE_SYSTEM_ID,
        source_product_id=TMDB_SOURCE_PRODUCT_ID,
        connector_id=TMDB_DAILY_CONNECTOR_ID,
        connector_version="1.0.0",
        image_digest=image,
        config_digest=config,
        policy_id=TMDB_POLICY_ID,
        policy_digest=policy.digest,
        transport_kind=TransportKind.DUMP,
        serialization=Serialization.JSON_LINES,
        change_semantics=ChangeSemantics.FULL_SNAPSHOT,
        completeness=Completeness.COMPLETE,
        delete_coverage=DeleteCoverage.NONE,
        coverage_scope={
            "origin": TMDB_FILES_ORIGIN,
            "entityKinds": list(TMDB_ENTITY_KINDS),
            "inventoryOnly": True,
            "exportDate": export_date.isoformat(),
        },
        raw_objects=tuple(raw_objects),
        acquired_at=acquired,
        record_count=sum(counts.values()),
        error_count=0,
        retry_count=retry_count,
        rate_limit_count=rate_limit_count,
    )

    def envelopes() -> Iterator[ConnectorRecordEnvelope]:
        for kind, path, raw_object in zip(
            TMDB_ENTITY_KINDS,
            (export_paths[item] for item in TMDB_ENTITY_KINDS),
            raw_objects,
            strict=True,
        ):
            for index, record in enumerate(iter_tmdb_export(path)):
                yield build_connector_record_envelope(
                    payload={
                        "capture": "daily-id-export",
                        "entityKind": kind,
                        "record": record,
                    },
                    batch_id=batch.batch_id,
                    source_system_id=batch.source_system_id,
                    source_product_id=batch.source_product_id,
                    source_namespace_id=_NAMESPACE_BY_KIND[kind],
                    source_record_id=str(record["id"]),
                    operation=RecordOperation.UPSERT,
                    observed_at=batch.acquired_at,
                    ingested_at=batch.acquired_at,
                    payload_schema="tmdb-daily-id-export-row-v1",
                    raw_object=raw_object,
                    source_location=f"/{kind}/line/{index + 1}",
                    policy_id=batch.policy_id,
                    policy_digest=batch.policy_digest,
                )

    return publish_connector_capture(
        destination_prefix=destination_prefix,
        batch=batch,
        envelopes=envelopes(),
        store=store,
        record_shard_bytes=record_shard_bytes,
    )


@dataclass(frozen=True)
class TMDBApiResponse:
    status: int
    body: bytes
    retry_count: int = 0
    rate_limit_count: int = 0


class TMDBApiFetcher(Protocol):
    def fetch(
        self,
        path: str,
        query: Mapping[str, str | int],
    ) -> TMDBApiResponse: ...


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class TMDBHttpClient:
    """Fixed-origin TMDB v3 client; credentials are sent only in a header."""

    def __init__(
        self,
        *,
        read_token: str,
        user_agent: str,
        timeout_seconds: float = 30,
        minimum_interval_seconds: float = 0.05,
        max_attempts: int = 5,
        max_response_bytes: int = DEFAULT_MAX_API_BYTES,
        opener: Any | None = None,
        clock=time.monotonic,
        sleeper=time.sleep,
    ) -> None:
        if not read_token.strip() or len(read_token) > 4096:
            raise ValueError("a bounded TMDB API Read Access Token is required")
        if not user_agent.strip() or len(user_agent) > 256:
            raise ValueError("a bounded identifying User-Agent is required")
        if (
            timeout_seconds <= 0
            or minimum_interval_seconds < 0
            or max_attempts < 1
            or max_response_bytes < 1
        ):
            raise ValueError("TMDB HTTP limits are invalid")
        self.read_token = read_token.strip()
        self.user_agent = user_agent.strip()
        self.timeout_seconds = timeout_seconds
        self.minimum_interval_seconds = minimum_interval_seconds
        self.max_attempts = max_attempts
        self.max_response_bytes = max_response_bytes
        self.opener = opener or build_opener(_NoRedirect)
        self.clock = clock
        self.sleeper = sleeper
        self._last_request_at: float | None = None

    def fetch(
        self,
        path: str,
        query: Mapping[str, str | int],
    ) -> TMDBApiResponse:
        if _API_PATH.fullmatch(path) is None:
            raise ValueError("TMDB path is outside the approved API endpoints")
        normalized_query = {str(key): str(value) for key, value in query.items()}
        url = f"{TMDB_API_ORIGIN}{path}?{urlencode(normalized_query)}"
        retries = 0
        rate_limits = 0
        for attempt in range(1, self.max_attempts + 1):
            self._throttle()
            request = Request(
                url,
                headers={
                    "Accept": "application/json",
                    "Accept-Encoding": "identity",
                    "Authorization": f"Bearer {self.read_token}",
                    "User-Agent": self.user_agent,
                },
            )
            response = None
            try:
                response = self.opener.open(request, timeout=self.timeout_seconds)
                self._validate_final_url(
                    response.geturl(),
                    path=path,
                    query=normalized_query,
                )
                body = response.read(self.max_response_bytes + 1)
                if len(body) > self.max_response_bytes:
                    raise ValueError("TMDB API response exceeds configured byte limit")
                return TMDBApiResponse(
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
                    query=normalized_query,
                )
                body = exc.read(self.max_response_bytes + 1)
                if status == 404:
                    return TMDBApiResponse(
                        status=404,
                        body=body,
                        retry_count=retries,
                        rate_limit_count=rate_limits,
                    )
                retryable = status in {408, 429, 500, 502, 503, 504}
                if not retryable or attempt == self.max_attempts:
                    raise RuntimeError(f"TMDB API failed with HTTP {status}") from exc
                if status == 429:
                    rate_limits += 1
                retries += 1
                self.sleeper(_retry_delay(exc.headers, attempt))
            except URLError as exc:
                if attempt == self.max_attempts:
                    raise RuntimeError("TMDB API request failed") from exc
                retries += 1
                self.sleeper(min(30.0, float(2 ** (attempt - 1))))
            finally:
                if response is not None:
                    with suppress(Exception):
                        response.close()
        raise AssertionError("TMDB retry loop did not terminate")

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
        query: Mapping[str, str],
    ) -> None:
        parsed = urlsplit(url)
        if (
            parsed.scheme != "https"
            or parsed.hostname != "api.themoviedb.org"
            or parsed.port not in {None, 443}
            or parsed.path != path
            or parse_qs(parsed.query) != {key: [value] for key, value in query.items()}
            or parsed.username is not None
            or parsed.password is not None
            or parsed.fragment
        ):
            raise ValueError("TMDB API redirected outside the approved endpoint")


@dataclass(frozen=True)
class _ChangedRecord:
    kind: str
    source_id: int
    status: int
    payload_path: Path | None
    raw_object: ObjectRef


def _decode_change_page(
    body: bytes,
    *,
    expected_page: int,
) -> tuple[list[int], int]:
    try:
        payload = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("TMDB change page must be UTF-8 JSON") from exc
    if (
        not isinstance(payload, dict)
        or payload.get("page") != expected_page
        or isinstance(payload.get("total_pages"), bool)
        or not isinstance(payload.get("total_pages"), int)
        or not isinstance(payload.get("results"), list)
    ):
        raise ValueError("TMDB change page has an invalid shape")
    ids = []
    for value in payload["results"]:
        if (
            not isinstance(value, dict)
            or isinstance(value.get("id"), bool)
            or not isinstance(value.get("id"), int)
            or value["id"] <= 0
        ):
            raise ValueError("TMDB change page contains an invalid id")
        ids.append(value["id"])
    return ids, payload["total_pages"]


def _retry_delay(headers: Any, attempt: int) -> float:
    value = headers.get("Retry-After") if headers is not None else None
    if value is not None:
        try:
            return min(60.0, max(0.0, float(value)))
        except ValueError:
            pass
    return min(30.0, float(2 ** (attempt - 1)))


def _ordered_tmdb_changed_ids(
    changed_ids: Mapping[str, Iterable[int]],
) -> tuple[tuple[str, int], ...]:
    if set(changed_ids) != set(TMDB_ENTITY_KINDS):
        raise ValueError("TMDB change plan requires movie, tv, and person IDs")
    ordered: list[tuple[str, int]] = []
    for kind in TMDB_ENTITY_KINDS:
        values = tuple(changed_ids[kind])
        if any(
            isinstance(source_id, bool)
            or not isinstance(source_id, int)
            or source_id <= 0
            for source_id in values
        ):
            raise ValueError("TMDB change plan contains an invalid source ID")
        if len(values) != len(set(values)):
            raise ValueError("TMDB change plan contains duplicate source IDs")
        ordered.extend((kind, source_id) for source_id in sorted(values))
    return tuple(ordered)


def plan_tmdb_change_windows(
    changed_ids: Mapping[str, Iterable[int]],
    *,
    window_start: date,
    window_end: date,
    max_changed_ids: int = DEFAULT_MAX_CHANGED_IDS,
    watermark: str | None = None,
) -> tuple[CaptureWindowPlan, ...]:
    """Plan deterministic TMDB detail batches from a complete changed-ID set."""

    if window_end < window_start or window_end - window_start > timedelta(days=13):
        raise ValueError("TMDB change window must be between 1 and 14 inclusive days")
    ordered = _ordered_tmdb_changed_ids(changed_ids)
    start = f"{window_start.isoformat()}T00:00:00Z"
    end = f"{window_end.isoformat()}T23:59:59Z"
    return plan_bounded_capture_windows(
        source_product_id=TMDB_SOURCE_PRODUCT_ID,
        window_start=start,
        window_end=end,
        item_keys=(f"{kind}:{source_id}" for kind, source_id in ordered),
        max_items=max_changed_ids,
        watermark=watermark or window_end.isoformat(),
    )


def capture_tmdb_changes(
    *,
    window_start: date,
    window_end: date,
    destination_prefix: str,
    acquired_at: str,
    image_digest: str,
    config_digest: str,
    fetcher: TMDBApiFetcher,
    store: RuntimeObjectStore,
    max_change_pages: int = DEFAULT_MAX_CHANGE_PAGES,
    max_changed_ids: int = DEFAULT_MAX_CHANGED_IDS,
    max_api_bytes: int = DEFAULT_MAX_API_BYTES,
    record_shard_bytes: int = DEFAULT_RECORD_SHARD_BYTES,
    window_cursor: str | None = None,
    watermark: str | None = None,
) -> PublishedConnectorCapture:
    if window_end < window_start or window_end - window_start > timedelta(days=13):
        raise ValueError("TMDB change window must be between 1 and 14 inclusive days")
    if min(max_change_pages, max_changed_ids, max_api_bytes) < 1:
        raise ValueError("TMDB change capture limits must be positive")
    acquired = require_rfc3339(acquired_at, label="acquired_at")
    image = require_sha256(image_digest, label="image_digest")
    config = require_sha256(config_digest, label="config_digest")
    policy = tmdb_rights_profile()
    capture_id = deterministic_key(
        "tmdb-changes-detail-capture-v1",
        {
            "windowStart": window_start.isoformat(),
            "windowEnd": window_end.isoformat(),
            "acquiredAt": acquired,
            "imageDigest": image,
            "configDigest": config,
            "policyDigest": policy.digest,
        },
    ).removeprefix("sha256:")
    payload_spool = TemporaryDirectory(prefix="tmdb-change-payloads-")
    payload_root = Path(payload_spool.name)
    raw_objects: list[ObjectRef] = []
    changed: dict[str, set[int]] = {kind: set() for kind in TMDB_ENTITY_KINDS}
    retry_count = 0
    rate_limit_count = 0

    def publish_raw(body: bytes, *parts: str) -> ObjectRef:
        if not body or len(body) > max_api_bytes:
            raise ValueError("TMDB raw API object is empty or exceeds its byte limit")
        digest = "sha256:" + hashlib.sha256(body).hexdigest()
        reference = store.upload_bytes(
            body,
            join_uri(
                destination_prefix,
                "tmdb",
                "changes",
                capture_id,
                *parts,
                f"{digest}.json",
            ),
            media_type="application/json",
            object_format="OBJECT_FORMAT_JSON",
            max_bytes=max_api_bytes,
        ).object_ref
        raw_objects.append(reference)
        return reference

    query_dates = {
        "start_date": window_start.isoformat(),
        "end_date": window_end.isoformat(),
    }
    for kind in TMDB_ENTITY_KINDS:
        page = 1
        total_pages = 1
        while page <= total_pages:
            if page > max_change_pages:
                raise RuntimeError("TMDB change page limit reached")
            response = fetcher.fetch(
                f"/3/{kind}/changes",
                {**query_dates, "page": page},
            )
            retry_count += response.retry_count
            rate_limit_count += response.rate_limit_count
            if response.status != 200:
                raise RuntimeError(
                    f"TMDB {kind} changes returned HTTP {response.status}"
                )
            publish_raw(response.body, kind, "change-pages", f"page={page:05d}")
            ids, total_pages = _decode_change_page(
                response.body,
                expected_page=page,
            )
            if total_pages == 0:
                if page != 1 or ids:
                    raise RuntimeError("TMDB empty change response is inconsistent")
                break
            if total_pages < page or total_pages > max_change_pages:
                raise RuntimeError("TMDB change response has an invalid page count")
            changed[kind].update(ids)
            page += 1

    ordered_changed = _ordered_tmdb_changed_ids(changed)
    plans = plan_tmdb_change_windows(
        changed,
        window_start=window_start,
        window_end=window_end,
        max_changed_ids=max_changed_ids,
    )
    try:
        selected_plan = select_capture_window(plans, cursor=window_cursor)
    except ValueError as exc:
        if window_cursor is None and len(plans) > 1:
            cursors = ", ".join(plan.cursor for plan in plans)
            raise RuntimeError(
                "TMDB changed-ID inventory requires "
                f"{len(plans)} explicit bounded windows; "
                f"rerun with one window_cursor: {cursors}"
            ) from exc
        raise
    selected_changed = ordered_changed[
        selected_plan.item_offset : (
            selected_plan.item_offset + selected_plan.item_count
        )
    ]
    records: list[_ChangedRecord] = []
    append_by_kind = {
        "movie": "credits,external_ids,translations,images",
        "tv": "credits,external_ids,translations,images",
        "person": "combined_credits,external_ids,translations,images",
    }
    for kind, source_id in selected_changed:
        response = fetcher.fetch(
            f"/3/{kind}/{source_id}",
            {
                "append_to_response": append_by_kind[kind],
                "include_image_language": "en,null",
            },
        )
        retry_count += response.retry_count
        rate_limit_count += response.rate_limit_count
        if response.status not in {200, 404}:
            raise RuntimeError(
                f"TMDB {kind}/{source_id} returned HTTP {response.status}"
            )
        body = response.body or canonical_json_bytes(
            {
                "connectorObservation": "not-found",
                "entityKind": kind,
                "id": source_id,
                "status": response.status,
            },
            newline=True,
        )
        payload_path = None
        if response.status == 200:
            try:
                value = json.loads(body)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ValueError("TMDB detail must be UTF-8 JSON") from exc
            if not isinstance(value, dict) or value.get("id") != source_id:
                raise ValueError("TMDB detail identity does not match request")
            payload_path = payload_root / f"{kind}-{source_id}.json"
            payload_path.write_bytes(
                canonical_json_bytes(
                    {
                        "capture": "changes-detail",
                        "entityKind": kind,
                        "detail": value,
                    }
                )
            )
        raw_object = publish_raw(
            body,
            kind,
            "details",
            f"id={source_id}",
        )
        records.append(
            _ChangedRecord(
                kind=kind,
                source_id=source_id,
                status=response.status,
                payload_path=payload_path,
                raw_object=raw_object,
            )
        )
    batch = build_connector_batch_manifest(
        source_system_id=TMDB_SOURCE_SYSTEM_ID,
        source_product_id=TMDB_SOURCE_PRODUCT_ID,
        connector_id=TMDB_CHANGES_CONNECTOR_ID,
        connector_version="1.0.0",
        image_digest=image,
        config_digest=config,
        policy_id=TMDB_POLICY_ID,
        policy_digest=policy.digest,
        transport_kind=TransportKind.API,
        serialization=Serialization.JSON,
        change_semantics=ChangeSemantics.DELTA,
        completeness=Completeness.COMPLETE,
        delete_coverage=DeleteCoverage.EXPLICIT,
        coverage_scope={
            "origin": TMDB_API_ORIGIN,
            "entityKinds": list(TMDB_ENTITY_KINDS),
            "capture": "changes-with-current-details",
            "windowPlanId": selected_plan.plan_id,
            "windowCursor": selected_plan.cursor,
            "windowShardIndex": selected_plan.shard_index,
            "windowShardCount": selected_plan.shard_count,
            "windowChangedIds": selected_plan.item_count,
            "totalChangedIds": selected_plan.total_items,
        },
        source_window=SourceWindow(
            start=f"{window_start.isoformat()}T00:00:00Z",
            end=f"{window_end.isoformat()}T23:59:59Z",
        ),
        watermark_before=watermark or window_start.isoformat(),
        watermark_after=window_end.isoformat(),
        raw_objects=tuple(raw_objects),
        acquired_at=acquired,
        record_count=len(records),
        error_count=0,
        retry_count=retry_count,
        rate_limit_count=rate_limit_count,
    )

    def envelopes() -> Iterator[ConnectorRecordEnvelope]:
        for record in records:
            common = {
                "batch_id": batch.batch_id,
                "source_system_id": batch.source_system_id,
                "source_product_id": batch.source_product_id,
                "source_namespace_id": _NAMESPACE_BY_KIND[record.kind],
                "source_record_id": str(record.source_id),
                "observed_at": batch.acquired_at,
                "ingested_at": batch.acquired_at,
                "payload_schema": "tmdb-changes-detail-v1",
                "raw_object": record.raw_object,
                "source_location": f"/3/{record.kind}/{record.source_id}",
                "policy_id": batch.policy_id,
                "policy_digest": batch.policy_digest,
            }
            if record.status == 404:
                yield build_connector_record_envelope(
                    operation=RecordOperation.DELETE,
                    **common,
                )
            else:
                if record.payload_path is None:
                    raise RuntimeError("TMDB UPSERT payload spool is missing")
                yield build_connector_record_envelope(
                    payload=json.loads(record.payload_path.read_bytes()),
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
