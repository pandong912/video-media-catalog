"""Bounded, keyless Europeana OAI-PMH EDM capture."""

from __future__ import annotations

import math
import re
import tempfile
import threading
import time
import xml.etree.ElementTree as ET
from collections.abc import Iterable, Iterator
from contextlib import suppress
from dataclasses import dataclass, replace
from datetime import UTC
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from defusedxml.common import DefusedXmlException
from defusedxml.ElementTree import fromstring

from video_media_catalog.canonical import (
    canonical_json_bytes,
    deterministic_key,
    sha256_digest,
)
from video_media_catalog.connector import (
    CaptureWindowStatus,
    ChangeSemantics,
    Completeness,
    DeleteCoverage,
    RecordOperation,
    Serialization,
    SourceWatermark,
    SourceWindow,
    TransportKind,
    build_capture_window_receipt,
    build_connector_batch_manifest,
    build_connector_record_envelope,
)
from video_media_catalog.connector_publish import (
    DEFAULT_RECORD_SHARD_BYTES,
    PublishedCaptureWindowControl,
    PublishedConnectorCapture,
    publish_capture_window_commit,
    publish_connector_capture,
)
from video_media_catalog.eidr import normalize_eidr_id, normalize_imdb_id
from video_media_catalog.europeana import (
    EUROPEANA_METADATA_LICENSE_URI,
    EUROPEANA_OAI_CONNECTOR_ID,
    EUROPEANA_RECORD_NAMESPACE_ID,
    EUROPEANA_SOURCE_PRODUCT_ID,
    EUROPEANA_SOURCE_SYSTEM_ID,
    europeana_metadata_rights_profile,
    europeana_record_url,
    normalize_europeana_record_id,
)
from video_media_catalog.models import ObjectRef
from video_media_catalog.object_store import RuntimeObjectStore
from video_media_catalog.storage import join_uri
from video_media_catalog.v2_contracts import (
    parse_rfc3339,
    require_rfc3339,
    require_sha256,
)

EUROPEANA_OAI_ORIGIN = "https://api.europeana.eu"
EUROPEANA_OAI_HOST = "api.europeana.eu"
EUROPEANA_OAI_PATH = "/oai/record/"
EUROPEANA_OAI_ENDPOINT = f"{EUROPEANA_OAI_ORIGIN}{EUROPEANA_OAI_PATH}"
DEFAULT_EUROPEANA_USER_AGENT = (
    "video-media-catalog-europeana/1.0 "
    "(https://github.com/pandong912/video-media-catalog)"
)

DEFAULT_REQUEST_TIMEOUT_SECONDS = 30.0
DEFAULT_MINIMUM_REQUEST_INTERVAL_SECONDS = 0.25
DEFAULT_MAX_ATTEMPTS = 5
DEFAULT_RETRY_INITIAL_BACKOFF_SECONDS = 1.0
DEFAULT_RETRY_MAX_BACKOFF_SECONDS = 30.0
DEFAULT_MAX_RETRY_AFTER_SECONDS = 60.0
DEFAULT_MAX_PAGE_BYTES = 16 * 1024 * 1024
DEFAULT_MAX_TOTAL_BYTES = 80 * 1024 * 1024
DEFAULT_MAX_RECORD_BYTES = 1024 * 1024
DEFAULT_MAX_PAGES = 5
DEFAULT_MAX_RECORDS = 100
MAX_TIMEOUT_SECONDS = 120.0
MAX_MINIMUM_REQUEST_INTERVAL_SECONDS = 60.0
MAX_ATTEMPTS = 10
MAX_BACKOFF_SECONDS = 300.0
MAX_PAGE_BYTES = 64 * 1024 * 1024
MAX_TOTAL_BYTES = 1024 * 1024 * 1024
MAX_RECORD_BYTES = 16 * 1024 * 1024
MAX_PAGES = 1_000
MAX_RECORDS = 10_000
MAX_RESUMPTION_TOKEN_LENGTH = 1_024
MAX_SET_SPEC_LENGTH = 512
CONTROL_OBJECT_MAX_BYTES = 16 * 1024 * 1024

OAI_NS = "http://www.openarchives.org/OAI/2.0/"
RDF_NS = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
XML_NS = "http://www.w3.org/XML/1998/namespace"
DC_NS = "http://purl.org/dc/elements/1.1/"
DCTERMS_NS = "http://purl.org/dc/terms/"
EDM_NS = "http://www.europeana.eu/schemas/edm/"
ORE_NS = "http://www.openarchives.org/ore/terms/"
SKOS_NS = "http://www.w3.org/2004/02/skos/core#"

_OAI = f"{{{OAI_NS}}}"
_RDF_RESOURCE = f"{{{RDF_NS}}}resource"
_RDF_ABOUT = f"{{{RDF_NS}}}about"
_XML_LANG = f"{{{XML_NS}}}lang"
_EIDR_IN_TEXT = re.compile(
    r"10\.5240/(?:[0-9A-Z]{4}-){5}[0-9A-Z]",
    re.IGNORECASE,
)
_IMDB_IN_TEXT = re.compile(
    r"(?<![A-Za-z0-9])(?:tt|nm|co)[0-9]{7,8}(?![0-9])",
    re.IGNORECASE,
)


class EuropeanaOAIError(RuntimeError):
    """Fail-closed OAI transport or protocol error."""


class EuropeanaBadResumptionTokenError(EuropeanaOAIError):
    """The source rejected an opaque checkpoint token."""


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class _RetryableStatus(Exception):
    def __init__(self, status: int, headers: Any) -> None:
        self.status = status
        self.headers = headers
        super().__init__(str(status))


@dataclass(frozen=True, slots=True)
class EuropeanaOAIRecord:
    source_record_id: str
    datestamp: str
    payload: dict[str, Any]


@dataclass(frozen=True, slots=True)
class EuropeanaOAIPage:
    body: bytes
    records: tuple[EuropeanaOAIRecord, ...]
    response_date: str
    resumption_token: str | None
    cursor: int | None = None
    complete_list_size: int | None = None
    token_expiration: str | None = None
    no_records_match: bool = False
    retry_count: int = 0
    rate_limit_count: int = 0


class EuropeanaOAIPageFetcher(Protocol):
    def fetch_list_records(
        self,
        *,
        set_spec: str | None,
        window_start: str,
        window_end: str,
        resumption_token: str | None,
    ) -> EuropeanaOAIPage: ...


@dataclass(frozen=True, slots=True)
class EuropeanaCaptureResult:
    capture: PublishedConnectorCapture
    control: PublishedCaptureWindowControl
    page_count: int
    record_count: int
    terminal: bool
    next_resumption_token: str | None
    retry_count: int
    rate_limit_count: int
    total_raw_bytes: int


def _bounded_finite(
    value: float,
    *,
    label: str,
    minimum: float,
    maximum: float,
) -> float:
    normalized = float(value)
    if not math.isfinite(normalized) or not minimum <= normalized <= maximum:
        raise ValueError(f"{label} must be between {minimum} and {maximum}")
    return normalized


def _bounded_integer(
    value: int,
    *,
    label: str,
    minimum: int,
    maximum: int,
) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not minimum <= value <= maximum
    ):
        raise ValueError(f"{label} must be between {minimum} and {maximum}")
    return value


def _retry_after_seconds(
    headers: Any,
    *,
    now: float,
    maximum: float,
) -> float | None:
    value = headers.get("Retry-After") if headers is not None else None
    if value is None:
        return None
    normalized = str(value).strip()
    try:
        seconds = float(normalized)
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(normalized)
        except (TypeError, ValueError, OverflowError):
            return None
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=UTC)
        seconds = retry_at.timestamp() - now
    if not math.isfinite(seconds):
        return None
    return min(maximum, max(0.0, seconds))


def _normalize_set_spec(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    if (
        not normalized
        or len(normalized) > MAX_SET_SPEC_LENGTH
        or any(character.isspace() for character in normalized)
        or any(character in normalized for character in "?#&")
    ):
        raise ValueError("set_spec must be a bounded OAI set identifier")
    return normalized


def _normalize_token(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    if (
        not normalized
        or len(normalized) > MAX_RESUMPTION_TOKEN_LENGTH
        or any(character in normalized for character in "\r\n\0")
    ):
        raise ValueError("resumption token must be non-empty and bounded")
    return normalized


def europeana_oai_url(
    *,
    set_spec: str | None,
    window_start: str,
    window_end: str,
    resumption_token: str | None = None,
) -> str:
    """Build the sole accepted ListRecords URL shape."""

    token = _normalize_token(resumption_token)
    if token is not None:
        query = (("verb", "ListRecords"), ("resumptionToken", token))
    else:
        start = require_rfc3339(window_start, label="window_start")
        end = require_rfc3339(window_end, label="window_end")
        if parse_rfc3339(end) < parse_rfc3339(start):
            raise ValueError("Europeana OAI window end precedes its start")
        query_values: list[tuple[str, str]] = [
            ("verb", "ListRecords"),
            ("metadataPrefix", "edm"),
            ("from", start),
            ("until", end),
        ]
        normalized_set = _normalize_set_spec(set_spec)
        if normalized_set is not None:
            query_values.append(("set", normalized_set))
        query = tuple(query_values)
    return f"{EUROPEANA_OAI_ENDPOINT}?{urlencode(query)}"


def _validate_response_url(actual: str, *, expected: str) -> None:
    parsed = urlsplit(actual)
    requested = urlsplit(expected)
    try:
        port = parsed.port
    except ValueError as exc:
        raise EuropeanaOAIError(
            "Europeana OAI response URL has an invalid port"
        ) from exc
    if (
        parsed.scheme != "https"
        or parsed.hostname != EUROPEANA_OAI_HOST
        or port not in {None, 443}
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path != EUROPEANA_OAI_PATH
        or parsed.fragment
        or parse_qs(parsed.query, keep_blank_values=True)
        != parse_qs(requested.query, keep_blank_values=True)
    ):
        raise EuropeanaOAIError(
            "Europeana OAI request left the approved official endpoint"
        )


def _content_length(headers: Any) -> int | None:
    value = headers.get("Content-Length") if headers is not None else None
    if value is None:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise EuropeanaOAIError("invalid Europeana Content-Length") from exc
    if parsed < 0:
        raise EuropeanaOAIError("invalid Europeana Content-Length")
    return parsed


class EuropeanaOAIClient:
    """Fixed-origin, redirect-free, respectfully paced OAI-PMH client."""

    def __init__(
        self,
        *,
        user_agent: str = DEFAULT_EUROPEANA_USER_AGENT,
        request_timeout_seconds: float = DEFAULT_REQUEST_TIMEOUT_SECONDS,
        minimum_request_interval_seconds: float = (
            DEFAULT_MINIMUM_REQUEST_INTERVAL_SECONDS
        ),
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        max_page_bytes: int = DEFAULT_MAX_PAGE_BYTES,
        max_records_per_page: int = DEFAULT_MAX_RECORDS,
        max_record_bytes: int = DEFAULT_MAX_RECORD_BYTES,
        retry_initial_backoff_seconds: float = (DEFAULT_RETRY_INITIAL_BACKOFF_SECONDS),
        retry_max_backoff_seconds: float = DEFAULT_RETRY_MAX_BACKOFF_SECONDS,
        max_retry_after_seconds: float = DEFAULT_MAX_RETRY_AFTER_SECONDS,
        opener: Any | None = None,
        clock=time.monotonic,
        wall_clock=time.time,
        sleeper=time.sleep,
    ) -> None:
        normalized_agent = user_agent.strip()
        if not normalized_agent or len(normalized_agent) > 256:
            raise ValueError("a bounded identifying User-Agent is required")
        self.request_timeout_seconds = _bounded_finite(
            request_timeout_seconds,
            label="request_timeout_seconds",
            minimum=0.001,
            maximum=MAX_TIMEOUT_SECONDS,
        )
        self.minimum_request_interval_seconds = _bounded_finite(
            minimum_request_interval_seconds,
            label="minimum_request_interval_seconds",
            minimum=0.0,
            maximum=MAX_MINIMUM_REQUEST_INTERVAL_SECONDS,
        )
        self.max_attempts = _bounded_integer(
            max_attempts,
            label="max_attempts",
            minimum=1,
            maximum=MAX_ATTEMPTS,
        )
        self.max_page_bytes = _bounded_integer(
            max_page_bytes,
            label="max_page_bytes",
            minimum=1,
            maximum=MAX_PAGE_BYTES,
        )
        self.max_records_per_page = _bounded_integer(
            max_records_per_page,
            label="max_records_per_page",
            minimum=1,
            maximum=MAX_RECORDS,
        )
        self.max_record_bytes = _bounded_integer(
            max_record_bytes,
            label="max_record_bytes",
            minimum=1,
            maximum=MAX_RECORD_BYTES,
        )
        self.retry_initial_backoff_seconds = _bounded_finite(
            retry_initial_backoff_seconds,
            label="retry_initial_backoff_seconds",
            minimum=0.001,
            maximum=MAX_BACKOFF_SECONDS,
        )
        self.retry_max_backoff_seconds = _bounded_finite(
            retry_max_backoff_seconds,
            label="retry_max_backoff_seconds",
            minimum=0.001,
            maximum=MAX_BACKOFF_SECONDS,
        )
        if self.retry_initial_backoff_seconds > self.retry_max_backoff_seconds:
            raise ValueError("initial retry backoff exceeds maximum backoff")
        self.max_retry_after_seconds = _bounded_finite(
            max_retry_after_seconds,
            label="max_retry_after_seconds",
            minimum=0.0,
            maximum=MAX_BACKOFF_SECONDS,
        )
        self.user_agent = normalized_agent
        self.opener = opener or build_opener(_NoRedirect)
        self.clock = clock
        self.wall_clock = wall_clock
        self.sleeper = sleeper
        self._last_request_at: float | None = None
        self._throttle_lock = threading.Lock()

    def fetch_list_records(
        self,
        *,
        set_spec: str | None,
        window_start: str,
        window_end: str,
        resumption_token: str | None,
    ) -> EuropeanaOAIPage:
        url = europeana_oai_url(
            set_spec=set_spec,
            window_start=window_start,
            window_end=window_end,
            resumption_token=resumption_token,
        )
        retries = 0
        rate_limits = 0
        for attempt in range(1, self.max_attempts + 1):
            self._throttle()
            request = Request(
                url,
                headers={
                    "Accept": "application/xml",
                    "Accept-Encoding": "identity",
                    "User-Agent": self.user_agent,
                },
                method="GET",
            )
            response = None
            try:
                try:
                    response = self.opener.open(
                        request,
                        timeout=self.request_timeout_seconds,
                    )
                except HTTPError as exc:
                    response = exc
                _validate_response_url(response.geturl(), expected=url)
                status = int(response.status)
                if 300 <= status < 400:
                    raise EuropeanaOAIError("Europeana OAI redirects are not accepted")
                if status == 429 or 500 <= status < 600:
                    raise _RetryableStatus(status, response.headers)
                if status != 200:
                    raise EuropeanaOAIError(
                        f"Europeana OAI failed closed with HTTP {status}"
                    )
                media_type = (
                    str(response.headers.get("Content-Type") or "")
                    .partition(";")[0]
                    .strip()
                    .lower()
                )
                if media_type not in {"application/xml", "text/xml"}:
                    raise EuropeanaOAIError(
                        "Europeana OAI success response must be XML"
                    )
                declared = _content_length(response.headers)
                if declared is not None and declared > self.max_page_bytes:
                    raise EuropeanaOAIError(
                        "Europeana OAI page exceeds configured byte bound"
                    )
                body = response.read(self.max_page_bytes + 1)
                if not isinstance(body, bytes) or not body:
                    raise EuropeanaOAIError("Europeana OAI returned an empty response")
                if len(body) > self.max_page_bytes:
                    raise EuropeanaOAIError(
                        "Europeana OAI page exceeds configured byte bound"
                    )
                page = parse_europeana_oai_page(
                    body,
                    max_records=self.max_records_per_page,
                    max_record_bytes=self.max_record_bytes,
                )
                return replace(
                    page,
                    retry_count=retries,
                    rate_limit_count=rate_limits,
                )
            except _RetryableStatus as exc:
                if exc.status == 429:
                    rate_limits += 1
                if attempt == self.max_attempts:
                    raise EuropeanaOAIError(
                        f"Europeana OAI exhausted retries after HTTP {exc.status}"
                    ) from exc
                retries += 1
                self.sleeper(self._retry_delay(exc.headers, attempt))
            except (URLError, TimeoutError, ConnectionError, OSError) as exc:
                if attempt == self.max_attempts:
                    raise EuropeanaOAIError(
                        "Europeana OAI request exhausted network retries"
                    ) from exc
                retries += 1
                self.sleeper(self._retry_delay(None, attempt))
            finally:
                if response is not None:
                    with suppress(Exception):
                        response.close()
        raise AssertionError("Europeana OAI retry loop did not terminate")

    def _throttle(self) -> None:
        with self._throttle_lock:
            now = self.clock()
            if self._last_request_at is not None:
                remaining = self.minimum_request_interval_seconds - (
                    now - self._last_request_at
                )
                if remaining > 0:
                    self.sleeper(remaining)
            self._last_request_at = self.clock()

    def _retry_delay(self, headers: Any, attempt: int) -> float:
        retry_after = _retry_after_seconds(
            headers,
            now=float(self.wall_clock()),
            maximum=self.max_retry_after_seconds,
        )
        if retry_after is not None:
            return retry_after
        return min(
            self.retry_max_backoff_seconds,
            self.retry_initial_backoff_seconds * (2 ** (attempt - 1)),
        )


def _namespace(tag: str) -> str:
    return tag[1:].partition("}")[0] if tag.startswith("{") else ""


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].rsplit(":", 1)[-1]


def _text(element: ET.Element) -> str | None:
    value = " ".join("".join(element.itertext()).split())
    return value or None


def _field_value(element: ET.Element) -> str | None:
    resource = element.attrib.get(_RDF_RESOURCE)
    if resource and resource.strip():
        return resource.strip()
    return _text(element)


def _safe_reference_url(value: str) -> str | None:
    if len(value) > 2048:
        return None
    parsed = urlsplit(value)
    try:
        port = parsed.port
    except ValueError:
        return None
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or port not in {None, 80, 443}
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        return None
    return value


def _unique_strings(values: Iterable[str | None]) -> list[str]:
    return sorted({value for value in values if isinstance(value, str) and value})


def _unique_objects(values: Iterable[dict[str, str]]) -> list[dict[str, str]]:
    keyed: dict[tuple[tuple[str, str], ...], dict[str, str]] = {}
    for value in values:
        normalized = {
            key: item for key, item in value.items() if isinstance(item, str) and item
        }
        if normalized:
            keyed[tuple(sorted(normalized.items()))] = normalized
    return [keyed[key] for key in sorted(keyed)]


def _fields(
    resources: Iterable[ET.Element],
    *,
    namespaces: set[str],
    names: set[str],
) -> Iterator[ET.Element]:
    for resource in resources:
        for field in resource:
            if _namespace(field.tag) in namespaces and _local_name(field.tag) in names:
                yield field


def _label_objects(
    value: str,
    *,
    resources_by_about: dict[str, ET.Element],
    role: str | None = None,
) -> list[dict[str, str]]:
    resource = resources_by_about.get(value)
    labels: list[dict[str, str]] = []
    if resource is not None:
        for field in resource:
            if (
                _namespace(field.tag) == SKOS_NS
                and _local_name(field.tag) in {"prefLabel", "altLabel"}
                and (text := _text(field))
            ):
                item = {
                    "value": text,
                    "uri": value,
                    "language": field.attrib.get(_XML_LANG, "und"),
                }
                if role is not None:
                    item["role"] = role
                labels.append(item)
    if labels:
        return labels
    item = {"value": value}
    if _safe_reference_url(value) is not None:
        item["uri"] = value
    if role is not None:
        item["role"] = role
    return [item]


def _descriptive_objects(
    resources: Iterable[ET.Element],
) -> list[ET.Element]:
    return [
        resource
        for resource in resources
        if _local_name(resource.tag) in {"ProvidedCHO", "Proxy"}
    ]


def _aggregation_objects(
    resources: Iterable[ET.Element],
) -> list[ET.Element]:
    return [
        resource
        for resource in resources
        if _local_name(resource.tag) in {"Aggregation", "EuropeanaAggregation"}
    ]


def _labeled_values(
    fields: Iterable[ET.Element],
    *,
    resources_by_about: dict[str, ET.Element],
    role: str | None = None,
) -> list[dict[str, str]]:
    values: list[dict[str, str]] = []
    for field in fields:
        value = _field_value(field)
        if not value:
            continue
        if field.attrib.get(_RDF_RESOURCE):
            values.extend(
                _label_objects(
                    value,
                    resources_by_about=resources_by_about,
                    role=role,
                )
            )
            continue
        item = {
            "value": value,
            "language": field.attrib.get(_XML_LANG, "und"),
        }
        if role is not None:
            item["role"] = role
        values.append(item)
    return _unique_objects(values)


def _extract_external_ids(values: Iterable[str]) -> list[dict[str, str]]:
    identifiers: dict[tuple[str, str], dict[str, str]] = {}
    for source in values:
        for match in _EIDR_IN_TEXT.finditer(source):
            try:
                value = normalize_eidr_id(match.group(0))
            except ValueError:
                continue
            identifiers[("eidr-content", value)] = {
                "namespace": "eidr-content",
                "value": value,
                "source": source,
            }
        for match in _IMDB_IN_TEXT.finditer(source):
            try:
                value = normalize_imdb_id(match.group(0))
            except ValueError:
                continue
            namespace = {
                "tt": "imdb-title",
                "nm": "imdb-name",
                "co": "imdb-company",
            }[value[:2]]
            identifiers[(namespace, value)] = {
                "namespace": namespace,
                "value": value,
                "source": source,
            }
    return [identifiers[key] for key in sorted(identifiers)]


def _parse_edm_record(
    record: ET.Element,
    *,
    max_record_bytes: int,
) -> EuropeanaOAIRecord:
    header = record.find(f"{_OAI}header")
    if header is None:
        raise EuropeanaOAIError("Europeana OAI record is missing its header")
    if header.attrib.get("status") == "deleted":
        raise EuropeanaOAIError(
            "Europeana advertises deletedRecord=no; deleted headers are unsupported"
        )
    identifier = header.findtext(f"{_OAI}identifier")
    datestamp = header.findtext(f"{_OAI}datestamp")
    if identifier is None or datestamp is None:
        raise EuropeanaOAIError("Europeana OAI record header is incomplete")
    source_record_id = normalize_europeana_record_id(identifier)
    modified = require_rfc3339(datestamp, label="OAI record datestamp")
    set_specs = _unique_strings(
        element.text.strip() if element.text else None
        for element in header.findall(f"{_OAI}setSpec")
    )
    metadata = record.find(f"{_OAI}metadata")
    if metadata is None or len(metadata) != 1:
        raise EuropeanaOAIError(
            "Europeana OAI record must contain one EDM metadata root"
        )
    rdf = metadata[0]
    if _namespace(rdf.tag) != RDF_NS or _local_name(rdf.tag) != "RDF":
        raise EuropeanaOAIError("Europeana OAI metadata must be EDM RDF/XML")
    resources = list(rdf)
    resources_by_about = {
        about: resource
        for resource in resources
        if (about := resource.attrib.get(_RDF_ABOUT))
    }
    descriptive = _descriptive_objects(resources)
    aggregations = _aggregation_objects(resources)

    title_fields = list(
        _fields(
            descriptive,
            namespaces={DC_NS},
            names={"title"},
        )
    )
    alternate_title_fields = list(
        _fields(
            descriptive,
            namespaces={DCTERMS_NS},
            names={"alternative"},
        )
    )
    titles = [
        *_labeled_values(
            title_fields,
            resources_by_about=resources_by_about,
            role="PRIMARY",
        ),
        *_labeled_values(
            alternate_title_fields,
            resources_by_about=resources_by_about,
            role="ALTERNATE",
        ),
    ]
    descriptions = _labeled_values(
        _fields(
            descriptive,
            namespaces={DC_NS, DCTERMS_NS},
            names={"description", "abstract"},
        ),
        resources_by_about=resources_by_about,
    )
    creators = _labeled_values(
        _fields(
            descriptive,
            namespaces={DC_NS, DCTERMS_NS},
            names={"creator"},
        ),
        resources_by_about=resources_by_about,
    )
    contributors = _labeled_values(
        _fields(
            descriptive,
            namespaces={DC_NS, DCTERMS_NS},
            names={"contributor"},
        ),
        resources_by_about=resources_by_about,
    )
    providers = _labeled_values(
        _fields(
            aggregations,
            namespaces={EDM_NS},
            names={"provider"},
        ),
        resources_by_about=resources_by_about,
        role="PROVIDER",
    )
    data_providers = _labeled_values(
        _fields(
            aggregations,
            namespaces={EDM_NS},
            names={"dataProvider"},
        ),
        resources_by_about=resources_by_about,
        role="DATA_PROVIDER",
    )

    def values(
        source: Iterable[ET.Element],
        *,
        namespaces: set[str],
        names: set[str],
    ) -> list[str]:
        result: list[str] = []
        for field in _fields(source, namespaces=namespaces, names=names):
            value = _field_value(field)
            if not value:
                continue
            resolved = _label_objects(
                value,
                resources_by_about=resources_by_about,
            )
            result.extend(item["value"] for item in resolved)
        return _unique_strings(result)

    times = values(
        descriptive,
        namespaces={DC_NS, DCTERMS_NS},
        names={"date", "temporal", "created", "issued"},
    )
    languages = _unique_strings(
        [
            *values(
                descriptive,
                namespaces={DC_NS, DCTERMS_NS},
                names={"language"},
            ),
            *values(
                aggregations,
                namespaces={EDM_NS},
                names={"language"},
            ),
        ]
    )
    countries = values(
        aggregations,
        namespaces={EDM_NS},
        names={"country"},
    )
    types = _unique_strings(
        [
            *values(
                descriptive,
                namespaces={DC_NS, DCTERMS_NS, EDM_NS},
                names={"type"},
            ),
            *values(
                aggregations,
                namespaces={EDM_NS},
                names={"type"},
            ),
        ]
    )
    identifiers = values(
        descriptive,
        namespaces={DC_NS, DCTERMS_NS},
        names={"identifier"},
    )

    def reference_urls(names: set[str]) -> list[str]:
        return _unique_strings(
            safe
            for field in _fields(
                aggregations,
                namespaces={EDM_NS},
                names=names,
            )
            if (value := _field_value(field))
            if (safe := _safe_reference_url(value))
        )

    landing_urls = reference_urls({"landingPage", "isShownAt"})
    preview_urls = reference_urls({"preview", "object"})
    media_urls = reference_urls({"isShownBy", "hasView"})
    edm_rights = values(
        resources,
        namespaces={EDM_NS},
        names={"rights"},
    )
    dc_rights = values(
        descriptive,
        namespaces={DC_NS, DCTERMS_NS},
        names={"rights", "license", "accessRights"},
    )
    all_rights = _unique_strings([*edm_rights, *dc_rights])
    rights_statements = [
        value
        for value in all_rights
        if (urlsplit(value).hostname or "").lower().endswith("rightsstatements.org")
    ]
    licenses = [
        value
        for value in all_rights
        if (urlsplit(value).hostname or "").lower().endswith("creativecommons.org")
    ]
    rights_status = "DECLARED" if all_rights else "MISSING_ASSUME_COPYRIGHT"
    referenced_resources: list[dict[str, Any]] = []
    for role, urls in (
        ("PREVIEW", preview_urls),
        ("MEDIA", media_urls),
    ):
        for url in urls:
            resource = resources_by_about.get(url)
            direct_edm = (
                []
                if resource is None
                else values(
                    (resource,),
                    namespaces={EDM_NS},
                    names={"rights"},
                )
            )
            direct_dc = (
                []
                if resource is None
                else values(
                    (resource,),
                    namespaces={DC_NS, DCTERMS_NS},
                    names={"rights", "license", "accessRights"},
                )
            )
            resource_edm = direct_edm or edm_rights
            resource_dc = direct_dc or dc_rights
            resource_all = _unique_strings([*resource_edm, *resource_dc])
            referenced_resources.append(
                {
                    "url": url,
                    "role": role,
                    "status": (
                        "DECLARED" if resource_all else "MISSING_ASSUME_COPYRIGHT"
                    ),
                    "rightsBasis": (
                        "RESOURCE" if direct_edm or direct_dc else "RECORD"
                    ),
                    "edmRights": resource_edm,
                    "dcRights": resource_dc,
                    "rightsStatements": [
                        value
                        for value in resource_all
                        if (urlsplit(value).hostname or "")
                        .lower()
                        .endswith("rightsstatements.org")
                    ],
                    "licenses": [
                        value
                        for value in resource_all
                        if (urlsplit(value).hostname or "")
                        .lower()
                        .endswith("creativecommons.org")
                    ],
                }
            )
    oai_identifier = identifier.strip()
    record_urls = _unique_strings(
        (
            europeana_record_url(source_record_id),
            _safe_reference_url(oai_identifier),
        )
    )
    payload: dict[str, Any] = {
        "id": source_record_id,
        "oaiIdentifier": oai_identifier,
        "datestamp": modified,
        "setSpecs": set_specs,
        "titles": _unique_objects(titles),
        "descriptions": descriptions,
        "times": times,
        "languages": languages,
        "countries": countries,
        "types": types,
        "providers": providers,
        "dataProviders": data_providers,
        "creators": creators,
        "contributors": contributors,
        "recordUrls": record_urls,
        "landingUrls": landing_urls,
        "previewUrls": preview_urls,
        "mediaUrls": media_urls,
        "identifiers": identifiers,
        "externalIds": _extract_external_ids(identifiers),
        "metadataRights": {
            "licenseId": "CC0-1.0",
            "licenseUri": EUROPEANA_METADATA_LICENSE_URI,
            "appliesTo": "METADATA_ONLY",
        },
        "digitalObjectRights": {
            "status": rights_status,
            "edmRights": edm_rights,
            "dcRights": dc_rights,
            "rightsStatements": rights_statements,
            "licenses": licenses,
            "referencedResources": referenced_resources,
            "appliesTo": "DIGITAL_OBJECT_AND_PREVIEW",
        },
        "binaryAcquisition": "DISABLED",
    }
    if len(canonical_json_bytes(payload)) > max_record_bytes:
        raise EuropeanaOAIError(
            f"Europeana record {source_record_id} exceeds configured byte bound"
        )
    return EuropeanaOAIRecord(
        source_record_id=source_record_id,
        datestamp=modified,
        payload=payload,
    )


def parse_europeana_oai_page(
    body: bytes,
    *,
    max_records: int = DEFAULT_MAX_RECORDS,
    max_record_bytes: int = DEFAULT_MAX_RECORD_BYTES,
) -> EuropeanaOAIPage:
    """Decode one bounded ListRecords response with hardened XML parsing."""

    _bounded_integer(
        max_records,
        label="max_records",
        minimum=1,
        maximum=MAX_RECORDS,
    )
    _bounded_integer(
        max_record_bytes,
        label="max_record_bytes",
        minimum=1,
        maximum=MAX_RECORD_BYTES,
    )
    if not body:
        raise EuropeanaOAIError("Europeana OAI response is empty")
    upper = body.upper()
    if b"<!DOCTYPE" in upper or b"<!ENTITY" in upper:
        raise EuropeanaOAIError("DOCTYPE and ENTITY declarations are not accepted")
    try:
        root = fromstring(
            body,
            forbid_dtd=True,
            forbid_entities=True,
            forbid_external=True,
        )
    except (ET.ParseError, DefusedXmlException) as exc:
        raise EuropeanaOAIError("Europeana OAI returned invalid XML") from exc
    if root.tag != f"{_OAI}OAI-PMH":
        raise EuropeanaOAIError("Europeana OAI response has an unexpected root")
    response_date = root.findtext(f"{_OAI}responseDate")
    if response_date is None:
        raise EuropeanaOAIError("Europeana OAI responseDate is missing")
    response_date = require_rfc3339(response_date, label="OAI responseDate")
    errors = root.findall(f"{_OAI}error")
    if errors:
        codes = [str(error.attrib.get("code") or "") for error in errors]
        if codes == ["noRecordsMatch"]:
            return EuropeanaOAIPage(
                body=body,
                records=(),
                response_date=response_date,
                resumption_token=None,
                no_records_match=True,
            )
        if "badResumptionToken" in codes:
            raise EuropeanaBadResumptionTokenError(
                "Europeana rejected the supplied resumption token"
            )
        raise EuropeanaOAIError(
            f"Europeana OAI protocol error: {','.join(codes) or 'unknown'}"
        )
    listing = root.find(f"{_OAI}ListRecords")
    if listing is None:
        raise EuropeanaOAIError("Europeana OAI response omitted ListRecords")
    record_elements = listing.findall(f"{_OAI}record")
    if len(record_elements) > max_records:
        raise EuropeanaOAIError(
            "Europeana OAI page exceeds configured record-count bound"
        )
    records = tuple(
        _parse_edm_record(record, max_record_bytes=max_record_bytes)
        for record in record_elements
    )
    identities = [record.source_record_id for record in records]
    if len(identities) != len(set(identities)):
        raise EuropeanaOAIError("Europeana OAI page contains duplicate record IDs")
    token_element = listing.find(f"{_OAI}resumptionToken")
    token = None
    cursor = None
    complete_list_size = None
    expiration = None
    if token_element is not None:
        raw_token = token_element.text
        token = (
            None
            if raw_token is None or not raw_token.strip()
            else _normalize_token(raw_token)
        )
        for attribute, label in (
            ("cursor", "cursor"),
            ("completeListSize", "completeListSize"),
        ):
            raw = token_element.attrib.get(attribute)
            if raw is None:
                continue
            try:
                parsed = int(raw)
            except ValueError as exc:
                raise EuropeanaOAIError(
                    f"Europeana resumptionToken {label} is invalid"
                ) from exc
            if parsed < 0:
                raise EuropeanaOAIError(f"Europeana resumptionToken {label} is invalid")
            if attribute == "cursor":
                cursor = parsed
            else:
                complete_list_size = parsed
        expiration = token_element.attrib.get("expirationDate")
        if expiration is not None:
            expiration = require_rfc3339(
                expiration,
                label="resumptionToken expirationDate",
            )
    return EuropeanaOAIPage(
        body=body,
        records=records,
        response_date=response_date,
        resumption_token=token,
        cursor=cursor,
        complete_list_size=complete_list_size,
        token_expiration=expiration,
    )


def _validate_resume_watermark(
    watermark: SourceWatermark,
    *,
    window_start: str,
    window_end: str,
    config_digest: str,
    image_digest: str,
    policy_digest: str,
) -> str:
    if (
        watermark.source_product_id != EUROPEANA_SOURCE_PRODUCT_ID
        or watermark.window_start != window_start
        or watermark.window_end != window_end
        or watermark.config_digest != config_digest
        or watermark.image_digest != image_digest
        or watermark.policy_digest != policy_digest
    ):
        raise ValueError("Europeana resume watermark does not bind this capture")
    if watermark.cursor is None:
        raise ValueError("Europeana resume watermark is terminal")
    return _normalize_token(watermark.cursor) or ""


def capture_europeana_oai(
    *,
    destination_prefix: str,
    acquired_at: str,
    window_start: str,
    window_end: str,
    image_digest: str,
    config_digest: str,
    fetcher: EuropeanaOAIPageFetcher,
    store: RuntimeObjectStore,
    set_spec: str | None = None,
    resume_watermark: SourceWatermark | None = None,
    max_pages: int = DEFAULT_MAX_PAGES,
    max_records: int = DEFAULT_MAX_RECORDS,
    max_page_bytes: int = DEFAULT_MAX_PAGE_BYTES,
    max_total_bytes: int = DEFAULT_MAX_TOTAL_BYTES,
    max_record_bytes: int = DEFAULT_MAX_RECORD_BYTES,
    record_shard_bytes: int = DEFAULT_RECORD_SHARD_BYTES,
) -> EuropeanaCaptureResult:
    """Capture a bounded OAI window as immutable PARTIAL/DELTA artifacts."""

    acquired = require_rfc3339(acquired_at, label="acquired_at")
    start = require_rfc3339(window_start, label="window_start")
    end = require_rfc3339(window_end, label="window_end")
    if parse_rfc3339(end) < parse_rfc3339(start):
        raise ValueError("Europeana capture window end precedes its start")
    image = require_sha256(image_digest, label="image_digest")
    config = require_sha256(config_digest, label="config_digest")
    normalized_set = _normalize_set_spec(set_spec)
    max_pages = _bounded_integer(
        max_pages,
        label="max_pages",
        minimum=1,
        maximum=MAX_PAGES,
    )
    max_records = _bounded_integer(
        max_records,
        label="max_records",
        minimum=1,
        maximum=MAX_RECORDS,
    )
    max_page_bytes = _bounded_integer(
        max_page_bytes,
        label="max_page_bytes",
        minimum=1,
        maximum=MAX_PAGE_BYTES,
    )
    max_total_bytes = _bounded_integer(
        max_total_bytes,
        label="max_total_bytes",
        minimum=1,
        maximum=MAX_TOTAL_BYTES,
    )
    max_record_bytes = _bounded_integer(
        max_record_bytes,
        label="max_record_bytes",
        minimum=1,
        maximum=MAX_RECORD_BYTES,
    )
    if record_shard_bytes < 1:
        raise ValueError("record_shard_bytes must be positive")
    policy = europeana_metadata_rights_profile()
    token = (
        None
        if resume_watermark is None
        else _validate_resume_watermark(
            resume_watermark,
            window_start=start,
            window_end=end,
            config_digest=config,
            image_digest=image,
            policy_digest=policy.digest,
        )
    )
    watermark_before = token or start
    capture_id = deterministic_key(
        "europeana-oai-capture-v1",
        {
            "acquiredAt": acquired,
            "windowStart": start,
            "windowEnd": end,
            "setSpec": normalized_set,
            "imageDigest": image,
            "configDigest": config,
            "policyDigest": policy.digest,
            "watermarkBefore": watermark_before,
        },
    ).removeprefix("sha256:")
    pages: list[tuple[EuropeanaOAIPage, ObjectRef]] = []
    records: list[tuple[EuropeanaOAIRecord, ObjectRef, int, int]] = []
    seen_ids: set[str] = set()
    seen_tokens: set[str] = set() if token is None else {token}
    total_bytes = 0
    retries = 0
    rate_limits = 0
    terminal = False
    next_token = token
    last_response_date = acquired
    last_datestamp: str | None = None

    for page_index in range(max_pages):
        page = fetcher.fetch_list_records(
            set_spec=normalized_set,
            window_start=start,
            window_end=end,
            resumption_token=next_token,
        )
        if not page.body or len(page.body) > max_page_bytes:
            raise EuropeanaOAIError("Europeana raw page violates its byte bound")
        total_bytes += len(page.body)
        if total_bytes > max_total_bytes:
            raise EuropeanaOAIError("Europeana capture exceeds total raw byte bound")
        if len(records) + len(page.records) > max_records:
            raise EuropeanaOAIError(
                "Europeana page would cross max_records; no partial page was committed"
            )
        page_digest = sha256_digest(page.body)
        raw_object = store.upload_bytes(
            page.body,
            join_uri(
                destination_prefix,
                "europeana",
                "oai-pages",
                capture_id,
                f"page={page_index:05d}",
                f"{page_digest.removeprefix('sha256:')}.xml",
            ),
            media_type="application/xml",
            object_format="OBJECT_FORMAT_OTHER",
            max_bytes=max_page_bytes,
        ).object_ref
        pages.append((page, raw_object))
        for record_index, record in enumerate(page.records):
            if record.source_record_id in seen_ids:
                raise EuropeanaOAIError(
                    "Europeana capture contains duplicate record IDs"
                )
            seen_ids.add(record.source_record_id)
            if len(canonical_json_bytes(record.payload)) > max_record_bytes:
                raise EuropeanaOAIError(
                    "Europeana normalized record exceeds its byte bound"
                )
            records.append((record, raw_object, page_index, record_index))
            if last_datestamp is None or record.datestamp > last_datestamp:
                last_datestamp = record.datestamp
        retries += page.retry_count
        rate_limits += page.rate_limit_count
        last_response_date = page.response_date
        next_token = page.resumption_token
        if next_token is None:
            terminal = True
            break
        if next_token in seen_tokens:
            raise EuropeanaOAIError("Europeana repeated a resumption token")
        seen_tokens.add(next_token)
        if len(records) >= max_records:
            break

    if not pages:
        raise AssertionError("Europeana capture loop produced no raw page")
    batch = build_connector_batch_manifest(
        source_system_id=EUROPEANA_SOURCE_SYSTEM_ID,
        source_product_id=EUROPEANA_SOURCE_PRODUCT_ID,
        connector_id=EUROPEANA_OAI_CONNECTOR_ID,
        connector_version="1.0.0",
        image_digest=image,
        config_digest=config,
        policy_id=policy.policy_id,
        policy_digest=policy.digest,
        transport_kind=TransportKind.FEED,
        serialization=Serialization.XML,
        change_semantics=ChangeSemantics.DELTA,
        completeness=Completeness.PARTIAL,
        delete_coverage=DeleteCoverage.NONE,
        coverage_scope={
            "endpoint": EUROPEANA_OAI_PATH,
            "metadataPrefix": "edm",
            "setSpec": normalized_set,
            "windowStart": start,
            "windowEnd": end,
            "pageCount": len(pages),
            "recordCount": len(records),
            "maxPages": max_pages,
            "maxRecords": max_records,
            "maxPageBytes": max_page_bytes,
            "maxTotalBytes": max_total_bytes,
            "maxRecordBytes": max_record_bytes,
            "terminal": terminal,
            "metadataLicenseUri": EUROPEANA_METADATA_LICENSE_URI,
            "mediaBinaryAcquisition": "DISABLED",
            "sourceCompleteness": "PARTIAL",
        },
        source_window=SourceWindow(start=start, end=end),
        watermark_before=watermark_before,
        watermark_after=next_token or last_datestamp or last_response_date,
        raw_objects=tuple(reference for _, reference in pages),
        acquired_at=acquired,
        record_count=len(records),
        error_count=0,
        retry_count=retries,
        rate_limit_count=rate_limits,
    )

    def envelopes() -> Iterator[Any]:
        for record, raw_object, page_index, record_index in records:
            yield build_connector_record_envelope(
                payload=record.payload,
                batch_id=batch.batch_id,
                source_system_id=EUROPEANA_SOURCE_SYSTEM_ID,
                source_product_id=EUROPEANA_SOURCE_PRODUCT_ID,
                source_namespace_id=EUROPEANA_RECORD_NAMESPACE_ID,
                source_record_id=record.source_record_id,
                source_revision=record.datestamp,
                operation=RecordOperation.UPSERT,
                source_modified_at=record.datestamp,
                observed_at=acquired,
                ingested_at=acquired,
                payload_schema="europeana-edm-record-v1",
                raw_object=raw_object,
                source_location=(
                    f"{EUROPEANA_OAI_PATH}page/{page_index}/record/{record_index}"
                ),
                policy_id=policy.policy_id,
                policy_digest=policy.digest,
            )

    capture = publish_connector_capture(
        destination_prefix=destination_prefix,
        batch=batch,
        envelopes=envelopes(),
        store=store,
        record_shard_bytes=record_shard_bytes,
    )
    receipt = build_capture_window_receipt(
        source_product_id=EUROPEANA_SOURCE_PRODUCT_ID,
        window_start=start,
        window_end=end,
        cursor=next_token,
        watermark=last_datestamp or last_response_date,
        batch_object=capture.batch_manifest_object,
        status=(
            CaptureWindowStatus.COMMITTED if records else CaptureWindowStatus.EMPTY
        ),
        config_digest=config,
        image_digest=image,
        policy_digest=policy.digest,
    )
    control = publish_capture_window_commit(
        destination_prefix=destination_prefix,
        receipt=receipt,
        store=store,
    )
    return EuropeanaCaptureResult(
        capture=capture,
        control=control,
        page_count=len(pages),
        record_count=len(records),
        terminal=terminal,
        next_resumption_token=next_token,
        retry_count=retries,
        rate_limit_count=rate_limits,
        total_raw_bytes=total_bytes,
    )


def read_europeana_watermark(
    *,
    reference: ObjectRef,
    store: RuntimeObjectStore,
) -> SourceWatermark:
    """Read and verify one immutable generic SourceWatermark."""

    if (
        reference.media_type
        != "application/vnd.video-media-catalog.source-watermark.v1+json"
        or reference.format != "OBJECT_FORMAT_JSON"
        or not 0 < reference.size_bytes <= CONTROL_OBJECT_MAX_BYTES
    ):
        raise ValueError("Europeana watermark ObjectRef has an invalid contract")
    store.verify(reference, max_bytes=CONTROL_OBJECT_MAX_BYTES)
    with tempfile.TemporaryDirectory(prefix="europeana-watermark-") as directory:
        materialized = store.download(
            reference,
            Path(directory) / "watermark.json",
            max_bytes=CONTROL_OBJECT_MAX_BYTES,
        )
        watermark = SourceWatermark.model_validate_json(materialized.path.read_bytes())
    if watermark.source_product_id != EUROPEANA_SOURCE_PRODUCT_ID:
        raise ValueError("watermark belongs to another source product")
    return watermark
