from __future__ import annotations

import re
from io import BytesIO
from pathlib import Path
from urllib.error import HTTPError

import pytest

from video_media_catalog.connector import ConnectorRecordEnvelope
from video_media_catalog.europeana_oai import (
    EUROPEANA_OAI_ENDPOINT,
    EuropeanaBadResumptionTokenError,
    EuropeanaOAIClient,
    EuropeanaOAIError,
    capture_europeana_oai,
    europeana_oai_url,
    parse_europeana_oai_page,
    read_europeana_watermark,
)
from video_media_catalog.europeana_oai_cli import build_parser
from video_media_catalog.object_store import BoundedObjectStore
from video_media_catalog.storage import local_path

FIXTURE = Path(__file__).parent / "fixtures" / "europeana_oai.xml"
IMAGE = "sha256:" + ("a" * 64)
CONFIG = "sha256:" + ("b" * 64)
START = "2026-09-25T00:00:00Z"
END = "2026-09-25T23:59:59Z"


def _terminal_page() -> bytes:
    body = FIXTURE.read_bytes()
    body = body.replace(b"item-1", b"item-3").replace(b"item-2", b"item-4")
    return re.sub(
        rb"\s*<resumptionToken\b.*?</resumptionToken>",
        b"",
        body,
        flags=re.DOTALL,
    )


def _error_page(code: str, message: str) -> bytes:
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <responseDate>2026-09-26T00:00:05Z</responseDate>
  <request verb="ListRecords">{EUROPEANA_OAI_ENDPOINT}</request>
  <error code="{code}">{message}</error>
</OAI-PMH>""".encode()


class FakeFetcher:
    def __init__(self, *bodies: bytes) -> None:
        self.pages = [parse_europeana_oai_page(body) for body in bodies]
        self.calls: list[str | None] = []

    def fetch_list_records(
        self,
        *,
        set_spec: str | None,
        window_start: str,
        window_end: str,
        resumption_token: str | None,
    ):
        assert set_spec == "123"
        assert window_start == START
        assert window_end == END
        self.calls.append(resumption_token)
        return self.pages.pop(0)


class FakeResponse(BytesIO):
    def __init__(
        self,
        body: bytes,
        *,
        url: str,
        status: int = 200,
        headers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(body)
        self.status = status
        self._url = url
        self.headers = {"Content-Type": "application/xml"}
        self.headers.update(headers or {})

    def geturl(self) -> str:
        return self._url


class QueueOpener:
    def __init__(self, *outcomes) -> None:
        self.outcomes = list(outcomes)
        self.calls = []

    def open(self, request, *, timeout):
        self.calls.append((request, timeout))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


def _client(opener: QueueOpener, *, sleeper=lambda _: None, **values):
    return EuropeanaOAIClient(
        opener=opener,
        minimum_request_interval_seconds=0,
        sleeper=sleeper,
        **values,
    )


def test_europeana_capture_is_partial_delta_and_resumable(tmp_path) -> None:
    store = BoundedObjectStore(client=object())
    first_fetcher = FakeFetcher(FIXTURE.read_bytes())
    first = capture_europeana_oai(
        destination_prefix=tmp_path.as_uri(),
        acquired_at="2026-09-26T00:00:05Z",
        window_start=START,
        window_end=END,
        image_digest=IMAGE,
        config_digest=CONFIG,
        fetcher=first_fetcher,
        store=store,
        set_spec="123",
        max_pages=1,
        max_records=2,
        record_shard_bytes=16_384,
    )

    assert first_fetcher.calls == [None]
    assert first.record_count == 2
    assert not first.terminal
    assert first.next_resumption_token == "opaque-token-2"
    batch = first.capture.batch_manifest
    assert batch.change_semantics.value == "DELTA"
    assert batch.completeness.value == "PARTIAL"
    assert batch.delete_coverage.value == "NONE"
    assert batch.coverage_scope["mediaBinaryAcquisition"] == "DISABLED"
    assert first.control.source_watermark.cursor == "opaque-token-2"
    assert local_path(first.control.source_watermark_object.uri).exists()
    restored = read_europeana_watermark(
        reference=first.control.source_watermark_object,
        store=store,
    )
    assert restored == first.control.source_watermark
    with pytest.raises(EuropeanaOAIError, match="repeated a resumption token"):
        capture_europeana_oai(
            destination_prefix=tmp_path.as_uri(),
            acquired_at="2026-09-26T00:05:05Z",
            window_start=START,
            window_end=END,
            image_digest=IMAGE,
            config_digest=CONFIG,
            fetcher=FakeFetcher(FIXTURE.read_bytes()),
            store=store,
            set_spec="123",
            resume_watermark=restored,
            max_pages=1,
            max_records=2,
            record_shard_bytes=16_384,
        )

    records = [
        ConnectorRecordEnvelope.model_validate_json(line)
        for reference in first.capture.record_set_manifest.record_objects
        for line in local_path(reference.uri).read_bytes().splitlines()
    ]
    assert [record.source_record_id for record in records] == [
        "/123/item-1",
        "/123/item-2",
    ]
    assert all(record.raw_object.media_type == "application/xml" for record in records)

    second_fetcher = FakeFetcher(_terminal_page())
    second = capture_europeana_oai(
        destination_prefix=tmp_path.as_uri(),
        acquired_at="2026-09-26T00:10:05Z",
        window_start=START,
        window_end=END,
        image_digest=IMAGE,
        config_digest=CONFIG,
        fetcher=second_fetcher,
        store=store,
        set_spec="123",
        resume_watermark=restored,
        max_pages=1,
        max_records=2,
        record_shard_bytes=16_384,
    )
    assert second_fetcher.calls == ["opaque-token-2"]
    assert second.terminal
    assert second.next_resumption_token is None
    assert second.control.source_watermark.cursor is None
    assert second.control.source_watermark.watermark == "2026-09-25T12:35:56Z"


def test_europeana_capture_replay_is_deterministic(tmp_path) -> None:
    values = {
        "destination_prefix": tmp_path.as_uri(),
        "acquired_at": "2026-09-26T00:00:05Z",
        "window_start": START,
        "window_end": END,
        "image_digest": IMAGE,
        "config_digest": CONFIG,
        "store": BoundedObjectStore(client=object()),
        "set_spec": "123",
        "max_pages": 1,
        "max_records": 2,
        "record_shard_bytes": 16_384,
    }
    first = capture_europeana_oai(
        fetcher=FakeFetcher(FIXTURE.read_bytes()),
        **values,
    )
    replay = capture_europeana_oai(
        fetcher=FakeFetcher(FIXTURE.read_bytes()),
        **values,
    )
    assert replay.capture.batch_manifest == first.capture.batch_manifest
    assert replay.capture.record_set_manifest == first.capture.record_set_manifest
    assert replay.control.source_watermark == first.control.source_watermark
    assert replay.control.receipt == first.control.receipt


def test_europeana_capture_commits_empty_no_records_window(tmp_path) -> None:
    body = _error_page("noRecordsMatch", "No records found!")
    result = capture_europeana_oai(
        destination_prefix=tmp_path.as_uri(),
        acquired_at="2026-09-26T00:00:05Z",
        window_start=START,
        window_end=END,
        image_digest=IMAGE,
        config_digest=CONFIG,
        fetcher=FakeFetcher(body),
        store=BoundedObjectStore(client=object()),
        set_spec="123",
    )
    assert result.record_count == 0
    assert result.terminal
    assert result.capture.record_set_manifest.record_objects == ()
    assert result.control.receipt.status.value == "EMPTY"


def test_europeana_client_is_keyless_fixed_origin_and_resumes() -> None:
    initial_url = europeana_oai_url(
        set_spec="123",
        window_start=START,
        window_end=END,
    )
    opener = QueueOpener(FakeResponse(FIXTURE.read_bytes(), url=initial_url))
    client = _client(opener)
    page = client.fetch_list_records(
        set_spec="123",
        window_start=START,
        window_end=END,
        resumption_token=None,
    )
    assert len(page.records) == 2
    request, timeout = opener.calls[0]
    headers = {key.lower(): value for key, value in request.header_items()}
    assert request.full_url == initial_url
    assert timeout == 30
    assert headers["user-agent"]
    assert headers["accept-encoding"] == "identity"
    assert "authorization" not in headers
    assert "x-api-key" not in headers
    assert "wskey" not in request.full_url

    token_url = europeana_oai_url(
        set_spec="ignored-on-resume",
        window_start=START,
        window_end=END,
        resumption_token="opaque token/+",
    )
    assert "metadataPrefix" not in token_url
    assert "set=" not in token_url
    assert "resumptionToken=opaque+token%2F%2B" in token_url


def test_europeana_client_honors_retry_after_and_retries_5xx() -> None:
    url = europeana_oai_url(
        set_spec="123",
        window_start=START,
        window_end=END,
    )
    throttled = HTTPError(
        url,
        429,
        "rate limited",
        {"Retry-After": "7"},
        BytesIO(),
    )
    unavailable = HTTPError(url, 503, "unavailable", {}, BytesIO())
    sleeps: list[float] = []
    client = _client(
        QueueOpener(
            throttled,
            unavailable,
            FakeResponse(FIXTURE.read_bytes(), url=url),
        ),
        sleeper=sleeps.append,
        retry_initial_backoff_seconds=0.5,
    )
    page = client.fetch_list_records(
        set_spec="123",
        window_start=START,
        window_end=END,
        resumption_token=None,
    )
    assert sleeps == [7, 1.0]
    assert page.retry_count == 2
    assert page.rate_limit_count == 1


def test_europeana_client_retries_timeout_then_fails_closed() -> None:
    client = _client(
        QueueOpener(TimeoutError("slow"), TimeoutError("still slow")),
        max_attempts=2,
    )
    with pytest.raises(EuropeanaOAIError, match="network retries"):
        client.fetch_list_records(
            set_spec="123",
            window_start=START,
            window_end=END,
            resumption_token=None,
        )


@pytest.mark.parametrize(
    "response",
    [
        FakeResponse(
            b"<xml/>",
            url=europeana_oai_url(
                set_spec="123",
                window_start=START,
                window_end=END,
            ),
            headers={"Content-Length": "999"},
        ),
        FakeResponse(
            b"x" * 9,
            url=europeana_oai_url(
                set_spec="123",
                window_start=START,
                window_end=END,
            ),
        ),
    ],
)
def test_europeana_client_rejects_oversize_response(response: FakeResponse) -> None:
    client = _client(QueueOpener(response), max_page_bytes=8)
    with pytest.raises(EuropeanaOAIError, match="byte bound"):
        client.fetch_list_records(
            set_spec="123",
            window_start=START,
            window_end=END,
            resumption_token=None,
        )


def test_europeana_client_rejects_redirect_or_off_origin_response() -> None:
    url = europeana_oai_url(
        set_spec="123",
        window_start=START,
        window_end=END,
    )
    redirect = _client(
        QueueOpener(
            FakeResponse(
                b"",
                url=url,
                status=302,
                headers={"Location": "https://attacker.example/oai"},
            )
        )
    )
    with pytest.raises(EuropeanaOAIError, match="redirect"):
        redirect.fetch_list_records(
            set_spec="123",
            window_start=START,
            window_end=END,
            resumption_token=None,
        )

    wrong_host = _client(
        QueueOpener(
            FakeResponse(
                FIXTURE.read_bytes(),
                url=url.replace("api.europeana.eu", "attacker.example"),
            )
        )
    )
    with pytest.raises(EuropeanaOAIError, match="approved"):
        wrong_host.fetch_list_records(
            set_spec="123",
            window_start=START,
            window_end=END,
            resumption_token=None,
        )


def test_europeana_oai_errors_are_semantically_distinct() -> None:
    empty = parse_europeana_oai_page(_error_page("noRecordsMatch", "No records found!"))
    assert empty.no_records_match
    assert empty.records == ()

    with pytest.raises(EuropeanaBadResumptionTokenError):
        parse_europeana_oai_page(_error_page("badResumptionToken", "Expired token"))


def test_europeana_capture_rejects_page_boundary_record_loss(tmp_path) -> None:
    with pytest.raises(EuropeanaOAIError, match="partial page"):
        capture_europeana_oai(
            destination_prefix=tmp_path.as_uri(),
            acquired_at="2026-09-26T00:00:05Z",
            window_start=START,
            window_end=END,
            image_digest=IMAGE,
            config_digest=CONFIG,
            fetcher=FakeFetcher(FIXTURE.read_bytes()),
            store=BoundedObjectStore(client=object()),
            set_spec="123",
            max_records=1,
        )


def test_europeana_cli_is_bounded_and_has_no_static_key_argument() -> None:
    parser = build_parser()
    parsed = parser.parse_args(
        [
            "--destination-prefix",
            "file:///tmp/europeana",
            "--acquired-at",
            "2026-09-26T00:00:00Z",
            "--window-start",
            START,
            "--window-end",
            END,
            "--image-digest",
            IMAGE,
        ]
    )
    assert parsed.max_pages == 5
    assert parsed.max_records == 100
    assert parsed.checkpoint_uri is None
    option_strings = {
        option for action in parser._actions for option in action.option_strings
    }
    assert "--api-key" not in option_strings
    assert "--wskey" not in option_strings
