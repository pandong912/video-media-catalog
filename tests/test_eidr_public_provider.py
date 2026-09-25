from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pytest

from video_media_catalog.community_sources import eidr_rights_profile
from video_media_catalog.eidr_backfill import EidrLookupStatus
from video_media_catalog.eidr_public_provider import (
    EIDR_PUBLIC_PROVIDER_ID,
    EidrPublicProvider,
    EidrPublicProviderError,
)
from video_media_catalog.models import Checksum, ObjectRef

EIDR_ID = "10.5240/AAAA-BBBB-CCCC-DDDD-EEEE-C"
EXPECTED_URL = (
    f"https://resolve.eidr.org/EIDR/object/{EIDR_ID}?type=Full&followAlias=false"
)
XML = f"<FullMetadata><ID>{EIDR_ID}</ID></FullMetadata>".encode()


def _evidence() -> ObjectRef:
    return ObjectRef(
        uri="file:///tmp/eidr-public-authorization.json",
        format="OBJECT_FORMAT_JSON",
        media_type="application/json",
        checksum=Checksum(value="a" * 64),
        size_bytes=1,
    )


class FakeResponse:
    def __init__(
        self,
        *,
        status: int,
        body: bytes = b"",
        url: str = EXPECTED_URL,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        self.status = status
        self.body = body
        self.url = url
        self.headers = {"Content-Type": "application/xml"} if status == 200 else {}
        self.headers.update(headers or {})
        self.closed = False
        self.read_limit: int | None = None

    def geturl(self) -> str:
        return self.url

    def read(self, limit: int) -> bytes:
        self.read_limit = limit
        return self.body[:limit]

    def close(self) -> None:
        self.closed = True


class SequenceOpener:
    def __init__(self, *outcomes: FakeResponse | BaseException) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[tuple[Any, float]] = []

    def open(self, request: Any, *, timeout: float) -> FakeResponse:
        self.calls.append((request, timeout))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


def _provider(
    opener: SequenceOpener,
    *,
    sleeper=lambda _: None,
    **values: Any,
) -> EidrPublicProvider:
    return EidrPublicProvider(
        authorization_object=_evidence(),
        authorization_issued_at="2026-09-20T00:00:00Z",
        opener=opener,
        minimum_request_interval_seconds=0,
        sleeper=sleeper,
        **values,
    )


def test_public_provider_uses_only_anonymous_exact_resolution() -> None:
    response = FakeResponse(
        status=200,
        body=XML,
        headers={"Content-Length": str(len(XML))},
    )
    opener = SequenceOpener(response)
    provider = _provider(
        opener,
        request_timeout_seconds=2,
    )

    result = provider.lookup_exact(eidr_ids=(EIDR_ID.lower(),), request_id="request")

    assert result[0].status == EidrLookupStatus.FOUND
    assert result[0].xml == XML
    assert result[0].attempt_count == 1
    request, timeout = opener.calls[0]
    headers = {key.lower(): value for key, value in request.header_items()}
    assert request.full_url == EXPECTED_URL
    assert request.method == "GET"
    assert headers["accept"] == "application/xml"
    assert headers["accept-encoding"] == "identity"
    assert headers["user-agent"]
    assert "authorization" not in headers
    assert timeout == 2
    assert response.read_limit == provider.max_xml_bytes + 1
    assert provider.authorization.provider_id == EIDR_PUBLIC_PROVIDER_ID
    assert provider.authorization.complete_feed_allowed is False
    assert provider.authorization.policy_digest == eidr_rights_profile().digest


def test_public_provider_maps_404_to_not_found() -> None:
    result = _provider(SequenceOpener(FakeResponse(status=404))).lookup_exact(
        eidr_ids=(EIDR_ID,),
        request_id="request",
    )
    assert result[0].status == EidrLookupStatus.NOT_FOUND
    assert result[0].xml is None


@pytest.mark.parametrize(
    "content_type",
    [
        "application/xml",
        "application/xml; charset=UTF-8",
        "text/xml",
        "text/xml; charset=utf-8",
    ],
)
def test_public_provider_accepts_xml_content_types(content_type: str) -> None:
    provider = _provider(
        SequenceOpener(
            FakeResponse(
                status=200,
                body=XML,
                headers={"Content-Type": content_type},
            )
        )
    )
    assert (
        provider.lookup_exact(
            eidr_ids=(EIDR_ID,),
            request_id="request",
        )[0].status
        == EidrLookupStatus.FOUND
    )


@pytest.mark.parametrize("content_type", ["", "application/json", "text/plain"])
def test_public_provider_rejects_non_xml_content_type(content_type: str) -> None:
    provider = _provider(
        SequenceOpener(
            FakeResponse(
                status=200,
                body=XML,
                headers={"Content-Type": content_type},
            )
        )
    )
    with pytest.raises(EidrPublicProviderError, match="XML Content-Type"):
        provider.lookup_exact(eidr_ids=(EIDR_ID,), request_id="request")


def test_public_provider_honors_retry_after_and_counts_rate_limit() -> None:
    sleeps: list[float] = []
    provider = _provider(
        SequenceOpener(
            FakeResponse(status=429, headers={"Retry-After": "7"}),
            FakeResponse(status=200, body=XML),
        ),
        sleeper=sleeps.append,
    )

    result = provider.lookup_exact(eidr_ids=(EIDR_ID,), request_id="request")[0]

    assert sleeps == [7]
    assert result.attempt_count == 2
    assert result.retry_count == 1
    assert result.rate_limit_count == 1


def test_public_provider_enforces_minimum_request_interval() -> None:
    second_id = "10.5240/FFFF-EEEE-DDDD-CCCC-BBBB-A"
    now = [0.0]
    sleeps: list[float] = []

    def sleep(delay: float) -> None:
        sleeps.append(delay)
        now[0] += delay

    provider = EidrPublicProvider(
        authorization_object=_evidence(),
        authorization_issued_at="2026-09-20T00:00:00Z",
        opener=SequenceOpener(
            FakeResponse(status=200, body=XML),
            FakeResponse(
                status=200,
                body=XML,
                url=(
                    "https://resolve.eidr.org/EIDR/object/"
                    f"{second_id}?type=Full&followAlias=false"
                ),
            ),
        ),
        minimum_request_interval_seconds=0.5,
        clock=lambda: now[0],
        sleeper=sleep,
    )
    provider.lookup_exact(eidr_ids=(EIDR_ID, second_id), request_id="request")
    assert sleeps == [0.5]


def test_public_provider_retries_all_5xx_with_exponential_backoff() -> None:
    sleeps: list[float] = []
    provider = _provider(
        SequenceOpener(
            FakeResponse(status=500),
            FakeResponse(status=599),
            FakeResponse(status=200, body=XML),
        ),
        sleeper=sleeps.append,
        retry_initial_backoff_seconds=0.25,
        retry_max_backoff_seconds=1,
    )

    result = provider.lookup_exact(eidr_ids=(EIDR_ID,), request_id="request")[0]

    assert sleeps == [0.25, 0.5]
    assert result.attempt_count == 3
    assert result.retry_count == 2
    assert result.rate_limit_count == 0


def test_public_provider_retries_timeout_then_fails_closed() -> None:
    provider = _provider(
        SequenceOpener(TimeoutError("slow"), TimeoutError("still slow")),
        max_attempts=2,
    )

    with pytest.raises(EidrPublicProviderError, match="network retries"):
        provider.lookup_exact(eidr_ids=(EIDR_ID,), request_id="request")


def test_public_provider_reports_timeout_retry_on_later_success() -> None:
    provider = _provider(
        SequenceOpener(TimeoutError("slow"), FakeResponse(status=200, body=XML)),
        max_attempts=2,
    )
    result = provider.lookup_exact(eidr_ids=(EIDR_ID,), request_id="request")[0]
    assert result.attempt_count == 2
    assert result.retry_count == 1
    assert result.rate_limit_count == 0


@pytest.mark.parametrize(
    "response",
    [
        FakeResponse(status=200, body=XML, headers={"Content-Length": "999"}),
        FakeResponse(status=200, body=b"x" * 9),
    ],
)
def test_public_provider_rejects_oversize_xml(response: FakeResponse) -> None:
    provider = _provider(SequenceOpener(response), max_xml_bytes=8)
    with pytest.raises(EidrPublicProviderError, match="byte bound"):
        provider.lookup_exact(eidr_ids=(EIDR_ID,), request_id="request")


def test_public_provider_rejects_redirect_and_non_allowlisted_host() -> None:
    redirect = _provider(
        SequenceOpener(
            FakeResponse(
                status=302,
                headers={"Location": "https://attacker.example/EIDR/object/id"},
            )
        )
    )
    with pytest.raises(EidrPublicProviderError, match="approved"):
        redirect.lookup_exact(eidr_ids=(EIDR_ID,), request_id="request")

    wrong_host = _provider(
        SequenceOpener(
            FakeResponse(
                status=200,
                body=XML,
                url=EXPECTED_URL.replace("resolve.eidr.org", "attacker.example"),
            )
        )
    )
    with pytest.raises(EidrPublicProviderError, match="approved"):
        wrong_host.lookup_exact(eidr_ids=(EIDR_ID,), request_id="request")


def test_public_provider_does_not_follow_alias_redirect() -> None:
    provider = _provider(
        SequenceOpener(
            FakeResponse(
                status=302,
                headers={
                    "Location": (
                        "https://resolve.eidr.org/EIDR/object/"
                        "10.5240/FFFF-EEEE-DDDD-CCCC-BBBB-A"
                        "?type=Full&followAlias=false"
                    )
                },
            )
        )
    )
    with pytest.raises(EidrPublicProviderError, match="approved"):
        provider.lookup_exact(eidr_ids=(EIDR_ID,), request_id="request")


def test_public_provider_fails_closed_on_other_4xx() -> None:
    provider = _provider(SequenceOpener(FakeResponse(status=403)))
    with pytest.raises(EidrPublicProviderError, match="HTTP 403"):
        provider.lookup_exact(eidr_ids=(EIDR_ID,), request_id="request")
