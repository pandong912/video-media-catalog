from __future__ import annotations

import json
from io import BytesIO
from urllib.error import HTTPError

import pytest

from video_media_catalog.connector import ConnectorRecordEnvelope
from video_media_catalog.object_store import BoundedObjectStore
from video_media_catalog.storage import local_path
from video_media_catalog.tvmaze_sync import (
    TVMazeHttpFetcher,
    TVMazePageFetch,
    capture_tvmaze_show_index,
)


class FakeFetcher:
    def __init__(self, pages: list[bytes]) -> None:
        self.pages = pages
        self.calls: list[int] = []

    def fetch_page(self, page: int) -> TVMazePageFetch:
        self.calls.append(page)
        if page >= len(self.pages):
            return TVMazePageFetch(status=404, body=b"")
        return TVMazePageFetch(
            status=200,
            body=self.pages[page],
            retry_count=int(page == 0),
            rate_limit_count=int(page == 0),
        )


class FakeResponse(BytesIO):
    def __init__(self, body: bytes, *, url: str, status: int = 200) -> None:
        super().__init__(body)
        self.status = status
        self.headers = {}
        self._url = url

    def geturl(self) -> str:
        return self._url


class QueueOpener:
    def __init__(self, outcomes) -> None:
        self.outcomes = list(outcomes)

    def open(self, request, *, timeout):
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


def _show(show_id: int) -> dict[str, object]:
    return {
        "id": show_id,
        "name": f"Show {show_id}",
        "type": "Scripted",
        "language": "English",
        "updated": 1_700_000_000 + show_id,
        "genres": [],
        "externals": {"imdb": f"tt{show_id:07d}"},
    }


def test_tvmaze_sync_publishes_replayable_commit_last_outputs(tmp_path) -> None:
    pages = [
        json.dumps([_show(1)]).encode(),
        json.dumps([_show(2)]).encode(),
    ]
    fetcher = FakeFetcher(pages)
    store = BoundedObjectStore(client=object())
    result = capture_tvmaze_show_index(
        destination_prefix=tmp_path.as_uri(),
        acquired_at="2026-09-19T00:00:00Z",
        image_digest="sha256:" + ("a" * 64),
        config_digest="sha256:" + ("b" * 64),
        fetcher=fetcher,
        store=store,
        max_pages=10,
        record_shard_bytes=2048,
    )

    assert fetcher.calls == [0, 1, 2]
    assert result.page_count == 2
    assert result.record_count == 2
    assert local_path(result.batch_manifest_object.uri).exists()
    assert local_path(result.record_set_manifest_object.uri).exists()
    assert result.batch_manifest.retry_count == 1
    assert result.batch_manifest.rate_limit_count == 1

    records = []
    for reference in result.record_set_manifest.record_objects:
        records.extend(
            ConnectorRecordEnvelope.model_validate_json(line)
            for line in local_path(reference.uri).read_bytes().splitlines()
        )
    assert [record.source_record_id for record in records] == ["1", "2"]

    repeated = capture_tvmaze_show_index(
        destination_prefix=tmp_path.as_uri(),
        acquired_at="2026-09-19T00:00:00Z",
        image_digest="sha256:" + ("a" * 64),
        config_digest="sha256:" + ("b" * 64),
        fetcher=FakeFetcher(pages),
        store=store,
        max_pages=10,
        record_shard_bytes=2048,
    )
    assert repeated.batch_manifest == result.batch_manifest
    assert repeated.record_set_manifest == result.record_set_manifest
    assert (
        local_path(repeated.record_set_manifest_object.uri).read_bytes()
        == local_path(result.record_set_manifest_object.uri).read_bytes()
    )


def test_tvmaze_sync_refuses_partial_snapshot_at_page_limit(tmp_path) -> None:
    fetcher = FakeFetcher([json.dumps([_show(1)]).encode()])
    with pytest.raises(RuntimeError, match="page limit"):
        capture_tvmaze_show_index(
            destination_prefix=tmp_path.as_uri(),
            acquired_at="2026-09-19T00:00:00Z",
            image_digest="sha256:" + ("a" * 64),
            config_digest="sha256:" + ("b" * 64),
            fetcher=fetcher,
            store=BoundedObjectStore(client=object()),
            max_pages=1,
            record_shard_bytes=2048,
        )
    assert not tuple(tmp_path.glob("**/record-set.json"))


def test_tvmaze_http_fetcher_retries_429_without_following_redirects() -> None:
    url = "https://api.tvmaze.com/shows?page=0"
    throttled = HTTPError(
        url,
        429,
        "rate limited",
        {"Retry-After": "0"},
        BytesIO(),
    )
    fetcher = TVMazeHttpFetcher(
        user_agent="video-media-catalog/test test@example.com",
        minimum_interval_seconds=0,
        opener=QueueOpener(
            [
                throttled,
                FakeResponse(b"[]", url=url),
            ]
        ),
        sleeper=lambda _: None,
    )
    result = fetcher.fetch_page(0)
    assert result.status == 200
    assert result.retry_count == 1
    assert result.rate_limit_count == 1


def test_tvmaze_http_fetcher_rejects_off_origin_final_url() -> None:
    fetcher = TVMazeHttpFetcher(
        user_agent="video-media-catalog/test test@example.com",
        minimum_interval_seconds=0,
        opener=QueueOpener(
            [
                FakeResponse(
                    b"[]",
                    url="https://example.com/shows?page=0",
                )
            ]
        ),
    )
    with pytest.raises(ValueError, match="approved endpoint"):
        fetcher.fetch_page(0)
