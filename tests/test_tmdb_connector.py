from __future__ import annotations

import gzip
import json
from datetime import date
from io import BytesIO

import pytest

from video_media_catalog.connector import ConnectorRecordEnvelope, RecordOperation
from video_media_catalog.object_store import BoundedObjectStore
from video_media_catalog.rights import UsageAction
from video_media_catalog.storage import local_path
from video_media_catalog.tmdb import map_tmdb_record, tmdb_rights_profile
from video_media_catalog.tmdb_sync import (
    TMDBApiResponse,
    TMDBHttpClient,
    capture_tmdb_changes,
    capture_tmdb_daily_exports,
    plan_tmdb_change_windows,
)


def _write_export(path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, mode="wt", encoding="utf-8") as handle:
        handle.write(json.dumps(payload) + "\n")


def _records(result) -> list[ConnectorRecordEnvelope]:
    return [
        ConnectorRecordEnvelope.model_validate_json(line)
        for reference in result.record_set_manifest.record_objects
        for line in local_path(reference.uri).read_bytes().splitlines()
    ]


class FakeTMDB:
    def fetch(self, path, query):
        if path.endswith("/changes"):
            kind = path.split("/")[2]
            results = (
                [{"id": 10}]
                if kind == "movie"
                else ([{"id": 20}] if kind == "person" else [])
            )
            return TMDBApiResponse(
                status=200,
                body=json.dumps(
                    {
                        "page": int(query["page"]),
                        "total_pages": 1,
                        "results": results,
                    }
                ).encode(),
            )
        if path == "/3/movie/10":
            return TMDBApiResponse(
                status=200,
                body=json.dumps(
                    {
                        "id": 10,
                        "title": "Example Movie",
                        "original_title": "Original Example",
                        "original_language": "en",
                        "release_date": "2020-01-02",
                        "runtime": 120,
                        "vote_average": 8.5,
                        "vote_count": 100,
                        "external_ids": {"imdb_id": "tt0000010"},
                        "credits": {
                            "cast": [
                                {
                                    "id": 30,
                                    "credit_id": "credit-1",
                                    "character": "Lead",
                                    "order": 0,
                                }
                            ],
                            "crew": [],
                        },
                        "translations": {"translations": []},
                        "images": {
                            "posters": [
                                {
                                    "file_path": "/poster.jpg",
                                    "width": 1000,
                                    "height": 1500,
                                }
                            ]
                        },
                    }
                ).encode(),
            )
        if path == "/3/person/20":
            return TMDBApiResponse(status=404, body=b"")
        raise AssertionError((path, query))


class EmptyTMDB:
    def fetch(self, path, query):
        assert path.endswith("/changes")
        return TMDBApiResponse(
            status=200,
            body=json.dumps({"page": 1, "total_pages": 0, "results": []}).encode(),
        )


class RecordingOpener:
    def __init__(self) -> None:
        self.request = None

    def open(self, request, *, timeout):
        self.request = request
        response = BytesIO(b'{"id":10}')
        response.status = 200
        response.geturl = lambda: request.full_url
        return response


def test_tmdb_daily_export_uses_inventory_without_delete_inference(tmp_path) -> None:
    paths = {}
    for kind, source_id in (("movie", 1), ("tv", 2), ("person", 3)):
        path = tmp_path / "inputs" / f"{kind}.json.gz"
        _write_export(
            path,
            {
                "id": source_id,
                "adult": False,
                "popularity": 1.25,
                "original_title": f"{kind} title",
            },
        )
        paths[kind] = path
    result = capture_tmdb_daily_exports(
        export_paths=paths,
        export_date=date(2026, 9, 20),
        destination_prefix=(tmp_path / "output").as_uri(),
        acquired_at="2026-09-20T08:00:00Z",
        image_digest="sha256:" + ("a" * 64),
        config_digest="sha256:" + ("b" * 64),
        store=BoundedObjectStore(client=object()),
    )

    assert result.batch_manifest.record_count == 3
    assert result.batch_manifest.delete_coverage.value == "NONE"
    mapped = [map_tmdb_record(item) for item in _records(result)]
    assert {item.entity_type_assertions[0].entity_type for item in mapped} == {
        "MOVIE",
        "TV_SERIES",
        "PERSON",
    }


def test_tmdb_changes_capture_details_assets_and_explicit_deletes(tmp_path) -> None:
    result = capture_tmdb_changes(
        window_start=date(2026, 9, 19),
        window_end=date(2026, 9, 20),
        destination_prefix=tmp_path.as_uri(),
        acquired_at="2026-09-20T09:00:00Z",
        image_digest="sha256:" + ("a" * 64),
        config_digest="sha256:" + ("b" * 64),
        fetcher=FakeTMDB(),
        store=BoundedObjectStore(client=object()),
    )

    assert result.batch_manifest.change_semantics.value == "DELTA"
    assert result.batch_manifest.delete_coverage.value == "EXPLICIT"
    records = _records(result)
    assert [item.operation for item in records] == [
        RecordOperation.UPSERT,
        RecordOperation.DELETE,
    ]
    movie = map_tmdb_record(records[0])
    assert movie.omitted_asset_count == 1
    assert any(
        item.namespace_id == "imdb-title" and item.value == "tt0000010"
        for item in movie.identifier_assertions
    )
    assert movie.relationship_assertions[0].predicate == "cast_member"
    deleted = map_tmdb_record(records[1])
    assert not deleted.field_assertions


def test_tmdb_rights_require_personal_noncommercial_context() -> None:
    policy = tmdb_rights_profile()
    assert policy.allows(
        UsageAction.DISPLAY,
        audience="research",
        purpose="research",
    )
    assert not policy.allows(
        UsageAction.DISPLAY,
        audience="*",
        purpose="research",
    )
    assert UsageAction.REDISTRIBUTE not in policy.permissions


def test_tmdb_token_is_header_only_and_never_enters_url() -> None:
    opener = RecordingOpener()
    client = TMDBHttpClient(
        read_token="secret-read-token",
        user_agent="video-media-catalog/test",
        minimum_interval_seconds=0,
        opener=opener,
    )
    response = client.fetch(
        "/3/movie/10",
        {"append_to_response": "credits"},
    )

    assert response.status == 200
    assert "secret-read-token" not in opener.request.full_url
    assert opener.request.get_header("Authorization") == "Bearer secret-read-token"


def test_empty_tmdb_change_window_commits_without_fake_records(tmp_path) -> None:
    result = capture_tmdb_changes(
        window_start=date(2026, 9, 20),
        window_end=date(2026, 9, 20),
        destination_prefix=tmp_path.as_uri(),
        acquired_at="2026-09-20T09:00:00Z",
        image_digest="sha256:" + ("a" * 64),
        config_digest="sha256:" + ("b" * 64),
        fetcher=EmptyTMDB(),
        store=BoundedObjectStore(client=object()),
    )

    assert result.batch_manifest.record_count == 0
    assert result.record_set_manifest.record_objects == ()


def test_tmdb_changes_require_and_honor_explicit_bounded_window(tmp_path) -> None:
    arguments = {
        "window_start": date(2026, 9, 19),
        "window_end": date(2026, 9, 20),
        "destination_prefix": tmp_path.as_uri(),
        "acquired_at": "2026-09-20T09:00:00Z",
        "image_digest": "sha256:" + ("a" * 64),
        "config_digest": "sha256:" + ("b" * 64),
        "fetcher": FakeTMDB(),
        "store": BoundedObjectStore(client=object()),
        "max_changed_ids": 1,
    }
    with pytest.raises(RuntimeError, match="2 explicit bounded windows"):
        capture_tmdb_changes(**arguments)

    plans = plan_tmdb_change_windows(
        {"movie": {10}, "tv": set(), "person": {20}},
        window_start=date(2026, 9, 19),
        window_end=date(2026, 9, 20),
        max_changed_ids=1,
    )
    result = capture_tmdb_changes(
        **arguments,
        window_cursor=plans[0].cursor,
    )

    assert result.batch_manifest.record_count == 1
    assert result.batch_manifest.coverage_scope["windowShardCount"] == 2
    assert _records(result)[0].source_record_id == "10"
