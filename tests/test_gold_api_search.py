from __future__ import annotations

import pytest

from video_media_catalog.api_search import InvalidCursor
from video_media_catalog.gold_api_search import (
    GoldCursorCodec,
    GoldSearchParameters,
    build_gold_external_identifier_query,
    build_gold_search_query,
)


def _parameters() -> GoldSearchParameters:
    return GoldSearchParameters(
        q="example",
        entity_level="SERIES",
        entity_kind="TV_SERIES",
        language="en",
        has_conflicts=False,
        page_size=20,
    )


def test_gold_cursor_binds_concrete_index_query_and_expiry() -> None:
    codec = GoldCursorCodec(
        b"x" * 32,
        ttl_seconds=60,
        clock=lambda: 100,
    )
    parameters = _parameters()
    index = "media-catalog-research-" + ("a" * 24)
    cursor = codec.encode(
        index=index,
        sort=[2.5, "sha256:" + ("b" * 64)],
        fingerprint=parameters.fingerprint(),
    )
    state = codec.decode(cursor, fingerprint=parameters.fingerprint())
    assert state.index == index
    assert state.expires_at == 160

    expired = GoldCursorCodec(
        b"x" * 32,
        ttl_seconds=60,
        clock=lambda: 160,
    )
    with pytest.raises(InvalidCursor, match="expired"):
        expired.decode(cursor, fingerprint=parameters.fingerprint())
    with pytest.raises(InvalidCursor, match="does not match"):
        codec.decode(cursor, fingerprint="different")


def test_gold_query_uses_only_fixed_grammar() -> None:
    query = build_gold_search_query(
        _parameters(),
        owner_subject="owner-123",
        search_after=[2.5, "sha256:" + ("b" * 64)],
        timeout_ms=5000,
    )
    filters = query["query"]["bool"]["filter"]
    assert {"term": {"ownerSubject": "owner-123"}} in filters
    assert {"term": {"entityLevel": "SERIES"}} in filters
    assert {"term": {"entityKind": "TV_SERIES"}} in filters
    assert {"term": {"conflictCount": 0}} in filters
    assert query["search_after"][1].startswith("sha256:")
    assert query["sort"][-1] == {"entityKey": {"order": "asc"}}


def test_gold_external_identifier_query_binds_namespace_and_value() -> None:
    query = build_gold_external_identifier_query(
        namespace="imdb-title",
        value="tt0000001",
        owner_subject="owner-123",
        timeout_ms=5000,
    )
    filters = query["query"]["bool"]["filter"]
    assert filters[0] == {"term": {"ownerSubject": "owner-123"}}
    nested = filters[1]["nested"]["query"]["bool"]["filter"]
    assert {"term": {"externalIdentifiers.namespace": "imdb-title"}} in nested
    assert {"term": {"externalIdentifiers.value": "tt0000001"}} in nested
