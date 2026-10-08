from __future__ import annotations

from video_media_catalog.temporal.activities.silver import _client_token


def test_emr_client_token_is_stable_per_workflow_execution() -> None:
    first = _client_token("sha256:" + ("a" * 64), "run-1")

    assert first == _client_token("sha256:" + ("a" * 64), "run-1")
    assert first != _client_token("sha256:" + ("a" * 64), "run-2")
    assert first.startswith("tmdb-ss-")
    assert len(first) <= 64
