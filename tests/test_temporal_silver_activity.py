from __future__ import annotations

import pytest

from video_media_catalog.temporal.activities.silver import (
    _client_token,
    _verify_summary,
)
from video_media_catalog.temporal.errors import NonRetryableSilverError


def test_emr_client_token_is_stable_per_workflow_execution() -> None:
    first = _client_token("sha256:" + ("a" * 64), "run-1")

    assert first == _client_token("sha256:" + ("a" * 64), "run-1")
    assert first != _client_token("sha256:" + ("a" * 64), "run-2")
    assert first.startswith("tmdb-ss-")
    assert len(first) <= 64


def test_source_silver_allows_null_snapshot_only_for_empty_tables() -> None:
    summary = {
        "runId": "sha256:" + ("a" * 64),
        "commitKey": "sha256:" + ("b" * 64),
        "tableCounts": {"empty": 0, "populated": 2},
        "tableSnapshotIds": {"empty": None, "populated": 123},
    }

    _verify_summary(summary, batch_id="sha256:" + ("c" * 64))

    summary["tableSnapshotIds"]["populated"] = None
    with pytest.raises(NonRetryableSilverError, match="rows but no snapshot"):
        _verify_summary(summary, batch_id="sha256:" + ("c" * 64))
