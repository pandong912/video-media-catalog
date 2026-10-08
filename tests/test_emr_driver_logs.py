from __future__ import annotations

import gzip
import json
from typing import Any

import pytest

from video_media_catalog.emr_driver_logs import (
    parse_source_silver_summary,
    read_source_silver_summary,
)


def test_parse_source_silver_summary_uses_last_matching_line() -> None:
    first = {"runId": "sha256:aaa", "commitKey": "one", "tableCounts": {"a": 1}}
    second = {
        "runId": "sha256:bbb",
        "commitKey": "two",
        "tableCounts": {"a": 2},
        "tableSnapshotIds": {"a": 9},
    }
    text = "\n".join(
        [
            "noise",
            json.dumps(first),
            json.dumps({"event": "progress"}),
            json.dumps(second),
        ]
    )
    assert parse_source_silver_summary(text)["commitKey"] == "two"


def test_read_source_silver_summary_from_gzip_object() -> None:
    summary = {
        "runId": "sha256:abc",
        "commitKey": "commit-1",
        "tableCounts": {"source_record": 3},
        "tableSnapshotIds": {"source_record": 11},
    }
    body = gzip.compress((json.dumps(summary) + "\n").encode())

    class _Body:
        def read(self, _size: int) -> bytes:
            return body

    class _Client:
        def list_objects_v2(self, **request: Any) -> dict[str, Any]:
            assert "SPARK_DRIVER/stdout" in request["Prefix"]
            return {
                "Contents": [
                    {
                        "Key": request["Prefix"] + ".gz",
                        "Size": len(body),
                    }
                ]
            }

        def get_object(self, **request: Any) -> dict[str, Any]:
            assert request["Key"].endswith(".gz")
            return {"Body": _Body()}

    loaded = read_source_silver_summary(
        s3_client=_Client(),
        log_uri="s3://bucket/raw/wikidata/emr-serverless-logs/",
        application_id="app-1",
        job_run_id="job-1",
    )
    assert loaded["runId"] == "sha256:abc"


def test_parse_requires_summary() -> None:
    with pytest.raises(ValueError, match="did not contain"):
        parse_source_silver_summary('{"event":"state"}\n')
