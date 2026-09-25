from __future__ import annotations

from typing import Any

import pytest

from video_media_catalog.emr_serverless_cli import build_parser, run


def _parsed(*extra: str):
    return build_parser().parse_args(
        [
            "--application-name",
            "media-catalog-wikidata-full-media",
            "--execution-role-arn",
            "arn:aws:iam::123456789012:role/media-catalog-emr-runtime",
            "--job-name",
            "wikidata-full-media-run",
            "--client-token",
            "workflow-uid-1",
            "--entry-point",
            (
                "local:///opt/video-media-catalog/src/video_media_catalog/"
                "wikidata_full_backfill_cli.py"
            ),
            "--log-uri",
            "s3://catalog/raw/wikidata/emr-logs/",
            "--aws-region",
            "us-east-1",
            *extra,
            "--",
            "--dump-uri",
            "s3://catalog/raw/wikidata.json.bz2",
        ]
    )


class _Client:
    def __init__(self, states: list[str]) -> None:
        self.states = states
        self.start_request: dict[str, Any] | None = None
        self.cancel_requests: list[dict[str, str]] = []

    def list_applications(self, **_request: Any) -> dict[str, Any]:
        return {
            "applications": [
                {
                    "id": "00fakerefapp",
                    "name": "media-catalog-wikidata-full-media",
                    "state": "STARTED",
                }
            ]
        }

    def start_job_run(self, **request: Any) -> dict[str, str]:
        self.start_request = request
        return {
            "arn": (
                "arn:aws:emr-serverless:us-east-1:123456789012:"
                "/applications/00fakerefapp/jobruns/00fakejob"
            ),
            "applicationId": "00fakerefapp",
            "jobRunId": "00fakejob",
        }

    def get_job_run(self, **_request: Any) -> dict[str, Any]:
        return {
            "jobRun": {
                "applicationId": "00fakerefapp",
                "jobRunId": "00fakejob",
                "state": self.states.pop(0),
            }
        }

    def cancel_job_run(self, **request: str) -> None:
        self.cancel_requests.append(request)


def test_submit_waits_for_success_with_shuffle_optimized_disk() -> None:
    client = _Client(["SUBMITTED", "RUNNING", "SUCCESS"])

    result = run(_parsed(), client=client, sleep=lambda _seconds: None)

    assert result["state"] == "SUCCESS"
    assert client.cancel_requests == []
    assert client.start_request is not None
    spark_submit = client.start_request["jobDriver"]["sparkSubmit"]
    assert spark_submit["entryPointArguments"] == [
        "--dump-uri",
        "s3://catalog/raw/wikidata.json.bz2",
    ]
    parameters = spark_submit["sparkSubmitParameters"]
    assert "spark.emr-serverless.executor.disk=200G" in parameters
    assert "spark.emr-serverless.executor.disk.type=SHUFFLE_OPTIMIZED" in parameters
    assert "spark.speculation=false" in parameters
    assert "spark.dynamicAllocation.enabled=false" in parameters


def test_submit_surfaces_failed_job_without_cancelling_terminal_run() -> None:
    client = _Client(["FAILED"])

    with pytest.raises(RuntimeError, match="ended in FAILED"):
        run(_parsed(), client=client, sleep=lambda _seconds: None)

    assert client.cancel_requests == []


def test_submit_cancels_nonterminal_job_when_polling_breaks() -> None:
    client = _Client([])

    with pytest.raises(IndexError):
        run(_parsed(), client=client, sleep=lambda _seconds: None)

    assert client.cancel_requests == [
        {"applicationId": "00fakerefapp", "jobRunId": "00fakejob"}
    ]


def test_submit_requires_forwarded_entry_point_arguments() -> None:
    parsed = build_parser().parse_args(
        [
            "--application-name",
            "media-catalog-wikidata-full-media",
            "--execution-role-arn",
            "arn:aws:iam::123456789012:role/runtime",
            "--job-name",
            "wikidata-full-media-run",
            "--client-token",
            "workflow-uid-1",
            "--entry-point",
            "local:///opt/wikidata_full_backfill_cli.py",
            "--log-uri",
            "s3://catalog/raw/logs/",
            "--aws-region",
            "us-east-1",
        ]
    )

    with pytest.raises(ValueError, match="entry-point argument"):
        run(parsed, client=_Client(["SUCCESS"]), sleep=lambda _seconds: None)
