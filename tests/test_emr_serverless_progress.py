from __future__ import annotations

from typing import Any

from video_media_catalog.emr_serverless_cli import build_parser, run


def _parsed():
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
            "local:///opt/video-media-catalog/src/video_media_catalog/community_cli.py",
            "--log-uri",
            "s3://catalog/raw/wikidata/emr-logs/",
            "--aws-region",
            "us-east-1",
            "--",
            "--batch-manifest-uri",
            "s3://catalog/landing/batch.json",
        ]
    )


class _Client:
    def __init__(self, states: list[str]) -> None:
        self.states = states
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
        return {
            "arn": "arn:aws:emr-serverless:us-east-1:123: /applications/a/jobruns/j",
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


def test_progress_callback_and_cancel_path() -> None:
    client = _Client(["SUBMITTED", "SUBMITTED", "RUNNING", "SUCCESS"])
    events: list[str] = []

    result = run(
        _parsed(),
        client=client,
        sleep=lambda _seconds: None,
        progress_callback=lambda event: events.append(str(event["event"])),
    )
    assert result["state"] == "SUCCESS"
    assert events[0] == "submitted"
    assert "state" in events
    assert "poll" in events


def test_should_cancel_triggers_cancel() -> None:
    client = _Client(["RUNNING", "RUNNING"])

    try:
        run(
            _parsed(),
            client=client,
            sleep=lambda _seconds: None,
            should_cancel=lambda: True,
        )
        raise AssertionError("expected cancel")
    except RuntimeError as exc:
        assert "cancelled by caller" in str(exc)
    assert client.cancel_requests
