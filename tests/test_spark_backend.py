from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from video_media_catalog.spark_backend import (
    AliyunEmrServerlessSparkBackend,
    AwsEmrServerlessSparkBackend,
    SparkSubmitRequest,
)


def _request(provider: str) -> SparkSubmitRequest:
    aws = provider == "AWS_EMR_SERVERLESS"
    return SparkSubmitRequest(
        provider=provider,  # type: ignore[arg-type]
        application_id="app-1",
        workload_identity_reference=(
            "arn:aws:iam::123456789012:role/catalog"
            if aws
            else "acs:ram::1234567890123456:role/catalog"
        ),
        client_token="stable-token",
        job_name="stable-job",
        entry_point="local:///opt/catalog/job.py",
        entry_point_arguments=("--input", "object"),
        spark_submit_parameters="--conf spark.dynamicAllocation.enabled=false",
        execution_timeout_minutes=60,
        max_attempts=1,
        log_uri="s3://bucket/logs/" if aws else "oss://bucket/logs/",
    )


class AwsClient:
    def __init__(self) -> None:
        self.started: dict[str, Any] | None = None
        self.cancelled: dict[str, Any] | None = None

    def list_applications(self, **_request: Any) -> dict[str, Any]:
        return {
            "applications": [{"id": "app-1", "name": "catalog", "state": "STARTED"}]
        }

    def start_job_run(self, **request: Any) -> dict[str, str]:
        self.started = request
        return {"jobRunId": "job-1", "arn": "arn:aws:emr:job-1"}

    def get_job_run(self, **_request: Any) -> dict[str, Any]:
        return {
            "jobRun": {
                "applicationId": "app-1",
                "jobRunId": "job-1",
                "state": "SUCCESS",
                "updatedAt": datetime(2026, 10, 23, 0, 10, tzinfo=UTC),
            }
        }

    def cancel_job_run(self, **request: Any) -> dict[str, str]:
        self.cancelled = request
        return {"state": "CANCELLING"}


class AliyunClient:
    def __init__(self) -> None:
        self.started: dict[str, Any] | None = None
        self.cancelled: dict[str, Any] | None = None

    def submit_job_run(self, **request: Any) -> dict[str, str]:
        self.started = request
        return {"jobRunId": "job-2", "jobRunArn": "acs:emr:job-2"}

    def get_job_run(self, **_request: Any) -> dict[str, str]:
        return {
            "jobRunId": "job-2",
            "state": "FAILED",
            "stateDetails": "bounded failure",
            "updatedAt": "2026-10-23T00:11:00Z",
        }

    def cancel_job_run(self, **request: Any) -> dict[str, str]:
        self.cancelled = request
        return {"state": "CANCEL_REQUESTED"}


def test_aws_adapter_separates_submit_observe_and_cancel() -> None:
    client = AwsClient()
    backend = AwsEmrServerlessSparkBackend(client)
    assert backend.resolve_application_id("catalog") == "app-1"

    identity = backend.submit(_request("AWS_EMR_SERVERLESS"))
    observation = backend.observe(identity)
    cancelled = backend.cancel(identity, reason="operator requested")

    assert client.started is not None
    assert client.started["clientToken"] == "stable-token"
    assert observation.outcome == "SUCCESS"
    assert observation.observed_at == "2026-10-23T00:10:00Z"
    assert observation.terminal
    assert cancelled.accepted
    assert client.cancelled == {
        "applicationId": "app-1",
        "jobRunId": "job-1",
    }


def test_aliyun_adapter_separates_submit_observe_and_cancel() -> None:
    client = AliyunClient()
    backend = AliyunEmrServerlessSparkBackend(client)

    identity = backend.submit(_request("ALIYUN_EMR_SERVERLESS_SPARK"))
    observation = backend.observe(identity)
    cancelled = backend.cancel(identity, reason="plan superseded")

    assert client.started is not None
    assert client.started["workspace_id"] == "app-1"
    assert observation.outcome == "FAILED"
    assert observation.state_details == "bounded failure"
    assert cancelled.provider_state == "CANCEL_REQUESTED"
    assert client.cancelled == {
        "workspace_id": "app-1",
        "job_run_id": "job-2",
        "reason": "plan superseded",
    }
