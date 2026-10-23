"""One-shot Spark Serverless backend adapters.

Adapters deliberately split submit, observe, and cancel. They do not poll,
write Catalog or Platform databases, or publish Iceberg state.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal, Protocol

BackendProvider = Literal[
    "AWS_EMR_SERVERLESS",
    "ALIYUN_EMR_SERVERLESS_SPARK",
]
BackendOutcome = Literal["RUNNING", "SUCCESS", "FAILED", "CANCELLED"]

_SUCCESS_STATES = frozenset({"SUCCESS", "SUCCEEDED", "FINISHED"})
_FAILED_STATES = frozenset({"FAILED", "ERROR"})
_CANCELLED_STATES = frozenset({"CANCELLED", "CANCELED"})


def _bounded(value: str, *, label: str, maximum: int) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > maximum
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise ValueError(f"{label} must be canonical bounded text")
    return value


def _outcome(state: str) -> BackendOutcome:
    normalized = state.upper()
    if normalized in _SUCCESS_STATES:
        return "SUCCESS"
    if normalized in _FAILED_STATES:
        return "FAILED"
    if normalized in _CANCELLED_STATES:
        return "CANCELLED"
    return "RUNNING"


def _observed_at(value: Any) -> str:
    if value is None:
        return ""
    if not isinstance(value, datetime):
        return str(value)
    if value.tzinfo is None:
        raise ValueError("provider observation time must include a timezone")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class SparkSubmitRequest:
    provider: BackendProvider
    application_id: str
    workload_identity_reference: str
    client_token: str
    job_name: str
    entry_point: str
    entry_point_arguments: tuple[str, ...]
    spark_submit_parameters: str
    execution_timeout_minutes: int
    max_attempts: int
    log_uri: str

    def __post_init__(self) -> None:
        _bounded(self.application_id, label="application_id", maximum=255)
        _bounded(
            self.workload_identity_reference,
            label="workload_identity_reference",
            maximum=512,
        )
        _bounded(self.client_token, label="client_token", maximum=64)
        _bounded(self.job_name, label="job_name", maximum=64)
        _bounded(self.entry_point, label="entry_point", maximum=2_048)
        _bounded(self.log_uri, label="log_uri", maximum=2_048)
        if not self.entry_point_arguments or len(self.entry_point_arguments) > 1_024:
            raise ValueError("entry_point_arguments must be bounded and non-empty")
        if self.execution_timeout_minutes < 1 or self.max_attempts < 1:
            raise ValueError("execution timeout and max attempts must be positive")


@dataclass(frozen=True)
class SparkBackendIdentity:
    provider: BackendProvider
    application_id: str
    job_run_id: str
    job_run_arn: str | None = None

    def __post_init__(self) -> None:
        _bounded(self.application_id, label="application_id", maximum=255)
        _bounded(self.job_run_id, label="job_run_id", maximum=255)
        if self.job_run_arn is not None:
            _bounded(self.job_run_arn, label="job_run_arn", maximum=2_048)


@dataclass(frozen=True)
class SparkBackendObservation:
    identity: SparkBackendIdentity
    provider_state: str
    outcome: BackendOutcome
    state_details: str = ""
    observed_at: str = ""
    raw: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _bounded(self.provider_state, label="provider_state", maximum=64)
        if len(self.state_details) > 4_096:
            raise ValueError("state_details exceeds the bounded receipt summary")
        if len(self.raw) > 128:
            raise ValueError("raw backend observation is not bounded")

    @property
    def terminal(self) -> bool:
        return self.outcome != "RUNNING"


@dataclass(frozen=True)
class SparkCancelReceipt:
    identity: SparkBackendIdentity
    accepted: bool
    provider_state: str


class SparkBackendAdapter(Protocol):
    def submit(self, request: SparkSubmitRequest) -> SparkBackendIdentity: ...

    def observe(
        self,
        identity: SparkBackendIdentity,
    ) -> SparkBackendObservation: ...

    def cancel(
        self,
        identity: SparkBackendIdentity,
        *,
        reason: str,
    ) -> SparkCancelReceipt: ...


class AwsEmrServerlessSparkBackend:
    provider: BackendProvider = "AWS_EMR_SERVERLESS"

    def __init__(self, client: Any) -> None:
        self.client = client

    def resolve_application_id(self, application_name: str) -> str:
        applications: list[dict[str, Any]] = []
        request: dict[str, Any] = {}
        while True:
            response = self.client.list_applications(**request)
            applications.extend(response.get("applications", []))
            token = response.get("nextToken")
            if not token:
                break
            request["nextToken"] = token
        matches = [
            application
            for application in applications
            if application.get("name") == application_name
            and application.get("state") in {"CREATED", "STARTED", "STOPPED"}
        ]
        if len(matches) != 1:
            states = sorted(
                str(application.get("state"))
                for application in applications
                if application.get("name") == application_name
            )
            raise RuntimeError(
                "expected exactly one runnable EMR Serverless application "
                f"named {application_name!r}; found {len(matches)} "
                f"(states={states})"
            )
        return str(matches[0]["id"])

    def submit(self, request: SparkSubmitRequest) -> SparkBackendIdentity:
        if request.provider != self.provider:
            raise ValueError("AWS adapter received a different provider request")
        response = self.client.start_job_run(
            applicationId=request.application_id,
            clientToken=request.client_token,
            executionRoleArn=request.workload_identity_reference,
            executionTimeoutMinutes=request.execution_timeout_minutes,
            jobDriver={
                "sparkSubmit": {
                    "entryPoint": request.entry_point,
                    "entryPointArguments": list(request.entry_point_arguments),
                    "sparkSubmitParameters": request.spark_submit_parameters,
                }
            },
            name=request.job_name,
            configurationOverrides={
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {"logUri": request.log_uri}
                }
            },
            retryPolicy={"maxAttempts": request.max_attempts},
        )
        return SparkBackendIdentity(
            provider=self.provider,
            application_id=request.application_id,
            job_run_id=str(response["jobRunId"]),
            job_run_arn=(None if response.get("arn") is None else str(response["arn"])),
        )

    def observe(
        self,
        identity: SparkBackendIdentity,
    ) -> SparkBackendObservation:
        self._require_identity(identity)
        response = self.client.get_job_run(
            applicationId=identity.application_id,
            jobRunId=identity.job_run_id,
        )
        job_run = dict(response["jobRun"])
        state = str(job_run["state"])
        return SparkBackendObservation(
            identity=identity,
            provider_state=state,
            outcome=_outcome(state),
            state_details=str(job_run.get("stateDetails") or ""),
            observed_at=_observed_at(
                job_run.get("updatedAt") or job_run.get("createdAt")
            ),
            raw=job_run,
        )

    def cancel(
        self,
        identity: SparkBackendIdentity,
        *,
        reason: str,
    ) -> SparkCancelReceipt:
        self._require_identity(identity)
        _bounded(reason, label="cancel reason", maximum=512)
        response = self.client.cancel_job_run(
            applicationId=identity.application_id,
            jobRunId=identity.job_run_id,
        )
        state = str((response or {}).get("state") or "CANCEL_REQUESTED")
        return SparkCancelReceipt(identity, True, state)

    def _require_identity(self, identity: SparkBackendIdentity) -> None:
        if identity.provider != self.provider:
            raise ValueError("AWS adapter received a different provider identity")


class AliyunEmrServerlessSparkBackend:
    """SDK-neutral adapter over an allowlisted Alibaba Cloud API client."""

    provider: BackendProvider = "ALIYUN_EMR_SERVERLESS_SPARK"

    def __init__(self, client: Any) -> None:
        self.client = client

    def submit(self, request: SparkSubmitRequest) -> SparkBackendIdentity:
        if request.provider != self.provider:
            raise ValueError("ALIYUN adapter received a different provider request")
        response = self.client.submit_job_run(
            workspace_id=request.application_id,
            client_token=request.client_token,
            role_arn=request.workload_identity_reference,
            name=request.job_name,
            entry_point=request.entry_point,
            entry_point_arguments=list(request.entry_point_arguments),
            spark_submit_parameters=request.spark_submit_parameters,
            execution_timeout_minutes=request.execution_timeout_minutes,
            max_attempts=request.max_attempts,
            log_uri=request.log_uri,
        )
        return SparkBackendIdentity(
            provider=self.provider,
            application_id=request.application_id,
            job_run_id=str(response["jobRunId"]),
            job_run_arn=(
                None
                if response.get("jobRunArn") is None
                else str(response["jobRunArn"])
            ),
        )

    def observe(
        self,
        identity: SparkBackendIdentity,
    ) -> SparkBackendObservation:
        self._require_identity(identity)
        job_run = dict(
            self.client.get_job_run(
                workspace_id=identity.application_id,
                job_run_id=identity.job_run_id,
            )
        )
        state = str(job_run["state"])
        return SparkBackendObservation(
            identity=identity,
            provider_state=state,
            outcome=_outcome(state),
            state_details=str(job_run.get("stateDetails") or ""),
            observed_at=_observed_at(
                job_run.get("updatedAt") or job_run.get("createdAt")
            ),
            raw=job_run,
        )

    def cancel(
        self,
        identity: SparkBackendIdentity,
        *,
        reason: str,
    ) -> SparkCancelReceipt:
        self._require_identity(identity)
        _bounded(reason, label="cancel reason", maximum=512)
        response = self.client.cancel_job_run(
            workspace_id=identity.application_id,
            job_run_id=identity.job_run_id,
            reason=reason,
        )
        state = str((response or {}).get("state") or "CANCEL_REQUESTED")
        return SparkCancelReceipt(identity, True, state)

    def _require_identity(self, identity: SparkBackendIdentity) -> None:
        if identity.provider != self.provider:
            raise ValueError("ALIYUN adapter received a different provider identity")
