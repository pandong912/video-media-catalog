"""One-shot Spark Serverless backend adapters.

Adapters deliberately split submit, observe, and cancel. They do not poll,
write Catalog or Platform databases, or publish Iceberg state.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any, Literal, Protocol

from video_media_catalog.execution_contracts import canonical_semantic_digest
from video_media_catalog.execution_p4 import (
    DeterministicFailureInjector,
    ObservedCostEvidence,
    ObservedResourceEvidence,
)

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


def _scaled_integer(value: Any, multiplier: int) -> int:
    if value is None:
        return 0
    try:
        scaled = Decimal(str(value)) * multiplier
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("provider resource evidence must be numeric") from exc
    if not scaled.is_finite() or scaled < 0:
        raise ValueError("provider resource evidence must be finite and non-negative")
    return int(scaled.to_integral_value(rounding=ROUND_HALF_UP))


def _integer(value: Any) -> int:
    return _scaled_integer(value, 1)


def _first_present(*values: Any) -> Any:
    return next((value for value in values if value is not None), None)


def _resource_evidence(
    provider: BackendProvider,
    job_run: dict[str, Any],
) -> ObservedResourceEvidence:
    utilization = dict(
        job_run.get("totalResourceUtilization")
        or job_run.get("resourceUtilization")
        or {}
    )
    duration_seconds = _first_present(
        job_run.get("totalExecutionDurationSeconds"),
        job_run.get("executionDurationSeconds"),
        utilization.get("durationSeconds"),
    )
    vcpu_hours = _first_present(
        utilization.get("vCPUHour"),
        utilization.get("vcpuHours"),
    )
    memory_gb_hours = _first_present(
        utilization.get("memoryGBHour"),
        utilization.get("memoryGbHours"),
    )
    storage_gb_hours = _first_present(
        utilization.get("storageGBHour"),
        utilization.get("storageGbHours"),
    )
    input_bytes = _first_present(
        utilization.get("inputBytes"),
        job_run.get("inputBytes"),
    )
    output_bytes = _first_present(
        utilization.get("outputBytes"),
        job_run.get("outputBytes"),
    )
    shuffle_read_bytes = _first_present(
        utilization.get("shuffleReadBytes"),
        job_run.get("shuffleReadBytes"),
    )
    shuffle_write_bytes = _first_present(
        utilization.get("shuffleWriteBytes"),
        job_run.get("shuffleWriteBytes"),
    )
    complete = all(
        value is not None
        for value in (
            duration_seconds,
            vcpu_hours,
            memory_gb_hours,
            storage_gb_hours,
            input_bytes,
            output_bytes,
            shuffle_read_bytes,
            shuffle_write_bytes,
        )
    )
    return ObservedResourceEvidence.create(
        scope="PROVIDER_OBSERVED",
        provider=provider,
        duration_milliseconds=_scaled_integer(duration_seconds, 1_000),
        vcpu_milliseconds=_scaled_integer(vcpu_hours, 3_600_000),
        memory_gb_milliseconds=_scaled_integer(memory_gb_hours, 3_600_000),
        storage_gb_milliseconds=_scaled_integer(storage_gb_hours, 3_600_000),
        input_bytes=_integer(input_bytes),
        output_bytes=_integer(output_bytes),
        shuffle_read_bytes=_integer(shuffle_read_bytes),
        shuffle_write_bytes=_integer(shuffle_write_bytes),
        complete=complete,
    )


def _cost_evidence(
    provider: BackendProvider,
    job_run: dict[str, Any],
    resource: ObservedResourceEvidence,
) -> ObservedCostEvidence:
    reported = job_run.get("totalExecutionCost") or job_run.get("cost")
    if not isinstance(reported, dict):
        return ObservedCostEvidence.create(
            scope="PROVIDER_OBSERVED",
            provider=provider,
            status="UNAVAILABLE",
            resource_evidence_digest=resource.evidence_digest,
        )
    amount = reported.get("amount")
    currency = reported.get("currency")
    if amount is None or not currency:
        return ObservedCostEvidence.create(
            scope="PROVIDER_OBSERVED",
            provider=provider,
            status="UNAVAILABLE",
            resource_evidence_digest=resource.evidence_digest,
        )
    return ObservedCostEvidence.create(
        scope="PROVIDER_OBSERVED",
        provider=provider,
        status="PROVIDER_REPORTED",
        amount_micros=_scaled_integer(amount, 1_000_000),
        currency=str(currency),
        rate_card_digest=(
            None
            if reported.get("rateCardDigest") is None
            else str(reported["rateCardDigest"])
        ),
        resource_evidence_digest=resource.evidence_digest,
    )


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
    resource_evidence: ObservedResourceEvidence | None = None
    cost_evidence: ObservedCostEvidence | None = None
    resource_evidence_digest: str = ""
    cost_evidence_digest: str = ""
    raw: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _bounded(self.provider_state, label="provider_state", maximum=64)
        if len(self.state_details) > 4_096:
            raise ValueError("state_details exceeds the bounded receipt summary")
        if len(self.raw) > 128:
            raise ValueError("raw backend observation is not bounded")
        if self.resource_evidence is None and self.cost_evidence is not None:
            raise ValueError("cost evidence requires resource evidence")
        if (
            self.resource_evidence is not None
            and self.resource_evidence.provider != self.identity.provider
        ):
            raise ValueError("resource evidence provider differs from identity")
        if (
            self.cost_evidence is not None
            and self.cost_evidence.provider != self.identity.provider
        ):
            raise ValueError("cost evidence provider differs from identity")
        for label, digest in (
            ("resource_evidence_digest", self.resource_evidence_digest),
            ("cost_evidence_digest", self.cost_evidence_digest),
        ):
            if digest and (
                len(digest) != 64
                or any(character not in "0123456789abcdef" for character in digest)
            ):
                raise ValueError(f"{label} must be a lowercase SHA-256")
        if (
            self.resource_evidence is not None
            and self.resource_evidence_digest
            and self.resource_evidence.evidence_digest != self.resource_evidence_digest
        ):
            raise ValueError("resource evidence and digest differ")
        if (
            self.cost_evidence is not None
            and self.cost_evidence_digest
            and self.cost_evidence.evidence_digest != self.cost_evidence_digest
        ):
            raise ValueError("cost evidence and digest differ")

    @property
    def terminal(self) -> bool:
        return self.outcome != "RUNNING"

    @property
    def bound_resource_evidence_digest(self) -> str:
        if self.resource_evidence is not None:
            return self.resource_evidence.evidence_digest
        return self.resource_evidence_digest

    @property
    def bound_cost_evidence_digest(self) -> str:
        if self.cost_evidence is not None:
            return self.cost_evidence.evidence_digest
        return self.cost_evidence_digest

    @property
    def evidence_digest(self) -> str:
        return canonical_semantic_digest(
            "video.media-catalog.spark-observation-evidence.p4.v1",
            self.identity.provider,
            self.identity.application_id,
            self.identity.job_run_id,
            self.provider_state,
            self.outcome,
            self.observed_at,
            self.bound_resource_evidence_digest,
            self.bound_cost_evidence_digest,
        )


@dataclass(frozen=True)
class SparkCancelReceipt:
    identity: SparkBackendIdentity
    accepted: bool
    provider_state: str


@dataclass(frozen=True)
class SparkReconcileResult:
    identity: SparkBackendIdentity
    disposition: Literal["ADOPTED_RUNNING", "ADOPTED_TERMINAL"]
    observation: SparkBackendObservation
    reconcile_digest: str

    def __post_init__(self) -> None:
        if self.observation.identity != self.identity:
            raise ValueError("reconciled observation identity does not match")
        expected_disposition = (
            "ADOPTED_TERMINAL" if self.observation.terminal else "ADOPTED_RUNNING"
        )
        if self.disposition != expected_disposition:
            raise ValueError("reconcile disposition differs from observation")
        expected = canonical_semantic_digest(
            "video.media-catalog.spark-reconcile-result.p4.v1",
            self.identity.provider,
            self.identity.application_id,
            self.identity.job_run_id,
            self.disposition,
            self.observation.evidence_digest,
        )
        if self.reconcile_digest != expected:
            raise ValueError("reconcile digest does not match")

    @classmethod
    def create(
        cls,
        observation: SparkBackendObservation,
    ) -> SparkReconcileResult:
        disposition: Literal["ADOPTED_RUNNING", "ADOPTED_TERMINAL"] = (
            "ADOPTED_TERMINAL" if observation.terminal else "ADOPTED_RUNNING"
        )
        digest = canonical_semantic_digest(
            "video.media-catalog.spark-reconcile-result.p4.v1",
            observation.identity.provider,
            observation.identity.application_id,
            observation.identity.job_run_id,
            disposition,
            observation.evidence_digest,
        )
        return cls(
            identity=observation.identity,
            disposition=disposition,
            observation=observation,
            reconcile_digest=digest,
        )


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

    def reconcile(
        self,
        identity: SparkBackendIdentity,
    ) -> SparkReconcileResult: ...


class AwsEmrServerlessSparkBackend:
    provider: BackendProvider = "AWS_EMR_SERVERLESS"

    def __init__(
        self,
        client: Any,
        *,
        failure_injector: DeterministicFailureInjector | None = None,
    ) -> None:
        self.client = client
        self.failure_injector = failure_injector

    def _trip(self, operation: str) -> None:
        if self.failure_injector is not None:
            self.failure_injector.trip(operation)

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
        self._trip("submit.before")
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
        identity = SparkBackendIdentity(
            provider=self.provider,
            application_id=request.application_id,
            job_run_id=str(response["jobRunId"]),
            job_run_arn=(None if response.get("arn") is None else str(response["arn"])),
        )
        self._trip("submit.after")
        return identity

    def observe(
        self,
        identity: SparkBackendIdentity,
    ) -> SparkBackendObservation:
        self._require_identity(identity)
        self._trip("observe.before")
        response = self.client.get_job_run(
            applicationId=identity.application_id,
            jobRunId=identity.job_run_id,
        )
        job_run = dict(response["jobRun"])
        state = str(job_run["state"])
        resource_evidence = _resource_evidence(self.provider, job_run)
        observation = SparkBackendObservation(
            identity=identity,
            provider_state=state,
            outcome=_outcome(state),
            state_details=str(job_run.get("stateDetails") or ""),
            observed_at=_observed_at(
                job_run.get("updatedAt") or job_run.get("createdAt")
            ),
            resource_evidence=resource_evidence,
            cost_evidence=_cost_evidence(
                self.provider,
                job_run,
                resource_evidence,
            ),
            raw=job_run,
        )
        self._trip("observe.after")
        return observation

    def cancel(
        self,
        identity: SparkBackendIdentity,
        *,
        reason: str,
    ) -> SparkCancelReceipt:
        self._require_identity(identity)
        _bounded(reason, label="cancel reason", maximum=512)
        self._trip("cancel.before")
        response = self.client.cancel_job_run(
            applicationId=identity.application_id,
            jobRunId=identity.job_run_id,
        )
        state = str((response or {}).get("state") or "CANCEL_REQUESTED")
        receipt = SparkCancelReceipt(identity, True, state)
        self._trip("cancel.after")
        return receipt

    def reconcile(
        self,
        identity: SparkBackendIdentity,
    ) -> SparkReconcileResult:
        self._require_identity(identity)
        self._trip("reconcile.before")
        result = SparkReconcileResult.create(self.observe(identity))
        self._trip("reconcile.after")
        return result

    def _require_identity(self, identity: SparkBackendIdentity) -> None:
        if identity.provider != self.provider:
            raise ValueError("AWS adapter received a different provider identity")


class AliyunEmrServerlessSparkBackend:
    """SDK-neutral adapter over an allowlisted Alibaba Cloud API client."""

    provider: BackendProvider = "ALIYUN_EMR_SERVERLESS_SPARK"

    def __init__(
        self,
        client: Any,
        *,
        failure_injector: DeterministicFailureInjector | None = None,
    ) -> None:
        self.client = client
        self.failure_injector = failure_injector

    def _trip(self, operation: str) -> None:
        if self.failure_injector is not None:
            self.failure_injector.trip(operation)

    def submit(self, request: SparkSubmitRequest) -> SparkBackendIdentity:
        if request.provider != self.provider:
            raise ValueError("ALIYUN adapter received a different provider request")
        self._trip("submit.before")
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
        identity = SparkBackendIdentity(
            provider=self.provider,
            application_id=request.application_id,
            job_run_id=str(response["jobRunId"]),
            job_run_arn=(
                None
                if response.get("jobRunArn") is None
                else str(response["jobRunArn"])
            ),
        )
        self._trip("submit.after")
        return identity

    def observe(
        self,
        identity: SparkBackendIdentity,
    ) -> SparkBackendObservation:
        self._require_identity(identity)
        self._trip("observe.before")
        job_run = dict(
            self.client.get_job_run(
                workspace_id=identity.application_id,
                job_run_id=identity.job_run_id,
            )
        )
        state = str(job_run["state"])
        resource_evidence = _resource_evidence(self.provider, job_run)
        observation = SparkBackendObservation(
            identity=identity,
            provider_state=state,
            outcome=_outcome(state),
            state_details=str(job_run.get("stateDetails") or ""),
            observed_at=_observed_at(
                job_run.get("updatedAt") or job_run.get("createdAt")
            ),
            resource_evidence=resource_evidence,
            cost_evidence=_cost_evidence(
                self.provider,
                job_run,
                resource_evidence,
            ),
            raw=job_run,
        )
        self._trip("observe.after")
        return observation

    def cancel(
        self,
        identity: SparkBackendIdentity,
        *,
        reason: str,
    ) -> SparkCancelReceipt:
        self._require_identity(identity)
        _bounded(reason, label="cancel reason", maximum=512)
        self._trip("cancel.before")
        response = self.client.cancel_job_run(
            workspace_id=identity.application_id,
            job_run_id=identity.job_run_id,
            reason=reason,
        )
        state = str((response or {}).get("state") or "CANCEL_REQUESTED")
        receipt = SparkCancelReceipt(identity, True, state)
        self._trip("cancel.after")
        return receipt

    def reconcile(
        self,
        identity: SparkBackendIdentity,
    ) -> SparkReconcileResult:
        self._require_identity(identity)
        self._trip("reconcile.before")
        result = SparkReconcileResult.create(self.observe(identity))
        self._trip("reconcile.after")
        return result

    def _require_identity(self, identity: SparkBackendIdentity) -> None:
        if identity.provider != self.provider:
            raise ValueError("ALIYUN adapter received a different provider identity")
