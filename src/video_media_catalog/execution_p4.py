"""P4 operational evidence and hooks for Catalog execution.

This module is provider-neutral and contains no network, filesystem, clock, or
database access. Production integrations implement the hook protocol outside
the deterministic Workflow.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal, Protocol

from video_media_catalog.execution_contracts import canonical_semantic_digest

EvidenceScope = Literal["PROVIDER_OBSERVED", "SYNTHETIC_LOCAL"]
CostEvidenceStatus = Literal["PROVIDER_REPORTED", "UNAVAILABLE", "SYNTHETIC"]

_SHA256_LENGTH = 64
_ALLOWED_FAILURE_OPERATIONS = frozenset(
    {
        "submit.before",
        "submit.after",
        "observe.before",
        "observe.after",
        "cancel.before",
        "cancel.after",
        "reconcile.before",
        "reconcile.after",
    }
)


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


def _sha256(value: str, *, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != _SHA256_LENGTH
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{label} must be a lowercase SHA-256")
    return value


def _nonnegative(value: int, *, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"{label} must be a non-negative integer")
    return value


@dataclass(frozen=True)
class ObservedResourceEvidence:
    scope: EvidenceScope
    provider: str
    duration_milliseconds: int
    vcpu_milliseconds: int
    memory_gb_milliseconds: int
    storage_gb_milliseconds: int
    input_bytes: int
    output_bytes: int
    shuffle_read_bytes: int
    shuffle_write_bytes: int
    complete: bool
    evidence_digest: str

    def __post_init__(self) -> None:
        if self.scope not in ("PROVIDER_OBSERVED", "SYNTHETIC_LOCAL"):
            raise ValueError("resource evidence scope is not supported")
        _bounded(self.provider, label="provider", maximum=64)
        for label in (
            "duration_milliseconds",
            "vcpu_milliseconds",
            "memory_gb_milliseconds",
            "storage_gb_milliseconds",
            "input_bytes",
            "output_bytes",
            "shuffle_read_bytes",
            "shuffle_write_bytes",
        ):
            _nonnegative(getattr(self, label), label=label)
        if not isinstance(self.complete, bool):
            raise ValueError("complete must be a boolean")
        _sha256(self.evidence_digest, label="evidence_digest")
        if self.evidence_digest != self.computed_digest():
            raise ValueError("resource evidence digest does not match")

    def computed_digest(self) -> str:
        return canonical_semantic_digest(
            "video.media-catalog.execution-resource-evidence.p4.v1",
            self.scope,
            self.provider,
            str(self.duration_milliseconds),
            str(self.vcpu_milliseconds),
            str(self.memory_gb_milliseconds),
            str(self.storage_gb_milliseconds),
            str(self.input_bytes),
            str(self.output_bytes),
            str(self.shuffle_read_bytes),
            str(self.shuffle_write_bytes),
            str(self.complete).lower(),
        )

    @classmethod
    def create(
        cls,
        *,
        scope: EvidenceScope,
        provider: str,
        duration_milliseconds: int = 0,
        vcpu_milliseconds: int = 0,
        memory_gb_milliseconds: int = 0,
        storage_gb_milliseconds: int = 0,
        input_bytes: int = 0,
        output_bytes: int = 0,
        shuffle_read_bytes: int = 0,
        shuffle_write_bytes: int = 0,
        complete: bool = False,
    ) -> ObservedResourceEvidence:
        fields = (
            scope,
            provider,
            str(duration_milliseconds),
            str(vcpu_milliseconds),
            str(memory_gb_milliseconds),
            str(storage_gb_milliseconds),
            str(input_bytes),
            str(output_bytes),
            str(shuffle_read_bytes),
            str(shuffle_write_bytes),
            str(complete).lower(),
        )
        return cls(
            scope=scope,
            provider=provider,
            duration_milliseconds=duration_milliseconds,
            vcpu_milliseconds=vcpu_milliseconds,
            memory_gb_milliseconds=memory_gb_milliseconds,
            storage_gb_milliseconds=storage_gb_milliseconds,
            input_bytes=input_bytes,
            output_bytes=output_bytes,
            shuffle_read_bytes=shuffle_read_bytes,
            shuffle_write_bytes=shuffle_write_bytes,
            complete=complete,
            evidence_digest=canonical_semantic_digest(
                "video.media-catalog.execution-resource-evidence.p4.v1",
                *fields,
            ),
        )


@dataclass(frozen=True)
class ObservedCostEvidence:
    scope: EvidenceScope
    provider: str
    status: CostEvidenceStatus
    amount_micros: int | None
    currency: str | None
    rate_card_digest: str | None
    resource_evidence_digest: str
    evidence_digest: str

    def __post_init__(self) -> None:
        if self.scope not in ("PROVIDER_OBSERVED", "SYNTHETIC_LOCAL"):
            raise ValueError("cost evidence scope is not supported")
        if self.status not in ("PROVIDER_REPORTED", "UNAVAILABLE", "SYNTHETIC"):
            raise ValueError("cost evidence status is not supported")
        _bounded(self.provider, label="provider", maximum=64)
        _sha256(
            self.resource_evidence_digest,
            label="resource_evidence_digest",
        )
        if self.status == "PROVIDER_REPORTED":
            if self.scope != "PROVIDER_OBSERVED":
                raise ValueError(
                    "provider-reported cost requires PROVIDER_OBSERVED scope"
                )
            if self.amount_micros is None or self.currency is None:
                raise ValueError("provider-reported cost requires amount and currency")
        elif self.status == "UNAVAILABLE":
            if self.scope != "PROVIDER_OBSERVED":
                raise ValueError("unavailable cost requires PROVIDER_OBSERVED scope")
            if (
                self.amount_micros is not None
                or self.currency is not None
                or self.rate_card_digest is not None
            ):
                raise ValueError("unavailable cost cannot contain estimated values")
        elif self.scope != "SYNTHETIC_LOCAL":
            raise ValueError("synthetic cost must use SYNTHETIC_LOCAL scope")
        if self.amount_micros is not None:
            _nonnegative(self.amount_micros, label="amount_micros")
            if self.amount_micros > 1_000_000_000_000_000:
                raise ValueError("amount_micros exceeds the bounded contract")
        if self.currency is not None and (
            len(self.currency) != 3
            or not self.currency.isascii()
            or not self.currency.isupper()
            or not self.currency.isalpha()
        ):
            raise ValueError("currency must be a three-letter uppercase code")
        if self.rate_card_digest is not None:
            _sha256(self.rate_card_digest, label="rate_card_digest")
        _sha256(self.evidence_digest, label="evidence_digest")
        if self.evidence_digest != self.computed_digest():
            raise ValueError("cost evidence digest does not match")

    def computed_digest(self) -> str:
        return canonical_semantic_digest(
            "video.media-catalog.execution-cost-evidence.p4.v1",
            self.scope,
            self.provider,
            self.status,
            "" if self.amount_micros is None else str(self.amount_micros),
            self.currency or "",
            self.rate_card_digest or "",
            self.resource_evidence_digest,
        )

    @classmethod
    def create(
        cls,
        *,
        scope: EvidenceScope,
        provider: str,
        status: CostEvidenceStatus,
        resource_evidence_digest: str,
        amount_micros: int | None = None,
        currency: str | None = None,
        rate_card_digest: str | None = None,
    ) -> ObservedCostEvidence:
        digest = canonical_semantic_digest(
            "video.media-catalog.execution-cost-evidence.p4.v1",
            scope,
            provider,
            status,
            "" if amount_micros is None else str(amount_micros),
            currency or "",
            rate_card_digest or "",
            resource_evidence_digest,
        )
        return cls(
            scope=scope,
            provider=provider,
            status=status,
            amount_micros=amount_micros,
            currency=currency,
            rate_card_digest=rate_card_digest,
            resource_evidence_digest=resource_evidence_digest,
            evidence_digest=digest,
        )


@dataclass(frozen=True)
class SyntheticLocalPerformanceEvidence:
    scope: Literal["SYNTHETIC_LOCAL"]
    dataset_kind: Literal["DETERMINISTIC_FIXTURE"]
    file_count: int
    total_bytes: int
    small_file_threshold_bytes: int
    small_file_count: int
    max_to_median_size_ratio_bps: int
    synthetic_duration_milliseconds: int
    synthetic_throughput_bytes_per_second: int
    production_slo_claimed: Literal[False]
    evidence_digest: str

    def __post_init__(self) -> None:
        for label in (
            "file_count",
            "total_bytes",
            "small_file_threshold_bytes",
            "small_file_count",
            "max_to_median_size_ratio_bps",
            "synthetic_duration_milliseconds",
            "synthetic_throughput_bytes_per_second",
        ):
            _nonnegative(getattr(self, label), label=label)
        if self.file_count < 1 or self.synthetic_duration_milliseconds < 1:
            raise ValueError("synthetic evidence requires files and duration")
        if self.small_file_count > self.file_count:
            raise ValueError("small_file_count exceeds file_count")
        _sha256(self.evidence_digest, label="evidence_digest")
        if self.evidence_digest != self.computed_digest():
            raise ValueError("synthetic performance evidence digest does not match")

    def computed_digest(self) -> str:
        return canonical_semantic_digest(
            "video.media-catalog.synthetic-local-performance-evidence.p4.v1",
            self.scope,
            self.dataset_kind,
            str(self.file_count),
            str(self.total_bytes),
            str(self.small_file_threshold_bytes),
            str(self.small_file_count),
            str(self.max_to_median_size_ratio_bps),
            str(self.synthetic_duration_milliseconds),
            str(self.synthetic_throughput_bytes_per_second),
            "false",
        )

    @classmethod
    def create(
        cls,
        *,
        file_sizes: tuple[int, ...],
        small_file_threshold_bytes: int,
        synthetic_duration_milliseconds: int,
    ) -> SyntheticLocalPerformanceEvidence:
        if not file_sizes or any(
            not isinstance(size, int) or isinstance(size, bool) or size < 1
            for size in file_sizes
        ):
            raise ValueError("synthetic file sizes must be positive integers")
        if small_file_threshold_bytes < 1 or synthetic_duration_milliseconds < 1:
            raise ValueError("synthetic thresholds and duration must be positive")
        ordered = tuple(sorted(file_sizes))
        total = sum(ordered)
        median = ordered[(len(ordered) - 1) // 2]
        values = {
            "file_count": len(ordered),
            "total_bytes": total,
            "small_file_threshold_bytes": small_file_threshold_bytes,
            "small_file_count": sum(
                size < small_file_threshold_bytes for size in ordered
            ),
            "max_to_median_size_ratio_bps": ordered[-1] * 10_000 // median,
            "synthetic_duration_milliseconds": synthetic_duration_milliseconds,
            "synthetic_throughput_bytes_per_second": (
                total * 1_000 // synthetic_duration_milliseconds
            ),
        }
        digest = canonical_semantic_digest(
            "video.media-catalog.synthetic-local-performance-evidence.p4.v1",
            "SYNTHETIC_LOCAL",
            "DETERMINISTIC_FIXTURE",
            *(str(value) for value in values.values()),
            "false",
        )
        return cls(
            scope="SYNTHETIC_LOCAL",
            dataset_kind="DETERMINISTIC_FIXTURE",
            production_slo_claimed=False,
            evidence_digest=digest,
            **values,
        )

    def as_json(self) -> dict[str, str | int | bool]:
        return {
            "schemaVersion": (
                "video.media-catalog.synthetic-local-performance-evidence.p4.v1"
            ),
            "scope": self.scope,
            "datasetKind": self.dataset_kind,
            "fileCount": self.file_count,
            "totalBytes": self.total_bytes,
            "smallFileThresholdBytes": self.small_file_threshold_bytes,
            "smallFileCount": self.small_file_count,
            "maxToMedianSizeRatioBps": self.max_to_median_size_ratio_bps,
            "syntheticDurationMilliseconds": self.synthetic_duration_milliseconds,
            "syntheticThroughputBytesPerSecond": (
                self.synthetic_throughput_bytes_per_second
            ),
            "productionSloClaimed": self.production_slo_claimed,
            "evidenceDigest": self.evidence_digest,
        }


@dataclass(frozen=True)
class MetricSample:
    name: str
    value: int
    labels: tuple[tuple[str, str], ...]

    def __post_init__(self) -> None:
        _bounded(self.name, label="metric name", maximum=128)
        _nonnegative(self.value, label="metric value")
        if self.labels != tuple(sorted(self.labels)) or len(self.labels) > 16:
            raise ValueError("metric labels must be bounded and sorted")
        if len({key for key, _ in self.labels}) != len(self.labels):
            raise ValueError("metric label keys must be unique")
        for key, value in self.labels:
            _bounded(key, label="metric label key", maximum=64)
            _bounded(value, label="metric label value", maximum=128)


@dataclass(frozen=True)
class AuditEvent:
    event_type: str
    outcome: str
    execution_attempt_id: str
    backend_job_id: str | None
    evidence_digest: str | None

    def __post_init__(self) -> None:
        _bounded(self.event_type, label="audit event type", maximum=64)
        _bounded(self.outcome, label="audit outcome", maximum=64)
        _bounded(
            self.execution_attempt_id,
            label="execution attempt id",
            maximum=64,
        )
        if self.backend_job_id is not None:
            _bounded(self.backend_job_id, label="backend job id", maximum=255)
        if self.evidence_digest is not None:
            _sha256(self.evidence_digest, label="audit evidence digest")


class CatalogExecutionHooks(Protocol):
    def record_metric(self, sample: MetricSample) -> None: ...

    def record_audit(self, event: AuditEvent) -> None: ...


class NoopCatalogExecutionHooks:
    def record_metric(self, sample: MetricSample) -> None:
        del sample

    def record_audit(self, event: AuditEvent) -> None:
        del event


@dataclass(frozen=True)
class CatalogExecutionHealth:
    status: Literal["READY", "NOT_READY"]
    p3_manifest_present: bool
    p4_manifest_present: bool
    production_activation_allowed: bool
    reasons: tuple[str, ...]


def evaluate_catalog_execution_health(
    p3_lock: Mapping[str, object],
    p4_lock: Mapping[str, object],
) -> CatalogExecutionHealth:
    p3_manifest = bool(
        isinstance(p3_lock.get("p3Manifest"), dict)
        and p3_lock["p3Manifest"].get("present") is True  # type: ignore[union-attr]
    )
    p4_manifest = bool(
        isinstance(p4_lock.get("p4Manifest"), dict)
        and p4_lock["p4Manifest"].get("present") is True  # type: ignore[union-attr]
    )
    p4_provenance = p4_lock.get("platformProvenance")
    p4_committed_provenance = bool(
        isinstance(p4_provenance, dict)
        and p4_provenance.get("p4ArtifactsPresentInObservedCommit") is True
        and isinstance(p4_provenance.get("p4ArtifactCommit"), str)
    )
    activation = (
        p3_lock.get("runtimeActivationAllowed") is True
        and p4_lock.get("runtimeActivationAllowed") is True
        and p3_lock.get("claimsRuntimeActivation") is True
        and p4_lock.get("claimsRuntimeActivation") is True
    )
    reasons: list[str] = []
    if not p3_manifest:
        reasons.append("P3_MANIFEST_MISSING")
    if not p4_manifest:
        reasons.append("P4_MANIFEST_MISSING")
    if p4_manifest and not p4_committed_provenance:
        reasons.append("P4_COMMITTED_PROVENANCE_MISSING")
    if not activation:
        reasons.append("PRODUCTION_ACTIVATION_DISABLED")
    return CatalogExecutionHealth(
        status="READY" if not reasons else "NOT_READY",
        p3_manifest_present=p3_manifest,
        p4_manifest_present=p4_manifest,
        production_activation_allowed=activation,
        reasons=tuple(reasons),
    )


class InjectedCatalogExecutionFailure(RuntimeError):
    pass


class DeterministicFailureInjector:
    """Stateful local-only failure injector used by deterministic tests."""

    def __init__(
        self,
        failures: Mapping[str, int],
        *,
        scope: Literal["SYNTHETIC_LOCAL"] = "SYNTHETIC_LOCAL",
    ) -> None:
        if scope != "SYNTHETIC_LOCAL":
            raise ValueError("failure injection is restricted to SYNTHETIC_LOCAL")
        unknown = set(failures) - _ALLOWED_FAILURE_OPERATIONS
        if unknown:
            raise ValueError(f"unsupported failure injection operations: {unknown!r}")
        if any(
            not isinstance(count, int) or isinstance(count, bool) or count < 0
            for count in failures.values()
        ):
            raise ValueError("failure injection counts must be non-negative integers")
        self._remaining = dict(failures)

    def trip(self, operation: str) -> None:
        if operation not in _ALLOWED_FAILURE_OPERATIONS:
            raise ValueError(f"unsupported failure injection operation {operation!r}")
        remaining = self._remaining.get(operation, 0)
        if remaining < 1:
            return
        self._remaining[operation] = remaining - 1
        raise InjectedCatalogExecutionFailure(
            f"synthetic local failure injected at {operation}"
        )
