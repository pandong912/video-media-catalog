"""Distributed quality metrics and immutable validation-stage contracts."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal
from urllib.parse import urlsplit

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_serializer,
    field_validator,
    model_validator,
)

from video_media_catalog.canonical import canonical_json, sha256_digest
from video_media_catalog.commit import ControlPublisher, PublishedObject
from video_media_catalog.constants import CURATED_TABLE_KEYS
from video_media_catalog.identity import require_canonical_uuid7
from video_media_catalog.models import ObjectRef
from video_media_catalog.runtime_args import RuntimeArguments
from video_media_catalog.spark_input import LandingInput

VALIDATE_STAGE = "media-catalog-validate"
QUALITY_SCHEMA_VERSION = "1.0"
QUALITY_ALGORITHM_ID = "media-catalog-quality-gate-v1"
QUALITY_REPORT_MEDIA_TYPE = "application/vnd.video-media-catalog.quality-report+json"
QUALITY_SUMMARY_MEDIA_TYPE = "application/vnd.video-media-catalog.quality-summary+json"
_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")


def _to_camel(value: str) -> str:
    first, *rest = value.split("_")
    return first + "".join(part.capitalize() for part in rest)


def _now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _validate_time(value: str) -> str:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("timestamp must be RFC3339") from exc
    if parsed.tzinfo is None:
        raise ValueError("timestamp must include a timezone")
    return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")


class QualityConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expected_entity_count: int = Field(default=0, ge=0)
    entity_count_tolerance_percent: float = Field(default=5.0, ge=0, le=100)
    minimum_name_coverage: float = Field(default=0.0, ge=0, le=1)
    max_closure_iterations: int = Field(default=64, gt=0)

    @property
    def minimum_entity_count(self) -> int | None:
        if self.expected_entity_count == 0:
            return None
        ratio = 1 - self.entity_count_tolerance_percent / 100
        return math.ceil(self.expected_entity_count * ratio)

    @property
    def maximum_entity_count(self) -> int | None:
        if self.expected_entity_count == 0:
            return None
        ratio = 1 + self.entity_count_tolerance_percent / 100
        return math.floor(self.expected_entity_count * ratio)

    @property
    def digest(self) -> str:
        return sha256_digest(
            canonical_json(
                {
                    "algorithmId": QUALITY_ALGORITHM_ID,
                    "entityCountTolerancePercent": (
                        self.entity_count_tolerance_percent
                    ),
                    "expectedEntityCount": self.expected_entity_count,
                    "maxClosureIterations": self.max_closure_iterations,
                    "minimumNameCoverage": self.minimum_name_coverage,
                }
            )
        )


class QualityMetrics(BaseModel):
    model_config = ConfigDict(extra="forbid")

    table_counts: dict[str, int]
    null_primary_keys: dict[str, int]
    duplicate_primary_keys: dict[str, int]
    dangling_relation_subjects: int = Field(ge=0)
    dangling_relation_objects: int = Field(ge=0)
    ingest_error_count: int = Field(ge=0)
    named_entity_count: int = Field(ge=0)
    name_coverage: float = Field(ge=0, le=1)

    @field_validator(
        "table_counts",
        "null_primary_keys",
        "duplicate_primary_keys",
    )
    @classmethod
    def validate_table_metrics(cls, value: dict[str, int]) -> dict[str, int]:
        if set(value) != set(CURATED_TABLE_KEYS):
            raise ValueError("quality table metrics must contain exactly six tables")
        if any(isinstance(count, bool) or count < 0 for count in value.values()):
            raise ValueError("quality table metrics must be non-negative integers")
        return value

    @model_validator(mode="after")
    def validate_derived_metrics(self) -> QualityMetrics:
        if self.ingest_error_count != self.table_counts["catalog_ingest_error"]:
            raise ValueError("ingest error count must equal the error table count")
        entity_count = self.table_counts["catalog_entity"]
        if self.named_entity_count > entity_count:
            raise ValueError("named entity count exceeds entity count")
        expected = self.named_entity_count / entity_count if entity_count else 1.0
        if not math.isclose(self.name_coverage, expected, abs_tol=1e-12):
            raise ValueError("name coverage does not match entity/name counts")
        return self


class QualityReport(BaseModel):
    model_config = ConfigDict(
        alias_generator=_to_camel,
        extra="forbid",
        populate_by_name=True,
    )

    schema_version: Literal["1.0"] = QUALITY_SCHEMA_VERSION
    stage: Literal["media-catalog-validate"] = VALIDATE_STAGE
    algorithm_id: Literal["media-catalog-quality-gate-v1"] = QUALITY_ALGORITHM_ID
    run_id: str
    job_spec_id: str
    tenant_id: str
    attempt: int = Field(gt=0)
    image_digest: str
    landing_manifest_id: str
    landing_manifest_digest: str
    expected_entity_count: int = Field(ge=0)
    minimum_entity_count: int | None = Field(default=None, ge=0)
    maximum_entity_count: int | None = Field(default=None, ge=0)
    entity_count_tolerance_percent: float = Field(ge=0, le=100)
    minimum_name_coverage: float = Field(ge=0, le=1)
    max_closure_iterations: int = Field(gt=0)
    table_counts: dict[str, int]
    null_primary_keys: dict[str, int]
    duplicate_primary_keys: dict[str, int]
    dangling_relation_subjects: int = Field(ge=0)
    dangling_relation_objects: int = Field(ge=0)
    ingest_error_count: int = Field(ge=0)
    named_entity_count: int = Field(ge=0)
    name_coverage: float = Field(ge=0, le=1)
    violations: list[str]
    status: Literal["PASS", "FAILED"]
    config_digest: str
    started_at: str
    completed_at: str

    @field_validator("run_id", "job_spec_id", "tenant_id")
    @classmethod
    def validate_uuid7(cls, value: str) -> str:
        return require_canonical_uuid7(value)

    @field_validator(
        "landing_manifest_id",
        "landing_manifest_digest",
        "config_digest",
        "image_digest",
    )
    @classmethod
    def validate_digest(cls, value: str) -> str:
        if _SHA256.fullmatch(value) is None:
            raise ValueError("quality digests must use sha256:<64 lowercase hex>")
        return value

    @field_validator("started_at", "completed_at")
    @classmethod
    def validate_timestamp(cls, value: str) -> str:
        return _validate_time(value)

    @field_validator(
        "table_counts",
        "null_primary_keys",
        "duplicate_primary_keys",
    )
    @classmethod
    def validate_table_maps(cls, value: dict[str, int]) -> dict[str, int]:
        if set(value) != set(CURATED_TABLE_KEYS):
            raise ValueError("quality report must contain all six table metrics")
        if any(isinstance(count, bool) or count < 0 for count in value.values()):
            raise ValueError("quality report metrics must be non-negative integers")
        return value

    @model_validator(mode="after")
    def validate_report(self) -> QualityReport:
        config = QualityConfig(
            expected_entity_count=self.expected_entity_count,
            entity_count_tolerance_percent=self.entity_count_tolerance_percent,
            minimum_name_coverage=self.minimum_name_coverage,
            max_closure_iterations=self.max_closure_iterations,
        )
        if config.digest != self.config_digest:
            raise ValueError("quality config digest does not bind report thresholds")
        if self.minimum_entity_count != config.minimum_entity_count:
            raise ValueError("minimum entity count does not match quality config")
        if self.maximum_entity_count != config.maximum_entity_count:
            raise ValueError("maximum entity count does not match quality config")
        metrics = QualityMetrics(
            table_counts=self.table_counts,
            null_primary_keys=self.null_primary_keys,
            duplicate_primary_keys=self.duplicate_primary_keys,
            dangling_relation_subjects=self.dangling_relation_subjects,
            dangling_relation_objects=self.dangling_relation_objects,
            ingest_error_count=self.ingest_error_count,
            named_entity_count=self.named_entity_count,
            name_coverage=self.name_coverage,
        )
        expected_violations = quality_violations(metrics, config)
        if self.violations != expected_violations:
            raise ValueError("quality report violations do not match its metrics")
        expected_status = "FAILED" if expected_violations else "PASS"
        if self.status != expected_status:
            raise ValueError("quality report status does not match its violations")
        started = datetime.fromisoformat(self.started_at.replace("Z", "+00:00"))
        completed = datetime.fromisoformat(self.completed_at.replace("Z", "+00:00"))
        if completed < started:
            raise ValueError("quality report completedAt precedes startedAt")
        return self

    @field_serializer(
        "attempt",
        "expected_entity_count",
        "minimum_entity_count",
        "maximum_entity_count",
        "dangling_relation_subjects",
        "dangling_relation_objects",
        "ingest_error_count",
        "named_entity_count",
        "max_closure_iterations",
        when_used="json",
    )
    def serialize_int64(self, value: int | None) -> str | None:
        return None if value is None else str(value)

    @field_serializer(
        "table_counts",
        "null_primary_keys",
        "duplicate_primary_keys",
        when_used="json",
    )
    def serialize_metric_map(self, value: dict[str, int]) -> dict[str, str]:
        return {key: str(count) for key, count in value.items()}

    def json_bytes(self) -> bytes:
        return (
            canonical_json(
                self.model_dump(mode="json", by_alias=True, exclude_none=False)
            ).encode("utf-8")
            + b"\n"
        )


class QualitySummary(BaseModel):
    model_config = ConfigDict(
        alias_generator=_to_camel,
        extra="forbid",
        populate_by_name=True,
    )

    schema_version: Literal["1.0"] = QUALITY_SCHEMA_VERSION
    stage: Literal["media-catalog-validate"] = VALIDATE_STAGE
    status: Literal["PASS", "FAILED"]
    report: ObjectRef
    run_id: str
    job_spec_id: str
    tenant_id: str
    attempt: int = Field(gt=0)
    input_manifest: ObjectRef
    image_digest: str
    landing_manifest_id: str
    landing_manifest_digest: str
    quality_config_digest: str

    @field_validator("run_id", "job_spec_id", "tenant_id")
    @classmethod
    def validate_uuid7(cls, value: str) -> str:
        return require_canonical_uuid7(value)

    @field_validator(
        "landing_manifest_id",
        "landing_manifest_digest",
        "quality_config_digest",
        "image_digest",
    )
    @classmethod
    def validate_digest(cls, value: str) -> str:
        if _SHA256.fullmatch(value) is None:
            raise ValueError("quality summary digests must use sha256:<hex>")
        return value

    @model_validator(mode="after")
    def validate_report_reference(self) -> QualitySummary:
        parsed = urlsplit(self.report.uri)
        if (
            self.report.format != "OBJECT_FORMAT_JSON"
            or self.report.media_type != QUALITY_REPORT_MEDIA_TYPE
            or self.report.size_bytes <= 0
        ):
            raise ValueError("quality summary report ObjectRef is invalid")
        if parsed.scheme == "s3" and (
            self.report.etag is None
            or self.report.object_version is None
            or self.report.object_version == "null"
        ):
            raise ValueError("S3 quality report requires ETag and VersionId")
        if (
            self.input_manifest.format != "OBJECT_FORMAT_PARQUET"
            or self.input_manifest.media_type != "application/vnd.apache.parquet"
            or self.input_manifest.etag is None
            or self.input_manifest.object_version is None
            or self.input_manifest.object_version == "null"
        ):
            raise ValueError("quality summary input manifest ObjectRef is invalid")
        return self

    @field_serializer("attempt", when_used="json")
    def serialize_attempt(self, value: int) -> str:
        return str(value)

    def json_bytes(self) -> bytes:
        return (
            canonical_json(
                self.model_dump(mode="json", by_alias=True, exclude_none=True)
            ).encode("utf-8")
            + b"\n"
        )


@dataclass(frozen=True)
class QualityPublication:
    report: QualityReport
    summary: QualitySummary
    report_object: PublishedObject
    summary_object: PublishedObject


def compute_quality_metrics(frames: dict[str, Any]) -> QualityMetrics:
    """Compute all gate metrics using Spark aggregations and anti-joins."""

    if set(frames) != set(CURATED_TABLE_KEYS):
        raise ValueError("quality validation requires all six curated tables")
    from pyspark.sql import functions as F

    table_counts: dict[str, int] = {}
    null_keys: dict[str, int] = {}
    duplicate_keys: dict[str, int] = {}
    for table, primary_key in CURATED_TABLE_KEYS.items():
        frame = frames[table]
        aggregate = frame.agg(
            F.count(F.lit(1)).alias("row_count"),
            F.sum(
                F.when(F.col(primary_key).isNull(), F.lit(1)).otherwise(F.lit(0))
            ).alias("null_count"),
        ).first()
        table_counts[table] = int(aggregate["row_count"])
        null_keys[table] = int(aggregate["null_count"] or 0)
        duplicate_keys[table] = (
            frame.where(F.col(primary_key).isNotNull())
            .groupBy(primary_key)
            .count()
            .where(F.col("count") > 1)
            .count()
        )

    entity_keys = frames["catalog_entity"].select("entity_key").dropDuplicates()
    relation = frames["catalog_relation"]
    dangling_subjects = (
        relation.select(F.col("subject_entity_key").alias("entity_key"))
        .join(entity_keys, "entity_key", "left_anti")
        .count()
    )
    dangling_objects = (
        relation.select(F.col("object_entity_key").alias("entity_key"))
        .join(entity_keys, "entity_key", "left_anti")
        .count()
    )
    named_entities = (
        frames["catalog_name"]
        .select("entity_key")
        .dropDuplicates()
        .join(entity_keys, "entity_key", "inner")
        .count()
    )
    entity_count = table_counts["catalog_entity"]
    coverage = named_entities / entity_count if entity_count else 1.0
    return QualityMetrics(
        table_counts=table_counts,
        null_primary_keys=null_keys,
        duplicate_primary_keys=duplicate_keys,
        dangling_relation_subjects=dangling_subjects,
        dangling_relation_objects=dangling_objects,
        ingest_error_count=table_counts["catalog_ingest_error"],
        named_entity_count=named_entities,
        name_coverage=coverage,
    )


def quality_violations(
    metrics: QualityMetrics,
    config: QualityConfig,
) -> list[str]:
    violations: list[str] = []
    for table in CURATED_TABLE_KEYS:
        if count := metrics.null_primary_keys[table]:
            violations.append(f"NULL_PRIMARY_KEY:{table}:{count}")
        if count := metrics.duplicate_primary_keys[table]:
            violations.append(f"DUPLICATE_PRIMARY_KEY:{table}:{count}")
    if metrics.ingest_error_count:
        violations.append(f"INGEST_ERRORS:{metrics.ingest_error_count}")
    if metrics.dangling_relation_subjects:
        violations.append(
            f"DANGLING_RELATION_SUBJECTS:{metrics.dangling_relation_subjects}"
        )
    if metrics.dangling_relation_objects:
        violations.append(
            f"DANGLING_RELATION_OBJECTS:{metrics.dangling_relation_objects}"
        )
    if metrics.name_coverage < config.minimum_name_coverage:
        violations.append(
            "NAME_COVERAGE_BELOW_MINIMUM:"
            f"{metrics.name_coverage:.12g}<{config.minimum_name_coverage:.12g}"
        )
    minimum = config.minimum_entity_count
    maximum = config.maximum_entity_count
    entity_count = metrics.table_counts["catalog_entity"]
    if (
        minimum is not None
        and maximum is not None
        and not minimum <= entity_count <= maximum
    ):
        violations.append(
            f"ENTITY_COUNT_OUT_OF_RANGE:{entity_count}:[{minimum},{maximum}]"
        )
    return violations


def build_quality_report(
    *,
    runtime: RuntimeArguments,
    landing_input: LandingInput,
    config: QualityConfig,
    metrics: QualityMetrics,
    started_at: str,
    completed_at: str | None = None,
) -> QualityReport:
    violations = quality_violations(metrics, config)
    return QualityReport(
        run_id=runtime.run_id,
        job_spec_id=runtime.job_spec_id,
        tenant_id=runtime.tenant_id,
        attempt=runtime.attempt,
        image_digest=runtime.image_digest,
        landing_manifest_id=landing_input.manifest.manifest_id,
        landing_manifest_digest=landing_input.manifest_digest,
        expected_entity_count=config.expected_entity_count,
        minimum_entity_count=config.minimum_entity_count,
        maximum_entity_count=config.maximum_entity_count,
        entity_count_tolerance_percent=config.entity_count_tolerance_percent,
        minimum_name_coverage=config.minimum_name_coverage,
        max_closure_iterations=config.max_closure_iterations,
        table_counts=metrics.table_counts,
        null_primary_keys=metrics.null_primary_keys,
        duplicate_primary_keys=metrics.duplicate_primary_keys,
        dangling_relation_subjects=metrics.dangling_relation_subjects,
        dangling_relation_objects=metrics.dangling_relation_objects,
        ingest_error_count=metrics.ingest_error_count,
        named_entity_count=metrics.named_entity_count,
        name_coverage=metrics.name_coverage,
        violations=violations,
        status="FAILED" if violations else "PASS",
        config_digest=config.digest,
        started_at=started_at,
        completed_at=completed_at or _now(),
    )


def publish_quality_report(
    *,
    publisher: ControlPublisher,
    runtime: RuntimeArguments,
    landing_input: LandingInput,
    report: QualityReport,
) -> QualityPublication:
    report_object = publisher.publish_immutable(
        "quality-report.json",
        report.json_bytes(),
    )
    report_ref = report_object.object_ref(QUALITY_REPORT_MEDIA_TYPE)
    summary = QualitySummary(
        status=report.status,
        report=report_ref,
        run_id=runtime.run_id,
        job_spec_id=runtime.job_spec_id,
        tenant_id=runtime.tenant_id,
        attempt=runtime.attempt,
        input_manifest=runtime.input_manifest,
        image_digest=runtime.image_digest,
        landing_manifest_id=landing_input.manifest.manifest_id,
        landing_manifest_digest=landing_input.manifest_digest,
        quality_config_digest=report.config_digest,
    )
    summary_object = publisher.publish_immutable(
        "quality-summary.json",
        summary.json_bytes(),
    )
    return QualityPublication(report, summary, report_object, summary_object)


def read_pending_quality_report(
    *,
    publisher: ControlPublisher,
    runtime: RuntimeArguments,
    landing_input: LandingInput,
    expected_config_digest: str,
) -> QualityReport | None:
    """Recover a valid report written before a missing commit-last summary."""

    report_object = publisher.read_optional("quality-report.json")
    if report_object is None:
        return None
    report = QualityReport.model_validate_json(report_object.payload)
    if (
        report.run_id != runtime.run_id
        or report.job_spec_id != runtime.job_spec_id
        or report.tenant_id != runtime.tenant_id
        or report.attempt != runtime.attempt
        or report.image_digest != runtime.image_digest
        or report.landing_manifest_id != landing_input.manifest.manifest_id
        or report.landing_manifest_digest != landing_input.manifest_digest
        or report.config_digest != expected_config_digest
    ):
        raise ValueError("pending quality report conflicts with current input/config")
    return report


def _object_ref_matches(reference: ObjectRef, published: PublishedObject) -> bool:
    return reference == published.object_ref(QUALITY_REPORT_MEDIA_TYPE)


def read_quality_gate(
    *,
    publisher: ControlPublisher,
    runtime: RuntimeArguments,
    landing_input: LandingInput,
    expected_config_digest: str | None = None,
    required: bool = True,
    require_pass: bool = True,
) -> QualityPublication | None:
    """Read and fully bind commit-last summary to report and current input."""

    summary_object = publisher.read_optional("quality-summary.json")
    if summary_object is None:
        if required:
            raise ValueError("required quality summary is missing")
        return None
    summary = QualitySummary.model_validate_json(summary_object.payload)
    expected_identity = (
        summary.run_id == runtime.run_id
        and summary.job_spec_id == runtime.job_spec_id
        and summary.tenant_id == runtime.tenant_id
        and summary.attempt == runtime.attempt
        and summary.input_manifest == runtime.input_manifest
        and summary.image_digest == runtime.image_digest
        and summary.landing_manifest_id == landing_input.manifest.manifest_id
        and summary.landing_manifest_digest == landing_input.manifest_digest
    )
    if not expected_identity:
        raise ValueError("quality summary does not bind the current input identity")
    if (
        expected_config_digest is not None
        and summary.quality_config_digest != expected_config_digest
    ):
        raise ValueError("quality summary does not bind the expected quality config")

    report_object = publisher.read_optional("quality-report.json")
    if report_object is None:
        raise ValueError("quality summary references a missing report")
    if not _object_ref_matches(summary.report, report_object):
        raise ValueError("quality summary report ObjectRef verification failed")
    report = QualityReport.model_validate_json(report_object.payload)
    if (
        report.status != summary.status
        or report.run_id != runtime.run_id
        or report.job_spec_id != runtime.job_spec_id
        or report.tenant_id != runtime.tenant_id
        or report.attempt != runtime.attempt
        or report.image_digest != runtime.image_digest
        or report.landing_manifest_id != landing_input.manifest.manifest_id
        or report.landing_manifest_digest != landing_input.manifest_digest
        or report.config_digest != summary.quality_config_digest
    ):
        raise ValueError("quality report does not bind its summary and current input")
    if require_pass and report.status != "PASS":
        raise ValueError("quality gate did not pass")
    return QualityPublication(report, summary, report_object, summary_object)
