"""Provider-SDK-neutral boundaries for Platform unified execution contracts.

The JSON shapes in this module mirror the vendored Platform schemas. Digest
calculation mirrors ``Sha256Digest.canonicalTuple`` in the Platform kernel:
each UTF-8 field is prefixed by its four-byte, big-endian length.
"""

from __future__ import annotations

import hashlib
import hmac
import re
import struct
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Annotated, Any, Literal
from urllib.parse import unquote, urlsplit

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    StringConstraints,
    model_validator,
)

from video_media_catalog.canonical import canonical_json_bytes
from video_media_catalog.v2_contracts import to_camel

OBJECT_REF_V2_SCHEMA_VERSION = "2.0.0"
EXECUTION_INPUT_MANIFEST_SCHEMA_VERSION = "1.0.0"
EXECUTION_PLAN_REQUEST_SCHEMA_VERSION = "1.0.0"
EXECUTION_PLAN_MANIFEST_SCHEMA_VERSION = "1.0.0"
PARTITION_RECEIPT_SCHEMA_VERSION = "1.0.0"
RECEIPT_SET_SCHEMA_VERSION = "1.0.0"
ROOT_MANIFEST_SCHEMA_VERSION = "1.0.0"

EXECUTION_INPUT_MANIFEST_MEDIA_TYPE = (
    "application/vnd.video-world.execution-input-manifest.v1+json"
)
EXECUTION_PLAN_REQUEST_MEDIA_TYPE = (
    "application/vnd.video-world.execution-plan-request.v1+json"
)
EXECUTION_PLAN_MANIFEST_MEDIA_TYPE = (
    "application/vnd.video-world.execution-plan-manifest.v1+json"
)
PARTITION_RECEIPT_MEDIA_TYPE = "application/vnd.video-world.partition-receipt.v1+json"
RECEIPT_SET_MEDIA_TYPE = "application/vnd.video-world.receipt-set.v1+json"
ROOT_MANIFEST_MEDIA_TYPE = "application/vnd.video-world.execution-root-manifest.v1+json"

_MAX_OBJECT_BYTES = 1_125_899_906_842_624
_UUID_V7_PATTERN = (
    r"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-"
    r"[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)
_SHA256_PATTERN = r"^[0-9a-f]{64}$"
_TOKEN_PATTERN = r"^[A-Z][A-Z0-9_]{0,63}$"
_PLANNER_VERSION = re.compile(r"^[0-9A-Za-z][0-9A-Za-z._+-]{0,63}$")
_PROFILE_KEY = re.compile(r"^[a-z][a-z0-9-]{0,63}$")
_REGION = re.compile(r"^[a-z0-9][a-z0-9-]{0,62}[a-z0-9]$")
_MEDIA_TYPE = re.compile(
    r"^[a-z0-9][a-z0-9!#$&^_.+-]{0,126}/"
    r"[a-z0-9][a-z0-9!#$&^_.+-]{0,126}$"
)
_BUCKET = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
_CONTROL = re.compile(r"[\x00-\x1f\x7f-\x9f]")
_BAD_PERCENT_ESCAPE = re.compile(r"%(?![0-9A-Fa-f]{2})")
_MUTABLE_VERSION_TOKENS = frozenset(
    {"current", "head", "latest", "mutable", "none", "null", "unversioned"}
)
_RFC3339 = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})"
    r"(?P<fraction>\.\d{1,9})?"
    r"(?P<zone>Z|[+-]\d{2}:\d{2})$"
)

UuidV7 = Annotated[
    str,
    StringConstraints(strict=True, pattern=_UUID_V7_PATTERN),
]
Sha256Hex = Annotated[
    str,
    StringConstraints(strict=True, pattern=_SHA256_PATTERN),
]
CanonicalToken = Annotated[
    str,
    StringConstraints(strict=True, pattern=_TOKEN_PATTERN),
]
StrictCount = Annotated[int, Field(strict=True, ge=0, le=1_000_000_000_000)]
StrictOrdinal = Annotated[int, Field(strict=True, ge=0, le=999_999)]
StrictObjectBytes = Annotated[
    int,
    Field(strict=True, ge=0, le=_MAX_OBJECT_BYTES),
]


def _canonical_instant(value: Any) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("timestamp must include a timezone")
        normalized = value.astimezone(UTC)
        base = normalized.strftime("%Y-%m-%dT%H:%M:%S")
        nanoseconds = normalized.microsecond * 1_000
    elif isinstance(value, str):
        if value != value.strip():
            raise ValueError("timestamp must be an exact RFC3339 value")
        match = _RFC3339.fullmatch(value)
        if match is None:
            raise ValueError("timestamp must be an RFC3339 instant")
        fraction = (match.group("fraction") or ".0")[1:]
        nanoseconds = int(fraction.ljust(9, "0"))
        try:
            parsed = datetime.fromisoformat(
                match.group("date") + match.group("zone").replace("Z", "+00:00")
            )
        except ValueError as exc:
            raise ValueError("timestamp must be an RFC3339 instant") from exc
        base = parsed.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S")
    else:
        raise TypeError("timestamp must be an RFC3339 string or datetime")

    if nanoseconds == 0:
        suffix = ""
    elif nanoseconds % 1_000_000 == 0:
        suffix = f".{nanoseconds // 1_000_000:03d}"
    elif nanoseconds % 1_000 == 0:
        suffix = f".{nanoseconds // 1_000:06d}"
    else:
        suffix = f".{nanoseconds:09d}"
    return f"{base}{suffix}Z"


InstantText = Annotated[
    str,
    BeforeValidator(_canonical_instant),
    StringConstraints(strict=True),
]


def _instant_key(value: str) -> tuple[datetime, int]:
    match = _RFC3339.fullmatch(value)
    if match is None or match.group("zone") != "Z":
        raise ValueError("timestamp is not a canonical UTC instant")
    fraction = (match.group("fraction") or ".0")[1:]
    return (
        datetime.fromisoformat(match.group("date")).replace(tzinfo=UTC),
        int(fraction.ljust(9, "0")),
    )


def canonical_semantic_digest(domain: str, *fields: str) -> str:
    """Return Platform-compatible SHA-256 for a domain-separated field tuple."""

    return canonical_tuple_sha256((domain, *fields))


def canonical_tuple_sha256(fields: Iterable[str]) -> str:
    """Hash unambiguous UTF-8 fields using Platform's canonical tuple format."""

    digest = hashlib.sha256()
    for field in fields:
        if not isinstance(field, str):
            raise TypeError("canonical digest fields must be strings")
        encoded = field.encode("utf-8")
        if len(encoded) > 0x7FFF_FFFF:
            raise ValueError("canonical digest field is too large")
        digest.update(struct.pack(">I", len(encoded)))
        digest.update(encoded)
    return digest.hexdigest()


def _require_bounded_text(value: str, *, label: str, maximum: int) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > maximum
        or _CONTROL.search(value) is not None
    ):
        raise ValueError(f"{label} must be canonical bounded text")
    return value


def _decoded_key(value: str) -> str:
    if _BAD_PERCENT_ESCAPE.search(value) is not None:
        raise ValueError("object key contains invalid percent encoding")
    decoded = value
    for _ in range(8):
        try:
            next_value = unquote(decoded, errors="strict")
        except (UnicodeDecodeError, ValueError) as exc:
            raise ValueError("object key contains invalid percent encoding") from exc
        if next_value == decoded:
            return decoded
        decoded = next_value
    raise ValueError("object key contains excessive percent encoding")


def _require_object_key(value: str) -> str:
    value = _require_bounded_text(value, label="objectKey", maximum=1_024)
    if (
        value.startswith("/")
        or any(character in value for character in "\\%?#")
        or len(value.encode("utf-8")) > 1_024
    ):
        raise ValueError("objectKey must be a canonical relative object key")
    for candidate in (value, _decoded_key(value)):
        if _CONTROL.search(candidate) is not None or "\\" in candidate:
            raise ValueError("objectKey contains forbidden characters")
        if any(part in {"", ".", ".."} for part in candidate.split("/")):
            raise ValueError("objectKey contains an empty, dot, or dot-dot segment")
    return value


def _require_provider_uri(
    value: str,
    *,
    provider: Literal["AWS_S3", "ALIBABA_OSS"],
    object_key: str,
) -> str:
    value = _require_bounded_text(value, label="uri", maximum=2_048)
    if "\\" in value:
        raise ValueError("uri contains forbidden characters")
    parsed = urlsplit(value)
    scheme = "s3" if provider == "AWS_S3" else "oss"
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError("uri must be an exact credential-free provider URI") from exc
    if (
        parsed.scheme != scheme
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or port is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("uri must be an exact credential-free provider URI")
    bucket = parsed.netloc
    if bucket != parsed.hostname or _BUCKET.fullmatch(bucket) is None or ".." in bucket:
        raise ValueError("uri bucket must be lowercase and canonical")
    expected = f"{scheme}://{bucket}/{object_key}"
    if value != expected or parsed.path != f"/{object_key}":
        raise ValueError("uri must exactly bind the declared objectKey")
    return value


class ExecutionContractModel(BaseModel):
    """Strict immutable lower-camel execution boundary."""

    model_config = ConfigDict(
        alias_generator=to_camel,
        extra="forbid",
        frozen=True,
        populate_by_name=True,
    )

    def json_bytes(self) -> bytes:
        return canonical_json_bytes(
            self.model_dump(mode="json", by_alias=True),
            newline=True,
        )


ObjectFormat = Literal["BYTES", "JSON", "JSONL", "PARQUET", "PROTOBUF", "OTHER"]
ObjectProvider = Literal["AWS_S3", "ALIBABA_OSS"]


class ObjectRefV2(ExecutionContractModel):
    """Credential-free provider-neutral immutable object identity."""

    schema_version: Literal["2.0.0"]
    object_ref_id: UuidV7
    tenant_id: UuidV7
    project_id: UuidV7
    data_plane_binding_id: UuidV7
    data_plane_id: UuidV7
    provider: ObjectProvider
    uri: str
    object_key: str
    format: ObjectFormat
    media_type: str
    checksum: Sha256Hex
    size_bytes: Annotated[int, Field(strict=True, ge=1, le=_MAX_OBJECT_BYTES)]
    etag: str
    version_token: str
    schema_digest: Sha256Hex | None
    encryption_context_digest: Sha256Hex
    identity_digest: Sha256Hex
    created_at: InstantText

    @model_validator(mode="after")
    def validate_identity(self) -> ObjectRefV2:
        object_key = _require_object_key(self.object_key)
        _require_provider_uri(
            self.uri,
            provider=self.provider,
            object_key=object_key,
        )
        if len(self.media_type) > 255 or _MEDIA_TYPE.fullmatch(self.media_type) is None:
            raise ValueError("mediaType must be a canonical type/subtype")
        _require_bounded_text(self.etag, label="etag", maximum=512)
        _require_bounded_text(
            self.version_token,
            label="versionToken",
            maximum=1_024,
        )
        if (
            self.version_token.casefold() in _MUTABLE_VERSION_TOKENS
            or any(character.isspace() for character in self.version_token)
            or any(character in "\\?#" for character in self.version_token)
            or _CONTROL.search(self.version_token) is not None
        ):
            raise ValueError("versionToken must name an exact immutable version")
        if (self.format == "BYTES") != (self.schema_digest is None):
            raise ValueError("only BYTES ObjectRef v2 may omit schemaDigest")
        expected = self.computed_identity_digest()
        if not hmac.compare_digest(self.identity_digest, expected):
            raise ValueError(
                "identityDigest does not match the non-recursive ObjectRef projection"
            )
        return self

    def computed_identity_digest(self) -> str:
        return canonical_semantic_digest(
            "video.world.object-ref.v2",
            self.schema_version,
            self.tenant_id,
            self.project_id,
            self.data_plane_binding_id,
            self.data_plane_id,
            self.provider,
            self.uri,
            self.object_key,
            self.format,
            self.media_type,
            self.checksum,
            str(self.size_bytes),
            self.etag,
            self.version_token,
            self.schema_digest or "",
            self.encryption_context_digest,
        )

    @classmethod
    def create(
        cls,
        *,
        object_ref_id: str,
        tenant_id: str,
        project_id: str,
        data_plane_binding_id: str,
        data_plane_id: str,
        provider: ObjectProvider,
        uri: str,
        object_key: str,
        format: ObjectFormat,
        media_type: str,
        checksum: str,
        size_bytes: int,
        etag: str,
        version_token: str,
        schema_digest: str | None,
        encryption_context_digest: str,
        created_at: str | datetime,
    ) -> ObjectRefV2:
        fields = (
            OBJECT_REF_V2_SCHEMA_VERSION,
            tenant_id,
            project_id,
            data_plane_binding_id,
            data_plane_id,
            provider,
            uri,
            object_key,
            format,
            media_type,
            checksum,
            str(size_bytes),
            etag,
            version_token,
            schema_digest or "",
            encryption_context_digest,
        )
        identity_digest = canonical_semantic_digest(
            "video.world.object-ref.v2",
            *fields,
        )
        return cls(
            schema_version=OBJECT_REF_V2_SCHEMA_VERSION,
            object_ref_id=object_ref_id,
            tenant_id=tenant_id,
            project_id=project_id,
            data_plane_binding_id=data_plane_binding_id,
            data_plane_id=data_plane_id,
            provider=provider,
            uri=uri,
            object_key=object_key,
            format=format,
            media_type=media_type,
            checksum=checksum,
            size_bytes=size_bytes,
            etag=etag,
            version_token=version_token,
            schema_digest=schema_digest,
            encryption_context_digest=encryption_context_digest,
            identity_digest=identity_digest,
            created_at=created_at,
        )


class ManifestInput(ExecutionContractModel):
    ordinal: StrictOrdinal
    role: CanonicalToken
    object: ObjectRefV2
    content_semantic_digest: Sha256Hex
    record_count: StrictCount

    def projection_digest(self) -> str:
        return canonical_semantic_digest(
            "video.world.execution-manifest-input.v1",
            str(self.ordinal),
            self.role,
            self.object.identity_digest,
            self.content_semantic_digest,
            str(self.record_count),
        )


class ExecutionInputManifest(ExecutionContractModel):
    schema_version: Literal["1.0.0"]
    tenant_id: UuidV7
    project_id: UuidV7
    source_request_id: UuidV7
    execution_plan_id: UuidV7
    plan_revision: Annotated[int, Field(strict=True, ge=1)]
    plan_request_digest: Sha256Hex
    execution_partition_id: UuidV7
    partition_ordinal: StrictOrdinal
    partition_digest: Sha256Hex
    source_authority_digest: Sha256Hex
    inputs: Annotated[
        tuple[ManifestInput, ...],
        Field(min_length=1, max_length=1_000_000),
    ]
    total_record_count: StrictCount
    created_at: InstantText
    semantic_digest: Sha256Hex

    @model_validator(mode="after")
    def validate_manifest(self) -> ExecutionInputManifest:
        roles: set[str] = set()
        for ordinal, item in enumerate(self.inputs):
            if item.ordinal != ordinal or item.role in roles:
                raise ValueError("inputs require contiguous ordinals and unique roles")
            roles.add(item.role)
            if (
                item.object.tenant_id != self.tenant_id
                or item.object.project_id != self.project_id
            ):
                raise ValueError(
                    "input ObjectRefs must match the manifest tenant and project"
                )
        if self.total_record_count != sum(item.record_count for item in self.inputs):
            raise ValueError("totalRecordCount does not match inputs")
        expected = self.computed_semantic_digest()
        if not hmac.compare_digest(self.semantic_digest, expected):
            raise ValueError(
                "semanticDigest does not match the input manifest projection"
            )
        return self

    def computed_semantic_digest(self) -> str:
        return canonical_semantic_digest(
            "video.world.execution-input-manifest.hash-projection.v1",
            self.schema_version,
            self.tenant_id,
            self.project_id,
            self.source_request_id,
            self.execution_plan_id,
            str(self.plan_revision),
            self.plan_request_digest,
            self.execution_partition_id,
            str(self.partition_ordinal),
            self.partition_digest,
            self.source_authority_digest,
            str(self.total_record_count),
            *(item.projection_digest() for item in self.inputs),
        )

    @classmethod
    def create(
        cls,
        *,
        tenant_id: str,
        project_id: str,
        source_request_id: str,
        source_authority_digest: str,
        execution_plan_id: str,
        plan_revision: int,
        plan_request_digest: str,
        execution_partition_id: str,
        partition_ordinal: int,
        partition_digest: str,
        inputs: Iterable[ManifestInput],
        created_at: str | datetime,
    ) -> ExecutionInputManifest:
        canonical_inputs = tuple(inputs)
        total = sum(item.record_count for item in canonical_inputs)
        semantic_digest = canonical_semantic_digest(
            "video.world.execution-input-manifest.hash-projection.v1",
            EXECUTION_INPUT_MANIFEST_SCHEMA_VERSION,
            tenant_id,
            project_id,
            source_request_id,
            execution_plan_id,
            str(plan_revision),
            plan_request_digest,
            execution_partition_id,
            str(partition_ordinal),
            partition_digest,
            source_authority_digest,
            str(total),
            *(item.projection_digest() for item in canonical_inputs),
        )
        return cls(
            schema_version=EXECUTION_INPUT_MANIFEST_SCHEMA_VERSION,
            tenant_id=tenant_id,
            project_id=project_id,
            source_request_id=source_request_id,
            execution_plan_id=execution_plan_id,
            plan_revision=plan_revision,
            plan_request_digest=plan_request_digest,
            execution_partition_id=execution_partition_id,
            partition_ordinal=partition_ordinal,
            partition_digest=partition_digest,
            source_authority_digest=source_authority_digest,
            inputs=canonical_inputs,
            total_record_count=total,
            created_at=created_at,
            semantic_digest=semantic_digest,
        )


class ExecutionPlanRequest(ExecutionContractModel):
    schema_version: Literal["1.0.0"]
    tenant_id: UuidV7
    project_id: UuidV7
    source_kind: Literal[
        "ASSET_VERSION",
        "PROCESSING_ORDER",
        "DATASET_REVISION",
        "PRODUCTION_PROJECT",
        "OPERATOR_RUN",
    ]
    source_identity: UuidV7
    source_revision: Annotated[int, Field(strict=True, ge=1)]
    input_manifest_semantic_digest: Sha256Hex
    execution_stage: CanonicalToken
    capability: CanonicalToken
    partition_policy: CanonicalToken
    partition_policy_digest: Sha256Hex
    planner_key: str
    planner_version: str
    planner_release_digest: Sha256Hex
    execution_profile_digest: Sha256Hex
    output_schema_digest: Sha256Hex
    model_digest: Sha256Hex
    config_digest: Sha256Hex
    policy_digest: Sha256Hex
    rights_fence_digest: Sha256Hex
    denylist_fence_digest: Sha256Hex
    derivative_fence_digest: Sha256Hex
    backend_eligibility_digest: Sha256Hex
    data_plane_eligibility_digest: Sha256Hex
    retry_policy_digest: Sha256Hex
    checkpoint_policy_digest: Sha256Hex
    reducer_contract_digest: Sha256Hex
    priority_class: Literal["BULK", "STANDARD", "INTERACTIVE", "CRITICAL"]
    cost_budget_digest: Sha256Hex
    finalization_mode: Literal[
        "WORLD_COMMIT_LAST",
        "EXTERNAL_AUTHORITY_ONLY",
        "RECEIPT_ONLY",
    ]
    replaces_plan_revision: Annotated[int, Field(strict=True, ge=1)] | None = None
    replan_reason: CanonicalToken | None = None
    plan_request_digest: Sha256Hex

    @model_validator(mode="after")
    def validate_request(self) -> ExecutionPlanRequest:
        _require_bounded_text(self.planner_key, label="plannerKey", maximum=128)
        if _PLANNER_VERSION.fullmatch(self.planner_version) is None:
            raise ValueError("plannerVersion is not canonical")
        if (self.replaces_plan_revision is None) != (self.replan_reason is None):
            raise ValueError(
                "replacesPlanRevision and replanReason must be supplied together"
            )
        if not hmac.compare_digest(
            self.plan_request_digest,
            self.computed_plan_request_digest(),
        ):
            raise ValueError(
                "planRequestDigest does not match the complete plan request projection"
            )
        return self

    def json_bytes(self) -> bytes:
        return canonical_json_bytes(
            self.model_dump(mode="json", by_alias=True, exclude_none=True),
            newline=True,
        )

    def computed_plan_request_digest(self) -> str:
        return canonical_semantic_digest(
            "video.world.execution-plan-request.hash-projection.v1",
            self.schema_version,
            self.tenant_id,
            self.project_id,
            self.source_kind,
            self.source_identity,
            str(self.source_revision),
            self.input_manifest_semantic_digest,
            self.execution_stage,
            self.capability,
            self.partition_policy,
            self.partition_policy_digest,
            self.planner_key,
            self.planner_version,
            self.planner_release_digest,
            self.execution_profile_digest,
            self.output_schema_digest,
            self.model_digest,
            self.config_digest,
            self.policy_digest,
            self.rights_fence_digest,
            self.denylist_fence_digest,
            self.derivative_fence_digest,
            self.backend_eligibility_digest,
            self.data_plane_eligibility_digest,
            self.retry_policy_digest,
            self.checkpoint_policy_digest,
            self.reducer_contract_digest,
            self.priority_class,
            self.cost_budget_digest,
            self.finalization_mode,
            (
                ""
                if self.replaces_plan_revision is None
                else str(self.replaces_plan_revision)
            ),
            self.replan_reason or "",
        )

    @classmethod
    def create(cls, **fields: Any) -> ExecutionPlanRequest:
        """Build and validate a request while deriving its recursive-safe digest."""

        draft = cls.model_construct(
            schema_version=EXECUTION_PLAN_REQUEST_SCHEMA_VERSION,
            plan_request_digest="0" * 64,
            **fields,
        )
        return cls(
            schema_version=EXECUTION_PLAN_REQUEST_SCHEMA_VERSION,
            plan_request_digest=draft.computed_plan_request_digest(),
            **fields,
        )


class PartitionDescriptor(ExecutionContractModel):
    execution_partition_id: UuidV7
    partition_ordinal: StrictOrdinal
    partition_key: str
    execution_stage: CanonicalToken
    plan_request_digest: Sha256Hex
    workload_digest: Sha256Hex
    execution_profile_digest: Sha256Hex
    input_projection_digest: Sha256Hex
    coverage_start: Annotated[int, Field(strict=True, ge=0)]
    coverage_end: Annotated[int, Field(strict=True, ge=1)]
    required_for_completion: Annotated[bool, Field(strict=True)]
    estimated_input_bytes: StrictObjectBytes
    estimated_output_bytes: StrictObjectBytes

    @model_validator(mode="after")
    def validate_partition(self) -> PartitionDescriptor:
        _require_bounded_text(
            self.partition_key,
            label="partitionKey",
            maximum=255,
        )
        if self.coverage_end <= self.coverage_start:
            raise ValueError("partition coverage must be a non-empty interval")
        return self

    def projection_digest(self) -> str:
        return canonical_semantic_digest(
            "video.world.execution-partition-descriptor.v1",
            self.execution_partition_id,
            str(self.partition_ordinal),
            self.partition_key,
            self.execution_stage,
            self.plan_request_digest,
            self.workload_digest,
            self.execution_profile_digest,
            self.input_projection_digest,
            str(self.coverage_start),
            str(self.coverage_end),
            str(self.required_for_completion).lower(),
            str(self.estimated_input_bytes),
            str(self.estimated_output_bytes),
        )

    def coverage_projection_digest(self) -> str:
        return canonical_semantic_digest(
            "video.world.execution-partition.coverage.v1",
            str(self.partition_ordinal),
            self.workload_digest,
            str(self.coverage_start),
            str(self.coverage_end),
            str(self.required_for_completion).lower(),
        )


class ExecutionPlanManifest(ExecutionContractModel):
    schema_version: Literal["1.0.0"]
    tenant_id: UuidV7
    project_id: UuidV7
    execution_plan_id: UuidV7
    plan_revision: Annotated[int, Field(strict=True, ge=1)]
    plan_request_digest: Sha256Hex
    plan_digest: Sha256Hex
    planning_input_manifest_digest: Sha256Hex
    partitions: Annotated[
        tuple[PartitionDescriptor, ...],
        Field(min_length=1, max_length=1_000_000),
    ]
    dependency_digests: Annotated[
        tuple[Sha256Hex, ...],
        Field(max_length=10_000_000),
    ]
    barrier_digests: Annotated[
        tuple[Sha256Hex, ...],
        Field(max_length=1_000_000),
    ]
    coverage_digest: Sha256Hex
    created_at: InstantText
    semantic_digest: Sha256Hex

    @model_validator(mode="after")
    def validate_manifest(self) -> ExecutionPlanManifest:
        partition_ids: set[str] = set()
        partition_keys: set[str] = set()
        for ordinal, partition in enumerate(self.partitions):
            if (
                partition.partition_ordinal != ordinal
                or partition.execution_partition_id in partition_ids
                or partition.partition_key in partition_keys
            ):
                raise ValueError(
                    "partitions require contiguous ordinals and unique identities"
                )
            partition_ids.add(partition.execution_partition_id)
            partition_keys.add(partition.partition_key)
            if partition.plan_request_digest != self.plan_request_digest:
                raise ValueError("partitions must match the planRequestDigest")
        if len(set(self.dependency_digests)) != len(self.dependency_digests):
            raise ValueError("dependencyDigests must be unique")
        if len(set(self.barrier_digests)) != len(self.barrier_digests):
            raise ValueError("barrierDigests must be unique")
        if not hmac.compare_digest(
            self.coverage_digest,
            self.computed_coverage_digest(),
        ):
            raise ValueError(
                "coverageDigest does not match complete partition coverage"
            )
        if not hmac.compare_digest(self.plan_digest, self.computed_plan_digest()):
            raise ValueError("planDigest does not match the complete plan projection")
        expected = self.computed_semantic_digest()
        if not hmac.compare_digest(self.semantic_digest, expected):
            raise ValueError(
                "semanticDigest does not match the plan manifest projection"
            )
        return self

    def computed_coverage_digest(self) -> str:
        return canonical_semantic_digest(
            "video.world.execution-plan.coverage.v1",
            str(len(self.partitions)),
            *(partition.coverage_projection_digest() for partition in self.partitions),
        )

    def computed_plan_digest(self) -> str:
        return canonical_semantic_digest(
            "video.world.execution-plan.hash-projection.v1",
            self.plan_request_digest,
            self.planning_input_manifest_digest,
            str(len(self.partitions)),
            *(partition.projection_digest() for partition in self.partitions),
            str(len(self.dependency_digests)),
            *self.dependency_digests,
            str(len(self.barrier_digests)),
            *self.barrier_digests,
            self.coverage_digest,
        )

    def computed_semantic_digest(self) -> str:
        return canonical_semantic_digest(
            "video.world.execution-plan-manifest.hash-projection.v1",
            self.schema_version,
            self.tenant_id,
            self.project_id,
            self.execution_plan_id,
            str(self.plan_revision),
            self.plan_request_digest,
            self.plan_digest,
            self.planning_input_manifest_digest,
            str(len(self.partitions)),
            *(partition.projection_digest() for partition in self.partitions),
            str(len(self.dependency_digests)),
            *self.dependency_digests,
            str(len(self.barrier_digests)),
            *self.barrier_digests,
            self.coverage_digest,
        )

    @classmethod
    def create(
        cls,
        *,
        tenant_id: str,
        project_id: str,
        execution_plan_id: str,
        plan_revision: int,
        plan_request_digest: str,
        planning_input_manifest_digest: str,
        partitions: Iterable[PartitionDescriptor],
        dependency_digests: Iterable[str],
        barrier_digests: Iterable[str],
        created_at: str | datetime,
    ) -> ExecutionPlanManifest:
        canonical_partitions = tuple(partitions)
        canonical_dependencies = tuple(dependency_digests)
        canonical_barriers = tuple(barrier_digests)
        coverage_digest = canonical_semantic_digest(
            "video.world.execution-plan.coverage.v1",
            str(len(canonical_partitions)),
            *(
                partition.coverage_projection_digest()
                for partition in canonical_partitions
            ),
        )
        plan_digest = canonical_semantic_digest(
            "video.world.execution-plan.hash-projection.v1",
            plan_request_digest,
            planning_input_manifest_digest,
            str(len(canonical_partitions)),
            *(partition.projection_digest() for partition in canonical_partitions),
            str(len(canonical_dependencies)),
            *canonical_dependencies,
            str(len(canonical_barriers)),
            *canonical_barriers,
            coverage_digest,
        )
        semantic_digest = canonical_semantic_digest(
            "video.world.execution-plan-manifest.hash-projection.v1",
            EXECUTION_PLAN_MANIFEST_SCHEMA_VERSION,
            tenant_id,
            project_id,
            execution_plan_id,
            str(plan_revision),
            plan_request_digest,
            plan_digest,
            planning_input_manifest_digest,
            str(len(canonical_partitions)),
            *(partition.projection_digest() for partition in canonical_partitions),
            str(len(canonical_dependencies)),
            *canonical_dependencies,
            str(len(canonical_barriers)),
            *canonical_barriers,
            coverage_digest,
        )
        return cls(
            schema_version=EXECUTION_PLAN_MANIFEST_SCHEMA_VERSION,
            tenant_id=tenant_id,
            project_id=project_id,
            execution_plan_id=execution_plan_id,
            plan_revision=plan_revision,
            plan_request_digest=plan_request_digest,
            plan_digest=plan_digest,
            planning_input_manifest_digest=planning_input_manifest_digest,
            partitions=canonical_partitions,
            dependency_digests=canonical_dependencies,
            barrier_digests=canonical_barriers,
            coverage_digest=coverage_digest,
            created_at=created_at,
            semantic_digest=semantic_digest,
        )


class WorldStagedResult(ExecutionContractModel):
    kind: Literal["WORLD_STAGED"]
    objects: Annotated[
        tuple[ObjectRefV2, ...],
        Field(min_length=1, max_length=10_000),
    ]
    output_digest: Sha256Hex
    schema_digest: Sha256Hex
    record_count: StrictObjectBytes
    byte_count: StrictObjectBytes
    result_digest: Sha256Hex

    @model_validator(mode="after")
    def validate_result(self) -> WorldStagedResult:
        identities = tuple(item.identity_digest for item in self.objects)
        if len(set(identities)) != len(identities):
            raise ValueError("WORLD_STAGED objects must have unique identities")
        expected = self.computed_result_digest()
        if not hmac.compare_digest(self.result_digest, expected):
            raise ValueError("resultDigest does not match the WORLD_STAGED projection")
        return self

    def computed_result_digest(self) -> str:
        return canonical_semantic_digest(
            "video.world.partition-result.world-staged.v1",
            str(len(self.objects)),
            *(item.identity_digest for item in self.objects),
            self.output_digest,
            self.schema_digest,
            str(self.record_count),
            str(self.byte_count),
        )

    @classmethod
    def create(
        cls,
        *,
        objects: Iterable[ObjectRefV2],
        output_digest: str,
        schema_digest: str,
        record_count: int,
        byte_count: int,
    ) -> WorldStagedResult:
        canonical_objects = tuple(objects)
        result_digest = canonical_semantic_digest(
            "video.world.partition-result.world-staged.v1",
            str(len(canonical_objects)),
            *(item.identity_digest for item in canonical_objects),
            output_digest,
            schema_digest,
            str(record_count),
            str(byte_count),
        )
        return cls(
            kind="WORLD_STAGED",
            objects=canonical_objects,
            output_digest=output_digest,
            schema_digest=schema_digest,
            record_count=record_count,
            byte_count=byte_count,
            result_digest=result_digest,
        )


class HumanDecisionResult(ExecutionContractModel):
    kind: Literal["HUMAN_DECISION"]
    work_item_id: UuidV7
    final_revision: Annotated[int, Field(strict=True, ge=1)]
    decision: CanonicalToken
    actor_authority_digest: Sha256Hex
    rubric_digest: Sha256Hex
    evidence_digest: Sha256Hex
    artifact_fact_binding_digests: Annotated[
        tuple[Sha256Hex, ...],
        Field(max_length=10_000),
    ]
    result_digest: Sha256Hex

    @model_validator(mode="after")
    def validate_result(self) -> HumanDecisionResult:
        if len(set(self.artifact_fact_binding_digests)) != len(
            self.artifact_fact_binding_digests
        ):
            raise ValueError("artifactFactBindingDigests must be unique")
        expected = self.computed_result_digest()
        if not hmac.compare_digest(self.result_digest, expected):
            raise ValueError(
                "resultDigest does not match the HUMAN_DECISION projection"
            )
        return self

    def computed_result_digest(self) -> str:
        return canonical_semantic_digest(
            "video.world.partition-result.human-decision.v1",
            self.work_item_id,
            str(self.final_revision),
            self.decision,
            self.actor_authority_digest,
            self.rubric_digest,
            self.evidence_digest,
            str(len(self.artifact_fact_binding_digests)),
            *self.artifact_fact_binding_digests,
        )

    @classmethod
    def create(
        cls,
        *,
        work_item_id: str,
        final_revision: int,
        decision: str,
        actor_authority_digest: str,
        rubric_digest: str,
        evidence_digest: str,
        artifact_fact_binding_digests: Iterable[str],
    ) -> HumanDecisionResult:
        artifact_digests = tuple(artifact_fact_binding_digests)
        result_digest = canonical_semantic_digest(
            "video.world.partition-result.human-decision.v1",
            work_item_id,
            str(final_revision),
            decision,
            actor_authority_digest,
            rubric_digest,
            evidence_digest,
            str(len(artifact_digests)),
            *artifact_digests,
        )
        return cls(
            kind="HUMAN_DECISION",
            work_item_id=work_item_id,
            final_revision=final_revision,
            decision=decision,
            actor_authority_digest=actor_authority_digest,
            rubric_digest=rubric_digest,
            evidence_digest=evidence_digest,
            artifact_fact_binding_digests=artifact_digests,
            result_digest=result_digest,
        )


class ExternalAuthorityResult(ExecutionContractModel):
    kind: Literal["EXTERNAL_AUTHORITY"]
    authority_kind: CanonicalToken
    authority_id: str
    authority_result_digest: Sha256Hex
    completion_receipt_object: ObjectRefV2
    completion_receipt_digest: Sha256Hex
    result_digest: Sha256Hex

    @model_validator(mode="after")
    def validate_result(self) -> ExternalAuthorityResult:
        _require_bounded_text(
            self.authority_id,
            label="authorityId",
            maximum=255,
        )
        expected = self.computed_result_digest()
        if not hmac.compare_digest(self.result_digest, expected):
            raise ValueError(
                "resultDigest does not match the EXTERNAL_AUTHORITY projection"
            )
        return self

    def computed_result_digest(self) -> str:
        return canonical_semantic_digest(
            "video.world.partition-result.external-authority.v1",
            self.authority_kind,
            self.authority_id,
            self.authority_result_digest,
            self.completion_receipt_object.identity_digest,
            self.completion_receipt_digest,
        )

    @classmethod
    def create(
        cls,
        *,
        authority_kind: str,
        authority_id: str,
        authority_result_digest: str,
        completion_receipt_object: ObjectRefV2,
        completion_receipt_digest: str,
    ) -> ExternalAuthorityResult:
        result_digest = canonical_semantic_digest(
            "video.world.partition-result.external-authority.v1",
            authority_kind,
            authority_id,
            authority_result_digest,
            completion_receipt_object.identity_digest,
            completion_receipt_digest,
        )
        return cls(
            kind="EXTERNAL_AUTHORITY",
            authority_kind=authority_kind,
            authority_id=authority_id,
            authority_result_digest=authority_result_digest,
            completion_receipt_object=completion_receipt_object,
            completion_receipt_digest=completion_receipt_digest,
            result_digest=result_digest,
        )


PartitionResult = Annotated[
    WorldStagedResult | HumanDecisionResult | ExternalAuthorityResult,
    Field(discriminator="kind"),
]
ResultKind = Literal["WORLD_STAGED", "HUMAN_DECISION", "EXTERNAL_AUTHORITY"]


class PartitionReceipt(ExecutionContractModel):
    schema_version: Literal["1.0.0"]
    receipt_id: UuidV7
    tenant_id: UuidV7
    execution_attempt_id: UuidV7
    execution_plan_id: UuidV7
    execution_partition_id: UuidV7
    plan_digest: Sha256Hex
    input_manifest_digest: Sha256Hex
    producer_digest: Sha256Hex
    result: PartitionResult
    produced_at: InstantText
    recorded_at: InstantText
    semantic_digest: Sha256Hex

    @model_validator(mode="after")
    def validate_receipt(self) -> PartitionReceipt:
        if isinstance(self.result, WorldStagedResult):
            result_objects = self.result.objects
        elif isinstance(self.result, ExternalAuthorityResult):
            result_objects = (self.result.completion_receipt_object,)
        else:
            result_objects = ()
        if any(item.tenant_id != self.tenant_id for item in result_objects):
            raise ValueError("result ObjectRefs must match the receipt tenant")
        if _instant_key(self.recorded_at) < _instant_key(self.produced_at):
            raise ValueError("recordedAt must not precede producedAt")
        expected = self.computed_semantic_digest()
        if not hmac.compare_digest(self.semantic_digest, expected):
            raise ValueError(
                "semanticDigest does not match the partition receipt projection"
            )
        return self

    @property
    def result_kind(self) -> ResultKind:
        return self.result.kind

    @property
    def result_digest(self) -> str:
        return self.result.result_digest

    def computed_semantic_digest(self) -> str:
        return canonical_semantic_digest(
            "video.world.partition-receipt.hash-projection.v1",
            self.schema_version,
            self.receipt_id,
            self.tenant_id,
            self.execution_attempt_id,
            self.execution_plan_id,
            self.execution_partition_id,
            self.plan_digest,
            self.input_manifest_digest,
            self.producer_digest,
            self.result.kind,
            self.result.result_digest,
            self.produced_at,
        )

    @classmethod
    def create(
        cls,
        *,
        receipt_id: str,
        tenant_id: str,
        execution_attempt_id: str,
        execution_plan_id: str,
        execution_partition_id: str,
        plan_digest: str,
        input_manifest_digest: str,
        producer_digest: str,
        result: WorldStagedResult | HumanDecisionResult | ExternalAuthorityResult,
        produced_at: str | datetime,
        recorded_at: str | datetime,
    ) -> PartitionReceipt:
        canonical_produced_at = _canonical_instant(produced_at)
        semantic_digest = canonical_semantic_digest(
            "video.world.partition-receipt.hash-projection.v1",
            PARTITION_RECEIPT_SCHEMA_VERSION,
            receipt_id,
            tenant_id,
            execution_attempt_id,
            execution_plan_id,
            execution_partition_id,
            plan_digest,
            input_manifest_digest,
            producer_digest,
            result.kind,
            result.result_digest,
            canonical_produced_at,
        )
        return cls(
            schema_version=PARTITION_RECEIPT_SCHEMA_VERSION,
            receipt_id=receipt_id,
            tenant_id=tenant_id,
            execution_attempt_id=execution_attempt_id,
            execution_plan_id=execution_plan_id,
            execution_partition_id=execution_partition_id,
            plan_digest=plan_digest,
            input_manifest_digest=input_manifest_digest,
            producer_digest=producer_digest,
            result=result,
            produced_at=canonical_produced_at,
            recorded_at=recorded_at,
            semantic_digest=semantic_digest,
        )


class ExecutedReceiptEntry(ExecutionContractModel):
    binding_kind: Literal["EXECUTED"]
    partition_ordinal: StrictOrdinal
    execution_partition_id: UuidV7
    execution_attempt_id: UuidV7
    receipt_id: UuidV7
    result_kind: ResultKind
    result_digest: Sha256Hex
    receipt_semantic_digest: Sha256Hex

    def projection_digest(self) -> str:
        return canonical_semantic_digest(
            "video.world.receipt-set-entry.executed.v1",
            str(self.partition_ordinal),
            self.execution_partition_id,
            self.execution_attempt_id,
            self.receipt_id,
            self.result_kind,
            self.result_digest,
            self.receipt_semantic_digest,
        )


class ReusedReceiptEntry(ExecutionContractModel):
    binding_kind: Literal["REUSED"]
    partition_ordinal: StrictOrdinal
    execution_partition_id: UuidV7
    source_plan_id: UuidV7
    source_partition_id: UuidV7
    source_receipt_tenant_id: UuidV7
    source_receipt_id: UuidV7
    result_kind: ResultKind
    result_digest: Sha256Hex
    source_receipt_semantic_digest: Sha256Hex
    reuse_binding_id: UuidV7
    reuse_binding_digest: Sha256Hex

    def projection_digest(self) -> str:
        return canonical_semantic_digest(
            "video.world.receipt-set-entry.reused.v1",
            str(self.partition_ordinal),
            self.execution_partition_id,
            self.source_plan_id,
            self.source_partition_id,
            self.source_receipt_tenant_id,
            self.source_receipt_id,
            self.result_kind,
            self.result_digest,
            self.source_receipt_semantic_digest,
            self.reuse_binding_id,
            self.reuse_binding_digest,
        )


ReceiptSetEntry = Annotated[
    ExecutedReceiptEntry | ReusedReceiptEntry,
    Field(discriminator="binding_kind"),
]


class ReceiptSet(ExecutionContractModel):
    schema_version: Literal["1.0.0"]
    tenant_id: UuidV7
    execution_plan_id: UuidV7
    plan_digest: Sha256Hex
    receipts: Annotated[
        tuple[ReceiptSetEntry, ...],
        Field(min_length=1, max_length=1_000_000),
    ]
    created_at: InstantText
    semantic_digest: Sha256Hex

    @model_validator(mode="after")
    def validate_receipt_set(self) -> ReceiptSet:
        partition_ids: set[str] = set()
        receipt_ids: set[str] = set()
        for ordinal, receipt in enumerate(self.receipts):
            receipt_id = (
                receipt.receipt_id
                if isinstance(receipt, ExecutedReceiptEntry)
                else receipt.source_receipt_id
            )
            if (
                receipt.partition_ordinal != ordinal
                or receipt.execution_partition_id in partition_ids
                or receipt_id in receipt_ids
            ):
                raise ValueError(
                    "receipts require contiguous ordinals and unique identities"
                )
            partition_ids.add(receipt.execution_partition_id)
            receipt_ids.add(receipt_id)
            if (
                isinstance(receipt, ReusedReceiptEntry)
                and receipt.source_receipt_tenant_id != self.tenant_id
            ):
                raise ValueError(
                    "reused receipt tenant must match the receipt-set tenant"
                )
        expected = self.computed_semantic_digest()
        if not hmac.compare_digest(self.semantic_digest, expected):
            raise ValueError("semanticDigest does not match the receipt-set projection")
        return self

    def computed_semantic_digest(self) -> str:
        return canonical_semantic_digest(
            "video.world.receipt-set.hash-projection.v1",
            self.schema_version,
            self.tenant_id,
            self.execution_plan_id,
            self.plan_digest,
            str(len(self.receipts)),
            *(receipt.projection_digest() for receipt in self.receipts),
        )

    @classmethod
    def create(
        cls,
        *,
        tenant_id: str,
        execution_plan_id: str,
        plan_digest: str,
        receipts: Iterable[ReceiptSetEntry],
        created_at: str | datetime,
    ) -> ReceiptSet:
        canonical_receipts = tuple(receipts)
        semantic_digest = canonical_semantic_digest(
            "video.world.receipt-set.hash-projection.v1",
            RECEIPT_SET_SCHEMA_VERSION,
            tenant_id,
            execution_plan_id,
            plan_digest,
            str(len(canonical_receipts)),
            *(receipt.projection_digest() for receipt in canonical_receipts),
        )
        return cls(
            schema_version=RECEIPT_SET_SCHEMA_VERSION,
            tenant_id=tenant_id,
            execution_plan_id=execution_plan_id,
            plan_digest=plan_digest,
            receipts=canonical_receipts,
            created_at=created_at,
            semantic_digest=semantic_digest,
        )


class RootManifest(ExecutionContractModel):
    """Immutable post-execution receipt closure."""

    schema_version: Literal["1.0.0"]
    tenant_id: UuidV7
    project_id: UuidV7
    execution_plan_id: UuidV7
    plan_revision: Annotated[int, Field(strict=True, ge=1)]
    plan_request_digest: Sha256Hex
    plan_digest: Sha256Hex
    plan_manifest_object: ObjectRefV2
    plan_manifest_semantic_digest: Sha256Hex
    receipt_set_object: ObjectRefV2
    receipt_set_semantic_digest: Sha256Hex
    result_kind: ResultKind
    result_digest: Sha256Hex
    result_objects: Annotated[
        tuple[ObjectRefV2, ...],
        Field(max_length=10_000),
    ]
    created_at: InstantText
    semantic_digest: Sha256Hex

    @model_validator(mode="after")
    def validate_closure(self) -> RootManifest:
        for object_ref, media_type, label in (
            (
                self.plan_manifest_object,
                EXECUTION_PLAN_MANIFEST_MEDIA_TYPE,
                "planManifestObject",
            ),
            (self.receipt_set_object, RECEIPT_SET_MEDIA_TYPE, "receiptSetObject"),
        ):
            if (
                object_ref.tenant_id != self.tenant_id
                or object_ref.project_id != self.project_id
                or object_ref.media_type != media_type
            ):
                raise ValueError(
                    f"{label} must match the root tenant, project, and media type"
                )
        identities = tuple(item.identity_digest for item in self.result_objects)
        if len(set(identities)) != len(identities) or any(
            item.tenant_id != self.tenant_id or item.project_id != self.project_id
            for item in self.result_objects
        ):
            raise ValueError(
                "resultObjects must be unique and match the root tenant and project"
            )
        if (self.result_kind == "HUMAN_DECISION") != (not self.result_objects):
            raise ValueError("resultObjects must contain the applicable result closure")
        if not hmac.compare_digest(
            self.semantic_digest,
            self.computed_semantic_digest(),
        ):
            raise ValueError(
                "semanticDigest does not match the complete root receipt closure"
            )
        return self

    def computed_semantic_digest(self) -> str:
        return canonical_semantic_digest(
            "video.world.execution-root-manifest.hash-projection.v1",
            self.schema_version,
            self.tenant_id,
            self.project_id,
            self.execution_plan_id,
            str(self.plan_revision),
            self.plan_request_digest,
            self.plan_digest,
            self.plan_manifest_object.identity_digest,
            self.plan_manifest_semantic_digest,
            self.receipt_set_object.identity_digest,
            self.receipt_set_semantic_digest,
            self.result_kind,
            self.result_digest,
            str(len(self.result_objects)),
            *(item.identity_digest for item in self.result_objects),
        )

    @classmethod
    def create(
        cls,
        *,
        tenant_id: str,
        project_id: str,
        execution_plan_id: str,
        plan_revision: int,
        plan_request_digest: str,
        plan_digest: str,
        plan_manifest_object: ObjectRefV2,
        plan_manifest_semantic_digest: str,
        receipt_set_object: ObjectRefV2,
        receipt_set_semantic_digest: str,
        result_kind: ResultKind,
        result_digest: str,
        result_objects: Iterable[ObjectRefV2],
        created_at: str | datetime,
    ) -> RootManifest:
        canonical_objects = tuple(result_objects)
        semantic_digest = canonical_semantic_digest(
            "video.world.execution-root-manifest.hash-projection.v1",
            ROOT_MANIFEST_SCHEMA_VERSION,
            tenant_id,
            project_id,
            execution_plan_id,
            str(plan_revision),
            plan_request_digest,
            plan_digest,
            plan_manifest_object.identity_digest,
            plan_manifest_semantic_digest,
            receipt_set_object.identity_digest,
            receipt_set_semantic_digest,
            result_kind,
            result_digest,
            str(len(canonical_objects)),
            *(item.identity_digest for item in canonical_objects),
        )
        return cls(
            schema_version=ROOT_MANIFEST_SCHEMA_VERSION,
            tenant_id=tenant_id,
            project_id=project_id,
            execution_plan_id=execution_plan_id,
            plan_revision=plan_revision,
            plan_request_digest=plan_request_digest,
            plan_digest=plan_digest,
            plan_manifest_object=plan_manifest_object,
            plan_manifest_semantic_digest=plan_manifest_semantic_digest,
            receipt_set_object=receipt_set_object,
            receipt_set_semantic_digest=receipt_set_semantic_digest,
            result_kind=result_kind,
            result_digest=result_digest,
            result_objects=canonical_objects,
            created_at=created_at,
            semantic_digest=semantic_digest,
        )


CloudProvider = Literal["AWS", "ALIYUN"]
SparkBackendKind = Literal[
    "AWS_EMR_SERVERLESS",
    "ALIYUN_EMR_SERVERLESS_SPARK",
]
ExecutionPlanKind = Literal["EXECUTION", "TRANSFER"]

CATALOG_PROVIDER_PROFILE_SCHEMA_VERSION = "1.0.0"
CATALOG_TRANSFER_PLAN_SCHEMA_VERSION = "1.0.0"
CATALOG_COMPLETION_RECEIPT_SCHEMA_VERSION = "1.0.0"
CATALOG_COMPLETION_RECEIPT_MEDIA_TYPE = (
    "application/vnd.video-media-catalog.execution-completion-receipt.v1+json"
)


class WorkloadIdentityBinding(ExecutionContractModel):
    """Credential-free reference to one cloud workload identity."""

    cloud_provider: CloudProvider
    identity_reference: str
    subject_digest: Sha256Hex
    audience: str
    semantic_digest: Sha256Hex

    @model_validator(mode="after")
    def validate_binding(self) -> WorkloadIdentityBinding:
        reference = _require_bounded_text(
            self.identity_reference,
            label="identityReference",
            maximum=512,
        )
        audience = _require_bounded_text(
            self.audience,
            label="audience",
            maximum=255,
        )
        if any(marker in reference.casefold() for marker in ("secret", "token=", "?")):
            raise ValueError("identityReference must not contain credentials")
        if self.cloud_provider == "AWS":
            aws_role = r"arn:aws:iam::[0-9]{12}:role/[A-Za-z0-9+=,.@_/-]+"
            if re.fullmatch(aws_role, reference) is None:
                raise ValueError("AWS workload identity must be an IAM role ARN")
        elif (
            re.fullmatch(
                r"acs:ram::[0-9]{6,32}:role/[A-Za-z0-9+=,.@_/-]+",
                reference,
            )
            is None
        ):
            raise ValueError("ALIYUN workload identity must be a RAM role ARN")
        _ = audience
        if not hmac.compare_digest(
            self.semantic_digest,
            self.computed_semantic_digest(),
        ):
            raise ValueError("workload identity semanticDigest does not match")
        return self

    def computed_semantic_digest(self) -> str:
        return canonical_semantic_digest(
            "video.media-catalog.workload-identity-binding.v1",
            self.cloud_provider,
            self.identity_reference,
            self.subject_digest,
            self.audience,
        )

    @classmethod
    def create(
        cls,
        *,
        cloud_provider: CloudProvider,
        identity_reference: str,
        subject_digest: str,
        audience: str,
    ) -> WorkloadIdentityBinding:
        semantic_digest = canonical_semantic_digest(
            "video.media-catalog.workload-identity-binding.v1",
            cloud_provider,
            identity_reference,
            subject_digest,
            audience,
        )
        return cls(
            cloud_provider=cloud_provider,
            identity_reference=identity_reference,
            subject_digest=subject_digest,
            audience=audience,
            semantic_digest=semantic_digest,
        )


class SparkProviderProfile(ExecutionContractModel):
    """Pinned provider/runtime profile; it contains no credentials."""

    schema_version: Literal["1.0.0"]
    profile_key: str
    cloud_provider: CloudProvider
    object_provider: ObjectProvider
    backend_kind: SparkBackendKind
    region: str
    application_id: str
    workload_identity: WorkloadIdentityBinding
    max_input_bytes: Annotated[int, Field(strict=True, ge=1, le=_MAX_OBJECT_BYTES)]
    max_output_bytes: Annotated[int, Field(strict=True, ge=1, le=_MAX_OBJECT_BYTES)]
    transfer_only: Annotated[bool, Field(strict=True)]
    profile_digest: Sha256Hex

    @model_validator(mode="after")
    def validate_profile(self) -> SparkProviderProfile:
        if _PROFILE_KEY.fullmatch(self.profile_key) is None:
            raise ValueError("profileKey is not canonical")
        if _REGION.fullmatch(self.region) is None:
            raise ValueError("region is not canonical")
        _require_bounded_text(
            self.application_id,
            label="applicationId",
            maximum=255,
        )
        expected = {
            "AWS": ("AWS_S3", "AWS_EMR_SERVERLESS"),
            "ALIYUN": ("ALIBABA_OSS", "ALIYUN_EMR_SERVERLESS_SPARK"),
        }[self.cloud_provider]
        if (self.object_provider, self.backend_kind) != expected:
            raise ValueError(
                "cloudProvider, objectProvider, and backendKind must agree"
            )
        if self.workload_identity.cloud_provider != self.cloud_provider:
            raise ValueError("workload identity provider must match profile")
        if not hmac.compare_digest(
            self.profile_digest,
            self.computed_profile_digest(),
        ):
            raise ValueError("profileDigest does not match provider profile")
        return self

    def computed_profile_digest(self) -> str:
        return canonical_semantic_digest(
            "video.media-catalog.spark-provider-profile.v1",
            self.schema_version,
            self.profile_key,
            self.cloud_provider,
            self.object_provider,
            self.backend_kind,
            self.region,
            self.application_id,
            self.workload_identity.semantic_digest,
            str(self.max_input_bytes),
            str(self.max_output_bytes),
            str(self.transfer_only).lower(),
        )

    @classmethod
    def create(
        cls,
        *,
        profile_key: str,
        cloud_provider: CloudProvider,
        object_provider: ObjectProvider,
        backend_kind: SparkBackendKind,
        region: str,
        application_id: str,
        workload_identity: WorkloadIdentityBinding,
        max_input_bytes: int,
        max_output_bytes: int,
        transfer_only: bool = False,
    ) -> SparkProviderProfile:
        profile_digest = canonical_semantic_digest(
            "video.media-catalog.spark-provider-profile.v1",
            CATALOG_PROVIDER_PROFILE_SCHEMA_VERSION,
            profile_key,
            cloud_provider,
            object_provider,
            backend_kind,
            region,
            application_id,
            workload_identity.semantic_digest,
            str(max_input_bytes),
            str(max_output_bytes),
            str(transfer_only).lower(),
        )
        return cls(
            schema_version=CATALOG_PROVIDER_PROFILE_SCHEMA_VERSION,
            profile_key=profile_key,
            cloud_provider=cloud_provider,
            object_provider=object_provider,
            backend_kind=backend_kind,
            region=region,
            application_id=application_id,
            workload_identity=workload_identity,
            max_input_bytes=max_input_bytes,
            max_output_bytes=max_output_bytes,
            transfer_only=transfer_only,
            profile_digest=profile_digest,
        )


class TransferPlanBinding(ExecutionContractModel):
    """Explicit authorization for one cross-cloud transfer plan."""

    schema_version: Literal["1.0.0"]
    plan_kind: Literal["TRANSFER"]
    transfer_plan_id: UuidV7
    transfer_plan_digest: Sha256Hex
    source_provider: CloudProvider
    destination_provider: CloudProvider
    source_object_identity_digests: Annotated[
        tuple[Sha256Hex, ...],
        Field(min_length=1, max_length=10_000),
    ]
    destination_prefix: str
    semantic_digest: Sha256Hex

    @model_validator(mode="after")
    def validate_transfer(self) -> TransferPlanBinding:
        if self.source_provider == self.destination_provider:
            raise ValueError("TRANSFER plan must cross cloud providers")
        if len(set(self.source_object_identity_digests)) != len(
            self.source_object_identity_digests
        ):
            raise ValueError("TRANSFER source identities must be unique")
        scheme = "s3" if self.destination_provider == "AWS" else "oss"
        parsed = urlsplit(self.destination_prefix)
        if (
            parsed.scheme != scheme
            or not parsed.netloc
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError(
                "destinationPrefix must be a credential-free destination URI"
            )
        if not hmac.compare_digest(
            self.semantic_digest,
            self.computed_semantic_digest(),
        ):
            raise ValueError("TRANSFER semanticDigest does not match")
        return self

    def computed_semantic_digest(self) -> str:
        return canonical_semantic_digest(
            "video.media-catalog.transfer-plan-binding.v1",
            self.schema_version,
            self.plan_kind,
            self.transfer_plan_id,
            self.transfer_plan_digest,
            self.source_provider,
            self.destination_provider,
            str(len(self.source_object_identity_digests)),
            *self.source_object_identity_digests,
            self.destination_prefix,
        )

    @classmethod
    def create(
        cls,
        *,
        transfer_plan_id: str,
        transfer_plan_digest: str,
        source_provider: CloudProvider,
        destination_provider: CloudProvider,
        source_object_identity_digests: Iterable[str],
        destination_prefix: str,
    ) -> TransferPlanBinding:
        identities = tuple(source_object_identity_digests)
        semantic_digest = canonical_semantic_digest(
            "video.media-catalog.transfer-plan-binding.v1",
            CATALOG_TRANSFER_PLAN_SCHEMA_VERSION,
            "TRANSFER",
            transfer_plan_id,
            transfer_plan_digest,
            source_provider,
            destination_provider,
            str(len(identities)),
            *identities,
            destination_prefix,
        )
        return cls(
            schema_version=CATALOG_TRANSFER_PLAN_SCHEMA_VERSION,
            plan_kind="TRANSFER",
            transfer_plan_id=transfer_plan_id,
            transfer_plan_digest=transfer_plan_digest,
            source_provider=source_provider,
            destination_provider=destination_provider,
            source_object_identity_digests=identities,
            destination_prefix=destination_prefix,
            semantic_digest=semantic_digest,
        )


def object_ref_cloud_provider(reference: ObjectRefV2) -> CloudProvider:
    return "AWS" if reference.provider == "AWS_S3" else "ALIYUN"


def validate_execution_data_locality(
    *,
    profile: SparkProviderProfile,
    input_objects: Iterable[ObjectRefV2],
    plan_kind: ExecutionPlanKind,
    transfer_plan: TransferPlanBinding | None,
) -> tuple[ObjectRefV2, ...]:
    """Fail closed unless execution is data-local or an explicit TRANSFER."""

    objects = tuple(input_objects)
    if not objects:
        raise ValueError("execution requires at least one input ObjectRef v2")
    identities = tuple(item.identity_digest for item in objects)
    if len(set(identities)) != len(identities):
        raise ValueError("input ObjectRef v2 identities must be unique")
    input_clouds = {object_ref_cloud_provider(item) for item in objects}
    if plan_kind == "EXECUTION":
        if transfer_plan is not None or profile.transfer_only:
            raise ValueError("ordinary execution cannot carry a TRANSFER binding")
        if input_clouds != {profile.cloud_provider}:
            raise ValueError("cross-cloud execution requires an explicit TRANSFER plan")
        return objects
    if transfer_plan is None:
        raise ValueError("TRANSFER execution requires a transfer plan binding")
    if (
        not profile.transfer_only
        or transfer_plan.destination_provider != profile.cloud_provider
        or input_clouds != {transfer_plan.source_provider}
        or identities != transfer_plan.source_object_identity_digests
    ):
        raise ValueError("TRANSFER plan does not bind the exact source and profile")
    return objects


class CatalogBackendCompletionReceipt(ExecutionContractModel):
    """Bounded external-authority evidence returned to Platform."""

    schema_version: Literal["1.0.0"]
    tenant_id: UuidV7
    project_id: UuidV7
    execution_plan_id: UuidV7
    execution_partition_id: UuidV7
    execution_attempt_id: UuidV7
    provider_profile_digest: Sha256Hex
    backend_kind: SparkBackendKind
    backend_application_id: str
    backend_job_id: str
    terminal_state: Literal["SUCCESS", "FAILED", "CANCELLED"]
    result_kind: Literal["EXTERNAL_AUTHORITY"]
    authority_result_digest: Sha256Hex
    summary_digest: Sha256Hex
    observed_at: InstantText
    semantic_digest: Sha256Hex

    @model_validator(mode="after")
    def validate_receipt(self) -> CatalogBackendCompletionReceipt:
        _require_bounded_text(
            self.backend_application_id,
            label="backendApplicationId",
            maximum=255,
        )
        _require_bounded_text(
            self.backend_job_id,
            label="backendJobId",
            maximum=255,
        )
        if not hmac.compare_digest(
            self.semantic_digest,
            self.computed_semantic_digest(),
        ):
            raise ValueError("completion receipt semanticDigest does not match")
        return self

    def computed_semantic_digest(self) -> str:
        return canonical_semantic_digest(
            "video.media-catalog.backend-completion-receipt.v1",
            self.schema_version,
            self.tenant_id,
            self.project_id,
            self.execution_plan_id,
            self.execution_partition_id,
            self.execution_attempt_id,
            self.provider_profile_digest,
            self.backend_kind,
            self.backend_application_id,
            self.backend_job_id,
            self.terminal_state,
            self.result_kind,
            self.authority_result_digest,
            self.summary_digest,
            self.observed_at,
        )

    @classmethod
    def create(
        cls,
        *,
        tenant_id: str,
        project_id: str,
        execution_plan_id: str,
        execution_partition_id: str,
        execution_attempt_id: str,
        provider_profile_digest: str,
        backend_kind: SparkBackendKind,
        backend_application_id: str,
        backend_job_id: str,
        terminal_state: Literal["SUCCESS", "FAILED", "CANCELLED"],
        authority_result_digest: str,
        summary_digest: str,
        observed_at: str | datetime,
    ) -> CatalogBackendCompletionReceipt:
        canonical_observed_at = _canonical_instant(observed_at)
        semantic_digest = canonical_semantic_digest(
            "video.media-catalog.backend-completion-receipt.v1",
            CATALOG_COMPLETION_RECEIPT_SCHEMA_VERSION,
            tenant_id,
            project_id,
            execution_plan_id,
            execution_partition_id,
            execution_attempt_id,
            provider_profile_digest,
            backend_kind,
            backend_application_id,
            backend_job_id,
            terminal_state,
            "EXTERNAL_AUTHORITY",
            authority_result_digest,
            summary_digest,
            canonical_observed_at,
        )
        return cls(
            schema_version=CATALOG_COMPLETION_RECEIPT_SCHEMA_VERSION,
            tenant_id=tenant_id,
            project_id=project_id,
            execution_plan_id=execution_plan_id,
            execution_partition_id=execution_partition_id,
            execution_attempt_id=execution_attempt_id,
            provider_profile_digest=provider_profile_digest,
            backend_kind=backend_kind,
            backend_application_id=backend_application_id,
            backend_job_id=backend_job_id,
            terminal_state=terminal_state,
            result_kind="EXTERNAL_AUTHORITY",
            authority_result_digest=authority_result_digest,
            summary_digest=summary_digest,
            observed_at=canonical_observed_at,
            semantic_digest=semantic_digest,
        )
