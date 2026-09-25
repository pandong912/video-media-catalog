"""Contracts and deterministic identity for Wikidata full-media backfills."""

from __future__ import annotations

from typing import Any, Literal, Self
from urllib.parse import urlsplit

from pydantic import Field, ValidationInfo, field_validator, model_validator

from video_media_catalog.canonical import deterministic_key
from video_media_catalog.connector import (
    MAX_RECORD_PARTITIONS_PER_EPOCH,
    MAX_RECORD_SET_EPOCHS,
    MAX_RECORD_SHARDS_PER_PARTITION,
    CaptureWindowPlan,
    CaptureWindowReceipt,
    CaptureWindowStatus,
    build_capture_window_receipt,
    plan_bounded_capture_windows,
)
from video_media_catalog.constants import (
    CREDIT_ORGANIZATION_PROPERTIES,
    CREDIT_PERSON_PROPERTIES,
    MEDIA_ENTITY_TYPES,
)
from video_media_catalog.models import ObjectRef
from video_media_catalog.source_mappers import WIKIDATA_SOURCE_PRODUCT_ID
from video_media_catalog.v2_contracts import (
    V2ContractModel,
    digest_identity,
    require_rfc3339,
    require_sha256,
)

FULL_MEDIA_ALGORITHM_ID = "video-media-catalog-wikidata-full-media-v1"
FULL_MEDIA_PAYLOAD_SCHEMA = "wikidata-full-media-v1"
FULL_MEDIA_PARENT_PROPERTIES = frozenset({"P179", "P361", "P4908"})
DEFAULT_TARGET_SHARD_BYTES = 8 * 1024 * 1024
DEFAULT_MAX_SHARD_BYTES = 16 * 1024 * 1024
DEFAULT_SHARDS_PER_PARTITION = 256
DEFAULT_PARTITIONS_PER_EPOCH = 64
_ZERO_DIGEST = "sha256:" + ("0" * 64)


def full_media_coverage_scope() -> dict[str, Any]:
    """Stable monthly scope; dump identity and physical sharding stay outside it."""

    return {
        "algorithmId": FULL_MEDIA_ALGORITHM_ID,
        "coverageId": "wikidata-full-media",
        "coverageVersion": "1",
        "sourceNamespaceId": "wikidata-item",
        "rootEntityTypes": sorted(MEDIA_ENTITY_TYPES),
        "parentClosureProperties": sorted(FULL_MEDIA_PARENT_PROPERTIES),
        "creditClosure": {
            "personProperties": sorted(CREDIT_PERSON_PROPERTIES),
            "organizationProperties": sorted(CREDIT_ORGANIZATION_PROPERTIES),
        },
    }


class FullMediaBackfillConfig(V2ContractModel):
    """Semantic and physical settings bound to a deterministic backfill."""

    schema_version: str = "1.0"
    algorithm_id: Literal["video-media-catalog-wikidata-full-media-v1"] = (
        FULL_MEDIA_ALGORITHM_ID
    )
    max_closure_iterations: int = Field(default=64, ge=1, le=1024)
    target_shard_bytes: int = Field(
        default=DEFAULT_TARGET_SHARD_BYTES,
        ge=1,
    )
    max_shard_bytes: int = Field(
        default=DEFAULT_MAX_SHARD_BYTES,
        ge=1,
    )
    max_shards_per_partition: int = Field(
        default=DEFAULT_SHARDS_PER_PARTITION,
        ge=1,
        le=MAX_RECORD_SHARDS_PER_PARTITION,
    )
    max_partitions_per_epoch: int = Field(
        default=DEFAULT_PARTITIONS_PER_EPOCH,
        ge=1,
        le=MAX_RECORD_PARTITIONS_PER_EPOCH,
    )
    max_epochs: int = Field(
        default=MAX_RECORD_SET_EPOCHS, ge=1, le=MAX_RECORD_SET_EPOCHS
    )

    @model_validator(mode="after")
    def validate_shard_sizes(self) -> Self:
        if self.target_shard_bytes > self.max_shard_bytes:
            raise ValueError("target_shard_bytes must not exceed max_shard_bytes")
        return self

    @property
    def digest(self) -> str:
        return digest_identity(
            self.model_dump(mode="json", by_alias=True, exclude_none=True)
        )

    @property
    def max_supported_shards(self) -> int:
        return (
            self.max_shards_per_partition
            * self.max_partitions_per_epoch
            * self.max_epochs
        )


class WikidataFullMediaProfile(V2ContractModel):
    """Bounded aggregate output for profile and full-backfill planning."""

    schema_version: str = "1.0"
    algorithm_id: Literal["video-media-catalog-wikidata-full-media-v1"] = (
        FULL_MEDIA_ALGORITHM_ID
    )
    build_digest: str
    config_digest: str
    image_digest: str
    batch_id: str
    dump: ObjectRef
    coverage_scope: dict[str, Any]
    coverage_scope_digest: str
    root_counts: dict[str, int]
    selected_type_counts: dict[str, int]
    parent_count: int = Field(ge=0)
    credit_person_count: int = Field(ge=0)
    credit_organization_count: int = Field(ge=0)
    record_count: int = Field(gt=0)
    relation_edge_count: int = Field(ge=0)
    parent_edge_count: int = Field(ge=0)
    credit_edge_count: int = Field(ge=0)
    envelope_bytes: int = Field(gt=0)
    estimated_shards: int = Field(gt=0)
    estimated_partitions: int = Field(gt=0)
    estimated_epochs: int = Field(gt=0)

    @field_validator(
        "build_digest",
        "config_digest",
        "image_digest",
        "batch_id",
        "coverage_scope_digest",
    )
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("root_counts", "selected_type_counts")
    @classmethod
    def validate_counts(cls, value: dict[str, int]) -> dict[str, int]:
        if any(isinstance(count, bool) or count < 0 for count in value.values()):
            raise ValueError("profile counts must be non-negative integers")
        return dict(sorted(value.items()))

    @model_validator(mode="after")
    def validate_profile(self) -> Self:
        if set(self.root_counts) != set(MEDIA_ENTITY_TYPES):
            raise ValueError("root_counts must contain every media entity type")
        if sum(self.selected_type_counts.values()) != self.record_count:
            raise ValueError("selected_type_counts must sum to record_count")
        if digest_identity(self.coverage_scope) != self.coverage_scope_digest:
            raise ValueError("coverage_scope_digest does not bind coverage_scope")
        if self.coverage_scope != full_media_coverage_scope():
            raise ValueError("profile coverage scope is not the full-media scope")
        return self


def full_media_build_digest(
    *,
    dump: ObjectRef,
    config_digest: str,
    image_digest: str,
) -> str:
    return digest_identity(
        {
            "algorithmId": FULL_MEDIA_ALGORITHM_ID,
            "dump": dump.model_dump(mode="json", by_alias=True, exclude_none=True),
            "configDigest": require_sha256(config_digest),
            "imageDigest": require_sha256(image_digest),
        }
    )


def full_media_dump_window(dump_date: str) -> tuple[str, str]:
    """Return the monthly dump window used for capture control."""

    iso_date = f"{dump_date[0:4]}-{dump_date[4:6]}-{dump_date[6:8]}"
    return f"{iso_date}T00:00:00Z", f"{iso_date}T23:59:59Z"


def plan_full_media_epoch_windows(
    profile: WikidataFullMediaProfile,
    *,
    dump_date: str,
    max_epochs_per_window: int = 1,
) -> tuple[CaptureWindowPlan, ...]:
    """Expose bounded epoch slices as explicit capture-window plans."""

    if max_epochs_per_window < 1:
        raise ValueError("max_epochs_per_window must be positive")
    keys = tuple(f"epoch:{index:05d}" for index in range(profile.estimated_epochs))
    window_start, window_end = full_media_dump_window(dump_date)
    return plan_bounded_capture_windows(
        source_product_id=WIKIDATA_SOURCE_PRODUCT_ID,
        window_start=window_start,
        window_end=window_end,
        item_keys=keys,
        max_items=max_epochs_per_window,
        watermark=dump_date,
    )


def build_full_media_capture_receipt(
    *,
    batch_object: ObjectRef,
    build_digest: str,
    config_digest: str,
    image_digest: str,
    policy_digest: str,
    dump_date: str,
    record_count: int,
) -> CaptureWindowReceipt:
    """Bind one immutable full-media batch to its monthly dump window."""

    window_start, window_end = full_media_dump_window(dump_date)
    status = (
        CaptureWindowStatus.EMPTY
        if record_count == 0
        else CaptureWindowStatus.COMMITTED
    )
    return build_capture_window_receipt(
        source_product_id=WIKIDATA_SOURCE_PRODUCT_ID,
        window_start=window_start,
        window_end=window_end,
        cursor=build_digest,
        watermark=dump_date,
        batch_object=batch_object,
        status=status,
        config_digest=config_digest,
        image_digest=image_digest,
        policy_digest=policy_digest,
    )


def _validate_immutable_reference(reference: ObjectRef, *, label: str) -> None:
    scheme = urlsplit(reference.uri).scheme
    if scheme not in {"file", "s3"}:
        raise ValueError(f"{label} must use file:// or s3://")
    if reference.size_bytes <= 0:
        raise ValueError(f"{label} must be non-empty")
    if scheme == "s3" and (reference.etag is None or reference.object_version is None):
        raise ValueError(f"{label} S3 reference requires ETag and VersionId")


class WikidataFullMediaBackfillCommit(V2ContractModel):
    """Final control object published only after every data/control child."""

    schema_version: str = "1.0"
    status: Literal["COMPLETE"] = "COMPLETE"
    commit_id: str
    build_digest: str
    config_digest: str
    image_digest: str
    dump: ObjectRef
    batch_manifest: ObjectRef
    record_set_manifest: ObjectRef
    source_watermark: ObjectRef
    window_receipt: ObjectRef
    profile: WikidataFullMediaProfile
    created_at: str

    @field_validator(
        "commit_id",
        "build_digest",
        "config_digest",
        "image_digest",
    )
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("created_at")
    @classmethod
    def validate_created_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @model_validator(mode="after")
    def validate_commit(self, info: ValidationInfo) -> Self:
        for label, reference in (
            ("dump", self.dump),
            ("batch manifest", self.batch_manifest),
            ("record-set manifest", self.record_set_manifest),
            ("source watermark", self.source_watermark),
            ("window receipt", self.window_receipt),
        ):
            _validate_immutable_reference(reference, label=label)
        expected_build = full_media_build_digest(
            dump=self.dump,
            config_digest=self.config_digest,
            image_digest=self.image_digest,
        )
        if self.build_digest != expected_build:
            raise ValueError("build_digest does not bind dump/config/image")
        if (
            self.profile.build_digest != self.build_digest
            or self.profile.config_digest != self.config_digest
            or self.profile.image_digest != self.image_digest
            or self.profile.dump != self.dump
        ):
            raise ValueError("profile does not bind the committed build")
        if not (info.context or {}).get("skip_identity"):
            expected = deterministic_key(
                "wikidata-full-media-backfill-commit-v1",
                _commit_identity(self),
            )
            if self.commit_id != expected:
                raise ValueError("commit_id does not match commit identity")
        return self


def _commit_identity(
    commit: WikidataFullMediaBackfillCommit,
) -> dict[str, Any]:
    return {
        "schemaVersion": commit.schema_version,
        "status": commit.status,
        "buildDigest": commit.build_digest,
        "configDigest": commit.config_digest,
        "imageDigest": commit.image_digest,
        "dump": commit.dump.model_dump(
            mode="json",
            by_alias=True,
            exclude_none=True,
        ),
        "batchManifest": commit.batch_manifest.model_dump(
            mode="json",
            by_alias=True,
            exclude_none=True,
        ),
        "recordSetManifest": commit.record_set_manifest.model_dump(
            mode="json",
            by_alias=True,
            exclude_none=True,
        ),
        "sourceWatermark": commit.source_watermark.model_dump(
            mode="json",
            by_alias=True,
            exclude_none=True,
        ),
        "windowReceipt": commit.window_receipt.model_dump(
            mode="json",
            by_alias=True,
            exclude_none=True,
        ),
        "profile": commit.profile.model_dump(
            mode="json",
            by_alias=True,
            exclude_none=True,
        ),
        "createdAt": commit.created_at,
    }


def build_wikidata_full_media_commit(
    **values: Any,
) -> WikidataFullMediaBackfillCommit:
    provisional = WikidataFullMediaBackfillCommit.model_validate(
        {**values, "commit_id": _ZERO_DIGEST},
        context={"skip_identity": True},
    )
    normalized = provisional.model_dump(mode="python")
    normalized["commit_id"] = deterministic_key(
        "wikidata-full-media-backfill-commit-v1",
        _commit_identity(provisional),
    )
    return WikidataFullMediaBackfillCommit.model_validate(normalized)
