"""Immutable, reference-only release boundary for Global Research Catalog."""

from __future__ import annotations

import hashlib
from enum import StrEnum
from typing import Any, Literal, Self

from pydantic import Field, ValidationInfo, field_validator, model_validator

from video_media_catalog.canonical import deterministic_key
from video_media_catalog.catalog_source_policy import load_catalog_source_policy
from video_media_catalog.gold import (
    GOLD_RELEASE_PLAN_MEDIA_TYPE,
    GoldReleasePlan,
)
from video_media_catalog.gold_ingest import (
    GOLD_RELEASE_COMMIT_MEDIA_TYPE,
    GoldReleaseCommit,
)
from video_media_catalog.gold_search_index import (
    GOLD_INDEX_BUILD_MANIFEST_MEDIA_TYPE,
    GOLD_INDEX_VALIDATION_RECEIPT_MEDIA_TYPE,
    MAPPING_DIGEST,
    RESEARCH_INDEX_PREFIX,
    RESEARCH_READ_ALIAS,
    GoldIndexBuildManifest,
    GoldIndexValidationReceipt,
)
from video_media_catalog.models import ObjectRef
from video_media_catalog.predicate_registry import (
    load_research_predicate_registry,
)
from video_media_catalog.reference_ontology import (
    load_global_reference_ontology,
)
from video_media_catalog.research_snapshot import SourceSnapshotPin
from video_media_catalog.v2_contracts import (
    V2ContractModel,
    require_rfc3339,
    require_sha256,
)

GLOBAL_REFERENCE_RELEASE_MEDIA_TYPE = (
    "application/vnd.video-media-catalog.global-reference-release.v1+json"
)
_ZERO_DIGEST = "sha256:" + ("0" * 64)


class ReferenceCapability(StrEnum):
    RESEARCH = "RESEARCH"
    DISPLAY = "DISPLAY"
    IDENTITY = "IDENTITY"


class ReferenceSlice(StrEnum):
    NEWS_JOURNALISM = "NEWS/JOURNALISM"
    LEARNING_INSTRUCTION = "LEARNING/INSTRUCTION"
    MUSIC = "MUSIC"


_REFERENCE_SNAPSHOT_COORDINATES = frozenset(
    {
        (
            "wikidata",
            "wikidata-json-dump",
            "wikidata-full-media-backfill",
            "DUMP_OBJECT",
            "wikidata-global-reference-entity",
        ),
        (
            "musicbrainz",
            "musicbrainz-core-dump",
            "musicbrainz-core-dump-snapshot",
            "DUMP_OBJECT",
            "musicbrainz-core-entity",
        ),
        (
            "wikidata",
            "wikidata-music-reference",
            "wikidata-music-reference-snapshot",
            "DUMP_OBJECT",
            "wikidata-global-reference-entity",
        ),
    }
)
_SLICE_SOURCE_PRODUCTS = {
    ReferenceSlice.NEWS_JOURNALISM: frozenset({"wikidata-json-dump"}),
    ReferenceSlice.LEARNING_INSTRUCTION: frozenset({"wikidata-json-dump"}),
    ReferenceSlice.MUSIC: frozenset(
        {
            "musicbrainz-core-dump",
            "wikidata-music-reference",
        }
    ),
}


class WorldZeroWriteAttestation(V2ContractModel):
    tenant_canonical_write_count: Literal[0] = 0
    world_materialization_count: Literal[0] = 0
    world_publication_count: Literal[0] = 0
    alias_activation_count: Literal[0] = 0

    @field_validator(
        "tenant_canonical_write_count",
        "world_materialization_count",
        "world_publication_count",
        "alias_activation_count",
        mode="before",
    )
    @classmethod
    def validate_exact_zero(cls, value: object) -> object:
        if type(value) is not int or value != 0:
            raise ValueError("World write attestation counters must be integer zero")
        return value


class ReferenceSliceSourcePin(V2ContractModel):
    reference_slice: ReferenceSlice
    source_snapshot_pin_id: str

    @field_validator("source_snapshot_pin_id")
    @classmethod
    def validate_pin_id(cls, value: str) -> str:
        return require_sha256(value, label="reference slice source snapshot")


class ValidatedConcreteReferenceIndex(V2ContractModel):
    build_id: str
    release_plan_id: str
    concrete_index: str
    read_alias: str
    mapping_digest: str
    config_digest: str
    document_count: int = Field(ge=0)
    build_manifest: ObjectRef
    validation_receipt: ObjectRef

    @field_validator(
        "release_plan_id",
        "mapping_digest",
        "config_digest",
    )
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("build_id")
    @classmethod
    def validate_build_id(cls, value: str) -> str:
        normalized = value.strip().lower()
        require_sha256(f"sha256:{normalized}", label="reference index build")
        return normalized

    @model_validator(mode="after")
    def validate_evidence(self) -> Self:
        _require_immutable_object(
            self.build_manifest,
            label="Gold index build manifest",
            media_type=GOLD_INDEX_BUILD_MANIFEST_MEDIA_TYPE,
        )
        _require_immutable_object(
            self.validation_receipt,
            label="Gold index validation receipt",
            media_type=GOLD_INDEX_VALIDATION_RECEIPT_MEDIA_TYPE,
        )
        if self.read_alias != RESEARCH_READ_ALIAS:
            raise ValueError("validated index must pin the research read alias")
        if self.mapping_digest != MAPPING_DIGEST:
            raise ValueError("validated index must pin the current OpenSearch mapping")
        if not self.concrete_index.startswith(f"{RESEARCH_INDEX_PREFIX}-"):
            raise ValueError(
                "validated index must use the research concrete index prefix"
            )
        return self


class GlobalReferenceReleaseManifest(V2ContractModel):
    schema_version: Literal["1.0"] = "1.0"
    release_id: str
    previous_release_id: str | None = None
    authority_scope: Literal["GLOBAL_REFERENCE_ONLY"] = "GLOBAL_REFERENCE_ONLY"
    tenant_world_identity_authority: Literal[False] = False
    capabilities: tuple[ReferenceCapability, ...]
    reference_slices: tuple[ReferenceSlice, ...]
    ontology_registry_id: str
    ontology_registry_version: str
    ontology_digest: str
    predicate_registry_id: str
    predicate_registry_version: str
    predicate_registry_digest: str
    source_policy_schema_version: Literal[
        "video.world.catalog-source-policy-registry.v1"
    ]
    source_policy_registry_version: str
    source_policy_digest: str
    source_snapshots: tuple[SourceSnapshotPin, ...]
    slice_source_pins: tuple[ReferenceSliceSourcePin, ...]
    gold_release_plan_id: str
    gold_release_plan: ObjectRef
    gold_release_commit: ObjectRef
    validated_index: ValidatedConcreteReferenceIndex
    world_zero_write_attestation: WorldZeroWriteAttestation
    created_at: str

    @field_validator(
        "release_id",
        "previous_release_id",
        "ontology_digest",
        "predicate_registry_digest",
        "source_policy_digest",
        "gold_release_plan_id",
    )
    @classmethod
    def validate_digest(cls, value: str | None) -> str | None:
        return None if value is None else require_sha256(value)

    @field_validator("capabilities")
    @classmethod
    def normalize_capabilities(
        cls,
        value: tuple[ReferenceCapability, ...],
    ) -> tuple[ReferenceCapability, ...]:
        normalized = tuple(sorted(set(value), key=str))
        if set(normalized) != set(ReferenceCapability):
            raise ValueError(
                "reference releases permit exactly RESEARCH, DISPLAY and IDENTITY"
            )
        return normalized

    @field_validator("reference_slices")
    @classmethod
    def normalize_slices(
        cls,
        value: tuple[ReferenceSlice, ...],
    ) -> tuple[ReferenceSlice, ...]:
        normalized = tuple(sorted(set(value), key=str))
        if set(normalized) != set(ReferenceSlice):
            raise ValueError(
                "global reference release must contain every reference slice"
            )
        return normalized

    @field_validator("source_snapshots")
    @classmethod
    def normalize_source_snapshots(
        cls,
        value: tuple[SourceSnapshotPin, ...],
    ) -> tuple[SourceSnapshotPin, ...]:
        normalized = tuple(sorted(value, key=lambda item: item.pin_id))
        if not normalized or len({item.pin_id for item in normalized}) != len(
            normalized
        ):
            raise ValueError("reference release source snapshots must be unique")
        for pin in normalized:
            coordinates = (
                pin.source_system_id,
                pin.source_product_id,
                pin.connector_id,
                pin.revision_kind.value,
                pin.schema_id,
            )
            if coordinates not in _REFERENCE_SNAPSHOT_COORDINATES:
                raise ValueError(
                    "reference slices require reviewed Wikidata or MusicBrainz "
                    "core captures"
                )
            for reference in pin.raw_objects:
                _require_immutable_object(
                    reference,
                    label="source snapshot object",
                )
        return normalized

    @field_validator("slice_source_pins")
    @classmethod
    def normalize_slice_source_pins(
        cls,
        value: tuple[ReferenceSliceSourcePin, ...],
    ) -> tuple[ReferenceSliceSourcePin, ...]:
        normalized = tuple(
            sorted(
                value,
                key=lambda item: (
                    str(item.reference_slice),
                    item.source_snapshot_pin_id,
                ),
            )
        )
        if {item.reference_slice for item in normalized} != set(ReferenceSlice):
            raise ValueError("each reference slice requires source snapshot pins")
        keys = {
            (item.reference_slice, item.source_snapshot_pin_id) for item in normalized
        }
        if len(keys) != len(normalized):
            raise ValueError("reference release contains duplicate slice source pins")
        counts = {
            reference_slice: sum(
                item.reference_slice == reference_slice for item in normalized
            )
            for reference_slice in ReferenceSlice
        }
        if (
            counts[ReferenceSlice.NEWS_JOURNALISM] != 1
            or counts[ReferenceSlice.LEARNING_INSTRUCTION] != 1
            or counts[ReferenceSlice.MUSIC] != 2
        ):
            raise ValueError(
                "News/Learning require one pin and Music requires two source pins"
            )
        return normalized

    @field_validator("created_at")
    @classmethod
    def validate_created_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @model_validator(mode="after")
    def validate_release(self, info: ValidationInfo) -> Self:
        _require_immutable_object(
            self.gold_release_plan,
            label="Gold release plan",
            media_type=GOLD_RELEASE_PLAN_MEDIA_TYPE,
        )
        _require_immutable_object(
            self.gold_release_commit,
            label="Gold release commit",
            media_type=GOLD_RELEASE_COMMIT_MEDIA_TYPE,
        )
        if self.previous_release_id == self.release_id:
            raise ValueError("reference release cannot supersede itself")
        if self.validated_index.release_plan_id != self.gold_release_plan_id:
            raise ValueError("validated index does not pin the Gold release plan")
        source_pins = {item.pin_id: item for item in self.source_snapshots}
        source_pin_ids = set(source_pins)
        if any(
            item.source_snapshot_pin_id not in source_pin_ids
            for item in self.slice_source_pins
        ):
            raise ValueError("reference slice points outside release source snapshots")
        for reference_slice in ReferenceSlice:
            products = {
                source_pins[item.source_snapshot_pin_id].source_product_id
                for item in self.slice_source_pins
                if item.reference_slice == reference_slice
            }
            if products != _SLICE_SOURCE_PRODUCTS[reference_slice]:
                raise ValueError(
                    f"{reference_slice.value} source snapshot products drifted"
                )
        if not (info.context or {}).get("skip_identity"):
            ontology = load_global_reference_ontology()
            predicates = load_research_predicate_registry()
            source_policy = load_catalog_source_policy()
            if (
                self.ontology_registry_id != ontology.registry_id
                or self.ontology_registry_version != ontology.registry_version
                or self.ontology_digest != ontology.digest
            ):
                raise ValueError("release ontology coordinates drifted from registry")
            if (
                self.predicate_registry_id != predicates.registry_id
                or self.predicate_registry_version != predicates.registry_version
                or self.predicate_registry_digest != predicates.digest
            ):
                raise ValueError("release predicate coordinates drifted from registry")
            if (
                self.source_policy_schema_version
                != source_policy.canonical_registry.schema_version
                or self.source_policy_registry_version
                != source_policy.canonical_registry.registry_version
                or self.source_policy_digest != source_policy.digest
            ):
                raise ValueError("release source-policy coordinates drifted from v1")
            expected = deterministic_key(
                "global-reference-release-v1",
                _release_identity(self),
            )
            if self.release_id != expected:
                raise ValueError("release_id does not match reference release content")
        return self


def _require_immutable_object(
    reference: ObjectRef,
    *,
    label: str,
    media_type: str | None = None,
) -> None:
    if reference.size_bytes <= 0:
        raise ValueError(f"{label} must be non-empty")
    if media_type is not None and reference.media_type != media_type:
        raise ValueError(f"{label} has an unexpected media type")
    if reference.uri.startswith("s3://") and (
        reference.etag is None or reference.object_version is None
    ):
        raise ValueError(f"{label} must pin S3 ETag and ObjectVersion")


def _release_identity(release: GlobalReferenceReleaseManifest) -> dict[str, Any]:
    payload = release.model_dump(
        mode="json",
        by_alias=True,
        exclude_none=True,
    )
    payload.pop("releaseId")
    return payload


def _require_model_reference(
    model: V2ContractModel,
    reference: ObjectRef,
    *,
    label: str,
    media_type: str,
) -> None:
    _require_immutable_object(reference, label=label, media_type=media_type)
    payload = model.json_bytes()
    if (
        reference.size_bytes != len(payload)
        or reference.checksum.value != hashlib.sha256(payload).hexdigest()
    ):
        raise ValueError(f"{label} ObjectRef does not bind the supplied model")


def build_global_reference_release_manifest(
    *,
    gold_plan: GoldReleasePlan,
    gold_plan_reference: ObjectRef,
    gold_commit: GoldReleaseCommit,
    gold_commit_reference: ObjectRef,
    index_build: GoldIndexBuildManifest,
    index_validation: GoldIndexValidationReceipt,
    index_build_manifest: ObjectRef,
    index_validation_receipt: ObjectRef,
    **values: Any,
) -> GlobalReferenceReleaseManifest:
    """Build a content-addressed release after exact index-evidence reconciliation."""

    ontology = load_global_reference_ontology()
    predicates = load_research_predicate_registry()
    source_policy = load_catalog_source_policy()
    _require_model_reference(
        gold_plan,
        gold_plan_reference,
        label="Gold release plan",
        media_type=GOLD_RELEASE_PLAN_MEDIA_TYPE,
    )
    _require_model_reference(
        gold_commit,
        gold_commit_reference,
        label="Gold release commit",
        media_type=GOLD_RELEASE_COMMIT_MEDIA_TYPE,
    )
    _require_model_reference(
        index_build,
        index_build_manifest,
        label="Gold index build manifest",
        media_type=GOLD_INDEX_BUILD_MANIFEST_MEDIA_TYPE,
    )
    _require_model_reference(
        index_validation,
        index_validation_receipt,
        label="Gold index validation receipt",
        media_type=GOLD_INDEX_VALIDATION_RECEIPT_MEDIA_TYPE,
    )
    source_snapshots = tuple(
        SourceSnapshotPin.model_validate(item)
        for item in (values.get("source_snapshots") or ())
    )
    slice_source_pins = tuple(
        ReferenceSliceSourcePin.model_validate(item)
        for item in (values.get("slice_source_pins") or ())
    )
    values = {
        **values,
        "source_snapshots": source_snapshots,
        "slice_source_pins": slice_source_pins,
    }
    release_pin_ids = {item.pin_id for item in source_snapshots}
    sliced_pin_ids = {item.source_snapshot_pin_id for item in slice_source_pins}
    gold_pin_ids_by_source = {
        source: set(pin_ids)
        for source, pin_ids in gold_plan.source_snapshot_pin_ids.items()
    }
    gold_pin_ids = {
        pin_id for pin_ids in gold_pin_ids_by_source.values() for pin_id in pin_ids
    }
    required_domains = {
        "NEWS",
        "JOURNALISM",
        "LEARNING",
        "INSTRUCTION",
        "MUSIC",
    }
    if (
        gold_commit.release_plan_id != gold_plan.release_plan_id
        or gold_commit.table_counts != gold_plan.expected_counts
        or index_build.build_mode != "FULL"
        or index_build.release_plan_id != gold_plan.release_plan_id
        or index_validation.release_plan_id != gold_plan.release_plan_id
        or index_build.release_commit != gold_commit_reference
        or index_build.table_snapshot_ids != gold_commit.table_snapshot_ids
        or index_validation.build_id != index_build.build_id
        or index_validation.mapping_digest != index_build.mapping_digest
        or index_validation.config_digest != index_build.config_digest
        or index_validation.index != index_build.index
        or index_validation.alias != index_build.alias
        or index_validation.document_count != index_build.document_count
        or index_validation.build_manifest != index_build_manifest
        or index_build.gold_entity_count
        != gold_commit.table_counts["community_gold_entity"]
        or index_build.successful_document_count != index_build.document_count
        or index_build.failed_document_count != 0
        or index_build.concrete_index_document_count != index_build.document_count
    ):
        raise ValueError("Gold index build and validation evidence do not reconcile")
    if (
        index_build.source_policy_digest != source_policy.digest
        or gold_plan.source_policy_digest != source_policy.digest
        or gold_commit.source_policy_digest != source_policy.digest
    ):
        raise ValueError("Gold evidence does not use canonical source-policy v1")
    if (
        gold_plan.reference_ontology_digest != ontology.digest
        or gold_plan.reference_predicate_registry_digest != predicates.digest
    ):
        raise ValueError("Gold plan does not bind the reference ontology registries")
    if not release_pin_ids or not release_pin_ids <= gold_pin_ids:
        raise ValueError("release source snapshots are absent from the Gold plan")
    if any(
        pin.pin_id not in gold_pin_ids_by_source.get(pin.source_product_id, set())
        for pin in source_snapshots
    ):
        raise ValueError("release source snapshot is bound to the wrong Gold source")
    if not sliced_pin_ids or sliced_pin_ids != release_pin_ids:
        raise ValueError("reference slice source pins are not release-bound")
    if any(
        gold_plan.domain_coverage.get(domain, 0) <= 0 for domain in required_domains
    ):
        raise ValueError("Gold plan does not cover every reference slice")

    manifest_values = {
        **values,
        "release_id": _ZERO_DIGEST,
        "capabilities": tuple(ReferenceCapability),
        "reference_slices": tuple(ReferenceSlice),
        "ontology_registry_id": ontology.registry_id,
        "ontology_registry_version": ontology.registry_version,
        "ontology_digest": ontology.digest,
        "predicate_registry_id": predicates.registry_id,
        "predicate_registry_version": predicates.registry_version,
        "predicate_registry_digest": predicates.digest,
        "source_policy_schema_version": (
            source_policy.canonical_registry.schema_version
        ),
        "source_policy_registry_version": (
            source_policy.canonical_registry.registry_version
        ),
        "source_policy_digest": source_policy.digest,
        "gold_release_plan_id": gold_plan.release_plan_id,
        "gold_release_plan": gold_plan_reference,
        "gold_release_commit": gold_commit_reference,
        "validated_index": ValidatedConcreteReferenceIndex(
            build_id=index_build.build_id,
            release_plan_id=index_build.release_plan_id,
            concrete_index=index_build.index,
            read_alias=index_build.alias,
            mapping_digest=index_build.mapping_digest,
            config_digest=index_build.config_digest,
            document_count=index_build.document_count,
            build_manifest=index_build_manifest,
            validation_receipt=index_validation_receipt,
        ),
        "world_zero_write_attestation": WorldZeroWriteAttestation(),
    }
    provisional = GlobalReferenceReleaseManifest.model_validate(
        manifest_values,
        context={"skip_identity": True},
    )
    normalized = provisional.model_dump(mode="python")
    normalized["release_id"] = deterministic_key(
        "global-reference-release-v1",
        _release_identity(provisional),
    )
    return GlobalReferenceReleaseManifest.model_validate(normalized)
