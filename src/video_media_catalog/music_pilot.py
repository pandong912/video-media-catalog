"""Governed 20-asset Music Pilot cohort, index profile, and match baseline."""

from __future__ import annotations

import json
import re
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal, Self

from pydantic import Field, ValidationInfo, field_validator, model_validator

from video_media_catalog.canonical import deterministic_key
from video_media_catalog.catalog_source_policy import load_catalog_source_policy
from video_media_catalog.gold import research_policy
from video_media_catalog.gold_match_parameters import (
    catalog_match_algorithm_identity,
)
from video_media_catalog.gold_search_index import (
    MAPPING_DIGEST,
    PROJECTION_VERSION,
    RESEARCH_INDEX_PREFIX,
    gold_index_name,
)
from video_media_catalog.identity_v2 import IdentityConflict, build_identity_conflict
from video_media_catalog.music_sources import WIKIDATA_MUSIC_SOURCE_PRODUCT_ID
from video_media_catalog.predicate_registry import (
    load_research_predicate_registry,
)
from video_media_catalog.reference_fixture import MaterializedReferenceFixture
from video_media_catalog.reference_ontology import (
    load_global_reference_ontology,
)
from video_media_catalog.v2_contracts import (
    V2ContractModel,
    digest_identity,
    require_rfc3339,
    require_sha256,
)

_QID = re.compile(r"Q[1-9][0-9]*")
_MBID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
_EXPECTED_CASE_COUNTS = {
    "EXACT_RECORDING": 8,
    "LIVE_WORK_PARENT": 8,
    "CONFLICTING_RECORDINGS": 1,
    "REMIX_OR_MEDIA_VARIANT": 1,
    "NO_AUTHORITY_COORDINATE": 2,
}


class MusicPilotDecision(StrEnum):
    EXACT = "EXACT"
    REVIEW_REQUIRED = "REVIEW_REQUIRED"
    ABSTAIN = "ABSTAIN"


class MusicPilotCoordinateDisposition(StrEnum):
    EXACT_RECORDING = "EXACT_RECORDING"
    WORK_PARENT_ONLY = "WORK_PARENT_ONLY"
    CONFLICT_ONLY = "CONFLICT_ONLY"
    VARIANT_ABSTAIN_ONLY = "VARIANT_ABSTAIN_ONLY"


class MusicCoordinateEvidence(V2ContractModel):
    kind: str
    property: str | None = None
    statement_id: str | None = None
    authority_revision: str
    title: str | None = None

    @field_validator("kind", "property", "statement_id", "authority_revision", "title")
    @classmethod
    def validate_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized or len(normalized) > 4096:
            raise ValueError("Music coordinate evidence text must be bounded")
        return normalized


class MusicCoordinateReview(V2ContractModel):
    mapping_kind: Literal[
        "AUTHORITY_DECLARED",
        "RESEARCH_CANDIDATE_SAME_WORK_PARENT",
        "RESEARCH_CANDIDATE_SAME_RECORDING",
    ]
    review_status: Literal["NOT_REQUIRED", "PENDING"]
    reviewed_at: str | None = None
    reviewed_by: str | None = None
    evidence: tuple[MusicCoordinateEvidence, ...] = Field(min_length=1)

    @field_validator("reviewed_at")
    @classmethod
    def validate_reviewed_at(cls, value: str | None) -> str | None:
        return None if value is None else require_rfc3339(value)

    @field_validator("reviewed_by")
    @classmethod
    def validate_reviewed_by(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized.startswith("urn:") or len(normalized) > 2048:
            raise ValueError("Music coordinate reviewer must be an auditable URN")
        return normalized

    @model_validator(mode="after")
    def validate_review(self) -> Self:
        pending = self.mapping_kind.startswith("RESEARCH_CANDIDATE_")
        if pending != (self.review_status == "PENDING"):
            raise ValueError("Music coordinate review status is inconsistent")
        if self.reviewed_at is not None or self.reviewed_by is not None:
            raise ValueError("unreviewed Music coordinates cannot claim a reviewer")
        return self


class MusicAuthorityCoordinate(V2ContractModel):
    wikidata_qid: str
    musicbrainz_mbid: str
    entity_type: Literal["MUSIC_RECORDING", "MUSICAL_WORK"]
    coordinate_review: MusicCoordinateReview

    @field_validator("wikidata_qid")
    @classmethod
    def validate_qid(cls, value: str) -> str:
        normalized = value.strip().upper()
        if _QID.fullmatch(normalized) is None:
            raise ValueError("Music Pilot coordinate requires a Wikidata QID")
        return normalized

    @field_validator("musicbrainz_mbid")
    @classmethod
    def validate_mbid(cls, value: str) -> str:
        normalized = value.strip().lower()
        if _MBID.fullmatch(normalized) is None:
            raise ValueError("Music Pilot coordinate requires a MusicBrainz ID")
        return normalized

    @property
    def coordinate_key(self) -> str:
        return "|".join((self.entity_type, self.wikidata_qid, self.musicbrainz_mbid))


class MusicPilotConflictEvidence(V2ContractModel):
    wikidata_shared_media_object: str

    @field_validator("wikidata_shared_media_object")
    @classmethod
    def validate_media_object(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized or len(normalized) > 2048:
            raise ValueError("Music Pilot shared media object must be bounded")
        return normalized


class MusicPilotRecordingRelationEvidence(V2ContractModel):
    target_id: str
    title: str
    relation_type: Literal["remix"]
    direction: Literal["backward", "forward"]

    @field_validator("target_id")
    @classmethod
    def validate_target_id(cls, value: str) -> str:
        normalized = value.strip().lower()
        if _MBID.fullmatch(normalized) is None:
            raise ValueError("Music Pilot variant target requires an MBID")
        return normalized

    @field_validator("title")
    @classmethod
    def validate_title(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized or len(normalized) > 2048:
            raise ValueError("Music Pilot variant relation title must be bounded")
        return normalized


class MusicPilotVariantEvidence(V2ContractModel):
    wikidata_media_object: str
    musicbrainz_recording_relation: MusicPilotRecordingRelationEvidence

    @field_validator("wikidata_media_object")
    @classmethod
    def validate_media_object(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized or len(normalized) > 2048:
            raise ValueError("Music Pilot variant media object must be bounded")
        return normalized


class MusicPilotFeatures(V2ContractModel):
    title: str
    artists: tuple[str, ...] = ()
    duration_ms: int | None = Field(default=None, gt=0)
    performance_type: Literal["RECORDED", "LIVE", "VARIANT", "UNKNOWN"]
    authority_coordinates: tuple[MusicAuthorityCoordinate, ...] = Field(
        default=(),
        max_length=2,
    )
    conflict_evidence: MusicPilotConflictEvidence | None = None
    variant_evidence: MusicPilotVariantEvidence | None = None

    @field_validator("title")
    @classmethod
    def validate_title(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized or len(normalized) > 2048:
            raise ValueError("Music Pilot title must be non-empty and bounded")
        return normalized

    @field_validator("artists")
    @classmethod
    def normalize_artists(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(sorted({item.strip() for item in value if item.strip()}))
        if any(len(item) > 2048 for item in normalized):
            raise ValueError("Music Pilot artist is too long")
        return normalized


class MusicPilotExpected(V2ContractModel):
    decision: MusicPilotDecision
    reason: str
    candidate_entity_type: str | None = None
    candidate_namespace: str | None = None
    candidate_id: str | None = None
    forbid_entity_type: str | None = None
    forbid_decision: str | None = None

    @field_validator(
        "reason",
        "candidate_entity_type",
        "candidate_namespace",
        "candidate_id",
        "forbid_entity_type",
        "forbid_decision",
    )
    @classmethod
    def validate_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized or len(normalized) > 2048:
            raise ValueError("Music Pilot expected value must be bounded")
        return normalized


class MusicPilotAsset(V2ContractModel):
    asset_id: str
    case: Literal[
        "EXACT_RECORDING",
        "LIVE_WORK_PARENT",
        "CONFLICTING_RECORDINGS",
        "REMIX_OR_MEDIA_VARIANT",
        "NO_AUTHORITY_COORDINATE",
    ]
    feature_provenance: str | None = None
    features: MusicPilotFeatures
    expected: MusicPilotExpected

    @field_validator("asset_id")
    @classmethod
    def validate_asset_id(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized.startswith("music-pilot-") or len(normalized) > 128:
            raise ValueError("Music Pilot asset ID is invalid")
        return normalized

    @model_validator(mode="after")
    def validate_case(self) -> Self:
        coordinates = self.features.authority_coordinates
        if self.case == "EXACT_RECORDING":
            valid = (
                len(coordinates) == 1
                and coordinates[0].entity_type == "MUSIC_RECORDING"
                and self.features.performance_type == "RECORDED"
                and self.expected.decision == MusicPilotDecision.EXACT
            )
        elif self.case == "LIVE_WORK_PARENT":
            valid = (
                len(coordinates) == 1
                and coordinates[0].entity_type == "MUSICAL_WORK"
                and self.features.performance_type == "LIVE"
                and self.expected.decision == MusicPilotDecision.REVIEW_REQUIRED
                and self.expected.forbid_entity_type == "MUSIC_RECORDING"
            )
        elif self.case == "CONFLICTING_RECORDINGS":
            evidence = self.features.conflict_evidence
            valid = (
                len(coordinates) == 2
                and len({item.coordinate_key for item in coordinates}) == 2
                and len({item.wikidata_qid for item in coordinates}) == 2
                and len({item.musicbrainz_mbid for item in coordinates}) == 2
                and self.features.performance_type == "RECORDED"
                and evidence is not None
                and self.features.title == evidence.wikidata_shared_media_object
                and self.expected.decision == MusicPilotDecision.ABSTAIN
            )
        elif self.case == "REMIX_OR_MEDIA_VARIANT":
            evidence = self.features.variant_evidence
            valid = (
                len(coordinates) == 1
                and self.features.performance_type == "VARIANT"
                and evidence is not None
                and self.features.title == evidence.wikidata_media_object
                and (
                    evidence.musicbrainz_recording_relation.target_id
                    != coordinates[0].musicbrainz_mbid
                )
                and self.expected.decision == MusicPilotDecision.ABSTAIN
                and self.expected.forbid_decision == MusicPilotDecision.EXACT.value
            )
        else:
            valid = (
                not coordinates
                and self.features.performance_type == "UNKNOWN"
                and self.feature_provenance == "NEGATIVE_CONTROL"
                and self.expected.decision == MusicPilotDecision.ABSTAIN
            )
        if not valid:
            raise ValueError(f"Music Pilot {self.case} fixture is inconsistent")
        return self


class MusicPilotSourceFixtureRef(V2ContractModel):
    path: str
    source_revision: str
    raw_projection_digest: str

    @field_validator("source_revision", "raw_projection_digest")
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("path")
    @classmethod
    def validate_path(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized.endswith(".json") or "/" in normalized or "\\" in normalized:
            raise ValueError("Music Pilot source fixture path must be a local filename")
        return normalized


class MusicPilotCohort(V2ContractModel):
    schema_version: Literal["video.media-catalog.music-pilot.v1"]
    cohort_id: Literal["music-reference-gold-match-pilot-2026-10-17"]
    captured_at: str
    authority_scope: Literal["GLOBAL_REFERENCE_ONLY"]
    tenant_world_identity_authority: Literal[False]
    source_policy_version: Literal["1.4.0"]
    source_fixtures: dict[str, MusicPilotSourceFixtureRef]
    config_digest: str
    assets: tuple[MusicPilotAsset, ...]
    cohort_digest: str

    @field_validator("captured_at")
    @classmethod
    def validate_captured_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @field_validator("config_digest", "cohort_digest")
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @model_validator(mode="after")
    def validate_cohort(self) -> Self:
        if set(self.source_fixtures) != {"musicbrainz", "wikidata"}:
            raise ValueError("Music Pilot requires MusicBrainz and Wikidata fixtures")
        if len(self.assets) != 20 or len(
            {item.asset_id for item in self.assets}
        ) != len(self.assets):
            raise ValueError("Music Pilot requires 20 unique assets")
        counts = {
            case: sum(asset.case == case for asset in self.assets)
            for case in _EXPECTED_CASE_COUNTS
        }
        if counts != _EXPECTED_CASE_COUNTS:
            raise ValueError("Music Pilot case distribution changed")
        material = [
            asset.model_dump(mode="json", by_alias=True, exclude_none=True)
            for asset in self.assets
        ]
        if self.cohort_digest != digest_identity(material):
            raise ValueError("Music Pilot cohort digest does not bind its assets")
        return self

    @property
    def authority_coordinates(self) -> tuple[MusicAuthorityCoordinate, ...]:
        by_key = {
            coordinate.coordinate_key: coordinate
            for asset in self.assets
            for coordinate in asset.features.authority_coordinates
        }
        return tuple(by_key[key] for key in sorted(by_key))

    @property
    def authority_coordinate_digest(self) -> str:
        return digest_identity(
            [
                item.model_dump(mode="json", by_alias=True, exclude_none=True)
                for item in self.authority_coordinates
            ]
        )


class MusicPilotProfileCoordinate(V2ContractModel):
    coordinate_key: str
    wikidata_qid: str
    musicbrainz_mbid: str
    entity_type: Literal["MUSIC_RECORDING", "MUSICAL_WORK"]
    disposition: MusicPilotCoordinateDisposition
    mapping_kind: Literal[
        "AUTHORITY_DECLARED",
        "RESEARCH_CANDIDATE_SAME_WORK_PARENT",
        "RESEARCH_CANDIDATE_SAME_RECORDING",
    ]
    review_status: Literal["NOT_REQUIRED", "PENDING"]
    review_digest: str

    @field_validator("wikidata_qid")
    @classmethod
    def validate_qid(cls, value: str) -> str:
        normalized = value.strip().upper()
        if _QID.fullmatch(normalized) is None:
            raise ValueError("Music Pilot profile coordinate requires a QID")
        return normalized

    @field_validator("musicbrainz_mbid")
    @classmethod
    def validate_mbid(cls, value: str) -> str:
        normalized = value.strip().lower()
        if _MBID.fullmatch(normalized) is None:
            raise ValueError("Music Pilot profile coordinate requires an MBID")
        return normalized

    @field_validator("review_digest")
    @classmethod
    def validate_review_digest(cls, value: str) -> str:
        return require_sha256(value, label="Music Pilot coordinate review")

    @model_validator(mode="after")
    def validate_coordinate(self) -> Self:
        expected = "|".join(
            (self.entity_type, self.wikidata_qid, self.musicbrainz_mbid)
        )
        if self.coordinate_key != expected:
            raise ValueError("Music Pilot profile coordinate key is inconsistent")
        if (self.disposition == MusicPilotCoordinateDisposition.WORK_PARENT_ONLY) != (
            self.entity_type == "MUSICAL_WORK"
        ):
            raise ValueError("Music Pilot work-parent disposition is inconsistent")
        if self.mapping_kind.startswith("RESEARCH_CANDIDATE_") != (
            self.review_status == "PENDING"
        ):
            raise ValueError("Music Pilot profile review status is inconsistent")
        return self


class MusicPilotMatchProfile(V2ContractModel):
    schema_version: Literal["1.0"] = "1.0"
    profile_id: str
    cohort_id: str
    cohort_digest: str
    authority_coordinate_digest: str
    coordinate_keys: tuple[str, ...]
    coordinates: tuple[MusicPilotProfileCoordinate, ...]
    source_snapshot_pin_ids: dict[str, str]
    source_revisions: dict[str, str]
    source_fixture_digests: dict[str, str]
    capture_image_digests: dict[str, str]
    source_policy_registry_version: str
    source_policy_digest: str
    ontology_registry_version: str
    ontology_digest: str
    predicate_registry_version: str
    predicate_digest: str
    gold_policy_version: str
    gold_policy_digest: str
    mapping_version: str
    mapping_digest: str
    algorithm_id: str
    algorithm_schema_version: str
    algorithm_digest: str
    concrete_index: str
    coordinate_count: int = Field(gt=0)
    validated_index_manifest_required: Literal[True] = True
    production_activation_allowed: Literal[False] = False
    cutover_allowed: Literal[False] = False
    created_at: str

    @field_validator(
        "profile_id",
        "cohort_digest",
        "authority_coordinate_digest",
        "source_policy_digest",
        "ontology_digest",
        "predicate_digest",
        "gold_policy_digest",
        "mapping_digest",
        "algorithm_digest",
    )
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator(
        "source_snapshot_pin_ids",
        "source_revisions",
        "source_fixture_digests",
        "capture_image_digests",
    )
    @classmethod
    def validate_source_coordinates(cls, value: dict[str, str]) -> dict[str, str]:
        if set(value) != {
            "musicbrainz-core-dump",
            WIKIDATA_MUSIC_SOURCE_PRODUCT_ID,
        }:
            raise ValueError("Music Pilot profile requires both authority sources")
        return dict(sorted(value.items()))

    @field_validator(
        "source_snapshot_pin_ids",
        "source_fixture_digests",
        "capture_image_digests",
    )
    @classmethod
    def validate_source_digests(cls, value: dict[str, str]) -> dict[str, str]:
        return {
            key: require_sha256(item, label="Music Pilot source digest")
            for key, item in value.items()
        }

    @field_validator("source_revisions")
    @classmethod
    def validate_revisions(cls, value: dict[str, str]) -> dict[str, str]:
        return {
            key: require_sha256(item, label="Music Pilot source revision")
            for key, item in value.items()
        }

    @field_validator("created_at")
    @classmethod
    def validate_created_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @model_validator(mode="after")
    def validate_identity(self, info: ValidationInfo) -> Self:
        if (
            tuple(sorted(set(self.coordinate_keys))) != self.coordinate_keys
            or self.coordinate_count != len(self.coordinate_keys)
            or tuple(item.coordinate_key for item in self.coordinates)
            != self.coordinate_keys
        ):
            raise ValueError("Music Pilot profile coordinate index is inconsistent")
        source_policy = load_catalog_source_policy()
        ontology = load_global_reference_ontology()
        predicates = load_research_predicate_registry()
        gold_policy = research_policy()
        if (
            self.source_policy_registry_version
            != source_policy.canonical_registry.registry_version
            or self.source_policy_digest != source_policy.digest
        ):
            raise ValueError("Music Pilot profile source policy is not canonical")
        if (
            self.ontology_registry_version != ontology.registry_version
            or self.ontology_digest != ontology.digest
        ):
            raise ValueError("Music Pilot profile ontology is not canonical")
        if (
            self.predicate_registry_version != predicates.registry_version
            or self.predicate_digest != predicates.digest
        ):
            raise ValueError("Music Pilot profile predicate registry is not canonical")
        if (
            self.gold_policy_version != gold_policy.policy_version
            or self.gold_policy_digest != gold_policy.digest
        ):
            raise ValueError("Music Pilot profile Gold policy is not canonical")
        if (
            self.mapping_version != PROJECTION_VERSION
            or self.mapping_digest != MAPPING_DIGEST
        ):
            raise ValueError("Music Pilot profile search mapping is not canonical")
        if not (info.context or {}).get("skip_identity"):
            expected = deterministic_key(
                "music-pilot-match-profile-v1",
                _profile_identity(self),
            )
            if self.profile_id != expected:
                raise ValueError("Music Pilot profile ID does not match its content")
        return self


def _profile_identity(profile: MusicPilotMatchProfile) -> dict[str, Any]:
    value = profile.model_dump(mode="json", by_alias=True, exclude_none=True)
    value.pop("profileId")
    return value


def load_music_pilot_cohort(path: str | Path) -> MusicPilotCohort:
    return MusicPilotCohort.model_validate_json(Path(path).read_text(encoding="utf-8"))


def load_music_pilot_match_profile(path: str | Path) -> MusicPilotMatchProfile:
    return MusicPilotMatchProfile.model_validate_json(
        Path(path).read_text(encoding="utf-8")
    )


def _require_fixture_binding(
    *,
    cohort: MusicPilotCohort,
    name: str,
    source_product_id: str,
    fixture: MaterializedReferenceFixture,
) -> None:
    expected = cohort.source_fixtures[name]
    raw_digest = "sha256:" + fixture.fixture.raw_object.checksum.value
    if (
        fixture.fixture.source_product_id != source_product_id
        or fixture.fixture.source_revision != expected.source_revision
        or raw_digest != expected.raw_projection_digest
        or fixture.fixture.config_digest != cohort.config_digest
        or fixture.fixture.captured_at != cohort.captured_at
    ):
        raise ValueError(f"Music Pilot {name} fixture differs from cohort pin")


def _require_authority_coordinate_facts(
    *,
    cohort: MusicPilotCohort,
    musicbrainz: MaterializedReferenceFixture,
    wikidata: MaterializedReferenceFixture,
) -> None:
    def record_title(payload: dict[str, Any]) -> str:
        name = payload.get("name")
        if isinstance(name, str) and name.strip():
            return name.strip()
        labels = payload.get("labels")
        if isinstance(labels, dict):
            for language in ("en", "ru"):
                label = labels.get(language)
                value = label.get("value") if isinstance(label, dict) else None
                if isinstance(value, str) and value.strip():
                    return value.strip()
        return str(payload["id"])

    musicbrainz_by_id = {
        item.source_node.source_id: item for item in musicbrainz.mapped
    }
    wikidata_by_id = {item.source_node.source_id: item for item in wikidata.mapped}
    musicbrainz_payloads = {
        item.source_record_id: json.loads(item.payload_json or "{}")
        for item in musicbrainz.captured.envelopes
    }
    wikidata_payloads = {
        item.source_record_id: json.loads(item.payload_json or "{}")
        for item in wikidata.captured.envelopes
    }
    for coordinate in cohort.authority_coordinates:
        musicbrainz_record = musicbrainz_by_id.get(coordinate.musicbrainz_mbid)
        wikidata_record = wikidata_by_id.get(coordinate.wikidata_qid)
        if (
            musicbrainz_record is None
            or wikidata_record is None
            or musicbrainz_record.source_node.referent_kind != coordinate.entity_type
            or wikidata_record.source_node.referent_kind != coordinate.entity_type
        ):
            raise ValueError(
                "Music Pilot coordinate is absent from pinned authority facts"
            )
        namespace = (
            "musicbrainz-recording"
            if coordinate.entity_type == "MUSIC_RECORDING"
            else "musicbrainz-work"
        )
        direct_cross_id = any(
            item.namespace_id == namespace and item.value == coordinate.musicbrainz_mbid
            for item in wikidata_record.identifier_assertions
        )
        authority_declared = (
            coordinate.coordinate_review.mapping_kind == "AUTHORITY_DECLARED"
        )
        if direct_cross_id != authority_declared:
            raise ValueError(
                "Music Pilot coordinate review differs from authority cross-ID"
            )
        property_id = "P4404" if coordinate.entity_type == "MUSIC_RECORDING" else "P435"
        statements = (
            wikidata_payloads[coordinate.wikidata_qid]
            .get("claims", {})
            .get(property_id, [])
        )
        direct_statement = next(
            (
                statement
                for statement in statements
                if (
                    statement.get("mainsnak", {}).get("datavalue", {}).get("value")
                    == coordinate.musicbrainz_mbid
                )
            ),
            None,
        )
        evidence = coordinate.coordinate_review.evidence
        if authority_declared:
            if (
                direct_statement is None
                or len(evidence) != 1
                or evidence[0].kind != "WIKIDATA_DIRECT_CROSS_ID"
                or evidence[0].property != property_id
                or evidence[0].statement_id != direct_statement.get("id")
                or evidence[0].authority_revision
                != wikidata_payloads[coordinate.wikidata_qid]["authorityRevision"]
            ):
                raise ValueError(
                    "Music Pilot direct coordinate evidence is not source-bound"
                )
        else:
            by_kind = {item.kind: item for item in evidence}
            wikidata_evidence = by_kind.get("WIKIDATA_REVISION_AND_LABEL")
            musicbrainz_evidence = by_kind.get("MUSICBRAINZ_CAPTURE_AND_TITLE")
            if (
                direct_statement is not None
                or len(evidence) != 2
                or set(by_kind)
                != {
                    "WIKIDATA_REVISION_AND_LABEL",
                    "MUSICBRAINZ_CAPTURE_AND_TITLE",
                }
                or wikidata_evidence is None
                or musicbrainz_evidence is None
                or wikidata_evidence.authority_revision
                != wikidata_payloads[coordinate.wikidata_qid]["authorityRevision"]
                or musicbrainz_evidence.authority_revision
                != musicbrainz_payloads[coordinate.musicbrainz_mbid][
                    "authorityRevision"
                ]
                or wikidata_evidence.title
                != record_title(wikidata_payloads[coordinate.wikidata_qid])
                or musicbrainz_evidence.title
                != record_title(musicbrainz_payloads[coordinate.musicbrainz_mbid])
            ):
                raise ValueError(
                    "Music Pilot reviewed coordinate evidence is not source-bound"
                )
    for asset in cohort.assets:
        if asset.case == "CONFLICTING_RECORDINGS":
            evidence = asset.features.conflict_evidence
            assert evidence is not None
            shared_object = evidence.wikidata_shared_media_object
            for coordinate in asset.features.authority_coordinates:
                mapped = wikidata_by_id[coordinate.wikidata_qid]
                if not any(
                    assertion.predicate == "media_object"
                    and json.loads(assertion.value_json) == shared_object
                    and assertion.qualifiers.get("authorityProperty") == "P51"
                    for assertion in mapped.field_assertions
                ):
                    raise ValueError(
                        "Music Pilot conflict does not share one pinned media object"
                    )
            continue
        if asset.case != "REMIX_OR_MEDIA_VARIANT":
            continue
        coordinate = asset.features.authority_coordinates[0]
        evidence = asset.features.variant_evidence
        assert evidence is not None
        relation = evidence.musicbrainz_recording_relation
        wikidata_record = wikidata_by_id[coordinate.wikidata_qid]
        musicbrainz_record = musicbrainz_by_id[coordinate.musicbrainz_mbid]
        has_media_object = any(
            assertion.predicate == "media_object"
            and json.loads(assertion.value_json) == evidence.wikidata_media_object
            and assertion.qualifiers.get("authorityProperty") == "P51"
            for assertion in wikidata_record.field_assertions
        )
        has_remix_relation = any(
            assertion.predicate == "media_variant"
            and json.loads(assertion.value_json) == relation.relation_type
            and assertion.qualifiers.get("authorityRelation") == relation.relation_type
            and assertion.qualifiers.get("direction") == relation.direction
            and assertion.qualifiers.get("targetId") == relation.target_id
            and assertion.qualifiers.get("targetTitle") == relation.title
            for assertion in musicbrainz_record.field_assertions
        )
        if not has_media_object or not has_remix_relation:
            raise ValueError("Music Pilot variant lacks pinned authority evidence")


def build_music_pilot_match_profile(
    *,
    cohort: MusicPilotCohort,
    musicbrainz: MaterializedReferenceFixture,
    wikidata: MaterializedReferenceFixture,
    algorithm_contract: dict[str, Any],
) -> MusicPilotMatchProfile:
    """Build a deterministic concrete-index profile without activating an alias."""

    _require_fixture_binding(
        cohort=cohort,
        name="musicbrainz",
        source_product_id="musicbrainz-core-dump",
        fixture=musicbrainz,
    )
    _require_fixture_binding(
        cohort=cohort,
        name="wikidata",
        source_product_id=WIKIDATA_MUSIC_SOURCE_PRODUCT_ID,
        fixture=wikidata,
    )
    _require_authority_coordinate_facts(
        cohort=cohort,
        musicbrainz=musicbrainz,
        wikidata=wikidata,
    )
    source_policy = load_catalog_source_policy()
    ontology = load_global_reference_ontology()
    predicates = load_research_predicate_registry()
    gold_policy = research_policy()
    # Preserve the currently published cross-repository algorithm coordinate.
    # Local Pilot dispositions are stricter until Compute freezes abstention.
    algorithm_id, algorithm_schema, algorithm_digest = catalog_match_algorithm_identity(
        algorithm_contract
    )
    disposition_by_case = {
        "EXACT_RECORDING": MusicPilotCoordinateDisposition.EXACT_RECORDING,
        "LIVE_WORK_PARENT": MusicPilotCoordinateDisposition.WORK_PARENT_ONLY,
        "CONFLICTING_RECORDINGS": MusicPilotCoordinateDisposition.CONFLICT_ONLY,
        "REMIX_OR_MEDIA_VARIANT": (
            MusicPilotCoordinateDisposition.VARIANT_ABSTAIN_ONLY
        ),
    }
    profile_coordinates_by_key: dict[str, MusicPilotProfileCoordinate] = {}
    for asset in cohort.assets:
        disposition = disposition_by_case.get(asset.case)
        if disposition is None:
            continue
        for coordinate in asset.features.authority_coordinates:
            review = coordinate.coordinate_review
            profile_coordinate = MusicPilotProfileCoordinate(
                coordinate_key=coordinate.coordinate_key,
                wikidata_qid=coordinate.wikidata_qid,
                musicbrainz_mbid=coordinate.musicbrainz_mbid,
                entity_type=coordinate.entity_type,
                disposition=disposition,
                mapping_kind=review.mapping_kind,
                review_status=review.review_status,
                review_digest=digest_identity(
                    review.model_dump(
                        mode="json",
                        by_alias=True,
                        exclude_none=True,
                    )
                ),
            )
            existing = profile_coordinates_by_key.setdefault(
                coordinate.coordinate_key,
                profile_coordinate,
            )
            if existing != profile_coordinate:
                raise ValueError(
                    "Music Pilot coordinate has conflicting profile dispositions"
                )
    profile_coordinates = tuple(
        profile_coordinates_by_key[key] for key in sorted(profile_coordinates_by_key)
    )
    coordinate_keys = tuple(item.coordinate_key for item in profile_coordinates)
    build_material = {
        "cohortDigest": cohort.cohort_digest,
        "authorityCoordinateDigest": cohort.authority_coordinate_digest,
        "sourcePins": {
            "musicbrainz-core-dump": musicbrainz.captured.pin.pin_id,
            WIKIDATA_MUSIC_SOURCE_PRODUCT_ID: wikidata.captured.pin.pin_id,
        },
        "sourceFixtureDigests": {
            "musicbrainz-core-dump": musicbrainz.fixture.digest,
            WIKIDATA_MUSIC_SOURCE_PRODUCT_ID: wikidata.fixture.digest,
        },
        "captureImageDigests": {
            "musicbrainz-core-dump": musicbrainz.fixture.image_digest,
            WIKIDATA_MUSIC_SOURCE_PRODUCT_ID: wikidata.fixture.image_digest,
        },
        "sourcePolicyDigest": source_policy.digest,
        "ontologyDigest": ontology.digest,
        "predicateDigest": predicates.digest,
        "goldPolicyDigest": gold_policy.digest,
        "mappingDigest": MAPPING_DIGEST,
        "algorithmDigest": algorithm_digest,
    }
    build_id = deterministic_key(
        "music-pilot-concrete-index-v1",
        build_material,
    ).removeprefix("sha256:")
    values = {
        "profile_id": "sha256:" + ("0" * 64),
        "cohort_id": cohort.cohort_id,
        "cohort_digest": cohort.cohort_digest,
        "authority_coordinate_digest": cohort.authority_coordinate_digest,
        "coordinate_keys": coordinate_keys,
        "coordinates": profile_coordinates,
        "source_snapshot_pin_ids": build_material["sourcePins"],
        "source_revisions": {
            "musicbrainz-core-dump": musicbrainz.fixture.source_revision,
            WIKIDATA_MUSIC_SOURCE_PRODUCT_ID: wikidata.fixture.source_revision,
        },
        "source_fixture_digests": build_material["sourceFixtureDigests"],
        "capture_image_digests": build_material["captureImageDigests"],
        "source_policy_registry_version": (
            source_policy.canonical_registry.registry_version
        ),
        "source_policy_digest": source_policy.digest,
        "ontology_registry_version": ontology.registry_version,
        "ontology_digest": ontology.digest,
        "predicate_registry_version": predicates.registry_version,
        "predicate_digest": predicates.digest,
        "gold_policy_version": gold_policy.policy_version,
        "gold_policy_digest": gold_policy.digest,
        "mapping_version": PROJECTION_VERSION,
        "mapping_digest": MAPPING_DIGEST,
        "algorithm_id": algorithm_id,
        "algorithm_schema_version": algorithm_schema,
        "algorithm_digest": algorithm_digest,
        "concrete_index": gold_index_name(RESEARCH_INDEX_PREFIX, build_id),
        "coordinate_count": len(coordinate_keys),
        "created_at": cohort.captured_at,
    }
    provisional = MusicPilotMatchProfile.model_validate(
        values,
        context={"skip_identity": True},
    )
    values["profile_id"] = deterministic_key(
        "music-pilot-match-profile-v1",
        _profile_identity(provisional),
    )
    return MusicPilotMatchProfile.model_validate(values)


class MusicPilotMatchResult(V2ContractModel):
    result_id: str
    asset_id: str
    decision: MusicPilotDecision
    reason: str
    candidate_entity_type: str | None = None
    candidate_namespace: str | None = None
    candidate_id: str | None = None
    candidate_count: int = Field(ge=0)
    score: int | None = Field(default=None, ge=0)
    confidence: float | None = Field(default=None, ge=0, le=1)
    profile_id: str

    @field_validator("result_id", "profile_id")
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)


def _match_result(
    asset: MusicPilotAsset,
    profile: MusicPilotMatchProfile,
    *,
    decision: MusicPilotDecision,
    reason: str,
    coordinate: MusicAuthorityCoordinate | None = None,
    candidate_count: int,
) -> MusicPilotMatchResult:
    namespace = None
    candidate_id = None
    entity_type = None
    score = None
    confidence = None
    if coordinate is not None:
        entity_type = coordinate.entity_type
        namespace = (
            "musicbrainz-recording"
            if coordinate.entity_type == "MUSIC_RECORDING"
            else "musicbrainz-work"
        )
        candidate_id = coordinate.musicbrainz_mbid
    if decision == MusicPilotDecision.EXACT:
        score = 1000
        confidence = 1.0
    identity = {
        "assetId": asset.asset_id,
        "decision": decision.value,
        "reason": reason,
        "candidateEntityType": entity_type,
        "candidateNamespace": namespace,
        "candidateId": candidate_id,
        "candidateCount": candidate_count,
        "score": score,
        "confidence": confidence,
        "profileId": profile.profile_id,
    }
    return MusicPilotMatchResult(
        result_id=deterministic_key("music-pilot-match-result-v1", identity),
        asset_id=asset.asset_id,
        decision=decision,
        reason=reason,
        candidate_entity_type=entity_type,
        candidate_namespace=namespace,
        candidate_id=candidate_id,
        candidate_count=candidate_count,
        score=score,
        confidence=confidence,
        profile_id=profile.profile_id,
    )


def match_music_pilot_asset(
    asset: MusicPilotAsset,
    *,
    profile: MusicPilotMatchProfile,
    source_policy_digest: str,
    authority_coordinate_digest: str,
    algorithm_digest: str,
) -> MusicPilotMatchResult:
    """Apply the fail-closed Music baseline to one reviewed Pilot feature row."""

    if require_sha256(source_policy_digest) != profile.source_policy_digest:
        raise ValueError("Music match source-policy digest differs from profile")
    if (
        require_sha256(authority_coordinate_digest)
        != profile.authority_coordinate_digest
    ):
        raise ValueError("Music match authority digest differs from profile")
    if require_sha256(algorithm_digest) != profile.algorithm_digest:
        raise ValueError("Music match algorithm digest differs from profile")
    coordinates = asset.features.authority_coordinates
    if any(item.coordinate_key not in profile.coordinate_keys for item in coordinates):
        raise ValueError("Music match coordinate is absent from concrete profile")
    if not coordinates:
        return _match_result(
            asset,
            profile,
            decision=MusicPilotDecision.ABSTAIN,
            reason="NO_AUTHORITY_COORDINATE",
            candidate_count=0,
        )
    if len(coordinates) != 1:
        return _match_result(
            asset,
            profile,
            decision=MusicPilotDecision.ABSTAIN,
            reason="MULTIPLE_EXACT_IDENTIFIER_CANDIDATES",
            candidate_count=len(coordinates),
        )
    coordinate = coordinates[0]
    indexed_coordinate = next(
        item
        for item in profile.coordinates
        if item.coordinate_key == coordinate.coordinate_key
    )
    if asset.features.performance_type == "LIVE":
        if (
            coordinate.entity_type != "MUSICAL_WORK"
            or indexed_coordinate.disposition
            != MusicPilotCoordinateDisposition.WORK_PARENT_ONLY
        ):
            return _match_result(
                asset,
                profile,
                decision=MusicPilotDecision.ABSTAIN,
                reason="LIVE_ASSET_HAS_NON_WORK_COORDINATE",
                candidate_count=1,
            )
        return _match_result(
            asset,
            profile,
            decision=MusicPilotDecision.REVIEW_REQUIRED,
            reason="PARENT_CONSTRAINED_WORK_ONLY",
            coordinate=coordinate,
            candidate_count=1,
        )
    if (
        asset.features.performance_type == "VARIANT"
        or asset.features.variant_evidence
        or indexed_coordinate.disposition
        == MusicPilotCoordinateDisposition.VARIANT_ABSTAIN_ONLY
    ):
        return _match_result(
            asset,
            profile,
            decision=MusicPilotDecision.ABSTAIN,
            reason="REMIX_OR_VARIANT_REQUIRES_DISTINCT_RECORDING_AUTHORITY",
            candidate_count=1,
        )
    if (
        coordinate.entity_type != "MUSIC_RECORDING"
        or indexed_coordinate.disposition
        != MusicPilotCoordinateDisposition.EXACT_RECORDING
    ):
        return _match_result(
            asset,
            profile,
            decision=MusicPilotDecision.ABSTAIN,
            reason="EXACT_MATCH_REQUIRES_RECORDING_COORDINATE",
            candidate_count=1,
        )
    return _match_result(
        asset,
        profile,
        decision=MusicPilotDecision.EXACT,
        reason="UNIQUE_REVIEWED_EXACT_RECORDING_COORDINATE",
        coordinate=coordinate,
        candidate_count=1,
    )


def replay_music_pilot(
    cohort: MusicPilotCohort,
    *,
    profile: MusicPilotMatchProfile,
) -> tuple[MusicPilotMatchResult, ...]:
    return tuple(
        match_music_pilot_asset(
            asset,
            profile=profile,
            source_policy_digest=profile.source_policy_digest,
            authority_coordinate_digest=profile.authority_coordinate_digest,
            algorithm_digest=profile.algorithm_digest,
        )
        for asset in cohort.assets
    )


def build_music_coordinate_review_conflict(
    coordinate: MusicAuthorityCoordinate,
    *,
    candidate_entity_keys: tuple[str, str],
    assertion_keys: tuple[str, ...],
    observed_at: str,
    authority_coordinate_digest: str,
) -> IdentityConflict:
    """Stage an approved manual cross-coordinate as ordinary identity curation."""

    if not coordinate.coordinate_review.mapping_kind.startswith("RESEARCH_CANDIDATE_"):
        raise ValueError("authority-declared coordinate does not require curation")
    review_digest = digest_identity(
        coordinate.coordinate_review.model_dump(
            mode="json",
            by_alias=True,
            exclude_none=True,
        )
    )
    from video_media_catalog.assertions import SourceNodeRef

    return build_identity_conflict(
        source_node=SourceNodeRef(
            namespace_id="music-coordinate-review",
            source_id=f"{coordinate.wikidata_qid}:{coordinate.musicbrainz_mbid}",
            referent_kind=coordinate.entity_type,
        ),
        candidate_entity_keys=candidate_entity_keys,
        assertion_keys=assertion_keys,
        reason="RESEARCH_CANDIDATE_MUSIC_AUTHORITY_COORDINATE",
        observed_at=observed_at,
        policy_id="music-pilot-coordinate-review",
        policy_digest=require_sha256(authority_coordinate_digest),
        details={
            "wikidataQid": coordinate.wikidata_qid,
            "musicbrainzMbid": coordinate.musicbrainz_mbid,
            "entityType": coordinate.entity_type,
            "mappingKind": coordinate.coordinate_review.mapping_kind,
            "reviewStatus": coordinate.coordinate_review.review_status,
            "reviewDigest": review_digest,
        },
    )
