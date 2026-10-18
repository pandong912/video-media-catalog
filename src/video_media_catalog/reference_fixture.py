"""Small checksum-pinned reference fixtures that exercise the production mapper."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Self
from urllib.parse import urlsplit

from pydantic import Field, field_validator, model_validator

from video_media_catalog.assertions import Citation, build_citation
from video_media_catalog.community_sources import (
    musicbrainz_core_rights_profile,
    wikidata_rights_profile,
)
from video_media_catalog.connector import Serialization
from video_media_catalog.models import ObjectRef
from video_media_catalog.music_sources import (
    WIKIDATA_MUSIC_CONNECTOR_ID,
    WIKIDATA_MUSIC_NAMESPACE_ID,
    WIKIDATA_MUSIC_SOURCE_PRODUCT_ID,
    map_musicbrainz_core_record,
    map_wikidata_music_record,
    musicbrainz_namespace_for_type,
)
from video_media_catalog.research_snapshot import (
    CapturedSnapshot,
    SnapshotRevisionKind,
    build_source_snapshot_pin,
    capture_pinned_records,
)
from video_media_catalog.source_mapper import MappedAssertions
from video_media_catalog.v2_contracts import (
    V2ContractModel,
    digest_identity,
    require_https_url,
    require_rfc3339,
    require_sha256,
    require_slug,
)


class ReferenceFixtureCitation(V2ContractModel):
    citation_id: str
    source_record_id: str
    source_url: str
    title: str | None = None
    publisher: str | None = None
    retrieved_at: str
    license_uri: str | None = None
    attribution_text: str | None = None
    raw_object_hash: str

    @field_validator("citation_id")
    @classmethod
    def validate_citation_id(cls, value: str) -> str:
        return require_slug(value, label="fixture citation")

    @field_validator("source_url", "license_uri")
    @classmethod
    def validate_url(cls, value: str | None) -> str | None:
        return None if value is None else require_https_url(value)

    @field_validator("retrieved_at")
    @classmethod
    def validate_retrieved_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @field_validator("raw_object_hash")
    @classmethod
    def validate_raw_object_hash(cls, value: str) -> str:
        return require_sha256(value, label="fixture raw object hash")

    def build(self, *, source_product_id: str) -> Citation:
        return build_citation(
            source_product_id=source_product_id,
            source_record_id=self.source_record_id,
            source_url=self.source_url,
            title=self.title,
            publisher=self.publisher,
            retrieved_at=self.retrieved_at,
            license_uri=self.license_uri,
            attribution_text=self.attribution_text,
            raw_object_hash=self.raw_object_hash,
        )


class ReferenceFixtureRecord(V2ContractModel):
    payload: dict[str, Any]
    citation_ids: tuple[str, ...] = ()
    source_modified_at: str | None = None
    valid_from: str | None = None
    valid_to: str | None = None

    @field_validator("citation_ids")
    @classmethod
    def normalize_citation_ids(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(
            sorted({require_slug(item, label="fixture citation") for item in value})
        )

    @field_validator("source_modified_at", "valid_from", "valid_to")
    @classmethod
    def validate_timestamp(cls, value: str | None) -> str | None:
        return None if value is None else require_rfc3339(value)

    @model_validator(mode="after")
    def validate_payload(self) -> Self:
        record_id = self.payload.get("id")
        if not isinstance(record_id, str) or not record_id.strip():
            raise ValueError("reference fixture record requires a payload id")
        return self


class ReferenceCaptureReceipt(V2ContractModel):
    """Verifiable receipt for one live response used by a minimal projection."""

    request_url: str
    retrieved_at: str
    response_hash: str
    authority_revision: str
    source_record_ids: tuple[str, ...]
    projected_record_ids: tuple[str, ...]
    projection_paths: tuple[str, ...]

    @field_validator("request_url")
    @classmethod
    def validate_request_url(cls, value: str) -> str:
        return require_https_url(value)

    @field_validator("retrieved_at")
    @classmethod
    def validate_retrieved_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @field_validator("response_hash")
    @classmethod
    def validate_response_hash(cls, value: str) -> str:
        return require_sha256(value, label="capture response hash")

    @field_validator("authority_revision")
    @classmethod
    def validate_authority_revision(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized or len(normalized) > 4096:
            raise ValueError("capture authority revision must be non-empty and bounded")
        return normalized

    @field_validator("source_record_ids", "projected_record_ids")
    @classmethod
    def validate_record_ids(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if (
            not value
            or len(value) != len(set(value))
            or tuple(sorted(value)) != value
            or any(not item.strip() or len(item) > 2048 for item in value)
        ):
            raise ValueError("capture record IDs must be unique, sorted, and bounded")
        return value

    @field_validator("projection_paths")
    @classmethod
    def validate_projection_paths(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if (
            not value
            or len(value) != len(set(value))
            or any(
                not item.startswith("/") or len(item) > 2048 or item != item.strip()
                for item in value
            )
        ):
            raise ValueError("capture projection paths must be unique JSON paths")
        return value


class PinnedReferenceFixture(V2ContractModel):
    schema_version: Literal["video.media-catalog.reference-fixture.v1"]
    fixture_id: str
    domain: Literal["NEWS", "LEARNING", "MUSIC"]
    source_system_id: str = "wikidata"
    source_product_id: str = "wikidata-json-dump"
    connector_id: str = "wikidata-full-media-backfill"
    revision_kind: SnapshotRevisionKind = SnapshotRevisionKind.DUMP_OBJECT
    schema_id: str = "wikidata-global-reference-entity"
    source_revision: str
    captured_at: str
    raw_object: ObjectRef
    image_digest: str
    config_digest: str
    capture_receipts: tuple[ReferenceCaptureReceipt, ...] = ()
    citations: tuple[ReferenceFixtureCitation, ...]
    records: tuple[ReferenceFixtureRecord, ...] = Field(min_length=1, max_length=64)

    @field_validator("fixture_id")
    @classmethod
    def validate_fixture_id(cls, value: str) -> str:
        return require_slug(value, label="reference fixture")

    @field_validator(
        "source_system_id",
        "source_product_id",
        "connector_id",
        "schema_id",
    )
    @classmethod
    def validate_source_coordinate(cls, value: str) -> str:
        return require_slug(value, label="reference fixture source coordinate")

    @field_validator("captured_at")
    @classmethod
    def validate_captured_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @field_validator("image_digest", "config_digest")
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("source_revision")
    @classmethod
    def validate_source_revision(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized or len(normalized) > 1024:
            raise ValueError("fixture source revision must be non-empty and bounded")
        return normalized

    @field_validator("capture_receipts")
    @classmethod
    def validate_capture_receipts(
        cls,
        value: tuple[ReferenceCaptureReceipt, ...],
    ) -> tuple[ReferenceCaptureReceipt, ...]:
        urls = tuple(item.request_url for item in value)
        if len(urls) != len(set(urls)) or tuple(sorted(urls)) != urls:
            raise ValueError("capture receipts must have unique, sorted request URLs")
        return value

    @model_validator(mode="after")
    def validate_fixture(self) -> Self:
        coordinates = (
            self.source_system_id,
            self.source_product_id,
            self.connector_id,
            self.schema_id,
        )
        wikidata = (
            "wikidata",
            "wikidata-json-dump",
            "wikidata-full-media-backfill",
            "wikidata-global-reference-entity",
        )
        musicbrainz = (
            "musicbrainz",
            "musicbrainz-core-dump",
            "musicbrainz-core-dump-snapshot",
            "musicbrainz-core-entity",
        )
        wikidata_music = (
            "wikidata",
            WIKIDATA_MUSIC_SOURCE_PRODUCT_ID,
            WIKIDATA_MUSIC_CONNECTOR_ID,
            "wikidata-global-reference-entity",
        )
        if self.domain in {"NEWS", "LEARNING"} and coordinates != wikidata:
            raise ValueError("News/Learning fixtures require pinned Wikidata")
        if self.domain == "MUSIC" and coordinates not in {
            wikidata_music,
            musicbrainz,
        }:
            raise ValueError(
                "Music fixtures require pinned Wikidata or MusicBrainz core"
            )
        citation_ids = {item.citation_id for item in self.citations}
        if len(citation_ids) != len(self.citations):
            raise ValueError("reference fixture citation IDs must be unique")
        record_ids = tuple(str(item.payload["id"]) for item in self.records)
        if len(record_ids) != len(set(record_ids)):
            raise ValueError("reference fixture record IDs must be unique")
        for record in self.records:
            if record.payload.get("researchDomain") != self.domain:
                raise ValueError("reference fixture record domain mismatch")
            unknown = set(record.citation_ids) - citation_ids
            if unknown:
                raise ValueError("reference fixture record cites an unknown citation")
        raw_digest = f"sha256:{self.raw_object.checksum.value}"
        if any(citation.raw_object_hash != raw_digest for citation in self.citations):
            raise ValueError("reference fixture citation does not pin its raw object")
        if self.capture_receipts:
            expected_host = (
                "musicbrainz.org"
                if self.source_product_id == "musicbrainz-core-dump"
                else "www.wikidata.org"
            )
            if any(
                urlsplit(item.request_url).hostname != expected_host
                or item.retrieved_at != self.captured_at
                or not set(item.source_record_ids) <= set(item.projected_record_ids)
                for item in self.capture_receipts
            ):
                raise ValueError(
                    "reference fixture capture receipt differs from source boundary"
                )
            receipt_payload = [
                item.model_dump(mode="json", by_alias=True, exclude_none=True)
                for item in self.capture_receipts
            ]
            if self.source_revision != digest_identity(receipt_payload):
                raise ValueError(
                    "reference fixture source revision does not bind capture receipts"
                )
            projected = {
                record_id
                for receipt in self.capture_receipts
                for record_id in receipt.projected_record_ids
            }
            if not set(record_ids) <= projected:
                raise ValueError(
                    "reference fixture contains records absent from capture receipts"
                )
        return self

    @property
    def digest(self) -> str:
        material = self.model_dump(mode="json", by_alias=True, exclude_none=True)
        if not self.capture_receipts:
            material.pop("captureReceipts", None)
        return digest_identity(material)


@dataclass(frozen=True)
class MaterializedReferenceFixture:
    fixture: PinnedReferenceFixture
    citations: tuple[Citation, ...]
    captured: CapturedSnapshot
    mapped: tuple[MappedAssertions, ...]


def load_reference_fixture(path: str | Path) -> PinnedReferenceFixture:
    fixture_path = Path(path)
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    fixture = PinnedReferenceFixture.model_validate(payload)
    raw_path = fixture_path.parent / Path(urlsplit(fixture.raw_object.uri).path).name
    raw_bytes = raw_path.read_bytes()
    if (
        len(raw_bytes) != fixture.raw_object.size_bytes
        or hashlib.sha256(raw_bytes).hexdigest() != fixture.raw_object.checksum.value
    ):
        raise ValueError("reference fixture raw sidecar differs from its immutable pin")
    raw_records = tuple(
        json.loads(line) for line in raw_bytes.decode("utf-8").splitlines()
    )
    if raw_records != tuple(item.payload for item in fixture.records):
        raise ValueError("reference fixture records differ from the pinned raw sidecar")
    return fixture


def materialize_reference_fixture(
    fixture: PinnedReferenceFixture,
    *,
    serialization: Serialization = Serialization.JSON,
) -> MaterializedReferenceFixture:
    citations_by_id = {
        item.citation_id: item.build(source_product_id=fixture.source_product_id)
        for item in fixture.citations
    }
    record_by_id = {str(item.payload["id"]): item for item in fixture.records}
    pin = build_source_snapshot_pin(
        source_system_id=fixture.source_system_id,
        source_product_id=fixture.source_product_id,
        connector_id=fixture.connector_id,
        revision_kind=fixture.revision_kind,
        source_revision=fixture.source_revision,
        schema_id=fixture.schema_id,
        raw_objects=(fixture.raw_object,),
        captured_at=fixture.captured_at,
    )
    policy = (
        musicbrainz_core_rights_profile()
        if fixture.source_product_id == "musicbrainz-core-dump"
        else wikidata_rights_profile()
    )
    captured = capture_pinned_records(
        pin=pin,
        records=tuple(item.payload for item in fixture.records),
        source_namespace=lambda record: (
            musicbrainz_namespace_for_type(str(record.get("entityType") or ""))
            if fixture.source_product_id == "musicbrainz-core-dump"
            else WIKIDATA_MUSIC_NAMESPACE_ID
            if fixture.source_product_id == WIKIDATA_MUSIC_SOURCE_PRODUCT_ID
            else "wikidata-item"
        ),
        source_record_id=lambda record: str(record["id"]),
        citation_keys=lambda record: tuple(
            citations_by_id[citation_id].citation_key
            for citation_id in record_by_id[str(record["id"])].citation_ids
        ),
        source_modified_at=lambda record: (
            record_by_id[str(record["id"])].source_modified_at
        ),
        valid_from=lambda record: record_by_id[str(record["id"])].valid_from,
        valid_to=lambda record: record_by_id[str(record["id"])].valid_to,
        policy_id=policy.policy_id,
        policy_digest=policy.digest,
        image_digest=fixture.image_digest,
        config_digest=fixture.config_digest,
        serialization=serialization,
    )
    if fixture.source_product_id == "musicbrainz-core-dump":
        mapped = tuple(
            map_musicbrainz_core_record(envelope) for envelope in captured.envelopes
        )
    elif fixture.source_product_id == WIKIDATA_MUSIC_SOURCE_PRODUCT_ID:
        mapped = tuple(
            map_wikidata_music_record(envelope) for envelope in captured.envelopes
        )
    else:
        from video_media_catalog.source_mappers import map_wikidata_research_record

        mapped = tuple(
            map_wikidata_research_record(envelope) for envelope in captured.envelopes
        )
    return MaterializedReferenceFixture(
        fixture=fixture,
        citations=tuple(citations_by_id[key] for key in sorted(citations_by_id)),
        captured=captured,
        mapped=mapped,
    )
