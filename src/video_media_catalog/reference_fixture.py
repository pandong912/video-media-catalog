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
from video_media_catalog.community_sources import wikidata_rights_profile
from video_media_catalog.models import ObjectRef
from video_media_catalog.reference_sources import map_wikidata_reference_record
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

    def build(self) -> Citation:
        return build_citation(
            source_product_id="wikidata-json-dump",
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


class PinnedReferenceFixture(V2ContractModel):
    schema_version: Literal["video.media-catalog.reference-fixture.v1"]
    fixture_id: str
    domain: Literal["NEWS", "LEARNING"]
    source_revision: str
    captured_at: str
    raw_object: ObjectRef
    image_digest: str
    config_digest: str
    citations: tuple[ReferenceFixtureCitation, ...]
    records: tuple[ReferenceFixtureRecord, ...] = Field(min_length=1, max_length=64)

    @field_validator("fixture_id")
    @classmethod
    def validate_fixture_id(cls, value: str) -> str:
        return require_slug(value, label="reference fixture")

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

    @model_validator(mode="after")
    def validate_fixture(self) -> Self:
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
        return self

    @property
    def digest(self) -> str:
        return digest_identity(
            self.model_dump(mode="json", by_alias=True, exclude_none=True)
        )


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
) -> MaterializedReferenceFixture:
    citations_by_id = {item.citation_id: item.build() for item in fixture.citations}
    record_by_id = {str(item.payload["id"]): item for item in fixture.records}
    pin = build_source_snapshot_pin(
        source_system_id="wikidata",
        source_product_id="wikidata-json-dump",
        connector_id="wikidata-full-media-backfill",
        revision_kind=SnapshotRevisionKind.DUMP_OBJECT,
        source_revision=fixture.source_revision,
        schema_id="wikidata-global-reference-entity",
        raw_objects=(fixture.raw_object,),
        captured_at=fixture.captured_at,
    )
    policy = wikidata_rights_profile()
    captured = capture_pinned_records(
        pin=pin,
        records=tuple(item.payload for item in fixture.records),
        source_namespace=lambda _record: "wikidata-item",
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
    )
    mapped = tuple(
        map_wikidata_reference_record(envelope) for envelope in captured.envelopes
    )
    return MaterializedReferenceFixture(
        fixture=fixture,
        citations=tuple(citations_by_id[key] for key in sorted(citations_by_id)),
        captured=captured,
        mapped=mapped,
    )
