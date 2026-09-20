"""HTTP response models for the bounded community Gold v2 API."""

from __future__ import annotations

from typing import Literal

from pydantic import Field

from video_media_catalog.api_models import APIModel


class GoldTitle(APIModel):
    value: str
    language: str
    title_role: str


class GoldAttributes(APIModel):
    formats: list[str]
    languages: list[str]
    statuses: list[str]
    premiered: list[str]
    ended: list[str]
    runtime_minutes: list[str]
    average_runtime_minutes: list[str]
    genres: list[str]


class GoldExternalIdentifier(APIModel):
    namespace: str
    value: str
    issuer: str
    referent_kind: str


class GoldRelationSummary(APIModel):
    predicate: str
    count: int = Field(ge=0)


class GoldSourceBadge(APIModel):
    source_product_id: str
    display_name: str
    source_url: str
    policy_zones: list[str]
    assertion_count: int = Field(ge=0)
    winning_assertion_count: int = Field(ge=0)


class GoldWinningAssertionSummary(APIModel):
    kind: Literal["FIELD", "IDENTIFIER"]
    assertion_id: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    predicate: str
    value_json: str
    qualifiers_json: str
    resolution_status: str
    source_product_id: str
    source_record_id: str
    source_path: str
    observed_at: str
    citation_keys: list[str] = Field(max_length=16)
    citation_overflow: int = Field(ge=0)


class GoldRightsSummary(APIModel):
    source_product_id: str
    policy_id: str
    policy_zone: str
    license_id: str
    license_uri: str | None = None
    attribution_text: str
    source_url: str
    share_alike: bool


class GoldConflictSummary(APIModel):
    predicate: str
    scope_hash: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    reason: str
    assertion_ids: list[str] = Field(max_length=32)
    assertion_overflow: int = Field(ge=0)
    candidate_values_json: list[str] = Field(max_length=16)
    candidate_value_overflow: int = Field(ge=0)
    source_product_ids: list[str]


class GoldOverflow(APIModel):
    titles: int = Field(ge=0)
    external_identifiers: int = Field(ge=0)
    relation_types: int = Field(ge=0)
    source_badges: int = Field(ge=0)
    winning_assertions: int = Field(ge=0)
    citation_keys: int = Field(ge=0)
    rights: int = Field(ge=0)
    conflicts: int = Field(ge=0)
    formats: int = Field(ge=0)
    languages: int = Field(ge=0)
    statuses: int = Field(ge=0)
    premiered: int = Field(ge=0)
    ended: int = Field(ge=0)
    runtime_minutes: int = Field(ge=0)
    average_runtime_minutes: int = Field(ge=0)
    genres: int = Field(ge=0)


class GoldCatalogEntity(APIModel):
    entity_key: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    entity_level: str
    entity_kind: str
    status: str
    release_plan_id: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    context_id: Literal["personal-research"]
    display_name: str
    display_language: str
    titles: list[GoldTitle]
    attributes: GoldAttributes
    external_identifiers: list[GoldExternalIdentifier]
    relation_summary: list[GoldRelationSummary]
    source_badges: list[GoldSourceBadge] = Field(max_length=32)
    winning_assertions: list[GoldWinningAssertionSummary] = Field(max_length=128)
    rights: list[GoldRightsSummary] = Field(max_length=32)
    conflict_count: int = Field(ge=0)
    conflict_predicates: list[str]
    conflicts: list[GoldConflictSummary] = Field(max_length=64)
    source_node_count: int = Field(ge=0)
    overflow: GoldOverflow


class GoldCatalogEntitySummary(APIModel):
    entity_key: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    entity_level: str
    entity_kind: str
    display_name: str
    display_language: str
    release_plan_id: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    context_id: Literal["personal-research"]
    conflict_count: int = Field(ge=0)
    external_identifiers: list[GoldExternalIdentifier] = Field(max_length=5)
    source_badges: list[GoldSourceBadge] = Field(max_length=5)


class GoldSearchResponse(APIModel):
    items: list[GoldCatalogEntitySummary]
    next_cursor: str | None = None
    total_value: int = Field(ge=0)
    total_relation: Literal["eq", "gte"]
