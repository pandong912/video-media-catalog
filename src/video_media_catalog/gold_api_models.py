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


class GoldOverflow(APIModel):
    titles: int = Field(ge=0)
    external_identifiers: int = Field(ge=0)
    relation_types: int = Field(ge=0)
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
    display_name: str
    display_language: str
    titles: list[GoldTitle]
    attributes: GoldAttributes
    external_identifiers: list[GoldExternalIdentifier]
    relation_summary: list[GoldRelationSummary]
    conflict_count: int = Field(ge=0)
    conflict_predicates: list[str]
    source_node_count: int = Field(ge=0)
    overflow: GoldOverflow


class GoldCatalogEntitySummary(APIModel):
    entity_key: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    entity_level: str
    entity_kind: str
    display_name: str
    display_language: str
    release_plan_id: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    conflict_count: int = Field(ge=0)
    external_identifiers: list[GoldExternalIdentifier] = Field(max_length=5)


class GoldSearchResponse(APIModel):
    items: list[GoldCatalogEntitySummary]
    next_cursor: str | None = None
    total_value: int = Field(ge=0)
    total_relation: Literal["eq", "gte"]
