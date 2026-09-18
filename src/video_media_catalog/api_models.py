"""OpenAPI response contracts for the read-only catalog service."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


def _camel(value: str) -> str:
    first, *rest = value.split("_")
    return first + "".join(item.capitalize() for item in rest)


class APIModel(BaseModel):
    model_config = ConfigDict(
        alias_generator=_camel,
        populate_by_name=True,
        extra="forbid",
    )


class CatalogName(APIModel):
    name_type: str | None = None
    language: str | None = None
    value: str
    source: str | None = None
    source_record_id: str | None = None


class CatalogDescription(APIModel):
    language: str
    value: str


class CatalogSitelink(APIModel):
    site: str
    title: str
    url: str | None = None
    badges: list[str]


class CatalogAttributes(APIModel):
    release_dates: list[str]
    durations: list[str]
    languages: list[str]
    countries: list[str]
    genres: list[str]
    episode_counts: list[str]
    season_counts: list[str]
    modified: list[str]


class CatalogExternalIdentifier(APIModel):
    scheme: str
    value: str
    source: str | None = None
    source_record_id: str | None = None


class CatalogRelation(APIModel):
    relation_key: str
    relation_type: str
    object_entity_key: str
    ordinal: str | None = None
    source: str
    source_record_id: str


class CatalogRelationSummary(APIModel):
    relation_type: str
    count: int = Field(ge=0)


class CatalogSourceRecord(APIModel):
    record_key: str | None = None
    source: str
    source_record_id: str | None = None
    source_revision: str | None = None
    modified: str | None = None


class CatalogEntity(APIModel):
    entity_key: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    entity_type: Literal[
        "MOVIE",
        "TV_SERIES",
        "TV_SEASON",
        "TV_EPISODE",
        "PERSON",
        "ORGANIZATION",
        "UNKNOWN",
    ]
    canonical_source: str
    canonical_source_id: str
    display_name: str
    display_language: str
    description: str | None = None
    names: list[CatalogName]
    descriptions: list[CatalogDescription]
    sitelinks: list[CatalogSitelink]
    attributes: CatalogAttributes
    external_identifiers: list[CatalogExternalIdentifier]
    relations: list[CatalogRelation]
    relation_summary: list[CatalogRelationSummary]
    parent_keys: list[str]
    source_record_ids: list[str]
    source_records: list[CatalogSourceRecord]


class CatalogEntitySummary(APIModel):
    entity_key: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    entity_type: str
    display_name: str
    display_language: str
    description: str | None = None
    external_identifiers: list[CatalogExternalIdentifier] = Field(max_length=5)


class SearchResponse(APIModel):
    items: list[CatalogEntitySummary]
    next_cursor: str | None = None
    total_value: int = Field(ge=0)
    total_relation: Literal["eq", "gte"]


class HealthResponse(APIModel):
    status: Literal["ok"]


class ProblemDetails(APIModel):
    type: str
    title: str
    status: int
    code: str
    detail: str
    retryable: bool
    instance: str
