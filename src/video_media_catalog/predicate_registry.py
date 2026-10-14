"""Versioned predicate contracts for sports and animation research assertions."""

from __future__ import annotations

import json
from enum import StrEnum
from pathlib import Path
from typing import Self

from pydantic import field_validator, model_validator

from video_media_catalog.assertions import ValueType
from video_media_catalog.v2_contracts import (
    V2ContractModel,
    digest_identity,
    require_slug,
)


class ResearchDomain(StrEnum):
    SPORTS = "SPORTS"
    ANIMATION = "ANIMATION"


class PredicateKind(StrEnum):
    FIELD = "FIELD"
    RELATIONSHIP = "RELATIONSHIP"


class PredicateResolutionOperator(StrEnum):
    SINGLE = "SINGLE"
    SET_UNION = "SET_UNION"
    KEEP_ALL = "KEEP_ALL"


class PredicateDefinition(V2ContractModel):
    predicate: str
    kind: PredicateKind
    value_type: ValueType | None = None
    domains: tuple[ResearchDomain, ...]
    resolution_operator: PredicateResolutionOperator
    temporal: bool = False

    @field_validator("predicate")
    @classmethod
    def validate_predicate(cls, value: str) -> str:
        return require_slug(value, label="research predicate")

    @field_validator("domains")
    @classmethod
    def normalize_domains(
        cls,
        value: tuple[ResearchDomain, ...],
    ) -> tuple[ResearchDomain, ...]:
        normalized = tuple(sorted(set(value), key=str))
        if not normalized:
            raise ValueError("research predicate requires at least one domain")
        return normalized

    @model_validator(mode="after")
    def validate_kind(self) -> Self:
        if self.kind == PredicateKind.FIELD and self.value_type is None:
            raise ValueError("field predicate requires valueType")
        if self.kind == PredicateKind.RELATIONSHIP and self.value_type is not None:
            raise ValueError("relationship predicate cannot declare valueType")
        if (
            self.temporal
            and self.resolution_operator != PredicateResolutionOperator.KEEP_ALL
        ):
            raise ValueError("temporal predicates must preserve every observation")
        return self


class PredicateRegistry(V2ContractModel):
    schema_version: str
    registry_id: str
    predicates: tuple[PredicateDefinition, ...]

    @field_validator("schema_version")
    @classmethod
    def validate_schema_version(cls, value: str) -> str:
        if value != "1.0":
            raise ValueError("unsupported predicate registry schemaVersion")
        return value

    @field_validator("registry_id")
    @classmethod
    def validate_registry_id(cls, value: str) -> str:
        return require_slug(value, label="predicate registry_id")

    @field_validator("predicates")
    @classmethod
    def normalize_predicates(
        cls,
        value: tuple[PredicateDefinition, ...],
    ) -> tuple[PredicateDefinition, ...]:
        normalized = tuple(sorted(value, key=lambda item: item.predicate))
        names = tuple(item.predicate for item in normalized)
        if not names or len(names) != len(set(names)):
            raise ValueError("predicate registry must contain unique predicates")
        return normalized

    @property
    def digest(self) -> str:
        return digest_identity(
            self.model_dump(mode="json", by_alias=True, exclude_none=True)
        )

    def definition_for(self, predicate: str) -> PredicateDefinition:
        normalized = require_slug(predicate, label="research predicate")
        try:
            return next(
                item for item in self.predicates if item.predicate == normalized
            )
        except StopIteration as exc:
            raise ValueError(f"unregistered research predicate: {normalized}") from exc


def load_research_predicate_registry(
    path: Path | None = None,
) -> PredicateRegistry:
    if path is None:
        repository_contract = (
            Path(__file__).resolve().parents[2]
            / "contracts/catalog/research-predicate-registry.v1.json"
        )
        path = (
            repository_contract
            if repository_contract.is_file()
            else Path(__file__).resolve().parent
            / "contracts/research-predicate-registry.v1.json"
        )
    payload = json.loads(path.read_text(encoding="utf-8"))
    return PredicateRegistry.model_validate(payload)
