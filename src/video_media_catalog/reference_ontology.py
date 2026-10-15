"""Versioned global-reference ontology with no tenant World authority."""

from __future__ import annotations

import json
import re
from enum import StrEnum
from functools import lru_cache
from pathlib import Path
from typing import Literal, Self

from pydantic import field_validator, model_validator

from video_media_catalog.v2_contracts import (
    V2ContractModel,
    digest_identity,
    require_slug,
)

_SEMVER = re.compile(r"^[1-9][0-9]*\.[0-9]+\.[0-9]+$")
_CONCEPT = re.compile(r"^[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)*$")


class CanonicalSubjectRoot(StrEnum):
    AGENT = "AGENT"
    PLACE = "PLACE"
    PHYSICAL_ENTITY = "PHYSICAL_ENTITY"
    EVENT = "EVENT"
    WORK = "WORK"
    CONCEPT = "CONCEPT"
    FICTIONAL_ENTITY = "FICTIONAL_ENTITY"


class ReferenceDomain(StrEnum):
    GENERAL = "GENERAL"
    FILM_TV = "FILM_TV"
    SPORTS = "SPORTS"
    ANIMATION = "ANIMATION"
    NEWS = "NEWS"
    JOURNALISM = "JOURNALISM"
    LEARNING = "LEARNING"
    INSTRUCTION = "INSTRUCTION"


class ReferenceTypeConcept(V2ContractModel):
    type_concept_id: str
    concept_version: str
    canonical_root: CanonicalSubjectRoot
    domains: tuple[ReferenceDomain, ...]

    @field_validator("type_concept_id")
    @classmethod
    def validate_type_concept_id(cls, value: str) -> str:
        normalized = value.strip().upper()
        if len(normalized) > 128 or _CONCEPT.fullmatch(normalized) is None:
            raise ValueError("reference type concept must be an uppercase stable token")
        return normalized

    @field_validator("concept_version")
    @classmethod
    def validate_concept_version(cls, value: str) -> str:
        normalized = value.strip()
        if _SEMVER.fullmatch(normalized) is None:
            raise ValueError("reference type concept version must be semantic")
        return normalized

    @field_validator("domains")
    @classmethod
    def normalize_domains(
        cls,
        value: tuple[ReferenceDomain, ...],
    ) -> tuple[ReferenceDomain, ...]:
        normalized = tuple(sorted(set(value), key=str))
        if not normalized:
            raise ValueError("reference type concept requires at least one domain")
        return normalized


class GlobalReferenceOntology(V2ContractModel):
    schema_version: Literal["1.0"] = "1.0"
    registry_id: str
    registry_version: str
    authority: Literal["GLOBAL_REFERENCE_ONLY"]
    tenant_world_identity_authority: Literal[False] = False
    predicate_registry_id: str
    predicate_registry_version: str
    root_types: tuple[CanonicalSubjectRoot, ...]
    type_concepts: tuple[ReferenceTypeConcept, ...]

    @field_validator("registry_id", "predicate_registry_id")
    @classmethod
    def validate_registry_id(cls, value: str) -> str:
        return require_slug(value, label="reference registry")

    @field_validator("registry_version", "predicate_registry_version")
    @classmethod
    def validate_registry_version(cls, value: str) -> str:
        normalized = value.strip()
        if _SEMVER.fullmatch(normalized) is None:
            raise ValueError("reference registry version must be semantic")
        return normalized

    @field_validator("root_types")
    @classmethod
    def normalize_roots(
        cls,
        value: tuple[CanonicalSubjectRoot, ...],
    ) -> tuple[CanonicalSubjectRoot, ...]:
        normalized = tuple(sorted(set(value), key=str))
        if set(normalized) != set(CanonicalSubjectRoot):
            raise ValueError("reference ontology must expose exactly seven root types")
        return normalized

    @field_validator("type_concepts")
    @classmethod
    def normalize_type_concepts(
        cls,
        value: tuple[ReferenceTypeConcept, ...],
    ) -> tuple[ReferenceTypeConcept, ...]:
        normalized = tuple(sorted(value, key=lambda item: item.type_concept_id))
        names = tuple(item.type_concept_id for item in normalized)
        if not names or len(names) != len(set(names)):
            raise ValueError("reference ontology type concepts must be unique")
        return normalized

    @model_validator(mode="after")
    def validate_root_concepts(self) -> Self:
        concepts = {item.type_concept_id: item for item in self.type_concepts}
        for root in CanonicalSubjectRoot:
            concept = concepts.get(root.value)
            if concept is None or concept.canonical_root != root:
                raise ValueError("every reference root must be a direct type concept")
        return self

    @property
    def digest(self) -> str:
        return digest_identity(
            self.model_dump(mode="json", by_alias=True, exclude_none=True)
        )

    def concept_for(self, type_concept_id: str) -> ReferenceTypeConcept:
        normalized = type_concept_id.strip().upper()
        try:
            return next(
                item
                for item in self.type_concepts
                if item.type_concept_id == normalized
            )
        except StopIteration as exc:
            raise ValueError(
                f"unregistered reference type concept: {normalized}"
            ) from exc

    def root_for(self, type_concept_id: str) -> CanonicalSubjectRoot:
        return self.concept_for(type_concept_id).canonical_root


REFERENCE_ONTOLOGY_RELATIVE_PATH = Path(
    "contracts/catalog/global-reference-ontology.v1.json"
)


def default_reference_ontology_path() -> Path:
    repository_contract = (
        Path(__file__).resolve().parents[2] / REFERENCE_ONTOLOGY_RELATIVE_PATH
    )
    if repository_contract.is_file():
        return repository_contract
    return (
        Path(__file__).resolve().parent / "contracts/global-reference-ontology.v1.json"
    )


@lru_cache(maxsize=1)
def load_global_reference_ontology(
    path: Path | None = None,
) -> GlobalReferenceOntology:
    source = default_reference_ontology_path() if path is None else path
    payload = json.loads(source.read_text(encoding="utf-8"))
    ontology = GlobalReferenceOntology.model_validate(payload)

    from video_media_catalog.predicate_registry import (
        load_research_predicate_registry,
    )

    predicates = load_research_predicate_registry()
    if (
        predicates.registry_id != ontology.predicate_registry_id
        or predicates.registry_version != ontology.predicate_registry_version
    ):
        raise ValueError("reference ontology predicate-registry coordinate drifted")
    return ontology
