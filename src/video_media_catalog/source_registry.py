"""Supplier-neutral source and schema registry contracts."""

from __future__ import annotations

import re
from enum import StrEnum
from typing import Self

from pydantic import field_validator, model_validator

from video_media_catalog.rights import RightsProfile
from video_media_catalog.v2_contracts import (
    V2ContractModel,
    digest_identity,
    require_https_url,
    require_sha256,
    require_slug,
)


class RegistryStatus(StrEnum):
    ACTIVE = "active"
    PAUSED = "paused"
    RETIRED = "retired"


class SourceProductKind(StrEnum):
    INTERNAL_CATALOG = "internal_catalog"
    IDENTIFIER_REGISTRY = "identifier_registry"
    KNOWLEDGE_GRAPH = "knowledge_graph"
    COMMUNITY_DATABASE = "community_database"
    CULTURAL_HERITAGE = "cultural_heritage"
    GOVERNMENT_DATA = "government_data"
    PLATFORM_API = "platform_api"
    COMMERCIAL_FEED = "commercial_feed"


class SchemaCompatibility(StrEnum):
    FIXED = "fixed"
    ADDITIVE = "additive"
    VERSIONED = "versioned"


class SourceSystem(V2ContractModel):
    source_system_id: str
    name: str
    operator: str
    homepage: str
    status: RegistryStatus = RegistryStatus.ACTIVE

    @field_validator("source_system_id")
    @classmethod
    def validate_id(cls, value: str) -> str:
        return require_slug(value, label="source_system_id")

    @field_validator("name", "operator")
    @classmethod
    def validate_names(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized or len(normalized) > 256:
            raise ValueError("source system names must be non-empty")
        return normalized

    @field_validator("homepage")
    @classmethod
    def validate_homepage(cls, value: str) -> str:
        return require_https_url(value, label="source homepage")


class SourceProduct(V2ContractModel):
    source_product_id: str
    source_system_id: str
    name: str
    kind: SourceProductKind
    policy_id: str
    connector_id: str
    documentation_url: str
    status: RegistryStatus = RegistryStatus.ACTIVE

    @field_validator(
        "source_product_id",
        "source_system_id",
        "policy_id",
        "connector_id",
    )
    @classmethod
    def validate_ids(cls, value: str) -> str:
        return require_slug(value, label="source product reference")

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized or len(normalized) > 256:
            raise ValueError("source product name must be non-empty")
        return normalized

    @field_validator("documentation_url")
    @classmethod
    def validate_documentation_url(cls, value: str) -> str:
        return require_https_url(value, label="source documentation URL")


class SourceNamespace(V2ContractModel):
    namespace_id: str
    source_product_id: str
    issuer: str
    referent_kinds: tuple[str, ...]
    identifier_pattern: str | None = None
    case_sensitive: bool = True

    @field_validator("namespace_id", "source_product_id")
    @classmethod
    def validate_ids(cls, value: str) -> str:
        return require_slug(value, label="source namespace reference")

    @field_validator("issuer")
    @classmethod
    def validate_issuer(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized or len(normalized) > 256:
            raise ValueError("namespace issuer must be non-empty")
        return normalized

    @field_validator("referent_kinds")
    @classmethod
    def normalize_referent_kinds(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(sorted({item.strip().upper() for item in value if item}))
        if not normalized:
            raise ValueError("source namespace requires referent_kinds")
        return normalized

    @field_validator("identifier_pattern")
    @classmethod
    def validate_identifier_pattern(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if len(value) > 512:
            raise ValueError("identifier_pattern is too long")
        try:
            re.compile(value)
        except re.error as exc:
            raise ValueError("identifier_pattern must be a valid regex") from exc
        return value

    def accepts(self, value: str) -> bool:
        if self.identifier_pattern is None:
            return bool(value)
        flags = 0 if self.case_sensitive else re.IGNORECASE
        return re.fullmatch(self.identifier_pattern, value, flags=flags) is not None


class SchemaContract(V2ContractModel):
    schema_id: str
    schema_version: str
    media_type: str
    compatibility: SchemaCompatibility
    schema_digest: str
    schema_url: str | None = None

    @field_validator("schema_id")
    @classmethod
    def validate_schema_id(cls, value: str) -> str:
        return require_slug(value, label="schema_id")

    @field_validator("schema_version", "media_type")
    @classmethod
    def validate_text(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized or len(normalized) > 256:
            raise ValueError("schema metadata must be non-empty")
        return normalized

    @field_validator("schema_digest")
    @classmethod
    def validate_schema_digest(cls, value: str) -> str:
        return require_sha256(value, label="schema_digest")

    @field_validator("schema_url")
    @classmethod
    def validate_schema_url(cls, value: str | None) -> str | None:
        return None if value is None else require_https_url(value, label="schema URL")


class SourceRegistrySnapshot(V2ContractModel):
    """A complete, deterministic registry snapshot used by one ingest run."""

    schema_version: str = "2.0"
    registry_id: str
    source_systems: tuple[SourceSystem, ...]
    source_products: tuple[SourceProduct, ...]
    source_namespaces: tuple[SourceNamespace, ...]
    schema_contracts: tuple[SchemaContract, ...]
    rights_profiles: tuple[RightsProfile, ...]

    @field_validator("registry_id")
    @classmethod
    def validate_registry_id(cls, value: str) -> str:
        return require_slug(value, label="registry_id")

    @field_validator("source_systems")
    @classmethod
    def sort_systems(cls, value: tuple[SourceSystem, ...]) -> tuple[SourceSystem, ...]:
        return tuple(sorted(value, key=lambda item: item.source_system_id))

    @field_validator("source_products")
    @classmethod
    def sort_products(
        cls, value: tuple[SourceProduct, ...]
    ) -> tuple[SourceProduct, ...]:
        return tuple(sorted(value, key=lambda item: item.source_product_id))

    @field_validator("source_namespaces")
    @classmethod
    def sort_namespaces(
        cls, value: tuple[SourceNamespace, ...]
    ) -> tuple[SourceNamespace, ...]:
        return tuple(sorted(value, key=lambda item: item.namespace_id))

    @field_validator("schema_contracts")
    @classmethod
    def sort_schemas(
        cls, value: tuple[SchemaContract, ...]
    ) -> tuple[SchemaContract, ...]:
        return tuple(
            sorted(value, key=lambda item: (item.schema_id, item.schema_version))
        )

    @field_validator("rights_profiles")
    @classmethod
    def sort_policies(
        cls, value: tuple[RightsProfile, ...]
    ) -> tuple[RightsProfile, ...]:
        return tuple(sorted(value, key=lambda item: item.policy_id))

    @model_validator(mode="after")
    def validate_references(self) -> Self:
        systems = _unique_map(
            self.source_systems,
            lambda item: item.source_system_id,
            "source_system_id",
        )
        policies = _unique_map(
            self.rights_profiles,
            lambda item: item.policy_id,
            "policy_id",
        )
        products = _unique_map(
            self.source_products,
            lambda item: item.source_product_id,
            "source_product_id",
        )
        _unique_map(
            self.source_namespaces,
            lambda item: item.namespace_id,
            "namespace_id",
        )
        _unique_map(
            self.schema_contracts,
            lambda item: f"{item.schema_id}@{item.schema_version}",
            "schema contract",
        )
        for product in self.source_products:
            if product.source_system_id not in systems:
                raise ValueError(
                    f"unknown source_system_id: {product.source_system_id}"
                )
            if product.policy_id not in policies:
                raise ValueError(f"unknown policy_id: {product.policy_id}")
        for namespace in self.source_namespaces:
            if namespace.source_product_id not in products:
                raise ValueError(
                    f"unknown source_product_id: {namespace.source_product_id}"
                )
        return self

    @property
    def digest(self) -> str:
        return digest_identity(
            self.model_dump(mode="json", by_alias=True, exclude_none=True)
        )


def _unique_map(items, key, label: str):
    result = {}
    for item in items:
        identity = key(item)
        if identity in result:
            raise ValueError(f"duplicate {label}: {identity}")
        result[identity] = item
    return result
