"""Portable Catalog source policy plus local implementation bindings."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType
from typing import Any, Literal, Self
from urllib.parse import urlsplit

from pydantic import Field, field_validator, model_validator

from video_media_catalog.canonical import canonical_json_bytes, sha256_digest
from video_media_catalog.v2_contracts import V2ContractModel, require_slug

CATALOG_SOURCE_POLICY_SCHEMA_VERSION = "video.world.catalog-source-policy-registry.v1"
CATALOG_SOURCE_POLICY_DIGEST = (
    "sha256:f752400d08b822099c77c489d605b21fb72b4b9b325c6bcce681ac31a361c859"
)
_REQUIRED_CANONICAL_SOURCE_IDS = frozenset(
    {
        "wikidata",
        "eidr",
        "imdb",
        "tvmaze",
        "tmdb",
        "openfootball",
        "statsbomb",
        "madb",
    }
)
_EXPECTED_IMPLEMENTATION_SOURCE_MAP = {
    "wikidata-json-dump": "wikidata",
    "eidr-public-registry": "eidr",
    "imdb-non-commercial-datasets": "imdb",
    "tvmaze-public-api": "tvmaze",
    "tmdb-research": "tmdb",
    "openfootball-git": "openfootball",
    "statsbomb-open-data": "statsbomb",
    "madb-public-data": "madb",
}
_TOKEN = re.compile(r"^[a-z][a-z0-9]*(?:[.-][a-z0-9]+)*$")
_SEMVER = re.compile(r"^[1-9][0-9]*\.[0-9]+\.[0-9]+$")


class CatalogSourceActivation(StrEnum):
    ENABLED = "enabled"
    PENDING = "pending"
    EXCLUDED = "excluded"


class CatalogCanonicalSourceStatus(StrEnum):
    APPROVED_FOR_OFFLINE_IDENTITY = "APPROVED_FOR_OFFLINE_IDENTITY"
    PENDING_LEGAL_REVIEW = "PENDING_LEGAL_REVIEW"
    EXCLUDED_FROM_V1 = "EXCLUDED_FROM_V1"


class CatalogCanonicalCapability(StrEnum):
    RETRIEVAL = "RETRIEVAL"
    RANKING = "RANKING"
    IDENTITY = "IDENTITY"
    MATERIALIZATION = "MATERIALIZATION"
    DISPLAY = "DISPLAY"


class CatalogSourceCapability(StrEnum):
    """Catalog implementation operations gated by the portable policy."""

    CAPTURE = "capture"
    FRESHNESS = "freshness"
    IDENTITY_SPINE = "identity_spine"
    MATERIALIZATION = "materialization"
    GOLD_PROJECTION = "gold_projection"


_CANONICAL_REQUIREMENTS = {
    CatalogSourceCapability.CAPTURE: frozenset({CatalogCanonicalCapability.RETRIEVAL}),
    CatalogSourceCapability.FRESHNESS: frozenset(
        {CatalogCanonicalCapability.RETRIEVAL}
    ),
    CatalogSourceCapability.IDENTITY_SPINE: frozenset(
        {CatalogCanonicalCapability.IDENTITY}
    ),
    CatalogSourceCapability.MATERIALIZATION: frozenset(
        {CatalogCanonicalCapability.MATERIALIZATION}
    ),
    CatalogSourceCapability.GOLD_PROJECTION: frozenset(
        {
            CatalogCanonicalCapability.IDENTITY,
            CatalogCanonicalCapability.DISPLAY,
        }
    ),
}


def _canonical_token(value: str, *, label: str) -> str:
    normalized = value.strip().lower()
    if len(normalized) > 128 or _TOKEN.fullmatch(normalized) is None:
        raise ValueError(f"{label} must be a canonical Catalog token")
    return normalized


class CatalogCanonicalNamespacePolicy(V2ContractModel):
    namespace: str
    aliases: tuple[str, ...] = ()
    materializable_predicates: tuple[str, ...] = ()

    @field_validator("namespace")
    @classmethod
    def validate_namespace(cls, value: str) -> str:
        return _canonical_token(value, label="namespace")

    @field_validator("aliases", "materializable_predicates")
    @classmethod
    def validate_tokens(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(
            _canonical_token(item, label="Catalog policy token") for item in value
        )
        if len(normalized) != len(set(normalized)):
            raise ValueError("Catalog policy token array contains duplicates")
        return normalized

    @model_validator(mode="after")
    def validate_aliases(self) -> Self:
        if len(self.aliases) > 16 or len(self.materializable_predicates) > 32:
            raise ValueError("Catalog namespace policy exceeds its bound")
        if self.namespace in self.aliases:
            raise ValueError("Catalog namespace alias repeats its canonical name")
        return self

    @property
    def matching_namespaces(self) -> tuple[str, ...]:
        return tuple(sorted((self.namespace, *self.aliases)))


class CatalogCanonicalSourcePolicy(V2ContractModel):
    source_id: str
    status: CatalogCanonicalSourceStatus
    license_id: str
    license_uri: str
    attribution: str
    capabilities: tuple[CatalogCanonicalCapability, ...]
    namespaces: tuple[CatalogCanonicalNamespacePolicy, ...]

    @field_validator("source_id")
    @classmethod
    def validate_source_id(cls, value: str) -> str:
        return _canonical_token(value, label="sourceId")

    @field_validator("license_id")
    @classmethod
    def validate_license_id(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized or len(normalized) > 128:
            raise ValueError("Catalog licenseId is invalid")
        return normalized

    @field_validator("license_uri")
    @classmethod
    def validate_license_uri(cls, value: str) -> str:
        normalized = value.strip()
        parsed = urlsplit(normalized)
        if (
            len(normalized) > 2048
            or parsed.scheme not in {"http", "https"}
            or not parsed.netloc
        ):
            raise ValueError("Catalog licenseUri must be HTTP(S)")
        return normalized

    @field_validator("attribution")
    @classmethod
    def validate_attribution(cls, value: str) -> str:
        normalized = value.strip()
        if len(normalized) > 512:
            raise ValueError("Catalog attribution exceeds its bound")
        return normalized

    @field_validator("capabilities")
    @classmethod
    def validate_capabilities(
        cls,
        value: tuple[CatalogCanonicalCapability, ...],
    ) -> tuple[CatalogCanonicalCapability, ...]:
        if not value or len(value) > 5 or len(value) != len(set(value)):
            raise ValueError("Catalog source capabilities are invalid")
        return value

    @field_validator("namespaces")
    @classmethod
    def validate_namespaces(
        cls,
        value: tuple[CatalogCanonicalNamespacePolicy, ...],
    ) -> tuple[CatalogCanonicalNamespacePolicy, ...]:
        canonical = [item.namespace for item in value]
        if not value or len(value) > 32 or len(canonical) != len(set(canonical)):
            raise ValueError("Catalog source namespaces are invalid")
        return value

    @model_validator(mode="after")
    def validate_authorization(self) -> Self:
        capabilities = set(self.capabilities)
        if CatalogCanonicalCapability.RETRIEVAL not in capabilities:
            raise ValueError("Catalog source must allow retrieval")
        if (
            self.status != CatalogCanonicalSourceStatus.APPROVED_FOR_OFFLINE_IDENTITY
            and capabilities
            & {
                CatalogCanonicalCapability.IDENTITY,
                CatalogCanonicalCapability.MATERIALIZATION,
            }
        ):
            raise ValueError(
                "pending or excluded Catalog source cannot drive identity spine"
            )
        if (
            CatalogCanonicalCapability.MATERIALIZATION in capabilities
            and CatalogCanonicalCapability.IDENTITY not in capabilities
        ):
            raise ValueError("Catalog materialization requires identity capability")
        if CatalogCanonicalCapability.MATERIALIZATION not in capabilities and any(
            item.materializable_predicates for item in self.namespaces
        ):
            raise ValueError(
                "non-materializing Catalog source cannot list materializable predicates"
            )
        return self

    def allows(self, capability: CatalogCanonicalCapability) -> bool:
        if capability in {
            CatalogCanonicalCapability.IDENTITY,
            CatalogCanonicalCapability.MATERIALIZATION,
        } and (
            self.status != CatalogCanonicalSourceStatus.APPROVED_FOR_OFFLINE_IDENTITY
        ):
            return False
        return capability in self.capabilities

    @property
    def activation(self) -> CatalogSourceActivation:
        return {
            CatalogCanonicalSourceStatus.APPROVED_FOR_OFFLINE_IDENTITY: (
                CatalogSourceActivation.ENABLED
            ),
            CatalogCanonicalSourceStatus.PENDING_LEGAL_REVIEW: (
                CatalogSourceActivation.PENDING
            ),
            CatalogCanonicalSourceStatus.EXCLUDED_FROM_V1: (
                CatalogSourceActivation.EXCLUDED
            ),
        }[self.status]


class CatalogCanonicalPolicyRegistry(V2ContractModel):
    schema_version: Literal["video.world.catalog-source-policy-registry.v1"] = (
        CATALOG_SOURCE_POLICY_SCHEMA_VERSION
    )
    registry_version: str
    sources: tuple[CatalogCanonicalSourcePolicy, ...] = Field(
        min_length=1,
        max_length=32,
    )

    @field_validator("registry_version")
    @classmethod
    def validate_registry_version(cls, value: str) -> str:
        normalized = value.strip()
        if _SEMVER.fullmatch(normalized) is None:
            raise ValueError("Catalog source-policy version is invalid")
        return normalized

    @field_validator("sources")
    @classmethod
    def validate_sources(
        cls,
        value: tuple[CatalogCanonicalSourcePolicy, ...],
    ) -> tuple[CatalogCanonicalSourcePolicy, ...]:
        source_ids = [item.source_id for item in value]
        if len(source_ids) != len(set(source_ids)):
            raise ValueError("Catalog source-policy source IDs are duplicated")
        if set(source_ids) != _REQUIRED_CANONICAL_SOURCE_IDS:
            raise ValueError(
                "Catalog source-policy must contain exactly the required v1 sources"
            )
        return value

    @model_validator(mode="after")
    def validate_namespace_claims(self) -> Self:
        claimed: set[str] = set()
        for source in self.sources:
            for namespace in source.namespaces:
                overlap = claimed & set(namespace.matching_namespaces)
                if overlap:
                    raise ValueError(
                        "Catalog namespace is claimed more than once: "
                        + ", ".join(sorted(overlap))
                    )
                claimed.update(namespace.matching_namespaces)
        return self

    @property
    def digest(self) -> str:
        return sha256_digest(
            canonical_json_bytes(
                self.model_dump(mode="json", by_alias=True, exclude_none=True)
            )
        )

    def source(self, source_id: str) -> CatalogCanonicalSourcePolicy:
        normalized = _canonical_token(source_id, label="sourceId")
        for source in self.sources:
            if source.source_id == normalized:
                return source
        raise ValueError(f"unknown canonical Catalog source: {normalized}")

    def resolve_namespace(
        self,
        namespace: str,
    ) -> tuple[CatalogCanonicalSourcePolicy, CatalogCanonicalNamespacePolicy]:
        normalized = _canonical_token(namespace, label="namespace")
        for source in self.sources:
            for policy in source.namespaces:
                if normalized in policy.matching_namespaces:
                    return source, policy
        raise ValueError(f"unknown canonical Catalog namespace: {normalized}")


class CatalogNamespaceImplementation(V2ContractModel):
    namespace_id: str
    canonical_namespace: str

    @field_validator("namespace_id")
    @classmethod
    def validate_namespace_id(cls, value: str) -> str:
        return require_slug(value, label="Catalog implementation namespace")

    @field_validator("canonical_namespace")
    @classmethod
    def validate_canonical_namespace(cls, value: str) -> str:
        return _canonical_token(value, label="canonical namespace")


class CatalogSourceImplementation(V2ContractModel):
    """Technical support metadata; never an independent authorization."""

    source_product_id: str
    source_id: str
    rights_policy_id: str
    supported_capabilities: tuple[CatalogSourceCapability, ...]
    namespaces: tuple[CatalogNamespaceImplementation, ...]

    @field_validator("source_product_id", "rights_policy_id")
    @classmethod
    def validate_internal_ids(cls, value: str) -> str:
        return require_slug(value, label="Catalog implementation reference")

    @field_validator("source_id")
    @classmethod
    def validate_source_id(cls, value: str) -> str:
        return _canonical_token(value, label="canonical sourceId")

    @field_validator("supported_capabilities")
    @classmethod
    def normalize_capabilities(
        cls,
        value: tuple[CatalogSourceCapability, ...],
    ) -> tuple[CatalogSourceCapability, ...]:
        if not value:
            raise ValueError("Catalog implementation requires capabilities")
        return tuple(sorted(set(value), key=lambda item: item.value))

    @field_validator("namespaces")
    @classmethod
    def validate_namespace_mappings(
        cls,
        value: tuple[CatalogNamespaceImplementation, ...],
    ) -> tuple[CatalogNamespaceImplementation, ...]:
        namespace_ids = [item.namespace_id for item in value]
        if not value or len(namespace_ids) != len(set(namespace_ids)):
            raise ValueError("Catalog implementation namespace mappings must be unique")
        return tuple(sorted(value, key=lambda item: item.namespace_id))


_ALL_TECHNICAL_CAPABILITIES = tuple(CatalogSourceCapability)

CATALOG_SOURCE_IMPLEMENTATIONS = (
    CatalogSourceImplementation(
        source_product_id="wikidata-json-dump",
        source_id="wikidata",
        rights_policy_id="wikidata-structured-data-cc0",
        supported_capabilities=_ALL_TECHNICAL_CAPABILITIES,
        namespaces=(
            CatalogNamespaceImplementation(
                namespace_id="wikidata-item",
                canonical_namespace="wikidata-item",
            ),
        ),
    ),
    CatalogSourceImplementation(
        source_product_id="eidr-public-registry",
        source_id="eidr",
        rights_policy_id="eidr-public-registry",
        supported_capabilities=(
            CatalogSourceCapability.CAPTURE,
            CatalogSourceCapability.IDENTITY_SPINE,
            CatalogSourceCapability.MATERIALIZATION,
            CatalogSourceCapability.GOLD_PROJECTION,
        ),
        namespaces=(
            CatalogNamespaceImplementation(
                namespace_id="eidr-content",
                canonical_namespace="eidr-content",
            ),
        ),
    ),
    CatalogSourceImplementation(
        source_product_id="imdb-non-commercial-datasets",
        source_id="imdb",
        rights_policy_id="imdb-research-noncommercial",
        supported_capabilities=_ALL_TECHNICAL_CAPABILITIES,
        namespaces=(
            CatalogNamespaceImplementation(
                namespace_id="imdb-title",
                canonical_namespace="imdb-title",
            ),
        ),
    ),
    CatalogSourceImplementation(
        source_product_id="tvmaze-public-api",
        source_id="tvmaze",
        rights_policy_id="tvmaze-api-cc-by-sa",
        supported_capabilities=_ALL_TECHNICAL_CAPABILITIES,
        namespaces=(
            CatalogNamespaceImplementation(
                namespace_id="tvmaze-show",
                canonical_namespace="tvmaze-show",
            ),
        ),
    ),
    CatalogSourceImplementation(
        source_product_id="tmdb-research",
        source_id="tmdb",
        rights_policy_id="tmdb-research-noncommercial",
        supported_capabilities=_ALL_TECHNICAL_CAPABILITIES,
        namespaces=(
            CatalogNamespaceImplementation(
                namespace_id="tmdb-movie",
                canonical_namespace="tmdb-movie",
            ),
            CatalogNamespaceImplementation(
                namespace_id="tmdb-tv",
                canonical_namespace="tmdb-tv",
            ),
        ),
    ),
    CatalogSourceImplementation(
        source_product_id="openfootball-git",
        source_id="openfootball",
        rights_policy_id="openfootball-cc0",
        supported_capabilities=_ALL_TECHNICAL_CAPABILITIES,
        namespaces=(
            CatalogNamespaceImplementation(
                namespace_id="openfootball-competition",
                canonical_namespace="openfootball-competition",
            ),
            CatalogNamespaceImplementation(
                namespace_id="openfootball-event",
                canonical_namespace="openfootball-event",
            ),
            CatalogNamespaceImplementation(
                namespace_id="openfootball-team",
                canonical_namespace="openfootball-team",
            ),
        ),
    ),
    CatalogSourceImplementation(
        source_product_id="statsbomb-open-data",
        source_id="statsbomb",
        rights_policy_id="statsbomb-open-data-research",
        supported_capabilities=_ALL_TECHNICAL_CAPABILITIES,
        namespaces=(
            CatalogNamespaceImplementation(
                namespace_id="statsbomb-match",
                canonical_namespace="statsbomb-match",
            ),
        ),
    ),
    CatalogSourceImplementation(
        source_product_id="madb-public-data",
        source_id="madb",
        rights_policy_id="madb-public-data-attributed",
        supported_capabilities=_ALL_TECHNICAL_CAPABILITIES,
        namespaces=(
            CatalogNamespaceImplementation(
                namespace_id="madb-work",
                canonical_namespace="madb-work",
            ),
        ),
    ),
)


@dataclass(frozen=True)
class CatalogSourcePolicyEntry:
    implementation: CatalogSourceImplementation
    canonical: CatalogCanonicalSourcePolicy

    @property
    def source_product_id(self) -> str:
        return self.implementation.source_product_id

    @property
    def source_id(self) -> str:
        return self.canonical.source_id

    @property
    def rights_policy_id(self) -> str:
        return self.implementation.rights_policy_id

    @property
    def activation(self) -> CatalogSourceActivation:
        return self.canonical.activation

    @property
    def supported_capabilities(self) -> tuple[CatalogSourceCapability, ...]:
        return self.implementation.supported_capabilities

    @property
    def enabled_capabilities(self) -> tuple[CatalogSourceCapability, ...]:
        return tuple(
            capability
            for capability in self.supported_capabilities
            if self.allows(capability)
        )

    def allows(self, capability: CatalogSourceCapability) -> bool:
        return capability in self.supported_capabilities and all(
            self.canonical.allows(required)
            for required in _CANONICAL_REQUIREMENTS[capability]
        )


@dataclass(frozen=True)
class CatalogNamespacePolicyEntry:
    implementation: CatalogNamespaceImplementation
    source: CatalogSourcePolicyEntry
    canonical: CatalogCanonicalNamespacePolicy

    @property
    def namespace_id(self) -> str:
        return self.implementation.namespace_id

    @property
    def source_product_id(self) -> str:
        return self.source.source_product_id

    @property
    def activation(self) -> CatalogSourceActivation:
        return self.source.activation

    @property
    def supported_capabilities(self) -> tuple[CatalogSourceCapability, ...]:
        return self.source.supported_capabilities

    @property
    def enabled_capabilities(self) -> tuple[CatalogSourceCapability, ...]:
        return tuple(
            capability
            for capability in self.supported_capabilities
            if self.allows(capability)
        )

    def allows(self, capability: CatalogSourceCapability) -> bool:
        return self.source.allows(capability)


@dataclass(frozen=True)
class CatalogSourcePolicy:
    """Canonical authorization intersected with local technical support."""

    canonical_registry: CatalogCanonicalPolicyRegistry
    implementations: tuple[CatalogSourceImplementation, ...]
    _sources_by_product: dict[str, CatalogSourcePolicyEntry] = field(
        init=False,
        repr=False,
        compare=False,
    )
    _namespaces_by_id: dict[str, CatalogNamespacePolicyEntry] = field(
        init=False,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        products = [item.source_product_id for item in self.implementations]
        source_ids = [item.source_id for item in self.implementations]
        if len(products) != len(set(products)):
            raise ValueError("Catalog source-product mapping is duplicated")
        if len(source_ids) != len(set(source_ids)):
            raise ValueError("canonical Catalog source mapping is conflicting")
        if set(source_ids) != _REQUIRED_CANONICAL_SOURCE_IDS:
            raise ValueError(
                "Catalog implementation mapping must cover exactly the required "
                "canonical sources"
            )
        if dict(zip(products, source_ids, strict=True)) != (
            _EXPECTED_IMPLEMENTATION_SOURCE_MAP
        ):
            raise ValueError(
                "Catalog source-product mapping differs from the explicit "
                "canonical mapping"
            )

        sources: dict[str, CatalogSourcePolicyEntry] = {}
        namespaces: dict[str, CatalogNamespacePolicyEntry] = {}
        for implementation in self.implementations:
            canonical = self.canonical_registry.source(implementation.source_id)
            source = CatalogSourcePolicyEntry(implementation, canonical)
            sources[implementation.source_product_id] = source
            for mapping in implementation.namespaces:
                canonical_source, canonical_namespace = (
                    self.canonical_registry.resolve_namespace(
                        mapping.canonical_namespace
                    )
                )
                if canonical_source.source_id != implementation.source_id:
                    raise ValueError(
                        "Catalog namespace mapping conflicts with canonical source"
                    )
                if mapping.namespace_id in namespaces:
                    raise ValueError(
                        "Catalog implementation namespace mapping is duplicated"
                    )
                namespaces[mapping.namespace_id] = CatalogNamespacePolicyEntry(
                    implementation=mapping,
                    source=source,
                    canonical=canonical_namespace,
                )
        object.__setattr__(
            self,
            "_sources_by_product",
            MappingProxyType(sources),
        )
        object.__setattr__(
            self,
            "_namespaces_by_id",
            MappingProxyType(namespaces),
        )

    @property
    def digest(self) -> str:
        return self.canonical_registry.digest

    @property
    def sources(self) -> tuple[CatalogSourcePolicyEntry, ...]:
        return tuple(
            self._sources_by_product[key] for key in sorted(self._sources_by_product)
        )

    @property
    def namespaces(self) -> tuple[CatalogNamespacePolicyEntry, ...]:
        return tuple(
            self._namespaces_by_id[key] for key in sorted(self._namespaces_by_id)
        )

    def source(self, source_product_id: str) -> CatalogSourcePolicyEntry:
        normalized = require_slug(
            source_product_id,
            label="catalog source_product_id",
        )
        try:
            return self._sources_by_product[normalized]
        except KeyError as exc:
            raise ValueError(
                f"source is absent from canonical Catalog mapping: {normalized}"
            ) from exc

    def canonical_source_id(self, source_product_id: str) -> str:
        return self.source(source_product_id).source_id

    def namespace(self, namespace_id: str) -> CatalogNamespacePolicyEntry:
        normalized = require_slug(
            namespace_id,
            label="catalog namespace_id",
        )
        try:
            return self._namespaces_by_id[normalized]
        except KeyError as exc:
            raise ValueError(
                f"namespace is absent from canonical Catalog mapping: {normalized}"
            ) from exc

    def allows_materializable_predicate(
        self,
        source_product_id: str,
        predicate: str,
    ) -> bool:
        try:
            source = self.source(source_product_id)
        except ValueError:
            return False
        normalized = _canonical_token(
            predicate,
            label="Catalog materializable predicate",
        )
        return source.allows(CatalogSourceCapability.MATERIALIZATION) and any(
            normalized in namespace.materializable_predicates
            for namespace in source.canonical.namespaces
        )

    def allows_source(
        self,
        source_product_id: str,
        capability: CatalogSourceCapability,
    ) -> bool:
        try:
            source = self.source(source_product_id)
        except ValueError:
            return False
        return source.allows(capability)

    def require_source(
        self,
        source_product_id: str,
        capability: CatalogSourceCapability,
    ) -> None:
        source = self.source(source_product_id)
        if not source.allows(capability):
            raise ValueError(
                f"{source.source_product_id} ({source.source_id}) is not enabled "
                f"for {capability.value} by canonical Catalog policy"
            )

    def enabled_source_ids(
        self,
        capability: CatalogSourceCapability,
    ) -> tuple[str, ...]:
        return tuple(
            source.source_product_id
            for source in self.sources
            if source.allows(capability)
        )

    def enabled_namespace_ids(
        self,
        capability: CatalogSourceCapability,
    ) -> tuple[str, ...]:
        return tuple(
            namespace.namespace_id
            for namespace in self.namespaces
            if namespace.allows(capability)
        )

    def validate_registry(self, registry: Any) -> None:
        products = {item.source_product_id: item for item in registry.source_products}
        namespaces = {item.namespace_id: item for item in registry.source_namespaces}
        for implementation in self.implementations:
            product = products.get(implementation.source_product_id)
            if product is None:
                raise ValueError(
                    "Catalog source is absent from source registry: "
                    f"{implementation.source_product_id}"
                )
            if product.policy_id != implementation.rights_policy_id:
                raise ValueError(
                    "Catalog source rights policy differs for "
                    f"{implementation.source_product_id}"
                )
            for mapping in implementation.namespaces:
                registered = namespaces.get(mapping.namespace_id)
                if registered is None:
                    raise ValueError(
                        "Catalog namespace is absent from source registry: "
                        f"{mapping.namespace_id}"
                    )
                if registered.source_product_id != implementation.source_product_id:
                    raise ValueError(
                        f"Catalog namespace source differs for {mapping.namespace_id}"
                    )


CATALOG_SOURCE_POLICY_RELATIVE_PATH = Path(
    "contracts/catalog/catalog-source-policy.v1.json"
)


def _reject_duplicate_keys(
    pairs: list[tuple[str, Any]],
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key in Catalog source policy: {key}")
        result[key] = value
    return result


def default_catalog_source_policy_path() -> Path:
    repository_contract = (
        Path(__file__).resolve().parents[2] / CATALOG_SOURCE_POLICY_RELATIVE_PATH
    )
    if repository_contract.is_file():
        return repository_contract
    return Path(__file__).resolve().parent / "contracts/catalog-source-policy.v1.json"


def load_catalog_source_policy(
    path: str | Path | None = None,
    *,
    registry: Any | None = None,
    implementations: tuple[CatalogSourceImplementation, ...] | None = None,
) -> CatalogSourcePolicy:
    source = default_catalog_source_policy_path() if path is None else Path(path)
    try:
        payload = json.loads(
            source.read_text(encoding="utf-8"),
            object_pairs_hook=_reject_duplicate_keys,
        )
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot load Catalog source policy: {source}") from exc
    canonical_registry = CatalogCanonicalPolicyRegistry.model_validate(payload)
    normalized_payload = canonical_registry.model_dump(
        mode="json",
        by_alias=True,
        exclude_none=True,
    )
    if canonical_json_bytes(payload) != canonical_json_bytes(normalized_payload):
        raise ValueError("Catalog source policy changes meaning when normalized")
    if canonical_registry.digest != CATALOG_SOURCE_POLICY_DIGEST:
        raise ValueError(
            "Catalog source policy differs from the canonical semantic digest"
        )
    policy = CatalogSourcePolicy(
        canonical_registry=canonical_registry,
        implementations=(
            CATALOG_SOURCE_IMPLEMENTATIONS
            if implementations is None
            else implementations
        ),
    )
    if registry is not None:
        policy.validate_registry(registry)
    return policy
