"""Reviewed bootstrap registry for the community-first catalog foundation."""

from __future__ import annotations

from video_media_catalog.rights import PolicyZone, RightsProfile, UsageAction
from video_media_catalog.source_registry import (
    SourceNamespace,
    SourceProduct,
    SourceProductKind,
    SourceRegistrySnapshot,
    SourceSystem,
)
from video_media_catalog.tvmaze import (
    tvmaze_registry_entries,
    tvmaze_rights_profile,
)


def _open_actions() -> tuple[UsageAction, ...]:
    return (
        UsageAction.STORE,
        UsageAction.TRANSFORM,
        UsageAction.DISPLAY,
        UsageAction.SEARCH,
        UsageAction.EXPORT,
        UsageAction.REDISTRIBUTE,
        UsageAction.DERIVE,
        UsageAction.EMBED,
        UsageAction.ML_TRAIN,
        UsageAction.ML_EVALUATE,
    )


def wikidata_rights_profile() -> RightsProfile:
    return RightsProfile(
        policy_id="wikidata-structured-data-cc0",
        policy_version="2026-09-19",
        zone=PolicyZone.OPEN_CC0,
        license_id="CC0-1.0",
        license_uri="https://creativecommons.org/publicdomain/zero/1.0/",
        terms_url="https://www.wikidata.org/wiki/Wikidata:Licensing",
        permissions=_open_actions(),
        audiences=("*",),
        territories=("*",),
        notes=(
            "Applies to Wikidata structured data, not linked Wikimedia Commons "
            "files or text from other Wikimedia projects."
        ),
    )


def eidr_rights_profile() -> RightsProfile:
    return RightsProfile(
        policy_id="eidr-public-registry",
        policy_version="2026-09-19",
        zone=PolicyZone.PUBLIC_REGISTRY,
        license_id="EIDR-Public-Registry-Terms",
        terms_url="https://www.eidr.org/our-work",
        permissions=tuple(
            action for action in _open_actions() if action != UsageAction.REDISTRIBUTE
        ),
        audiences=("*",),
        territories=("*",),
        notes=(
            "EIDR records may be used and shared, but the registry may not be "
            "repackaged and presented as a separate proprietary registry."
        ),
    )


def internal_key_continuity_profile() -> RightsProfile:
    return RightsProfile(
        policy_id="internal-key-continuity",
        policy_version="1.0",
        zone=PolicyZone.INTERNAL,
        license_id="PROJECT-INTERNAL",
        terms_url="https://github.com/pandong912/video-media-catalog",
        permissions=_open_actions(),
        audiences=("internal",),
        territories=("*",),
        notes=(
            "Covers project-created stable keys and migration metadata, not "
            "the external source facts referenced by those keys."
        ),
    )


def build_community_registry() -> SourceRegistrySnapshot:
    tvmaze_system, tvmaze_product, tvmaze_namespace = tvmaze_registry_entries()
    wikidata_system = SourceSystem(
        source_system_id="wikidata",
        name="Wikidata",
        operator="Wikimedia Foundation and Wikidata community",
        homepage="https://www.wikidata.org/",
    )
    eidr_system = SourceSystem(
        source_system_id="eidr",
        name="Entertainment Identifier Registry",
        operator="EIDR Association",
        homepage="https://www.eidr.org/",
    )
    internal_system = SourceSystem(
        source_system_id="video-media-catalog",
        name="Video Media Catalog",
        operator="video-media-catalog project",
        homepage="https://github.com/pandong912/video-media-catalog",
    )
    wikidata_product = SourceProduct(
        source_product_id="wikidata-json-dump",
        source_system_id="wikidata",
        name="Wikidata JSON entity dump",
        kind=SourceProductKind.KNOWLEDGE_GRAPH,
        policy_id="wikidata-structured-data-cc0",
        connector_id="wikidata-v1-adapter",
        documentation_url=("https://www.wikidata.org/wiki/Wikidata:Database_download"),
    )
    eidr_product = SourceProduct(
        source_product_id="eidr-public-registry",
        source_system_id="eidr",
        name="EIDR public registry records",
        kind=SourceProductKind.IDENTIFIER_REGISTRY,
        policy_id="eidr-public-registry",
        connector_id="eidr-v1-adapter",
        documentation_url="https://www.eidr.org/faq",
    )
    v1_product = SourceProduct(
        source_product_id="media-catalog-v1",
        source_system_id="video-media-catalog",
        name="Published Wikidata/EIDR v1 catalog",
        kind=SourceProductKind.INTERNAL_CATALOG,
        policy_id="internal-key-continuity",
        connector_id="media-catalog-v1-key-migration",
        documentation_url=("https://github.com/pandong912/video-media-catalog"),
    )
    return SourceRegistrySnapshot(
        registry_id="community-catalog-bootstrap",
        source_systems=(
            wikidata_system,
            eidr_system,
            internal_system,
            tvmaze_system,
        ),
        source_products=(
            wikidata_product,
            eidr_product,
            v1_product,
            tvmaze_product,
        ),
        source_namespaces=(
            SourceNamespace(
                namespace_id="wikidata-item",
                source_product_id="wikidata-json-dump",
                issuer="Wikidata",
                referent_kinds=(
                    "EDITORIAL_WORK",
                    "SERIES",
                    "SEASON",
                    "EPISODE",
                    "AGENT",
                    "ORGANIZATION",
                ),
                identifier_pattern=r"Q[1-9][0-9]*",
            ),
            SourceNamespace(
                namespace_id="eidr-content",
                source_product_id="eidr-public-registry",
                issuer="EIDR Association",
                referent_kinds=(
                    "EDITORIAL_WORK",
                    "SERIES",
                    "SEASON",
                    "EPISODE",
                    "EDIT",
                    "MANIFESTATION",
                ),
                identifier_pattern=(r"10\.5240/(?:[0-9A-Z]{4}-){5}[0-9A-Z]"),
                case_sensitive=False,
            ),
            tvmaze_namespace,
        ),
        schema_contracts=(),
        rights_profiles=(
            wikidata_rights_profile(),
            eidr_rights_profile(),
            internal_key_continuity_profile(),
            tvmaze_rights_profile(),
        ),
    )
