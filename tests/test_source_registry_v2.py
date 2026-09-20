from __future__ import annotations

import pytest

from video_media_catalog.community_sources import build_community_registry
from video_media_catalog.identity_spark import exact_id_namespace_rows
from video_media_catalog.source_registry import SourceRegistrySnapshot


def test_bootstrap_community_registry_is_deterministic_and_referenced() -> None:
    first = build_community_registry()
    second = SourceRegistrySnapshot.model_validate_json(first.json_bytes())

    assert first == second
    assert first.digest == second.digest
    assert {product.source_product_id for product in first.source_products} == {
        "wikidata-json-dump",
        "eidr-public-registry",
        "identity-resolution-v2",
        "imdb-non-commercial-datasets",
        "media-catalog-v1",
        "tmdb-personal-research",
        "tvmaze-public-api",
    }
    wikidata = next(
        item for item in first.source_namespaces if item.namespace_id == "wikidata-item"
    )
    assert wikidata.accepts("Q42")
    assert not wikidata.accepts("42")
    namespaces = {item.namespace_id for item in first.source_namespaces}
    assert {
        "imdb-title",
        "imdb-name",
        "tmdb-movie",
        "tmdb-tv",
        "tmdb-person",
    } <= namespaces
    policies = {item.policy_id: item for item in first.rights_profiles}
    assert policies["imdb-personal-noncommercial"].zone.value == "research_private"
    assert policies["tmdb-personal-noncommercial"].zone.value == "research_private"


def test_registry_rejects_dangling_product_references() -> None:
    registry = build_community_registry()
    products = list(registry.source_products)
    products[0] = products[0].model_copy(update={"policy_id": "missing-policy"})
    with pytest.raises(ValueError, match="unknown policy_id"):
        SourceRegistrySnapshot(
            **{
                **registry.model_dump(mode="python"),
                "source_products": tuple(products),
            }
        )


def test_registry_rejects_duplicate_namespace() -> None:
    registry = build_community_registry()
    with pytest.raises(ValueError, match="duplicate namespace_id"):
        SourceRegistrySnapshot(
            **{
                **registry.model_dump(mode="python"),
                "source_namespaces": (
                    *registry.source_namespaces,
                    registry.source_namespaces[0],
                ),
            }
        )


def test_registry_drives_supported_exact_id_namespaces() -> None:
    registry = build_community_registry()
    namespaces = {
        namespace.namespace_id: namespace for namespace in registry.source_namespaces
    }
    assert {
        "wikidata-item",
        "imdb-title",
        "imdb-name",
        "tmdb-movie",
        "tmdb-tv",
        "tmdb-person",
        "eidr-content",
        "tvmaze-show",
    }.issubset(namespaces)
    assert namespaces["imdb-title"].normalize("tt0000001") == "TT0000001"
    assert (
        namespaces["eidr-content"].normalize("10.5240/aaaa-bbbb-cccc-dddd-eeee-f")
        == "10.5240/AAAA-BBBB-CCCC-DDDD-EEEE-F"
    )

    rows = exact_id_namespace_rows(registry)
    imdb = {
        (row["scheme"], row["referent_kind"])
        for row in rows
        if row["namespace_id"] == "imdb-title"
    }
    assert ("imdb", "SERIES") in imdb
    assert ("imdb-title", "EDITORIAL_WORK") in imdb
