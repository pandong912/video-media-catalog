from __future__ import annotations

import pytest

from video_media_catalog.community_sources import (
    EIDR_EXACT_LOOKUP_CONNECTOR_ID,
    build_community_registry,
)
from video_media_catalog.identity_spark import exact_id_namespace_rows
from video_media_catalog.source_registry import (
    SourceProduct,
    SourceProductKind,
    SourceRegistrySnapshot,
)
from video_media_catalog.tmdb import (
    TMDB_CHANGES_CONNECTOR_ID,
    TMDB_DAILY_CONNECTOR_ID,
)
from video_media_catalog.tvmaze import (
    TVMAZE_CONNECTOR_ID,
    TVMAZE_DELTA_CONNECTOR_ID,
)
from video_media_catalog.v1_adapters import EIDR_CONNECTOR_ID


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
        "tmdb-research",
        "tvmaze-public-api",
    }
    wikidata = next(
        item for item in first.source_namespaces if item.namespace_id == "wikidata-item"
    )
    assert wikidata.accepts("Q42")
    assert not wikidata.accepts("42")
    namespaces = {item.namespace_id for item in first.source_namespaces}
    assert {
        "douban-work",
        "douban-person",
        "imdb-title",
        "imdb-name",
        "imdb-company",
        "tmdb-movie",
        "tmdb-tv",
        "tmdb-person",
    } <= namespaces
    policies = {item.policy_id: item for item in first.rights_profiles}
    assert policies["imdb-research-noncommercial"].zone.value == "research_private"
    assert policies["tmdb-research-noncommercial"].zone.value == "research_private"


def test_registry_declares_every_source_product_connector() -> None:
    products = {
        item.source_product_id: item
        for item in build_community_registry().source_products
    }

    assert set(products["tmdb-research"].connector_ids) == {
        TMDB_DAILY_CONNECTOR_ID,
        TMDB_CHANGES_CONNECTOR_ID,
    }
    assert set(products["tvmaze-public-api"].connector_ids) == {
        TVMAZE_CONNECTOR_ID,
        TVMAZE_DELTA_CONNECTOR_ID,
    }
    assert set(products["eidr-public-registry"].connector_ids) == {
        EIDR_CONNECTOR_ID,
        EIDR_EXACT_LOOKUP_CONNECTOR_ID,
    }


def test_source_product_accepts_legacy_single_connector_field() -> None:
    product = SourceProduct.model_validate(
        {
            "sourceProductId": "legacy-research-product",
            "sourceSystemId": "legacy-research-system",
            "name": "Legacy research product",
            "kind": SourceProductKind.PLATFORM_API,
            "policyId": "legacy-research-policy",
            "connectorId": "legacy-research-connector",
            "documentationUrl": "https://example.com/research",
        }
    )

    assert product.connector_ids == ("legacy-research-connector",)
    assert "connectorIds" in product.model_dump(mode="json", by_alias=True)


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
        "douban-work",
        "douban-person",
        "imdb-title",
        "imdb-name",
        "imdb-company",
        "tmdb-movie",
        "tmdb-tv",
        "tmdb-person",
        "eidr-content",
        "tvmaze-show",
    }.issubset(namespaces)
    assert "douban-subject" not in namespaces
    assert namespaces["douban-work"].normalize("1295644") == "1295644"
    assert namespaces["douban-person"].normalize("30123456") == "30123456"
    for invalid in ("0", "01", "-1", "123/path", "\uff11\uff12\uff13"):
        assert not namespaces["douban-work"].accepts(invalid)
        with pytest.raises(ValueError, match="douban-work"):
            namespaces["douban-work"].normalize(invalid)
    assert namespaces["imdb-title"].normalize("tt0000001") == "TT0000001"
    assert namespaces["imdb-company"].normalize("co0001757") == "CO0001757"
    assert not namespaces["imdb-company"].accepts("tt0000001")
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
    imdb_company = {
        (row["scheme"], row["referent_kind"])
        for row in rows
        if row["namespace_id"] == "imdb-company"
    }
    assert ("imdb", "ORGANIZATION") in imdb_company
    assert ("imdb-company", "ORGANIZATION") in imdb_company
    douban_work = {
        (row["scheme"], row["referent_kind"])
        for row in rows
        if row["namespace_id"] == "douban-work"
    }
    assert ("douban", "EDITORIAL_WORK") in douban_work
    assert ("douban-subject", "SERIES") in douban_work
    assert ("douban-work", "EPISODE") in douban_work
    douban_person = {
        (row["scheme"], row["referent_kind"])
        for row in rows
        if row["namespace_id"] == "douban-person"
    }
    assert douban_person == {
        ("douban", "AGENT"),
        ("douban-person", "AGENT"),
        ("douban-subject", "AGENT"),
    }
