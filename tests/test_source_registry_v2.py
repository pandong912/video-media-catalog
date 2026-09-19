from __future__ import annotations

import pytest

from video_media_catalog.community_sources import build_community_registry
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
        "media-catalog-v1",
        "tvmaze-public-api",
    }
    wikidata = next(
        item for item in first.source_namespaces if item.namespace_id == "wikidata-item"
    )
    assert wikidata.accepts("Q42")
    assert not wikidata.accepts("42")


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
