from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from video_media_catalog.canonical import canonical_json_bytes, sha256_digest
from video_media_catalog.catalog_source_policy import (
    CATALOG_SOURCE_IMPLEMENTATIONS,
    CATALOG_SOURCE_POLICY_DIGEST,
    CatalogCanonicalCapability,
    CatalogSourceActivation,
    CatalogSourceCapability,
    CatalogSourcePolicy,
    default_catalog_source_policy_path,
    load_catalog_source_policy,
)
from video_media_catalog.community_ingest import (
    IngestRunKind,
    build_community_ingest_run,
)
from video_media_catalog.community_sources import build_community_registry
from video_media_catalog.community_tables import DATA_TABLE_COLUMNS
from video_media_catalog.gold_spark_transform import (
    _validate_identity_source_policy_provenance,
)
from video_media_catalog.identifier_blocking import exact_id_namespace_rows


def test_canonical_policy_digest_and_source_mapping_are_exact() -> None:
    artifact = default_catalog_source_policy_path()
    payload = json.loads(artifact.read_text())
    policy = load_catalog_source_policy(registry=build_community_registry())

    assert sha256_digest(canonical_json_bytes(payload)) == (
        "sha256:27749e3d25228edbcf5341873fd89b96c1ad8e8b2fcc806c6ae9256e17be5512"
    )
    assert policy.digest == CATALOG_SOURCE_POLICY_DIGEST
    assert policy.canonical_registry.registry_version == "1.3.0"
    assert {
        source.source_product_id: source.source_id for source in policy.sources
    } == {
        "eidr-public-registry": "eidr",
        "imdb-non-commercial-datasets": "imdb",
        "madb-public-data": "madb",
        "musicbrainz-core-dump": "musicbrainz",
        "openfootball-git": "openfootball",
        "statsbomb-open-data": "statsbomb",
        "tmdb-research": "tmdb",
        "tvmaze-public-api": "tvmaze",
        "wikidata-json-dump": "wikidata",
    }


def test_live_permissions_are_intersected_with_technical_support() -> None:
    policy = load_catalog_source_policy()

    expected_sources = {
        "eidr-public-registry",
        "imdb-non-commercial-datasets",
        "madb-public-data",
        "musicbrainz-core-dump",
        "openfootball-git",
        "statsbomb-open-data",
        "tmdb-research",
        "tvmaze-public-api",
        "wikidata-json-dump",
    }
    for capability in (
        CatalogSourceCapability.IDENTITY_SPINE,
        CatalogSourceCapability.MATERIALIZATION,
        CatalogSourceCapability.GOLD_PROJECTION,
    ):
        assert set(policy.enabled_source_ids(capability)) == expected_sources

    for source_product_id in expected_sources:
        source = policy.source(source_product_id)
        assert source.activation == CatalogSourceActivation.ENABLED
        assert set(source.canonical.capabilities) == {
            CatalogCanonicalCapability.RETRIEVAL,
            CatalogCanonicalCapability.RANKING,
            CatalogCanonicalCapability.IDENTITY,
            CatalogCanonicalCapability.MATERIALIZATION,
            CatalogCanonicalCapability.DISPLAY,
        }
        assert CatalogSourceCapability.IDENTITY_SPINE in (source.supported_capabilities)
        assert CatalogSourceCapability.IDENTITY_SPINE in source.enabled_capabilities
        assert policy.allows_source(
            source_product_id,
            CatalogSourceCapability.CAPTURE,
        )
        assert policy.allows_source(
            source_product_id,
            CatalogSourceCapability.MATERIALIZATION,
        )
        assert policy.allows_source(
            source_product_id,
            CatalogSourceCapability.GOLD_PROJECTION,
        )


def test_only_canonical_namespaces_can_drive_exact_identity() -> None:
    policy = load_catalog_source_policy()

    expected_namespaces = {
        "eidr-content",
        "imdb-title",
        "madb-work",
        "musicbrainz-artist",
        "musicbrainz-recording",
        "musicbrainz-release",
        "musicbrainz-release-group",
        "musicbrainz-work",
        "openfootball-competition",
        "openfootball-event",
        "openfootball-team",
        "statsbomb-match",
        "isrc",
        "iswc",
        "tmdb-movie",
        "tmdb-tv",
        "tvmaze-show",
        "wikidata-item",
    }
    assert (
        set(policy.enabled_namespace_ids(CatalogSourceCapability.IDENTITY_SPINE))
        == expected_namespaces
    )
    identity_rows = exact_id_namespace_rows(
        build_community_registry(),
        allowed_namespace_ids=policy.enabled_namespace_ids(
            CatalogSourceCapability.IDENTITY_SPINE
        ),
    )
    assert {str(row["namespace_id"]) for row in identity_rows} == expected_namespaces
    for namespace_id in (
        "douban-work",
        "eidr-alternate",
        "imdb-name",
        "thetvdb-series",
    ):
        assert namespace_id not in policy.enabled_namespace_ids(
            CatalogSourceCapability.IDENTITY_SPINE
        )


def test_materializable_predicates_are_enforced_per_canonical_source() -> None:
    policy = load_catalog_source_policy()

    assert policy.allows_materializable_predicate(
        "wikidata-json-dump",
        "identity.title",
    )
    assert policy.allows_materializable_predicate(
        "wikidata-json-dump",
        "hierarchy.parent",
    )
    assert policy.allows_materializable_predicate(
        "eidr-public-registry",
        "hierarchy.series",
    )
    assert policy.allows_materializable_predicate(
        "eidr-public-registry",
        "identity.title",
    )
    assert policy.allows_materializable_predicate(
        "imdb-non-commercial-datasets",
        "identity.title",
    )
    assert policy.allows_materializable_predicate(
        "tvmaze-public-api",
        "hierarchy.series",
    )
    assert policy.allows_materializable_predicate(
        "tmdb-research",
        "identity.alias",
    )
    assert policy.allows_materializable_predicate(
        "openfootball-git",
        "hierarchy.parent",
    )
    assert policy.allows_materializable_predicate(
        "madb-public-data",
        "identity.title",
    )
    assert policy.allows_materializable_predicate(
        "musicbrainz-core-dump",
        "identity.title",
    )
    assert not policy.allows_materializable_predicate(
        "musicbrainz-core-dump",
        "genre",
    )
    assert not policy.allows_materializable_predicate(
        "statsbomb-open-data",
        "sports.event.in-game-stats",
    )
    assert not policy.allows_materializable_predicate(
        "unknown-source",
        "identity.title",
    )


def test_unknown_source_mapping_fails_closed() -> None:
    policy = load_catalog_source_policy()

    assert not policy.allows_source(
        "unknown-source",
        CatalogSourceCapability.MATERIALIZATION,
    )
    with pytest.raises(ValueError, match="absent from canonical Catalog mapping"):
        policy.canonical_source_id("unknown-source")

    implementations = tuple(
        implementation.model_copy(update={"source_id": "unknown"})
        if implementation.source_product_id == "tmdb-research"
        else implementation
        for implementation in CATALOG_SOURCE_IMPLEMENTATIONS
    )
    with pytest.raises(ValueError, match="cover exactly"):
        CatalogSourcePolicy(
            canonical_registry=policy.canonical_registry,
            implementations=implementations,
        )

    unknown_product = tuple(
        implementation.model_copy(update={"source_product_id": "unknown-source"})
        if implementation.source_product_id == "tmdb-research"
        else implementation
        for implementation in CATALOG_SOURCE_IMPLEMENTATIONS
    )
    with pytest.raises(ValueError, match="differs from the explicit"):
        CatalogSourcePolicy(
            canonical_registry=policy.canonical_registry,
            implementations=unknown_product,
        )


def test_conflicting_source_and_namespace_mappings_fail_closed() -> None:
    policy = load_catalog_source_policy()
    conflicting_source = tuple(
        implementation.model_copy(update={"source_id": "imdb"})
        if implementation.source_product_id == "tvmaze-public-api"
        else implementation
        for implementation in CATALOG_SOURCE_IMPLEMENTATIONS
    )
    with pytest.raises(ValueError, match="mapping is conflicting"):
        CatalogSourcePolicy(
            canonical_registry=policy.canonical_registry,
            implementations=conflicting_source,
        )

    conflicting_namespace = tuple(
        implementation.model_copy(
            update={
                "namespaces": (
                    implementation.namespaces[0].model_copy(
                        update={"canonical_namespace": "wikidata-item"}
                    ),
                )
            }
        )
        if implementation.source_product_id == "tvmaze-public-api"
        else implementation
        for implementation in CATALOG_SOURCE_IMPLEMENTATIONS
    )
    with pytest.raises(ValueError, match="conflicts with canonical source"):
        CatalogSourcePolicy(
            canonical_registry=policy.canonical_registry,
            implementations=conflicting_namespace,
        )


def test_pending_source_cannot_gain_identity_capability(tmp_path: Path) -> None:
    payload = json.loads(default_catalog_source_policy_path().read_text())
    mutated = copy.deepcopy(payload)
    imdb = next(source for source in mutated["sources"] if source["sourceId"] == "imdb")
    imdb["status"] = "PENDING_LEGAL_REVIEW"
    path = tmp_path / "policy.json"
    path.write_text(json.dumps(mutated))

    with pytest.raises(ValueError, match="cannot drive identity spine"):
        load_catalog_source_policy(path)


def test_alternate_valid_artifact_cannot_replace_canonical_digest(
    tmp_path: Path,
) -> None:
    payload = json.loads(default_catalog_source_policy_path().read_text())
    payload["sources"][0]["attribution"] = "Changed attribution"
    path = tmp_path / "policy.json"
    path.write_text(json.dumps(payload))

    with pytest.raises(ValueError, match="canonical semantic digest"):
        load_catalog_source_policy(path)


def test_gold_rejects_identity_run_from_another_source_policy() -> None:
    policy = load_catalog_source_policy()

    def identity_run(source_policy_digest: str):
        return build_community_ingest_run(
            run_kind=IngestRunKind.IDENTITY_RESOLUTION,
            source_product_id="community-identity",
            input_id="sha256:" + ("1" * 64),
            policy_id="community-identity-resolution",
            policy_digest="sha256:" + ("2" * 64),
            image_digest="sha256:" + ("3" * 64),
            config_digest="sha256:" + ("4" * 64),
            started_at="2026-10-14T00:00:00Z",
            expected_counts={table: 0 for table in DATA_TABLE_COLUMNS},
            input_manifest={
                "sourcePolicyDigest": source_policy_digest,
            },
        )

    _validate_identity_source_policy_provenance(
        (identity_run(policy.digest),),
        source_policy=policy,
    )
    _validate_identity_source_policy_provenance(
        (
            identity_run("sha256:" + ("0" * 64)),
            identity_run(policy.digest),
        ),
        source_policy=policy,
    )
    with pytest.raises(ValueError, match="does not bind"):
        _validate_identity_source_policy_provenance(
            (identity_run("sha256:" + ("0" * 64)),),
            source_policy=policy,
        )
