from __future__ import annotations

import json
from pathlib import Path

import pytest

from video_media_catalog.gold_ingest import GOLD_RELEASE_COMMIT_MEDIA_TYPE
from video_media_catalog.gold_match_parameters import (
    catalog_match_algorithm_identity,
    match_parameters_from_manifest,
)
from video_media_catalog.gold_search_index import (
    MAPPING_DIGEST,
    RESEARCH_READ_ALIAS,
    GoldIndexBuildManifest,
    gold_index_config_identity,
)
from video_media_catalog.gold_tables import GOLD_DATA_COLUMNS
from video_media_catalog.models import Checksum, ObjectRef

_ALGORITHM_CONTRACT = json.loads(
    (
        Path(__file__).parent
        / "fixtures"
        / "catalog-gold-match-algorithm.v1.json"
    ).read_text()
)


def _manifest(index: str) -> GoldIndexBuildManifest:
    config = gold_index_config_identity(
        read_alias=RESEARCH_READ_ALIAS,
        index_prefix="media-catalog-research",
        shards=1,
        replicas=0,
        bulk_chunk_size=100,
        bulk_max_chunk_bytes=5 * 1024 * 1024,
        image_digest="sha256:" + ("e" * 64),
    )
    return GoldIndexBuildManifest(
        build_id="b" * 64,
        release_plan_id="sha256:" + ("c" * 64),
        context_id="research",
        release_commit=ObjectRef(
            uri="s3://bucket/release-commit.json",
            format="OBJECT_FORMAT_JSON",
            media_type=GOLD_RELEASE_COMMIT_MEDIA_TYPE,
            checksum=Checksum(value="a" * 64),
            size_bytes=100,
            etag="etag",
            object_version="version",
        ),
        table_snapshot_ids={
            table: (10 if table == "community_gold_entity" else None)
            for table in GOLD_DATA_COLUMNS
        },
        mapping_digest=MAPPING_DIGEST,
        config_identity=config,
        config_digest=config.digest,
        document_count=1,
        gold_entity_count=1,
        successful_document_count=1,
        failed_document_count=0,
        concrete_index_document_count=1,
        partition_receipt_count=1,
        partition_receipt_digest="sha256:" + ("f" * 64),
        index=index,
        alias=RESEARCH_READ_ALIAS,
        completed_at="2026-09-19T00:00:00Z",
    )


def test_parameter_package_pins_concrete_index_and_current_mapping() -> None:
    index = "media-catalog-research-" + ("b" * 24)
    document = match_parameters_from_manifest(
        _manifest(index),
        algorithm_contract=_ALGORITHM_CONTRACT,
        search_endpoint="https://vpc-ai-video-media-catalog-dev.internal/",
        aws_region="us-east-1",
    )
    assert document["concreteIndex"] == index
    assert document["mappingDigest"] == MAPPING_DIGEST
    assert document["algorithmDigest"] == (
        "sha256:014224280780e1ce61bb381e903b047dfa426b15d9454a25ef4f09d95caf8471"
    )
    assert document["algorithmId"] == "catalog-asset-match-v1"
    assert document["searchEndpoint"].endswith(".internal")
    assert document["contextId"] == "research"


def test_parameter_package_rejects_alias_and_placeholder_endpoint() -> None:
    with pytest.raises(ValueError, match="concreteIndex"):
        match_parameters_from_manifest(
            _manifest(RESEARCH_READ_ALIAS),
            algorithm_contract=_ALGORITHM_CONTRACT,
            search_endpoint="https://vpc-ai-video-media-catalog-dev.internal",
            aws_region="us-east-1",
        )
    with pytest.raises(ValueError, match="placeholder"):
        match_parameters_from_manifest(
            _manifest("media-catalog-research-" + ("a" * 24)),
            algorithm_contract=_ALGORITHM_CONTRACT,
            search_endpoint="https://search.example.invalid",
            aws_region="us-east-1",
        )


def test_parameter_package_rejects_unknown_algorithm_contract() -> None:
    with pytest.raises(ValueError, match="algorithm ID"):
        catalog_match_algorithm_identity(
            {**_ALGORITHM_CONTRACT, "algorithmId": "catalog-asset-match-v2"}
        )
