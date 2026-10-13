from __future__ import annotations

from pathlib import Path
from typing import Any

from video_media_catalog.catalog_source_policy import load_catalog_source_policy
from video_media_catalog.gold_index_promote_cli import build_parser, run
from video_media_catalog.gold_ingest import GOLD_RELEASE_COMMIT_MEDIA_TYPE
from video_media_catalog.gold_search_index import (
    MAPPING_DIGEST,
    RESEARCH_INDEX_PREFIX,
    RESEARCH_READ_ALIAS,
    GoldIndexActivationReceipt,
    GoldIndexBuildManifest,
    gold_index_config_identity,
)
from video_media_catalog.gold_tables import GOLD_DATA_COLUMNS
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.object_store import BoundedObjectStore
from video_media_catalog.storage import digest_file


class _Indices:
    def __init__(self, index: str) -> None:
        self.index = index
        self.alias_targets = {"media-catalog-research-old"}

    def get_mapping(self, *, index: str) -> dict[str, Any]:
        assert index == self.index
        return {index: {"mappings": {"_meta": {"mappingDigest": MAPPING_DIGEST}}}}

    def refresh(self, *, index: str) -> None:
        assert index == self.index

    def get_alias(self, *, name: str) -> dict[str, Any]:
        return {index: {"aliases": {name: {}}} for index in self.alias_targets}

    def update_aliases(self, *, body: dict[str, Any]) -> None:
        for action in body["actions"]:
            if "remove" in action:
                self.alias_targets.discard(action["remove"]["index"])
            if "add" in action:
                self.alias_targets.add(action["add"]["index"])


class _Client:
    def __init__(self, index: str) -> None:
        self.indices = _Indices(index)

    def count(self, *, index: str, body: Any = None) -> dict[str, int]:
        assert index == self.indices.index
        return {"count": 3}


def _manifest(tmp_path: Path) -> GoldIndexBuildManifest:
    release_ref = ObjectRef(
        uri=(tmp_path / "release.json").as_uri(),
        format="OBJECT_FORMAT_JSON",
        media_type=GOLD_RELEASE_COMMIT_MEDIA_TYPE,
        checksum=Checksum(value="a" * 64),
        size_bytes=100,
    )
    config = gold_index_config_identity(
        read_alias=RESEARCH_READ_ALIAS,
        index_prefix=RESEARCH_INDEX_PREFIX,
        shards=1,
        replicas=0,
        bulk_chunk_size=100,
        bulk_max_chunk_bytes=4096,
        image_digest="sha256:" + ("b" * 64),
    )
    return GoldIndexBuildManifest(
        build_id="c" * 64,
        release_plan_id="sha256:" + ("d" * 64),
        release_commit=release_ref,
        table_snapshot_ids={table: 1 for table in GOLD_DATA_COLUMNS},
        mapping_digest=MAPPING_DIGEST,
        source_policy_digest=load_catalog_source_policy().digest,
        config_identity=config,
        config_digest=config.digest,
        document_count=3,
        gold_entity_count=3,
        successful_document_count=3,
        concrete_index_document_count=3,
        partition_receipt_count=1,
        partition_receipt_digest="sha256:" + ("e" * 64),
        index="media-catalog-research-" + ("c" * 24),
        alias=RESEARCH_READ_ALIAS,
        completed_at="2026-10-12T00:00:00Z",
    )


def test_promote_cli_validates_cas_and_publishes_activation(
    tmp_path: Path,
) -> None:
    manifest = _manifest(tmp_path)
    manifest_path = tmp_path / "gold-index-build.json"
    manifest_path.write_bytes(manifest.json_bytes())
    digest, size = digest_file(manifest_path)
    activation_dir = tmp_path / "activations"
    parsed = build_parser().parse_args(
        [
            "--build-manifest-uri",
            manifest_path.as_uri(),
            "--build-manifest-hash",
            digest,
            "--build-manifest-size",
            str(size),
            "--expected-build-id",
            manifest.build_id,
            "--expected-release-plan-id",
            manifest.release_plan_id,
            "--expected-old-index",
            "media-catalog-research-old",
            "--activation-prefix",
            activation_dir.as_uri(),
            "--activated-at",
            "2026-10-12T01:00:00Z",
            "--authorized-by",
            "group:world-release-managers",
            "--authorization-reason",
            "Approved after shadow validation.",
            "--opensearch-endpoint",
            "http://localhost:9200",
            "--allow-insecure-opensearch",
        ]
    )
    client = _Client(manifest.index)

    result = run(
        parsed,
        store=BoundedObjectStore(client=object()),
        client=client,
    )

    assert result["status"] == "ACTIVATED"
    assert client.indices.alias_targets == {manifest.index}
    receipt_path = activation_dir / f"gold-index-activation-{manifest.build_id}.json"
    receipt = GoldIndexActivationReceipt.model_validate_json(receipt_path.read_bytes())
    assert receipt.expected_previous_indices == ("media-catalog-research-old",)
    assert receipt.changed is True
