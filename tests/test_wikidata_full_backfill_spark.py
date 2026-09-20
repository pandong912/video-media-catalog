from __future__ import annotations

import bz2
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.wikidata_full_backfill import FullMediaBackfillConfig
from video_media_catalog.wikidata_full_backfill_spark import build_full_media_backfill
from video_media_catalog.wikidata_subset_spark import (
    configure_bfs_materialize_dir,
    normalize_dump,
)


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    scratch = tmp_path_factory.mktemp("full-media-bfs")
    session = (
        SparkSession.builder.master("local[2]")
        .appName("video-media-catalog-wikidata-full-media-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    configure_bfs_materialize_dir(session, scratch.as_uri())
    yield session
    session.stop()


@pytest.mark.spark
def test_small_fixture_profiles_full_media_selection(
    spark: SparkSession,
    fixture_dir: Path,
    tmp_path: Path,
) -> None:
    dump_path = tmp_path / "wikidata-20260901-all.json.bz2"
    with bz2.open(dump_path, "wb") as handle:
        handle.write((fixture_dir / "wikidata.json").read_bytes())
    normalized = normalize_dump(spark, dump_path.as_uri())
    dump = ObjectRef(
        uri=dump_path.as_uri(),
        format="OBJECT_FORMAT_OTHER",
        media_type="application/octet-stream",
        checksum=Checksum(value="a" * 64),
        size_bytes=dump_path.stat().st_size,
        created_at="2026-09-01T00:00:00Z",
    )
    config = FullMediaBackfillConfig(
        target_shard_bytes=512,
        max_shard_bytes=4096,
        max_shards_per_partition=8,
        max_partitions_per_epoch=4,
        max_epochs=4,
    )
    build = build_full_media_backfill(
        spark,
        normalized,
        dump=dump,
        config=config,
        image_digest="sha256:" + ("b" * 64),
        dump_date="20260901",
        acquired_at="2026-09-01T00:00:00Z",
    )
    try:
        assert build.profile.record_count == 9
        assert build.profile.root_counts["MOVIE"] == 2
        assert build.profile.parent_count == 0
        assert build.profile.parent_edge_count >= 1
        assert build.profile.credit_person_count >= 2
        assert build.profile.estimated_shards >= 1
        assert build.batch.connector_id == "wikidata-full-media-backfill"
        assert build.batch.watermark_after == "20260901"
        assert build.batch.delete_coverage.value == "SNAPSHOT_DIFF"
    finally:
        build.unpersist()
        normalized.unpersist()
