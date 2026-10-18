from __future__ import annotations

import hashlib

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import SparkSession

from video_media_catalog.community_iceberg import CommunityCatalogTables
from video_media_catalog.community_ingest import (
    IngestRunKind,
    build_community_ingest_run,
)
from video_media_catalog.community_snapshot import build_committed_run_digest
from video_media_catalog.community_tables import DATA_TABLE_COLUMNS
from video_media_catalog.iceberg import CatalogConfig
from video_media_catalog.research_silver_cli import (
    _incremental_unprocessed_source_runs_dataframe,
    _validate_epoch_delta,
)


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[2]")
        .appName("silver-epoch-summary-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.mark.spark
def test_committed_run_summary_collects_deterministic_buckets(
    spark: SparkSession,
    tmp_path,
) -> None:
    runs = (
        "sha256:aa" + ("0" * 62),
        "sha256:aa" + ("1" * 62),
        "sha256:bb" + ("2" * 62),
    )
    frame = spark.createDataFrame(
        [(run_id,) for run_id in reversed(runs)], "run_id STRING"
    )
    tables = CommunityCatalogTables(
        spark,
        CatalogConfig(
            catalog_name="media",
            namespace="video_media_catalog",
            warehouse=(tmp_path / "warehouse").as_uri(),
        ),
    )
    count, digest = tables.committed_run_summary(frame)
    aa_digest = hashlib.sha256("\n".join(runs[:2]).encode()).hexdigest()
    bb_digest = hashlib.sha256(runs[2].encode()).hexdigest()
    expected = build_committed_run_digest(
        run_count=3,
        buckets=(
            ("aa", 2, "sha256:" + aa_digest),
            ("bb", 1, "sha256:" + bb_digest),
        ),
    )
    assert count == 3
    assert digest == expected


@pytest.mark.spark
def test_committed_run_summary_rejects_duplicate_commit_rows(
    spark: SparkSession,
    tmp_path,
) -> None:
    run_id = "sha256:aa" + ("0" * 62)
    frame = spark.createDataFrame([(run_id,), (run_id,)], "run_id STRING")
    tables = CommunityCatalogTables(
        spark,
        CatalogConfig(
            catalog_name="media",
            namespace="video_media_catalog",
            warehouse=(tmp_path / "warehouse").as_uri(),
        ),
    )
    with pytest.raises(RuntimeError, match="duplicate run IDs"):
        tables.committed_run_summary(frame)


@pytest.mark.spark
def test_parentless_epoch_selects_pinned_runs_from_concurrent_snapshot(
    spark: SparkSession,
    tmp_path,
) -> None:
    pinned = (
        "sha256:aa" + ("0" * 62),
        "sha256:bb" + ("1" * 62),
    )
    concurrent = "sha256:cc" + ("2" * 62)
    available = spark.createDataFrame(
        [(run_id,) for run_id in (*pinned, concurrent)],
        "run_id STRING",
    )
    tables = CommunityCatalogTables(
        spark,
        CatalogConfig(
            catalog_name="media",
            namespace="video_media_catalog",
            warehouse=(tmp_path / "warehouse").as_uri(),
        ),
    )

    selected = tables.select_committed_runs(
        available,
        pinned,
        label="parentless Silver epoch",
    )

    assert {row["run_id"] for row in selected.collect()} == set(pinned)
    with pytest.raises(ValueError, match="uncommitted"):
        tables.select_committed_runs(
            available,
            (*pinned, "sha256:dd" + ("3" * 62)),
            label="parentless Silver epoch",
        )


@pytest.mark.spark
def test_epoch_delta_is_validated_with_distributed_anti_joins(
    spark: SparkSession,
) -> None:
    parent_id = "sha256:aa" + ("0" * 62)
    delta_id = "sha256:bb" + ("1" * 62)
    parent = spark.createDataFrame([(parent_id,)], "run_id STRING")
    current = spark.createDataFrame(
        [(parent_id,), (delta_id,)],
        "run_id STRING",
    )
    _validate_epoch_delta(
        spark,
        current_runs=current,
        parent_runs=parent,
        delta_run_ids=(delta_id,),
    )
    with pytest.raises(ValueError, match="do not match"):
        _validate_epoch_delta(
            spark,
            current_runs=current,
            parent_runs=parent,
            delta_run_ids=(parent_id,),
        )


@pytest.mark.spark
def test_epoch_incremental_identity_finds_distributed_unprocessed_delta(
    spark: SparkSession,
) -> None:
    counts = {table: 0 for table in DATA_TABLE_COLUMNS}

    def source_run(suffix: str):
        return build_community_ingest_run(
            run_kind=IngestRunKind.SOURCE_ASSERTIONS,
            source_product_id="tvmaze-public-api",
            input_id="sha256:" + (suffix * 64),
            policy_id="tvmaze-api-cc-by-sa",
            policy_digest="sha256:" + ("a" * 64),
            image_digest="sha256:" + ("b" * 64),
            config_digest="sha256:" + ("c" * 64),
            started_at="2026-10-18T00:00:00Z",
            expected_counts=counts,
            input_manifest={"recordSetId": "sha256:" + (suffix * 64)},
        )

    represented = source_run("1")
    delta = source_run("2")
    identity = build_community_ingest_run(
        run_kind=IngestRunKind.IDENTITY_RESOLUTION,
        source_product_id="identity-resolution-v2",
        input_id="sha256:" + ("3" * 64),
        policy_id="internal-key-continuity",
        policy_digest="sha256:" + ("d" * 64),
        image_digest="sha256:" + ("e" * 64),
        config_digest="sha256:" + ("f" * 64),
        started_at="2026-10-18T00:01:00Z",
        expected_counts=counts,
        input_manifest={
            "pinnedInputs": {"sourceRunIds": [represented.run_id]},
        },
    )
    runs = spark.createDataFrame(
        [
            (run.run_id, run.run_kind.value, run.json_bytes().decode())
            for run in (represented, delta, identity)
        ],
        "run_id STRING, run_kind STRING, manifest_json STRING",
    )

    unprocessed = _incremental_unprocessed_source_runs_dataframe(runs)

    assert [row["run_id"] for row in unprocessed.collect()] == [delta.run_id]
    with pytest.raises(ValueError, match="requires a baseline Identity run"):
        _incremental_unprocessed_source_runs_dataframe(
            runs.where("run_kind = 'SOURCE_ASSERTIONS'")
        )
    unknown_identity = build_community_ingest_run(
        run_kind=IngestRunKind.IDENTITY_RESOLUTION,
        source_product_id="identity-resolution-v2",
        input_id="sha256:" + ("4" * 64),
        policy_id="internal-key-continuity",
        policy_digest="sha256:" + ("d" * 64),
        image_digest="sha256:" + ("e" * 64),
        config_digest="sha256:" + ("f" * 64),
        started_at="2026-10-18T00:02:00Z",
        expected_counts=counts,
        input_manifest={
            "pinnedInputs": {
                "sourceRunIds": ["sha256:" + ("9" * 64)],
            },
        },
    )
    unknown_runs = runs.unionByName(
        spark.createDataFrame(
            [
                (
                    unknown_identity.run_id,
                    unknown_identity.run_kind.value,
                    unknown_identity.json_bytes().decode(),
                )
            ],
            "run_id STRING, run_kind STRING, manifest_json STRING",
        )
    )
    with pytest.raises(ValueError, match="absent from the pinned Silver epoch"):
        _incremental_unprocessed_source_runs_dataframe(unknown_runs)


@pytest.mark.spark
def test_incremental_identity_strips_only_pinned_key_overlap(
    spark: SparkSession,
) -> None:
    table = "community_entity_ledger"
    counts = {name: 0 for name in DATA_TABLE_COLUMNS}
    counts[table] = 3
    run = build_community_ingest_run(
        run_kind=IngestRunKind.IDENTITY_RESOLUTION,
        source_product_id="identity-resolution-v2",
        input_id="sha256:" + ("1" * 64),
        policy_id="internal-key-continuity",
        policy_digest="sha256:" + ("2" * 64),
        image_digest="sha256:" + ("3" * 64),
        config_digest="sha256:" + ("4" * 64),
        started_at="2026-10-18T00:01:00Z",
        expected_counts=counts,
        input_manifest={"identityMode": "incremental"},
    )
    pinned_run_id = "sha256:" + ("5" * 64)
    foreign_run_id = "sha256:" + ("6" * 64)
    incoming = spark.createDataFrame(
        [
            (run.run_id, "entity-a"),
            (run.run_id, "entity-b"),
            (run.run_id, "entity-c"),
        ],
        "run_id STRING, entity_key STRING",
    )
    pinned_runs = spark.createDataFrame([(pinned_run_id,)], "run_id STRING")

    class ExistingTableSpark:
        def __init__(self, existing):
            self.existing = existing

        def table(self, _name: str):
            return self.existing

    def strip(existing_rows):
        tables = CommunityCatalogTables(
            ExistingTableSpark(
                spark.createDataFrame(
                    existing_rows,
                    "run_id STRING, entity_key STRING",
                )
            ),
            CatalogConfig(
                catalog_name="media",
                namespace="community_v2",
                warehouse="file:///tmp/community-v2-test",
            ),
        )
        return tables._strip_existing_identity_keys(
            run,
            {table: incoming},
            pinned_identity_runs=pinned_runs,
        )

    rebuilt, prepared, foreign_tables = strip(
        [
            (pinned_run_id, "entity-a"),
            (foreign_run_id, "unrelated-entity"),
        ]
    )

    assert rebuilt.expected_counts[table] == 2
    assert foreign_tables == ()
    assert {
        row["entity_key"] for row in prepared[table].select("entity_key").collect()
    } == {"entity-b", "entity-c"}
    assert {row["run_id"] for row in prepared[table].select("run_id").collect()} == {
        rebuilt.run_id
    }

    _, _, foreign_tables = strip(
        [
            (pinned_run_id, "entity-a"),
            (foreign_run_id, "entity-c"),
        ]
    )

    assert foreign_tables == (table,)
