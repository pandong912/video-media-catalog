from __future__ import annotations

import inspect

from video_media_catalog import (
    community_iceberg,
    gold_cli,
    gold_spark_transform,
    research_silver_cli,
    source_lifecycle,
)


def test_epoch_consumers_do_not_collect_historical_run_ids() -> None:
    gold_source = inspect.getsource(gold_cli.run)
    identity_source = inspect.getsource(research_silver_cli._run_identity)
    publication_source = inspect.getsource(research_silver_cli._run_epoch_publication)
    assert ".collect()" not in gold_source
    assert ".collect()" not in identity_source
    assert ".collect()" not in publication_source
    assert "committed_runs=committed_runs" in gold_source
    assert "committed_runs=committed_runs" in identity_source


def test_run_digest_collects_only_fixed_bucket_summaries() -> None:
    source = inspect.getsource(
        community_iceberg.CommunityCatalogTables.committed_run_summary
    )
    assert 'F.substring("run_id", 8, 2)' in source
    assert '.groupBy("_bucket")' in source
    assert "fixed 256 bucket summaries" in source
    assert "collect_list" in source


def test_gold_transform_and_lifecycle_accept_distributed_run_frame() -> None:
    gold_source = inspect.getsource(gold_spark_transform.build_distributed_gold)
    lifecycle_source = inspect.getsource(source_lifecycle.bind_committed_source_records)
    assert "committed_runs: Any | None" in gold_source
    assert "committed_runs=selected_runs" in gold_source
    assert "committed_runs: Any | None" in lifecycle_source


def test_run_metadata_is_read_from_exact_snapshot() -> None:
    source = inspect.getsource(
        community_iceberg.CommunityCatalogTables.visible_run_dataframe
    )
    assert '.option("snapshot-id", str(run_snapshot_id))' in source
    assert '.load(self.table_name("community_ingest_run"))' in source
