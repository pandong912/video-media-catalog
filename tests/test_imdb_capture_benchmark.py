from __future__ import annotations

from argparse import Namespace

import pytest

from video_media_catalog.imdb_capture_benchmark import (
    run_imdb_capture_benchmark,
)
from video_media_catalog.imdb_capture_benchmark_cli import run


def test_imdb_capture_benchmark_compares_complete_outputs(tmp_path) -> None:
    report = run_imdb_capture_benchmark(
        tmp_path,
        rows_per_dataset=20,
        parallelism=2,
        record_shard_bytes=4096,
    )

    assert report.total_records == 140
    assert report.serial.record_count == report.parallel.record_count == 140
    assert report.serial.shard_count >= 7
    assert report.parallel.shard_count >= 7
    assert report.serial.rows_per_second > 0
    assert report.parallel.rows_per_second > 0
    assert report.serial.logical_digest == report.parallel.logical_digest
    assert report.speedup > 0


def test_imdb_capture_benchmark_requires_large_scale_confirmation() -> None:
    with pytest.raises(ValueError, match="confirm-large-scale"):
        run(
            Namespace(
                rows_per_dataset=100_001,
                parallelism=7,
                record_shard_bytes=4096,
                work_dir=None,
                confirm_large_scale=False,
            )
        )
