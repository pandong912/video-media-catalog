"""CLI for bounded IMDb connector throughput benchmarks."""

from __future__ import annotations

import argparse
import tempfile
from collections.abc import Sequence
from dataclasses import asdict
from pathlib import Path

from video_media_catalog.canonical import canonical_json
from video_media_catalog.imdb_capture_benchmark import (
    run_imdb_capture_benchmark,
)
from video_media_catalog.imdb_sync import DEFAULT_IMDB_RECORD_SHARD_BYTES

DEFAULT_BENCHMARK_ROWS_PER_DATASET = 10_000
MAX_UNCONFIRMED_ROWS_PER_DATASET = 100_000


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="video-media-catalog-imdb-capture-benchmark",
        description="Compare serial and dataset-parallel IMDb capture throughput.",
    )
    parser.add_argument(
        "--rows-per-dataset",
        type=int,
        default=DEFAULT_BENCHMARK_ROWS_PER_DATASET,
    )
    parser.add_argument("--parallelism", type=int, default=7)
    parser.add_argument(
        "--record-shard-bytes",
        type=int,
        default=DEFAULT_IMDB_RECORD_SHARD_BYTES,
    )
    parser.add_argument("--work-dir")
    parser.add_argument(
        "--confirm-large-scale",
        action="store_true",
        help="required above 100,000 synthetic rows per dataset",
    )
    return parser


def run(parsed: argparse.Namespace) -> dict[str, object]:
    if parsed.rows_per_dataset < 1:
        raise ValueError("rows-per-dataset must be positive")
    if (
        parsed.rows_per_dataset > MAX_UNCONFIRMED_ROWS_PER_DATASET
        and not parsed.confirm_large_scale
    ):
        raise ValueError("large benchmark requires --confirm-large-scale")
    if parsed.record_shard_bytes < 1:
        raise ValueError("record-shard-bytes must be positive")
    if parsed.work_dir:
        root = Path(parsed.work_dir).resolve()
        root.mkdir(parents=True, exist_ok=True)
        report = run_imdb_capture_benchmark(
            root,
            rows_per_dataset=parsed.rows_per_dataset,
            parallelism=parsed.parallelism,
            record_shard_bytes=parsed.record_shard_bytes,
        )
        return asdict(report)
    with tempfile.TemporaryDirectory(prefix="imdb-capture-benchmark-") as directory:
        report = run_imdb_capture_benchmark(
            Path(directory),
            rows_per_dataset=parsed.rows_per_dataset,
            parallelism=parsed.parallelism,
            record_shard_bytes=parsed.record_shard_bytes,
        )
        return asdict(report)


def main(argv: Sequence[str] | None = None) -> int:
    print(canonical_json(run(build_parser().parse_args(argv))))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
