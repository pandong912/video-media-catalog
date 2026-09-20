"""CLI for the opt-in synthetic Identity Spark benchmark."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from typing import Any

from video_media_catalog.canonical import canonical_json
from video_media_catalog.identity_benchmark import (
    run_identity_synthetic_benchmark,
)
from video_media_catalog.identity_spark import IdentityResolutionConfig

LARGE_SCALE_NODE_COUNTS = {
    "1m": 1_000_000,
    "5m": 5_000_000,
}


def build_parser() -> argparse.ArgumentParser:
    defaults = IdentityResolutionConfig()
    parser = argparse.ArgumentParser(
        prog="video-media-catalog-identity-benchmark",
        description=(
            "Run a synthetic exact-blocking Spark benchmark. The 1M/5M "
            "profiles require explicit large-scale confirmation."
        ),
    )
    parser.add_argument(
        "--scale",
        choices=("small", *LARGE_SCALE_NODE_COUNTS),
        default="small",
        type=str.lower,
    )
    parser.add_argument("--small-node-count", type=int, default=10_000)
    parser.add_argument(
        "--confirm-large-scale",
        action="store_true",
        help="required before the 1M or 5M profile can start Spark",
    )
    parser.add_argument("--component-size", type=int, default=8)
    parser.add_argument("--conflict-every-components", type=int, default=20)
    parser.add_argument("--partitions", type=int)
    parser.add_argument("--master")
    parser.add_argument(
        "--shuffle-partitions",
        type=int,
        default=64,
    )
    parser.add_argument(
        "--max-label-iterations",
        type=int,
        default=defaults.max_exact_blocking_label_iterations,
    )
    parser.add_argument(
        "--max-component-size",
        type=int,
        default=defaults.max_exact_blocking_component_size,
    )
    parser.add_argument(
        "--max-node-candidate-keys",
        type=int,
        default=defaults.max_exact_blocking_node_candidate_keys,
    )
    parser.add_argument(
        "--max-component-candidate-keys",
        type=int,
        default=defaults.max_exact_blocking_component_candidate_keys,
    )
    return parser


def benchmark_node_count(parsed: argparse.Namespace) -> int:
    if parsed.scale in LARGE_SCALE_NODE_COUNTS:
        if not parsed.confirm_large_scale:
            raise ValueError("1M/5M benchmark requires --confirm-large-scale")
        return LARGE_SCALE_NODE_COUNTS[parsed.scale]
    if parsed.small_node_count < 1:
        raise ValueError("--small-node-count must be positive")
    return int(parsed.small_node_count)


def run(parsed: argparse.Namespace) -> dict[str, Any]:
    node_count = benchmark_node_count(parsed)
    config = IdentityResolutionConfig(
        max_exact_blocking_label_iterations=parsed.max_label_iterations,
        max_exact_blocking_component_size=parsed.max_component_size,
        max_exact_blocking_node_candidate_keys=(parsed.max_node_candidate_keys),
        max_exact_blocking_component_candidate_keys=(
            parsed.max_component_candidate_keys
        ),
    )
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("video-media-catalog-identity-benchmark")
        .config("spark.ui.enabled", "false")
        .config(
            "spark.sql.shuffle.partitions",
            str(parsed.shuffle_partitions),
        )
    )
    if parsed.master:
        builder = builder.master(parsed.master)
    spark = builder.getOrCreate()
    try:
        return run_identity_synthetic_benchmark(
            spark,
            node_count=node_count,
            component_size=parsed.component_size,
            conflict_every_components=parsed.conflict_every_components,
            partitions=parsed.partitions,
            resolution_config=config,
        )
    finally:
        spark.stop()


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    parsed = parser.parse_args(argv)
    try:
        result = run(parsed)
    except ValueError as exc:
        parser.error(str(exc))
    print(canonical_json(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
