"""Repeatable local benchmark for IMDb dataset-parallel publication."""

from __future__ import annotations

import csv
import gzip
import hashlib
import json
import resource
import time
from dataclasses import dataclass
from pathlib import Path

from video_media_catalog.canonical import canonical_json, sha256_digest
from video_media_catalog.imdb import IMDB_DATASET_COLUMNS, IMDB_DATASET_FILES
from video_media_catalog.imdb_sync import capture_imdb_snapshot_parallel
from video_media_catalog.object_store import BoundedObjectStore
from video_media_catalog.storage import local_path


@dataclass(frozen=True)
class ImdbCaptureBenchmarkRun:
    mode: str
    parallelism: int
    record_count: int
    shard_count: int
    elapsed_seconds: float
    cpu_seconds: float
    rows_per_second: float
    output_bytes: int
    logical_digest: str


@dataclass(frozen=True)
class ImdbCaptureBenchmarkReport:
    rows_per_dataset: int
    total_records: int
    serial: ImdbCaptureBenchmarkRun
    parallel: ImdbCaptureBenchmarkRun
    speedup: float


def _synthetic_row(dataset: str, index: int) -> tuple[str, ...]:
    title_id = f"tt{index:09d}"
    name_id = f"nm{index:09d}"
    rows = {
        "title.basics.tsv.gz": (
            title_id,
            "movie",
            f"Example {index}",
            f"Example {index}",
            "0",
            "2020",
            r"\N",
            "120",
            "Drama",
        ),
        "title.akas.tsv.gz": (
            title_id,
            "1",
            f"Example {index}",
            "US",
            "en",
            "imdbDisplay",
            r"\N",
            "1",
        ),
        "title.episode.tsv.gz": (title_id, f"tt{index + 1:09d}", "1", "1"),
        "title.crew.tsv.gz": (title_id, name_id, name_id),
        "title.principals.tsv.gz": (
            title_id,
            "1",
            name_id,
            "actor",
            r"\N",
            '["Lead"]',
        ),
        "title.ratings.tsv.gz": (title_id, "8.0", "100"),
        "name.basics.tsv.gz": (
            name_id,
            f"Example Person {index}",
            "1980",
            r"\N",
            "actor",
            title_id,
        ),
    }
    return rows[dataset]


def generate_imdb_benchmark_datasets(
    root: Path,
    *,
    rows_per_dataset: int,
) -> dict[str, Path]:
    if rows_per_dataset < 1:
        raise ValueError("rows_per_dataset must be positive")
    root.mkdir(parents=True, exist_ok=True)
    paths: dict[str, Path] = {}
    for dataset in IMDB_DATASET_FILES:
        path = root / dataset
        with gzip.open(path, mode="wt", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle, delimiter="\t", lineterminator="\n")
            writer.writerow(IMDB_DATASET_COLUMNS[dataset])
            for index in range(1, rows_per_dataset + 1):
                writer.writerow(_synthetic_row(dataset, index))
        paths[dataset] = path
    return paths


def _cpu_seconds() -> float:
    own = resource.getrusage(resource.RUSAGE_SELF)
    children = resource.getrusage(resource.RUSAGE_CHILDREN)
    return own.ru_utime + own.ru_stime + children.ru_utime + children.ru_stime


def _logical_record_digest(capture) -> str:
    digest = hashlib.sha256()
    for reference in capture.record_set_manifest.record_objects:
        with local_path(reference.uri).open("rb") as handle:
            for line in handle:
                value = json.loads(line)
                digest.update(
                    canonical_json(
                        {
                            "sourceRecordId": value["sourceRecordId"],
                            "payload": json.loads(value["payloadJson"]),
                        }
                    ).encode("utf-8")
                )
                digest.update(b"\n")
    return "sha256:" + digest.hexdigest()


def _run_capture(
    *,
    mode: str,
    parallelism: int,
    dataset_paths: dict[str, Path],
    output: Path,
    record_shard_bytes: int,
) -> ImdbCaptureBenchmarkRun:
    started_cpu = _cpu_seconds()
    started = time.perf_counter()
    capture = capture_imdb_snapshot_parallel(
        dataset_paths=dataset_paths,
        destination_prefix=output.as_uri(),
        acquired_at="2026-09-20T00:00:00Z",
        image_digest="sha256:" + ("a" * 64),
        config_digest=sha256_digest(f"imdb-capture-benchmark:{mode}"),
        store=BoundedObjectStore(client=object()),
        dataset_parallelism=parallelism,
        record_shard_bytes=record_shard_bytes,
    )
    elapsed = time.perf_counter() - started
    cpu = _cpu_seconds() - started_cpu
    record_set = capture.record_set_manifest
    return ImdbCaptureBenchmarkRun(
        mode=mode,
        parallelism=parallelism,
        record_count=record_set.record_count,
        shard_count=len(record_set.record_objects),
        elapsed_seconds=elapsed,
        cpu_seconds=cpu,
        rows_per_second=record_set.record_count / elapsed,
        output_bytes=sum(item.size_bytes for item in record_set.record_objects),
        logical_digest=_logical_record_digest(capture),
    )


def run_imdb_capture_benchmark(
    root: Path,
    *,
    rows_per_dataset: int,
    parallelism: int,
    record_shard_bytes: int,
) -> ImdbCaptureBenchmarkReport:
    if not 2 <= parallelism <= len(IMDB_DATASET_FILES):
        raise ValueError("parallelism must be between 2 and 7")
    paths = generate_imdb_benchmark_datasets(
        root / "inputs",
        rows_per_dataset=rows_per_dataset,
    )
    serial = _run_capture(
        mode="serial",
        parallelism=1,
        dataset_paths=paths,
        output=root / "serial-output",
        record_shard_bytes=record_shard_bytes,
    )
    parallel = _run_capture(
        mode="parallel",
        parallelism=parallelism,
        dataset_paths=paths,
        output=root / "parallel-output",
        record_shard_bytes=record_shard_bytes,
    )
    if serial.record_count != parallel.record_count:
        raise RuntimeError("serial and parallel benchmark record counts differ")
    if serial.logical_digest != parallel.logical_digest:
        raise RuntimeError("serial and parallel benchmark records differ")
    return ImdbCaptureBenchmarkReport(
        rows_per_dataset=rows_per_dataset,
        total_records=serial.record_count,
        serial=serial,
        parallel=parallel,
        speedup=parallel.rows_per_second / serial.rows_per_second,
    )
