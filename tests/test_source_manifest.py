from __future__ import annotations

import hashlib
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from video_media_catalog.source_manifest import (
    SOURCE_MANIFEST_SCHEMA,
    SourceManifestEntry,
    load_source_manifest,
    write_source_manifest,
)


def _entry() -> SourceManifestEntry:
    return SourceManifestEntry(
        source="wikidata",
        uri="s3://catalog-input/wikidata.json.bz2",
        sha256="a" * 64,
        size_bytes=123,
        compression="bzip2",
        object_version="version-1",
        etag="etag-1",
        license="CC0-1.0",
    )


def test_source_manifest_schema_round_trip(tmp_path: Path) -> None:
    path = tmp_path / "source-manifest.parquet"
    second_path = tmp_path / "source-manifest-second.parquet"
    write_source_manifest(path, [_entry()])
    write_source_manifest(second_path, [_entry()])
    entries = load_source_manifest(path)

    assert entries == [_entry()]
    assert pq.ParquetFile(path).schema_arrow == SOURCE_MANIFEST_SCHEMA
    assert hashlib.sha256(path.read_bytes()).hexdigest()
    assert path.read_bytes() == second_path.read_bytes()


def test_source_manifest_rejects_unknown_columns(tmp_path: Path) -> None:
    path = tmp_path / "bad.parquet"
    table = pa.Table.from_pylist(
        [{"source": "wikidata", "uri": "s3://bucket/key", "extra": "x"}]
    )
    pq.write_table(table, path)
    with pytest.raises(ValueError, match="columns"):
        load_source_manifest(path)


def test_source_manifest_rejects_duplicate_sources(tmp_path: Path) -> None:
    path = tmp_path / "duplicate.parquet"
    write_source_manifest(path, [_entry(), _entry()])
    with pytest.raises(ValueError, match="duplicate"):
        load_source_manifest(path)


def test_source_manifest_requires_explicit_license() -> None:
    with pytest.raises(ValueError, match="license"):
        SourceManifestEntry(
            **{
                **_entry().model_dump(),
                "license": " ",
            }
        )
