"""Deterministic Parquet landing writer and commit-last publication."""

from __future__ import annotations

import tempfile
from collections.abc import Iterable, Iterator
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from video_media_catalog.canonical import deterministic_key
from video_media_catalog.constants import ALGORITHM_SPEC_ID
from video_media_catalog.eidr import iter_eidr_records
from video_media_catalog.models import (
    LandingManifest,
    LandingRecord,
    LandingShard,
    LandingSummary,
    SourceObject,
)
from video_media_catalog.storage import (
    atomic_write_bytes,
    digest_file,
    file_uri,
    local_path,
    publish_file_immutable,
)
from video_media_catalog.wikidata import detect_compression, iter_wikidata_records

LANDING_SCHEMA = pa.schema(
    [
        pa.field("record_key", pa.string(), nullable=False),
        pa.field("source", pa.string(), nullable=False),
        pa.field("source_record_id", pa.string(), nullable=False),
        pa.field("source_revision", pa.string(), nullable=True),
        pa.field("modified", pa.string(), nullable=True),
        pa.field("source_hash", pa.string(), nullable=False),
        pa.field("payload_json", pa.string(), nullable=False),
    ],
    metadata={
        b"contract": b"video-media-catalog.landing",
        b"schema_version": b"1.0",
    },
)


def _manifest_identity(
    *,
    input_manifest_digest: str | None,
    sources: list[SourceObject],
    shards: list[LandingShard],
    record_count: int,
    source_counts: dict[str, int],
) -> dict[str, object]:
    return {
        "schemaVersion": "1.0",
        "algorithmSpecId": ALGORITHM_SPEC_ID,
        "inputManifestDigest": input_manifest_digest,
        "sources": [
            source.model_dump(mode="json", by_alias=True, exclude_none=True)
            for source in sources
        ],
        "shards": [
            shard.model_dump(mode="json", by_alias=True, exclude_none=True)
            for shard in shards
        ],
        "recordCount": record_count,
        "sourceCounts": source_counts,
    }


def validate_landing_manifest(manifest: LandingManifest) -> None:
    if manifest.algorithm_spec_id != ALGORITHM_SPEC_ID:
        raise ValueError(
            f"unsupported landing algorithm spec: {manifest.algorithm_spec_id}"
        )
    if sum(manifest.source_counts.values()) != manifest.record_count:
        raise ValueError("landing source counts do not equal record count")
    if any(count < 0 for count in manifest.source_counts.values()):
        raise ValueError("landing source counts must not be negative")
    if sum(shard.record_count for shard in manifest.shards) != manifest.record_count:
        raise ValueError("landing shard counts do not equal record count")
    if len({source.source for source in manifest.sources}) != len(manifest.sources):
        raise ValueError("landing manifest contains a duplicate source")
    if set(manifest.source_counts) != {source.source for source in manifest.sources}:
        raise ValueError("landing source counts do not match source objects")
    if len({shard.uri for shard in manifest.shards}) != len(manifest.shards):
        raise ValueError("landing manifest contains a duplicate shard URI")
    expected_id = deterministic_key(
        "landing-manifest",
        _manifest_identity(
            input_manifest_digest=manifest.input_manifest_digest,
            sources=manifest.sources,
            shards=manifest.shards,
            record_count=manifest.record_count,
            source_counts=manifest.source_counts,
        ),
    )
    if manifest.manifest_id != expected_id:
        raise ValueError(
            f"landing manifest ID mismatch: expected {expected_id}, "
            f"got {manifest.manifest_id}"
        )


def build_landing_manifest(
    *,
    sources: list[SourceObject],
    shards: list[LandingShard],
    source_counts: dict[str, int],
    input_manifest_digest: str | None = None,
) -> LandingManifest:
    record_count = sum(source_counts.values())
    identity = _manifest_identity(
        input_manifest_digest=input_manifest_digest,
        sources=sources,
        shards=shards,
        record_count=record_count,
        source_counts=source_counts,
    )
    manifest = LandingManifest(
        manifest_id=deterministic_key("landing-manifest", identity),
        input_manifest_digest=input_manifest_digest,
        sources=sources,
        shards=shards,
        record_count=record_count,
        source_counts=source_counts,
    )
    validate_landing_manifest(manifest)
    return manifest


def _chunks(
    records: Iterable[LandingRecord], size: int
) -> Iterator[list[LandingRecord]]:
    chunk: list[LandingRecord] = []
    for record in records:
        chunk.append(record)
        if len(chunk) == size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def _normalize_expected_digest(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.lower()
    return normalized if normalized.startswith("sha256:") else f"sha256:{normalized}"


def _source_object(
    source: str,
    path: Path,
    compression: str,
    expected_digest: str | None,
) -> SourceObject:
    digest, size = digest_file(path)
    expected = _normalize_expected_digest(expected_digest)
    if expected is not None and digest != expected:
        raise ValueError(
            f"{source} source checksum mismatch: expected {expected}, got {digest}"
        )
    return SourceObject(
        source=source,
        uri=file_uri(path),
        checksum=digest,
        size_bytes=size,
        compression=compression,
    )


def _write_shard(
    output_root: Path,
    index: int,
    records: list[LandingRecord],
) -> LandingShard:
    destination = output_root / "landing" / f"shard-{index:05d}.parquet"
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        prefix=f".{destination.name}.",
        suffix=".tmp",
        dir=destination.parent,
        delete=False,
    ) as handle:
        temporary = Path(handle.name)
    try:
        table = pa.Table.from_pylist(
            [record.model_dump(mode="python") for record in records],
            schema=LANDING_SCHEMA,
        )
        pq.write_table(
            table,
            temporary,
            compression="zstd",
            compression_level=9,
            use_dictionary=False,
            write_statistics=True,
            data_page_version="1.0",
            version="2.6",
            row_group_size=len(records),
        )
        publish_file_immutable(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)
    digest, size = digest_file(destination)
    return LandingShard(
        uri=file_uri(destination),
        checksum=digest,
        size_bytes=size,
        record_count=len(records),
        first_record_key=records[0].record_key,
        last_record_key=records[-1].record_key,
    )


def extract_landing(
    *,
    output_uri: str,
    wikidata_uri: str | None = None,
    eidr_xml_uri: str | None = None,
    shard_records: int = 50_000,
    wikidata_sha256: str | None = None,
    eidr_sha256: str | None = None,
) -> LandingSummary:
    """Extract local sources, publish shards and manifest, then summary last."""

    if wikidata_uri is None and eidr_xml_uri is None:
        raise ValueError("at least one source URI is required")
    if shard_records < 1:
        raise ValueError("shard_records must be positive")

    output_root = local_path(output_uri).resolve()
    sources: list[SourceObject] = []
    record_streams: list[Iterable[LandingRecord]] = []
    if wikidata_uri is not None:
        path = local_path(wikidata_uri).resolve()
        sources.append(
            _source_object(
                "wikidata",
                path,
                detect_compression(path),
                wikidata_sha256,
            )
        )
        record_streams.append(iter_wikidata_records(path))
    if eidr_xml_uri is not None:
        path = local_path(eidr_xml_uri).resolve()
        sources.append(_source_object("eidr", path, "plain", eidr_sha256))
        record_streams.append(iter_eidr_records(path))

    def records() -> Iterator[LandingRecord]:
        for stream in record_streams:
            yield from stream

    shards: list[LandingShard] = []
    source_counts = {"wikidata": 0, "eidr": 0}
    for index, chunk in enumerate(_chunks(records(), shard_records)):
        for record in chunk:
            source_counts[record.source] += 1
        shards.append(_write_shard(output_root, index, chunk))

    source_counts = {source.source: source_counts[source.source] for source in sources}
    manifest = build_landing_manifest(
        sources=sources,
        shards=shards,
        source_counts=source_counts,
    )
    manifest_path = output_root / "landing-manifest.json"
    atomic_write_bytes(manifest_path, manifest.json_bytes())
    manifest_digest, _ = digest_file(manifest_path)

    summary = LandingSummary(
        manifest_id=manifest.manifest_id,
        manifest_uri=file_uri(manifest_path),
        manifest_checksum=manifest_digest,
        record_count=manifest.record_count,
        shard_count=len(shards),
    )
    atomic_write_bytes(output_root / "landing-summary.json", summary.json_bytes())
    return summary
