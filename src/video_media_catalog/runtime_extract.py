"""Strict Argo/control-plane extraction mode."""

from __future__ import annotations

import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path

from video_media_catalog.landing import (
    build_landing_manifest,
    extract_landing,
)
from video_media_catalog.models import (
    LandingManifest,
    LandingShard,
    LandingSummary,
    ObjectRef,
    SourceObject,
)
from video_media_catalog.object_store import (
    BoundedObjectStore,
    RuntimeObjectStore,
)
from video_media_catalog.runtime_args import RuntimeArguments, join_uri
from video_media_catalog.source_manifest import (
    SourceManifestEntry,
    load_source_manifest,
)
from video_media_catalog.storage import local_path

EXTRACT_STAGE = "media-catalog-extract"
LANDING_MANIFEST_MEDIA_TYPE = (
    "application/vnd.video-media-catalog.landing-manifest.v1+json"
)
LANDING_SUMMARY_MEDIA_TYPE = (
    "application/vnd.video-media-catalog.landing-summary.v1+json"
)
DEFAULT_MAX_MANIFEST_BYTES = 64 * 1024 * 1024
DEFAULT_MAX_SOURCE_BYTES = 2 * 1024 * 1024 * 1024 * 1024
DEFAULT_MAX_EPHEMERAL_BYTES = 4 * 1024 * 1024 * 1024 * 1024
DEFAULT_MAX_OUTPUT_OBJECT_BYTES = 5 * 1024 * 1024 * 1024


@dataclass(frozen=True)
class RuntimeExtractConfig:
    shard_records: int = 50_000
    max_manifest_bytes: int = DEFAULT_MAX_MANIFEST_BYTES
    max_source_bytes: int = DEFAULT_MAX_SOURCE_BYTES
    max_ephemeral_bytes: int = DEFAULT_MAX_EPHEMERAL_BYTES
    max_output_object_bytes: int = DEFAULT_MAX_OUTPUT_OBJECT_BYTES
    ephemeral_dir: Path | None = None

    def __post_init__(self) -> None:
        values = (
            self.shard_records,
            self.max_manifest_bytes,
            self.max_source_bytes,
            self.max_ephemeral_bytes,
            self.max_output_object_bytes,
        )
        if any(value < 1 for value in values):
            raise ValueError("runtime extraction limits must be positive")


def _source_ref(entry: SourceManifestEntry) -> ObjectRef:
    return ObjectRef(
        uri=entry.uri,
        format="OBJECT_FORMAT_OTHER",
        media_type=(
            "application/xml" if entry.source == "eidr" else "application/json"
        ),
        checksum={"value": entry.sha256},
        size_bytes=entry.size_bytes,
        etag=entry.etag,
        object_version=entry.object_version,
    )


def _materialized_name(entry: SourceManifestEntry) -> str:
    if entry.source == "eidr":
        return "eidr.xml"
    suffix = {
        "plain": ".json",
        "gzip": ".json.gz",
        "bzip2": ".json.bz2",
    }[entry.compression]
    return f"wikidata{suffix}"


def _source_object(entry: SourceManifestEntry) -> SourceObject:
    return SourceObject(
        source=entry.source,
        uri=entry.uri,
        checksum=f"sha256:{entry.sha256}",
        size_bytes=entry.size_bytes,
        compression=entry.compression,
        object_version=entry.object_version,
        etag=entry.etag,
        license=entry.license,
    )


def _check_ephemeral_capacity(
    *,
    root: Path,
    manifest_size: int,
    entries: list[SourceManifestEntry],
    configured_limit: int,
) -> None:
    source_bytes = sum(entry.size_bytes for entry in entries)
    estimated = manifest_size + (2 * source_bytes)
    if estimated > configured_limit:
        raise ValueError(
            "estimated source spool plus landing output exceeds --max-ephemeral-bytes"
        )
    available = shutil.disk_usage(root).free
    if estimated > available:
        raise ValueError(
            f"ephemeral storage requires approximately {estimated} bytes, "
            f"only {available} bytes are free"
        )


def run_runtime_extract(
    args: RuntimeArguments,
    *,
    config: RuntimeExtractConfig | None = None,
    store: RuntimeObjectStore | None = None,
) -> LandingSummary:
    """Verify immutable inputs, spool sources, and publish summary last."""

    config = config or RuntimeExtractConfig()
    store = store or BoundedObjectStore(
        region=os.environ.get("AWS_REGION"),
        endpoint_url=os.environ.get("S3_ENDPOINT"),
        path_style_access=os.environ.get("S3_PATH_STYLE", "").lower()
        in {"1", "true", "yes"},
    )
    temporary_parent = (
        None
        if config.ephemeral_dir is None
        else str(config.ephemeral_dir.expanduser().resolve())
    )
    with tempfile.TemporaryDirectory(
        prefix="media-catalog-extract-",
        dir=temporary_parent,
    ) as directory:
        root = Path(directory)
        manifest_path = root / "source-manifest.parquet"
        store.download(
            args.input_manifest,
            manifest_path,
            max_bytes=config.max_manifest_bytes,
        )
        entries = load_source_manifest(manifest_path)
        if any(entry.size_bytes > config.max_source_bytes for entry in entries):
            raise ValueError("a source object exceeds --max-source-bytes")
        _check_ephemeral_capacity(
            root=root,
            manifest_size=args.manifest_size,
            entries=entries,
            configured_limit=config.max_ephemeral_bytes,
        )

        materialized: dict[str, Path] = {}
        for entry in entries:
            destination = root / "sources" / _materialized_name(entry)
            store.download(
                _source_ref(entry),
                destination,
                max_bytes=config.max_source_bytes,
            )
            materialized[entry.source] = destination

        local_landing_root = root / "landing-output"
        extract_landing(
            output_uri=str(local_landing_root),
            wikidata_uri=(
                str(materialized["wikidata"]) if "wikidata" in materialized else None
            ),
            eidr_xml_uri=(
                str(materialized["eidr"]) if "eidr" in materialized else None
            ),
            shard_records=config.shard_records,
            wikidata_sha256=next(
                (entry.sha256 for entry in entries if entry.source == "wikidata"),
                None,
            ),
            eidr_sha256=next(
                (entry.sha256 for entry in entries if entry.source == "eidr"),
                None,
            ),
        )
        local_manifest = LandingManifest.model_validate_json(
            (local_landing_root / "landing-manifest.json").read_bytes()
        )
        stage_prefix = args.stage_prefix(EXTRACT_STAGE)
        published_shards: list[LandingShard] = []
        for shard in local_manifest.shards:
            local_shard = local_path(shard.uri)
            destination_uri = join_uri(stage_prefix, "landing", local_shard.name)
            uploaded = store.upload_file(
                local_shard,
                destination_uri,
                media_type="application/vnd.apache.parquet",
                object_format="OBJECT_FORMAT_PARQUET",
                max_bytes=config.max_output_object_bytes,
            )
            published_shards.append(
                LandingShard(
                    uri=uploaded.object_ref.uri,
                    checksum=("sha256:" + uploaded.object_ref.checksum.value),
                    size_bytes=uploaded.object_ref.size_bytes,
                    record_count=shard.record_count,
                    first_record_key=shard.first_record_key,
                    last_record_key=shard.last_record_key,
                    etag=uploaded.object_ref.etag,
                    object_version=uploaded.object_ref.object_version,
                )
            )

        source_counts = dict(local_manifest.source_counts)
        manifest = build_landing_manifest(
            sources=[_source_object(entry) for entry in entries],
            shards=published_shards,
            source_counts=source_counts,
            input_manifest_digest=f"sha256:hex:{args.manifest_hash}",
        )
        manifest_uri = join_uri(stage_prefix, "landing-manifest.json")
        manifest_upload = store.upload_bytes(
            manifest.json_bytes(),
            manifest_uri,
            media_type=LANDING_MANIFEST_MEDIA_TYPE,
            object_format="OBJECT_FORMAT_JSON",
            max_bytes=config.max_manifest_bytes,
        )
        summary = LandingSummary(
            manifest_id=manifest.manifest_id,
            manifest_uri=manifest_upload.object_ref.uri,
            manifest_checksum=("sha256:" + manifest_upload.object_ref.checksum.value),
            record_count=manifest.record_count,
            shard_count=len(manifest.shards),
        )
        store.upload_bytes(
            summary.json_bytes(),
            join_uri(stage_prefix, "landing-summary.json"),
            media_type=LANDING_SUMMARY_MEDIA_TYPE,
            object_format="OBJECT_FORMAT_JSON",
            max_bytes=config.max_manifest_bytes,
        )
        return summary
