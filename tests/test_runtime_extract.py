from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

import pytest

from video_media_catalog.models import Checksum, LandingManifest, ObjectRef
from video_media_catalog.object_store import (
    MaterializedObject,
    ObjectStoreError,
    UploadResult,
)
from video_media_catalog.runtime_args import RuntimeArguments
from video_media_catalog.runtime_extract import (
    RuntimeExtractConfig,
    run_runtime_extract,
)
from video_media_catalog.source_manifest import (
    SourceManifestEntry,
    write_source_manifest,
)

RUN_ID = "01a081e8-6420-7000-8000-000000000202"
JOB_SPEC_ID = "01a081e8-6420-7000-8000-000000000203"
TENANT_ID = "01a081e8-6420-7000-8000-000000000204"


@dataclass
class Stored:
    payload: bytes
    etag: str | None = None
    version: str | None = None


class FakeStore:
    def __init__(self, objects: dict[str, Stored]) -> None:
        self.objects = objects
        self.publication_order: list[str] = []

    def download(
        self,
        object_ref: ObjectRef,
        destination: Path,
        *,
        max_bytes: int,
    ) -> MaterializedObject:
        stored = self.objects[object_ref.uri]
        digest = hashlib.sha256(stored.payload).hexdigest()
        if len(stored.payload) > max_bytes:
            raise ObjectStoreError("OBJECT_TOO_LARGE", "too large")
        if object_ref.size_bytes != len(stored.payload):
            raise ObjectStoreError("OBJECT_SIZE_MISMATCH", "size mismatch")
        if object_ref.checksum.value != digest:
            raise ObjectStoreError("OBJECT_CHECKSUM_MISMATCH", "hash mismatch")
        if object_ref.etag is not None and object_ref.etag != stored.etag:
            raise ObjectStoreError("OBJECT_ETAG_MISMATCH", "etag mismatch")
        if (
            object_ref.object_version is not None
            and object_ref.object_version != stored.version
        ):
            raise ObjectStoreError("OBJECT_VERSION_MISMATCH", "version mismatch")
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(stored.payload)
        return MaterializedObject(destination, len(stored.payload), digest)

    def upload_file(
        self,
        source: Path,
        destination_uri: str,
        *,
        media_type: str,
        object_format: str,
        max_bytes: int,
    ) -> UploadResult:
        return self.upload_bytes(
            source.read_bytes(),
            destination_uri,
            media_type=media_type,
            object_format=object_format,
            max_bytes=max_bytes,
        )

    def upload_bytes(
        self,
        payload: bytes,
        destination_uri: str,
        *,
        media_type: str,
        object_format: str,
        max_bytes: int,
    ) -> UploadResult:
        if len(payload) > max_bytes:
            raise ObjectStoreError("OBJECT_TOO_LARGE", "too large")
        existing = self.objects.get(destination_uri)
        reused = existing is not None
        if existing is not None and existing.payload != payload:
            raise ObjectStoreError("IMMUTABLE_OBJECT_CONFLICT", "different content")
        digest = hashlib.sha256(payload).hexdigest()
        self.objects[destination_uri] = Stored(
            payload,
            etag=f"etag-{digest[:12]}",
            version=f"version-{digest[:12]}",
        )
        self.publication_order.append(destination_uri)
        return UploadResult(
            ObjectRef(
                uri=destination_uri,
                format=object_format,
                media_type=media_type,
                checksum=Checksum(value=digest),
                size_bytes=len(payload),
                etag=f"etag-{digest[:12]}",
                object_version=f"version-{digest[:12]}",
            ),
            reused,
        )


def _runtime_fixture(
    fixture_dir: Path, tmp_path: Path
) -> tuple[RuntimeArguments, FakeStore]:
    wiki = (fixture_dir / "wikidata.json").read_bytes()
    eidr = (fixture_dir / "eidr.xml").read_bytes()
    wiki_uri = "s3://catalog-input/wikidata.json"
    eidr_uri = "s3://catalog-input/eidr.xml"
    entries = [
        SourceManifestEntry(
            source="wikidata",
            uri=wiki_uri,
            sha256=hashlib.sha256(wiki).hexdigest(),
            size_bytes=len(wiki),
            compression="plain",
            object_version="wiki-v1",
            etag="wiki-etag",
            license="CC0-1.0",
        ),
        SourceManifestEntry(
            source="eidr",
            uri=eidr_uri,
            sha256=hashlib.sha256(eidr).hexdigest(),
            size_bytes=len(eidr),
            compression="plain",
            object_version="eidr-v1",
            etag="eidr-etag",
            license="EIDR-authorized",
        ),
    ]
    manifest_path = tmp_path / "source-manifest.parquet"
    write_source_manifest(manifest_path, entries)
    manifest = manifest_path.read_bytes()
    manifest_uri = "s3://catalog-input/source-manifest.parquet"
    store = FakeStore(
        {
            manifest_uri: Stored(manifest, "manifest-etag", "manifest-v1"),
            wiki_uri: Stored(wiki, "wiki-etag", "wiki-v1"),
            eidr_uri: Stored(eidr, "eidr-etag", "eidr-v1"),
        }
    )
    runtime = RuntimeArguments(
        manifest_uri=manifest_uri,
        manifest_hash="sha256:hex:" + hashlib.sha256(manifest).hexdigest(),
        manifest_version="manifest-v1",
        manifest_etag="manifest-etag",
        manifest_size=len(manifest),
        run_id=RUN_ID,
        job_spec_id=JOB_SPEC_ID,
        tenant_id=TENANT_ID,
        attempt=2,
        output_prefix="s3://catalog-output/runs/" + RUN_ID,
        executor_image="registry.example/catalog@sha256:" + "c" * 64,
    )
    return runtime, store


def test_runtime_extract_verifies_and_publishes_commit_marker_last(
    fixture_dir: Path, tmp_path: Path
) -> None:
    runtime, store = _runtime_fixture(fixture_dir, tmp_path)
    ephemeral = tmp_path / "ephemeral"
    ephemeral.mkdir()
    config = RuntimeExtractConfig(
        shard_records=5,
        max_manifest_bytes=1024 * 1024,
        max_source_bytes=1024 * 1024,
        max_ephemeral_bytes=4 * 1024 * 1024,
        max_output_object_bytes=1024 * 1024,
        ephemeral_dir=ephemeral,
    )
    summary = run_runtime_extract(runtime, config=config, store=store)
    stage = f"{runtime.output_prefix}/attempt=2/stage=media-catalog-extract"
    manifest_uri = f"{stage}/landing-manifest.json"
    summary_uri = f"{stage}/landing-summary.json"

    assert store.publication_order[-1] == summary_uri
    assert summary.manifest_uri == manifest_uri
    landing = LandingManifest.model_validate_json(store.objects[manifest_uri].payload)
    assert landing.record_count == 20
    assert landing.input_manifest_digest == (f"sha256:hex:{runtime.manifest_hash}")
    assert all(shard.uri.startswith(f"{stage}/landing/") for shard in landing.shards)
    assert all(shard.etag and shard.object_version for shard in landing.shards)
    assert {source.uri for source in landing.sources} == {
        "s3://catalog-input/wikidata.json",
        "s3://catalog-input/eidr.xml",
    }
    assert all(source.object_version for source in landing.sources)

    first_objects = dict(store.objects)
    second = run_runtime_extract(runtime, config=config, store=store)
    assert second == summary
    assert all(
        store.objects[uri].payload == stored.payload
        for uri, stored in first_objects.items()
    )


def test_runtime_extract_rejects_manifest_metadata_mismatch(
    fixture_dir: Path, tmp_path: Path
) -> None:
    runtime, store = _runtime_fixture(fixture_dir, tmp_path)
    invalid = runtime.model_copy(update={"manifest_etag": "wrong"})
    with pytest.raises(ObjectStoreError, match="etag"):
        run_runtime_extract(
            invalid,
            config=RuntimeExtractConfig(max_ephemeral_bytes=4 * 1024 * 1024),
            store=store,
        )


def test_runtime_extract_rejects_source_checksum_mismatch(
    fixture_dir: Path, tmp_path: Path
) -> None:
    runtime, store = _runtime_fixture(fixture_dir, tmp_path)
    store.objects["s3://catalog-input/wikidata.json"].payload += b"\n"
    with pytest.raises(ObjectStoreError, match="mismatch"):
        run_runtime_extract(
            runtime,
            config=RuntimeExtractConfig(max_ephemeral_bytes=4 * 1024 * 1024),
            store=store,
        )
