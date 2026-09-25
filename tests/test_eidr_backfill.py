from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from video_media_catalog.community_snapshot import (
    CONTROL_MAX_BYTES,
    SILVER_SNAPSHOT_MEDIA_TYPE,
    build_community_silver_snapshot_set,
)
from video_media_catalog.community_sources import (
    EIDR_EXACT_LOOKUP_CONNECTOR_ID,
    build_community_registry,
    eidr_rights_profile,
)
from video_media_catalog.community_tables import DATA_TABLE_COLUMNS
from video_media_catalog.connector import (
    Completeness,
    ConnectorRecordEnvelope,
    DeleteCoverage,
)
from video_media_catalog.eidr_backfill import (
    EidrExactLookupResult,
    EidrLookupStatus,
    EidrProviderNotAuthorizedError,
    build_discovered_eidr_id_frame,
    build_eidr_discovery_source_binding,
    build_eidr_provider_authorization,
    iter_discovered_eidr_rows,
    publish_discovered_eidr_id_manifest,
    read_discovered_eidr_id_manifest,
    read_eidr_backfill_watermark,
    run_eidr_exact_lookup_batch,
)
from video_media_catalog.eidr_backfill_cli import build_parser
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.object_store import BoundedObjectStore
from video_media_catalog.storage import local_path

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession


def _object(
    path: Path,
    *,
    media_type: str,
    object_format: str = "OBJECT_FORMAT_JSON",
    created_at: str = "2026-09-20T00:00:00Z",
) -> ObjectRef:
    payload = path.read_bytes()
    return ObjectRef(
        uri=path.as_uri(),
        format=object_format,
        media_type=media_type,
        checksum=Checksum(value=hashlib.sha256(payload).hexdigest()),
        size_bytes=len(payload),
        created_at=created_at,
    )


def _movie_xml(eidr_id: str = "10.5240/AAAA-BBBB-CCCC-DDDD-EEEE-C") -> bytes:
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<md:Records xmlns:md="urn:eidr:fixture">
  <md:FullMetadata>
    <md:BaseObjectData>
      <md:ID>{eidr_id}</md:ID>
      <md:ReferentType>Movie</md:ReferentType>
      <md:ResourceName xml:lang="en">Example Movie EIDR</md:ResourceName>
      <md:LastModificationDate>2026-03-01T00:00:00Z</md:LastModificationDate>
    </md:BaseObjectData>
  </md:FullMetadata>
</md:Records>
""".encode()


class FakeAuthorizedProvider:
    def __init__(
        self,
        *,
        responses: dict[str, EidrExactLookupResult] | None = None,
        complete_feed_allowed: bool = False,
        tmp_path: Path,
    ) -> None:
        policy = eidr_rights_profile()
        auth_path = tmp_path / "authorization.json"
        auth_path.write_bytes(b'{"authorized": true}')
        self.authorization = build_eidr_provider_authorization(
            provider_id="fake-eidr-provider",
            authorization_object=_object(
                auth_path,
                media_type="application/json",
            ),
            policy_id=policy.policy_id,
            policy_digest=policy.digest,
            exact_lookup_allowed=True,
            complete_feed_allowed=complete_feed_allowed,
            issued_at="2026-09-20T00:00:00Z",
        )
        self.responses = responses or {}

    def lookup_exact(
        self,
        *,
        eidr_ids: tuple[str, ...],
        request_id: str,
    ):
        del request_id
        for eidr_id in eidr_ids:
            if eidr_id in self.responses:
                yield self.responses[eidr_id]
                continue
            yield EidrExactLookupResult(
                eidr_id=eidr_id,
                status=EidrLookupStatus.FOUND,
                xml=_movie_xml(eidr_id),
                attempt_count=2,
                retry_count=1,
                rate_limit_count=1,
            )


def _publish_fixture_manifest(
    tmp_path: Path,
    *,
    eidr_ids: tuple[str, ...],
    page_size: int = 4,
) -> tuple[object, ObjectRef, object, ObjectRef]:
    store = BoundedObjectStore(client=object())
    destination = tmp_path / "discovered"
    snapshot = build_community_silver_snapshot_set(
        committed_run_ids=("sha256:" + ("a" * 64),),
        run_snapshot_id=99,
        commit_snapshot_id=100,
        data_snapshot_ids={
            table: (201 if table == "community_identifier_assertion" else None)
            for table in DATA_TABLE_COLUMNS
        },
        created_at="2026-09-20T00:00:00Z",
    )
    snapshot_path = tmp_path / "silver-snapshot.json"
    snapshot_path.write_bytes(snapshot.json_bytes())
    snapshot_object = store.upload_bytes(
        snapshot.json_bytes(),
        (destination / "silver-snapshot.json").as_uri(),
        media_type=SILVER_SNAPSHOT_MEDIA_TYPE,
        object_format="OBJECT_FORMAT_JSON",
        max_bytes=CONTROL_MAX_BYTES,
    ).object_ref
    source = build_eidr_discovery_source_binding(
        source_release_id="sha256:" + ("b" * 64),
        silver_snapshot=snapshot,
        silver_snapshot_object=snapshot_object,
    )
    published = publish_discovered_eidr_id_manifest(
        rows=iter_discovered_eidr_rows(eidr_ids, page_size=page_size),
        source=source,
        destination_prefix=destination.as_uri(),
        created_at="2026-09-20T00:00:00Z",
        store=store,
        page_size=page_size,
    )
    return published.manifest, published.manifest_object, snapshot, snapshot_object


def test_registry_declares_exact_lookup_connector() -> None:
    products = {
        item.source_product_id: item
        for item in build_community_registry().source_products
    }
    assert products["eidr-public-registry"].connector_ids == (
        EIDR_EXACT_LOOKUP_CONNECTOR_ID,
    )


def test_discovered_manifest_is_deterministic_and_replayable(tmp_path: Path) -> None:
    manifest, manifest_object, _, _ = _publish_fixture_manifest(
        tmp_path,
        eidr_ids=(
            "10.5240/AAAA-BBBB-CCCC-DDDD-EEEE-C",
            "10.5240/FFFF-EEEE-DDDD-CCCC-BBBB-A",
        ),
        page_size=1,
    )
    assert manifest.id_count == 2
    assert manifest.page_count == 2
    assert manifest.pages[0].start_ordinal == 0
    assert manifest.pages[1].start_ordinal == 1

    store = BoundedObjectStore(client=object())
    destination = (tmp_path / "discovered").as_uri()
    repeated = publish_discovered_eidr_id_manifest(
        rows=iter_discovered_eidr_rows(
            (
                "10.5240/AAAA-BBBB-CCCC-DDDD-EEEE-C",
                "10.5240/FFFF-EEEE-DDDD-CCCC-BBBB-A",
            ),
            page_size=1,
        ),
        source=manifest.source,
        destination_prefix=destination,
        created_at="2026-09-20T00:00:00Z",
        store=store,
        page_size=1,
    )
    assert repeated.manifest == manifest
    assert repeated.manifest_object.checksum == manifest_object.checksum


def test_lookup_batch_requires_authorized_provider(tmp_path: Path) -> None:
    manifest, manifest_object, _, _ = _publish_fixture_manifest(
        tmp_path,
        eidr_ids=("10.5240/AAAA-BBBB-CCCC-DDDD-EEEE-C",),
    )
    store = BoundedObjectStore(client=object())
    with pytest.raises(EidrProviderNotAuthorizedError):
        run_eidr_exact_lookup_batch(
            manifest=manifest,
            manifest_object=manifest_object,
            destination_prefix=(tmp_path / "lookup").as_uri(),
            acquired_at="2026-09-20T00:01:00Z",
            image_digest="sha256:" + ("c" * 64),
            store=store,
            provider=None,
        )


def test_exact_lookup_publishes_partial_capture_and_window_receipt(
    tmp_path: Path,
) -> None:
    manifest, manifest_object, _, _ = _publish_fixture_manifest(
        tmp_path,
        eidr_ids=("10.5240/AAAA-BBBB-CCCC-DDDD-EEEE-C",),
    )
    store = BoundedObjectStore(client=object())
    provider = FakeAuthorizedProvider(tmp_path=tmp_path)
    destination = (tmp_path / "lookup").as_uri()
    result = run_eidr_exact_lookup_batch(
        manifest=manifest,
        manifest_object=manifest_object,
        destination_prefix=destination,
        acquired_at="2026-09-20T00:01:00Z",
        image_digest="sha256:" + ("c" * 64),
        store=store,
        provider=provider,
        batch_size=1,
    )

    assert result.completed is True
    assert result.lookup_batch is not None
    assert result.lookup_batch_object is not None
    assert result.receipt is not None
    assert result.receipt_object is not None
    assert result.watermark is not None
    assert result.watermark_object is not None
    assert result.capture is not None
    assert result.capture.batch_manifest.completeness == Completeness.PARTIAL
    assert result.capture.batch_manifest.delete_coverage == DeleteCoverage.NONE
    assert result.capture.batch_manifest.connector_id == EIDR_EXACT_LOOKUP_CONNECTOR_ID
    assert result.receipt.retry_count == 1
    assert result.receipt.rate_limit_count == 1
    assert result.receipt.next_ordinal == 1
    assert local_path(result.lookup_batch_object.uri).exists()
    assert local_path(result.receipt_object.uri).exists()
    assert local_path(result.watermark_object.uri).exists()

    records = []
    for reference in result.capture.record_set_manifest.record_objects:
        records.extend(
            ConnectorRecordEnvelope.model_validate_json(line)
            for line in local_path(reference.uri).read_bytes().splitlines()
        )
    assert [record.source_record_id for record in records] == [
        "10.5240/AAAA-BBBB-CCCC-DDDD-EEEE-C"
    ]


def test_watermark_advances_with_source_semaphore_one_batches(
    tmp_path: Path,
) -> None:
    manifest, manifest_object, _, _ = _publish_fixture_manifest(
        tmp_path,
        eidr_ids=(
            "10.5240/AAAA-BBBB-CCCC-DDDD-EEEE-C",
            "10.5240/FFFF-EEEE-DDDD-CCCC-BBBB-A",
        ),
        page_size=1,
    )
    store = BoundedObjectStore(client=object())
    provider = FakeAuthorizedProvider(tmp_path=tmp_path)
    destination = (tmp_path / "lookup").as_uri()
    first = run_eidr_exact_lookup_batch(
        manifest=manifest,
        manifest_object=manifest_object,
        destination_prefix=destination,
        acquired_at="2026-09-20T00:01:00Z",
        image_digest="sha256:" + ("c" * 64),
        store=store,
        provider=provider,
        batch_size=1,
    )
    assert first.completed is False
    assert first.watermark is not None
    assert first.watermark.next_ordinal == 1

    second = run_eidr_exact_lookup_batch(
        manifest=manifest,
        manifest_object=manifest_object,
        destination_prefix=destination,
        acquired_at="2026-09-20T00:02:00Z",
        image_digest="sha256:" + ("c" * 64),
        store=store,
        provider=provider,
        watermark=first.watermark,
        watermark_object=first.watermark_object,
        batch_size=1,
    )
    assert second.completed is True
    assert second.watermark is not None
    assert second.watermark.next_ordinal == 2
    assert second.watermark.previous_watermark_id == first.watermark.watermark_id


def test_completed_manifest_without_watermark_is_idempotent(tmp_path: Path) -> None:
    manifest, manifest_object, _, _ = _publish_fixture_manifest(
        tmp_path,
        eidr_ids=("10.5240/AAAA-BBBB-CCCC-DDDD-EEEE-C",),
    )
    store = BoundedObjectStore(client=object())
    provider = FakeAuthorizedProvider(tmp_path=tmp_path)
    destination = (tmp_path / "lookup").as_uri()
    finished = run_eidr_exact_lookup_batch(
        manifest=manifest,
        manifest_object=manifest_object,
        destination_prefix=destination,
        acquired_at="2026-09-20T00:01:00Z",
        image_digest="sha256:" + ("c" * 64),
        store=store,
        provider=provider,
        batch_size=1,
    )
    assert finished.completed is True
    assert finished.watermark is not None

    noop = run_eidr_exact_lookup_batch(
        manifest=manifest,
        manifest_object=manifest_object,
        destination_prefix=destination,
        acquired_at="2026-09-20T00:02:00Z",
        image_digest="sha256:" + ("c" * 64),
        store=store,
        provider=provider,
        watermark=finished.watermark,
        watermark_object=finished.watermark_object,
        batch_size=1,
    )
    assert noop.completed is True
    assert noop.lookup_batch is None
    assert noop.receipt is None


def test_not_found_batch_skips_connector_capture(tmp_path: Path) -> None:
    eidr_id = "10.5240/FFFF-EEEE-DDDD-CCCC-BBBB-A"
    manifest, manifest_object, _, _ = _publish_fixture_manifest(
        tmp_path,
        eidr_ids=(eidr_id,),
    )
    store = BoundedObjectStore(client=object())
    provider = FakeAuthorizedProvider(
        tmp_path=tmp_path,
        responses={
            eidr_id: EidrExactLookupResult(
                eidr_id=eidr_id,
                status=EidrLookupStatus.NOT_FOUND,
            )
        },
    )
    result = run_eidr_exact_lookup_batch(
        manifest=manifest,
        manifest_object=manifest_object,
        destination_prefix=(tmp_path / "lookup").as_uri(),
        acquired_at="2026-09-20T00:01:00Z",
        image_digest="sha256:" + ("c" * 64),
        store=store,
        provider=provider,
        batch_size=1,
    )
    assert result.capture is None
    assert result.receipt is not None
    assert result.receipt.items[0].status == EidrLookupStatus.NOT_FOUND
    assert result.receipt.connector_batch_id is None


def test_read_helpers_bind_immutable_objects(tmp_path: Path) -> None:
    manifest, manifest_object, _, _ = _publish_fixture_manifest(
        tmp_path,
        eidr_ids=("10.5240/AAAA-BBBB-CCCC-DDDD-EEEE-C",),
    )
    store = BoundedObjectStore(client=object())
    loaded_manifest = read_discovered_eidr_id_manifest(
        reference=manifest_object,
        store=store,
    )
    assert loaded_manifest == manifest

    provider = FakeAuthorizedProvider(tmp_path=tmp_path)
    result = run_eidr_exact_lookup_batch(
        manifest=manifest,
        manifest_object=manifest_object,
        destination_prefix=(tmp_path / "lookup").as_uri(),
        acquired_at="2026-09-20T00:01:00Z",
        image_digest="sha256:" + ("c" * 64),
        store=store,
        provider=provider,
        batch_size=1,
    )
    assert result.watermark_object is not None
    loaded_watermark = read_eidr_backfill_watermark(
        reference=result.watermark_object,
        store=store,
    )
    assert loaded_watermark == result.watermark


def test_cli_parser_exposes_discovery_and_lookup_commands() -> None:
    extract = build_parser().parse_args(
        [
            "extract-ids",
            "--silver-snapshot-uri",
            "file:///tmp/silver.json",
            "--silver-snapshot-hash",
            "a" * 64,
            "--silver-snapshot-size",
            "100",
            "--source-release-id",
            "sha256:" + ("b" * 64),
            "--destination-prefix",
            "file:///tmp/out",
            "--created-at",
            "2026-09-20T00:00:00Z",
            "--warehouse",
            "file:///tmp/warehouse",
        ]
    )
    assert extract.command == "extract-ids"
    assert extract.page_size > 0

    lookup = build_parser().parse_args(
        [
            "lookup-batch",
            "--manifest-uri",
            "file:///tmp/manifest.json",
            "--manifest-hash",
            "c" * 64,
            "--manifest-size",
            "100",
            "--destination-prefix",
            "file:///tmp/out",
            "--acquired-at",
            "2026-09-20T00:01:00Z",
            "--image-digest",
            "sha256:" + ("d" * 64),
            "--batch-size",
            "1",
        ]
    )
    assert lookup.command == "lookup-batch"
    assert lookup.batch_size == 1


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    try:
        session = (
            SparkSession.builder.master("local[2]")
            .appName("eidr-backfill-unit-test")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
    except Exception as exc:  # pragma: no cover - environment-specific JVM setup
        pytest.skip(f"Spark runtime unavailable: {exc}")
    yield session
    session.stop()


@pytest.mark.spark
def test_spark_frame_extracts_deduplicated_eidr_ids(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    del tmp_path
    snapshot = build_community_silver_snapshot_set(
        committed_run_ids=("sha256:" + ("a" * 64),),
        run_snapshot_id=99,
        commit_snapshot_id=100,
        data_snapshot_ids={
            table: (201 if table == "community_identifier_assertion" else None)
            for table in DATA_TABLE_COLUMNS
        },
        created_at="2026-09-20T00:00:00Z",
    )
    assertions = spark.createDataFrame(
        [
            (
                "sha256:" + ("a" * 64),
                "eidr-content",
                "10.5240/AAAA-BBBB-CCCC-DDDD-EEEE-C",
                "ACTIVE",
            ),
            (
                "sha256:" + ("a" * 64),
                "eidr-content",
                "doi:10.5240/ffff-eeee-dddd-cccc-bbbb-a",
                "ACTIVE",
            ),
            (
                "sha256:" + ("b" * 64),
                "eidr-content",
                "10.5240/1111-1111-1111-1111-1111-A",
                "ACTIVE",
            ),
            ("sha256:" + ("a" * 64), "imdb-title", "tt0000001", "ACTIVE"),
        ],
        "run_id STRING, namespace_id STRING, value STRING, status STRING",
    )
    frame = build_discovered_eidr_id_frame(
        assertions,
        silver_snapshot=snapshot,
        page_size=1,
    )
    rows = frame.orderBy("shard_key", "page_index", "eidr_id").collect()
    assert {row.eidr_id for row in rows} == {
        "10.5240/AAAA-BBBB-CCCC-DDDD-EEEE-C",
        "10.5240/FFFF-EEEE-DDDD-CCCC-BBBB-A",
    }
    assert all(row.page_index == 0 for row in rows)
