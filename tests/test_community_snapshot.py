from __future__ import annotations

from pathlib import Path

import pytest

from video_media_catalog.community_snapshot import (
    MAX_EPOCH_DELTA_RUNS,
    SILVER_EPOCH_MEDIA_TYPE,
    CommunitySilverEpochManifest,
    CommunitySilverEpochReference,
    build_committed_run_digest,
    build_community_silver_epoch_manifest,
    build_community_silver_snapshot_set,
    parse_community_silver_manifest,
)
from video_media_catalog.community_tables import (
    DATA_TABLE_COLUMNS,
    SOURCE_TABLES,
    build_community_table_mapping,
)
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.object_store import BoundedObjectStore, ObjectStoreError


def test_silver_snapshot_set_is_deterministic() -> None:
    values = {
        "committed_run_ids": (
            "sha256:" + ("b" * 64),
            "sha256:" + ("a" * 64),
        ),
        "run_snapshot_id": 99,
        "commit_snapshot_id": 100,
        "data_snapshot_ids": {
            table: (200 if table == "community_source_record" else None)
            for table in DATA_TABLE_COLUMNS
        },
        "created_at": "2026-09-19T00:00:00Z",
    }
    first = build_community_silver_snapshot_set(**values)
    second = build_community_silver_snapshot_set(
        **{
            **values,
            "committed_run_ids": tuple(reversed(values["committed_run_ids"])),
        }
    )
    assert first == second
    assert first == type(first).model_validate_json(first.json_bytes())


def test_silver_snapshot_set_requires_all_data_tables() -> None:
    with pytest.raises(ValueError, match="every data table"):
        build_community_silver_snapshot_set(
            committed_run_ids=("sha256:" + ("a" * 64),),
            run_snapshot_id=99,
            commit_snapshot_id=100,
            data_snapshot_ids={},
            created_at="2026-09-19T00:00:00Z",
        )


def test_generation_manifest_pins_physical_mapping_and_legacy_stays_fixed() -> None:
    generation = "catalog-2026-09"
    generated = build_community_silver_snapshot_set(
        committed_run_ids=("sha256:" + ("a" * 64),),
        run_snapshot_id=99,
        commit_snapshot_id=100,
        data_snapshot_ids={table: None for table in DATA_TABLE_COLUMNS},
        identity_generation_id=generation,
        created_at="2026-09-19T00:00:00Z",
    )
    mapping = build_community_table_mapping(generation)
    assert generated.table_mapping == mapping
    assert all(mapping[table] == table for table in SOURCE_TABLES)
    assert mapping["community_entity_ledger"] != "community_entity_ledger"
    assert parse_community_silver_manifest(generated.json_bytes()) == generated

    legacy = build_community_silver_snapshot_set(
        committed_run_ids=("sha256:" + ("a" * 64),),
        run_snapshot_id=99,
        commit_snapshot_id=100,
        data_snapshot_ids={table: None for table in DATA_TABLE_COLUMNS},
        created_at="2026-09-19T00:00:00Z",
    )
    assert legacy.identity_generation_id is None
    assert legacy.table_mapping is None

    payload = generated.model_dump(mode="python")
    payload["table_mapping"]["community_entity_ledger"] = "community_entity_ledger"
    with pytest.raises(ValueError, match="does not match"):
        type(generated).model_validate(payload, context={"skip_identity": True})


def _data_snapshots() -> dict[str, int | None]:
    return {
        table: (200 if table == "community_source_record" else None)
        for table in DATA_TABLE_COLUMNS
    }


def _epoch_ref(epoch_id: str) -> CommunitySilverEpochReference:
    return CommunitySilverEpochReference(
        epoch_id=epoch_id,
        object_ref=ObjectRef(
            uri="file:///tmp/silver-baseline.json",
            format="OBJECT_FORMAT_JSON",
            media_type=SILVER_EPOCH_MEDIA_TYPE,
            checksum=Checksum(value="f" * 64),
            size_bytes=100,
        ),
    )


def test_v2_snapshot_contract_no_longer_has_4096_run_limit() -> None:
    runs = tuple(
        "sha256:" + f"{index:064x}" for index in range(MAX_EPOCH_DELTA_RUNS + 1)
    )
    snapshot = build_community_silver_snapshot_set(
        committed_run_ids=runs,
        run_snapshot_id=99,
        commit_snapshot_id=100,
        data_snapshot_ids=_data_snapshots(),
        created_at="2026-09-19T00:00:00Z",
    )
    assert len(snapshot.committed_run_ids) == MAX_EPOCH_DELTA_RUNS + 1


def test_epoch_manifest_is_bounded_deterministic_and_v2_compatible() -> None:
    first_run = "sha256:" + ("1" * 64)
    second_run = "sha256:" + ("2" * 64)
    committed_digest = build_committed_run_digest(
        run_count=2,
        buckets=(
            ("22", 1, "sha256:" + ("b" * 64)),
            ("11", 1, "sha256:" + ("a" * 64)),
        ),
    )
    values = {
        "delta_run_ids": (second_run, first_run),
        "run_snapshot_id": 99,
        "commit_snapshot_id": 100,
        "data_snapshot_ids": _data_snapshots(),
        "source_watermarks": {
            "tvmaze-public-api": "since:20",
            "tmdb-research": "2026-09-20",
        },
        "committed_run_count": 2,
        "committed_run_digest": committed_digest,
        "created_at": "2026-09-20T00:00:00Z",
    }
    first = build_community_silver_epoch_manifest(**values)
    second = build_community_silver_epoch_manifest(
        **{
            **values,
            "delta_run_ids": tuple(reversed(values["delta_run_ids"])),
            "source_watermarks": dict(
                reversed(tuple(values["source_watermarks"].items()))
            ),
        }
    )
    assert first == second
    assert first.delta_run_ids == (first_run, second_run)
    assert first.json_bytes() == second.json_bytes()
    assert isinstance(
        parse_community_silver_manifest(first.json_bytes()),
        CommunitySilverEpochManifest,
    )

    legacy = build_community_silver_snapshot_set(
        committed_run_ids=(first_run,),
        run_snapshot_id=99,
        commit_snapshot_id=100,
        data_snapshot_ids=_data_snapshots(),
        created_at="2026-09-20T00:00:00Z",
    )
    assert parse_community_silver_manifest(legacy.json_bytes()) == legacy


def test_epoch_manifest_requires_parent_and_baseline_together() -> None:
    digest = build_committed_run_digest(
        run_count=1,
        buckets=(("aa", 1, "sha256:" + ("a" * 64)),),
    )
    with pytest.raises(ValueError, match="parent and baseline"):
        build_community_silver_epoch_manifest(
            parent_epoch=_epoch_ref("sha256:" + ("c" * 64)),
            delta_run_ids=(),
            run_snapshot_id=99,
            commit_snapshot_id=100,
            data_snapshot_ids=_data_snapshots(),
            source_watermarks={},
            committed_run_count=1,
            committed_run_digest=digest,
            created_at="2026-09-20T00:00:00Z",
        )


def test_epoch_delta_remains_bounded() -> None:
    runs = tuple(
        "sha256:" + f"{index:064x}" for index in range(MAX_EPOCH_DELTA_RUNS + 1)
    )
    with pytest.raises(ValueError, match="epoch delta supports at most"):
        build_community_silver_epoch_manifest(
            delta_run_ids=runs,
            run_snapshot_id=99,
            commit_snapshot_id=100,
            data_snapshot_ids=_data_snapshots(),
            source_watermarks={},
            committed_run_count=len(runs),
            committed_run_digest="sha256:" + ("a" * 64),
            created_at="2026-09-20T00:00:00Z",
        )


def test_epoch_object_publish_reuses_identical_and_rejects_conflict(
    tmp_path: Path,
) -> None:
    store = BoundedObjectStore(client=object())
    destination = (tmp_path / "epoch.json").as_uri()
    first = store.upload_bytes(
        b'{"epoch":"same"}\n',
        destination,
        media_type=SILVER_EPOCH_MEDIA_TYPE,
        object_format="OBJECT_FORMAT_JSON",
        max_bytes=1024,
    )
    second = store.upload_bytes(
        b'{"epoch":"same"}\n',
        destination,
        media_type=SILVER_EPOCH_MEDIA_TYPE,
        object_format="OBJECT_FORMAT_JSON",
        max_bytes=1024,
    )
    assert not first.reused
    assert second.reused
    with pytest.raises(ObjectStoreError) as raised:
        store.upload_bytes(
            b'{"epoch":"different"}\n',
            destination,
            media_type=SILVER_EPOCH_MEDIA_TYPE,
            object_format="OBJECT_FORMAT_JSON",
            max_bytes=1024,
        )
    assert raised.value.code == "IMMUTABLE_OBJECT_CONFLICT"
