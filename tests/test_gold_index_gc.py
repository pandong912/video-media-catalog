from __future__ import annotations

import copy
import hashlib
from pathlib import Path
from typing import Any

import pytest

from video_media_catalog.gold_index_gc import (
    GOLD_INDEX_ARCHIVE_RECEIPT_MEDIA_TYPE,
    GOLD_INDEX_GC_RESULT_MEDIA_TYPE,
    GOLD_INDEX_RESTORE_RECEIPT_MEDIA_TYPE,
    GoldIndexArchiveReceipt,
    GoldIndexGcResultReceipt,
    GoldIndexRestoreReceipt,
    archive_delete,
    register_snapshot_repository,
    restore_drill,
)
from video_media_catalog.gold_index_gc_cli import build_parser
from video_media_catalog.gold_search_index import (
    GOLD_INDEX_ACTIVATION_RECEIPT_MEDIA_TYPE,
    GOLD_INDEX_BUILD_MANIFEST_MEDIA_TYPE,
    GoldIndexActivationReceipt,
)
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.object_store import BoundedObjectStore
from video_media_catalog.storage import digest_file

ACTIVE = "media-catalog-research-" + ("a" * 24)
OLD = "media-catalog-research-" + ("b" * 24)
EXTRA_ONE = "media-catalog-research-" + ("c" * 24)
EXTRA_TWO = "media-catalog-research-" + ("d" * 24)
ALIAS = "media-catalog-research-read"
NOW = "2026-10-13T00:00:00Z"


class _NotFound(Exception):
    status_code = 404


class _Indices:
    def __init__(self, owner: _Client) -> None:
        self.owner = owner
        self.alias_calls = 0
        self.drift_alias_on_call: int | None = None

    def exists(self, *, index: str) -> bool:
        return index in self.owner.indexes

    def get_mapping(self, *, index: str) -> dict[str, Any]:
        value = self.owner.indexes[index]
        return {index: {"mappings": copy.deepcopy(value["mappings"])}}

    def get_settings(
        self,
        *,
        index: str,
        flat_settings: bool = False,
    ) -> dict[str, Any]:
        assert flat_settings is False
        value = self.owner.indexes[index]
        return {index: {"settings": copy.deepcopy(value["settings"])}}

    def refresh(self, *, index: str) -> None:
        assert index in self.owner.indexes

    def get_alias(self, *, name: str) -> dict[str, Any]:
        assert name == ALIAS
        self.alias_calls += 1
        targets = set(self.owner.alias_targets)
        if self.drift_alias_on_call == self.alias_calls:
            targets.add(OLD)
        return {index: {"aliases": {name: {}}} for index in targets}

    def get(self, *, index: str, **_kwargs: Any) -> dict[str, Any]:
        assert index == "media-catalog-research-*"
        return {
            name: {"settings": copy.deepcopy(value["settings"])}
            for name, value in self.owner.indexes.items()
        }

    def delete(self, *, index: str) -> None:
        self.owner.events.append("delete")
        self.owner.delete_calls += 1
        del self.owner.indexes[index]


class _Snapshot:
    def __init__(self, owner: _Client) -> None:
        self.owner = owner
        self.record: dict[str, Any] | None = None
        self.data: dict[str, Any] | None = None
        self.create_calls = 0
        self.restore_calls = 0
        self.last_create_body: dict[str, Any] | None = None
        self.last_restore_body: dict[str, Any] | None = None
        self.repository: dict[str, Any] | None = None

    def get(self, *, repository: str, snapshot: str) -> dict[str, Any]:
        assert repository == "research-archive"
        assert snapshot == "legacy-958"
        if self.record is None:
            raise _NotFound
        return {"snapshots": [copy.deepcopy(self.record)]}

    def create(
        self,
        *,
        repository: str,
        snapshot: str,
        body: dict[str, Any],
        wait_for_completion: bool,
    ) -> None:
        assert repository == "research-archive"
        assert snapshot == "legacy-958"
        assert wait_for_completion is False
        self.create_calls += 1
        self.last_create_body = copy.deepcopy(body)
        self.data = copy.deepcopy(self.owner.indexes[OLD])
        failed = self.owner.snapshot_failure
        self.record = {
            "snapshot": snapshot,
            "uuid": "snapshot-uuid",
            "state": "FAILED" if failed else "SUCCESS",
            "indices": list(body["indices"]),
            "include_global_state": body["include_global_state"],
            "metadata": copy.deepcopy(body["metadata"]),
            "failures": [{"reason": "failed"}] if failed else [],
            "shards": {
                "total": 1,
                "successful": 0 if failed else 1,
                "failed": 1 if failed else 0,
            },
        }

    def restore(
        self,
        *,
        repository: str,
        snapshot: str,
        body: dict[str, Any],
        wait_for_completion: bool,
    ) -> None:
        assert repository == "research-archive"
        assert snapshot == "legacy-958"
        assert wait_for_completion is False
        assert self.data is not None
        self.restore_calls += 1
        self.last_restore_body = copy.deepcopy(body)
        restored = copy.deepcopy(self.data)
        restored["settings"]["index"]["uuid"] = "restored-uuid"
        restored["settings"]["index"]["creation_date"] = "999"
        self.owner.indexes[OLD] = restored

    def get_repository(self, *, repository: str) -> dict[str, Any]:
        if self.repository is None:
            raise _NotFound
        return {repository: copy.deepcopy(self.repository)}

    def create_repository(
        self,
        *,
        repository: str,
        body: dict[str, Any],
        verify: bool,
    ) -> None:
        assert repository == "research-archive"
        assert verify is True
        self.repository = copy.deepcopy(body)


class _Cluster:
    def health(
        self,
        *,
        index: str,
    ) -> dict[str, Any]:
        assert index == OLD
        return {"status": "green", "timed_out": False}


class _Client:
    def __init__(self) -> None:
        mapping = {
            "dynamic": "strict",
            "properties": {"entityKey": {"type": "keyword"}},
        }
        self.indexes = {
            ACTIVE: {
                "mappings": copy.deepcopy(mapping),
                "settings": {
                    "index": {
                        "creation_date": "100",
                        "number_of_shards": "1",
                        "number_of_replicas": "1",
                        "uuid": "active-uuid",
                    }
                },
                "count": 1000,
            },
            OLD: {
                "mappings": copy.deepcopy(mapping),
                "settings": {
                    "index": {
                        "creation_date": "1",
                        "number_of_shards": "1",
                        "number_of_replicas": "1",
                        "uuid": "old-uuid",
                    }
                },
                "count": 958,
            },
        }
        self.alias_targets = {ACTIVE}
        self.events: list[str] = []
        self.delete_calls = 0
        self.snapshot_failure = False
        self.indices = _Indices(self)
        self.snapshot = _Snapshot(self)
        self.cluster = _Cluster()

    def count(self, *, index: str) -> dict[str, int]:
        return {"count": int(self.indexes[index]["count"])}


class _EventStore(BoundedObjectStore):
    def __init__(self, events: list[str]) -> None:
        super().__init__(client=object())
        self.events = events

    def publish_bytes_conditional(
        self,
        payload: bytes,
        destination_uri: str,
        *,
        media_type: str,
        object_format: str,
        max_bytes: int,
    ):
        self.events.append(f"publish:{media_type}")
        return super().publish_bytes_conditional(
            payload,
            destination_uri,
            media_type=media_type,
            object_format=object_format,
            max_bytes=max_bytes,
        )


def _paths(tmp_path: Path, suffix: str = "") -> tuple[str, str, str]:
    return (
        (tmp_path / f"archive{suffix}.json").as_uri(),
        (tmp_path / f"gc{suffix}.json").as_uri(),
        (tmp_path / f"restore{suffix}.json").as_uri(),
    )


def _activation_ref(
    tmp_path: Path,
    *,
    expected_old: str = OLD,
    reason: str = "Approved Research index activation.",
) -> ObjectRef:
    receipt = GoldIndexActivationReceipt(
        build_manifest=ObjectRef(
            uri=(tmp_path / "build-manifest.json").as_uri(),
            format="OBJECT_FORMAT_JSON",
            media_type=GOLD_INDEX_BUILD_MANIFEST_MEDIA_TYPE,
            checksum=Checksum(value="1" * 64),
            size_bytes=100,
        ),
        build_id="2" * 64,
        release_plan_id="sha256:" + ("3" * 64),
        mapping_digest="sha256:" + ("4" * 64),
        config_digest="sha256:" + ("5" * 64),
        index=ACTIVE,
        alias=ALIAS,
        expected_previous_indices=(expected_old,),
        changed=True,
        authorized_by="group:world-release-managers",
        authorization_reason=reason,
        activated_at=NOW,
    )
    payload = receipt.json_bytes()
    identity = hashlib.sha256(payload).hexdigest()[:16]
    path = tmp_path / f"activation-{identity}.json"
    path.write_bytes(payload)
    digest, size = digest_file(path)
    return ObjectRef(
        uri=path.as_uri(),
        format="OBJECT_FORMAT_JSON",
        media_type=GOLD_INDEX_ACTIVATION_RECEIPT_MEDIA_TYPE,
        checksum=Checksum(value=digest),
        size_bytes=size,
    )


def _archive(
    client: _Client,
    store: BoundedObjectStore,
    tmp_path: Path,
    *,
    execute: bool = True,
    operation_id: str = "legacy-958",
    activation_receipt: ObjectRef | None = None,
    reuse_archive_receipt: ObjectRef | None = None,
    additional_archive_indices: tuple[str, ...] = (),
    suffix: str = "",
) -> dict[str, Any]:
    archive_uri, gc_uri, _ = _paths(tmp_path, suffix)
    return archive_delete(
        client,
        store,
        operation_id=operation_id,
        repository="research-archive",
        snapshot="legacy-958",
        old_index=OLD,
        active_index=ACTIVE,
        activation_receipt=activation_receipt or _activation_ref(tmp_path),
        archive_receipt_uri=archive_uri,
        gc_result_uri=gc_uri,
        completed_at=NOW,
        delete_before_epoch_millis=50,
        additional_archive_indices=additional_archive_indices,
        execute=execute,
        reuse_archive_receipt=reuse_archive_receipt,
        snapshot_poll_seconds=0.01,
        sleep=lambda _seconds: None,
    )


def test_archive_delete_defaults_to_dry_run(tmp_path: Path) -> None:
    parsed = build_parser().parse_args(
        [
            "archive-delete",
            "--opensearch-endpoint",
            "http://localhost:9200",
            "--allow-insecure-opensearch",
            "--operation-id",
            "legacy-958",
            "--repository",
            "research-archive",
            "--snapshot",
            "legacy-958",
            "--old-index",
            OLD,
            "--active-index",
            ACTIVE,
            "--archive-receipt-uri",
            "file:///tmp/archive.json",
            "--gc-result-uri",
            "file:///tmp/gc.json",
            "--activation-receipt-uri",
            "file:///tmp/activation.json",
            "--activation-receipt-hash",
            "a" * 64,
            "--activation-receipt-size",
            "100",
            "--completed-at",
            NOW,
            "--delete-before-epoch-millis",
            "50",
        ]
    )

    assert parsed.execute is False
    client = _Client()
    result = _archive(
        client,
        BoundedObjectStore(client=object()),
        tmp_path,
        execute=False,
    )
    assert result["status"] == "DRY_RUN"
    assert result["documentCount"] == 958
    assert client.snapshot.create_calls == 0
    assert client.delete_calls == 0


def test_active_index_is_never_archived_or_deleted(tmp_path: Path) -> None:
    client = _Client()
    client.alias_targets.add(OLD)

    with pytest.raises(RuntimeError, match="still attached"):
        _archive(client, BoundedObjectStore(client=object()), tmp_path)

    assert client.snapshot.create_calls == 0
    assert client.delete_calls == 0


def test_snapshot_failure_performs_zero_deletes(tmp_path: Path) -> None:
    client = _Client()
    client.snapshot_failure = True

    with pytest.raises(RuntimeError, match="state FAILED"):
        _archive(client, BoundedObjectStore(client=object()), tmp_path)

    assert client.delete_calls == 0
    assert OLD in client.indexes


def test_alias_second_check_drift_performs_zero_deletes(tmp_path: Path) -> None:
    client = _Client()
    client.indices.drift_alias_on_call = 2

    with pytest.raises(RuntimeError, match="still attached"):
        _archive(client, BoundedObjectStore(client=object()), tmp_path)

    assert client.snapshot.create_calls == 1
    assert client.delete_calls == 0
    assert OLD in client.indexes


def test_archive_receipt_precedes_delete_and_result_follows(
    tmp_path: Path,
) -> None:
    client = _Client()
    store = _EventStore(client.events)

    result = _archive(client, store, tmp_path)

    assert result["status"] == "DELETED"
    assert client.snapshot.last_create_body is not None
    assert client.snapshot.last_create_body["indices"] == [OLD]
    assert client.snapshot.last_create_body["include_global_state"] is False
    assert client.snapshot.last_create_body["partial"] is False
    assert (
        client.snapshot.last_create_body["metadata"]["videoMediaCatalogOperationId"]
        == "legacy-958"
    )
    assert client.events == [
        f"publish:{GOLD_INDEX_ARCHIVE_RECEIPT_MEDIA_TYPE}",
        "delete",
        f"publish:{GOLD_INDEX_GC_RESULT_MEDIA_TYPE}",
    ]
    archive_uri, gc_uri, _ = _paths(tmp_path)
    archive = GoldIndexArchiveReceipt.model_validate_json(
        Path(archive_uri.removeprefix("file://")).read_bytes()
    )
    gc = GoldIndexGcResultReceipt.model_validate_json(
        Path(gc_uri.removeprefix("file://")).read_bytes()
    )
    assert archive.document_count == 958
    assert archive.activation_receipt == gc.activation_receipt
    assert gc.archive_receipt.checksum.value


def test_batch_archive_deletes_failed_shadows_and_restores_only_previous(
    tmp_path: Path,
) -> None:
    client = _Client()
    for index_name, uuid in (
        (EXTRA_ONE, "extra-one-uuid"),
        (EXTRA_TWO, "extra-two-uuid"),
    ):
        client.indexes[index_name] = copy.deepcopy(client.indexes[OLD])
        client.indexes[index_name]["settings"]["index"]["uuid"] = uuid
    store = BoundedObjectStore(client=object())
    activation = _activation_ref(tmp_path)

    first = _archive(
        client,
        store,
        tmp_path,
        activation_receipt=activation,
        additional_archive_indices=(EXTRA_ONE, EXTRA_TWO),
    )

    expected_snapshot_indices = tuple(sorted((OLD, EXTRA_ONE, EXTRA_TWO)))
    assert client.snapshot.last_create_body is not None
    assert client.snapshot.last_create_body["indices"] == list(
        expected_snapshot_indices
    )
    assert client.delete_calls == 3
    assert set(client.indexes) == {ACTIVE}
    archive = GoldIndexArchiveReceipt.model_validate_json(
        Path(_paths(tmp_path)[0].removeprefix("file://")).read_bytes()
    )
    assert archive.snapshot_indices == expected_snapshot_indices

    archive_ref = ObjectRef.model_validate(first["archiveReceipt"])
    _, _, restore_uri = _paths(tmp_path)
    restore_drill(
        client,
        store,
        operation_id="restore-batch-958",
        archive_receipt=archive_ref,
        restore_receipt_uri=restore_uri,
        completed_at=NOW,
        restore_poll_seconds=0.01,
        sleep=lambda _seconds: None,
    )
    assert set(client.indexes) == {ACTIVE, OLD}
    assert client.snapshot.last_restore_body is not None
    assert client.snapshot.last_restore_body["indices"] == [OLD]

    second = _archive(
        client,
        store,
        tmp_path,
        operation_id="legacy-batch-958-redo",
        activation_receipt=activation,
        reuse_archive_receipt=archive_ref,
        suffix="-redo",
    )
    assert second["status"] == "DELETED"
    assert client.snapshot.create_calls == 1
    assert set(client.indexes) == {ACTIVE}


def test_gc_is_idempotent_and_reuses_snapshot_after_restore(
    tmp_path: Path,
) -> None:
    client = _Client()
    store = BoundedObjectStore(client=object())
    activation = _activation_ref(tmp_path)
    first = _archive(
        client,
        store,
        tmp_path,
        activation_receipt=activation,
    )
    repeated = _archive(
        client,
        store,
        tmp_path,
        activation_receipt=activation,
    )

    assert repeated["reused"] is True
    assert client.snapshot.create_calls == 1
    assert client.delete_calls == 1
    with pytest.raises(RuntimeError, match="existing GC receipt"):
        _archive(
            client,
            store,
            tmp_path,
            activation_receipt=_activation_ref(
                tmp_path,
                reason="Conflicting idempotent activation receipt.",
            ),
        )
    assert client.snapshot.create_calls == 1
    assert client.delete_calls == 1

    archive_ref = ObjectRef.model_validate(first["archiveReceipt"])
    _, _, restore_uri = _paths(tmp_path)
    restored = restore_drill(
        client,
        store,
        operation_id="restore-958",
        archive_receipt=archive_ref,
        restore_receipt_uri=restore_uri,
        completed_at=NOW,
        restore_poll_seconds=0.01,
        sleep=lambda _seconds: None,
    )
    assert restored["status"] == "RESTORED"
    assert OLD in client.indexes
    assert client.alias_targets == {ACTIVE}
    assert client.snapshot.last_restore_body == {
        "indices": [OLD],
        "include_global_state": False,
        "include_aliases": False,
    }
    receipt = GoldIndexRestoreReceipt.model_validate_json(
        Path(restore_uri.removeprefix("file://")).read_bytes()
    )
    assert receipt.restored_index == OLD
    assert receipt.alias_indices_before == receipt.alias_indices_after == (ACTIVE,)
    repeated_restore = restore_drill(
        client,
        store,
        operation_id="restore-958",
        archive_receipt=archive_ref,
        restore_receipt_uri=restore_uri,
        completed_at="2026-10-14T00:00:00Z",
    )
    assert repeated_restore["reused"] is True
    assert client.snapshot.restore_calls == 1

    with pytest.raises(RuntimeError, match="archive receipt"):
        _archive(
            client,
            store,
            tmp_path,
            operation_id="legacy-958-conflict",
            activation_receipt=_activation_ref(
                tmp_path,
                reason="A different activation receipt.",
            ),
            reuse_archive_receipt=archive_ref,
            suffix="-conflict",
        )
    assert client.snapshot.create_calls == 1
    assert client.delete_calls == 1

    second = _archive(
        client,
        store,
        tmp_path,
        operation_id="legacy-958-redo",
        activation_receipt=activation,
        reuse_archive_receipt=archive_ref,
        suffix="-redo",
    )
    assert second["status"] == "DELETED"
    assert client.snapshot.create_calls == 1
    assert client.delete_calls == 2


def test_activation_receipt_must_bind_exact_active_old_pair(
    tmp_path: Path,
) -> None:
    client = _Client()
    other_old = "media-catalog-research-" + ("c" * 24)

    with pytest.raises(RuntimeError, match="does not authorize"):
        _archive(
            client,
            BoundedObjectStore(client=object()),
            tmp_path,
            activation_receipt=_activation_ref(
                tmp_path,
                expected_old=other_old,
            ),
        )

    assert client.snapshot.create_calls == 0
    assert client.delete_calls == 0


def test_repository_registration_is_exact_and_idempotent() -> None:
    client = _Client()
    arguments = {
        "repository": "research-archive",
        "bucket": "video-world-snapshots",
        "base_path": "research/gold",
        "region": "us-east-1",
        "role_arn": "arn:aws:iam::123456789012:role/research-snapshot",
    }

    first = register_snapshot_repository(client, **arguments)
    repeated = register_snapshot_repository(client, **arguments)

    assert first["reused"] is False
    assert repeated["reused"] is True
    assert client.snapshot.repository is not None
    client.snapshot.repository["settings"]["base_path"] = "other/path"
    with pytest.raises(RuntimeError, match="conflicting"):
        register_snapshot_repository(client, **arguments)


def test_restore_receipt_has_expected_media_type(tmp_path: Path) -> None:
    client = _Client()
    store = BoundedObjectStore(client=object())
    result = _archive(client, store, tmp_path)
    archive_ref = ObjectRef.model_validate(result["archiveReceipt"])
    _, _, restore_uri = _paths(tmp_path)

    restored = restore_drill(
        client,
        store,
        operation_id="restore-media",
        archive_receipt=archive_ref,
        restore_receipt_uri=restore_uri,
        completed_at=NOW,
        restore_poll_seconds=0.01,
        sleep=lambda _seconds: None,
    )

    assert (
        restored["restoreReceipt"]["mediaType"] == GOLD_INDEX_RESTORE_RECEIPT_MEDIA_TYPE
    )
