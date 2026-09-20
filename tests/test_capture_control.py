from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from video_media_catalog.connector import (
    CaptureWindowReceipt,
    CaptureWindowStatus,
    SourceWatermark,
    build_capture_window_receipt,
    plan_bounded_capture_windows,
    select_capture_window,
    source_watermark_from_receipt,
)
from video_media_catalog.connector_publish import (
    publish_capture_window_commit,
    publish_capture_window_receipt,
)
from video_media_catalog.models import ObjectRef
from video_media_catalog.object_store import (
    BoundedObjectStore,
    ObjectStoreError,
    UploadResult,
)

_DIGESTS = {
    "config_digest": "sha256:" + ("a" * 64),
    "image_digest": "sha256:" + ("b" * 64),
    "policy_digest": "sha256:" + ("c" * 64),
}


def _batch_object(tmp_path: Path) -> tuple[BoundedObjectStore, ObjectRef]:
    store = BoundedObjectStore(client=object())
    result = store.publish_bytes_conditional(
        b'{"batchId":"sha256:test"}\n',
        (tmp_path / "batch.json").as_uri(),
        media_type=("application/vnd.video-media-catalog.connector-batch.v2+json"),
        object_format="OBJECT_FORMAT_JSON",
        max_bytes=1024,
    )
    return store, result.object_ref


def _receipt(batch_ref: ObjectRef, **overrides) -> CaptureWindowReceipt:
    values = {
        "source_product_id": "tvmaze-public-api",
        "window_start": "2026-09-20T00:00:00Z",
        "window_end": "2026-09-20T23:59:59Z",
        "cursor": "sha256:" + ("d" * 64),
        "watermark": "2026-09-20T23:59:59Z",
        "batch_object": batch_ref,
        "status": CaptureWindowStatus.COMMITTED,
        **_DIGESTS,
    }
    values.update(overrides)
    return build_capture_window_receipt(**values)


def test_watermark_and_receipt_are_canonical_deterministic_and_frozen(
    tmp_path: Path,
) -> None:
    _, batch_object = _batch_object(tmp_path)
    receipt = _receipt(batch_object)
    replay = _receipt(batch_object)

    assert receipt == replay
    assert receipt.json_bytes() == replay.json_bytes()
    assert receipt.json_bytes().endswith(b"\n")
    assert receipt == CaptureWindowReceipt.model_validate_json(receipt.json_bytes())

    watermark = source_watermark_from_receipt(receipt)
    assert watermark == SourceWatermark.model_validate_json(watermark.json_bytes())
    assert watermark.watermark_id.startswith("sha256:")

    tampered = json.loads(receipt.json_bytes())
    tampered["configDigest"] = "sha256:" + ("e" * 64)
    with pytest.raises(ValidationError, match="receipt_id"):
        CaptureWindowReceipt.model_validate(tampered)
    with pytest.raises(ValidationError, match="frozen"):
        receipt.status = CaptureWindowStatus.EMPTY


def test_failed_receipt_cannot_bind_batch_or_advance_watermark(tmp_path: Path) -> None:
    _, batch_object = _batch_object(tmp_path)
    with pytest.raises(ValidationError, match="requires exactly one"):
        _receipt(batch_object, status=CaptureWindowStatus.FAILED)

    failed = _receipt(
        batch_object,
        status=CaptureWindowStatus.FAILED,
        batch_object=None,
    )
    with pytest.raises(ValueError, match="cannot advance"):
        source_watermark_from_receipt(failed)


def test_bounded_planner_covers_every_item_without_truncation() -> None:
    keys = tuple(f"movie:{index:05d}" for index in range(40_001))
    plans = plan_bounded_capture_windows(
        source_product_id="tmdb-research",
        window_start="2026-09-01T00:00:00Z",
        window_end="2026-09-14T23:59:59Z",
        item_keys=keys,
        max_items=20_000,
        watermark="2026-09-14",
    )
    replay = plan_bounded_capture_windows(
        source_product_id="tmdb-research",
        window_start="2026-09-01T00:00:00Z",
        window_end="2026-09-14T23:59:59Z",
        item_keys=keys,
        max_items=20_000,
        watermark="2026-09-14",
    )

    assert plans == replay
    assert [plan.item_count for plan in plans] == [20_000, 20_000, 1]
    assert [plan.item_offset for plan in plans] == [0, 20_000, 40_000]
    assert sum(plan.item_count for plan in plans) == len(keys)
    assert len({plan.cursor for plan in plans}) == len(plans)
    with pytest.raises(ValueError, match="explicit window cursors"):
        select_capture_window(plans, cursor=None)
    assert select_capture_window(plans, cursor=plans[1].cursor) == plans[1]


def test_empty_inventory_still_produces_explicit_empty_window() -> None:
    plans = plan_bounded_capture_windows(
        source_product_id="tvmaze-public-api",
        window_start="2026-09-20T00:00:00Z",
        window_end="2026-09-20T23:59:59Z",
        item_keys=(),
        max_items=20_000,
    )
    assert len(plans) == 1
    assert plans[0].item_offset == 0
    assert plans[0].item_count == 0
    assert plans[0].total_items == 0


class _RecordingStore:
    def __init__(self, delegate: BoundedObjectStore) -> None:
        self.delegate = delegate
        self.events: list[str] = []

    def verify(self, object_ref: ObjectRef, *, max_bytes: int) -> None:
        self.events.append("verify-batch")
        self.delegate.verify(object_ref, max_bytes=max_bytes)

    def upload_bytes(
        self,
        payload: bytes,
        destination_uri: str,
        *,
        media_type: str,
        object_format: str,
        max_bytes: int,
    ) -> UploadResult:
        self.events.append(
            "publish-receipt"
            if "capture-window-receipt" in media_type
            else ("publish-watermark")
        )
        return self.delegate.upload_bytes(
            payload,
            destination_uri,
            media_type=media_type,
            object_format=object_format,
            max_bytes=max_bytes,
        )


def test_window_control_is_commit_last_and_conflicts_fail_closed(
    tmp_path: Path,
) -> None:
    delegate, batch_object = _batch_object(tmp_path)
    store = _RecordingStore(delegate)
    receipt = _receipt(batch_object)
    destination = (tmp_path / "control").as_uri()

    published = publish_capture_window_commit(
        destination_prefix=destination,
        receipt=receipt,
        store=store,
    )
    assert store.events == [
        "verify-batch",
        "publish-watermark",
        "publish-receipt",
    ]
    assert Path(published.source_watermark_object.uri.removeprefix("file://")).exists()
    assert Path(published.receipt_object.uri.removeprefix("file://")).exists()

    repeated = publish_capture_window_receipt(
        destination_prefix=destination,
        receipt=receipt,
        store=delegate,
    )
    assert repeated.reused

    divergent = _receipt(
        batch_object,
        config_digest="sha256:" + ("f" * 64),
    )
    with pytest.raises(ObjectStoreError) as raised:
        publish_capture_window_receipt(
            destination_prefix=destination,
            receipt=divergent,
            store=delegate,
        )
    assert raised.value.code == "IMMUTABLE_OBJECT_CONFLICT"
