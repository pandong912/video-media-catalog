from __future__ import annotations

from typing import Any

import pytest

from video_media_catalog.opensearch_index import switch_read_alias


class _Indices:
    def __init__(self, targets: list[str]) -> None:
        self.targets = set(targets)
        self.update_calls: list[dict[str, Any]] = []

    def get_alias(self, *, name: str) -> dict[str, Any]:
        return {index: {"aliases": {name: {}}} for index in self.targets}

    def update_aliases(self, *, body: dict[str, Any]) -> None:
        self.update_calls.append(body)
        for action in body["actions"]:
            if "remove" in action:
                self.targets.discard(action["remove"]["index"])
            if "add" in action:
                self.targets.add(action["add"]["index"])


class _Client:
    def __init__(self, targets: list[str]) -> None:
        self.indices = _Indices(targets)


def test_alias_switch_rejects_unexpected_current_target() -> None:
    client = _Client(["media-catalog-research-old"])

    with pytest.raises(RuntimeError, match="compare-and-swap failed"):
        switch_read_alias(
            client,
            alias="media-catalog-research-read",
            target_index="media-catalog-research-new",
            expected_current=["media-catalog-research-other"],
        )

    assert client.indices.targets == {"media-catalog-research-old"}
    assert client.indices.update_calls == []


def test_alias_switch_verifies_exact_postcondition() -> None:
    client = _Client(["media-catalog-research-old"])

    assert switch_read_alias(
        client,
        alias="media-catalog-research-read",
        target_index="media-catalog-research-new",
        expected_current=["media-catalog-research-old"],
    )

    assert client.indices.targets == {"media-catalog-research-new"}


def test_alias_switch_retry_is_idempotent_after_activation() -> None:
    client = _Client(["media-catalog-research-new"])

    assert not switch_read_alias(
        client,
        alias="media-catalog-research-read",
        target_index="media-catalog-research-new",
        expected_current=["media-catalog-research-old"],
    )
    assert client.indices.update_calls == []
