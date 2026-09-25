"""OpenSearch helpers shared by Gold batch index builds."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

DEFAULT_MAX_BULK_BYTES = 5 * 1024 * 1024

_SAFE_INDEX_NAME = re.compile(r"^[a-z0-9][a-z0-9_-]{0,254}$")


def _validate_index_name(value: str, *, label: str) -> str:
    if _SAFE_INDEX_NAME.fullmatch(value) is None:
        raise ValueError(f"{label} is not a safe lowercase OpenSearch name")
    return value


def _status_code(exc: Exception) -> int | None:
    status = getattr(exc, "status_code", None)
    if isinstance(status, int):
        return status
    info = getattr(exc, "info", None)
    if isinstance(info, Mapping):
        raw = info.get("status")
        return raw if isinstance(raw, int) else None
    return None


@dataclass
class BulkResult:
    document_count: int = 0
    error_count: int = 0
    errors: list[dict[str, Any]] = field(default_factory=list)

    def merge(self, other: BulkResult) -> None:
        self.document_count += other.document_count
        self.error_count += other.error_count
        remaining = max(0, 20 - len(self.errors))
        self.errors.extend(other.errors[:remaining])

    def as_dict(self) -> dict[str, Any]:
        return {
            "documentCount": self.document_count,
            "errorCount": self.error_count,
            "errors": self.errors,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> BulkResult:
        return cls(
            document_count=int(value["documentCount"]),
            error_count=int(value["errorCount"]),
            errors=list(value.get("errors") or []),
        )


def index_document_count(client: Any, *, index_name: str) -> int:
    client.indices.refresh(index=index_name)
    response = client.count(index=index_name)
    count = response.get("count")
    if not isinstance(count, int) or isinstance(count, bool) or count < 0:
        raise RuntimeError("OpenSearch count response is invalid")
    return count


def current_alias_indices(client: Any, *, alias: str) -> list[str]:
    _validate_index_name(alias, label="read alias")
    try:
        response = client.indices.get_alias(name=alias)
    except Exception as exc:
        if _status_code(exc) == 404:
            return []
        raise
    if not isinstance(response, Mapping):
        raise RuntimeError("OpenSearch alias response is invalid")
    return sorted(str(index) for index in response)


def switch_read_alias(client: Any, *, alias: str, target_index: str) -> bool:
    """Atomically point one read alias at exactly one concrete index."""

    _validate_index_name(alias, label="read alias")
    _validate_index_name(target_index, label="target index")
    current = current_alias_indices(client, alias=alias)
    if current == [target_index]:
        return False
    actions = [
        {"remove": {"index": index, "alias": alias}}
        for index in current
        if index != target_index
    ]
    actions.append(
        {
            "add": {
                "index": target_index,
                "alias": alias,
                "is_write_index": False,
            }
        }
    )
    client.indices.update_aliases(body={"actions": actions})
    return True
