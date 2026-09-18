"""Bounded-memory Wikidata JSON dump reader."""

from __future__ import annotations

import bz2
import gzip
import json
import re
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, TextIO

from video_media_catalog.canonical import canonical_json, deterministic_key, source_hash
from video_media_catalog.constants import RELEVANT_WIKIDATA_PROPERTIES
from video_media_catalog.models import LandingRecord

_ENTITY_ID = re.compile(r"^[A-Z][1-9][0-9]*$")


class WikidataParseError(ValueError):
    def __init__(self, path: Path, line_number: int, message: str) -> None:
        super().__init__(f"{path}:{line_number}: {message}")
        self.path = path
        self.line_number = line_number


def detect_compression(path: Path) -> str:
    with path.open("rb") as handle:
        magic = handle.read(3)
    if magic.startswith(b"\x1f\x8b"):
        return "gzip"
    if magic.startswith(b"BZh"):
        return "bzip2"
    return "plain"


@contextmanager
def open_text_dump(path: Path) -> Iterator[TextIO]:
    compression = detect_compression(path)
    if compression == "gzip":
        opener = gzip.open
    elif compression == "bzip2":
        opener = bz2.open
    else:
        opener = Path.open
    with opener(path, mode="rt", encoding="utf-8-sig", newline="") as handle:
        yield handle


def iter_wikidata_entities(path: Path) -> Iterator[dict[str, Any]]:
    """Yield one entity at a time without loading the dump array."""

    seen_content = False
    with open_text_dump(path) as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            text = raw_line.strip()
            if not text:
                continue
            if not seen_content:
                seen_content = True
                if text.startswith("["):
                    text = text[1:].lstrip()
            if not text or text == "]":
                continue
            if text.endswith("]"):
                text = text[:-1].rstrip()
            if text.endswith(","):
                text = text[:-1].rstrip()
            if not text:
                continue
            try:
                entity = json.loads(text)
            except json.JSONDecodeError as exc:
                raise WikidataParseError(
                    path,
                    line_number,
                    f"invalid one-entity-per-line JSON: {exc.msg}",
                ) from exc
            if not isinstance(entity, dict):
                raise WikidataParseError(
                    path, line_number, "entity line must contain a JSON object"
                )
            yield entity


def _copy_language_map(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    return {
        str(language): payload
        for language, payload in value.items()
        if isinstance(payload, dict)
    }


def _copy_aliases(value: Any) -> dict[str, list[dict[str, Any]]]:
    if not isinstance(value, dict):
        return {}
    aliases: dict[str, list[dict[str, Any]]] = {}
    for language, candidates in value.items():
        if not isinstance(candidates, list):
            continue
        copied = [candidate for candidate in candidates if isinstance(candidate, dict)]
        if copied:
            aliases[str(language)] = copied
    return aliases


def _copy_sitelinks(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    result: dict[str, Any] = {}
    for site, link in value.items():
        if not isinstance(link, dict):
            continue
        result[str(site)] = {
            key: link[key] for key in ("site", "title", "badges", "url") if key in link
        }
    return result


def _copy_statement(statement: Any) -> dict[str, Any] | None:
    if not isinstance(statement, dict):
        return None
    copied = {
        key: statement[key]
        for key in ("id", "rank", "mainsnak", "qualifiers", "qualifiers-order")
        if key in statement
    }
    if not isinstance(copied.get("mainsnak"), dict):
        return None
    copied.setdefault("rank", "normal")
    copied.setdefault("qualifiers", {})
    return copied


def normalize_wikidata_entity(entity: dict[str, Any]) -> dict[str, Any]:
    entity_id = entity.get("id")
    if not isinstance(entity_id, str) or _ENTITY_ID.fullmatch(entity_id) is None:
        raise ValueError("Wikidata entity is missing a valid id")
    raw_claims = entity.get("claims")
    claims: dict[str, list[dict[str, Any]]] = {}
    if isinstance(raw_claims, dict):
        for prop in sorted(RELEVANT_WIKIDATA_PROPERTIES):
            statements = raw_claims.get(prop)
            if not isinstance(statements, list):
                continue
            copied = [
                result
                for statement in statements
                if (result := _copy_statement(statement)) is not None
            ]
            if copied:
                claims[prop] = copied
    return {
        "id": entity_id,
        "type": entity.get("type", "item"),
        "lastrevid": entity.get("lastrevid"),
        "modified": entity.get("modified"),
        "labels": _copy_language_map(entity.get("labels")),
        "descriptions": _copy_language_map(entity.get("descriptions")),
        "aliases": _copy_aliases(entity.get("aliases")),
        "sitelinks": _copy_sitelinks(entity.get("sitelinks")),
        "claims": claims,
    }


def landing_record(entity: dict[str, Any]) -> LandingRecord:
    payload = normalize_wikidata_entity(entity)
    digest = source_hash(payload)
    revision = payload.get("lastrevid")
    source_revision = None if revision is None else str(revision)
    return LandingRecord(
        record_key=deterministic_key(
            "source-record",
            {
                "source": "wikidata",
                "sourceRecordId": payload["id"],
                "sourceRevision": source_revision,
                "sourceHash": digest,
            },
        ),
        source="wikidata",
        source_record_id=payload["id"],
        source_revision=source_revision,
        modified=payload.get("modified"),
        source_hash=digest,
        payload_json=canonical_json(payload),
    )


def iter_wikidata_records(path: Path) -> Iterator[LandingRecord]:
    for entity in iter_wikidata_entities(path):
        yield landing_record(entity)
