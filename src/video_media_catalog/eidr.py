"""Offline, namespace-tolerant EIDR XML parser."""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Protocol

from defusedxml.common import DefusedXmlException
from defusedxml.ElementTree import iterparse

_EIDR_ID = re.compile(
    r"^10\.5240/(?:[0-9A-Z]{4}-){5}[0-9A-Z]$",
    re.IGNORECASE,
)
_IMDB_ID = re.compile(
    r"^(?:tt|nm|co|ev|ch|ni)[0-9]{7,8}$",
    re.IGNORECASE,
)
_RECORD_ELEMENTS = frozenset({"FullMetadata", "EIDRRecord", "Record"})
_TITLE_ELEMENTS = frozenset(
    {
        "ResourceName",
        "Title",
        "OriginalTitle",
        "AlternateTitle",
        "AlternateResourceName",
    }
)


class EidrParseError(ValueError):
    pass


class EidrProviderNotConfiguredError(RuntimeError):
    pass


class EidrProvider(Protocol):
    """Explicitly configured exact-lookup provider; no default implementation."""

    def fetch_by_eidr_id(self, eidr_id: str) -> bytes: ...

    def fetch_by_imdb_id(self, imdb_id: str) -> bytes: ...


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].rsplit(":", 1)[-1]


def _text(element: ET.Element) -> str | None:
    value = "".join(element.itertext()).strip()
    return value or None


def _descendants(element: ET.Element, names: set[str] | frozenset[str]):
    for candidate in element.iter():
        if _local_name(candidate.tag) in names:
            yield candidate


def normalize_eidr_id(value: str) -> str:
    normalized = value.strip()
    for prefix in ("https://doi.org/", "http://doi.org/", "doi:"):
        if normalized.lower().startswith(prefix):
            normalized = normalized[len(prefix) :]
            break
    normalized = normalized.upper()
    if _EIDR_ID.fullmatch(normalized) is None:
        raise ValueError(f"invalid EIDR ID: {value!r}")
    return normalized


def normalize_imdb_id(value: str) -> str:
    normalized = value.strip().lower()
    if _IMDB_ID.fullmatch(normalized) is None:
        raise ValueError(f"invalid IMDb ID: {value!r}")
    return normalized


def _first_text(element: ET.Element, names: set[str]) -> str | None:
    for candidate in _descendants(element, names):
        if value := _text(candidate):
            return value
    return None


def _language(element: ET.Element) -> str | None:
    for key, value in element.attrib.items():
        if _local_name(key).lower() in {"lang", "language"} and value.strip():
            return value.strip()
    return None


def _extract_titles(element: ET.Element) -> list[dict[str, str]]:
    titles: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for candidate in _descendants(element, _TITLE_ELEMENTS):
        value = _text(candidate)
        if not value:
            continue
        local = _local_name(candidate.tag)
        name_type = (
            "PRIMARY"
            if local in {"ResourceName", "Title", "OriginalTitle"}
            else "ALIAS"
        )
        language = _language(candidate) or "und"
        identity = (name_type, language, value)
        if identity not in seen:
            seen.add(identity)
            titles.append({"type": name_type, "language": language, "value": value})
    return titles


def _extract_values(element: ET.Element, names: set[str]) -> list[str]:
    return sorted(
        {
            value
            for candidate in _descendants(element, names)
            if (value := _text(candidate))
        }
    )


def _extract_alternate_ids(element: ET.Element) -> list[dict[str, str]]:
    identifiers: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for candidate in _descendants(element, {"AlternateID", "AlternateIdentifier"}):
        value = _text(candidate)
        if not value:
            continue
        domain = next(
            (
                attr_value
                for attr_name, attr_value in candidate.attrib.items()
                if _local_name(attr_name).lower()
                in {"domain", "type", "scheme", "namespace"}
            ),
            "",
        ).lower()
        if _IMDB_ID.fullmatch(value.strip()):
            scheme, normalized = "imdb", normalize_imdb_id(value)
        elif _EIDR_ID.fullmatch(value.strip()):
            scheme, normalized = "eidr", normalize_eidr_id(value)
        else:
            scheme = domain or "alternate"
            normalized = value.strip()
        identity = (scheme, normalized)
        if identity not in seen:
            seen.add(identity)
            identifiers.append({"scheme": scheme, "value": normalized})
    return identifiers


def _extract_parent_relations(element: ET.Element) -> list[dict[str, str]]:
    relation_types = {
        "SeriesInfo": "PART_OF_SERIES",
        "SeasonInfo": "PART_OF_SERIES",
        "EpisodeInfo": "PART_OF_SEASON",
        "EditInfo": "EDIT_OF",
    }
    relations: list[dict[str, str]] = []
    seen: set[tuple[str, str, str | None]] = set()
    for container in element.iter():
        relation_type = relation_types.get(_local_name(container.tag))
        if relation_type is None:
            continue
        ordinal = _first_text(
            container, {"SequenceNumber", "EpisodeNumber", "SeasonNumber"}
        )
        for parent in _descendants(
            container, {"Parent", "ParentID", "SeriesID", "SeasonID"}
        ):
            value = _text(parent)
            if not value:
                continue
            match = re.search(r"10\.5240/[0-9A-Z-]+", value, re.IGNORECASE)
            if match is None:
                continue
            parent_id = normalize_eidr_id(match.group(0))
            identity = (relation_type, parent_id, ordinal)
            if identity in seen:
                continue
            seen.add(identity)
            relation: dict[str, str] = {
                "type": relation_type,
                "targetEidrId": parent_id,
            }
            if ordinal:
                relation["ordinal"] = ordinal
            relations.append(relation)
    return relations


def _record_type(element: ET.Element) -> str | None:
    present = {_local_name(candidate.tag) for candidate in element.iter()}
    for xml_name, record_type in (
        ("EpisodeInfo", "EPISODE"),
        ("SeasonInfo", "SEASON"),
        ("SeriesInfo", "SERIES"),
        ("EditInfo", "EDIT"),
    ):
        if xml_name in present:
            return record_type
    return None


def parse_eidr_element(element: ET.Element) -> dict[str, object]:
    raw_id = _first_text(element, {"EIDR-ID", "EIDRID", "ID", "Identifier"})
    if raw_id is None:
        raise EidrParseError("EIDR record is missing an ID")
    try:
        eidr_id = normalize_eidr_id(raw_id)
    except ValueError as exc:
        raise EidrParseError(str(exc)) from exc
    referent_type = _first_text(element, {"ReferentType"})
    structural_type = _first_text(element, {"StructuralType"})
    modified = _first_text(
        element, {"LastModificationDate", "LastModified", "Modified"}
    )
    return {
        "id": eidr_id,
        "referentType": referent_type,
        "structuralType": structural_type,
        "recordType": _record_type(element),
        "titles": _extract_titles(element),
        "languages": _extract_values(
            element, {"OriginalLanguage", "Language", "VersionLanguage"}
        ),
        "releaseDate": _first_text(element, {"ReleaseDate", "OriginalReleaseDate"}),
        "duration": _first_text(element, {"ApproximateLength", "Duration", "Runtime"}),
        "countries": _extract_values(
            element, {"CountryOfOrigin", "Country", "ReleaseCountry"}
        ),
        "parentRelations": _extract_parent_relations(element),
        "alternateIds": _extract_alternate_ids(element),
        "modified": modified,
    }


def _reject_unsafe_xml(path: Path) -> None:
    with path.open("rb") as handle:
        prefix = handle.read(64 * 1024).upper()
    if b"<!DOCTYPE" in prefix or b"<!ENTITY" in prefix:
        raise EidrParseError("DOCTYPE and ENTITY declarations are not accepted")


def iter_eidr_payloads(path: Path) -> Iterator[dict[str, object]]:
    """Stream local XML records and clear parsed elements promptly."""

    _reject_unsafe_xml(path)
    yielded = False
    try:
        context = iterparse(
            path,
            events=("start", "end"),
            forbid_dtd=True,
            forbid_entities=True,
            forbid_external=True,
        )
        root: ET.Element | None = None
        for event, element in context:
            if event == "start" and root is None:
                root = element
                continue
            if event != "end":
                continue
            if _local_name(element.tag) not in _RECORD_ELEMENTS:
                continue
            try:
                payload = parse_eidr_element(element)
            except EidrParseError:
                if _local_name(element.tag) != "Record":
                    raise
            else:
                yielded = True
                yield payload
            finally:
                element.clear()
                if root is not None and root is not element:
                    root.clear()
        if not yielded:
            if root is None:
                raise EidrParseError("EIDR XML is empty")
            yield parse_eidr_element(root)
            root.clear()
    except (ET.ParseError, DefusedXmlException) as exc:
        raise EidrParseError(f"{path}: invalid XML: {exc}") from exc


def fetch_exact(
    *,
    eidr_ids: Iterable[str] = (),
    imdb_ids: Iterable[str] = (),
    provider: EidrProvider | None = None,
) -> Iterator[bytes]:
    """Perform opt-in exact lookups only; never searches by title."""

    if provider is None:
        raise EidrProviderNotConfiguredError(
            "EIDR provider is not configured; use a local XML input or "
            "supply an explicit exact-lookup provider"
        )

    def fetches() -> Iterator[bytes]:
        for eidr_id in eidr_ids:
            yield provider.fetch_by_eidr_id(normalize_eidr_id(eidr_id))
        for imdb_id in imdb_ids:
            yield provider.fetch_by_imdb_id(normalize_imdb_id(imdb_id))

    return fetches()
