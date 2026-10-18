#!/usr/bin/env python3
"""Capture the reviewed Music Pilot authority slice as minimal pinned projections.

The script intentionally stores only the source facts consumed by the Music
mapper. Each projection is accompanied by a receipt for the HTTPS response from
which it was derived, so the fixture can be audited without checking a mutable
live API during tests.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import time
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

MUSICBRAINZ_BASE = "https://musicbrainz.org/ws/2"
WIKIDATA_API = "https://www.wikidata.org/w/api.php"
USER_AGENT = (
    "video-media-catalog-music-pilot/1.0 "
    "(https://github.com/pandong912/video-media-catalog)"
)
CC0_URI = "https://creativecommons.org/publicdomain/zero/1.0/"
FIXTURE_SCHEMA = "video.media-catalog.reference-fixture.v1"
CAPTURED_AT_DEFAULT = "2026-10-17T03:00:00Z"
WIKIDATA_MUSIC_SOURCE_PRODUCT_ID = "wikidata-music-reference"
WIKIDATA_MUSIC_CONNECTOR_ID = "wikidata-music-reference-snapshot"

EXACT_RECORDINGS = (
    ("Q66662496", "9aa56ede-83be-44ea-aeb5-d61268972c53"),
    ("Q105942412", "321d235c-a32a-4f6c-b84d-0e151ffb58be"),
    ("Q105978624", "e1ded706-16ec-45c3-87c4-0ae7a26f56d3"),
    ("Q106098098", "b611eecb-2e93-46ad-aaf4-f00f02a913ff"),
    ("Q106182588", "020152a7-172e-45c5-812a-a60d02cb2a27"),
    ("Q106435189", "61b03fdf-24aa-4c82-b5d6-1e25691e8347"),
    ("Q109795255", "c40d7b44-7583-4675-852a-10ddbea8cf46"),
    ("Q113148225", "7fb6969c-bb00-48ec-84df-151fd56875c2"),
)
LIVE_WORK_PARENTS = (
    ("Q2607570", "9dc29ea6-7a6f-3cc5-aa37-560b04afb0d7"),
    ("Q17591232", "4f5a74d0-d183-37f1-b07a-913652e82667"),
    ("Q138836655", "8b3a9214-bbec-311d-a61b-874bf15dfdde"),
    ("Q908534", "a488a020-884d-3349-a7e7-18993ffed13e"),
    ("Q960586", "e9914523-2452-3987-8d4d-08f8e4f3c302"),
    ("Q1140459", "660d0d25-4253-4d30-a818-61fc94e375bf"),
    ("Q12860108", "ba28e7e2-862e-3438-9faa-13ebb6e9030c"),
    ("Q29117946", "8d331505-4d88-39ae-81c7-bec5da77af96"),
)
CONFLICT_RECORDINGS = (
    ("Q113151843", "d9da08d3-3c24-4f4c-b1e7-beeee60bab08"),
    ("Q113153791", "71d7fa30-ae9e-431b-9fce-a6ecb63f3cfa"),
)
VARIANT_RECORDING = (
    "Q108042886",
    "b70b3468-8f1a-4fa3-b853-122f57d98f92",
)
CONFLICT_MEDIA_OBJECT = "12-the-doctor.flac"
VARIANT_MEDIA_OBJECT = "Там, за чертой (DIP Project remix full version).mp3"
VARIANT_RELATION_TARGET_ID = "ae81a568-f6f4-4a69-9da0-b8561247642d"
NO_AUTHORITY_MEDIA_OBJECTS = (
    "Annette Hanshaw Big City Blues.ogg",
    ("1925 (USA) Archives 1925 03 19 Ben Bernie Orchestra - Sweet Georgia Brown.mp3"),
)

_QID = re.compile(r"Q[1-9][0-9]*")
_WIKIDATA_URL = re.compile(r"https?://www\.wikidata\.org/(?:wiki/)?(Q[1-9][0-9]*)")
_MB_ENTITY_TYPES = {
    "MUSIC_ARTIST": "artist",
    "MUSICAL_WORK": "work",
    "MUSIC_RECORDING": "recording",
}
_WIKIDATA_CLAIMS = frozenset(
    {
        "P31",
        "P51",
        "P86",
        "P175",
        "P434",
        "P435",
        "P676",
        "P1243",
        "P1827",
        "P2550",
        "P4404",
    }
)


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _sha256_bytes(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


def _sha256_json(value: Any) -> str:
    return _sha256_bytes(_canonical_json(value).encode())


def _chunked(values: tuple[str, ...], size: int) -> Iterable[tuple[str, ...]]:
    for offset in range(0, len(values), size):
        yield values[offset : offset + size]


class _Fetcher:
    def __init__(self) -> None:
        self._last_musicbrainz_request = 0.0

    def json(self, url: str, *, musicbrainz: bool = False) -> tuple[Any, str]:
        for attempt in range(3):
            if musicbrainz:
                delay = 1.1 - (time.monotonic() - self._last_musicbrainz_request)
                if delay > 0:
                    time.sleep(delay)
            completed = subprocess.run(
                (
                    "curl",
                    "--fail",
                    "--silent",
                    "--show-error",
                    "--retry",
                    "5",
                    "--retry-all-errors",
                    "--retry-delay",
                    "2",
                    "--connect-timeout",
                    "20",
                    "--max-time",
                    "90",
                    "--http1.1",
                    "--proto",
                    "=https",
                    "--tlsv1.2",
                    "--header",
                    "Accept: application/json",
                    "--user-agent",
                    USER_AGENT,
                    url,
                ),
                check=True,
                capture_output=True,
            )
            raw = completed.stdout
            if musicbrainz:
                self._last_musicbrainz_request = time.monotonic()
            try:
                return json.loads(raw), _sha256_bytes(raw)
            except json.JSONDecodeError:
                if attempt == 2:
                    raise
                time.sleep(2)
        raise AssertionError("unreachable capture retry state")


def _aliases(value: object) -> list[dict[str, str]]:
    if not isinstance(value, list):
        return []
    result = {
        (
            str(item.get("name") or "").strip(),
            str(item.get("locale") or "und").strip() or "und",
        )
        for item in value
        if isinstance(item, Mapping) and str(item.get("name") or "").strip()
    }
    return [
        {"name": name, "locale": locale}
        for name, locale in sorted(result, key=lambda item: (item[1], item[0]))
    ]


def _wikidata_qids(relations: object) -> list[str]:
    if not isinstance(relations, list):
        return []
    result: set[str] = set()
    for relation in relations:
        if not isinstance(relation, Mapping):
            continue
        target = relation.get("url")
        resource = target.get("resource") if isinstance(target, Mapping) else None
        match = _WIKIDATA_URL.fullmatch(str(resource or ""))
        if match is not None:
            result.add(match.group(1))
    return sorted(result)


def _artist_payload(artist: Mapping[str, Any]) -> dict[str, Any]:
    artist_type = str(artist.get("type") or "").strip()
    agent_kind = {
        "Person": "PERSON",
        "Group": "ORGANIZATION",
        "Orchestra": "ORGANIZATION",
        "Choir": "ORGANIZATION",
    }.get(artist_type)
    payload = {
        "id": str(artist["id"]).lower(),
        "researchDomain": "MUSIC",
        "entityType": "MUSIC_ARTIST",
        "coreDataset": "MUSICBRAINZ_CORE_CC0",
        "name": str(artist["name"]),
        "aliases": _aliases(artist.get("aliases")),
        "wikidataQids": _wikidata_qids(artist.get("relations")),
        "relations": [],
    }
    if artist_type:
        payload["musicBrainzArtistType"] = artist_type
    if agent_kind is not None:
        payload["agentKind"] = agent_kind
    return payload


def _merge_record(
    records: dict[str, dict[str, Any]],
    incoming: dict[str, Any],
) -> None:
    record_id = str(incoming["id"])
    existing = records.get(record_id)
    if existing is None:
        records[record_id] = incoming
        return
    for field in ("researchDomain", "entityType", "coreDataset"):
        if existing.get(field) != incoming.get(field):
            raise ValueError(f"authority record {record_id} changed {field}")
    for field in (
        "name",
        "durationMs",
        "firstReleaseDate",
        "authorityRevision",
        "musicBrainzArtistType",
        "agentKind",
    ):
        current = existing.get(field)
        candidate = incoming.get(field)
        if current is not None and candidate is not None and current != candidate:
            raise ValueError(f"authority record {record_id} changed {field}")
        if current is None and candidate is not None:
            existing[field] = candidate
    for field in (
        "aliases",
        "isrcs",
        "iswcs",
        "wikidataQids",
        "relations",
        "recordingVariants",
    ):
        combined = {
            _canonical_json(item): item
            for item in (*existing.get(field, []), *incoming.get(field, []))
        }
        if combined:
            existing[field] = [combined[key] for key in sorted(combined)]


def _musicbrainz_url(entity_type: str, entity_id: str) -> str:
    entity = _MB_ENTITY_TYPES[entity_type]
    includes = {
        "MUSIC_RECORDING": (
            "aliases+artist-credits+isrcs+recording-rels+url-rels+work-rels"
        ),
        "MUSICAL_WORK": "aliases+artist-rels+url-rels",
        "MUSIC_ARTIST": "aliases+url-rels",
    }[entity_type]
    query = urlencode({"inc": includes, "fmt": "json"})
    return f"{MUSICBRAINZ_BASE}/{entity}/{entity_id}?{query}"


def _musicbrainz_payload(
    entity_type: str,
    response: Mapping[str, Any],
    records: dict[str, dict[str, Any]],
    *,
    authority_revision: str,
) -> tuple[dict[str, Any], set[str]]:
    entity_id = str(response["id"]).lower()
    payload: dict[str, Any] = {
        "id": entity_id,
        "researchDomain": "MUSIC",
        "entityType": entity_type,
        "coreDataset": "MUSICBRAINZ_CORE_CC0",
        "authorityRevision": authority_revision,
        "name": str(response.get("title") or response.get("name") or entity_id),
        "aliases": _aliases(response.get("aliases")),
        "wikidataQids": _wikidata_qids(response.get("relations")),
        "relations": [],
    }
    projected_ids = {entity_id}
    if entity_type == "MUSIC_RECORDING":
        if isinstance(response.get("length"), int):
            payload["durationMs"] = response["length"]
        if response.get("first-release-date"):
            payload["firstReleaseDate"] = str(response["first-release-date"])
        payload["isrcs"] = sorted(
            {
                str(value).strip().upper()
                for value in response.get("isrcs") or []
                if str(value).strip()
            }
        )
        for credit in response.get("artist-credit") or []:
            if not isinstance(credit, Mapping) or not isinstance(
                credit.get("artist"), Mapping
            ):
                continue
            artist = dict(credit["artist"])
            artist_payload = _artist_payload(artist)
            _merge_record(records, artist_payload)
            artist_id = str(artist["id"]).lower()
            projected_ids.add(artist_id)
            qualifiers = {
                "role": "PERFORMER",
                "creditedAs": str(credit.get("name") or artist["name"]),
            }
            if artist_payload.get("agentKind"):
                qualifiers["targetAgentKind"] = artist_payload["agentKind"]
            payload["relations"].append(
                {
                    "predicate": "credit",
                    "targetId": artist_id,
                    "targetType": "MUSIC_ARTIST",
                    "qualifiers": qualifiers,
                }
            )
        variants = []
        for relation in response.get("relations") or []:
            if not isinstance(relation, Mapping):
                continue
            target_type = relation.get("target-type")
            if target_type == "work" and isinstance(relation.get("work"), Mapping):
                work = relation["work"]
                work_id = str(work["id"]).lower()
                projected_ids.add(work_id)
                _merge_record(
                    records,
                    {
                        "id": work_id,
                        "researchDomain": "MUSIC",
                        "entityType": "MUSICAL_WORK",
                        "coreDataset": "MUSICBRAINZ_CORE_CC0",
                        "name": str(work["title"]),
                        "iswcs": sorted(work.get("iswcs") or []),
                        "aliases": [],
                        "wikidataQids": [],
                        "relations": [],
                    },
                )
                payload["relations"].append(
                    {
                        "predicate": "recording_of",
                        "targetId": work_id,
                        "targetType": "MUSICAL_WORK",
                        "qualifiers": {
                            "musicBrainzRelationType": str(
                                relation.get("type") or "performance"
                            ),
                            "direction": str(relation.get("direction") or ""),
                        },
                    }
                )
            elif target_type == "recording" and isinstance(
                relation.get("recording"), Mapping
            ):
                target = relation["recording"]
                variants.append(
                    {
                        "targetId": str(target["id"]).lower(),
                        "title": str(target["title"]),
                        "relationType": str(relation.get("type") or ""),
                        "direction": str(relation.get("direction") or ""),
                    }
                )
        if variants:
            payload["recordingVariants"] = variants
    elif entity_type == "MUSICAL_WORK":
        payload["iswcs"] = sorted(
            {
                str(value).strip().upper()
                for value in response.get("iswcs") or []
                if str(value).strip()
            }
        )
        for relation in response.get("relations") or []:
            if not isinstance(relation, Mapping) or not isinstance(
                relation.get("artist"), Mapping
            ):
                continue
            artist = dict(relation["artist"])
            artist_payload = _artist_payload(artist)
            _merge_record(records, artist_payload)
            artist_id = str(artist["id"]).lower()
            projected_ids.add(artist_id)
            role = str(relation.get("type") or "CREDIT").upper().replace(" ", "_")
            qualifiers = {
                "role": role,
                "creditedAs": str(artist["name"]),
            }
            if artist_payload.get("agentKind"):
                qualifiers["targetAgentKind"] = artist_payload["agentKind"]
            payload["relations"].append(
                {
                    "predicate": "credit",
                    "targetId": artist_id,
                    "targetType": "MUSIC_ARTIST",
                    "qualifiers": qualifiers,
                }
            )
    payload["relations"] = [
        item
        for _, item in sorted(
            {_canonical_json(item): item for item in payload["relations"]}.items()
        )
    ]
    return payload, projected_ids


def _capture_musicbrainz(
    fetcher: _Fetcher,
    *,
    captured_at: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    primary = (
        *(("MUSIC_RECORDING", mbid) for _, mbid in EXACT_RECORDINGS),
        *(("MUSICAL_WORK", mbid) for _, mbid in LIVE_WORK_PARENTS),
        *(("MUSIC_RECORDING", mbid) for _, mbid in CONFLICT_RECORDINGS),
        ("MUSIC_RECORDING", VARIANT_RECORDING[1]),
    )
    records: dict[str, dict[str, Any]] = {}
    receipts: list[dict[str, Any]] = []
    fetched: set[tuple[str, str]] = set()

    def fetch(entity_type: str, entity_id: str) -> None:
        coordinate = (entity_type, entity_id)
        if coordinate in fetched:
            return
        fetched.add(coordinate)
        url = _musicbrainz_url(entity_type, entity_id)
        response, response_hash = fetcher.json(url, musicbrainz=True)
        if not isinstance(response, Mapping) or response.get("id") != entity_id:
            raise ValueError(f"MusicBrainz response does not bind {entity_id}")
        payload, projected_ids = _musicbrainz_payload(
            entity_type,
            response,
            records,
            authority_revision=response_hash,
        )
        _merge_record(records, payload)
        receipts.append(
            {
                "requestUrl": url,
                "retrievedAt": captured_at,
                "responseHash": response_hash,
                "authorityRevision": response_hash,
                "sourceRecordIds": [entity_id],
                "projectedRecordIds": sorted(projected_ids),
                "projectionPaths": [
                    "/aliases",
                    "/artist-credit",
                    "/first-release-date",
                    "/id",
                    "/isrcs",
                    "/iswcs",
                    "/length",
                    "/name",
                    "/relations",
                    "/title",
                    "/type",
                ],
            }
        )

    for entity_type, entity_id in primary:
        fetch(entity_type, entity_id)
    related_work_ids = sorted(
        {
            str(relation["targetId"])
            for record in records.values()
            for relation in record.get("relations", [])
            if relation.get("targetType") == "MUSICAL_WORK"
        }
    )
    for work_id in related_work_ids:
        fetch("MUSICAL_WORK", work_id)
    return (
        sorted(
            records.values(),
            key=lambda item: (str(item["entityType"]), str(item["id"])),
        ),
        sorted(receipts, key=lambda item: item["requestUrl"]),
    )


def _statement_value(statement: object) -> Any:
    if not isinstance(statement, Mapping):
        return None
    mainsnak = statement.get("mainsnak")
    if not isinstance(mainsnak, Mapping) or mainsnak.get("snaktype") != "value":
        return None
    datavalue = mainsnak.get("datavalue")
    return datavalue.get("value") if isinstance(datavalue, Mapping) else None


def _claim_values(entity: Mapping[str, Any], property_id: str) -> list[Any]:
    claims = entity.get("claims")
    if not isinstance(claims, Mapping):
        return []
    return [
        value
        for statement in claims.get(property_id) or []
        if isinstance(statement, Mapping)
        and statement.get("rank") != "deprecated"
        and (value := _statement_value(statement)) is not None
    ]


def _normalize_statement(statement: Mapping[str, Any]) -> dict[str, Any] | None:
    value = _statement_value(statement)
    if value is None:
        return None
    mainsnak = statement["mainsnak"]
    assert isinstance(mainsnak, Mapping)
    datavalue = mainsnak["datavalue"]
    assert isinstance(datavalue, Mapping)
    return {
        "id": statement.get("id"),
        "rank": statement.get("rank", "normal"),
        "mainsnak": {
            "snaktype": "value",
            "property": mainsnak.get("property"),
            "datatype": mainsnak.get("datatype"),
            "datavalue": {
                "value": value,
                "type": datavalue.get("type"),
            },
        },
    }


def _wikidata_labels(value: object) -> dict[str, dict[str, str]]:
    if not isinstance(value, Mapping):
        return {}
    return {
        str(language): {
            "language": str(label.get("language") or language),
            "value": str(label["value"]),
        }
        for language, label in sorted(value.items())
        if isinstance(label, Mapping) and str(label.get("value") or "").strip()
    }


def _wikidata_aliases(value: object) -> dict[str, list[dict[str, str]]]:
    if not isinstance(value, Mapping):
        return {}
    result: dict[str, list[dict[str, str]]] = {}
    for language, aliases in sorted(value.items()):
        normalized = [
            {
                "language": str(alias.get("language") or language),
                "value": str(alias["value"]),
            }
            for alias in aliases
            if isinstance(alias, Mapping) and str(alias.get("value") or "").strip()
        ]
        if normalized:
            result[str(language)] = normalized
    return result


def _qid(value: object) -> str | None:
    if isinstance(value, Mapping) and _QID.fullmatch(str(value.get("id") or "")):
        return str(value["id"])
    return None


def _wikidata_agent_kind(entity: Mapping[str, Any]) -> str | None:
    instance_of = {
        item
        for value in _claim_values(entity, "P31")
        if (item := _qid(value)) is not None
    }
    kinds = {
        kind
        for qid, kind in {
            "Q5": "PERSON",
            "Q215380": "ORGANIZATION",
            "Q2088357": "ORGANIZATION",
            "Q43229": "ORGANIZATION",
            "Q4830453": "ORGANIZATION",
        }.items()
        if qid in instance_of
    }
    if len(kinds) > 1:
        raise ValueError(f"Wikidata agent has conflicting concrete kinds: {kinds}")
    return next(iter(kinds), None)


def _wikidata_payload(
    entity: Mapping[str, Any],
    *,
    entity_type: str,
) -> tuple[dict[str, Any], dict[str, str]]:
    qid = str(entity["id"])
    claims: dict[str, list[dict[str, Any]]] = {}
    for property_id in sorted(_WIKIDATA_CLAIMS):
        source_claims = entity.get("claims")
        statements = (
            source_claims.get(property_id) or []
            if isinstance(source_claims, Mapping)
            else []
        )
        normalized = [
            item
            for statement in statements
            if isinstance(statement, Mapping)
            and (item := _normalize_statement(statement)) is not None
        ]
        if normalized:
            claims[property_id] = normalized
    relations: list[dict[str, Any]] = []
    related_types: dict[str, str] = {}
    for property_id, role in (
        ("P175", "PERFORMER"),
        ("P86", "COMPOSER"),
        ("P676", "LYRICIST"),
    ):
        for value in _claim_values(entity, property_id):
            target = _qid(value)
            if target is None:
                continue
            related_types[target] = "MUSIC_ARTIST"
            relations.append(
                {
                    "predicate": "credit",
                    "targetId": target,
                    "targetType": "MUSIC_ARTIST",
                    "qualifiers": {
                        "role": role,
                        "wikidataProperty": property_id,
                    },
                }
            )
    for value in _claim_values(entity, "P2550"):
        target = _qid(value)
        if target is None:
            continue
        related_types[target] = "MUSICAL_WORK"
        relations.append(
            {
                "predicate": "recording_of",
                "targetId": target,
                "targetType": "MUSICAL_WORK",
                "qualifiers": {"wikidataProperty": "P2550"},
            }
        )
    payload: dict[str, Any] = {
        "id": qid,
        "researchDomain": "MUSIC",
        "entityTypeHint": entity_type,
        "authorityRevision": str(entity["lastrevid"]),
        "authorityModifiedAt": str(entity["modified"]),
        "labels": _wikidata_labels(entity.get("labels")),
        "aliases": _wikidata_aliases(entity.get("aliases")),
        "claims": claims,
        "relations": [
            item
            for _, item in sorted(
                {_canonical_json(item): item for item in relations}.items()
            )
        ],
    }
    if entity_type == "MUSIC_ARTIST":
        agent_kind = _wikidata_agent_kind(entity)
        if agent_kind is not None:
            payload["agentKind"] = agent_kind
    media_objects = sorted(
        {
            str(value).strip()
            for value in _claim_values(entity, "P51")
            if isinstance(value, str) and value.strip()
        }
    )
    if media_objects:
        payload["mediaObjects"] = media_objects
    return payload, related_types


def _wikidata_url(qids: tuple[str, ...]) -> str:
    return f"{WIKIDATA_API}?{
        urlencode(
            {
                'action': 'wbgetentities',
                'ids': '|'.join(qids),
                'props': 'info|labels|aliases|claims',
                'languages': 'en|ru',
                'format': 'json',
                'formatversion': '2',
            }
        )
    }"


def _fetch_wikidata_entities(
    fetcher: _Fetcher,
    qids: tuple[str, ...],
    *,
    captured_at: str,
) -> tuple[dict[str, Mapping[str, Any]], list[dict[str, Any]]]:
    entities: dict[str, Mapping[str, Any]] = {}
    receipts = []
    for chunk in _chunked(tuple(sorted(set(qids))), 25):
        url = _wikidata_url(chunk)
        response, response_hash = fetcher.json(url)
        response_entities = (
            response.get("entities") if isinstance(response, Mapping) else None
        )
        if not isinstance(response_entities, Mapping):
            raise ValueError("Wikidata response has no entity map")
        revisions = []
        for qid in chunk:
            entity = response_entities.get(qid)
            if not isinstance(entity, Mapping) or entity.get("missing") is not None:
                raise ValueError(f"Wikidata response does not bind {qid}")
            if not isinstance(entity.get("lastrevid"), int) or not entity.get(
                "modified"
            ):
                raise ValueError(f"Wikidata response has no revision for {qid}")
            entities[qid] = entity
            revisions.append(f"{qid}@{entity['lastrevid']}")
        receipts.append(
            {
                "requestUrl": url,
                "retrievedAt": captured_at,
                "responseHash": response_hash,
                "authorityRevision": ",".join(revisions),
                "sourceRecordIds": list(chunk),
                "projectedRecordIds": list(chunk),
                "projectionPaths": [
                    "/aliases",
                    "/claims/P31",
                    "/claims/P51",
                    "/claims/P86",
                    "/claims/P175",
                    "/claims/P434",
                    "/claims/P435",
                    "/claims/P676",
                    "/claims/P1243",
                    "/claims/P1827",
                    "/claims/P2550",
                    "/claims/P4404",
                    "/id",
                    "/labels",
                    "/lastrevid",
                    "/modified",
                ],
            }
        )
    return entities, receipts


def _capture_wikidata(
    fetcher: _Fetcher,
    *,
    captured_at: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    type_by_qid = {
        **{qid: "MUSIC_RECORDING" for qid, _ in EXACT_RECORDINGS},
        **{qid: "MUSICAL_WORK" for qid, _ in LIVE_WORK_PARENTS},
        **{qid: "MUSIC_RECORDING" for qid, _ in CONFLICT_RECORDINGS},
        VARIANT_RECORDING[0]: "MUSIC_RECORDING",
    }
    primary_qids = tuple(sorted(type_by_qid))
    entities, receipts = _fetch_wikidata_entities(
        fetcher,
        primary_qids,
        captured_at=captured_at,
    )
    records: dict[str, dict[str, Any]] = {}
    related_types: dict[str, str] = {}
    for qid in primary_qids:
        payload, related = _wikidata_payload(
            entities[qid],
            entity_type=type_by_qid[qid],
        )
        records[qid] = payload
        for target, target_type in related.items():
            existing = related_types.get(target)
            if existing is not None and existing != target_type:
                raise ValueError(
                    f"Wikidata target {target} has conflicting Music types"
                )
            related_types[target] = target_type

    secondary_qids = tuple(sorted(set(related_types) - set(type_by_qid)))
    if secondary_qids:
        related_entities, related_receipts = _fetch_wikidata_entities(
            fetcher,
            secondary_qids,
            captured_at=captured_at,
        )
        entities.update(related_entities)
        receipts.extend(related_receipts)
        for qid in secondary_qids:
            payload, _ = _wikidata_payload(
                entities[qid],
                entity_type=related_types[qid],
            )
            records[qid] = payload
    for record in records.values():
        relations = record.get("relations") or []
        for relation in relations:
            if relation.get("targetType") != "MUSIC_ARTIST":
                continue
            target = str(relation["targetId"])
            target_entity = entities.get(target)
            if target_entity is None:
                raise ValueError(f"Wikidata relation target {target} was not captured")
            agent_kind = _wikidata_agent_kind(target_entity)
            if agent_kind is not None:
                relation["qualifiers"]["targetAgentKind"] = agent_kind
        record["relations"] = sorted(relations, key=_canonical_json)
    return (
        sorted(
            records.values(),
            key=lambda item: (str(item["entityTypeHint"]), str(item["id"])),
        ),
        sorted(receipts, key=lambda item: item["requestUrl"]),
    )


def _write_reference_fixture(
    *,
    root: Path,
    stem: str,
    fixture_id: str,
    source_system_id: str,
    source_product_id: str,
    connector_id: str,
    schema_id: str,
    records: list[dict[str, Any]],
    receipts: list[dict[str, Any]],
    captured_at: str,
    image_digest: str,
    config_digest: str,
) -> dict[str, Any]:
    sidecar_name = f"{stem}.jsonl"
    sidecar_bytes = b"".join(
        (_canonical_json(record) + "\n").encode() for record in records
    )
    sidecar_digest = _sha256_bytes(sidecar_bytes)
    (root / sidecar_name).write_bytes(sidecar_bytes)
    receipt_digest = _sha256_json(receipts)
    source_name = "MusicBrainz" if source_system_id == "musicbrainz" else "Wikidata"
    attribution = (
        "MusicBrainz core database contributors"
        if source_system_id == "musicbrainz"
        else "Wikidata contributors"
    )
    entity_segment = {
        "MUSIC_ARTIST": "artist",
        "MUSICAL_WORK": "work",
        "MUSIC_RECORDING": "recording",
    }
    citations = []
    fixture_records = []
    for record in records:
        record_id = str(record["id"])
        entity_type = str(record.get("entityType") or record.get("entityTypeHint"))
        source_url = (
            f"https://musicbrainz.org/{entity_segment[entity_type]}/{record_id}"
            if source_system_id == "musicbrainz"
            else f"https://www.wikidata.org/wiki/{record_id}"
        )
        labels = record.get("labels")
        title = record.get("name")
        if title is None and isinstance(labels, Mapping):
            selected = labels.get("en") or labels.get("ru")
            if isinstance(selected, Mapping):
                title = selected.get("value")
        citation_id = f"{source_system_id}-{record_id.lower()}"
        citations.append(
            {
                "citationId": citation_id,
                "sourceRecordId": record_id,
                "sourceUrl": source_url,
                "title": title,
                "publisher": source_name,
                "retrievedAt": captured_at,
                "licenseUri": CC0_URI,
                "attributionText": attribution,
                "rawObjectHash": sidecar_digest,
            }
        )
        fixture_record: dict[str, Any] = {
            "payload": record,
            "citationIds": [citation_id],
        }
        if record.get("authorityModifiedAt"):
            fixture_record["sourceModifiedAt"] = record["authorityModifiedAt"]
        fixture_records.append(fixture_record)
    fixture = {
        "schemaVersion": FIXTURE_SCHEMA,
        "fixtureId": fixture_id,
        "domain": "MUSIC",
        "sourceSystemId": source_system_id,
        "sourceProductId": source_product_id,
        "connectorId": connector_id,
        "revisionKind": "DUMP_OBJECT",
        "schemaId": schema_id,
        "sourceRevision": receipt_digest,
        "capturedAt": captured_at,
        "rawObject": {
            "uri": f"file:///tests/fixtures/research/{sidecar_name}",
            "format": "OBJECT_FORMAT_JSON",
            "mediaType": "application/x-ndjson",
            "checksum": {
                "algorithm": "CHECKSUM_ALGORITHM_SHA256",
                "encoding": "CHECKSUM_ENCODING_HEX",
                "value": sidecar_digest.removeprefix("sha256:"),
            },
            "sizeBytes": len(sidecar_bytes),
        },
        "imageDigest": image_digest,
        "configDigest": config_digest,
        "captureReceipts": receipts,
        "citations": citations,
        "records": fixture_records,
    }
    (root / f"{stem}.json").write_text(
        json.dumps(fixture, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return fixture


def _direct_claim_evidence(
    record: Mapping[str, Any],
    *,
    property_id: str,
    expected_value: str,
) -> dict[str, Any] | None:
    claims = record.get("claims")
    statements = claims.get(property_id) if isinstance(claims, Mapping) else None
    for statement in statements or []:
        if _statement_value(statement) == expected_value:
            return {
                "kind": "WIKIDATA_DIRECT_CROSS_ID",
                "property": property_id,
                "statementId": statement.get("id"),
                "authorityRevision": record["authorityRevision"],
            }
    return None


def _record_title(record: Mapping[str, Any]) -> str:
    if record.get("name"):
        return str(record["name"])
    labels = record.get("labels")
    if isinstance(labels, Mapping):
        for language in ("en", "ru"):
            label = labels.get(language)
            if isinstance(label, Mapping) and label.get("value"):
                return str(label["value"])
    return str(record["id"])


def _musicbrainz_artists(record: Mapping[str, Any]) -> list[str]:
    return sorted(
        {
            str(
                relation.get("qualifiers", {}).get("creditedAs") or relation["targetId"]
            )
            for relation in record.get("relations") or []
            if relation.get("predicate") == "credit"
        }
    )


def _coordinate(
    *,
    qid: str,
    mbid: str,
    entity_type: str,
    wikidata: Mapping[str, Any],
    musicbrainz: Mapping[str, Any],
    captured_at: str,
) -> dict[str, Any]:
    property_id = "P4404" if entity_type == "MUSIC_RECORDING" else "P435"
    direct = _direct_claim_evidence(
        wikidata,
        property_id=property_id,
        expected_value=mbid,
    )
    if direct is not None:
        review = {
            "mappingKind": "AUTHORITY_DECLARED",
            "reviewStatus": "NOT_REQUIRED",
            "evidence": [direct],
        }
    else:
        review = {
            "mappingKind": (
                "RESEARCH_CANDIDATE_SAME_WORK_PARENT"
                if entity_type == "MUSICAL_WORK"
                else "RESEARCH_CANDIDATE_SAME_RECORDING"
            ),
            "reviewStatus": "PENDING",
            "evidence": [
                {
                    "kind": "WIKIDATA_REVISION_AND_LABEL",
                    "authorityRevision": wikidata["authorityRevision"],
                    "title": _record_title(wikidata),
                },
                {
                    "kind": "MUSICBRAINZ_CAPTURE_AND_TITLE",
                    "authorityRevision": musicbrainz["authorityRevision"],
                    "title": _record_title(musicbrainz),
                },
            ],
        }
    return {
        "wikidataQid": qid,
        "musicbrainzMbid": mbid,
        "entityType": entity_type,
        "coordinateReview": review,
    }


def _write_cohort(
    *,
    root: Path,
    musicbrainz_records: list[dict[str, Any]],
    wikidata_records: list[dict[str, Any]],
    musicbrainz_fixture: Mapping[str, Any],
    wikidata_fixture: Mapping[str, Any],
    captured_at: str,
    config_digest: str,
) -> dict[str, Any]:
    mb = {str(item["id"]): item for item in musicbrainz_records}
    wd = {str(item["id"]): item for item in wikidata_records}
    assets: list[dict[str, Any]] = []

    for index, (qid, mbid) in enumerate(EXACT_RECORDINGS, start=1):
        coordinate = _coordinate(
            qid=qid,
            mbid=mbid,
            entity_type="MUSIC_RECORDING",
            wikidata=wd[qid],
            musicbrainz=mb[mbid],
            captured_at=captured_at,
        )
        assets.append(
            {
                "assetId": f"music-pilot-exact-{index:02d}",
                "case": "EXACT_RECORDING",
                "features": {
                    "title": _record_title(mb[mbid]),
                    "artists": _musicbrainz_artists(mb[mbid]),
                    "durationMs": mb[mbid].get("durationMs"),
                    "performanceType": "RECORDED",
                    "authorityCoordinates": [coordinate],
                },
                "expected": {
                    "decision": "EXACT",
                    "candidateEntityType": "MUSIC_RECORDING",
                    "candidateNamespace": "musicbrainz-recording",
                    "candidateId": mbid,
                    "reason": "UNIQUE_REVIEWED_EXACT_RECORDING_COORDINATE",
                },
            }
        )

    for index, (qid, mbid) in enumerate(LIVE_WORK_PARENTS, start=1):
        coordinate = _coordinate(
            qid=qid,
            mbid=mbid,
            entity_type="MUSICAL_WORK",
            wikidata=wd[qid],
            musicbrainz=mb[mbid],
            captured_at=captured_at,
        )
        assets.append(
            {
                "assetId": f"music-pilot-live-{index:02d}",
                "case": "LIVE_WORK_PARENT",
                "features": {
                    "title": _record_title(mb[mbid]),
                    "artists": _musicbrainz_artists(mb[mbid]),
                    "performanceType": "LIVE",
                    "authorityCoordinates": [coordinate],
                },
                "expected": {
                    "decision": "REVIEW_REQUIRED",
                    "candidateEntityType": "MUSICAL_WORK",
                    "candidateNamespace": "musicbrainz-work",
                    "candidateId": mbid,
                    "reason": "PARENT_CONSTRAINED_WORK_ONLY",
                    "forbidEntityType": "MUSIC_RECORDING",
                },
            }
        )

    conflict_coordinates = [
        _coordinate(
            qid=qid,
            mbid=mbid,
            entity_type="MUSIC_RECORDING",
            wikidata=wd[qid],
            musicbrainz=mb[mbid],
            captured_at=captured_at,
        )
        for qid, mbid in CONFLICT_RECORDINGS
    ]
    if any(
        CONFLICT_MEDIA_OBJECT not in wd[qid].get("mediaObjects", [])
        for qid, _ in CONFLICT_RECORDINGS
    ):
        raise ValueError("conflict coordinates do not share the pinned media object")
    assets.append(
        {
            "assetId": "music-pilot-conflict-01",
            "case": "CONFLICTING_RECORDINGS",
            "features": {
                "title": CONFLICT_MEDIA_OBJECT,
                "artists": sorted(
                    {
                        artist
                        for _, mbid in CONFLICT_RECORDINGS
                        for artist in _musicbrainz_artists(mb[mbid])
                    }
                ),
                "performanceType": "RECORDED",
                "authorityCoordinates": conflict_coordinates,
                "conflictEvidence": {
                    "wikidataSharedMediaObject": CONFLICT_MEDIA_OBJECT,
                },
            },
            "expected": {
                "decision": "ABSTAIN",
                "reason": "MULTIPLE_EXACT_IDENTIFIER_CANDIDATES",
            },
        }
    )

    variant_qid, variant_mbid = VARIANT_RECORDING
    variant_coordinate = _coordinate(
        qid=variant_qid,
        mbid=variant_mbid,
        entity_type="MUSIC_RECORDING",
        wikidata=wd[variant_qid],
        musicbrainz=mb[variant_mbid],
        captured_at=captured_at,
    )
    variant_relations = [
        relation
        for relation in mb[variant_mbid].get("recordingVariants", [])
        if relation.get("targetId") == VARIANT_RELATION_TARGET_ID
        and relation.get("relationType") == "remix"
    ]
    if (
        VARIANT_MEDIA_OBJECT not in wd[variant_qid].get("mediaObjects", [])
        or len(variant_relations) != 1
    ):
        raise ValueError("variant evidence is not uniquely source-bound")
    variant_relation = variant_relations[0]
    assets.append(
        {
            "assetId": "music-pilot-variant-01",
            "case": "REMIX_OR_MEDIA_VARIANT",
            "features": {
                "title": VARIANT_MEDIA_OBJECT,
                "artists": [],
                "performanceType": "VARIANT",
                "authorityCoordinates": [variant_coordinate],
                "variantEvidence": {
                    "wikidataMediaObject": VARIANT_MEDIA_OBJECT,
                    "musicbrainzRecordingRelation": variant_relation,
                },
            },
            "expected": {
                "decision": "ABSTAIN",
                "reason": "REMIX_OR_VARIANT_REQUIRES_DISTINCT_RECORDING_AUTHORITY",
                "forbidDecision": "EXACT",
            },
        }
    )
    for index, media_object in enumerate(NO_AUTHORITY_MEDIA_OBJECTS, start=1):
        assets.append(
            {
                "assetId": f"music-pilot-no-coordinate-{index:02d}",
                "case": "NO_AUTHORITY_COORDINATE",
                "featureProvenance": "NEGATIVE_CONTROL",
                "features": {
                    "title": media_object,
                    "artists": [],
                    "performanceType": "UNKNOWN",
                    "authorityCoordinates": [],
                },
                "expected": {
                    "decision": "ABSTAIN",
                    "reason": "NO_AUTHORITY_COORDINATE",
                },
            }
        )
    if len(assets) != 20:
        raise RuntimeError("Music Pilot cohort must contain exactly 20 assets")
    payload = {
        "schemaVersion": "video.media-catalog.music-pilot.v1",
        "cohortId": "music-reference-gold-match-pilot-2026-10-17",
        "capturedAt": captured_at,
        "authorityScope": "GLOBAL_REFERENCE_ONLY",
        "tenantWorldIdentityAuthority": False,
        "sourcePolicyVersion": "1.4.0",
        "sourceFixtures": {
            "musicbrainz": {
                "path": "musicbrainz-music-pilot.v1.json",
                "sourceRevision": musicbrainz_fixture["sourceRevision"],
                "rawProjectionDigest": (
                    "sha256:" + musicbrainz_fixture["rawObject"]["checksum"]["value"]
                ),
            },
            "wikidata": {
                "path": "wikidata-music-pilot.v1.json",
                "sourceRevision": wikidata_fixture["sourceRevision"],
                "rawProjectionDigest": (
                    "sha256:" + wikidata_fixture["rawObject"]["checksum"]["value"]
                ),
            },
        },
        "configDigest": config_digest,
        "assets": assets,
        "cohortDigest": _sha256_json(assets),
    }
    (root / "music-pilot-cohort.v1.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("tests/fixtures/research"),
    )
    parser.add_argument("--captured-at", default=CAPTURED_AT_DEFAULT)
    args = parser.parse_args()
    captured_at = (
        datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        if args.captured_at == "now"
        else args.captured_at
    )
    output_root = args.output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    script_digest = _sha256_bytes(Path(__file__).read_bytes())
    config = {
        "exactRecordings": EXACT_RECORDINGS,
        "liveWorkParents": LIVE_WORK_PARENTS,
        "conflictRecordings": CONFLICT_RECORDINGS,
        "conflictMediaObject": CONFLICT_MEDIA_OBJECT,
        "variantRecording": VARIANT_RECORDING,
        "variantMediaObject": VARIANT_MEDIA_OBJECT,
        "variantRelationTargetId": VARIANT_RELATION_TARGET_ID,
        "noAuthorityMediaObjects": NO_AUTHORITY_MEDIA_OBJECTS,
        "projectionVersion": "1.1.0",
    }
    config_digest = _sha256_json(config)
    fetcher = _Fetcher()
    musicbrainz_records, musicbrainz_receipts = _capture_musicbrainz(
        fetcher,
        captured_at=captured_at,
    )
    wikidata_records, wikidata_receipts = _capture_wikidata(
        fetcher,
        captured_at=captured_at,
    )
    musicbrainz_fixture = _write_reference_fixture(
        root=output_root,
        stem="musicbrainz-music-pilot.v1",
        fixture_id="musicbrainz-music-pilot-v1",
        source_system_id="musicbrainz",
        source_product_id="musicbrainz-core-dump",
        connector_id="musicbrainz-core-dump-snapshot",
        schema_id="musicbrainz-core-entity",
        records=musicbrainz_records,
        receipts=musicbrainz_receipts,
        captured_at=captured_at,
        image_digest=script_digest,
        config_digest=config_digest,
    )
    wikidata_fixture = _write_reference_fixture(
        root=output_root,
        stem="wikidata-music-pilot.v1",
        fixture_id="wikidata-music-pilot-v1",
        source_system_id="wikidata",
        source_product_id=WIKIDATA_MUSIC_SOURCE_PRODUCT_ID,
        connector_id=WIKIDATA_MUSIC_CONNECTOR_ID,
        schema_id="wikidata-global-reference-entity",
        records=wikidata_records,
        receipts=wikidata_receipts,
        captured_at=captured_at,
        image_digest=script_digest,
        config_digest=config_digest,
    )
    cohort = _write_cohort(
        root=output_root,
        musicbrainz_records=musicbrainz_records,
        wikidata_records=wikidata_records,
        musicbrainz_fixture=musicbrainz_fixture,
        wikidata_fixture=wikidata_fixture,
        captured_at=captured_at,
        config_digest=config_digest,
    )
    print(
        _canonical_json(
            {
                "assets": len(cohort["assets"]),
                "cohortDigest": cohort["cohortDigest"],
                "musicbrainzRecords": len(musicbrainz_records),
                "musicbrainzRevision": musicbrainz_fixture["sourceRevision"],
                "wikidataRecords": len(wikidata_records),
                "wikidataRevision": wikidata_fixture["sourceRevision"],
            }
        )
    )


if __name__ == "__main__":
    main()
