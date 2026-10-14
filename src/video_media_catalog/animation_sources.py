"""Silver mappers and bounded public client for animation research sources."""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import urljoin, urlsplit

from video_media_catalog.assertions import SourceNodeRef, ValueType
from video_media_catalog.connector import ConnectorRecordEnvelope, RecordOperation
from video_media_catalog.eidr import normalize_eidr_id, normalize_imdb_id
from video_media_catalog.source_mapper import AssertionBuilder, MappedAssertions

WIKIDATA_ANIMATION_MAPPER_ID = "wikidata-animation-source-mapper"
MADB_MAPPER_ID = "madb-animation-source-mapper"

_ANIMATION_ENTITY_TYPES = {
    "WORK": ("EDITORIAL_WORK", "WORK"),
    "EDITORIAL_WORK": ("EDITORIAL_WORK", "WORK"),
    "MOVIE": ("EDITORIAL_WORK", "MOVIE"),
    "SERIES": ("SERIES", "TV_SERIES"),
    "TV_SERIES": ("SERIES", "TV_SERIES"),
    "SEASON": ("SEASON", "TV_SEASON"),
    "TV_SEASON": ("SEASON", "TV_SEASON"),
    "EPISODE": ("EPISODE", "TV_EPISODE"),
    "TV_EPISODE": ("EPISODE", "TV_EPISODE"),
    "CHARACTER": ("CHARACTER", "CHARACTER"),
    "PERSON": ("AGENT", "PERSON"),
    "AGENT": ("AGENT", "AGENT"),
    "ORGANIZATION": ("ORGANIZATION", "ORGANIZATION"),
}
_TRADITIONS = frozenset({"ANIME", "DONGHUA", "WESTERN", "UNKNOWN"})


def _active_payload(
    envelope: ConnectorRecordEnvelope,
    *,
    source_system_id: str,
    source_product_id: str,
) -> dict[str, Any]:
    if (
        envelope.source_system_id != source_system_id
        or envelope.source_product_id != source_product_id
        or envelope.operation != RecordOperation.UPSERT
        or envelope.payload_json is None
    ):
        raise ValueError("record is not an active supported animation envelope")
    payload = json.loads(envelope.payload_json)
    if not isinstance(payload, dict):
        raise ValueError("animation source payload must be an object")
    return payload


def _entity_projection(value: object) -> tuple[str, str]:
    normalized = str(value or "WORK").strip().upper()
    try:
        return _ANIMATION_ENTITY_TYPES[normalized]
    except KeyError as exc:
        raise ValueError(f"unsupported animation entity type: {normalized}") from exc


def _add_titles(
    builder: AssertionBuilder,
    titles: object,
    source_path: str,
) -> None:
    if isinstance(titles, str):
        builder.add_field(
            "title",
            ValueType.STRING,
            titles,
            source_path,
            {"language": "und", "titleRole": "PRIMARY"},
        )
    elif isinstance(titles, dict):
        for language, title in sorted(titles.items()):
            value = title.get("value") if isinstance(title, dict) else title
            if isinstance(value, str) and value.strip():
                builder.add_field(
                    "title",
                    ValueType.STRING,
                    value.strip(),
                    f"{source_path}/{language}",
                    {"language": str(language), "titleRole": "PRIMARY"},
                )


def _verified_tradition(payload: dict[str, Any]) -> str:
    tradition = payload.get("tradition")
    if isinstance(tradition, dict):
        value = str(tradition.get("value") or "UNKNOWN").strip().upper()
        evidence = tradition.get("evidence")
        if (
            value in _TRADITIONS - {"UNKNOWN"}
            and isinstance(evidence, str)
            and evidence.strip()
        ):
            return value
    return "UNKNOWN"


def _add_profile(builder: AssertionBuilder, payload: dict[str, Any]) -> None:
    builder.add_field(
        "release_form",
        ValueType.STRING,
        payload.get("releaseForm"),
        "/releaseForm",
    )
    techniques = payload.get("techniques", payload.get("animationTechniques"))
    if isinstance(techniques, list):
        for index, technique in enumerate(techniques):
            builder.add_field(
                "animation_technique",
                ValueType.STRING,
                technique,
                f"/techniques/{index}",
            )
    builder.add_field(
        "animation_tradition",
        ValueType.STRING,
        _verified_tradition(payload),
        "/tradition",
    )


def _add_animation_relations(
    builder: AssertionBuilder,
    payload: dict[str, Any],
    *,
    namespace_prefix: str,
) -> None:
    adaptations = payload.get("adaptations")
    if isinstance(adaptations, list):
        for index, adaptation in enumerate(adaptations):
            if not isinstance(adaptation, dict) or adaptation.get("id") is None:
                continue
            builder.add_relationship(
                "adaptation_of",
                SourceNodeRef(
                    namespace_id=f"{namespace_prefix}-work",
                    source_id=str(adaptation["id"]),
                    referent_kind="EDITORIAL_WORK",
                ),
                f"/adaptations/{index}",
                {
                    key: adaptation[key]
                    for key in ("sourceMedium", "relationship")
                    if adaptation.get(key) is not None
                },
            )
    characters = payload.get("characters")
    if isinstance(characters, list):
        for character_index, character in enumerate(characters):
            if not isinstance(character, dict) or character.get("id") is None:
                continue
            character_id = str(character["id"])
            builder.add_relationship(
                "character_appearance",
                SourceNodeRef(
                    namespace_id=f"{namespace_prefix}-character",
                    source_id=character_id,
                    referent_kind="CHARACTER",
                ),
                f"/characters/{character_index}",
                {
                    "role": character.get("role"),
                    "name": character.get("name"),
                },
            )
            performers = character.get("voicePerformers")
            if isinstance(performers, list):
                for performer_index, performer in enumerate(performers):
                    if not isinstance(performer, dict) or performer.get("id") is None:
                        continue
                    builder.add_relationship(
                        "voice_performer",
                        SourceNodeRef(
                            namespace_id=f"{namespace_prefix}-agent",
                            source_id=str(performer["id"]),
                            referent_kind="AGENT",
                        ),
                        (
                            f"/characters/{character_index}/"
                            f"voicePerformers/{performer_index}"
                        ),
                        {
                            "characterId": character_id,
                            "language": performer.get("language"),
                        },
                    )
    credits = payload.get("credits")
    if isinstance(credits, list):
        for index, credit in enumerate(credits):
            if not isinstance(credit, dict) or credit.get("agentId") is None:
                continue
            builder.add_relationship(
                "production_credit",
                SourceNodeRef(
                    namespace_id=f"{namespace_prefix}-agent",
                    source_id=str(credit["agentId"]),
                    referent_kind=str(credit.get("referentKind") or "AGENT").upper(),
                ),
                f"/credits/{index}",
                {
                    key: credit[key]
                    for key in ("role", "department", "characterId")
                    if credit.get(key) is not None
                },
            )


def map_madb_animation_record(
    envelope: ConnectorRecordEnvelope,
) -> MappedAssertions:
    payload = _active_payload(
        envelope,
        source_system_id="madb",
        source_product_id="madb-public-data",
    )
    record_type = str(payload.get("type") or "work").strip().lower()
    namespace_id = {
        "work": "madb-work",
        "character": "madb-character",
        "agent": "madb-agent",
        "organization": "madb-agent",
    }.get(record_type)
    if namespace_id is None:
        raise ValueError(f"unsupported MADB record type: {record_type}")
    referent_kind, entity_type = _entity_projection(
        payload.get("entityType")
        or (
            "CHARACTER"
            if record_type == "character"
            else "ORGANIZATION"
            if record_type == "organization"
            else "AGENT"
            if record_type == "agent"
            else "WORK"
        )
    )
    record_id = str(payload.get("id") or "").strip()
    if (
        record_id != envelope.source_record_id
        or envelope.source_namespace_id != namespace_id
    ):
        raise ValueError("MADB payload identity does not match its envelope")
    node = SourceNodeRef(
        namespace_id=namespace_id,
        source_id=record_id,
        referent_kind=referent_kind,
    )
    builder = AssertionBuilder(
        envelope=envelope,
        source_node=node,
        mapper_id=MADB_MAPPER_ID,
        mapper_version="1.0.0",
    )
    builder.add_identifier(
        namespace_id,
        record_id,
        "Media Arts Database",
        referent_kind,
        "/id",
    )
    builder.add_entity_type(entity_type, "/entityType")
    _add_titles(builder, payload.get("titles", payload.get("title")), "/titles")
    if record_type == "work":
        _add_profile(builder, payload)
        _add_animation_relations(builder, payload, namespace_prefix="madb")
        parent = payload.get("parent")
        if isinstance(parent, dict) and parent.get("id") is not None:
            parent_kind, _ = _entity_projection(parent.get("entityType"))
            builder.add_relationship(
                {
                    "SERIES": "part_of_series",
                    "SEASON": "part_of_season",
                }.get(parent_kind, "part_of"),
                SourceNodeRef(
                    namespace_id="madb-work",
                    source_id=str(parent["id"]),
                    referent_kind=parent_kind,
                ),
                "/parent",
                {
                    key: parent[key]
                    for key in ("seasonNumber", "episodeNumber", "ordinal")
                    if parent.get(key) is not None
                },
            )
    return builder.build()


def _wikidata_claim_values(
    payload: dict[str, Any],
    property_id: str,
) -> tuple[Any, ...]:
    claims = payload.get("claims")
    if not isinstance(claims, dict) or not isinstance(claims.get(property_id), list):
        return ()
    values: list[Any] = []
    for statement in claims[property_id]:
        if not isinstance(statement, dict) or statement.get("rank") == "deprecated":
            continue
        mainsnak = statement.get("mainsnak")
        datavalue = mainsnak.get("datavalue") if isinstance(mainsnak, dict) else None
        value = datavalue.get("value") if isinstance(datavalue, dict) else None
        if isinstance(value, dict) and isinstance(value.get("id"), str):
            value = value["id"]
        values.append(value)
    return tuple(values)


def map_wikidata_animation_record(
    envelope: ConnectorRecordEnvelope,
) -> MappedAssertions:
    payload = _active_payload(
        envelope,
        source_system_id="wikidata",
        source_product_id="wikidata-json-dump",
    )
    if payload.get("researchDomain") != "ANIMATION":
        raise ValueError("Wikidata animation record requires researchDomain=ANIMATION")
    referent_kind, entity_type = _entity_projection(payload.get("entityTypeHint"))
    qid = str(payload.get("id") or "").strip()
    if (
        qid != envelope.source_record_id
        or envelope.source_namespace_id != "wikidata-item"
    ):
        raise ValueError("Wikidata animation payload identity does not match envelope")
    node = SourceNodeRef(
        namespace_id="wikidata-item",
        source_id=qid,
        referent_kind=referent_kind,
    )
    builder = AssertionBuilder(
        envelope=envelope,
        source_node=node,
        mapper_id=WIKIDATA_ANIMATION_MAPPER_ID,
        mapper_version="1.0.0",
    )
    builder.add_identifier(
        "wikidata-item",
        qid,
        "Wikidata",
        referent_kind,
        "/id",
    )
    builder.add_entity_type(entity_type, "/entityTypeHint")
    _add_titles(builder, payload.get("labels"), "/labels")
    aliases = payload.get("aliases")
    if isinstance(aliases, dict):
        for language, values in sorted(aliases.items()):
            if not isinstance(values, list):
                continue
            for index, alias in enumerate(values):
                value = alias.get("value") if isinstance(alias, dict) else alias
                if isinstance(value, str):
                    builder.add_field(
                        "title",
                        ValueType.STRING,
                        value,
                        f"/aliases/{language}/{index}",
                        {"language": language, "titleRole": "ALIAS"},
                    )
    _add_profile(builder, payload)

    for property_id, namespace_id, normalizer in (
        ("P345", "imdb-title", normalize_imdb_id),
        ("P2704", "eidr-content", normalize_eidr_id),
    ):
        for index, value in enumerate(_wikidata_claim_values(payload, property_id)):
            if not isinstance(value, str):
                continue
            try:
                normalized = normalizer(value)
            except ValueError:
                continue
            builder.add_identifier(
                namespace_id,
                normalized,
                "IMDb" if namespace_id == "imdb-title" else "EIDR Association",
                referent_kind,
                f"/claims/{property_id}/{index}",
            )
    for property_id, predicate, target_kind in (
        ("P144", "adaptation_of", "EDITORIAL_WORK"),
        ("P674", "character_appearance", "CHARACTER"),
        ("P725", "voice_performer", "AGENT"),
    ):
        for index, target in enumerate(_wikidata_claim_values(payload, property_id)):
            if isinstance(target, str):
                builder.add_relationship(
                    predicate,
                    SourceNodeRef(
                        namespace_id="wikidata-item",
                        source_id=target,
                        referent_kind=target_kind,
                    ),
                    f"/claims/{property_id}/{index}",
                )
    for property_id in ("P57", "P58", "P162", "P272"):
        for index, target in enumerate(_wikidata_claim_values(payload, property_id)):
            if isinstance(target, str):
                builder.add_relationship(
                    "production_credit",
                    SourceNodeRef(
                        namespace_id="wikidata-item",
                        source_id=target,
                        referent_kind=(
                            "ORGANIZATION" if property_id == "P272" else "AGENT"
                        ),
                    ),
                    f"/claims/{property_id}/{index}",
                    {"wikidataProperty": property_id},
                )
    return builder.build()


@dataclass(frozen=True)
class MadbHttpResponse:
    status: int
    body: bytes
    headers: Mapping[str, str]


class MadbHttpTransport(Protocol):
    def get(self, url: str) -> MadbHttpResponse: ...


class MadbPublicClient:
    """Fixed-origin, rate-limited client whose responses must be archived by callers."""

    def __init__(
        self,
        *,
        transport: MadbHttpTransport,
        base_url: str = "https://mediaarts-db.artmuseums.go.jp/",
        max_attempts: int = 3,
        min_interval_seconds: float = 1.0,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        parsed = urlsplit(base_url)
        if (
            parsed.scheme != "https"
            or parsed.hostname != "mediaarts-db.artmuseums.go.jp"
            or parsed.username is not None
            or parsed.password is not None
        ):
            raise ValueError("MADB client requires the reviewed fixed HTTPS origin")
        if not 1 <= max_attempts <= 5:
            raise ValueError("MADB max_attempts must be between 1 and 5")
        if not 0 <= min_interval_seconds <= 60:
            raise ValueError("MADB rate-limit interval is out of range")
        self.transport = transport
        self.base_url = base_url
        self.max_attempts = max_attempts
        self.min_interval_seconds = min_interval_seconds
        self.sleeper = sleeper

    def fetch_json(self, relative_path: str) -> tuple[dict[str, Any], int, int]:
        if not relative_path.startswith("/") or ".." in relative_path:
            raise ValueError("MADB path must be an absolute safe relative path")
        url = urljoin(self.base_url, relative_path.lstrip("/"))
        retries = 0
        rate_limits = 0
        for attempt in range(1, self.max_attempts + 1):
            if attempt > 1 or self.min_interval_seconds:
                self.sleeper(self.min_interval_seconds)
            response = self.transport.get(url)
            if response.status == 200:
                payload = json.loads(response.body)
                if not isinstance(payload, dict):
                    raise ValueError("MADB response must be a JSON object")
                return payload, retries, rate_limits
            retryable = response.status in {408, 429, 500, 502, 503, 504}
            if not retryable or attempt == self.max_attempts:
                raise RuntimeError(f"MADB request failed with HTTP {response.status}")
            retries = attempt
            if response.status == 429:
                rate_limits += 1
        raise AssertionError("MADB retry loop did not terminate")
