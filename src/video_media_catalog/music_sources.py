"""Silver mappers for pinned MusicBrainz core and Wikidata Music records."""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from typing import Any

from video_media_catalog.assertions import SourceNodeRef, ValueType
from video_media_catalog.connector import ConnectorRecordEnvelope, RecordOperation
from video_media_catalog.predicate_registry import (
    PredicateKind,
    ResearchDomain,
    load_research_predicate_registry,
)
from video_media_catalog.reference_ontology import (
    ReferenceDomain,
    load_global_reference_ontology,
)
from video_media_catalog.source_mapper import AssertionBuilder, MappedAssertions

MUSICBRAINZ_CORE_MAPPER_ID = "musicbrainz-core-source-mapper"
MUSICBRAINZ_CORE_MAPPER_VERSION = "1.3.0"
WIKIDATA_MUSIC_MAPPER_ID = "wikidata-music-source-mapper"
WIKIDATA_MUSIC_MAPPER_VERSION = "1.3.0"
MUSICBRAINZ_CORE_SCHEMA_ID = "musicbrainz-core-entity"
MUSICBRAINZ_CORE_DATASET = "MUSICBRAINZ_CORE_CC0"

_MBID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
_ISRC = re.compile(r"[A-Z]{2}[A-Z0-9]{3}[0-9]{7}")
_ISWC = re.compile(r"T-[0-9]{3}\.[0-9]{3}\.[0-9]{3}-[0-9]")
_QID = re.compile(r"Q[1-9][0-9]*")

_MUSIC_TYPES = {
    "MUSIC_ARTIST": "musicbrainz-artist",
    "MUSIC_LABEL": "musicbrainz-label",
    "MUSICAL_WORK": "musicbrainz-work",
    "MUSIC_RECORDING": "musicbrainz-recording",
    "MUSIC_RELEASE_GROUP": "musicbrainz-release-group",
    "MUSIC_RELEASE": "musicbrainz-release",
}
_FORBIDDEN_CORE_KEYS = frozenset(
    {
        "annotation",
        "annotations",
        "coverart",
        "coverartarchive",
        "genre",
        "genres",
        "genrelist",
        "livefeed",
        "rating",
        "ratings",
        "ratingvalue",
        "supplementary",
        "tag",
        "tags",
        "taglist",
        "userrating",
        "usertag",
        "usertags",
    }
)
_FORBIDDEN_CORE_PREFIXES = (
    "annotation",
    "coverart",
    "folksonomy",
    "genre",
    "livefeed",
    "rating",
    "supplementary",
    "tag",
    "userrating",
    "usertag",
)
_WIKIDATA_IDENTIFIER_PROPERTIES = {
    "P434": ("musicbrainz-artist", "MUSIC_ARTIST", _MBID),
    "P966": ("musicbrainz-label", "MUSIC_LABEL", _MBID),
    "P435": ("musicbrainz-work", "MUSICAL_WORK", _MBID),
    "P436": ("musicbrainz-release-group", "MUSIC_RELEASE_GROUP", _MBID),
    "P4404": ("musicbrainz-recording", "MUSIC_RECORDING", _MBID),
    "P5813": ("musicbrainz-release", "MUSIC_RELEASE", _MBID),
    "P1243": ("isrc", "MUSIC_RECORDING", _ISRC),
    "P1827": ("iswc", "MUSICAL_WORK", _ISWC),
}
_MUSIC_AGENT_KINDS = frozenset({"PERSON", "ORGANIZATION"})
_MUSICBRAINZ_ARTIST_AGENT_KINDS = {
    "person": "PERSON",
    "group": "ORGANIZATION",
    "orchestra": "ORGANIZATION",
    "choir": "ORGANIZATION",
}
_WIKIDATA_AGENT_INSTANCE_KINDS = {
    "Q5": "PERSON",
    "Q215380": "ORGANIZATION",
    "Q2088357": "ORGANIZATION",
    "Q43229": "ORGANIZATION",
    "Q4830453": "ORGANIZATION",
}


def musicbrainz_namespace_for_type(entity_type: str) -> str:
    try:
        return _MUSIC_TYPES[entity_type.strip().upper()]
    except KeyError as exc:
        raise ValueError(f"unsupported Music entity type: {entity_type}") from exc


def _normalize_key(value: object) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value).lower())


def _reject_non_core_data(value: Any, *, path: str = "") -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            normalized_key = _normalize_key(key)
            if normalized_key in _FORBIDDEN_CORE_KEYS or normalized_key.startswith(
                _FORBIDDEN_CORE_PREFIXES
            ):
                raise ValueError(
                    f"MusicBrainz core payload contains excluded data at {path}/{key}"
                )
            _reject_non_core_data(item, path=f"{path}/{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _reject_non_core_data(item, path=f"{path}/{index}")


def _payload(envelope: ConnectorRecordEnvelope) -> dict[str, Any]:
    if envelope.operation != RecordOperation.UPSERT or envelope.payload_json is None:
        raise ValueError("record is not an active Music reference envelope")
    payload = json.loads(envelope.payload_json)
    if not isinstance(payload, dict):
        raise ValueError("Music reference payload must be an object")
    if str(payload.get("id") or "") != envelope.source_record_id:
        raise ValueError("Music payload identity does not match its envelope")
    if payload.get("researchDomain") != ResearchDomain.MUSIC.value:
        raise ValueError("Music reference payload requires researchDomain=MUSIC")
    return payload


def _entity_type(payload: dict[str, Any]) -> str:
    value = str(payload.get("entityType") or payload.get("entityTypeHint") or "")
    entity_type = value.strip().upper()
    musicbrainz_namespace_for_type(entity_type)
    concept = load_global_reference_ontology().concept_for(entity_type)
    if ReferenceDomain.MUSIC not in concept.domains:
        raise ValueError("Music entity type is outside the MUSIC ontology slice")
    return entity_type


def _normalize_mbid(value: object, *, label: str) -> str:
    normalized = str(value or "").strip().lower()
    if _MBID.fullmatch(normalized) is None:
        raise ValueError(f"{label} must be a MusicBrainz identifier")
    return normalized


def _normalize_isrc(value: object) -> str:
    normalized = re.sub(r"[-\s]", "", str(value or "")).upper()
    if _ISRC.fullmatch(normalized) is None:
        raise ValueError("ISRC is invalid")
    return normalized


def _normalize_iswc(value: object) -> str:
    raw = str(value or "").strip().upper()
    digits = "".join(character for character in raw if character.isdigit())
    normalized = (
        f"T-{digits[:3]}.{digits[3:6]}.{digits[6:9]}-{digits[9:]}"
        if len(digits) == 10
        else raw
    )
    if _ISWC.fullmatch(normalized) is None:
        raise ValueError("ISWC is invalid")
    return normalized


def _add_titles(builder: AssertionBuilder, payload: dict[str, Any]) -> None:
    name = payload.get("name")
    if isinstance(name, str) and name.strip():
        builder.add_field(
            "title",
            ValueType.STRING,
            name.strip(),
            "/name",
            {"language": "und", "titleRole": "PRIMARY"},
        )
    labels = payload.get("labels")
    if isinstance(labels, dict):
        for language, label in sorted(labels.items()):
            value = label.get("value") if isinstance(label, dict) else label
            if isinstance(value, str) and value.strip():
                builder.add_field(
                    "title",
                    ValueType.STRING,
                    value.strip(),
                    f"/labels/{language}",
                    {"language": str(language), "titleRole": "PRIMARY"},
                )
    aliases = payload.get("aliases")
    if isinstance(aliases, list):
        for index, alias in enumerate(aliases):
            value = (
                alias.get("name", alias.get("value"))
                if isinstance(alias, dict)
                else alias
            )
            if isinstance(value, str) and value.strip():
                language = (
                    alias.get("locale", "und") if isinstance(alias, dict) else "und"
                )
                builder.add_field(
                    "title",
                    ValueType.STRING,
                    value.strip(),
                    f"/aliases/{index}",
                    {"language": str(language or "und"), "titleRole": "ALIAS"},
                )
    elif isinstance(aliases, dict):
        for language, values in sorted(aliases.items()):
            if not isinstance(values, list):
                continue
            for index, alias in enumerate(values):
                value = alias.get("value") if isinstance(alias, dict) else alias
                if isinstance(value, str) and value.strip():
                    builder.add_field(
                        "title",
                        ValueType.STRING,
                        value.strip(),
                        f"/aliases/{language}/{index}",
                        {"language": str(language), "titleRole": "ALIAS"},
                    )


def _add_relations(
    builder: AssertionBuilder,
    payload: dict[str, Any],
    *,
    target_namespace: Callable[[str, str], str],
) -> None:
    relations = payload.get("relations")
    if relations is None:
        return
    if not isinstance(relations, list):
        raise ValueError("Music relations must be an array")
    predicates = load_research_predicate_registry()
    ontology = load_global_reference_ontology()
    for index, relation in enumerate(relations):
        if not isinstance(relation, dict):
            raise ValueError("Music relation must be an object")
        predicate = str(relation.get("predicate") or "").strip().lower()
        definition = predicates.definition_for(predicate)
        if (
            definition.kind != PredicateKind.RELATIONSHIP
            or ResearchDomain.MUSIC not in definition.domains
        ):
            raise ValueError(f"Music relation registry mismatch: {predicate}")
        target_type = str(relation.get("targetType") or "").strip().upper()
        target_concept = ontology.concept_for(target_type)
        if ReferenceDomain.MUSIC not in target_concept.domains:
            raise ValueError("Music relation target is outside the MUSIC slice")
        target_id = str(relation.get("targetId") or "").strip()
        if not target_id:
            raise ValueError("Music relation requires targetId")
        namespace_id = target_namespace(target_type, target_id)
        qualifiers = relation.get("qualifiers") or {}
        if not isinstance(qualifiers, dict):
            raise ValueError("Music relation qualifiers must be an object")
        normalized_qualifiers = dict(qualifiers)
        target_agent_kind = normalized_qualifiers.pop("targetAgentKind", None)
        if target_type == "MUSIC_LABEL":
            target_agent_kind = target_agent_kind or "ORGANIZATION"
        if target_agent_kind is not None:
            normalized_agent_kind = str(target_agent_kind).strip().upper()
            if (
                target_type not in {"MUSIC_ARTIST", "MUSIC_LABEL"}
                or normalized_agent_kind not in _MUSIC_AGENT_KINDS
            ):
                raise ValueError("Music relation target agent kind is invalid")
            normalized_qualifiers["agentKind"] = normalized_agent_kind
        builder.add_relationship(
            predicate,
            SourceNodeRef(
                namespace_id=namespace_id,
                source_id=target_id,
                referent_kind=target_type,
            ),
            f"/relations/{index}",
            normalized_qualifiers,
        )


def _musicbrainz_target_namespace(target_type: str, target_id: str) -> str:
    _normalize_mbid(target_id, label="MusicBrainz relation target")
    return musicbrainz_namespace_for_type(target_type)


def _add_musicbrainz_recording_variants(
    builder: AssertionBuilder,
    payload: dict[str, Any],
    *,
    entity_type: str,
) -> None:
    variants = payload.get("recordingVariants") or []
    if not isinstance(variants, list):
        raise ValueError("MusicBrainz recordingVariants must be an array")
    if variants and entity_type != "MUSIC_RECORDING":
        raise ValueError("MusicBrainz recordingVariants require MUSIC_RECORDING")
    for index, variant in enumerate(variants):
        if not isinstance(variant, dict):
            raise ValueError("MusicBrainz recording variant must be an object")
        target_id = _normalize_mbid(
            variant.get("targetId"),
            label="MusicBrainz recording variant target",
        )
        relation_type = str(variant.get("relationType") or "").strip().lower()
        direction = str(variant.get("direction") or "").strip().lower()
        target_title = str(variant.get("title") or "").strip()
        if not relation_type or direction not in {"forward", "backward"}:
            raise ValueError("MusicBrainz recording variant relation is invalid")
        builder.add_field(
            "media_variant",
            ValueType.STRING,
            relation_type,
            f"/recordingVariants/{index}/relationType",
            {
                "authorityRelation": relation_type,
                "direction": direction,
                "targetId": target_id,
                **({"targetTitle": target_title} if target_title else {}),
            },
        )


def _wikidata_claim_item_ids(
    payload: dict[str, Any],
    property_id: str,
) -> tuple[str, ...]:
    claims = payload.get("claims")
    if not isinstance(claims, dict) or not isinstance(claims.get(property_id), list):
        return ()
    values = []
    for statement in claims[property_id]:
        if not isinstance(statement, dict) or statement.get("rank") == "deprecated":
            continue
        mainsnak = statement.get("mainsnak")
        datavalue = mainsnak.get("datavalue") if isinstance(mainsnak, dict) else None
        value = datavalue.get("value") if isinstance(datavalue, dict) else None
        qid = value.get("id") if isinstance(value, dict) else None
        if isinstance(qid, str) and _QID.fullmatch(qid):
            values.append(qid)
    return tuple(values)


def _add_music_agent_kind(
    builder: AssertionBuilder,
    payload: dict[str, Any],
    *,
    entity_type: str,
    source_system: str,
) -> None:
    if entity_type not in {"MUSIC_ARTIST", "MUSIC_LABEL"}:
        return
    explicit = payload.get("agentKind")
    explicit_kind = str(explicit or "").strip().upper() or None
    if explicit_kind is not None and explicit_kind not in _MUSIC_AGENT_KINDS:
        raise ValueError("Music agentKind must be PERSON or ORGANIZATION")
    if entity_type == "MUSIC_LABEL":
        derived_kind = "ORGANIZATION"
        source_path = "/entityType"
    elif source_system == "musicbrainz":
        artist_type = str(payload.get("musicBrainzArtistType") or "").strip().lower()
        derived_kind = _MUSICBRAINZ_ARTIST_AGENT_KINDS.get(artist_type)
        source_path = "/musicBrainzArtistType"
    else:
        derived = {
            kind
            for qid in _wikidata_claim_item_ids(payload, "P31")
            if (kind := _WIKIDATA_AGENT_INSTANCE_KINDS.get(qid)) is not None
        }
        if len(derived) > 1:
            raise ValueError("Wikidata Music artist has conflicting concrete kinds")
        derived_kind = next(iter(derived), None)
        source_path = "/claims/P31"
    if (
        explicit_kind is not None
        and derived_kind is not None
        and explicit_kind != derived_kind
    ):
        raise ValueError("Music agentKind differs from authority type evidence")
    selected = derived_kind or explicit_kind
    if selected is not None:
        builder.add_field(
            "music_agent_kind",
            ValueType.STRING,
            selected,
            source_path if derived_kind is not None else "/agentKind",
            {"musicRole": entity_type},
        )


def map_musicbrainz_core_record(
    envelope: ConnectorRecordEnvelope,
) -> MappedAssertions:
    """Map one normalized record from the offline MusicBrainz core CC0 dump."""

    if (
        envelope.source_system_id != "musicbrainz"
        or envelope.source_product_id != "musicbrainz-core-dump"
        or envelope.payload_schema != MUSICBRAINZ_CORE_SCHEMA_ID
    ):
        raise ValueError("record is not a MusicBrainz core dump envelope")
    payload = _payload(envelope)
    if payload.get("coreDataset") != MUSICBRAINZ_CORE_DATASET:
        raise ValueError("MusicBrainz payload is not marked as core CC0 data")
    _reject_non_core_data(payload)
    entity_type = _entity_type(payload)
    namespace_id = musicbrainz_namespace_for_type(entity_type)
    mbid = _normalize_mbid(payload["id"], label="record id")
    if envelope.source_namespace_id != namespace_id:
        raise ValueError("MusicBrainz namespace does not match entity type")
    node = SourceNodeRef(
        namespace_id=namespace_id,
        source_id=mbid,
        referent_kind=entity_type,
    )
    builder = AssertionBuilder(
        envelope=envelope,
        source_node=node,
        mapper_id=MUSICBRAINZ_CORE_MAPPER_ID,
        mapper_version=MUSICBRAINZ_CORE_MAPPER_VERSION,
    )
    builder.add_identifier(namespace_id, mbid, "MusicBrainz", entity_type, "/id")
    builder.add_entity_type(entity_type, "/entityType")
    _add_titles(builder, payload)
    _add_music_agent_kind(
        builder,
        payload,
        entity_type=entity_type,
        source_system="musicbrainz",
    )

    isrcs = payload.get("isrcs") or []
    if isrcs and entity_type != "MUSIC_RECORDING":
        raise ValueError("ISRC identifiers require MUSIC_RECORDING")
    for index, value in enumerate(isrcs):
        builder.add_identifier(
            "isrc",
            _normalize_isrc(value),
            "IFPI",
            "MUSIC_RECORDING",
            f"/isrcs/{index}",
        )
    iswcs = payload.get("iswcs") or []
    if iswcs and entity_type != "MUSICAL_WORK":
        raise ValueError("ISWC identifiers require MUSICAL_WORK")
    for index, value in enumerate(iswcs):
        builder.add_identifier(
            "iswc",
            _normalize_iswc(value),
            "CISAC",
            "MUSICAL_WORK",
            f"/iswcs/{index}",
        )
    for index, value in enumerate(payload.get("wikidataQids") or []):
        qid = str(value).strip().upper()
        if _QID.fullmatch(qid) is None:
            raise ValueError("Wikidata QID is invalid")
        builder.add_identifier(
            "wikidata-item",
            qid,
            "Wikidata",
            entity_type,
            f"/wikidataQids/{index}",
        )
    _add_relations(
        builder,
        payload,
        target_namespace=_musicbrainz_target_namespace,
    )
    _add_musicbrainz_recording_variants(
        builder,
        payload,
        entity_type=entity_type,
    )
    return builder.build()


def _wikidata_claim_values(
    payload: dict[str, Any],
    property_id: str,
) -> tuple[str, ...]:
    claims = payload.get("claims")
    if not isinstance(claims, dict) or not isinstance(claims.get(property_id), list):
        return ()
    values: list[str] = []
    for statement in claims[property_id]:
        if not isinstance(statement, dict) or statement.get("rank") == "deprecated":
            continue
        mainsnak = statement.get("mainsnak")
        datavalue = mainsnak.get("datavalue") if isinstance(mainsnak, dict) else None
        value = datavalue.get("value") if isinstance(datavalue, dict) else None
        if isinstance(value, str):
            values.append(value)
    return tuple(values)


def _add_wikidata_media_objects(
    builder: AssertionBuilder,
    payload: dict[str, Any],
    *,
    entity_type: str,
) -> None:
    values = _wikidata_claim_values(payload, "P51")
    if entity_type != "MUSIC_RECORDING":
        return
    for index, raw in enumerate(values):
        value = raw.strip()
        if not value:
            raise ValueError("Wikidata P51 media object must be non-empty")
        builder.add_field(
            "media_object",
            ValueType.STRING,
            value,
            f"/claims/P51/{index}/mainsnak/datavalue/value",
            {"authorityProperty": "P51"},
        )


def map_wikidata_music_record(
    envelope: ConnectorRecordEnvelope,
) -> MappedAssertions:
    """Map one reviewed Wikidata Music record without mutable lookups."""

    if (
        envelope.source_system_id != "wikidata"
        or envelope.source_product_id != "wikidata-json-dump"
        or envelope.source_namespace_id != "wikidata-item"
    ):
        raise ValueError("record is not a Wikidata Music envelope")
    payload = _payload(envelope)
    entity_type = _entity_type(payload)
    qid = str(payload["id"]).strip().upper()
    if _QID.fullmatch(qid) is None:
        raise ValueError("Wikidata Music record requires a QID")
    node = SourceNodeRef(
        namespace_id="wikidata-item",
        source_id=qid,
        referent_kind=entity_type,
    )
    builder = AssertionBuilder(
        envelope=envelope,
        source_node=node,
        mapper_id=WIKIDATA_MUSIC_MAPPER_ID,
        mapper_version=WIKIDATA_MUSIC_MAPPER_VERSION,
    )
    builder.add_identifier("wikidata-item", qid, "Wikidata", entity_type, "/id")
    builder.add_entity_type(entity_type, "/entityTypeHint")
    _add_titles(builder, payload)
    _add_music_agent_kind(
        builder,
        payload,
        entity_type=entity_type,
        source_system="wikidata",
    )
    for property_id, (
        namespace_id,
        referent_kind,
        pattern,
    ) in _WIKIDATA_IDENTIFIER_PROPERTIES.items():
        if referent_kind != entity_type:
            continue
        for index, raw in enumerate(_wikidata_claim_values(payload, property_id)):
            if namespace_id.startswith("musicbrainz-"):
                normalized = _normalize_mbid(raw, label=property_id)
            elif namespace_id == "isrc":
                normalized = _normalize_isrc(raw)
            else:
                normalized = _normalize_iswc(raw)
            if pattern.fullmatch(normalized) is None:
                raise ValueError(f"{property_id} identifier is invalid")
            builder.add_identifier(
                namespace_id,
                normalized,
                (
                    "MusicBrainz"
                    if namespace_id.startswith("musicbrainz-")
                    else "IFPI"
                    if namespace_id == "isrc"
                    else "CISAC"
                ),
                referent_kind,
                f"/claims/{property_id}/{index}",
            )
    _add_relations(
        builder,
        payload,
        target_namespace=_wikidata_target_namespace,
    )
    _add_wikidata_media_objects(
        builder,
        payload,
        entity_type=entity_type,
    )
    return builder.build()


def _wikidata_target_namespace(_target_type: str, target_id: str) -> str:
    if _QID.fullmatch(target_id) is None:
        raise ValueError("Wikidata relation target is invalid")
    return "wikidata-item"
