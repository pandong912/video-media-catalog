"""Reviewed Wikidata and EIDR projections into Silver assertions."""

from __future__ import annotations

import json
import re
from typing import Any

from video_media_catalog.connector import ConnectorRecordEnvelope, RecordOperation
from video_media_catalog.constants import (
    DOUBAN_EXTERNAL_ID_PROPERTIES,
    ENTITY_TYPE_SEEDS,
    EXTERNAL_ID_PROPERTIES,
    RELATION_PROPERTIES,
)
from video_media_catalog.douban import normalize_douban_id
from video_media_catalog.eidr import normalize_eidr_id, normalize_imdb_id
from video_media_catalog.identity_resolution import referent_kind_for_entity_type
from video_media_catalog.imdb import (
    IMDB_COMPANY_NAMESPACE_ID,
    IMDB_NAME_NAMESPACE_ID,
    IMDB_TITLE_NAMESPACE_ID,
)
from video_media_catalog.source_mapper import AssertionBuilder, MappedAssertions

WIKIDATA_SOURCE_SYSTEM_ID = "wikidata"
WIKIDATA_SOURCE_PRODUCT_ID = "wikidata-json-dump"
WIKIDATA_NAMESPACE_ID = "wikidata-item"
WIKIDATA_FULL_MEDIA_CONNECTOR_ID = "wikidata-full-media-backfill"

EIDR_SOURCE_SYSTEM_ID = "eidr"
EIDR_SOURCE_PRODUCT_ID = "eidr-public-registry"
EIDR_NAMESPACE_ID = "eidr-content"

_QID = re.compile(r"Q[1-9][0-9]*")
_TYPE_PRECEDENCE = {
    "TV_EPISODE": 0,
    "TV_SEASON": 1,
    "TV_SERIES": 2,
    "MOVIE": 3,
    "PERSON": 4,
    "ORGANIZATION": 5,
}
_EIDR_REFERENT_KIND = {
    "WORK": "EDITORIAL_WORK",
    "MOVIE": "MOVIE",
    "TV_SERIES": "SERIES",
    "TV_SEASON": "SEASON",
    "TV_EPISODE": "EPISODE",
    "EDIT": "EDIT",
    "MANIFESTATION": "MANIFESTATION",
}


def _external_identifier_referent_kind(
    namespace_id: str,
    entity_type: str | None,
) -> str:
    if namespace_id == IMDB_NAME_NAMESPACE_ID:
        return "AGENT"
    if namespace_id == IMDB_COMPANY_NAMESPACE_ID:
        return "ORGANIZATION"
    return referent_kind_for_entity_type(entity_type or "EDITORIAL_WORK")


def _imdb_namespace_id(value: str) -> str | None:
    if value.startswith("tt"):
        return IMDB_TITLE_NAMESPACE_ID
    if value.startswith("nm"):
        return IMDB_NAME_NAMESPACE_ID
    if value.startswith("co"):
        return IMDB_COMPANY_NAMESPACE_ID
    return None


def statement_value(statement: dict[str, Any]) -> Any:
    snak = statement.get("mainsnak")
    if not isinstance(snak, dict) or snak.get("snaktype", "value") != "value":
        return None
    datavalue = snak.get("datavalue")
    if not isinstance(datavalue, dict):
        return None
    value = datavalue.get("value")
    if isinstance(value, dict):
        entity_id = value.get("id")
        if isinstance(entity_id, str):
            return entity_id
        if value.get("entity-type") == "item" and isinstance(
            value.get("numeric-id"), int
        ):
            return f"Q{value['numeric-id']}"
    return value


def statements(payload: dict[str, Any], prop: str) -> list[dict[str, Any]]:
    claims = payload.get("claims")
    if not isinstance(claims, dict) or not isinstance(claims.get(prop), list):
        return []
    candidates = [
        item
        for item in claims[prop]
        if isinstance(item, dict) and item.get("rank") != "deprecated"
    ]
    preferred = [item for item in candidates if item.get("rank") == "preferred"]
    return preferred or candidates


def qid_values(payload: dict[str, Any], prop: str) -> list[str]:
    return [
        value
        for statement in statements(payload, prop)
        if isinstance((value := statement_value(statement)), str)
        and _QID.fullmatch(value)
    ]


def _wikidata_type(payload: dict[str, Any]) -> str | None:
    values = {
        ENTITY_TYPE_SEEDS[value]
        for statement in statements(payload, "P31")
        if isinstance((value := statement_value(statement)), str)
        and value in ENTITY_TYPE_SEEDS
    }
    hint = payload.get("entityTypeHint")
    if isinstance(hint, str) and hint.upper() in _TYPE_PRECEDENCE:
        values.add(hint.upper())
    return min(values, key=_TYPE_PRECEDENCE.__getitem__) if values else None


def _wikidata_date(value: Any) -> str | None:
    if not isinstance(value, dict):
        return None
    raw = value.get("time")
    precision = value.get("precision")
    if not isinstance(raw, str) or not raw.startswith("+") or len(raw) < 5:
        return None
    year = raw[1:5]
    if isinstance(precision, int) and precision >= 11 and len(raw) >= 11:
        return f"{year}-{raw[6:8]}-{raw[9:11]}"
    if isinstance(precision, int) and precision >= 10 and len(raw) >= 8:
        return f"{year}-{raw[6:8]}"
    return year


def _quantity(value: Any) -> str | None:
    if not isinstance(value, dict) or not isinstance(value.get("amount"), str):
        return None
    normalized = value["amount"].lstrip("+")
    return normalized or None


def _eidr_entity_type(payload: dict[str, Any]) -> str:
    record_type = str(payload.get("recordType") or "").upper()
    hierarchy_type = {
        "EPISODE": "TV_EPISODE",
        "SEASON": "TV_SEASON",
        "SERIES": "TV_SERIES",
    }.get(record_type)
    if hierarchy_type is not None:
        return hierarchy_type
    structural_type = str(payload.get("structuralType") or "").strip().lower()
    structural = {
        "abstraction": "WORK",
        "performance": "EDIT",
        "digital": "MANIFESTATION",
    }.get(structural_type)
    if structural is not None:
        return structural
    if record_type == "EDIT":
        return "EDIT"
    referent_type = str(payload.get("referentType") or "").lower()
    if "episode" in referent_type:
        return "TV_EPISODE"
    if "season" in referent_type:
        return "TV_SEASON"
    if "series" in referent_type:
        return "TV_SERIES"
    if any(token in referent_type for token in ("movie", "film", "television", "tv")):
        return "MOVIE"
    return "UNKNOWN"


def _ordinal(statement: dict[str, Any]) -> str | None:
    qualifiers = statement.get("qualifiers")
    if not isinstance(qualifiers, dict):
        return None
    values = qualifiers.get("P1545")
    if not isinstance(values, list):
        return None
    for value in values:
        if isinstance(value, dict):
            extracted = statement_value({"mainsnak": value})
            if extracted is not None:
                return str(extracted)
    return None


def map_wikidata_record(
    envelope: ConnectorRecordEnvelope,
) -> MappedAssertions:
    """Map one normalized Wikidata source record."""

    if (
        envelope.source_system_id != WIKIDATA_SOURCE_SYSTEM_ID
        or envelope.source_product_id != WIKIDATA_SOURCE_PRODUCT_ID
        or envelope.source_namespace_id != WIKIDATA_NAMESPACE_ID
        or envelope.operation != RecordOperation.UPSERT
        or envelope.payload_json is None
    ):
        raise ValueError("record is not an active Wikidata envelope")
    payload = json.loads(envelope.payload_json)
    if not isinstance(payload, dict) or payload.get("id") != envelope.source_record_id:
        raise ValueError("Wikidata payload identity does not match its envelope")
    entity_type = _wikidata_type(payload)
    identifier_referent_kind = referent_kind_for_entity_type(
        entity_type or "EDITORIAL_WORK"
    )
    from video_media_catalog.assertions import SourceNodeRef, ValueType

    node = SourceNodeRef(
        namespace_id=WIKIDATA_NAMESPACE_ID,
        source_id=envelope.source_record_id,
        referent_kind="EDITORIAL_WORK",
    )
    builder = AssertionBuilder(
        envelope=envelope,
        source_node=node,
        mapper_id="wikidata-source-mapper",
        mapper_version="2.0.0",
    )
    builder.add_identifier(
        WIKIDATA_NAMESPACE_ID,
        envelope.source_record_id,
        "Wikidata",
        identifier_referent_kind,
        "/id",
    )
    if entity_type is not None:
        builder.add_entity_type(entity_type, "/claims/P31")

    labels = payload.get("labels")
    if isinstance(labels, dict):
        for language, value in sorted(labels.items()):
            if isinstance(value, dict) and isinstance(value.get("value"), str):
                builder.add_field(
                    "title",
                    ValueType.STRING,
                    value["value"],
                    f"/labels/{language}/value",
                    {"language": str(language), "titleRole": "PRIMARY"},
                )
    aliases = payload.get("aliases")
    if isinstance(aliases, dict):
        for language, values in sorted(aliases.items()):
            if not isinstance(values, list):
                continue
            for index, value in enumerate(values):
                if isinstance(value, dict) and isinstance(value.get("value"), str):
                    builder.add_field(
                        "title",
                        ValueType.STRING,
                        value["value"],
                        f"/aliases/{language}/{index}/value",
                        {"language": str(language), "titleRole": "ALIAS"},
                    )
    for index, statement in enumerate(statements(payload, "P1476")):
        value = statement_value(statement)
        if isinstance(value, dict) and isinstance(value.get("text"), str):
            builder.add_field(
                "title",
                ValueType.STRING,
                value["text"],
                f"/claims/P1476/{index}/mainsnak/datavalue/value/text",
                {
                    "language": value.get("language") or "und",
                    "titleRole": "TITLE",
                },
            )
    descriptions = payload.get("descriptions")
    if isinstance(descriptions, dict):
        for language, value in sorted(descriptions.items()):
            if isinstance(value, dict) and isinstance(value.get("value"), str):
                builder.add_field(
                    "description",
                    ValueType.STRING,
                    value["value"],
                    f"/descriptions/{language}/value",
                    {"language": str(language)},
                )

    field_specs = {
        "P577": ("release_date", ValueType.DATE, _wikidata_date),
        "P2047": ("runtime_minutes", ValueType.DECIMAL, _quantity),
        "P1113": ("episode_count", ValueType.DECIMAL, _quantity),
        "P2437": ("season_count", ValueType.DECIMAL, _quantity),
    }
    for prop, (predicate, value_type, normalizer) in field_specs.items():
        for index, statement in enumerate(statements(payload, prop)):
            builder.add_field(
                predicate,
                value_type,
                normalizer(statement_value(statement)),
                f"/claims/{prop}/{index}/mainsnak/datavalue/value",
            )
    for prop, predicate in {
        "P364": "language",
        "P495": "country",
        "P136": "genre",
    }.items():
        for index, statement in enumerate(statements(payload, prop)):
            value = statement_value(statement)
            if isinstance(value, str):
                builder.add_field(
                    predicate,
                    ValueType.STRING,
                    value,
                    f"/claims/{prop}/{index}/mainsnak/datavalue/value",
                    {"vocabulary": "wikidata-item"},
                )

    for prop, namespace in EXTERNAL_ID_PROPERTIES.items():
        for index, statement in enumerate(statements(payload, prop)):
            value = statement_value(statement)
            if not isinstance(value, str):
                continue
            douban_spec = DOUBAN_EXTERNAL_ID_PROPERTIES.get(prop)
            try:
                normalized = (
                    normalize_douban_id(value)
                    if douban_spec is not None
                    else (
                        normalize_imdb_id(value)
                        if namespace == "imdb"
                        else normalize_eidr_id(value)
                    )
                )
            except ValueError:
                continue
            if douban_spec is not None:
                target_namespace, referent_kind = douban_spec
            else:
                target_namespace = (
                    _imdb_namespace_id(normalized)
                    if namespace == "imdb"
                    else EIDR_NAMESPACE_ID
                )
                referent_kind = _external_identifier_referent_kind(
                    target_namespace or "",
                    entity_type,
                )
            if target_namespace is None:
                continue
            builder.add_identifier(
                target_namespace,
                normalized,
                {"imdb": "IMDb", "eidr": "EIDR", "douban": "Douban"}[namespace],
                referent_kind,
                f"/claims/{prop}/{index}/mainsnak/datavalue/value",
            )

    for prop, predicate in RELATION_PROPERTIES.items():
        for index, statement in enumerate(statements(payload, prop)):
            target = statement_value(statement)
            if not isinstance(target, str) or _QID.fullmatch(target) is None:
                continue
            builder.add_relationship(
                predicate,
                SourceNodeRef(
                    namespace_id=WIKIDATA_NAMESPACE_ID,
                    source_id=target,
                    referent_kind="EDITORIAL_WORK",
                ),
                f"/claims/{prop}/{index}",
                {
                    "ordinal": _ordinal(statement),
                    "rank": statement.get("rank", "normal"),
                    "statementId": statement.get("id"),
                },
            )
    return builder.build()


def map_eidr_record(envelope: ConnectorRecordEnvelope) -> MappedAssertions:
    """Map one normalized EIDR exact-lookup record."""

    if (
        envelope.source_system_id != EIDR_SOURCE_SYSTEM_ID
        or envelope.source_product_id != EIDR_SOURCE_PRODUCT_ID
        or envelope.source_namespace_id != EIDR_NAMESPACE_ID
        or envelope.operation != RecordOperation.UPSERT
        or envelope.payload_json is None
    ):
        raise ValueError("record is not an active EIDR envelope")
    payload = json.loads(envelope.payload_json)
    if not isinstance(payload, dict) or payload.get("id") != envelope.source_record_id:
        raise ValueError("EIDR payload identity does not match its envelope")
    entity_type = _eidr_entity_type(payload)
    referent_kind = _EIDR_REFERENT_KIND.get(entity_type, "EDITORIAL_WORK")
    from video_media_catalog.assertions import SourceNodeRef, ValueType

    node = SourceNodeRef(
        namespace_id=EIDR_NAMESPACE_ID,
        source_id=envelope.source_record_id,
        referent_kind=referent_kind,
    )
    builder = AssertionBuilder(
        envelope=envelope,
        source_node=node,
        mapper_id="eidr-source-mapper",
        mapper_version="2.0.0",
    )
    builder.add_identifier(
        EIDR_NAMESPACE_ID,
        envelope.source_record_id,
        "EIDR Association",
        referent_kind,
        "/id",
    )
    if entity_type != "UNKNOWN":
        structural_type = str(payload.get("structuralType") or "").strip().lower()
        source_path = (
            "/structuralType"
            if str(payload.get("recordType") or "").upper()
            not in {"EPISODE", "SEASON", "SERIES"}
            and structural_type in {"abstraction", "performance", "digital"}
            else "/recordType"
        )
        builder.add_entity_type(entity_type, source_path)
    for index, title in enumerate(payload.get("titles") or []):
        if not isinstance(title, dict):
            continue
        builder.add_field(
            "title",
            ValueType.STRING,
            title.get("value"),
            f"/titles/{index}/value",
            {
                "language": title.get("language") or "und",
                "titleRole": title.get("type") or "TITLE",
            },
        )
    for predicate, key, value_type in (
        ("release_date", "releaseDate", ValueType.DATE),
        ("referent_type", "referentType", ValueType.STRING),
        ("structural_type", "structuralType", ValueType.STRING),
    ):
        builder.add_field(predicate, value_type, payload.get(key), f"/{key}")
    duration = payload.get("duration")
    is_iso_duration = isinstance(duration, str) and duration.startswith("P")
    builder.add_field(
        "duration" if is_iso_duration else "duration_text",
        ValueType.DURATION if is_iso_duration else ValueType.STRING,
        duration,
        "/duration",
    )
    for key, predicate in (("languages", "language"), ("countries", "country")):
        for index, value in enumerate(payload.get(key) or []):
            builder.add_field(
                predicate,
                ValueType.STRING,
                value,
                f"/{key}/{index}",
            )
    for index, alternate in enumerate(payload.get("alternateIds") or []):
        if not isinstance(alternate, dict):
            continue
        scheme = str(alternate.get("scheme") or "alternate").lower()
        value = alternate.get("value")
        namespace = {
            "imdb": (
                "imdb-name"
                if isinstance(value, str) and value.lower().startswith("nm")
                else "imdb-title"
            ),
            "eidr": EIDR_NAMESPACE_ID,
        }.get(scheme, "eidr-alternate")
        builder.add_identifier(
            namespace,
            value,
            "EIDR Association",
            referent_kind,
            f"/alternateIds/{index}/value",
        )
    for index, relation in enumerate(payload.get("parentRelations") or []):
        if not isinstance(relation, dict) or not relation.get("targetEidrId"):
            continue
        predicate = str(relation.get("type") or "PART_OF")
        target_kind = {
            "PART_OF_SEASON": "SEASON",
            "PART_OF_SERIES": "SERIES",
            "EDIT_OF": "MOVIE",
        }.get(predicate, "EDITORIAL_WORK")
        builder.add_relationship(
            predicate,
            SourceNodeRef(
                namespace_id=EIDR_NAMESPACE_ID,
                source_id=str(relation["targetEidrId"]),
                referent_kind=target_kind,
            ),
            f"/parentRelations/{index}",
            (
                {"ordinal": str(relation["ordinal"])}
                if relation.get("ordinal") is not None
                else {}
            ),
        )
    return builder.build()
