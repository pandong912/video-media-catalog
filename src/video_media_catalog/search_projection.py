"""Deterministic, rebuildable search documents derived from curated tables."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from typing import Any

from video_media_catalog.canonical import canonical_json

DISPLAY_LANGUAGE_ORDER = ("zh-hans", "zh", "en", "mul")
PARENT_RELATION_TYPES = frozenset(
    {"EDIT_OF", "PART_OF", "PART_OF_SEASON", "PART_OF_SERIES", "SEASON"}
)

_NAME_TYPE_PRIORITY = {
    "PRIMARY": 0,
    "TITLE": 1,
    "SHORT": 2,
    "ALIAS": 99,
}
_CORE_ATTRIBUTE_KEYS = (
    "releaseDates",
    "durations",
    "languages",
    "countries",
    "genres",
    "episodeCounts",
    "seasonCounts",
    "modified",
)


def _as_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return dict(value)
    if hasattr(value, "asDict"):
        return dict(value.asDict(recursive=True))
    raise TypeError(f"expected a row-like value, got {type(value).__name__}")


def _field(value: Mapping[str, Any], snake: str, camel: str) -> Any:
    return value.get(snake, value.get(camel))


def _language(value: Any) -> str:
    normalized = str(value or "und").strip().lower().replace("_", "-")
    return normalized or "und"


def _language_sort_key(language: str) -> tuple[int, str]:
    try:
        return DISPLAY_LANGUAGE_ORDER.index(language), ""
    except ValueError:
        return len(DISPLAY_LANGUAGE_ORDER), language


def _name_sort_key(name: Mapping[str, Any]) -> tuple[Any, ...]:
    language = _language(_field(name, "language", "language"))
    name_type = str(_field(name, "name_type", "nameType") or "OTHER").upper()
    value = str(_field(name, "value", "value") or "")
    return (
        *_language_sort_key(language),
        _NAME_TYPE_PRIORITY.get(name_type, 50),
        name_type,
        value.casefold(),
        value,
        str(_field(name, "source", "source") or ""),
        str(_field(name, "source_record_id", "sourceRecordId") or ""),
    )


def select_display_name(
    names: Iterable[Mapping[str, Any] | Any],
    *,
    fallback: str,
) -> tuple[str, str]:
    """Select one display name using the fixed language fallback policy."""

    candidates = []
    for raw_name in names:
        name = _as_dict(raw_name)
        value = str(_field(name, "value", "value") or "")
        if value.strip():
            candidates.append(name)
    if not candidates:
        return fallback, "und"
    selected = min(candidates, key=_name_sort_key)
    return (
        str(_field(selected, "value", "value")),
        _language(_field(selected, "language", "language")),
    )


def _stable_unique(
    values: Iterable[dict[str, Any]],
    *,
    fields: tuple[str, ...],
) -> list[dict[str, Any]]:
    unique = {
        tuple(
            "" if value.get(field) is None else str(value.get(field))
            for field in fields
        ): value
        for value in values
    }
    return [unique[key] for key in sorted(unique)]


def _normalize_names(values: Iterable[Any]) -> list[dict[str, str]]:
    names = []
    for raw in values:
        value = _as_dict(raw)
        text = str(_field(value, "value", "value") or "")
        if not text:
            continue
        names.append(
            {
                "nameType": str(
                    _field(value, "name_type", "nameType") or "OTHER"
                ).upper(),
                "language": _language(_field(value, "language", "language")),
                "value": text,
                "source": str(_field(value, "source", "source") or ""),
                "sourceRecordId": str(
                    _field(value, "source_record_id", "sourceRecordId") or ""
                ),
            }
        )
    return _stable_unique(
        names,
        fields=("language", "nameType", "value", "source", "sourceRecordId"),
    )


def _normalize_identifiers(values: Iterable[Any]) -> list[dict[str, str]]:
    identifiers = []
    for raw in values:
        value = _as_dict(raw)
        scheme = str(_field(value, "scheme", "scheme") or "").lower()
        identifier = str(_field(value, "value", "value") or "")
        if not scheme or not identifier:
            continue
        identifiers.append(
            {
                "scheme": scheme,
                "value": identifier,
                "source": str(_field(value, "source", "source") or ""),
                "sourceRecordId": str(
                    _field(value, "source_record_id", "sourceRecordId") or ""
                ),
            }
        )
    return _stable_unique(
        identifiers,
        fields=("scheme", "value", "source", "sourceRecordId"),
    )


def _normalize_relations(
    values: Iterable[Any],
) -> tuple[list[dict[str, str | None]], list[dict[str, Any]], list[str]]:
    relations = []
    for raw in values:
        value = _as_dict(raw)
        relation_type = str(
            _field(value, "relation_type", "relationType") or ""
        ).upper()
        object_key = str(_field(value, "object_entity_key", "objectEntityKey") or "")
        if not relation_type or not object_key:
            continue
        ordinal = _field(value, "ordinal", "ordinal")
        relations.append(
            {
                "relationKey": str(_field(value, "relation_key", "relationKey") or ""),
                "relationType": relation_type,
                "objectEntityKey": object_key,
                "ordinal": None if ordinal is None else str(ordinal),
                "source": str(_field(value, "source", "source") or ""),
                "sourceRecordId": str(
                    _field(value, "source_record_id", "sourceRecordId") or ""
                ),
            }
        )
    stable_relations = _stable_unique(
        relations,
        fields=(
            "relationType",
            "objectEntityKey",
            "ordinal",
            "source",
            "sourceRecordId",
            "relationKey",
        ),
    )
    counts: dict[str, int] = {}
    for relation in stable_relations:
        relation_type = str(relation["relationType"])
        counts[relation_type] = counts.get(relation_type, 0) + 1
    summary = [
        {"relationType": relation_type, "count": count}
        for relation_type, count in sorted(counts.items())
    ]
    parents = sorted(
        {
            str(relation["objectEntityKey"])
            for relation in stable_relations
            if relation["relationType"] in PARENT_RELATION_TYPES
        }
    )
    return stable_relations, summary, parents


def _attribute_string(value: Any) -> str:
    return value if isinstance(value, str) else canonical_json(value)


def _attribute_values(value: Any) -> list[str]:
    if value is None:
        return []
    values = value if isinstance(value, list) else [value]
    return sorted({_attribute_string(item) for item in values if item is not None})


def _attribute_contributions(attributes_json: str) -> list[dict[str, Any]]:
    value = json.loads(attributes_json or "{}")
    if not isinstance(value, dict):
        raise ValueError("entity attributes_json must contain a JSON object")
    sources = value.get("sources")
    if isinstance(sources, list):
        result = []
        for raw in sources:
            contribution = _as_dict(raw)
            payload = contribution.get("attributes_json")
            if payload is None:
                payload = contribution.get("attributesJson")
            parsed = json.loads(payload) if isinstance(payload, str) else payload
            if not isinstance(parsed, dict):
                continue
            result.append(
                {
                    "source": str(contribution.get("source") or ""),
                    "attributes": parsed,
                }
            )
        return result

    result = []
    wikidata = value.get("wikidata")
    if isinstance(wikidata, dict):
        result.append({"source": "wikidata", "attributes": wikidata})
    eidr = value.get("eidr")
    for item in eidr if isinstance(eidr, list) else []:
        if isinstance(item, dict):
            result.append({"source": "eidr", "attributes": item})
    return result


def normalize_attributes(
    attributes_json: str,
) -> tuple[list[dict[str, str]], list[dict[str, Any]], dict[str, list[str]]]:
    """Extract fixed searchable attributes from the versioned fact payload."""

    descriptions: list[dict[str, str]] = []
    sitelinks: list[dict[str, Any]] = []
    core: dict[str, set[str]] = {key: set() for key in _CORE_ATTRIBUTE_KEYS}
    for contribution in _attribute_contributions(attributes_json):
        attributes = contribution["attributes"]
        raw_descriptions = attributes.get("descriptions")
        if isinstance(raw_descriptions, dict):
            for language, raw in raw_descriptions.items():
                description = _as_dict(raw) if not isinstance(raw, str) else {}
                text = raw if isinstance(raw, str) else description.get("value")
                if isinstance(text, str) and text:
                    descriptions.append(
                        {
                            "language": _language(
                                description.get("language") or language
                            ),
                            "value": text,
                        }
                    )
        raw_sitelinks = attributes.get("sitelinks")
        if isinstance(raw_sitelinks, dict):
            for site, raw in raw_sitelinks.items():
                sitelink = _as_dict(raw)
                title = sitelink.get("title")
                if not isinstance(title, str) or not title:
                    continue
                badges = sitelink.get("badges")
                sitelinks.append(
                    {
                        "site": str(site),
                        "title": title,
                        "url": (
                            str(sitelink["url"])
                            if isinstance(sitelink.get("url"), str)
                            else None
                        ),
                        "badges": sorted(
                            str(item) for item in badges if isinstance(item, str)
                        )
                        if isinstance(badges, list)
                        else [],
                    }
                )
        for key in _CORE_ATTRIBUTE_KEYS:
            core[key].update(_attribute_values(attributes.get(key)))
        if contribution["source"] == "eidr":
            core["releaseDates"].update(
                _attribute_values(attributes.get("releaseDate"))
            )
            core["durations"].update(_attribute_values(attributes.get("duration")))

    return (
        _stable_unique(descriptions, fields=("language", "value")),
        _stable_unique(sitelinks, fields=("site", "title", "url", "badges")),
        {key: sorted(values) for key, values in core.items()},
    )


def project_entity(row: Mapping[str, Any] | Any) -> dict[str, Any]:
    """Convert one joined curated entity row into a stable search document."""

    value = _as_dict(row)
    names = _normalize_names(value.get("names") or [])
    canonical_id = str(_field(value, "canonical_source_id", "canonicalSourceId") or "")
    display_name, display_language = select_display_name(
        names,
        fallback=canonical_id,
    )
    identifiers = _normalize_identifiers(
        value.get("external_identifiers") or value.get("externalIdentifiers") or []
    )
    relations, relation_summary, parent_keys = _normalize_relations(
        value.get("relations") or []
    )
    descriptions, sitelinks, attributes = normalize_attributes(
        str(_field(value, "attributes_json", "attributesJson") or "{}")
    )
    source_records = []
    for raw in value.get("source_records") or value.get("sourceRecords") or []:
        record = _as_dict(raw)
        source_record_id = str(
            _field(record, "source_record_id", "sourceRecordId") or ""
        )
        source = str(_field(record, "source", "source") or "")
        record_key = str(_field(record, "record_key", "recordKey") or "")
        if source_record_id and source:
            source_records.append(
                {
                    "recordKey": record_key,
                    "source": source,
                    "sourceRecordId": source_record_id,
                }
            )
    source_records = _stable_unique(
        source_records,
        fields=("source", "sourceRecordId", "recordKey"),
    )
    return {
        "entityKey": str(_field(value, "entity_key", "entityKey") or ""),
        "entityType": str(_field(value, "entity_type", "entityType") or "UNKNOWN"),
        "canonicalSource": str(
            _field(value, "canonical_source", "canonicalSource") or ""
        ),
        "canonicalSourceId": canonical_id,
        "displayName": display_name,
        "displayLanguage": display_language,
        "names": names,
        "descriptions": descriptions,
        "sitelinks": sitelinks,
        "attributes": attributes,
        "externalIdentifiers": identifiers,
        "relations": relations,
        "relationSummary": relation_summary,
        "parentKeys": parent_keys,
        "sourceRecordIds": sorted(
            {str(record["sourceRecordId"]) for record in source_records}
        ),
        "sourceRecords": source_records,
    }


def projection_schema() -> Any:
    """Return the explicit Spark schema for projected documents."""

    from pyspark.sql.types import (
        ArrayType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    string = StringType()
    return StructType(
        [
            StructField("entityKey", string, False),
            StructField("entityType", string, False),
            StructField("canonicalSource", string, False),
            StructField("canonicalSourceId", string, False),
            StructField("displayName", string, False),
            StructField("displayLanguage", string, False),
            StructField(
                "names",
                ArrayType(
                    StructType(
                        [
                            StructField("nameType", string, False),
                            StructField("language", string, False),
                            StructField("value", string, False),
                            StructField("source", string, False),
                            StructField("sourceRecordId", string, False),
                        ]
                    ),
                    False,
                ),
                False,
            ),
            StructField(
                "descriptions",
                ArrayType(
                    StructType(
                        [
                            StructField("language", string, False),
                            StructField("value", string, False),
                        ]
                    ),
                    False,
                ),
                False,
            ),
            StructField(
                "sitelinks",
                ArrayType(
                    StructType(
                        [
                            StructField("site", string, False),
                            StructField("title", string, False),
                            StructField("url", string, True),
                            StructField("badges", ArrayType(string, False), False),
                        ]
                    ),
                    False,
                ),
                False,
            ),
            StructField(
                "attributes",
                StructType(
                    [
                        StructField(key, ArrayType(string, False), False)
                        for key in _CORE_ATTRIBUTE_KEYS
                    ]
                ),
                False,
            ),
            StructField(
                "externalIdentifiers",
                ArrayType(
                    StructType(
                        [
                            StructField("scheme", string, False),
                            StructField("value", string, False),
                            StructField("source", string, False),
                            StructField("sourceRecordId", string, False),
                        ]
                    ),
                    False,
                ),
                False,
            ),
            StructField(
                "relations",
                ArrayType(
                    StructType(
                        [
                            StructField("relationKey", string, False),
                            StructField("relationType", string, False),
                            StructField("objectEntityKey", string, False),
                            StructField("ordinal", string, True),
                            StructField("source", string, False),
                            StructField("sourceRecordId", string, False),
                        ]
                    ),
                    False,
                ),
                False,
            ),
            StructField(
                "relationSummary",
                ArrayType(
                    StructType(
                        [
                            StructField("relationType", string, False),
                            StructField("count", IntegerType(), False),
                        ]
                    ),
                    False,
                ),
                False,
            ),
            StructField("parentKeys", ArrayType(string, False), False),
            StructField("sourceRecordIds", ArrayType(string, False), False),
            StructField(
                "sourceRecords",
                ArrayType(
                    StructType(
                        [
                            StructField("recordKey", string, False),
                            StructField("source", string, False),
                            StructField("sourceRecordId", string, False),
                        ]
                    ),
                    False,
                ),
                False,
            ),
        ]
    )


def build_projection(spark: Any, tables: Mapping[str, Any]) -> Any:
    """Build one distributed projection row per curated entity."""

    from pyspark.sql import functions as F

    required = {
        "catalog_source_record",
        "catalog_entity",
        "catalog_name",
        "catalog_external_identifier",
        "catalog_relation",
        "catalog_ingest_error",
    }
    if set(tables) != required:
        raise ValueError("all six curated table dataframes are required")

    names = (
        tables["catalog_name"]
        .groupBy("entity_key")
        .agg(
            F.sort_array(
                F.collect_set(
                    F.struct(
                        "name_type",
                        "language",
                        "value",
                        "source",
                        "source_record_id",
                    )
                )
            ).alias("names")
        )
    )
    identifiers = (
        tables["catalog_external_identifier"]
        .groupBy("entity_key")
        .agg(
            F.sort_array(
                F.collect_set(F.struct("scheme", "value", "source", "source_record_id"))
            ).alias("external_identifiers")
        )
    )
    relations = (
        tables["catalog_relation"]
        .groupBy("subject_entity_key")
        .agg(
            F.sort_array(
                F.collect_set(
                    F.struct(
                        "relation_key",
                        "relation_type",
                        "object_entity_key",
                        "ordinal",
                        "source",
                        "source_record_id",
                    )
                )
            ).alias("relations")
        )
    )
    source_records = (
        tables["catalog_source_record"]
        .groupBy("entity_key")
        .agg(
            F.sort_array(
                F.collect_set(F.struct("record_key", "source", "source_record_id"))
            ).alias("source_records")
        )
    )
    joined = (
        tables["catalog_entity"]
        .join(names, "entity_key", "left")
        .join(identifiers, "entity_key", "left")
        .join(
            relations,
            F.col("entity_key") == relations["subject_entity_key"],
            "left",
        )
        .drop("subject_entity_key")
        .join(source_records, "entity_key", "left")
    )
    return spark.createDataFrame(
        joined.rdd.map(project_entity),
        schema=projection_schema(),
    )
