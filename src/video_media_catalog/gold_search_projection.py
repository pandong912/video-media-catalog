"""Bounded, deterministic OpenSearch documents from one Gold release."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

DISPLAY_LANGUAGES = ("zh-hans", "zh-hant", "zh", "en", "und")
MAX_TITLES = 64
MAX_IDENTIFIERS = 64
MAX_ATTRIBUTE_VALUES = 128
MAX_RELATION_TYPES = 64

_ATTRIBUTE_PREDICATES = {
    "format": "formats",
    "language": "languages",
    "status": "statuses",
    "premiered": "premiered",
    "ended": "ended",
    "runtime_minutes": "runtimeMinutes",
    "average_runtime_minutes": "averageRuntimeMinutes",
    "genre": "genres",
}


def _dict(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if hasattr(value, "asDict"):
        return dict(value.asDict(recursive=True))
    raise TypeError(f"expected row-like value, got {type(value).__name__}")


def _title_sort(value: dict[str, Any]) -> tuple[Any, ...]:
    language = str(value.get("language") or "und").lower().replace("_", "-")
    try:
        language_rank = DISPLAY_LANGUAGES.index(language)
    except ValueError:
        language_rank = len(DISPLAY_LANGUAGES)
    role = str(value.get("titleRole") or "OTHER").upper()
    role_rank = {"PRIMARY": 0, "TITLE": 1, "ALIAS": 10}.get(role, 5)
    text = str(value["value"])
    return language_rank, language, role_rank, text.casefold(), text


def project_gold_entity(row: Any) -> dict[str, Any]:
    value = _dict(row)
    titles = []
    attributes: dict[str, set[str]] = {
        name: set() for name in _ATTRIBUTE_PREDICATES.values()
    }
    for raw in value.get("fields") or []:
        field = _dict(raw)
        if field.get("resolution_status") not in {"SELECTED", "SET"}:
            continue
        parsed = json.loads(field["value_json"])
        predicate = str(field["predicate"])
        if predicate == "title" and isinstance(parsed, str):
            qualifiers = json.loads(field["qualifiers_json"])
            titles.append(
                {
                    "value": parsed,
                    "language": str(qualifiers.get("language") or "und"),
                    "titleRole": str(qualifiers.get("titleRole") or "OTHER").upper(),
                }
            )
        attribute = _ATTRIBUTE_PREDICATES.get(predicate)
        if attribute is not None and parsed is not None:
            attributes[attribute].add(str(parsed))
    titles = sorted(
        {
            (item["value"], item["language"], item["titleRole"]): item
            for item in titles
        }.values(),
        key=_title_sort,
    )
    title_overflow = max(0, len(titles) - MAX_TITLES)
    titles = titles[:MAX_TITLES]
    canonical_id = str(value["entity_key"])
    display = titles[0]["value"] if titles else canonical_id
    display_language = titles[0]["language"] if titles else "und"

    identifiers = sorted(
        {
            (
                str(_dict(item)["namespace_id"]),
                str(_dict(item)["value"]),
                str(_dict(item)["issuer"]),
                str(_dict(item)["referent_kind"]),
            )
            for item in (value.get("identifiers") or [])
        }
    )
    identifier_overflow = max(0, len(identifiers) - MAX_IDENTIFIERS)
    identifier_documents = [
        {
            "namespace": namespace,
            "value": identifier,
            "issuer": issuer,
            "referentKind": referent_kind,
        }
        for namespace, identifier, issuer, referent_kind in identifiers[
            :MAX_IDENTIFIERS
        ]
    ]
    relation_summary = sorted(
        (
            {
                "predicate": str(_dict(item)["predicate"]),
                "count": int(_dict(item)["count"]),
            }
            for item in (value.get("relation_summary") or [])
        ),
        key=lambda item: item["predicate"],
    )
    relation_overflow = max(0, len(relation_summary) - MAX_RELATION_TYPES)
    relation_summary = relation_summary[:MAX_RELATION_TYPES]
    conflict_predicates = sorted(
        {str(_dict(item)["predicate"]) for item in (value.get("conflicts") or [])}
    )
    normalized_attributes = {}
    attribute_overflow = {}
    for name, items in attributes.items():
        values = sorted(items)
        attribute_overflow[name] = max(0, len(values) - MAX_ATTRIBUTE_VALUES)
        normalized_attributes[name] = values[:MAX_ATTRIBUTE_VALUES]
    return {
        "entityKey": canonical_id,
        "entityLevel": str(value["entity_level"]),
        "entityKind": str(value["entity_kind"]),
        "status": str(value["status"]),
        "releasePlanId": str(value["release_plan_id"]),
        "displayName": display,
        "displayLanguage": display_language,
        "titles": titles,
        "attributes": normalized_attributes,
        "externalIdentifiers": identifier_documents,
        "relationSummary": relation_summary,
        "conflictCount": int(value.get("conflict_count") or 0),
        "conflictPredicates": conflict_predicates,
        "sourceNodeCount": int(value.get("source_node_count") or 0),
        "overflow": {
            "titles": title_overflow,
            "externalIdentifiers": identifier_overflow,
            "relationTypes": relation_overflow,
            **attribute_overflow,
        },
    }


def projection_schema():
    from pyspark.sql.types import (
        ArrayType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    string = StringType()
    return StructType(
        [
            StructField("entityKey", string, False),
            StructField("entityLevel", string, False),
            StructField("entityKind", string, False),
            StructField("status", string, False),
            StructField("releasePlanId", string, False),
            StructField("displayName", string, False),
            StructField("displayLanguage", string, False),
            StructField(
                "titles",
                ArrayType(
                    StructType(
                        [
                            StructField("value", string, False),
                            StructField("language", string, False),
                            StructField("titleRole", string, False),
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
                        StructField(
                            name,
                            ArrayType(string, False),
                            False,
                        )
                        for name in _ATTRIBUTE_PREDICATES.values()
                    ]
                ),
                False,
            ),
            StructField(
                "externalIdentifiers",
                ArrayType(
                    StructType(
                        [
                            StructField("namespace", string, False),
                            StructField("value", string, False),
                            StructField("issuer", string, False),
                            StructField("referentKind", string, False),
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
                            StructField("predicate", string, False),
                            StructField("count", LongType(), False),
                        ]
                    ),
                    False,
                ),
                False,
            ),
            StructField("conflictCount", LongType(), False),
            StructField("conflictPredicates", ArrayType(string, False), False),
            StructField("sourceNodeCount", LongType(), False),
            StructField(
                "overflow",
                StructType(
                    [
                        StructField("titles", IntegerType(), False),
                        StructField("externalIdentifiers", IntegerType(), False),
                        StructField("relationTypes", IntegerType(), False),
                        *[
                            StructField(name, IntegerType(), False)
                            for name in _ATTRIBUTE_PREDICATES.values()
                        ],
                    ]
                ),
                False,
            ),
        ]
    )


def build_gold_search_projection(
    spark: Any,
    *,
    gold_tables: Mapping[str, Any],
    release_plan_id: str,
):
    from pyspark.sql import functions as F

    entities = gold_tables["community_gold_entity"].where(
        F.col("release_plan_id") == release_plan_id
    )
    fields = (
        gold_tables["community_gold_field"]
        .where(F.col("release_plan_id") == release_plan_id)
        .groupBy("entity_key")
        .agg(
            F.sort_array(
                F.collect_set(
                    F.struct(
                        "predicate",
                        "value_json",
                        "qualifiers_json",
                        "resolution_status",
                    )
                )
            ).alias("fields")
        )
    )
    identifiers = (
        gold_tables["community_gold_identifier"]
        .where(F.col("release_plan_id") == release_plan_id)
        .groupBy("entity_key")
        .agg(
            F.sort_array(
                F.collect_set(
                    F.struct(
                        "namespace_id",
                        "value",
                        "issuer",
                        "referent_kind",
                    )
                )
            ).alias("identifiers")
        )
    )
    relation_summary = (
        gold_tables["community_gold_relation"]
        .where(F.col("release_plan_id") == release_plan_id)
        .groupBy("subject_entity_key", "predicate")
        .count()
        .groupBy("subject_entity_key")
        .agg(
            F.sort_array(F.collect_set(F.struct("predicate", "count"))).alias(
                "relation_summary"
            )
        )
    )
    conflicts = (
        gold_tables["community_gold_conflict"]
        .where(F.col("release_plan_id") == release_plan_id)
        .groupBy("entity_key")
        .agg(
            F.count(F.lit(1)).alias("conflict_count"),
            F.sort_array(F.collect_set(F.struct("predicate"))).alias("conflicts"),
        )
    )
    joined = (
        entities.join(fields, "entity_key", "left")
        .join(identifiers, "entity_key", "left")
        .join(
            relation_summary,
            entities.entity_key == relation_summary.subject_entity_key,
            "left",
        )
        .drop("subject_entity_key")
        .join(conflicts, "entity_key", "left")
    )
    return spark.createDataFrame(
        joined.rdd.map(project_gold_entity),
        schema=projection_schema(),
    )
