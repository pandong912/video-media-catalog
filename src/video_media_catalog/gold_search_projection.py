"""Bounded, deterministic OpenSearch documents from one Gold release."""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Mapping
from typing import Any

from video_media_catalog.canonical import canonical_json
from video_media_catalog.gold import (
    RESEARCH_CONTEXT_ID,
    GoldAssertionLineage,
    assertion_lineage_from_trace,
)
from video_media_catalog.v2_contracts import require_oidc_subject

DISPLAY_LANGUAGES = ("zh-hans", "zh-hant", "zh", "en", "und")
MAX_TITLES = 64
MAX_IDENTIFIERS = 64
MAX_ATTRIBUTE_VALUES = 128
MAX_RELATION_TYPES = 64
MAX_SOURCE_BADGES = 32
MAX_WINNING_ASSERTIONS = 128
MAX_CITATION_KEYS = 16
MAX_RIGHTS_SUMMARIES = 32
MAX_CONFLICTS = 64
MAX_CONFLICT_ASSERTIONS = 32
MAX_CONFLICT_VALUES = 16

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


def _assertion_ids(value: dict[str, Any]) -> tuple[str, ...]:
    raw = value.get("assertion_ids_json")
    if not isinstance(raw, str):
        raise ValueError("Gold projection requires assertion_ids_json")
    parsed = json.loads(raw)
    if not isinstance(parsed, list) or not all(
        isinstance(item, str) for item in parsed
    ):
        raise ValueError("Gold assertion IDs must be a JSON string array")
    return tuple(sorted(set(parsed)))


def _lineage(value: dict[str, Any]) -> tuple[GoldAssertionLineage, ...]:
    trace = value.get("trace_json")
    if not isinstance(trace, str):
        raise ValueError("Gold projection requires trace_json")
    return assertion_lineage_from_trace(trace)


def project_gold_entity(row: Any, *, owner_subject: str) -> dict[str, Any]:
    value = _dict(row)
    owner = require_oidc_subject(owner_subject)
    titles = []
    attributes: dict[str, set[str]] = {
        name: set() for name in _ATTRIBUTE_PREDICATES.values()
    }
    lineage_by_id: dict[str, GoldAssertionLineage] = {}
    winning: list[dict[str, Any]] = []
    winning_ids: set[str] = set()
    citation_overflow = 0

    def register_lineage(items: tuple[GoldAssertionLineage, ...]) -> None:
        for item in items:
            existing = lineage_by_id.get(item.assertion_id)
            if existing is not None and existing != item:
                raise ValueError("assertion lineage changed within one Gold entity")
            lineage_by_id[item.assertion_id] = item

    for raw in value.get("fields") or []:
        field = _dict(raw)
        field_lineage = _lineage(field)
        register_lineage(field_lineage)
        if field.get("resolution_status") not in {"SELECTED", "SET"}:
            continue
        assertion_ids = _assertion_ids(field)
        selected = field.get("selected_assertion_id")
        winner_ids = (
            (selected,) if field["resolution_status"] == "SELECTED" else assertion_ids
        )
        if not all(isinstance(item, str) for item in winner_ids):
            raise ValueError("selected Gold field has invalid assertion lineage")
        field_lineage_by_id = {item.assertion_id: item for item in field_lineage}
        missing = set(winner_ids) - set(field_lineage_by_id)
        if missing:
            raise ValueError("selected Gold value has incomplete assertion lineage")
        parsed = json.loads(field["value_json"])
        predicate = str(field["predicate"])
        for assertion_id in winner_ids:
            item = field_lineage_by_id[assertion_id]
            citations = list(item.citation_keys)
            citation_overflow += max(0, len(citations) - MAX_CITATION_KEYS)
            winning.append(
                {
                    "kind": "FIELD",
                    "assertionId": assertion_id,
                    "predicate": predicate,
                    "valueJson": field["value_json"],
                    "qualifiersJson": field["qualifiers_json"],
                    "resolutionStatus": field["resolution_status"],
                    "sourceProductId": item.source_product_id,
                    "sourceRecordId": item.source_record_id,
                    "sourcePath": item.source_path,
                    "observedAt": item.observed_at,
                    "citationKeys": citations[:MAX_CITATION_KEYS],
                    "citationOverflow": max(
                        0,
                        len(citations) - MAX_CITATION_KEYS,
                    ),
                }
            )
            winning_ids.add(assertion_id)
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

    raw_identifiers = [_dict(item) for item in (value.get("identifiers") or [])]
    for identifier in raw_identifiers:
        identifier_lineage = _lineage(identifier)
        register_lineage(identifier_lineage)
        assertion_ids = _assertion_ids(identifier)
        identifier_lineage_by_id = {
            item.assertion_id: item for item in identifier_lineage
        }
        if set(assertion_ids) - set(identifier_lineage_by_id):
            raise ValueError("Gold identifier has incomplete assertion lineage")
        identifier_value = canonical_json(
            {
                "issuer": identifier["issuer"],
                "referentKind": identifier["referent_kind"],
                "value": identifier["value"],
            }
        )
        for assertion_id in assertion_ids:
            item = identifier_lineage_by_id[assertion_id]
            citations = list(item.citation_keys)
            citation_overflow += max(0, len(citations) - MAX_CITATION_KEYS)
            winning.append(
                {
                    "kind": "IDENTIFIER",
                    "assertionId": assertion_id,
                    "predicate": str(identifier["namespace_id"]),
                    "valueJson": identifier_value,
                    "qualifiersJson": "{}",
                    "resolutionStatus": "ACCEPTED",
                    "sourceProductId": item.source_product_id,
                    "sourceRecordId": item.source_record_id,
                    "sourcePath": item.source_path,
                    "observedAt": item.observed_at,
                    "citationKeys": citations[:MAX_CITATION_KEYS],
                    "citationOverflow": max(
                        0,
                        len(citations) - MAX_CITATION_KEYS,
                    ),
                }
            )
            winning_ids.add(assertion_id)

    identifiers = sorted(
        {
            (
                str(item["namespace_id"]),
                str(item["value"]),
                str(item["issuer"]),
                str(item["referent_kind"]),
            )
            for item in raw_identifiers
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
    conflict_documents = []
    for raw in value.get("conflicts") or []:
        conflict = _dict(raw)
        conflict_lineage = _lineage(conflict)
        register_lineage(conflict_lineage)
        assertion_ids = _assertion_ids(conflict)
        if set(assertion_ids) - {item.assertion_id for item in conflict_lineage}:
            raise ValueError("Gold conflict has incomplete assertion lineage")
        candidates = json.loads(conflict["candidate_values_json"])
        if not isinstance(candidates, list):
            raise ValueError("Gold conflict candidates must be a list")
        conflict_documents.append(
            {
                "predicate": str(conflict["predicate"]),
                "scopeHash": str(conflict["scope_hash"]),
                "reason": str(conflict["reason"]),
                "assertionIds": list(assertion_ids[:MAX_CONFLICT_ASSERTIONS]),
                "assertionOverflow": max(
                    0,
                    len(assertion_ids) - MAX_CONFLICT_ASSERTIONS,
                ),
                "candidateValuesJson": [
                    canonical_json(item) for item in candidates[:MAX_CONFLICT_VALUES]
                ],
                "candidateValueOverflow": max(
                    0,
                    len(candidates) - MAX_CONFLICT_VALUES,
                ),
                "sourceProductIds": sorted(
                    {item.source_product_id for item in conflict_lineage}
                ),
            }
        )
    conflict_documents.sort(
        key=lambda item: (
            item["predicate"],
            item["scopeHash"],
            item["reason"],
        )
    )
    reported_conflict_count = int(value.get("conflict_count") or 0)
    if reported_conflict_count != len(conflict_documents):
        raise ValueError("Gold conflict count differs from conflict lineage")
    conflict_predicates = sorted({item["predicate"] for item in conflict_documents})

    badge_counts: dict[str, set[str]] = defaultdict(set)
    winning_badge_counts: dict[str, set[str]] = defaultdict(set)
    badge_identity: dict[str, tuple[str, str, set[str]]] = {}
    rights: dict[tuple[str, str], dict[str, Any]] = {}
    for assertion_id, item in lineage_by_id.items():
        product_id = item.source_product_id
        badge_counts[product_id].add(assertion_id)
        if assertion_id in winning_ids:
            winning_badge_counts[product_id].add(assertion_id)
        badge = badge_identity.get(product_id)
        if badge is None:
            badge_identity[product_id] = (
                item.source_name,
                item.rights.source_url,
                {item.rights.policy_zone.value},
            )
        else:
            if badge[:2] != (item.source_name, item.rights.source_url):
                raise ValueError("source badge metadata changed within one entity")
            badge[2].add(item.rights.policy_zone.value)
        rights_key = (product_id, item.rights.policy_id)
        rights_document = {
            "sourceProductId": product_id,
            "policyId": item.rights.policy_id,
            "policyZone": item.rights.policy_zone.value,
            "licenseId": item.rights.license_id,
            "licenseUri": item.rights.license_uri,
            "attributionText": item.rights.attribution_text,
            "sourceUrl": item.rights.source_url,
            "shareAlike": item.rights.share_alike,
        }
        existing_rights = rights.get(rights_key)
        if existing_rights is not None and existing_rights != rights_document:
            raise ValueError("rights metadata changed within one Gold entity")
        rights[rights_key] = rights_document

    source_badges = [
        {
            "sourceProductId": product_id,
            "displayName": badge_identity[product_id][0],
            "sourceUrl": badge_identity[product_id][1],
            "policyZones": sorted(badge_identity[product_id][2]),
            "assertionCount": len(badge_counts[product_id]),
            "winningAssertionCount": len(winning_badge_counts[product_id]),
        }
        for product_id in sorted(badge_identity)
    ]
    rights_documents = [rights[key] for key in sorted(rights)]
    winning.sort(
        key=lambda item: (
            item["kind"],
            item["predicate"],
            item["assertionId"],
        )
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
        "contextId": RESEARCH_CONTEXT_ID,
        "ownerSubject": owner,
        "displayName": display,
        "displayLanguage": display_language,
        "titles": titles,
        "attributes": normalized_attributes,
        "externalIdentifiers": identifier_documents,
        "relationSummary": relation_summary,
        "sourceBadges": source_badges[:MAX_SOURCE_BADGES],
        "winningAssertions": winning[:MAX_WINNING_ASSERTIONS],
        "rights": rights_documents[:MAX_RIGHTS_SUMMARIES],
        "conflictCount": reported_conflict_count,
        "conflictPredicates": conflict_predicates,
        "conflicts": conflict_documents[:MAX_CONFLICTS],
        "sourceNodeCount": int(value.get("source_node_count") or 0),
        "overflow": {
            "titles": title_overflow,
            "externalIdentifiers": identifier_overflow,
            "relationTypes": relation_overflow,
            "sourceBadges": max(0, len(source_badges) - MAX_SOURCE_BADGES),
            "winningAssertions": max(
                0,
                len(winning) - MAX_WINNING_ASSERTIONS,
            ),
            "citationKeys": citation_overflow,
            "rights": max(0, len(rights_documents) - MAX_RIGHTS_SUMMARIES),
            "conflicts": max(0, len(conflict_documents) - MAX_CONFLICTS),
            **attribute_overflow,
        },
    }


def projection_schema():
    from pyspark.sql.types import (
        ArrayType,
        BooleanType,
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
            StructField("contextId", string, False),
            StructField("ownerSubject", string, False),
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
            StructField(
                "sourceBadges",
                ArrayType(
                    StructType(
                        [
                            StructField("sourceProductId", string, False),
                            StructField("displayName", string, False),
                            StructField("sourceUrl", string, False),
                            StructField(
                                "policyZones",
                                ArrayType(string, False),
                                False,
                            ),
                            StructField("assertionCount", LongType(), False),
                            StructField(
                                "winningAssertionCount",
                                LongType(),
                                False,
                            ),
                        ]
                    ),
                    False,
                ),
                False,
            ),
            StructField(
                "winningAssertions",
                ArrayType(
                    StructType(
                        [
                            StructField("kind", string, False),
                            StructField("assertionId", string, False),
                            StructField("predicate", string, False),
                            StructField("valueJson", string, False),
                            StructField("qualifiersJson", string, False),
                            StructField("resolutionStatus", string, False),
                            StructField("sourceProductId", string, False),
                            StructField("sourceRecordId", string, False),
                            StructField("sourcePath", string, False),
                            StructField("observedAt", string, False),
                            StructField(
                                "citationKeys",
                                ArrayType(string, False),
                                False,
                            ),
                            StructField(
                                "citationOverflow",
                                IntegerType(),
                                False,
                            ),
                        ]
                    ),
                    False,
                ),
                False,
            ),
            StructField(
                "rights",
                ArrayType(
                    StructType(
                        [
                            StructField("sourceProductId", string, False),
                            StructField("policyId", string, False),
                            StructField("policyZone", string, False),
                            StructField("licenseId", string, False),
                            StructField("licenseUri", string, True),
                            StructField("attributionText", string, False),
                            StructField("sourceUrl", string, False),
                            StructField(
                                "shareAlike",
                                BooleanType(),
                                False,
                            ),
                        ]
                    ),
                    False,
                ),
                False,
            ),
            StructField("conflictCount", LongType(), False),
            StructField("conflictPredicates", ArrayType(string, False), False),
            StructField(
                "conflicts",
                ArrayType(
                    StructType(
                        [
                            StructField("predicate", string, False),
                            StructField("scopeHash", string, False),
                            StructField("reason", string, False),
                            StructField(
                                "assertionIds",
                                ArrayType(string, False),
                                False,
                            ),
                            StructField(
                                "assertionOverflow",
                                IntegerType(),
                                False,
                            ),
                            StructField(
                                "candidateValuesJson",
                                ArrayType(string, False),
                                False,
                            ),
                            StructField(
                                "candidateValueOverflow",
                                IntegerType(),
                                False,
                            ),
                            StructField(
                                "sourceProductIds",
                                ArrayType(string, False),
                                False,
                            ),
                        ]
                    ),
                    False,
                ),
                False,
            ),
            StructField("sourceNodeCount", LongType(), False),
            StructField(
                "overflow",
                StructType(
                    [
                        StructField("titles", IntegerType(), False),
                        StructField("externalIdentifiers", IntegerType(), False),
                        StructField("relationTypes", IntegerType(), False),
                        StructField("sourceBadges", IntegerType(), False),
                        StructField("winningAssertions", IntegerType(), False),
                        StructField("citationKeys", IntegerType(), False),
                        StructField("rights", IntegerType(), False),
                        StructField("conflicts", IntegerType(), False),
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
    owner_subject: str,
):
    from pyspark.sql import functions as F

    owner = require_oidc_subject(owner_subject)
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
                        "resolution_key",
                        "predicate",
                        "value_json",
                        "qualifiers_json",
                        "resolution_status",
                        "selected_assertion_id",
                        "assertion_ids_json",
                        "trace_json",
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
                        "assertion_ids_json",
                        "trace_json",
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
            F.sort_array(
                F.collect_set(
                    F.struct(
                        "predicate",
                        "scope_hash",
                        "reason",
                        "assertion_ids_json",
                        "candidate_values_json",
                        "trace_json",
                    )
                )
            ).alias("conflicts"),
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
        joined.rdd.map(lambda row: project_gold_entity(row, owner_subject=owner)),
        schema=projection_schema(),
    )
