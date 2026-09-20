"""Distributed Spark normalization for the curated media catalog."""

from __future__ import annotations

import json
from typing import Any

from video_media_catalog.canonical import (
    canonical_json,
    deterministic_key,
    source_hash,
)
from video_media_catalog.constants import (
    CREDIT_ORGANIZATION_PROPERTIES,
    CREDIT_PERSON_PROPERTIES,
    ENTITY_TYPE_SEEDS,
    MEDIA_ENTITY_TYPES,
    RELATION_PROPERTIES,
)
from video_media_catalog.eidr import normalize_eidr_id
from video_media_catalog.transform import (
    _eidr_type,
    _external_ids,
    _external_value,
    _qid_values,
    _qualifier_ordinal,
    _statement_value,
    _statements,
    _wikidata_attributes,
)

_TYPE_PRIORITY = {
    "TV_EPISODE": 0,
    "TV_SEASON": 1,
    "TV_SERIES": 2,
    "MOVIE": 3,
    "PERSON": 4,
    "ORGANIZATION": 5,
    "UNKNOWN": 99,
}


def _normalize_row(row: Any) -> dict[str, Any]:
    base = {
        "record_key": row["record_key"],
        "source": row["source"],
        "source_record_id": row["source_record_id"],
        "source_revision": row["source_revision"],
        "modified": row["modified"],
        "source_hash": row["source_hash"],
        "payload_json": row["payload_json"],
        "parse_error": None,
        "direct_types": [],
        "subclass_parents": [],
        "entity_type_hint": "UNKNOWN",
        "names": [],
        "external_ids": [],
        "relations": [],
        "attributes_json": "{}",
    }
    try:
        payload = json.loads(row["payload_json"])
        if not isinstance(payload, dict):
            raise ValueError("payload must be a JSON object")
        actual_hash = source_hash(payload)
        if actual_hash != row["source_hash"]:
            raise ValueError(
                f"source hash mismatch: expected {row['source_hash']}, "
                f"computed {actual_hash}"
            )
        expected_key = deterministic_key(
            "source-record",
            {
                "source": row["source"],
                "sourceRecordId": row["source_record_id"],
                "sourceRevision": row["source_revision"],
                "sourceHash": row["source_hash"],
            },
        )
        if expected_key != row["record_key"]:
            raise ValueError(
                f"record key mismatch: expected {expected_key}, got {row['record_key']}"
            )
        if row["source"] == "wikidata":
            base.update(_normalize_wikidata(payload))
        elif row["source"] == "eidr":
            base.update(_normalize_eidr(payload))
        else:
            raise ValueError(f"unsupported source: {row['source']!r}")
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        base["parse_error"] = str(exc)
    return base


def _normalize_wikidata(payload: dict[str, Any]) -> dict[str, Any]:
    qid = str(payload["id"])
    names: list[dict[str, str]] = []
    labels = payload.get("labels")
    if isinstance(labels, dict):
        for language, label in labels.items():
            if isinstance(label, dict) and isinstance(label.get("value"), str):
                names.append(
                    {
                        "name_type": "PRIMARY",
                        "language": str(label.get("language") or language),
                        "value": label["value"],
                    }
                )
    aliases = payload.get("aliases")
    if isinstance(aliases, dict):
        for language, values in aliases.items():
            if not isinstance(values, list):
                continue
            for alias in values:
                if isinstance(alias, dict) and isinstance(alias.get("value"), str):
                    names.append(
                        {
                            "name_type": "ALIAS",
                            "language": str(alias.get("language") or language),
                            "value": alias["value"],
                        }
                    )
    for statement in _statements(payload, "P1476"):
        value = _statement_value(statement)
        if isinstance(value, dict) and isinstance(value.get("text"), str):
            names.append(
                {
                    "name_type": "TITLE",
                    "language": str(value.get("language") or "und"),
                    "value": value["text"],
                }
            )
    external_ids = [
        {"scheme": "wikidata", "value": qid},
        *[
            {"scheme": scheme, "value": value}
            for scheme, value in _external_ids(payload)
        ],
    ]
    relations: list[dict[str, str | None]] = []
    for prop, relation_type in RELATION_PROPERTIES.items():
        for statement in _statements(payload, prop):
            target = _statement_value(statement)
            if not isinstance(target, str) or not target.startswith("Q"):
                continue
            hint = (
                "PERSON"
                if prop in CREDIT_PERSON_PROPERTIES
                else (
                    "ORGANIZATION"
                    if prop in CREDIT_ORGANIZATION_PROPERTIES
                    else "UNKNOWN"
                )
            )
            relations.append(
                {
                    "relation_type": relation_type,
                    "target_source": "wikidata",
                    "target_id": target,
                    "ordinal": _qualifier_ordinal(statement),
                    "target_type_hint": hint,
                    "attributes_json": canonical_json(
                        {
                            "property": prop,
                            "rank": statement.get("rank", "normal"),
                            "statementId": statement.get("id"),
                            "qualifiers": statement.get("qualifiers") or {},
                        }
                    ),
                }
            )
    return {
        "direct_types": sorted(set(_qid_values(payload, "P31"))),
        "subclass_parents": sorted(set(_qid_values(payload, "P279"))),
        "names": sorted(
            names,
            key=lambda value: (
                value["name_type"],
                value["language"],
                value["value"],
            ),
        ),
        "external_ids": sorted(
            external_ids, key=lambda value: (value["scheme"], value["value"])
        ),
        "relations": relations,
        "attributes_json": canonical_json(_wikidata_attributes(payload)),
    }


def _normalize_eidr(payload: dict[str, Any]) -> dict[str, Any]:
    eidr_id = normalize_eidr_id(str(payload["id"]))
    names = [
        {
            "name_type": str(title.get("type") or "TITLE"),
            "language": str(title.get("language") or "und"),
            "value": str(title["value"]),
        }
        for title in payload.get("titles", [])
        if isinstance(title, dict) and title.get("value")
    ]
    external_ids = [{"scheme": "eidr", "value": eidr_id}]
    for alternate in payload.get("alternateIds", []):
        if not isinstance(alternate, dict):
            continue
        scheme = str(alternate.get("scheme") or "alternate").lower()
        value = _external_value(scheme, alternate.get("value"))
        if value:
            external_ids.append({"scheme": scheme, "value": value})
    relations = [
        {
            "relation_type": str(relation.get("type") or "PART_OF"),
            "target_source": "eidr",
            "target_id": normalize_eidr_id(str(relation["targetEidrId"])),
            "ordinal": (
                str(relation["ordinal"])
                if relation.get("ordinal") is not None
                else None
            ),
            "target_type_hint": "UNKNOWN",
            "attributes_json": canonical_json(
                {"targetEidrId": relation["targetEidrId"]}
            ),
        }
        for relation in payload.get("parentRelations", [])
        if isinstance(relation, dict) and relation.get("targetEidrId")
    ]
    attributes = {
        key: payload.get(key)
        for key in (
            "id",
            "referentType",
            "recordType",
            "languages",
            "releaseDate",
            "duration",
            "countries",
            "modified",
        )
    }
    return {
        "entity_type_hint": _eidr_type(payload),
        "names": sorted(
            names,
            key=lambda value: (
                value["name_type"],
                value["language"],
                value["value"],
            ),
        ),
        "external_ids": sorted(
            external_ids, key=lambda value: (value["scheme"], value["value"])
        ),
        "relations": relations,
        "attributes_json": canonical_json(attributes),
    }


def _entity_key(source: str, source_id: str) -> str:
    return deterministic_key(
        "catalog-entity", {"canonicalSource": source, "canonicalSourceId": source_id}
    )


def _name_key(
    entity_key: str,
    name_type: str,
    language: str,
    value: str,
    source: str,
    source_record_id: str,
) -> str:
    return deterministic_key(
        "catalog-name",
        {
            "entityKey": entity_key,
            "nameType": name_type,
            "language": language,
            "value": value,
            "source": source,
            "sourceRecordId": source_record_id,
        },
    )


def _identifier_key(
    entity_key: str,
    scheme: str,
    value: str,
    source: str,
    source_record_id: str,
) -> str:
    return deterministic_key(
        "catalog-external-identifier",
        {
            "entityKey": entity_key,
            "scheme": scheme,
            "value": value,
            "source": source,
            "sourceRecordId": source_record_id,
        },
    )


def _relation_key(
    subject_key: str,
    relation_type: str,
    object_key: str,
    ordinal: str | None,
    source: str,
    source_record_id: str,
    attributes_json: str,
) -> str:
    return deterministic_key(
        "catalog-relation",
        {
            "subjectEntityKey": subject_key,
            "relationType": relation_type,
            "objectEntityKey": object_key,
            "ordinal": ordinal,
            "source": source,
            "sourceRecordId": source_record_id,
            "attributes": json.loads(attributes_json),
        },
    )


def _error_key(
    source: str, source_record_id: str, error_code: str, details_json: str
) -> str:
    return deterministic_key(
        "catalog-ingest-error",
        {
            "source": source,
            "sourceRecordId": source_record_id,
            "errorCode": error_code,
            "details": json.loads(details_json),
        },
    )


def _aggregate_attributes(contributions_json: str | None) -> str:
    values = json.loads(contributions_json) if contributions_json else []
    return canonical_json({"sources": values})


def _intermediate_schema():
    from pyspark.sql.types import (
        ArrayType,
        StringType,
        StructField,
        StructType,
    )

    name = StructType(
        [
            StructField("name_type", StringType(), False),
            StructField("language", StringType(), False),
            StructField("value", StringType(), False),
        ]
    )
    external_id = StructType(
        [
            StructField("scheme", StringType(), False),
            StructField("value", StringType(), False),
        ]
    )
    relation = StructType(
        [
            StructField("relation_type", StringType(), False),
            StructField("target_source", StringType(), False),
            StructField("target_id", StringType(), False),
            StructField("ordinal", StringType(), True),
            StructField("target_type_hint", StringType(), False),
            StructField("attributes_json", StringType(), False),
        ]
    )
    return StructType(
        [
            StructField("record_key", StringType(), False),
            StructField("source", StringType(), False),
            StructField("source_record_id", StringType(), False),
            StructField("source_revision", StringType(), True),
            StructField("modified", StringType(), True),
            StructField("source_hash", StringType(), False),
            StructField("payload_json", StringType(), False),
            StructField("parse_error", StringType(), True),
            StructField("direct_types", ArrayType(StringType(), False), False),
            StructField("subclass_parents", ArrayType(StringType(), False), False),
            StructField("entity_type_hint", StringType(), False),
            StructField("names", ArrayType(name, False), False),
            StructField("external_ids", ArrayType(external_id, False), False),
            StructField("relations", ArrayType(relation, False), False),
            StructField("attributes_json", StringType(), False),
        ]
    )


def _choose_type_expression(column: Any) -> Any:
    from pyspark.sql import functions as F

    priority = F.create_map(
        *[
            item
            for entity_type, number in _TYPE_PRIORITY.items()
            for item in (F.lit(entity_type), F.lit(number))
        ]
    )
    return F.struct(
        priority[column].alias("priority"),
        column.alias("entity_type"),
    )


def transform_landing(
    spark: Any,
    landing: Any,
    *,
    max_closure_iterations: int = 64,
) -> dict[str, Any]:
    """Transform landing rows with distributed joins and bounded closure rounds."""

    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType

    required = {
        "record_key",
        "source",
        "source_record_id",
        "source_revision",
        "modified",
        "source_hash",
        "payload_json",
    }
    missing = sorted(required - set(landing.columns))
    if missing:
        raise ValueError(f"landing dataframe is missing columns: {missing}")
    normalized = spark.createDataFrame(
        landing.select(*sorted(required)).rdd.map(_normalize_row),
        schema=_intermediate_schema(),
    ).persist()
    valid = normalized.where(F.col("parse_error").isNull())
    wiki = valid.where(F.col("source") == "wikidata")
    eidr = valid.where(F.col("source") == "eidr")

    closure = spark.createDataFrame(
        sorted(ENTITY_TYPE_SEEDS.items()), ["class_id", "entity_type"]
    ).localCheckpoint(eager=True)
    edges = (
        wiki.select(
            F.col("source_record_id").alias("child"),
            F.explode("subclass_parents").alias("parent"),
        )
        .dropDuplicates()
        .persist()
    )
    for _ in range(max_closure_iterations):
        candidates = (
            edges.join(
                closure,
                edges.parent == closure.class_id,
                "inner",
            )
            .select(F.col("child").alias("class_id"), "entity_type")
            .dropDuplicates()
        )
        delta = candidates.join(
            closure, ["class_id", "entity_type"], "left_anti"
        ).persist()
        if delta.count() == 0:
            delta.unpersist()
            break
        previous = closure
        closure = (
            previous.unionByName(delta).dropDuplicates().localCheckpoint(eager=True)
        )
        previous.unpersist()
        delta.unpersist()
    else:
        raise RuntimeError(
            f"P31/P279 closure did not converge in {max_closure_iterations} rounds"
        )

    direct_types = wiki.select(
        F.col("source_record_id").alias("qid"),
        F.explode("direct_types").alias("class_id"),
    )
    classified = (
        direct_types.join(closure, "class_id", "inner")
        .select("qid", "entity_type")
        .dropDuplicates()
        .groupBy("qid")
        .agg(F.min(_choose_type_expression(F.col("entity_type"))).alias("chosen"))
        .select("qid", F.col("chosen.entity_type").alias("entity_type"))
    )
    wiki_types = (
        wiki.select(F.col("source_record_id").alias("qid"))
        .dropDuplicates()
        .join(classified, "qid", "left")
        .fillna({"entity_type": "UNKNOWN"})
        .persist()
    )

    wiki_relations = (
        wiki.select(
            F.col("source_record_id").alias("subject_id"),
            F.explode("relations").alias("relation"),
        )
        .join(
            wiki_types.select(
                F.col("qid").alias("subject_id"),
                F.col("entity_type").alias("subject_type"),
            ),
            "subject_id",
        )
        .where(F.col("subject_type").isin(sorted(MEDIA_ENTITY_TYPES)))
        .persist()
    )
    relation_targets = wiki_relations.select(
        F.col("relation.target_id").alias("qid"),
        F.col("relation.target_type_hint").alias("hint_type"),
    ).dropDuplicates()
    target_types = (
        relation_targets.join(wiki_types, "qid", "left")
        .withColumn(
            "candidate_type",
            F.when(
                F.col("entity_type").isNotNull() & (F.col("entity_type") != "UNKNOWN"),
                F.col("entity_type"),
            ).otherwise(F.col("hint_type")),
        )
        .groupBy("qid")
        .agg(F.min(_choose_type_expression(F.col("candidate_type"))).alias("chosen"))
        .select("qid", F.col("chosen.entity_type").alias("entity_type"))
    )

    wiki_exact = (
        wiki.select(
            F.col("source_record_id").alias("qid"),
            F.explode("external_ids").alias("identifier"),
        )
        .where(F.col("identifier.scheme").isin("eidr", "imdb"))
        .select(
            "qid",
            F.col("identifier.scheme").alias("scheme"),
            F.col("identifier.value").alias("value"),
        )
        .dropDuplicates()
    )
    eidr_exact = (
        eidr.select(
            F.col("record_key").alias("eidr_record_key"),
            F.explode("external_ids").alias("identifier"),
        )
        .where(F.col("identifier.scheme").isin("eidr", "imdb"))
        .select(
            "eidr_record_key",
            F.col("identifier.scheme").alias("scheme"),
            F.col("identifier.value").alias("value"),
        )
        .dropDuplicates()
    )
    candidates = (
        eidr_exact.join(wiki_exact, ["scheme", "value"], "inner")
        .groupBy("eidr_record_key")
        .agg(
            F.countDistinct("qid").alias("candidate_count"),
            F.min("qid").alias("candidate_qid"),
            F.sort_array(F.collect_set("qid")).alias("candidate_qids"),
        )
    )
    eidr_resolved = (
        eidr.alias("e")
        .join(
            candidates,
            F.col("e.record_key") == F.col("eidr_record_key"),
            "left",
        )
        .withColumn(
            "candidate_qid",
            F.when(F.col("candidate_count") == 1, F.col("candidate_qid")),
        )
        .join(
            wiki_types.select(
                F.col("qid").alias("candidate_qid"),
                F.col("entity_type").alias("wiki_type"),
            ),
            "candidate_qid",
            "left",
        )
        .withColumn(
            "type_conflict",
            F.col("candidate_qid").isNotNull()
            & F.col("wiki_type").isNotNull()
            & (F.col("wiki_type") != "UNKNOWN")
            & (F.col("entity_type_hint") != "UNKNOWN")
            & (F.col("wiki_type") != F.col("entity_type_hint")),
        )
        .withColumn(
            "matched_qid",
            F.when(~F.col("type_conflict"), F.col("candidate_qid")),
        )
        .withColumn(
            "canonical_source",
            F.when(F.col("matched_qid").isNotNull(), F.lit("wikidata")).otherwise(
                F.lit("eidr")
            ),
        )
        .withColumn(
            "canonical_source_id",
            F.coalesce(F.col("matched_qid"), F.col("source_record_id")),
        )
        .withColumn(
            "resolved_type",
            F.when(
                F.col("matched_qid").isNotNull()
                & F.col("wiki_type").isNotNull()
                & (F.col("wiki_type") != "UNKNOWN"),
                F.col("wiki_type"),
            ).otherwise(F.col("entity_type_hint")),
        )
        .persist()
    )

    entity_key_udf = F.udf(_entity_key, StringType())
    known_wiki = wiki_types.where(F.col("entity_type") != "UNKNOWN")
    matched_wiki = eidr_resolved.where(F.col("matched_qid").isNotNull()).select(
        F.col("matched_qid").alias("qid"),
        F.col("resolved_type").alias("entity_type"),
    )
    wiki_entity_types = (
        known_wiki.unionByName(target_types)
        .unionByName(matched_wiki)
        .groupBy("qid")
        .agg(F.min(_choose_type_expression(F.col("entity_type"))).alias("chosen"))
        .select("qid", F.col("chosen.entity_type").alias("entity_type"))
    )
    wiki_entities = wiki_entity_types.select(
        entity_key_udf(F.lit("wikidata"), F.col("qid")).alias("entity_key"),
        "entity_type",
        F.lit("wikidata").alias("canonical_source"),
        F.col("qid").alias("canonical_source_id"),
    )
    eidr_entities = eidr_resolved.where(F.col("matched_qid").isNull()).select(
        entity_key_udf(F.lit("eidr"), F.col("source_record_id")).alias("entity_key"),
        F.col("resolved_type").alias("entity_type"),
        F.lit("eidr").alias("canonical_source"),
        F.col("source_record_id").alias("canonical_source_id"),
    )

    eidr_identity = eidr_resolved.select(
        F.col("source_record_id").alias("eidr_id"),
        "canonical_source",
        "canonical_source_id",
    ).dropDuplicates(["eidr_id"])
    eidr_relations = eidr_resolved.select(
        F.col("source_record_id").alias("subject_id"),
        "canonical_source",
        "canonical_source_id",
        F.explode("relations").alias("relation"),
    )
    missing_eidr_parents = (
        eidr_relations.select(F.col("relation.target_id").alias("eidr_id"))
        .dropDuplicates()
        .join(eidr_identity, "eidr_id", "left_anti")
        .select(
            entity_key_udf(F.lit("eidr"), F.col("eidr_id")).alias("entity_key"),
            F.lit("UNKNOWN").alias("entity_type"),
            F.lit("eidr").alias("canonical_source"),
            F.col("eidr_id").alias("canonical_source_id"),
        )
    )
    entity_candidates = wiki_entities.unionByName(eidr_entities).unionByName(
        missing_eidr_parents
    )
    entity_base = (
        entity_candidates.groupBy("entity_key")
        .agg(
            F.min(
                F.struct(
                    _choose_type_expression(F.col("entity_type")).alias("typed"),
                    F.col("canonical_source"),
                    F.col("canonical_source_id"),
                )
            ).alias("chosen")
        )
        .select(
            "entity_key",
            F.col("chosen.typed.entity_type").alias("entity_type"),
            F.col("chosen.canonical_source").alias("canonical_source"),
            F.col("chosen.canonical_source_id").alias("canonical_source_id"),
        )
        .persist()
    )

    wiki_attributes = wiki.join(
        wiki_entity_types,
        wiki.source_record_id == wiki_entity_types.qid,
        "inner",
    ).select(
        entity_key_udf(F.lit("wikidata"), F.col("source_record_id")).alias(
            "entity_key"
        ),
        F.lit("wikidata").alias("source"),
        "source_record_id",
        "attributes_json",
    )
    eidr_attributes = eidr_resolved.select(
        entity_key_udf("canonical_source", "canonical_source_id").alias("entity_key"),
        F.lit("eidr").alias("source"),
        "source_record_id",
        "attributes_json",
    )
    attributes_udf = F.udf(_aggregate_attributes, StringType())
    attributes = (
        wiki_attributes.unionByName(eidr_attributes)
        .select(
            "entity_key",
            F.struct("source", "source_record_id", "attributes_json").alias(
                "contribution"
            ),
        )
        .groupBy("entity_key")
        .agg(
            F.to_json(F.sort_array(F.collect_set("contribution"))).alias(
                "contributions_json"
            )
        )
        .select(
            "entity_key",
            attributes_udf("contributions_json").alias("attributes_json"),
        )
    )
    catalog_entity = (
        entity_base.join(attributes, "entity_key", "left")
        .withColumn(
            "attributes_json",
            F.coalesce("attributes_json", F.lit(canonical_json({"sources": []}))),
        )
        .select(
            "entity_key",
            "entity_type",
            "canonical_source",
            "canonical_source_id",
            "attributes_json",
        )
        .dropDuplicates(["entity_key"])
    )

    wiki_mapping = wiki_entity_types.select(
        F.lit("wikidata").alias("source"),
        F.col("qid").alias("source_record_id"),
        entity_key_udf(F.lit("wikidata"), F.col("qid")).alias("entity_key"),
    )
    eidr_mapping = eidr_resolved.select(
        F.lit("eidr").alias("source"),
        "source_record_id",
        entity_key_udf("canonical_source", "canonical_source_id").alias("entity_key"),
    )
    record_mapping = wiki_mapping.unionByName(eidr_mapping).dropDuplicates(
        ["source", "source_record_id"]
    )
    catalog_source_record = (
        valid.join(record_mapping, ["source", "source_record_id"], "inner")
        .select(
            "record_key",
            "source",
            "source_record_id",
            "source_revision",
            "modified",
            "source_hash",
            "entity_key",
            "payload_json",
        )
        .dropDuplicates(["record_key"])
    )

    name_key_udf = F.udf(_name_key, StringType())
    exploded_names = valid.join(
        record_mapping, ["source", "source_record_id"], "inner"
    ).select(
        "entity_key",
        "source",
        "source_record_id",
        F.explode("names").alias("name"),
    )
    catalog_name = exploded_names.select(
        name_key_udf(
            "entity_key",
            "name.name_type",
            "name.language",
            "name.value",
            "source",
            "source_record_id",
        ).alias("name_key"),
        "entity_key",
        F.col("name.name_type").alias("name_type"),
        F.col("name.language").alias("language"),
        F.col("name.value").alias("value"),
        "source",
        "source_record_id",
    ).dropDuplicates(["name_key"])

    identifier_key_udf = F.udf(_identifier_key, StringType())
    exploded_identifiers = valid.join(
        record_mapping, ["source", "source_record_id"], "inner"
    ).select(
        "entity_key",
        "source",
        "source_record_id",
        F.explode("external_ids").alias("identifier"),
    )
    catalog_external_identifier = exploded_identifiers.select(
        identifier_key_udf(
            "entity_key",
            "identifier.scheme",
            "identifier.value",
            "source",
            "source_record_id",
        ).alias("identifier_key"),
        "entity_key",
        F.col("identifier.scheme").alias("scheme"),
        F.col("identifier.value").alias("value"),
        "source",
        "source_record_id",
    ).dropDuplicates(["identifier_key"])

    relation_key_udf = F.udf(_relation_key, StringType())
    wiki_relation_rows = wiki_relations.select(
        entity_key_udf(F.lit("wikidata"), "subject_id").alias("subject_entity_key"),
        F.col("relation.relation_type").alias("relation_type"),
        entity_key_udf(F.lit("wikidata"), F.col("relation.target_id")).alias(
            "object_entity_key"
        ),
        F.col("relation.ordinal").alias("ordinal"),
        F.lit("wikidata").alias("source"),
        F.col("subject_id").alias("source_record_id"),
        F.col("relation.attributes_json").alias("attributes_json"),
    )
    resolved_parent = eidr_relations.alias("r").join(
        eidr_identity.alias("p"),
        F.col("r.relation.target_id") == F.col("p.eidr_id"),
        "left",
    )
    eidr_relation_rows = resolved_parent.select(
        entity_key_udf(
            F.col("r.canonical_source"), F.col("r.canonical_source_id")
        ).alias("subject_entity_key"),
        F.col("r.relation.relation_type").alias("relation_type"),
        entity_key_udf(
            F.coalesce(F.col("p.canonical_source"), F.lit("eidr")),
            F.coalesce(F.col("p.canonical_source_id"), F.col("r.relation.target_id")),
        ).alias("object_entity_key"),
        F.col("r.relation.ordinal").alias("ordinal"),
        F.lit("eidr").alias("source"),
        F.col("r.subject_id").alias("source_record_id"),
        F.col("r.relation.attributes_json").alias("attributes_json"),
    )
    catalog_relation = (
        wiki_relation_rows.unionByName(eidr_relation_rows)
        .select(
            relation_key_udf(
                "subject_entity_key",
                "relation_type",
                "object_entity_key",
                "ordinal",
                "source",
                "source_record_id",
                "attributes_json",
            ).alias("relation_key"),
            "subject_entity_key",
            "relation_type",
            "object_entity_key",
            "ordinal",
            "source",
            "source_record_id",
            "attributes_json",
        )
        .dropDuplicates(["relation_key"])
    )

    invalid_errors = normalized.where(F.col("parse_error").isNotNull()).select(
        "source",
        "source_record_id",
        F.lit("INVALID_LANDING_RECORD").alias("error_code"),
        F.col("parse_error").alias("message"),
        F.to_json(F.struct(F.col("record_key"))).alias("details_json"),
    )
    ambiguous_errors = eidr_resolved.where(F.col("candidate_count") > 1).select(
        F.lit("eidr").alias("source"),
        "source_record_id",
        F.lit("EXACT_IDENTIFIER_CONFLICT").alias("error_code"),
        F.lit("EIDR exact identifiers resolve to multiple Wikidata entities").alias(
            "message"
        ),
        F.to_json(F.struct(F.col("candidate_qids").alias("wikidataIds"))).alias(
            "details_json"
        ),
    )
    type_errors = eidr_resolved.where(F.col("type_conflict")).select(
        F.lit("eidr").alias("source"),
        "source_record_id",
        F.lit("ENTITY_TYPE_CONFLICT").alias("error_code"),
        F.lit("EIDR referent type conflicts with Wikidata type closure").alias(
            "message"
        ),
        F.to_json(
            F.struct(
                F.col("entity_type_hint").alias("eidrType"),
                F.col("candidate_qid").alias("wikidataId"),
                F.col("wiki_type").alias("wikidataType"),
            )
        ).alias("details_json"),
    )
    error_key_udf = F.udf(_error_key, StringType())
    catalog_ingest_error = (
        invalid_errors.unionByName(ambiguous_errors)
        .unionByName(type_errors)
        .select(
            error_key_udf(
                "source", "source_record_id", "error_code", "details_json"
            ).alias("error_key"),
            "source",
            "source_record_id",
            "error_code",
            "message",
            "details_json",
        )
        .dropDuplicates(["error_key"])
    )

    return {
        "catalog_source_record": catalog_source_record,
        "catalog_entity": catalog_entity,
        "catalog_name": catalog_name,
        "catalog_external_identifier": catalog_external_identifier,
        "catalog_relation": catalog_relation,
        "catalog_ingest_error": catalog_ingest_error,
    }
