"""Replayable v2 adapters and mappers for existing Wikidata/EIDR v1 inputs."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

from video_media_catalog.connector import (
    ChangeSemantics,
    Completeness,
    ConnectorRecordEnvelope,
    DeleteCoverage,
    RecordOperation,
    Serialization,
    TransportKind,
    build_connector_batch_manifest,
    build_connector_record_envelope,
)
from video_media_catalog.connector_publish import (
    DEFAULT_RECORD_SHARD_BYTES,
    PublishedConnectorCapture,
    publish_connector_capture,
)
from video_media_catalog.constants import (
    ENTITY_TYPE_SEEDS,
    EXTERNAL_ID_PROPERTIES,
    RELATION_PROPERTIES,
)
from video_media_catalog.eidr import (
    iter_eidr_records,
    normalize_eidr_id,
    normalize_imdb_id,
)
from video_media_catalog.identity_resolution import referent_kind_for_entity_type
from video_media_catalog.imdb import (
    IMDB_COMPANY_NAMESPACE_ID,
    IMDB_NAME_NAMESPACE_ID,
    IMDB_TITLE_NAMESPACE_ID,
)
from video_media_catalog.models import ObjectRef
from video_media_catalog.object_store import RuntimeObjectStore
from video_media_catalog.source_mapper import AssertionBuilder, MappedAssertions
from video_media_catalog.wikidata import iter_wikidata_records

WIKIDATA_SOURCE_SYSTEM_ID = "wikidata"
WIKIDATA_SOURCE_PRODUCT_ID = "wikidata-json-dump"
WIKIDATA_NAMESPACE_ID = "wikidata-item"
WIKIDATA_CONNECTOR_ID = "wikidata-v2-adapter"
WIKIDATA_POLICY_ID = "wikidata-structured-data-cc0"

EIDR_SOURCE_SYSTEM_ID = "eidr"
EIDR_SOURCE_PRODUCT_ID = "eidr-public-registry"
EIDR_NAMESPACE_ID = "eidr-content"
EIDR_CONNECTOR_ID = "eidr-v2-adapter"
EIDR_POLICY_ID = "eidr-public-registry"

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
    "MOVIE": "MOVIE",
    "TV_SERIES": "SERIES",
    "TV_SEASON": "SEASON",
    "TV_EPISODE": "EPISODE",
    "EDIT": "EDIT",
}


def _external_identifier_referent_kind(
    namespace_id: str,
    entity_type: str | None,
) -> str:
    if namespace_id == IMDB_NAME_NAMESPACE_ID:
        return "AGENT"
    if namespace_id == IMDB_COMPANY_NAMESPACE_ID:
        return "ORGANIZATION"
    if namespace_id == IMDB_TITLE_NAMESPACE_ID:
        return referent_kind_for_entity_type(entity_type or "EDITORIAL_WORK")
    return referent_kind_for_entity_type(entity_type or "EDITORIAL_WORK")


def _imdb_namespace_id(value: str) -> str | None:
    if value.startswith("tt"):
        return IMDB_TITLE_NAMESPACE_ID
    if value.startswith("nm"):
        return IMDB_NAME_NAMESPACE_ID
    if value.startswith("co"):
        return IMDB_COMPANY_NAMESPACE_ID
    return None


def _statement_value(statement: dict[str, Any]) -> Any:
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


def _statements(payload: dict[str, Any], prop: str) -> list[dict[str, Any]]:
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


def _wikidata_type(payload: dict[str, Any]) -> str | None:
    values = {
        ENTITY_TYPE_SEEDS[value]
        for statement in _statements(payload, "P31")
        if isinstance((value := _statement_value(statement)), str)
        and value in ENTITY_TYPE_SEEDS
    }
    hint = payload.get("v1EntityType")
    if isinstance(hint, str) and hint.upper() in _TYPE_PRECEDENCE:
        values.add(hint.upper())
    return min(values, key=_TYPE_PRECEDENCE.__getitem__) if values else None


def _wikidata_type_hints(path: Path) -> tuple[dict[str, str], int]:
    """Resolve the bounded v1 subset's P31/P279 closure before enveloping."""

    direct_types: dict[str, set[str]] = {}
    subclass_parents: dict[str, set[str]] = {}
    count = 0
    for record in iter_wikidata_records(path):
        count += 1
        payload = json.loads(record.payload_json)
        qid = record.source_record_id
        direct_types[qid] = {
            value
            for statement in _statements(payload, "P31")
            if isinstance((value := _statement_value(statement)), str)
            and _QID.fullmatch(value)
        }
        subclass_parents[qid] = {
            value
            for statement in _statements(payload, "P279")
            if isinstance((value := _statement_value(statement)), str)
            and _QID.fullmatch(value)
        }
    closure: dict[str, set[str]] = {
        qid: {entity_type} for qid, entity_type in ENTITY_TYPE_SEEDS.items()
    }
    for _ in range(64):
        changed = False
        for child, parents in subclass_parents.items():
            inherited = set().union(*(closure.get(parent, set()) for parent in parents))
            if not inherited.issubset(closure.setdefault(child, set())):
                closure[child].update(inherited)
                changed = True
        if not changed:
            break
    else:
        raise RuntimeError("Wikidata P31/P279 closure did not converge")
    hints: dict[str, str] = {}
    for qid, classes in direct_types.items():
        values = set().union(*(closure.get(class_id, set()) for class_id in classes))
        if values:
            hints[qid] = min(values, key=_TYPE_PRECEDENCE.__getitem__)
    return hints, count


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
    mapped = {
        "EPISODE": "TV_EPISODE",
        "SEASON": "TV_SEASON",
        "SERIES": "TV_SERIES",
        "EDIT": "EDIT",
    }.get(record_type)
    if mapped is not None:
        return mapped
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
            extracted = _statement_value({"mainsnak": value})
            if extracted is not None:
                return str(extracted)
    return None


def map_wikidata_record(
    envelope: ConnectorRecordEnvelope,
) -> MappedAssertions:
    """Map one normalized Wikidata item; class closure may supply v1EntityType."""

    if (
        envelope.source_system_id != WIKIDATA_SOURCE_SYSTEM_ID
        or envelope.source_product_id != WIKIDATA_SOURCE_PRODUCT_ID
        or envelope.source_namespace_id != WIKIDATA_NAMESPACE_ID
        or envelope.operation != RecordOperation.UPSERT
        or envelope.payload_json is None
    ):
        raise ValueError("record is not an active Wikidata v2 envelope")
    payload = json.loads(envelope.payload_json)
    if not isinstance(payload, dict) or payload.get("id") != envelope.source_record_id:
        raise ValueError("Wikidata payload identity does not match its envelope")
    entity_type = _wikidata_type(payload)
    source_referent_kind = "EDITORIAL_WORK"
    identifier_referent_kind = referent_kind_for_entity_type(
        entity_type or source_referent_kind
    )
    from video_media_catalog.assertions import SourceNodeRef, ValueType

    node = SourceNodeRef(
        namespace_id=WIKIDATA_NAMESPACE_ID,
        source_id=envelope.source_record_id,
        referent_kind=source_referent_kind,
    )
    builder = AssertionBuilder(
        envelope=envelope,
        source_node=node,
        mapper_id="wikidata-v2-mapper",
        mapper_version="1.0.0",
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
    for index, statement in enumerate(_statements(payload, "P1476")):
        value = _statement_value(statement)
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
        for index, statement in enumerate(_statements(payload, prop)):
            builder.add_field(
                predicate,
                value_type,
                normalizer(_statement_value(statement)),
                f"/claims/{prop}/{index}/mainsnak/datavalue/value",
            )
    for prop, predicate in {
        "P364": "language",
        "P495": "country",
        "P136": "genre",
    }.items():
        for index, statement in enumerate(_statements(payload, prop)):
            value = _statement_value(statement)
            if isinstance(value, str):
                builder.add_field(
                    predicate,
                    ValueType.STRING,
                    value,
                    f"/claims/{prop}/{index}/mainsnak/datavalue/value",
                    {"vocabulary": "wikidata-item"},
                )

    for prop, namespace in EXTERNAL_ID_PROPERTIES.items():
        for index, statement in enumerate(_statements(payload, prop)):
            value = _statement_value(statement)
            if not isinstance(value, str):
                continue
            try:
                normalized = (
                    normalize_imdb_id(value)
                    if namespace == "imdb"
                    else (normalize_eidr_id(value) if namespace == "eidr" else value)
                )
            except ValueError:
                continue
            target_namespace = (
                _imdb_namespace_id(normalized)
                if namespace == "imdb"
                else {
                    "eidr": EIDR_NAMESPACE_ID,
                    "douban": "douban-subject",
                }[namespace]
            )
            if target_namespace is None:
                continue
            builder.add_identifier(
                target_namespace,
                normalized,
                {"imdb": "IMDb", "eidr": "EIDR", "douban": "Douban"}[namespace],
                _external_identifier_referent_kind(target_namespace, entity_type),
                f"/claims/{prop}/{index}/mainsnak/datavalue/value",
            )

    for prop, predicate in RELATION_PROPERTIES.items():
        for index, statement in enumerate(_statements(payload, prop)):
            target = _statement_value(statement)
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
    if (
        envelope.source_system_id != EIDR_SOURCE_SYSTEM_ID
        or envelope.source_product_id != EIDR_SOURCE_PRODUCT_ID
        or envelope.source_namespace_id != EIDR_NAMESPACE_ID
        or envelope.operation != RecordOperation.UPSERT
        or envelope.payload_json is None
    ):
        raise ValueError("record is not an active EIDR v2 envelope")
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
        mapper_id="eidr-v2-mapper",
        mapper_version="1.0.0",
    )
    builder.add_identifier(
        EIDR_NAMESPACE_ID,
        envelope.source_record_id,
        "EIDR Association",
        referent_kind,
        "/id",
    )
    if entity_type != "UNKNOWN":
        builder.add_entity_type(entity_type, "/recordType")
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
            "imdb": "imdb-name"
            if isinstance(value, str) and value.lower().startswith("nm")
            else "imdb-title",
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


def _iter_adapter_records(
    source: str,
    path: Path,
) -> Iterable[tuple[dict[str, Any], str, str | None, str | None, str]]:
    records = (
        iter_wikidata_records(path) if source == "wikidata" else iter_eidr_records(path)
    )
    for index, record in enumerate(records):
        yield (
            json.loads(record.payload_json),
            record.source_record_id,
            record.source_revision,
            record.modified,
            f"/records/{index}",
        )


def capture_v1_adapter(
    *,
    source: str,
    input_path: Path,
    raw_object: ObjectRef,
    destination_prefix: str,
    acquired_at: str,
    image_digest: str,
    config_digest: str,
    coverage_id: str,
    store: RuntimeObjectStore,
    eidr_complete_snapshot: bool = False,
    record_shard_bytes: int = DEFAULT_RECORD_SHARD_BYTES,
) -> PublishedConnectorCapture:
    """Wrap a verified v1 source object in v2 manifests and envelopes."""

    if source not in {"wikidata", "eidr"}:
        raise ValueError("source must be wikidata or eidr")
    digest = hashlib.sha256()
    size = 0
    with input_path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
            size += len(chunk)
    if size != raw_object.size_bytes or digest.hexdigest() != raw_object.checksum.value:
        raise ValueError("v1 adapter input does not match its raw ObjectRef")
    normalized_coverage = coverage_id.strip()
    if not normalized_coverage or len(normalized_coverage) > 256:
        raise ValueError("coverage_id must be non-empty and bounded")
    type_hints: dict[str, str] = {}
    if source == "wikidata":
        type_hints, records = _wikidata_type_hints(input_path)
    else:
        records = sum(1 for _ in _iter_adapter_records(source, input_path))
    if records < 1:
        raise ValueError(f"{source} adapter input contains no records")
    from video_media_catalog.community_sources import (
        eidr_rights_profile,
        wikidata_rights_profile,
    )

    if source == "wikidata":
        system_id = WIKIDATA_SOURCE_SYSTEM_ID
        product_id = WIKIDATA_SOURCE_PRODUCT_ID
        namespace_id = WIKIDATA_NAMESPACE_ID
        connector_id = WIKIDATA_CONNECTOR_ID
        policy = wikidata_rights_profile()
        serialization = Serialization.JSON_LINES
        completeness = Completeness.COMPLETE
        delete_coverage = DeleteCoverage.SNAPSHOT_DIFF
        schema = "wikidata-entity-v2-adapter-v1"
    else:
        system_id = EIDR_SOURCE_SYSTEM_ID
        product_id = EIDR_SOURCE_PRODUCT_ID
        namespace_id = EIDR_NAMESPACE_ID
        connector_id = EIDR_CONNECTOR_ID
        policy = eidr_rights_profile()
        serialization = Serialization.XML
        completeness = (
            Completeness.COMPLETE if eidr_complete_snapshot else Completeness.PARTIAL
        )
        delete_coverage = (
            DeleteCoverage.SNAPSHOT_DIFF
            if eidr_complete_snapshot
            else DeleteCoverage.NONE
        )
        schema = "eidr-record-v2-adapter-v1"
    batch = build_connector_batch_manifest(
        source_system_id=system_id,
        source_product_id=product_id,
        connector_id=connector_id,
        connector_version="1.0.0",
        image_digest=image_digest,
        config_digest=config_digest,
        policy_id=policy.policy_id,
        policy_digest=policy.digest,
        transport_kind=TransportKind.DUMP,
        serialization=serialization,
        change_semantics=ChangeSemantics.FULL_SNAPSHOT,
        completeness=completeness,
        delete_coverage=delete_coverage,
        coverage_scope={
            "adapter": connector_id,
            "coverageId": normalized_coverage,
        },
        raw_objects=(raw_object,),
        acquired_at=acquired_at,
        record_count=records,
        error_count=0,
    )

    def envelopes() -> Iterator[ConnectorRecordEnvelope]:
        for payload, record_id, revision, modified, location in _iter_adapter_records(
            source, input_path
        ):
            if source == "wikidata" and record_id in type_hints:
                payload = {**payload, "v1EntityType": type_hints[record_id]}
            yield build_connector_record_envelope(
                payload=payload,
                batch_id=batch.batch_id,
                source_system_id=system_id,
                source_product_id=product_id,
                source_namespace_id=namespace_id,
                source_record_id=record_id,
                source_revision=revision,
                operation=RecordOperation.UPSERT,
                source_modified_at=modified,
                observed_at=batch.acquired_at,
                ingested_at=batch.acquired_at,
                payload_schema=schema,
                raw_object=raw_object,
                source_location=location,
                policy_id=batch.policy_id,
                policy_digest=batch.policy_digest,
            )

    return publish_connector_capture(
        destination_prefix=destination_prefix,
        batch=batch,
        envelopes=envelopes(),
        store=store,
        record_shard_bytes=record_shard_bytes,
    )
