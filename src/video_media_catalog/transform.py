"""Deterministic source-to-curated normalization.

The functions in this module are Spark-independent so transformation rules can be
tested without a JVM. The Spark driver uses the same canonical row builder.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
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
    EXTERNAL_ID_PROPERTIES,
    MEDIA_ENTITY_TYPES,
    RELATION_PROPERTIES,
)
from video_media_catalog.eidr import normalize_eidr_id, normalize_imdb_id
from video_media_catalog.models import LandingRecord

_TYPE_PRECEDENCE = {
    "TV_EPISODE": 0,
    "TV_SEASON": 1,
    "TV_SERIES": 2,
    "MOVIE": 3,
    "PERSON": 4,
    "ORGANIZATION": 5,
    "UNKNOWN": 99,
}
_QID = re.compile(r"^Q[1-9][0-9]*$")


@dataclass
class CuratedRows:
    catalog_source_record: list[dict[str, Any]] = field(default_factory=list)
    catalog_entity: list[dict[str, Any]] = field(default_factory=list)
    catalog_name: list[dict[str, Any]] = field(default_factory=list)
    catalog_external_identifier: list[dict[str, Any]] = field(default_factory=list)
    catalog_relation: list[dict[str, Any]] = field(default_factory=list)
    catalog_ingest_error: list[dict[str, Any]] = field(default_factory=list)

    def tables(self) -> dict[str, list[dict[str, Any]]]:
        return {
            name: sorted(getattr(self, name), key=lambda row: next(iter(row.values())))
            for name in (
                "catalog_source_record",
                "catalog_entity",
                "catalog_name",
                "catalog_external_identifier",
                "catalog_relation",
                "catalog_ingest_error",
            )
        }


@dataclass
class _EntityState:
    entity_key: str
    entity_type: str
    canonical_source: str
    canonical_source_id: str
    attributes: dict[str, Any] = field(default_factory=dict)


def _landing_record(value: LandingRecord | dict[str, Any]) -> LandingRecord:
    return (
        value
        if isinstance(value, LandingRecord)
        else LandingRecord.model_validate(value)
    )


def _statements(payload: dict[str, Any], prop: str) -> list[dict[str, Any]]:
    claims = payload.get("claims")
    if not isinstance(claims, dict):
        return []
    values = claims.get(prop)
    if not isinstance(values, list):
        return []
    candidates = [
        value
        for value in values
        if isinstance(value, dict) and value.get("rank") != "deprecated"
    ]
    preferred = [value for value in candidates if value.get("rank") == "preferred"]
    return preferred or candidates


def _snak_value(snak: Any) -> Any:
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


def _statement_value(statement: dict[str, Any]) -> Any:
    return _snak_value(statement.get("mainsnak"))


def _qid_values(payload: dict[str, Any], prop: str) -> list[str]:
    return [
        value
        for statement in _statements(payload, prop)
        if isinstance((value := _statement_value(statement)), str)
        and _QID.fullmatch(value)
    ]


def _qualifier_ordinal(statement: dict[str, Any]) -> str | None:
    qualifiers = statement.get("qualifiers")
    if not isinstance(qualifiers, dict):
        return None
    values = qualifiers.get("P1545")
    if not isinstance(values, list):
        return None
    for snak in values:
        value = _snak_value(snak)
        if value is not None:
            return str(value)
    return None


def _external_value(scheme: str, value: Any) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        if scheme == "imdb":
            return normalize_imdb_id(value)
        if scheme == "eidr":
            return normalize_eidr_id(value)
    except ValueError:
        return None
    return value.strip()


def _external_ids(payload: dict[str, Any]) -> list[tuple[str, str]]:
    result: list[tuple[str, str]] = []
    for prop, scheme in EXTERNAL_ID_PROPERTIES.items():
        for statement in _statements(payload, prop):
            if normalized := _external_value(scheme, _statement_value(statement)):
                result.append((scheme, normalized))
    return sorted(set(result))


def _classification_closure(
    wikidata: dict[str, dict[str, Any]],
) -> dict[str, set[str]]:
    closure: dict[str, set[str]] = {
        qid: {entity_type} for qid, entity_type in ENTITY_TYPE_SEEDS.items()
    }
    edges = {
        qid: set(_qid_values(payload, "P279")) for qid, payload in wikidata.items()
    }
    changed = True
    while changed:
        changed = False
        for child, parents in edges.items():
            inherited = set().union(*(closure.get(parent, set()) for parent in parents))
            if not inherited.issubset(closure.setdefault(child, set())):
                closure[child].update(inherited)
                changed = True
    return closure


def _choose_type(types: set[str]) -> str:
    return min(types or {"UNKNOWN"}, key=lambda value: _TYPE_PRECEDENCE[value])


def _wikidata_type(payload: dict[str, Any], closure: dict[str, set[str]]) -> str:
    types: set[str] = set()
    for direct_type in _qid_values(payload, "P31"):
        types.update(closure.get(direct_type, set()))
    return _choose_type(types)


def _eidr_type(payload: dict[str, Any]) -> str:
    record_type = str(payload.get("recordType") or "").upper()
    if record_type == "EPISODE":
        return "TV_EPISODE"
    if record_type == "SEASON":
        return "TV_SEASON"
    if record_type == "SERIES":
        return "TV_SERIES"
    value = str(payload.get("referentType") or "").lower()
    if "episode" in value:
        return "TV_EPISODE"
    if "season" in value:
        return "TV_SEASON"
    if "series" in value:
        return "TV_SERIES"
    if any(token in value for token in ("movie", "film", "television", "tv")):
        return "MOVIE"
    return "UNKNOWN"


def _entity_key(source: str, source_id: str) -> str:
    return deterministic_key(
        "catalog-entity", {"canonicalSource": source, "canonicalSourceId": source_id}
    )


def _error_row(
    *,
    source: str,
    source_record_id: str,
    code: str,
    message: str,
    details: dict[str, Any],
) -> dict[str, Any]:
    details_json = canonical_json(details)
    return {
        "error_key": deterministic_key(
            "catalog-ingest-error",
            {
                "source": source,
                "sourceRecordId": source_record_id,
                "errorCode": code,
                "details": details,
            },
        ),
        "source": source,
        "source_record_id": source_record_id,
        "error_code": code,
        "message": message,
        "details_json": details_json,
    }


def _add_name(
    rows: dict[str, dict[str, Any]],
    *,
    entity_key: str,
    name_type: str,
    language: str,
    value: str,
    source: str,
    source_record_id: str,
) -> None:
    identity = {
        "entityKey": entity_key,
        "nameType": name_type,
        "language": language or "und",
        "value": value,
        "source": source,
        "sourceRecordId": source_record_id,
    }
    row_key = deterministic_key("catalog-name", identity)
    rows[row_key] = {
        "name_key": row_key,
        "entity_key": entity_key,
        "name_type": name_type,
        "language": language or "und",
        "value": value,
        "source": source,
        "source_record_id": source_record_id,
    }


def _add_external_id(
    rows: dict[str, dict[str, Any]],
    *,
    entity_key: str,
    scheme: str,
    value: str,
    source: str,
    source_record_id: str,
) -> None:
    identity = {
        "entityKey": entity_key,
        "scheme": scheme,
        "value": value,
        "source": source,
        "sourceRecordId": source_record_id,
    }
    row_key = deterministic_key("catalog-external-identifier", identity)
    rows[row_key] = {
        "identifier_key": row_key,
        "entity_key": entity_key,
        "scheme": scheme,
        "value": value,
        "source": source,
        "source_record_id": source_record_id,
    }


def _add_relation(
    rows: dict[str, dict[str, Any]],
    *,
    subject_key: str,
    relation_type: str,
    object_key: str,
    ordinal: str | None,
    source: str,
    source_record_id: str,
    attributes: dict[str, Any],
) -> None:
    identity = {
        "subjectEntityKey": subject_key,
        "relationType": relation_type,
        "objectEntityKey": object_key,
        "ordinal": ordinal,
        "source": source,
        "sourceRecordId": source_record_id,
        "attributes": attributes,
    }
    row_key = deterministic_key("catalog-relation", identity)
    rows[row_key] = {
        "relation_key": row_key,
        "subject_entity_key": subject_key,
        "relation_type": relation_type,
        "object_entity_key": object_key,
        "ordinal": ordinal,
        "source": source,
        "source_record_id": source_record_id,
        "attributes_json": canonical_json(attributes),
    }


def _wikidata_names(
    payload: dict[str, Any],
    *,
    entity_key: str,
    names: dict[str, dict[str, Any]],
) -> None:
    record_id = str(payload["id"])
    labels = payload.get("labels")
    if isinstance(labels, dict):
        for language, label in labels.items():
            if isinstance(label, dict) and isinstance(label.get("value"), str):
                _add_name(
                    names,
                    entity_key=entity_key,
                    name_type="PRIMARY",
                    language=str(label.get("language") or language),
                    value=label["value"],
                    source="wikidata",
                    source_record_id=record_id,
                )
    aliases = payload.get("aliases")
    if isinstance(aliases, dict):
        for language, values in aliases.items():
            if not isinstance(values, list):
                continue
            for alias in values:
                if isinstance(alias, dict) and isinstance(alias.get("value"), str):
                    _add_name(
                        names,
                        entity_key=entity_key,
                        name_type="ALIAS",
                        language=str(alias.get("language") or language),
                        value=alias["value"],
                        source="wikidata",
                        source_record_id=record_id,
                    )
    for statement in _statements(payload, "P1476"):
        value = _statement_value(statement)
        if isinstance(value, dict) and isinstance(value.get("text"), str):
            _add_name(
                names,
                entity_key=entity_key,
                name_type="TITLE",
                language=str(value.get("language") or "und"),
                value=value["text"],
                source="wikidata",
                source_record_id=record_id,
            )


def _wikidata_attributes(payload: dict[str, Any]) -> dict[str, Any]:
    properties = {
        "P577": "releaseDates",
        "P2047": "durations",
        "P364": "languages",
        "P495": "countries",
        "P136": "genres",
        "P1113": "episodeCounts",
        "P2437": "seasonCounts",
    }
    attributes: dict[str, Any] = {
        "modified": payload.get("modified"),
        "descriptions": payload.get("descriptions") or {},
        "sitelinks": payload.get("sitelinks") or {},
    }
    for prop, name in properties.items():
        values = [
            value
            for statement in _statements(payload, prop)
            if (value := _statement_value(statement)) is not None
        ]
        if values:
            attributes[name] = values
    return attributes


def build_curated_rows(
    records: list[LandingRecord | dict[str, Any]],
) -> CuratedRows:
    """Build all six curated tables with exact-only cross-source identity."""

    valid: list[tuple[LandingRecord, dict[str, Any]]] = []
    errors: dict[str, dict[str, Any]] = {}
    for raw_record in records:
        record = _landing_record(raw_record)
        try:
            payload = json.loads(record.payload_json)
            if not isinstance(payload, dict):
                raise ValueError("payload must be an object")
            actual_hash = source_hash(payload)
            if actual_hash != record.source_hash:
                raise ValueError(
                    f"source hash mismatch: expected {record.source_hash}, "
                    f"computed {actual_hash}"
                )
            expected_key = deterministic_key(
                "source-record",
                {
                    "source": record.source,
                    "sourceRecordId": record.source_record_id,
                    "sourceRevision": record.source_revision,
                    "sourceHash": record.source_hash,
                },
            )
            if expected_key != record.record_key:
                raise ValueError(
                    f"record key mismatch: expected {expected_key}, "
                    f"got {record.record_key}"
                )
        except (json.JSONDecodeError, ValueError) as exc:
            row = _error_row(
                source=record.source,
                source_record_id=record.source_record_id,
                code="INVALID_LANDING_RECORD",
                message=str(exc),
                details={"recordKey": record.record_key},
            )
            errors[row["error_key"]] = row
            continue
        valid.append((record, payload))

    wikidata = {
        record.source_record_id: payload
        for record, payload in valid
        if record.source == "wikidata"
    }
    eidr = [(record, payload) for record, payload in valid if record.source == "eidr"]
    closure = _classification_closure(wikidata)
    wikidata_types = {
        qid: _wikidata_type(payload, closure) for qid, payload in wikidata.items()
    }

    exact_index: dict[tuple[str, str], set[str]] = {}
    for qid, payload in wikidata.items():
        for scheme, value in _external_ids(payload):
            if scheme in {"eidr", "imdb"}:
                exact_index.setdefault((scheme, value), set()).add(qid)

    eidr_match: dict[str, str | None] = {}
    for record, payload in eidr:
        identifiers = {("eidr", str(payload["id"]))}
        for alternate in payload.get("alternateIds", []):
            if not isinstance(alternate, dict):
                continue
            scheme = str(alternate.get("scheme") or "").lower()
            value = _external_value(scheme, alternate.get("value"))
            if value is not None and scheme in {"eidr", "imdb"}:
                identifiers.add((scheme, value))
        candidates = set().union(
            *(exact_index.get(identifier, set()) for identifier in identifiers)
        )
        match: str | None = next(iter(candidates)) if len(candidates) == 1 else None
        if len(candidates) > 1:
            row = _error_row(
                source="eidr",
                source_record_id=record.source_record_id,
                code="EXACT_IDENTIFIER_CONFLICT",
                message="EIDR exact identifiers resolve to multiple Wikidata entities",
                details={
                    "identifiers": [
                        {"scheme": scheme, "value": value}
                        for scheme, value in sorted(identifiers)
                    ],
                    "wikidataIds": sorted(candidates),
                },
            )
            errors[row["error_key"]] = row
        elif match is not None:
            wiki_type = wikidata_types.get(match, "UNKNOWN")
            eidr_entity_type = _eidr_type(payload)
            if (
                wiki_type != "UNKNOWN"
                and eidr_entity_type != "UNKNOWN"
                and wiki_type != eidr_entity_type
            ):
                row = _error_row(
                    source="eidr",
                    source_record_id=record.source_record_id,
                    code="ENTITY_TYPE_CONFLICT",
                    message="EIDR referent type conflicts with Wikidata type closure",
                    details={
                        "eidrType": eidr_entity_type,
                        "wikidataId": match,
                        "wikidataType": wiki_type,
                    },
                )
                errors[row["error_key"]] = row
                match = None
        eidr_match[record.source_record_id] = match

    referenced_hints: dict[str, set[str]] = {}
    for qid, payload in wikidata.items():
        if wikidata_types[qid] not in MEDIA_ENTITY_TYPES:
            continue
        for prop in CREDIT_PERSON_PROPERTIES:
            for target in _qid_values(payload, prop):
                referenced_hints.setdefault(target, set()).add("PERSON")
        for prop in CREDIT_ORGANIZATION_PROPERTIES:
            for target in _qid_values(payload, prop):
                referenced_hints.setdefault(target, set()).add("ORGANIZATION")

    entities: dict[str, _EntityState] = {}

    def ensure_entity(
        source: str,
        source_id: str,
        entity_type: str,
        attributes: dict[str, Any] | None = None,
    ) -> _EntityState:
        key = _entity_key(source, source_id)
        state = entities.get(key)
        if state is None:
            state = _EntityState(key, entity_type, source, source_id)
            entities[key] = state
        elif state.entity_type == "UNKNOWN" and entity_type != "UNKNOWN":
            state.entity_type = entity_type
        if attributes:
            state.attributes.update(attributes)
        return state

    for qid, payload in wikidata.items():
        entity_type = wikidata_types[qid]
        if entity_type == "UNKNOWN" and qid in referenced_hints:
            entity_type = _choose_type(referenced_hints[qid])
        if entity_type != "UNKNOWN":
            ensure_entity(
                "wikidata",
                qid,
                entity_type,
                {"wikidata": _wikidata_attributes(payload)},
            )

    for record, payload in eidr:
        match = eidr_match[record.source_record_id]
        entity_type = _eidr_type(payload)
        if match is not None:
            matched_type = wikidata_types.get(match, "UNKNOWN")
            state = ensure_entity(
                "wikidata",
                match,
                entity_type if matched_type == "UNKNOWN" else matched_type,
            )
        else:
            state = ensure_entity("eidr", record.source_record_id, entity_type)
        state.attributes.setdefault("eidr", []).append(
            {
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
        )

    source_rows: dict[str, dict[str, Any]] = {}
    names: dict[str, dict[str, Any]] = {}
    identifiers: dict[str, dict[str, Any]] = {}
    relations: dict[str, dict[str, Any]] = {}

    for record, payload in valid:
        if record.source == "wikidata":
            entity_key = _entity_key("wikidata", record.source_record_id)
            if entity_key not in entities:
                continue
            _wikidata_names(payload, entity_key=entity_key, names=names)
            _add_external_id(
                identifiers,
                entity_key=entity_key,
                scheme="wikidata",
                value=record.source_record_id,
                source="wikidata",
                source_record_id=record.source_record_id,
            )
            for scheme, value in _external_ids(payload):
                _add_external_id(
                    identifiers,
                    entity_key=entity_key,
                    scheme=scheme,
                    value=value,
                    source="wikidata",
                    source_record_id=record.source_record_id,
                )
        else:
            match = eidr_match[record.source_record_id]
            entity_key = (
                _entity_key("wikidata", match)
                if match is not None
                else _entity_key("eidr", record.source_record_id)
            )
            for title in payload.get("titles", []):
                if isinstance(title, dict) and isinstance(title.get("value"), str):
                    _add_name(
                        names,
                        entity_key=entity_key,
                        name_type=str(title.get("type") or "TITLE"),
                        language=str(title.get("language") or "und"),
                        value=title["value"],
                        source="eidr",
                        source_record_id=record.source_record_id,
                    )
            _add_external_id(
                identifiers,
                entity_key=entity_key,
                scheme="eidr",
                value=record.source_record_id,
                source="eidr",
                source_record_id=record.source_record_id,
            )
            for alternate in payload.get("alternateIds", []):
                if not isinstance(alternate, dict):
                    continue
                scheme = str(alternate.get("scheme") or "alternate").lower()
                value = _external_value(scheme, alternate.get("value"))
                if value:
                    _add_external_id(
                        identifiers,
                        entity_key=entity_key,
                        scheme=scheme,
                        value=value,
                        source="eidr",
                        source_record_id=record.source_record_id,
                    )
        source_rows[record.record_key] = {
            "record_key": record.record_key,
            "source": record.source,
            "source_record_id": record.source_record_id,
            "source_revision": record.source_revision,
            "modified": record.modified,
            "source_hash": record.source_hash,
            "entity_key": entity_key,
            "payload_json": record.payload_json,
        }

    for qid, payload in wikidata.items():
        subject_key = _entity_key("wikidata", qid)
        subject = entities.get(subject_key)
        if subject is None or subject.entity_type not in MEDIA_ENTITY_TYPES:
            continue
        for prop, relation_type in RELATION_PROPERTIES.items():
            for statement in _statements(payload, prop):
                target = _statement_value(statement)
                if not isinstance(target, str) or _QID.fullmatch(target) is None:
                    continue
                hint = (
                    "PERSON"
                    if prop in CREDIT_PERSON_PROPERTIES
                    else (
                        "ORGANIZATION"
                        if prop in CREDIT_ORGANIZATION_PROPERTIES
                        else wikidata_types.get(target, "UNKNOWN")
                    )
                )
                target_state = ensure_entity("wikidata", target, hint)
                _add_relation(
                    relations,
                    subject_key=subject_key,
                    relation_type=relation_type,
                    object_key=target_state.entity_key,
                    ordinal=_qualifier_ordinal(statement),
                    source="wikidata",
                    source_record_id=qid,
                    attributes={
                        "property": prop,
                        "rank": statement.get("rank", "normal"),
                        "statementId": statement.get("id"),
                        "qualifiers": statement.get("qualifiers") or {},
                    },
                )

    eidr_canonical = {
        record.source_record_id: (
            _entity_key("wikidata", eidr_match[record.source_record_id])
            if eidr_match[record.source_record_id] is not None
            else _entity_key("eidr", record.source_record_id)
        )
        for record, _ in eidr
    }
    for record, payload in eidr:
        subject_key = eidr_canonical[record.source_record_id]
        for relation in payload.get("parentRelations", []):
            if not isinstance(relation, dict):
                continue
            target_id = relation.get("targetEidrId")
            if not isinstance(target_id, str):
                continue
            target_key = eidr_canonical.get(target_id)
            if target_key is None:
                target_state = ensure_entity("eidr", target_id, "UNKNOWN")
                target_key = target_state.entity_key
            _add_relation(
                relations,
                subject_key=subject_key,
                relation_type=str(relation.get("type") or "PART_OF"),
                object_key=target_key,
                ordinal=(
                    str(relation["ordinal"])
                    if relation.get("ordinal") is not None
                    else None
                ),
                source="eidr",
                source_record_id=record.source_record_id,
                attributes={"targetEidrId": target_id},
            )

    entity_rows = {
        key: {
            "entity_key": state.entity_key,
            "entity_type": state.entity_type,
            "canonical_source": state.canonical_source,
            "canonical_source_id": state.canonical_source_id,
            "attributes_json": canonical_json(
                {
                    **state.attributes,
                    **(
                        {
                            "eidr": sorted(
                                state.attributes["eidr"],
                                key=lambda value: str(value.get("id")),
                            )
                        }
                        if isinstance(state.attributes.get("eidr"), list)
                        else {}
                    ),
                }
            ),
        }
        for key, state in entities.items()
    }
    return CuratedRows(
        catalog_source_record=sorted(
            source_rows.values(), key=lambda row: row["record_key"]
        ),
        catalog_entity=sorted(entity_rows.values(), key=lambda row: row["entity_key"]),
        catalog_name=sorted(names.values(), key=lambda row: row["name_key"]),
        catalog_external_identifier=sorted(
            identifiers.values(), key=lambda row: row["identifier_key"]
        ),
        catalog_relation=sorted(
            relations.values(), key=lambda row: row["relation_key"]
        ),
        catalog_ingest_error=sorted(errors.values(), key=lambda row: row["error_key"]),
    )
