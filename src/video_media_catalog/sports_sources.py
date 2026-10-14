"""Silver mappers for pinned Wikidata, openfootball, and StatsBomb sports data."""

from __future__ import annotations

import json
from typing import Any

from video_media_catalog.assertions import SourceNodeRef, ValueType
from video_media_catalog.connector import ConnectorRecordEnvelope, RecordOperation
from video_media_catalog.source_mapper import AssertionBuilder, MappedAssertions
from video_media_catalog.v2_contracts import require_rfc3339

WIKIDATA_SPORTS_MAPPER_ID = "wikidata-sports-source-mapper"
OPENFOOTBALL_MAPPER_ID = "openfootball-source-mapper"
STATSBOMB_MAPPER_ID = "statsbomb-source-mapper"

_SPORTS_TYPES = frozenset(
    {
        "SPORTS_COMPETITION",
        "SPORTS_SEASON",
        "SPORTS_STAGE",
        "SPORTS_EVENT",
        "SPORTS_PARTICIPANT",
        "SPORTS_ENTRY",
        "SPORTS_VENUE",
    }
)

_OPENFOOTBALL_TYPES = {
    "competition": (
        "openfootball-competition",
        "SPORTS_COMPETITION",
    ),
    "season": ("openfootball-season", "SPORTS_SEASON"),
    "stage": ("openfootball-stage", "SPORTS_STAGE"),
    "event": ("openfootball-event", "SPORTS_EVENT"),
    "participant": ("openfootball-team", "SPORTS_PARTICIPANT"),
    "entry": ("openfootball-entry", "SPORTS_ENTRY"),
    "venue": ("openfootball-venue", "SPORTS_VENUE"),
}


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
        raise ValueError("record is not an active supported sports envelope")
    payload = json.loads(envelope.payload_json)
    if not isinstance(payload, dict):
        raise ValueError("sports source payload must be an object")
    return payload


def _supersedes(payload: dict[str, Any], key: str) -> str | None:
    revisions = payload.get("supersedes")
    if not isinstance(revisions, dict):
        return None
    value = revisions.get(key)
    return value if isinstance(value, str) else None


def _add_sports_fields(
    builder: AssertionBuilder,
    payload: dict[str, Any],
) -> None:
    fields = (
        ("discipline", ValueType.STRING, "discipline"),
        ("event_date", ValueType.DATE, "eventDate"),
        ("scheduled_start", ValueType.TIMESTAMP, "scheduledStart"),
        ("event_status", ValueType.STRING, "status"),
        ("round_number", ValueType.STRING, "round"),
        ("score", ValueType.JSON, "score"),
        ("period_score", ValueType.JSON, "periodScores"),
        ("set_score", ValueType.JSON, "setScores"),
        ("placement", ValueType.INTEGER, "placement"),
        ("outcome", ValueType.STRING, "outcome"),
        ("standing", ValueType.JSON, "standings"),
        ("match_statistics", ValueType.JSON, "statistics"),
        ("lineup", ValueType.JSON, "lineups"),
        ("freeze_frame", ValueType.JSON, "freezeFrames"),
    )
    for predicate, value_type, key in fields:
        value = payload.get(key)
        if predicate == "scheduled_start" and isinstance(value, str):
            value = require_rfc3339(value, label="scheduledStart")
        builder.add_field(
            predicate,
            value_type,
            value,
            f"/{key}",
            supersedes_assertion_id=_supersedes(payload, predicate),
        )


def _add_title(
    builder: AssertionBuilder,
    value: object,
    source_path: str,
    *,
    language: str = "und",
) -> None:
    if isinstance(value, str) and value.strip():
        builder.add_field(
            "title",
            ValueType.STRING,
            value.strip(),
            source_path,
            {"language": language, "titleRole": "PRIMARY"},
        )


def map_openfootball_record(
    envelope: ConnectorRecordEnvelope,
) -> MappedAssertions:
    payload = _active_payload(
        envelope,
        source_system_id="openfootball",
        source_product_id="openfootball-git",
    )
    record_type = str(payload.get("type") or "").strip().lower()
    try:
        namespace_id, referent_kind = _OPENFOOTBALL_TYPES[record_type]
    except KeyError as exc:
        raise ValueError(
            f"unsupported openfootball record type: {record_type}"
        ) from exc
    record_id = str(payload.get("id") or "").strip()
    if (
        record_id != envelope.source_record_id
        or envelope.source_namespace_id != namespace_id
    ):
        raise ValueError("openfootball payload identity does not match its envelope")
    node = SourceNodeRef(
        namespace_id=namespace_id,
        source_id=record_id,
        referent_kind=referent_kind,
    )
    builder = AssertionBuilder(
        envelope=envelope,
        source_node=node,
        mapper_id=OPENFOOTBALL_MAPPER_ID,
        mapper_version="1.0.0",
    )
    builder.add_identifier(
        namespace_id,
        record_id,
        "openfootball",
        referent_kind,
        "/id",
    )
    builder.add_entity_type(referent_kind, "/type")
    _add_title(builder, payload.get("name"), "/name")
    if record_type == "event" and not payload.get("name"):
        participants = payload.get("participants")
        if isinstance(participants, list):
            names = [
                str(item.get("name")).strip()
                for item in participants
                if isinstance(item, dict) and item.get("name")
            ]
            _add_title(builder, " vs ".join(names), "/participants")
    _add_sports_fields(builder, payload)

    for key, predicate, target_namespace, target_kind in (
        (
            "competitionId",
            "part_of_competition",
            "openfootball-competition",
            "SPORTS_COMPETITION",
        ),
        (
            "seasonId",
            "part_of_sports_season",
            "openfootball-season",
            "SPORTS_SEASON",
        ),
        ("stageId", "part_of_stage", "openfootball-stage", "SPORTS_STAGE"),
        ("eventId", "part_of", "openfootball-event", "SPORTS_EVENT"),
        (
            "participantId",
            "participant",
            "openfootball-team",
            "SPORTS_PARTICIPANT",
        ),
        ("venueId", "held_at", "openfootball-venue", "SPORTS_VENUE"),
    ):
        value = payload.get(key)
        if value is not None and str(value).strip():
            builder.add_relationship(
                predicate,
                SourceNodeRef(
                    namespace_id=target_namespace,
                    source_id=str(value).strip(),
                    referent_kind=target_kind,
                ),
                f"/{key}",
            )
    participants = payload.get("participants")
    if isinstance(participants, list):
        for index, participant in enumerate(participants):
            if not isinstance(participant, dict) or participant.get("id") is None:
                continue
            builder.add_relationship(
                "participant",
                SourceNodeRef(
                    namespace_id="openfootball-team",
                    source_id=str(participant["id"]),
                    referent_kind="SPORTS_PARTICIPANT",
                ),
                f"/participants/{index}",
                {
                    key: participant[key]
                    for key in ("role", "score", "outcome", "placement")
                    if participant.get(key) is not None
                },
            )
    return builder.build()


def _statsbomb_team(
    payload: dict[str, Any],
    side: str,
) -> tuple[str, str] | None:
    team = payload.get(f"{side}_team")
    if not isinstance(team, dict):
        return None
    team_id = team.get(f"{side}_team_id", team.get("id"))
    name = team.get(f"{side}_team_name", team.get("name"))
    if team_id is None:
        return None
    return str(team_id), str(name or team_id)


def map_statsbomb_match_record(
    envelope: ConnectorRecordEnvelope,
) -> MappedAssertions:
    payload = _active_payload(
        envelope,
        source_system_id="statsbomb",
        source_product_id="statsbomb-open-data",
    )
    match_id = str(payload.get("match_id") or payload.get("id") or "").strip()
    if (
        match_id != envelope.source_record_id
        or envelope.source_namespace_id != "statsbomb-match"
    ):
        raise ValueError("StatsBomb payload identity does not match its envelope")
    node = SourceNodeRef(
        namespace_id="statsbomb-match",
        source_id=match_id,
        referent_kind="SPORTS_EVENT",
    )
    builder = AssertionBuilder(
        envelope=envelope,
        source_node=node,
        mapper_id=STATSBOMB_MAPPER_ID,
        mapper_version="1.0.0",
    )
    builder.add_identifier(
        "statsbomb-match",
        match_id,
        "StatsBomb",
        "SPORTS_EVENT",
        "/match_id",
    )
    builder.add_entity_type("SPORTS_EVENT", "/match_id")
    teams = tuple(
        (side, team)
        for side in ("home", "away")
        if (team := _statsbomb_team(payload, side))
    )
    _add_title(
        builder,
        " vs ".join(team[1] for _, team in teams),
        "/home_team|away_team",
    )
    normalized = dict(payload)
    normalized.setdefault("discipline", "football")
    if payload.get("match_date") is not None:
        normalized.setdefault("eventDate", payload["match_date"])
    if payload.get("home_score") is not None or payload.get("away_score") is not None:
        normalized.setdefault(
            "score",
            {
                "home": payload.get("home_score"),
                "away": payload.get("away_score"),
            },
        )
    normalized.setdefault("status", payload.get("match_status"))
    _add_sports_fields(builder, normalized)

    competition = payload.get("competition")
    if isinstance(competition, dict) and competition.get("competition_id") is not None:
        builder.add_relationship(
            "part_of_competition",
            SourceNodeRef(
                namespace_id="statsbomb-competition",
                source_id=str(competition["competition_id"]),
                referent_kind="SPORTS_COMPETITION",
            ),
            "/competition/competition_id",
        )
    season = payload.get("season")
    if isinstance(season, dict) and season.get("season_id") is not None:
        builder.add_relationship(
            "part_of_sports_season",
            SourceNodeRef(
                namespace_id="statsbomb-season",
                source_id=str(season["season_id"]),
                referent_kind="SPORTS_SEASON",
            ),
            "/season/season_id",
        )
    for side, team in teams:
        builder.add_relationship(
            "participant",
            SourceNodeRef(
                namespace_id="statsbomb-team",
                source_id=team[0],
                referent_kind="SPORTS_PARTICIPANT",
            ),
            f"/{side}_team",
            {"role": side.upper()},
        )
    stadium = payload.get("stadium")
    if isinstance(stadium, dict) and stadium.get("id") is not None:
        builder.add_relationship(
            "held_at",
            SourceNodeRef(
                namespace_id="statsbomb-venue",
                source_id=str(stadium["id"]),
                referent_kind="SPORTS_VENUE",
            ),
            "/stadium/id",
        )
    return builder.build()


def _claim_values(payload: dict[str, Any], property_id: str) -> tuple[Any, ...]:
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


def map_wikidata_sports_record(
    envelope: ConnectorRecordEnvelope,
) -> MappedAssertions:
    payload = _active_payload(
        envelope,
        source_system_id="wikidata",
        source_product_id="wikidata-json-dump",
    )
    entity_type = str(payload.get("entityTypeHint") or "").strip().upper()
    if entity_type not in _SPORTS_TYPES:
        raise ValueError("Wikidata sports record requires a reviewed entityTypeHint")
    qid = str(payload.get("id") or "").strip()
    if (
        qid != envelope.source_record_id
        or envelope.source_namespace_id != "wikidata-item"
    ):
        raise ValueError("Wikidata sports payload identity does not match its envelope")
    node = SourceNodeRef(
        namespace_id="wikidata-item",
        source_id=qid,
        referent_kind=entity_type,
    )
    builder = AssertionBuilder(
        envelope=envelope,
        source_node=node,
        mapper_id=WIKIDATA_SPORTS_MAPPER_ID,
        mapper_version="1.0.0",
    )
    builder.add_identifier("wikidata-item", qid, "Wikidata", entity_type, "/id")
    builder.add_entity_type(entity_type, "/entityTypeHint")
    labels = payload.get("labels")
    if isinstance(labels, dict):
        for language, label in sorted(labels.items()):
            if isinstance(label, dict):
                _add_title(
                    builder,
                    label.get("value"),
                    f"/labels/{language}/value",
                    language=str(language),
                )
    for index, discipline in enumerate(_claim_values(payload, "P641")):
        if isinstance(discipline, str):
            builder.add_field(
                "discipline",
                ValueType.STRING,
                discipline,
                f"/claims/P641/{index}",
                {"vocabulary": "wikidata-item"},
            )
    _add_sports_fields(builder, payload)
    for property_id, predicate, target_kind in (
        ("P276", "held_at", "SPORTS_VENUE"),
        ("P710", "participant", "SPORTS_PARTICIPANT"),
    ):
        for index, target in enumerate(_claim_values(payload, property_id)):
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
    parent = payload.get("parent")
    if isinstance(parent, dict) and parent.get("id") and parent.get("kind"):
        target_kind = str(parent["kind"]).strip().upper()
        predicate = {
            "SPORTS_COMPETITION": "part_of_competition",
            "SPORTS_SEASON": "part_of_sports_season",
            "SPORTS_STAGE": "part_of_stage",
            "SPORTS_EVENT": "part_of",
        }.get(target_kind)
        if predicate is not None:
            builder.add_relationship(
                predicate,
                SourceNodeRef(
                    namespace_id="wikidata-item",
                    source_id=str(parent["id"]),
                    referent_kind=target_kind,
                ),
                "/parent",
            )
    return builder.build()
