"""Official IMDb non-commercial TSV contracts, parser, and Silver mapper."""

from __future__ import annotations

import csv
import gzip
import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from video_media_catalog.assertions import SourceNodeRef, ValueType
from video_media_catalog.connector import ConnectorRecordEnvelope, RecordOperation
from video_media_catalog.identity_resolution import referent_kind_for_entity_type
from video_media_catalog.rights import PolicyZone, RightsProfile, UsageAction
from video_media_catalog.source_mapper import AssertionBuilder, MappedAssertions
from video_media_catalog.source_registry import (
    RegistryStatus,
    SourceNamespace,
    SourceProduct,
    SourceProductKind,
    SourceSystem,
)

IMDB_SOURCE_SYSTEM_ID = "imdb"
IMDB_SOURCE_PRODUCT_ID = "imdb-non-commercial-datasets"
IMDB_RECORD_NAMESPACE_ID = "imdb-record"
IMDB_TITLE_NAMESPACE_ID = "imdb-title"
IMDB_NAME_NAMESPACE_ID = "imdb-name"
IMDB_CONNECTOR_ID = "imdb-official-tsv"
IMDB_POLICY_ID = "imdb-research-noncommercial"
IMDB_DATASET_ORIGIN = "https://datasets.imdbws.com"

IMDB_DATASET_FILES = (
    "title.basics.tsv.gz",
    "title.akas.tsv.gz",
    "title.episode.tsv.gz",
    "title.crew.tsv.gz",
    "title.principals.tsv.gz",
    "title.ratings.tsv.gz",
    "name.basics.tsv.gz",
)
IMDB_DATASET_COLUMNS = {
    "title.basics.tsv.gz": (
        "tconst",
        "titleType",
        "primaryTitle",
        "originalTitle",
        "isAdult",
        "startYear",
        "endYear",
        "runtimeMinutes",
        "genres",
    ),
    "title.akas.tsv.gz": (
        "titleId",
        "ordering",
        "title",
        "region",
        "language",
        "types",
        "attributes",
        "isOriginalTitle",
    ),
    "title.episode.tsv.gz": (
        "tconst",
        "parentTconst",
        "seasonNumber",
        "episodeNumber",
    ),
    "title.crew.tsv.gz": ("tconst", "directors", "writers"),
    "title.principals.tsv.gz": (
        "tconst",
        "ordering",
        "nconst",
        "category",
        "job",
        "characters",
    ),
    "title.ratings.tsv.gz": ("tconst", "averageRating", "numVotes"),
    "name.basics.tsv.gz": (
        "nconst",
        "primaryName",
        "birthYear",
        "deathYear",
        "primaryProfession",
        "knownForTitles",
    ),
}

_TITLE_TYPE_MAP = {
    "movie": "MOVIE",
    "short": "MOVIE",
    "tvmovie": "MOVIE",
    "video": "MOVIE",
    "tvseries": "TV_SERIES",
    "tvminiseries": "TV_SERIES",
    "tvseason": "TV_SEASON",
    "tvepisode": "TV_EPISODE",
}


def imdb_rights_profile() -> RightsProfile:
    return RightsProfile(
        policy_id=IMDB_POLICY_ID,
        policy_version="2026-09-20",
        zone=PolicyZone.RESEARCH_PRIVATE,
        license_id="IMDb-Non-Commercial-Datasets",
        terms_url=(
            "https://help.imdb.com/article/imdb/general-information/"
            "can-i-use-imdb-data-in-my-software/G5JTRESSHJBBHTGX"
        ),
        permissions=(
            UsageAction.STORE,
            UsageAction.TRANSFORM,
            UsageAction.DISPLAY,
            UsageAction.SEARCH,
            UsageAction.DERIVE,
        ),
        audiences=("research",),
        purposes=("research",),
        territories=("*",),
        attribution_text=(
            "Information courtesy of IMDb (https://www.imdb.com). Used with permission."
        ),
        purge_on_termination=True,
        notes=(
            "Only official datasets.imdbws.com TSV files may be acquired. "
            "Website scraping, redistribution, export, resale, and shared database "
            "publication are not permitted by this profile."
        ),
    )


def imdb_registry_entries() -> tuple[
    SourceSystem,
    SourceProduct,
    tuple[SourceNamespace, ...],
]:
    system = SourceSystem(
        source_system_id=IMDB_SOURCE_SYSTEM_ID,
        name="IMDb",
        operator="IMDb.com, Inc.",
        homepage="https://www.imdb.com/",
        status=RegistryStatus.ACTIVE,
    )
    product = SourceProduct(
        source_product_id=IMDB_SOURCE_PRODUCT_ID,
        source_system_id=IMDB_SOURCE_SYSTEM_ID,
        name="IMDb Non-Commercial Datasets",
        kind=SourceProductKind.COMMERCIAL_FEED,
        policy_id=IMDB_POLICY_ID,
        connector_id=IMDB_CONNECTOR_ID,
        documentation_url="https://developer.imdb.com/non-commercial-datasets/",
    )
    namespaces = (
        SourceNamespace(
            namespace_id=IMDB_RECORD_NAMESPACE_ID,
            source_product_id=IMDB_SOURCE_PRODUCT_ID,
            issuer="IMDb",
            referent_kinds=(
                "EDITORIAL_WORK",
                "SERIES",
                "SEASON",
                "EPISODE",
                "PERSON",
            ),
        ),
        SourceNamespace(
            namespace_id=IMDB_TITLE_NAMESPACE_ID,
            source_product_id=IMDB_SOURCE_PRODUCT_ID,
            issuer="IMDb",
            referent_kinds=("EDITORIAL_WORK", "SERIES", "SEASON", "EPISODE"),
            identifier_pattern=r"tt[0-9]{7,12}",
            case_sensitive=False,
        ),
        SourceNamespace(
            namespace_id=IMDB_NAME_NAMESPACE_ID,
            source_product_id=IMDB_SOURCE_PRODUCT_ID,
            issuer="IMDb",
            referent_kinds=("PERSON",),
            identifier_pattern=r"nm[0-9]{7,12}",
            case_sensitive=False,
        ),
    )
    return system, product, namespaces


def iter_imdb_rows(path: Path, dataset: str) -> Iterator[dict[str, str | None]]:
    """Stream one official gzip TSV and reject silent schema drift."""

    expected = IMDB_DATASET_COLUMNS.get(dataset)
    if expected is None:
        raise ValueError(f"unsupported IMDb dataset: {dataset}")
    with gzip.open(path, mode="rt", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        if tuple(reader.fieldnames or ()) != expected:
            raise ValueError(f"IMDb {dataset} columns do not match the contract")
        for line_number, row in enumerate(reader, start=2):
            if None in row or any(value is None for value in row.values()):
                raise ValueError(f"IMDb {dataset}:{line_number} is malformed")
            normalized = {
                key: None if value == r"\N" else value for key, value in row.items()
            }
            yield normalized


def imdb_record_id(dataset: str, row: dict[str, Any]) -> str:
    prefix = dataset.removesuffix(".tsv.gz")
    if dataset == "title.akas.tsv.gz":
        parts = (row.get("titleId"), row.get("ordering"))
    elif dataset == "title.principals.tsv.gz":
        parts = (row.get("tconst"), row.get("ordering"))
    elif dataset == "name.basics.tsv.gz":
        parts = (row.get("nconst"),)
    else:
        parts = (row.get("tconst"),)
    if any(not isinstance(item, str) or not item for item in parts):
        raise ValueError(f"IMDb {dataset} row is missing its primary key")
    return f"{prefix}:{':'.join(parts)}"


def _split(value: Any) -> list[str]:
    if not isinstance(value, str) or not value:
        return []
    return [item for item in value.split(",") if item]


def _integer(value: Any) -> int | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _subject_for_row(
    dataset: str,
    row: dict[str, Any],
) -> tuple[SourceNodeRef, str | None]:
    if dataset == "name.basics.tsv.gz":
        source_id = row.get("nconst")
        if not isinstance(source_id, str):
            raise ValueError("IMDb name row is missing nconst")
        return (
            SourceNodeRef(
                namespace_id=IMDB_NAME_NAMESPACE_ID,
                source_id=source_id.lower(),
                referent_kind="PERSON",
            ),
            "PERSON",
        )
    source_id = (
        row.get("titleId") if dataset == "title.akas.tsv.gz" else row.get("tconst")
    )
    if not isinstance(source_id, str):
        raise ValueError("IMDb title row is missing tconst/titleId")
    entity_type = (
        _TITLE_TYPE_MAP.get(str(row.get("titleType") or "").lower())
        if dataset == "title.basics.tsv.gz"
        else ("TV_EPISODE" if dataset == "title.episode.tsv.gz" else None)
    )
    return (
        SourceNodeRef(
            namespace_id=IMDB_TITLE_NAMESPACE_ID,
            source_id=source_id.lower(),
            referent_kind="EDITORIAL_WORK",
        ),
        entity_type,
    )


def map_imdb_record(envelope: ConnectorRecordEnvelope) -> MappedAssertions:
    if (
        envelope.source_system_id != IMDB_SOURCE_SYSTEM_ID
        or envelope.source_product_id != IMDB_SOURCE_PRODUCT_ID
        or envelope.source_namespace_id != IMDB_RECORD_NAMESPACE_ID
        or envelope.operation != RecordOperation.UPSERT
        or envelope.payload_json is None
    ):
        raise ValueError("record is not an active IMDb TSV envelope")
    payload = json.loads(envelope.payload_json)
    if not isinstance(payload, dict):
        raise ValueError("IMDb envelope payload must be an object")
    dataset = payload.get("dataset")
    row = payload.get("row")
    if dataset not in IMDB_DATASET_COLUMNS or not isinstance(row, dict):
        raise ValueError("IMDb envelope payload has an unknown dataset")
    if imdb_record_id(str(dataset), row) != envelope.source_record_id:
        raise ValueError("IMDb payload identity does not match its envelope")

    node, entity_type = _subject_for_row(str(dataset), row)
    identifier_referent_kind = (
        referent_kind_for_entity_type(entity_type)
        if entity_type is not None
        else node.referent_kind
    )
    builder = AssertionBuilder(
        envelope=envelope,
        source_node=node,
        mapper_id="imdb-official-tsv-mapper",
        mapper_version="1.0.0",
    )
    builder.add_identifier(
        node.namespace_id,
        node.source_id,
        "IMDb",
        identifier_referent_kind,
        (
            "/row/nconst"
            if node.namespace_id == IMDB_NAME_NAMESPACE_ID
            else ("/row/titleId" if dataset == "title.akas.tsv.gz" else "/row/tconst")
        ),
    )
    if entity_type is not None:
        builder.add_entity_type(entity_type, "/row/titleType")

    if dataset == "title.basics.tsv.gz":
        builder.add_field(
            "title",
            ValueType.STRING,
            row.get("primaryTitle"),
            "/row/primaryTitle",
            {"language": "und", "titleRole": "PRIMARY"},
        )
        builder.add_field(
            "title",
            ValueType.STRING,
            row.get("originalTitle"),
            "/row/originalTitle",
            {"language": "und", "titleRole": "ORIGINAL"},
        )
        if row.get("isAdult") in {"0", "1"}:
            builder.add_field(
                "is_adult",
                ValueType.BOOLEAN,
                row["isAdult"] == "1",
                "/row/isAdult",
            )
        for predicate, key in (
            ("release_year", "startYear"),
            ("end_year", "endYear"),
        ):
            builder.add_field(
                predicate,
                ValueType.DATE,
                row.get(key),
                f"/row/{key}",
            )
        builder.add_field(
            "runtime_minutes",
            ValueType.INTEGER,
            _integer(row.get("runtimeMinutes")),
            "/row/runtimeMinutes",
        )
        for index, genre in enumerate(_split(row.get("genres"))):
            builder.add_field(
                "genre",
                ValueType.STRING,
                genre,
                f"/row/genres/{index}",
                {"vocabulary": "imdb"},
            )
    elif dataset == "title.akas.tsv.gz":
        builder.add_field(
            "title",
            ValueType.STRING,
            row.get("title"),
            "/row/title",
            {
                "language": row.get("language") or "und",
                "region": row.get("region"),
                "titleRole": (
                    "ORIGINAL" if row.get("isOriginalTitle") == "1" else "ALIAS"
                ),
                "types": _split(row.get("types")),
                "attributes": _split(row.get("attributes")),
            },
        )
    elif dataset == "title.episode.tsv.gz":
        parent = row.get("parentTconst")
        if isinstance(parent, str):
            builder.add_relationship(
                "PART_OF_SERIES",
                SourceNodeRef(
                    namespace_id=IMDB_TITLE_NAMESPACE_ID,
                    source_id=parent.lower(),
                    referent_kind="EDITORIAL_WORK",
                ),
                "/row/parentTconst",
                {
                    "seasonNumber": _integer(row.get("seasonNumber")),
                    "episodeNumber": _integer(row.get("episodeNumber")),
                },
            )
        builder.add_field(
            "season_number",
            ValueType.INTEGER,
            _integer(row.get("seasonNumber")),
            "/row/seasonNumber",
        )
        builder.add_field(
            "episode_number",
            ValueType.INTEGER,
            _integer(row.get("episodeNumber")),
            "/row/episodeNumber",
        )
    elif dataset == "title.crew.tsv.gz":
        for key, predicate in (("directors", "DIRECTED_BY"), ("writers", "WRITTEN_BY")):
            for index, name_id in enumerate(_split(row.get(key))):
                builder.add_relationship(
                    predicate,
                    SourceNodeRef(
                        namespace_id=IMDB_NAME_NAMESPACE_ID,
                        source_id=name_id.lower(),
                        referent_kind="PERSON",
                    ),
                    f"/row/{key}/{index}",
                )
    elif dataset == "title.principals.tsv.gz":
        name_id = row.get("nconst")
        if isinstance(name_id, str):
            predicate = {
                "actor": "CAST_MEMBER",
                "actress": "CAST_MEMBER",
                "director": "DIRECTED_BY",
                "writer": "WRITTEN_BY",
                "producer": "PRODUCED_BY",
                "composer": "COMPOSED_BY",
                "cinematographer": "DIRECTOR_OF_PHOTOGRAPHY",
                "editor": "FILM_EDITOR",
                "archive_footage": "ARCHIVE_FOOTAGE",
                "archive_sound": "ARCHIVE_SOUND",
                "self": "SELF",
            }.get(str(row.get("category") or "").lower(), "CREDITED")
            characters = row.get("characters")
            parsed_characters: Any = []
            if isinstance(characters, str):
                try:
                    parsed_characters = json.loads(characters)
                except json.JSONDecodeError:
                    parsed_characters = [characters]
            builder.add_relationship(
                predicate,
                SourceNodeRef(
                    namespace_id=IMDB_NAME_NAMESPACE_ID,
                    source_id=name_id.lower(),
                    referent_kind="PERSON",
                ),
                "/row/nconst",
                {
                    "ordering": _integer(row.get("ordering")),
                    "category": row.get("category"),
                    "job": row.get("job"),
                    "characters": parsed_characters,
                },
            )
    elif dataset == "title.ratings.tsv.gz":
        builder.add_field(
            "rating_average",
            ValueType.DECIMAL,
            row.get("averageRating"),
            "/row/averageRating",
            {"scale": 10, "metric": "imdb-user-rating"},
        )
        builder.add_field(
            "rating_count",
            ValueType.INTEGER,
            _integer(row.get("numVotes")),
            "/row/numVotes",
            {"metric": "imdb-user-rating"},
        )
    elif dataset == "name.basics.tsv.gz":
        builder.add_field(
            "title",
            ValueType.STRING,
            row.get("primaryName"),
            "/row/primaryName",
            {"language": "und", "titleRole": "PRIMARY"},
        )
        for predicate, key in (
            ("birth_year", "birthYear"),
            ("death_year", "deathYear"),
        ):
            builder.add_field(
                predicate,
                ValueType.DATE,
                row.get(key),
                f"/row/{key}",
            )
        for index, profession in enumerate(_split(row.get("primaryProfession"))):
            builder.add_field(
                "profession",
                ValueType.STRING,
                profession,
                f"/row/primaryProfession/{index}",
                {"vocabulary": "imdb"},
            )
        for index, title_id in enumerate(_split(row.get("knownForTitles"))):
            builder.add_relationship(
                "KNOWN_FOR",
                SourceNodeRef(
                    namespace_id=IMDB_TITLE_NAMESPACE_ID,
                    source_id=title_id.lower(),
                    referent_kind="EDITORIAL_WORK",
                ),
                f"/row/knownForTitles/{index}",
            )
    return builder.build()
