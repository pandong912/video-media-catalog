"""Versioned pipeline constants shared by extraction and commit stages."""

from __future__ import annotations

import hashlib

LANDING_SCHEMA_VERSION = "1.0"
CURATED_SCHEMA_VERSION = "1.0"
CONTROL_SCHEMA_VERSION = "1.0"

STAGE = "media-catalog-commit"
PRODUCER = "video-media-catalog-spark/1.0.0"
ALGORITHM_SPEC_ID = "media-catalog-wikidata-eidr-v1"
ALGORITHM_DIGEST = (
    "sha256:" + hashlib.sha256(ALGORITHM_SPEC_ID.encode("utf-8")).hexdigest()
)

RELEVANT_WIKIDATA_PROPERTIES = frozenset(
    {
        "P31",
        "P279",
        "P1476",
        "P577",
        "P2047",
        "P364",
        "P495",
        "P136",
        "P179",
        "P361",
        "P4908",
        "P57",
        "P58",
        "P161",
        "P162",
        "P272",
        "P344",
        "P1040",
        "P725",
        "P345",
        "P2704",
        "P4529",
        "P5284",
        "P1545",
        "P155",
        "P156",
        "P1113",
        "P2437",
    }
)

ENTITY_TYPE_SEEDS = {
    "Q11424": "MOVIE",
    "Q5398426": "TV_SERIES",
    "Q3464665": "TV_SEASON",
    "Q21191270": "TV_EPISODE",
    "Q5": "PERSON",
    "Q43229": "ORGANIZATION",
}

MEDIA_ENTITY_TYPES = frozenset({"MOVIE", "TV_SERIES", "TV_SEASON", "TV_EPISODE"})

CREDIT_PERSON_PROPERTIES = {
    "P57": "DIRECTED_BY",
    "P58": "WRITTEN_BY",
    "P161": "CAST_MEMBER",
    "P162": "PRODUCED_BY",
    "P344": "DIRECTOR_OF_PHOTOGRAPHY",
    "P1040": "FILM_EDITOR",
    "P725": "VOICE_ACTOR",
}
CREDIT_ORGANIZATION_PROPERTIES = {"P272": "PRODUCTION_COMPANY"}

RELATION_PROPERTIES = {
    **CREDIT_PERSON_PROPERTIES,
    **CREDIT_ORGANIZATION_PROPERTIES,
    "P179": "PART_OF_SERIES",
    "P361": "PART_OF",
    "P4908": "SEASON",
    "P155": "FOLLOWS",
    "P156": "FOLLOWED_BY",
}

EXTERNAL_ID_PROPERTIES = {
    "P345": "imdb",
    "P2704": "eidr",
    "P4529": "douban",
    "P5284": "douban",
}

CURATED_TABLE_KEYS = {
    "catalog_source_record": "record_key",
    "catalog_entity": "entity_key",
    "catalog_name": "name_key",
    "catalog_external_identifier": "identifier_key",
    "catalog_relation": "relation_key",
    "catalog_ingest_error": "error_key",
}
