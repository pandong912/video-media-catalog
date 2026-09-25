"""Shared source-mapping constants."""

from __future__ import annotations

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

DOUBAN_WORK_NAMESPACE_ID = "douban-work"
DOUBAN_PERSON_NAMESPACE_ID = "douban-person"
DOUBAN_LEGACY_NAMESPACE_ID = "douban-subject"
DOUBAN_LEGACY_SCHEME = "douban"

DOUBAN_EXTERNAL_ID_PROPERTIES = {
    "P4529": (DOUBAN_WORK_NAMESPACE_ID, "EDITORIAL_WORK"),
    "P5284": (DOUBAN_PERSON_NAMESPACE_ID, "AGENT"),
}

EXTERNAL_ID_PROPERTIES = {
    "P345": "imdb",
    "P2704": "eidr",
    **{prop: DOUBAN_LEGACY_SCHEME for prop in DOUBAN_EXTERNAL_ID_PROPERTIES},
}
