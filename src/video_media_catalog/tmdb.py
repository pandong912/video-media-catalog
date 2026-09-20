"""TMDB personal-research registry, rights policy, and source mapper."""

from __future__ import annotations

import json
from typing import Any

from video_media_catalog.assertions import SourceNodeRef, ValueType
from video_media_catalog.connector import ConnectorRecordEnvelope, RecordOperation
from video_media_catalog.rights import PolicyZone, RightsProfile, UsageAction
from video_media_catalog.source_mapper import AssertionBuilder, MappedAssertions
from video_media_catalog.source_registry import (
    RegistryStatus,
    SourceNamespace,
    SourceProduct,
    SourceProductKind,
    SourceSystem,
)

TMDB_SOURCE_SYSTEM_ID = "tmdb"
TMDB_SOURCE_PRODUCT_ID = "tmdb-personal-research"
TMDB_DAILY_SOURCE_PRODUCT_ID = TMDB_SOURCE_PRODUCT_ID
TMDB_API_SOURCE_PRODUCT_ID = TMDB_SOURCE_PRODUCT_ID
TMDB_MOVIE_NAMESPACE_ID = "tmdb-movie"
TMDB_TV_NAMESPACE_ID = "tmdb-tv"
TMDB_PERSON_NAMESPACE_ID = "tmdb-person"
TMDB_DAILY_CONNECTOR_ID = "tmdb-daily-id-export"
TMDB_CHANGES_CONNECTOR_ID = "tmdb-changes-detail"
TMDB_POLICY_ID = "tmdb-personal-noncommercial"
TMDB_FILES_ORIGIN = "https://files.tmdb.org"
TMDB_API_ORIGIN = "https://api.themoviedb.org"

TMDB_ENTITY_KINDS = ("movie", "tv", "person")
_NAMESPACE_BY_KIND = {
    "movie": TMDB_MOVIE_NAMESPACE_ID,
    "tv": TMDB_TV_NAMESPACE_ID,
    "person": TMDB_PERSON_NAMESPACE_ID,
}
_TYPE_BY_KIND = {
    "movie": "MOVIE",
    "tv": "TV_SERIES",
    "person": "PERSON",
}
_REFERENT_BY_KIND = {
    "movie": "MOVIE",
    "tv": "SERIES",
    "person": "PERSON",
}


def tmdb_rights_profile() -> RightsProfile:
    return RightsProfile(
        policy_id=TMDB_POLICY_ID,
        policy_version="2026-09-20",
        zone=PolicyZone.RESEARCH_PRIVATE,
        license_id="TMDB-API-Terms-NonCommercial",
        terms_url="https://www.themoviedb.org/api-terms-of-use",
        permissions=(
            UsageAction.STORE,
            UsageAction.TRANSFORM,
            UsageAction.DISPLAY,
            UsageAction.SEARCH,
            UsageAction.DERIVE,
        ),
        audiences=("personal-research",),
        purposes=("personal-research",),
        territories=("*",),
        attribution_text=(
            "This product uses the TMDB API but is not endorsed or certified by TMDB."
        ),
        purge_on_termination=True,
        notes=(
            "Non-commercial personal research only. UI use also requires an approved "
            "TMDB logo in About/Credits. Image references remain subject to separate "
            "asset rights review; this profile does not authorize image redistribution."
        ),
    )


def tmdb_registry_entries() -> tuple[
    SourceSystem,
    SourceProduct,
    tuple[SourceNamespace, ...],
]:
    system = SourceSystem(
        source_system_id=TMDB_SOURCE_SYSTEM_ID,
        name="The Movie Database (TMDB)",
        operator="TMDB",
        homepage="https://www.themoviedb.org/",
        status=RegistryStatus.ACTIVE,
    )
    product = SourceProduct(
        source_product_id=TMDB_SOURCE_PRODUCT_ID,
        source_system_id=TMDB_SOURCE_SYSTEM_ID,
        name="TMDB API and daily ID exports",
        kind=SourceProductKind.PLATFORM_API,
        policy_id=TMDB_POLICY_ID,
        connector_id="tmdb-replayable-capture",
        documentation_url="https://developer.themoviedb.org/docs/getting-started",
    )
    namespaces = tuple(
        SourceNamespace(
            namespace_id=_NAMESPACE_BY_KIND[kind],
            source_product_id=TMDB_SOURCE_PRODUCT_ID,
            issuer="TMDB",
            referent_kinds=(_REFERENT_BY_KIND[kind],),
            identifier_pattern=r"[1-9][0-9]*",
        )
        for kind in TMDB_ENTITY_KINDS
    )
    return system, product, namespaces


def _node(kind: str, source_id: str) -> SourceNodeRef:
    try:
        return SourceNodeRef(
            namespace_id=_NAMESPACE_BY_KIND[kind],
            source_id=source_id,
            referent_kind=_REFERENT_BY_KIND[kind],
        )
    except KeyError as exc:
        raise ValueError(f"unsupported TMDB entity kind: {kind}") from exc


def _add_identifier_fields(
    builder: AssertionBuilder,
    detail: dict[str, Any],
    *,
    kind: str,
) -> None:
    values: dict[str, tuple[Any, str]] = {}
    external = detail.get("external_ids")
    if isinstance(external, dict):
        values.update(
            {
                key: (value, f"/detail/external_ids/{key}")
                for key, value in external.items()
            }
        )
    if detail.get("imdb_id"):
        values["imdb_id"] = (detail["imdb_id"], "/detail/imdb_id")
    specs = {
        "imdb_id": (
            "imdb-name" if kind == "person" else "imdb-title",
            "IMDb",
        ),
        "wikidata_id": ("wikidata-item", "Wikidata"),
        "tvdb_id": ("thetvdb-series", "TheTVDB"),
    }
    for field, (namespace, issuer) in specs.items():
        value, source_path = values.get(field, (None, ""))
        if isinstance(value, str | int) and not isinstance(value, bool) and value != "":
            builder.add_identifier(
                namespace,
                value,
                issuer,
                builder.source_node.referent_kind,
                source_path,
            )


def _add_translations(
    builder: AssertionBuilder,
    detail: dict[str, Any],
) -> None:
    translations = detail.get("translations")
    if not isinstance(translations, dict) or not isinstance(
        translations.get("translations"), list
    ):
        return
    for index, translation in enumerate(translations["translations"]):
        if not isinstance(translation, dict) or not isinstance(
            translation.get("data"), dict
        ):
            continue
        language = translation.get("iso_639_1") or "und"
        region = translation.get("iso_3166_1")
        data = translation["data"]
        title_key = "title" if data.get("title") else "name"
        title = data.get(title_key)
        builder.add_field(
            "title",
            ValueType.STRING,
            title,
            f"/detail/translations/translations/{index}/data/{title_key}",
            {
                "language": language,
                "region": region,
                "titleRole": "TRANSLATION",
            },
        )
        for key, predicate in (
            ("overview", "description"),
            ("biography", "biography"),
            ("tagline", "tagline"),
        ):
            builder.add_field(
                predicate,
                ValueType.STRING,
                data.get(key),
                f"/detail/translations/translations/{index}/data/{key}",
                {"language": language, "region": region},
            )


def _add_credits(
    builder: AssertionBuilder,
    detail: dict[str, Any],
) -> None:
    combined = detail.get("combined_credits")
    if isinstance(combined, dict):
        for group, predicate in (
            ("cast", "PERFORMED_IN"),
            ("crew", "WORKED_ON"),
        ):
            values = combined.get(group)
            if not isinstance(values, list):
                continue
            for index, credit in enumerate(values):
                if (
                    not isinstance(credit, dict)
                    or not isinstance(credit.get("id"), int)
                    or credit.get("media_type") not in {"movie", "tv"}
                ):
                    continue
                builder.add_relationship(
                    predicate,
                    _node(str(credit["media_type"]), str(credit["id"])),
                    f"/detail/combined_credits/{group}/{index}",
                    {
                        "creditId": credit.get("credit_id"),
                        "character": credit.get("character"),
                        "job": credit.get("job"),
                        "department": credit.get("department"),
                    },
                )
        return
    credits = detail.get("credits")
    if not isinstance(credits, dict):
        return
    for group, default_predicate in (("cast", "CAST_MEMBER"), ("crew", "CREDITED")):
        values = credits.get(group)
        if not isinstance(values, list):
            continue
        for index, credit in enumerate(values):
            if not isinstance(credit, dict) or not isinstance(credit.get("id"), int):
                continue
            predicate = default_predicate
            if group == "crew":
                predicate = {
                    "Director": "DIRECTED_BY",
                    "Writer": "WRITTEN_BY",
                    "Screenplay": "WRITTEN_BY",
                    "Producer": "PRODUCED_BY",
                    "Executive Producer": "PRODUCED_BY",
                    "Director of Photography": "DIRECTOR_OF_PHOTOGRAPHY",
                    "Editor": "FILM_EDITOR",
                    "Original Music Composer": "COMPOSED_BY",
                }.get(str(credit.get("job") or ""), "CREDITED")
            builder.add_relationship(
                predicate,
                _node("person", str(credit["id"])),
                f"/detail/credits/{group}/{index}",
                {
                    "creditId": credit.get("credit_id"),
                    "order": credit.get("order"),
                    "character": credit.get("character"),
                    "job": credit.get("job"),
                    "department": credit.get("department")
                    or credit.get("known_for_department"),
                },
            )


def _add_images(
    builder: AssertionBuilder,
    detail: dict[str, Any],
) -> int:
    images: list[tuple[str, dict[str, Any], str]] = []
    for key, image_type in (
        ("poster_path", "poster"),
        ("backdrop_path", "backdrop"),
        ("profile_path", "profile"),
    ):
        if isinstance(detail.get(key), str):
            images.append((image_type, {"file_path": detail[key]}, f"/detail/{key}"))
    appended = detail.get("images")
    if isinstance(appended, dict):
        for group, image_type in (
            ("posters", "poster"),
            ("backdrops", "backdrop"),
            ("profiles", "profile"),
        ):
            values = appended.get(group)
            if isinstance(values, list):
                images.extend(
                    (
                        image_type,
                        value,
                        f"/detail/images/{group}/{index}/file_path",
                    )
                    for index, value in enumerate(values)
                    if isinstance(value, dict)
                )
    seen: set[tuple[str, str]] = set()
    for image_type, image, source_path in images:
        path = image.get("file_path")
        if not isinstance(path, str) or (image_type, path) in seen:
            continue
        seen.add((image_type, path))
        builder.add_field(
            "image_path",
            ValueType.STRING,
            path,
            source_path,
            {
                "imageType": image_type,
                "language": image.get("iso_639_1"),
                "aspectRatio": image.get("aspect_ratio"),
                "width": image.get("width"),
                "height": image.get("height"),
                "assetReviewRequired": True,
            },
        )
    return len(seen)


def map_tmdb_record(envelope: ConnectorRecordEnvelope) -> MappedAssertions:
    if (
        envelope.source_system_id != TMDB_SOURCE_SYSTEM_ID
        or envelope.source_product_id != TMDB_SOURCE_PRODUCT_ID
    ):
        raise ValueError("record is not a TMDB envelope")
    kind = {
        TMDB_MOVIE_NAMESPACE_ID: "movie",
        TMDB_TV_NAMESPACE_ID: "tv",
        TMDB_PERSON_NAMESPACE_ID: "person",
    }.get(envelope.source_namespace_id)
    if kind is None:
        raise ValueError("TMDB envelope has an unknown namespace")
    node = _node(kind, envelope.source_record_id)
    builder = AssertionBuilder(
        envelope=envelope,
        source_node=node,
        mapper_id="tmdb-personal-research-mapper",
        mapper_version="1.0.0",
    )
    if envelope.operation != RecordOperation.UPSERT:
        return builder.build()
    if envelope.payload_json is None:
        raise ValueError("TMDB UPSERT requires an inline payload")
    payload = json.loads(envelope.payload_json)
    if not isinstance(payload, dict) or payload.get("entityKind") != kind:
        raise ValueError("TMDB payload kind does not match its envelope")
    record = payload.get("record")
    detail = payload.get("detail")
    value = detail if isinstance(detail, dict) else record
    if not isinstance(value, dict) or str(value.get("id")) != envelope.source_record_id:
        raise ValueError("TMDB payload identity does not match its envelope")

    builder.add_identifier(
        node.namespace_id,
        node.source_id,
        "TMDB",
        node.referent_kind,
        "/detail/id" if isinstance(detail, dict) else "/record/id",
    )
    builder.add_entity_type(_TYPE_BY_KIND[kind], "/entityKind")
    if isinstance(record, dict):
        title_key = next(
            (
                key
                for key in ("original_title", "original_name", "name")
                if record.get(key)
            ),
            "name",
        )
        title = record.get(title_key)
        builder.add_field(
            "title",
            ValueType.STRING,
            title,
            f"/record/{title_key}",
            {"language": "und", "titleRole": "ORIGINAL"},
        )
        builder.add_field(
            "popularity",
            ValueType.DECIMAL,
            record.get("popularity"),
            "/record/popularity",
            {"metric": "tmdb-popularity"},
        )
        if isinstance(record.get("adult"), bool):
            builder.add_field(
                "is_adult",
                ValueType.BOOLEAN,
                record["adult"],
                "/record/adult",
            )
        return builder.build()

    title_key = next(
        (
            key
            for key in ("title", "name", "original_title", "original_name")
            if detail.get(key)
        ),
        "name",
    )
    title = detail.get(title_key)
    builder.add_field(
        "title",
        ValueType.STRING,
        title,
        f"/detail/{title_key}",
        {
            "language": detail.get("original_language") or "und",
            "titleRole": "PRIMARY",
        },
    )
    original_key = "original_title" if detail.get("original_title") else "original_name"
    original = detail.get(original_key)
    if original and original != title:
        builder.add_field(
            "title",
            ValueType.STRING,
            original,
            f"/detail/{original_key}",
            {
                "language": detail.get("original_language") or "und",
                "titleRole": "ORIGINAL",
            },
        )
    for predicate, key in (
        ("description", "overview"),
        ("biography", "biography"),
        ("tagline", "tagline"),
        ("status", "status"),
        ("place_of_birth", "place_of_birth"),
        ("known_for_department", "known_for_department"),
    ):
        builder.add_field(
            predicate,
            ValueType.STRING,
            detail.get(key),
            f"/detail/{key}",
        )
    episode_runtimes = detail.get("episode_run_time")
    if isinstance(episode_runtimes, list):
        for index, runtime in enumerate(episode_runtimes):
            builder.add_field(
                "runtime_minutes",
                ValueType.INTEGER,
                runtime,
                f"/detail/episode_run_time/{index}",
                {"scope": "episode"},
            )
    if isinstance(detail.get("adult"), bool):
        builder.add_field(
            "is_adult",
            ValueType.BOOLEAN,
            detail["adult"],
            "/detail/adult",
        )
    for predicate, key in (
        ("release_date", "release_date"),
        ("first_air_date", "first_air_date"),
        ("last_air_date", "last_air_date"),
        ("birthday", "birthday"),
        ("deathday", "deathday"),
    ):
        builder.add_field(
            predicate,
            ValueType.DATE,
            detail.get(key),
            f"/detail/{key}",
        )
    for predicate, key in (
        ("runtime_minutes", "runtime"),
        ("episode_count", "number_of_episodes"),
        ("season_count", "number_of_seasons"),
        ("gender", "gender"),
    ):
        builder.add_field(
            predicate,
            ValueType.INTEGER,
            detail.get(key),
            f"/detail/{key}",
        )
    for predicate, key, metric in (
        ("rating_average", "vote_average", "tmdb-user-rating"),
        ("rating_count", "vote_count", "tmdb-user-rating"),
        ("popularity", "popularity", "tmdb-popularity"),
    ):
        value_type = ValueType.INTEGER if key == "vote_count" else ValueType.DECIMAL
        builder.add_field(
            predicate,
            value_type,
            detail.get(key),
            f"/detail/{key}",
            {"metric": metric},
        )
    for key, predicate, value_key in (
        ("genres", "genre", "name"),
        ("production_countries", "country", "iso_3166_1"),
        ("spoken_languages", "language", "iso_639_1"),
    ):
        values = detail.get(key)
        if not isinstance(values, list):
            continue
        for index, item in enumerate(values):
            if isinstance(item, dict):
                builder.add_field(
                    predicate,
                    ValueType.STRING,
                    item.get(value_key),
                    f"/detail/{key}/{index}/{value_key}",
                    {"vocabulary": "tmdb"},
                )
    for index, alias in enumerate(detail.get("also_known_as") or []):
        builder.add_field(
            "title",
            ValueType.STRING,
            alias,
            f"/detail/also_known_as/{index}",
            {"language": "und", "titleRole": "ALIAS"},
        )
    _add_identifier_fields(builder, detail, kind=kind)
    _add_translations(builder, detail)
    _add_credits(builder, detail)
    omitted_assets = _add_images(builder, detail)
    return builder.build(omitted_asset_count=omitted_assets)
