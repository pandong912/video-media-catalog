"""Europeana OAI source registry, metadata rights, and Silver mapper."""

from __future__ import annotations

import json
import re
from typing import Any
from urllib.parse import urlsplit

from video_media_catalog.connector import ConnectorRecordEnvelope, RecordOperation
from video_media_catalog.rights import PolicyZone, RightsProfile, UsageAction
from video_media_catalog.source_mapper import AssertionBuilder, MappedAssertions
from video_media_catalog.source_registry import (
    SourceNamespace,
    SourceProduct,
    SourceProductKind,
    SourceSystem,
)

EUROPEANA_SOURCE_SYSTEM_ID = "europeana"
EUROPEANA_SOURCE_PRODUCT_ID = "europeana-oai-edm"
EUROPEANA_RECORD_NAMESPACE_ID = "europeana-record"
EUROPEANA_OAI_CONNECTOR_ID = "europeana-oai-pmh"
EUROPEANA_METADATA_POLICY_ID = "europeana-metadata-cc0"
EUROPEANA_RECORD_REFERENT_KIND = "CULTURAL_HERITAGE_OBJECT"
EUROPEANA_METADATA_LICENSE_URI = "https://creativecommons.org/publicdomain/zero/1.0/"

_RECORD_ID = re.compile(r"/[^/?#\s]+/[^?#\s]+")


def normalize_europeana_record_id(value: str) -> str:
    """Normalize an OAI identifier or Record API ID to ``/dataset/local-id``."""

    normalized = value.strip()
    parsed = urlsplit(normalized)
    if parsed.scheme or parsed.netloc:
        try:
            port = parsed.port
        except ValueError as exc:
            raise ValueError("Europeana record identifier has an invalid port") from exc
        prefix = "/item/"
        if (
            parsed.scheme not in {"http", "https"}
            or parsed.hostname != "data.europeana.eu"
            or (parsed.scheme == "http" and port not in {None, 80})
            or (parsed.scheme == "https" and port not in {None, 443})
            or parsed.username is not None
            or parsed.password is not None
            or not parsed.path.startswith(prefix)
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("Europeana OAI identifier is outside data.europeana.eu")
        normalized = f"/{parsed.path.removeprefix(prefix)}"
    if len(normalized) > 2048 or _RECORD_ID.fullmatch(normalized) is None:
        raise ValueError(f"invalid Europeana record ID: {value!r}")
    return normalized


def europeana_record_url(record_id: str) -> str:
    """Return the official HTTPS item page for a normalized record ID."""

    return (
        "https://www.europeana.eu/item/"
        f"{normalize_europeana_record_id(record_id).lstrip('/')}"
    )


def _open_actions() -> tuple[UsageAction, ...]:
    return (
        UsageAction.STORE,
        UsageAction.TRANSFORM,
        UsageAction.DISPLAY,
        UsageAction.SEARCH,
        UsageAction.EXPORT,
        UsageAction.REDISTRIBUTE,
        UsageAction.DERIVE,
        UsageAction.EMBED,
        UsageAction.ML_TRAIN,
        UsageAction.ML_EVALUATE,
    )


def europeana_metadata_rights_profile() -> RightsProfile:
    """CC0 applies to Europeana metadata, never implicitly to linked objects."""

    return RightsProfile(
        policy_id=EUROPEANA_METADATA_POLICY_ID,
        policy_version="2026-09-26",
        zone=PolicyZone.OPEN_CC0,
        license_id="CC0-1.0",
        license_uri=EUROPEANA_METADATA_LICENSE_URI,
        terms_url="https://www.europeana.eu/en/rights/terms-of-use",
        permissions=_open_actions(),
        audiences=("*",),
        territories=("*",),
        notes=(
            "This profile covers Europeana metadata under the Data Exchange "
            "Agreement. It does not grant rights to linked digital objects, "
            "previews, thumbnails, audio, or video. Per-record edm:rights and "
            "dc:rights facts remain separate and control those objects."
        ),
    )


def europeana_registry_entries() -> tuple[
    SourceSystem,
    SourceProduct,
    SourceNamespace,
]:
    return (
        SourceSystem(
            source_system_id=EUROPEANA_SOURCE_SYSTEM_ID,
            name="Europeana",
            operator="Europeana Foundation",
            homepage="https://www.europeana.eu/",
        ),
        SourceProduct(
            source_product_id=EUROPEANA_SOURCE_PRODUCT_ID,
            source_system_id=EUROPEANA_SOURCE_SYSTEM_ID,
            name="Europeana OAI-PMH EDM metadata",
            kind=SourceProductKind.CULTURAL_HERITAGE,
            policy_id=EUROPEANA_METADATA_POLICY_ID,
            connector_ids=(EUROPEANA_OAI_CONNECTOR_ID,),
            documentation_url=(
                "https://europeana.atlassian.net/wiki/spaces/EF/pages/"
                "2324463617/Dataset+download+and+OAI-PMH+service"
            ),
        ),
        SourceNamespace(
            namespace_id=EUROPEANA_RECORD_NAMESPACE_ID,
            source_product_id=EUROPEANA_SOURCE_PRODUCT_ID,
            issuer="Europeana Foundation",
            referent_kinds=(EUROPEANA_RECORD_REFERENT_KIND,),
            scheme_aliases=("europeana",),
            identifier_pattern=_RECORD_ID.pattern,
        ),
    )


def _objects(payload: dict[str, Any], key: str) -> list[dict[str, Any]]:
    value = payload.get(key)
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _strings(payload: dict[str, Any], key: str) -> list[str]:
    value = payload.get(key)
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str) and item]


def _add_labeled_fields(
    builder: AssertionBuilder,
    payload: dict[str, Any],
    *,
    key: str,
    predicate: str,
) -> None:
    from video_media_catalog.assertions import ValueType

    for index, item in enumerate(_objects(payload, key)):
        value = item.get("value")
        if not isinstance(value, str) or not value:
            continue
        qualifiers = {
            field: item[field]
            for field in ("language", "uri", "role")
            if isinstance(item.get(field), str) and item[field]
        }
        builder.add_field(
            predicate,
            ValueType.STRING,
            value,
            f"/{key}/{index}/value",
            qualifiers,
        )


def map_europeana_record(envelope: ConnectorRecordEnvelope) -> MappedAssertions:
    """Map CC0 metadata while retaining object rights as separate facts."""

    if (
        envelope.source_system_id != EUROPEANA_SOURCE_SYSTEM_ID
        or envelope.source_product_id != EUROPEANA_SOURCE_PRODUCT_ID
        or envelope.source_namespace_id != EUROPEANA_RECORD_NAMESPACE_ID
        or envelope.operation != RecordOperation.UPSERT
        or envelope.payload_json is None
    ):
        raise ValueError("record is not an active Europeana OAI envelope")
    payload = json.loads(envelope.payload_json)
    if not isinstance(payload, dict):
        raise ValueError("Europeana payload must be an object")
    record_id = normalize_europeana_record_id(str(payload.get("id") or ""))
    if record_id != envelope.source_record_id:
        raise ValueError("Europeana payload identity does not match its envelope")

    from video_media_catalog.assertions import SourceNodeRef, ValueType

    node = SourceNodeRef(
        namespace_id=EUROPEANA_RECORD_NAMESPACE_ID,
        source_id=record_id,
        referent_kind=EUROPEANA_RECORD_REFERENT_KIND,
    )
    builder = AssertionBuilder(
        envelope=envelope,
        source_node=node,
        mapper_id="europeana-edm-source-mapper",
        mapper_version="1.0.0",
    )
    builder.add_identifier(
        EUROPEANA_RECORD_NAMESPACE_ID,
        record_id,
        "Europeana Foundation",
        EUROPEANA_RECORD_REFERENT_KIND,
        "/id",
    )
    builder.add_entity_type(EUROPEANA_RECORD_REFERENT_KIND, "/types")

    for index, title in enumerate(_objects(payload, "titles")):
        value = title.get("value")
        if not isinstance(value, str) or not value:
            continue
        builder.add_field(
            "title",
            ValueType.STRING,
            value,
            f"/titles/{index}/value",
            {
                "language": title.get("language") or "und",
                "titleRole": title.get("role") or "TITLE",
            },
        )
    _add_labeled_fields(
        builder,
        payload,
        key="descriptions",
        predicate="description",
    )
    _add_labeled_fields(builder, payload, key="creators", predicate="creator")
    _add_labeled_fields(builder, payload, key="contributors", predicate="contributor")
    _add_labeled_fields(builder, payload, key="providers", predicate="provider")
    _add_labeled_fields(
        builder,
        payload,
        key="dataProviders",
        predicate="data_provider",
    )

    for key, predicate in (
        ("times", "temporal"),
        ("languages", "language"),
        ("countries", "country"),
        ("types", "media_type"),
        ("recordUrls", "record_url"),
        ("landingUrls", "landing_url"),
        ("previewUrls", "preview_url"),
        ("mediaUrls", "media_url"),
        ("identifiers", "external_identifier"),
    ):
        for index, value in enumerate(_strings(payload, key)):
            qualifiers = (
                {"referenceOnly": True} if key in {"previewUrls", "mediaUrls"} else None
            )
            builder.add_field(
                predicate,
                ValueType.STRING,
                value,
                f"/{key}/{index}",
                qualifiers,
            )

    builder.add_field(
        "metadata_license_uri",
        ValueType.STRING,
        EUROPEANA_METADATA_LICENSE_URI,
        "/metadataRights/licenseUri",
        {"appliesTo": "METADATA_ONLY"},
    )
    rights = payload.get("digitalObjectRights")
    if not isinstance(rights, dict):
        raise ValueError("Europeana payload must carry digitalObjectRights")
    status = rights.get("status")
    if not isinstance(status, str) or not status:
        raise ValueError("Europeana digital-object rights status is required")
    builder.add_field(
        "digital_object_rights_status",
        ValueType.STRING,
        status,
        "/digitalObjectRights/status",
        {"appliesTo": "DIGITAL_OBJECT_AND_PREVIEW"},
    )
    for key, predicate in (
        ("edmRights", "edm_rights"),
        ("dcRights", "dc_rights"),
        ("rightsStatements", "rights_statement"),
        ("licenses", "license_uri"),
    ):
        values = rights.get(key)
        if not isinstance(values, list):
            continue
        for index, value in enumerate(values):
            if isinstance(value, str) and value:
                builder.add_field(
                    predicate,
                    ValueType.STRING,
                    value,
                    f"/digitalObjectRights/{key}/{index}",
                    {"appliesTo": "DIGITAL_OBJECT_AND_PREVIEW"},
                )
    referenced_resources = rights.get("referencedResources")
    if isinstance(referenced_resources, list):
        for index, resource_rights in enumerate(referenced_resources):
            if isinstance(resource_rights, dict):
                builder.add_field(
                    "linked_object_rights",
                    ValueType.JSON,
                    resource_rights,
                    f"/digitalObjectRights/referencedResources/{index}",
                    {
                        "appliesTo": resource_rights.get("url"),
                        "referenceRole": resource_rights.get("role"),
                    },
                )

    for index, external in enumerate(_objects(payload, "externalIds")):
        namespace = external.get("namespace")
        value = external.get("value")
        if not isinstance(namespace, str) or not isinstance(value, str):
            continue
        issuer = {
            "eidr-content": "EIDR Association",
            "imdb-title": "IMDb",
            "imdb-name": "IMDb",
            "imdb-company": "IMDb",
        }.get(namespace)
        if issuer is None:
            continue
        referent_kind = {
            "imdb-name": "AGENT",
            "imdb-company": "ORGANIZATION",
        }.get(namespace, "EDITORIAL_WORK")
        builder.add_identifier(
            namespace,
            value,
            issuer,
            referent_kind,
            f"/externalIds/{index}/value",
        )

    omitted_urls = set(_strings(payload, "previewUrls"))
    omitted_urls.update(_strings(payload, "mediaUrls"))
    return builder.build(omitted_asset_count=len(omitted_urls))
