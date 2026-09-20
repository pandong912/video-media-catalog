"""TVmaze community connector and source-to-assertion mapping."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any

from video_media_catalog.assertions import (
    AssertionProvenance,
    FieldAssertion,
    SourceNodeRef,
    ValueType,
    build_entity_type_assertion,
    build_field_assertion,
    build_identifier_assertion,
)
from video_media_catalog.connector import (
    ChangeSemantics,
    CommunityConnector,
    Completeness,
    ConnectorBatchManifest,
    ConnectorRecordEnvelope,
    DeleteCoverage,
    RecordOperation,
    Serialization,
    TransportKind,
    build_connector_record_envelope,
    validate_envelopes_against_batch,
)
from video_media_catalog.rights import PolicyZone, RightsProfile, UsageAction
from video_media_catalog.source_mapper import MappedAssertions
from video_media_catalog.source_registry import (
    RegistryStatus,
    SourceNamespace,
    SourceProduct,
    SourceProductKind,
    SourceSystem,
)

TVMAZE_SOURCE_SYSTEM_ID = "tvmaze"
TVMAZE_SOURCE_PRODUCT_ID = "tvmaze-public-api"
TVMAZE_SHOW_NAMESPACE_ID = "tvmaze-show"
TVMAZE_CONNECTOR_ID = "tvmaze-show-index"
TVMAZE_DELTA_CONNECTOR_ID = "tvmaze-show-updates"
TVMAZE_POLICY_ID = "tvmaze-api-cc-by-sa"


def tvmaze_rights_profile() -> RightsProfile:
    """Return the reviewed policy used by the public TVmaze API connector."""

    return RightsProfile(
        policy_id=TVMAZE_POLICY_ID,
        policy_version="2026-09-19",
        zone=PolicyZone.OPEN_SHAREALIKE,
        license_id="CC-BY-SA-version-unspecified",
        terms_url="https://www.tvmaze.com/api",
        permissions=(
            UsageAction.STORE,
            UsageAction.TRANSFORM,
            UsageAction.DISPLAY,
            UsageAction.SEARCH,
            UsageAction.EXPORT,
            UsageAction.REDISTRIBUTE,
            UsageAction.DERIVE,
        ),
        audiences=("*",),
        territories=("*",),
        attribution_text="TV data provided by TVmaze (https://www.tvmaze.com).",
        share_alike=True,
        purge_on_termination=False,
        notes=(
            "The API page does not identify a Creative Commons version. "
            "Image rights require separate review before asset publication."
        ),
    )


def tvmaze_registry_entries() -> tuple[
    SourceSystem,
    SourceProduct,
    SourceNamespace,
]:
    system = SourceSystem(
        source_system_id=TVMAZE_SOURCE_SYSTEM_ID,
        name="TVmaze",
        operator="TVmaze.com",
        homepage="https://www.tvmaze.com/",
        status=RegistryStatus.ACTIVE,
    )
    product = SourceProduct(
        source_product_id=TVMAZE_SOURCE_PRODUCT_ID,
        source_system_id=TVMAZE_SOURCE_SYSTEM_ID,
        name="TVmaze public API",
        kind=SourceProductKind.COMMUNITY_DATABASE,
        policy_id=TVMAZE_POLICY_ID,
        connector_ids=(
            TVMAZE_CONNECTOR_ID,
            TVMAZE_DELTA_CONNECTOR_ID,
        ),
        documentation_url="https://www.tvmaze.com/api",
    )
    namespace = SourceNamespace(
        namespace_id=TVMAZE_SHOW_NAMESPACE_ID,
        source_product_id=TVMAZE_SOURCE_PRODUCT_ID,
        issuer="TVmaze",
        referent_kinds=("SERIES",),
        identifier_pattern=r"[1-9][0-9]*",
    )
    return system, product, namespace


class TVMazeMappedAssertions(MappedAssertions):
    """Backward-compatible TVmaze mapper result type."""


class TVMazeShowConnector(CommunityConnector):
    """Decode immutable `/shows?page=N` responses into replayable envelopes."""

    source_product_id = TVMAZE_SOURCE_PRODUCT_ID

    def decode(
        self,
        manifest: ConnectorBatchManifest,
        raw_payloads,
    ) -> tuple[ConnectorRecordEnvelope, ...]:
        self._validate_manifest(manifest)
        payloads = tuple(raw_payloads)
        if len(payloads) != len(manifest.raw_objects):
            raise ValueError("TVmaze page count does not match raw_objects")

        envelopes: list[ConnectorRecordEnvelope] = []
        seen_ids: set[int] = set()
        for page_number, (raw, raw_object) in enumerate(
            zip(payloads, manifest.raw_objects, strict=True)
        ):
            for envelope in self.decode_page(
                manifest,
                raw,
                raw_object=raw_object,
                page_number=page_number,
            ):
                show_id = int(envelope.source_record_id)
                if show_id in seen_ids:
                    raise ValueError(f"duplicate TVmaze show id: {show_id}")
                seen_ids.add(show_id)
                envelopes.append(envelope)
        return validate_envelopes_against_batch(manifest, envelopes)

    def decode_page(
        self,
        manifest: ConnectorBatchManifest,
        raw: bytes,
        *,
        raw_object,
        page_number: int,
    ) -> tuple[ConnectorRecordEnvelope, ...]:
        """Decode one captured page without retaining the complete source."""

        self._validate_manifest(manifest)
        if page_number < 0:
            raise ValueError("TVmaze page number must be non-negative")
        if (
            raw_object.size_bytes != len(raw)
            or raw_object.checksum.value != hashlib.sha256(raw).hexdigest()
            or raw_object.format != "OBJECT_FORMAT_JSON"
            or raw_object.media_type != "application/json"
        ):
            raise ValueError("TVmaze raw page does not match its ObjectRef")
        result = []
        for index, show in enumerate(_decode_page(raw)):
            show_id = _show_id(show)
            updated = show.get("updated")
            revision = str(updated) if isinstance(updated, int) else None
            modified_at = (
                None
                if revision is None
                else datetime.fromtimestamp(updated, tz=UTC)
                .isoformat()
                .replace("+00:00", "Z")
            )
            result.append(
                build_connector_record_envelope(
                    payload=show,
                    batch_id=manifest.batch_id,
                    source_system_id=manifest.source_system_id,
                    source_product_id=manifest.source_product_id,
                    source_namespace_id=TVMAZE_SHOW_NAMESPACE_ID,
                    source_record_id=str(show_id),
                    source_revision=revision,
                    operation=RecordOperation.UPSERT,
                    source_modified_at=modified_at,
                    observed_at=manifest.acquired_at,
                    ingested_at=manifest.acquired_at,
                    payload_schema="tvmaze-show-v1",
                    raw_object=raw_object,
                    source_location=f"/page/{page_number}/item/{index}",
                    policy_id=manifest.policy_id,
                    policy_digest=manifest.policy_digest,
                )
            )
        return tuple(result)

    @staticmethod
    def _validate_manifest(manifest: ConnectorBatchManifest) -> None:
        expected = (
            manifest.source_system_id == TVMAZE_SOURCE_SYSTEM_ID
            and manifest.source_product_id == TVMAZE_SOURCE_PRODUCT_ID
            and manifest.connector_id == TVMAZE_CONNECTOR_ID
            and manifest.transport_kind == TransportKind.API
            and manifest.serialization == Serialization.JSON
            and manifest.change_semantics == ChangeSemantics.FULL_SNAPSHOT
            and manifest.completeness == Completeness.COMPLETE
            and manifest.delete_coverage == DeleteCoverage.SNAPSHOT_DIFF
        )
        if not expected:
            raise ValueError("manifest is not a complete TVmaze show-index snapshot")
        policy = tvmaze_rights_profile()
        if (
            manifest.policy_id != policy.policy_id
            or manifest.policy_digest != policy.digest
        ):
            raise ValueError("TVmaze batch does not bind the reviewed policy")


def _decode_page(raw: bytes) -> list[dict[str, Any]]:
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("TVmaze page must be UTF-8 JSON") from exc
    if not isinstance(value, list):
        raise ValueError("TVmaze show-index page must be a JSON array")
    if any(not isinstance(item, dict) for item in value):
        raise ValueError("TVmaze show-index page contains a non-object")
    return value


def _show_id(show: dict[str, Any]) -> int:
    value = show.get("id")
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError("TVmaze show requires a positive integer id")
    return value


def map_tvmaze_show(
    envelope: ConnectorRecordEnvelope,
) -> TVMazeMappedAssertions:
    """Map reusable TVmaze metadata; intentionally do not publish image URLs."""

    if (
        envelope.source_system_id != TVMAZE_SOURCE_SYSTEM_ID
        or envelope.source_product_id != TVMAZE_SOURCE_PRODUCT_ID
        or envelope.source_namespace_id != TVMAZE_SHOW_NAMESPACE_ID
    ):
        raise ValueError("record is not a TVmaze show envelope")
    node = SourceNodeRef(
        namespace_id=TVMAZE_SHOW_NAMESPACE_ID,
        source_id=envelope.source_record_id,
        referent_kind="SERIES",
    )
    if envelope.operation != RecordOperation.UPSERT:
        return TVMazeMappedAssertions(source_node=node)
    if envelope.payload_json is None:
        raise ValueError("active TVmaze show envelope requires a payload")
    show = json.loads(envelope.payload_json)
    if not isinstance(show, dict) or str(_show_id(show)) != envelope.source_record_id:
        raise ValueError("TVmaze payload identity does not match its envelope")

    def provenance(path: str) -> AssertionProvenance:
        return AssertionProvenance(
            envelope_key=envelope.envelope_key,
            source_path=path,
            mapper_id="tvmaze-show-mapper",
            mapper_version="1.0.0",
            policy_id=envelope.policy_id,
            policy_digest=envelope.policy_digest,
            observed_at=envelope.observed_at,
            source_modified_at=envelope.source_modified_at,
        )

    fields: list[FieldAssertion] = []

    def add_field(
        predicate: str,
        value_type: ValueType,
        value: Any,
        path: str,
        qualifiers: dict[str, Any] | None = None,
    ) -> None:
        if value is None or value == "":
            return
        fields.append(
            build_field_assertion(
                subject=node,
                predicate=predicate,
                value_type=value_type,
                value=value,
                qualifiers=qualifiers,
                provenance=provenance(path),
            )
        )

    language = show.get("language")
    title_qualifiers = {
        "language": "und",
        "titleRole": "PRIMARY",
    }
    if isinstance(language, str):
        title_qualifiers["sourceLanguage"] = language
    add_field(
        "title",
        ValueType.STRING,
        show.get("name"),
        "/name",
        title_qualifiers,
    )
    add_field("format", ValueType.STRING, show.get("type"), "/type")
    add_field("language", ValueType.STRING, language, "/language")
    add_field("status", ValueType.STRING, show.get("status"), "/status")
    add_field("premiered", ValueType.DATE, show.get("premiered"), "/premiered")
    add_field("ended", ValueType.DATE, show.get("ended"), "/ended")
    add_field(
        "runtime_minutes",
        ValueType.INTEGER,
        show.get("runtime"),
        "/runtime",
    )
    add_field(
        "average_runtime_minutes",
        ValueType.INTEGER,
        show.get("averageRuntime"),
        "/averageRuntime",
    )
    genres = show.get("genres")
    if isinstance(genres, list):
        for index, genre in enumerate(genres):
            if isinstance(genre, str) and genre:
                add_field(
                    "genre",
                    ValueType.STRING,
                    genre,
                    f"/genres/{index}",
                    {"vocabulary": "tvmaze"},
                )

    identifiers = [
        build_identifier_assertion(
            subject=node,
            namespace_id=TVMAZE_SHOW_NAMESPACE_ID,
            value=envelope.source_record_id,
            issuer="TVmaze",
            referent_kind="SERIES",
            provenance=provenance("/id"),
        )
    ]
    externals = show.get("externals")
    if isinstance(externals, dict):
        external_specs = {
            "imdb": ("imdb-title", "IMDb"),
            "thetvdb": ("thetvdb-series", "TheTVDB"),
            "tvrage": ("tvrage-show", "TVRage"),
        }
        for source_field, (namespace, issuer) in external_specs.items():
            value = externals.get(source_field)
            if isinstance(value, str | int) and not isinstance(value, bool):
                identifiers.append(
                    build_identifier_assertion(
                        subject=node,
                        namespace_id=namespace,
                        value=str(value),
                        issuer=issuer,
                        referent_kind="SERIES",
                        provenance=provenance(f"/externals/{source_field}"),
                    )
                )

    omitted_assets = int(bool(show.get("image")))
    return TVMazeMappedAssertions(
        source_node=node,
        field_assertions=tuple(fields),
        identifier_assertions=tuple(identifiers),
        entity_type_assertions=(
            build_entity_type_assertion(
                subject=node,
                entity_type="SERIES",
                provenance=provenance("/type"),
            ),
        ),
        omitted_asset_count=omitted_assets,
    )
