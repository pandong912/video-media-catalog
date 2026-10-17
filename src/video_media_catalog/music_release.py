"""Immutable Music Catalog release-request and capture bundle publication."""

from __future__ import annotations

import copy
import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from video_media_catalog.canonical import canonical_json_bytes, deterministic_key
from video_media_catalog.connector import (
    ConnectorBatchManifest,
    ConnectorRecordEnvelope,
    ConnectorRecordSetManifest,
    Serialization,
    build_connector_record_set_manifest,
)
from video_media_catalog.models import ObjectRef
from video_media_catalog.music_pilot import (
    MusicPilotMatchProfile,
    load_music_pilot_match_profile,
)
from video_media_catalog.object_store import (
    RuntimeObjectStore,
    conditional_publish_bytes,
)
from video_media_catalog.reference_fixture import (
    PinnedReferenceFixture,
    load_reference_fixture,
    materialize_reference_fixture,
)
from video_media_catalog.storage import join_uri
from video_media_catalog.temporal.activities.research_domain import (
    RESEARCH_CAPTURE_MANIFEST_SCHEMA,
    _parse_manifest,
)
from video_media_catalog.temporal.admin_cli import _env_from_mapping
from video_media_catalog.temporal.golden_admin_cli import _input_from_mapping
from video_media_catalog.temporal.models import ObjectRefPayload
from video_media_catalog.temporal.names import GOLDEN_WORKFLOW_TASK_QUEUE
from video_media_catalog.temporal.research_domain_admin_cli import (
    _CAPTURE_PREFIX,
    _CATALOG_ROOT,
    _EMR_RUNTIME_ROLE,
    _WAREHOUSE,
    _domain_input_from_mapping,
)
from video_media_catalog.temporal.research_domain_models import (
    ResearchDomainInput,
    ResearchSourceProfile,
)
from video_media_catalog.v2_contracts import digest_identity, require_sha256

MUSIC_RELEASE_REQUEST_SCHEMA = "video.media-catalog.music-release-request.v1"
MUSIC_RELEASE_PROFILE_TEMPLATE_SCHEMA = (
    "video.media-catalog.music-release-profile-template.v1"
)
MUSIC_RELEASE_REQUEST_MEDIA_TYPE = (
    "application/vnd.video-media-catalog.music-release-request.v1+json"
)
RESEARCH_CAPTURE_MANIFEST_MEDIA_TYPE = (
    "application/vnd.video-media-catalog.research-capture-manifest.v1+json"
)
CONNECTOR_BATCH_MEDIA_TYPE = (
    "application/vnd.video-media-catalog.connector-batch.v2+json"
)
CONNECTOR_RECORD_SET_MEDIA_TYPE = (
    "application/vnd.video-media-catalog.record-set.v2+json"
)
CONNECTOR_ENVELOPE_MEDIA_TYPE = (
    "application/vnd.video-media-catalog.connector-record-envelope.v2+ndjson"
)
ZERO_IMAGE_DIGEST = "sha256:" + ("0" * 64)
SOURCE_ORDER = ("musicbrainz-core-dump", "wikidata-json-dump")
MUSIC_RELEASE_GATES = (
    "cutoverAllowed",
    "datasetPublicationEnabled",
    "liveCutoverClaimed",
    "openSearchAliasActivationAllowed",
    "reverseDualWriteAllowed",
    "referenceWorldMaterializationAllowed",
    "referenceWorldPublicationAllowed",
    "tenantCanonicalWriteAllowed",
)

_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MUSICBRAINZ_FIXTURE = (
    _REPOSITORY_ROOT
    / "tests"
    / "fixtures"
    / "research"
    / "musicbrainz-music-pilot.v1.json"
)
DEFAULT_WIKIDATA_FIXTURE = (
    _REPOSITORY_ROOT
    / "tests"
    / "fixtures"
    / "research"
    / "wikidata-music-pilot.v1.json"
)
DEFAULT_MATCH_PROFILE = (
    _REPOSITORY_ROOT
    / "tests"
    / "fixtures"
    / "research"
    / "music-pilot-match-profile.v1.json"
)

_SOURCE_REVISION = re.compile(r"^[0-9a-f]{40}(?:[0-9a-f]{24})?$")
_REVIEWED_REQUEST_KEYS = frozenset(
    {
        "schemaVersion",
        "status",
        "domain",
        "sourceRevision",
        "workflowId",
        "sourcePolicyDigest",
        "algorithmDigest",
        "profileTemplateDigest",
        "profileTemplate",
        "researchDomain",
        "indexProjection",
        "gates",
    }
)


@dataclass(frozen=True)
class PublishedMusicCapture:
    """Published artifacts for one curated authority fixture."""

    source_product_id: str
    source_snapshot_pin_id: str
    batch_manifest: ConnectorBatchManifest
    batch_manifest_object: ObjectRef
    envelope_object: ObjectRef
    record_set_manifest: ConnectorRecordSetManifest
    record_set_manifest_object: ObjectRef
    capture_manifest: dict[str, Any]
    capture_manifest_object: ObjectRef
    raw_object: ObjectRef


@dataclass(frozen=True)
class MusicReleaseBundle:
    """One canonical release request and all capture objects it binds."""

    release_id: str
    request: dict[str, Any]
    request_object: ObjectRef
    captures: tuple[PublishedMusicCapture, ...]
    publishable: bool


def object_ref_mapping(reference: ObjectRef) -> dict[str, Any]:
    """Emit an external ObjectRef with an integer sizeBytes value."""

    value: dict[str, Any] = {
        "uri": reference.uri,
        "format": reference.format,
        "mediaType": reference.media_type,
        "checksum": {
            "algorithm": reference.checksum.algorithm,
            "encoding": reference.checksum.encoding,
            "value": reference.checksum.value,
        },
        "sizeBytes": int(reference.size_bytes),
    }
    if reference.etag is not None:
        value["etag"] = reference.etag
    if reference.object_version is not None:
        value["objectVersion"] = reference.object_version
    return value


def _external_mapping(value: Any) -> Any:
    """Normalize model JSON mappings at external ObjectRef boundaries."""

    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json", by_alias=True, exclude_none=True)
    if isinstance(value, dict):
        normalized: dict[str, Any] = {}
        for key, item in value.items():
            if key == "sizeBytes":
                if isinstance(item, bool):
                    raise ValueError("ObjectRef sizeBytes must be an integer")
                normalized[key] = int(item)
            else:
                normalized[key] = _external_mapping(item)
        return normalized
    if isinstance(value, list | tuple):
        return [_external_mapping(item) for item in value]
    return value


def _model_json_bytes(value: Any) -> bytes:
    return canonical_json_bytes(_external_mapping(value), newline=True)


def _normalized_reference(reference: ObjectRef) -> ObjectRef:
    """Drop mutable observation time while retaining immutable S3 identity."""

    return ObjectRef(
        uri=reference.uri,
        format=reference.format,
        media_type=reference.media_type,
        checksum=reference.checksum,
        size_bytes=reference.size_bytes,
        etag=reference.etag,
        object_version=reference.object_version,
    )


def _publish_addressed(
    *,
    store: RuntimeObjectStore,
    destination_prefix: str,
    source_product_id: str,
    category: str,
    suffix: str,
    payload: bytes,
    media_type: str,
    object_format: str,
    require_s3_metadata: bool,
) -> ObjectRef:
    if not payload:
        raise ValueError("content-addressed Music release objects must be non-empty")
    digest = hashlib.sha256(payload).hexdigest()
    destination = join_uri(
        destination_prefix,
        "objects",
        source_product_id,
        category,
        f"{digest}{suffix}",
    )
    published = conditional_publish_bytes(
        store,
        payload,
        destination,
        media_type=media_type,
        object_format=object_format,
        max_bytes=len(payload),
    )
    verified = store.head(
        published.object_ref,
        max_bytes=len(payload),
        require_immutable_metadata=require_s3_metadata,
    )
    verified = _normalized_reference(verified)
    if require_s3_metadata and (
        urlsplit(verified.uri).scheme != "s3"
        or not verified.etag
        or not verified.object_version
    ):
        raise ValueError("published Music release object is not immutable S3")
    return verified


def _fixture_raw_path(
    fixture_path: Path,
    fixture: PinnedReferenceFixture,
) -> Path:
    raw_name = Path(urlsplit(fixture.raw_object.uri).path).name
    return fixture_path.parent / raw_name


def _fixture_with_raw_object(
    fixture: PinnedReferenceFixture,
    raw_object: ObjectRef,
) -> PinnedReferenceFixture:
    value = fixture.model_dump(mode="json", by_alias=True, exclude_none=True)
    value["rawObject"] = object_ref_mapping(raw_object)
    return PinnedReferenceFixture.model_validate(value)


def _capture_profile_id(source_product_id: str, release_id: str) -> str:
    return f"music-release-{source_product_id}-{release_id[:24]}"


def _publish_capture(
    *,
    fixture_path: Path,
    fixture: PinnedReferenceFixture,
    release_id: str,
    destination_prefix: str,
    store: RuntimeObjectStore,
    require_s3_metadata: bool,
) -> PublishedMusicCapture:
    source_product_id = fixture.source_product_id
    raw_path = _fixture_raw_path(fixture_path, fixture)
    raw_payload = raw_path.read_bytes()
    raw_object = _publish_addressed(
        store=store,
        destination_prefix=destination_prefix,
        source_product_id=source_product_id,
        category="raw",
        suffix=".ndjson",
        payload=raw_payload,
        media_type=fixture.raw_object.media_type,
        object_format=fixture.raw_object.format,
        require_s3_metadata=require_s3_metadata,
    )
    if (
        raw_object.checksum.value != fixture.raw_object.checksum.value
        or raw_object.size_bytes != fixture.raw_object.size_bytes
    ):
        raise ValueError("published raw object differs from the curated fixture")

    materialized = materialize_reference_fixture(
        _fixture_with_raw_object(fixture, raw_object),
        serialization=Serialization.JSON_LINES,
    )
    captured = materialized.captured
    batch_payload = _model_json_bytes(captured.batch_manifest)
    # Exercise the production parser over the exact bytes that will be published.
    ConnectorBatchManifest.model_validate_json(batch_payload)
    batch_object = _publish_addressed(
        store=store,
        destination_prefix=destination_prefix,
        source_product_id=source_product_id,
        category="batch-manifests",
        suffix=".json",
        payload=batch_payload,
        media_type=CONNECTOR_BATCH_MEDIA_TYPE,
        object_format="OBJECT_FORMAT_JSON",
        require_s3_metadata=require_s3_metadata,
    )

    envelope_payload = b"".join(
        _model_json_bytes(envelope) for envelope in captured.envelopes
    )
    for line in envelope_payload.splitlines():
        ConnectorRecordEnvelope.model_validate_json(line)
    envelope_object = _publish_addressed(
        store=store,
        destination_prefix=destination_prefix,
        source_product_id=source_product_id,
        category="record-envelopes",
        suffix=".ndjson",
        payload=envelope_payload,
        media_type=CONNECTOR_ENVELOPE_MEDIA_TYPE,
        object_format="OBJECT_FORMAT_OTHER",
        require_s3_metadata=require_s3_metadata,
    )

    record_set = build_connector_record_set_manifest(
        batch_id=captured.batch_manifest.batch_id,
        source_product_id=source_product_id,
        policy_id=captured.batch_manifest.policy_id,
        policy_digest=captured.batch_manifest.policy_digest,
        record_objects=(envelope_object,),
        record_count=len(captured.envelopes),
        first_envelope_key=captured.envelopes[0].envelope_key,
        last_envelope_key=captured.envelopes[-1].envelope_key,
        created_at=captured.pin.captured_at,
    )
    record_set_payload = _model_json_bytes(record_set)
    ConnectorRecordSetManifest.model_validate_json(record_set_payload)
    record_set_object = _publish_addressed(
        store=store,
        destination_prefix=destination_prefix,
        source_product_id=source_product_id,
        category="record-set-manifests",
        suffix=".json",
        payload=record_set_payload,
        media_type=CONNECTOR_RECORD_SET_MEDIA_TYPE,
        object_format="OBJECT_FORMAT_JSON",
        require_s3_metadata=require_s3_metadata,
    )

    profile_id = _capture_profile_id(source_product_id, release_id)
    capture_manifest = {
        "schemaVersion": RESEARCH_CAPTURE_MANIFEST_SCHEMA,
        "profileId": profile_id,
        "domain": "MUSIC",
        "sourceProductId": source_product_id,
        "watermark": fixture.source_revision,
        "acquiredAt": fixture.captured_at,
        "captures": [
            {
                "kind": "snapshot",
                "sourceProductId": source_product_id,
                "batchId": captured.batch_manifest.batch_id,
                "recordSetId": record_set.record_set_id,
                "recordCount": len(captured.envelopes),
                "batchManifest": object_ref_mapping(batch_object),
                "recordSetManifest": object_ref_mapping(record_set_object),
                "retryCount": 0,
                "rateLimitCount": 0,
            }
        ],
    }
    capture_payload = canonical_json_bytes(capture_manifest, newline=True)
    capture_object = _publish_addressed(
        store=store,
        destination_prefix=destination_prefix,
        source_product_id=source_product_id,
        category="capture-manifests",
        suffix=".json",
        payload=capture_payload,
        media_type=RESEARCH_CAPTURE_MANIFEST_MEDIA_TYPE,
        object_format="OBJECT_FORMAT_JSON",
        require_s3_metadata=require_s3_metadata,
    )
    return PublishedMusicCapture(
        source_product_id=source_product_id,
        source_snapshot_pin_id=captured.pin.pin_id,
        batch_manifest=captured.batch_manifest,
        batch_manifest_object=batch_object,
        envelope_object=envelope_object,
        record_set_manifest=record_set,
        record_set_manifest_object=record_set_object,
        capture_manifest=capture_manifest,
        capture_manifest_object=capture_object,
        raw_object=raw_object,
    )


def _transform_image_digests(
    value: Any,
    replacement: str,
) -> tuple[Any, int]:
    if isinstance(value, dict):
        transformed: dict[str, Any] = {}
        count = 0
        for key, item in value.items():
            if key == "imageDigest":
                transformed[key] = replacement
                count += 1
            else:
                transformed[key], nested = _transform_image_digests(
                    item,
                    replacement,
                )
                count += nested
        return transformed, count
    if isinstance(value, list):
        transformed_items = []
        count = 0
        for item in value:
            transformed, nested = _transform_image_digests(item, replacement)
            transformed_items.append(transformed)
            count += nested
        return transformed_items, count
    return value, 0


def _image_digest_values(value: Any) -> tuple[Any, ...]:
    values: list[Any] = []
    if isinstance(value, dict):
        for key, item in value.items():
            if key == "imageDigest":
                values.append(item)
            else:
                values.extend(_image_digest_values(item))
    elif isinstance(value, list):
        for item in value:
            values.extend(_image_digest_values(item))
    return tuple(values)


def _require_source_revision(value: str) -> str:
    normalized = value.strip().lower()
    if _SOURCE_REVISION.fullmatch(normalized) is None:
        raise ValueError("sourceRevision must be a full 40 or 64 character commit")
    return normalized


def _load_release_inputs(
    *,
    musicbrainz_fixture_path: Path,
    wikidata_fixture_path: Path,
    match_profile_path: Path,
) -> tuple[
    tuple[tuple[Path, PinnedReferenceFixture], ...],
    MusicPilotMatchProfile,
]:
    fixtures = (
        (
            musicbrainz_fixture_path,
            load_reference_fixture(musicbrainz_fixture_path),
        ),
        (
            wikidata_fixture_path,
            load_reference_fixture(wikidata_fixture_path),
        ),
    )
    profile = load_music_pilot_match_profile(match_profile_path)
    if tuple(fixture.source_product_id for _, fixture in fixtures) != SOURCE_ORDER:
        raise ValueError("Music release fixtures are not in canonical source order")
    for _path, fixture in fixtures:
        source_product_id = fixture.source_product_id
        baseline = materialize_reference_fixture(fixture)
        if (
            fixture.digest != profile.source_fixture_digests[source_product_id]
            or fixture.source_revision != profile.source_revisions[source_product_id]
            or baseline.captured.pin.pin_id
            != profile.source_snapshot_pin_ids[source_product_id]
        ):
            raise ValueError(
                f"{source_product_id} fixture differs from the checked-in profile"
            )
    return fixtures, profile


def _prepare_golden_template(
    golden_build: Mapping[str, Any],
    *,
    profile: MusicPilotMatchProfile,
    source_revision: str,
) -> tuple[str, dict[str, Any]]:
    original = copy.deepcopy(dict(golden_build))
    _input_from_mapping(original)
    identity_input, count = _transform_image_digests(original, ZERO_IMAGE_DIGEST)
    if count < 1:
        raise ValueError("Golden Build template requires env.imageDigest")
    identity_input.pop("buildId", None)
    release_id = deterministic_key(
        "music-catalog-release-request-v1",
        {
            "sourceRevision": source_revision,
            "profileId": profile.profile_id,
            "sourceSnapshotPinIds": profile.source_snapshot_pin_ids,
            "sourceRevisions": profile.source_revisions,
            "cohortDigest": profile.cohort_digest,
            "algorithmDigest": profile.algorithm_digest,
            "sourcePolicyDigest": profile.source_policy_digest,
            "goldenBuildTemplate": identity_input,
        },
    ).removeprefix("sha256:")
    prepared, _count = _transform_image_digests(original, ZERO_IMAGE_DIGEST)
    prepared["buildId"] = release_id
    _input_from_mapping(prepared)
    return release_id, prepared


def _source_env() -> dict[str, Any]:
    return {
        "destinationPrefix": _CAPTURE_PREFIX,
        "imageDigest": ZERO_IMAGE_DIGEST,
        "userAgent": "video-media-catalog/music-release-v1",
        "awsRegion": "us-east-1",
        "warehouseUri": _WAREHOUSE,
        "recordStagingPrefix": f"{_WAREHOUSE}/research/control/record-shards",
        "silverCheckpointPrefix": (
            f"{_WAREHOUSE}/research/control/source-silver-checkpoints"
        ),
        "pipelineSummaryPrefix": (
            f"{_CATALOG_ROOT}/landing/research/pipeline-summaries"
        ),
        "emrApplicationName": (
            "ai-video-platform-dev-media-catalog-wikidata-reference"
        ),
        "emrExecutionRoleArn": _EMR_RUNTIME_ROLE,
        "emrLogUri": f"{_CATALOG_ROOT}/raw/wikidata/emr-serverless-logs/",
        "emrEntryPoint": (
            "local:///opt/video-media-catalog/src/video_media_catalog/community_cli.py"
        ),
        "catalogType": "glue",
        "catalogName": "media",
        "glueNamespace": "video_media_catalog",
    }


def _research_domain_mapping(
    *,
    captures: tuple[PublishedMusicCapture, ...],
    golden_build: dict[str, Any],
    release_id: str,
) -> dict[str, Any]:
    return {
        "domain": "MUSIC",
        "sources": [
            {
                "profileId": _capture_profile_id(
                    capture.source_product_id,
                    release_id,
                ),
                "domain": "MUSIC",
                "sourceProductId": capture.source_product_id,
                "captureManifest": object_ref_mapping(capture.capture_manifest_object),
                "env": _source_env(),
                "watermark": capture.capture_manifest["watermark"],
                "identitySpine": True,
                "materialization": False,
                "goldContribution": True,
                "maximumAttempts": 3,
                "initialRetrySeconds": 30,
                "maximumRetrySeconds": 600,
                "captureTimeoutMinutes": 30,
                "silverTimeoutMinutes": 720,
                "maxManifestBytes": 4 * 1024 * 1024,
            }
            for capture in captures
        ],
        "goldenBuild": golden_build,
        "goldenTaskQueue": GOLDEN_WORKFLOW_TASK_QUEUE,
        "continueAsNewAfterSources": 2,
        "failureBudget": 1,
    }


def _profile_template(
    *,
    profile: MusicPilotMatchProfile,
    captures: tuple[PublishedMusicCapture, ...],
) -> dict[str, Any]:
    template = _external_mapping(profile)
    template["templateSchemaVersion"] = MUSIC_RELEASE_PROFILE_TEMPLATE_SCHEMA
    template["sourcePins"] = [
        {
            "sourceProductId": capture.source_product_id,
            "sourceSnapshotPinId": capture.source_snapshot_pin_id,
            "baselineSourceSnapshotPinId": profile.source_snapshot_pin_ids[
                capture.source_product_id
            ],
            "sourceRevision": profile.source_revisions[capture.source_product_id],
            "fixtureDigest": profile.source_fixture_digests[capture.source_product_id],
            "captureManifest": object_ref_mapping(capture.capture_manifest_object),
        }
        for capture in captures
    ]
    template["runtime"] = {"imageDigest": ZERO_IMAGE_DIGEST}
    return template


def _index_projection(release_id: str) -> dict[str, Any]:
    return {
        "projectionId": f"music-reference-{release_id[:24]}",
        "shards": 6,
        "replicas": 1,
        "bulkChunkSize": 500,
        "bulkMaxChunkBytes": 5 * 1024 * 1024,
    }


def _is_capture_destination(destination_prefix: str) -> bool:
    destination = urlsplit(destination_prefix)
    boundary = urlsplit(_CAPTURE_PREFIX)
    path_segments = destination.path.split("/")
    return (
        destination.scheme == "s3"
        and destination.netloc == boundary.netloc
        and destination.username is None
        and destination.password is None
        and not destination.query
        and not destination.fragment
        and not any(segment in {".", ".."} for segment in path_segments)
        and (
            destination.path.rstrip("/") == boundary.path.rstrip("/")
            or destination.path.startswith(boundary.path.rstrip("/") + "/")
        )
    )


def _local_domain_input(values: dict[str, Any]) -> ResearchDomainInput:
    sources = tuple(
        ResearchSourceProfile(
            profile_id=str(source["profileId"]),
            domain="MUSIC",
            source_product_id=str(source["sourceProductId"]),
            capture_manifest=ObjectRefPayload.from_mapping(source["captureManifest"]),
            env=_env_from_mapping(source["env"]),
            watermark=str(source["watermark"]),
            identity_spine=True,
            materialization=False,
            gold_contribution=True,
        )
        for source in values["sources"]
    )
    return ResearchDomainInput(
        domain="MUSIC",
        sources=sources,
        golden_build=_input_from_mapping(values["goldenBuild"]),
        golden_task_queue=str(values["goldenTaskQueue"]),
        continue_as_new_after_sources=int(values["continueAsNewAfterSources"]),
        failure_budget=int(values["failureBudget"]),
    )


def _assert_integer_object_sizes(value: Any) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            if key == "sizeBytes" and (
                isinstance(item, bool) or not isinstance(item, int)
            ):
                raise ValueError("external ObjectRef sizeBytes must be an integer")
            _assert_integer_object_sizes(item)
    elif isinstance(value, list):
        for item in value:
            _assert_integer_object_sizes(item)


def _validate_capture_consumption(
    *,
    domain_input: ResearchDomainInput,
    captures: tuple[PublishedMusicCapture, ...],
) -> None:
    for profile, capture in zip(
        domain_input.sources,
        captures,
        strict=True,
    ):
        _parse_manifest(capture.capture_manifest, profile=profile)


def _build_request(
    *,
    source_revision: str,
    release_id: str,
    profile: MusicPilotMatchProfile,
    captures: tuple[PublishedMusicCapture, ...],
    research_domain: dict[str, Any],
    status: str,
) -> dict[str, Any]:
    profile_template = _profile_template(profile=profile, captures=captures)
    request = {
        "schemaVersion": MUSIC_RELEASE_REQUEST_SCHEMA,
        "status": status,
        "domain": "MUSIC",
        "sourceRevision": source_revision,
        "workflowId": f"research-music-release-{release_id}",
        "sourcePolicyDigest": profile.source_policy_digest,
        "algorithmDigest": profile.algorithm_digest,
        "profileTemplateDigest": digest_identity(profile_template),
        "profileTemplate": profile_template,
        "researchDomain": research_domain,
        "indexProjection": _index_projection(release_id),
        "gates": {name: False for name in MUSIC_RELEASE_GATES},
    }
    _assert_integer_object_sizes(request)
    return request


def _publish_bundle(
    *,
    source_revision: str,
    golden_build: Mapping[str, Any],
    destination_prefix: str,
    store: RuntimeObjectStore,
    musicbrainz_fixture_path: Path,
    wikidata_fixture_path: Path,
    match_profile_path: Path,
    production: bool,
) -> MusicReleaseBundle:
    source_revision = _require_source_revision(source_revision)
    if production and not _is_capture_destination(destination_prefix):
        raise ValueError(
            "Music release destination must stay under the production capture prefix"
        )
    if not production and urlsplit(destination_prefix).scheme != "file":
        raise ValueError("dry-run Music release destination must use file://")

    fixtures, profile = _load_release_inputs(
        musicbrainz_fixture_path=musicbrainz_fixture_path,
        wikidata_fixture_path=wikidata_fixture_path,
        match_profile_path=match_profile_path,
    )
    release_id, prepared_golden = _prepare_golden_template(
        golden_build,
        profile=profile,
        source_revision=source_revision,
    )
    captures = tuple(
        _publish_capture(
            fixture_path=path,
            fixture=fixture,
            release_id=release_id,
            destination_prefix=destination_prefix,
            store=store,
            require_s3_metadata=production,
        )
        for path, fixture in fixtures
    )
    if tuple(item.source_product_id for item in captures) != SOURCE_ORDER:
        raise ValueError("published Music captures are not in canonical order")
    research_domain = _research_domain_mapping(
        captures=captures,
        golden_build=prepared_golden,
        release_id=release_id,
    )
    if production:
        domain_input = _domain_input_from_mapping(research_domain)
    else:
        domain_input = _local_domain_input(research_domain)
    _validate_capture_consumption(
        domain_input=domain_input,
        captures=captures,
    )
    request = _build_request(
        source_revision=source_revision,
        release_id=release_id,
        profile=profile,
        captures=captures,
        research_domain=research_domain,
        status="REVIEWED" if production else "DRAFT",
    )
    if production:
        validate_music_release_request(request)
    request_payload = canonical_json_bytes(request, newline=True)
    request_object = _publish_addressed(
        store=store,
        destination_prefix=destination_prefix,
        source_product_id="music-release",
        category="release-requests",
        suffix=".json",
        payload=request_payload,
        media_type=MUSIC_RELEASE_REQUEST_MEDIA_TYPE,
        object_format="OBJECT_FORMAT_JSON",
        require_s3_metadata=production,
    )
    return MusicReleaseBundle(
        release_id=release_id,
        request=request,
        request_object=request_object,
        captures=captures,
        publishable=production,
    )


def publish_reviewed_music_release_bundle(
    *,
    source_revision: str,
    golden_build: Mapping[str, Any],
    destination_prefix: str,
    store: RuntimeObjectStore,
    musicbrainz_fixture_path: Path = DEFAULT_MUSICBRAINZ_FIXTURE,
    wikidata_fixture_path: Path = DEFAULT_WIKIDATA_FIXTURE,
    match_profile_path: Path = DEFAULT_MATCH_PROFILE,
) -> MusicReleaseBundle:
    """Publish and HEAD-verify one production REVIEWED Music release bundle."""

    return _publish_bundle(
        source_revision=source_revision,
        golden_build=golden_build,
        destination_prefix=destination_prefix,
        store=store,
        musicbrainz_fixture_path=musicbrainz_fixture_path,
        wikidata_fixture_path=wikidata_fixture_path,
        match_profile_path=match_profile_path,
        production=True,
    )


def publish_local_music_release_bundle(
    *,
    source_revision: str,
    golden_build: Mapping[str, Any],
    destination_directory: Path,
    store: RuntimeObjectStore,
    musicbrainz_fixture_path: Path = DEFAULT_MUSICBRAINZ_FIXTURE,
    wikidata_fixture_path: Path = DEFAULT_WIKIDATA_FIXTURE,
    match_profile_path: Path = DEFAULT_MATCH_PROFILE,
) -> MusicReleaseBundle:
    """Write a non-publishable DRAFT bundle without inventing S3 metadata."""

    destination_directory.mkdir(parents=True, exist_ok=True)
    return _publish_bundle(
        source_revision=source_revision,
        golden_build=golden_build,
        destination_prefix=destination_directory.resolve().as_uri(),
        store=store,
        musicbrainz_fixture_path=musicbrainz_fixture_path,
        wikidata_fixture_path=wikidata_fixture_path,
        match_profile_path=match_profile_path,
        production=False,
    )


def _coerce_immutable_s3_reference(
    value: ObjectRef | Mapping[str, Any],
    *,
    expected_media_type: str | None = None,
) -> ObjectRef:
    reference = (
        value if isinstance(value, ObjectRef) else ObjectRef.model_validate(dict(value))
    )
    if (
        urlsplit(reference.uri).scheme != "s3"
        or reference.size_bytes <= 0
        or not reference.etag
        or not reference.object_version
    ):
        raise ValueError("reviewed request provenance must be immutable S3")
    if expected_media_type is not None and reference.media_type != expected_media_type:
        raise ValueError("reviewed request provenance mediaType is invalid")
    return _normalized_reference(reference)


def _validate_request_shape(
    request: Mapping[str, Any],
    *,
    expected_status: str,
    expected_image_digest: str,
    resolved: bool = False,
) -> ResearchDomainInput:
    expected_keys = set(_REVIEWED_REQUEST_KEYS)
    if resolved:
        expected_keys.add("reviewedRequest")
    if set(request) != expected_keys:
        raise ValueError("Music release request fields differ from the v1 contract")
    if request.get("schemaVersion") != MUSIC_RELEASE_REQUEST_SCHEMA:
        raise ValueError("Music release request schemaVersion is unsupported")
    if request.get("status") != expected_status:
        raise ValueError(f"Music release request status must be {expected_status}")
    if request.get("domain") != "MUSIC":
        raise ValueError("Music release request domain must be MUSIC")
    _require_source_revision(str(request.get("sourceRevision") or ""))

    gates = request.get("gates")
    if (
        not isinstance(gates, dict)
        or set(gates) != set(MUSIC_RELEASE_GATES)
        or any(value is not False for value in gates.values())
    ):
        raise ValueError("all eight Music release gates must be false")

    profile_template = request.get("profileTemplate")
    if not isinstance(profile_template, dict):
        raise ValueError("Music release profileTemplate must be an object")
    if (
        profile_template.get("templateSchemaVersion")
        != MUSIC_RELEASE_PROFILE_TEMPLATE_SCHEMA
    ):
        raise ValueError("Music release profileTemplate schema is unsupported")
    if request.get("profileTemplateDigest") != digest_identity(profile_template):
        raise ValueError("profileTemplateDigest does not bind profileTemplate")
    for top_level, template_name in (
        ("sourcePolicyDigest", "sourcePolicyDigest"),
        ("algorithmDigest", "algorithmDigest"),
    ):
        digest = require_sha256(str(request.get(top_level) or ""))
        if profile_template.get(template_name) != digest:
            raise ValueError(f"{top_level} differs from profileTemplate")

    image_digests = _image_digest_values(request)
    if not image_digests or any(
        value != expected_image_digest for value in image_digests
    ):
        raise ValueError(f"all imageDigest fields must equal {expected_image_digest}")

    research_domain = request.get("researchDomain")
    if not isinstance(research_domain, dict):
        raise ValueError("Music release researchDomain must be an object")
    domain_input = _domain_input_from_mapping(research_domain)
    source_products = tuple(source.source_product_id for source in domain_input.sources)
    if source_products != SOURCE_ORDER:
        raise ValueError("Music release sources are not in canonical order")
    if any(
        not source.identity_spine
        or source.materialization
        or not source.gold_contribution
        for source in domain_input.sources
    ):
        raise ValueError("Music release source contribution flags drifted")
    build_id = domain_input.golden_build.build_id
    if request.get("workflowId") != f"research-music-release-{build_id}":
        raise ValueError("Music release workflowId differs from Golden buildId")

    source_pins = profile_template.get("sourcePins")
    if (
        not isinstance(source_pins, list)
        or tuple(
            item.get("sourceProductId") if isinstance(item, dict) else None
            for item in source_pins
        )
        != SOURCE_ORDER
    ):
        raise ValueError("profileTemplate source pins are not in canonical order")
    baseline_pins = profile_template.get("sourceSnapshotPinIds")
    source_revisions = profile_template.get("sourceRevisions")
    fixture_digests = profile_template.get("sourceFixtureDigests")
    if not all(
        isinstance(item, dict)
        for item in (baseline_pins, source_revisions, fixture_digests)
    ):
        raise ValueError("profileTemplate source baseline maps are invalid")
    for source, pin in zip(
        research_domain["sources"],
        source_pins,
        strict=True,
    ):
        source_product_id = source.get("sourceProductId")
        if not isinstance(pin, dict) or pin.get("captureManifest") != source.get(
            "captureManifest"
        ):
            raise ValueError("profileTemplate source pin capture binding drifted")
        require_sha256(str(pin.get("sourceSnapshotPinId") or ""))
        baseline_pin = require_sha256(str(pin.get("baselineSourceSnapshotPinId") or ""))
        if (
            baseline_pins.get(source_product_id) != baseline_pin
            or source_revisions.get(source_product_id) != pin.get("sourceRevision")
            or fixture_digests.get(source_product_id) != pin.get("fixtureDigest")
        ):
            raise ValueError("profileTemplate source baseline pin drifted")

    projection = request.get("indexProjection")
    if (
        not isinstance(projection, dict)
        or set(projection)
        != {
            "projectionId",
            "shards",
            "replicas",
            "bulkChunkSize",
            "bulkMaxChunkBytes",
        }
        or not str(projection.get("projectionId") or "").startswith(
            "music-reference-"
        )
        or projection.get("shards") != 6
        or projection.get("replicas") != 1
        or projection.get("bulkChunkSize") != 500
        or projection.get("bulkMaxChunkBytes") != 5 * 1024 * 1024
    ):
        raise ValueError("Music release indexProjection must be an object")
    _assert_integer_object_sizes(request)
    return domain_input


def validate_music_release_request(
    request: Mapping[str, Any],
) -> ResearchDomainInput:
    """Fail closed over one initial REVIEWED request."""

    return _validate_request_shape(
        request,
        expected_status="REVIEWED",
        expected_image_digest=ZERO_IMAGE_DIGEST,
    )


def _verify_reviewed_request_reference(
    request: Mapping[str, Any],
    reference: ObjectRef,
) -> None:
    payload = canonical_json_bytes(request, newline=True)
    if (
        reference.size_bytes != len(payload)
        or reference.checksum.value != hashlib.sha256(payload).hexdigest()
    ):
        raise ValueError(
            "reviewed request ObjectRef does not bind the canonical request"
        )


def resolve_music_release_request(
    request: Mapping[str, Any],
    *,
    image_digest: str,
    reviewed_request: ObjectRef | Mapping[str, Any],
) -> dict[str, Any]:
    """Resolve only imageDigest placeholders and bind REVIEWED provenance."""

    actual_digest = require_sha256(
        image_digest,
        label="Music release runtime image digest",
    )
    if actual_digest == ZERO_IMAGE_DIGEST:
        raise ValueError("runtime image digest must be non-zero")
    reviewed_reference = _coerce_immutable_s3_reference(
        reviewed_request,
        expected_media_type=MUSIC_RELEASE_REQUEST_MEDIA_TYPE,
    )
    value = copy.deepcopy(dict(request))

    if "reviewedRequest" in value:
        _validate_request_shape(
            value,
            expected_status="REVIEWED",
            expected_image_digest=actual_digest,
            resolved=True,
        )
        if value.get("reviewedRequest") != object_ref_mapping(reviewed_reference):
            raise ValueError(
                "resolved request provenance differs from supplied ObjectRef"
            )
        reviewed_value = copy.deepcopy(value)
        reviewed_value.pop("reviewedRequest")
        reviewed_value, count = _transform_image_digests(
            reviewed_value,
            ZERO_IMAGE_DIGEST,
        )
        if count < 1:
            raise ValueError("resolved request contains no imageDigest bindings")
        reviewed_value["profileTemplateDigest"] = digest_identity(
            reviewed_value["profileTemplate"]
        )
        validate_music_release_request(reviewed_value)
        _verify_reviewed_request_reference(reviewed_value, reviewed_reference)
        return value

    validate_music_release_request(value)
    _verify_reviewed_request_reference(value, reviewed_reference)
    resolved, count = _transform_image_digests(value, actual_digest)
    if count < 1:
        raise ValueError("reviewed request contains no imageDigest placeholders")
    resolved["profileTemplateDigest"] = digest_identity(resolved["profileTemplate"])
    resolved["reviewedRequest"] = object_ref_mapping(reviewed_reference)
    _validate_request_shape(
        resolved,
        expected_status="REVIEWED",
        expected_image_digest=actual_digest,
        resolved=True,
    )
    return resolved
