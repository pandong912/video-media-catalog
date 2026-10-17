from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from video_media_catalog.canonical import canonical_json_bytes
from video_media_catalog.community_sources import build_community_registry
from video_media_catalog.connector import (
    ConnectorBatchManifest,
    ConnectorRecordEnvelope,
    ConnectorRecordSetManifest,
    Serialization,
)
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.music_release import (
    MUSIC_RELEASE_GATES,
    MUSIC_RELEASE_REQUEST_MEDIA_TYPE,
    SOURCE_ORDER,
    ZERO_IMAGE_DIGEST,
    object_ref_mapping,
    publish_reviewed_music_release_bundle,
    resolve_music_release_request,
    validate_music_release_request,
)
from video_media_catalog.music_release_cli import _ProtectedS3Client, main
from video_media_catalog.object_store import ObjectStoreError, UploadResult
from video_media_catalog.source_silver import build_source_silver_rows
from video_media_catalog.temporal.research_domain_admin_cli import (
    _domain_input_from_mapping,
)
from video_media_catalog.v2_contracts import digest_identity

CATALOG_ROOT = "s3://ai-video-platform-dev-media-catalog-209479308066"
CAPTURE_PREFIX = f"{CATALOG_ROOT}/landing/research/capture"
WAREHOUSE = f"{CATALOG_ROOT}/warehouse"
EMR_ROLE = (
    "arn:aws:iam::209479308066:role/ai-video-platform-dev-media-catalog-emr-runtime"
)
SOURCE_REVISION = "9" * 40
RUNTIME_DIGEST = "sha256:" + ("f" * 64)


class _FakeS3Store:
    def __init__(self) -> None:
        self.payloads: dict[str, bytes] = {}
        self.references: dict[str, ObjectRef] = {}
        self.head_calls: list[str] = []

    def upload_bytes(
        self,
        payload: bytes,
        destination_uri: str,
        *,
        media_type: str,
        object_format: str,
        max_bytes: int,
    ) -> UploadResult:
        return self.publish_bytes_conditional(
            payload,
            destination_uri,
            media_type=media_type,
            object_format=object_format,
            max_bytes=max_bytes,
        )

    def publish_bytes_conditional(
        self,
        payload: bytes,
        destination_uri: str,
        *,
        media_type: str,
        object_format: str,
        max_bytes: int,
    ) -> UploadResult:
        if len(payload) > max_bytes:
            raise ObjectStoreError("OBJECT_TOO_LARGE", "test object too large")
        digest = hashlib.sha256(payload).hexdigest()
        existing = self.payloads.get(destination_uri)
        if existing is not None and existing != payload:
            raise ObjectStoreError(
                "IMMUTABLE_OBJECT_CONFLICT",
                "test object changed",
            )
        reference = ObjectRef(
            uri=destination_uri,
            format=object_format,
            media_type=media_type,
            checksum=Checksum(value=digest),
            size_bytes=len(payload),
            etag=f"etag-{digest}",
            object_version=f"version-{digest}",
        )
        self.payloads[destination_uri] = payload
        self.references[destination_uri] = reference
        return UploadResult(reference, existing is not None)

    def head(
        self,
        object_ref: ObjectRef,
        *,
        max_bytes: int,
        require_immutable_metadata: bool = False,
    ) -> ObjectRef:
        self.head_calls.append(object_ref.uri)
        expected = self.references[object_ref.uri]
        if (
            expected.checksum != object_ref.checksum
            or expected.size_bytes != object_ref.size_bytes
            or expected.etag != object_ref.etag
            or expected.object_version != object_ref.object_version
            or expected.size_bytes > max_bytes
        ):
            raise ObjectStoreError("OBJECT_MISMATCH", "test reference drifted")
        if require_immutable_metadata and (
            not expected.etag or not expected.object_version
        ):
            raise ObjectStoreError(
                "IMMUTABLE_METADATA_REQUIRED",
                "test metadata missing",
            )
        return expected

    def verify(self, object_ref: ObjectRef, *, max_bytes: int) -> None:
        self.head(object_ref, max_bytes=max_bytes)


def _ref(uri: str, marker: str) -> dict[str, Any]:
    return {
        "uri": uri,
        "format": "OBJECT_FORMAT_JSON",
        "mediaType": "application/json",
        "checksum": {
            "algorithm": "CHECKSUM_ALGORITHM_SHA256",
            "encoding": "CHECKSUM_ENCODING_HEX",
            "value": marker * 64,
        },
        "sizeBytes": 128,
        "etag": f"etag-{marker}",
        "objectVersion": f"version-{marker}",
    }


def _run(marker: str) -> str:
    return "sha256:" + (marker * 64)


def _golden_build() -> dict[str, Any]:
    return {
        "buildId": "replace-me",
        "identityGenerationId": "pure-source-2026-09-r1",
        "baselineSnapshot": _ref(
            f"{WAREHOUSE}/research/control/golden-builds/baselines/base.json",
            "a",
        ),
        "tmdbPipelineSummary": _ref(
            f"{CATALOG_ROOT}/landing/research/pipeline-summaries/tmdb/base.json",
            "b",
        ),
        "baselineSourceRunIds": [_run("1"), _run("2"), _run("3")],
        "baselineIdentityRunId": _run("4"),
        "baselineIdentityManifest": _ref(
            f"{WAREHOUSE}/research/control/identity/base.json",
            "c",
        ),
        "baselineIdentitySourceRunIds": [_run("3")],
        "tmdbSourceRunIds": [_run("5")],
        "sourceWatermarks": {
            "imdb-non-commercial-datasets": "2026-09-20T22:03:01Z",
            "tmdb-research": "2026-10-07",
            "tvmaze-public-api": "2026-09-25T18:45:56Z",
            "wikidata-json-dump": "20260914",
        },
        "freshnessOverride": {
            "reason": "Reviewed baseline pins for the Music release.",
            "imdbAcquiredAt": "2026-09-20T22:03:01Z",
            "tvmazeAcquiredAt": "2026-09-25T18:45:56Z",
            "maxSloHours": 720,
        },
        "env": {
            "awsRegion": "us-east-1",
            "warehouseUri": WAREHOUSE,
            "controlPrefix": f"{WAREHOUSE}/research/control",
            "goldOutputPrefix": f"{WAREHOUSE}/research/gold",
            "pipelineSummaryPrefix": (
                f"{WAREHOUSE}/research/control/golden-builds/summaries"
            ),
            "emrApplicationName": (
                "ai-video-platform-dev-media-catalog-wikidata-reference"
            ),
            "goldEmrApplicationName": (
                "ai-video-platform-dev-media-catalog-golden-release"
            ),
            "emrExecutionRoleArn": EMR_ROLE,
            "emrLogUri": (f"{CATALOG_ROOT}/raw/wikidata/emr-serverless-logs/"),
            "goldEmrLogUri": f"{CATALOG_ROOT}/raw/golden/emr-serverless-logs/",
            "researchSilverEntryPoint": (
                "local:///opt/video-media-catalog/src/"
                "video_media_catalog/research_silver_cli.py"
            ),
            "goldEntryPoint": (
                "local:///opt/video-media-catalog/src/video_media_catalog/gold_cli.py"
            ),
            "imageDigest": "sha256:" + ("e" * 64),
            "catalogType": "glue",
            "catalogName": "media",
            "glueNamespace": "video_media_catalog",
        },
    }


def _bundle(store: _FakeS3Store):
    return publish_reviewed_music_release_bundle(
        source_revision=SOURCE_REVISION,
        golden_build=_golden_build(),
        destination_prefix=f"{CAPTURE_PREFIX}/music-release/v1",
        store=store,
    )


def _assert_integer_sizes(value: Any) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            if key == "sizeBytes":
                assert type(item) is int
            _assert_integer_sizes(item)
    elif isinstance(value, list):
        for item in value:
            _assert_integer_sizes(item)


def _image_values(value: Any) -> list[Any]:
    result: list[Any] = []
    if isinstance(value, dict):
        for key, item in value.items():
            if key == "imageDigest":
                result.append(item)
            else:
                result.extend(_image_values(item))
    elif isinstance(value, list):
        for item in value:
            result.extend(_image_values(item))
    return result


def _reviewed_reference(request: dict[str, Any]) -> ObjectRef:
    payload = canonical_json_bytes(request, newline=True)
    digest = hashlib.sha256(payload).hexdigest()
    return ObjectRef(
        uri=f"s3://reviewed-requests/{digest}.json",
        format="OBJECT_FORMAT_JSON",
        media_type=MUSIC_RELEASE_REQUEST_MEDIA_TYPE,
        checksum=Checksum(value=digest),
        size_bytes=len(payload),
        etag=f"etag-{digest}",
        object_version=f"version-{digest}",
    )


def test_reviewed_bundle_is_deterministic_ordered_and_consumable() -> None:
    store = _FakeS3Store()
    first = _bundle(store)
    second = _bundle(store)

    assert first.request == second.request
    assert first.request_object == second.request_object
    assert first.request_object.uri.startswith(
        f"{CATALOG_ROOT}/landing/research/music/release-requests/"
    )
    assert first.captures == second.captures
    assert first.publishable is True
    assert tuple(item.source_product_id for item in first.captures) == SOURCE_ORDER
    assert first.request["status"] == "REVIEWED"
    assert first.request["domain"] == "MUSIC"
    assert len(first.request["gates"]) == 8
    assert tuple(first.request["gates"]) == MUSIC_RELEASE_GATES
    assert set(first.request["gates"].values()) == {False}
    assert first.request["workflowId"].startswith("research-music-release-")
    assert first.request["indexProjection"] == {
        "projectionId": f"music-reference-{first.release_id[:24]}",
        "shards": 6,
        "replicas": 1,
        "bulkChunkSize": 500,
        "bulkMaxChunkBytes": 5 * 1024 * 1024,
    }
    assert set(_image_values(first.request)) == {ZERO_IMAGE_DIGEST}
    _assert_integer_sizes(first.request)

    domain = validate_music_release_request(first.request)
    assert domain == _domain_input_from_mapping(first.request["researchDomain"])
    assert tuple(source.source_product_id for source in domain.sources) == SOURCE_ORDER
    assert all(
        source.identity_spine
        and not source.materialization
        and source.gold_contribution
        for source in domain.sources
    )
    golden_input = _golden_build()
    golden_output = first.request["researchDomain"]["goldenBuild"]
    assert golden_output["buildId"] == first.release_id
    assert golden_output["env"]["imageDigest"] == ZERO_IMAGE_DIGEST
    for key in golden_input:
        if key not in {"buildId", "env"}:
            assert golden_output[key] == golden_input[key]
    assert {
        key: value
        for key, value in golden_output["env"].items()
        if key != "imageDigest"
    } == {
        key: value for key, value in golden_input["env"].items() if key != "imageDigest"
    }

    for capture in first.captures:
        references = (
            capture.raw_object,
            capture.batch_manifest_object,
            capture.envelope_object,
            capture.record_set_manifest_object,
            capture.capture_manifest_object,
        )
        assert all(
            reference.uri.startswith(f"{CAPTURE_PREFIX}/")
            and reference.etag
            and reference.object_version
            and reference.size_bytes > 0
            for reference in references
        )
        assert all(
            reference.checksum.value in reference.uri for reference in references
        )

        batch = ConnectorBatchManifest.model_validate_json(
            store.payloads[capture.batch_manifest_object.uri]
        )
        assert batch.serialization is Serialization.JSON_LINES
        record_set = ConnectorRecordSetManifest.model_validate_json(
            store.payloads[capture.record_set_manifest_object.uri]
        )
        envelopes = tuple(
            ConnectorRecordEnvelope.model_validate_json(line)
            for line in store.payloads[capture.envelope_object.uri].splitlines()
        )
        assert all(
            item.uri.startswith("s3://") and item.etag and item.object_version
            for item in batch.raw_objects
        )
        assert all(
            envelope.raw_object.uri.startswith("s3://")
            and envelope.raw_object.etag
            and envelope.raw_object.object_version
            for envelope in envelopes
        )
        assert record_set.record_objects == (capture.envelope_object,)
        run, rows = build_source_silver_rows(
            registry=build_community_registry(),
            batch=batch,
            record_set=record_set,
            envelopes=envelopes,
        )
        assert run.input_manifest["sourceSnapshotPinId"] == (
            capture.source_snapshot_pin_id
        )
        assert len(rows["community_source_record"]) == record_set.record_count

    assert len(store.head_calls) >= 11
    assert store.payloads[first.request_object.uri] == canonical_json_bytes(
        first.request,
        newline=True,
    )
    round_tripped = json.loads(store.payloads[first.request_object.uri])
    assert validate_music_release_request(round_tripped)


def test_reviewed_request_destination_is_fail_closed() -> None:
    with pytest.raises(ValueError, match="production request prefix"):
        publish_reviewed_music_release_bundle(
            source_revision=SOURCE_REVISION,
            golden_build=_golden_build(),
            destination_prefix=f"{CAPTURE_PREFIX}/music-release/v1",
            request_destination_prefix=f"{CAPTURE_PREFIX}/wrong",
            store=_FakeS3Store(),
        )


def test_resolve_is_fail_closed_exact_and_idempotent() -> None:
    bundle = _bundle(_FakeS3Store())
    reviewed = copy.deepcopy(bundle.request)
    reviewed["profileTemplate"]["unrelatedZeroDigest"] = ZERO_IMAGE_DIGEST
    reviewed["profileTemplateDigest"] = digest_identity(reviewed["profileTemplate"])
    reference = _reviewed_reference(reviewed)

    resolved = resolve_music_release_request(
        reviewed,
        image_digest=RUNTIME_DIGEST,
        reviewed_request=reference,
    )
    assert resolved["status"] == "REVIEWED"
    assert set(_image_values(resolved)) == {RUNTIME_DIGEST}
    assert resolved["profileTemplate"]["unrelatedZeroDigest"] == ZERO_IMAGE_DIGEST
    assert resolved["profileTemplateDigest"] == digest_identity(
        resolved["profileTemplate"]
    )
    assert resolved["reviewedRequest"] == object_ref_mapping(reference)
    assert _domain_input_from_mapping(resolved["researchDomain"])
    assert (
        resolve_music_release_request(
            resolved,
            image_digest=RUNTIME_DIGEST,
            reviewed_request=reference,
        )
        == resolved
    )
    assert (
        resolve_music_release_request(
            reviewed,
            image_digest=RUNTIME_DIGEST,
            reviewed_request=reference,
        )
        == resolved
    )

    gate_drift = copy.deepcopy(reviewed)
    gate_drift["gates"]["cutoverAllowed"] = True
    with pytest.raises(ValueError, match="gates"):
        resolve_music_release_request(
            gate_drift,
            image_digest=RUNTIME_DIGEST,
            reviewed_request=_reviewed_reference(gate_drift),
        )

    prebound = copy.deepcopy(reviewed)
    prebound["researchDomain"]["sources"][0]["env"]["imageDigest"] = RUNTIME_DIGEST
    with pytest.raises(ValueError, match="all imageDigest"):
        resolve_music_release_request(
            prebound,
            image_digest=RUNTIME_DIGEST,
            reviewed_request=_reviewed_reference(prebound),
        )

    source_order_drift = copy.deepcopy(reviewed)
    source_order_drift["researchDomain"]["sources"].reverse()
    with pytest.raises(ValueError, match="ordered MusicBrainz"):
        resolve_music_release_request(
            source_order_drift,
            image_digest=RUNTIME_DIGEST,
            reviewed_request=_reviewed_reference(source_order_drift),
        )

    profile_drift = copy.deepcopy(reviewed)
    profile_drift["profileTemplate"]["cohortId"] = "drifted"
    with pytest.raises(ValueError, match="profileTemplateDigest"):
        resolve_music_release_request(
            profile_drift,
            image_digest=RUNTIME_DIGEST,
            reviewed_request=_reviewed_reference(profile_drift),
        )

    wrong_reference = _reviewed_reference(reviewed)
    wrong_reference = wrong_reference.model_copy(
        update={"size_bytes": wrong_reference.size_bytes + 1}
    )
    with pytest.raises(ValueError, match="does not bind"):
        resolve_music_release_request(
            reviewed,
            image_digest=RUNTIME_DIGEST,
            reviewed_request=wrong_reference,
        )
    with pytest.raises(ValueError, match="non-zero"):
        resolve_music_release_request(
            reviewed,
            image_digest=ZERO_IMAGE_DIGEST,
            reviewed_request=reference,
        )


def test_publish_reviewed_dry_run_writes_local_non_publishable_bundle(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    golden_path = tmp_path / "golden.json"
    golden_path.write_text(json.dumps(_golden_build()), encoding="utf-8")
    bundle_directory = tmp_path / "bundle"
    request_path = tmp_path / "request.json"

    assert (
        main(
            [
                "publish-reviewed",
                "--source-revision",
                SOURCE_REVISION,
                "--golden-build-json",
                str(golden_path),
                "--dry-run",
                "--bundle-directory",
                str(bundle_directory),
                "--request-output",
                str(request_path),
            ]
        )
        == 0
    )
    output = json.loads(capsys.readouterr().out)
    request = json.loads(request_path.read_text(encoding="utf-8"))
    assert output["dryRun"] is True
    assert output["publishable"] is False
    assert request["status"] == "DRAFT"
    assert output["requestTemplate"]["uri"].startswith("file://")
    assert bundle_directory.is_dir()


def test_s3_client_enforces_owner_and_kms_controls() -> None:
    class Client:
        def __init__(self) -> None:
            self.calls: list[tuple[str, dict[str, Any]]] = []

        def put_object(self, **values: Any) -> dict[str, Any]:
            self.calls.append(("put", values))
            return {}

        def head_object(self, **values: Any) -> dict[str, Any]:
            self.calls.append(("head", values))
            return {
                "ServerSideEncryption": "aws:kms",
                "SSEKMSKeyId": "arn:aws:kms:us-east-1:123456789012:key/test",
            }

    client = Client()
    protected = _ProtectedS3Client(
        client,
        kms_key_id="arn:aws:kms:us-east-1:123456789012:key/test",
        expected_bucket_owner="123456789012",
    )
    protected.put_object(Bucket="bucket", Key="key")
    protected.head_object(Bucket="bucket", Key="key")

    assert client.calls[0][1]["ExpectedBucketOwner"] == "123456789012"
    assert client.calls[0][1]["ServerSideEncryption"] == "aws:kms"
    assert client.calls[0][1]["SSEKMSKeyId"].endswith(":key/test")
    assert client.calls[1][1]["ExpectedBucketOwner"] == "123456789012"
