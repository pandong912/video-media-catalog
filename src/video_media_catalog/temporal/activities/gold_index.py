"""EKS Job launch and independent validation for Research Gold indexing."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import ssl
import tempfile
import time
import urllib.error
import urllib.request
from dataclasses import asdict
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit

from temporalio import activity
from temporalio.exceptions import ApplicationError

from video_media_catalog.canonical import canonical_json_bytes
from video_media_catalog.catalog_source_policy import load_catalog_source_policy
from video_media_catalog.gold_index_cli import INDEX_JOB_RESULT_MEDIA_TYPE
from video_media_catalog.gold_search_index import (
    GOLD_INDEX_BUILD_MANIFEST_MEDIA_TYPE,
    GOLD_INDEX_VALIDATION_RECEIPT_MEDIA_TYPE,
    RESEARCH_INDEX_PREFIX,
    RESEARCH_READ_ALIAS,
    GoldIndexBuildManifest,
    GoldIndexValidationReceipt,
    gold_index_config_identity,
    validate_gold_index_contents,
    validate_gold_index_mapping,
)
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.object_store import BoundedObjectStore, S3Location
from video_media_catalog.opensearch_client import (
    OpenSearchConnection,
    create_opensearch_client,
)
from video_media_catalog.storage import digest_file, join_uri, local_path
from video_media_catalog.temporal.gold_index_models import (
    GoldIndexJobResult,
    GoldIndexProjectionInput,
)
from video_media_catalog.temporal.models import ObjectRefPayload

_CONTROL_MAX_BYTES = 16 * 1024 * 1024
_KUBERNETES_TOKEN = Path("/var/run/secrets/kubernetes.io/serviceaccount/token")
_KUBERNETES_CA = Path("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
_SAFE_ID = re.compile(r"^[a-z0-9][a-z0-9-]{0,62}$")
_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
_SPARK_MEMORY = re.compile(r"^[1-9][0-9]*[mg]$")
_RUNTIME_UID = 10001


def _object_ref(payload: ObjectRefPayload) -> ObjectRef:
    return ObjectRef(
        uri=payload.uri,
        format=payload.format,
        media_type=payload.media_type,
        checksum=Checksum(
            algorithm="CHECKSUM_ALGORITHM_SHA256",
            value=payload.checksum_digest(),
        ),
        size_bytes=payload.size_bytes,
        etag=payload.etag,
        object_version=payload.object_version,
    )


def _payload(reference: ObjectRef) -> ObjectRefPayload:
    return ObjectRefPayload.from_mapping(
        reference.model_dump(mode="json", by_alias=True, exclude_none=True)
    )


def _image_digest(image: str) -> str:
    _, separator, digest = image.rpartition("@")
    if not separator or _SHA256.fullmatch(digest) is None:
        raise ValueError("builder image must be pinned by @sha256 digest")
    return digest


def _validate_input(input_data: GoldIndexProjectionInput) -> None:
    if _SAFE_ID.fullmatch(input_data.projection_id) is None:
        raise ValueError("projection_id must be a safe lowercase identifier")
    if _SHA256.fullmatch(input_data.expected_release_plan_id) is None:
        raise ValueError("expected_release_plan_id must be a SHA-256 digest")
    release = _object_ref(input_data.release_commit)
    if release.size_bytes < 1:
        raise ValueError("release commit must not be empty")
    if release.uri.startswith("s3://") and (
        release.etag is None or release.object_version is None
    ):
        raise ValueError("S3 release commit must be version and ETag pinned")
    env = input_data.env
    for value, label in (
        (env.kubernetes_namespace, "Kubernetes namespace"),
        (env.builder_service_account, "builder service account"),
    ):
        if _SAFE_ID.fullmatch(value) is None:
            raise ValueError(f"{label} is invalid")
    _image_digest(env.builder_image)
    if _SPARK_MEMORY.fullmatch(env.spark_driver_memory) is None:
        raise ValueError("Spark driver memory must use a positive m or g value")
    OpenSearchConnection(
        endpoint=env.opensearch_endpoint,
        aws_region=env.aws_region,
        service=env.opensearch_service,
        timeout_seconds=float(env.request_timeout_seconds),
    )
    for value, label in (
        (env.warehouse_uri, "warehouse URI"),
        (env.manifest_prefix, "manifest prefix"),
        (env.result_prefix, "result prefix"),
        (env.validation_prefix, "validation prefix"),
    ):
        parsed = urlsplit(value)
        if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.lstrip("/"):
            raise ValueError(f"{label} must be a non-empty s3:// URI")
    if not 60 <= env.active_deadline_seconds <= 86_400:
        raise ValueError("active deadline must be between 60 seconds and 24 hours")
    if env.min_free_storage_gib < 1:
        raise ValueError("minimum OpenSearch free storage must be positive")
    if not 1 <= env.max_jvm_pressure_percent <= 99:
        raise ValueError("maximum OpenSearch JVM pressure must be between 1 and 99")
    if env.max_cluster_pending_tasks < 0:
        raise ValueError("maximum OpenSearch pending tasks must not be negative")


def _job_name(input_data: GoldIndexProjectionInput) -> str:
    suffix = hashlib.sha256(canonical_json_bytes(asdict(input_data))).hexdigest()[:12]
    prefix = input_data.projection_id[:35].rstrip("-")
    return f"research-index-{prefix}-{suffix}"


def _result_uri(input_data: GoldIndexProjectionInput) -> str:
    return join_uri(
        input_data.env.result_prefix,
        f"{input_data.projection_id}.json",
    )


def _job_args(
    input_data: GoldIndexProjectionInput,
    *,
    completed_at: str,
) -> list[str]:
    release = input_data.release_commit
    env = input_data.env
    arguments = [
        "--release-commit-uri",
        release.uri,
        "--release-commit-hash",
        release.checksum_value,
        "--release-commit-size",
        str(release.size_bytes),
        "--manifest-prefix",
        env.manifest_prefix,
        "--result-uri",
        _result_uri(input_data),
        "--completed-at",
        completed_at,
        "--image-digest",
        _image_digest(env.builder_image),
        "--catalog-type",
        env.catalog_type,
        "--catalog-name",
        env.catalog_name,
        "--namespace",
        env.glue_namespace,
        "--warehouse",
        env.warehouse_uri,
        "--aws-region",
        env.aws_region,
        "--opensearch-endpoint",
        env.opensearch_endpoint,
        "--opensearch-service",
        env.opensearch_service,
        "--read-alias",
        RESEARCH_READ_ALIAS,
        "--index-prefix",
        RESEARCH_INDEX_PREFIX,
        "--shards",
        str(env.shards),
        "--replicas",
        str(env.replicas),
        "--bulk-chunk-size",
        str(env.bulk_chunk_size),
        "--bulk-max-chunk-bytes",
        str(env.bulk_max_chunk_bytes),
        "--bulk-partitions",
        str(env.bulk_partitions),
        "--bulk-workers",
        str(env.bulk_workers),
        "--master",
        env.spark_master,
        "--shuffle-partitions",
        str(env.shuffle_partitions),
        "--request-timeout-seconds",
        str(env.request_timeout_seconds),
        "--min-free-storage-gib",
        str(env.min_free_storage_gib),
        "--max-jvm-pressure-percent",
        str(env.max_jvm_pressure_percent),
        "--max-cluster-pending-tasks",
        str(env.max_cluster_pending_tasks),
    ]
    if release.object_version:
        arguments.extend(["--release-commit-version", release.object_version])
    if release.etag:
        arguments.extend(["--release-commit-etag", release.etag])
    return arguments


def _job_manifest(
    input_data: GoldIndexProjectionInput,
    *,
    completed_at: str,
) -> dict[str, Any]:
    env = input_data.env
    input_digest = (
        "sha256:"
        + hashlib.sha256(
            canonical_json_bytes(
                {
                    "input": asdict(input_data),
                    "completedAt": completed_at,
                }
            )
        ).hexdigest()
    )
    name = _job_name(input_data)
    labels = {
        "app.kubernetes.io/name": "research-gold-index",
        "app.kubernetes.io/component": "builder",
        "video-world.io/projection-id": input_data.projection_id,
    }
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": name,
            "namespace": env.kubernetes_namespace,
            "labels": labels,
            "annotations": {
                "video-world.io/input-digest": input_digest,
                "video-world.io/release-plan-id": (input_data.expected_release_plan_id),
            },
        },
        "spec": {
            "backoffLimit": 0,
            "activeDeadlineSeconds": env.active_deadline_seconds,
            "ttlSecondsAfterFinished": 604_800,
            "template": {
                "metadata": {"labels": labels},
                "spec": {
                    "serviceAccountName": env.builder_service_account,
                    "automountServiceAccountToken": True,
                    "restartPolicy": "Never",
                    "terminationGracePeriodSeconds": 30,
                    "securityContext": {
                        "runAsNonRoot": True,
                        "runAsUser": _RUNTIME_UID,
                        "runAsGroup": _RUNTIME_UID,
                        "fsGroup": _RUNTIME_UID,
                        "seccompProfile": {"type": "RuntimeDefault"},
                    },
                    "nodeSelector": dict(sorted(env.node_selector.items())),
                    "containers": [
                        {
                            "name": "builder",
                            "image": env.builder_image,
                            "imagePullPolicy": "IfNotPresent",
                            "command": ["video-media-catalog-gold-index"],
                            "workingDir": "/work",
                            "args": _job_args(
                                input_data,
                                completed_at=completed_at,
                            ),
                            "env": [
                                {"name": "AWS_REGION", "value": env.aws_region},
                                {
                                    "name": "AWS_DEFAULT_REGION",
                                    "value": env.aws_region,
                                },
                                {
                                    "name": "SPARK_LOCAL_DIRS",
                                    "value": "/work/spark",
                                },
                                {
                                    "name": "PYSPARK_SUBMIT_ARGS",
                                    "value": (
                                        "--driver-memory "
                                        f"{env.spark_driver_memory} "
                                        "pyspark-shell"
                                    ),
                                },
                                {"name": "TMPDIR", "value": "/work/tmp"},
                                {"name": "HOME", "value": "/work"},
                            ],
                            "resources": {
                                "requests": {
                                    "cpu": env.cpu_request,
                                    "memory": env.memory_request,
                                    "ephemeral-storage": (
                                        env.ephemeral_storage_request
                                    ),
                                },
                                "limits": {
                                    "cpu": env.cpu_limit,
                                    "memory": env.memory_limit,
                                    "ephemeral-storage": (env.ephemeral_storage_limit),
                                },
                            },
                            "securityContext": {
                                "allowPrivilegeEscalation": False,
                                "readOnlyRootFilesystem": True,
                                "runAsNonRoot": True,
                                "runAsUser": _RUNTIME_UID,
                                "capabilities": {"drop": ["ALL"]},
                            },
                            "terminationMessagePolicy": "FallbackToLogsOnError",
                            "volumeMounts": [
                                {"name": "work", "mountPath": "/work"},
                                {"name": "spark-local", "mountPath": "/work/spark"},
                                {"name": "tmp", "mountPath": "/work/tmp"},
                                {"name": "tmp", "mountPath": "/tmp"},
                            ],
                        }
                    ],
                    "volumes": [
                        {"name": "work", "emptyDir": {}},
                        {"name": "spark-local", "emptyDir": {}},
                        {"name": "tmp", "emptyDir": {}},
                    ],
                },
            },
        },
    }


class _KubernetesApi:
    def __init__(
        self,
        *,
        host: str | None = None,
        port: str | None = None,
        token_path: Path = _KUBERNETES_TOKEN,
        ca_path: Path = _KUBERNETES_CA,
    ) -> None:
        api_host = host or os.environ.get("KUBERNETES_SERVICE_HOST", "")
        api_port = port or os.environ.get("KUBERNETES_SERVICE_PORT_HTTPS", "443")
        if not api_host:
            raise RuntimeError("Kubernetes in-cluster API host is unavailable")
        self.base_url = f"https://{api_host}:{api_port}"
        self.token = token_path.read_text(encoding="utf-8").strip()
        if not self.token:
            raise RuntimeError("Kubernetes service-account token is empty")
        self.ssl_context = ssl.create_default_context(cafile=str(ca_path))

    def request(
        self,
        method: str,
        path: str,
        *,
        body: dict[str, Any] | None = None,
    ) -> tuple[int, dict[str, Any]]:
        payload = None if body is None else canonical_json_bytes(body)
        request = urllib.request.Request(
            self.base_url + path,
            data=payload,
            method=method,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(
                request,
                context=self.ssl_context,
                timeout=30,
            ) as response:
                return response.status, json.loads(response.read())
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            error = json.loads(raw) if raw else {}
            return exc.code, error

    def create_or_get_job(self, manifest: dict[str, Any]) -> dict[str, Any]:
        metadata = manifest["metadata"]
        namespace = quote(metadata["namespace"], safe="")
        name = quote(metadata["name"], safe="")
        collection = f"/apis/batch/v1/namespaces/{namespace}/jobs"
        status, response = self.request("POST", collection, body=manifest)
        if status in {200, 201}:
            return response
        if status != 409:
            raise RuntimeError(
                f"Kubernetes Job create failed ({status}): "
                f"{response.get('message', 'unknown error')}"
            )
        status, existing = self.request("GET", f"{collection}/{name}")
        if status != 200:
            raise RuntimeError(
                f"Kubernetes Job lookup failed ({status}): "
                f"{existing.get('message', 'unknown error')}"
            )
        expected_digest = metadata["annotations"]["video-world.io/input-digest"]
        annotations = existing.get("metadata", {}).get("annotations", {})
        if annotations.get("video-world.io/input-digest") != expected_digest:
            raise RuntimeError("existing Kubernetes Job has a different input digest")
        return existing

    def assert_no_other_active_builder(
        self,
        *,
        namespace: str,
        expected_name: str,
    ) -> None:
        encoded_selector = quote(
            "app.kubernetes.io/name=research-gold-index,"
            "app.kubernetes.io/component=builder",
            safe="",
        )
        collection = (
            "/apis/batch/v1/namespaces/"
            f"{quote(namespace, safe='')}/jobs?labelSelector={encoded_selector}"
        )
        status, response = self.request("GET", collection)
        if status != 200:
            raise RuntimeError(
                f"Kubernetes Job list failed ({status}): "
                f"{response.get('message', 'unknown error')}"
            )
        active_names = []
        for item in response.get("items", []):
            name = item.get("metadata", {}).get("name")
            conditions = item.get("status", {}).get("conditions", [])
            terminal = any(
                condition.get("status") == "True"
                and condition.get("type") in {"Complete", "Failed"}
                for condition in conditions
            )
            if isinstance(name, str) and name != expected_name and not terminal:
                active_names.append(name)
        if active_names:
            raise RuntimeError(
                "another Research Gold index build is active: "
                + ",".join(sorted(active_names))
            )

    def assert_no_conflicting_jobs(
        self,
        *,
        namespace: str,
        expected_name: str,
        expected_workload: tuple[str, str],
        blocked_workloads: frozenset[tuple[str, str]],
    ) -> None:
        """Fail closed when a labeled, non-terminal conflicting Job exists."""

        collection = f"/apis/batch/v1/namespaces/{quote(namespace, safe='')}/jobs"
        status, response = self.request("GET", collection)
        if status != 200:
            raise RuntimeError(
                f"Kubernetes Job list failed ({status}): "
                f"{response.get('message', 'unknown error')}"
            )
        items = response.get("items")
        if not isinstance(items, list):
            raise RuntimeError("Kubernetes Job list response is invalid")
        conflicts: list[str] = []
        for item in items:
            if not isinstance(item, dict):
                raise RuntimeError("Kubernetes Job list item is invalid")
            metadata = item.get("metadata")
            job_status = item.get("status") or {}
            if not isinstance(metadata, dict) or not isinstance(job_status, dict):
                raise RuntimeError("Kubernetes Job metadata/status is invalid")
            labels = metadata.get("labels") or {}
            if not isinstance(labels, dict):
                raise RuntimeError("Kubernetes Job labels are invalid")
            workload = (
                str(labels.get("app.kubernetes.io/name", "")),
                str(labels.get("app.kubernetes.io/component", "")),
            )
            if workload not in blocked_workloads:
                continue
            name = metadata.get("name")
            if not isinstance(name, str) or not name:
                raise RuntimeError("conflicting Kubernetes Job has no valid name")
            conditions = job_status.get("conditions") or []
            if not isinstance(conditions, list):
                raise RuntimeError("Kubernetes Job conditions are invalid")
            terminal = any(
                isinstance(condition, dict)
                and condition.get("status") == "True"
                and condition.get("type") in {"Complete", "Failed"}
                for condition in conditions
            )
            expected_retry = name == expected_name and workload == expected_workload
            if not expected_retry and not terminal:
                conflicts.append(f"{name}({workload[0]}/{workload[1]})")
        if conflicts:
            raise RuntimeError(
                "conflicting Kubernetes Jobs are active: " + ",".join(sorted(conflicts))
            )

    def get_job(self, *, namespace: str, name: str) -> dict[str, Any]:
        path = (
            "/apis/batch/v1/namespaces/"
            f"{quote(namespace, safe='')}/jobs/{quote(name, safe='')}"
        )
        status, response = self.request("GET", path)
        if status != 200:
            raise RuntimeError(
                f"Kubernetes Job lookup failed ({status}): "
                f"{response.get('message', 'unknown error')}"
            )
        return response


def _wait_for_job(
    api: _KubernetesApi,
    *,
    namespace: str,
    name: str,
) -> None:
    while True:
        job = api.get_job(namespace=namespace, name=name)
        status = job.get("status", {})
        activity.heartbeat(
            {
                "phase": "eks-job",
                "jobName": name,
                "active": int(status.get("active", 0)),
                "succeeded": int(status.get("succeeded", 0)),
                "failed": int(status.get("failed", 0)),
            }
        )
        if int(status.get("succeeded", 0)) >= 1:
            return
        if int(status.get("failed", 0)) >= 1:
            conditions = status.get("conditions") or []
            reason = next(
                (
                    str(item.get("message") or item.get("reason"))
                    for item in conditions
                    if item.get("type") == "Failed"
                ),
                "Kubernetes Job failed",
            )
            raise ApplicationError(reason, non_retryable=True)
        time.sleep(15)


def _discover_ref(
    store: BoundedObjectStore,
    *,
    uri: str,
    media_type: str,
) -> ObjectRef:
    if urlsplit(uri).scheme == "file":
        digest, size = digest_file(local_path(uri))
        return ObjectRef(
            uri=uri,
            format="OBJECT_FORMAT_JSON",
            media_type=media_type,
            checksum=Checksum(value=digest),
            size_bytes=size,
        )
    location = S3Location.parse(uri)
    head = store.client.head_object(
        Bucket=location.bucket,
        Key=location.key,
        ChecksumMode="ENABLED",
    )
    metadata = head.get("Metadata") or {}
    checksum = metadata.get("sha256")
    if checksum is None and head.get("ChecksumSHA256"):
        checksum = base64.b64decode(head["ChecksumSHA256"]).hex()
    if not checksum:
        raise RuntimeError("control object is missing its SHA-256 checksum")
    etag = str(head.get("ETag") or "").strip('"') or None
    version = head.get("VersionId")
    if not etag or not version:
        raise RuntimeError("control object is not version and ETag pinned")
    return ObjectRef(
        uri=uri,
        format="OBJECT_FORMAT_JSON",
        media_type=media_type,
        checksum=Checksum(value=str(checksum)),
        size_bytes=int(head["ContentLength"]),
        etag=etag,
        object_version=str(version),
    )


def _read_json(
    store: BoundedObjectStore,
    reference: ObjectRef,
) -> dict[str, Any]:
    store.verify(reference, max_bytes=_CONTROL_MAX_BYTES)
    with tempfile.TemporaryDirectory(prefix="gold-index-control-") as directory:
        materialized = store.download(
            reference,
            Path(directory) / "control.json",
            max_bytes=_CONTROL_MAX_BYTES,
        )
        value = json.loads(materialized.path.read_bytes())
    if not isinstance(value, dict):
        raise RuntimeError("Gold index control object must contain a JSON object")
    return value


def _discover_optional_ref(
    store: BoundedObjectStore,
    *,
    uri: str,
    media_type: str,
) -> ObjectRef | None:
    from botocore.exceptions import ClientError

    try:
        return _discover_ref(store, uri=uri, media_type=media_type)
    except ClientError as exc:
        error = exc.response.get("Error", {})
        code = str(error.get("Code") or "")
        status = int(
            exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode") or 0
        )
        if code in {"404", "NoSuchKey", "NotFound"} or status == 404:
            return None
        raise


def _load_job_result(
    input_data: GoldIndexProjectionInput,
    store: BoundedObjectStore,
    *,
    job_name: str,
    optional: bool,
) -> GoldIndexJobResult | None:
    result_uri = _result_uri(input_data)
    result_ref = (
        _discover_optional_ref(
            store,
            uri=result_uri,
            media_type=INDEX_JOB_RESULT_MEDIA_TYPE,
        )
        if optional
        else _discover_ref(
            store,
            uri=result_uri,
            media_type=INDEX_JOB_RESULT_MEDIA_TYPE,
        )
    )
    if result_ref is None:
        return None

    result = _read_json(store, result_ref)
    if result.get("activationStatus") != "NOT_ACTIVATED":
        raise ApplicationError(
            "Gold index builder attempted an activation",
            non_retryable=True,
        )
    raw_manifest = result.get("manifest")
    if not isinstance(raw_manifest, dict):
        raise ApplicationError(
            "Gold index Job result is missing its build manifest",
            non_retryable=True,
        )
    build_ref = ObjectRef.model_validate(raw_manifest)
    if build_ref.media_type != GOLD_INDEX_BUILD_MANIFEST_MEDIA_TYPE:
        raise ApplicationError(
            "Gold index Job returned the wrong manifest media type",
            non_retryable=True,
        )
    build = GoldIndexBuildManifest.model_validate(_read_json(store, build_ref))
    if build.release_plan_id != input_data.expected_release_plan_id:
        raise ApplicationError(
            "Gold index build does not match the pinned release plan",
            non_retryable=True,
        )
    if build.release_commit != _object_ref(input_data.release_commit):
        raise ApplicationError(
            "Gold index build does not match the pinned release commit",
            non_retryable=True,
        )
    _require_current_source_policy(build)
    env = input_data.env
    expected_config = gold_index_config_identity(
        read_alias=RESEARCH_READ_ALIAS,
        index_prefix=RESEARCH_INDEX_PREFIX,
        shards=env.shards,
        replicas=env.replicas,
        bulk_chunk_size=env.bulk_chunk_size,
        bulk_max_chunk_bytes=env.bulk_max_chunk_bytes,
        image_digest=_image_digest(env.builder_image),
    )
    if (
        build.config_identity != expected_config
        or build.config_digest != expected_config.digest
    ):
        raise ApplicationError(
            "Gold index build does not match the pinned index configuration",
            non_retryable=True,
        )
    return GoldIndexJobResult(
        job_name=job_name,
        result_object=_payload(result_ref),
        build_manifest=_payload(build_ref),
    )


@activity.defn(name="SubmitGoldIndexBuildJob")
def submit_gold_index_build_job(
    input_data: GoldIndexProjectionInput,
    completed_at: str,
) -> GoldIndexJobResult:
    _validate_input(input_data)
    job_name = _job_name(input_data)
    store = BoundedObjectStore(region=input_data.env.aws_region)
    # completed_at changes on replay, so a successful Job's pod-template digest
    # cannot be reused directly. Immutable result/build evidence is reusable only
    # after every release, source-policy, and index-configuration pin is closed.
    existing = _load_job_result(
        input_data,
        store,
        job_name=job_name,
        optional=True,
    )
    if existing is not None:
        return existing

    manifest = _job_manifest(input_data, completed_at=completed_at)
    api = _KubernetesApi()
    api.assert_no_other_active_builder(
        namespace=input_data.env.kubernetes_namespace,
        expected_name=job_name,
    )
    api.create_or_get_job(manifest)
    _wait_for_job(
        api,
        namespace=input_data.env.kubernetes_namespace,
        name=job_name,
    )

    loaded = _load_job_result(
        input_data,
        store,
        job_name=job_name,
        optional=False,
    )
    if loaded is None:
        raise RuntimeError("Gold index Job completed without a result")
    return loaded


def _close(client: Any) -> None:
    transport = getattr(client, "transport", None)
    close = getattr(transport, "close", None)
    if callable(close):
        close()


def _require_current_source_policy(build: GoldIndexBuildManifest) -> None:
    if build.source_policy_digest != load_catalog_source_policy().digest:
        raise ApplicationError(
            "Gold index build does not bind the current canonical source policy",
            non_retryable=True,
        )


@activity.defn(name="ValidateGoldIndexBuild")
def validate_gold_index_build(
    input_data: GoldIndexProjectionInput,
    job_result: GoldIndexJobResult,
    validated_at: str,
) -> ObjectRefPayload:
    _validate_input(input_data)
    store = BoundedObjectStore(region=input_data.env.aws_region)
    build_ref = _object_ref(job_result.build_manifest)
    build = GoldIndexBuildManifest.model_validate(_read_json(store, build_ref))
    if build.release_plan_id != input_data.expected_release_plan_id:
        raise ApplicationError(
            "Gold index validation release plan mismatch",
            non_retryable=True,
        )
    _require_current_source_policy(build)
    client = create_opensearch_client(
        OpenSearchConnection(
            endpoint=input_data.env.opensearch_endpoint,
            aws_region=input_data.env.aws_region,
            service=input_data.env.opensearch_service,
            timeout_seconds=float(input_data.env.request_timeout_seconds),
        )
    )
    try:
        validate_gold_index_mapping(client, index_name=build.index)
        document_count = validate_gold_index_contents(
            client,
            index_name=build.index,
            release_plan_id=build.release_plan_id,
            expected_document_count=build.document_count,
            source_policy_digest=build.source_policy_digest,
        )
    finally:
        _close(client)

    receipt = GoldIndexValidationReceipt(
        build_manifest=build_ref,
        build_id=build.build_id,
        release_plan_id=build.release_plan_id,
        mapping_digest=build.mapping_digest,
        config_digest=build.config_digest,
        index=build.index,
        alias=build.alias,
        document_count=document_count,
        validated_at=validated_at,
    )
    receipt_ref = store.upload_bytes(
        receipt.json_bytes(),
        join_uri(
            input_data.env.validation_prefix,
            f"gold-index-validation-{build.build_id}.json",
        ),
        media_type=GOLD_INDEX_VALIDATION_RECEIPT_MEDIA_TYPE,
        object_format="OBJECT_FORMAT_JSON",
        max_bytes=_CONTROL_MAX_BYTES,
    ).object_ref
    return _payload(receipt_ref)
