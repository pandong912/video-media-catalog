from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient

from video_media_catalog.api import APISettings, create_app
from video_media_catalog.api_auth import Principal
from video_media_catalog.assertions import SourceNodeRef
from video_media_catalog.community_snapshot import SILVER_SNAPSHOT_MEDIA_TYPE
from video_media_catalog.identity_curation import (
    IDENTITY_CURATION_MANIFEST_MEDIA_TYPE,
    IdentityCurationAction,
    IdentityCurationOperation,
    PinnedSilverSnapshot,
    build_identity_curation_manifest,
    build_identity_curation_manifest_ref,
)
from video_media_catalog.identity_review import (
    IdentityConflictQueueItem,
    IdentityConflictQueuePage,
    IdentityCurationRequestState,
    IdentityCurationRequestStatus,
)
from video_media_catalog.identity_v2 import build_identity_conflict
from video_media_catalog.models import Checksum, ObjectRef


def _digest(character: str) -> str:
    return "sha256:" + (character * 64)


class FakeTransport:
    def __init__(self) -> None:
        self.requests: list[dict[str, Any]] = []

    def perform_request(self, **kwargs: Any) -> dict[str, Any]:
        self.requests.append(kwargs)
        return {
            "timed_out": False,
            "hits": {"total": {"value": 0, "relation": "eq"}, "hits": []},
        }


class FakeOpenSearch:
    def __init__(self) -> None:
        self.transport = FakeTransport()


class StaticVerifier:
    def __init__(
        self,
        *,
        subject: str = "reviewer-1",
        scopes: frozenset[str] = frozenset({"governance.read"}),
    ) -> None:
        self.subject = subject
        self.scopes = scopes

    def verify(self, _token: str) -> Principal:
        return Principal(subject=self.subject, scopes=self.scopes)


class StaticReviewReader:
    def __init__(self, *, operator_subject: str = "operator-1") -> None:
        source_node = SourceNodeRef(
            namespace_id="tvmaze-show",
            source_id="1",
            referent_kind="SERIES",
        )
        self.conflict = build_identity_conflict(
            materialization_id=_digest("1"),
            source_node=source_node,
            candidate_entity_keys=(_digest("2"),),
            assertion_keys=(_digest("3"),),
            reason="MULTIPLE_EXACT_IDENTIFIER_CANDIDATES",
            observed_at="2026-09-19T00:00:00Z",
            policy_id="internal-key-continuity",
            policy_digest=_digest("4"),
        )
        pinned = PinnedSilverSnapshot(
            object=ObjectRef(
                uri="file:///tmp/silver.json",
                format="OBJECT_FORMAT_JSON",
                media_type=SILVER_SNAPSHOT_MEDIA_TYPE,
                checksum=Checksum(value="5" * 64),
                size_bytes=100,
            ),
            snapshot_set_id=_digest("6"),
        )
        operation = IdentityCurationOperation(
            action=IdentityCurationAction.ACCEPT,
            conflict_key=self.conflict.conflict_key,
            source_node=source_node,
            assertion_keys=self.conflict.assertion_keys,
            candidate_entity_key=_digest("2"),
        )
        self.manifest = build_identity_curation_manifest(
            pinned_silver_snapshot=pinned,
            operations=(operation,),
            operator_subject=operator_subject,
            reason="reviewed exact identifiers",
            operated_at="2026-09-20T00:00:00Z",
            config_digest=_digest("7"),
            image_digest=_digest("8"),
        )
        reference = ObjectRef(
            uri="file:///tmp/curation.json",
            format="OBJECT_FORMAT_JSON",
            media_type=IDENTITY_CURATION_MANIFEST_MEDIA_TYPE,
            checksum=Checksum(value="9" * 64),
            size_bytes=100,
        )
        manifest_ref = build_identity_curation_manifest_ref(
            self.manifest,
            reference,
        )
        self.status = IdentityCurationRequestStatus(
            request_id=self.manifest.manifest_id,
            status=IdentityCurationRequestState.APPLIED,
            manifest=manifest_ref,
            operator_subject=operator_subject,
            submitted_at="2026-09-20T00:00:00Z",
            run_id=_digest("a"),
            commit_key=_digest("b"),
        )
        self.item = IdentityConflictQueueItem(
            snapshot_set_id=pinned.snapshot_set_id,
            conflict=self.conflict,
        )

    def list_conflicts(self, **_: Any) -> IdentityConflictQueuePage:
        return IdentityConflictQueuePage(items=(self.item,))

    def get_request(self, **values: Any):
        if values["request_id"] == self.manifest.manifest_id:
            return self.status
        return None

    def get_manifest(self, **values: Any):
        if values["request_id"] == self.manifest.manifest_id:
            return self.manifest
        return None


def _settings() -> APISettings:
    return APISettings(
        opensearch_endpoint="https://search.example",
        cursor_secret="x" * 32,
        aws_region="us-east-1",
        environment="test",
        oidc_issuer="https://issuer.example",
        oidc_jwks_uri="https://issuer.example/jwks.json",
        oidc_audience="media-catalog-api",
    )


def test_review_read_endpoints_require_governance_scope() -> None:
    reader = StaticReviewReader()
    search = FakeOpenSearch()
    app = create_app(
        _settings(),
        client=search,
        verifier=StaticVerifier(),
        review_reader=reader,
    )
    headers = {"Authorization": "Bearer signed-token"}
    request_path = (
        f"/api/v2/research/identity-curation/requests/{reader.manifest.manifest_id}"
    )
    with TestClient(app) as client:
        unauthenticated = client.get("/api/v2/research/identity-conflicts")
        queue = client.get(
            "/api/v2/research/identity-conflicts",
            headers=headers,
        )
        status = client.get(request_path, headers=headers)
        manifest = client.get(f"{request_path}/manifest", headers=headers)
        write_attempt = client.post(request_path, headers=headers)

    assert unauthenticated.status_code == 401
    assert queue.status_code == 200
    assert queue.json()["items"][0]["conflict"]["conflictKey"] == (
        reader.conflict.conflict_key
    )
    assert status.status_code == 200
    assert status.json()["status"] == "APPLIED"
    assert manifest.status_code == 200
    assert manifest.json()["operatorSubject"] == "operator-1"
    assert write_attempt.status_code == 405
    assert search.transport.requests == []


def test_review_routes_fail_closed_for_missing_scope() -> None:
    reader = StaticReviewReader()
    app = create_app(
        _settings(),
        client=FakeOpenSearch(),
        verifier=StaticVerifier(scopes=frozenset({"profile"})),
        review_reader=reader,
    )
    with TestClient(app) as client:
        response = client.get(
            "/api/v2/research/identity-conflicts",
            headers={"Authorization": "Bearer signed-token"},
        )
    assert response.status_code == 403
    assert response.json()["code"] == "INSUFFICIENT_SCOPE"


@pytest.mark.parametrize("subject", ["reviewer-a", "reviewer-b"])
def test_any_scoped_principal_can_read_shared_review_queue(subject: str) -> None:
    reader = StaticReviewReader(operator_subject="original-operator")
    app = create_app(
        _settings(),
        client=FakeOpenSearch(),
        verifier=StaticVerifier(subject=subject),
        review_reader=reader,
    )
    with TestClient(app) as client:
        response = client.get(
            "/api/v2/research/identity-conflicts",
            headers={"Authorization": "Bearer signed-token"},
        )

    assert response.status_code == 200
    assert response.json()["items"][0]["conflict"]["conflictKey"] == (
        reader.conflict.conflict_key
    )
