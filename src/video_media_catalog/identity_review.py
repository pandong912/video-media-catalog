"""Read-only projection contracts for identity review APIs."""

from __future__ import annotations

from enum import StrEnum
from typing import Protocol, Self

from pydantic import field_validator, model_validator

from video_media_catalog.identity_curation import (
    IdentityCurationManifest,
    IdentityCurationManifestRef,
)
from video_media_catalog.identity_v2 import IdentityConflict
from video_media_catalog.v2_contracts import (
    V2ContractModel,
    require_oidc_subject,
    require_rfc3339,
    require_sha256,
)


class IdentityCurationRequestState(StrEnum):
    SUBMITTED = "SUBMITTED"
    APPLIED = "APPLIED"
    FAILED = "FAILED"


class IdentityConflictQueueItem(V2ContractModel):
    snapshot_set_id: str
    conflict: IdentityConflict

    @field_validator("snapshot_set_id")
    @classmethod
    def validate_snapshot_set_id(cls, value: str) -> str:
        return require_sha256(value, label="snapshot_set_id")


class IdentityConflictQueuePage(V2ContractModel):
    items: tuple[IdentityConflictQueueItem, ...]
    next_cursor: str | None = None

    @field_validator("next_cursor")
    @classmethod
    def validate_cursor(cls, value: str | None) -> str | None:
        if value is not None and (not value or len(value) > 4_096):
            raise ValueError("review cursor is invalid")
        return value


class IdentityCurationRequestStatus(V2ContractModel):
    request_id: str
    status: IdentityCurationRequestState
    manifest: IdentityCurationManifestRef
    operator_subject: str
    submitted_at: str
    run_id: str | None = None
    commit_key: str | None = None
    failure_code: str | None = None

    @field_validator("request_id", "run_id", "commit_key")
    @classmethod
    def validate_digest(cls, value: str | None) -> str | None:
        return None if value is None else require_sha256(value)

    @field_validator("operator_subject")
    @classmethod
    def validate_operator_subject(cls, value: str) -> str:
        return require_oidc_subject(value, label="operator_subject")

    @field_validator("submitted_at")
    @classmethod
    def validate_submitted_at(cls, value: str) -> str:
        return require_rfc3339(value, label="submitted_at")

    @field_validator("failure_code")
    @classmethod
    def validate_failure_code(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip().upper()
        if (
            not normalized
            or len(normalized) > 128
            or any(not (item.isalnum() or item == "_") for item in normalized)
        ):
            raise ValueError("failure_code is invalid")
        return normalized

    @model_validator(mode="after")
    def validate_status(self) -> Self:
        if self.request_id != self.manifest.manifest_id:
            raise ValueError("curation request status is inconsistent")
        if self.status == IdentityCurationRequestState.APPLIED:
            if self.run_id is None or self.commit_key is None:
                raise ValueError("applied curation status requires run and commit")
        elif self.commit_key is not None:
            raise ValueError("only applied curation status may contain a commit")
        if self.status == IdentityCurationRequestState.FAILED:
            if self.failure_code is None:
                raise ValueError("failed curation status requires a failure code")
        elif self.failure_code is not None:
            raise ValueError("only failed curation status may contain a failure code")
        return self


class IdentityReviewReader(Protocol):
    """Read projection only; implementations must not accept API writes."""

    def list_conflicts(
        self,
        *,
        limit: int,
        cursor: str | None,
    ) -> IdentityConflictQueuePage: ...

    def get_request(
        self,
        *,
        request_id: str,
    ) -> IdentityCurationRequestStatus | None: ...

    def get_manifest(
        self,
        *,
        request_id: str,
    ) -> IdentityCurationManifest | None: ...


class EmptyIdentityReviewReader:
    """Safe default when no read projection has been configured."""

    def list_conflicts(
        self,
        *,
        limit: int,
        cursor: str | None,
    ) -> IdentityConflictQueuePage:
        if not 1 <= limit <= 100 or (cursor is not None and len(cursor) > 4_096):
            raise ValueError("empty review reader received invalid pagination")
        return IdentityConflictQueuePage(items=())

    def get_request(
        self,
        *,
        request_id: str,
    ) -> IdentityCurationRequestStatus | None:
        require_sha256(request_id, label="request_id")
        return None

    def get_manifest(
        self,
        *,
        request_id: str,
    ) -> IdentityCurationManifest | None:
        require_sha256(request_id, label="request_id")
        return None
