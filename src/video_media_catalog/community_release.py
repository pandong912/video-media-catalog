"""Policy-specific Gold release contracts for the community catalog v2."""

from __future__ import annotations

import re
from enum import StrEnum
from typing import Any, Self
from urllib.parse import unquote, urlsplit

from pydantic import Field, ValidationInfo, field_validator, model_validator

from video_media_catalog.canonical import deterministic_key
from video_media_catalog.models import ObjectRef
from video_media_catalog.rights import PolicyZone, RightsTerminationFence
from video_media_catalog.v2_contracts import (
    V2ContractModel,
    require_oidc_subject,
    require_rfc3339,
    require_sha256,
    require_slug,
)

_TABLE_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)+$")
_ZERO_DIGEST = "sha256:" + ("0" * 64)


class ReleasePolicyContext(V2ContractModel):
    context_id: str
    audience: str
    purpose: str
    territories: tuple[str, ...] = ("*",)
    as_of: str
    allowed_zones: tuple[PolicyZone, ...]

    @field_validator("context_id")
    @classmethod
    def validate_context_id(cls, value: str) -> str:
        return require_slug(value, label="context_id")

    @field_validator("audience", "purpose")
    @classmethod
    def validate_text(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized or len(normalized) > 128:
            raise ValueError("release policy context fields must be non-empty")
        return normalized

    @field_validator("territories")
    @classmethod
    def normalize_territories(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(sorted({item.strip().upper() for item in value if item}))
        if not normalized:
            raise ValueError("release context requires territories")
        return normalized

    @field_validator("allowed_zones")
    @classmethod
    def normalize_zones(cls, value: tuple[PolicyZone, ...]) -> tuple[PolicyZone, ...]:
        normalized = tuple(sorted(set(value), key=str))
        if not normalized:
            raise ValueError("release context requires allowed_zones")
        if PolicyZone.QUARANTINE in normalized:
            raise ValueError("quarantine cannot be published")
        return normalized

    @field_validator("as_of")
    @classmethod
    def validate_as_of(cls, value: str) -> str:
        return require_rfc3339(value)


class ReleaseInput(V2ContractModel):
    source_product_id: str
    batch_id: str
    ingest_run_id: str
    silver_snapshot_id: int = Field(gt=0)
    watermark: str | None = None

    @field_validator("source_product_id")
    @classmethod
    def validate_product_id(cls, value: str) -> str:
        return require_slug(value, label="source_product_id")

    @field_validator("batch_id", "ingest_run_id")
    @classmethod
    def validate_digest(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("watermark")
    @classmethod
    def validate_watermark(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized or len(normalized) > 1024:
            raise ValueError("watermark must be non-empty and bounded")
        return normalized


class GoldTableSnapshot(V2ContractModel):
    table_name: str
    snapshot_id: int = Field(gt=0)
    parent_snapshot_id: int | None = Field(default=None, gt=0)
    committed_at: str
    operation: str
    affected_record_count: int = Field(ge=0)
    total_record_count: int = Field(ge=0)
    schema_id: int = Field(ge=0)

    @field_validator("table_name")
    @classmethod
    def validate_table_name(cls, value: str) -> str:
        if _TABLE_NAME.fullmatch(value) is None:
            raise ValueError("table_name must be a fully qualified safe identifier")
        return value

    @field_validator("committed_at")
    @classmethod
    def validate_committed_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @field_validator("operation")
    @classmethod
    def validate_operation(cls, value: str) -> str:
        normalized = value.strip().lower()
        if not normalized or len(normalized) > 64:
            raise ValueError("table operation must be non-empty")
        return normalized

    @model_validator(mode="after")
    def validate_counts(self) -> Self:
        if self.affected_record_count > self.total_record_count:
            raise ValueError("affected record count exceeds snapshot total")
        return self


class CatalogReleaseManifest(V2ContractModel):
    schema_version: str = "2.0"
    release_id: str
    previous_release_id: str | None = None
    contract_id: str
    contract_version: str
    contract_digest: str
    policy_context: ReleasePolicyContext
    inputs: tuple[ReleaseInput, ...]
    identity_policy_digest: str
    field_policy_digest: str
    rights_registry_digest: str
    tables: tuple[GoldTableSnapshot, ...]
    quality_reports: tuple[ObjectRef, ...]
    attribution_manifest: ObjectRef | None = None
    created_at: str
    metrics: dict[str, str] = Field(default_factory=dict)

    @field_validator(
        "release_id",
        "previous_release_id",
        "contract_digest",
        "identity_policy_digest",
        "field_policy_digest",
        "rights_registry_digest",
    )
    @classmethod
    def validate_digest(cls, value: str | None) -> str | None:
        return None if value is None else require_sha256(value)

    @field_validator("contract_id")
    @classmethod
    def validate_contract_id(cls, value: str) -> str:
        return require_slug(value, label="contract_id")

    @field_validator("contract_version")
    @classmethod
    def validate_contract_version(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized or len(normalized) > 64:
            raise ValueError("contract_version must be non-empty")
        return normalized

    @field_validator("created_at")
    @classmethod
    def validate_created_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @field_validator("inputs")
    @classmethod
    def sort_inputs(cls, value: tuple[ReleaseInput, ...]) -> tuple[ReleaseInput, ...]:
        return tuple(
            sorted(
                value,
                key=lambda item: (
                    item.source_product_id,
                    item.batch_id,
                    item.ingest_run_id,
                ),
            )
        )

    @field_validator("tables")
    @classmethod
    def sort_tables(
        cls, value: tuple[GoldTableSnapshot, ...]
    ) -> tuple[GoldTableSnapshot, ...]:
        return tuple(sorted(value, key=lambda item: item.table_name))

    @field_validator("quality_reports")
    @classmethod
    def sort_quality_reports(
        cls, value: tuple[ObjectRef, ...]
    ) -> tuple[ObjectRef, ...]:
        return tuple(sorted(value, key=lambda item: item.uri))

    @model_validator(mode="after")
    def validate_release(self, info: ValidationInfo) -> Self:
        if self.previous_release_id == self.release_id:
            raise ValueError("release cannot supersede itself")
        if not self.inputs:
            raise ValueError("catalog release requires inputs")
        if not self.tables:
            raise ValueError("catalog release requires Gold table snapshots")
        input_keys = [(item.source_product_id, item.batch_id) for item in self.inputs]
        if len(input_keys) != len(set(input_keys)):
            raise ValueError("catalog release contains duplicate inputs")
        names = [table.table_name for table in self.tables]
        if len(names) != len(set(names)):
            raise ValueError("catalog release contains duplicate table snapshots")
        if not self.quality_reports:
            raise ValueError("catalog release requires quality reports")
        if any(reference.size_bytes <= 0 for reference in self.quality_reports):
            raise ValueError("quality report objects must be non-empty")
        if not (info.context or {}).get("skip_identity"):
            expected = deterministic_key(
                "community-catalog-release-v2", _release_identity(self)
            )
            if self.release_id != expected:
                raise ValueError("release_id does not match release identity")
        return self


def _release_identity(release: CatalogReleaseManifest) -> dict[str, Any]:
    return {
        "schemaVersion": release.schema_version,
        "previousReleaseId": release.previous_release_id,
        "contractId": release.contract_id,
        "contractVersion": release.contract_version,
        "contractDigest": release.contract_digest,
        "policyContext": release.policy_context.model_dump(
            mode="json", by_alias=True, exclude_none=True
        ),
        "inputs": [
            item.model_dump(mode="json", by_alias=True, exclude_none=True)
            for item in release.inputs
        ],
        "identityPolicyDigest": release.identity_policy_digest,
        "fieldPolicyDigest": release.field_policy_digest,
        "rightsRegistryDigest": release.rights_registry_digest,
        "tables": [
            table.model_dump(mode="json", by_alias=True, exclude_none=True)
            for table in release.tables
        ],
        "qualityReports": [
            reference.model_dump(mode="json", by_alias=True, exclude_none=True)
            for reference in release.quality_reports
        ],
        "attributionManifest": (
            None
            if release.attribution_manifest is None
            else release.attribution_manifest.model_dump(
                mode="json", by_alias=True, exclude_none=True
            )
        ),
        "createdAt": release.created_at,
        "metrics": release.metrics,
    }


def build_catalog_release_manifest(**values: Any) -> CatalogReleaseManifest:
    values = dict(values)
    provisional = CatalogReleaseManifest.model_validate(
        {**values, "release_id": _ZERO_DIGEST},
        context={"skip_identity": True},
    )
    normalized = provisional.model_dump(mode="python")
    normalized["release_id"] = deterministic_key(
        "community-catalog-release-v2", _release_identity(provisional)
    )
    return CatalogReleaseManifest.model_validate(normalized)


class RemovalAction(StrEnum):
    RIGHTS_FENCE = "RIGHTS_FENCE"
    RE_GOLD = "RE_GOLD"
    RE_INDEX = "RE_INDEX"
    PURGE_RESTRICTED = "PURGE_RESTRICTED"


class PurgeTargetKind(StrEnum):
    RAW = "RAW"
    DERIVED = "DERIVED"


def _uri_within_prefix(uri: str, prefix: str) -> bool:
    target = urlsplit(uri)
    allowed = urlsplit(prefix)
    if (
        target.scheme != allowed.scheme
        or target.netloc != allowed.netloc
        or target.query
        or target.fragment
        or allowed.query
        or allowed.fragment
    ):
        return False
    allowed_path = allowed.path.rstrip("/") or "/"
    return target.path == allowed_path or target.path.startswith(
        allowed_path.rstrip("/") + "/"
    )


def _validate_removal_uri(value: str, *, label: str) -> str:
    normalized = value.strip()
    parsed = urlsplit(normalized)
    decoded_path = unquote(parsed.path)
    if (
        parsed.scheme not in {"file", "s3"}
        or not parsed.path.startswith("/")
        or parsed.query
        or parsed.fragment
        or (parsed.scheme == "s3" and not parsed.netloc)
        or any(part in {".", ".."} for part in decoded_path.split("/"))
    ):
        raise ValueError(f"{label} must be a canonical file:// or s3:// URI")
    return normalized


class SourceRemovalImpact(V2ContractModel):
    assertion_counts: dict[str, int]
    affected_entity_count: int = Field(ge=0)
    affected_release_plan_ids: tuple[str, ...] = ()
    affected_indexes: tuple[str, ...] = ()

    @field_validator("assertion_counts")
    @classmethod
    def validate_assertion_counts(cls, value: dict[str, int]) -> dict[str, int]:
        normalized: dict[str, int] = {}
        for key, count in value.items():
            name = key.strip()
            if (
                not name
                or len(name) > 128
                or isinstance(count, bool)
                or count < 0
                or name in normalized
            ):
                raise ValueError(
                    "removal assertion counts must be unique, named, and non-negative"
                )
            normalized[name] = count
        return dict(sorted(normalized.items()))

    @field_validator("affected_release_plan_ids")
    @classmethod
    def normalize_release_ids(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(
            sorted(
                {require_sha256(item, label="affected release plan") for item in value}
            )
        )

    @field_validator("affected_indexes")
    @classmethod
    def normalize_indexes(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(sorted({item.strip() for item in value if item.strip()}))
        if any(len(item) > 255 for item in normalized):
            raise ValueError("affected index name is too long")
        return normalized

    @property
    def affected_assertion_count(self) -> int:
        return sum(self.assertion_counts.values())


class RestrictedPurgeTarget(V2ContractModel):
    target_id: str
    source_product_id: str
    kind: PurgeTargetKind
    uri: str
    restricted: bool = True

    @field_validator("target_id")
    @classmethod
    def validate_target_id(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("source_product_id")
    @classmethod
    def validate_source_product_id(cls, value: str) -> str:
        return require_slug(value, label="purge source_product_id")

    @field_validator("uri")
    @classmethod
    def validate_uri(cls, value: str) -> str:
        return _validate_removal_uri(value, label="purge target URI")

    @model_validator(mode="after")
    def validate_target(self) -> Self:
        if not self.restricted:
            raise ValueError("removal planner accepts only restricted purge targets")
        expected = deterministic_key(
            "source-removal-purge-target-v2",
            {
                "sourceProductId": self.source_product_id,
                "kind": self.kind.value,
                "uri": self.uri,
                "restricted": self.restricted,
            },
        )
        if self.target_id != expected:
            raise ValueError("target_id does not match purge target")
        return self


def build_restricted_purge_target(
    *,
    source_product_id: str,
    kind: PurgeTargetKind,
    uri: str,
) -> RestrictedPurgeTarget:
    identity = {
        "sourceProductId": require_slug(
            source_product_id,
            label="purge source_product_id",
        ),
        "kind": kind.value,
        "uri": uri.strip(),
        "restricted": True,
    }
    return RestrictedPurgeTarget(
        target_id=deterministic_key(
            "source-removal-purge-target-v2",
            identity,
        ),
        source_product_id=source_product_id,
        kind=kind,
        uri=uri,
        restricted=True,
    )


class SourceRemovalPlan(V2ContractModel):
    schema_version: str = "2.0"
    plan_id: str
    owner_subject: str
    rights_fence: RightsTerminationFence
    impact: SourceRemovalImpact
    actions: tuple[RemovalAction, ...]
    purge_targets: tuple[RestrictedPurgeTarget, ...] = ()
    dry_run: bool = True
    allowlist_prefixes: tuple[str, ...] = ()
    planned_at: str

    @field_validator("plan_id")
    @classmethod
    def validate_plan_id(cls, value: str) -> str:
        return require_sha256(value)

    @field_validator("owner_subject")
    @classmethod
    def validate_owner_subject(cls, value: str) -> str:
        return require_oidc_subject(value)

    @field_validator("actions")
    @classmethod
    def normalize_actions(
        cls,
        value: tuple[RemovalAction, ...],
    ) -> tuple[RemovalAction, ...]:
        normalized = tuple(sorted(set(value), key=str))
        if not normalized or RemovalAction.RIGHTS_FENCE not in normalized:
            raise ValueError("removal plan must install a rights fence")
        return normalized

    @field_validator("purge_targets")
    @classmethod
    def normalize_targets(
        cls,
        value: tuple[RestrictedPurgeTarget, ...],
    ) -> tuple[RestrictedPurgeTarget, ...]:
        normalized = tuple(sorted(value, key=lambda item: item.target_id))
        target_ids = [item.target_id for item in normalized]
        if len(target_ids) != len(set(target_ids)):
            raise ValueError("removal plan contains duplicate purge targets")
        return normalized

    @field_validator("allowlist_prefixes")
    @classmethod
    def normalize_allowlist(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(sorted({item.strip() for item in value if item.strip()}))
        for prefix in normalized:
            _validate_removal_uri(prefix, label="removal allowlist prefix")
        return normalized

    @field_validator("planned_at")
    @classmethod
    def validate_planned_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @model_validator(mode="after")
    def validate_plan(self, info: ValidationInfo) -> Self:
        expected_actions = {RemovalAction.RIGHTS_FENCE}
        if (
            self.impact.affected_assertion_count
            or self.impact.affected_entity_count
            or self.impact.affected_release_plan_ids
        ):
            expected_actions.add(RemovalAction.RE_GOLD)
            expected_actions.add(RemovalAction.RE_INDEX)
        elif self.impact.affected_indexes:
            expected_actions.add(RemovalAction.RE_INDEX)
        if self.rights_fence.purge_required and self.purge_targets:
            expected_actions.add(RemovalAction.PURGE_RESTRICTED)
        if set(self.actions) != expected_actions:
            raise ValueError("removal actions do not match the impact and rights fence")
        if any(
            target.source_product_id != self.rights_fence.source_product_id
            for target in self.purge_targets
        ):
            raise ValueError("purge target belongs to another source product")
        if self.purge_targets and not self.rights_fence.purge_required:
            raise ValueError("rights fence does not authorize termination purge")
        if not self.dry_run:
            if not self.allowlist_prefixes:
                raise ValueError("executable removal plan requires an allowlist")
            if any(
                not any(
                    _uri_within_prefix(target.uri, prefix)
                    for prefix in self.allowlist_prefixes
                )
                for target in self.purge_targets
            ):
                raise ValueError("purge target is outside the execution allowlist")
        if not (info.context or {}).get("skip_identity"):
            expected = deterministic_key(
                "source-removal-plan-v2",
                _source_removal_plan_identity(self),
            )
            if self.plan_id != expected:
                raise ValueError("plan_id does not match source removal plan")
        return self


def _source_removal_plan_identity(plan: SourceRemovalPlan) -> dict[str, Any]:
    return {
        "schemaVersion": plan.schema_version,
        "ownerSubject": plan.owner_subject,
        "rightsFence": plan.rights_fence.model_dump(
            mode="json", by_alias=True, exclude_none=True
        ),
        "impact": plan.impact.model_dump(mode="json", by_alias=True, exclude_none=True),
        "actions": [item.value for item in plan.actions],
        "purgeTargets": [
            item.model_dump(mode="json", by_alias=True, exclude_none=True)
            for item in plan.purge_targets
        ],
        "dryRun": plan.dry_run,
        "allowlistPrefixes": plan.allowlist_prefixes,
        "plannedAt": plan.planned_at,
    }


def build_source_removal_plan(
    *,
    owner_subject: str,
    rights_fence: RightsTerminationFence,
    impact: SourceRemovalImpact,
    purge_targets: tuple[RestrictedPurgeTarget, ...] = (),
    dry_run: bool = True,
    confirm_source_product_id: str | None = None,
    allowlist_prefixes: tuple[str, ...] = (),
    planned_at: str,
) -> SourceRemovalPlan:
    if not dry_run and confirm_source_product_id != rights_fence.source_product_id:
        raise ValueError(
            "executable removal requires exact source product confirmation"
        )
    actions = {RemovalAction.RIGHTS_FENCE}
    if (
        impact.affected_assertion_count
        or impact.affected_entity_count
        or impact.affected_release_plan_ids
    ):
        actions.update((RemovalAction.RE_GOLD, RemovalAction.RE_INDEX))
    elif impact.affected_indexes:
        actions.add(RemovalAction.RE_INDEX)
    if rights_fence.purge_required and purge_targets:
        actions.add(RemovalAction.PURGE_RESTRICTED)
    values = {
        "plan_id": _ZERO_DIGEST,
        "owner_subject": owner_subject,
        "rights_fence": rights_fence,
        "impact": impact,
        "actions": tuple(actions),
        "purge_targets": purge_targets,
        "dry_run": dry_run,
        "allowlist_prefixes": allowlist_prefixes,
        "planned_at": planned_at,
    }
    provisional = SourceRemovalPlan.model_validate(
        values,
        context={"skip_identity": True},
    )
    values["plan_id"] = deterministic_key(
        "source-removal-plan-v2",
        _source_removal_plan_identity(provisional),
    )
    return SourceRemovalPlan.model_validate(values)


class RemovalReceiptStatus(StrEnum):
    COMPLETED = "COMPLETED"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"


class SourceRemovalReceipt(V2ContractModel):
    schema_version: str = "2.0"
    receipt_id: str
    plan: SourceRemovalPlan
    status: RemovalReceiptStatus
    rights_fence_installed: bool
    purged_target_ids: tuple[str, ...]
    re_gold_release_plan_id: str | None = None
    re_index_build_id: str | None = None
    errors: tuple[str, ...] = ()
    executed_at: str

    @field_validator(
        "receipt_id",
        "re_gold_release_plan_id",
    )
    @classmethod
    def validate_digest(cls, value: str | None) -> str | None:
        return None if value is None else require_sha256(value)

    @field_validator("re_index_build_id")
    @classmethod
    def validate_index_build_id(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip().lower()
        if re.fullmatch(r"[0-9a-f]{64}", normalized) is None:
            raise ValueError("re_index_build_id must contain 64 lowercase hex digits")
        return normalized

    @field_validator("purged_target_ids")
    @classmethod
    def normalize_purged_target_ids(
        cls,
        value: tuple[str, ...],
    ) -> tuple[str, ...]:
        return tuple(
            sorted({require_sha256(item, label="purged target") for item in value})
        )

    @field_validator("errors")
    @classmethod
    def normalize_errors(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(sorted({item.strip() for item in value if item.strip()}))

    @field_validator("executed_at")
    @classmethod
    def validate_executed_at(cls, value: str) -> str:
        return require_rfc3339(value)

    @model_validator(mode="after")
    def validate_receipt(self, info: ValidationInfo) -> Self:
        if self.plan.dry_run:
            raise ValueError("dry-run removal plan cannot produce a receipt")
        planned_target_ids = {target.target_id for target in self.plan.purge_targets}
        if not set(self.purged_target_ids).issubset(planned_target_ids):
            raise ValueError("receipt contains an unplanned purge target")
        if self.status == RemovalReceiptStatus.COMPLETED:
            if self.errors:
                raise ValueError("completed removal receipt cannot contain errors")
            if not self.rights_fence_installed:
                raise ValueError("completed receipt requires an installed rights fence")
            if (
                RemovalAction.PURGE_RESTRICTED in self.plan.actions
                and set(self.purged_target_ids) != planned_target_ids
            ):
                raise ValueError("completed receipt must cover every purge target")
            if (
                RemovalAction.RE_GOLD in self.plan.actions
                and self.re_gold_release_plan_id is None
            ):
                raise ValueError("completed receipt requires the replacement Gold ID")
            if (
                RemovalAction.RE_INDEX in self.plan.actions
                and self.re_index_build_id is None
            ):
                raise ValueError("completed receipt requires the replacement index ID")
        elif not self.errors:
            raise ValueError("non-completed removal receipt requires errors")
        if not (info.context or {}).get("skip_identity"):
            expected = deterministic_key(
                "source-removal-receipt-v2",
                _source_removal_receipt_identity(self),
            )
            if self.receipt_id != expected:
                raise ValueError("receipt_id does not match source removal receipt")
        return self


def _source_removal_receipt_identity(
    receipt: SourceRemovalReceipt,
) -> dict[str, Any]:
    return {
        "schemaVersion": receipt.schema_version,
        "plan": receipt.plan.model_dump(mode="json", by_alias=True, exclude_none=True),
        "status": receipt.status.value,
        "rightsFenceInstalled": receipt.rights_fence_installed,
        "purgedTargetIds": receipt.purged_target_ids,
        "reGoldReleasePlanId": receipt.re_gold_release_plan_id,
        "reIndexBuildId": receipt.re_index_build_id,
        "errors": receipt.errors,
        "executedAt": receipt.executed_at,
    }


def build_source_removal_receipt(**values: Any) -> SourceRemovalReceipt:
    values = dict(values)
    provisional = SourceRemovalReceipt.model_validate(
        {**values, "receipt_id": _ZERO_DIGEST},
        context={"skip_identity": True},
    )
    normalized = provisional.model_dump(mode="python")
    normalized["receipt_id"] = deterministic_key(
        "source-removal-receipt-v2",
        _source_removal_receipt_identity(provisional),
    )
    return SourceRemovalReceipt.model_validate(normalized)
