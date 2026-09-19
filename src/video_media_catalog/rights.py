"""Machine-enforceable rights profiles for policy-isolated catalog data."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Self

from pydantic import Field, field_serializer, field_validator, model_validator

from video_media_catalog.v2_contracts import (
    V2ContractModel,
    digest_identity,
    parse_rfc3339,
    require_https_url,
    require_rfc3339,
    require_sha256,
    require_slug,
)


class PolicyZone(StrEnum):
    INTERNAL = "internal"
    OPEN_CC0 = "open_cc0"
    OPEN_ATTRIBUTED = "open_attributed"
    OPEN_SHAREALIKE = "open_sharealike"
    PUBLIC_REGISTRY = "public_registry"
    RESEARCH_PRIVATE = "research_private"
    FEDERATED_EPHEMERAL = "federated_ephemeral"
    COMMERCIAL = "commercial"
    QUARANTINE = "quarantine"


class UsageAction(StrEnum):
    STORE = "store"
    TRANSFORM = "transform"
    DISPLAY = "display"
    SEARCH = "search"
    EXPORT = "export"
    REDISTRIBUTE = "redistribute"
    DERIVE = "derive"
    EMBED = "embed"
    ML_TRAIN = "ml_train"
    ML_EVALUATE = "ml_evaluate"


def _sorted_unique(values: tuple[str, ...]) -> tuple[str, ...]:
    normalized = tuple(sorted({value.strip() for value in values if value.strip()}))
    if not normalized:
        raise ValueError("at least one value is required")
    return normalized


class RightsProfile(V2ContractModel):
    """One immutable policy snapshot attached to captured source data."""

    schema_version: str = "2.0"
    policy_id: str
    policy_version: str
    zone: PolicyZone
    license_id: str
    license_uri: str | None = None
    terms_url: str
    terms_digest: str | None = None
    permissions: tuple[UsageAction, ...]
    audiences: tuple[str, ...] = ("internal",)
    purposes: tuple[str, ...] = ("*",)
    territories: tuple[str, ...] = ("*",)
    attribution_text: str | None = None
    share_alike: bool = False
    max_cache_age_days: int | None = Field(default=None, gt=0)
    expires_at: str | None = None
    purge_on_termination: bool = False
    notes: str | None = Field(default=None, max_length=2000)

    @field_validator("policy_id")
    @classmethod
    def validate_policy_id(cls, value: str) -> str:
        return require_slug(value, label="policy_id")

    @field_validator("policy_version")
    @classmethod
    def validate_policy_version(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized or len(normalized) > 64:
            raise ValueError("policy_version must be a non-empty version")
        return normalized

    @field_validator("license_id")
    @classmethod
    def validate_license_id(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized or len(normalized) > 128:
            raise ValueError("license_id must be non-empty")
        return normalized

    @field_validator("license_uri", "terms_url")
    @classmethod
    def validate_urls(cls, value: str | None) -> str | None:
        return (
            None
            if value is None
            else require_https_url(value, label="rights policy URL")
        )

    @field_validator("terms_digest")
    @classmethod
    def validate_terms_digest(cls, value: str | None) -> str | None:
        return None if value is None else require_sha256(value, label="terms_digest")

    @field_validator("expires_at")
    @classmethod
    def validate_expires_at(cls, value: str | None) -> str | None:
        return None if value is None else require_rfc3339(value, label="expires_at")

    @field_validator("permissions")
    @classmethod
    def normalize_permissions(
        cls, value: tuple[UsageAction, ...]
    ) -> tuple[UsageAction, ...]:
        normalized = tuple(sorted(set(value), key=str))
        if not normalized:
            raise ValueError("at least one permission is required")
        return normalized

    @field_validator("audiences", "purposes", "territories")
    @classmethod
    def normalize_scopes(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _sorted_unique(value)

    @field_serializer("permissions", when_used="json")
    def serialize_permissions(self, value: tuple[UsageAction, ...]) -> list[str]:
        return [item.value for item in value]

    @model_validator(mode="after")
    def validate_policy_consistency(self) -> Self:
        if self.zone in {
            PolicyZone.OPEN_ATTRIBUTED,
            PolicyZone.OPEN_SHAREALIKE,
        } and not (self.attribution_text and self.attribution_text.strip()):
            raise ValueError("attributed policy zones require attribution_text")
        if self.zone == PolicyZone.OPEN_SHAREALIKE and not self.share_alike:
            raise ValueError("open_sharealike requires share_alike=true")
        if self.share_alike and self.zone != PolicyZone.OPEN_SHAREALIKE:
            raise ValueError("share_alike is only valid in open_sharealike")
        if (
            self.zone == PolicyZone.FEDERATED_EPHEMERAL
            and self.max_cache_age_days is None
        ):
            raise ValueError("federated_ephemeral requires max_cache_age_days")
        if self.zone == PolicyZone.OPEN_CC0 and self.purge_on_termination:
            raise ValueError("open_cc0 cannot require termination purge")
        return self

    @property
    def digest(self) -> str:
        return digest_identity(
            self.model_dump(mode="json", by_alias=True, exclude_none=True)
        )

    def allows(
        self,
        action: UsageAction,
        *,
        at: datetime | None = None,
        audience: str = "internal",
        purpose: str = "*",
        territory: str = "*",
    ) -> bool:
        current = (at or datetime.now(UTC)).astimezone(UTC)
        if self.expires_at is not None:
            expiry = datetime.fromisoformat(self.expires_at.replace("Z", "+00:00"))
            if current >= expiry:
                return False
        return (
            action in self.permissions
            and ("*" in self.audiences or audience in self.audiences)
            and ("*" in self.purposes or purpose in self.purposes)
            and ("*" in self.territories or territory in self.territories)
        )


class RightsEvaluation(V2ContractModel):
    allowed: bool
    requested_actions: tuple[UsageAction, ...]
    policy_ids: tuple[str, ...]
    denied_by: tuple[str, ...] = ()
    duties: tuple[str, ...] = ()
    earliest_expiry: str | None = None

    @field_serializer("requested_actions", when_used="json")
    def serialize_actions(self, value: tuple[UsageAction, ...]) -> list[str]:
        return [item.value for item in value]


def evaluate_rights(
    profiles: tuple[RightsProfile, ...],
    actions: tuple[UsageAction, ...],
    *,
    at: datetime | None = None,
    audience: str = "internal",
    purpose: str = "*",
    territory: str = "*",
) -> RightsEvaluation:
    """Evaluate the intersection of all contributing source policies."""

    if not profiles:
        raise ValueError("at least one rights profile is required")
    requested = tuple(sorted(set(actions), key=str))
    if not requested:
        raise ValueError("at least one requested action is required")
    current = (at or datetime.now(UTC)).astimezone(UTC)
    denied = sorted(
        {
            profile.policy_id
            for profile in profiles
            for action in requested
            if not profile.allows(
                action,
                at=current,
                audience=audience,
                purpose=purpose,
                territory=territory,
            )
        }
    )
    duties = sorted(
        {
            duty
            for profile in profiles
            for duty in (
                profile.attribution_text,
                "share-alike" if profile.share_alike else None,
            )
            if duty
        }
    )
    expiries = [
        profile.expires_at for profile in profiles if profile.expires_at is not None
    ]
    earliest_expiry = min(expiries, key=parse_rfc3339) if expiries else None
    return RightsEvaluation(
        allowed=not denied,
        requested_actions=requested,
        policy_ids=tuple(sorted({profile.policy_id for profile in profiles})),
        denied_by=tuple(denied),
        duties=tuple(duties),
        earliest_expiry=earliest_expiry,
    )
