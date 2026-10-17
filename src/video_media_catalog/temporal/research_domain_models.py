"""Temporal contracts for global-reference research-domain releases."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from video_media_catalog.temporal.golden_models import (
    GoldenBuildInput,
    GoldenBuildResult,
)
from video_media_catalog.temporal.models import (
    CaptureResult,
    ObjectRefPayload,
    PipelineEnv,
    SilverResult,
)
from video_media_catalog.temporal.names import GOLDEN_WORKFLOW_TASK_QUEUE

ResearchDomain = Literal[
    "SPORTS",
    "ANIMATION",
    "NEWS",
    "JOURNALISM",
    "LEARNING",
    "INSTRUCTION",
    "MUSIC",
]

_DOMAIN_SOURCES = {
    "SPORTS": frozenset(
        {
            "wikidata-json-dump",
            "openfootball-git",
            "statsbomb-open-data",
        }
    ),
    "ANIMATION": frozenset(
        {
            "wikidata-json-dump",
            "madb-public-data",
        }
    ),
    "NEWS": frozenset({"wikidata-json-dump"}),
    "JOURNALISM": frozenset({"wikidata-json-dump"}),
    "LEARNING": frozenset({"wikidata-json-dump"}),
    "INSTRUCTION": frozenset({"wikidata-json-dump"}),
    "MUSIC": frozenset(
        {
            "musicbrainz-core-dump",
            "wikidata-json-dump",
        }
    ),
}
_REFERENCE_ONLY_DOMAINS = frozenset(
    {
        "NEWS",
        "JOURNALISM",
        "LEARNING",
        "INSTRUCTION",
        "MUSIC",
    }
)


@dataclass(frozen=True)
class ResearchSourceProfile:
    """One immutable source capture consumed by a domain workflow."""

    profile_id: str
    domain: ResearchDomain
    source_product_id: str
    capture_manifest: ObjectRefPayload
    env: PipelineEnv
    watermark: str
    identity_spine: bool = False
    materialization: bool = False
    gold_contribution: bool | None = None
    maximum_attempts: int = 3
    initial_retry_seconds: int = 30
    maximum_retry_seconds: int = 600
    capture_timeout_minutes: int = 30
    silver_timeout_minutes: int = 720
    max_manifest_bytes: int = 4 * 1024 * 1024

    def __post_init__(self) -> None:
        contribution = (
            self.materialization
            if self.gold_contribution is None
            else self.gold_contribution
        )
        if not isinstance(contribution, bool):
            raise ValueError("research source gold_contribution must be a boolean")
        object.__setattr__(self, "gold_contribution", contribution)
        if self.source_product_id not in _DOMAIN_SOURCES[self.domain]:
            raise ValueError(
                f"{self.source_product_id} is not registered for {self.domain}"
            )
        if self.domain in _REFERENCE_ONLY_DOMAINS and (
            self.materialization
            or not self.identity_spine
            or not self.gold_contribution
        ):
            raise ValueError(
                f"{self.domain} profiles must enable identity and Gold contribution "
                "while remaining reference-only"
            )
        if not self.profile_id or len(self.profile_id) > 128:
            raise ValueError("research source profile_id must be non-empty and bounded")
        if not self.watermark or len(self.watermark) > 1024:
            raise ValueError("research source watermark must be non-empty and bounded")
        if not 1 <= self.maximum_attempts <= 5:
            raise ValueError("research source maximum_attempts must be between 1 and 5")
        if not 1 <= self.initial_retry_seconds <= self.maximum_retry_seconds:
            raise ValueError("research source retry intervals are invalid")
        if not 1 <= self.maximum_retry_seconds <= 3600:
            raise ValueError("research source maximum retry interval is too large")
        if not 1 <= self.capture_timeout_minutes <= 180:
            raise ValueError("research capture timeout is out of range")
        if not 1 <= self.silver_timeout_minutes <= 1440:
            raise ValueError("research Silver timeout is out of range")
        if not 1 <= self.max_manifest_bytes <= 16 * 1024 * 1024:
            raise ValueError("research capture manifest bound is out of range")


@dataclass(frozen=True)
class ResearchCaptureResult:
    profile_id: str
    domain: ResearchDomain
    source_product_id: str
    watermark: str
    acquired_at: str
    captures: tuple[CaptureResult, ...]


@dataclass(frozen=True)
class ResearchSourceResult:
    profile_id: str
    domain: ResearchDomain
    source_product_id: str
    watermark: str
    acquired_at: str
    capture_manifest: ObjectRefPayload
    captures: tuple[CaptureResult, ...]
    silvers: tuple[SilverResult, ...]


@dataclass(frozen=True)
class ResearchDomainInput:
    domain: ResearchDomain
    sources: tuple[ResearchSourceProfile, ...]
    golden_build: GoldenBuildInput
    golden_task_queue: str = GOLDEN_WORKFLOW_TASK_QUEUE
    next_source_index: int = 0
    completed_sources: tuple[ResearchSourceResult, ...] = ()
    failed_source_profile_ids: tuple[str, ...] = ()
    continue_as_new_after_sources: int = 2
    failure_budget: int = 1

    def __post_init__(self) -> None:
        if not self.sources:
            raise ValueError("research domain workflow requires at least one source")
        if (
            not self.golden_task_queue
            or len(self.golden_task_queue) > 128
            or any(
                character not in "abcdefghijklmnopqrstuvwxyz0123456789-._"
                for character in self.golden_task_queue
            )
        ):
            raise ValueError("research Golden task queue is invalid")
        if any(source.domain != self.domain for source in self.sources):
            raise ValueError("research domain workflow contains a cross-domain source")
        profile_ids = tuple(source.profile_id for source in self.sources)
        if len(profile_ids) != len(set(profile_ids)):
            raise ValueError("research domain source profile IDs must be unique")
        source_products = tuple(source.source_product_id for source in self.sources)
        if len(source_products) != len(set(source_products)):
            raise ValueError("research domain source products must be unique")
        if self.domain == "MUSIC" and source_products != (
            "musicbrainz-core-dump",
            "wikidata-json-dump",
        ):
            raise ValueError(
                "MUSIC workflow requires ordered MusicBrainz core and Wikidata sources"
            )
        if not 0 <= self.next_source_index <= len(self.sources):
            raise ValueError("research domain source cursor is out of range")
        if (
            len(self.completed_sources) + len(self.failed_source_profile_ids)
            != self.next_source_index
        ):
            raise ValueError("source outcomes must equal the source cursor")
        if len(self.failed_source_profile_ids) != len(
            set(self.failed_source_profile_ids)
        ):
            raise ValueError("failed source profile IDs must be unique")
        if not 1 <= self.continue_as_new_after_sources <= 32:
            raise ValueError("continue-as-new source bound is out of range")
        if not 1 <= self.failure_budget <= len(self.sources):
            raise ValueError("research domain failure budget is out of range")


@dataclass(frozen=True)
class ResearchDomainResult:
    domain: ResearchDomain
    sources: tuple[ResearchSourceResult, ...]
    golden: GoldenBuildResult
