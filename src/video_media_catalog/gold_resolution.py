"""Rights-first deterministic resolution from Silver assertions to Gold drafts."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from video_media_catalog.assertions import (
    AssertionStatus,
    FieldAssertion,
    IdentifierAssertion,
    RelationshipAssertion,
)
from video_media_catalog.canonical import canonical_json
from video_media_catalog.gold import (
    GoldReleasePlan,
    GoldResolutionPolicy,
    GoldResolutionStatus,
    ResolutionOperator,
    build_gold_conflict,
    build_gold_entity,
    build_gold_field,
    build_gold_identifier,
    build_gold_relation,
)
from video_media_catalog.identity_resolution import IdentityIndex
from video_media_catalog.rights import RightsProfile
from video_media_catalog.v2_contracts import parse_rfc3339


@dataclass(frozen=True)
class FieldDraft:
    entity_key: str
    predicate: str
    value_type: str
    value: Any | None
    qualifiers: dict[str, Any]
    status: GoldResolutionStatus
    assertion_ids: tuple[str, ...]
    selected_assertion_id: str | None
    trace: dict[str, Any]


@dataclass(frozen=True)
class IdentifierDraft:
    entity_key: str
    namespace_id: str
    value: str
    issuer: str
    referent_kind: str
    assertion_ids: tuple[str, ...]
    trace: dict[str, Any]


@dataclass(frozen=True)
class RelationDraft:
    subject_entity_key: str
    predicate: str
    object_entity_key: str
    qualifiers: dict[str, Any]
    assertion_ids: tuple[str, ...]
    trace: dict[str, Any]


@dataclass(frozen=True)
class ConflictDraft:
    entity_key: str
    predicate: str
    qualifiers: dict[str, Any]
    reason: str
    assertion_ids: tuple[str, ...]
    candidate_values: list[Any]
    trace: dict[str, Any]


@dataclass(frozen=True)
class GoldResolutionDraft:
    entity_keys: tuple[str, ...]
    source_node_counts: dict[str, int]
    fields: tuple[FieldDraft, ...]
    identifiers: tuple[IdentifierDraft, ...]
    relations: tuple[RelationDraft, ...]
    conflicts: tuple[ConflictDraft, ...]
    eligible_policy_counts: dict[str, int]
    withheld_assertion_count: int
    unresolved_identity_count: int

    @property
    def expected_counts(self) -> dict[str, int]:
        return {
            "community_gold_entity": len(self.entity_keys),
            "community_gold_field": len(self.fields),
            "community_gold_identifier": len(self.identifiers),
            "community_gold_relation": len(self.relations),
            "community_gold_conflict": len(self.conflicts),
        }

    def validate_quality(self, policy: GoldResolutionPolicy) -> None:
        field_total = len(self.fields)
        conflict_ratio = len(self.conflicts) / field_total if field_total else 0.0
        identity_denominator = len(self.entity_keys) + self.unresolved_identity_count
        unresolved_ratio = (
            self.unresolved_identity_count / identity_denominator
            if identity_denominator
            else 0.0
        )
        if conflict_ratio > policy.max_conflict_ratio:
            raise ValueError("Gold field conflict ratio exceeds policy budget")
        if unresolved_ratio > policy.max_unresolved_identity_ratio:
            raise ValueError("Gold unresolved identity ratio exceeds policy budget")

    def materialize(
        self,
        plan: GoldReleasePlan,
        identity_index: IdentityIndex,
    ) -> dict[str, tuple[Any, ...]]:
        if plan.expected_counts != self.expected_counts:
            raise ValueError("Gold plan counts do not match resolution draft")
        entities = []
        for entity_key in self.entity_keys:
            entity = identity_index.entities[entity_key]
            entities.append(
                build_gold_entity(
                    release_plan_id=plan.release_plan_id,
                    entity_key=entity_key,
                    entity_level=entity.entity_level.value,
                    entity_kind=entity.entity_kind,
                    status=entity.status.value,
                    source_node_count=self.source_node_counts.get(entity_key, 0),
                    trace={
                        "sourceNodeCount": self.source_node_counts.get(entity_key, 0)
                    },
                )
            )
        fields = tuple(
            build_gold_field(
                release_plan_id=plan.release_plan_id,
                entity_key=item.entity_key,
                predicate=item.predicate,
                value_type=item.value_type,
                value=item.value,
                qualifiers=item.qualifiers,
                resolution_status=item.status,
                assertion_ids=item.assertion_ids,
                selected_assertion_id=item.selected_assertion_id,
                trace=item.trace,
            )
            for item in self.fields
        )
        identifiers = tuple(
            build_gold_identifier(
                release_plan_id=plan.release_plan_id,
                entity_key=item.entity_key,
                namespace_id=item.namespace_id,
                value=item.value,
                issuer=item.issuer,
                referent_kind=item.referent_kind,
                assertion_ids=item.assertion_ids,
                trace=item.trace,
            )
            for item in self.identifiers
        )
        relations = tuple(
            build_gold_relation(
                release_plan_id=plan.release_plan_id,
                subject_entity_key=item.subject_entity_key,
                predicate=item.predicate,
                object_entity_key=item.object_entity_key,
                qualifiers=item.qualifiers,
                assertion_ids=item.assertion_ids,
                trace=item.trace,
            )
            for item in self.relations
        )
        conflicts = tuple(
            build_gold_conflict(
                release_plan_id=plan.release_plan_id,
                entity_key=item.entity_key,
                predicate=item.predicate,
                qualifiers=item.qualifiers,
                reason=item.reason,
                assertion_ids=item.assertion_ids,
                candidate_values=item.candidate_values,
                trace=item.trace,
            )
            for item in self.conflicts
        )
        return {
            "community_gold_entity": tuple(entities),
            "community_gold_field": fields,
            "community_gold_identifier": identifiers,
            "community_gold_relation": relations,
            "community_gold_conflict": conflicts,
        }


class _Eligibility:
    def __init__(
        self,
        *,
        profiles: dict[str, RightsProfile],
        plan_context,
        policy: GoldResolutionPolicy,
    ) -> None:
        self.profiles = profiles
        self.context = plan_context
        self.policy = policy
        self.as_of = parse_rfc3339(plan_context.as_of)
        self.policy_counts: Counter[str] = Counter()
        self.withheld = 0

    def permits(self, assertion) -> bool:
        provenance = assertion.provenance
        profile = self.profiles.get(provenance.policy_id)
        if profile is None:
            raise ValueError(f"unknown rights policy: {provenance.policy_id}")
        if profile.digest != provenance.policy_digest:
            raise ValueError("assertion policy digest differs from registry")
        if profile.zone not in self.context.allowed_zones:
            self.withheld += 1
            return False
        if (
            provenance.valid_from is not None
            and parse_rfc3339(provenance.valid_from) > self.as_of
        ):
            self.withheld += 1
            return False
        if (
            provenance.valid_to is not None
            and parse_rfc3339(provenance.valid_to) <= self.as_of
        ):
            self.withheld += 1
            return False
        if profile.max_cache_age_days is not None:
            cache_deadline = parse_rfc3339(provenance.observed_at) + timedelta(
                days=profile.max_cache_age_days
            )
            if self.as_of >= cache_deadline:
                self.withheld += 1
                return False
        territories = self.context.territories
        permitted = all(
            profile.allows(
                action,
                at=self.as_of,
                audience=self.context.audience,
                purpose=self.context.purpose,
                territory=territory,
            )
            for action in self.policy.requested_actions
            for territory in territories
        )
        if not permitted:
            self.withheld += 1
            return False
        self.policy_counts[profile.policy_id] += 1
        return True


def _active_and_eligible(assertion, eligibility: _Eligibility) -> bool:
    return assertion.status == AssertionStatus.ACTIVE and eligibility.permits(assertion)


def resolve_gold_draft(
    *,
    identity_index: IdentityIndex,
    field_assertions: tuple[FieldAssertion, ...],
    identifier_assertions: tuple[IdentifierAssertion, ...],
    relationship_assertions: tuple[RelationshipAssertion, ...],
    rights_profiles: tuple[RightsProfile, ...],
    policy_context,
    field_policy: GoldResolutionPolicy,
) -> GoldResolutionDraft:
    profiles = {profile.policy_id: profile for profile in rights_profiles}
    if len(profiles) != len(rights_profiles):
        raise ValueError("rights registry contains duplicate policy IDs")
    eligibility = _Eligibility(
        profiles=profiles,
        plan_context=policy_context,
        policy=field_policy,
    )
    unresolved = 0
    field_groups: dict[tuple[str, str, str], list[FieldAssertion]] = defaultdict(list)
    used_entity_keys: set[str] = set()

    for assertion in field_assertions:
        if not _active_and_eligible(assertion, eligibility):
            continue
        entity_key = identity_index.resolve_source_node(assertion.subject)
        if entity_key is None:
            unresolved += 1
            continue
        rule = field_policy.rule_for(assertion.predicate)
        if rule.operator == ResolutionOperator.NEVER_RESOLVE:
            eligibility.withheld += 1
            continue
        scope = {key: assertion.qualifiers.get(key) for key in rule.scope_qualifiers}
        scope_json = canonical_json(scope)
        field_groups[(entity_key, assertion.predicate, scope_json)].append(assertion)
        used_entity_keys.add(entity_key)

    fields: list[FieldDraft] = []
    conflicts: list[ConflictDraft] = []
    for (entity_key, predicate, scope_json), assertions in sorted(field_groups.items()):
        rule = field_policy.rule_for(predicate)
        scope = json.loads(scope_json)
        by_value: dict[tuple[str, str], list[FieldAssertion]] = defaultdict(list)
        for assertion in assertions:
            by_value[(assertion.value_type.value, assertion.value_json)].append(
                assertion
            )
        ordered_values = sorted(by_value)
        all_ids = tuple(sorted(assertion.assertion_id for assertion in assertions))
        trace = {
            "operator": rule.operator.value,
            "policyId": field_policy.policy_id,
            "policyVersion": field_policy.policy_version,
        }
        if rule.operator == ResolutionOperator.SINGLE:
            if len(ordered_values) == 1:
                value_type, value_json = ordered_values[0]
                winning = tuple(
                    sorted(
                        assertion.assertion_id
                        for assertion in by_value[ordered_values[0]]
                    )
                )
                fields.append(
                    FieldDraft(
                        entity_key=entity_key,
                        predicate=predicate,
                        value_type=value_type,
                        value=json.loads(value_json),
                        qualifiers=scope,
                        status=GoldResolutionStatus.SELECTED,
                        assertion_ids=all_ids,
                        selected_assertion_id=winning[0],
                        trace=trace,
                    )
                )
            else:
                candidates = [
                    json.loads(value_json) for _, value_json in ordered_values
                ]
                fields.append(
                    FieldDraft(
                        entity_key=entity_key,
                        predicate=predicate,
                        value_type="CONFLICT",
                        value=None,
                        qualifiers=scope,
                        status=GoldResolutionStatus.CONFLICTED,
                        assertion_ids=all_ids,
                        selected_assertion_id=None,
                        trace=trace,
                    )
                )
                conflicts.append(
                    ConflictDraft(
                        entity_key=entity_key,
                        predicate=predicate,
                        qualifiers=scope,
                        reason="MULTIPLE_ELIGIBLE_VALUES",
                        assertion_ids=all_ids,
                        candidate_values=candidates,
                        trace=trace,
                    )
                )
        else:
            for value_type, value_json in ordered_values:
                value_assertions = tuple(
                    sorted(
                        assertion.assertion_id
                        for assertion in by_value[(value_type, value_json)]
                    )
                )
                fields.append(
                    FieldDraft(
                        entity_key=entity_key,
                        predicate=predicate,
                        value_type=value_type,
                        value=json.loads(value_json),
                        qualifiers=scope,
                        status=GoldResolutionStatus.SET,
                        assertion_ids=value_assertions,
                        selected_assertion_id=None,
                        trace=trace,
                    )
                )

    identifier_groups: dict[
        tuple[str, str, str, str, str], list[IdentifierAssertion]
    ] = defaultdict(list)
    assignment_entities: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    for assertion in identifier_assertions:
        if not _active_and_eligible(assertion, eligibility):
            continue
        entity_key = identity_index.resolve_source_node(assertion.subject)
        if entity_key is None:
            unresolved += 1
            continue
        identifier_groups[
            (
                entity_key,
                assertion.namespace_id,
                assertion.value,
                assertion.issuer,
                assertion.referent_kind,
            )
        ].append(assertion)
        assignment_entities[
            (
                assertion.namespace_id,
                assertion.value,
                assertion.referent_kind,
            )
        ].add(entity_key)
        used_entity_keys.add(entity_key)
    collisions = {
        key: entities
        for key, entities in assignment_entities.items()
        if len(entities) > 1
    }
    if collisions:
        raise ValueError("eligible identifier resolves to multiple Gold entities")
    identifiers = tuple(
        IdentifierDraft(
            entity_key=entity_key,
            namespace_id=namespace_id,
            value=value,
            issuer=issuer,
            referent_kind=referent_kind,
            assertion_ids=tuple(
                sorted(assertion.assertion_id for assertion in assertions)
            ),
            trace={"policyId": field_policy.policy_id},
        )
        for (
            entity_key,
            namespace_id,
            value,
            issuer,
            referent_kind,
        ), assertions in sorted(identifier_groups.items())
    )

    relation_groups: dict[tuple[str, str, str, str], list[RelationshipAssertion]] = (
        defaultdict(list)
    )
    for assertion in relationship_assertions:
        if not _active_and_eligible(assertion, eligibility):
            continue
        subject = identity_index.resolve_source_node(assertion.subject)
        object_key = identity_index.resolve_source_node(assertion.object)
        if subject is None or object_key is None:
            unresolved += 1
            continue
        qualifiers_json = canonical_json(assertion.qualifiers)
        relation_groups[
            (subject, assertion.predicate, object_key, qualifiers_json)
        ].append(assertion)
        used_entity_keys.update((subject, object_key))
    relations = tuple(
        RelationDraft(
            subject_entity_key=subject,
            predicate=predicate,
            object_entity_key=object_key,
            qualifiers=json.loads(qualifiers_json),
            assertion_ids=tuple(
                sorted(assertion.assertion_id for assertion in assertions)
            ),
            trace={"policyId": field_policy.policy_id},
        )
        for (
            subject,
            predicate,
            object_key,
            qualifiers_json,
        ), assertions in sorted(relation_groups.items())
    )

    source_counts: Counter[str] = Counter()
    for entity_key in identity_index.source_memberships.values():
        resolved = identity_index.resolve_entity_key(entity_key)
        if resolved in used_entity_keys:
            source_counts[resolved] += 1
    for entity_key in used_entity_keys:
        if entity_key not in identity_index.entities:
            raise ValueError("Gold assertion resolves to unknown entity")
    return GoldResolutionDraft(
        entity_keys=tuple(sorted(used_entity_keys)),
        source_node_counts=dict(sorted(source_counts.items())),
        fields=tuple(
            sorted(
                fields,
                key=lambda item: (
                    item.entity_key,
                    item.predicate,
                    canonical_json(item.qualifiers),
                    canonical_json(item.value),
                ),
            )
        ),
        identifiers=identifiers,
        relations=relations,
        conflicts=tuple(
            sorted(
                conflicts,
                key=lambda item: (
                    item.entity_key,
                    item.predicate,
                    canonical_json(item.qualifiers),
                ),
            )
        ),
        eligible_policy_counts=dict(sorted(eligibility.policy_counts.items())),
        withheld_assertion_count=eligibility.withheld,
        unresolved_identity_count=unresolved,
    )
