# Community catalog Gold v2

## Scope

Gold is a policy-specific, release-isolated resolution of committed Silver
assertions. It is not a universal truth table. The same internal entities may
have different Gold releases for `public_cc0`, attributed/share-alike,
personal-research, commercial, or ML-eligible contexts.

Gold never changes v1 keys and never promotes a restricted assertion into an
open release.

## Release plan

Before Gold rows are written, the builder publishes an immutable release plan:

- `release_plan_id`;
- policy context, as-of time, territories, and allowed policy zones;
- exact committed Silver run IDs and input snapshot IDs;
- identity membership snapshot;
- rights registry, field policy, resolver, image, and config digests;
- expected row counts for every Gold table.

Every Gold row contains `release_plan_id`. A final release commit is visible
only after all expected rows are present and quality gates pass.

## Tables

All tables are Iceberg format version 2 with Zstandard Parquet. The initial
partition spec buckets by `release_plan_id`.

### `community_gold_entity`

Primary row key: deterministic `(release_plan_id, entity_key)`.

- `row_key`, `release_plan_id`, `entity_key`
- `entity_level`, `entity_kind`, `status`
- `source_node_count`
- `trace_json`

### `community_gold_field`

Primary key: `resolution_key`.

- `resolution_key`, `release_plan_id`, `entity_key`
- `predicate`, `scope_hash`
- `value_type`, nullable `value_json`, `qualifiers_json`
- `resolution_status`: `SELECTED`, `SET`, `CONFLICTED`, or `WITHHELD`
- nullable `selected_assertion_id`
- `assertion_ids_json`, `trace_json`

`SINGLE` policies publish one value only when all eligible active assertions in
the same scope agree. Competing values produce `CONFLICTED`; no arbitrary
provider wins. `SET_UNION` publishes one row per distinct value.

### `community_gold_identifier`

Primary key: `resolution_key`.

- `resolution_key`, `release_plan_id`, `entity_key`
- `namespace_id`, `value`, `issuer`, `referent_kind`
- `assertion_ids_json`, `trace_json`

Accepted identifier assignments are unique by namespace/value/referent kind
within a release. A value assigned to multiple entities blocks the release.

### `community_gold_relation`

Primary key: `resolution_key`.

- `resolution_key`, `release_plan_id`
- `subject_entity_key`, `predicate`, `object_entity_key`
- `qualifiers_json`, `assertion_ids_json`, `trace_json`

Both source nodes must have active identity memberships in the pinned identity
snapshot.

### `community_gold_conflict`

Primary key: `conflict_key`.

- `conflict_key`, `release_plan_id`, `entity_key`
- `predicate`, `scope_hash`, `reason`
- `assertion_ids_json`, `candidate_values_json`, `trace_json`

Conflicts are first-class queryable output, not ingest errors hidden from
consumers.

### `community_gold_release_plan`

Uniqueness key: `release_plan_id`.

- plan identity, context, inputs, expected counts, and `plan_json`.

### `community_gold_release_commit`

Uniqueness key: `release_plan_id`.

- `commit_key`, `release_plan_id`, `committed_at`
- actual row counts and exact table snapshot IDs
- quality report and attribution manifest ObjectRefs
- `commit_json`

## Rights gate

Each assertion must resolve to a registered rights profile. Before field
resolution, the requested actions, audience, purpose, territory, as-of time,
expiry, and allowed policy zones are evaluated.

An assertion is withheld when it is expired, outside scope, missing a policy,
or lacks any requested permission. Open releases must have zero dependencies
on `research_private`, `federated_ephemeral`, `commercial`, or quarantine
zones unless that release context explicitly allows the corresponding zone.

## Identity gate

Only effective entity memberships from committed identity runs are accepted.
Redirects are resolved to an acyclic survivor. Unresolved, rejected, ambiguous,
or multiply assigned source nodes are excluded and reported.

New source nodes receive an internal allocation once. Allocation may use a
deterministic request identity for retry safety, but the persisted entity key
is thereafter authoritative and is never recalculated from a provider ID.

## Quality and publication

Blocking checks include:

- every row belongs to the release plan;
- actual counts equal plan counts;
- every selected value has complete assertion, citation, and policy lineage;
- identifier uniqueness and redirect acyclicity;
- no relation has an unresolved endpoint;
- no forbidden rights-zone dependency;
- attribution coverage is 100 percent for attributed/share-alike assertions;
- conflict and unresolved rates remain within the release policy budget.

Rows are staged first. The release commit is inserted last. Search indexing
reads exact Gold snapshots plus the exact commit-table snapshot.
