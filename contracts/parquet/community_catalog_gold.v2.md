# Personal research catalog Gold v2

## Scope

Gold is the release-isolated resolution of committed Silver assertions for the
single `personal-research` context. This contract does not publish parallel
public, attributed, commercial, or ML Gold variants.

Gold never changes v1 keys. Research-private data is eligible only when its
registered policy explicitly permits the required personal research actions.

## Release plan

Before Gold rows are written, the builder publishes an immutable release plan:

- `release_plan_id`;
- exact owner OIDC subject;
- policy context, as-of time, territories, and allowed policy zones;
- exact committed Silver run IDs and input snapshot IDs;
- identity membership snapshot;
- rights registry, field policy, resolver, image, and config digests;
- expected row counts for every Gold table.

Every Gold row contains `release_plan_id`. A final release commit is visible
only after all expected rows are present and quality gates pass. The release
plan and final release commit both bind `owner_subject` and the literal
`context_id = personal-research`.

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

`trace_json.assertions[]` carries bounded, typed serving lineage for each
eligible assertion: source product/name/record/path, observation time,
citation keys, policy zone, license, attribution, source URL, and share-alike.

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

Each assertion must resolve to a registered rights profile and registered
source product. Before field resolution, `STORE`, `TRANSFORM`, `DISPLAY`, and
`SEARCH` permissions are evaluated for audience `personal`, purpose `research`,
territory, as-of time, expiry, cache age, policy digest, and allowed zone.

The default context permits open zones, `public_registry`, and
`research_private`; it never permits quarantine. Zone admission is not a
license bypass: an assertion is withheld when expired, outside scope, missing
a policy, or lacking any requested permission.

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
- every selected value has complete assertion, citation-key, source-product,
  license, attribution, and policy lineage;
- identifier uniqueness and redirect acyclicity;
- no relation has an unresolved endpoint;
- no forbidden rights-zone dependency;
- attribution claim counts exactly cover every eligible policy count;
- conflict and unresolved rates remain within the release policy budget.

Rows are staged first. The release commit is inserted last. Search indexing
reads exact Gold snapshots plus the exact commit-table snapshot.

## Personal research serving projection

The only v2 serving index is isolated from v1:

- index prefix: `media-catalog-research`;
- versioned indexes: `media-catalog-research-<build-id>`;
- read alias: `media-catalog-research-read`;
- document ID: internal `entityKey`;
- mapping: strict and version/digest bound;
- source: one immutable Gold release commit and its exact table snapshots.

Documents contain bounded titles, external identifiers, selected attributes,
relation counts, `sourceBadges`, `winningAssertions` with citation keys,
`rights`/attribution summaries, first-class `conflicts`, provenance release ID,
literal `contextId`, and overflow counts. Complete assertions and relation
edges remain in Iceberg. This slice exposes bounded citation keys plus source
record/path metadata because the current Silver schema has no dedicated
Citation table.

Release commit, index config digest, and index build manifest bind the exact
owner OIDC subject. Serving routes are only:

- `GET /api/v2/research/search`;
- `GET /api/v2/research/entities/{entityKey}`;
- `GET /api/v2/research/external-identifiers/{namespace}/{value}`.

Every v2 route requires `governance.read` and exact equality with the configured
owner `sub`. Building or switching the research alias never modifies
`media-catalog-entities-read`.

## UI contract

No UI source is present in this repository. A future read-only catalog UI must
consume the stable fields `sourceBadges`, `winningAssertions`, `rights`,
`conflicts`, and `overflow`; it must not infer licensing from provider names or
offer a public/personal mode switch.
