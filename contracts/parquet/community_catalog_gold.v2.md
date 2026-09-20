# Research catalog Gold v2

## Scope

Gold is the release-isolated resolution of committed Silver assertions for the
single `research` context. This contract does not publish parallel
public, attributed, commercial, or ML Gold variants.

Gold never changes v1 keys. Research-private data is eligible only when its
registered policy explicitly permits the required research actions.

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
`context_id = research`.

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
`SEARCH` permissions are evaluated for audience `research`, purpose `research`,
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

## Research serving projection

The only v2 serving index is isolated from v1:

- index prefix: `media-catalog-research`;
- versioned indexes: `media-catalog-research-<build-id>`;
- read alias: `media-catalog-research-read`;
- document ID: internal `entityKey`;
- mapping: strict and version/digest bound;
- source: one immutable Gold release commit and its exact table snapshots.

The default and authoritative publication path is a full rebuild into a new
versioned index followed by one atomic read-alias update. Full rebuilds are
intended to run weekly. Partition and worker concurrency are execution
parameters and do not change the existing build identity. Bulk action count and
wire bytes remain bounded, clients use the default AWS credential chain with
SigV4, transient requests are retried, and any partial failure fails closed.

Every deterministic `entityKey` partition publishes an immutable completion
receipt under the build ID. A receipt binds:

- the complete release-commit `ObjectRef`;
- mapping digest, config identity/digest, and image digest;
- concrete index, operation, partition ID, and partition count;
- input/success counts and an order-independent digest of all partition actions.

A retry may skip a partition only after recomputing and matching its receipt.
An existing receipt with any different identity, partition layout, count, or
input digest is a hard conflict.

Before an index build can be completed, Gold entity count, projected document
count, successful action count, zero failed actions, and concrete index document
count must reconcile. The concrete index must additionally contain only the
bound owner and target release plan. Any drift prevents alias publication.

Documents contain bounded titles, external identifiers, selected attributes,
relation counts, `sourceBadges`, `winningAssertions` with citation keys,
`rights`/attribution summaries, first-class `conflicts`, provenance release ID,
literal `contextId`, and overflow counts. Complete assertions and relation
edges remain in Iceberg. This slice exposes bounded citation keys plus source
record/path metadata because the current Silver schema has no dedicated
Citation table.

`externalIdentifiers[].url` is optional and derived at projection/API time.
Only bounded positive ASCII decimal IDs in a known namespace and compatible
referent kind receive a URL. `douban-work` uses
`https://movie.douban.com/subject/{id}/`; `douban-person` uses
`https://movie.douban.com/celebrity/{id}/`. Legacy `douban` and
`douban-subject` values are disambiguated by referent kind. Unknown namespaces,
conflicting kinds, zero/leading-zero IDs, overlong values, path/query fragments,
Unicode digits, and all other malformed values produce no URL. The URL is a
deterministic navigation convenience over a Wikidata-provided ID: the pipeline
does not fetch or store Douban titles, ratings, reviews, or images, and lineage
and rights remain Wikidata.

Release commit, index config digest, and index build manifest bind the exact
owner OIDC subject. Serving routes are only:

- `GET /api/v2/research/search`;
- `GET /api/v2/research/entities/{entityKey}`;
- `GET /api/v2/research/external-identifiers/{namespace}/{value}`.

Every v2 route requires `governance.read` and exact equality with the configured
owner `sub`. Building or switching the research alias never modifies
`media-catalog-entities-read`.

## Optional affected-entity indexing

Incremental indexing is disabled by default and requires an explicit immutable
affected-entity manifest. The manifest contains:

- target release plan, owner, context, and exact release-commit `ObjectRef`;
- base concrete index, base release plan, and base document count;
- a unique entity-key-sorted list of `UPSERT` or `DELETE` operations;
- an RFC3339 generation timestamp.

The incremental path never mutates the concrete index currently serving the
read alias. It copies the manifest-bound base into the target release's new
versioned index, updates release provenance, applies partitioned upsert/delete
actions, verifies every affected ID, then performs the same full target
owner/release/count checks before an atomic alias switch. This preserves the v2
cursor contract: cursors already issued against the base continue querying that
unchanged concrete index. The next weekly full rebuild remains authoritative.

## Offline sizing contract

The synthetic planner uses a bounded deterministic sample rather than creating
one million or five million documents. It reports average source and bulk-action
bytes, estimated primary-store bytes, recommended primary shards, effective
documents per bounded bulk request, request count, and estimated bulk duration.
Its assumptions are digest-bound. The planner performs no OpenSearch request and
is safe to run in unit tests and CI.

## UI contract

No UI source is present in this repository. A future read-only catalog UI must
consume the stable fields `sourceBadges`, `winningAssertions`, `rights`,
`conflicts`, and `overflow`; it must not infer licensing from provider names or
offer parallel serving modes.
