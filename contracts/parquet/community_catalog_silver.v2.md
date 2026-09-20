# Community catalog Silver v2

## Scope

This contract persists immutable connector records, source assertions, and
identity-ledger changes. It is parallel to the six v1 curated tables and does
not alter their schema or semantics.

All tables use Iceberg format version 2 and Zstandard Parquet. JSON columns use
the repository's canonical UTF-8 JSON encoding. Digests and logical keys use
`sha256:<64 lowercase hex>`.

The shared Spark mapper registry accepts committed record sets for
`wikidata-json-dump`, `eidr-public-registry`, `tvmaze-public-api`,
`imdb-non-commercial-datasets`, and `tmdb-research`. Mapping is
source-owned and deterministic. Spark executors read only immutable record
objects and never call source APIs or receive source credentials.

## Run visibility

Every data row carries a deterministic `run_id`. A row is visible to downstream
Gold processing only when the same run has one valid row in
`community_ingest_commit`.

Publication order is:

1. insert immutable `community_ingest_run`;
2. insert every staged data row;
3. verify actual per-run counts in every declared data table;
4. capture the exact latest table snapshot IDs used as upper bounds;
5. insert `community_ingest_commit` last.

A failed run may leave staged rows but no commit marker. Those rows are
invisible. Retrying identical immutable inputs reuses the same `run_id` and
keys. A new code, mapping, policy, input, or configuration digest creates a new
run.

The snapshot IDs in a commit are audit upper bounds, not a claim that the
snapshot contains only one run. Concurrent rows remain isolated by `run_id`.
Gold releases must pin both the data-table snapshots and the commit-table
snapshot, then join only committed runs.

## Silver snapshot set

`CommunitySilverSnapshotSet` is the small commit handoff to Gold. It contains:

- deterministic `snapshotSetId`;
- the exact committed run IDs included in this build;
- the exact `community_ingest_run` snapshot ID;
- the exact `community_ingest_commit` snapshot ID;
- one exact snapshot ID or explicit empty value for every Silver data table;
- creation time.

Gold readers first verify the snapshot-set ObjectRef, then time-travel every
table and anti-join any run not listed in the snapshot set.

The production publisher accepts at most 4,096 explicit run IDs. It captures
the commit snapshot before the run/data snapshots, validates one immutable run
manifest and one commit for every selected ID, checks manifest counts against
commit counts, and verifies every selected run's row count at each pinned data
snapshot before publication. It never infers a run selection from a latest
control object.

The published JSON itself is an immutable `ObjectRef`. S3 publication is valid
only when the returned reference includes URI, SHA-256, byte size, VersionId,
and ETag. Consumers must pass all five values and may not resolve an unversioned
latest key.

## Production stages

`video-media-catalog-research-silver` provides three auditable Spark stages:

1. `migrate-v1` verifies an immutable v1 `SnapshotSet`, time-travels all six
   declared v1 table snapshots, preserves every legacy key, and commits the v2
   ledger/map rows last.
2. `resolve-identity` verifies a pinned Silver snapshot, requires every
   explicitly selected source run to be a committed `SOURCE_ASSERTIONS` run,
   time-travels the pinned v1 entity/external-ID snapshots, and delegates to
   the registry-driven identity implementation. All identity tables, including
   empty redirect/merge/split frames, share one commit-last run boundary.
3. `publish-snapshot` performs the validation above and publishes the exact
   `CommunitySilverSnapshotSet` consumed by Gold.

All stages default to the existing `video_media_catalog` namespace and use the
AWS default credential chain, including an EMR Serverless execution role. They
do not accept static credentials or provision infrastructure.

`video-media-catalog-community-spark` verifies every record shard ObjectRef,
materializes versioned S3 bytes into checksum-addressed staging under an explicit
catalog warehouse staging prefix, and only then hands Spark immutable staged
URIs. S3 inputs require `--record-staging-prefix` within the warehouse bucket
and an allowed research write path; capture sibling prefixes are not used by
default. Spark must not read unversioned `s3://` keys for declared record
shards. Materialized inputs remain bound to the source checksum and byte size
through mapping, and the mapper rejects record count or envelope key bound drift.
The driver owns scratch TemporaryDirectory cleanup across verify, materialize,
and mapping.

## Run tables

### `community_ingest_run`

Primary key: `run_id`.

- `run_id STRING NOT NULL`
- `run_kind STRING NOT NULL`
- `source_product_id STRING NOT NULL`
- `input_id STRING NOT NULL`
- `policy_id STRING NOT NULL`
- `policy_digest STRING NOT NULL`
- `image_digest STRING NOT NULL`
- `config_digest STRING NOT NULL`
- `started_at STRING NOT NULL`
- `expected_counts_json STRING NOT NULL`
- `manifest_json STRING NOT NULL`

### `community_ingest_commit`

Uniqueness key: `run_id`. `commit_key` binds the immutable commit payload.

- `commit_key STRING NOT NULL`
- `run_id STRING NOT NULL`
- `committed_at STRING NOT NULL`
- `table_counts_json STRING NOT NULL`
- `table_snapshot_ids_json STRING NOT NULL`
- `commit_json STRING NOT NULL`

Only one immutable commit payload is valid for a `run_id`.

## Source and assertion tables

### `community_source_record`

Primary key: `envelope_key`.

- `envelope_key`, `run_id`, `batch_id`
- `source_system_id`, `source_product_id`, `source_namespace_id`
- `source_record_id`, nullable `source_revision`
- `operation`
- nullable `source_modified_at`, `valid_from`, `valid_to`, `expires_at`
- `observed_at`, `ingested_at`
- `payload_schema`, `source_hash`
- nullable `payload_json`, nullable `payload_object_json`
- `raw_object_json`, `source_location`
- `policy_id`, `policy_digest`, `citation_keys_json`

### `community_field_assertion`

Primary key: `assertion_id`.

- `assertion_id`, `run_id`
- subject namespace/source ID/referent kind
- `predicate`, `value_type`, `value_json`, `qualifiers_json`, `status`
- `provenance_json`, `policy_id`, `policy_digest`, `observed_at`

### `community_identifier_assertion`

Primary key: `assertion_id`.

- `assertion_id`, `run_id`
- subject namespace/source ID/referent kind
- identifier `namespace_id`, `value`, `issuer`, `referent_kind`
- `status`, `provenance_json`, `policy_id`, `policy_digest`, `observed_at`

### `community_relationship_assertion`

Primary key: `assertion_id`.

- `assertion_id`, `run_id`
- subject namespace/source ID/referent kind
- `predicate`
- object namespace/source ID/referent kind
- `qualifiers_json`, `status`, `provenance_json`
- `policy_id`, `policy_digest`, `observed_at`

### `community_entity_type_assertion`

Primary key: `assertion_id`.

- `assertion_id`, `run_id`
- subject namespace/source ID/referent kind
- `entity_type`, `status`, `provenance_json`
- `policy_id`, `policy_digest`, `observed_at`

Assertion IDs do not include a canonical entity key.

## Identity tables

### `community_external_id_index`

Primary key: `index_entry_key`.

- `index_entry_key`, `run_id`, `blocking_key`, `materialization_id`
- `namespace_id`, `normalized_value`, `referent_kind`
- `entity_key`, `assertion_keys_json`, `observed_at`
- `policy_id`, `policy_digest`, `index_json`

`blocking_key` is derived only from
`(namespace_id, normalized_value, referent_kind)`. `index_entry_key` also binds
the candidate entity, assertion keys, observation time, and exact policy
version. `materialization_id` binds the resolver input, image, configuration,
start time, and registry digest so a later immutable run never aliases an
earlier row. Multiple entries for one block are legal and must produce review
conflicts rather than arbitrary selection.

### `community_entity_ledger`

Primary key: `entity_key`.

- `entity_key`, `run_id`
- nullable internal `allocation_id`
- `entity_level`, `entity_kind`, `status`
- `created_at`, nullable `first_release_id`
- `imported_v1 BOOLEAN NOT NULL`

### `community_legacy_key_map`

Primary key: `legacy_key`.

- `legacy_key`, `run_id`, `legacy_kind`, `target_key`
- `imported_at`, `source_snapshot_set_id`

Published v1 keys are copied verbatim. They are never recomputed.

### `community_identity_evidence`

Primary key: `evidence_key`.

- `evidence_key`, `run_id`, `kind`
- source namespace/source ID/referent kind
- `candidate_entity_key`
- `assertion_keys_json`, `observed_at`
- `policy_id`, `policy_digest`, nullable `confidence`
- `details_json`, `evidence_json`

`PARENT_CONSTRAINED` evidence stores typed parent source/entity/membership,
hierarchy-relation assertion, and season/episode ordinal details inside
`details_json`.

### `community_identity_conflict`

Primary key: `conflict_key`.

- `conflict_key`, `run_id`, `materialization_id`
- source namespace/source ID/referent kind
- `candidate_entity_keys_json`, `assertion_keys_json`, `reason`
- `observed_at`, `policy_id`, `policy_digest`
- `details_json`, `conflict_json`

Conflicts are immutable review-queue observations. They do not create an
entity membership. `materialization_id` distinguishes resolver runs that
observe the same unresolved source assertions. Review outcomes are separate
identity decisions.

### `community_identity_decision`

Primary key: `decision_id`.

- `decision_id`, `run_id`, `status`
- source namespace/source ID/referent kind
- `entity_key`, `evidence_keys_json`
- `policy_version`, `decided_by`, `decided_at`, `reason`
- `decision_json`

### `community_entity_membership`

Primary key: `membership_key`.

- `membership_key`, `run_id`
- source namespace/source ID/referent kind
- `entity_key`, `decision_id`, `valid_from`, nullable `valid_to`

### `community_entity_redirect`

Primary key: `redirect_key`.

- `redirect_key`, `run_id`
- `source_entity_key`, `target_entity_key`
- `effective_at`, `decision_id`

Redirects must be acyclic.

### `community_entity_merge_event`

Primary key: `merge_event_key`.

- `merge_event_key`, `run_id`
- `entity_keys_json`, `survivor_entity_key`, `redirect_keys_json`
- `decision_id`, `effective_at`, `merged_by`, `reason`
- `event_json`

Every non-survivor entity has exactly one redirect recorded by the event.

### `community_entity_split_event`

Primary key: `split_event_key`.

- `split_event_key`, `run_id`
- `source_entity_key`, `target_entity_keys_json`, `assignments_json`
- `effective_at`, `split_by`, `reason`
- `event_json`

Assignments explicitly bind each affected source node to one target entity.
All target entities must be covered; a split does not imply a redirect.

## Partitioning

Run metadata and commits are bucketed by `run_id`. Source/assertion tables are
initially bucketed by `run_id` to support deterministic staging verification,
run-level removal, and compaction. Entity-ledger and redirect tables are
bucketed by their primary entity key for lookup.
`community_external_id_index` is bucketed by `blocking_key` so exact-ID joins
co-locate the namespace/value/referent tuple. Conflict and merge/split event
tables remain bucketed by `run_id` for review and audit replay.

This initial layout must be benchmarked before full community backfills.
Changing a partition spec is an Iceberg metadata evolution and does not change
logical key semantics.
