# Community catalog v2 contracts

## Compatibility

These contracts are additive and do not replace `media_catalog.v1`. V1 source
manifests, landing records, six curated tables, algorithm identity, control
objects, and API remain unchanged.

V2 identifiers use canonical JSON and domain-separated SHA-256 keys in the
existing `sha256:<64 lowercase hex>` form. New contracts must never reinterpret
or recompute a published v1 key.

## Rights profile

A rights profile is a versioned machine policy. It contains:

- `policyId`, `policyVersion`, and `policyDigest`;
- source terms and license URI;
- one physical policy zone;
- explicitly permitted actions;
- required attribution and share-alike duties;
- optional audience, purpose, and territory limits;
- optional acquisition expiry and maximum cache age;
- termination purge requirements.

Absence of a permission means denial. `ml_training` is never inferred from
`commercial`, `derivatives`, or an open-source software license.

## Source registry

The source registry contains:

- `SourceSystem`: operator identity and status;
- `SourceProduct`: one API, dump, archive, registry, or contracted feed;
- `SourceNamespace`: identifier namespace, legacy scheme aliases, referent
  kinds, validation, and case-normalization rules;
- `DatasetRelease`: immutable source publication and coverage;
- `SchemaContract`: native schema identity and compatibility rules.

Registry IDs are stable slugs. Provider and product names are display metadata
and may change without changing those IDs.

The bootstrap registry includes:

- Wikidata structured JSON under CC0, including Wikidata-observed external
  identifiers;
- EIDR public-registry records, without a default network search client;
- TVmaze public API under its free API share-alike policy;
- IMDb's seven official non-commercial TSV datasets, restricted to the
  `research_private` research audience and purpose;
- TMDB official daily ID exports and v3 API responses, restricted to the same
  research context and carrying TMDB attribution duties.

IMDb/TMDB profiles intentionally omit export, redistribution, embedding, and
ML permissions. Absence remains denial. Image references never inherit a
metadata permission automatically.

## ConnectorBatchManifest

Required identity fields:

- `batchId`;
- `sourceSystemId`, `sourceProductId`;
- `connectorId`, `connectorVersion`;
- `imageDigest`, `configDigest`;
- `policyId`, `policyDigest`.

Required source semantics:

- `transportKind`: `DUMP`, `API`, or `FEED`;
- `serialization`;
- `changeSemantics`: `FULL_SNAPSHOT`, `DELTA`, `LEASED_SNAPSHOT`, or
  `LEASED_DELTA`;
- `completeness`: `COMPLETE` or `PARTIAL`;
- `deleteCoverage`: `EXPLICIT`, `SNAPSHOT_DIFF`, or `NONE`;
- canonical `coverageScope` and its digest;
- optional source window and watermarks.

Required captured output:

- immutable raw `ObjectRef` values;
- `acquiredAt` and optional `replayableUntil`;
- non-negative record, error, retry, and rate-limit counts.

`batchId` is derived from immutable source identity, objects, policy, connector,
coverage, acquisition time, and counters. A re-run with the same identity must
produce the same manifest bytes. A leased batch requires `replayableUntil`.

Official API/dataset acquisition and Spark mapping are separate trust
boundaries. Acquisition may access only explicitly allowlisted official
origins, stores every response as an immutable raw `ObjectRef`, and publishes
the batch before normalized records. Spark reads only committed batch and
record-set objects; it never receives provider credentials and never calls a
provider API. Web-page scraping is outside this contract.

## ConnectorRecordEnvelope

Required fields:

- `envelopeKey`, `batchId`;
- source system/product/namespace and `sourceRecordId`;
- optional `sourceRevision`;
- `operation`: `UPSERT`, `DELETE`, `RETRACT`, `EXPIRE`, or
  `INFERRED_ABSENCE`;
- `observedAt` and `ingestedAt`;
- optional source-modified, valid-from/to, and expiry times;
- `payloadSchema`, `sourceHash`;
- exactly one inline `payloadJson` or immutable `payloadObject`;
- raw source object and source location;
- `policyId`, `policyDigest`, and citation keys.

For inline payloads, `sourceHash` must equal the canonical JSON hash.
`envelopeKey` is derived from the source record version, operation, payload
hash, batch, and policy. API pages are captured objects, not business records.

`INFERRED_ABSENCE` is legal only when comparing complete releases with equal
coverage scope and `deleteCoverage=SNAPSHOT_DIFF`.

## ConnectorRecordSetManifest

Decoded envelopes are written in bounded immutable shards. A commit-last record
set manifest binds the connector batch, policy, shard ObjectRefs, record count,
first/last streamed envelope keys, and creation time. Batch capture may succeed
while mapping fails; in that case the raw batch remains replayable but no record
set commit is published.

A valid zero-record delta has an empty `recordObjects` list and null key bounds;
connectors must not publish a fake NDJSON record merely to represent an empty
window. A non-empty record set requires at least one immutable record object.

Silver Spark ingestion must never read a mutable latest S3 key for a versioned
record shard. After verifying each shard ObjectRef, the driver materializes the
declared VersionId bytes into a checksum-addressed staging object under an
explicit catalog warehouse staging prefix, then Spark reads only the staged
inputs. S3 record shards require `--record-staging-prefix`; the prefix must stay
within the catalog warehouse bucket and under an allowed research write path such
as `<warehouse>/research/control/...` or
`landing/research/materialized-record-shards/...`. Missing or out-of-scope
prefixes fail closed; capture sibling prefixes are not used by default.
Materialization is commit-last and concurrent-safe: an existing staging object is
reused only when checksum and size match, otherwise ingestion fails closed. The
driver owns a scratch TemporaryDirectory for verify→materialize→mapping and
cleans local copies on success or failure. Mapping validates record count and
first/last envelope key bounds against the record-set manifest before and after
projection.

Concrete deletion rules are fail-closed:

- IMDb deletion inference is permitted only between complete seven-file
  snapshots with equal coverage;
- Wikidata deletion inference requires equal caller-declared coverage;
- EIDR defaults to partial coverage and no deletion inference;
- TMDB daily ID exports are inventory seeds and never imply deletion;
- TMDB and TVmaze API deltas emit DELETE only from an explicit not-found detail
  observation captured in the same batch.

## Assertions

V2 source mappers emit separate contracts:

- `FieldAssertion`;
- `IdentifierAssertion`;
- `RelationshipAssertion`;
- `EntityTypeAssertion`;
- `Citation`;
- `MediaAssetAssertion`;
- `MetricObservation`.

Every assertion binds:

- a source record envelope/version;
- source field path or native statement ID;
- typed value and qualifiers;
- validity and observation time;
- policy and citations;
- parser/mapping version;
- optional evidence and confidence.

Assertion identity does not include the current canonical entity key.

## Identity ledger

The identity ledger contains:

- permanent `EntityLedgerEntry` values;
- source entity nodes;
- materialized `ExternalIdIndexEntry` values;
- identity evidence;
- immutable identity conflicts;
- reversible decisions;
- effective entity memberships;
- entity redirects and merge/split events;
- `LegacyKeyMap` for every published v1 key.

The additive Silver tables are `community_external_id_index`,
`community_identity_conflict`, `community_entity_merge_event`, and
`community_entity_split_event`.

New entity keys are allocated from an internal canonical UUIDv7 allocation ID.
Provider identifiers are never key inputs. A redirect may not form a cycle.

Exact-ID blocking is driven by the pinned source-registry snapshot rather than
resolver code constants. A `SourceNamespace` defines accepted legacy scheme
aliases, validation, case handling, and compatible referent kinds. The
bootstrap registry covers Wikidata items, distinct `douban-work` and
`douban-person` identifiers, IMDb titles/names/companies, TMDB movies/TV/people,
EIDR content, TVmaze shows, and TheTVDB series. The legacy `douban` scheme and
`douban-subject` namespace are migration aliases selected by referent kind;
they are not a shared canonical namespace.
Registering matching metadata does not activate a connector or grant source
rights; identifier assertions still carry the policy of the source that
observed them. In particular, P4529/P5284 assertions remain
`wikidata-json-dump`/CC0 lineage and do not imply a Douban feed, API call, or
web-page acquisition.

`ExternalIdIndexEntry` materializes the blocking tuple
`(namespaceId, normalizedValue, referentKind)` and the candidate entity,
assertion keys, policy, and observation time. One block may intentionally have
multiple entries. A source node with one compatible candidate can be accepted;
zero candidates allocate a new internal entity; more than one candidate emits
an immutable `IdentityConflict` for review and does not create a decision or
membership.

`PARENT_CONSTRAINED` evidence is valid only for a season or episode. It binds
the child to a resolved parent source node/entity/membership, the hierarchy
relationship assertion, and the season/episode ordinal assertions. Parent
identity or numbering alone is insufficient.

Review decisions are immutable `ACCEPT`, `REJECT`, `UNCERTAIN`, or `REVOKE`
events. `ACCEPT` opens a membership, `REJECT` creates none, and `REVOKE`
closes the accepted membership interval without deleting history. Merge events
select the earliest stable entity as survivor and emit acyclic redirects for
all retired keys. Split events never guess a redirect: they record explicit
source-node-to-target assignments and preserve the original key in history.

## Unified research release

The serving release has one fixed context, `research`, and one configured OIDC
owner subject. It pins:

- committed connector batches and ingest runs;
- source watermarks and Silver snapshots;
- identity, field-resolution, and rights-policy digests;
- registered Gold contract version and exact table snapshots;
- quality report references;
- attribution manifest;
- affected and total row counts.

Readers may only use the final release commit. Research-private assertions are
eligible only after their registered policy passes the requested action,
audience, purpose, territory, expiry/cache, attribution, and digest checks.
There is no parallel public release or public serving alias in v2.
