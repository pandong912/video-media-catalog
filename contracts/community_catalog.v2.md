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
- `SourceNamespace`: identifier namespace, referent kinds, and validation;
- `DatasetRelease`: immutable source publication and coverage;
- `SchemaContract`: native schema identity and compatibility rules.

Registry IDs are stable slugs. Provider and product names are display metadata
and may change without changing those IDs.

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
- identity evidence;
- reversible decisions;
- effective entity memberships;
- entity redirects and merge/split events;
- `LegacyKeyMap` for every published v1 key.

New entity keys are allocated from an internal canonical UUIDv7 allocation ID.
Provider identifiers are never key inputs. A redirect may not form a cycle.

## Policy-specific release

A release identifies one audience, purpose, territory, and as-of time. It pins:

- committed connector batches and ingest runs;
- source watermarks and Silver snapshots;
- identity, field-resolution, and rights-policy digests;
- registered Gold contract version and exact table snapshots;
- quality report references;
- attribution manifest;
- affected and total row counts.

Readers may only use the final release commit. Open releases must pass an
anti-join proving that no assertion, identity decision, asset, or derived value
depends exclusively on a restricted policy zone.
