# Identity curation v2

## Scope

This contract defines the production control boundary for human identity review.
It adds no service, IAM role, storage bucket, catalog, or search index. The
curation Spark stage reuses the existing community Silver Iceberg tables and
publishes visibility through `community_ingest_commit` last.

## Immutable control objects

The media type for a curation manifest is
`application/vnd.video-media-catalog.identity-curation-manifest.v2+json`.
The manifest is supplied as a complete JSON `ObjectRef` containing URI,
SHA-256, byte size, and media type. An S3 reference must also contain an exact
VersionId and ETag; an unversioned latest key is invalid. `file://` references
are allowed only for local execution and are bound by SHA-256 and byte size.

The manifest contains:

- deterministic `manifestId` and `schemaVersion`;
- `pinnedSilverSnapshot.object` and its exact `snapshotSetId`;
- one or more bounded operations plus their exact `conflictKeys`;
- the deterministic output `decisionKeys`;
- exact OIDC `operatorSubject`, non-empty `reason`, and RFC3339 `operatedAt`;
- exact `configDigest` and `imageDigest`.

The pinned Silver object uses
`application/vnd.video-media-catalog.silver-snapshot-set.v2+json`. Consumers
must verify both ObjectRefs before reading any Iceberg snapshot.

## Operations

Every operation repeats the source node and assertion keys from one pinned
`IdentityConflict`. The stage rejects any mismatch.

- `ACCEPT` selects one conflict candidate and emits `HUMAN_REVIEW` evidence,
  an immutable `ACCEPT` decision, and one membership.
- `REJECT` selects one conflict candidate and emits evidence and an immutable
  `REJECT` decision without a membership.
- `MERGE` accounts for every candidate, requires an explicit expected survivor,
  verifies the stable earliest survivor, opens the reviewed source membership,
  and emits one redirect per retired entity plus one merge event.
- `REDIRECT` records one explicit compatible source-to-target redirect and
  opens the reviewed source membership at the target.
- `SPLIT` lists every current member of the source entity plus the conflict
  source exactly once. It closes prior membership versions, emits one reviewed
  decision and new membership per assignment, may allocate explicitly declared
  UUIDv7-backed target entities, and emits one split event. It never infers a
  redirect.

## Fail-closed rules

Before materialization, the stage requires:

1. every conflict to exist in the exact pinned snapshot;
2. every referenced existing entity to exist and be active;
3. source-node, entity-level, and entity-kind compatibility;
4. merge candidates to share one level and kind and the declared survivor to
   equal deterministic stable-survivor selection;
5. split assignments to cover all affected source nodes with no duplicate and
   every declared target to be used;
6. the combined pinned and proposed redirect graph to have one target per
   source and no cycle;
7. generated decision IDs to equal the manifest `decisionKeys`.

The implementation bounds operations, split assignments, redirect traversal,
and driver-side review state. Bounds are errors, never truncation.

## Run and visibility

The stage writes one `CommunityIngestRun` with
`runKind=IDENTITY_CURATION`, `inputId=manifestId`, and the manifest ObjectRef in
`inputManifest`. Evidence, decisions, memberships, redirects, merge/split
events, and required new ledger entries are materialized together. All data
tables are insert-only; `community_ingest_commit` is written only after exact
per-run row counts and owned Iceberg snapshots are verified.

Submitting the same manifest produces the same run and row keys. If its commit
already exists, `CommunityCatalogTables` verifies the immutable run manifest
and returns the existing commit without restaging. Reusing a logical row key
under a different run cannot satisfy per-run counts and therefore cannot
publish a commit.

## Submission boundary

Submission and application belong exclusively to the batch control plane:

- `video-media-catalog-identity-curation publish` validates and immutably
  publishes a complete manifest;
- `video-media-catalog-identity-curation apply` verifies both control objects,
  requires the runtime operator subject and image/config digests to equal the
  manifest, time-travels the pinned Silver snapshots, and commits the curation
  run.

This repository does not expose identity review or curation over HTTP.
