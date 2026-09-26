# Community Silver epoch manifest v3

## Media type and encoding

Media type:
`application/vnd.video-media-catalog.silver-epoch-manifest.v3+json`.

The object is canonical UTF-8 JSON with one trailing newline. Field names use
lower camel case. Digests use `sha256:<64 lowercase hex>`, and Iceberg snapshot
IDs are positive integers.

`epochId` is the domain-separated deterministic key
`community-silver-epoch-v3` over every field below except `epochId` itself.
Changing a snapshot, reference, generation, table mapping, delta, watermark,
count, digest, or timestamp therefore creates another epoch.

## Fields

- `schemaVersion`: literal `3.0`.
- `epochId`: deterministic manifest identity.
- `parentEpoch`: optional `CommunitySilverEpochReference`.
- `baselineEpoch`: optional `CommunitySilverEpochReference`.
- `deltaRunIds`: sorted unique run IDs, at most 4,096.
- `runSnapshotId`: exact `community_ingest_run` snapshot.
- `commitSnapshotId`: exact `community_ingest_commit` snapshot.
- `dataSnapshotIds`: every Silver data table mapped to an exact snapshot ID or
  explicit `null` for a table that has never had rows.
- `identityGenerationId` and `tableMapping`: optional as a pair. New
  generation-aware epochs include the bounded generation ID and the complete
  deterministic logical-to-physical mapping. Legacy fixed-name epochs omit
  both and remain readable.
- `sourceWatermarks`: source-product ID to bounded watermark.
- `committedRunCount`: total distinct committed runs.
- `committedRunDigest`: digest of the complete committed-run relation.
- `committedRunDigestAlgorithm`: literal
  `sha256-bucketed-run-ids-v1`.
- `createdAt`: normalized RFC3339 timestamp.

An epoch reference contains `epochId` plus `objectRef`. The ObjectRef must bind
JSON media type, URI, SHA-256, and byte size. S3 references additionally
require VersionId and ETag; local references cannot claim S3 metadata.

## Baseline and delta rules

A root baseline has neither `parentEpoch` nor `baselineEpoch`. It may summarize
an arbitrarily large pinned commit snapshot with an empty delta.

A child has both references. `parentEpoch` is the direct predecessor.
`baselineEpoch` is the root baseline, or the parent itself when the parent is
the root. Every parent run must remain in the child, and `deltaRunIds` must
equal the distributed anti-join of child and parent commit snapshots.

Source watermark keys cannot disappear. Adding or changing a watermark
requires a committed `SOURCE_ASSERTIONS` delta run for that source product.

## Committed-run digest

Readers and writers:

1. validate unique normalized run IDs in the exact commit snapshot;
2. bucket each ID by its first SHA-256 byte;
3. sort IDs in each bucket and SHA-256 hash their newline-delimited bytes;
4. produce at most 256 `(bucket, runCount, digest)` summaries;
5. compute a domain-separated canonical digest over the algorithm, total
   count, and sorted summaries.

Only bounded bucket summaries may cross to the driver. The complete run list
must remain distributed.

## Consumption

Consumers verify the immutable epoch ObjectRef, read the exact commit snapshot,
validate its count/digest, and use its distributed `run_id` DataFrame to join
the exact data snapshots from `tableMapping`. Run metadata is read from
`runSnapshotId`. For a generation-aware epoch, the committed-run relation is
all shared `SOURCE_ASSERTIONS` runs plus only `IDENTITY_RESOLUTION` and
`IDENTITY_CURATION` runs whose immutable run manifest declares that exact
generation and mapping. A consumer never discovers or substitutes a "latest"
generation.

Parent traversal is for audit validation, not for reconstructing the current
run set. Legacy `CommunitySilverSnapshotSet` schema `2.0` remains readable for
existing bounded releases.
