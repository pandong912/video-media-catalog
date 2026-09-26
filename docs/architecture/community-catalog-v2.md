# Research catalog batch architecture

## Boundary

This repository is a batch data-processing project. Its supported path is:

```text
official capture
  -> immutable connector manifests and record shards
  -> Silver source assertions
  -> Identity ledger
  -> research Gold release
  -> versioned OpenSearch index build
```

It does not host an HTTP service. OpenSearch documents are a rebuildable
projection; Silver and Gold Iceberg tables plus their commit-last control
objects remain the authoritative data products.

## Capture

Acquisition and transformation are separate trust boundaries:

- capture workloads access only reviewed official origins;
- every raw response or dataset file is stored as an immutable `ObjectRef`;
- batch, record-set, partition, and epoch manifests bind checksums, sizes,
  policy, source window, completeness, and deletion coverage;
- Spark reads committed immutable record shards and never receives provider
  credentials or calls provider endpoints.

Supported acquisition remains:

- Wikidata official dump sync and full-media backfill;
- EIDR discovered-ID exact lookup;
- TVmaze full and delta capture;
- IMDb official TSV snapshot capture;
- TMDB daily inventory and changes/detail capture.

### EIDR public exact resolution

The public EIDR path is anonymous and accepts only identifiers discovered from
a pinned Silver snapshot. It can issue only
`GET https://resolve.eidr.org/EIDR/object/{normalized-id}?type=Full&followAlias=false`
with XML `Accept` and an identifying `User-Agent`; it has no title-search or
credential surface. Redirects cannot change the HTTPS host, path, identifier,
or query. One explicit urllib request timeout bounds both connection and socket
reads; response bytes, request spacing, attempts, `Retry-After`, and
exponential backoff are also bounded. Successful responses must declare
`application/xml` or `text/xml`, with an optional charset. HTTP 404 becomes
`NOT_FOUND`; every other non-retryable 4xx fails closed.

The provider authorization is built from a checksum- and version-pinned
evidence `ObjectRef`, binds the current `eidr_rights_profile()`, and always has
`completeFeedAllowed=false`. Consequently every Connector capture remains
`PARTIAL` with `changeSemantics=DELTA` and `deleteCoverage=NONE`; this path
cannot create or consume an `EidrCompleteFeedProof`.

`lookup-manifest` repeats the existing commit-last single-batch primitive and
stops at the first of the discovered-manifest end, `max-batches`,
`max-duration-seconds`, or `max-ids`. Each aggregate entry preserves its lookup
batch, receipt, watermark, and optional existing Connector batch/record-set
pair. The final
`application/vnd.video-media-catalog.eidr-backfill-run-manifest.v1+json`
object is the bounded Source Silver fan-out input. The Connector sharded
record-set v2.1 contract is intentionally not used for this index: it binds one
`batchId`, while each exact-lookup window is an independently committed batch
with its own policy-bound envelopes.

`expand-source-silver-inputs` is the read-only orchestration boundary for this
aggregate. It accepts a pinned run-manifest `ObjectRef`, rejects an incomplete
run unless `--allow-partial-run` is explicitly present, verifies every nested
control `ObjectRef`, and emits only a bounded JSON array of
`batchManifest`/`recordSetManifest` ObjectRef pairs. Batches containing only
`NOT_FOUND` results have no Connector capture and therefore produce no array
entry. Argo may pass this exact array to `withParam`; each item remains an
ordinary single-batch input to `video-media-catalog-community-spark`.

## Silver and Identity

Silver schema `2.x` stores source records and assertions under deterministic
source-run IDs. Data becomes visible only after
`community_ingest_commit` verifies exact per-table counts and owned Iceberg
snapshots.

Identity resolution consumes only pinned committed source runs. Exact external
identifiers and existing source-run identity index rows provide blocking;
unmatched components allocate permanent internal UUIDv7-backed entity keys.
Ambiguity, oversized components, parent mismatch, and lifecycle uncertainty
emit conflicts instead of guessed memberships.

Source/assertion physical tables and run/commit control tables are fixed and
shared. Identity logical tables resolve to generation-specific physical tables
in the same Glue namespace. A full run reads no historical Identity tables and
requires an empty target generation; an incremental run must pin and reuse the
same generation and complete table mapping. This prevents global-key MERGE
matches in an older migration table from suppressing rows in a new full build.

The bounded `CommunitySilverSnapshotSet` remains schema `2.0`. Long-running
histories use `CommunitySilverEpochManifest` schema `3.0`, carrying bounded
deltas and distributed committed-run count/digest rather than a driver-sized
history list. Both contracts optionally carry generation plus a deterministic
logical-to-physical mapping; omission preserves the legacy fixed-table reader.

## Gold

Gold publishes one `research` context. Rights eligibility is applied before
source priority and field resolution. The release contains entity, field,
identifier, relation, and conflict tables, plus immutable quality,
attribution, plan, and release-commit objects.

Both bounded schema `2.0` Silver snapshots and schema `3.0` epochs are valid
Gold inputs. Their distinct versioned contracts are preserved.

## OpenSearch build

The batch indexer builds only the research family:

- prefix `media-catalog-research`;
- read alias `media-catalog-research-read`;
- strict mapping with projectionVersion `6`;
- deterministic build identity bound to one Gold release commit;
- partition receipts, count reconciliation, and atomic alias update.

Full rebuild is authoritative. The optional affected-entity path copies an
immutable base into a new concrete index and never mutates the old index.

## Operational safety

- S3 inputs and control objects are checksum- and version-pinned.
- AWS authentication uses the default credential chain; static keys are not
  CLI arguments.
- Iceberg maintenance defaults to plan-only and honors protected snapshots.
- Failed Identity attempts require dry-run-first, reverse-order snapshot
  rollback with no-commit, unchanged-head, parent-journal, and protected-ref
  guards. Partial runs are not directly retried, and EMR job attempts default
  to one.
- Source removal is dry-run-first and does not delete data unless a separate
  explicitly confirmed execution is invoked.
- Unit and Spark tests use local fixtures; CI does not run real AWS or
  production Spark jobs.
