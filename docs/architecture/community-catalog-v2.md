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

The bounded `CommunitySilverSnapshotSet` remains schema `2.0`. Long-running
histories use `CommunitySilverEpochManifest` schema `3.0`, carrying bounded
deltas and distributed committed-run count/digest rather than a driver-sized
history list.

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
- Source removal is dry-run-first and does not delete data unless a separate
  explicitly confirmed execution is invoked.
- Unit and Spark tests use local fixtures; CI does not run real AWS or
  production Spark jobs.
