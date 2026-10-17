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
- pre-archived MusicBrainz core CC0 dump capture for the Music reference slice.

Catalog Match activation is a separate versioned decision in
`contracts/catalog/catalog-source-policy.v1.json`, the same portable canonical
artifact consumed by Platform and Compute. Implemented connector or namespace
support does not imply live Identity/Gold enablement: Catalog maps its local
products to canonical source IDs, then intersects technical support with the
canonical grants. The nine explicitly mapped sources, including MusicBrainz
core, may drive only the capabilities granted by policy v1. The canonical policy digest is
pinned by Golden Identity, Gold release, release commit, and search documents;
there is no implementation-specific authorization digest. Unknown,
conflicting, pending or excluded mappings fail closed, and rights checks remain
an independent mandatory gate.

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

For personal research, `extract-wikidata-p2704` and `lookup-public-research`
bind a `wikidata-p2704-public-research` discovery scope into the manifest
identity, filter committed `wikidata-json-dump` P2704 assertions only, enforce
low-rate lookup bounds, and return `sourceSilverInputs` for the same Source
Silver fan-out. Operational and licensing detail lives in
`docs/sources/eidr-public-research.md`.

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
- strict mapping with projectionVersion `12`;
- mapping digest
  `sha256:f7b6e0b42f1daee66aa522faee88f8912f489410a53cc5d9ca856e88d93e416a`;
- deterministic build identity bound to one Gold release commit;
- partition receipts, count reconciliation, and independent validation;
  alias promotion remains a separate explicitly authorized operation.

Projection v12 retains nullable, scalar `parentEntityKey`, `seriesEntityKey`, and
`seasonEntityKey`. It also adds `domains`; bounded Sports competition, season,
stage, participant, venue and current-result fields; bounded Animation profile,
studio and character fields; bounded `observationHistory`; and versioned
CanonicalSubject roots/type concepts with bounded News/Learning/Music reference
candidates and relations. Music additionally exposes concrete agent kind,
bounded media objects, exact eligibility, work parents, credits, and explicit
variant signals. Every bounded collection has an explicit overflow counter,
while complete revision history
remains in `community_gold_observation`. Hierarchy keys are derived only from
rights-filtered Gold relations.
Ambiguous, missing, malformed, or over-bound parent candidates produce null;
episode-to-season-to-series traversal requires a unique season and a unique
season-to-series edge. Every document also carries the pinned
`sourcePolicyDigest`. Serving provenance uses canonical source IDs in
`sourceProductId` fields. Each live external identifier also carries its
canonical source, assertion ID, and bounded citation keys; unknown namespaces
or source/namespace ownership conflicts are omitted from materialization
evidence.

The Global Reference release boundary is described in
`docs/architecture/global-reference-catalog.md`. It binds a validated concrete
index but deliberately excludes alias activation and every tenant World write.

The Temporal Golden input must carry normalized `sourceRunStates`. IMDb,
TVmaze, Wikidata, TMDB, and scheduled domain sources declare exact committed run
IDs, run status, freshness status, watermark, optional acquisition time, and
explicit Identity, Gold-contribution, and materialization intent. Identity and
materialization default false. The
preflight requires the states to cover exactly the pinned source runs and
rejects stale, pending, failed, excluded, conflicting, or policy-disabled
materialization. Reference-only News/Learning/Music profiles set
`identitySpine=true`, `goldContribution=true`, and `materialization=false`;
Music uses both MusicBrainz core and Wikidata pinned runs. Legacy payloads remain deserializable, but new
preflight fails closed when explicit states are absent; downstream Identity and
Gold entrypoints independently reject disallowed ingest-run products. IMDb and
TVmaze override evidence remains explicit and never grants a capability by
itself; source-policy authorization is still required.

Full rebuild is authoritative. The optional affected-entity path copies an
immutable base into a new concrete index and never mutates the old index. It
requires the base and target to share `sourcePolicyDigest`; a source-policy
change forces a full rebuild. Whole-index validation checks both release and
source-policy provenance before publication.

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
