# Community-first media catalog v2

## Status

This document defines the v2 supplier-neutral source strategy and its unified
owner-only research serving context. It does not change the published
Wikidata/EIDR v1 contracts.
V1 remains the compatibility source for existing entity keys, snapshots,
OpenSearch indexes, and API responses.

The v2 goal is one supplier-neutral processing platform and one
`research` Gold/serving output. Sources share code, Silver, identity,
Gold, and the research index while retaining source-level policy, attribution,
expiry, and removal duties.

The current implementation includes the source/rights/connector/identity
contracts, Wikidata/EIDR adapters, TVmaze full/delta capture, IMDb TSV and TMDB
capture/mapping, and the run-fenced Silver Iceberg tables defined in
[`contracts/parquet/community_catalog_silver.v2.md`](../../contracts/parquet/community_catalog_silver.v2.md).
It also defines the deterministic Gold identity/rights/field-resolution
semantics, quality report, attribution binding, and release-fenced Gold tables
in
[`contracts/parquet/community_catalog_gold.v2.md`](../../contracts/parquet/community_catalog_gold.v2.md).
The distributed identity stage and Silver-to-Gold transform now consume exact
snapshot sets and committed run IDs without driver collection. A bounded,
strict-mapping OpenSearch projection publishes only to
`media-catalog-research-read`. Owner-only `/api/v2/research` routes require
`governance.read`, exact configured OIDC subject equality, and pagination
cursors bound to one concrete immutable index; v1 routes and alias remain
unchanged.

## Source portfolio

The initial source portfolio is split by effective rights rather than by
transport or provider:

- `open_cc0`: Wikidata structured data, Europeana metadata, DPLA metadata, and
  MusicBrainz core data.
- `open_attributed`: Japan Media Arts Database, BnF descriptive metadata, and
  individually approved national datasets.
- `open_sharealike`: TVmaze, the reusable fields from Bangumi Archive, and
  optional Wikipedia text.
- `public_registry`: EIDR records. EIDR is an identifier registry, not a rich
  metadata provider.
- `research_private`: IMDb non-commercial datasets, TMDB developer data, and
  project-licensed TheTVDB data.
- `federated_ephemeral`: user-triggered MAL, AniDB, AniList, YouTube, Vimeo, or
  platform-partner requests whose terms do not permit a permanent mirror.

Unknown or disputed rights always route to quarantine. Export, public serving,
and training paths fail closed.

## Shared architecture

```text
source product
  -> immutable raw capture
  -> connector batch manifest
  -> source-native record envelopes
  -> typed assertions and citations
  -> identity evidence and decisions
  -> owner-only research Gold release
  -> bounded OpenSearch projection / analytical Iceberg views
```

Network acquisition is separate from deterministic transformation. API
credentials are available only to acquisition workloads. Spark consumes
immutable object references and never calls source APIs.

## Source registry

The registry distinguishes concepts that v1 currently compresses into one
`source` string:

- source system: the operating organization or registry;
- source product: one API, dump, archive, or contracted feed;
- source namespace: one identifier namespace and referent scope;
- dataset release: one complete or partial source publication;
- ingest run: one processing attempt over a pinned release;
- schema contract: native schema and compatibility policy;
- rights profile: machine-enforced permitted actions and retention duties.

Provider renames, acquisitions, product migrations, and contract changes do not
alter internal entity keys.

## Connector contract

Every connector emits a batch manifest and record envelopes.

The batch manifest declares:

- source product, connector, code/config/policy digests;
- dump, API, or feed transport and source serialization;
- full, delta, or leased change semantics;
- complete or partial coverage scope;
- source window, watermark, and delete coverage;
- immutable raw object references;
- acquisition time, replay deadline, row and error counts;
- retry and rate-limit observations.

Each record envelope declares:

- source record ID and optional source revision;
- `UPSERT`, `DELETE`, `RETRACT`, `EXPIRE`, or inferred absence;
- source, observation, validity, ingestion, and expiry time;
- payload schema, canonical payload hash, and raw object location;
- JSON Pointer, XML path, or RDF statement location;
- policy ID/digest and citation keys.

Missing records imply deletion only between two complete releases with the same
coverage scope. Partial feeds and failed pagination never produce tombstones.

## Assertions and identity

Source records are not canonical entities. Mappers emit typed field,
identifier, relationship, and entity-type assertions. Assertions are keyed by
source record version, source field path, value, and qualifiers—not by the
current canonical entity key—so a later split does not rewrite source history.

Identity processing stores:

1. source entity nodes;
2. identity evidence;
3. `ACCEPT`, `REJECT`, `UNCERTAIN`, or `REVOKE` decisions;
4. effective entity memberships;
5. merge/split events and permanent redirects.

Exact identifiers are evidence only when issuer, namespace, referent kind, and
entity level are compatible. Titles, years, runtimes, and cast similarities may
create review candidates but never permanent automatic merges.

All published v1 entity keys are imported verbatim into the v2 ledger. New
entity keys are allocated from an internal allocation ID, not from a provider
identifier. Merges keep the earliest published survivor and redirect every old
key. Splits preserve history and require an explicit decision.

## Audiovisual domain boundaries

- `EDITORIAL_WORK`: movie, episode, programme, anime, variety-show unit, short,
  trailer, clip, or original online video.
- `SERIES`: continuing editorial work that organizes episodes.
- `SEASON` or `COLLECTION`: optional editorial grouping. Episode numbering is a
  qualified relation, not identity.
- `EDIT`: director, censorship, airline, broadcast, restored, or other content
  version.
- `MANIFESTATION` or `PACKAGE`: a technical/distribution embodiment such as a
  DCP, Blu-ray, localized package, or streaming package.
- `RELEASE_EVENT`, `BROADCAST_EVENT`, and `AVAILABILITY_OFFER`: territorial,
  temporal distribution facts.
- `ONLINE_PUBLICATION`: a platform upload, stream, VOD, or repost. A platform
  item is not automatically an editorial work.
- `AGENT`, `PLATFORM_ACCOUNT`, `REGULATORY_RECORD`, `METRIC_OBSERVATION`, and
  `MEDIA_ASSET` remain separate domains.

Schema.org is an output mapping. EBUCorePlus and MovieLabs MDDF are semantic and
distribution crosswalks. None of them is copied wholesale into physical tables.

## Unified research Gold

V2 publishes one owner-only `research` Gold. It does not build parallel
variants or a runtime mode switch. Open, public-registry, and
registered research-private assertions can coexist only after their individual
rights profiles permit research storage, transformation, display, and
search.

Resolution first evaluates rights eligibility, then entity level, locale,
territory and valid time, then field-specific authority, evidence, precision,
freshness, and deterministic tie-breaks. There is no global provider priority.
Each selected value retains its winning assertion and resolution trace.

## Infrastructure reuse and removal

The research catalog reuses the existing 100k baseline S3, Glue, EMR, IAM, and
OpenSearch infrastructure. It creates no separate warehouse, role, cluster, or
domain. Existing `video_media_catalog` Glue namespace tables, release commits,
owner checks, and the fixed `media-catalog-research-*` index family provide
logical boundaries. Policy metadata remains mandatory for expiry, attribution,
and removal.

Source removal:

1. stops acquisition and revokes credentials;
2. installs a rights fence that immediately blocks serving and export;
3. expires source assertions, assets, and identity evidence;
4. recomputes affected identities and Gold releases;
5. publishes replacement indexes;
6. removes raw objects, all S3 versions, Iceberg snapshots/orphans, old indexes,
   backups, CDN caches, and temporary data where the policy requires purge;
7. publishes a removal receipt containing scope, counts, deadline, and residual
   checks.

## Release isolation

V1 writes shared tables sequentially and then captures their latest snapshots.
V2 must not publish a snapshot accidentally advanced by another or failed run.

Every row carries `ingest_run_id`; only committed runs are eligible. Gold writes
to a run-specific candidate snapshot or WAP branch and validates the exact
candidate. A release manifest pins committed input runs, source watermarks,
Silver snapshots, identity/field/rights policy digests, exact Gold snapshots,
quality reports, and separate affected/total row counts.

## Initial delivery order

1. Publish the v2 source, rights, connector, assertion, identity, and release
   contracts while freezing v1.
2. Wrap the existing Wikidata and EIDR paths as v2-conformant adapters without
   changing their current output.
3. Add TVmaze as the first community adapter because it offers a documented
   full index, update indexes, stable IDs, and CC BY-SA API terms.
4. Add MADB and Bangumi only after their field-level policy maps are reviewed.
5. Build the single owner-only research Gold and research index.
6. Run a representative scale test before replacing the current small
   OpenSearch development domain.

## Non-goals for this slice

- no IMDb/TMDB webpage scraping or redistribution beyond source terms;
- no public serving or redistribution of the owner-only research catalog;
- no automatic fuzzy entity merge;
- no assumption that a metadata license also clears images or video;
- no full-platform YouTube, anime-site, or streaming-provider crawl;
- no v1 table, key, algorithm, or API behavior change.
