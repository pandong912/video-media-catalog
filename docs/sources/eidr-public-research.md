# EIDR public research subset

## Scope

This path builds a lawful, bounded research subset; it is not an EIDR Registry
mirror.

1. The official Wikidata dump is normalized into Source Silver.
2. Wikidata property P2704 becomes an `eidr-content` identifier assertion.
3. `extract-wikidata-p2704` reads one immutable Silver snapshot and accepts only:
   - committed `wikidata-json-dump` runs;
   - `wikidata-item` subjects;
   - active `eidr-content` assertions whose provenance path is `/claims/P2704/*`.
4. `lookup-public-research` resolves a small number of those already-known IDs
   through the fixed anonymous EIDR object endpoint.
5. Successful XML responses become ordinary partial Connector captures. Existing
   Source Silver can retain them beside IMDb, TMDB, TVmaze, and Wikidata.
   Identity, Gold, and OpenSearch consume only the sources authorized by the
   canonical Catalog policy; currently that means EIDR and Wikidata, while the
   other three remain retrieval/ranking-only.

The discovery scope is part of the manifest identity. An unscoped or non-Wikidata
manifest cannot be used with the public-research command.

## Runtime guardrails

- exact identifiers only; no title search, ID enumeration, or link traversal;
- HTTPS host, path, query, redirect, response type, and response size are fixed;
- anonymous access only; no EIDR credentials are accepted;
- one in-process request stream, at least one second between requests;
- default 25 IDs per batch and 250 IDs per run;
- hard limit 50 IDs per batch, 1,000 IDs and two hours per run;
- immutable authorization evidence, raw XML, receipts, retries, and watermarks;
- every capture is `PARTIAL` / `DELTA` / `NONE` for delete coverage;
- no schedule is created by this profile.

Repeated execution must remain an explicit research decision. Do not use
watermark continuation to approximate a complete Registry crawl. Obtain an EIDR
Participant feed agreement before building complete or continuously refreshed
coverage.

## GitOps one-shot Temporal handoff

The deployed TMDB Temporal worker also registers
`EidrPublicResearchWorkflow`, `DiscoverEidrPublicResearchIds`,
`LookupEidrPublicResearch`, and `WriteEidrPublicResearchSummary`. GitOps must
roll out that worker code and mount a reviewed JSON config, but it must not
create a Temporal Schedule for this workflow.

Start a reviewed run explicitly:

```bash
video-media-catalog-tmdb-temporal-admin \
  start-eidr-public-research \
  --config-file /config/eidr-public-research.json
```

The config pins:

- `silverSnapshot` with SHA-256, size, ETag, and VersionId;
- `sourceReleaseId`;
- `authorizationEvidence` with SHA-256, size, ETag, and VersionId, plus
  `authorizationIssuedAt`;
- `acquiredAt` and `emrImageDigest`;
- optional `maxIds`, `maxBatches`, `batchSize`, `maxDurationSeconds`,
  `minimumRequestIntervalSeconds`, and `discoveryPageSize`;
- `env`, using the existing `PipelineEnv` keys. In particular,
  `destinationPrefix` and `imageDigest` identify the ordinary lookup capture;
  the Glue warehouse/catalog, EMR application/role/log URI, Source Silver
  entrypoint, staging/checkpoint prefixes, summary prefix, and AWS region are
  all required.

All prefixes and pinned inputs for this production workflow are S3. The
discovery activity submits the fixed packaged entrypoint
`local:///opt/video-media-catalog/src/video_media_catalog/eidr_backfill_cli.py`
with `extract-wikidata-p2704`. The lookup activity then invokes
`lookup-public-research` in the ordinary worker. The workflow does not accept a
watermark and performs no continuation.

The admin command derives its workflow ID from the complete immutable config
and starts it with Temporal `REJECT_DUPLICATE`. Repeating the same command
therefore reports the existing execution instead of issuing another public
lookup. Changing the pinned config is an explicit new research decision.

## Handoff to the catalog

`lookup-public-research` returns `sourceSilverInputs`, one immutable
`batchManifest` / `recordSetManifest` pair per non-empty lookup batch. Submit
those pairs sequentially to `video-media-catalog-community-spark`; the Temporal
workflow does this through `SubmitSourceSilver` on the existing single-writer
queue.

After the final Source Silver commit, the workflow conditionally publishes an
immutable JSON summary below
`env.pipelineSummaryPrefix/eidr-public-research/`. The workflow result and
summary both expose `supplementalSourceRunIds`; copy that exact list into the
next `GoldenBuildInput.supplementalSourceRunIds`. Empty lookup batches produce
no Source Silver job and no supplemental run ID.

After Source Silver commits:

1. publish a new research Silver snapshot containing the new EIDR run IDs;
2. add those run IDs to the next pinned Golden Build's
   `supplementalSourceRunIds`; the workflow includes them in both the Silver
   snapshot and incremental Identity input. Remove any older `identityReuse`
   receipt unless it binds the same TMDB plus supplemental run set;
3. run the existing Identity, Gold, and OpenSearch publication path. The
   runtime source policy must still enable the source and canonical namespace;
   a pending/failed capture state cannot be materialized.

EIDR remains optional in the Gold freshness policy because public exact
resolution is not a complete feed. A release may attribute resolved records to
`eidr-public-registry`, but must never claim complete EIDR coverage.

## Rights

Wikidata structured data is handled under CC0. EIDR resolver responses retain
the separate `eidr-public-registry` rights profile and must not be repackaged as
a proprietary standalone Registry. IMDb and TMDB data keep their own independent
terms when the catalog merges cross-identifiers.
