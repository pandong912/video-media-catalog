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
   Source Silver, Identity, Gold, and OpenSearch stages merge them with IMDb,
   TMDB, TVmaze, and Wikidata.

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

## Handoff to the catalog

`lookup-public-research` returns `sourceSilverInputs`, one immutable
`batchManifest` / `recordSetManifest` pair per non-empty lookup batch. Submit
those pairs sequentially to `video-media-catalog-community-spark`; Iceberg
commits remain single-writer.

After Source Silver commits:

1. publish a new research Silver snapshot containing the new EIDR run IDs;
2. include those run IDs in the next pinned Golden Build;
3. run the existing Identity, Gold, and OpenSearch publication path.

EIDR remains optional in the Gold freshness policy because public exact
resolution is not a complete feed. A release may attribute resolved records to
`eidr-public-registry`, but must never claim complete EIDR coverage.

## Rights

Wikidata structured data is handled under CC0. EIDR resolver responses retain
the separate `eidr-public-registry` rights profile and must not be repackaged as
a proprietary standalone Registry. IMDb and TMDB data keep their own independent
terms when the catalog merges cross-identifiers.
