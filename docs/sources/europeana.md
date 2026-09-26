# Europeana official metadata integration

## Official access decision (verified 2026-09-26)

The first release uses only Europeana's official, keyless OAI-PMH service at
`https://api.europeana.eu/oai/record/`. It does not scrape Europeana web pages,
use a mirror, or download linked media.

Official sources:

- [Dataset download and OAI-PMH service](https://europeana.atlassian.net/wiki/spaces/EF/pages/2324463617/Dataset+download+and+OAI-PMH+service)
- [Accessing the APIs](https://europeana.atlassian.net/wiki/spaces/EF/pages/2462351393/Accessing+the+APIs)
- [Fair-use policy](https://europeana.atlassian.net/wiki/spaces/EF/pages/2704146433/Fair+use+policy+guidelines)
- [Europeana Terms of Use](https://www.europeana.eu/en/rights/terms-of-use)
- [Data Exchange Agreement](https://pro.europeana.eu/page/the-data-exchange-agreement)

The Search and Record APIs require an API key in 2026. `X-Api-Key` is the
preferred transport; the `wskey` query parameter is deprecated because URLs
leak keys. Personal-key limits were progressively reduced through April 2026,
while Project keys are intended for production services. No key is accepted by
this connector.

The official authentication documentation explicitly lists OAI-PMH, the
Thumbnail API, and SPARQL as exceptions to API-key authentication. OAI-PMH
provides EDM RDF/XML, date and dataset selection, and opaque resumption tokens.
The connector sends a single request at a time and applies a configurable wait
between requests. `429` and `5xx` responses honor `Retry-After` and bounded
backoff.

Search API support is deliberately out of scope. If it is added later, rows
must remain at or below the documented maximum of 100, cursor pagination must
start with `cursor=*`, and the next cursor must be URL-escaped. Any key must be
injected from an approved Secret/ExternalSecret through `X-Api-Key`; it must
never appear in Git, URLs, workflow parameters, or logs.

## Rights boundary

Europeana metadata is handled under CC0 according to the Data Exchange
Agreement. That policy applies only to metadata.

Every linked digital object, preview, thumbnail, audio file, or video file
keeps its record-level `edm:rights`, `dc:rights`, Creative Commons URI, and
RightsStatements URI. The mapper emits these as separate facts with
`appliesTo=DIGITAL_OBJECT_AND_PREVIEW`; it never converts them into the
metadata CC0 grant. If no object-rights value is present, the record carries
`MISSING_ASSUME_COPYRIGHT`, matching the Terms of Use's restrictive fallback.

The first release stores:

- normalized metadata and exact identifiers;
- the official Europeana record URL and provider landing URL;
- preview and media URLs as reference-only facts;
- object-level rights facts and the metadata-only CC0 fact.

It does not dereference, download, copy, embed, or redistribute media
binaries.

## Capture semantics

`video-media-catalog-europeana-oai` requires an explicit RFC 3339
`window-start` and `window-end`; an OAI dataset `set-spec` may narrow it
further. A run is always:

- `changeSemantics=DELTA`;
- `completeness=PARTIAL`;
- `deleteCoverage=NONE`;
- manual-only, with no Cron;
- bounded by page count, total record count, page bytes, total bytes, record
  bytes, request timeout, attempts, and shard bytes.

Europeana documents that its OAI service does not maintain a deleted-record
registry and recommends periodic complete collection re-harvests. Therefore,
even when a selected window reaches a terminal response, this integration does
not claim a complete Europeana snapshot and never infers deletion by absence.

Raw OAI pages, Capture v2 envelope shards, batch/record-set manifests, and
generic SourceWatermark/capture receipts are immutable. The next opaque
resumption token is stored only in the watermark object. A rejected
`badResumptionToken` fails closed; it never silently restarts from the
beginning. `noRecordsMatch` produces a valid immutable empty capture.

## Silver and identity mapping

The `europeana-record` namespace uses the official Record API ID shape
`/dataset/local-id`. The mapper emits titles and alternate titles,
descriptions, temporal values, languages, countries, types, providers, data
providers, creators, contributors, record/landing/preview/media URLs, object
rights, and generic source identifiers.

Identity blocking uses only:

- the exact Europeana record ID;
- explicit, syntax-valid EIDR IDs;
- explicit, syntax-valid IMDb title/name/company IDs.

Generic local identifiers remain facts and do not become blocking keys. No
title similarity or fuzzy automatic merge is introduced.

Gold places `europeana-oai-edm` after IMDb, TMDB, TVmaze, Wikidata, and EIDR
for core single-value fields. Europeana therefore enriches long-tail records
without replacing more authoritative core catalog values.

## 2026-09-26 manual canary

One keyless request was made to the official OAI endpoint for dataset set
`9200365`, bounded to one source page and 20 records. The linked media URLs
were not requested.

- Raw EDM XML: 203,376 bytes,
  `sha256:727049f1f2d56a720ec1d1e9e29c6776886928f1fb78cbbfdf58514cb9b6fb05`
- Parsed records: 20
- Next resumption token: present but not logged
- Capture semantics: `PARTIAL` / `DELTA`, terminal `false`
- Batch:
  `sha256:e3496686f8f909a1c0ae5d20f207616f964dbf9604d7331d69ce771504430b74`
- Record set:
  `sha256:e0838f5ad2777cda0401180fa5d13850a827e4f2384299e6acde0022354b6e1f`
- Source watermark:
  `sha256:50c974b090f2c55d767b2365105677ee12f50eefb4fd433941a9ebcc30cff6c2`
- Commit-last receipt:
  `sha256:e6c94c57b45fbe945923b86a4e6695ecd7ea5ef32ec1e190eef0a65969a264ef`

The macOS Python installation used for local verification did not trust the
machine's corporate/system CA chain, so its direct TLS attempt failed closed.
The official page was fetched with the OS-trusted `curl` client and then
processed through the production parser, bounds, immutable Capture v2
publisher, watermark, and receipt code. TLS verification was never disabled.
