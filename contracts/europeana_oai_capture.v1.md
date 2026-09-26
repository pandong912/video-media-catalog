# Europeana OAI capture contract v1

## Scope

The connector reads only `ListRecords` from
`https://api.europeana.eu/oai/record/` with `metadataPrefix=edm`. Initial
requests include the explicit `from` and `until` window and an optional
`set`. Resume requests contain only `verb=ListRecords` and the exact opaque
`resumptionToken`, as required by OAI-PMH.

Redirects, alternate hosts, alternate paths, URL credentials, fragments,
cookies, API keys, bearer tokens, and `wskey` parameters are rejected.

## Capture v2

Every batch declares:

- source system `europeana`;
- source product `europeana-oai-edm`;
- connector `europeana-oai-pmh`;
- source namespace `europeana-record`;
- `FEED` transport and `XML` serialization;
- `DELTA`, `PARTIAL`, and delete coverage `NONE`;
- metadata policy `europeana-metadata-cc0`;
- media binary acquisition `DISABLED`.

The raw objects are complete OAI response pages. Each normalized UPSERT
envelope binds one raw page and uses the OAI datestamp as both source revision
and source modification time. The source record ID is the normalized official
path `/dataset/local-id`.

The connector publishes raw pages, then the batch manifest, then bounded
NDJSON envelope shards, then the record-set manifest. It finally publishes a
generic immutable SourceWatermark and a verified capture-window receipt.

## Resume and terminal behavior

The SourceWatermark binds the product, window, configuration digest, image
digest, and metadata policy digest. Its `cursor` is the next opaque OAI
resumption token. Its `watermark` is the greatest observed OAI datestamp, or
the source response date for an empty window.

A watermark with no cursor is terminal and cannot be supplied as a resume
input. A resumed run must use the same window, configuration, image, and
policy. Repeated or oversized tokens fail closed.

`noRecordsMatch` commits an empty record set and terminal watermark.
`badResumptionToken` is a hard checkpoint error. Other OAI protocol errors are
hard failures.

## Normalized record payload

The payload schema is `europeana-edm-record-v1`. It contains:

- `id`, `oaiIdentifier`, `datestamp`, and `setSpecs`;
- `titles`, `descriptions`, `times`, `languages`, `countries`, and `types`;
- `providers`, `dataProviders`, `creators`, and `contributors`;
- `recordUrls`, `landingUrls`, `previewUrls`, and `mediaUrls`;
- source `identifiers` and validated `externalIds`;
- metadata-only `metadataRights`;
- separately scoped `digitalObjectRights`;
- `binaryAcquisition=DISABLED`.

`digitalObjectRights.status` is always present. It is `DECLARED` when at least
one `edm:rights` or `dc:rights` value exists and
`MISSING_ASSUME_COPYRIGHT` otherwise. The structure retains all discovered
`edmRights`, `dcRights`, RightsStatements URIs, and Creative Commons
license/tool URIs without broadening their grants. Each preview/media URL also
has a `referencedResources` entry with its effective rights, status, role, and
whether those rights came from the exact WebResource or the record-level
aggregation.

## Bounds

Defaults are five pages, 100 total records, 16 MiB per page, 80 MiB total raw
bytes, 1 MiB per normalized record, 8 MiB per envelope shard, a 30-second
request timeout, five attempts, and a 250 ms minimum request interval.

The implementation also enforces hard maxima. It never truncates an OAI page:
if a page would cross the total-record bound, the run fails before committing
that page so a resume cannot silently skip records.
