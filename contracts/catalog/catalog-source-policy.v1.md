# Catalog Match source policy v1

## Purpose

`catalog-source-policy.v1.json` is the portable canonical authorization
artifact shared with Platform and Compute. Its normalized JSON semantic digest
is:

`sha256:27749e3d25228edbcf5341873fd89b96c1ad8e8b2fcc806c6ae9256e17be5512`

The schema remains
`video.world.catalog-source-policy-registry.v1`; registry content version is
`1.3.0`.

Source entries define canonical `sourceId`, lifecycle status and granted
capabilities. Namespace entries define canonical namespace ownership and
aliases. This artifact is the only source-policy authorization digest.

Catalog-specific `sourceProductId`, capture/freshness support, Gold projection
support and local namespace IDs live in a separate implementation mapping.
Those facts describe technical support only. Runtime permissions are always
the intersection of that mapping and this canonical policy; the implementation
mapping never creates a second authorization policy or digest.

The digest is computed over the validated normalized JSON document. Object-key
order and formatting do not change it. Duplicate JSON keys, source IDs,
namespace IDs, aliases, unknown mappings and conflicting ownership fail closed.

## Current enablement

- Wikidata, MusicBrainz, EIDR, IMDb, TVmaze, TMDB, OpenFootball, StatsBomb
  and MADB are enabled for retrieval, display, Identity and selective
  materialization.
- MusicBrainz authorization is limited to immutable offline core database
  dumps under CC0. User tags, genres, ratings, annotations, supplementary or
  live feeds, and Cover Art Archive assets are excluded. The implementation
  accepts only the `musicbrainz-core-dump` product and rejects those fields.
- Their canonical title namespaces can all drive exact Identity blocking and
  Gold identifier projection for the personal-research catalog.
- Research Gold requires canonical `IDENTITY + DISPLAY`. `MATERIALIZATION` is
  independently enforced for the stable identity namespaces and predicates
  listed by each source; sports results, in-game statistics, Animation
  profiles, characters and credits are not materializable predicates.

Europeana, Douban, alternate EIDR IDs and other local integrations have no
canonical v1 mapping and fail closed.

## Fail-closed rules

A pending or excluded source cannot grant canonical `IDENTITY` or
`MATERIALIZATION`. Namespace ownership must agree with its canonical source.
The explicit implementation mapping is:

- `wikidata-json-dump` → `wikidata`
- `musicbrainz-core-dump` → `musicbrainz`
- `eidr-public-registry` → `eidr`
- `imdb-non-commercial-datasets` → `imdb`
- `tvmaze-public-api` → `tvmaze`
- `tmdb-research` → `tmdb`
- `openfootball-git` → `openfootball`
- `statsbomb-open-data` → `statsbomb`
- `madb-public-data` → `madb`

The loader validates this mapping, local namespace ownership and rights-policy
bindings against `SourceRegistrySnapshot`. Production Identity and Gold
entrypoints pin `sourcePolicyDigest`, filter exact-blocking namespaces through
the live intersection, and reject selected source runs that are not authorized
for Identity/materialization. Gold also rejects an Identity resolution run
whose input manifest does not bind the current canonical digest. Temporal
`sourceRunStates` are mandatory for new Golden preflights. All nine canonical
sources are eligible; each build selects and pins its exact committed source
runs for snapshot, Identity and Gold inputs.

Gold serving documents retain the compatibility field name `sourceProductId`
but write the canonical `sourceId` value. External identifiers additionally
bind one assertion ID and bounded citation keys, and are emitted only when
assertion source and canonical namespace ownership agree. Materialization
evidence is intersected with each namespace's `materializablePredicates`;
identity titles, aliases and hierarchy from all nine canonical sources are
permitted.

This policy never grants a license. Existing rights profiles, termination
fences, audience/purpose/territory checks, attribution, and Gold quality gates
remain independently mandatory.

## Change management

Capability or lifecycle changes require an updated portable canonical artifact,
reviewed semantic digest tests, and a new release/build identity. Existing
source-specific rights profiles remain mandatory; changing a Catalog
implementation mapping must not be used to bypass those gates.
