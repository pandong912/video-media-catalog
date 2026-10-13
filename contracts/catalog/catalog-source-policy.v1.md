# Catalog Match source policy v1

## Purpose

`catalog-source-policy.v1.json` is the portable canonical authorization
artifact shared with Platform and Compute. Its normalized JSON semantic digest
is:

`sha256:4da143ac61dd11a9aa59bd567b36af47fc57d9b4c0cb4f0cc09b1696a1a11277`

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

- Wikidata is enabled for retrieval, display, Identity and materialization.
- EIDR is enabled for retrieval, display, Identity and materialization.
- IMDb and TVmaze are pending and grant retrieval/ranking only.
- TMDB is excluded and grants retrieval/ranking only.

Therefore only local `wikidata-item` and `eidr-content` namespaces can drive
exact Identity blocking or Gold identifier projection. IMDb, TVmaze and TMDB
connectors may be captured and freshness-tracked, but their runs cannot be
selected for Identity or Gold. Europeana, Douban, alternate EIDR IDs and other
local integrations have no canonical v1 mapping and fail closed.

## Fail-closed rules

A pending or excluded source cannot grant canonical `IDENTITY` or
`MATERIALIZATION`. Namespace ownership must agree with its canonical source.
The explicit implementation mapping is:

- `wikidata-json-dump` → `wikidata`
- `eidr-public-registry` → `eidr`
- `imdb-non-commercial-datasets` → `imdb`
- `tvmaze-public-api` → `tvmaze`
- `tmdb-research` → `tmdb`

The loader validates this mapping, local namespace ownership and rights-policy
bindings against `SourceRegistrySnapshot`. Production Identity and Gold
entrypoints pin `sourcePolicyDigest`, filter exact-blocking namespaces through
the live intersection, and reject selected source runs that are not authorized
for Identity/materialization. Gold also rejects an Identity resolution run
whose input manifest does not bind the current canonical digest. Temporal
`sourceRunStates` may still track retrieval-only runs and their freshness, but
those runs are excluded from snapshot, Identity and Gold inputs.

This policy never grants a license. Existing rights profiles, termination
fences, audience/purpose/territory checks, attribution, and Gold quality gates
remain independently mandatory.

## Change management

Capability or lifecycle changes require an updated portable canonical artifact,
reviewed semantic digest tests, and a new release/build identity. Enabling a
pending source also requires its rights and operational evidence; changing a
Catalog implementation mapping must not be used to bypass those gates.
