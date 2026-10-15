# Global Research Catalog reference boundary

## Decision

Media Catalog owns a global, tenant-neutral reference ontology. It does not own
tenant World identity or publication authority.

The ontology contract is
`contracts/catalog/global-reference-ontology.v1.json`. Its seven exhaustive
`CanonicalSubject` roots are `AGENT`, `PLACE`, `PHYSICAL_ENTITY`, `EVENT`,
`WORK`, `CONCEPT`, and `FICTIONAL_ENTITY`. Concrete type concepts and
predicates are independently versioned. The registry declares
`authority=GLOBAL_REFERENCE_ONLY` and
`tenantWorldIdentityAuthority=false`.

This extends the existing Sports and Animation Capture → Silver → Identity →
Gold pipeline; it does not create a parallel authority or a tenant canonical
store.

## News/Journalism reference slice

Supported reference types are:

- news program, edition, and story;
- reported event and information source;
- claim and correction.

Supported relations include edition/story containment, reported event/source,
claim creation, and correction of a claim or story. Correction assertions keep
both processing time (`observedAt`) and valid time (`validFrom`/`validTo`);
`supersedesAssertionId` closes the prior candidate without deleting its Gold
observation. Equal-rank conflicting values produce a conflict and no selected
fact.

## Learning/Instruction reference slice

Supported reference types are:

- course, module, and lesson;
- learning concept and objective;
- prerequisite, procedure step, and demonstration.

Relations preserve course/module/lesson structure, taught concepts,
objectives, prerequisites, ordered procedure steps, and demonstrations.

Both slices use only the existing `wikidata-json-dump` mapping from
`catalog-source-policy.v1.json`. The policy file, registry version, semantic
digest, and commercial authorization are unchanged. Adding another source
requires a separately reviewed source-policy change; this implementation does
not infer permission from mapper support.

News and Learning production profiles must set `identitySpine=true`,
`goldContribution=true`, and `materialization=false`. Identity and Gold consume
the pinned source runs, while the separate materialization flag remains off.

## Data path

1. Capture pins a raw object by checksum and, for S3, ETag and ObjectVersion.
2. Silver emits versioned field, identifier, relationship, and entity-type
   assertions with citation keys and source snapshot provenance.
3. Identity accepts one exact candidate, allocates a global reference entity
   when none exists, and abstains on ambiguous exact candidates.
4. Gold retains current candidates, conflicts, citations, immutable source
   snapshots, and complete double-temporal observations.
5. OpenSearch projection v10 contributes bounded, searchable reference
   candidates and relations. News/Learning search contribution is restricted to
   `wikidata-json-dump` with `GOLD_PROJECTION`. Overflow is explicit; Gold
   remains complete.

The pinned News and Learning fixtures under `tests/fixtures/research` contain
small reviewed selections of real Wikidata entity identifiers. Their NDJSON
sidecars are checksum- and size-bound and exercise the production mapper.

## Release and World boundary

`contracts/catalog/global-reference-release.v1.schema.json` and
`reference_release.py` define a content-addressed immutable release. A release
must bind:

- ontology and predicate registry coordinates and digests;
- the unchanged source-policy v1 coordinate and digest;
- immutable source snapshot pins;
- one content-bound Gold release plan and Gold release commit;
- one concrete OpenSearch index;
- its content-bound immutable build manifest and independent validation
  receipt.

Capabilities are exactly `RESEARCH`, `DISPLAY`, and `IDENTITY`. The release
cannot carry `WORLD MATERIALIZATION` or `PUBLICATION`; it cannot carry an alias
activation receipt. Tenant canonical writes, World materializations, World
publications, and alias activations are all attested as zero.

Platform integration therefore consumes a release as candidate evidence only.
Platform must pin the release ID, ontology and predicate digests,
source-policy digest, Gold plan/commit `ObjectRef`s, source snapshot pin IDs,
concrete index, build-manifest `ObjectRef`, and validation-receipt `ObjectRef`.
Any tenant acceptance or World publication remains a separate Platform-owned
decision and transaction.
