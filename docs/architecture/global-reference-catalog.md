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
The current ontology registry is `1.2.0`
(`sha256:8f82f8998ac3b915340860b6b08d86d053b1257c72a9f49d6d7b6c19d944e584`)
and the predicate registry is `1.2.0`
(`sha256:d8942df842d6bb3947f65a6398f5635ccc53b2efd38b2fb970e8a5c53901c927`).
The Label-capable Gold resolution policy is `research-display-v2@2.5.0`
(`sha256:8b45e7b676f20a1b11e3a5c00b30cf8bed43c5a98dd6a0c39f33f9a42555a1cb`).

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

News and Learning use the existing `wikidata-json-dump` mapping from
`catalog-source-policy.v1.json`. Mapper support never infers permission from
technical capability; the MusicBrainz addition is an explicit source-policy v1
registry change with its own reviewed semantic digest.

News and Learning production profiles must set `identitySpine=true`,
`goldContribution=true`, and `materialization=false`. Identity and Gold consume
the pinned source runs, while the separate materialization flag remains off.

## Music reference slice

Music adds `MUSIC_ARTIST`, `MUSIC_LABEL`, `MUSICAL_WORK`,
`MUSIC_RECORDING`, `MUSIC_RELEASE_GROUP`, and `MUSIC_RELEASE` under the existing
`AGENT` and `WORK` roots. Artist and Label both use `AGENT`. Exact identifiers
cover the six MusicBrainz MBID namespaces, ISRC, ISWC, and Wikidata QID;
Wikidata `P966` crosswalks Label QIDs. Generic relationship assertions cover
`recording_of`, `release_membership`, `credit`, and `performance_link`; Label
credits reuse `credit` with `role=LABEL`, and no Music-only OpenSearch index
fields are introduced.

The slice requires two immutable captures in one release: MusicBrainz offline
core CC0 and Wikidata Music. MusicBrainz tags, genres, ratings, annotations,
supplementary/live feeds, and Cover Art Archive assets are rejected. Both
Temporal source profiles are Identity/Gold contributors with
`materialization=false`. One exact identifier may reuse an entity; multiple
exact candidates abstain, while parent-constrained recording/release matches
remain review-only.

## Data path

1. Capture pins a raw object by checksum and, for S3, ETag and ObjectVersion.
2. Silver emits versioned field, identifier, relationship, and entity-type
   assertions with citation keys and source snapshot provenance.
3. Identity accepts one exact candidate, allocates a global reference entity
   when none exists, and abstains on ambiguous exact candidates.
4. Gold retains current candidates, conflicts, citations, immutable source
   snapshots, and complete double-temporal observations.
5. OpenSearch projection v10 contributes bounded, searchable reference
   candidates and relations. News/Learning use `wikidata-json-dump`; Music uses
   `musicbrainz-core-dump` plus `wikidata-json-dump`. Every source still
   requires `GOLD_PROJECTION`. Overflow is explicit; Gold remains complete.

The pinned News, Learning, and Music fixtures under `tests/fixtures/research`
contain small reviewed selections of real Wikidata and MusicBrainz identifiers.
Their NDJSON sidecars are checksum- and size-bound and exercise the production
mappers without a runtime public API dependency.

## Release and World boundary

`contracts/catalog/global-reference-release.v1.schema.json` and
`reference_release.py` define a content-addressed immutable release. A release
must bind:

- ontology and predicate registry coordinates and digests;
- the unchanged source-policy v1 coordinate and digest;
- immutable source snapshot pins;
- exactly one News pin, one Learning pin, and both MusicBrainz/Wikidata Music
  pins for the Music slice;
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
