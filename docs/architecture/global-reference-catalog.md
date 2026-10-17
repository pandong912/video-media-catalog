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
The current ontology registry is `1.4.0`
(`sha256:6f605eea9acfe9365ada11c45e350a29851f11f613a45f07cc0897ecee000b90`)
and the predicate registry is `1.4.0`
(`sha256:c803567e0e0b9ba1b2f1cd4daa6750792c0bc1fc88cca8e2bcffd4216b916588`).
The Music Pilot-capable Gold resolution policy is `research-display-v2@2.7.0`
(`sha256:3e1ba89f63bc3d9eaaaddb6d3acc307c6db45c116c82a8445c8ea07d3641c590`).

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
`AGENT` and `WORK` roots. Artist and Label both use `AGENT`;
`music_agent_kind` preserves the authority-backed concrete `PERSON` or
`ORGANIZATION` kind. Exact identifiers
cover the six MusicBrainz MBID namespaces, ISRC, ISWC, and Wikidata QID;
Wikidata `P966` crosswalks Label QIDs. Generic relationship assertions cover
`recording_of`, `release_membership`, `credit`, and `performance_link`; Label
credits reuse `credit` with `role=LABEL`. `media_variant` preserves explicit
MusicBrainz variant relations, while Wikidata `P51` uses the separate
`media_object` predicate. OpenSearch projection v12 derives `musicAgentKind`,
`musicExactEligible`, `musicMediaObjects`, `musicWorkParentEntityKeys`,
`musicCredits`, and `musicVariantSignals` from those generic Gold assertions.

The slice requires two immutable captures in one release: MusicBrainz offline
core CC0 and Wikidata Music. MusicBrainz tags, genres, ratings, annotations,
supplementary/live feeds, and Cover Art Archive assets are rejected. Both
Temporal source profiles are Identity/Gold contributors with
`materialization=false`. One exact identifier may reuse an entity only when the
validated profile disposition and index eligibility both permit it. Multiple
exact candidates and missing coordinates abstain. Live assets are constrained
to reviewed `MUSICAL_WORK` parents and cannot collapse to a studio recording;
remix/media variants abstain.

The Compute-owned algorithm fixture still carries the earlier variant-review
contract. Its cross-repository digest is deliberately unchanged here; Compute
must freeze and publish the matching abstention contract before runtime
parameter publication.

## Data path

1. Capture pins a raw object by checksum and, for S3, ETag and ObjectVersion.
2. Silver emits versioned field, identifier, relationship, and entity-type
   assertions with citation keys and source snapshot provenance.
3. Identity accepts one exact candidate, allocates a global reference entity
   when none exists, and abstains on ambiguous exact candidates.
4. Gold retains current candidates, conflicts, citations, immutable source
   snapshots, and complete double-temporal observations.
5. OpenSearch projection v12 contributes bounded, searchable reference
   candidates and relations. News/Learning use `wikidata-json-dump`; Music uses
   `musicbrainz-core-dump` plus `wikidata-json-dump`. Every source still
   requires `GOLD_PROJECTION`. Overflow is explicit; Gold remains complete.

The pinned News, Learning, and Music fixtures under `tests/fixtures/research`
contain reviewed selections of real Wikidata and MusicBrainz identifiers. The
20-asset Music Pilot freezes 8 exact recordings, 8 live work reviews, 1 real
shared-media ambiguity, 1 remix abstention, and 2 no-authority abstentions. It
retains API capture receipts and explicitly marks the
three work coordinates whose cross-authority equivalence was approved by human
review rather than directly asserted by Wikidata. NDJSON sidecars are checksum-
and size-bound and exercise the production mappers without a runtime public API
dependency.

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
