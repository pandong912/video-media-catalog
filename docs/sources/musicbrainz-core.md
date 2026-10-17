# MusicBrainz core offline reference source

## Boundary

Catalog accepts only immutable MusicBrainz core database dumps under CC0 through
`musicbrainz-core-dump`. It does not call MusicBrainz web services at runtime
and does not ingest user tags, genres, ratings, annotations, supplementary or
live feeds, or Cover Art Archive assets.

The reviewed coordinates are:

- source system: `musicbrainz`
- source product: `musicbrainz-core-dump`
- connector: `musicbrainz-core-dump-snapshot`
- schema: `musicbrainz-core-entity`
- rights profile: `musicbrainz-core-data-cc0`
- dataset marker: `MUSICBRAINZ_CORE_CC0`

Every production profile must point to a pre-archived capture manifest with
SHA-256, size, ETag, and ObjectVersion. Source Silver also binds the resulting
`sourceSnapshotPinId`; mutable “latest” dump locations are not accepted.

## Music model

The mapper emits generic Silver title, identifier, relationship, and
entity-type assertions for:

- `MUSIC_ARTIST`
- `MUSIC_LABEL`
- `MUSICAL_WORK`
- `MUSIC_RECORDING`
- `MUSIC_RELEASE_GROUP`
- `MUSIC_RELEASE`

`MUSIC_ARTIST` and `MUSIC_LABEL` both map to the generic `AGENT` ontology root.
The `music_agent_kind` assertion preserves the authority-backed concrete
`PERSON` or `ORGANIZATION` kind instead of treating every artist as an
organization; labels resolve to `ORGANIZATION`.
Identifiers are the matching MusicBrainz MBID namespace plus ISRC, ISWC, and
reviewed Wikidata QID links. Wikidata property `P966` crosswalks a Label QID to
`musicbrainz.label` / `musicbrainz-label`. Relationships are `recording_of`,
`release_membership`, `credit`, and `performance_link`. `media_variant` retains
explicit MusicBrainz recording relations, while ordinary Wikidata `P51` media
objects use the separate `media_object` predicate. Gold continues to reuse the
generic assertions, while OpenSearch projection v12 derives bounded
`musicAgentKind`, `musicExactEligible`, `musicMediaObjects`,
`musicWorkParentEntityKeys`, `musicCredits`, and `musicVariantSignals` match
fields. Label participation in a
release or release group uses the existing
`credit` relation with `role=LABEL`, not a Label-specific predicate.
The Label-capable MusicBrainz and Wikidata Music mappers are versioned
`1.3.0`; Source Silver checkpoints bind MusicBrainz mapper `1.3.0` and the
Wikidata research dispatcher `2.2.0`.

## Release and matching

The Music reference slice requires both a MusicBrainz core pin and a Wikidata
Music pin. Both Temporal profiles use `identitySpine=true`,
`goldContribution=true`, and `materialization=false`; the monthly MusicBrainz
core profile includes Label records, so Label does not introduce a third source
or a Music-specific workflow. The capture uses the bounded 45-day Gold
freshness SLO in `research-gold-source-freshness-v1@1.1.0`. The local Pilot
accepts one exact ID only when both the v12 eligibility field and its pinned
profile disposition permit it. It returns `REVIEW_REQUIRED` only for
parent-constrained live work candidates, abstains on multiple exact candidates,
remix/variant media, or no coordinate, and forbids live assets from collapsing
onto arbitrary studio recordings.

The Catalog and Compute repositories pin the same canonical matching contract:
`sha256:638d10b15510a0d81e224692fdd5d1b9e35112bed1de0b9f289a56f9c233116d`.
It includes variant abstention, Music parent constraints, calibration,
negative-evidence weights, recall lanes, and the source-policy registry pin.

Publication, alias activation, live cutover, reverse dual write, tenant
canonical writes, and every World materialization/publication gate remain
false.

The production release build is pinned to one reviewed Catalog source commit.
It publishes separate Kubernetes-runtime and EMR-runtime images as
single-platform, scan-compatible image manifests, then records their distinct
digests, SBOMs, signatures, attestations, and zero-high/zero-critical scan
results in immutable release evidence. A failed build phase cannot reuse an
older image tag during receipt generation.

## Offline verification fixture

`tools/capture_music_pilot_reference.py` performs a read-only, rate-limited
capture from the public MusicBrainz and Wikidata APIs. Runtime matching never
calls those APIs. The script stores only mapper-consumed projections plus a
receipt for every HTTPS response (request URL, response SHA-256, authority
revision, projected IDs, and projection paths).

The frozen Pilot contains 42 MusicBrainz and 41 Wikidata entities supporting
20 asset cases: 8 exact recordings, 8 live work parents, 1 genuine shared-media
recording ambiguity, 1 remix/variant, and 2 no-coordinate controls. Wikidata does not
directly declare the MusicBrainz work ID for `Q138836655`, `Q960586`, or
`Q12860108`; those three pairs remain explicit
`RESEARCH_CANDIDATE_SAME_WORK_PARENT` coordinates with `PENDING` review status
and produce identity-review conflicts instead of pretending either a source or
a human reviewer approved the cross-ID.

Pinned coordinates:

- cohort digest:
  `sha256:ff44f41739c8967fb5d12471961489263566db9c27534177b0b3f1bc6b67884c`;
- authority-coordinate candidate digest:
  `sha256:f7acf2518c54acb103b044de38cc72b67cb02a593adfcf31c12a88b17ebe053a`;
- MusicBrainz source revision:
  `sha256:53af4e3cece1f12d26accd1cfa080e31932a637a3f31435723badc5cf8b3a46f`;
- MusicBrainz fixture digest:
  `sha256:78d691a35c29099834342cb810451e185ac9ac6f79f44043718a115cd878249a`;
- Wikidata source revision:
  `sha256:48949106cf45ed8ebdc26bdd41060054e5d3b492e934a85b9e83c13f6fad067f`;
- Wikidata fixture digest:
  `sha256:9197cff08d0e822643763df1903fa66462ef23fcc454c61bbcd18531f5e58f90`;
- MusicBrainz snapshot pin:
  `sha256:2df1c74f4d811dbdcaeca44a5c3166115569e10da83e8a79c916963097c79dd9`;
- Wikidata snapshot pin:
  `sha256:ed05bbe2ae011f5b7e2d02bcd75a8a548ba34028ca9025ebc6afdc3377c1016d`;
- concrete match profile:
  `sha256:87bac2481db4505b18fc4374e9c617988b5f60f5f7a91f81b2088afaa35d75ff`.

The older six-record MusicBrainz/two-record Wikidata fixture remains as a
small Label/release mapper regression; release and Pilot matching tests use the
new authority captures.

Recompute into a temporary directory when auditing live authority responses,
then review the diff before replacing immutable fixtures:

```bash
uv run python tools/capture_music_pilot_reference.py \
  --output-root /tmp/music-pilot-capture \
  --captured-at 2026-10-17T03:00:00Z

uv run python tools/build_music_pilot_match_profile.py
```

The profile builder consumes the fixed captures and the governed algorithm
contract; it does not query either authority or activate an index alias. The
profile sets `validatedIndexManifestRequired=true`; runtime match parameters
must still be published through the existing validated
`GoldIndexBuildManifest` path.
