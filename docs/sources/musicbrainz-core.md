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
Identifiers are the matching MusicBrainz MBID namespace plus ISRC, ISWC, and
reviewed Wikidata QID links. Wikidata property `P966` crosswalks a Label QID to
`musicbrainz.label` / `musicbrainz-label`. Relationships are `recording_of`,
`release_membership`, `credit`, and `performance_link`. Gold and OpenSearch
reuse generic identifier/relation collections; no Music-specific index field is
added. Label participation in a release or release group uses the existing
`credit` relation with `role=LABEL`, not a Label-specific predicate.
The Label-capable MusicBrainz and Wikidata Music mappers are versioned
`1.1.0`; Source Silver checkpoints bind MusicBrainz mapper `1.1.0` and the
Wikidata research dispatcher `2.1.0`.

## Release and matching

The Music reference slice requires both a MusicBrainz core pin and a Wikidata
Music pin. Both Temporal profiles use `identitySpine=true`,
`goldContribution=true`, and `materialization=false`; the monthly MusicBrainz
core profile includes Label records, so Label does not introduce a third source
or a Music-specific workflow. The capture uses the bounded 45-day Gold freshness SLO in
`research-gold-source-freshness-v1@1.1.0`. Catalog Match accepts one exact ID
candidate, abstains on multiple exact candidates, and leaves parent-constrained
recording/release candidates for human review. Its Label-capable algorithm
digest is
`sha256:0dcf5d5aff823f490439b447e60ef4fb3e2752bfb6765bf6ad2ebac282025bba`.

Publication, alias activation, live cutover, reverse dual write, tenant
canonical writes, and every World materialization/publication gate remain
false.

## Offline verification fixture

`tests/fixtures/research/musicbrainz-music-reference.v1.json` pins a small
six-record NDJSON sample covering Radiohead, Parlophone, and “High and Dry”,
including artist/label/work/recording/release-group/release MBIDs, ISRC
`GBAYE9400055`, ISWC `T-010.346.288-9`, and Wikidata QIDs `Q44190`, `Q208909`,
and `Q2469634`. The paired two-record Wikidata fixture verifies the
`Q208909`/`P966` Label crosswalk. Tests verify sidecar checksums before mapping
and never depend on a public API.

The fixture materializes to source snapshot pin
`sha256:dcdad6140f498eac0a569a8212ec96835fecc33d76666c6b542c65778ac8b7e7`.
Its paired Wikidata Music fixture materializes to
`sha256:946966b7efd21408500156ac7afd6a449a5d8e4f45d5ad22e2a274bc2df0d1fb`.
