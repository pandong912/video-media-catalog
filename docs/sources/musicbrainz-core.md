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
- `MUSICAL_WORK`
- `MUSIC_RECORDING`
- `MUSIC_RELEASE_GROUP`
- `MUSIC_RELEASE`

Identifiers are the matching MusicBrainz MBID namespace plus ISRC, ISWC, and
reviewed Wikidata QID links. Relationships are `recording_of`,
`release_membership`, `credit`, and `performance_link`. Gold and OpenSearch
reuse generic identifier/relation collections; no Music-specific index field is
added.

## Release and matching

The Music reference slice requires both a MusicBrainz core pin and a Wikidata
Music pin. Both Temporal profiles use `identitySpine=true`,
`goldContribution=true`, and `materialization=false`; the monthly MusicBrainz
capture uses the bounded 45-day Gold freshness SLO in
`research-gold-source-freshness-v1@1.1.0`. Catalog Match accepts one exact ID
candidate, abstains on multiple exact candidates, and leaves parent-constrained
recording/release candidates for human review.

Publication, alias activation, live cutover, reverse dual write, tenant
canonical writes, and every World materialization/publication gate remain
false.

## Offline verification fixture

`tests/fixtures/research/musicbrainz-music-reference.v1.json` pins a small
five-record NDJSON sample covering Radiohead and “High and Dry”, including
artist/work/recording/release-group/release MBIDs, ISRC `GBAYE9400055`, ISWC
`T-010.346.288-9`, and Wikidata QIDs `Q44190` and `Q2469634`. Tests verify the
sidecar checksum before mapping and never depend on a public API.

The fixture materializes to source snapshot pin
`sha256:dd89887a2ebd48b124082ba75835322665015ead20dac1278a2a333c48b1082c`.
Its paired Wikidata Music fixture materializes to
`sha256:4094e4c326b7f224af5621e4557aeaf5c8f75a6d014e26f3e8e8509beb4cf719`.
