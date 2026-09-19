# Reference catalog selection v1

## Goal

Build a deterministic internal reference catalog for long-form asset matching.
The primary budget contains content entities only. People and organizations are
selected from the credit closure under separate limits.

Default content budget:

- 30,000 movies;
- 2,000 series;
- 10,000 seasons;
- 58,000 episodes.

Default agent limits:

- 30,000 people;
- 5,000 organizations.

Quotas are provisional until an approved internal asset demand profile is
attached.

## Asset demand profile

The optional immutable profile records only aggregate metadata from sampled
assets:

- sample count and source manifest digest;
- content-type weights;
- language-QID weights;
- country-QID weights;
- decade weights;
- profile creation time.

It contains no video bytes, filenames, credentials, tenant identifiers, or
free-form asset text.

## Candidate features

Each candidate has:

- Wikidata QID and classified entity type;
- Wikipedia sitelink count;
- metadata completeness score;
- exact external identifier count;
- demand-profile score;
- hierarchy parent QIDs;
- relation targets used for agent closure.

Ranking order is deterministic:

1. demand score;
2. available child support for series and seasons;
3. completeness score;
4. exact-ID count;
5. sitelink count;
6. numeric QID.

Sitelinks are a notability signal only.

## Hierarchy selection

Movies and series are selected first.

Seasons linked to a selected series are ranked before unlinked fallback
seasons. Episodes linked to a selected season or series are ranked before
fallback episodes.

Every selected season and episode records `COMPLETE` or `PARTIAL` hierarchy
coverage. A fallback row is never silently presented as a complete hierarchy.

If a content quota cannot be filled from classified candidates, selection
fails. It does not consume agent capacity or silently transfer quota between
types.

## Agent closure

After content selection:

- people are ranked by reference count, candidate rank, then QID;
- organizations use the same rule under a separate limit;
- unknown targets may use relation-property hints;
- agents do not count against the content target.

## Audit

The immutable audit records:

- selection config, quality-threshold, combined build, and demand-profile
  digests;
- immutable dump, optional demand-profile, subset, and source-manifest
  ObjectRefs;
- selected content counts by type;
- selected people and organizations;
- complete/partial hierarchy counts;
- required-field and exact-ID coverage by type;
- per-stage fallback counts;
- output and classification-dependency row counts;
- relation statements pruned because their target was outside the selected
  content/agent closure.

Selection and audit must be independent of input order and Spark partitioning.
The `video-media-catalog-reference-subset` CLI publishes no subset or source
manifest when a quality gate fails. The legacy Wikidata subset CLI retains its
original selection semantics and output paths.
