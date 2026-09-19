# Catalog asset match v1

## Boundary

This contract proposes reference-catalog candidates for an existing
`AssetVersion`. It never creates, updates, or revokes a control-plane
`ReferenceLink`.

The control plane remains authoritative for:

- tenant fencing;
- explicit user confirmation;
- creation and revocation of reference links;
- producer run and attempt identity;
- audit and outbox events.

The matcher does not receive video bytes, object-store credentials, source
URIs, or unrestricted fact payloads.

## Match request

One request contains:

- canonical UUIDv7 `assetVersionId`;
- content type;
- primary and alternate titles;
- release year;
- duration in seconds;
- season and episode numbers;
- language tags;
- exact external identifiers.

At least one title or exact external identifier is required. Unknown fields
remain absent; they are not replaced by guessed values.

The request digest is SHA-256 over canonical JSON and is used for replay and
idempotency.

## Candidate generation

The retriever supplies a bounded set of Gold records from one immutable
`releasePlanId`. The deterministic ranker uses:

1. exact external identifiers;
2. normalized title similarity;
3. content-type compatibility;
4. release-year distance;
5. duration distance;
6. season and episode equality;
7. language overlap.

The ranker rejects an unbounded input set and emits at most 20 candidates.
Every candidate contains typed evidence and a deterministic candidate key.

Confidence tiers are `EXACT`, `HIGH`, `MEDIUM`, and `LOW`. A manifest has
`REVIEW_REQUIRED` even for `EXACT`; automatic reference-link creation is out of
scope until the offline evaluation gate is approved.

## Match manifest

The immutable JSON manifest binds:

- request digest;
- matcher algorithm and configuration digests;
- Gold release plan;
- concrete search index;
- generated timestamp;
- retrieval count;
- ordered candidates;
- `REVIEW_REQUIRED` or `NO_CANDIDATE` disposition.

Ordering is score descending, then entity key ascending. Pagination aliases are
not valid release identities.

## Evaluation

The production golden set must contain at least 300 reviewed cases and cover
movies, series, seasons, episodes, multilingual titles, remakes, ambiguous
names, and missing metadata.

The initial minimum type strata are 100 movies, 40 series, 40 seasons, and 100
episodes.

The report records overall and cohort-level:

- top-1 accuracy;
- recall at 5;
- no-candidate rate;
- exact-ID top-1 accuracy;
- false-positive rate at the proposed acceptance tier.

Default initial gates:

- top-1 accuracy at least 90%;
- recall at 5 at least 97%;
- exact-ID top-1 accuracy at least 99.5%;
- false-positive rate at most 0.5%.

These gates block automatic acceptance. They do not block review-only
candidate generation.
