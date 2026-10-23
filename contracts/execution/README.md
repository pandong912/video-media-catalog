# Vendored unified execution contracts

Status: `DESIGN_ONLY` / `PLANNED`.

Catalog vendors the upstream contract payloads byte-for-byte from
`video-world-platform/contracts/execution`: seven JSON Schemas, fourteen
fixtures (seven valid and seven invalid), and
`canonical-tuple-v1.golden.json`. The Platform README is not vendored.
`platform-execution-contracts.v1.lock.json` records the source coordinates,
supporting source-artifact digests, and a SHA-256 byte lock for each of the 22
vendored files.

Platform remains the schema and execution authority. These files establish only
a provider-SDK-neutral producer/consumer boundary:

- `object-ref-v2.v2` supports credential-free `AWS_S3` (`s3://`) and
  `ALIBABA_OSS` (`oss://`) coordinates; `versionToken` identifies one immutable
  provider version.
- `PlanRequest.inputManifestSemanticDigest` identifies the immutable upstream
  planning inventory. `execution-input-manifest.v1` is partition-specific and
  binds the selected plan and partition identity.
- `execution-plan-manifest.v1` is the immutable pre-dispatch plan graph. Its
  `planningInputManifestDigest` binds the same planning inventory, while each
  partition binds its own input projection.
- `root-manifest.v1` is the post-execution receipt closure over the exact plan
  manifest, receipt set, aggregate result, and applicable result objects.

Catalog does not wire these contracts into Temporal, submit EMR work, or
implement dispatch, admission, execution, or publication. Vendoring also does
not change Catalog release, Gold, or Iceberg authority.

The only remaining provenance limitation is that the contract directory and
the pinned supporting README, fixture generator, and canonical digest
implementation state are not fully represented by the recorded source commit.
That commit alone therefore cannot reconstruct this capture; the 22 per-file
and three source-evidence digests in
`platform-execution-contracts.v1.lock.json` are the byte-level lock.

A later P3 bridge may adapt validated Catalog requests, partition inputs, and
returned receipts at this Platform-owned boundary. That runtime bridge must be
implemented, tested, and released separately; it is not implemented here.
