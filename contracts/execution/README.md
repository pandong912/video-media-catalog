# Vendored unified execution contracts

Status: `DESIGN_ONLY` / `PLANNED`.

Catalog vendors the upstream contract payloads byte-for-byte from
`video-world-platform/contracts/execution`. The P0 capture contains seven JSON
Schemas, fourteen fixtures, and `canonical-tuple-v1.golden.json`;
`platform-execution-contracts.v1.lock.json` records its 22 byte locks. The P3
capture adds the final five schemas, ten fixtures, and
`execution-p3-artifact-manifest.v1.json`; the manifest and
`catalog-execution-p3.runtime-lock.v1.json` close their SHA-256 and byte-length
provenance. The P4 capture adds five schemas, eight fixtures, one `LOCAL_OFFLINE`
evidence artifact, and its 14-artifact manifest. The Platform README is not
vendored.

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

P3 adds a Catalog-owned `CatalogExecutionWorkflowV1` and one-shot Spark
submit/observe/cancel adapters. Platform may invoke it only through the pinned
cross-namespace Temporal contract. Neither repository receives credentials or
write access to the other repository's database, and Platform does not acquire
Catalog release, Gold, or Iceberg authority.

The only remaining provenance limitation is that the contract directory and
the pinned supporting README, fixture generator, and canonical digest
implementation state are not fully represented by the recorded source commit.
That commit alone therefore cannot reconstruct this capture; the 22 per-file
and three source-evidence digests in
`platform-execution-contracts.v1.lock.json` are the byte-level lock.

`catalog-execution-p3.runtime-lock.v1.json` records the observed Platform P2
manifest and the exact published P3 artifact manifest. Its status is
`MANIFEST_PUBLISHED`: the P3 lock-body digest, Catalog request/result schemas,
and manifest fixture profile references are allowlisted, while production
activation remains false. The P3 artifacts are not present in the observed
Platform HEAD, so the lock deliberately records no Platform P3 source commit,
commit receipt, publication receipt, activation receipt, or Catalog-owned
runtime-profile approval. The worker therefore still refuses production
startup without claiming live enablement.

`catalog-execution-p4.runtime-lock.v1.json` pins the Platform P4 `PLANNED`
manifest, its 14 byte-exact artifacts, and the manifest lock-body digest. The
artifacts are not present in the observed Platform HEAD, so no P4 source
commit, publication receipt, or activation receipt is claimed. P4 operational
code for provider evidence, checkpoint/reconcile, rollback, failure injection,
health, metrics, audit, and replay remains fail-closed. Platform retains
authority for its cost-observation companion; Catalog records provider evidence
but cannot fabricate a Platform receipt-linked cost observation.

Production activation and production SLO claims remain false. Both the vendored
Platform `LOCAL_OFFLINE` benchmark and the Catalog small-file/skew/throughput
artifact are synthetic/local evidence only; neither changes Catalog release or
Iceberg commit-last authority.
