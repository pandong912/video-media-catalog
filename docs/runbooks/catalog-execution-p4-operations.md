# Catalog execution P4 operations

## Runtime truth

Catalog P4 is implemented and locally testable, but not production-enabled.
`catalog-execution-p4.runtime-lock.v1.json` byte-locks the Platform P4
`PLANNED` manifest and all 14 listed artifacts. They are not present in the
observed Platform HEAD, so the lock records null P4 source
commit/publication/activation receipts and `runtimeActivationAllowed=false`.
Workers must fail closed until committed provenance and real runtime approvals
are pinned.

The health command is:

```bash
video-media-catalog-execution-temporal-worker --health-check
```

It prints bounded JSON. `NOT_READY` and a non-zero exit are expected while the
P4 manifest or activation receipts are absent.

## Evidence and hooks

Provider observations emit bounded resource evidence for duration, vCPU,
memory, storage, input/output bytes, and shuffle bytes when the provider
returns those values. Missing fields remain zero with `complete=false`.
Provider cost is `PROVIDER_REPORTED` only when the API returns both amount and
currency; otherwise it is explicitly `UNAVAILABLE`. Catalog does not estimate
money from an invented rate card.

The vendored Platform `execution-cost-observation.v1` contract is receipt-linked
and remains Platform-owned. Catalog cannot construct it without an
authoritative Platform receipt ID, estimate, and observation identity; provider
cost evidence is bound into the Catalog completion receipt only by digest.

Set `CATALOG_EXECUTION_HOOK_FACTORY` to an allowlisted
`video_media_catalog.*:factory` only after the implementation is deployed with
its credentials. The returned hook receives:

- metrics such as `catalog_execution_submit_total`,
  `catalog_execution_reconcile_total`, `catalog_execution_observe_total`,
  resource byte/duration samples, cancel count, and completion receipt count;
- credential-free audit events for submit/resume, orphan reconciliation,
  observation, cancellation/rollback, and completion-receipt publication.

Hook I/O runs only in Activities. Workflow code remains deterministic.

## Heartbeat, retry, and orphan reconciliation

Submit Activities heartbeat the exact provider/application/job identity and
stable client token. After an Activity retry, Catalog first adopts and
reconciles that identity rather than creating a second job. Every Workflow run
also executes one-shot reconciliation before polling.

For an orphan alert:

1. Stop automatic resubmission for the affected attempt.
2. Query `status` and `recoveryCheckpoint`.
3. Confirm provider, application, job ID, profile digest, and attempt identity.
4. Replay the persisted Temporal history with the current compatible Worker.
5. Run one-shot provider reconciliation using the exact job identity.
6. Adopt a matching running/terminal job; never infer success from submit or
   cancel acknowledgement.
7. If identity or exact-version evidence differs, keep the attempt failed
   closed and escalate.

## Cancellation, rollback, and disaster recovery

The `cancel` signal requests backend cancellation. The `rollback` signal uses
the same bounded cancellation path but records `rollbackRequested` in the
deterministic recovery checkpoint. A cancel acknowledgement is not terminal;
the Workflow continues observation until a provider terminal state or its
bounded observation budget is exhausted.

Disaster recovery restores Temporal history and Worker code, then reconciles
the exact backend identity from the checkpoint. Do not start a replacement job
with a different client token under the same attempt.

Rollback never deletes exact-version S3/OSS objects, rewrites Platform state,
or reverses an Iceberg snapshot. Catalog execution only publishes external
authority evidence. Existing release and Iceberg publication retain
commit-last authority and require their normal validated receipt closure.

## Failure injection

`DeterministicFailureInjector` is restricted to `SYNTHETIC_LOCAL`. It can fail
before/after submit, observe, cancel, or reconcile to test response loss,
Activity retry, orphan adoption, and replay. Do not expose it through
production payloads or environment configuration.

## Local performance evidence

`readiness/evidence/catalog-execution-p4-synthetic-local.v1.json` is generated
from a deterministic eight-file fixture. It labels small-file count, skew
ratio, synthetic duration, and synthetic throughput as `SYNTHETIC_LOCAL` and
sets `productionSloClaimed=false`.

This artifact is not a capacity test, production benchmark, cost calibration,
or SLO. Production throughput/SLO claims require provider credentials, real
workloads, rate cards, and environment-specific evidence.

## Remaining activation inputs

- committed Platform P4 artifact source commit and publication/activation
  receipts;
- cross-namespace Temporal mTLS/RBAC and disaster-recovery namespace access;
- AWS IAM/EMR Serverless/S3 versioning, checksum, and KMS permissions;
- Alibaba Cloud RAM/RRSA, EMR Serverless Spark, OSS versioning/checksum/KMS,
  and approved client factories;
- production metrics/audit backend credentials and retention policy;
- Catalog-owned provider-profile activation receipt.
