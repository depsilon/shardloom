# RFC 0029 — Correctness, Benchmarks, Execution Certificates, and Stateful Reuse

## Scope

This RFC defines implementation contracts for:
- CG-5 correctness/differential harness.
- CG-6 benchmark harness.
- CG-16 evidence-first execution certificates.
- CG-17 stateful result reuse / incremental execution.

## Correctness fixtures and baseline policy

- Golden Vortex fixtures are required.
- Coverage includes null/nested/dictionary/sparse/run-length/temporal fixtures.
- Spark/Polars/DataFusion are external baselines only.
- No fallback execution is permitted.

## Benchmark evidence requirements

- Runtime benchmark metrics.
- Peak memory.
- Bytes read/written.
- Decode avoided.
- Materialization avoided.
- Work avoided.

## Execution certificates (CG-16)

Each competitive run should emit verifiable metadata including:
- plan hash.
- input snapshot/manifest hash.
- selected/skipped segments.
- side effects performed.
- reproducibility metadata.

Execution certificate evidence must be split into deterministic artifacts before
the certificate can support broad competitive claims:
- plan evidence artifact with a stable plan reference and content hash
- input snapshot evidence artifact with manifest/snapshot hash
- output evidence artifact with result or payload hash
- selected/skipped segment trace artifact
- side-effect manifest artifact
- reproducibility metadata artifact
- correctness fixture or reference-output linkage
- diagnostics and fallback status

`ExecutionCertificateEvidenceSurfaceReport` is the report-only CG-16 surface for
these requirements. It records:
- schema version and report id
- linked `ExecutionCertificate` schema version
- artifact requirements
- required hash counts
- machine-readable artifact counts
- plan/input/output artifact counts
- segment trace artifact counts
- side-effect manifest artifact counts
- reproducibility metadata artifact counts
- plan hash required
- input snapshot hash required
- output hash required
- selected segment trace required
- skipped segment trace required
- side-effect manifest required
- reproducibility metadata required
- correctness fixture required
- deterministic field order required
- certificate evaluation disabled for report-only mode
- runtime/data/IO side-effect fields
- fallback attempted false
- fallback execution allowed false
- production claim disabled

The evidence surface is not itself a certified execution result. It is a
machine-readable checklist for what every real execution certificate must carry
once broader CG-16 paths are implemented.

CG-16 cannot close until:
- supported execution paths emit `ExecutionCertificate` records
- certificates include plan/input/output evidence refs
- certificates include or link content hashes for reproducibility
- certificates include selected/skipped segment traces
- certificates include side-effect manifests even when empty
- certificates include correctness fixture or reference-output linkage
- certificate fields are stable for JSON/agent consumption
- fallback execution remains disabled and explicitly reported
- performance or superiority claims remain blocked unless CG-5 and CG-6 evidence also exists

## Stateful reuse and incremental recompute (CG-17)

`StatefulReuseReport` is the report-only CG-17 surface for typed reuse and
incremental recompute eligibility. It records the cache families, key
requirements, invalidation proof requirements, correctness proof requirements,
execution-certificate linkage, and side-effect boundaries that must exist before
any cache lookup, cache write, cache replay, or incremental execution can be
enabled.

Typed cache and reuse boundaries:
- segment result
- predicate result
- encoded dictionary
- encoded filter
- layout decision
- execution certificate
- incremental manifest diff

Every boundary must declare:
- stable boundary id
- cache kind
- reuse eligibility status
- deterministic key requirement
- dataset snapshot scope
- plan hash scope
- semantic profile scope
- encoding/layout scope
- adapter fidelity scope
- correctness proof requirement
- invalidation proof requirement
- execution certificate requirement
- cross-dataset reuse disabled unless a later RFC explicitly proves safety
- fallback attempted false

Invalidation signals must be explicit and conservative:
- snapshot changed
- segment added
- segment removed
- segment replaced
- schema changed
- partition changed
- predicate changed
- semantic profile changed
- function version changed
- adapter fidelity changed
- unknown change

Each invalidation signal must map to a conservative action. Unknown or
unproven changes reject reuse and require recompute instead of guessing.

`stateful-reuse-plan` exposes the CG-17 report for humans and agents. Its
machine-readable fields include:
- schema version and report id
- stateful reuse status
- typed cache boundary count
- invalidation requirement count
- correctness proof required count
- invalidation proof required count
- execution certificate required count
- stable cache-kind order
- stable invalidation-signal order
- deterministic key requirement
- manifest diff requirement
- cache read disabled
- cache write disabled
- cache replay disabled
- incremental execution disabled
- runtime execution disabled
- data read/decode/materialization disabled
- object-store, write, and spill IO disabled
- external engine execution disabled
- fallback execution allowed false
- fallback attempted false
- production claim disabled

CG-17 cannot close until:
- supported reuse paths emit typed reuse boundaries
- cache keys are deterministic and scoped to dataset snapshot, plan hash,
  semantic profile, encoding/layout, and adapter fidelity
- every reusable result links to correctness proof and execution certificate
  evidence
- every reuse decision records invalidation proof or a conservative rejection
- manifest-diff incremental recompute records changed/unchanged segment evidence
- stale, unknown, schema-incompatible, or semantically changed inputs reject
  reuse deterministically
- cache read/write/replay behavior is separately validated before execution
- incremental recompute has correctness fixtures and no-fallback diagnostics
- performance or superiority claims remain blocked unless CG-5 and CG-6 evidence
  also exists

## Non-goals

- No runtime fallback/delegation.
- No competitive claims without reproducible harness evidence.
