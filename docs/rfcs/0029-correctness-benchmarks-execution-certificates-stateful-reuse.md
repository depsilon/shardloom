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

- Segment-result cache.
- Predicate-result cache.
- Encoded dictionary/filter cache.
- Incremental recompute from manifest diffs.
- Cache invalidation proof.

## Non-goals

- No runtime fallback/delegation.
- No competitive claims without reproducible harness evidence.
