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

## Stateful reuse and incremental recompute (CG-17)

- Segment-result cache.
- Predicate-result cache.
- Encoded dictionary/filter cache.
- Incremental recompute from manifest diffs.
- Cache invalidation proof.

## Non-goals

- No runtime fallback/delegation.
- No competitive claims without reproducible harness evidence.
