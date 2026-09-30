# Benchmarking Skill

## Purpose

Use this skill when creating benchmarks, comparing ShardLoom to other systems, or making any
performance, cost, memory, IO, decode, or scalability claim.

The goal is to make ShardLoom performance claims reproducible, credible, and engineering-grade.

## When to use

Use this skill for tasks involving:

- Benchmarks.
- Performance claims.
- Cost claims.
- Comparisons against Spark, DataFusion, DuckDB, Polars, Velox, or Vortex integrations.
- Profiling.
- Memory measurements.
- IO measurements.
- Decode measurements.
- Cloud-cost estimates.
- Benchmark documentation.

## Rules

- Do not make marketing claims without measurements.
- Every benchmark must identify the dataset, shape, scale, schema, and storage format.
- Every benchmark must identify hardware, operating system, runtime versions, and configuration.
- Distinguish cold-cache from warm-cache results.
- Measure more than wall-clock time.
- Track bytes read, bytes decoded, rows scanned, rows materialized, allocations, peak memory, CPU
  time, and wall time where possible.
- Benchmarks against other systems must name exact versions and configurations.
- Spark, DataFusion, DuckDB, Polars, and Velox may be benchmark baselines, not execution fallbacks.
- Prefer reproducible scripts over manually reported numbers.
- Avoid cherry-picked results.
- Include correctness validation for benchmark queries.

## Local end-to-end observations

- Freeze the source revision, build command/features, executable hash, workload,
  input generation and complete reference results before timing. A reused binary
  needs source-file equality with the requested revision, including Cargo files.
- Keep native builds, tests, profilers and timed workloads sequential under the
  shared workload guard and UAT lock. Record host load and process CPU/RSS samples
  around and during the run; ordinary desktop activity is observation context,
  not proof of an idle host or the cause of a timing change.
- Report native ingest, the sum of complete native query processes, and actual
  supervised workflow wall time separately. State whether hashing, validation,
  archiving and cleanup fall inside each clock. A sum of process times must not
  be labeled elapsed end-to-end time.
- Label a single sequential Full43 pass as one observation. Preserve every
  sample in comparative cohorts and apply their frozen symmetric scoring rule;
  never replace a paired cohort with a faster later single pass.
- Record physical RAM separately from memory admission policy, measured peak RSS,
  and any enforced limit. State prehash/cache effects and uncontrolled cache state.
- Preserve complete raw outputs, native timing/PID records, host observations,
  exact result checks and original/portable hashes before compacting logs or
  retiring verified duplicate payloads. Retained result equality is regression
  evidence; identify a fresh independent correctness oracle separately.

The [local procedure](../architecture/local-development-storage.md#current-runtime-observation-procedure)
and [September 30 observation](../benchmarks/current-runtime-e2e-2026-09-30.md)
provide the applied protocol and immutable evidence. These observations do not
change official benchmark, release-distribution or competitive-gate claims.

## Required checks

A benchmark PR should include:

- Dataset description.
- Query or workload description.
- Format and compression description.
- Engine versions.
- Hardware/runtime context.
- Cold/warm cache notes.
- Metrics collected.
- How to reproduce.
- Expected correctness result or reference validation.
- Limitations of the benchmark.

## Red flags

- "ShardLoom is faster" without a benchmark.
- Comparing against an unconfigured or intentionally disadvantaged baseline.
- Reporting only wall-clock time.
- Ignoring cold-cache behavior for object-store workloads.
- Ignoring memory and decode volume.
- Publishing results without reproducibility instructions.
- Treating benchmark competitors as fallback engines.

## Example Codex prompt fragment

When creating benchmarks, include this instruction:

"Use the Benchmarking skill. Include dataset shape, engine versions, hardware/runtime context,
cold/warm cache notes, and metrics beyond wall time. Do not make performance claims without
reproducible measurements."
