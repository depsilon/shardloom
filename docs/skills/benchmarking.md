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

## Timing communication decision

Standing maintainer decision, October 15, 2026: routine timing updates, answers, and performance
summaries report **engine time only**. This rule governs presentation; complete diagnostic
measurements and correctness checks remain required.

- Use the engine-owned `engine_execution_seconds` field. A standard Full43 total is its sum over
  one execution of each of the 43 queries. Provide per-query engine times when requested. Identify
  explicitly requested repeated or comparative cohorts with their declared aggregation rule.
- Use the same engine clock, workload, and declared methodology for comparisons. Missing engine
  clocks stay unavailable; never substitute process or supervised duration. An incomplete or
  invalid pass has no accepted full-suite engine total.
- Engine time includes query I/O, decoding, native operators, source checks, and provider
  certification within the prepared execution boundary. It excludes result serialization and
  UAT preparation, pre/post-run hashing, external result validation, archiving, and cleanup.
- Keep other clocks, CPU/RSS observations, and supervision details in raw diagnostic evidence.
  Show them only when explicitly requested. A timing-only answer does not need a validation-pass
  narrative. Disclose failures or disturbances that invalidate the reported result.
- Keep progress stage descriptions accurate: preparation before queries, engine execution during
  queries, and verification after queries. Do not describe a longer supervised or diagnostic
  duration as an engine regression. This policy does not remove safety or correctness checks,
  alter the runner, or authorize a new benchmark run.

The normal summary form is: `Full43 engine time: <seconds> s (43 queries, one execution each).`

## Isolated Full43 procedure

Standing maintainer decision, October 21, 2026: use the isolated single-pass
procedure for every routine Full43 observation. Inspect and finish or stop
unrelated task-owned work before starting; keep builds, tests, profilers,
subagents, evidence packaging, cleanup and other task work idle during queries.
Do not terminate unrelated user applications or operating-system services.

Use the standard query runner with one execution of each of the 43 queries.
Keep its storage watchdog, output ceilings, exclusive lock, deadlines, complete
result validation and process cleanup. Preserve the lightweight workload guard
and host observations. Do not add recursive build-cache/evidence-folder scans,
whole-input hashing, broad source inventories or other heavy outer-wrapper work
while queries execute. Perform those preparation and verification steps before
or after the pass. Active run outputs remain subject to the standard storage
checks; this does not raise a limit or remove a safety check.

Freeze the executable, source, input, reference values and resource settings;
record cache context and any disturbances. Report the engine sum only. Keep
diagnostic profiling and repeated comparisons as separately declared work,
preserve all earlier results, and do not rerun a suite merely to improve its
presentation. An isolated observation returning near an earlier value does not
by itself establish the cause of prior variation.

## Local end-to-end observations

- Freeze the source revision, build command/features, executable hash, workload,
  input generation and complete reference results before timing. A reused binary
  needs source-file equality with the requested revision, including Cargo files.
- When comparison worktrees share a Cargo target directory and package/version
  identities, a successful cached build is insufficient source proof. Use separate
  output directories or invalidate the affected package artifacts and verify their
  fresh compilation. Check role-distinguishing behavior or test inventories before
  timing; reject an unexpected identical executable. The October 15 compression
  screen caught a baseline binary reused for its candidate and rejected that
  setup before any measurements.
- Keep native builds, tests, profilers and timed workloads sequential under the
  shared workload guard and UAT lock. Record host load and process CPU/RSS samples
  around and during the run; ordinary desktop activity is observation context,
  not proof of an idle host or the cause of a timing change.
- Record native ingest, the sum of complete native query processes, and actual
  supervised workflow wall time separately in diagnostic evidence. State whether hashing, validation,
  archiving and cleanup fall inside each clock. A sum of process times must not
  be labeled elapsed end-to-end time.
- For all new query UAT and optimization runs, use the engine-owned prepared
  execution clock as the primary optimization measure. Record result serialization,
  remaining process work, complete process wall time, supervisor overhead and
  complete-value validation separately. Remaining process work includes startup,
  parsing/preparation, control/report work and exit; do not mislabel that residual
  as measured startup or preparation alone. Engine elapsed includes query-time
  I/O, decoding, native operators, source checks and provider certification;
  it is not pure CPU time. Keep process user/system CPU and peak RSS alongside it
  in diagnostic evidence; apply the timing communication decision to user-facing summaries.
- Both Full43 runners produce `query-timings.csv` and raw timing totals, retaining
  every query and repeat. A standard single pass has 43 rows. Require engine clocks
  by default; `--allow-missing-engine-timing` is only for explicitly identified
  historical binaries. Missing clocks remain unavailable. Keep ingest, build,
  input/output hashing, validation, archival, cleanup and actual workflow elapsed
  as distinct stage measurements in diagnostic evidence. Include complete per-query
  engine times with the UAT report and retain the full timing CSV before returning
  to optimization work. Never rewrite old clock meanings.
- Version 2 single-run and paired summaries score engine execution primarily and
  retain complete process scores under `secondary_process_wall`. The paired
  medians and candidate-minus-control deltas use the same declared clock as their
  role totals. `complete` describes passed result coverage; `timing_available`
  separately requires every expected clock sample. Missing historical clocks
  leave engine scores unavailable even when process scores and result checks pass.
  Version 1 archived summaries keep their original process-time meanings.
- Label a single sequential Full43 pass as one observation. Preserve every
  sample in comparative cohorts and apply their frozen symmetric scoring rule;
  never replace a paired cohort with a faster later single pass.
- Default future Full43 UAT to one execution of each of the 43 queries. Both
  query runners accept explicit `--repeats` when repeated trials are requested;
  a one-pass summary has no hot-run score. Preserve historical three-run results
  with their original counts and methodology. Do not add artificial pacing or
  battery/thermal admission between queries, trials or UAT stages.
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
