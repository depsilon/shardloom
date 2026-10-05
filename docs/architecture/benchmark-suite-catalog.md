# Traditional Analytics Benchmark Suite

Status: the parameterized harness interface is available for review; live acceptance and benchmark
claims are pending. The suite has one ShardLoom candidate, `shardloom`, using the public native
workflow. Raw versus prepared input and requested output are configuration choices for this
candidate, not separate engines or benchmark routes.

## Declaration Catalog

The executable catalog is [`benchmarks/traditional_analytics/workloads.py`](../../benchmarks/traditional_analytics/workloads.py).
Each named workload declares one or more complete SQL statements, required source roles, result
shape, and, where relevant, a declared write statement. The names and declarations shown by
`python3 benchmarks/traditional_analytics/run.py --list` are authoritative; this document does not
duplicate a long, drifting SQL catalog.

The catalog covers scans and ingestion, filters and projections, aggregates, grouping, ordering,
joins, windows, partition and many-file reads, null-heavy and high-cardinality data, casts and
writes, nested JSON, change overlays, and larger join/ETL shapes. Workload semantics are the complete
declaration; a workload name does not select private ShardLoom execution code. ShardLoom must admit
the declaration through its public native workflow or report it unsupported.

## Parameters

The public harness is [`benchmarks/traditional_analytics/run.py`](../../benchmarks/traditional_analytics/run.py).
Its user-facing controls include:

- `--shardloom-binary` and local-only `--workspace` for the candidate executable and fresh run
  workspace.
- `--input-state raw|prepared` to use generated compatibility inputs directly through the public
  workflow or prepare local Vortex inputs before query timing.
- `--formats` to select the source formats and `--output-format` to select `collect` or a declared
  output sink format.
- `--scenarios` and `--repeats` to select the complete SQL workloads and repeated observations.
- `--reference-engine` to select the independent correctness comparison. `--engines` may add
  independent comparison adapters; it does not add ShardLoom execution candidates.
- Dataset-shape and resource controls, including `--rows`, `--dim-rows`, `--dataset-profile`,
  `--memory-gb`, `--max-parallelism`, and `--timeout`.

Run `--help` for exact accepted choices and `--list` to inspect the full workload declarations.
Examples and defaults belong in the harness README so command syntax has one maintained source.

## Correctness And Evidence

The reference engine runs independently and its complete result values are frozen before the first
ShardLoom candidate execution. Each candidate result is compared with the frozen value for the same
format and workload. When a non-collect output format is requested, the candidate uses the public
write request and reads the committed output back through the public workflow before comparing its
complete result values. The candidate adapter records its declared SQL, output/readback receipts,
binary and harness hashes, input identities, no-fallback fields, and timing boundaries.

The report is complete only when all expected engine, format, workload, and repeat records are
present and passed. Unsupported or failed records and result mismatches make it incomplete; guard
and integrity failures make the run failed. Incomplete or failed runs return nonzero and provide no
timing claim. Benchmark baselines never execute ShardLoom work.

## Workspace And Timing Boundaries

Use a fresh local filesystem workspace. The harness applies a sequential-workload lock, local path
checks, free-space and artifact budgets, process-conflict detection, and sampled process-tree memory
observation. Run directories are unique and report creation is exclusive. It records and checks
executable and input hashes so a changed binary or fixture invalidates the run.

Input preparation, fixture creation, hashing, query/sink process time, output readback, baseline
execution, and actual end-to-end harness time are distinct. Source hashing may warm the filesystem
cache; output readback is outside candidate query timing but inside actual run wall time. A sum of
process timings is not elapsed end-to-end time. Reports must preserve cache context and hardware,
OS, runtime, dataset, format, and configuration details before any later timing analysis.

## Acceptance Status And Boundaries

The new harness interface and methodology are documented, but live acceptance has not been
established. Before treating any report as benchmark evidence, validate raw input, prepared input,
and a requested sink against an independent reference; inspect the complete report and receipts;
verify stable input/executable hashes and no-fallback status; and confirm that negative cases exit
nonzero. No present performance, production-readiness, or superiority claim is authorized here.

This suite is local and single-host. Managed services, object-store execution, broad production
workloads, and remote compute are outside its scope. External systems appear only as explicit
independent comparisons where the selected adapter supports the declaration.
