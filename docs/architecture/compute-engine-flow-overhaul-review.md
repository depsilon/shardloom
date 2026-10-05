# Compute Engine Flow Alignment Review

Status: source-aligned interface with complete local harness acceptance recorded in
the [October 5 acceptance report](../benchmarks/native-typed-reductions-full43-2026-10-05.md).
The matrix passes 1,408 complete records, including 704 native candidate records,
plus a 32-record input-state probe. This is correctness evidence, not a speedup claim.

## Candidate Flow

```text
deterministic local fixture
  -> raw compatibility inputs or explicitly prepared Vortex inputs
  -> complete SQL declaration from the workload catalog
  -> ShardLoom public native workflow
  -> complete result or requested public sink
  -> independent result comparison and evidence report
```

There is one candidate identity, `shardloom`. `raw` versus `prepared` input state and the output
format change the candidate's data boundary and requested result, not its engine or execution route.
The workload catalog declares complete SQL and result shapes; names do not select private
benchmark-only commands. Baselines are independent comparison processes and never execute residual
ShardLoom work.

The candidate accepts CSV, JSON, JSONL, Vortex, Parquet, Arrow IPC, Avro and ORC input
parameters. Vortex fixture creation uses public native preparation before input freezing and query
timing; prepared mode reuses those native artifacts. Independent comparison processes read the
original CSV for Vortex cases, and their actual input format is recorded separately. Those cases
establish complete-value correctness across equivalent logical data, not a same-format timing
comparison. Other inputs retain their own format in the comparison process.

## Source Alignment

- [`run.py`](../../benchmarks/traditional_analytics/run.py) declares the public harness options,
  freezes the independent reference results before candidate execution, compares complete values,
  records the report, and returns nonzero unless every expected case passes.
- [`workloads.py`](../../benchmarks/traditional_analytics/workloads.py) is the source of truth for
  complete SQL declarations, input roles, result shapes, and declared writes.
- [`native_public_runner.py`](../../benchmarks/traditional_analytics/native_public_runner.py)
  sends those declarations through the public workflow protocol, optionally prepares local Vortex
  inputs, and validates committed requested outputs by reading every value back. JSON/JSONL files
  use the shared complete-file decoder so schema-free empty results remain verifiable; other
  formats reopen through the public native workflow.
- [`worker.py`](../../benchmarks/traditional_analytics/worker.py) isolates deterministic fixture
  generation and independent baseline execution.
- [`fixtures.py`](../../benchmarks/traditional_analytics/fixtures.py) and
  [`resources.py`](../../benchmarks/traditional_analytics/resources.py) define generated dataset
  shape and sequential local-workspace/resource guards.

The harness records binary, harness-source, input, and reference hashes. It rejects changes to the
candidate executable or input data during a run. Preparation and output validation are separately
attributed from candidate query request time; full harness wall time includes fixture creation,
hashing, validation, and cleanup costs performed within the run.

The evidence verifier reconstructs complete workload declarations and their source bindings from
the frozen fixture and preparation receipts. Matching result hashes alone cannot admit a substituted
query or an input-format label that differs from the actual reader request.

## Validation Boundary

The accepted matrix verifies raw and prepared inputs, requested sink/readback,
exact results against an independent reference, stable hashes and no-fallback
evidence. Independent verification reconstructs all workload declarations and
source bindings. The original harness revision and executable remain identified;
complete source-byte and executable identity justify retaining those results
across the subsequent assertion-only acceptance changes. Unit checks also prove
nonzero outcomes for failure/unsupported cases. Reports remain incomplete when
expected records are missing or any record is unsupported, failed, or mismatched.

Do not infer performance, production readiness, or engine superiority from this
correctness matrix, source alignment, `--list`, or partial reports. Public
benchmark pages and broader product documentation require their own synchronized
review before publishing measurements.

## Complete Public Regression Families

The public relational acceptance runner has independent `base`, `unary`, `nested`,
`pivot`, `typed`, and `memory` families. Its `all` selector retains the same case
declarations. `run_native_relational_suite.py` runs the six families sequentially
in separate local output directories, retaining each original summary and its
SHA-256. The suite checks their disjoint union against a frozen name-to-row-count
manifest before reporting complete coverage. Every family must finish, preserve
input and executable identity, and match the same runtime/harness provenance.

This partition avoids repeated storage scans over earlier families' output files.
It retains the 3,000-second process-group deadline per family, the 12-GiB free-space
headroom, the 100-GiB total workspace ceiling, and a combined 192-MiB log ceiling.
The shared workload guard rejects overlap. Failed/interrupted observations remain
failed; they cannot be combined into a successful suite. This is harness structure,
not an engine performance claim or permission to weaken any result oracle.
