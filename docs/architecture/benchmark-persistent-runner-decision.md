# Benchmark Persistent Runner Decision

Status: Accepted for P7.5.7  
Scope: `benchmarks/traditional_analytics/run.py` and local ShardLoom comparative rows

## Summary

P7.5.7 keeps the current Python-driven per-scenario CLI runner and makes process overhead explicit
instead of adding a persistent in-process runner in this slice.

GAR-FLOW-2F adds a narrower runtime surface: `traditional-analytics-vortex-batch-run` can execute
multiple prepared/native Vortex scenarios in one ShardLoom process while preserving typed evidence
and no-fallback fields. That command is scoped process reuse only. The default comparative Python
harness remains per-scenario until a later harness integration slice consumes the batch command.

The benchmark harness now reports these attribution fields where feasible:

```text
total_runtime_millis
query_runtime_millis
cli_process_wall_millis
python_harness_overhead_millis
startup_warmup_millis
build_time_millis
build_time_excluded
preparation_millis
preparation_cli_process_wall_millis
preparation_included_in_timing
scenario_compute_millis
operator_compute_millis
computed_result_sink_write_millis
result_sink_write_millis
evidence_render_millis
persistent_runner_status
process_startup_attribution
python_harness_overhead_status
```

## Decision

Do not add a persistent daemon or hidden benchmark-only runner yet.

Reasons:

- The current typed CLI envelope is the contract under test for local users, Python clients, and
  future REST parity.
- A persistent runner would need to preserve typed envelopes, evidence artifacts, Native I/O
  certificate refs, execution-mode selection reports, and no-fallback diagnostics across multiple
  scenarios before it could replace the current path.
- Prepared Vortex reuse is now the primary performance comparison improvement; it avoids repeated
  compatibility import per scenario without changing the typed CLI contract.
- The remaining overhead is useful to see during release readiness because users will initially run
  the CLI and Python wrapper, not a hidden benchmark-only runner.

## Current Measurement Semantics

`query_runtime_millis` and `total_runtime_millis` are the outer Python harness wall time for a
single scenario iteration. For ShardLoom rows, this includes:

```text
Python harness dispatch
CLI process wall time
typed-envelope rendering/parsing
the selected ShardLoom execution mode
optional result-sink write/replay
```

`cli_process_wall_millis` is measured inside `subprocess_run` around the ShardLoom CLI process.

`python_harness_overhead_millis` is derived as:

```text
max(query_runtime_millis - cli_process_wall_millis, 0)
```

`build_time_millis` is measured separately during local CLI build and is excluded from per-scenario
timing. `startup_warmup_millis` excludes ShardLoom build time and no longer folds prepared-artifact
setup into startup. `preparation_millis` and `preparation_cli_process_wall_millis` describe
prepared Vortex artifact setup separately from scenario timing.

## Persistent Runner Requirements Before Implementation

A future persistent runner must:

- preserve `shardloom.output.v2` typed envelopes
- preserve execution-mode selection reports
- preserve Native I/O certificate refs and inline artifacts
- preserve result-sink replay evidence
- preserve no-fallback and external-engine-invoked fields
- report whether startup/warmup is amortized
- keep preparation timing separate from scenario timing
- keep unsupported rows deterministic
- avoid external engine fallback

Until a row is produced by the explicit scoped batch command or a later harness integration, default
comparative prepared/native rows must say:

```text
persistent_runner_status=process_per_scenario_attributed_not_reduced
```

The scoped batch command may instead say:

```text
persistent_runner_status=single_process_batch_runner_supported
```

That status does not authorize a daemon, service runtime, hidden fast mode, or performance claim.

## GAR-PERF-2F Session Runtime Follow-Up

`GAR-PERF-2F` is the planned bridge from this scoped batch runner to an explicit in-process
`ShardLoomSession`. The session target is caller-owned and local-artifact-scoped, not a daemon or
remote service. It should expose `session_id`, prepared-artifact cache hit/miss counts,
source-metadata/source-state cache hit/miss counts, source-state reuse count, prepared-artifact reuse
count, close/drop status, `fallback_attempted=false`, and `external_engine_invoked=false`.

The session follow-up must preserve the same typed envelope, execution-mode, evidence-level,
Native I/O, materialization/decode, result-sink, deterministic unsupported, and no-fallback fields
required by this persistent-runner decision.

`GAR-PERF-2G` is the paired allocation/buffer-pool follow-up. Any future buffer pool must remain
scoped to the explicit run/session lifecycle rather than this benchmark decision becoming a hidden
persistent global cache. Rows should report allocation profile status, buffer-pool enabled/scope,
buffer-reuse count, correctness digest, evidence-regression status,
`unsafe_lifetime_shortcut_used=false`, `fallback_attempted=false`, and
`external_engine_invoked=false`.

## GAR-FLOW-2C Admission Gate

The benchmark report now emits a report-only `persistent_runner_admission_gate`
artifact with:

```text
gate_id=gar-flow-2c.persistent_runner_admission.v1
support_status=report_only
persistent_runner_admitted=false
current_status=process_per_scenario_attributed_not_reduced
hidden_fast_mode_allowed=false
performance_claim_allowed=false
claim_gate_status=not_claim_grade
```

Every benchmark row must preserve these persistent-runner admission fields:

```text
persistent_runner_status
process_startup_attribution
python_harness_overhead_status
cli_process_wall_millis
python_harness_overhead_millis
startup_warmup_millis
build_time_millis
build_time_excluded
preparation_millis
preparation_cli_process_wall_millis
preparation_included_in_timing
```

A future persistent runner remains blocked until it has a lifecycle contract and
a protocol that preserves the same per-run typed envelopes, execution-mode
fields, Native I/O refs, operator blocker fields, materialization/decode
evidence, result-sink replay evidence, deterministic unsupported diagnostics,
and `fallback_attempted=false` / `external_engine_invoked=false` evidence. It
must also prove timing equivalence against process-per-scenario mode before any
process-overhead reduction can be interpreted as a benchmark improvement.

The source-grounded rationale is:

- [Apache Arrow's columnar format](https://arrow.apache.org/docs/format/Columnar.html) is
  data-adjacent and vectorization-friendly, so benchmark attribution must keep data-plane work
  distinct from process lifecycle overhead.
- [DuckDB's execution format](https://duckdb.org/docs/current/internals/vector) documents
  vectorized execution over vectors/data chunks, which frames operator work as batch/vector
  processing rather than process startup.
- [Spark SQL tuning](https://spark.apache.org/docs/3.5.6/sql-performance-tuning.html) exposes
  caching, batch-size, file partition, and open-cost settings, so local benchmark reports must
  separate setup/tuning choices from query/operator timing.
- [Vortex's Scan API](https://docs.vortex.dev/concepts/scanning) describes source/split/sink,
  pushdown, and compressed-array scan evidence. A persistent worker may not hide those Native I/O
  and pushdown artifacts.

## Non-Goals

This decision does not authorize runtime execution-mode changes, external engine fallback,
benchmark claims, REST server behavior, package publication, or managed-platform benchmark lanes.
