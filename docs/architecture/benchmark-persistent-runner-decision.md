# Benchmark Persistent Runner Decision

Status: Accepted for P7.5.7  
Scope: `benchmarks/traditional_analytics/run.py` and local ShardLoom comparative rows

## Summary

P7.5.7 keeps the current Python-driven per-scenario CLI runner and makes process overhead explicit
instead of adding a persistent in-process runner in this slice.

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

Do not add a persistent runner yet.

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

Until those requirements are met, reports must say:

```text
persistent_runner_status=process_per_scenario_attributed_not_reduced
```

## Non-Goals

This decision does not authorize runtime execution-mode changes, external engine fallback,
benchmark claims, REST server behavior, package publication, or managed-platform benchmark lanes.
