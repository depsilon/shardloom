<!-- SPDX-License-Identifier: Apache-2.0 -->

# Benchmark evidence, not leaderboard

## Quick Answer

- **Audience:** user comparing local evidence rows without overclaiming performance
- **Status:** `smoke_supported`
- **Execution mode:** `mixed_by_row`
- **Engine mode:** `batch`
- **Claim boundary:** Benchmarks are local evidence and attribution, not a speed leaderboard, performance claim, superiority claim, Spark-displacement claim, or production proof.

## Can ShardLoom Do This?

Benchmark evidence, not leaderboard has a scoped local path. Treat it as technical-preview evidence with the listed claim boundary.

## How To Try It

```powershell
python examples\local-vortex-benchmark\run.py --repo-root . --rows 64 --iterations 1
```

## Internal Flow

`local_benchmark_fixture, optional_external_baselines -> mixed_by_row -> batch -> timing_rows, coverage_rows, claim_boundary_notes -> evidence -> claim gate`

## Evidence You Should See

- `engine`
- `execution_mode`
- `source_read_millis`
- `compatibility_parse_millis`
- `vortex_prepare_millis`
- `vortex_scan_millis`
- `operator_compute_millis`
- `result_sink_write_millis`
- `claim_gate_status`
- `fallback_attempted=false`
- `external_engine_invoked=false`

## Expected Output Or Evidence

Timing rows and coverage rows that keep ShardLoom runtime lanes separate from optional external baselines.

## Common Mistakes

- `ranking_external_engines_as_public_claims`
- `comparing_import_costs_to_pure_query_speed`
- `hiding_external_baseline_only_status`

## Reference Files

- `docs/benchmarks/local-taxonomy-benchmark.md`
- `docs/benchmarks/baseline-comparison-boundary.md`
- `benchmarks/traditional_analytics/README.md`
- `docs/architecture/benchmark-suite-catalog.md`

## Related Use Cases

- `prepared-native-vortex-runtime-direction`
- `compatibility-import-certified-local`
