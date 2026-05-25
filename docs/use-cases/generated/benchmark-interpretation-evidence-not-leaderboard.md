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

## Claim Boundary

Benchmarks are local evidence and attribution, not a speed leaderboard, performance claim, superiority claim, Spark-displacement claim, or production proof.

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

- `docs/benchmarks/local-taxonomy-benchmark.md` - What this proves: Local benchmark taxonomy, evidence rows, and workload-scoped interpretation boundaries.
- `docs/benchmarks/baseline-comparison-boundary.md` - What this proves: Benchmark comparison boundaries and external-baseline-only policy.
- `benchmarks/traditional_analytics/README.md` - What this proves: Traditional analytics benchmark commands, scenarios, external baselines, and evidence interpretation.
- `docs/architecture/benchmark-suite-catalog.md` - What this proves: Benchmark scenario families and evidence coverage expectations.

## Related Use Cases

- `prepared-native-vortex-runtime-direction`
- `compatibility-import-certified-local`

## Related Field Guide Terms

- `website/field-guide/evidence-gated-compute.html` - Evidence-gated compute (`Start Here` / `smoke_supported`)
- `website/field-guide/native-vortex.html` - native_vortex (`Prepared/Native Vortex` / `smoke_supported`)
- `website/field-guide/claim-gate-status.html` - claim_gate_status (`Evidence + Certificates` / `runtime_supported`)
- `website/field-guide/benchmark-evidence.html` - Benchmark evidence (`Benchmarks` / `smoke_supported`)
- `website/field-guide/certified-cold-route.html` - Certified cold route (`Benchmarks` / `smoke_supported`)
- `website/field-guide/prepared-warm-route.html` - Prepared warm route (`Benchmarks` / `smoke_supported`)
- `website/field-guide/external-baseline-only.html` - external_baseline_only (`Benchmarks` / `runtime_supported`)
- `website/field-guide/scale-classes.html` - Scale classes (`Scale + Resource Envelope` / `planned`)
