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

- `website/field-guide/technical-preview.html` - Technical Preview (`Start Here` / `pre-release`)
- `website/field-guide/execution-modes.html` - Execution Modes (`Execution Modes` / `current-vocabulary`)
- `website/field-guide/compatibility-import-certified.html` - Compatibility Import Certified (`Execution Modes` / `current-certification-lane`)
- `website/field-guide/prepared-vortex.html` - Prepared Vortex (`Execution Modes` / `runtime-development-lane`)
- `website/field-guide/native-vortex.html` - Native Vortex (`Execution Modes` / `scoped-runtime-lane`)
- `website/field-guide/source-backed-scan.html` - Source-Backed Scan (`Vortex Runtime` / `scoped-evidence`)
- `website/field-guide/vortex-scan-api.html` - Vortex Scan API (`Vortex Runtime` / `report-only-to-scoped`)
- `website/field-guide/encoded-predicate-provider.html` - Encoded Predicate Provider (`Vortex Runtime` / `scoped-evidence`)
- `website/field-guide/selection-vector.html` - Selection Vector (`Vortex Runtime` / `scoped-evidence`)
- `website/field-guide/encoded-native.html` - Encoded-Native (`Vortex Runtime` / `claim-gated`)
- `website/field-guide/materialization-boundary.html` - Materialization Boundary (`Evidence And Claims` / `current-evidence`)
- `website/field-guide/claim-gates.html` - Claim Gates (`Evidence And Claims` / `core-contract`)
- `website/field-guide/external-baseline-only.html` - External Baseline Only (`Evidence And Claims` / `core-boundary`)
- `website/field-guide/benchmark-telemetry.html` - Benchmark Telemetry (`Benchmark Telemetry` / `current-evidence`)
- `website/field-guide/local-timing-context.html` - Local Timing Context (`Benchmark Telemetry` / `current-evidence`)
- `website/field-guide/prepared-native-batch-smoke.html` - Prepared/Native Batch Smoke (`Benchmark Telemetry` / `current-smoke-evidence`)
- `website/field-guide/benchmark-artifact-manifest.html` - Benchmark Artifact Manifest (`Benchmark Telemetry` / `current-publishing-contract`)
- `website/field-guide/benchmark-profile.html` - Benchmark Profile (`Benchmark Telemetry` / `current-publishing-contract`)
- `website/field-guide/evidence-level.html` - Evidence Level (`Performance Architecture` / `current-vocabulary`)
- `website/field-guide/minimal-runtime.html` - Minimal Runtime Evidence Level (`Performance Architecture` / `runtime-development`)
- `website/field-guide/compressed-encoded-kernel-registry.html` - Compressed/Encoded Kernel Registry (`Performance Architecture` / `scoped-evidence`)
- `website/field-guide/shardloom-session.html` - ShardLoom Session (`Performance Architecture` / `scoped-batch-runtime`)
- `website/field-guide/public-claim-boundary.html` - Public Claim Boundary (`Release And Trust` / `core-contract`)
