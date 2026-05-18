<!-- SPDX-License-Identifier: Apache-2.0 -->

# Prepared/native Vortex runtime direction

## Quick Answer

- **Audience:** user evaluating the current runtime-development lane
- **Status:** `smoke_supported`
- **Execution mode:** `prepared_vortex/native_vortex`
- **Engine mode:** `batch`
- **Claim boundary:** Prepared/native smoke and structural evidence only; no broad encoded-native, performance, superiority, SQL/DataFrame, object-store, lakehouse, Foundry, or Spark-replacement claim.

## Can ShardLoom Do This?

Prepared/native Vortex runtime direction has a scoped local path. Treat it as technical-preview evidence with the listed claim boundary.

## How To Try It

```powershell
python benchmarks\traditional_analytics\run.py --engines shardloom-prepared-vortex --formats csv,jsonl,parquet,arrow-ipc,avro,orc --scenario "filter + projection + limit" --dataset-profile tiny_smoke --rows 1000 --iterations 1 --output target\shardloom-prepared-vortex-smoke.json --regenerate
```

## Internal Flow

`local_prepared_vortex_artifact, benchmark_fixture -> prepared_vortex/native_vortex -> batch -> prepared_native_timing_rows, source_backed_scan_evidence -> evidence -> claim gate`

## Evidence You Should See

- `execution_mode`
- `source_backed_scan_used`
- `source_state_reuse_hit`
- `encoded_predicate_provider_status`
- `data_decoded`
- `data_materialized`
- `claim_gate_status`
- `fallback_attempted=false`
- `external_engine_invoked=false`

## Expected Output Or Evidence

Prepared Vortex rows separate from compatibility import rows, with source-backed scan and no-fallback fields where available.

## Common Mistakes

- `calling_prepared_vortex_production_ready`
- `treating_encoded_predicate_fields_as_encoded_native_claims`
- `expecting_sql_runtime`

## Reference Files

- `docs/architecture/compute-engine-flow-reference.md`
- `docs/benchmarks/local-taxonomy-benchmark.md`
- `benchmarks/traditional_analytics/README.md`
- `docs/architecture/benchmark-suite-catalog.md`

## Related Use Cases

- `compatibility-import-certified-local`
- `benchmark-interpretation-evidence-not-leaderboard`

## Related Field Guide Terms

- `website/field-guide/vortex-first.html` - Vortex-First (`Start Here` / `core-contract`)
- `website/field-guide/execution-modes.html` - Execution Modes (`Execution Modes` / `current-vocabulary`)
- `website/field-guide/prepared-vortex.html` - Prepared Vortex (`Execution Modes` / `runtime-development-lane`)
- `website/field-guide/native-vortex.html` - Native Vortex (`Execution Modes` / `scoped-runtime-lane`)
- `website/field-guide/auto-execution-mode.html` - Auto Execution Mode (`Execution Modes` / `transparent-selection`)
- `website/field-guide/engine-modes.html` - Engine Modes (`Engine Modes` / `current-vocabulary`)
- `website/field-guide/batch-engine.html` - Batch Engine (`Engine Modes` / `scoped-local`)
- `website/field-guide/vortex-native.html` - Vortex-Native (`Vortex Runtime` / `core-contract`)
- `website/field-guide/prepared-vortex-artifact.html` - Prepared Vortex Artifact (`Vortex Runtime` / `runtime-development-artifact`)
- `website/field-guide/source-backed-scan.html` - Source-Backed Scan (`Vortex Runtime` / `scoped-evidence`)
- `website/field-guide/vortex-scan-api.html` - Vortex Scan API (`Vortex Runtime` / `report-only-to-scoped`)
- `website/field-guide/encoded-predicate-provider.html` - Encoded Predicate Provider (`Vortex Runtime` / `scoped-evidence`)
- `website/field-guide/selection-vector.html` - Selection Vector (`Vortex Runtime` / `scoped-evidence`)
- `website/field-guide/residual-native.html` - Residual-Native (`Vortex Runtime` / `current-runtime-pattern`)
- `website/field-guide/encoded-native.html` - Encoded-Native (`Vortex Runtime` / `claim-gated`)
- `website/field-guide/materialization-boundary.html` - Materialization Boundary (`Evidence And Claims` / `current-evidence`)
- `website/field-guide/benchmark-telemetry.html` - Benchmark Telemetry (`Benchmark Telemetry` / `current-evidence`)
- `website/field-guide/prepared-native-batch-smoke.html` - Prepared/Native Batch Smoke (`Benchmark Telemetry` / `current-smoke-evidence`)
- `website/field-guide/source-state-reuse.html` - Source-State Reuse (`Benchmark Telemetry` / `scoped-evidence`)
- `website/field-guide/vortex-result-artifact.html` - Vortex Result Artifact (`I/O And Output` / `highest-fidelity-target`)
- `website/field-guide/minimal-runtime.html` - Minimal Runtime Evidence Level (`Performance Architecture` / `runtime-development`)
- `website/field-guide/fused-operator-pipeline.html` - Fused Operator Pipeline (`Performance Architecture` / `scoped-evidence`)
- `website/field-guide/compressed-encoded-kernel-registry.html` - Compressed/Encoded Kernel Registry (`Performance Architecture` / `scoped-evidence`)
- `website/field-guide/shardloom-session.html` - ShardLoom Session (`Performance Architecture` / `scoped-batch-runtime`)
