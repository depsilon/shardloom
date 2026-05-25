<!-- SPDX-License-Identifier: Apache-2.0 -->

# Prepared/native Vortex runtime direction

## Quick Answer

- **Audience:** user evaluating the current runtime-development lane
- **Status:** `smoke_supported`
- **Execution mode:** `prepared_vortex/native_vortex`
- **Engine mode:** `batch`
- **Claim boundary:** Prepared/native smoke and structural evidence only; prepared_vortex starts from VortexPreparedState, while shardloom-prepare-batch prepares local compatibility inputs in the same CLI process before child query timing. No broad encoded-native, performance, superiority, SQL/DataFrame, object-store, lakehouse, Foundry, or Spark-replacement claim.

## Can ShardLoom Do This?

Prepared/native Vortex runtime direction has a scoped local path. Treat it as technical-preview evidence with the listed claim boundary.

## Claim Boundary

Prepared/native smoke and structural evidence only; prepared_vortex starts from VortexPreparedState, while shardloom-prepare-batch prepares local compatibility inputs in the same CLI process before child query timing. No broad encoded-native, performance, superiority, SQL/DataFrame, object-store, lakehouse, Foundry, or Spark-replacement claim.

## How To Try It

```powershell
python benchmarks\traditional_analytics\run.py --engines shardloom-prepared-vortex,shardloom-prepare-batch --formats csv,jsonl,parquet,arrow-ipc,avro,orc --scenario "filter + projection + limit" --dataset-profile tiny_smoke --rows 1000 --iterations 1 --output target\shardloom-prepared-vortex-smoke.json --regenerate
```

## Internal Flow

`vortex_prepared_state, local_prepared_vortex_artifact, benchmark_fixture -> prepared_vortex/native_vortex -> batch -> prepared_native_timing_rows, source_backed_scan_evidence -> evidence -> claim gate`

## Evidence You Should See

- `execution_mode`
- `prepared_state_id`
- `prepared_state_digest`
- `timing_scope=warm_prepared_query`
- `source_backed_scan_used`
- `source_state_reuse_hit`
- `encoded_predicate_provider_status`
- `prepare_batch_route`
- `prepare_batch_preparation_millis`
- `prepare_batch_source_to_columnar_millis`
- `data_decoded`
- `data_materialized`
- `claim_gate_status`
- `fallback_attempted=false`
- `external_engine_invoked=false`

## Expected Output Or Evidence

Warm prepared Vortex rows separate from single-process prepare/batch rows, with source-backed scan, prepare_batch, and no-fallback fields where available.

## Common Mistakes

- `calling_prepared_vortex_production_ready`
- `treating_encoded_predicate_fields_as_encoded_native_claims`
- `expecting_sql_runtime`

## Reference Files

- `docs/architecture/compute-engine-flow-reference.md` - What this proves: Canonical execution-mode, engine-mode, evidence, and claim-gate flow definitions.
- `docs/benchmarks/local-taxonomy-benchmark.md` - What this proves: Local benchmark taxonomy, evidence rows, and workload-scoped interpretation boundaries.
- `benchmarks/traditional_analytics/README.md` - What this proves: Traditional analytics benchmark commands, scenarios, external baselines, and evidence interpretation.
- `docs/architecture/benchmark-suite-catalog.md` - What this proves: Benchmark scenario families and evidence coverage expectations.

## Related Use Cases

- `compatibility-import-certified-local`
- `benchmark-interpretation-evidence-not-leaderboard`

## Related Field Guide Terms

- `website/field-guide/source-state.html` - SourceState (`UniversalIngress` / `smoke_supported`)
- `website/field-guide/vortex-ingest.html` - vortex_ingest (`Vortex Ingest` / `smoke_supported`)
- `website/field-guide/vortex-prepared-state.html` - VortexPreparedState (`Vortex Ingest` / `smoke_supported`)
- `website/field-guide/prepared-state-reuse.html` - Prepared state reuse (`Vortex Ingest` / `smoke_supported`)
- `website/field-guide/prepared-vortex.html` - prepared_vortex (`Prepared/Native Vortex` / `smoke_supported`)
- `website/field-guide/native-vortex.html` - native_vortex (`Prepared/Native Vortex` / `smoke_supported`)
- `website/field-guide/source-backed-scan.html` - Source-backed scan (`Prepared/Native Vortex` / `smoke_supported`)
- `website/field-guide/materialization-boundary.html` - Materialization boundary (`Evidence + Certificates` / `smoke_supported`)
- `website/field-guide/prepared-warm-route.html` - Prepared warm route (`Benchmarks` / `smoke_supported`)
