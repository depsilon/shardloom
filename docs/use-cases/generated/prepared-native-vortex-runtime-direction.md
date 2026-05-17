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
