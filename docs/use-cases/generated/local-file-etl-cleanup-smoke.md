<!-- SPDX-License-Identifier: Apache-2.0 -->

# Local file ETL smoke

## Quick Answer

- **Audience:** user with local CSV or Parquet data
- **Status:** `smoke_supported`
- **Execution mode:** `compatibility_import_certified`
- **Engine mode:** `batch`
- **Claim boundary:** Scoped local technical-preview workflow only; not production ETL, broad SQL/DataFrame, object-store, lakehouse, Foundry, performance, or Spark-replacement proof.

## Can ShardLoom Do This?

Local file ETL smoke has a scoped local path. Treat it as technical-preview evidence with the listed claim boundary.

## How To Try It

```powershell
python examples\local-vortex-benchmark\run.py --repo-root . --rows 64 --iterations 1
```

## Internal Flow

`local_csv, local_parquet -> compatibility_import_certified -> batch -> local_vortex_artifact, local_result_sink_evidence -> evidence -> claim gate`

## Evidence You Should See

- `execution_mode`
- `claim_gate_status`
- `native_io_certificate_status`
- `materialization_boundary`
- `fallback_attempted=false`
- `external_engine_invoked=false`

## Expected Output Or Evidence

A target/shardloom-local-vortex-benchmark-smoke.json artifact with timing, coverage, result-sink, and no-fallback fields.

## Common Mistakes

- `reading_compatibility_import_as_pure_query_speed`
- `expecting_s3_io`
- `treating_smoke_as_production_etl`

## Reference Files

- `docs/getting-started/examples.md`
- `docs/getting-started/certified-local-workload.md`
- `docs/architecture/compute-engine-flow-reference.md`
- `docs/benchmarks/local-taxonomy-benchmark.md`
- `examples/local-vortex-benchmark/README.md`

## Related Use Cases

- `compatibility-import-certified-local`
- `messy-data-local-fixtures`
- `output-result-sink-and-fanout-boundary`
