<!-- SPDX-License-Identifier: Apache-2.0 -->

# Query scenario cookbook smoke

## Quick Answer

- **Audience:** user mapping a familiar analytics operation to a ShardLoom smoke scenario
- **Status:** `smoke_supported`
- **Execution mode:** `compatibility_import_certified`
- **Engine mode:** `batch`
- **Claim boundary:** Cookbook scenarios are scoped local evidence; they do not imply broad SQL, DataFrame, optimizer parity, performance, or production analytics support.

## Can ShardLoom Do This?

Query scenario cookbook smoke has a scoped local path. Treat it as technical-preview evidence with the listed claim boundary.

## How To Try It

```powershell
python benchmarks\traditional_analytics\run.py --engines shardloom --formats csv --scenario "group by aggregation" --dataset-profile tiny_smoke --rows 1000 --iterations 1 --output target\shardloom-group-by-smoke.json --regenerate
```

## Internal Flow

`local_benchmark_fixture -> compatibility_import_certified -> batch -> scenario_timing_rows, correctness_digest, evidence_rows -> evidence -> claim gate`

## Evidence You Should See

- `scenario`
- `correctness_digest`
- `execution_mode`
- `operator_compute_millis`
- `data_decoded`
- `data_materialized`
- `claim_gate_status`
- `fallback_attempted=false`
- `external_engine_invoked=false`

## Expected Output Or Evidence

A benchmark artifact for one named scenario with no-fallback evidence and correctness/timing rows.

## Common Mistakes

- `assuming_all_sql_group_by_shapes_are_supported`
- `ignoring_dataset_profile`
- `using_external_baselines_as_fallback`

## Reference Files

- `benchmarks/traditional_analytics/README.md`
- `docs/architecture/benchmark-suite-catalog.md`
- `docs/benchmarks/local-taxonomy-benchmark.md`
- `docs/benchmarks/baseline-comparison-boundary.md`

## Related Use Cases

- `local-file-etl-cleanup-smoke`
- `prepared-native-vortex-runtime-direction`
