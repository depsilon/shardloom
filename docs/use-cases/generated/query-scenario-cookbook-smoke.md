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

## Claim Boundary

Cookbook scenarios are scoped local evidence; they do not imply broad SQL, DataFrame, optimizer parity, performance, or production analytics support.

## How To Try It

```text
python benchmarks\traditional_analytics\run.py --engines shardloom --formats csv --scenario "group by aggregation" --dataset-profile tiny_smoke --rows 1000 --iterations 1 --output target\shardloom-group-by-smoke.json --regenerate
```

## Blocker

No current blocker is attached to this supported local smoke path beyond the claim boundary above.

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

- `benchmarks/traditional_analytics/README.md` - What this proves: This source anchors the page claim boundary, evidence fields, and support posture.
- `docs/architecture/benchmark-suite-catalog.md` - What this proves: This source anchors the page claim boundary, evidence fields, and support posture.
- `docs/benchmarks/local-taxonomy-benchmark.md` - What this proves: Local benchmark taxonomy, evidence rows, and workload-scoped interpretation boundaries.
- `docs/benchmarks/baseline-comparison-boundary.md` - What this proves: Benchmark comparison boundaries and external-baseline-only policy.

## Related Use Cases

- `local-file-etl-cleanup-smoke`
- `prepared-native-vortex-runtime-direction`

## Related Field Guide Terms

- `website/field-guide/source-state.html` - SourceState (`UniversalIngress` / `smoke_supported`)
- `website/field-guide/source-backed-scan.html` - Source-backed scan (`Prepared/Native Vortex` / `smoke_supported`)
- `website/field-guide/benchmark-evidence.html` - Benchmark evidence (`Benchmarks` / `smoke_supported`)
