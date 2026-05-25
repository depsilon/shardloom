<!-- SPDX-License-Identifier: Apache-2.0 -->

# Messy data fixture coverage

## Quick Answer

- **Audience:** user with dirty CSV, nested JSON, null-heavy, or CDC-like local fixture needs
- **Status:** `smoke_supported`
- **Execution mode:** `compatibility_import_certified`
- **Engine mode:** `batch`
- **Claim boundary:** Local fixture smoke coverage only; not a production data-quality, CDC/table-transaction, lakehouse, streaming, or performance claim.

## Can ShardLoom Do This?

Messy data fixture coverage has a scoped local path. Treat it as technical-preview evidence with the listed claim boundary.

## Claim Boundary

Local fixture smoke coverage only; not a production data-quality, CDC/table-transaction, lakehouse, streaming, or performance claim.

## How To Try It

```powershell
python benchmarks\traditional_analytics\run.py --engines shardloom --formats csv,jsonl --scenario "malformed timestamp / dirty CSV" --dataset-profile dirty_csv --rows 1000 --iterations 1 --output target\shardloom-dirty-csv-smoke.json --regenerate
```

## Internal Flow

`dirty_csv_fixture, nested_json_fixture, cdc_delta_overlay_fixture, null_heavy_fixture -> compatibility_import_certified -> batch -> local_benchmark_artifact, fixture_sidecars, evidence_rows -> evidence -> claim gate`

## Evidence You Should See

- `scenario_family`
- `dataset_profile`
- `source_metadata_snapshot_status`
- `source_state_reuse_hit`
- `materialization_boundary`
- `claim_gate_status`
- `fallback_attempted=false`
- `external_engine_invoked=false`

## Expected Output Or Evidence

Benchmark rows and fixture metadata for the selected messy-data scenario with no fallback.

## Common Mistakes

- `treating_fixture_cdc_as_table_commit`
- `expecting_general_json_schema_inference`
- `reading_smoke_as_data_quality_product`

## Reference Files

- `benchmarks/traditional_analytics/README.md` - What this proves: Traditional analytics benchmark commands, scenarios, external baselines, and evidence interpretation.
- `docs/architecture/benchmark-suite-catalog.md` - What this proves: Benchmark scenario families and evidence coverage expectations.
- `docs/benchmarks/local-taxonomy-benchmark.md` - What this proves: Local benchmark taxonomy, evidence rows, and workload-scoped interpretation boundaries.
- `docs/getting-started/examples.md` - What this proves: Current example catalog and local workflow entrypoints.

## Related Use Cases

- `query-scenario-cookbook-smoke`
- `benchmark-interpretation-evidence-not-leaderboard`

## Related Field Guide Terms

- `website/field-guide/source-adapter-status.html` - Source adapter status (`UniversalIngress` / `smoke_supported`)
- `website/field-guide/benchmark-evidence.html` - Benchmark evidence (`Benchmarks` / `smoke_supported`)
