<!-- SPDX-License-Identifier: Apache-2.0 -->

# Compatibility import certified local workload

## Quick Answer

- **Audience:** user who needs local compatibility input imported into a Vortex-backed evidence path
- **Status:** `smoke_supported`
- **Execution mode:** `compatibility_import_certified`
- **Engine mode:** `batch`
- **Claim boundary:** Certification lane evidence; not pure query speed, no performance or superiority claim, no production SQL/DataFrame/object-store/lakehouse/Foundry claim.

## Can ShardLoom Do This?

Compatibility import certified local workload has a scoped local path. Treat it as technical-preview evidence with the listed claim boundary.

## How To Try It

```powershell
python benchmarks\traditional_analytics\run.py --engines shardloom --formats csv,parquet --scenario "selective filter" --dataset-profile tiny_smoke --rows 256 --iterations 3 --shardloom-build-profile debug --shardloom-result-sink --skip-shardloom-native --no-markdown --output target\shardloom-local-taxonomy-smoke.json --regenerate
```

## Internal Flow

`local_csv, local_parquet, local_jsonl_when_scenario_admits_it -> compatibility_import_certified -> batch -> prepared_vortex_artifact, result_sink_artifact, execution_certificate, native_io_certificate -> evidence -> claim gate`

## Evidence You Should See

- `source_read_millis`
- `compatibility_parse_millis`
- `compatibility_to_vortex_import_millis`
- `vortex_write_millis`
- `vortex_reopen_millis`
- `vortex_scan_millis`
- `operator_compute_millis`
- `result_sink_write_millis`
- `evidence_render_millis`
- `total_runtime_millis`
- `claim_gate_status`
- `fallback_attempted=false`
- `external_engine_invoked=false`

## Expected Output Or Evidence

Benchmark JSON with compatibility import timing separated from Vortex scan/operator/result-sink evidence.

## Common Mistakes

- `comparing_import_certification_to_external_engine_query_time`
- `hiding_import_costs`
- `omitting_result_sink_evidence`

## Reference Files

- `docs/getting-started/certified-local-workload.md`
- `docs/architecture/compute-engine-flow-reference.md`
- `docs/benchmarks/local-taxonomy-benchmark.md`
- `docs/benchmarks/baseline-comparison-boundary.md`
- `benchmarks/traditional_analytics/README.md`

## Related Use Cases

- `prepared-native-vortex-runtime-direction`
- `benchmark-interpretation-evidence-not-leaderboard`
