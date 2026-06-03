<!-- SPDX-License-Identifier: Apache-2.0 -->

# Compatibility import certified local workload

## Quick Answer

- **Audience:** user who needs local compatibility input imported into a Vortex-backed evidence path
- **Status:** `smoke_supported`
- **Execution mode:** `compatibility_import_certified`
- **Engine mode:** `batch`
- **Claim boundary:** Certified cold-route evidence; CSV/JSONL cold imports should report text-adapter RecordBatch import without persistent traditional row buffers and writer-byte Vortex digest attribution after the current batch lands. This is not pure query speed, not a performance or superiority claim, and not a production SQL/DataFrame/object-store/lakehouse/Foundry claim.

## Can ShardLoom Do This?

Compatibility import certified local workload has a scoped local path. Treat it as technical-preview evidence with the listed claim boundary.

## Claim Boundary

Certified cold-route evidence; CSV/JSONL cold imports should report text-adapter RecordBatch import without persistent traditional row buffers and writer-byte Vortex digest attribution after the current batch lands. This is not pure query speed, not a performance or superiority claim, and not a production SQL/DataFrame/object-store/lakehouse/Foundry claim.

## How To Try It

```text
python benchmarks\traditional_analytics\run.py --engines shardloom --formats csv,parquet --scenario "selective filter" --dataset-profile tiny_smoke --rows 256 --iterations 3 --shardloom-build-profile debug --shardloom-result-sink --skip-shardloom-native --no-markdown --output target\shardloom-local-taxonomy-smoke.json --regenerate
```

## Blocker

No current blocker is attached to this supported local smoke path beyond the claim boundary above.

## Internal Flow

`local_csv, local_parquet, local_jsonl_when_scenario_admits_it -> compatibility_import_certified -> batch -> vortex_prepared_state, result_sink_artifact, execution_certificate, native_io_certificate -> evidence -> claim gate`

## Evidence You Should See

- `source_adapter_status`
- `ingress_route`
- `vortex_ingest_status`
- `source_read_millis`
- `compatibility_parse_millis`
- `source_state_materialization_layout`
- `source_state_record_batch_count`
- `source_state_columnar_preserved`
- `compatibility_to_vortex_import_millis`
- `vortex_array_build_strategy`
- `vortex_array_build_input_layout`
- `vortex_write_millis`
- `vortex_digest_millis`
- `vortex_reopen_millis`
- `vortex_scan_millis`
- `operator_compute_millis`
- `result_sink_write_millis`
- `evidence_render_millis`
- `total_runtime_millis`
- `timing_scope=cold_certified_end_to_end`
- `claim_gate_status`
- `fallback_attempted=false`
- `external_engine_invoked=false`

## Expected Output Or Evidence

Benchmark JSON with compatibility import timing separated from Vortex scan/operator/result-sink evidence, including source-state materialization layout, RecordBatch count, Vortex array-build strategy/input layout, and digest timing attribution.

## Common Mistakes

- `comparing_import_certification_to_external_engine_query_time`
- `hiding_import_costs`
- `omitting_result_sink_evidence`

## Reference Files

- `docs/getting-started/certified-local-workload.md` - What this proves: This source anchors the page claim boundary, evidence fields, and support posture.
- `docs/architecture/compute-engine-flow-reference.md` - What this proves: Canonical execution-mode, engine-mode, evidence, and claim-gate flow definitions.
- `docs/benchmarks/local-taxonomy-benchmark.md` - What this proves: Local benchmark taxonomy, evidence rows, and workload-scoped interpretation boundaries.
- `docs/benchmarks/baseline-comparison-boundary.md` - What this proves: Benchmark comparison boundaries and external-baseline-only policy.
- `benchmarks/traditional_analytics/README.md` - What this proves: This source anchors the page claim boundary, evidence fields, and support posture.

## Related Use Cases

- `prepared-native-vortex-runtime-direction`
- `benchmark-interpretation-evidence-not-leaderboard`

## Related Field Guide Terms

- `website/field-guide/compatibility-import-certified.html` - compatibility_import_certified (`Execution Routes` / `smoke_supported`)
- `website/field-guide/source-adapter-status.html` - Source adapter status (`UniversalIngress` / `smoke_supported`)
- `website/field-guide/certified-cold-route.html` - Certified cold route (`Benchmarks` / `smoke_supported`)
